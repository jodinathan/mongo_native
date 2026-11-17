//! Change streams: a pump task reads the driver stream ahead and `stream.next` drains it in batches.

use std::time::Duration;

use bson::{doc, Bson, Document, RawArrayBuf, RawDocumentBuf};
use mongodb::change_stream::event::ResumeToken;
use mongodb::change_stream::ChangeStream;
use mongodb::error::ErrorKind;
use mongodb::options::ChangeStreamOptions;
use mongodb::Client;
use tokio::sync::{mpsc, Mutex};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::error::{from_driver, NativeError};
use crate::registry;
use crate::reply;
use crate::request::Request;
use crate::runtime;

enum Message {
    Event(RawDocumentBuf, Option<ResumeToken>),
    Idle(Option<ResumeToken>),
    Failed(NativeError),
    Ended,
}

struct Inner {
    rx: mpsc::Receiver<Message>,
    token: Option<ResumeToken>,
    failure: Option<NativeError>,
    ended: bool,
}

/// A change stream whose events are read ahead by a pump task.
pub struct StreamState {
    max_await: Duration,
    max_events: usize,
    inner: Mutex<Inner>,
    pump: AbortHandle,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

/// What a change stream is opened with, kept to open it again after an election.
#[derive(Clone)]
pub struct Watch {
    /// The driver client.
    pub client: Client,
    /// The database watched.
    pub db: String,
    /// The collection watched, or the whole database when absent.
    pub coll: Option<String>,
    /// The aggregation stages after `$changeStream`.
    pub pipeline: Vec<Document>,
    /// The options of the first opening.
    pub options: ChangeStreamOptions,
}

impl Watch {
    /// Opens the stream; with [resume], right after that token instead of the first options' point.
    pub async fn open(&self, resume: Option<ResumeToken>) -> mongodb::error::Result<ChangeStream<RawDocumentBuf>> {
        let mut options = self.options.clone();
        if resume.is_some() {
            options.resume_after = resume;
            options.start_after = None;
            options.start_at_operation_time = None;
        }
        let db = self.client.database(&self.db);
        let stream = match &self.coll {
            Some(coll) => {
                db.collection::<Document>(coll)
                    .watch()
                    .pipeline(self.pipeline.clone())
                    .with_options(options)
                    .await?
            }
            None => db.watch().pipeline(self.pipeline.clone()).with_options(options).await?,
        };
        Ok(stream.with_type::<RawDocumentBuf>())
    }
}

/// Server codes after which a change stream resumes (the resumable list of the change stream
/// specification). MongoDB 9.0 answers `InterruptedDueToReplStateChange` (11602) to a `getMore`
/// after an election without the `ResumableChangeStreamError` label, which the driver alone does
/// not resume.
const RESUMABLE_CODES: [i32; 16] = [
    6, 7, 89, 91, 133, 150, 189, 234, 262, 9001, 10107, 11600, 11602, 13388, 13435, 13436,
];

fn resumable(e: &mongodb::error::Error) -> bool {
    match e.kind.as_ref() {
        ErrorKind::Command(c) => RESUMABLE_CODES.contains(&c.code),
        _ => false,
    }
}

impl StreamState {
    /// Starts the pump over [stream] opened from [watch], waiting up to [max_await] per batch and
    /// keeping up to [max_events] events ahead.
    pub fn start(
        watch: Watch,
        stream: ChangeStream<RawDocumentBuf>,
        max_await: Duration,
        max_events: usize,
        operation: String,
    ) -> StreamState {
        let token = stream.resume_token();
        let (tx, rx) = mpsc::channel(max_events.clamp(1, 10_000));
        let pump = runtime::handle()
            .spawn(pump(watch, stream, tx, operation))
            .abort_handle();
        StreamState {
            max_await,
            max_events,
            inner: Mutex::new(Inner {
                rx,
                token,
                failure: None,
                ended: false,
            }),
            pump,
        }
    }

    /// The resume token reached so far.
    pub async fn token(&self) -> Option<ResumeToken> {
        self.inner.lock().await.token.clone()
    }
}

async fn pump(watch: Watch, mut stream: ChangeStream<RawDocumentBuf>, tx: mpsc::Sender<Message>, operation: String) {
    let mut token = stream.resume_token();
    let mut reopenings = 0u32;
    loop {
        let message = match stream.next_if_any().await {
            Ok(Some(event)) => {
                reopenings = 0;
                token = stream.resume_token();
                Message::Event(event, token.clone())
            }
            Ok(None) if !stream.is_alive() => Message::Ended,
            Ok(None) => {
                reopenings = 0;
                token = stream.resume_token().or(token);
                Message::Idle(token.clone())
            }
            Err(e) if resumable(&e) && reopenings < 10 => {
                reopenings += 1;
                match reopen(&watch, &token, &mut reopenings).await {
                    Ok(next) => {
                        stream = next;
                        continue;
                    }
                    Err(e) => Message::Failed(from_driver(&operation, e)),
                }
            }
            Err(e) => Message::Failed(from_driver(&operation, e)),
        };
        let last = matches!(message, Message::Failed(_) | Message::Ended);
        if tx.send(message).await.is_err() || last {
            return;
        }
    }
}

async fn reopen(
    watch: &Watch,
    token: &Option<ResumeToken>,
    reopenings: &mut u32,
) -> mongodb::error::Result<ChangeStream<RawDocumentBuf>> {
    loop {
        match watch.open(token.clone()).await {
            Ok(stream) => return Ok(stream),
            Err(e) if resumable(&e) && *reopenings < 10 => {
                *reopenings += 1;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

fn ended(operation: &str) -> NativeError {
    NativeError::closed(
        operation,
        "the change stream ended: the server invalidated it (the watched collection or database was dropped or \
         renamed); open a new stream with startAfter set to the last resume token",
    )
}

fn token_bson(operation: &str, token: &Option<ResumeToken>) -> Result<Bson, NativeError> {
    match token {
        Some(t) => bson::to_bson(t)
            .map_err(|e| NativeError::bson(operation, format!("could not encode the resume token: {e}"))),
        None => Ok(Bson::Null),
    }
}

/// The reply of `watch`: the stream handle and its first resume token.
pub async fn opened(client: u64, state: StreamState, operation: &str) -> Result<RawDocumentBuf, NativeError> {
    let token = token_bson(operation, &state.token().await)?;
    let handle = registry::insert_stream(client, state, operation)?;
    reply::document(operation, &doc! { "stream": handle as i64, "resumeToken": token })
}

/// `stream.next`: waits up to the stream's `maxAwait` for events and returns them with the resume
/// token after the last one; a failure closes the stream.
pub async fn next(req: &Request, operation: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("stream")?;
    let state = registry::stream(handle, operation)?;
    let result = drain(&state, operation).await;
    if result.is_err() {
        drop(registry::remove_stream(handle));
    }
    let (events, token) = result?;
    let mut out = reply::document(operation, &doc! { "resumeToken": token_bson(operation, &token)? })?;
    out.append("events", events);
    Ok(out)
}

async fn drain(state: &StreamState, operation: &str) -> Result<(RawArrayBuf, Option<ResumeToken>), NativeError> {
    let mut inner = state.inner.lock().await;
    if let Some(failure) = inner.failure.take() {
        return Err(failure);
    }
    if inner.ended {
        return Err(ended(operation));
    }
    let deadline = Instant::now() + state.max_await;
    let mut events = RawArrayBuf::new();
    let mut count = 0usize;
    while count < state.max_events {
        let message = if count == 0 {
            match tokio::time::timeout_at(deadline, inner.rx.recv()).await {
                Ok(message) => message,
                Err(_) => break,
            }
        } else {
            match inner.rx.try_recv() {
                Ok(message) => Some(message),
                Err(_) => break,
            }
        };
        match message {
            Some(Message::Event(event, token)) => {
                events.push(event);
                if token.is_some() {
                    inner.token = token;
                }
                count += 1;
            }
            Some(Message::Idle(token)) => {
                if token.is_some() {
                    inner.token = token;
                }
            }
            Some(Message::Failed(e)) if count == 0 => return Err(e),
            Some(Message::Failed(e)) => {
                inner.failure = Some(e);
                break;
            }
            Some(Message::Ended) | None if count == 0 => return Err(ended(operation)),
            Some(Message::Ended) | None => {
                inner.ended = true;
                break;
            }
        }
    }
    Ok((events, inner.token.clone()))
}

/// `stream.close`: removes the stream; dropping it stops the pump and kills the server cursor.
pub async fn close(req: &Request, operation: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("stream")?;
    drop(registry::remove_stream(handle));
    reply::document(operation, &doc! {})
}
