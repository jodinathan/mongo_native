//! Process-wide registry of requests in flight and of the client, cursor and stream handles.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use bson::RawDocumentBuf;
use mongodb::{Client, Cursor};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::error::NativeError;
use crate::stream::StreamState;

/// A cursor shared between the registry and the request that is reading it.
pub type SharedCursor = Arc<tokio::sync::Mutex<Cursor<RawDocumentBuf>>>;

struct RequestEntry {
    abort: AbortHandle,
    operation: String,
    outcome_unknown: bool,
    client: Option<u64>,
    closed_by_client: bool,
}

struct ClientEntry {
    client: Client,
    closed: bool,
    inflight: HashSet<u64>,
}

struct CursorEntry {
    client: u64,
    cursor: SharedCursor,
}

struct StreamEntry {
    client: u64,
    state: Arc<StreamState>,
}

#[derive(Default)]
struct State {
    requests: HashMap<u64, RequestEntry>,
    clients: HashMap<u64, ClientEntry>,
    cursors: HashMap<u64, CursorEntry>,
    streams: HashMap<u64, StreamEntry>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| Mutex::new(State::default()));
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);
static SETTLED: Notify = Notify::const_new();

fn state() -> MutexGuard<'static, State> {
    STATE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn next_handle() -> u64 {
    NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
}

/// Records the task of [request] so that it can be cancelled.
pub fn track(request: u64, abort: AbortHandle) {
    state().requests.insert(
        request,
        RequestEntry {
            abort,
            operation: format!("request {request}"),
            outcome_unknown: false,
            client: None,
            closed_by_client: false,
        },
    );
}

/// Names the [operation] of [request], whether an interruption leaves its outcome unknown, and the
/// [client] it runs on; fails when that client is closed or unknown.
pub fn describe(request: u64, operation: &str, outcome_unknown: bool, client: Option<u64>) -> Result<(), NativeError> {
    let mut s = state();
    if let Some(handle) = client {
        match s.clients.get_mut(&handle) {
            Some(entry) if !entry.closed => {
                entry.inflight.insert(request);
            }
            _ => return Err(client_closed(operation)),
        }
    }
    if let Some(entry) = s.requests.get_mut(&request) {
        entry.operation = operation.to_string();
        entry.outcome_unknown = outcome_unknown;
        entry.client = client;
    }
    Ok(())
}

/// Drops [request] from the registry after its completion was delivered.
pub fn forget(request: u64) {
    let mut s = state();
    if let Some(entry) = s.requests.remove(&request) {
        if let Some(handle) = entry.client {
            if let Some(client) = s.clients.get_mut(&handle) {
                client.inflight.remove(&request);
            }
        }
    }
    drop(s);
    SETTLED.notify_waiters();
}

/// Aborts [request]; returns whether it was still running.
pub fn cancel(request: u64) -> bool {
    match state().requests.get(&request) {
        Some(entry) => {
            entry.abort.abort();
            true
        }
        None => false,
    }
}

/// The operation name recorded for [request].
pub fn operation(request: u64) -> String {
    state()
        .requests
        .get(&request)
        .map(|e| e.operation.clone())
        .unwrap_or_else(|| format!("request {request}"))
}

/// The failure of [request] after its task was aborted: closed when its client was closed,
/// cancelled otherwise.
pub fn interrupted(request: u64) -> NativeError {
    let s = state();
    match s.requests.get(&request) {
        Some(entry) if entry.closed_by_client => NativeError::closed(
            &entry.operation,
            "the client was closed while the operation was running; its outcome is unknown",
        )
        .with("outcomeUnknown", entry.outcome_unknown),
        Some(entry) => NativeError::new(
            "cancelled",
            &entry.operation,
            if entry.outcome_unknown {
                "the operation was cancelled; the server may have applied it"
            } else {
                "the operation was cancelled"
            },
        )
        .with("outcomeUnknown", entry.outcome_unknown),
        None => NativeError::new(
            "cancelled",
            &format!("request {request}"),
            "the operation was cancelled",
        )
        .with("outcomeUnknown", true),
    }
}

/// The failure of [operation] on a client that was closed or never existed.
pub fn client_closed(operation: &str) -> NativeError {
    NativeError::closed(operation, "the client is closed; connect again to keep working")
}

/// Registers an open [client] and returns its handle.
pub fn insert_client(client: Client) -> u64 {
    let handle = next_handle();
    state().clients.insert(
        handle,
        ClientEntry {
            client,
            closed: false,
            inflight: HashSet::new(),
        },
    );
    handle
}

/// The driver client behind [handle].
pub fn client(handle: u64, operation: &str) -> Result<Client, NativeError> {
    match state().clients.get(&handle) {
        Some(entry) if !entry.closed => Ok(entry.client.clone()),
        _ => Err(client_closed(operation)),
    }
}

/// What a closing client leaves to be dropped and shut down.
pub struct Closing {
    /// The driver client to shut down.
    pub client: Client,
    /// The cursors of the client, dropped inside the runtime so that they are killed on the server.
    pub cursors: Vec<SharedCursor>,
    /// The change streams of the client.
    pub streams: Vec<Arc<StreamState>>,
}

/// Marks [handle] closed, aborts its requests in flight and takes its cursors and streams.
pub fn close_client(handle: u64, operation: &str) -> Result<Closing, NativeError> {
    let mut s = state();
    let State {
        requests,
        clients,
        cursors,
        streams,
    } = &mut *s;
    let entry = match clients.get_mut(&handle) {
        Some(entry) if !entry.closed => entry,
        _ => return Err(client_closed(operation)),
    };
    entry.closed = true;
    for request in &entry.inflight {
        if let Some(r) = requests.get_mut(request) {
            r.closed_by_client = true;
            r.abort.abort();
        }
    }
    let client = entry.client.clone();
    let cursor_ids: Vec<u64> = cursors
        .iter()
        .filter(|(_, c)| c.client == handle)
        .map(|(id, _)| *id)
        .collect();
    let stream_ids: Vec<u64> = streams
        .iter()
        .filter(|(_, c)| c.client == handle)
        .map(|(id, _)| *id)
        .collect();
    Ok(Closing {
        client,
        cursors: cursor_ids
            .iter()
            .filter_map(|id| cursors.remove(id))
            .map(|c| c.cursor)
            .collect(),
        streams: stream_ids
            .iter()
            .filter_map(|id| streams.remove(id))
            .map(|c| c.state)
            .collect(),
    })
}

/// Waits until no request of [handle] is in flight.
pub async fn settled(handle: u64) {
    loop {
        let notified = SETTLED.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let busy = state().clients.get(&handle).is_some_and(|c| !c.inflight.is_empty());
        if !busy {
            return;
        }
        notified.await;
    }
}

/// Removes the closed client [handle].
pub fn remove_client(handle: u64) {
    state().clients.remove(&handle);
}

/// Registers [cursor] of [client] and returns its handle; fails when the client was closed meanwhile.
pub fn insert_cursor(client: u64, cursor: Cursor<RawDocumentBuf>, operation: &str) -> Result<u64, NativeError> {
    let mut s = state();
    if !s.clients.get(&client).is_some_and(|c| !c.closed) {
        return Err(client_closed(operation));
    }
    let handle = next_handle();
    s.cursors.insert(
        handle,
        CursorEntry {
            client,
            cursor: Arc::new(tokio::sync::Mutex::new(cursor)),
        },
    );
    Ok(handle)
}

/// The cursor behind [handle].
pub fn cursor(handle: u64, operation: &str) -> Result<SharedCursor, NativeError> {
    state()
        .cursors
        .get(&handle)
        .map(|c| c.cursor.clone())
        .ok_or_else(|| NativeError::closed(operation, "the cursor is closed or exhausted"))
}

/// Removes the cursor [handle] and returns it, so that it is dropped (and killed) by the caller.
pub fn remove_cursor(handle: u64) -> Option<SharedCursor> {
    state().cursors.remove(&handle).map(|c| c.cursor)
}

/// Registers the change stream [stream] of [client] and returns its handle.
pub fn insert_stream(client: u64, stream: StreamState, operation: &str) -> Result<u64, NativeError> {
    let mut s = state();
    if !s.clients.get(&client).is_some_and(|c| !c.closed) {
        return Err(client_closed(operation));
    }
    let handle = next_handle();
    s.streams.insert(
        handle,
        StreamEntry {
            client,
            state: Arc::new(stream),
        },
    );
    Ok(handle)
}

/// The change stream behind [handle].
pub fn stream(handle: u64, operation: &str) -> Result<Arc<StreamState>, NativeError> {
    state()
        .streams
        .get(&handle)
        .map(|s| s.state.clone())
        .ok_or_else(|| NativeError::closed(operation, "the change stream is closed"))
}

/// Removes the change stream [handle] and returns it, so that it is dropped by the caller.
pub fn remove_stream(handle: u64) -> Option<Arc<StreamState>> {
    state().streams.remove(&handle).map(|s| s.state)
}
