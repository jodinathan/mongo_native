//! Typed failures sent back to Dart as an error reply.

use bson::{doc, Bson, Document};
use mongodb::error::{
    BulkWriteError, Error, ErrorKind, IndexedWriteError, InsertManyError, PartialBulkWriteResult, WriteConcernError,
    WriteError, WriteFailure,
};

/// A failure described in the shape the Dart side turns into a `MongoNativeException`.
#[derive(Debug, Clone)]
pub struct NativeError(Box<Detail>);

/// The fields of a [NativeError].
#[derive(Debug, Clone)]
pub struct Detail {
    /// One of `write`, `bulkWrite`, `timeout`, `cancelled`, `command`, `writeConcern`, `network`,
    /// `serverSelection`, `authentication`, `bson`, `closed`, `internal`.
    pub kind: &'static str,
    /// The operation and its target, e.g. `updateOne on app.users`.
    pub operation: String,
    /// The text the source gave, without document values.
    pub message: String,
    /// The server error code, when there is one.
    pub code: Option<i32>,
    /// The server error code name, when there is one.
    pub code_name: Option<String>,
    /// The error labels the driver attached (`RetryableWriteError`, `TransientTransactionError`, …).
    pub labels: Vec<String>,
    /// Extra fields of the kind (`index`, `outcomeUnknown`, `writeErrors`, …).
    pub extra: Document,
}

impl std::ops::Deref for NativeError {
    type Target = Detail;

    fn deref(&self) -> &Detail {
        &self.0
    }
}

impl std::ops::DerefMut for NativeError {
    fn deref_mut(&mut self) -> &mut Detail {
        &mut self.0
    }
}

impl NativeError {
    /// Creates a failure of [kind] for [operation] with the source's [message].
    pub fn new(kind: &'static str, operation: &str, message: impl Into<String>) -> Self {
        NativeError(Box::new(Detail {
            kind,
            operation: operation.to_string(),
            message: strip_dup_key(&message.into()),
            code: None,
            code_name: None,
            labels: Vec::new(),
            extra: Document::new(),
        }))
    }

    /// Sets the server [code] and its [name]; an empty name is dropped.
    pub fn code(mut self, code: i32, name: Option<String>) -> Self {
        self.code = Some(code);
        self.code_name = name.filter(|n| !n.is_empty());
        self
    }

    /// Sets the error [labels].
    pub fn labels(mut self, labels: Vec<String>) -> Self {
        self.labels = labels;
        self
    }

    /// Adds the extra field [key] with [value].
    pub fn with(mut self, key: &str, value: impl Into<Bson>) -> Self {
        self.extra.insert(key, value.into());
        self
    }

    /// The error reply document.
    pub fn to_document(&self) -> Document {
        let mut out = doc! {
            "kind": self.kind,
            "operation": &self.operation,
            "message": &self.message,
            "labels": self.labels.iter().map(|l| Bson::String(l.clone())).collect::<Vec<_>>(),
        };
        if let Some(code) = self.code {
            out.insert("code", code);
        }
        if let Some(name) = &self.code_name {
            out.insert("codeName", name.clone());
        }
        for (k, v) in &self.extra {
            out.insert(k.clone(), v.clone());
        }
        out
    }

    /// A malformed request or reply.
    pub fn bson(operation: &str, message: impl Into<String>) -> Self {
        NativeError::new("bson", operation, message)
    }

    /// A call on a client, cursor or stream that is closed or unknown.
    pub fn closed(operation: &str, message: impl Into<String>) -> Self {
        NativeError::new("closed", operation, message)
    }

    /// A bug or an unexpected state of the native side.
    pub fn internal(operation: &str, message: impl Into<String>) -> Self {
        NativeError::new("internal", operation, message)
    }
}

/// Cuts the ` dup key: { … }` part of an `E11000` message, which echoes document values; the index
/// name stays.
pub fn strip_dup_key(message: &str) -> String {
    match message.find(" dup key:") {
        Some(at) => message[..at].to_string(),
        None => message.to_string(),
    }
}

/// Maps a driver error for [op] into a [NativeError].
pub fn from_driver(op: &str, e: Error) -> NativeError {
    let labels: Vec<String> = e.labels().iter().cloned().collect();
    let source = std::error::Error::source(&e).map(|s| s.to_string());
    let mapped = match *e.kind {
        ErrorKind::Command(c) if c.code == 11000 => NativeError::new("write", op, c.message)
            .code(c.code, Some(c.code_name))
            .with("index", 0i32),
        ErrorKind::Command(c) => NativeError::new("command", op, c.message).code(c.code, Some(c.code_name)),
        ErrorKind::Write(WriteFailure::WriteError(w)) => NativeError::new("write", op, w.message)
            .code(w.code, w.code_name)
            .with("index", 0i32),
        ErrorKind::Write(WriteFailure::WriteConcernError(w)) => {
            NativeError::new("writeConcern", op, w.message).code(w.code, Some(w.code_name))
        }
        ErrorKind::BulkWrite(b) => bulk(op, b, source),
        ErrorKind::InsertMany(m) => insert_many(op, m),
        ErrorKind::Io(io) => NativeError::new("network", op, io.to_string()),
        ErrorKind::ConnectionPoolCleared { message, .. } => NativeError::new("network", op, message),
        ErrorKind::DnsResolve { message, .. } => {
            NativeError::new("network", op, format!("DNS resolution failed: {message}"))
        }
        ErrorKind::ServerSelection { message, .. } => NativeError::new("serverSelection", op, message),
        ErrorKind::Authentication { message, .. } => NativeError::new("authentication", op, message),
        ErrorKind::BsonDeserialization(d) => NativeError::new("bson", op, d.to_string()),
        ErrorKind::BsonSerialization(s) => NativeError::new("bson", op, s.to_string()),
        ErrorKind::Shutdown => NativeError::closed(op, "the client was closed"),
        other => NativeError::internal(op, other.to_string()),
    };
    mapped.labels(labels)
}

fn write_error_doc(index: usize, e: &WriteError) -> Document {
    let mut d = doc! { "index": index as i64, "code": e.code, "message": strip_dup_key(&e.message) };
    if let Some(name) = e.code_name.as_ref().filter(|n| !n.is_empty()) {
        d.insert("codeName", name.clone());
    }
    d
}

fn indexed_write_error_doc(e: &IndexedWriteError) -> Document {
    let mut d = doc! { "index": e.index as i64, "code": e.code, "message": strip_dup_key(&e.message) };
    if let Some(name) = e.code_name.as_ref().filter(|n| !n.is_empty()) {
        d.insert("codeName", name.clone());
    }
    d
}

fn write_concern_error_doc(e: &WriteConcernError) -> Document {
    let mut d = doc! { "code": e.code, "message": strip_dup_key(&e.message) };
    if !e.code_name.is_empty() {
        d.insert("codeName", e.code_name.clone());
    }
    d
}

/// The summary document of a partial `bulkWrite` result.
pub fn partial_doc(partial: &PartialBulkWriteResult) -> Document {
    match partial {
        PartialBulkWriteResult::Summary(s) => doc! {
            "inserted": s.inserted_count, "matched": s.matched_count, "modified": s.modified_count,
            "deleted": s.deleted_count, "upserted": s.upserted_count, "upsertedIds": Vec::<Bson>::new(),
        },
        PartialBulkWriteResult::Verbose(v) => {
            let mut ids: Vec<(usize, Bson)> = v
                .update_results
                .iter()
                .filter_map(|(i, r)| r.upserted_id.clone().map(|id| (*i, id)))
                .collect();
            ids.sort_by_key(|(i, _)| *i);
            doc! {
                "inserted": v.summary.inserted_count, "matched": v.summary.matched_count,
                "modified": v.summary.modified_count, "deleted": v.summary.deleted_count,
                "upserted": v.summary.upserted_count,
                "upsertedIds": ids.into_iter().map(|(i, id)| Bson::Document(doc! { "index": i as i64, "id": id })).collect::<Vec<_>>(),
            }
        }
    }
}

fn bulk(op: &str, b: BulkWriteError, source: Option<String>) -> NativeError {
    let mut writes: Vec<(usize, WriteError)> = b.write_errors.into_iter().collect();
    writes.sort_by_key(|(i, _)| *i);
    let (code, name, message) = if let Some((index, first)) = writes.first() {
        (
            Some(first.code),
            first.code_name.clone(),
            format!(
                "{} write error(s); the first, at index {index}: {}",
                writes.len(),
                first.message
            ),
        )
    } else if let Some(first) = b.write_concern_errors.first() {
        (
            Some(first.code),
            Some(first.code_name.clone()),
            format!(
                "{} write concern error(s); the first: {}",
                b.write_concern_errors.len(),
                first.message
            ),
        )
    } else {
        (
            None,
            None,
            source.unwrap_or_else(|| "the bulk write failed without write errors".to_string()),
        )
    };
    let mut e = NativeError::new("bulkWrite", op, message)
        .with(
            "writeErrors",
            writes
                .iter()
                .map(|(i, w)| Bson::Document(write_error_doc(*i, w)))
                .collect::<Vec<_>>(),
        )
        .with(
            "writeConcernErrors",
            b.write_concern_errors
                .iter()
                .map(|w| Bson::Document(write_concern_error_doc(w)))
                .collect::<Vec<_>>(),
        );
    if let Some(partial) = &b.partial_result {
        e = e.with("partial", partial_doc(partial));
    }
    match code {
        Some(c) => e.code(c, name),
        None => e,
    }
}

fn insert_many(op: &str, m: InsertManyError) -> NativeError {
    let writes = m.write_errors.unwrap_or_default();
    let concern: Vec<WriteConcernError> = m.write_concern_error.into_iter().collect();
    if writes.is_empty() {
        if let Some(w) = concern.first() {
            return NativeError::new("writeConcern", op, w.message.clone()).code(w.code, Some(w.code_name.clone()));
        }
    }
    let (code, name, message) = match writes.iter().min_by_key(|w| w.index) {
        Some(first) => (
            Some(first.code),
            first.code_name.clone(),
            format!(
                "{} write error(s); the first, at index {}: {}",
                writes.len(),
                first.index,
                first.message
            ),
        ),
        None => (None, None, "insertMany failed without write errors".to_string()),
    };
    let e = NativeError::new("bulkWrite", op, message)
        .with(
            "writeErrors",
            writes
                .iter()
                .map(|w| Bson::Document(indexed_write_error_doc(w)))
                .collect::<Vec<_>>(),
        )
        .with(
            "writeConcernErrors",
            concern
                .iter()
                .map(|w| Bson::Document(write_concern_error_doc(w)))
                .collect::<Vec<_>>(),
        );
    match code {
        Some(c) => e.code(c, name),
        None => e,
    }
}
