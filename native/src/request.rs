//! Typed reading of the request documents built by `lib/src/collection.dart` and friends.

use std::time::Duration;

use bson::{Bson, Document, RawDocumentBuf, Timestamp};
use mongodb::options::{Acknowledgment, ReadConcern, UpdateModifications, WriteConcern};

use crate::error::NativeError;

/// A decoded request.
pub struct Request {
    /// The operation name (`insertOne`, `cursor.next`, …).
    pub op: String,
    doc: Document,
}

impl Request {
    /// Validates [bytes] as one BSON document and reads its `op`.
    pub fn parse(bytes: &[u8]) -> Result<Request, NativeError> {
        let raw = RawDocumentBuf::from_bytes(bytes.to_vec())
            .map_err(|e| NativeError::bson("decode request", format!("the request is not a BSON document: {e}")))?;
        let doc = raw
            .to_document()
            .map_err(|e| NativeError::bson("decode request", format!("the request is not valid BSON: {e}")))?;
        let op = match doc.get("op") {
            Some(Bson::String(op)) => op.clone(),
            _ => {
                return Err(NativeError::bson(
                    "decode request",
                    "the request has no string field `op`",
                ))
            }
        };
        Ok(Request { op, doc })
    }

    /// `op on db.coll`, `op on db` or `op`, for messages.
    pub fn target(&self) -> String {
        match (self.opt_str("db"), self.opt_str("coll")) {
            (Some(db), Some(coll)) => format!("{} on {db}.{coll}", self.op),
            (Some(db), None) => format!("{} on {db}", self.op),
            _ => self.op.clone(),
        }
    }

    fn missing(&self, key: &str, expected: &str) -> NativeError {
        NativeError::bson(
            &self.target(),
            format!("the request field `{key}` is missing or is not {expected}"),
        )
    }

    /// The raw value of [key].
    pub fn get(&self, key: &str) -> Option<&Bson> {
        match self.doc.get(key) {
            None | Some(Bson::Null) => None,
            Some(v) => Some(v),
        }
    }

    /// The string [key].
    pub fn str(&self, key: &str) -> Result<&str, NativeError> {
        self.opt_str(key).ok_or_else(|| self.missing(key, "a string"))
    }

    /// The string [key], when present.
    pub fn opt_str(&self, key: &str) -> Option<&str> {
        match self.get(key) {
            Some(Bson::String(s)) => Some(s.as_str()),
            _ => None,
        }
    }

    /// The integer [key], when present (int32 or int64).
    pub fn opt_int(&self, key: &str) -> Option<i64> {
        match self.get(key) {
            Some(Bson::Int32(v)) => Some(*v as i64),
            Some(Bson::Int64(v)) => Some(*v),
            _ => None,
        }
    }

    /// The integer [key].
    pub fn int(&self, key: &str) -> Result<i64, NativeError> {
        self.opt_int(key).ok_or_else(|| self.missing(key, "an integer"))
    }

    /// The handle [key].
    pub fn handle(&self, key: &str) -> Result<u64, NativeError> {
        let v = self.int(key)?;
        u64::try_from(v).map_err(|_| self.missing(key, "a positive handle"))
    }

    /// The boolean [key], or [default].
    pub fn bool_or(&self, key: &str, default: bool) -> bool {
        match self.get(key) {
            Some(Bson::Boolean(b)) => *b,
            _ => default,
        }
    }

    /// The boolean [key], when present.
    pub fn opt_bool(&self, key: &str) -> Option<bool> {
        match self.get(key) {
            Some(Bson::Boolean(b)) => Some(*b),
            _ => None,
        }
    }

    /// The duration in milliseconds [key], when present.
    pub fn millis(&self, key: &str) -> Option<Duration> {
        self.opt_int(key).map(|ms| Duration::from_millis(ms.max(0) as u64))
    }

    /// The document [key].
    pub fn doc(&self, key: &str) -> Result<Document, NativeError> {
        self.opt_doc(key).ok_or_else(|| self.missing(key, "a document"))
    }

    /// The document [key], when present.
    pub fn opt_doc(&self, key: &str) -> Option<Document> {
        match self.get(key) {
            Some(Bson::Document(d)) => Some(d.clone()),
            _ => None,
        }
    }

    /// The timestamp [key], when present.
    pub fn opt_timestamp(&self, key: &str) -> Option<Timestamp> {
        match self.get(key) {
            Some(Bson::Timestamp(t)) => Some(*t),
            _ => None,
        }
    }

    /// The list of documents [key].
    pub fn docs(&self, key: &str) -> Result<Vec<Document>, NativeError> {
        match self.get(key) {
            Some(Bson::Array(items)) => items
                .iter()
                .map(|item| match item {
                    Bson::Document(d) => Ok(d.clone()),
                    _ => Err(self.missing(key, "a list of documents")),
                })
                .collect(),
            _ => Err(self.missing(key, "a list of documents")),
        }
    }

    /// The update [key]: a document of operators or a pipeline.
    pub fn update(&self, key: &str) -> Result<UpdateModifications, NativeError> {
        match self.get(key) {
            Some(Bson::Document(d)) => Ok(UpdateModifications::Document(d.clone())),
            Some(Bson::Array(_)) => Ok(UpdateModifications::Pipeline(self.docs(key)?)),
            _ => Err(self.missing(key, "an update document or pipeline")),
        }
    }

    /// The write concern in `writeConcern`, when present.
    pub fn write_concern(&self) -> Option<WriteConcern> {
        let d = self.opt_doc("writeConcern")?;
        let mut wc = WriteConcern::default();
        wc.w = match d.get("w") {
            Some(Bson::String(s)) if s == "majority" => Some(Acknowledgment::Majority),
            Some(Bson::String(s)) => Some(Acknowledgment::Custom(s.clone())),
            Some(Bson::Int32(n)) => Some(Acknowledgment::Nodes(*n as u32)),
            Some(Bson::Int64(n)) => Some(Acknowledgment::Nodes(*n as u32)),
            _ => None,
        };
        wc.journal = match d.get("j") {
            Some(Bson::Boolean(b)) => Some(*b),
            _ => None,
        };
        wc.w_timeout = match d.get("wtimeoutMs") {
            Some(Bson::Int32(n)) => Some(Duration::from_millis(*n as u64)),
            Some(Bson::Int64(n)) => Some(Duration::from_millis(*n as u64)),
            _ => None,
        };
        Some(wc)
    }

    /// The read concern in `readConcern`, when present.
    pub fn read_concern(&self) -> Option<ReadConcern> {
        match self.opt_str("readConcern")? {
            "local" => Some(ReadConcern::local()),
            "majority" => Some(ReadConcern::majority()),
            "linearizable" => Some(ReadConcern::linearizable()),
            _ => None,
        }
    }
}
