//! Reply documents posted back to the Dart port of each request.

use bson::{Bson, Document, RawBson, RawDocumentBuf};

use crate::error::NativeError;

/// Status of a reply that carries the operation result.
pub const STATUS_OK: i32 = 0;
/// Status of a reply that carries a [NativeError].
pub const STATUS_ERROR: i32 = 1;

/// Encodes [doc] as the raw reply of [operation].
pub fn document(operation: &str, doc: &Document) -> Result<RawDocumentBuf, NativeError> {
    RawDocumentBuf::from_document(doc)
        .map_err(|e| NativeError::bson(operation, format!("could not encode the reply: {e}")))
}

/// Converts [value] into a raw value for a reply of [operation].
pub fn raw(operation: &str, value: Bson) -> Result<RawBson, NativeError> {
    RawBson::try_from(value).map_err(|e| NativeError::bson(operation, format!("could not encode the reply: {e}")))
}

/// The bytes of the error reply for [error].
pub fn error_bytes(error: &NativeError) -> Vec<u8> {
    match RawDocumentBuf::from_document(&error.to_document()) {
        Ok(raw) => raw.into_bytes(),
        Err(e) => {
            let fallback = NativeError::internal(&error.operation, format!("could not encode an error reply: {e}"));
            RawDocumentBuf::from_document(&fallback.to_document())
                .map(RawDocumentBuf::into_bytes)
                .unwrap_or_else(|_| RawDocumentBuf::new().into_bytes())
        }
    }
}
