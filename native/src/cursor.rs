//! Cursors of `find`, `aggregate` and `cursor.next`.

use bson::{doc, RawArrayBuf, RawDocumentBuf};
use mongodb::Cursor;

use crate::error::{from_driver, NativeError};
use crate::registry;
use crate::reply;
use crate::request::Request;

/// Reads up to [max] documents from [cursor]; returns them and whether the cursor is exhausted.
pub async fn read_batch(
    cursor: &mut Cursor<RawDocumentBuf>,
    max: usize,
    operation: &str,
) -> Result<(RawArrayBuf, bool), NativeError> {
    let mut batch = RawArrayBuf::new();
    let mut count = 0usize;
    while count < max {
        if !cursor.advance().await.map_err(|e| from_driver(operation, e))? {
            break;
        }
        batch.push(cursor.current().to_owned());
        count += 1;
    }
    Ok((batch, !cursor.has_next()))
}

/// Reads the first batch of a new [cursor] and registers it when more remains; the reply has
/// `cursor` (0 when exhausted) and `batch`.
pub async fn open(
    client: u64,
    mut cursor: Cursor<RawDocumentBuf>,
    batch_size: usize,
    operation: &str,
) -> Result<RawDocumentBuf, NativeError> {
    let (batch, exhausted) = read_batch(&mut cursor, batch_size, operation).await?;
    let handle = if exhausted {
        0
    } else {
        registry::insert_cursor(client, cursor, operation)?
    };
    let mut out = reply::document(operation, &doc! { "cursor": handle as i64 })?;
    out.append("batch", batch);
    Ok(out)
}

/// `cursor.next`: the next batch of the cursor and whether it is exhausted.
pub async fn next(req: &Request, operation: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("cursor")?;
    let batch_size = req.opt_int("batchSize").unwrap_or(1000).max(1) as usize;
    let shared = registry::cursor(handle, operation)?;
    let mut cursor = shared.lock().await;
    let (batch, exhausted) = read_batch(&mut cursor, batch_size, operation).await?;
    drop(cursor);
    if exhausted {
        registry::remove_cursor(handle);
    }
    let mut out = reply::document(operation, &doc! { "exhausted": exhausted })?;
    out.append("batch", batch);
    Ok(out)
}

/// `cursor.close`: removes the cursor, which is killed on the server when dropped.
pub async fn close(req: &Request, operation: &str) -> Result<RawDocumentBuf, NativeError> {
    let handle = req.handle("cursor")?;
    drop(registry::remove_cursor(handle));
    reply::document(operation, &doc! {})
}
