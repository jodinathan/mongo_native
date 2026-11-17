//! The process-wide tokio runtime and the delivery of completions.

use std::future::Future;
use std::sync::OnceLock;

use bson::RawDocumentBuf;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tokio::task::JoinError;

use crate::error::NativeError;
use crate::registry;
use crate::reply::{error_bytes, STATUS_ERROR, STATUS_OK};

/// Called once per request with its id, the status and the reply bytes, which the receiver releases
/// with `mn_free`.
pub type Completion = extern "C" fn(request: u64, status: i32, data: *mut u8, len: usize);

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// The runtime shared by every isolate of the process, started on first use with up to four workers.
pub fn handle() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get().min(4))
            .unwrap_or(2);
        Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("mongo-native")
            .enable_all()
            .build()
            .expect("mongo_native could not start its tokio runtime")
    })
}

/// Runs [work] for [request] and reports its outcome through [completion]: the result, its error,
/// the cancellation or the panic.
pub fn spawn<F>(request: u64, completion: Completion, work: F)
where
    F: Future<Output = Result<RawDocumentBuf, NativeError>> + Send + 'static,
{
    let rt = handle();
    let (tracked, gate) = oneshot::channel::<()>();
    let task = rt.spawn(async move {
        let _ = gate.await;
        work.await
    });
    registry::track(request, task.abort_handle());
    let _ = tracked.send(());
    rt.spawn(async move {
        let (status, bytes) = match task.await {
            Ok(Ok(doc)) => (STATUS_OK, doc.into_bytes()),
            Ok(Err(e)) => (STATUS_ERROR, error_bytes(&e)),
            Err(join) if join.is_cancelled() => (STATUS_ERROR, error_bytes(&registry::interrupted(request))),
            Err(join) => {
                let op = registry::operation(request);
                (STATUS_ERROR, error_bytes(&NativeError::internal(&op, panic_text(join))))
            }
        };
        deliver(request, completion, status, bytes);
        registry::forget(request);
    });
}

fn panic_text(join: JoinError) -> String {
    let payload = join.into_panic();
    let text = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_string());
    format!("the native side panicked: {text}; please report it with the operation that triggered it")
}

fn deliver(request: u64, completion: Completion, status: i32, bytes: Vec<u8>) {
    let mut boxed = bytes.into_boxed_slice();
    let len = boxed.len();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    completion(request, status, ptr, len);
}
