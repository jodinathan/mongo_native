//! C interface of `mongo_native`: one entry point that takes a BSON request and posts the reply to a
//! Dart port, over the official `mongodb` driver.

mod cursor;
mod dart;
mod error;
mod ops;
mod registry;
mod reply;
mod request;
mod runtime;
mod stream;

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};

/// The version of this C interface; the Dart side refuses a library that speaks another one.
pub const ABI_VERSION: u32 = 2;

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

/// Returns [ABI_VERSION].
#[no_mangle]
pub extern "C" fn mn_abi_version() -> u32 {
    ABI_VERSION
}

/// Reads the Dart VM functions from `NativeApi.initializeApiDLData` at [data]; returns 0, or -1 when
/// the VM speaks another major version of the dynamically linked API, -2 when it lacks a function
/// this library needs, -3 when [data] is null. Calling it again after a success does nothing.
///
/// # Safety
/// [data] must be null or the `NativeApi.initializeApiDLData` of the running VM.
#[no_mangle]
pub unsafe extern "C" fn mn_init(data: *mut c_void) -> isize {
    catch_unwind(|| unsafe { dart::initialize(data) }).unwrap_or(dart::INIT_MISSING)
}

/// The id of the native port that isolates name in `Isolate.addOnExitListener`, with the port of
/// their replies as the response: when an isolate ends, the clients it opened are closed. Returns 0
/// when the port could not be created or [mn_init] did not succeed.
#[no_mangle]
pub extern "C" fn mn_exit_port() -> i64 {
    catch_unwind(|| dart::exit_port(on_isolate_exit)).unwrap_or(0)
}

/// A new `SendPort` to the port [port], as a Dart handle.
///
/// # Safety
/// Must be called from Dart through FFI with a `Handle` return type, after [mn_init] succeeded.
#[no_mangle]
pub unsafe extern "C" fn mn_send_port(port: i64) -> dart::Handle {
    unsafe { dart::send_port(port) }
}

extern "C" fn on_isolate_exit(_destination: dart::Port, message: *mut dart::CObject) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(port) = unsafe { dart::integer(message) } {
            ops::retire(port);
        }
    }));
}

/// Copies the BSON request at `ptr`/`len`, runs it in the background and returns its id, unique in
/// the process; the reply is posted to the Dart port `port` as `[id, status, bytes]`. Returns 0 when
/// the request could not be queued.
///
/// # Safety
/// `ptr` must point to `len` readable bytes for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn mn_submit(port: i64, ptr: *const u8, len: usize) -> u64 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let request = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let bytes = if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
        };
        runtime::spawn(request, port, async move { ops::dispatch(request, port, bytes).await });
        request
    }));
    outcome.unwrap_or(0)
}

/// Aborts the request `request`; returns 1 when it was still running, 0 otherwise.
#[no_mangle]
pub extern "C" fn mn_cancel(request: u64) -> i32 {
    catch_unwind(|| registry::cancel(request) as i32).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::spec::BinarySubtype;
    use bson::{doc, oid::ObjectId, Binary, Bson, DateTime, Decimal128, Document, Regex, Timestamp};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Condvar, LazyLock, Mutex};
    use std::time::{Duration, Instant};

    type Replies = (Mutex<HashMap<u64, (i32, Vec<u8>)>>, Condvar);

    const PORT: dart::Port = 7;
    const CLOSED_PORT: dart::Port = 8;

    static REPLIES: LazyLock<Replies> = LazyLock::new(|| (Mutex::new(HashMap::new()), Condvar::new()));
    static INSTALLED: LazyLock<isize> = LazyLock::new(|| dart::testing::install(record));

    unsafe extern "C" fn record(port: dart::Port, message: *mut dart::CObject) -> bool {
        if port != PORT {
            return false;
        }
        let (request, status, bytes) = unsafe { dart::testing::read_reply(message) };
        let (map, signal) = &*REPLIES;
        map.lock().unwrap().insert(request, (status, bytes));
        signal.notify_all();
        true
    }

    fn installed() {
        assert_eq!(*INSTALLED, dart::INIT_OK);
    }

    fn wait(request: u64) -> (i32, Document) {
        let (map, signal) = &*REPLIES;
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut guard = map.lock().unwrap();
        loop {
            if let Some((status, bytes)) = guard.remove(&request) {
                let doc = bson::RawDocumentBuf::from_bytes(bytes).unwrap().to_document().unwrap();
                return (status, doc);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "no reply for request {request}");
            guard = signal.wait_timeout(guard, left).unwrap().0;
        }
    }

    fn submit(bytes: &[u8]) -> u64 {
        submit_to(PORT, bytes)
    }

    fn submit_to(port: dart::Port, bytes: &[u8]) -> u64 {
        installed();
        unsafe { mn_submit(port, bytes.as_ptr(), bytes.len()) }
    }

    fn encode(doc: &Document) -> Vec<u8> {
        let mut out = Vec::new();
        doc.to_writer(&mut out).unwrap();
        out
    }

    #[test]
    fn abi_version_is_two() {
        assert_eq!(mn_abi_version(), 2);
    }

    #[test]
    fn init_after_a_success_does_nothing() {
        installed();
        assert_eq!(unsafe { mn_init(std::ptr::null_mut()) }, dart::INIT_OK);
    }

    #[test]
    fn reply_to_a_closed_port_is_dropped_and_the_request_forgotten() {
        let id = submit_to(CLOSED_PORT, &encode(&doc! { "noop": 1 }));
        assert_ne!(id, 0);
        let deadline = Instant::now() + Duration::from_secs(20);
        while registry::tracked(id) {
            assert!(Instant::now() < deadline, "request {id} still tracked");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!REPLIES.0.lock().unwrap().contains_key(&id));
    }

    #[test]
    fn malformed_request_answers_kind_bson() {
        let id = submit(&[7, 0, 0, 0, 1, 2]);
        assert_ne!(id, 0);
        let (status, reply) = wait(id);
        assert_eq!(status, reply::STATUS_ERROR);
        assert_eq!(reply.get_str("kind").unwrap(), "bson");

        let id = submit(&encode(&doc! { "noop": 1 }));
        let (status, reply) = wait(id);
        assert_eq!(status, reply::STATUS_ERROR);
        assert_eq!(reply.get_str("kind").unwrap(), "bson");
        assert!(reply.get_str("message").unwrap().contains("`op`"));
    }

    #[test]
    fn panic_in_a_task_answers_kind_internal() {
        let id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        installed();
        runtime::spawn(id, PORT, async { panic!("forced panic for the test") });
        let (status, reply) = wait(id);
        assert_eq!(status, reply::STATUS_ERROR);
        assert_eq!(reply.get_str("kind").unwrap(), "internal");
        assert!(reply.get_str("message").unwrap().contains("forced panic for the test"));
    }

    #[test]
    fn request_ids_never_repeat_across_threads() {
        let bytes = encode(&doc! { "noop": 1 });
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let bytes = bytes.clone();
                std::thread::spawn(move || (0..500).map(|_| submit(&bytes)).collect::<Vec<u64>>())
            })
            .collect();
        let ids: Vec<u64> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
        let unique: HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len());
        assert!(!unique.contains(&0));
        for id in ids {
            assert_eq!(wait(id).0, reply::STATUS_ERROR);
        }
    }

    #[test]
    fn unknown_client_answers_kind_closed() {
        let id = submit(&encode(&doc! {
            "op": "insertOne", "client": 987_654_321i64, "db": "d", "coll": "c", "document": { "a": 1 },
        }));
        let (status, reply) = wait(id);
        assert_eq!(status, reply::STATUS_ERROR);
        assert_eq!(reply.get_str("kind").unwrap(), "closed");
        assert_eq!(reply.get_str("operation").unwrap(), "insertOne on d.c");
    }

    #[test]
    fn cancel_of_unknown_request_does_nothing() {
        assert_eq!(mn_cancel(u64::MAX), 0);
    }

    #[test]
    fn duplicate_key_values_are_cut() {
        let text = "E11000 duplicate key error collection: app.users index: email_1 dup key: { email: \"a@b.c\" }";
        assert_eq!(
            error::strip_dup_key(text),
            "E11000 duplicate key error collection: app.users index: email_1"
        );
    }

    fn fixtures() -> Vec<(&'static str, Document)> {
        let oid = ObjectId::parse_str("65f1a2b3c4d5e6f708192a3b").unwrap();
        let mut dec = [0u8; 16];
        dec.iter_mut().enumerate().for_each(|(i, b)| *b = i as u8 + 1);
        vec![
            (
                "all_types",
                doc! {
                    "double": 1.5f64,
                    "string": "olá, 世界",
                    "doc": { "inner": "x", "n": 1i32 },
                    "array": [1i32, "two", { "three": 3i32 }],
                    "bin0": Binary { subtype: BinarySubtype::Generic, bytes: vec![0, 1, 2, 255] },
                    "bin4": Binary { subtype: BinarySubtype::Uuid, bytes: (0u8..16).collect() },
                    "bin80": Binary { subtype: BinarySubtype::UserDefined(0x80), bytes: vec![9, 8, 7] },
                    "oid": oid,
                    "yes": true,
                    "no": false,
                    "date": DateTime::from_millis(1_731_715_200_123),
                    "nothing": Bson::Null,
                    "regex": Regex { pattern: "^a.c$".to_string(), options: "im".to_string() },
                    "int32": -2_147_483_648i32,
                    "int64": 9_007_199_254_740_993i64,
                    "ts": Timestamp { time: 1_731_715_200, increment: 7 },
                    "dec": Decimal128::from_bytes(dec),
                    "min": Bson::MinKey,
                    "max": Bson::MaxKey,
                },
            ),
            ("ref_id", doc! { "$ref": "users", "$id": 42i32, "$db": "app" }),
            ("empty", doc! {}),
        ]
    }

    #[test]
    #[ignore]
    fn write_fixtures() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../test/fixtures");
        std::fs::create_dir_all(&dir).unwrap();
        for (name, doc) in fixtures() {
            std::fs::write(dir.join(format!("{name}.bson")), encode(&doc)).unwrap();
        }
    }
}
