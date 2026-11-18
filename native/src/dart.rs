//! The Dart VM functions this library calls, found through the dynamically linked Dart API
//! (`NativeApi.initializeApiDLData`), the replies posted to Dart ports and the native port that
//! learns when an isolate ends.

use std::ffi::{c_char, c_void, CStr};
use std::sync::{Mutex, OnceLock};

/// A Dart port id (`Dart_Port`).
pub type Port = i64;

/// A `Dart_Handle`.
pub type Handle = *mut c_void;

/// Receives the messages of a native port (`Dart_NativeMessageHandler`).
pub type NativeHandler = extern "C" fn(destination: Port, message: *mut CObject);

type PostCObject = unsafe extern "C" fn(Port, *mut CObject) -> bool;
type NewNativePort = unsafe extern "C" fn(*const c_char, NativeHandler, bool) -> Port;
type NewSendPort = unsafe extern "C" fn(Port) -> Handle;

/// The major version of the dynamically linked API this library speaks.
const DL_MAJOR_VERSION: i32 = 2;

/// `mn_init` succeeded.
pub const INIT_OK: isize = 0;
/// The VM speaks another major version of the dynamically linked API.
pub const INIT_VERSION: isize = -1;
/// The VM does not export a function this library needs.
pub const INIT_MISSING: isize = -2;
/// The data pointer was null.
pub const INIT_NULL: isize = -3;

const K_INT32: i32 = 2;
const K_INT64: i32 = 3;
const K_ARRAY: i32 = 6;
const K_TYPED_DATA: i32 = 7;
const TYPED_DATA_UINT8: i32 = 2;

#[repr(C)]
struct DartApi {
    major: i32,
    minor: i32,
    functions: *const DartApiEntry,
}

#[repr(C)]
struct DartApiEntry {
    name: *const c_char,
    function: *const c_void,
}

/// A `Dart_CObject`, with only the members this library writes or reads.
#[repr(C)]
pub struct CObject {
    kind: i32,
    value: CValue,
}

#[repr(C)]
#[derive(Clone, Copy)]
union CValue {
    as_int32: i32,
    as_int64: i64,
    as_array: CArray,
    as_typed_data: CTypedData,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CArray {
    length: isize,
    values: *mut *mut CObject,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CTypedData {
    kind: i32,
    length: isize,
    values: *const u8,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::offset_of!(CObject, value) == 8);

struct Api {
    post: PostCObject,
    new_native_port: NewNativePort,
    new_send_port: NewSendPort,
}

static API: OnceLock<Api> = OnceLock::new();
static EXIT_PORT: Mutex<Port> = Mutex::new(0);

/// Reads the function table at [data] (`NativeApi.initializeApiDLData`); returns [INIT_OK] or the
/// reason it could not.
///
/// # Safety
/// [data] must be null or point to the dynamically linked API data of the running VM.
pub unsafe fn initialize(data: *const c_void) -> isize {
    if API.get().is_some() {
        return INIT_OK;
    }
    if data.is_null() {
        return INIT_NULL;
    }
    let api = unsafe { &*(data as *const DartApi) };
    if api.major != DL_MAJOR_VERSION {
        return INIT_VERSION;
    }
    let find = |wanted: &str| -> *const c_void {
        let mut entry = api.functions;
        loop {
            let e = unsafe { &*entry };
            if e.name.is_null() {
                return std::ptr::null();
            }
            if unsafe { CStr::from_ptr(e.name) }.to_bytes() == wanted.as_bytes() {
                return e.function;
            }
            entry = unsafe { entry.add(1) };
        }
    };
    let post = find("Dart_PostCObject");
    let new_native_port = find("Dart_NewNativePort");
    let new_send_port = find("Dart_NewSendPort");
    if post.is_null() || new_native_port.is_null() || new_send_port.is_null() {
        return INIT_MISSING;
    }
    let _ = API.set(unsafe {
        Api {
            post: std::mem::transmute::<*const c_void, PostCObject>(post),
            new_native_port: std::mem::transmute::<*const c_void, NewNativePort>(new_native_port),
            new_send_port: std::mem::transmute::<*const c_void, NewSendPort>(new_send_port),
        }
    });
    INIT_OK
}

/// Posts the reply `[request, status, bytes]` to [port]; returns whether the VM queued it. The VM
/// copies [bytes], so they stay with the caller either way. A closed port, or a VM that was never
/// initialized, answers false.
pub fn post_reply(port: Port, request: u64, status: i32, bytes: &[u8]) -> bool {
    let Some(api) = API.get() else {
        return false;
    };
    let mut id = CObject {
        kind: K_INT64,
        value: CValue {
            as_int64: request as i64,
        },
    };
    let mut code = CObject {
        kind: K_INT32,
        value: CValue { as_int32: status },
    };
    let mut data = CObject {
        kind: K_TYPED_DATA,
        value: CValue {
            as_typed_data: CTypedData {
                kind: TYPED_DATA_UINT8,
                length: bytes.len() as isize,
                values: bytes.as_ptr(),
            },
        },
    };
    let mut items: [*mut CObject; 3] = [&mut id, &mut code, &mut data];
    let mut message = CObject {
        kind: K_ARRAY,
        value: CValue {
            as_array: CArray {
                length: items.len() as isize,
                values: items.as_mut_ptr(),
            },
        },
    };
    unsafe { (api.post)(port, &mut message) }
}

/// The native port, created on first use with [handler], that isolates name in
/// `Isolate.addOnExitListener`; 0 when the VM was not initialized or refused to create it.
pub fn exit_port(handler: NativeHandler) -> Port {
    let Some(api) = API.get() else {
        return 0;
    };
    let mut port = EXIT_PORT.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *port == 0 {
        *port = unsafe { (api.new_native_port)(c"mongo_native isolate exits".as_ptr(), handler, false) };
    }
    *port
}

/// A new `SendPort` to [port].
///
/// # Safety
/// Must be called on a thread that has entered an isolate, inside an API scope (a Dart FFI call
/// that returns a `Handle`), after a successful [initialize].
pub unsafe fn send_port(port: Port) -> Handle {
    let api = API.get().expect("send_port called before mn_init");
    unsafe { (api.new_send_port)(port) }
}

/// The integer carried by [message], when it is one.
///
/// # Safety
/// [message] must be null or point to a valid `Dart_CObject`.
pub unsafe fn integer(message: *const CObject) -> Option<i64> {
    let m = unsafe { message.as_ref()? };
    match m.kind {
        K_INT32 => Some(unsafe { m.value.as_int32 } as i64),
        K_INT64 => Some(unsafe { m.value.as_int64 }),
        _ => None,
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;

    extern "C" fn no_native_port(_: *const c_char, _: NativeHandler, _: bool) -> Port {
        0
    }

    extern "C" fn no_send_port(_: Port) -> Handle {
        std::ptr::null_mut()
    }

    /// Initializes the API with a table whose `Dart_PostCObject` is [post].
    pub fn install(post: unsafe extern "C" fn(Port, *mut CObject) -> bool) -> isize {
        let entries = [
            DartApiEntry {
                name: c"Dart_PostCObject".as_ptr(),
                function: post as *const c_void,
            },
            DartApiEntry {
                name: c"Dart_NewNativePort".as_ptr(),
                function: no_native_port as *const c_void,
            },
            DartApiEntry {
                name: c"Dart_NewSendPort".as_ptr(),
                function: no_send_port as *const c_void,
            },
            DartApiEntry {
                name: std::ptr::null(),
                function: std::ptr::null(),
            },
        ];
        let api = DartApi {
            major: DL_MAJOR_VERSION,
            minor: 0,
            functions: entries.as_ptr(),
        };
        unsafe { initialize(&api as *const DartApi as *const c_void) }
    }

    /// The request id, status and bytes of a reply message built by [post_reply].
    ///
    /// # Safety
    /// [message] must be a message passed by [post_reply] to the installed post function.
    pub unsafe fn read_reply(message: *const CObject) -> (u64, i32, Vec<u8>) {
        unsafe {
            let array = (*message).value.as_array;
            assert_eq!((*message).kind, K_ARRAY);
            assert_eq!(array.length, 3);
            let id = integer(*array.values).unwrap() as u64;
            let status = integer(*array.values.add(1)).unwrap() as i32;
            let data = &**array.values.add(2);
            assert_eq!(data.kind, K_TYPED_DATA);
            let typed = data.value.as_typed_data;
            assert_eq!(typed.kind, TYPED_DATA_UINT8);
            let bytes = std::slice::from_raw_parts(typed.values, typed.length as usize).to_vec();
            (id, status, bytes)
        }
    }
}
