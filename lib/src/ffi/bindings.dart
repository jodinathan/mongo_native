import 'dart:ffi';

@Native<Uint32 Function()>(symbol: 'mn_abi_version')
external int _abiVersion();

@Native<IntPtr Function(Pointer<Void>)>(symbol: 'mn_init')
external int _init(Pointer<Void> data);

@Native<Int64 Function()>(symbol: 'mn_exit_port')
external int _exitPort();

@Native<Handle Function(Int64)>(symbol: 'mn_send_port')
external Object _sendPort(int port);

@Native<Uint64 Function(Int64, Pointer<Uint8>, Size)>(symbol: 'mn_submit')
external int _submit(int port, Pointer<Uint8> data, int len);

@Native<Int32 Function(Uint64)>(symbol: 'mn_cancel')
external int _cancel(int request);

/// The C functions of the native library, bundled by the build hook as the code asset of this
/// library.
final class NativeBindings {
  /// The bindings.
  const NativeBindings();

  /// The C interface version the loaded library speaks.
  int abiVersion() => _abiVersion();

  /// Hands the Dart VM functions ([NativeApi.initializeApiDLData]) to the library; returns 0, or
  /// -1 (another major version of the API), -2 (a missing function), -3 (no data).
  int initialize(Pointer<Void> data) => _init(data);

  /// The id of the native port told when an isolate ends, or 0 when it could not be created.
  int exitPort() => _exitPort();

  /// A `SendPort` to the port [port].
  Object sendPort(int port) => _sendPort(port);

  /// Queues the BSON request [data]/[len], whose reply is posted to the port [port] as
  /// `[id, status, bytes]`; returns its id, or 0 when the native side refused it.
  int submit(int port, Pointer<Uint8> data, int len) => _submit(port, data, len);

  /// Aborts the request [request]; returns 1 when it was still running.
  int cancel(int request) => _cancel(request);
}
