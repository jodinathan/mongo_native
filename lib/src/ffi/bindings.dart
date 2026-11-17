import 'dart:ffi';

/// The completion callback of a request: id, status, reply bytes and their length.
typedef CompletionFn = Void Function(Uint64 request, Int32 status, Pointer<Uint8> data, Size len);

@Native<Uint32 Function()>(symbol: 'mn_abi_version')
external int _abiVersion();

@Native<Uint64 Function(Pointer<NativeFunction<CompletionFn>>, Pointer<Uint8>, Size)>(symbol: 'mn_submit')
external int _submit(Pointer<NativeFunction<CompletionFn>> completion, Pointer<Uint8> data, int len);

@Native<Int32 Function(Uint64)>(symbol: 'mn_cancel')
external int _cancel(int request);

@Native<Void Function(Pointer<Uint8>, Size)>(symbol: 'mn_free')
external void _free(Pointer<Uint8> data, int len);

/// The four C functions of the native library, bundled by the build hook as the code asset of
/// this library.
final class NativeBindings {
  /// The bindings.
  const NativeBindings();

  /// The C interface version the loaded library speaks.
  int abiVersion() => _abiVersion();

  /// Queues the BSON request [data]/[len]; returns its id, or 0 when the native side refused it.
  int submit(Pointer<NativeFunction<CompletionFn>> completion, Pointer<Uint8> data, int len) =>
      _submit(completion, data, len);

  /// Aborts the request [request]; returns 1 when it was still running.
  int cancel(int request) => _cancel(request);

  /// Releases a reply buffer received by the completion callback.
  void free(Pointer<Uint8> data, int len) => _free(data, len);
}
