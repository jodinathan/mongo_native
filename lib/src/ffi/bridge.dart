import 'dart:async';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import '../bson/codec.dart';
import '../bson/errors.dart';
import '../cancel.dart';
import '../errors.dart';
import 'bindings.dart';

const _bindings = NativeBindings();

/// The link between this isolate and the native side: one completion listener and the requests
/// waiting on it.
final class NativeBridge {
  NativeBridge._() {
    final int version;
    try {
      version = _bindings.abiVersion();
    } on ArgumentError catch (e) {
      throw MongoInternalException(
        operation: 'load the native library',
        message:
            'the native library of mongo_native is not available (${e.message}); run with a Dart SDK '
            'that supports build hooks (3.10 or newer) so that hook/build.dart provides it, or point '
            '`hooks: user_defines: mongo_native: lib:` in the root pubspec.yaml to a prebuilt library',
      );
    }
    if (version != kAbiVersion) {
      throw MongoInternalException(
        operation: 'load the native library',
        message:
            'the library speaks ABI $version and this package expects $kAbiVersion; delete '
            '.dart_tool/hooks_runner and run again so that the hook builds a matching library',
      );
    }
    _completion = NativeCallable<CompletionFn>.listener(_complete)..keepIsolateAlive = false;
  }

  /// The C interface version this package speaks.
  static const kAbiVersion = 1;

  static NativeBridge? _current;

  /// The bridge of this isolate; once closed, the next call creates another with a new listener.
  static NativeBridge get current => _current ??= NativeBridge._();

  late final NativeCallable<CompletionFn> _completion;
  final _pending = <int, Completer<Map<String, Object?>>>{};
  var _clients = 0;
  var _closed = false;

  /// Sends [request] to the native side and completes with the decoded reply; throws the
  /// [MongoNativeException] the native side described. [cancel] asks the running operation to stop.
  Future<Map<String, Object?>> call(Map<String, Object?> request, {MongoCancelToken? cancel}) {
    if (_closed) {
      return Future.error(
        MongoClosedException(
          operation: '${request['op']}',
          message: 'the native bridge of this client was released; connect again',
        ),
      );
    }
    final Uint8List bytes;
    try {
      bytes = BsonCodec.encode(request);
    } on ArgumentError catch (e) {
      return Future.error(e);
    }
    final buf = malloc<Uint8>(bytes.length);
    final int id;
    try {
      buf.asTypedList(bytes.length).setAll(0, bytes);
      id = _bindings.submit(_completion.nativeFunction, buf, bytes.length);
    } finally {
      malloc.free(buf);
    }
    if (id == 0) {
      return Future.error(
        MongoInternalException(
          operation: '${request['op']}',
          message: 'the native side refused the request (it panicked while queueing it)',
        ),
      );
    }
    final done = Completer<Map<String, Object?>>();
    _pending[id] = done;
    _completion.keepIsolateAlive = true;
    if (cancel == null) return done.future;
    final detach = attachCancel(cancel, () => _bindings.cancel(id));
    return done.future.whenComplete(detach);
  }

  /// Counts one client open over this bridge.
  void retain() => _clients++;

  /// Discounts one closed client and closes the bridge when nothing depends on it anymore.
  void releaseClient() {
    _clients--;
    _closeIfIdle();
  }

  void _complete(int id, int status, Pointer<Uint8> data, int len) {
    final copy = Uint8List.fromList(data.asTypedList(len));
    _bindings.free(data, len);
    final done = _pending.remove(id);
    if (_pending.isEmpty) _completion.keepIsolateAlive = false;
    if (done != null) {
      try {
        final reply = BsonCodec.decode(copy);
        if (status == 0) {
          done.complete(reply);
        } else {
          done.completeError(MongoNativeException.fromReply(reply));
        }
      } on BsonFormatException catch (e, st) {
        done.completeError(MongoBsonException(operation: 'decode the native reply', message: e.toString()), st);
      }
    }
    _closeIfIdle();
  }

  void _closeIfIdle() {
    if (_closed || _clients > 0 || _pending.isNotEmpty) return;
    _closed = true;
    _completion.close();
    if (identical(_current, this)) _current = null;
  }
}
