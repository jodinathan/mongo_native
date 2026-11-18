import 'dart:async';
import 'dart:ffi';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import '../bson/codec.dart';
import '../bson/errors.dart';
import '../cancel.dart';
import '../errors.dart';
import 'bindings.dart';

const _bindings = NativeBindings();

/// The link between this isolate and the native side: one port that receives the replies and the
/// requests waiting on it.
///
/// The native side posts each reply to that port, so a reply for an isolate that ended is dropped
/// by the VM. The isolate also names the port in an exit listener of the native side, which closes
/// the clients the isolate left open when it ends.
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
    final init = _bindings.initialize(NativeApi.initializeApiDLData);
    if (init != 0) {
      throw MongoInternalException(
        operation: 'load the native library',
        message: switch (init) {
          -1 =>
            'the dynamically linked API of this Dart VM has a major version other than 2, the one the '
                'native library speaks; use a Dart 3 SDK (3.10 or newer) or a mongo_native release that '
                'supports this SDK',
          -2 =>
            'the Dart VM does not export Dart_PostCObject, Dart_NewNativePort or Dart_NewSendPort, '
                'which the native library needs; use a Dart SDK from 3.10 on',
          _ => 'the native library could not read the Dart VM API (code $init)',
        },
      );
    }
    final exitPort = _bindings.exitPort();
    if (exitPort == 0) {
      throw MongoInternalException(
        operation: 'load the native library',
        message: 'the Dart VM refused to create the native port that tells mongo_native when an isolate ends',
      );
    }
    _exits = _bindings.sendPort(exitPort) as SendPort;
    _replies = RawReceivePort(_receive, 'mongo_native replies')..keepIsolateAlive = false;
    _port = _replies.sendPort.nativePort;
    Isolate.current.addOnExitListener(_exits, response: _port);
  }

  /// The C interface version this package speaks.
  static const kAbiVersion = 2;

  static NativeBridge? _current;

  /// The bridge of this isolate; once closed, the next call creates another with a new port.
  static NativeBridge get current => _current ??= NativeBridge._();

  late final SendPort _exits;
  late final RawReceivePort _replies;
  late final int _port;
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
      id = _bindings.submit(_port, buf, bytes.length);
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
    _replies.keepIsolateAlive = true;
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

  void _receive(Object? message) {
    if (message case [final int id, final int status, final Uint8List bytes]) {
      final done = _pending.remove(id);
      if (_pending.isEmpty) _replies.keepIsolateAlive = false;
      if (done != null) {
        try {
          final reply = BsonCodec.decode(bytes);
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
  }

  void _closeIfIdle() {
    if (_closed || _clients > 0 || _pending.isNotEmpty) return;
    _closed = true;
    _replies.close();
    Isolate.current.removeOnExitListener(_exits);
    if (identical(_current, this)) _current = null;
  }
}
