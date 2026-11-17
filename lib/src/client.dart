import 'dart:async';

import 'cancel.dart';
import 'database.dart';
import 'errors.dart';
import 'ffi/bridge.dart';
import 'options.dart';
import 'results.dart';
import 'write_models.dart';

/// A connection pool to one deployment (replica set, sharded cluster or single server), monitored by
/// the official Rust driver: it follows elections and frozen members, and retries retryable writes
/// and reads once.
///
/// Connecting does not contact the server; the first operation does, and fails with a typed
/// [MongoNativeException] when the server cannot be reached or refuses the credentials.
final class MongoClient {
  MongoClient._(this._bridge, this._handle, this.options);

  final NativeBridge _bridge;
  final int _handle;
  bool _closed = false;
  Future<void>? _closing;

  /// The options this client was opened with.
  final MongoClientOptions options;

  /// Opens a client with [options]; throws [MongoNativeException] for an invalid URI or options.
  static Future<MongoClient> connect(MongoClientOptions options) async {
    final bridge = NativeBridge.current;
    bridge.retain();
    try {
      final reply = await bridge.call({
        'op': 'client.open',
        'uri': options.uri,
        'appName': ?options.appName,
        'maxPoolSize': options.maxPoolSize,
        'minPoolSize': options.minPoolSize,
        'maxIdleTimeMs': options.maxIdleTime.inMilliseconds,
        'connectTimeoutMs': options.connectTimeout.inMilliseconds,
        'serverSelectionTimeoutMs': options.serverSelectionTimeout.inMilliseconds,
        'heartbeatFrequencyMs': options.heartbeatFrequency.inMilliseconds,
        if (options.tls case final tls?) 'tls': {'caFile': ?tls.caFile, 'certificateKeyFile': ?tls.certificateKeyFile},
        'writeConcern': writeConcernRequest(options.writeConcern),
        'readConcern': options.readConcern.name,
        'retryWrites': options.retryWrites,
        'retryReads': options.retryReads,
        'timeoutMs': options.operationTimeout.inMilliseconds,
      });
      return MongoClient._(bridge, reply['client']! as int, options);
    } catch (_) {
      bridge.releaseClient();
      rethrow;
    }
  }

  /// Whether [close] was called.
  bool get isClosed => _closed;

  /// The database [name].
  MongoDatabase database(String name) => MongoDatabase(this, name);

  /// Runs [models] in one `bulkWrite` command (MongoDB 8.0 or newer); the models may target different
  /// collections. With [ordered], the first failed write stops the rest.
  ///
  /// Throws [MongoBulkWriteException] with every failed write and what was applied.
  Future<MongoBulkWriteResult> bulkWrite(
    List<MongoWriteModel> models, {
    bool ordered = true,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final reply = await clientCall(
      this,
      {
        'op': 'bulkWrite',
        'models': [for (final m in models) writeModelRequest(m)],
        'ordered': ordered,
        if (writeConcern != null) 'writeConcern': writeConcernRequest(writeConcern),
      },
      clock: plainClock(this, timeout),
      cancel: cancel,
    );
    return MongoBulkWriteResult.fromReply(reply);
  }

  /// Closes the client: new calls throw [MongoClosedException], operations in flight end with
  /// [MongoClosedException] (or their result, when it arrived first), and cursors and change streams
  /// of this client are closed. Completes once every one of them has completed. Later calls return
  /// the same future.
  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    _closed = true;
    try {
      await _bridge.call({
        'op': 'client.close',
        'client': _handle,
        'timeoutMs': (options.operationTimeout + const Duration(seconds: 30)).inMilliseconds,
      });
    } on MongoClosedException {
      return;
    } finally {
      _bridge.releaseClient();
    }
  }
}

/// The client clock of an operation without a server clock: [timeout] or the client default.
Duration plainClock(MongoClient client, Duration? timeout) => timeout ?? client.options.operationTimeout;

/// The client clock and the server clock (`maxTimeMS`) of a read: without [maxTime], the server
/// gets the timeout and the client waits one second more, so that the server answer arrives first.
({Duration clock, Duration maxTime}) readClocks(MongoClient client, Duration? timeout, Duration? maxTime) {
  final budget = timeout ?? client.options.operationTimeout;
  if (maxTime == null) return (clock: budget + const Duration(seconds: 1), maxTime: budget);
  return (clock: budget, maxTime: maxTime);
}

/// Sends [request] on [client] with the client clock [clock]; fails with [MongoClosedException]
/// when the client is closed.
Future<Map<String, Object?>> clientCall(
  MongoClient client,
  Map<String, Object?> request, {
  required Duration clock,
  MongoCancelToken? cancel,
}) {
  if (client._closed) {
    final target = switch ((request['db'], request['coll'])) {
      (final String db, final String coll) => '${request['op']} on $db.$coll',
      (final String db, _) => '${request['op']} on $db',
      _ => '${request['op']}',
    };
    return Future.error(
      MongoClosedException(operation: target, message: 'the client is closed; connect again to keep working'),
    );
  }
  return client._bridge.call({...request, 'client': client._handle, 'timeoutMs': clock.inMilliseconds}, cancel: cancel);
}
