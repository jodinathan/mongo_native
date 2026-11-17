import 'bson/types.dart';
import 'cancel.dart';
import 'client.dart';
import 'errors.dart';

/// The kind of change of a [MongoChangeEvent].
enum MongoOperationType {
  /// A document was inserted.
  insert,

  /// A document was updated with operators.
  update,

  /// A document was replaced.
  replace,

  /// A document was deleted.
  delete,

  /// The stream can no longer continue (its collection or database was dropped or renamed); it is
  /// the last event, and the next call throws.
  invalidate,

  /// A collection was dropped.
  drop,

  /// A database was dropped.
  dropDatabase,

  /// A collection was renamed.
  rename,

  /// Any other event (DDL events of newer servers).
  other,
}

/// One change, as the server described it.
final class MongoChangeEvent {
  /// Creates the event.
  const MongoChangeEvent({
    required this.resumeToken,
    required this.clusterTime,
    required this.operationType,
    required this.raw,
    this.database,
    this.collection,
    this.documentKey,
  });

  /// Reads the event from the document the server sent; throws [MongoBsonException] when the
  /// pipeline removed `_id` or `clusterTime`.
  factory MongoChangeEvent.fromDocument(Map<String, Object?> doc, String operation) {
    final token = doc['_id'];
    final time = doc['clusterTime'];
    if (token is! Map<String, Object?> || time is! BsonTimestamp) {
      throw MongoBsonException(
        operation: operation,
        message: 'a change event has no `_id` or `clusterTime`; keep both fields in the watch pipeline',
      );
    }
    final ns = doc['ns'];
    final type = doc['operationType'];
    return MongoChangeEvent(
      resumeToken: token,
      clusterTime: time,
      operationType: MongoOperationType.values.firstWhere(
        (t) => t.name == type,
        orElse: () => MongoOperationType.other,
      ),
      database: ns is Map ? ns['db'] as String? : null,
      collection: ns is Map ? ns['coll'] as String? : null,
      documentKey: doc['documentKey'] as Map<String, Object?>?,
      raw: doc,
    );
  }

  /// The point right after this event, for `resumeAfter`.
  final Map<String, Object?> resumeToken;

  /// When the change was applied, in the server's logical clock.
  final BsonTimestamp clusterTime;

  /// The kind of change.
  final MongoOperationType operationType;

  /// The database of the change.
  final String? database;

  /// The collection of the change.
  final String? collection;

  /// The `_id` (and shard key) of the changed document.
  final Map<String, Object?>? documentKey;

  /// The whole event as the server sent it, after the watch pipeline.
  final Map<String, Object?> raw;
}

/// The events of one `next` call and the point after them.
final class MongoChangeBatch {
  /// Creates the batch.
  const MongoChangeBatch(this.events, this.resumeToken);

  /// The events, in server order; empty when nothing arrived within `maxAwait`.
  final List<MongoChangeEvent> events;

  /// The point after this batch (the server's `postBatchResumeToken` when the batch ended a server
  /// batch), present even when [events] is empty.
  final Map<String, Object?>? resumeToken;
}

/// A change stream; elections and network failures are resumed by the driver without a gap.
final class MongoChangeStream {
  MongoChangeStream._(this._client, this._handle, this._operation, this._maxAwait, this._token);

  final MongoClient _client;
  final String _operation;
  final Duration _maxAwait;
  int _handle;
  Map<String, Object?>? _token;

  /// Waits up to `maxAwait` for events; the batch is empty when nothing arrived.
  ///
  /// Throws [MongoNativeException] when the stream cannot resume (e.g. `ChangeStreamHistoryLost`,
  /// code 286) and [MongoClosedException] after an `invalidate` event; either way the stream closes.
  Future<MongoChangeBatch> next({MongoCancelToken? cancel}) async {
    if (_handle == 0) {
      throw MongoClosedException(operation: 'stream.next $_operation', message: 'the change stream is closed');
    }
    final Map<String, Object?> r;
    try {
      r = await clientCall(
        _client,
        {'op': 'stream.next', 'stream': _handle},
        clock: _maxAwait + _client.options.operationTimeout,
        cancel: cancel,
      );
    } on MongoCancelledException {
      rethrow;
    } on MongoTimeoutException {
      rethrow;
    } on MongoNativeException {
      _handle = 0;
      rethrow;
    }
    final token = r['resumeToken'] as Map<String, Object?>?;
    if (token != null) _token = token;
    final events = [
      for (final e in (r['events']! as List).cast<Map<String, Object?>>())
        MongoChangeEvent.fromDocument(e, 'stream.next $_operation'),
    ];
    return MongoChangeBatch(events, _token);
  }

  /// The point after the last batch, for `resumeAfter`.
  Map<String, Object?>? get resumeToken => _token;

  /// Closes the stream; does nothing when it is already closed.
  Future<void> close() async {
    final handle = _handle;
    if (handle == 0) return;
    _handle = 0;
    if (_client.isClosed) return;
    await clientCall(_client, {'op': 'stream.close', 'stream': handle}, clock: _client.options.operationTimeout);
  }
}

/// Opens a change stream on [db] (or on its collection [coll]).
Future<MongoChangeStream> openChangeStream(
  MongoClient client, {
  required String db,
  String? coll,
  required List<Map<String, Object?>> pipeline,
  Map<String, Object?>? resumeAfter,
  BsonTimestamp? startAtOperationTime,
  required Duration maxAwait,
  required int batchSize,
}) async {
  final r = await clientCall(client, {
    'op': 'watch',
    'db': db,
    'coll': ?coll,
    'pipeline': pipeline,
    'resumeAfter': ?resumeAfter,
    'startAtOperationTime': ?startAtOperationTime,
    'maxAwaitMs': maxAwait.inMilliseconds,
    'batchSize': batchSize,
  }, clock: client.options.operationTimeout);
  return MongoChangeStream._(
    client,
    r['stream']! as int,
    coll == null ? 'on $db' : 'on $db.$coll',
    maxAwait,
    r['resumeToken'] as Map<String, Object?>?,
  );
}
