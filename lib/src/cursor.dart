import 'cancel.dart';
import 'client.dart';

/// The documents of a `find` or `aggregate`, read in batches.
///
/// A cursor that is not read to the end keeps a cursor open on the server until [close] (or until
/// the client closes).
final class MongoCursor {
  MongoCursor._(this._client, this._db, this._coll, this._handle, this._first, this._batchSize);

  final MongoClient _client;
  final String _db;
  final String _coll;
  int _handle;
  List<Map<String, Object?>>? _first;
  final int _batchSize;

  /// Whether the server cursor is gone (exhausted or closed).
  bool get isExhausted => _handle == 0 && _first == null;

  /// The next batch, empty when the cursor is exhausted; the cursor closes itself when exhausted.
  Future<List<Map<String, Object?>>> nextBatch({Duration? timeout, MongoCancelToken? cancel}) async {
    final first = _first;
    if (first != null) {
      _first = null;
      if (first.isNotEmpty || _handle == 0) return first;
    }
    if (_handle == 0) return const [];
    final r = await clientCall(
      _client,
      {'op': 'cursor.next', 'db': _db, 'coll': _coll, 'cursor': _handle, 'batchSize': _batchSize},
      clock: plainClock(_client, timeout),
      cancel: cancel,
    );
    if (r['exhausted'] == true) _handle = 0;
    return (r['batch']! as List).cast<Map<String, Object?>>();
  }

  /// Every remaining document; throws [StateError] (and closes the cursor) above [max] documents.
  Future<List<Map<String, Object?>>> toList({int max = 100000}) async {
    final out = <Map<String, Object?>>[];
    while (true) {
      final batch = await nextBatch();
      if (batch.isEmpty) return out;
      out.addAll(batch);
      if (out.length > max) {
        await close();
        throw StateError(
          'the cursor over $_db.$_coll holds more than $max documents; narrow the filter, set a limit '
          'or read it with nextBatch',
        );
      }
    }
  }

  /// Closes the cursor on the server; does nothing when it is already exhausted or closed.
  Future<void> close() async {
    _first = null;
    final handle = _handle;
    if (handle == 0) return;
    _handle = 0;
    if (_client.isClosed) return;
    await clientCall(_client, {
      'op': 'cursor.close',
      'db': _db,
      'coll': _coll,
      'cursor': handle,
    }, clock: plainClock(_client, null));
  }
}

/// The cursor described by the reply [reply] of a `find` or `aggregate` on [db].[coll].
MongoCursor openCursor(MongoClient client, String db, String coll, Map<String, Object?> reply, int batchSize) =>
    MongoCursor._(
      client,
      db,
      coll,
      reply['cursor']! as int,
      (reply['batch']! as List).cast<Map<String, Object?>>(),
      batchSize,
    );
