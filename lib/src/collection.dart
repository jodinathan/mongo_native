import 'bson/types.dart';
import 'cancel.dart';
import 'change_stream.dart';
import 'client.dart';
import 'cursor.dart';
import 'database.dart';
import 'options.dart';
import 'results.dart';
import 'write_models.dart';

/// One collection of a [MongoDatabase].
///
/// Every operation takes a client clock `timeout` (default `MongoClientOptions.operationTimeout`)
/// and a `cancel` token. Reads also take `maxTime`, the server clock (`maxTimeMS`): without it the
/// server gets `timeout` and the client waits one second more.
final class MongoCollection {
  /// The collection [name] of [database]; same as `database.collection(name)`.
  MongoCollection(this.database, this.name);

  /// The database this collection belongs to.
  final MongoDatabase database;

  /// The collection name.
  final String name;

  MongoClient get _client => database.client;

  Map<String, Object?> _base(String op) => {'op': op, 'db': database.name, 'coll': name};

  Future<Map<String, Object?>> _write(
    String op,
    Map<String, Object?> fields,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  ) => clientCall(
    _client,
    {..._base(op), ...fields, if (writeConcern != null) 'writeConcern': writeConcernRequest(writeConcern)},
    clock: plainClock(_client, timeout),
    cancel: cancel,
  );

  Future<Map<String, Object?>> _read(
    String op,
    Map<String, Object?> fields,
    Duration? maxTime,
    MongoReadConcern? readConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  ) {
    final clocks = readClocks(_client, timeout, maxTime);
    return clientCall(
      _client,
      {
        ..._base(op),
        ...fields,
        'maxTimeMs': clocks.maxTime.inMilliseconds,
        if (readConcern != null) 'readConcern': readConcern.name,
      },
      clock: clocks.clock,
      cancel: cancel,
    );
  }

  static MongoUpdateResult _updateResult(Map<String, Object?> r) =>
      MongoUpdateResult(r['matched']! as int, r['modified']! as int, r['upsertedId']);

  /// Inserts [doc]; an ObjectId `_id` is added when it has none.
  ///
  /// Throws [MongoWriteException] (e.g. `isDuplicateKey`) when the server refuses it.
  Future<MongoInsertResult> insertOne(
    Map<String, Object?> doc, {
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _write('insertOne', {'document': doc}, writeConcern, timeout, cancel);
    return MongoInsertResult([r['insertedId']]);
  }

  /// Inserts [docs]; with [ordered], the first failure stops the rest.
  ///
  /// Throws [MongoBulkWriteException] with every failed write.
  Future<MongoInsertResult> insertMany(
    List<Map<String, Object?>> docs, {
    bool ordered = true,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _write('insertMany', {'documents': docs, 'ordered': ordered}, writeConcern, timeout, cancel);
    return MongoInsertResult((r['insertedIds']! as List).cast<Object?>());
  }

  /// Updates the first document matching [filter] with [update], a document of operators or a
  /// pipeline (`List<Map<String, Object?>>`, needed for `$$NOW`).
  Future<MongoUpdateResult> updateOne(
    Map<String, Object?> filter,
    Object update, {
    bool upsert = false,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    checkUpdate(update);
    final r = await _write(
      'updateOne',
      {'filter': filter, 'update': update, 'upsert': upsert},
      writeConcern,
      timeout,
      cancel,
    );
    return _updateResult(r);
  }

  /// Updates every document matching [filter] with [update], a document of operators or a pipeline.
  Future<MongoUpdateResult> updateMany(
    Map<String, Object?> filter,
    Object update, {
    bool upsert = false,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    checkUpdate(update);
    final r = await _write(
      'updateMany',
      {'filter': filter, 'update': update, 'upsert': upsert},
      writeConcern,
      timeout,
      cancel,
    );
    return _updateResult(r);
  }

  /// Replaces the first document matching [filter] with [replacement].
  Future<MongoUpdateResult> replaceOne(
    Map<String, Object?> filter,
    Map<String, Object?> replacement, {
    bool upsert = false,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _write(
      'replaceOne',
      {'filter': filter, 'replacement': replacement, 'upsert': upsert},
      writeConcern,
      timeout,
      cancel,
    );
    return _updateResult(r);
  }

  /// Deletes the first document matching [filter].
  Future<MongoDeleteResult> deleteOne(
    Map<String, Object?> filter, {
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _write('deleteOne', {'filter': filter}, writeConcern, timeout, cancel);
    return MongoDeleteResult(r['deleted']! as int);
  }

  /// Deletes every document matching [filter].
  Future<MongoDeleteResult> deleteMany(
    Map<String, Object?> filter, {
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _write('deleteMany', {'filter': filter}, writeConcern, timeout, cancel);
    return MongoDeleteResult(r['deleted']! as int);
  }

  /// The first document matching [filter] in [sort] order, or null.
  Future<Map<String, Object?>?> findOne(
    Map<String, Object?> filter, {
    Map<String, Object?>? projection,
    Map<String, Object?>? sort,
    Duration? maxTime,
    MongoReadConcern? readConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _read(
      'findOne',
      {'filter': filter, 'projection': ?projection, 'sort': ?sort},
      maxTime,
      readConcern,
      timeout,
      cancel,
    );
    return r['document'] as Map<String, Object?>?;
  }

  /// A cursor over the documents matching [filter]; the first batch of up to [batchSize] documents
  /// arrives with it. [hint] names the index to use.
  Future<MongoCursor> find(
    Map<String, Object?> filter, {
    Map<String, Object?>? projection,
    Map<String, Object?>? sort,
    int skip = 0,
    int? limit,
    int batchSize = 1000,
    String? hint,
    Duration? maxTime,
    MongoReadConcern? readConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _read(
      'find',
      {
        'filter': filter,
        'projection': ?projection,
        'sort': ?sort,
        'skip': skip,
        'limit': ?limit,
        'batchSize': batchSize,
        'hint': ?hint,
      },
      maxTime,
      readConcern,
      timeout,
      cancel,
    );
    return openCursor(_client, database.name, name, r, batchSize);
  }

  /// Updates the first document matching [filter] and returns it after the update ([returnAfter])
  /// or before it; null when nothing matched and nothing was upserted.
  Future<Map<String, Object?>?> findOneAndUpdate(
    Map<String, Object?> filter,
    Object update, {
    bool upsert = false,
    bool returnAfter = true,
    Map<String, Object?>? sort,
    Map<String, Object?>? projection,
    Duration? maxTime,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    checkUpdate(update);
    final clocks = readClocks(_client, timeout, maxTime);
    final r = await _write(
      'findOneAndUpdate',
      {
        'filter': filter,
        'update': update,
        'upsert': upsert,
        'returnAfter': returnAfter,
        'sort': ?sort,
        'projection': ?projection,
        'maxTimeMs': clocks.maxTime.inMilliseconds,
      },
      writeConcern,
      clocks.clock,
      cancel,
    );
    return r['document'] as Map<String, Object?>?;
  }

  /// Deletes the first document matching [filter] in [sort] order and returns it, or null.
  Future<Map<String, Object?>?> findOneAndDelete(
    Map<String, Object?> filter, {
    Map<String, Object?>? sort,
    Map<String, Object?>? projection,
    Duration? maxTime,
    MongoWriteConcern? writeConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final clocks = readClocks(_client, timeout, maxTime);
    final r = await _write(
      'findOneAndDelete',
      {'filter': filter, 'sort': ?sort, 'projection': ?projection, 'maxTimeMs': clocks.maxTime.inMilliseconds},
      writeConcern,
      clocks.clock,
      cancel,
    );
    return r['document'] as Map<String, Object?>?;
  }

  /// The number of documents matching [filter], counted by the server.
  Future<int> countDocuments(
    Map<String, Object?> filter, {
    String? hint,
    Duration? maxTime,
    MongoReadConcern? readConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _read('countDocuments', {'filter': filter, 'hint': ?hint}, maxTime, readConcern, timeout, cancel);
    return r['count']! as int;
  }

  /// A cursor over the result of [pipeline]; the first batch arrives with it.
  Future<MongoCursor> aggregate(
    List<Map<String, Object?>> pipeline, {
    int batchSize = 1000,
    Duration? maxTime,
    MongoReadConcern? readConcern,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _read(
      'aggregate',
      {'pipeline': pipeline, 'batchSize': batchSize},
      maxTime,
      readConcern,
      timeout,
      cancel,
    );
    return openCursor(_client, database.name, name, r, batchSize);
  }

  /// Creates [indexes] (existing identical indexes are kept) and returns their names.
  Future<List<String>> createIndexes(
    List<MongoIndexModel> indexes, {
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final r = await _write(
      'createIndexes',
      {
        'indexes': [for (final i in indexes) indexRequest(i)],
      },
      null,
      timeout,
      cancel,
    );
    return (r['names']! as List).cast<String>();
  }

  /// Drops the index [name].
  Future<void> dropIndex(String name, {Duration? timeout, MongoCancelToken? cancel}) =>
      _write('dropIndex', {'name': name}, null, timeout, cancel);

  /// Every index of this collection, as the server describes it.
  Future<List<Map<String, Object?>>> listIndexes({Duration? timeout, MongoCancelToken? cancel}) async {
    final r = await clientCall(_client, _base('listIndexes'), clock: plainClock(_client, timeout), cancel: cancel);
    return (r['indexes']! as List).cast<Map<String, Object?>>();
  }

  /// Opens a change stream over this collection; see [MongoDatabase.watch].
  Future<MongoChangeStream> watch({
    List<Map<String, Object?>> pipeline = const [],
    Map<String, Object?>? resumeAfter,
    BsonTimestamp? startAtOperationTime,
    Duration maxAwait = const Duration(milliseconds: 500),
    int batchSize = 1000,
  }) => openChangeStream(
    _client,
    db: database.name,
    coll: name,
    pipeline: pipeline,
    resumeAfter: resumeAfter,
    startAtOperationTime: startAtOperationTime,
    maxAwait: maxAwait,
    batchSize: batchSize,
  );
}
