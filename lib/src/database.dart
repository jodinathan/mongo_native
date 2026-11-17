import 'bson/types.dart';
import 'cancel.dart';
import 'change_stream.dart';
import 'client.dart';
import 'collection.dart';

/// One database of a [MongoClient].
final class MongoDatabase {
  /// The database [name] of [client]; same as `client.database(name)`.
  MongoDatabase(this.client, this.name);

  /// The client this database belongs to.
  final MongoClient client;

  /// The database name.
  final String name;

  /// The collection [name] of this database.
  MongoCollection collection(String name) => MongoCollection(this, name);

  /// Runs [command] as given (the package adds no `maxTimeMS`) and returns the server answer.
  ///
  /// Declare [readOnly] for commands that never write (`hello`, `ping`, `buildInfo`, …): a timeout or
  /// cancellation of a command that is not read-only leaves its outcome unknown.
  Future<Map<String, Object?>> runCommand(
    Map<String, Object?> command, {
    bool readOnly = false,
    Duration? timeout,
    MongoCancelToken? cancel,
  }) async {
    final reply = await clientCall(
      client,
      {'op': 'runCommand', 'db': name, 'command': command, 'readOnly': readOnly},
      clock: plainClock(client, timeout),
      cancel: cancel,
    );
    return reply['result']! as Map<String, Object?>;
  }

  /// The names of the collections of this database.
  Future<List<String>> listCollectionNames({Duration? timeout, MongoCancelToken? cancel}) async {
    final reply = await clientCall(
      client,
      {'op': 'listCollectionNames', 'db': name},
      clock: plainClock(client, timeout),
      cancel: cancel,
    );
    return (reply['names']! as List).cast<String>();
  }

  /// Drops the collection [name] with its indexes; dropping a missing collection succeeds.
  Future<void> dropCollection(String name, {Duration? timeout, MongoCancelToken? cancel}) => clientCall(
    client,
    {'op': 'dropCollection', 'db': this.name, 'coll': name},
    clock: plainClock(client, timeout),
    cancel: cancel,
  );

  /// Opens a change stream over every collection of this database.
  ///
  /// [pipeline] filters and shapes the events (keep `_id`, the resume token). [resumeAfter] or
  /// [startAtOperationTime] start after a known point. Each `next` waits up to [maxAwait] and returns
  /// up to [batchSize] events. Elections and network failures are resumed by the driver without a
  /// gap; a stream that cannot resume throws and closes.
  Future<MongoChangeStream> watch({
    List<Map<String, Object?>> pipeline = const [],
    Map<String, Object?>? resumeAfter,
    BsonTimestamp? startAtOperationTime,
    Duration maxAwait = const Duration(milliseconds: 500),
    int batchSize = 1000,
  }) => openChangeStream(
    client,
    db: name,
    pipeline: pipeline,
    resumeAfter: resumeAfter,
    startAtOperationTime: startAtOperationTime,
    maxAwait: maxAwait,
    batchSize: batchSize,
  );
}
