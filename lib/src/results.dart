/// The ids of the inserted documents.
final class MongoInsertResult {
  /// Creates the result with [insertedIds] in request order.
  const MongoInsertResult(this.insertedIds);

  /// The `_id` of each inserted document, in request order (generated ones included).
  final List<Object?> insertedIds;
}

/// The counts of an update or replace, as the server reported them.
final class MongoUpdateResult {
  /// Creates the result.
  const MongoUpdateResult(this.matched, this.modified, this.upsertedId);

  /// Documents that matched the filter.
  final int matched;

  /// Documents actually changed.
  final int modified;

  /// The `_id` of the document created by an upsert, or null.
  final Object? upsertedId;
}

/// The count of a delete, as the server reported it.
final class MongoDeleteResult {
  /// Creates the result.
  const MongoDeleteResult(this.deleted);

  /// Documents deleted.
  final int deleted;
}

/// The counts of a `bulkWrite`, as the server reported them.
final class MongoBulkWriteResult {
  /// Creates the result.
  const MongoBulkWriteResult(this.inserted, this.matched, this.modified, this.deleted, this.upserted, this.upsertedIds);

  /// Reads the result from a native reply.
  factory MongoBulkWriteResult.fromReply(Map<String, Object?> reply) => MongoBulkWriteResult(
    reply['inserted'] as int? ?? 0,
    reply['matched'] as int? ?? 0,
    reply['modified'] as int? ?? 0,
    reply['deleted'] as int? ?? 0,
    reply['upserted'] as int? ?? 0,
    {
      for (final e in (reply['upsertedIds'] as List? ?? const []).cast<Map<String, Object?>>())
        e['index'] as int: e['id'],
    },
  );

  /// Documents inserted.
  final int inserted;

  /// Documents matched by updates and replaces.
  final int matched;

  /// Documents changed by updates and replaces.
  final int modified;

  /// Documents deleted.
  final int deleted;

  /// Documents created by upserts.
  final int upserted;

  /// The `_id` created by each upsert, by position of the model in the request.
  final Map<int, Object?> upsertedIds;
}
