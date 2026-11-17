/// One write of `MongoClient.bulkWrite`; the models of one call may target different collections.
sealed class MongoWriteModel {
  /// Targets [database].[collection].
  const MongoWriteModel(this.database, this.collection);

  /// The database of the target collection.
  final String database;

  /// The target collection.
  final String collection;
}

/// Inserts [document].
final class MongoInsertOne extends MongoWriteModel {
  /// Creates the insert of [document] into [database].[collection].
  const MongoInsertOne(super.database, super.collection, this.document);

  /// The document to insert; the server side adds an ObjectId `_id` when it has none.
  final Map<String, Object?> document;
}

/// Updates the first document matching [filter].
final class MongoUpdateOne extends MongoWriteModel {
  /// Creates the update; [update] is a document of operators or a pipeline (a list of stages).
  MongoUpdateOne(super.database, super.collection, this.filter, this.update, {this.upsert = false}) {
    checkUpdate(update);
  }

  /// Which document to update.
  final Map<String, Object?> filter;

  /// The operators (`{r'$set': …}`) or the pipeline.
  final Object update;

  /// Whether to insert a document built from [filter] and [update] when none matches.
  final bool upsert;
}

/// Updates every document matching [filter].
final class MongoUpdateMany extends MongoWriteModel {
  /// Creates the update; [update] is a document of operators or a pipeline (a list of stages).
  MongoUpdateMany(super.database, super.collection, this.filter, this.update, {this.upsert = false}) {
    checkUpdate(update);
  }

  /// Which documents to update.
  final Map<String, Object?> filter;

  /// The operators (`{r'$set': …}`) or the pipeline.
  final Object update;

  /// Whether to insert a document built from [filter] and [update] when none matches.
  final bool upsert;
}

/// Replaces the first document matching [filter] with [replacement].
final class MongoReplaceOne extends MongoWriteModel {
  /// Creates the replacement.
  const MongoReplaceOne(super.database, super.collection, this.filter, this.replacement, {this.upsert = false});

  /// Which document to replace.
  final Map<String, Object?> filter;

  /// The new document (without operators).
  final Map<String, Object?> replacement;

  /// Whether to insert [replacement] when nothing matches.
  final bool upsert;
}

/// Deletes the first document matching [filter].
final class MongoDeleteOne extends MongoWriteModel {
  /// Creates the delete.
  const MongoDeleteOne(super.database, super.collection, this.filter);

  /// Which document to delete.
  final Map<String, Object?> filter;
}

/// Deletes every document matching [filter].
final class MongoDeleteMany extends MongoWriteModel {
  /// Creates the delete.
  const MongoDeleteMany(super.database, super.collection, this.filter);

  /// Which documents to delete.
  final Map<String, Object?> filter;
}

/// Throws [ArgumentError] unless [update] is a document of operators or a list of pipeline stages.
void checkUpdate(Object update) {
  if (update is Map<String, Object?>) return;
  if (update is List && update.every((stage) => stage is Map<String, Object?>)) return;
  throw ArgumentError.value(
    update.runtimeType,
    'update',
    'must be a Map<String, Object?> of operators or a List<Map<String, Object?>> pipeline',
  );
}

/// The request form of [model].
Map<String, Object?> writeModelRequest(MongoWriteModel model) {
  final target = {'db': model.database, 'coll': model.collection};
  return switch (model) {
    MongoInsertOne(:final document) => {'type': 'insertOne', ...target, 'document': document},
    MongoUpdateOne(:final filter, :final update, :final upsert) => {
      'type': 'updateOne',
      ...target,
      'filter': filter,
      'update': update,
      'upsert': upsert,
    },
    MongoUpdateMany(:final filter, :final update, :final upsert) => {
      'type': 'updateMany',
      ...target,
      'filter': filter,
      'update': update,
      'upsert': upsert,
    },
    MongoReplaceOne(:final filter, :final replacement, :final upsert) => {
      'type': 'replaceOne',
      ...target,
      'filter': filter,
      'replacement': replacement,
      'upsert': upsert,
    },
    MongoDeleteOne(:final filter) => {'type': 'deleteOne', ...target, 'filter': filter},
    MongoDeleteMany(:final filter) => {'type': 'deleteMany', ...target, 'filter': filter},
  };
}
