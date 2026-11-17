/// MongoDB for Dart over the official Rust driver (`mongodb` 3.10.1) through FFI.
///
/// Open a [MongoClient], reach a [MongoCollection] through [MongoDatabase], and catch
/// [MongoNativeException]: every server and network failure is thrown as one of its subtypes.
library;

export 'src/bson/codec.dart' show BsonCodec;
export 'src/bson/errors.dart' show BsonFormatException;
export 'src/bson/types.dart'
    show BsonBinary, BsonDecimal128, BsonMaxKey, BsonMinKey, BsonObjectId, BsonRegex, BsonTimestamp;
export 'src/cancel.dart' show MongoCancelToken;
export 'src/change_stream.dart' show MongoChangeBatch, MongoChangeEvent, MongoChangeStream, MongoOperationType;
export 'src/client.dart' show MongoClient;
export 'src/collection.dart' show MongoCollection;
export 'src/cursor.dart' show MongoCursor;
export 'src/database.dart' show MongoDatabase;
export 'src/errors.dart'
    show
        MongoAuthenticationException,
        MongoBsonException,
        MongoBulkWriteException,
        MongoCancelledException,
        MongoClosedException,
        MongoCommandException,
        MongoInternalException,
        MongoNativeException,
        MongoNetworkException,
        MongoServerSelectionException,
        MongoTimeoutException,
        MongoWriteConcernException,
        MongoWriteException;
export 'src/options.dart'
    show MongoClientOptions, MongoIndexModel, MongoReadConcern, MongoTlsOptions, MongoWriteConcern;
export 'src/results.dart' show MongoBulkWriteResult, MongoDeleteResult, MongoInsertResult, MongoUpdateResult;
export 'src/write_models.dart'
    show
        MongoDeleteMany,
        MongoDeleteOne,
        MongoInsertOne,
        MongoReplaceOne,
        MongoUpdateMany,
        MongoUpdateOne,
        MongoWriteModel;
