import 'results.dart';

/// Every failure of the server, the network or the native side, always thrown and never returned.
sealed class MongoNativeException implements Exception {
  /// Creates the failure of [operation] with the source's [message].
  const MongoNativeException({
    required this.operation,
    required this.message,
    this.code,
    this.codeName,
    this.labels = const {},
  });

  /// Builds the exception described by an error reply of the native side.
  factory MongoNativeException.fromReply(Map<String, Object?> reply) {
    final operation = reply['operation'] as String? ?? 'unknown operation';
    final message = reply['message'] as String? ?? 'no message';
    final code = reply['code'] as int?;
    final codeName = reply['codeName'] as String?;
    final labels = {...?(reply['labels'] as List?)?.cast<String>()};
    final outcomeUnknown = reply['outcomeUnknown'] as bool? ?? false;
    switch (reply['kind']) {
      case 'write':
        return MongoWriteException(
          operation: operation,
          message: message,
          code: code,
          codeName: codeName,
          labels: labels,
          index: reply['index'] as int? ?? 0,
        );
      case 'bulkWrite':
        final writes = (reply['writeErrors'] as List? ?? const [])
            .cast<Map<String, Object?>>()
            .map(
              (w) => MongoWriteException(
                operation: operation,
                message: w['message'] as String? ?? '',
                code: w['code'] as int?,
                codeName: w['codeName'] as String?,
                index: w['index'] as int? ?? 0,
              ),
            )
            .toList();
        final concerns = (reply['writeConcernErrors'] as List? ?? const [])
            .cast<Map<String, Object?>>()
            .map(
              (w) => MongoWriteConcernException(
                operation: operation,
                message: w['message'] as String? ?? '',
                code: w['code'] as int?,
                codeName: w['codeName'] as String?,
              ),
            )
            .toList();
        final partial = reply['partial'] as Map<String, Object?>?;
        return MongoBulkWriteException(
          operation: operation,
          message: message,
          code: code,
          codeName: codeName,
          labels: labels,
          writeErrors: writes,
          writeConcernErrors: concerns,
          partial: partial == null ? null : MongoBulkWriteResult.fromReply(partial),
        );
      case 'timeout':
        return MongoTimeoutException(
          operation: operation,
          message: message,
          labels: labels,
          outcomeUnknown: outcomeUnknown,
        );
      case 'cancelled':
        return MongoCancelledException(
          operation: operation,
          message: message,
          labels: labels,
          outcomeUnknown: outcomeUnknown,
        );
      case 'command':
        return MongoCommandException(
          operation: operation,
          message: message,
          code: code,
          codeName: codeName,
          labels: labels,
        );
      case 'writeConcern':
        return MongoWriteConcernException(
          operation: operation,
          message: message,
          code: code,
          codeName: codeName,
          labels: labels,
        );
      case 'network':
        return MongoNetworkException(operation: operation, message: message, labels: labels);
      case 'serverSelection':
        return MongoServerSelectionException(operation: operation, message: message, labels: labels);
      case 'authentication':
        return MongoAuthenticationException(operation: operation, message: message, labels: labels);
      case 'bson':
        return MongoBsonException(operation: operation, message: message);
      case 'closed':
        return MongoClosedException(operation: operation, message: message);
      default:
        return MongoInternalException(operation: operation, message: message, code: code, codeName: codeName);
    }
  }

  /// The operation and its target, e.g. `updateOne on app.users`.
  final String operation;

  /// The text the source gave (server, network, native side), without document values.
  final String message;

  /// The server error code, e.g. 11000 for a duplicate key.
  final int? code;

  /// The server error code name, e.g. `DuplicateKey`.
  final String? codeName;

  /// The error labels of the driver, e.g. `RetryableWriteError`.
  final Set<String> labels;

  @override
  String toString() =>
      'MongoDB $operation failed'
      '${code == null ? '' : ' (code $code${codeName == null ? '' : ' $codeName'})'}: $message';
}

/// A write refused by the server for one document (duplicate key, validation, …).
final class MongoWriteException extends MongoNativeException {
  /// Creates the failure of the write at [index].
  const MongoWriteException({
    required super.operation,
    required super.message,
    super.code,
    super.codeName,
    super.labels,
    this.index = 0,
  });

  /// The position of the failed write in the request (0 for single-document operations).
  final int index;

  /// Whether the write broke a unique index (code 11000).
  bool get isDuplicateKey => code == 11000;
}

/// A `bulkWrite` or `insertMany` with failed writes or write concern errors.
final class MongoBulkWriteException extends MongoNativeException {
  /// Creates the failure with each write error, each write concern error and what was applied.
  const MongoBulkWriteException({
    required super.operation,
    required super.message,
    super.code,
    super.codeName,
    super.labels,
    this.writeErrors = const [],
    this.writeConcernErrors = const [],
    this.partial,
  });

  /// The failed writes, by position in the request.
  final List<MongoWriteException> writeErrors;

  /// The write concern failures.
  final List<MongoWriteConcernException> writeConcernErrors;

  /// What the server applied before the failure, when it said so (`bulkWrite` only).
  final MongoBulkWriteResult? partial;
}

/// The client clock ran out before the answer arrived.
final class MongoTimeoutException extends MongoNativeException {
  /// Creates the timeout of [operation].
  const MongoTimeoutException({
    required super.operation,
    required super.message,
    super.labels,
    this.outcomeUnknown = false,
  });

  /// Whether the server may have applied the operation (writes and `runCommand` without `readOnly`).
  final bool outcomeUnknown;
}

/// The operation was cancelled through its `MongoCancelToken`.
final class MongoCancelledException extends MongoNativeException {
  /// Creates the cancellation of [operation].
  const MongoCancelledException({
    required super.operation,
    required super.message,
    super.labels,
    this.outcomeUnknown = false,
  });

  /// Whether the server may have applied the operation (writes and `runCommand` without `readOnly`).
  final bool outcomeUnknown;
}

/// The server refused a command (`MaxTimeMSExpired`, `Unauthorized`, …).
final class MongoCommandException extends MongoNativeException {
  /// Creates the refusal of [operation].
  const MongoCommandException({
    required super.operation,
    required super.message,
    super.code,
    super.codeName,
    super.labels,
  });
}

/// The write was applied on the primary but the write concern was not satisfied.
final class MongoWriteConcernException extends MongoNativeException {
  /// Creates the write concern failure of [operation].
  const MongoWriteConcernException({
    required super.operation,
    required super.message,
    super.code,
    super.codeName,
    super.labels,
  });
}

/// The connection failed or was cut.
final class MongoNetworkException extends MongoNativeException {
  /// Creates the network failure of [operation].
  const MongoNetworkException({required super.operation, required super.message, super.labels});
}

/// No suitable server (for writes, a primary) was found within `serverSelectionTimeout`.
final class MongoServerSelectionException extends MongoNativeException {
  /// Creates the server selection failure of [operation].
  const MongoServerSelectionException({required super.operation, required super.message, super.labels});
}

/// The credentials were refused.
final class MongoAuthenticationException extends MongoNativeException {
  /// Creates the authentication failure of [operation].
  const MongoAuthenticationException({required super.operation, required super.message, super.labels});
}

/// A request or reply that is not valid BSON.
final class MongoBsonException extends MongoNativeException {
  /// Creates the encoding failure of [operation].
  const MongoBsonException({required super.operation, required super.message});
}

/// A call on a closed client, cursor or change stream.
final class MongoClosedException extends MongoNativeException {
  /// Creates the failure of [operation] on something closed.
  const MongoClosedException({required super.operation, required super.message});
}

/// A bug or an unexpected state of the native side (a panic, a library that does not load).
final class MongoInternalException extends MongoNativeException {
  /// Creates the internal failure of [operation].
  const MongoInternalException({required super.operation, required super.message, super.code, super.codeName});
}
