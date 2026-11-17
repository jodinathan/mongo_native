/// Bytes that are not a valid BSON document.
final class BsonFormatException implements Exception {
  /// Creates the failure [message] found at byte [offset].
  const BsonFormatException(this.message, this.offset);

  /// What is wrong with the bytes.
  final String message;

  /// The position of the problem, counted from the first byte of the buffer.
  final int offset;

  @override
  String toString() => 'BsonFormatException at byte $offset: $message';
}
