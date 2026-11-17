import 'dart:math';
import 'dart:typed_data';

bool _sameBytes(List<int> a, List<int> b) {
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

/// BSON binary data with a subtype other than 0 (generic data decodes as a plain [Uint8List]).
final class BsonBinary {
  /// Creates binary [bytes] of [subtype] (0 to 255).
  BsonBinary(this.subtype, this.bytes) {
    if (subtype < 0 || subtype > 255) {
      throw ArgumentError.value(subtype, 'subtype', 'must be between 0 and 255');
    }
  }

  /// The BSON binary subtype, e.g. 4 for a UUID.
  final int subtype;

  /// The payload.
  final Uint8List bytes;

  @override
  bool operator ==(Object other) => other is BsonBinary && other.subtype == subtype && _sameBytes(other.bytes, bytes);

  @override
  int get hashCode => Object.hash(subtype, Object.hashAll(bytes));

  @override
  String toString() => 'BsonBinary(subtype: $subtype, ${bytes.length} bytes)';
}

/// A BSON timestamp: the server's logical clock (`clusterTime`, oplog positions).
final class BsonTimestamp implements Comparable<BsonTimestamp> {
  /// Creates the timestamp of [seconds] since the epoch and the [increment] within that second.
  const BsonTimestamp(this.seconds, this.increment);

  /// Seconds since the Unix epoch (unsigned 32 bits).
  final int seconds;

  /// Ordinal of the operation within [seconds] (unsigned 32 bits).
  final int increment;

  @override
  int compareTo(BsonTimestamp other) {
    final bySeconds = seconds.compareTo(other.seconds);
    return bySeconds != 0 ? bySeconds : increment.compareTo(other.increment);
  }

  @override
  bool operator ==(Object other) => other is BsonTimestamp && other.seconds == seconds && other.increment == increment;

  @override
  int get hashCode => Object.hash(seconds, increment);

  @override
  String toString() => 'BsonTimestamp($seconds, $increment)';
}

/// A 12-byte BSON ObjectId.
final class BsonObjectId implements Comparable<BsonObjectId> {
  /// Wraps the 12 [bytes] of an ObjectId.
  BsonObjectId(Uint8List bytes) : bytes = Uint8List.fromList(bytes) {
    if (bytes.length != 12) {
      throw ArgumentError.value(bytes.length, 'bytes', 'an ObjectId has 12 bytes');
    }
  }

  /// Parses the 24 hexadecimal characters of an ObjectId.
  factory BsonObjectId.fromHex(String hex) {
    if (hex.length != 24 || !RegExp(r'^[0-9a-fA-F]{24}$').hasMatch(hex)) {
      throw FormatException('an ObjectId is 24 hexadecimal characters', hex);
    }
    final out = Uint8List(12);
    for (var i = 0; i < 12; i++) {
      out[i] = int.parse(hex.substring(i * 2, i * 2 + 2), radix: 16);
    }
    return BsonObjectId(out);
  }

  /// A new ObjectId: the current second, a random value fixed per process and an increasing counter.
  factory BsonObjectId.generate() {
    final out = Uint8List(12);
    final seconds = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    out.buffer.asByteData().setUint32(0, seconds & 0xFFFFFFFF);
    out.setRange(4, 9, _processUnique);
    final counter = _counter = (_counter + 1) & 0xFFFFFF;
    out[9] = (counter >> 16) & 0xFF;
    out[10] = (counter >> 8) & 0xFF;
    out[11] = counter & 0xFF;
    return BsonObjectId(out);
  }

  static final _random = Random.secure();
  static final _processUnique = List<int>.generate(5, (_) => _random.nextInt(256));
  static int _counter = _random.nextInt(0xFFFFFF);

  /// The 12 bytes.
  final Uint8List bytes;

  /// The creation time embedded in the first four bytes.
  DateTime get timestamp =>
      DateTime.fromMillisecondsSinceEpoch(bytes.buffer.asByteData().getUint32(0) * 1000, isUtc: true);

  /// The 24 lowercase hexadecimal characters.
  String toHexString() => bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

  @override
  int compareTo(BsonObjectId other) {
    for (var i = 0; i < 12; i++) {
      final c = bytes[i].compareTo(other.bytes[i]);
      if (c != 0) return c;
    }
    return 0;
  }

  @override
  bool operator ==(Object other) => other is BsonObjectId && _sameBytes(other.bytes, bytes);

  @override
  int get hashCode => Object.hashAll(bytes);

  @override
  String toString() => 'BsonObjectId(${toHexString()})';
}

/// A BSON decimal128, kept as its 16 raw bytes (IEEE 754-2008, little endian).
final class BsonDecimal128 {
  /// Wraps the 16 raw [bytes].
  BsonDecimal128(Uint8List bytes) : bytes = Uint8List.fromList(bytes) {
    if (bytes.length != 16) {
      throw ArgumentError.value(bytes.length, 'bytes', 'a decimal128 has 16 bytes');
    }
  }

  /// The 16 raw bytes.
  final Uint8List bytes;

  @override
  bool operator ==(Object other) => other is BsonDecimal128 && _sameBytes(other.bytes, bytes);

  @override
  int get hashCode => Object.hashAll(bytes);

  @override
  String toString() => 'BsonDecimal128(${bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join()})';
}

/// A BSON regular expression, as the server stores it.
final class BsonRegex {
  /// Creates the expression [pattern] with the server [options] (`i`, `m`, `s`, `x`, …).
  const BsonRegex(this.pattern, [this.options = '']);

  /// The pattern.
  final String pattern;

  /// The option letters.
  final String options;

  @override
  bool operator ==(Object other) => other is BsonRegex && other.pattern == pattern && other.options == options;

  @override
  int get hashCode => Object.hash(pattern, options);

  @override
  String toString() => 'BsonRegex(/$pattern/$options)';
}

/// The BSON value that sorts before every other.
final class BsonMinKey {
  /// The min key.
  const BsonMinKey();

  @override
  bool operator ==(Object other) => other is BsonMinKey;

  @override
  int get hashCode => 0x7F;

  @override
  String toString() => 'BsonMinKey';
}

/// The BSON value that sorts after every other.
final class BsonMaxKey {
  /// The max key.
  const BsonMaxKey();

  @override
  bool operator ==(Object other) => other is BsonMaxKey;

  @override
  int get hashCode => 0xFF;

  @override
  String toString() => 'BsonMaxKey';
}
