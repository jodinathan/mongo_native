import 'dart:convert';
import 'dart:typed_data';

import 'errors.dart';
import 'types.dart';

/// Encodes and decodes BSON documents with plain Dart values.
///
/// | BSON | Dart |
/// |---|---|
/// | double | `double` |
/// | string | `String` |
/// | document | `Map<String, Object?>` (in arrival order) |
/// | array | `List<Object?>` |
/// | binary subtype 0 | `Uint8List` |
/// | binary, other subtypes | [BsonBinary] |
/// | ObjectId | [BsonObjectId] |
/// | boolean | `bool` |
/// | UTC datetime | `DateTime` (UTC) |
/// | null | `null` |
/// | regex | [BsonRegex] |
/// | int32 / int64 | `int` (encoded as int32 when it fits) |
/// | timestamp | [BsonTimestamp] |
/// | decimal128 | [BsonDecimal128] |
/// | min key / max key | [BsonMinKey] / [BsonMaxKey] |
abstract final class BsonCodec {
  /// The deepest nesting of documents and arrays accepted, the top document included.
  static const maxDepth = 100;

  /// Encodes [document]; throws [ArgumentError] naming the key path of a value without a BSON type,
  /// of a key with a zero byte or of nesting deeper than [maxDepth].
  static Uint8List encode(Map<String, Object?> document) {
    final w = _Writer();
    _writeDocument(w, document, r'$', 1);
    return w.takeBytes();
  }

  /// Decodes one document that fills all of [bytes]; throws [BsonFormatException] with the position
  /// of the first problem.
  static Map<String, Object?> decode(Uint8List bytes) {
    final r = _Reader(bytes);
    if (bytes.length < 5) {
      throw BsonFormatException('a document needs at least 5 bytes, got ${bytes.length}', 0);
    }
    final declared = r.int32At(0);
    if (declared != bytes.length) {
      throw BsonFormatException(
        declared < bytes.length
            ? 'the document declares $declared bytes and ${bytes.length - declared} bytes follow it'
            : 'the document declares $declared bytes but only ${bytes.length} are present',
        0,
      );
    }
    return r.document(0, bytes.length, 1);
  }
}

void _writeDocument(_Writer w, Map<Object?, Object?> document, String path, int depth) {
  if (depth > BsonCodec.maxDepth) {
    throw ArgumentError('nesting deeper than ${BsonCodec.maxDepth} levels at $path');
  }
  final start = w.length;
  w.int32(0);
  for (final entry in document.entries) {
    final key = entry.key;
    if (key is! String) {
      throw ArgumentError('the key ${Error.safeToString(key)} at $path is not a String');
    }
    _writeElement(w, key, entry.value, '$path.$key', depth);
  }
  w.byte(0);
  w.patchInt32(start, w.length - start);
}

void _writeArray(_Writer w, List<Object?> items, String path, int depth) {
  if (depth > BsonCodec.maxDepth) {
    throw ArgumentError('nesting deeper than ${BsonCodec.maxDepth} levels at $path');
  }
  final start = w.length;
  w.int32(0);
  for (var i = 0; i < items.length; i++) {
    _writeElement(w, '$i', items[i], '$path[$i]', depth);
  }
  w.byte(0);
  w.patchInt32(start, w.length - start);
}

void _writeElement(_Writer w, String key, Object? value, String path, int depth) {
  final typeAt = w.length;
  w.byte(0);
  w.cstring(key, path);
  switch (value) {
    case null:
      w.patchByte(typeAt, 0x0A);
    case final bool b:
      w.patchByte(typeAt, 0x08);
      w.byte(b ? 1 : 0);
    case final int i:
      if (i >= -0x80000000 && i <= 0x7FFFFFFF) {
        w.patchByte(typeAt, 0x10);
        w.int32(i);
      } else {
        w.patchByte(typeAt, 0x12);
        w.int64(i);
      }
    case final double d:
      w.patchByte(typeAt, 0x01);
      w.float64(d);
    case final String s:
      w.patchByte(typeAt, 0x02);
      final bytes = utf8.encode(s);
      w.int32(bytes.length + 1);
      w.bytes(bytes);
      w.byte(0);
    case final DateTime t:
      w.patchByte(typeAt, 0x09);
      w.int64(t.toUtc().millisecondsSinceEpoch);
    case final Uint8List bytes:
      w.patchByte(typeAt, 0x05);
      w.int32(bytes.length);
      w.byte(0);
      w.bytes(bytes);
    case final BsonBinary b:
      w.patchByte(typeAt, 0x05);
      w.int32(b.bytes.length);
      w.byte(b.subtype);
      w.bytes(b.bytes);
    case final BsonObjectId o:
      w.patchByte(typeAt, 0x07);
      w.bytes(o.bytes);
    case final BsonRegex r:
      w.patchByte(typeAt, 0x0B);
      w.cstring(r.pattern, '$path (pattern)');
      w.cstring(r.options, '$path (options)');
    case final BsonTimestamp t:
      if (t.seconds < 0 || t.seconds > 0xFFFFFFFF || t.increment < 0 || t.increment > 0xFFFFFFFF) {
        throw ArgumentError('the timestamp at $path does not fit in two unsigned 32-bit numbers');
      }
      w.patchByte(typeAt, 0x11);
      w.uint32(t.increment);
      w.uint32(t.seconds);
    case final BsonDecimal128 d:
      w.patchByte(typeAt, 0x13);
      w.bytes(d.bytes);
    case BsonMinKey():
      w.patchByte(typeAt, 0xFF);
    case BsonMaxKey():
      w.patchByte(typeAt, 0x7F);
    case final Map<Object?, Object?> m:
      w.patchByte(typeAt, 0x03);
      _writeDocument(w, m, path, depth + 1);
    case final List<Object?> l:
      w.patchByte(typeAt, 0x04);
      _writeArray(w, l, path, depth + 1);
    default:
      throw ArgumentError('the value at $path has no BSON type: ${value.runtimeType}');
  }
}

final class _Writer {
  Uint8List _buf = Uint8List(256);
  late ByteData _data = ByteData.sublistView(_buf);
  int length = 0;

  void _ensure(int extra) {
    if (length + extra <= _buf.length) return;
    var size = _buf.length * 2;
    while (size < length + extra) {
      size *= 2;
    }
    final next = Uint8List(size)..setRange(0, length, _buf);
    _buf = next;
    _data = ByteData.sublistView(next);
  }

  void byte(int v) {
    _ensure(1);
    _buf[length++] = v;
  }

  void patchByte(int at, int v) => _buf[at] = v;

  void int32(int v) {
    _ensure(4);
    _data.setInt32(length, v, Endian.little);
    length += 4;
  }

  void uint32(int v) {
    _ensure(4);
    _data.setUint32(length, v, Endian.little);
    length += 4;
  }

  void patchInt32(int at, int v) => _data.setInt32(at, v, Endian.little);

  void int64(int v) {
    _ensure(8);
    _data.setInt64(length, v, Endian.little);
    length += 8;
  }

  void float64(double v) {
    _ensure(8);
    _data.setFloat64(length, v, Endian.little);
    length += 8;
  }

  void bytes(List<int> v) {
    _ensure(v.length);
    _buf.setRange(length, length + v.length, v);
    length += v.length;
  }

  void cstring(String s, String path) {
    final encoded = utf8.encode(s);
    if (encoded.contains(0)) {
      throw ArgumentError('the key or string at $path contains a zero byte, which BSON cannot hold here');
    }
    bytes(encoded);
    byte(0);
  }

  Uint8List takeBytes() => Uint8List.sublistView(_buf, 0, length);
}

final class _Reader {
  _Reader(this.buf) : data = ByteData.sublistView(buf);

  final Uint8List buf;
  final ByteData data;
  static const _utf8 = Utf8Decoder(allowMalformed: false);

  int int32At(int at) => data.getInt32(at, Endian.little);

  void _need(int at, int size, int end, String what) {
    if (at < 0 || at + size > end) {
      throw BsonFormatException('$what needs $size bytes but only ${end - at} remain', at);
    }
  }

  Map<String, Object?> document(int start, int parentEnd, int depth) {
    if (depth > BsonCodec.maxDepth) {
      throw BsonFormatException('nesting deeper than ${BsonCodec.maxDepth} levels', start);
    }
    _need(start, 4, parentEnd, 'a document length');
    final size = int32At(start);
    if (size < 5) {
      throw BsonFormatException('a document declares $size bytes; the minimum is 5', start);
    }
    final end = start + size;
    if (end > parentEnd) {
      throw BsonFormatException(
        'a document declares $size bytes but its container leaves only ${parentEnd - start}',
        start,
      );
    }
    if (buf[end - 1] != 0) {
      throw BsonFormatException('the document does not end with a zero byte', end - 1);
    }
    final out = <String, Object?>{};
    var at = start + 4;
    while (at < end - 1) {
      final type = buf[at];
      final typeAt = at;
      at++;
      final (key, afterKey) = _cstring(at, end - 1);
      at = afterKey;
      final (value, next) = _value(type, typeAt, at, end - 1, depth);
      out[key] = value;
      at = next;
    }
    if (at != end - 1) {
      throw BsonFormatException('an element runs past the end of its document', at);
    }
    return out;
  }

  List<Object?> array(int start, int parentEnd, int depth) => document(start, parentEnd, depth).values.toList();

  (String, int) _cstring(int at, int end) {
    var i = at;
    while (i < end && buf[i] != 0) {
      i++;
    }
    if (i >= end) {
      throw BsonFormatException('a key or C string has no terminating zero byte', at);
    }
    return (_decodeUtf8(at, i), i + 1);
  }

  String _decodeUtf8(int from, int to) {
    try {
      return _utf8.convert(buf, from, to);
    } on FormatException catch (e) {
      throw BsonFormatException('invalid UTF-8: ${e.message}', e.offset ?? from);
    }
  }

  (Object?, int) _value(int type, int typeAt, int at, int end, int depth) {
    switch (type) {
      case 0x01:
        _need(at, 8, end, 'a double');
        return (data.getFloat64(at, Endian.little), at + 8);
      case 0x02:
        _need(at, 4, end, 'a string length');
        final size = int32At(at);
        if (size < 1) {
          throw BsonFormatException('a string declares $size bytes; the minimum is 1', at);
        }
        _need(at + 4, size, end, 'a string');
        final last = at + 4 + size - 1;
        if (buf[last] != 0) {
          throw BsonFormatException('a string does not end with a zero byte', last);
        }
        return (_decodeUtf8(at + 4, last), last + 1);
      case 0x03:
        final doc = document(at, end, depth + 1);
        return (doc, at + int32At(at));
      case 0x04:
        final list = array(at, end, depth + 1);
        return (list, at + int32At(at));
      case 0x05:
        _need(at, 5, end, 'a binary header');
        final size = int32At(at);
        if (size < 0) {
          throw BsonFormatException('a binary declares $size bytes', at);
        }
        final subtype = buf[at + 4];
        _need(at + 5, size, end, 'a binary payload');
        final bytes = Uint8List.fromList(Uint8List.sublistView(buf, at + 5, at + 5 + size));
        return (subtype == 0 ? bytes : BsonBinary(subtype, bytes), at + 5 + size);
      case 0x07:
        _need(at, 12, end, 'an ObjectId');
        return (BsonObjectId(Uint8List.sublistView(buf, at, at + 12)), at + 12);
      case 0x08:
        _need(at, 1, end, 'a boolean');
        final b = buf[at];
        if (b > 1) {
          throw BsonFormatException('a boolean holds $b; only 0 and 1 are valid', at);
        }
        return (b == 1, at + 1);
      case 0x09:
        _need(at, 8, end, 'a datetime');
        final ms = data.getInt64(at, Endian.little);
        try {
          return (DateTime.fromMillisecondsSinceEpoch(ms, isUtc: true), at + 8);
        } on ArgumentError {
          throw BsonFormatException('the datetime $ms ms is outside the range of DateTime', at);
        }
      case 0x0A:
        return (null, at);
      case 0x0B:
        final (pattern, afterPattern) = _cstring(at, end);
        final (options, afterOptions) = _cstring(afterPattern, end);
        return (BsonRegex(pattern, options), afterOptions);
      case 0x10:
        _need(at, 4, end, 'an int32');
        return (int32At(at), at + 4);
      case 0x11:
        _need(at, 8, end, 'a timestamp');
        final increment = data.getUint32(at, Endian.little);
        final seconds = data.getUint32(at + 4, Endian.little);
        return (BsonTimestamp(seconds, increment), at + 8);
      case 0x12:
        _need(at, 8, end, 'an int64');
        return (data.getInt64(at, Endian.little), at + 8);
      case 0x13:
        _need(at, 16, end, 'a decimal128');
        return (BsonDecimal128(Uint8List.sublistView(buf, at, at + 16)), at + 16);
      case 0xFF:
        return (const BsonMinKey(), at);
      case 0x7F:
        return (const BsonMaxKey(), at);
      case 0x06 || 0x0C || 0x0D || 0x0E || 0x0F:
        throw BsonFormatException(
          'type 0x${type.toRadixString(16).padLeft(2, '0')} is deprecated and not supported',
          typeAt,
        );
      default:
        throw BsonFormatException('unknown type 0x${type.toRadixString(16).padLeft(2, '0')}', typeAt);
    }
  }
}
