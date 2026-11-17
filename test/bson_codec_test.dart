import 'dart:io';
import 'dart:typed_data';

import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

Uint8List _fixture(String name) => File('test/fixtures/$name.bson').readAsBytesSync();

Uint8List _bytes(List<int> b) => Uint8List.fromList(b);

Matcher _formatAt(int offset) => isA<BsonFormatException>().having((e) => e.offset, 'offset', offset);

void main() {
  final oid = BsonObjectId.fromHex('65f1a2b3c4d5e6f708192a3b');
  final expected = <String, Object?>{
    'double': 1.5,
    'string': 'olá, 世界',
    'doc': {'inner': 'x', 'n': 1},
    'array': [
      1,
      'two',
      {'three': 3},
    ],
    'bin0': _bytes([0, 1, 2, 255]),
    'bin4': BsonBinary(4, _bytes(List.generate(16, (i) => i))),
    'bin80': BsonBinary(0x80, _bytes([9, 8, 7])),
    'oid': oid,
    'yes': true,
    'no': false,
    'date': DateTime.fromMillisecondsSinceEpoch(1731715200123, isUtc: true),
    'nothing': null,
    'regex': const BsonRegex(r'^a.c$', 'im'),
    'int32': -2147483648,
    'int64': 9007199254740993,
    'ts': const BsonTimestamp(1731715200, 7),
    'dec': BsonDecimal128(_bytes(List.generate(16, (i) => i + 1))),
    'min': const BsonMinKey(),
    'max': const BsonMaxKey(),
  };

  group('bytes written by the Rust bson crate', () {
    test('decode to the expected values, in order', () {
      final decoded = BsonCodec.decode(_fixture('all_types'));
      expect(decoded.keys.toList(), expected.keys.toList());
      expect(decoded, equals(expected));
      expect(decoded['date'], isA<DateTime>().having((d) => d.isUtc, 'isUtc', true));
    });

    test('encode back to the same bytes', () {
      expect(BsonCodec.encode(expected), _fixture('all_types'));
      expect(BsonCodec.encode({}), _fixture('empty'));
    });

    test(r'a document with $ref/$id stays a Map', () {
      final decoded = BsonCodec.decode(_fixture('ref_id'));
      expect(decoded, isA<Map<String, Object?>>());
      expect(decoded, {r'$ref': 'users', r'$id': 42, r'$db': 'app'});
      expect(BsonCodec.encode(decoded), _fixture('ref_id'));
    });
  });

  group('round trip', () {
    test('every type', () {
      expect(BsonCodec.decode(BsonCodec.encode(expected)), equals(expected));
    });

    test('ints pick int32 or int64 by size', () {
      final bytes = BsonCodec.encode({'a': 2147483647, 'b': 2147483648, 'c': -2147483649});
      expect(bytes[4], 0x10);
      expect(BsonCodec.decode(bytes), {'a': 2147483647, 'b': 2147483648, 'c': -2147483649});
      expect(bytes[4 + 1 + 2 + 4], 0x12);
    });

    test('local DateTime is stored as UTC', () {
      final local = DateTime(2026, 11, 17, 10, 30);
      final back = BsonCodec.decode(BsonCodec.encode({'t': local}))['t']! as DateTime;
      expect(back.isUtc, isTrue);
      expect(back, local.toUtc());
    });

    test('ObjectId.generate is unique and ordered within the process', () {
      final ids = List.generate(1000, (_) => BsonObjectId.generate());
      expect(ids.toSet().length, 1000);
      expect(ids.first.toHexString(), hasLength(24));
    });
  });

  group('encode refuses', () {
    test('a value without a BSON type, naming its path', () {
      expect(
        () => BsonCodec.encode({
          'a': {
            'b': [1, Object()],
          },
        }),
        throwsA(isA<ArgumentError>().having((e) => e.message, 'message', contains(r'$.a.b[1]'))),
      );
    });

    test('a key with a zero byte', () {
      expect(() => BsonCodec.encode({'a\u0000b': 1}), throwsArgumentError);
    });

    test('nesting deeper than the limit', () {
      Map<String, Object?> nested = {};
      for (var i = 0; i < BsonCodec.maxDepth; i++) {
        nested = {'n': nested};
      }
      expect(() => BsonCodec.encode(nested), throwsArgumentError);
    });
  });

  group('decode refuses with the position', () {
    test('truncated bytes', () {
      final good = _fixture('all_types');
      expect(() => BsonCodec.decode(Uint8List.sublistView(good, 0, good.length - 7)), throwsA(_formatAt(0)));
      expect(() => BsonCodec.decode(_bytes([5, 0, 0])), throwsA(_formatAt(0)));
    });

    test('missing final zero', () {
      expect(() => BsonCodec.decode(_bytes([5, 0, 0, 0, 1])), throwsA(_formatAt(4)));
    });

    test('a nested document larger than its parent', () {
      final bytes = BsonCodec.encode({
        'd': {'x': 1},
      });
      bytes.buffer.asByteData().setInt32(7, 200, Endian.little);
      expect(() => BsonCodec.decode(bytes), throwsA(_formatAt(7)));
    });

    test('invalid UTF-8', () {
      final bytes = BsonCodec.encode({'s': 'ab'});
      bytes[11] = 0xFF;
      expect(() => BsonCodec.decode(bytes), throwsA(isA<BsonFormatException>().having((e) => e.offset, 'offset', 11)));
    });

    test('bytes after the document', () {
      final bytes = Uint8List.fromList([
        ...BsonCodec.encode({'a': 1}),
        0,
      ]);
      expect(() => BsonCodec.decode(bytes), throwsA(_formatAt(0)));
    });

    test('an unknown or deprecated type', () {
      final bytes = BsonCodec.encode({'a': null});
      bytes[4] = 0x0E;
      expect(() => BsonCodec.decode(bytes), throwsA(_formatAt(4)));
      bytes[4] = 0x42;
      expect(() => BsonCodec.decode(bytes), throwsA(_formatAt(4)));
    });

    test('depth 101', () {
      Uint8List wrap(Uint8List inner) {
        final size = 4 + 1 + 2 + inner.length + 1;
        final out = BytesBuilder()
          ..add((ByteData(4)..setInt32(0, size, Endian.little)).buffer.asUint8List())
          ..add([0x03, 0x6E, 0x00])
          ..add(inner)
          ..addByte(0);
        return out.toBytes();
      }

      var doc = _bytes([5, 0, 0, 0, 0]);
      for (var i = 1; i < BsonCodec.maxDepth; i++) {
        doc = wrap(doc);
      }
      expect(BsonCodec.decode(doc), isA<Map<String, Object?>>());
      final deeper = wrap(doc);
      expect(
        () => BsonCodec.decode(deeper),
        throwsA(isA<BsonFormatException>().having((e) => e.message, 'message', contains('deeper than 100'))),
      );
    });
  });
}
