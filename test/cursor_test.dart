import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

void main() {
  late MongoClient client;
  late MongoDatabase db;
  late MongoCollection items;

  Future<int> idleCursors(String coll) async {
    final cursor = await client.database('admin').runCommand({
      'aggregate': 1,
      'pipeline': [
        {
          r'$currentOp': {'idleCursors': true, 'allUsers': true},
        },
        {
          r'$match': {'type': 'idleCursor', 'ns': '${db.name}.$coll'},
        },
      ],
      'cursor': {},
    }, readOnly: true);
    return ((cursor['cursor']! as Map)['firstBatch']! as List).length;
  }

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('cursor'));
    items = db.collection('items');
    await items.insertMany([
      for (var i = 0; i < 25; i++) {'_id': i},
    ]);
  });

  tearDownAll(() async {
    if (!client.isClosed) {
      await db.runCommand({'dropDatabase': 1});
      await client.close();
    }
  });

  test('nextBatch returns batches of batchSize until an empty one, then the cursor is exhausted', () async {
    final cursor = await items.find({}, sort: {'_id': 1}, batchSize: 10);
    final sizes = <int>[];
    final seen = <Object?>[];
    while (true) {
      final batch = await cursor.nextBatch();
      if (batch.isEmpty) break;
      sizes.add(batch.length);
      seen.addAll(batch.map((d) => d['_id']));
    }
    expect(sizes, [10, 10, 5]);
    expect(seen, List.generate(25, (i) => i));
    expect(cursor.isExhausted, isTrue);
    expect(await cursor.nextBatch(), isEmpty);
    expect(await idleCursors('items'), 0);
  });

  test('a result that fits in the first batch needs no server cursor', () async {
    final cursor = await items.find({
      '_id': {r'$lt': 3},
    }, batchSize: 10);
    expect(cursor.isExhausted, isFalse);
    expect(await cursor.nextBatch(), hasLength(3));
    expect(cursor.isExhausted, isTrue);
  });

  test('toList refuses above max and closes the cursor', () async {
    final cursor = await items.find({}, batchSize: 5);
    await expectLater(cursor.toList(max: 7), throwsStateError);
    expect(cursor.isExhausted, isTrue);
    expect(await idleCursors('items'), 0);
  });

  test('close kills the server cursor', () async {
    final cursor = await items.find({}, batchSize: 5);
    expect(await cursor.nextBatch(), hasLength(5));
    expect(await idleCursors('items'), 1);
    await cursor.close();
    expect(await cursor.nextBatch(), isEmpty);
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (await idleCursors('items') != 0 && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    expect(await idleCursors('items'), 0);
  });

  test('aggregate cursors page the same way', () async {
    final cursor = await items.aggregate([
      {
        r'$sort': {'_id': -1},
      },
    ], batchSize: 20);
    expect(await cursor.nextBatch(), hasLength(20));
    expect(await cursor.nextBatch(), hasLength(5));
    expect(await cursor.nextBatch(), isEmpty);
  });

  test('a cursor of a closed client throws MongoClosedException', () async {
    final other = await connectTest();
    final cursor = await other.database(db.name).collection('items').find({}, batchSize: 5);
    await cursor.nextBatch();
    await other.close();
    await expectLater(cursor.nextBatch(), throwsA(isA<MongoClosedException>()));
  });
}
