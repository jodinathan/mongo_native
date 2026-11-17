import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

void main() {
  late MongoClient client;
  late MongoDatabase db;

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('crud'));
  });

  tearDownAll(() async {
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('insertOne generates an ObjectId and findOne reads it back', () async {
    final c = db.collection('people');
    final r = await c.insertOne({'name': 'Ana', 'age': 31});
    expect(r.insertedIds.single, isA<BsonObjectId>());
    final found = await c.findOne({'_id': r.insertedIds.single});
    expect(found, {'_id': r.insertedIds.single, 'name': 'Ana', 'age': 31});
    expect(await c.findOne({'name': 'nobody'}), isNull);
  });

  test('insertMany keeps the request order of ids', () async {
    final c = db.collection('many');
    final r = await c.insertMany([
      {'_id': 3},
      {'_id': 1},
      {'_id': 2},
    ]);
    expect(r.insertedIds, [3, 1, 2]);
    expect(await c.countDocuments({}), 3);
  });

  test('updateOne, updateMany and replaceOne report the server counts', () async {
    final c = db.collection('updates');
    await c.insertMany([
      {'_id': 1, 'g': 'a', 'n': 0},
      {'_id': 2, 'g': 'a', 'n': 0},
      {'_id': 3, 'g': 'b', 'n': 0},
    ]);
    final one = await c.updateOne(
      {'g': 'a'},
      {
        r'$inc': {'n': 1},
      },
    );
    expect((one.matched, one.modified, one.upsertedId), (1, 1, null));
    final many = await c.updateMany(
      {'g': 'a'},
      {
        r'$set': {'n': 5},
      },
    );
    expect((many.matched, many.modified), (2, 2));
    final same = await c.updateMany(
      {'g': 'a'},
      {
        r'$set': {'n': 5},
      },
    );
    expect((same.matched, same.modified), (2, 0));
    final up = await c.updateOne(
      {'_id': 9},
      {
        r'$set': {'g': 'z'},
      },
      upsert: true,
    );
    expect((up.matched, up.modified, up.upsertedId), (0, 0, 9));
    final replaced = await c.replaceOne({'_id': 3}, {'g': 'c', 'whole': true});
    expect((replaced.matched, replaced.modified), (1, 1));
    expect(await c.findOne({'_id': 3}), {'_id': 3, 'g': 'c', 'whole': true});
  });

  test(r'pipeline update with $$NOW', () async {
    final c = db.collection('pipeline');
    await c.insertOne({'_id': 1});
    await c.updateOne(
      {'_id': 1},
      [
        {
          r'$set': {'at': r'$$NOW'},
        },
      ],
    );
    final doc = await c.findOne({'_id': 1});
    expect(doc!['at'], isA<DateTime>());
    expect(() => c.updateOne({'_id': 1}, 'not an update'), throwsArgumentError);
  });

  test('deleteOne and deleteMany report the server counts', () async {
    final c = db.collection('deletes');
    await c.insertMany([
      for (var i = 0; i < 5; i++) {'_id': i, 'odd': i.isOdd},
    ]);
    expect((await c.deleteOne({'odd': true})).deleted, 1);
    expect((await c.deleteMany({'odd': false})).deleted, 3);
    expect(await c.countDocuments({}), 1);
  });

  test('findOneAndUpdate returns after or before, and upserts', () async {
    final c = db.collection('counters');
    final created = await c.findOneAndUpdate(
      {'_id': 'seq'},
      {
        r'$inc': {'n': 1},
      },
      upsert: true,
    );
    expect(created, {'_id': 'seq', 'n': 1});
    final before = await c.findOneAndUpdate(
      {'_id': 'seq'},
      {
        r'$inc': {'n': 1},
      },
      returnAfter: false,
      projection: {'n': 1, '_id': 0},
    );
    expect(before, {'n': 1});
    expect(
      await c.findOneAndUpdate(
        {'_id': 'missing'},
        {
          r'$inc': {'n': 1},
        },
      ),
      isNull,
    );
  });

  test('findOneAndDelete honors sort', () async {
    final c = db.collection('queue');
    await c.insertMany([
      for (var i = 0; i < 3; i++) {'_id': i, 'p': 3 - i},
    ]);
    final first = await c.findOneAndDelete({}, sort: {'p': 1});
    expect(first, {'_id': 2, 'p': 1});
    expect(await c.countDocuments({}), 2);
  });

  test('find with projection, sort, skip, limit and hint', () async {
    final c = db.collection('find');
    await c.createIndexes([
      const MongoIndexModel(keys: {'k': 1}, name: 'k_1'),
    ]);
    await c.insertMany([
      for (var i = 0; i < 10; i++) {'_id': i, 'k': i % 3, 'x': 'v$i'},
    ]);
    final cursor = await c.find(
      {
        'k': {r'$gte': 1},
      },
      projection: {'x': 1},
      sort: {'_id': -1},
      skip: 1,
      limit: 3,
      hint: 'k_1',
    );
    expect(await cursor.toList(), [
      {'_id': 7, 'x': 'v7'},
      {'_id': 5, 'x': 'v5'},
      {'_id': 4, 'x': 'v4'},
    ]);
  });

  test('aggregate and countDocuments', () async {
    final c = db.collection('agg');
    await c.insertMany([
      for (var i = 0; i < 6; i++) {'g': i % 2, 'v': i},
    ]);
    final cursor = await c.aggregate([
      {
        r'$group': {
          '_id': r'$g',
          'total': {r'$sum': r'$v'},
        },
      },
      {
        r'$sort': {'_id': 1},
      },
    ]);
    expect(await cursor.toList(), [
      {'_id': 0, 'total': 6},
      {'_id': 1, 'total': 9},
    ]);
    expect(await c.countDocuments({'g': 1}, maxTime: const Duration(seconds: 5)), 3);
  });

  test('createIndexes, listIndexes and dropIndex', () async {
    final c = db.collection('indexes');
    final names = await c.createIndexes([
      const MongoIndexModel(keys: {'email': 1}, unique: true, name: 'email_unique'),
      const MongoIndexModel(keys: {'at': 1}, expireAfter: Duration(hours: 1)),
      const MongoIndexModel(
        keys: {'loc': '2dsphere'},
        partialFilter: {
          'loc': {r'$exists': true},
        },
      ),
    ]);
    expect(names, ['email_unique', 'at_1', 'loc_2dsphere']);
    final listed = await c.listIndexes();
    final byName = {for (final i in listed) i['name']: i};
    expect(byName.keys, containsAll(['_id_', 'email_unique', 'at_1', 'loc_2dsphere']));
    expect(byName['email_unique']!['unique'], isTrue);
    expect(byName['at_1']!['expireAfterSeconds'], 3600);
    await c.dropIndex('email_unique');
    expect((await c.listIndexes()).map((i) => i['name']), isNot(contains('email_unique')));
  });

  test('listCollectionNames and dropCollection', () async {
    await db.collection('temp').insertOne({'a': 1});
    expect(await db.listCollectionNames(), contains('temp'));
    await db.dropCollection('temp');
    expect(await db.listCollectionNames(), isNot(contains('temp')));
    await db.dropCollection('never-existed');
  });

  test('runCommand returns the server answer', () async {
    final hello = await db.runCommand({'hello': 1}, readOnly: true);
    expect(hello['setName'], 'mn-rs');
    expect(hello['isWritablePrimary'], isTrue);
  });

  test('bulkWrite runs each model kind across collections with the server counts', () async {
    final a = db.collection('bulk_a');
    final b = db.collection('bulk_b');
    await a.insertMany([
      {'_id': 1, 'v': 0},
      {'_id': 2, 'v': 0},
      {'_id': 3, 'v': 0},
    ]);
    await b.insertMany([
      {'_id': 1, 'v': 0},
      {'_id': 2, 'v': 0},
    ]);
    final r = await client.bulkWrite([
      MongoInsertOne(db.name, 'bulk_a', {'_id': 4, 'v': 0}),
      MongoUpdateOne(
        db.name,
        'bulk_a',
        {'_id': 1},
        {
          r'$set': {'v': 1},
        },
      ),
      MongoUpdateMany(db.name, 'bulk_b', {}, {
        r'$set': {'v': 2},
      }),
      MongoReplaceOne(db.name, 'bulk_a', {'_id': 2}, {'v': 9}),
      MongoUpdateOne(
        db.name,
        'bulk_b',
        {'_id': 7},
        {
          r'$set': {'v': 7},
        },
        upsert: true,
      ),
      MongoDeleteOne(db.name, 'bulk_a', {'_id': 3}),
      MongoDeleteMany(db.name, 'bulk_b', {
        'v': {r'$ne': 7},
      }),
    ]);
    expect(r.inserted, 1);
    expect(r.matched, 1 + 2 + 1);
    expect(r.modified, 1 + 2 + 1);
    expect(r.upserted, 1);
    expect(r.deleted, 1 + 2);
    expect(r.upsertedIds, {4: 7});
    expect(await a.countDocuments({}), 3);
    expect(await b.findOne({}), {'_id': 7, 'v': 7});
  });

  test('a write concern of one member and a local read concern are accepted per call', () async {
    final c = db.collection('concerns');
    await c.insertOne({'_id': 1}, writeConcern: MongoWriteConcern.acknowledged);
    expect(await c.findOne({'_id': 1}, readConcern: MongoReadConcern.local), {'_id': 1});
  });
}
