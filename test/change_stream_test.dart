import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

Future<List<MongoChangeEvent>> _collect(
  MongoChangeStream stream,
  int count, {
  Duration within = const Duration(seconds: 20),
}) async {
  final out = <MongoChangeEvent>[];
  final deadline = DateTime.now().add(within);
  while (out.length < count) {
    if (DateTime.now().isAfter(deadline)) {
      throw StateError('only ${out.length} of $count events within $within');
    }
    out.addAll((await stream.next()).events);
  }
  return out;
}

void main() {
  late MongoClient client;
  late MongoDatabase db;

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('stream'));
  });

  tearDownAll(() async {
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('MongoCollection.watch delivers insert, update, replace and delete in order', () async {
    final c = db.collection('watched');
    await c.insertOne({'_id': 0});
    final stream = await c.watch();
    expect(stream.resumeToken, isNotNull);
    await c.insertOne({'_id': 1});
    await c.updateOne(
      {'_id': 1},
      {
        r'$set': {'v': 1},
      },
    );
    await c.replaceOne({'_id': 1}, {'v': 2});
    await c.deleteOne({'_id': 1});
    final events = await _collect(stream, 4);
    expect(events.map((e) => e.operationType), [
      MongoOperationType.insert,
      MongoOperationType.update,
      MongoOperationType.replace,
      MongoOperationType.delete,
    ]);
    expect(events.every((e) => e.documentKey!['_id'] == 1), isTrue);
    expect(events.every((e) => e.database == db.name && e.collection == 'watched'), isTrue);
    for (var i = 1; i < events.length; i++) {
      expect(events[i].clusterTime.compareTo(events[i - 1].clusterTime), greaterThan(0));
    }
    expect(stream.resumeToken, events.last.resumeToken);
    await stream.close();
  });

  test('an empty batch comes after maxAwait and still carries a resume token', () async {
    final stream = await db.collection('quiet').watch(maxAwait: const Duration(milliseconds: 300));
    final watch = Stopwatch()..start();
    final batch = await stream.next();
    expect(batch.events, isEmpty);
    expect(batch.resumeToken, isNotNull);
    expect(watch.elapsed, greaterThanOrEqualTo(const Duration(milliseconds: 250)));
    await stream.close();
  });

  test('MongoDatabase.watch sees every collection; a pipeline filters', () async {
    final stream = await db.watch(
      pipeline: [
        {
          r'$match': {
            'ns.coll': {
              r'$in': ['db_a', 'db_b'],
            },
          },
        },
        {
          r'$project': {'documentKey': 1, 'ns': 1, 'clusterTime': 1, 'operationType': 1},
        },
      ],
    );
    await db.collection('db_a').insertOne({'_id': 'a'});
    await db.collection('other').insertOne({'_id': 'x'});
    await db.collection('db_b').insertOne({'_id': 'b'});
    final events = await _collect(stream, 2);
    expect(events.map((e) => (e.collection, e.documentKey!['_id'])), [('db_a', 'a'), ('db_b', 'b')]);
    expect(events.first.raw.containsKey('fullDocument'), isFalse);
    await stream.close();
  });

  test('resumeAfter and startAtOperationTime pick up where a closed stream stopped', () async {
    final c = db.collection('resume');
    final stream = await c.watch();
    await c.insertOne({'_id': 'before'});
    final first = await _collect(stream, 1);
    final token = stream.resumeToken!;
    await stream.close();
    await c.insertMany([
      for (var i = 0; i < 5; i++) {'_id': 'after-$i'},
    ]);
    final resumed = await c.watch(resumeAfter: token);
    final events = await _collect(resumed, 5);
    expect(events.map((e) => e.documentKey!['_id']), [for (var i = 0; i < 5; i++) 'after-$i']);
    await resumed.close();
    final fromTime = await c.watch(startAtOperationTime: first.single.clusterTime);
    final replay = await _collect(fromTime, 6);
    expect(replay.first.documentKey!['_id'], 'before');
    await fromTime.close();
  });

  test(
    'dropping the watched collection delivers drop and invalidate, then next throws and the stream is closed',
    () async {
      final c = db.collection('doomed');
      await c.insertOne({'_id': 1});
      final stream = await c.watch();
      await db.dropCollection('doomed');
      final events = await _collect(stream, 2);
      expect(events.map((e) => e.operationType), [MongoOperationType.drop, MongoOperationType.invalidate]);
      await expectLater(stream.next(), throwsA(isA<MongoClosedException>()));
      await expectLater(stream.next(), throwsA(isA<MongoClosedException>()));
    },
  );

  test('ChangeStreamHistoryLost (286) on getMore throws MongoCommandException 286 and closes the stream', () async {
    const appName = 'mn-stream-286';
    final victim = await connectTest(appName: appName);
    final port = await primaryPort(client);
    try {
      final stream = await victim
          .database(db.name)
          .collection('lost')
          .watch(maxAwait: const Duration(milliseconds: 200));
      await failCommand(port, {
        'failCommands': ['getMore'],
        'errorCode': 286,
        'appName': appName,
      });
      await expectLater(() async {
        for (var i = 0; i < 50; i++) {
          await stream.next();
        }
      }(), throwsA(isA<MongoCommandException>().having((e) => e.code, 'code', 286)));
      await expectLater(stream.next(), throwsA(isA<MongoClosedException>()));
    } finally {
      await clearFailCommand(port);
      await victim.close();
    }
  });
}
