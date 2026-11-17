import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

void main() {
  late MongoClient client;
  late MongoDatabase db;
  late int original;

  setUpAll(() async {
    client = await connectTest();
    await waitHealthy(client);
    original = await primaryPort(client);
    db = client.database(uniqueDb('streamfail'));
  });

  tearDownAll(() async {
    await waitPrimaryBack(client, original);
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test(
    'a step-down in the middle of 1000 inserts: the stream delivers each documentKey once, in clusterTime order',
    () async {
      final coll = db.collection('events');
      final stream = await coll.watch();
      final writer = await connectTest();
      final admin = await connectTest();
      final inserted = <int>[];
      final writes = () async {
        final target = writer.database(db.name).collection('events');
        for (var i = 0; i < 1000; i++) {
          if (i == 500) await stepDown(admin);
          try {
            await target.insertOne({'_id': i});
            inserted.add(i);
          } on MongoNativeException {
            continue;
          }
        }
      }();

      final seen = <Object?>[];
      final times = <BsonTimestamp>[];
      var writesDone = false;
      final done = writes.whenComplete(() => writesDone = true);
      final deadline = DateTime.now().add(const Duration(minutes: 3));
      while (!(writesDone && seen.length >= inserted.length)) {
        if (DateTime.now().isAfter(deadline)) fail('the stream delivered ${seen.length} of ${inserted.length} events');
        for (final e in (await stream.next()).events) {
          seen.add(e.documentKey!['_id']);
          times.add(e.clusterTime);
        }
      }
      await done;
      await writer.close();
      await admin.close();
      await stream.close();

      expect(inserted, hasLength(1000));
      expect(seen.toSet().length, seen.length, reason: 'an event was delivered twice');
      expect(seen, inserted);
      for (var i = 1; i < times.length; i++) {
        expect(times[i].compareTo(times[i - 1]), greaterThan(0));
      }
    },
    timeout: const Timeout(Duration(minutes: 5)),
  );

  test('closing, inserting 100 and reopening with resumeAfter delivers the 100', () async {
    await waitHealthy(client);
    final coll = db.collection('resume');
    final stream = await coll.watch();
    await coll.insertOne({'_id': -1});
    while ((await stream.next()).events.isEmpty) {}
    final token = stream.resumeToken!;
    await stream.close();
    await coll.insertMany([
      for (var i = 0; i < 100; i++) {'_id': i},
    ]);
    final reopened = await coll.watch(resumeAfter: token);
    final seen = <Object?>[];
    final deadline = DateTime.now().add(const Duration(seconds: 30));
    while (seen.length < 100 && DateTime.now().isBefore(deadline)) {
      seen.addAll((await reopened.next()).events.map((e) => e.documentKey!['_id']));
    }
    await reopened.close();
    expect(seen, List.generate(100, (i) => i));
  }, timeout: const Timeout(Duration(minutes: 2)));
}
