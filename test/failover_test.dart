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
    db = client.database(uniqueDb('failover'));
  });

  tearDownAll(() async {
    await waitPrimaryBack(client, original);
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('2000 majority inserts with a step-down at the 500th: each succeeds or throws a typed exception, '
      'and the count is exactly the successes', () async {
    final admin = await connectTest();
    final coll = db.collection('writes');
    final succeeded = <int>[];
    final failures = <MongoNativeException>[];
    for (var i = 0; i < 2000; i++) {
      if (i == 500) await stepDown(admin);
      try {
        await coll.insertOne({'_id': i}, writeConcern: MongoWriteConcern.majority);
        succeeded.add(i);
      } on MongoNativeException catch (e) {
        failures.add(e);
      }
    }
    await admin.close();
    expect(await primaryPort(client), isNot(original));
    final count = await coll.countDocuments({}, readConcern: MongoReadConcern.majority);
    expect(failures.whereType<MongoTimeoutException>().where((e) => e.outcomeUnknown), isEmpty);
    expect(count, succeeded.length, reason: 'failures: $failures');
    final cursor = await coll.find({}, projection: {'_id': 1}, sort: {'_id': 1});
    expect((await cursor.toList()).map((d) => d['_id']), succeeded);
  }, timeout: const Timeout(Duration(minutes: 5)));
}
