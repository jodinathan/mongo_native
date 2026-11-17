import 'dart:io';

import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

void main() {
  late MongoClient client;
  late MongoDatabase db;
  late int original;
  int? frozenPid;

  setUpAll(() async {
    client = await connectTest(operationTimeout: const Duration(seconds: 5));
    await waitHealthy(client);
    original = await primaryPort(client);
    db = client.database(uniqueDb('frozen'));
  });

  tearDown(() {
    if (frozenPid != null) Process.killPid(frozenPid!, ProcessSignal.sigcont);
    frozenPid = null;
  });

  tearDownAll(() async {
    await waitPrimaryBack(client, original);
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('a frozen primary: stuck inserts fail typed within the timeout, the new primary takes writes on the same '
      'client, and every confirmed majority write survives', () async {
    final coll = db.collection('writes');
    final confirmed = <int>[];
    var next = 0;
    for (; next < 50; next++) {
      await coll.insertOne({'_id': next});
      confirmed.add(next);
    }
    final pid = await memberPid(original);
    expect(Process.killPid(pid, ProcessSignal.sigstop), isTrue);
    frozenPid = pid;
    final frozenAt = DateTime.now();

    final failures = <MongoNativeException>[];
    var streak = 0;
    DateTime? recoveredAt;
    final deadline = DateTime.now().add(const Duration(seconds: 90));
    while (streak < 20) {
      if (DateTime.now().isAfter(deadline)) fail('no recovery within 90 s; failures: $failures');
      final watch = Stopwatch()..start();
      try {
        await coll.insertOne({'_id': next}, writeConcern: MongoWriteConcern.majority);
        confirmed.add(next);
        streak++;
        recoveredAt ??= DateTime.now();
      } on MongoNativeException catch (e) {
        expect(e, anyOf(isA<MongoTimeoutException>(), isA<MongoNetworkException>()));
        expect(watch.elapsed, lessThan(const Duration(seconds: 7)));
        failures.add(e);
        streak = 0;
        recoveredAt = null;
      }
      next++;
    }
    expect(failures, isNotEmpty);
    expect(recoveredAt!.difference(frozenAt), lessThan(const Duration(seconds: 30)));
    expect(await primaryPort(client), isNot(original));

    Process.killPid(pid, ProcessSignal.sigcont);
    frozenPid = null;
    await waitHealthy(client);
    final cursor = await coll.find(
      {
        '_id': {r'$in': confirmed},
      },
      projection: {'_id': 1},
      readConcern: MongoReadConcern.majority,
    );
    expect((await cursor.toList()).length, confirmed.length);
  }, timeout: const Timeout(Duration(minutes: 5)));
}
