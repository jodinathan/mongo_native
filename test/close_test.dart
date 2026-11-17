import 'dart:io';

import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

void main() {
  const appName = 'mn-close';
  late MongoClient client;
  late MongoDatabase db;
  late int primary;

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('close'));
    await db.collection('items').insertOne({'_id': 'seed'});
    primary = await primaryPort(client);
    await failCommand(primary, {
      'failCommands': ['insert'],
      'blockConnection': true,
      'blockTimeMS': 2000,
      'appName': appName,
    });
  });

  tearDownAll(() async {
    await clearFailCommand(primary);
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('closing with 50 requests in flight completes each with MongoClosedException or its result', () async {
    final victim = await connectTest(appName: appName);
    final coll = victim.database(db.name).collection('items');
    final pending = [
      for (var i = 0; i < 50; i++)
        coll.insertOne({'_id': 'in-flight-$i'}).then<Object>((r) => r, onError: (Object e) => e),
    ];
    await Future<void>.delayed(const Duration(milliseconds: 300));
    final watch = Stopwatch()..start();
    await victim.close().timeout(const Duration(seconds: 20));
    final outcomes = await Future.wait(pending).timeout(const Duration(seconds: 5));
    expect(watch.elapsed, lessThan(const Duration(seconds: 20)));
    for (final outcome in outcomes) {
      expect(outcome, anyOf(isA<MongoInsertResult>(), isA<MongoClosedException>()));
    }
    expect(outcomes.whereType<MongoClosedException>(), isNotEmpty);
  });

  test('calls after close throw MongoClosedException, and close is idempotent', () async {
    final other = await connectTest();
    final first = other.close();
    expect(identical(other.close(), first), isTrue);
    await first;
    expect(other.isClosed, isTrue);
    await expectLater(
      other.database(db.name).collection('items').findOne({}),
      throwsA(isA<MongoClosedException>().having((e) => e.operation, 'operation', 'findOne on ${db.name}.items')),
    );
  });

  test('a process that closes with requests in flight exits cleanly', () async {
    final result = await Process.run(Platform.resolvedExecutable, [
      'run',
      'test/support/scripts/close_in_flight.dart',
      appName,
      db.name,
    ]).timeout(const Duration(minutes: 3));
    expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
    expect('${result.stdout}', contains('MongoClosedException'));
  }, timeout: const Timeout(Duration(minutes: 4)));
}
