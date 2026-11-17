import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

void main() {
  const appName = 'mn-timeout-cancel';
  late MongoClient client;
  late MongoClient victim;
  late MongoDatabase db;
  late int primary;

  setUpAll(() async {
    client = await connectTest();
    victim = await connectTest(appName: appName);
    db = client.database(uniqueDb('timeout'));
    await db.collection('items').insertOne({'_id': 1, 'n': 0});
    primary = await primaryPort(client);
  });

  tearDown(() => clearFailCommand(primary));

  tearDownAll(() async {
    await db.runCommand({'dropDatabase': 1});
    await victim.close();
    await client.close();
  });

  Future<void> block(List<String> commands, {int ms = 3000}) =>
      failCommand(primary, {'failCommands': commands, 'blockConnection': true, 'blockTimeMS': ms, 'appName': appName});

  test('a read-only runCommand past its timeout throws MongoTimeoutException(outcomeUnknown: false)', () async {
    final watch = Stopwatch()..start();
    await expectLater(
      victim
          .database('admin')
          .runCommand(
            {'sleep': 1, 'millis': 3000, 'lock': 'none'},
            readOnly: true,
            timeout: const Duration(milliseconds: 500),
          ),
      throwsA(isA<MongoTimeoutException>().having((e) => e.outcomeUnknown, 'outcomeUnknown', false)),
    );
    expect(watch.elapsed, lessThan(const Duration(milliseconds: 2500)));
  });

  test('find with maxTime under the timeout throws the server MaxTimeMSExpired (50)', () async {
    final watch = Stopwatch()..start();
    await expectLater(
      victim
          .database(db.name)
          .collection('items')
          .find(
            {r'$where': 'sleep(5000) || true'},
            maxTime: const Duration(milliseconds: 500),
            timeout: const Duration(seconds: 3),
          ),
      throwsA(isA<MongoCommandException>().having((e) => e.code, 'code', 50)),
    );
    expect(watch.elapsed, lessThan(const Duration(seconds: 3)));
  });

  test('cancelling a blocked insertOne throws MongoCancelledException(outcomeUnknown: true)', () async {
    await block(['insert']);
    final token = MongoCancelToken();
    final watch = Stopwatch()..start();
    final pending = victim.database(db.name).collection('items').insertOne({'_id': 2}, cancel: token);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    token.cancel();
    await expectLater(
      pending,
      throwsA(isA<MongoCancelledException>().having((e) => e.outcomeUnknown, 'outcomeUnknown', true)),
    );
    expect(watch.elapsed, lessThan(const Duration(milliseconds: 2500)));
  });

  test('a blocked findOneAndUpdate past its timeout throws MongoTimeoutException(outcomeUnknown: true)', () async {
    await block(['findAndModify']);
    await expectLater(
      victim
          .database(db.name)
          .collection('items')
          .findOneAndUpdate(
            {'_id': 1},
            {
              r'$inc': {'n': 1},
            },
            timeout: const Duration(milliseconds: 500),
            maxTime: const Duration(seconds: 5),
          ),
      throwsA(isA<MongoTimeoutException>().having((e) => e.outcomeUnknown, 'outcomeUnknown', true)),
    );
  });

  test('cancelling a read leaves the outcome known, and cancelling after completion does nothing', () async {
    await block(['find']);
    final token = MongoCancelToken();
    final pending = victim.database(db.name).collection('items').findOne({}, cancel: token);
    await Future<void>.delayed(const Duration(milliseconds: 200));
    token.cancel();
    await expectLater(
      pending,
      throwsA(isA<MongoCancelledException>().having((e) => e.outcomeUnknown, 'outcomeUnknown', false)),
    );
    await clearFailCommand(primary);
    final late = MongoCancelToken();
    expect(await victim.database(db.name).collection('items').findOne({'_id': 1}, cancel: late), isNotNull);
    late.cancel();
  });

  test('an already cancelled token cancels at once', () async {
    final token = MongoCancelToken()..cancel();
    await expectLater(
      victim.database(db.name).collection('items').insertOne({'_id': 99}, cancel: token),
      throwsA(isA<MongoCancelledException>()),
    );
  });
}
