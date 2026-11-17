import 'dart:io';
import 'dart:isolate';

import 'package:mongo_native/mongo_native.dart';
import 'package:mongo_native/src/ffi/bridge.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

Future<List<Object?>> _insertMany(String uri, String dbName, String prefix) async {
  final client = await MongoClient.connect(MongoClientOptions(uri: uri));
  try {
    final coll = client.database(dbName).collection('bridge');
    final results = await Future.wait([
      for (var i = 0; i < 500; i++) coll.insertOne({'_id': '$prefix-$i'}),
    ]);
    return [for (final r in results) r.insertedIds.single];
  } finally {
    await client.close();
  }
}

Future<List<Object?>> _inIsolate(String uri, String dbName, String prefix) =>
    Isolate.run(() => _insertMany(uri, dbName, prefix));

Future<(bool, Object?)> _reconnect(String uri) => Isolate.run(() async {
  final first = await MongoClient.connect(MongoClientOptions(uri: uri));
  final bridge = NativeBridge.current;
  await first.database('admin').runCommand({'ping': 1}, readOnly: true);
  await first.close();
  final second = await MongoClient.connect(MongoClientOptions(uri: uri));
  final renewed = !identical(bridge, NativeBridge.current);
  final pong = await second.database('admin').runCommand({'ping': 1}, readOnly: true);
  await second.close();
  return (renewed, pong['ok']);
});

void main() {
  late MongoClient client;
  late MongoDatabase db;

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('bridge'));
  });

  tearDownAll(() async {
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('two isolates with their own clients complete 500 concurrent inserts each with their own results', () async {
    final uri = testUri();
    final name = db.name;
    final results = await Future.wait([_inIsolate(uri, name, 'a'), _inIsolate(uri, name, 'b')]);
    expect(results[0], [for (var i = 0; i < 500; i++) 'a-$i']);
    expect(results[1], [for (var i = 0; i < 500; i++) 'b-$i']);
    expect(await db.collection('bridge').countDocuments({}), 1000);
  });

  test('connect, close and connect again in the same isolate uses a new bridge', () async {
    final isolate = await _reconnect(testUri());
    expect(isolate.$1, isTrue);
    expect(isolate.$2, 1.0);
  });

  test('a script that reads and leaves its client open exits by itself', () async {
    final result = await Process.run(Platform.resolvedExecutable, [
      'run',
      'test/support/scripts/idle_client.dart',
    ]).timeout(const Duration(minutes: 3));
    expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
    expect('${result.stdout}', contains('read mn-rs'));
  }, timeout: const Timeout(Duration(minutes: 4)));
}
