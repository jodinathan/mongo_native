import 'dart:async';
import 'dart:isolate';

import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

typedef _Victim = ({SendPort ready, String uri, String appName, String db});

/// Opens a client named [_Victim.appName], starts 20 finds that sleep 2.5 s on the server, a change
/// stream read and a cursor read, says it is ready and waits on them until the isolate is killed.
Future<void> _busyVictim(_Victim v) async {
  final client = await MongoClient.connect(MongoClientOptions(uri: v.uri, appName: v.appName));
  final coll = client.database(v.db).collection('items');
  final stream = await coll.watch();
  final cursor = await coll.find({}, batchSize: 1);
  await cursor.nextBatch();
  final pending = <Future<Object?>>[
    for (var i = 0; i < 20; i++)
      coll
          .findOne({'_id': 'seed-$i', r'$where': 'sleep(2500) || true'})
          .then<Object?>((r) => r, onError: (Object e) => e),
    stream.next().then<Object?>((r) => r, onError: (Object e) => e),
    cursor.nextBatch().then<Object?>((r) => r, onError: (Object e) => e),
  ];
  await Future<void>.delayed(const Duration(milliseconds: 300));
  v.ready.send('busy');
  await Future.wait(pending);
}

/// Opens a client named [_Victim.appName], reads once, says it is ready and ends with the client
/// still open.
Future<void> _idleVictim(_Victim v) async {
  final client = await MongoClient.connect(MongoClientOptions(uri: v.uri, appName: v.appName));
  await client.database(v.db).collection('items').findOne({});
  v.ready.send('idle');
}

/// The connections the primary has from clients named [appName], idle ones included.
Future<int> _connectionsOf(MongoClient client, String appName) async {
  final reply = await admin(client, {
    'aggregate': 1,
    'pipeline': [
      {
        r'$currentOp': {'allUsers': true, 'idleConnections': true, 'idleSessions': false},
      },
      {
        r'$match': {'appName': appName},
      },
      {r'$count': 'n'},
    ],
    'cursor': <String, Object?>{},
  }, readOnly: true);
  final batch = ((reply['cursor']! as Map)['firstBatch']! as List).cast<Map<String, Object?>>();
  return batch.isEmpty ? 0 : batch.single['n']! as int;
}

/// Polls until the primary has no connection from [appName]; returns how long it took.
Future<Duration> _waitNoConnections(MongoClient client, String appName, {required Duration within}) async {
  final watch = Stopwatch()..start();
  while (true) {
    final n = await _connectionsOf(client, appName);
    if (n == 0) return watch.elapsed;
    if (watch.elapsed > within) {
      throw StateError('the primary still has $n connections from $appName after $within');
    }
    await Future<void>.delayed(const Duration(milliseconds: 250));
  }
}

void main() {
  const busyApp = 'mn-isolate-busy';
  const idleApp = 'mn-isolate-idle';
  late MongoClient client;
  late MongoDatabase db;

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('isolate'));
    await db.collection('items').insertMany([
      for (var i = 0; i < 20; i++) {'_id': 'seed-$i'},
    ]);
  });

  tearDownAll(() async {
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('killing an isolate with operations in flight keeps the process alive and frees its client', () async {
    final ready = ReceivePort();
    final isolate = await Isolate.spawn(_busyVictim, (
      ready: ready.sendPort,
      uri: testUri(),
      appName: busyApp,
      db: db.name,
    ));
    expect(await ready.first.timeout(const Duration(seconds: 30)), 'busy');
    isolate.kill(priority: Isolate.immediate);

    await Future<void>.delayed(const Duration(seconds: 4));
    final coll = db.collection('items');
    expect(await coll.countDocuments({}), 20);
    final other = await connectTest();
    try {
      expect(await other.database(db.name).collection('items').findOne({'_id': 'seed-3'}), {'_id': 'seed-3'});
    } finally {
      await other.close();
    }
    await _waitNoConnections(client, busyApp, within: const Duration(seconds: 15));
  }, timeout: const Timeout(Duration(minutes: 2)));

  test('an isolate that ends with its client open releases the client', () async {
    final ready = ReceivePort();
    await Isolate.spawn(_idleVictim, (ready: ready.sendPort, uri: testUri(), appName: idleApp, db: db.name));
    expect(await ready.first.timeout(const Duration(seconds: 30)), 'idle');
    await _waitNoConnections(client, idleApp, within: const Duration(seconds: 15));
  }, timeout: const Timeout(Duration(minutes: 2)));
}
