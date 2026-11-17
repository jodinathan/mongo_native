import 'dart:io';
import 'dart:math';

import 'package:mongo_native/mongo_native.dart';

/// The URI written by `dart run tool/replset.dart start`.
String testUri() {
  final file = File('.dart_tool/mongo_native_test_uri');
  if (!file.existsSync()) {
    throw StateError(
      'no test replica set: run `dart run tool/replset.dart start --nodes 3 --base-port 27117` first '
      '(and `stop` afterwards)',
    );
  }
  return file.readAsStringSync().trim();
}

/// The parts of [testUri]; a URI with several hosts is not a `Uri`, so it is read by pattern.
({String user, String password, String hosts}) testUriParts() {
  final match = RegExp(r'^mongodb://([^:]+):([^@]+)@([^/]+)/').firstMatch(testUri());
  if (match == null) throw StateError('unexpected test URI shape: ${testUri()}');
  return (user: match[1]!, password: match[2]!, hosts: match[3]!);
}

/// The replica set URI with other credentials.
String replsetUri({required String user, required String password, String authSource = 'admin'}) =>
    'mongodb://$user:$password@${testUriParts().hosts}/?replicaSet=mn-rs&authSource=$authSource';

/// The URI of the single member on [port], with the test credentials and a direct connection.
String memberUri(int port) {
  final parts = testUriParts();
  return 'mongodb://${parts.user}:${parts.password}@127.0.0.1:$port/?directConnection=true&authSource=admin';
}

/// A client on the test replica set.
Future<MongoClient> connectTest({
  String? appName,
  Duration operationTimeout = const Duration(seconds: 30),
  Duration serverSelectionTimeout = const Duration(seconds: 15),
  int maxPoolSize = 32,
  String? uri,
}) => MongoClient.connect(
  MongoClientOptions(
    uri: uri ?? testUri(),
    appName: appName,
    operationTimeout: operationTimeout,
    serverSelectionTimeout: serverSelectionTimeout,
    maxPoolSize: maxPoolSize,
  ),
);

/// A database name no other test uses.
String uniqueDb(String prefix) => 'mn_${prefix}_${Random().nextInt(1 << 32).toRadixString(36)}';

/// Runs [command] on `admin`.
Future<Map<String, Object?>> admin(MongoClient client, Map<String, Object?> command, {bool readOnly = false}) =>
    client.database('admin').runCommand(command, readOnly: readOnly);

/// The port of the current primary.
Future<int> primaryPort(MongoClient client) async {
  final hello = await admin(client, {'hello': 1}, readOnly: true);
  final primary = hello['primary'] as String?;
  if (primary == null) throw StateError('the replica set has no primary right now');
  return int.parse(primary.split(':').last);
}

/// Waits until a primary other than [notPort] is elected; returns its port.
Future<int> waitForPrimary(MongoClient client, {int? notPort, Duration within = const Duration(seconds: 60)}) async {
  final deadline = DateTime.now().add(within);
  while (true) {
    try {
      final port = await primaryPort(client);
      if (port != notPort) return port;
    } on Object {
      if (DateTime.now().isAfter(deadline)) rethrow;
    }
    if (DateTime.now().isAfter(deadline)) throw StateError('no new primary within $within');
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }
}

/// Asks the primary to step down for [seconds] (the server refuses less than `secondaryCatchUpPeriodSecs`).
Future<void> stepDown(MongoClient client, {int seconds = 20}) async {
  try {
    await admin(client, {'replSetStepDown': seconds, 'secondaryCatchUpPeriodSecs': 10});
  } on MongoNetworkException {
    return;
  }
}

/// The process id of the member on [port], read from its `serverStatus`.
Future<int> memberPid(int port) async {
  final member = await MongoClient.connect(
    MongoClientOptions(uri: memberUri(port), readConcern: MongoReadConcern.local),
  );
  try {
    final status = await member.database('admin').runCommand({'serverStatus': 1}, readOnly: true);
    return status['pid']! as int;
  } finally {
    await member.close();
  }
}

/// Turns the `failCommand` fail point on in the member on [port] with [data] and [mode].
Future<void> failCommand(int port, Map<String, Object?> data, {Object mode = 'alwaysOn'}) async {
  final member = await MongoClient.connect(
    MongoClientOptions(uri: memberUri(port), readConcern: MongoReadConcern.local),
  );
  try {
    await member.database('admin').runCommand({'configureFailPoint': 'failCommand', 'mode': mode, 'data': data});
  } finally {
    await member.close();
  }
}

/// Turns the `failCommand` fail point off in the member on [port].
Future<void> clearFailCommand(int port) async {
  final member = await MongoClient.connect(
    MongoClientOptions(uri: memberUri(port), readConcern: MongoReadConcern.local),
  );
  try {
    await member.database('admin').runCommand({'configureFailPoint': 'failCommand', 'mode': 'off'});
  } finally {
    await member.close();
  }
}

/// Waits until every member is up and the set has a primary, so that a test starts from a healthy set.
Future<void> waitHealthy(MongoClient client, {Duration within = const Duration(seconds: 60)}) async {
  final deadline = DateTime.now().add(within);
  while (true) {
    try {
      final status = await admin(client, {'replSetGetStatus': 1}, readOnly: true);
      final members = (status['members']! as List).cast<Map<String, Object?>>();
      final states = members.map((m) => m['stateStr']).toList();
      if (states.where((s) => s == 'PRIMARY').length == 1 && states.every((s) => s == 'PRIMARY' || s == 'SECONDARY')) {
        return;
      }
    } on MongoNativeException {
      if (DateTime.now().isAfter(deadline)) rethrow;
    }
    if (DateTime.now().isAfter(deadline)) throw StateError('the replica set is not healthy within $within');
    await Future<void>.delayed(const Duration(milliseconds: 250));
  }
}

/// Waits until the member on [port] is primary again (it takes over by priority once its step-down
/// period ends), so that the next test starts from the usual topology.
Future<void> waitPrimaryBack(MongoClient client, int port, {Duration within = const Duration(seconds: 120)}) async {
  final deadline = DateTime.now().add(within);
  while (true) {
    try {
      if (await primaryPort(client) == port) {
        await waitHealthy(client);
        return;
      }
    } on Object {
      if (DateTime.now().isAfter(deadline)) rethrow;
    }
    if (DateTime.now().isAfter(deadline)) {
      throw StateError('the member on $port did not become primary again within $within');
    }
    await Future<void>.delayed(const Duration(milliseconds: 500));
  }
}
