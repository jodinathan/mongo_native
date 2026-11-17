import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:mongo_native/mongo_native.dart';

const _setName = 'mn-rs';
const _user = 'mn-test';
final _root = Directory('.dart_tool/mongo_native_replset');
final _uriFile = File('.dart_tool/mongo_native_test_uri');
final _nodesFile = File('${_root.path}/nodes');

/// Starts (`start --nodes N --base-port P`) or stops (`stop`) the local replica set of the tests.
///
/// `start` launches N `mongod` from `$MONGOD_PREFIX/bin` (default `~/.local/mongodb-9.0.8`) on
/// ports P to P+N-1, initiates the set with fast elections, creates the user `mn-test` and writes the
/// URI to `.dart_tool/mongo_native_test_uri`. `stop` ends every node and deletes their data.
Future<void> main(List<String> args) async {
  if (args.isEmpty || (args.first != 'start' && args.first != 'stop')) {
    stderr.writeln('usage: dart run tool/replset.dart start [--nodes 3] [--base-port 27117] | stop');
    exit(64);
  }
  try {
    if (args.first == 'stop') {
      await _stop();
    } else {
      await _start(
        nodes: int.parse(_option(args, '--nodes') ?? '3'),
        basePort: int.parse(_option(args, '--base-port') ?? '27117'),
      );
    }
  } on Object catch (e) {
    stderr.writeln('replset ${args.first} failed: $e');
    exit(1);
  }
  exit(0);
}

String? _option(List<String> args, String name) {
  final at = args.indexOf(name);
  return at >= 0 && at + 1 < args.length ? args[at + 1] : null;
}

String get _prefix {
  final fromEnv = Platform.environment['MONGOD_PREFIX'];
  if (fromEnv != null && fromEnv.isNotEmpty) return fromEnv;
  return '${Platform.environment['HOME']}/.local/mongodb-9.0.8';
}

Future<void> _start({required int nodes, required int basePort}) async {
  final mongod = File('$_prefix/bin/mongod');
  if (!mongod.existsSync()) {
    throw StateError('mongod not found at ${mongod.path}; install MongoDB 9.0.8 there or set MONGOD_PREFIX');
  }
  if (_nodesFile.existsSync()) {
    throw StateError(
      'a replica set from a previous start is still registered; run `dart run tool/replset.dart stop` first',
    );
  }
  for (var i = 0; i < nodes; i++) {
    if (await _listening(basePort + i)) {
      throw StateError('port ${basePort + i} is already in use; pick another --base-port');
    }
  }
  _root.createSync(recursive: true);
  final random = Random.secure();
  final keyFile = File('${_root.path}/keyfile')
    ..writeAsStringSync(base64.encode(List<int>.generate(756, (_) => random.nextInt(256))));
  await Process.run('chmod', ['600', keyFile.path]);
  final lines = <String>[];
  for (var i = 0; i < nodes; i++) {
    final port = basePort + i;
    final dbPath = Directory('${_root.path}/n$port')..createSync();
    final pidFile = '${_root.path}/n$port.pid';
    lines.add('$port ${dbPath.absolute.path} ${File(pidFile).absolute.path}');
    _nodesFile.writeAsStringSync('${lines.join('\n')}\n');
    await Process.start(mongod.path, [
      '--replSet',
      _setName,
      '--keyFile',
      keyFile.absolute.path,
      '--bind_ip',
      '127.0.0.1',
      '--port',
      '$port',
      '--dbpath',
      dbPath.absolute.path,
      '--logpath',
      '${_root.absolute.path}/n$port.log',
      '--pidfilepath',
      File(pidFile).absolute.path,
      '--setParameter',
      'enableTestCommands=1',
    ], mode: ProcessStartMode.detached);
  }
  for (var i = 0; i < nodes; i++) {
    final port = basePort + i;
    final deadline = DateTime.now().add(const Duration(seconds: 60));
    while (!await _listening(port)) {
      if (DateTime.now().isAfter(deadline)) {
        throw StateError('mongod on port $port did not listen within 60 s; see ${_root.path}/n$port.log');
      }
      await Future<void>.delayed(const Duration(milliseconds: 200));
    }
  }

  final seed = await MongoClient.connect(
    MongoClientOptions(
      uri: 'mongodb://127.0.0.1:$basePort/?directConnection=true',
      writeConcern: MongoWriteConcern.acknowledged,
      readConcern: MongoReadConcern.local,
    ),
  );
  try {
    final admin = seed.database('admin');
    await admin.runCommand({
      'replSetInitiate': {
        '_id': _setName,
        'members': [
          for (var i = 0; i < nodes; i++) {'_id': i, 'host': '127.0.0.1:${basePort + i}', 'priority': i == 0 ? 2 : 1},
        ],
        'settings': {'electionTimeoutMillis': 2000, 'heartbeatIntervalMillis': 500},
      },
    });
    final deadline = DateTime.now().add(const Duration(seconds: 60));
    while (true) {
      final hello = await admin.runCommand({'hello': 1}, readOnly: true);
      if (hello['isWritablePrimary'] == true) break;
      if (DateTime.now().isAfter(deadline)) throw StateError('no PRIMARY within 60 s after replSetInitiate');
      await Future<void>.delayed(const Duration(milliseconds: 200));
    }
    final password = List.generate(24, (_) => 'abcdefghijkmnpqrstuvwxyz23456789'[random.nextInt(32)]).join();
    await admin.runCommand({
      'createUser': _user,
      'pwd': password,
      'roles': [
        {'role': 'root', 'db': 'admin'},
      ],
      'writeConcern': {'w': 'majority'},
    });
    final hosts = [for (var i = 0; i < nodes; i++) '127.0.0.1:${basePort + i}'].join(',');
    _uriFile.writeAsStringSync('mongodb://$_user:$password@$hosts/?replicaSet=$_setName&authSource=admin');
  } finally {
    await seed.close();
  }
  stdout.writeln('replica set $_setName up on ports $basePort-${basePort + nodes - 1}; URI in ${_uriFile.path}');
}

Future<bool> _listening(int port) async {
  try {
    final socket = await Socket.connect('127.0.0.1', port, timeout: const Duration(milliseconds: 300));
    socket.destroy();
    return true;
  } on SocketException {
    return false;
  }
}

Future<void> _stop() async {
  if (!_nodesFile.existsSync()) {
    stdout.writeln('no replica set registered; nothing to stop');
    if (_uriFile.existsSync()) _uriFile.deleteSync();
    return;
  }
  for (final line in _nodesFile.readAsLinesSync().where((l) => l.trim().isNotEmpty)) {
    final [port, _, pidPath] = line.split(' ');
    final pidFile = File(pidPath);
    if (!pidFile.existsSync()) continue;
    final pid = int.tryParse(pidFile.readAsStringSync().trim());
    if (pid == null) continue;
    Process.killPid(pid, ProcessSignal.sigcont);
    Process.killPid(pid, ProcessSignal.sigterm);
    final deadline = DateTime.now().add(const Duration(seconds: 30));
    while (await _alive(pid)) {
      if (DateTime.now().isAfter(deadline)) {
        Process.killPid(pid, ProcessSignal.sigkill);
        break;
      }
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }
    stdout.writeln('mongod on port $port (pid $pid) stopped');
  }
  _root.deleteSync(recursive: true);
  if (_uriFile.existsSync()) _uriFile.deleteSync();
}

Future<bool> _alive(int pid) async => (await Process.run('kill', ['-0', '$pid'])).exitCode == 0;
