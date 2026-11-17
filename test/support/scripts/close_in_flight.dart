import 'dart:io';

import '../replset.dart';

Future<void> main(List<String> args) async {
  final client = await connectTest(appName: args.first);
  final coll = client.database(args[1]).collection('items');
  final pending = [
    for (var i = 0; i < 50; i++)
      coll.insertOne({'_id': 'script-$i'}).then<String>((_) => 'ok', onError: (Object e) => '${e.runtimeType}'),
  ];
  await Future<void>.delayed(const Duration(milliseconds: 300));
  await client.close();
  final outcomes = await Future.wait(pending);
  print('outcomes ${outcomes.toSet().join(',')}');
  exitCode = outcomes.every((o) => o == 'ok' || o == 'MongoClosedException') ? 0 : 3;
}
