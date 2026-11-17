import '../replset.dart';

Future<void> main() async {
  final client = await connectTest();
  final hello = await client.database('admin').runCommand({'hello': 1}, readOnly: true);
  print('read ${hello['setName']}; leaving the client open');
}
