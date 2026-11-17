import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:mongo_native/src/hook/resolve.dart';
import 'package:test/test.dart';

const _triple = 'x86_64-unknown-linux-gnu';
const _file = 'libmongo_native_ffi.so';

void main() {
  late Directory tmp;
  late HttpServer server;
  late Map<String, List<int>> served;
  late int requests;

  Uri dir(String name) => Directory('${tmp.path}/$name/').uri;

  String fakeCargo() {
    final script = File('${tmp.path}/fakebin/cargo')
      ..createSync(recursive: true)
      ..writeAsStringSync('''#!/bin/sh
if [ "\$1" = "--version" ]; then echo "cargo 1.99.0 (fake)"; exit 0; fi
td=""; tg=""
while [ \$# -gt 0 ]; do
  case "\$1" in
    --target-dir) td="\$2"; shift ;;
    --target) tg="\$2"; shift ;;
  esac
  shift
done
/bin/mkdir -p "\$td/\$tg/release" && printf compiled > "\$td/\$tg/release/$_file"
''');
    Process.runSync('chmod', ['+x', script.path]);
    return script.path;
  }

  Future<Uri> resolve({
    Uri? prebuilt,
    Map<String, String> checksums = const {},
    String cargo = 'cargo',
    Map<String, String>? environment,
  }) => resolveNativeLibrary(
    os: OS.linux,
    arch: Architecture.x64,
    packageRoot: Directory.current.uri,
    cacheDir: dir('cache'),
    prebuilt: prebuilt,
    checksums: checksums,
    version: '9.9.9',
    releaseBase: 'http://127.0.0.1:${server.port}/releases',
    cargo: cargo,
    environment: environment ?? {'PATH': '${tmp.path}/emptybin'},
    downloadTimeout: const Duration(seconds: 5),
  );

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('mn_hook_');
    Directory('${tmp.path}/emptybin').createSync();
    served = {};
    requests = 0;
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      requests++;
      final body = served[request.uri.path];
      if (body == null) {
        request.response.statusCode = HttpStatus.notFound;
      } else {
        request.response.add(body);
      }
      await request.response.close();
    });
  });

  tearDown(() async {
    await server.close(force: true);
    tmp.deleteSync(recursive: true);
  });

  test('an existing prebuilt library is used as is', () async {
    final lib = File('${tmp.path}/mine.so')..writeAsStringSync('mine');
    expect(await resolve(prebuilt: lib.uri), lib.absolute.uri);
    expect(requests, 0);
  });

  test('a missing prebuilt library throws StateError with the resolved path', () async {
    await expectLater(
      resolve(prebuilt: Uri.file('${tmp.path}/absent.so')),
      throwsA(isA<StateError>().having((e) => e.message, 'message', contains('${tmp.path}/absent.so'))),
    );
  });

  test('a download with the right sha256 is cached and used', () async {
    final bytes = 'prebuilt-binary'.codeUnits;
    served['/releases/v9.9.9/$_triple-$_file'] = bytes;
    final uri = await resolve(checksums: {_triple: sha256.convert(bytes).toString()});
    expect(File.fromUri(uri).readAsStringSync(), 'prebuilt-binary');
    expect(uri.path, endsWith('/cache/9.9.9/$_triple/$_file'));
    expect(requests, 1);
    await resolve(checksums: {_triple: sha256.convert(bytes).toString()});
    expect(requests, 1);
  });

  test('a wrong sha256 without cargo throws StateError citing both sums and deletes the download', () async {
    final bytes = 'tampered'.codeUnits;
    served['/releases/v9.9.9/$_triple-$_file'] = bytes;
    final expected = 'a' * 64;
    await expectLater(
      resolve(checksums: {_triple: expected}),
      throwsA(
        isA<StateError>()
            .having((e) => e.message, 'message', contains(expected))
            .having((e) => e.message, 'message', contains(sha256.convert(bytes).toString())),
      ),
    );
    expect(File('${tmp.path}/cache/9.9.9/$_triple/$_file').existsSync(), isFalse);
    expect(File('${tmp.path}/cache/9.9.9/$_triple/$_file.download').existsSync(), isFalse);
  });

  test('without a sum for the target nothing is requested and cargo builds it', () async {
    final uri = await resolve(cargo: fakeCargo());
    expect(requests, 0);
    expect(File.fromUri(uri).readAsStringSync(), 'compiled');
    expect(uri.path, endsWith('/cache/cargo/$_triple/release/$_file'));
  }, testOn: '!windows');

  test('a 404 falls back to cargo', () async {
    final uri = await resolve(checksums: {_triple: 'b' * 64}, cargo: fakeCargo());
    expect(requests, 1);
    expect(File.fromUri(uri).readAsStringSync(), 'compiled');
  }, testOn: '!windows');

  test('with nothing available the message says to install Rust or set user_defines', () async {
    await expectLater(
      resolve(),
      throwsA(
        isA<StateError>()
            .having((e) => e.message, 'message', contains('rustup'))
            .having((e) => e.message, 'message', contains('hooks: user_defines: mongo_native: lib:'))
            .having((e) => e.message, 'message', contains('no checksum for $_triple'))
            .having((e) => e.message, 'message', contains('cargo is not installed')),
      ),
    );
    expect(requests, 0);
  });
}
