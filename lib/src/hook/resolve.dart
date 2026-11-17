import 'dart:async';
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';

import 'checksums.dart';
import 'targets.dart';

/// Finds the native library for [os]/[arch]:
///
/// 1. [prebuilt], when given (the `hooks: user_defines: mongo_native: lib:` of the root pubspec),
///    used as is;
/// 2. the library of the Release `v[version]`, cached under [cacheDir] or downloaded from
///    [releaseBase], only when [checksums] has its sha256 and the bytes match it;
/// 3. a build with `cargo` ([cargo], run with [environment]) into [cacheDir];
/// 4. otherwise a [StateError] with the reason of each step and what to do.
Future<Uri> resolveNativeLibrary({
  required OS os,
  required Architecture arch,
  required Uri packageRoot,
  required Uri cacheDir,
  Uri? prebuilt,
  Map<String, String> checksums = kChecksums,
  String version = kChecksumsVersion,
  String releaseBase = kReleaseBase,
  String cargo = 'cargo',
  Map<String, String>? environment,
  Duration downloadTimeout = const Duration(seconds: 60),
}) async {
  if (prebuilt != null) {
    final file = File.fromUri(prebuilt);
    if (!file.existsSync()) {
      throw StateError(
        'mongo_native: the library set in `hooks: user_defines: mongo_native: lib:` does not exist at '
        '${file.path}; fix the path (it is relative to the pubspec.yaml that declares it) or remove the entry',
      );
    }
    return file.absolute.uri;
  }

  final reasons = <String>[];
  final target = kTargets[(os, arch)];
  if (target == null) {
    reasons.add('no prebuilt library exists for $os/$arch');
  } else {
    final (triple, fileName) = target;
    final expected = checksums[triple];
    if (expected == null) {
      reasons.add('no checksum for $triple in this version ($version), so nothing is downloaded');
    } else {
      final downloaded = await _download(
        url: Uri.parse('$releaseBase/v$version/$triple-$fileName'),
        destination: File.fromUri(cacheDir.resolve('$version/$triple/$fileName')),
        expected: expected,
        timeout: downloadTimeout,
        reasons: reasons,
      );
      if (downloaded != null) return downloaded.uri;
    }
  }

  final built = await _cargoBuild(
    cargo: cargo,
    environment: environment,
    packageRoot: packageRoot,
    cacheDir: cacheDir,
    target: target,
    reasons: reasons,
  );
  if (built != null) return built.uri;

  throw StateError(
    'mongo_native: no native library for $os/$arch.\n'
    '${reasons.map((r) => '  - $r').join('\n')}\n'
    'Install Rust 1.88 or newer (https://rustup.rs, `rustup` puts `cargo` on the PATH) so that the '
    'library is compiled, or point `hooks: user_defines: mongo_native: lib:` in the root pubspec.yaml '
    'to a prebuilt library.',
  );
}

Future<File?> _download({
  required Uri url,
  required File destination,
  required String expected,
  required Duration timeout,
  required List<String> reasons,
}) async {
  if (destination.existsSync()) {
    final cached = sha256.convert(await destination.readAsBytes()).toString();
    if (cached == expected) return destination;
    await destination.delete();
  }
  await destination.parent.create(recursive: true);
  final partial = File('${destination.path}.download');
  String? lastFailure;
  for (var attempt = 1; attempt <= 3; attempt++) {
    final http = HttpClient()..connectionTimeout = timeout;
    try {
      final request = await http.getUrl(url).timeout(timeout);
      final response = await request.close().timeout(timeout);
      if (response.statusCode != HttpStatus.ok) {
        await response.drain<void>();
        lastFailure = 'GET $url answered HTTP ${response.statusCode}';
        if (response.statusCode == HttpStatus.notFound) break;
        continue;
      }
      final sink = partial.openWrite();
      await response.timeout(timeout).pipe(sink);
      final actual = sha256.convert(await partial.readAsBytes()).toString();
      if (actual != expected) {
        await partial.delete();
        reasons.add('the download of $url has sha256 $actual, expected $expected; it was deleted');
        return null;
      }
      await partial.rename(destination.path);
      return destination;
    } on Object catch (e) {
      lastFailure = 'GET $url failed: $e';
    } finally {
      http.close(force: true);
    }
  }
  if (partial.existsSync()) await partial.delete();
  reasons.add(lastFailure ?? 'GET $url failed');
  return null;
}

Future<File?> _cargoBuild({
  required String cargo,
  required Map<String, String>? environment,
  required Uri packageRoot,
  required Uri cacheDir,
  required (String, String)? target,
  required List<String> reasons,
}) async {
  if (target == null) {
    reasons.add('cargo was not tried: there is no Rust target for this platform');
    return null;
  }
  final (triple, fileName) = target;
  try {
    final probe = await Process.run(cargo, ['--version'], environment: environment);
    if (probe.exitCode != 0) {
      reasons.add('`$cargo --version` exited with ${probe.exitCode}: ${probe.stderr}'.trim());
      return null;
    }
  } on ProcessException catch (e) {
    reasons.add('cargo is not installed or not on the PATH (${e.message})');
    return null;
  }
  final targetDir = Directory.fromUri(cacheDir.resolve('cargo'));
  final manifest = File.fromUri(packageRoot.resolve('native/Cargo.toml'));
  final result = await Process.run(cargo, [
    'build',
    '--release',
    '--locked',
    '--manifest-path',
    manifest.path,
    '--target-dir',
    targetDir.path,
    '--target',
    triple,
  ], environment: environment);
  final output = File('${targetDir.path}/$triple/release/$fileName');
  if (result.exitCode != 0) {
    final stderr = '${result.stderr}'.trim().split('\n');
    final tail = stderr.length > 15 ? stderr.sublist(stderr.length - 15) : stderr;
    reasons.add('`cargo build --release --target $triple` exited with ${result.exitCode}:\n${tail.join('\n')}');
    return null;
  }
  if (!output.existsSync()) {
    reasons.add('cargo finished but ${output.path} does not exist');
    return null;
  }
  return output.absolute;
}
