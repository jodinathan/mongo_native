import 'dart:io';

import 'package:mongo_native/src/hook/checksums.dart';
import 'package:mongo_native/src/hook/targets.dart';

const _checksumsFile = 'lib/src/hook/checksums.dart';

/// `dart run tool/checksums.dart SHA256SUMS` rewrites `lib/src/hook/checksums.dart` with the package
/// version and the sha256 of the five prebuilt libraries listed in SHA256SUMS (`<sum>  <triple>-<file>`
/// per line). `dart run tool/checksums.dart --check` fails unless that file matches the pubspec
/// version and has a sum for every target.
void main(List<String> args) {
  if (args.length != 1) {
    stderr.writeln('usage: dart run tool/checksums.dart SHA256SUMS | --check');
    exit(64);
  }
  final version = _pubspecVersion();
  final expected = {for (final (triple, file) in kTargets.values) triple: '$triple-$file'};
  if (args.single == '--check') {
    final problems = [
      if (kChecksumsVersion != version) 'kChecksumsVersion is $kChecksumsVersion but pubspec.yaml says $version',
      for (final triple in expected.keys)
        if (!RegExp(r'^[0-9a-f]{64}$').hasMatch(kChecksums[triple] ?? '')) 'no sha256 for $triple in kChecksums',
    ];
    if (problems.isNotEmpty) {
      stderr.writeln(
        '$_checksumsFile is not ready for the release:\n${problems.map((p) => '  - $p').join('\n')}\n'
        'Run the release-binaries workflow and merge its pull request before tagging.',
      );
      exit(1);
    }
    stdout.writeln('$_checksumsFile matches $version with ${expected.length} sums');
    return;
  }

  final sums = <String, String>{};
  for (final line in File(args.single).readAsLinesSync().where((l) => l.trim().isNotEmpty)) {
    final match = RegExp(r'^([0-9a-f]{64})\s+\*?(\S+)$').firstMatch(line.trim());
    if (match == null) {
      stderr.writeln('unexpected line in ${args.single}: "$line"; expected "<sha256>  <file>"');
      exit(1);
    }
    final triple = expected.entries.where((e) => e.value == match[2]).map((e) => e.key).firstOrNull;
    if (triple != null) sums[triple] = match[1]!;
  }
  final missing = expected.keys.where((t) => !sums.containsKey(t)).toList();
  if (missing.isNotEmpty) {
    stderr.writeln('${args.single} has no sum for ${missing.join(', ')}; every release ships the five libraries');
    exit(1);
  }
  final entries = [for (final triple in expected.keys) "  '$triple': '${sums[triple]}',"].join('\n');
  File(_checksumsFile).writeAsStringSync('''/// The package version the [kChecksums] belong to.
const kChecksumsVersion = '$version';

/// The sha256 of each prebuilt library of the Release `v\$kChecksumsVersion`, by target triple.
const kChecksums = <String, String>{
$entries
};
''');
  stdout.writeln('$_checksumsFile now holds the sums of $version');
}

String _pubspecVersion() {
  final match = RegExp(r'^version:\s*(\S+)', multiLine: true).firstMatch(File('pubspec.yaml').readAsStringSync());
  if (match == null) throw StateError('pubspec.yaml has no version');
  return match[1]!;
}
