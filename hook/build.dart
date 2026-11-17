import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';
import 'package:mongo_native/src/hook/resolve.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;
    final prebuilt = input.userDefines.path('lib');
    final file = await resolveNativeLibrary(
      os: input.config.code.targetOS,
      arch: input.config.code.targetArchitecture,
      packageRoot: input.packageRoot,
      cacheDir: input.outputDirectoryShared,
      prebuilt: prebuilt,
    );
    output.assets.code.add(
      CodeAsset(
        package: input.packageName,
        name: 'src/ffi/bindings.dart',
        linkMode: DynamicLoadingBundled(),
        file: file,
      ),
    );
    if (prebuilt != null) {
      output.dependencies.add(prebuilt);
      return;
    }
    final native = input.packageRoot.resolve('native/');
    output.dependencies.addAll([
      native.resolve('Cargo.toml'),
      native.resolve('Cargo.lock'),
      for (final f in Directory.fromUri(native.resolve('src/')).listSync().whereType<File>()) f.uri,
    ]);
  });
}
