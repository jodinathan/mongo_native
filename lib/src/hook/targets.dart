import 'package:code_assets/code_assets.dart';

/// Where the Releases of the prebuilt libraries live.
const kReleaseBase = 'https://github.com/jodinathan/mongo_native/releases/download';

/// The platforms with a prebuilt library: the Rust target triple and the library file name.
final kTargets = <(OS, Architecture), (String, String)>{
  (OS.macOS, Architecture.arm64): ('aarch64-apple-darwin', 'libmongo_native_ffi.dylib'),
  (OS.macOS, Architecture.x64): ('x86_64-apple-darwin', 'libmongo_native_ffi.dylib'),
  (OS.linux, Architecture.x64): ('x86_64-unknown-linux-gnu', 'libmongo_native_ffi.so'),
  (OS.linux, Architecture.arm64): ('aarch64-unknown-linux-gnu', 'libmongo_native_ffi.so'),
  (OS.windows, Architecture.x64): ('x86_64-pc-windows-msvc', 'mongo_native_ffi.dll'),
};
