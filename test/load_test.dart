import 'package:mongo_native/src/ffi/bindings.dart';
import 'package:mongo_native/src/ffi/bridge.dart';
import 'package:test/test.dart';

void main() {
  test('the bundled library loads and speaks ABI 1', () {
    expect(const NativeBindings().abiVersion(), 1);
    expect(NativeBridge.kAbiVersion, 1);
  });

  test('cancelling an unknown request does nothing', () {
    expect(const NativeBindings().cancel(1 << 60), 0);
  });
}
