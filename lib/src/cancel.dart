/// Cancels the operations it was passed to.
///
/// One token can serve several operations; cancelling it cancels every one still running. A
/// cancelled operation throws `MongoCancelledException`.
final class MongoCancelToken {
  /// Creates a token that is not cancelled.
  MongoCancelToken();

  bool _cancelled = false;
  final _listeners = <void Function()>{};

  /// Whether [cancel] was called.
  bool get isCancelled => _cancelled;

  /// Asks every operation still running with this token to stop; later calls do nothing.
  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    final listeners = _listeners.toList();
    _listeners.clear();
    for (final listener in listeners) {
      listener();
    }
  }
}

/// Runs [onCancel] when [token] is cancelled (at once if it already was); returns the function that
/// detaches it.
void Function() attachCancel(MongoCancelToken token, void Function() onCancel) {
  if (token._cancelled) {
    onCancel();
    return () {};
  }
  token._listeners.add(onCancel);
  return () => token._listeners.remove(onCancel);
}
