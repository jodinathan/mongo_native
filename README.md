# mongo_native

MongoDB for Dart over the official Rust driver ([`mongodb`](https://crates.io/crates/mongodb) 3.10.1)
through FFI.

The driver does the hard parts of talking to a replica set: it monitors the topology (elections,
frozen or unreachable members), retries retryable writes and reads once, resumes change streams and
keeps a real connection pool. This package gives it an asynchronous Dart API and turns every server,
network and native failure into a typed exception.

```dart
import 'package:mongo_native/mongo_native.dart';

Future<void> main() async {
  final client = await MongoClient.connect(
    const MongoClientOptions(uri: 'mongodb://app:secret@db1,db2,db3/?replicaSet=rs0&authSource=admin'),
  );
  final users = client.database('app').collection('users');
  try {
    await users.insertOne({'_id': 1, 'email': 'ana@example.com'});
  } on MongoWriteException catch (e) {
    if (!e.isDuplicateKey) rethrow;
  }
  final cursor = await users.find({'email': {r'$exists': true}}, sort: {'_id': 1}, limit: 50);
  print(await cursor.toList());
  await client.close();
}
```

## Platforms and the native library

The package ships no binary. Its build hook (`hook/build.dart`) provides the native library, in this
order:

1. **A library you point to.** In the `pubspec.yaml` at the root of your project (or workspace),
   relative to that file:

   ```yaml
   hooks:
     user_defines:
       mongo_native:
         lib: native/libmongo_native_ffi.dylib
   ```

   Environment variables do not reach build hooks (the hooks runner filters the environment), so
   this is the way to choose the library.
2. **The prebuilt library of the GitHub Release** of the package version, checked against the
   sha256 written in the package. Prebuilt libraries exist for macOS arm64 and x64, Linux x64 and
   arm64 (glibc 2.28 or newer) and Windows x64. A download whose sum does not match is deleted and
   never used.
3. **A build with `cargo`**, when Rust 1.88 or newer is installed (<https://rustup.rs>). The result
   is cached by the hooks runner, so it happens once per machine and target. Versions without
   published sums (for example a git dependency) always take this path.

When none works, the build fails with the reason of each step and these instructions.

Requires Dart 3.10 or newer.

## API

`MongoClient.connect` → `client.database(name)` → `database.collection(name)`.

| Object | Operations |
|---|---|
| `MongoClient` | `bulkWrite` (several collections in one command), `close` |
| `MongoDatabase` | `runCommand`, `listCollectionNames`, `dropCollection`, `watch` |
| `MongoCollection` | `insertOne`, `insertMany`, `updateOne`, `updateMany`, `replaceOne`, `deleteOne`, `deleteMany`, `findOne`, `find`, `findOneAndUpdate`, `findOneAndDelete`, `countDocuments`, `aggregate`, `createIndexes`, `dropIndex`, `listIndexes`, `watch` |
| `MongoCursor` | `nextBatch`, `toList`, `close` |
| `MongoChangeStream` | `next`, `resumeToken`, `close` |

Documents are `Map<String, Object?>`. BSON values map to `double`, `String`, `Map`, `List`,
`Uint8List` (generic binary), `bool`, `DateTime` (UTC), `null`, `int` (int32 or int64) and the
`BsonObjectId`, `BsonBinary`, `BsonTimestamp`, `BsonDecimal128`, `BsonRegex`, `BsonMinKey` and
`BsonMaxKey` classes. `update` arguments take a document of operators or a pipeline
(`List<Map<String, Object?>>`, needed for `$$NOW`).

Connecting does not contact the server: the first operation does, and an unreachable server or a
refused password surfaces there as an exception. Options of `MongoClientOptions` win over the same
options in the URI; the defaults are `w: majority` with journal, `readConcern: majority`, retryable
writes and reads, a 2 s heartbeat and a 30 s operation timeout.

### Change streams

```dart
final stream = await client.database('app').watch(pipeline: [
  {r'$match': {'ns.coll': 'orders'}},
]);
while (true) {
  final batch = await stream.next();          // waits up to maxAwait (500 ms)
  for (final event in batch.events) {
    print('${event.operationType} ${event.documentKey}');
  }
  saveCheckpoint(batch.resumeToken);          // present even for an empty batch
}
```

Elections and network failures are resumed without a gap and without repeating an event. A stream
that cannot continue throws and closes: `ChangeStreamHistoryLost` (286) as a
`MongoCommandException`; after an `invalidate` event (the watched collection or database was dropped
or renamed), which is delivered as the last event, the next call throws `MongoClosedException`.
Reopen with `resumeAfter` from a saved token.

## Every error throws

No operation returns an error as a value. Each failure is a subtype of the sealed
`MongoNativeException`, with `operation` (`'updateOne on app.users'`), `message` (the text of the
source, without document values: the ` dup key: { … }` part of `E11000` is cut, the index name
stays), `code`, `codeName` and the driver `labels`.

| Exception | When |
|---|---|
| `MongoWriteException` | the server refused one document (`isDuplicateKey`, validation) |
| `MongoBulkWriteException` | `bulkWrite`/`insertMany` with failed writes: `writeErrors`, `writeConcernErrors`, `partial` |
| `MongoWriteConcernException` | the write concern was not satisfied (e.g. code 100) |
| `MongoCommandException` | the server refused a command (`MaxTimeMSExpired` 50, `Unauthorized` 13, …) |
| `MongoTimeoutException` | the client clock ran out (`outcomeUnknown`) |
| `MongoCancelledException` | cancelled through a `MongoCancelToken` (`outcomeUnknown`) |
| `MongoNetworkException` | the connection failed or was cut |
| `MongoServerSelectionException` | no suitable server (a primary, for writes) within `serverSelectionTimeout` |
| `MongoAuthenticationException` | the credentials were refused |
| `MongoBsonException` | a request or reply that is not valid BSON |
| `MongoClosedException` | a call on a closed client, cursor or stream |
| `MongoInternalException` | a native panic or a library that does not load |

### Timeouts, cancellation and `outcomeUnknown`

Every operation takes `timeout` (the client clock, default `operationTimeout`, never infinite) and
`cancel`. Reads (`find`, `findOne`, `aggregate`, `countDocuments`) and `findOneAndUpdate` /
`findOneAndDelete` also take `maxTime`, sent to the server as `maxTimeMS`: without it the server gets
`timeout` and the client waits one second more, so the server's `MaxTimeMSExpired` arrives first.

When the client clock runs out or the operation is cancelled, the server may or may not have applied
it. `outcomeUnknown` says which:

| Operation | `outcomeUnknown` |
|---|---|
| `insert*`, `update*`, `replaceOne`, `delete*`, `bulkWrite`, `findOneAndUpdate`, `findOneAndDelete`, `createIndexes`, `dropIndex`, `dropCollection` | `true` |
| `find`, `findOne`, `aggregate`, `countDocuments`, `listIndexes`, `listCollectionNames`, cursor and stream reads | `false` |
| `runCommand` | `!readOnly` (declare `readOnly: true` for `hello`, `ping`, `buildInfo`, …) |

`runCommand` sends the command as given; put `maxTimeMS` in it when you want a server clock.

`MongoClient.close()` refuses new calls, ends the calls in flight with `MongoClosedException` (or
their result, when it arrived first), closes the client's cursors and streams, and completes after
all of them. An idle open client does not keep the process alive.

## Requirements and limits

- MongoDB 4.2 or newer for the CRUD operations; `bulkWrite` (the cross-collection command) needs
  MongoDB 8.0 or newer, and older servers refuse it with their own message.
- Change streams need a replica set or a sharded cluster.
- TLS reads PEM files only (`caFile`, `certificateKeyFile`).
- Not included: sessions and multi-document transactions, GridFS, client-side field level
  encryption, Atlas Search (`$search`).

## License

MIT. The native library bundles Rust crates under Apache-2.0, MIT and other permissive licenses,
listed in [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES).
