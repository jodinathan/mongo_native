# Changelog

## 0.1.0

- `MongoClient` over the official Rust driver (`mongodb` 3.10.1) through FFI: replica set
  monitoring, retryable writes and reads, connection pool.
- CRUD, `find`/`aggregate` cursors, `findOneAndUpdate`/`findOneAndDelete`, `countDocuments`,
  indexes, `runCommand` and cross-collection `bulkWrite` (MongoDB 8.0+).
- Change streams on a collection or a database, resumed across elections without a gap.
- Every failure thrown as a typed `MongoNativeException`; client timeout, server `maxTime` and
  cancellation, with `outcomeUnknown` for writes.
- Own BSON codec with strict validation.
- Build hook: prebuilt library from `hooks: user_defines`, the GitHub Release (checked by sha256)
  or a `cargo` build.

## Publishing

The release is prepared by two workflows that are only run by hand:

1. Run **release-binaries** (`workflow_dispatch`) with the version of `pubspec.yaml`. It builds the
   five libraries, creates the draft Release `v<version>` with them, `SHA256SUMS` and
   `THIRD_PARTY_LICENSES`, and opens a pull request that writes the sums into
   `lib/src/hook/checksums.dart`.
2. Merge that pull request.
3. Publish the first version by hand with `dart pub publish` (pub.dev requires it to create the
   package).
4. On the package admin page at pub.dev, enable automated publishing from GitHub Actions for
   `jodinathan/mongo_native` with the tag pattern `v{{version}}`.
5. Push the tag `v<version>` on the merged commit. The **publish** workflow checks the sums,
   publishes to pub.dev through OIDC and takes the Release out of draft. For the first version,
   already published by hand in step 3, the publish job refuses the existing version; take that
   Release out of draft with `gh release edit v<version> --draft=false`.

From then on, dependents use `mongo_native: ^<version>` instead of a git dependency.
