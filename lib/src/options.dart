/// How to reach and talk to a deployment.
///
/// The options set here win over the same options in [uri].
final class MongoClientOptions {
  /// Creates the options of a client for [uri] (`mongodb://` or `mongodb+srv://`).
  const MongoClientOptions({
    required this.uri,
    this.appName,
    this.maxPoolSize = 32,
    this.minPoolSize = 0,
    this.maxIdleTime = const Duration(minutes: 5),
    this.connectTimeout = const Duration(seconds: 10),
    this.serverSelectionTimeout = const Duration(seconds: 15),
    this.heartbeatFrequency = const Duration(seconds: 2),
    this.operationTimeout = const Duration(seconds: 30),
    this.tls,
    this.writeConcern = MongoWriteConcern.majority,
    this.readConcern = MongoReadConcern.majority,
    this.retryWrites = true,
    this.retryReads = true,
  });

  /// The connection string, with hosts, credentials and the replica set name.
  final String uri;

  /// The name the server logs for this client.
  final String? appName;

  /// The most connections per server.
  final int maxPoolSize;

  /// The connections per server kept open even when idle.
  final int minPoolSize;

  /// How long an idle connection stays in the pool.
  final Duration maxIdleTime;

  /// How long opening a connection may take.
  final Duration connectTimeout;

  /// How long an operation waits for a suitable server (a primary, for writes).
  final Duration serverSelectionTimeout;

  /// How often each server is checked; a frozen primary is noticed within about this time.
  final Duration heartbeatFrequency;

  /// The client clock of an operation that does not pass its own `timeout`; never infinite.
  final Duration operationTimeout;

  /// The TLS files, when the deployment requires TLS.
  final MongoTlsOptions? tls;

  /// The write concern of writes that do not pass their own.
  final MongoWriteConcern writeConcern;

  /// The read concern of reads that do not pass their own.
  final MongoReadConcern readConcern;

  /// Whether a write that failed on a retryable error (election, network) is retried once.
  final bool retryWrites;

  /// Whether a read that failed on a retryable error is retried once.
  final bool retryReads;
}

/// TLS files, as PEM paths.
final class MongoTlsOptions {
  /// Creates the TLS options; with no file, the system roots validate the server.
  const MongoTlsOptions({this.caFile, this.certificateKeyFile});

  /// The certificate authorities that validate the server certificate.
  final String? caFile;

  /// The client certificate and its private key, for X.509 authentication.
  final String? certificateKeyFile;
}

/// How many members must acknowledge a write before it is answered.
final class MongoWriteConcern {
  /// Creates a write concern of [w] members, or of the majority when [majority] is true.
  const MongoWriteConcern({this.w, bool majority = false, this.journal, this.timeout}) : isMajority = majority;

  /// Acknowledged by the majority of the members, on disk.
  static const majority = MongoWriteConcern(majority: true, journal: true);

  /// Acknowledged by the primary alone.
  static const acknowledged = MongoWriteConcern(w: 1);

  /// The number of members; ignored when [isMajority] is true.
  final int? w;

  /// Whether the majority of the members must acknowledge.
  final bool isMajority;

  /// Whether the acknowledgement waits for the on-disk journal.
  final bool? journal;

  /// How long the server waits for the acknowledgements (`wtimeout`).
  final Duration? timeout;
}

/// The consistency of what a read returns.
enum MongoReadConcern {
  /// The newest data of the member, which an election may roll back.
  local,

  /// Data acknowledged by the majority, which no election rolls back.
  majority,

  /// Majority data reflecting every write acknowledged before the read started.
  linearizable,
}

/// One index of `createIndexes`.
final class MongoIndexModel {
  /// Creates the index over [keys], e.g. `{'email': 1}` or `{'at': '2dsphere'}`.
  const MongoIndexModel({required this.keys, this.name, this.unique, this.expireAfter, this.partialFilter});

  /// The fields and their kind: `1`, `-1`, `'2dsphere'`, `'text'`, `'hashed'`.
  final Map<String, Object> keys;

  /// The index name; the server derives one from [keys] when null.
  final String? name;

  /// Whether two documents may not share the indexed values.
  final bool? unique;

  /// The age after which a document is deleted (TTL index over a date field).
  final Duration? expireAfter;

  /// Only documents matching this filter enter the index.
  final Map<String, Object?>? partialFilter;
}

/// The request form of [concern].
Map<String, Object?> writeConcernRequest(MongoWriteConcern concern) => {
  'w': concern.isMajority ? 'majority' : concern.w,
  if (concern.journal != null) 'j': concern.journal,
  if (concern.timeout != null) 'wtimeoutMs': concern.timeout!.inMilliseconds,
};

/// The request form of [index].
Map<String, Object?> indexRequest(MongoIndexModel index) => {
  'keys': index.keys,
  if (index.name != null) 'name': index.name,
  if (index.unique != null) 'unique': index.unique,
  if (index.expireAfter != null) 'expireAfterSeconds': index.expireAfter!.inSeconds,
  if (index.partialFilter != null) 'partialFilter': index.partialFilter,
};
