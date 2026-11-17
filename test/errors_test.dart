import 'package:mongo_native/mongo_native.dart';
import 'package:test/test.dart';

import 'support/replset.dart';

TypeMatcher<T> _duplicate<T extends MongoNativeException>() => isA<T>()
    .having((e) => e.code, 'code', 11000)
    .having((e) => e.message, 'message', isNot(contains('dup key')))
    .having((e) => e.message, 'message', contains('email_unique'));

void main() {
  late MongoClient client;
  late MongoDatabase db;
  late MongoCollection users;

  setUpAll(() async {
    client = await connectTest();
    db = client.database(uniqueDb('errors'));
    users = db.collection('users');
    await users.createIndexes([
      const MongoIndexModel(keys: {'email': 1}, unique: true, name: 'email_unique'),
    ]);
    await users.insertOne({'_id': 1, 'email': 'ana@example.com'});
  });

  tearDownAll(() async {
    await db.runCommand({'dropDatabase': 1});
    await client.close();
  });

  test('duplicate key on insertOne throws MongoWriteException without the key values', () async {
    final error = await users
        .insertOne({'_id': 2, 'email': 'ana@example.com'})
        .then<Object?>((_) => null, onError: (Object e) => e);
    expect(error, _duplicate<MongoWriteException>());
    expect((error! as MongoWriteException).isDuplicateKey, isTrue);
    expect(error.toString(), startsWith('MongoDB insertOne on ${db.name}.users failed (code 11000'));
  });

  test('duplicate key on an upserting updateOne throws MongoWriteException', () async {
    await expectLater(
      users.updateOne(
        {'_id': 3},
        {
          r'$set': {'email': 'ana@example.com'},
        },
        upsert: true,
      ),
      throwsA(_duplicate<MongoWriteException>()),
    );
  });

  test('duplicate key on findOneAndUpdate throws MongoWriteException', () async {
    await expectLater(
      users.findOneAndUpdate(
        {'_id': 4},
        {
          r'$set': {'email': 'ana@example.com'},
        },
        upsert: true,
      ),
      throwsA(_duplicate<MongoWriteException>()),
    );
  });

  test('duplicate key in bulkWrite throws MongoBulkWriteException with each write error', () async {
    final error = await client
        .bulkWrite([
          MongoInsertOne(db.name, 'users', {'_id': 10, 'email': 'new@example.com'}),
          MongoInsertOne(db.name, 'users', {'_id': 11, 'email': 'ana@example.com'}),
        ])
        .then<Object?>((_) => null, onError: (Object e) => e);
    expect(error, _duplicate<MongoBulkWriteException>());
    final bulk = error! as MongoBulkWriteException;
    expect(bulk.writeErrors.single.index, 1);
    expect(bulk.writeErrors.single.isDuplicateKey, isTrue);
    expect(bulk.writeErrors.single.message, isNot(contains('dup key')));
    expect(bulk.partial?.inserted, 1);
  });

  test('duplicate key in insertMany throws MongoBulkWriteException', () async {
    await expectLater(
      users.insertMany([
        {'_id': 20, 'email': 'x@example.com'},
        {'_id': 21, 'email': 'ana@example.com'},
      ]),
      throwsA(
        _duplicate<MongoBulkWriteException>().having((e) => e.writeErrors.single.index, 'writeErrors[0].index', 1),
      ),
    );
  });

  test('an unsatisfiable write concern throws MongoWriteConcernException code 100', () async {
    await expectLater(
      users.insertOne({'_id': 30}, writeConcern: const MongoWriteConcern(w: 5, timeout: Duration(seconds: 1))),
      throwsA(isA<MongoWriteConcernException>().having((e) => e.code, 'code', 100)),
    );
  });

  test('a user without permission gets MongoCommandException code 13', () async {
    final password = 'reader-${DateTime.now().microsecondsSinceEpoch}';
    await db.runCommand({
      'createUser': 'mn-reader',
      'pwd': password,
      'roles': [
        {'role': 'read', 'db': db.name},
      ],
    });
    final reader = await connectTest(
      uri: replsetUri(user: 'mn-reader', password: password, authSource: db.name),
    );
    try {
      expect(await reader.database(db.name).collection('users').countDocuments({}), greaterThan(0));
      await expectLater(
        reader.database(db.name).collection('users').insertOne({'_id': 40}),
        throwsA(isA<MongoCommandException>().having((e) => e.code, 'code', 13)),
      );
    } finally {
      await reader.close();
      await db.runCommand({'dropUser': 'mn-reader'});
    }
  });

  test('a wrong password throws MongoAuthenticationException', () async {
    final wrong = await connectTest(
      uri: replsetUri(user: 'mn-test', password: 'wrong-password'),
      serverSelectionTimeout: const Duration(seconds: 5),
    );
    try {
      await expectLater(
        wrong.database(db.name).collection('users').findOne({}),
        throwsA(isA<MongoAuthenticationException>()),
      );
    } finally {
      await wrong.close();
    }
  });

  test('a failCommand error on find throws MongoCommandException with its code', () async {
    final port = await primaryPort(client);
    const appName = 'mn-errors-failpoint';
    final victim = await connectTest(appName: appName);
    await failCommand(
      port,
      {
        'failCommands': ['find'],
        'errorCode': 2,
        'appName': appName,
      },
      mode: {'times': 1},
    );
    try {
      await expectLater(
        victim.database(db.name).collection('users').findOne({}),
        throwsA(
          isA<MongoCommandException>()
              .having((e) => e.code, 'code', 2)
              .having((e) => e.codeName, 'codeName', 'BadValue'),
        ),
      );
    } finally {
      await clearFailCommand(port);
      await victim.close();
    }
  });
}
