'use strict';

describe('serverAtLeast', function () {
  let db;
  const schemaName = 'spec';

  before(function () {
    return resetDb('empty').then(instance => db = instance);
  });

  beforeEach(function () {
    return db.createSchema(schemaName);
  });

  after(function () {
    return db.dropSchema(schemaName, {cascade: true}).then(() => {
      return db.instance.$pool.end();
    });
  });

  it('automatically detects server version', function () {
    assert.isOk(db.serverVersion);
    assert.match(db.serverVersion, /^\d+\.\d+/);
  });

  it('tests whether the version meets a target', function () {
    assert.isTrue(db.serverAtLeast(db.serverVersion));

    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.2.3'));
  });

  it('tests whether the version exceeds a target', function () {
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.2.2'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.1.3'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '0.999.999'));
  });

  it('tests whether the version fails to meet a target', function () {
    assert.isFalse(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.2.4'));
    assert.isFalse(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.3.3'));
    assert.isFalse(db.serverAtLeast.call({serverVersion: '1.2.3'}, '2.0.0'));
  });

  it('compares longer serverVersions to shorter targets', function () {
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '0'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.2'));
    assert.isFalse(db.serverAtLeast.call({serverVersion: '1.2.3'}, '1.3'));
    assert.isFalse(db.serverAtLeast.call({serverVersion: '1.2.3'}, '2'));
  });

  it('compares shorter serverVersions to longer targets', function () {
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2'}, '1.2.3'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2'}, '1.1.3'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2'}, '0.0.1'));
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2'}, '0.0.0.999'));
  });

  it('resolves zeroes', function () {
    assert.isTrue(db.serverAtLeast.call({serverVersion: '1.2'}, '1.2.0'));
  });
});
