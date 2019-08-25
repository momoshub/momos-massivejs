'use strict';

const loader = require('../../lib/loader/functions');

describe('functions', function () {
  let db;

  before(async () => {
    db = await resetDb('functions');
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('should query for a list of functions and procedures', async () => {
    const functions = await loader(db);

    assert.isArray(functions);
    assert.isTrue(functions.length > 0);
    assert.property(functions[0], 'name');
    assert.property(functions[0], 'schema');
    assert.notProperty(functions[0], 'sql');
    assert.isTrue(functions.some(f => f.kind === 'p'));
  });

  describe('server < 11', function () {
    let realServerVersion;

    before(function () {
      realServerVersion = db.serverVersion;

      db.serverVersion = '10.2';
    });

    after(function () {
      db.serverVersion = realServerVersion;
    });

    it('should not recognize procs if the server is too old', async () => {
      const functions = await loader(db);

      assert.isArray(functions);
      assert.isTrue(functions.length > 0);
      assert.property(functions[0], 'name');
      assert.property(functions[0], 'schema');
      assert.notProperty(functions[0], 'sql');
      assert.isTrue(functions.some(f => f.kind === undefined));
    });
  });
});
