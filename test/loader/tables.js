'use strict';

const loader = require('../../lib/loader/tables');

describe('tables', function () {
  let db;

  before(async () => {
    db = await resetDb('updatables');
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('should query for a list of tables', async () => {
    db.loader = _.defaults({allowedSchemas: '', blacklist: '', exceptions: ''}, db.loader);
    const tables = await loader(db);

    assert.isArray(tables);
    assert.lengthOf(tables, 6);
    assert.property(tables[0], 'schema');
    assert.property(tables[0], 'name');
    assert.property(tables[0], 'parent');
    assert.property(tables[0], 'pk');
  });

  it('should ignore null keys in the pk property', async () => {
    db.loader = _.defaults({whitelist: 'no_pk'}, db.loader);
    const tables = await loader(db);

    assert.lengthOf(tables, 1);

    assert.equal(tables[0].name, 'no_pk');
    assert.isNull(tables[0].pk);
  });
});
