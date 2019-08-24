'use strict';

const path = require('path');
const loader = require('../../lib/loader/scripts');

describe('scripts', function () {
  let db;

  before(async () => {
    db = await resetDb('empty');
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('should query for a list of scripts', async () => {
    db.loader = {scripts: path.resolve(__dirname, '../helpers/scripts/loader')};

    const scripts = await loader(db);

    assert.isArray(scripts);
    assert.lengthOf(scripts, 1);
    assert.isTrue(scripts[0].hasOwnProperty('name'));
    assert.isTrue(scripts[0].hasOwnProperty('schema'));
    assert.isTrue(scripts[0].hasOwnProperty('sql'));
    assert.instanceOf(scripts[0].sql, pgp.QueryFile);
  });
});
