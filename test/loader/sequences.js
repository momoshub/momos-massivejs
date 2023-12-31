'use strict';

const loader = require('../../lib/loader/sequences');

describe('sequences', function () {
  let db;

  before(async () => {
    db = await resetDb('sequences');
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('should query for a list of non-pk sequences', async () => {
    db.loader = _.defaults({allowedSchemas: '', blacklist: '', exceptions: ''}, db.loader);

    const sequences = await loader(db);

    assert.isArray(sequences);
    assert.lengthOf(sequences, 2);
    assert.property(sequences[0], 'schema');
    assert.property(sequences[0], 'name');
  });
});
