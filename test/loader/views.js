'use strict';

const loader = require('../../lib/loader/views');

describe('views', function () {
  let db;

  before(async () => {
    db = await resetDb('views');
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('should query for a list of views', async () => {
    db.loader = _.defaults({
      allowedSchemas: '',
      blacklist: '',
      exceptions: ''
    }, db.loader);

    const views = await loader(db);

    assert.isArray(views);
    assert.lengthOf(views, 2);
    assert.property(views[0], 'schema');
    assert.property(views[0], 'name');
    assert.deepEqual(views.map(v => v.name).sort(), [
      'vals_ending_with_e',
      'vals_starting_with_t'
    ]);
  });

  describe('server < 9.3', function () {
    let realServerVersion;

    before(function () {
      realServerVersion = db.serverVersion;

      db.serverVersion = '9.2';
    });

    after(function () {
      db.serverVersion = realServerVersion;
    });

    it('should exclude materialized views if the server is too old', async () => {
      db.loader = _.defaults({
        allowedSchemas: '',
        blacklist: '',
        exceptions: ''
      }, db.loader);

      const views = await loader(db);

      assert.isArray(views);
      assert.lengthOf(views, 1);
      assert.property(views[0], 'schema');
      assert.property(views[0], 'name');
      assert.equal(views[0].name, 'vals_starting_with_t');
    });
  });

  it('should exclude materialized views', async () => {
    db.loader = _.defaults({
      allowedSchemas: '',
      blacklist: '',
      exceptions: '',
      excludeMatViews: true
    }, db.loader);

    const views = await loader(db);

    assert.isArray(views);
    assert.lengthOf(views, 1);
    assert.property(views[0], 'schema');
    assert.property(views[0], 'name');
    assert.equal(views[0].name, 'vals_starting_with_t');
  });
});
