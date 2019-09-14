'use strict';

describe('reload', function () {
  let db;

  before(async () => {
    const initDb = await resetDb('foreign-keys');

    // reconnect with a pool size of 1 to make it easier to change runtime settings on all connections
    db = await massive({
      host: global.host,
      user: 'postgres',
      database: 'massive',
      poolSize: 1
    }, massive.loader);

    // close original connection
    initDb.instance.$pool.end();
  });

  after(() => db.instance.$pool.end());

  it('expires the compound entity cache on db.reload()', async function () {
    const a = db.alpha.join('beta');

    await db.reload();

    assert.isEmpty(db.entityCache);

    const b = db.alpha.join('beta');

    assert.notEqual(a, b);
  });

  it('defaults currentSchema to "public"', () => {
    assert.equal(db.currentSchema, 'public');
  });

  // this test case changes the search path and should remain last in the file
  it('picks up change in current schema', () => {
    return db.query('SET search_path=sch')
      .then(() => db.reload())
      .then(() => assert.equal(db.currentSchema, 'sch'));
  });
});
