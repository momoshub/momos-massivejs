'use strict';

describe('refresh', function () {
  let db;

  beforeEach(function () {
    return resetDb('views').then(instance => db = instance);
  });

  afterEach(function () {
    return db.instance.$pool.end();
  });

  it('refreshes a materialized view', async () => {
    const before = await db.vals_ending_with_e.count({});

    assert.equal(before, 2);

    await db.vals.insert({string: 'five'});

    const afterWithoutRefresh = await db.vals_ending_with_e.count({});

    assert.equal(afterWithoutRefresh, 2);

    await db.vals_ending_with_e.refresh();

    const afterWithRefresh = await db.vals_ending_with_e.count({});

    assert.equal(afterWithRefresh, 3);
  });

  it('refreshes concurrently', async () => {
    const before = await db.vals_ending_with_e.count({});

    assert.equal(before, 2);

    await db.vals.insert({string: 'five'});

    const afterWithoutRefresh = await db.vals_ending_with_e.count({});

    assert.equal(afterWithoutRefresh, 2);

    await db.vals_ending_with_e.refresh(true);

    const afterWithRefresh = await db.vals_ending_with_e.count({});

    assert.equal(afterWithRefresh, 3);
  });

  it('rejects if called for anything other than a materialized view', function () {
    return db.vals.refresh()
      .then(() => { assert.fail(); })
      .catch(err => {
        assert.equal(err.message, '"vals" is not a materialized view');
      });
  });
});
