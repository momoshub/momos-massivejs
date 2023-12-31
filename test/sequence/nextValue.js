'use strict';

describe('nextValue', function () {
  let db;

  before(function () {
    return resetDb('sequences').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('gets the next value for a sequence', async () => {
    const lastValue = await db.one_counter.lastValue();
    const nextValue = await db.one_counter.nextValue();

    assert.isAbove(Number(nextValue), Number(lastValue));
  });
});
