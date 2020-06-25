'use strict';

const Mutex = require('../../lib/util/mutex');

describe('Mutex', function () {
  let db;

  before(function () {
    return resetDb('empty').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('locks', function (done) {
    const m = new Mutex(db.instance.$config.promise);
    let released = false;

    m.acquire().then(() => {
      assert.isTrue(m.locked);

      m.acquire().then(() => {
        assert.isTrue(m.locked);
        assert.isTrue(released);

        done();
      });

      setTimeout(() => {
        assert.isTrue(m.locked);

        released = true;

        m.release();

        assert.isFalse(m.locked);
      }, 1000);
    });
  });
});
