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
    const mutex = new Mutex(db.instance.$config.promise);
    let released = false;

    mutex.acquire().then(m => {
      assert.equal(mutex, m);
      assert.isTrue(mutex.locked);

      mutex.acquire().then(m2 => {
        assert.equal(mutex, m);
        assert.equal(m, m2);
        assert.isTrue(mutex.locked);
        assert.isTrue(released);

        done();
      });

      setTimeout(() => {
        assert.isTrue(mutex.locked);

        released = true;

        mutex.release();

        assert.isFalse(mutex.locked);
      }, 1000);
    });
  });
});
