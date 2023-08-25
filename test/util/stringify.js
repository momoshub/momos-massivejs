'use strict';

const stringify = require('../../lib/util/stringify');

describe('stringify', function () {
  it('stringifies', function () {
    assert.strictEqual(stringify(1), '1');
  });

  it('stringifies arrays', function () {
    const arr = stringify([1.23, 4, true]);
    assert.deepEqual(arr, ['1.23', '4', 'true']);
    assert.isString(arr[0]);
    assert.isString(arr[1]);
    assert.isString(arr[2]);
  });
});
