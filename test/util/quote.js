'use strict';

const quote = require('../../lib/util/quote');

describe('quote', function () {
  it('quotes a value', function () {
    assert.equal(quote('hi'), '"hi"');
  });
});
