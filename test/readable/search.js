'use strict';

describe('search', function () {
  let db;

  before(async () => {
    db = await resetDb('data-products-users');
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('returns 4 products for term \'product\'', function () {
    return db.products.search({fields: ['name'], term: 'Product'}).then(res => {
      assert.lengthOf(res, 4);
    });
  });

  it('returns 1 products for term \'3\'', function () {
    return db.products.search({fields: ['name'], term: '3'}).then(res => {
      assert.lengthOf(res, 1);
    });
  });

  it('returns 1 Users for term \'test\'', function () {
    return db.Users.search({fields: ['Name'], term: 'test'}).then(res => {
      assert.lengthOf(res, 1);
    });
  });

  it('returns 4 products for term \'description\' using multiple fields', function () {
    return db.products.search({fields: ['Name', 'description'], term: 'description'}).then(res => {
      assert.lengthOf(res, 4);
    });
  });

  it('returns 0 products for term \'none\' using multiple fields', function () {
    return db.products.search({fields: ['Name', 'description'], term: 'none'}).then(res => {
      assert.lengthOf(res, 0);
    });
  });

  it('returns 2 products for term \'description\' using multiple fields when limit is set to 2', function () {
    return db.products.search({fields: ['Name', 'description'], term: 'description'}, {limit: 2}).then(res => {
      assert.lengthOf(res, 2);
    });
  });

  it('returns same correct element when offset is set', async () => {
    const one = await db.products.search({fields: ['Name', 'description'], term: 'description'});
    const two = await db.products.search({fields: ['Name', 'description'], term: 'description'}, {offset: 1});

    assert.equal(one[1].id, two[0].id);
  });

  it('returns results filtered by where', function () {
    return db.products.search({fields: ['description'], term: 'description', where: {'in_stock': true}}).then(res => {
      assert.lengthOf(res, 2);
    });
  });

  it('supports keyset pagination', function () {
    return db.products.search({
      fields: ['description'],
      term: 'description'
    }, {
      order: [{field: 'id', last: 2}],
      pageLength: 2
    }).then(res => {
      assert.lengthOf(res, 2);
      assert.equal(res[0].id, 3);
      assert.equal(res[1].id, 4);
    });
  });

  it('supports keyset pagination with where', function () {
    return db.products.search({
      fields: ['description'],
      term: 'description',
      where: {'price > ': 20}
    }, {
      order: [{field: 'id', last: 2}],
      pageLength: 2
    }).then(res => {
      assert.lengthOf(res, 2);
      assert.equal(res[0].id, 3);
      assert.equal(res[1].id, 4);
    });
  });

  describe('parsers', function () {
    it('uses the plain parser', function () {
      return db.products.search({
        fields: ['description'],
        term: '2 description',
        parser: 'plain'
      }).then(res => {
        assert.lengthOf(res, 1);
      });
    });

    it('uses the phrase parser', function () {
      return db.products.search({
        fields: ['description'],
        term: '2 description',
        parser: 'phrase'
      }).then(res => {
        assert.lengthOf(res, 1);
      });
    });

    it('uses the websearch parser', function () {
      return db.products.search({
        fields: ['description'],
        term: 'description -2',
        parser: 'websearch'
      }).then(res => {
        assert.lengthOf(res, 3);
      });
    });
  });

  it('requires fields or tsv', function () {
    let caught = false;

    return db.products.search({})
      .then(() => assert.fail())
      .catch(err => {
        assert.equal(err.message, 'Plan must contain a fields array or tsv string');

        caught = true;
      })
      .then(() => assert.isTrue(caught));
  });

  it('requires a term', function () {
    let caught = false;

    return db.products.search({fields: ['description']})
      .then(() => assert.fail())
      .catch(err => {
        assert.equal(err.message, 'Plan must contain a term string');

        caught = true;
      })
      .then(() => assert.isTrue(caught));
  });
});
