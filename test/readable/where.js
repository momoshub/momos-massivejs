'use strict';

describe('where', function () {
  let db;

  before(function () {
    return resetDb('singletable').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('executes a handwritten WHERE clause', function () {
    return db.products.where('id=$1 OR id=$2', [1, 2]).then(res => assert.lengthOf(res, 2));
  });

  it('executes a handwritten WHERE clause without parameters', function () {
    return db.products.where('id=1').then(res => assert.lengthOf(res, 1));
  });

  it('executes a handwritten WHERE clause with options', function () {
    return db.products.where('id=$1 OR id=$2', [1, 2], {order: [{field: 'id', direction: 'desc'}]}).then(res => {
      assert.lengthOf(res, 2);

      assert.equal(res[0].id, 2);
      assert.equal(res[1].id, 1);
    });
  });

  it('executes a handwritten where clause with a raw param', function () {
    return db.products.where('id=$1', 1).then(res => assert.lengthOf(res, 1));
  });

  it('uses named parameters', function () {
    return db.products.where('id=${id}', {id: 1}).then(res => assert.lengthOf(res, 1));
  });

  it('applies ordering options', async function () {
    const result = await db.products.where('id=$1 OR id=$2', [1, 2], {
      order: [{field: 'id', direction: 'desc'}]
    });

    assert.lengthOf(result, 2);
    assert.equal(result[0].id, 2);
    assert.equal(result[1].id, 1);
  });

  it('voids the warranty', async function () {
    const result = await db.products.where('id=$1 OR id=$2 ORDER BY id DESC NULLS LAST LIMIT 2', [1, 2]);

    assert.lengthOf(result, 2);
    assert.equal(result[0].id, 2);
    assert.equal(result[1].id, 1);
  });

  it('breaks after voiding the warranty', function () {
    return db.products.where('id=$1 OR id=$2 ORDER BY id DESC NULLS LAST LIMIT 2', [1, 2], {
      order: [{field: 'id', direction: 'desc', nulls: 'first'}],
      limit: 2
    }).then(() => assert.fail())
      .catch(err => assert.equal(err.code, '42601'));
  });
});
