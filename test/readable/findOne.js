'use strict';

describe('findOne', function () {
  let db;

  before(function () {
    return resetDb('data-products-orders').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  describe('all records', function () {
    it('returns first record with findOne no args', function () {
      return db.products.findOne().then(res => assert.equal(res.id, 1));
    });
  });

  describe('primary keys', function () {
    it('findOnes by a numeric key and returns a result object', function () {
      return db.products.findOne(1).then(res => {
        assert.isObject(res);
        assert.equal(res.id, 1);
      });
    });

    it('findOnes by a string/uuid key and returns a result object', async () => {
      const order = await db.orders.findOne();
      assert.isOk(order);

      const res = await db.orders.findOne(order.id);
      assert.equal(res.id, order.id);
    });
  });

  describe('no records', function () {
    it('returns null with a primary key', function () {
      return db.products.findOne(35565).then(res => assert.isNull(res));
    });

    it('returns null with a criteria object', function () {
      return db.products.findOne({id: 35565}).then(res => assert.isNull(res));
    });
  });

  describe('options', function () {
    it('applies options', function () {
      return db.products.findOne(1, {build: true}).then(res => {
        assert.equal(res.sql, 'SELECT * FROM "products" WHERE "id" = $1 LIMIT 1');
        assert.deepEqual(res.params, [1]);
      });
    });
  });
});
