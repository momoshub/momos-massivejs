'use strict';

const Writable = require('../../lib/writable');
const Statement = require('../../lib/statement/statement');

describe('Statement', function () {
  const source = new Writable({
    name: 'testsource',
    schema: 'public',
    pk: ['id'],
    columns: ['id', 'field1', 'field2', 'string', 'boolean', 'int', 'number', 'object', 'array', 'emptyArray'],
    db: {
      currentSchema: 'public'
    }
  });

  describe('ctor', function () {
    it('should have defaults', function () {
      const query = new Statement(source);

      assert.isFalse(query.only);
      assert.deepEqual(query.returning, ['*']);
      assert.isFalse(query.build);
      assert.isFalse(query.document);
      assert.isUndefined(query.decompose);
      assert.isFalse(query.single);
      assert.isFalse(query.stream);
    });
  });

  describe('setCriteria', function () {
    it('sets criteria and params', function () {
      const statement = new Statement(source);

      assert.isUndefined(statement.conditions);
      assert.isUndefined(statement.params);

      statement.setCriteria({one: 'two'});

      assert.equal(statement.conditions, '"one" = $1');
      assert.deepEqual(statement.params, ['two']);
      assert.isFalse(statement.isPkSearch);
    });

    it('prepends initial params', function () {
      const statement = new Statement(source);

      assert.isUndefined(statement.conditions);
      assert.isUndefined(statement.params);

      statement.setCriteria({one: 'two'}, ['three']);

      assert.equal(statement.conditions, '"one" = $2');
      assert.deepEqual(statement.params, ['three', 'two']);
      assert.isFalse(statement.isPkSearch);
    });

    it('detects primitive pk searches', function () {
      const statement = new Statement(source);

      assert.isUndefined(statement.conditions);
      assert.isUndefined(statement.params);

      statement.setCriteria({id: 1});

      assert.equal(statement.conditions, '"id" = $1');
      assert.deepEqual(statement.params, [1]);
      assert.isTrue(statement.isPkSearch);
    });
  });
});
