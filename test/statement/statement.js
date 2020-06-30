'use strict';

const Readable = require('../../lib/readable');
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

  describe('isPkSearch', function () {
    it('should accept simple criteria', function () {
      assert.isTrue(new Statement(source).isPkSearch({id: 1}));
    });

    it('should accept complex criteria', function () {
      assert.isTrue(new Statement(source).isPkSearch({'id >=': 1}));
    });

    it('catches columns with similar names', function () {
      assert.isFalse(new Statement(source).isPkSearch({identifier: 1}));
      assert.isFalse(new Statement(source).isPkSearch({id_entifier: 1}));
    });
  });

  describe('setCriteria', function () {
    it('sets criteria and params', function () {
      const statement = new Statement(source);

      assert.isUndefined(statement.predicate);
      assert.isUndefined(statement.params);

      statement.setCriteria({one: 'two'});

      assert.equal(statement.predicate, '"one" = $1');
      assert.deepEqual(statement.params, ['two']);
    });

    it('prepends initial params', function () {
      const statement = new Statement(source);

      assert.isUndefined(statement.predicate);
      assert.isUndefined(statement.params);

      statement.setCriteria({one: 'two'}, ['three']);

      assert.equal(statement.predicate, '"one" = $2');
      assert.deepEqual(statement.params, ['three', 'two']);
    });

    it('detects primitive pk searches', function () {
      const statement = new Statement(source);

      assert.isUndefined(statement.predicate);
      assert.isUndefined(statement.params);

      statement.setCriteria({id: 1});

      assert.equal(statement.predicate, '"id" = $1');
      assert.deepEqual(statement.params, [1]);
    });

    it('throws if a primitive pk search on an entity without pk', function () {
      const view = new Readable({
        name: 'testsource',
        schema: 'public',
        columns: ['id', 'field1', 'field2', 'string', 'boolean', 'int', 'number', 'object', 'array', 'emptyArray'],
        db: {
          currentSchema: 'public'
        }
      });

      assert.throws(() => {
        new Statement(view).setCriteria(1);
      }, '"testsource" doesn\'t have a primary key.');
    });
  });
});
