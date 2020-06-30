'use strict';

const Writable = require('../../lib/writable');
const Delete = require('../../lib/statement/delete');

describe('Delete', function () {
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
      const query = new Delete(source);

      assert.isFalse(query.only);
      assert.equal(query.predicate, 'TRUE');
      assert.deepEqual(query.returning, ['*']);
      assert.lengthOf(query.params, 0);
      assert.isFalse(query.build);
      assert.isFalse(query.document);
      assert.isUndefined(query.decompose);
      assert.isFalse(query.single);
      assert.isFalse(query.stream);
    });
  });

  describe('format', function () {
    it('should return a basic delete statement for the specified criteria', function () {
      const result = new Delete(source);
      assert.equal(result.format(), 'DELETE FROM "testsource" WHERE TRUE RETURNING *');
    });

    it('should build a WHERE clause with criteria', function () {
      const result = new Delete(source, {field1: 'value1'});
      assert.equal(result.format(), 'DELETE FROM "testsource" WHERE "field1" = $1 RETURNING *');
    });

    it('should build a WHERE clause with a pk', function () {
      const result = new Delete(source, 1);
      assert.equal(result.format(), 'DELETE FROM "testsource" WHERE "id" = $1 RETURNING *');
    });

    it('should set ONLY', function () {
      const result = new Delete(source, {field1: 'value1'}, {only: true});
      assert.equal(result.format(), 'DELETE FROM ONLY "testsource" WHERE "field1" = $1 RETURNING *');
    });

    it('should restrict returned fields', function () {
      const result = new Delete(source, {field1: 'value1'}, {fields: ['field1', 'field2']});
      assert.equal(result.format(), 'DELETE FROM "testsource" WHERE "field1" = $1 RETURNING "field1", "field2"');
    });
  });
});
