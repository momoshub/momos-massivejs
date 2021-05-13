'use strict';

const Writable = require('../../lib/writable');
const Update = require('../../lib/statement/update');

describe('Update', function () {
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
      const query = new Update(source);

      assert.equal(query.source.delimitedFullName, '"testsource"');

      assert.isFalse(query.only);
      assert.deepEqual(query.changes, []);
      assert.equal(query.predicate, 'TRUE');
      assert.deepEqual(query.returning, ['*']);
      assert.lengthOf(query.params, 0);
      assert.isFalse(query.build);
      assert.isFalse(query.document);
      assert.isUndefined(query.decompose);
      assert.isFalse(query.single);
      assert.isFalse(query.stream);
    });

    it('should apply options', function () {
      const query = new Update(source, {}, {}, {
        build: true,
        decompose: true,
        document: true,
        only: true,
        single: true,
        stream: true,
        fields: ['field1', 'field2']
      });

      assert.equal(query.source.delimitedFullName, '"testsource"');
      assert.isTrue(query.build);
      assert.isTrue(query.decompose);
      assert.isTrue(query.document);
      assert.isTrue(query.only);
      assert.isTrue(query.stream);
      assert.sameMembers(query.returning, ['"field1"', '"field2"']);
    });
  });

  describe('format', function () {
    it('should return a basic update statement for the specified changes', function () {
      const result = new Update(source, {field1: 'value1'});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1 WHERE TRUE RETURNING *');
      assert.deepEqual(result.params, ['value1']);
    });

    it('should accommodate multiple changes', function () {
      const result = new Update(source, {field1: 'value1', field2: 2});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1, "field2" = $2 WHERE TRUE RETURNING *');
      assert.deepEqual(result.params, ['value1', 2]);
    });

    it('should ignore nonexistent columns', function () {
      const result = new Update(source, {not_a_field_1: 0, field1: 'value1', field2: 2, not_a_field_2: 3});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1, "field2" = $2 WHERE TRUE RETURNING *');
      assert.deepEqual(result.params, ['value1', 2]);
    });

    it('should build a WHERE clause with criteria', function () {
      const result = new Update(source, {field1: 'value1'}, {field1: 'value2'});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1 WHERE "field1" = $2 RETURNING *');
    });

    it('should build a WHERE clause using the document generator', function () {
      const result = new Update(source, {field1: 'value1'}, {thing: 1}, {document: true});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1 WHERE "body" @> $2 RETURNING *');
    });

    it('should forestall the document generator if the criteria form a pk search', function () {
      const result = new Update(source, {field1: 'value1'}, {id: 1}, {document: true});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1 WHERE "id" = $2 RETURNING *');
    });

    it('should set ONLY', function () {
      const result = new Update(source, {field1: 'value1'}, {}, {only: true});
      assert.equal(result.format(), 'UPDATE ONLY "testsource" SET "field1" = $1 WHERE TRUE RETURNING *');
    });

    it('should restrict returned fields', function () {
      const result = new Update(source, {field1: 'value1'}, {field1: 'value2'}, {fields: ['field1', 'field2']});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = $1 WHERE "field1" = $2 RETURNING "field1", "field2"');
      assert.deepEqual(result.params, ['value1', 'value2']);
    });

    it('should build raw SQL', function () {
      const result = new Update(source, {$set: {field1: '"field1" + 1'}});
      assert.equal(result.format(), 'UPDATE "testsource" SET "field1" = "field1" + 1 WHERE TRUE RETURNING *');
    });

    it('should not allow $set and the change field to alter the same column', function () {
      assert.throws(
        () => new Update(source, {field1: 'value', $set: {field1: '"field1" + 1'}}).format(),
        'The key \'field1\' may not be defined in both the change map and the $set map.'
      );
    });
  });
});
