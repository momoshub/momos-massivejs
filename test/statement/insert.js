'use strict';

const Writable = require('../../lib/writable');
const Insert = require('../../lib/statement/insert');

describe('Insert', function () {
  const source = new Writable({
    name: 'testsource',
    schema: 'public',
    columns: ['id', 'field1', 'field2', 'string', 'boolean', 'int', 'number', 'object', 'array', 'emptyArray'],
    pk: ['id'],
    db: {
      currentSchema: 'public'
    }
  });

  describe('ctor', function () {
    it('should have defaults', function () {
      const query = new Insert(source, {field1: 'val'});

      assert.equal(query.source.delimitedFullName, '"testsource"');

      assert.isFalse(query.only);
      assert.deepEqual(query.records, [{field1: 'val'}]);
      assert.deepEqual(query.returning, ['*']);
      assert.deepEqual(query.params, ['val']);
      assert.isFalse(query.build);
      assert.isFalse(query.document);
      assert.isUndefined(query.decompose);
      assert.isTrue(query.single);
      assert.isFalse(query.stream);
    });

    it('detects record arrays', function () {
      const query = new Insert(source, [{field1: 'val1'}, {field1: 'val2'}]);

      assert.equal(query.source.delimitedFullName, '"testsource"');

      assert.isFalse(query.only);
      assert.deepEqual(query.records, [{field1: 'val1'}, {field1: 'val2'}]);
      assert.deepEqual(query.returning, ['*']);
      assert.deepEqual(query.params, ['val1', 'val2']);
      assert.isFalse(query.build);
      assert.isFalse(query.document);
      assert.isUndefined(query.decompose);
      assert.isFalse(query.single);
      assert.isFalse(query.stream);
    });

    it('should process columns and parameters', function () {
      const query = new Insert(source, {
        string: 'hi',
        boolean: true,
        int: 123,
        number: 456.78,
        object: {field: 'value'},
        array: [1, 2, 3],
        emptyArray: []
      });

      assert.lengthOf(query.columns, 7);
      assert.deepEqual(query.columns, ['string', 'boolean', 'int', 'number', 'object', 'array', 'emptyArray']);
      assert.lengthOf(query.params, 7);
      assert.deepEqual(query.params, ['hi', true, 123, 456.78, {field: 'value'}, [1, 2, 3], []]);
    });
  });

  describe('compileFieldSet', function () {
    it('creates a set of field names', function () {
      const insert = new Insert(source, {field1: 'value1'});
      const fields = insert.compileFieldSet([{
        one: 'two',
        three: 'four'
      }, {
        five: 'six'
      }, {
        five: 'seven',
        eight: 'nine'
      }]);

      assert.sameMembers(fields, ['one', 'three', 'five', 'eight']);
    });
  });

  describe('format', function () {
    it('should return a basic update statement for the specified changes', function () {
      const result = new Insert(source, {field1: 'value1'});
      assert.equal(result.format(), 'INSERT INTO "testsource" ("field1") VALUES ($1) RETURNING *');
      assert.deepEqual(result.params, ['value1']);
    });

    it('should join fields and values with commas', function () {
      const result = new Insert(source, {field1: 'value1', field2: 2});
      assert.equal(result.format(), 'INSERT INTO "testsource" ("field1", "field2") VALUES ($1, $2) RETURNING *');
      assert.deepEqual(result.params, ['value1', 2]);
    });

    it('should handle multiple records', function () {
      const result = new Insert(source, [{field1: 'value1', field2: 2}, {field1: 'value2', field2: 3}]);
      assert.equal(result.format(), 'INSERT INTO "testsource" ("field1", "field2") VALUES ($1, $2), ($3, $4) RETURNING *');
      assert.deepEqual(result.params, ['value1', 2, 'value2', 3]);
    });

    it('should handle multiple records with fields out of order', function () {
      const result = new Insert(source, [{field1: 'value1', field2: 2}, {field2: 3, field1: 'value2'}]);
      assert.equal(result.format(), 'INSERT INTO "testsource" ("field1", "field2") VALUES ($1, $2), ($3, $4) RETURNING *');
      assert.deepEqual(result.params, ['value1', 2, 'value2', 3]);
    });

    it('should combine keys of partial records', function () {
      const result = new Insert(source, [{field1: 'value1'}, {field2: 'value2'}]);
      assert.equal(result.format(), 'INSERT INTO "testsource" ("field1", "field2") VALUES ($1, $2), ($3, $4) RETURNING *');
      assert.deepEqual(result.params, ['value1', undefined, undefined, 'value2']);
    });

    it('should restrict returned fields', function () {
      const result = new Insert(source, {field1: 'value1'}, {fields: ['field1', 'field2']});
      assert.equal(result.format(), 'INSERT INTO "testsource" ("field1") VALUES ($1) RETURNING "field1", "field2"');
      assert.deepEqual(result.params, ['value1']);
    });

    describe('onConflict', function () {
      it('handles old onConflictIgnore', function () {
        const result = new Insert(source, {field1: 'value1'}, {onConflictIgnore: true});
        assert.equal(result.format(), 'INSERT INTO "testsource" ("field1") VALUES ($1) ON CONFLICT DO NOTHING RETURNING *');
        assert.deepEqual(result.params, ['value1']);
      });

      it('handles old onConflictUpdate', function () {
        const result = new Insert(source, {field1: 'value1'}, {onConflictUpdate: ['id']});
        assert.equal(result.format(), 'INSERT INTO "testsource" ("field1") VALUES ($1) ON CONFLICT ("id") DO UPDATE SET "field1" = EXCLUDED."field1" RETURNING *');
        assert.deepEqual(result.params, ['value1']);
      });

      it('throws if overspecified', function () {
        assert.throws(() => new Insert(source, {field1: 'value1'}, {
          onConflictIgnore: true,
          onConflictUpdate: ['id'],
          onConflict: {
            target: ['id']
          }
        }).format(), 'The "onConflictIgnore", "onConflictUpdate", and "onConflict" options are mutually exclusive');
      });

      it('throws without an action', function () {
        assert.throws(() => new Insert(source, {field1: 'value1'}, {
          onConflict: {
            target: ['id']
          }
        }).format(), 'onConflict must specify an action of ignore or update');
      });

      it('handles a basic onConflict update', function () {
        const result = new Insert(source, {field1: 'value1'}, {
          onConflict: {
            action: 'update',
            target: 'id'
          }
        });
        assert.equal(result.format(), 'INSERT INTO "testsource" ("field1") VALUES ($1) ON CONFLICT ("id") DO UPDATE SET "field1" = EXCLUDED."field1" RETURNING *');
        assert.deepEqual(result.params, ['value1']);
      });

      it('handles onConflict update targeting multiple fields', function () {
        const result = new Insert(source, {
          field1: 'value1',
          object: 'value2'
        }, {
          onConflict: {
            action: 'update',
            target: ['id', 'field2']
          }
        });

        assert.equal(result.format(), 'INSERT INTO "testsource" ("field1", "object") VALUES ($1, $2) ON CONFLICT ("id", "field2") DO UPDATE SET "field1" = EXCLUDED."field1", "object" = EXCLUDED."object" RETURNING *');
        assert.deepEqual(result.params, ['value1', 'value2']);
      });

      it('handles onConflict update with fields and exclusions', function () {
        const result = new Insert(source, {
          field1: 'value1',
          object: 'value2'
        }, {
          onConflict: {
            action: 'update',
            target: ['id', 'field2'],
            exclude: ['object']
          }
        });

        assert.equal(result.format(), 'INSERT INTO "testsource" ("field1", "object") VALUES ($1, $2) ON CONFLICT ("id", "field2") DO UPDATE SET "field1" = EXCLUDED."field1" RETURNING *');
        assert.deepEqual(result.params, ['value1', 'value2']);
      });
    });

    describe('deep insert', function () {
      it('should create junction queries', function () {
        const result = new Insert(
          source,
          {
            field1: 'value1',
            junction_one: [{
              j1fk: 10,
              source_id: undefined,
              j1field: 'something'
            }],
            junction_many: [{
              source_id_another_name: undefined,
              j2fk: 101,
              j2field: 'j2f1'
            }, {
              source_id_another_name: undefined,
              j2fk: 102,
              j2field: null
            }],
            'junction.in_schema': [{
              source_id: undefined,
              jsfk: 111,
              jsfield: 'abc'
            }]
          }, {
            deepInsert: true
          }
        );

        assert.equal(result.format(), [
          'WITH inserted AS (INSERT INTO "testsource" ("field1") VALUES ($1) RETURNING *), ',
          'q_0_0 AS (INSERT INTO "junction_one" ("source_id", "j1fk", "j1field") SELECT "id", $2, $3 FROM inserted), ',
          'q_1_0 AS (INSERT INTO "junction_many" ("source_id_another_name", "j2fk", "j2field") SELECT "id", $4, $5 FROM inserted), ',
          'q_1_1 AS (INSERT INTO "junction_many" ("source_id_another_name", "j2fk", "j2field") SELECT "id", $6, $7 FROM inserted), ',
          'q_2_0 AS (INSERT INTO "junction"."in_schema" ("source_id", "jsfk", "jsfield") SELECT "id", $8, $9 FROM inserted) ',
          'SELECT * FROM inserted'
        ].join(''));
        assert.deepEqual(result.params, ['value1', 10, 'something', 101, 'j2f1', 102, null, 111, 'abc']);
      });

      it('should not create junction queries when deepInsert is explicitly disabled', function () {
        const result = new Insert(
          source,
          {
            field1: 'value1',
            junction_one: [{
              j1fk: 10,
              source_id: undefined,
              j1field: 'something'
            }]
          },
          {deepInsert: false}
        );

        assert.equal(result.format(), 'INSERT INTO "testsource" ("field1") VALUES ($1) RETURNING *');
        assert.deepEqual(result.params, ['value1']);
      });

      it('should throw when trying to create junction queries for multiple records', function () {
        assert.throws(() => new Insert(
          source,
          [{
            field1: 'value1',
            junction_one: [{
              j1fk: 10,
              j1field: 'something'
            }]
          }, {
            field1: 'value2',
            junction_one: [{
              j1fk: 20,
              j1field: 'something else'
            }]
          }], {
            deepInsert: true
          }), 'Multi-table or deep insert is only supported for single records.');
      });

      it('should throw for non-array junctions', function () {
        const x = new Insert(
          source, {
            field1: 'value1',
            not_a_junction: 'q'
          }, {
            deepInsert: true
          }
        );

        assert.throws(x.format.bind(x), 'Dependent records in a deep or multi-table insert must be supplied as arrays.');
      });
    });
  });
});
