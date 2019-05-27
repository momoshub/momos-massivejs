'use strict';

const Readable = require('../../lib/readable');
const Writable = require('../../lib/writable');
const Select = require('../../lib/statement/select');

describe('Select', function () {
  // TODO centralize
  const source = new Writable({
    name: 'mytable',
    schema: 'public',
    columns: ['id', 'field', 'col1', 'col2', 'body'],
    pk: ['id'],
    db: {
      currentSchema: 'public',
      entityCache: {},
      jointable1: new Writable({
        name: 'jointable1',
        schema: 'public',
        columns: ['id', 'mytable_id', 'val1'],
        pk: ['id'],
        db: {currentSchema: 'public'}
      }),
      jointable2: new Writable({
        name: 'jointable2',
        schema: 'public',
        columns: ['id', 'jointable1_id', 'val2'],
        pk: ['id'],
        db: {currentSchema: 'public'}
      }),
      myschema: {
        jointable3: new Writable({
          name: 'jointable3',
          schema: 'myschema',
          columns: ['id', 'mytable_id', 'val3'],
          pk: ['id'],
          db: {currentSchema: 'public'}
        })
      }
    }
  });

  describe('ctor', function () {
    it('has defaults', function () {
      const query = new Select(source);

      assert.equal(query.fields, '*');
      assert.equal(query.generator, 'tableGenerator');
      assert.isFalse(query.single);
      assert.equal(query.order, 'ORDER BY "id"');
    });

    it('applies options', function () {
      const query = new Select(source, {}, {build: true});

      assert.isTrue(query.build);
    });

    it('generates a WHERE with a defined table', function () {
      const query = new Select(source, {field: 'val'});

      assert.equal(query.where.conditions, '"field" = $1');
      assert.deepEqual(query.where.params, ['val']);
    });

    it('generates a WHERE with an implicit table', function () {
      const query = new Select(source, {field: 'val'});

      assert.equal(query.where.conditions, '"field" = $1');
      assert.deepEqual(query.where.params, ['val']);
    });
  });

  describe('format', function () {
    it('should return a basic select', function () {
      const result = new Select(source);
      assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id"');
    });

    it('orders by position if no pk is present', function () {
      const readable = new Readable({
        name: 'mytable',
        schema: 'public',
        columns: ['id', 'field', 'col1', 'col2', 'body'],
        db: {
          currentSchema: 'public',
          entityCache: {},
          jointable1: new Writable({
            name: 'jointable1',
            schema: 'public',
            columns: ['id', 'mytable_id', 'val1'],
            pk: ['id'],
            db: {currentSchema: 'public'}
          }),
          jointable2: new Writable({
            name: 'jointable2',
            schema: 'public',
            columns: ['id', 'jointable1_id', 'val2'],
            pk: ['id'],
            db: {currentSchema: 'public'}
          }),
          myschema: {
            jointable3: new Writable({
              name: 'jointable3',
              schema: 'myschema',
              columns: ['id', 'mytable_id', 'val3'],
              pk: ['id'],
              db: {currentSchema: 'public'}
            })
          }
        }
      });

      const result = new Select(readable);
      assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY 1');
    });

    it('should add an ONLY', function () {
      const result = new Select(source, {}, {only: true});
      assert.equal(result.format(), 'SELECT * FROM ONLY "mytable" WHERE TRUE ORDER BY "id"');
    });

    describe('fields', function () {
      it('should interpolate fields', function () {
        const result = new Select(source, {}, {fields: ['col1']});
        assert.equal(result.format(), 'SELECT "col1" FROM "mytable" WHERE TRUE ORDER BY "id"');
      });

      it('should join arrays', function () {
        const result = new Select(source, {}, {fields: ['col1', 'col2']});
        assert.equal(result.format(), 'SELECT "col1","col2" FROM "mytable" WHERE TRUE ORDER BY "id"');
      });

      it('should parse JSON fields', function () {
        const result = new Select(source, {}, {
          fields: [
            'field.element',
            'field.array[0]',
            'field.array[1].nested[2].element'
          ]
        });

        assert.equal(result.format(), `SELECT "field"->>'element',"field"#>>'{array,0}',"field"#>>'{array,1,nested,2,element}' FROM "mytable" WHERE TRUE ORDER BY "id"`);
      });

      it('should alias fields in document mode', function () {
        const result = new Select(source, {}, {
          fields: ['one', 'two'],
          document: true
        });

        assert.equal(result.format(), `SELECT "body"->>'one' AS "one","body"->>'two' AS "two",id FROM "mytable" WHERE TRUE ORDER BY "id"`);
      });

      it('should add expressions', function () {
        const result = new Select(source, {}, {
          exprs: {
            colsum: 'col1 + col2',
            coldiff: 'col1 - col2'
          }
        });

        assert.equal(result.format(), 'SELECT col1 + col2 AS "colsum",col1 - col2 AS "coldiff" FROM "mytable" WHERE TRUE ORDER BY "id"');
      });

      it('should add fields and expressions', function () {
        const result = new Select(source, {}, {
          fields: ['col1', 'col2'],
          exprs: {
            colsum: 'col1 + col2',
            coldiff: 'col1 - col2'
          }
        });

        assert.equal(result.format(), 'SELECT "col1","col2",col1 + col2 AS "colsum",col1 - col2 AS "coldiff" FROM "mytable" WHERE TRUE ORDER BY "id"');
      });
    });

    describe('for update/for share', function () {
      it('adds FOR UPDATE', function () {
        const result = new Select(source, {}, {forUpdate: true});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" FOR UPDATE');
      });

      it('adds FOR SHARE', function () {
        const result = new Select(source, {}, {forShare: true});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" FOR SHARE');
      });

      it('applies limits with a FOR', function () {
        const result = new Select(source, {}, {forUpdate: true, limit: 1});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" FOR UPDATE LIMIT 1');
      });
    });

    describe('offset and limit', function () {
      it('should add an offset', function () {
        const result = new Select(source, {}, {offset: 10});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" OFFSET 10');
      });

      it('should limit single queries to one result', function () {
        const result = new Select(source, {}, {single: true});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" LIMIT 1');
      });

      it('should add a limit', function () {
        const result = new Select(source, {}, {limit: 10});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" LIMIT 10');
      });

      it('should add both offset and limit', function () {
        const result = new Select(source, {}, {offset: 10, limit: 10});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "id" OFFSET 10 LIMIT 10');
      });
    });

    describe('keyset pagination', function () {
      it('tests the last values of the sort fields', function () {
        const result = new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1',
            last: 123
          }, {
            field: 'col2',
            last: 456
          }]
        });

        assert.equal(result.pageLength, 10);
        assert.equal(result.pagination, '("col1","col2") > ($1,$2)');
        assert.equal(result.where.conditions, 'TRUE');
        assert.isEmpty(result.where.params);
        assert.deepEqual(result.params, [123, 456]);
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE AND ("col1","col2") > ($1,$2) ORDER BY "col1" ASC,"col2" ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('reverses direction depending on the first field', function () {
        const result = new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1',
            direction: 'desc',
            last: 123
          }, {
            field: 'col2',
            direction: 'asc',
            last: 456
          }]
        });

        assert.equal(result.pageLength, 10);
        assert.equal(result.pagination, '("col1","col2") < ($1,$2)');
        assert.equal(result.where.conditions, 'TRUE');
        assert.isEmpty(result.where.params);
        assert.deepEqual(result.params, [123, 456]);
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE AND ("col1","col2") < ($1,$2) ORDER BY "col1" DESC,"col2" ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('starts from the beginning', function () {
        const result = new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1'
          }, {
            field: 'col2'
          }]
        });

        assert.equal(result.pageLength, 10);
        assert.isUndefined(result.pagination);
        assert.equal(result.where.conditions, 'TRUE');
        assert.isEmpty(result.where.params);
        assert.deepEqual(result.params, []);
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE ORDER BY "col1" ASC,"col2" ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('works with pregenerated where specs', function () {
        const result = new Select(source, {
          conditions: 'col2 = $1',
          params: [1]
        }, {
          pageLength: 10,
          order: [{
            field: 'col1',
            last: 5
          }]
        });

        assert.equal(result.where.conditions, 'col2 = $1');
        assert.deepEqual(result.params, [1, 5]);
        assert.equal(result.pagination, '("col1") > ($2)');
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE col2 = $1 AND ("col1") > ($2) ORDER BY "col1" ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('applies body and type options', function () {
        const result = new Select(source, {
          col2: 'value2'
        }, {
          document: true,
          pageLength: 10,
          order: [{
            field: 'col1',
            type: 'int',
            last: 5
          }]
        });

        assert.equal(result.where.conditions, '"col2" = $1');
        assert.deepEqual(result.params, ['value2', 5]);
        assert.equal(result.pagination, '(("col1")::int) > ($2)');
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE "col2" = $1 AND (("col1")::int) > ($2) ORDER BY ("col1")::int ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('requires an order definition', function (done) {
        const result = new Select(source, {}, {pageLength: 10});

        try {
          result.format();
        } catch (err) {
          assert.equal(err.message, 'Keyset paging with pageLength requires options.order');

          done();
        }
      });

      it('does not work with offsets', function (done) {
        const result = new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1'
          }],
          offset: 10
        });

        try {
          result.format();
        } catch (err) {
          assert.equal(err.message, 'Keyset paging cannot be used with offset and limit');

          done();
        }
      });

      it('does not work with limits', function (done) {
        const result = new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1'
          }],
          limit: 10
        });

        try {
          result.format();
        } catch (err) {
          assert.equal(err.message, 'Keyset paging cannot be used with offset and limit');

          done();
        }
      });
    });

    it('assembles and adds join clauses', function () {
      const joinSource = source.join({
        'jointable1': {
          type: 'INNER',
          on: {mytable_id: 'id'},
          jt2: {
            type: 'INNER',
            relation: 'jointable2',
            on: {jointable1_id: 'jointable1.id'}
          }
        },
        'myschema.jointable3': {
          type: 'LEFT OUTER',
          on: {mytable_id: 'id'}
        }
      });

      const result = new Select(joinSource, {}, {}).format();

      assert.equal(result, [
        'SELECT "mytable"."id" AS "mytable__id",',
        '"mytable"."field" AS "mytable__field",',
        '"mytable"."col1" AS "mytable__col1",',
        '"mytable"."col2" AS "mytable__col2",',
        '"mytable"."body" AS "mytable__body",',
        '"jointable1"."id" AS "jointable1__id",',
        '"jointable1"."mytable_id" AS "jointable1__mytable_id",',
        '"jointable1"."val1" AS "jointable1__val1",',
        '"jt2"."id" AS "jt2__id",',
        '"jt2"."jointable1_id" AS "jt2__jointable1_id",',
        '"jt2"."val2" AS "jt2__val2",',
        '"jointable3"."id" AS "jointable3__id",',
        '"jointable3"."mytable_id" AS "jointable3__mytable_id",',
        '"jointable3"."val3" AS "jointable3__val3" ',
        'FROM "mytable" ',
        'INNER JOIN "jointable1" ON ("jointable1"."mytable_id" = "mytable"."id") ',
        'INNER JOIN "jointable2" AS "jt2" ON ("jt2"."jointable1_id" = "jointable1"."id") ',
        'LEFT OUTER JOIN "myschema"."jointable3" AS "jointable3" ON ("jointable3"."mytable_id" = "mytable"."id") ',
        'WHERE TRUE ORDER BY "mytable"."id"'
      ].join(''));
    });
  });
});
