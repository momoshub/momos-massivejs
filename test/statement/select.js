'use strict';

const Readable = require('../../lib/readable');
const Writable = require('../../lib/writable');
const Select = require('../../lib/statement/select');

describe('Select', function () {
  // TODO centralize
  const source = new Writable({
    name: 'mytable',
    schema: 'public',
    columns: ['id', 'field', 'col1', 'col2', 'body', 'timestamp', 'timestamptz', 'date'],
    types: {
      id: undefined,
      field: undefined,
      col1: undefined,
      col2: undefined,
      body: undefined,
      timestamp: 'timestamp without time zone',
      timestamptz: 'timestamp with time zone',
      date: 'date'
    },
    pk: ['id'],
    db: {
      currentSchema: 'public',
      entityCache: {},
      jointable1: new Writable({
        name: 'jointable1',
        schema: 'public',
        columns: ['id', 'mytable_id', 'val1', 'timestamp', 'date'],
        types: {
          id: undefined,
          mytable_id: undefined,
          val1: undefined,
          timestamp: 'timestamp without time zone',
          date: 'date'
        },
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
      const query = new Select(source, {});

      assert.isFalse(query.only);
      assert.deepEqual(query.selectList, ['*']);
      assert.equal(query.predicate, 'TRUE');
      assert.lengthOf(query.order, 0);
      assert.isUndefined(query.offset);
      assert.isUndefined(query.limit);
      assert.isUndefined(query.pageLength);
      assert.isUndefined(query.lock);
      assert.lengthOf(query.params, 0);
      assert.isFalse(query.build);
      assert.isFalse(query.document);
      assert.isUndefined(query.decompose);
      assert.isFalse(query.single);
      assert.isFalse(query.stream);
    });

    it('applies options', function () {
      const query = new Select(source, {}, {build: true});

      assert.isTrue(query.build);
    });

    it('generates a WHERE with a defined table', function () {
      const query = new Select(source, {field: 'val'});

      assert.equal(query.predicate, '"field" = $1');
      assert.deepEqual(query.params, ['val']);
    });

    it('generates a WHERE with an implicit table', function () {
      const query = new Select(source, {field: 'val'});

      assert.equal(query.predicate, '"field" = $1');
      assert.deepEqual(query.params, ['val']);
    });

    it('should build an order clause from an array of sort criteria', function () {
      const query = new Select(source, {}, {
        order: [
          {field: 'col1'},
          {expr: 'col1 + col2'}
        ]
      });

      assert.equal(query.order, '"col1" ASC,col1 + col2 ASC');
    });

    it('should apply directions', function () {
      const query = new Select(source, {}, {
        order: [
          {field: 'col1', direction: 'desc'},
          {expr: 'col1 + col2', direction: 'asc'}
        ]
      });

      assert.equal(query.order, '"col1" DESC,col1 + col2 ASC');
    });

    it('should be case-insensitive about directions', function () {
      const query = new Select(source, {}, {
        order: [
          {field: 'col1', direction: 'DESC'},
          {expr: 'col1 + col2', direction: 'ASC'}
        ]
      });

      assert.equal(query.order, '"col1" DESC,col1 + col2 ASC');
    });

    it('should apply null positioning', function () {
      const query = new Select(source, {}, {
        order: [
          {field: 'col1', direction: 'desc', nulls: 'last'},
          {expr: 'col1 + col2', direction: 'asc', nulls: 'first'}
        ]
      });

      assert.equal(query.order, '"col1" DESC NULLS LAST,col1 + col2 ASC NULLS FIRST');
    });

    it('should be case-insensitive about null positioning', function () {
      const query = new Select(source, {}, {
        order: [
          {field: 'col1', direction: 'DESC', nulls: 'last'},
          {expr: 'col1 + col2', direction: 'ASC', nulls: 'first'}
        ]
      });

      assert.equal(query.order, '"col1" DESC NULLS LAST,col1 + col2 ASC NULLS FIRST');
    });

    it('should apply both cast type and direction', function () {
      const query = new Select(source, {}, {
        order: [
          {field: 'col1', type: 'int', direction: 'desc'},
          {expr: 'col1 + col2', type: 'text', direction: 'asc'}
        ]
      });

      assert.equal(query.order, '("col1")::int DESC,(col1 + col2)::text ASC');
    });
  });

  describe('buildSelectList', function () {
    it('fills in *', function () {
      const query = new Select(source, {});

      assert.deepEqual(query.buildSelectList(), ['*']);
    });

    it('errors if nothing is explicitly passed', function () {
      const query = new Select(source, {});

      assert.throws(() => query.buildSelectList(null, {}), 'At least one of fields or exprs, if supplied, must define a field or expression to select.');
      assert.throws(() => query.buildSelectList([], null), 'At least one of fields or exprs, if supplied, must define a field or expression to select.');
      assert.throws(() => query.buildSelectList([], {}), 'At least one of fields or exprs, if supplied, must define a field or expression to select.');
    });

    it('should quote fields', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList(['col1']);

      assert.deepEqual(list, ['"col1"']);
    });

    it('should quote multiple fields', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList(['col1', 'col2']);

      assert.deepEqual(list, ['"col1"', '"col2"']);
    });

    it('should parse JSON fields', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList([
        'field.element',
        'field.array[0]',
        'field.array[1].nested[2].element'
      ]);

      assert.deepEqual(list, [
        '"field"->>\'element\'',
        '"field"#>>\'{array,0}\'',
        '"field"#>>\'{array,1,nested,2,element}\''
      ]);
    });

    it('should add id and alias fields in document mode', function () {
      const query = new Select(source, {}, {document: true});
      const list = query.buildSelectList(['one', 'two']);

      assert.deepEqual(list, [
        '"id"',
        '"body"->>\'one\' AS "one"',
        '"body"->>\'two\' AS "two"'
      ]);
    });

    it('aliases fields', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList({
        one_aliased: 'one',
        two_aliased: 'two',
        json_aliased: 'field.array[0].element'
      });

      assert.deepEqual(list, [
        '"one" AS "one_aliased"',
        '"two" AS "two_aliased"',
        '"field"#>>\'{array,0,element}\' AS "json_aliased"'
      ]);
    });

    it('aliases fields with star', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList({
        '*': true,
        one_aliased: 'col1',
        two_aliased: 'col2',
        json_aliased: 'field.array[0].element'
      });

      assert.sameMembers(list, [
        '"id"',
        '"col1" AS "one_aliased"',
        '"col2" AS "two_aliased"',
        '"field"',
        '"field"#>>\'{array,0,element}\' AS "json_aliased"',
        '"body"',
        '"timestamp"',
        '"timestamptz"',
        '"date"'
      ]);
    });

    it('should add expressions', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList([], {
        colsum: 'col1 + col2',
        coldiff: 'col1 - col2'
      });

      assert.deepEqual(list, [
        'col1 + col2 AS "colsum"',
        'col1 - col2 AS "coldiff"'
      ]);
    });

    it('should add fields and expressions', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList(['col1', 'col2'], {
        colsum: 'col1 + col2',
        coldiff: 'col1 - col2'
      });

      assert.deepEqual(list, [
        '"col1"',
        '"col2"',
        'col1 + col2 AS "colsum"',
        'col1 - col2 AS "coldiff"'
      ]);
    });

    it('should add aliased fields and expressions', function () {
      const query = new Select(source, {});
      const list = query.buildSelectList({
        one_aliased: 'one',
        two_aliased: 'two',
        json_aliased: 'field.array[0].element'
      }, {
        colsum: 'col1 + col2',
        coldiff: 'col1 - col2'
      });

      assert.deepEqual(list, [
        '"one" AS "one_aliased"',
        '"two" AS "two_aliased"',
        '"field"#>>\'{array,0,element}\' AS "json_aliased"',
        'col1 + col2 AS "colsum"',
        'col1 - col2 AS "coldiff"'
      ]);
    });
  });

  describe('buildOrderExpression', function () {
    const query = new Select(source, {});

    it('throws if an expression is null or undefined', function () {
      assert.throws(() => query.buildOrderExpression(null));
      assert.throws(() => query.buildOrderExpression(undefined));
    });

    it('throws if no field or expr is supplied', function () {
      assert.throws(() => query.buildOrderExpression({type: 'int'}), 'Missing order field or expr.');
    });

    it('quotes fields', function () {
      assert.equal(query.buildOrderExpression({field: 'col1'}), `"col1"`);
    });

    it('does not quote exprs', function () {
      assert.equal(query.buildOrderExpression({expr: 'col1 + col2'}), 'col1 + col2');
    });

    it('applies explicit cast types', function () {
      assert.equal(query.buildOrderExpression({field: 'col1', type: 'int'}), '("col1")::int');
      assert.equal(query.buildOrderExpression({expr: 'col1 + col2', type: 'text'}), '(col1 + col2)::text');
    });

    it('quotes and applies implicit cast types to fields', function () {
      assert.equal(query.buildOrderExpression({field: 'col1::int'}), '"col1"::int');
    });

    it('returns a body field with useBody', function () {
      assert.equal(query.buildOrderExpression({field: 'col1'}, true), '"body"->\'col1\'');
    });

    it('returns a body field using as-text operations with useBody and an explicit type', function () {
      assert.equal(query.buildOrderExpression({field: 'col1', type: 'int'}, true), '("body"->>\'col1\')::int');
    });

    it('ignores useBody with an expr', function () {
      assert.equal(query.buildOrderExpression({expr: 'col1 + col2'}, true), 'col1 + col2');
    });

    it('processes JSON arrays', function () {
      assert.equal(query.buildOrderExpression({field: 'jsonarray[1]'}), '"jsonarray"->1');
    });

    it('processes complex JSON paths', function () {
      assert.equal(query.buildOrderExpression({field: 'complex.element[0].with.nested.properties'}), `"complex"#>'{element,0,with,nested,properties}'`);
    });
  });

  describe('format', function () {
    it('should return a basic select', function () {
      const result = new Select(source, {});
      assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE');
    });

    it('should join field arrays', function () {
      const result = new Select(source, {}, {fields: ['col1', 'col2']});
      assert.equal(result.format(), 'SELECT "col1","col2" FROM "mytable" WHERE TRUE');
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

      const result = new Select(readable, {});
      assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE');
    });

    it('adds DISTINCT', function () {
      const result = new Select(source, {}, {distinct: true});
      assert.equal(result.format(), 'SELECT DISTINCT * FROM "mytable" WHERE TRUE');
    });

    it('should add an ONLY', function () {
      const result = new Select(source, {}, {only: true});
      assert.equal(result.format(), 'SELECT * FROM ONLY "mytable" WHERE TRUE');
    });

    describe('for update/for share', function () {
      it('adds FOR UPDATE', function () {
        const result = new Select(source, {}, {forUpdate: true});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR UPDATE');
      });

      it('adds FOR SHARE', function () {
        const result = new Select(source, {}, {forShare: true});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR SHARE');
      });

      it('parses explicit lock option FOR UPDATE', function () {
        const result = new Select(source, {}, {lock: {strength: 'UPDATE'}});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR UPDATE');
      });

      it('parses explicit lock option FOR SHARE', function () {
        const result = new Select(source, {}, {lock: {strength: 'SHARE'}});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR SHARE');
      });

      it('parses explicit lock option FOR NO KEY UPDATE', function () {
        const result = new Select(source, {}, {lock: {strength: 'NO KEY UPDATE'}});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR NO KEY UPDATE');
      });

      it('parses explicit lock option FOR KEY SHARE', function () {
        const result = new Select(source, {}, {lock: {strength: 'KEY SHARE'}});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR KEY SHARE');
      });

      it('parses explicit lock option NOWAIT', function () {
        const result = new Select(source, {}, {lock: {strength: 'UPDATE', lockedRows: 'NOWAIT'}});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR UPDATE NOWAIT');
      });

      it('parses explicit lock option SKIP LOCKED', function () {
        const result = new Select(source, {}, {lock: {strength: 'UPDATE', lockedRows: 'SKIP LOCKED'}});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR UPDATE SKIP LOCKED');
      });

      it('applies limits with a FOR', function () {
        const result = new Select(source, {}, {forUpdate: true, limit: 1});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE FOR UPDATE LIMIT 1');
      });
    });

    describe('offset and limit', function () {
      it('should add an offset', function () {
        const result = new Select(source, {}, {offset: 10});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE OFFSET 10');
      });

      it('should limit single queries to one result', function () {
        const result = new Select(source, {}, {single: true});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE LIMIT 1');
      });

      it('should add a limit', function () {
        const result = new Select(source, {}, {limit: 10});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE LIMIT 10');
      });

      it('should add both offset and limit', function () {
        const result = new Select(source, {}, {offset: 10, limit: 10});
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE TRUE OFFSET 10 LIMIT 10');
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
        assert.equal(result.predicate, 'TRUE');
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
        assert.equal(result.predicate, 'TRUE');
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
        assert.equal(result.predicate, 'TRUE');
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

        assert.equal(result.predicate, 'col2 = $1');
        assert.deepEqual(result.params, [1, 5]);
        assert.equal(result.pagination, '("col1") > ($2)');
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE col2 = $1 AND ("col1") > ($2) ORDER BY "col1" ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('applies body and type options', function () {
        const result = new Select(source, {
          col2: 'value2'
        }, {
          pageLength: 10,
          order: [{
            field: 'col1',
            type: 'int',
            last: 5
          }]
        });

        assert.equal(result.predicate, '"col2" = $1');
        assert.deepEqual(result.params, ['value2', 5]);
        assert.equal(result.pagination, '(("col1")::int) > ($2)');
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE "col2" = $1 AND (("col1")::int) > ($2) ORDER BY ("col1")::int ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('is compatible with document queries', function () {
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

        assert.equal(result.predicate, '"body" @> $1');
        assert.deepEqual(result.params, [JSON.stringify({col2: 'value2'}), 5]);
        assert.equal(result.pagination, '(("col1")::int) > ($2)');
        assert.equal(result.format(), 'SELECT * FROM "mytable" WHERE "body" @> $1 AND (("col1")::int) > ($2) ORDER BY ("col1")::int ASC FETCH FIRST 10 ROWS ONLY');
      });

      it('requires an order definition', function () {
        assert.throws(() => new Select(source, {}, {pageLength: 10}), 'Keyset paging with pageLength requires an explicit order directive');
      });

      it('does not work with offsets', function () {
        assert.throws(() => new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1'
          }],
          offset: 10
        }), 'Keyset paging cannot be used with offset and limit');
      });

      it('does not work with limits', function () {
        assert.throws(() => new Select(source, {}, {
          pageLength: 10,
          order: [{
            field: 'col1'
          }],
          limit: 10
        }), 'Keyset paging cannot be used with offset and limit');
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
        '"mytable"."timestamp" AS "mytable__timestamp",',
        '"mytable"."timestamptz" AS "mytable__timestamptz",',
        '"mytable"."date" AS "mytable__date",',
        '"jointable1"."id" AS "jointable1__id",',
        '"jointable1"."mytable_id" AS "jointable1__mytable_id",',
        '"jointable1"."val1" AS "jointable1__val1",',
        '"jointable1"."timestamp" AS "jointable1__timestamp",',
        '"jointable1"."date" AS "jointable1__date",',
        '"jt2"."id" AS "jt2__id",',
        '"jt2"."jointable1_id" AS "jt2__jointable1_id",',
        '"jt2"."val2" AS "jt2__val2",',
        '"jointable3"."id" AS "jointable3__id",',
        '"jointable3"."mytable_id" AS "jointable3__mytable_id",',
        '"jointable3"."val3" AS "jointable3__val3" ',
        'FROM "mytable" ',
        'INNER JOIN "jointable1" ON "jointable1"."mytable_id" = "mytable"."id" ',
        'INNER JOIN "jointable2" AS "jt2" ON "jt2"."jointable1_id" = "jointable1"."id" ',
        'LEFT OUTER JOIN "myschema"."jointable3" AS "jointable3" ON "jointable3"."mytable_id" = "mytable"."id" ',
        'WHERE TRUE'
      ].join(''));
    });

    describe('date casting', function () {
      const date = new Date();
      const timestamp = new Date();
      const timestamptz = new Date();

      it('should cast date columns according to the postgres date type', function () {
        const result = new Select(source, {date, timestamp, timestamptz});
        assert.equal(
          result.format(),
          'SELECT * FROM "mytable" WHERE "date" = $1::date AND "timestamp" = $2::timestamp AND "timestamptz" = $3::timestamptz'
        );
      });

      it('should cast date columns according to their types on a join base table', function () {
        const joinSource = source.join({
          'jointable1': {
            type: 'INNER',
            on: {mytable_id: 'id'}
          }
        });

        const result = new Select(joinSource, {date, timestamp});
        assert.include(result.format(), '"mytable"."date" = $1::date AND "mytable"."timestamp" = $2::timestamp');
      });

      it('should cast date columns according to their types on a joined table', function () {
        const joinSource = source.join({
          'jointable1': {
            type: 'INNER',
            on: {mytable_id: 'id'}
          }
        });

        const result = new Select(joinSource, {'jointable1.date': date, 'jointable1.timestamp': timestamp});
        assert.include(result.format(), '"jointable1"."date" = $1::date AND "jointable1"."timestamp" = $2::timestamp');
      });
    });
  });
});
