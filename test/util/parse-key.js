'use strict';

const Writable = require('../../lib/writable');
const parseKey = require('../../lib/util/parse-key');
const ops = require('../../lib/statement/operations');

describe('parseKey', function () {
  const db = {
    currentSchema: 'public',
    entityCache: {}
  };

  db.mytable = new Writable({ // only Writables have pks and we need to test joins
    name: 'mytable',
    schema: 'public',
    columns: ['id', 'field', 'col1', 'col2', 'body'],
    pk: ['id'],
    db
  });

  db.jointable1 = new Writable({
    name: 'jointable1',
    schema: 'public',
    columns: ['id', 'mytable_id', 'val1'],
    pk: ['id'],
    db
  });

  db.myschema = {
    jointable2: new Writable({
      name: 'jointable2',
      schema: 'myschema',
      columns: ['id', 'mytable_id', 'val2'],
      pk: ['id'],
      db
    })
  };

  const source = db.mytable;

  const joinSource = source.join({
    'jointable1': {
      type: 'inner',
      on: {mytable_id: 'id'}
    },
    'myschema.jointable2': {
      type: 'inner',
      on: {mytable_id: 'id'}
    },
    'jt2alias': {
      type: 'inner',
      relation: 'myschema.jointable2',
      on: {mytable_id: 'id'}
    }
  });

  describe('field identifiers', function () {
    it('should quote an unquoted field identifier', function () {
      const result = parseKey('myfield', source, ops);
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"myfield"');
      assert.equal(result.lhs, '"myfield"');
      assert.equal(result.relation, 'mytable');
      assert.isUndefined(result.schema);
    });

    it('should not double-quote a quoted field identifier', function () {
      const result = parseKey('"my field"', source, ops);
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"my field"');
      assert.equal(result.lhs, '"my field"');
      assert.equal(result.relation, 'mytable');
      assert.isUndefined(result.schema);
    });
  });

  describe('JSON traversal', function () {
    it('should format a shallow JSON path', function () {
      const result = parseKey('json.property', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->>\'property\'');
      assert.deepEqual(result.jsonElements, ['property']);
    });

    it('should format a shallow JSON path with a numeric key', function () {
      const result = parseKey('json.123', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->>\'123\'');
      assert.deepEqual(result.jsonElements, ['123']);
    });

    it('should format a JSON array path', function () {
      const result = parseKey('json[123]', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->>123');
      assert.deepEqual(result.jsonElements, ['123']);
    });

    it('should format a deep JSON path', function () {
      const result = parseKey('json.outer.inner', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"#>>\'{outer,inner}\'');
      assert.deepEqual(result.jsonElements, ['outer', 'inner']);
    });

    it('should format a JSON path with a quoted field', function () {
      const result = parseKey('"json field".outer.inner', source, ops);
      assert.equal(result.field, 'json field');
      assert.equal(result.path, '"json field"');
      assert.equal(result.lhs, '"json field"#>>\'{outer,inner}\'');
      assert.deepEqual(result.jsonElements, ['outer', 'inner']);
    });

    it('should format a JSON path with a quoted field containing special characters', function () {
      const result = parseKey('"json.fiel[d]".outer.inner', source, ops);
      assert.equal(result.field, 'json.fiel[d]');
      assert.equal(result.path, '"json.fiel[d]"');
      assert.equal(result.lhs, '"json.fiel[d]"#>>\'{outer,inner}\'');
      assert.deepEqual(result.jsonElements, ['outer', 'inner']);
    });

    it('should format a deep JSON path with numeric keys', function () {
      const result = parseKey('json.123.456', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"#>>\'{123,456}\'');
      assert.deepEqual(result.jsonElements, ['123', '456']);
    });

    it('should format mixed JSON paths', function () {
      const result = parseKey('json.array[1].field.array[2]', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"#>>\'{array,1,field,array,2}\'');
      assert.deepEqual(result.jsonElements, ['array', '1', 'field', 'array', '2']);
    });

    it('should format a shallow JSON path with as-text off', function () {
      const result = parseKey('json.property', source, false);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->\'property\'');
      assert.deepEqual(result.jsonElements, ['property']);
    });

    it('should format a JSON array path with as-text off', function () {
      const result = parseKey('json[123]', source, false);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->123');
      assert.deepEqual(result.jsonElements, ['123']);
    });

    it('should format a deep JSON path with as-text off', function () {
      const result = parseKey('json.outer.inner', source, false);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"#>\'{outer,inner}\'');
      assert.deepEqual(result.jsonElements, ['outer', 'inner']);
    });

    it('should force as-text on if JSON has cast', function () {
      const result = parseKey('json.property::int', source, false);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '("json"->>\'property\')::int');
      assert.deepEqual(result.jsonElements, ['property']);
    });
  });

  describe('casting', function () {
    it('should cast fields without an operator', function () {
      const result = parseKey('field::text', source, ops);
      assert.equal(result.field, 'field');
      assert.equal(result.path, '"field"');
      assert.equal(result.lhs, '"field"::text');
      assert.isUndefined(result.appended);
    });
  });

  describe('multi-table keys', function () {
    it('should quote an unquoted field identifier for a table', function () {
      const result = parseKey('jointable1.myfield', joinSource, ops);
      assert.equal(result.relation, 'jointable1');
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"jointable1"."myfield"');
      assert.equal(result.lhs, '"jointable1"."myfield"');
    });

    it('should quote an unquoted field identifier with the origin table and schema', function () {
      const joinSourceInSchema = source.db.myschema.jointable2.join({
        mytable: {
          type: 'inner',
          on: {id: 'mytable_id'}
        }
      });

      const result = parseKey('myschema.jointable2.myfield', joinSourceInSchema, ops);
      assert.equal(result.schema, 'myschema');
      assert.equal(result.relation, 'jointable2');
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"myschema"."jointable2"."myfield"');
      assert.equal(result.lhs, '"myschema"."jointable2"."myfield"');
    });

    it('should quote an unquoted field identifier with an alias', function () {
      const result = parseKey('jt2alias.myfield', joinSource, ops);
      assert.isUndefined(result.schema);
      assert.equal(result.relation, 'jt2alias');
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"jt2alias"."myfield"');
      assert.equal(result.lhs, '"jt2alias"."myfield"');
    });

    it('should quote an unquoted field identifier for a table', function () {
      const relationSource = source.join({
        'jt': {
          type: 'inner',
          relation: 'jointable1',
          on: {mytable_id: 'id'}
        }
      });

      const result = parseKey('jointable1.myfield', relationSource, ops);
      assert.equal(result.relation, 'jointable1');
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"jt"."myfield"');
      assert.equal(result.lhs, '"jt"."myfield"');
    });

    it('should default to the origin for a join source if no schema/table is specified', function () {
      const result = parseKey('myfield', joinSource, ops);
      assert.equal(result.schema, 'public');
      assert.equal(result.relation, 'mytable');
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"mytable"."myfield"');
      assert.equal(result.lhs, '"mytable"."myfield"');
    });

    it('should not double-quote a quoted field identifier for a table', function () {
      const relationSource = source.join({
        'jt': {
          type: 'inner',
          relation: 'jointable1',
          on: {mytable_id: 'id'}
        }
      });

      const result = parseKey('"jointable1"."my field"', relationSource, ops);
      assert.equal(result.relation, 'jointable1');
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"jt"."my field"');
      assert.equal(result.lhs, '"jt"."my field"');
    });

    it('should alias a quoted field identifier for a table and schema', function () {
      const result = parseKey('"myschema"."jointable2"."my field"', joinSource, ops);
      assert.equal(result.schema, 'myschema');
      assert.equal(result.relation, 'jointable2');
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"jointable2"."my field"');
      assert.equal(result.lhs, '"jointable2"."my field"');
    });

    it('should not double-quote mixed quoting situations for a table', function () {
      const result = parseKey('jointable1."my field"', joinSource, ops);
      assert.equal(result.relation, 'jointable1');
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"jointable1"."my field"');
      assert.equal(result.lhs, '"jointable1"."my field"');
    });

    it('should not double-quote mixed quoting situations for a table and schema', function () {
      const result = parseKey('"myschema".jointable2."my field"', joinSource, ops);
      assert.equal(result.schema, 'myschema');
      assert.equal(result.relation, 'jointable2');
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"jointable2"."my field"');
      assert.equal(result.lhs, '"jointable2"."my field"');
    });

    it('should format mixed JSON paths with a table', function () {
      const result = parseKey('jointable1.json.array[1].field.array[2]', joinSource, ops);
      assert.equal(result.relation, 'jointable1');
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"jointable1"."json"');
      assert.equal(result.lhs, '"jointable1"."json"#>>\'{array,1,field,array,2}\'');
      assert.deepEqual(result.jsonElements, ['array', '1', 'field', 'array', '2']);
    });

    it('should format mixed JSON paths with a table and schema', function () {
      const result = parseKey('myschema.jointable2.json.array[1].field.array[2]', joinSource, ops);
      assert.equal(result.schema, 'myschema');
      assert.equal(result.relation, 'jointable2');
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"jointable2"."json"');
      assert.equal(result.lhs, '"jointable2"."json"#>>\'{array,1,field,array,2}\'');
      assert.deepEqual(result.jsonElements, ['array', '1', 'field', 'array', '2']);
    });
  });

  describe('withAppendix and operations', function () {
    it('should default to equivalence', function () {
      const result = parseKey.withAppendix('myfield', source, ops);
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"myfield"');
      assert.equal(result.lhs, '"myfield"');
      assert.equal(result.appended.operator, '=');
      assert.isUndefined(result.mutator);
    });

    it('should get the operation details for an unquoted identifier', function () {
      const result = parseKey.withAppendix('myfield <=', source, ops);
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"myfield"');
      assert.equal(result.lhs, '"myfield"');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get the operation details for a quoted identifier', function () {
      const result = parseKey.withAppendix('"my field" <=', source, ops);
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"my field"');
      assert.equal(result.lhs, '"my field"');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get an operation comprising multiple tokens', function () {
      const result = parseKey.withAppendix('myfield not similar to', source, ops);
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"myfield"');
      assert.equal(result.lhs, '"myfield"');
      assert.equal(result.appended.operator, 'NOT SIMILAR TO');
      assert.isUndefined(result.mutator);
    });

    it('should allow any amount of whitespace', function () {
      const result = parseKey.withAppendix(' \r\n \t myfield  \r\n  \t  \t <= \r\n\t', source, ops);
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"myfield"');
      assert.equal(result.lhs, '"myfield"');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get the appropriate mutator', function () {
      const result = parseKey.withAppendix('"my field" @>', source, ops);
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"my field"');
      assert.equal(result.lhs, '"my field"');
      assert.equal(result.appended.operator, '@>');
      assert.equal(typeof result.appended.mutator, 'function');
      assert.deepEqual(result.appended.mutator({value: ['hi'], params: [], offset: 1}), {
        offset: 1,
        value: '$1',
        params: ['{hi}']
      });
    });

    it('should get operations for a shallow JSON path', function () {
      const result = parseKey.withAppendix('json.key <=', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->>\'key\'');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get operations for a deep JSON path', function () {
      const result = parseKey.withAppendix('json.outer.inner <=', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"#>>\'{outer,inner}\'');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get operations for a JSON array', function () {
      const result = parseKey.withAppendix('json[1] <=', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '"json"->>1');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should match > properly', function () {
      const result = parseKey.withAppendix('field >', source, ops);
      assert.equal(result.field, 'field');
      assert.equal(result.path, '"field"');
      assert.equal(result.lhs, '"field"');
      assert.equal(result.appended.operator, '>');
      assert.isUndefined(result.mutator);
    });

    it('should match >= properly', function () {
      const result = parseKey.withAppendix('field >=', source, ops);
      assert.equal(result.field, 'field');
      assert.equal(result.path, '"field"');
      assert.equal(result.lhs, '"field"');
      assert.equal(result.appended.operator, '>=');
      assert.isUndefined(result.mutator);
    });

    it('should match the longest possible operator', function () {
      const result = parseKey.withAppendix('field ~~*', source, ops); // ~ and ~* are also operators
      assert.equal(result.field, 'field');
      assert.equal(result.path, '"field"');
      assert.equal(result.lhs, '"field"');
      assert.equal(result.appended.operator, 'ILIKE');
      assert.isUndefined(result.mutator);
    });

    it('should ignore the case of LIKE and similar operators', function () {
      const result = parseKey.withAppendix('field LikE', source, ops);
      assert.equal(result.field, 'field');
      assert.equal(result.path, '"field"');
      assert.equal(result.lhs, '"field"');
      assert.equal(result.appended.operator, 'LIKE');
      assert.isUndefined(result.mutator);
    });

    it('should not clobber a field with an operator in the name', function () {
      const result = parseKey.withAppendix('is_field is', source, ops);
      assert.equal(result.field, 'is_field');
      assert.equal(result.path, '"is_field"');
      assert.equal(result.lhs, '"is_field"');
      assert.equal(result.appended.operator, 'IS');
      assert.isUndefined(result.mutator);
    });

    it('should not clobber a quoted field with an operator in the name', function () {
      const result = parseKey.withAppendix('"this is a field" is', source, ops);
      assert.equal(result.field, 'this is a field');
      assert.equal(result.path, '"this is a field"');
      assert.equal(result.lhs, '"this is a field"');
      assert.equal(result.appended.operator, 'IS');
      assert.isUndefined(result.mutator);
    });

    it('should get the operation details for an identifier with table', function () {
      const result = parseKey.withAppendix('"jointable1"."my field" <=', joinSource, ops);
      assert.equal(result.relation, 'jointable1');
      assert.equal(result.field, 'my field');
      assert.equal(result.path, '"jointable1"."my field"');
      assert.equal(result.lhs, '"jointable1"."my field"');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get the operation details for an identifier with table and schema', function () {
      const result = parseKey.withAppendix('myschema.jointable2.myfield <=', joinSource, ops);
      assert.equal(result.schema, 'myschema');
      assert.equal(result.relation, 'jointable2');
      assert.equal(result.field, 'myfield');
      assert.equal(result.path, '"jointable2"."myfield"');
      assert.equal(result.lhs, '"jointable2"."myfield"');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should get operations for a deep JSON path with table and schema', function () {
      const result = parseKey.withAppendix('myschema.jointable2.json.outer.inner <=', joinSource, ops);
      assert.equal(result.schema, 'myschema');
      assert.equal(result.relation, 'jointable2');
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"jointable2"."json"');
      assert.equal(result.lhs, '"jointable2"."json"#>>\'{outer,inner}\'');
      assert.equal(result.appended.operator, '<=');
      assert.isUndefined(result.mutator);
    });

    it('should format mixed JSON paths', function () {
      const result = parseKey.withAppendix('json.array[1].field.array[2]::boolean LIKE', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '("json"#>>\'{array,1,field,array,2}\')::boolean');
      assert.equal(result.appended.operator, 'LIKE');
      assert.isUndefined(result.mutator);
    });

    it('should format quoted fields with mixed JSON paths', function () {
      const result = parseKey.withAppendix('"json".array[1].field.array[2]::boolean LIKE', source, ops);
      assert.equal(result.field, 'json');
      assert.equal(result.path, '"json"');
      assert.equal(result.lhs, '("json"#>>\'{array,1,field,array,2}\')::boolean');
      assert.equal(result.appended.operator, 'LIKE');
      assert.isUndefined(result.mutator);
    });

    describe('casting with operations', function () {
      it('should cast fields', function () {
        const result = parseKey.withAppendix('field::text LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '"field"::text');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast fields with shallow JSON paths', function () {
        const result = parseKey.withAppendix('field.element::boolean LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '("field"->>\'element\')::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast fields with deep JSON paths', function () {
        const result = parseKey.withAppendix('field.one.two::boolean LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '("field"#>>\'{one,two}\')::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast fields with JSON arrays', function () {
        const result = parseKey.withAppendix('field[123]::boolean LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '("field"->>123)::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast quoted fields without an operator', function () {
        const result = parseKey.withAppendix('"field"::text', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '"field"::text');
        assert.equal(result.appended.operator, '=');
        assert.isUndefined(result.mutator);
      });

      it('should cast quoted fields', function () {
        const result = parseKey.withAppendix('"field"::text LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '"field"::text');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast quoted fields with JSON operations', function () {
        const result = parseKey.withAppendix('"field".element::boolean LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '("field"->>\'element\')::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast quoted fields with deep JSON paths', function () {
        const result = parseKey.withAppendix('"field".one.two::boolean LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '("field"#>>\'{one,two}\')::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast quoted fields with JSON arrays', function () {
        const result = parseKey.withAppendix('"field"[123]::boolean LIKE', source, ops);
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"field"');
        assert.equal(result.lhs, '("field"->>123)::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast fields with a table', function () {
        const result = parseKey.withAppendix('jointable1.field::text LIKE', joinSource, ops);
        assert.equal(result.relation, 'jointable1');
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"jointable1"."field"');
        assert.equal(result.lhs, '"jointable1"."field"::text');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast fields with a schema and table', function () {
        const result = parseKey.withAppendix('myschema.jointable2.field::text LIKE', joinSource, ops);
        assert.equal(result.schema, 'myschema');
        assert.equal(result.relation, 'jointable2');
        assert.equal(result.field, 'field');
        assert.equal(result.path, '"jointable2"."field"');
        assert.equal(result.lhs, '"jointable2"."field"::text');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });

      it('should cast mixed JSON paths with schema and table', function () {
        const result = parseKey.withAppendix('myschema.jointable2.json.array[1].field.array[2]::boolean LIKE', joinSource, ops);
        assert.equal(result.schema, 'myschema');
        assert.equal(result.relation, 'jointable2');
        assert.equal(result.field, 'json');
        assert.equal(result.path, '"jointable2"."json"');
        assert.equal(result.lhs, '("jointable2"."json"#>>\'{array,1,field,array,2}\')::boolean');
        assert.equal(result.appended.operator, 'LIKE');
        assert.isUndefined(result.mutator);
      });
    });
  });
});
