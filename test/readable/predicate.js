'use strict';

const Writable = require('../../lib/writable');

describe('predicate', function () {
  const source = new Writable({
    name: 'mytable',
    schema: 'public',
    columns: ['id', 'field', 'col1', 'col2', 'body', 'x', 'y', 'z'],
    pk: ['id'],
    db: {
      currentSchema: 'public',
      entityCache: {},
      jointable1: new Writable({
        name: 'jointable1',
        schema: 'public',
        columns: ['id', 'mytable_id', 'val1', 'a', 'b', 'c'],
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

  beforeEach(function () {
    source.db.entityCache = {};
  });

  it('adds params', function () {
    const conjunction = source.predicate(
      {field: 'value'},
      0,
      source.forWhere
    );

    assert.equal(conjunction.predicate, '"field" = $1');
    assert.equal(conjunction.params.length, 1);
    assert.equal(conjunction.params[0], 'value');
  });

  it('creates join definitions', function () {
    const joined = source.join({
      jointable1: {
        on: {mytable_id: 'id'}
      }
    });

    const conjunction = joined.predicate(
      {mytable_id: 'id'},
      0,
      joined.forJoin,
      'jointable1'
    );

    assert.equal(conjunction.predicates, '"jointable1"."mytable_id" = "mytable"."id"');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('creates complex criteria', function () {
    const joined = source.join({
      'jointable1': {
        on: {
          mytable_id: 'id',
          or: [{
            a: 'x'
          }, {
            b: 'y',
            c: 'z'
          }]
        }
      }
    });

    const conjunction = joined.predicate(
      {mytable_id: 'id', or: [{a: 'x'}, {b: 'y', c: 'z'}]},
      0,
      joined.forJoin,
      'jointable1'
    );

    assert.equal(conjunction.predicate, [
      '"jointable1"."mytable_id" = "mytable"."id" AND ',
      '(("jointable1"."a" = "mytable"."x") OR ',
      '("jointable1"."b" = "mytable"."y" AND ',
      '"jointable1"."c" = "mytable"."z"))'
    ].join(''));
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('uses supplied aliases', function () {
    const joined = source.join({
      'jt3': {
        relation: 'myschema.jointable3',
        on: {mytable_id: 'id'}
      }
    });

    const conjunction = joined.predicate(
      {mytable_id: 'id'},
      0,
      joined.forJoin,
      'jt3'
    );

    assert.equal(conjunction.predicates, '"jt3"."mytable_id" = "mytable"."id"');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('ignores schemas in favor of aliases', function () {
    const joined = source.join({
      'myschema.jointable3': {
        on: {mytable_id: 'id'}
      }
    });

    const conjunction = joined.predicate(
      {mytable_id: 'id'},
      0,
      joined.forJoin,
      'jointable3'
    );

    assert.equal(conjunction.predicates, '"jointable3"."mytable_id" = "mytable"."id"');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('routes join keys to the appropriate relations', function () {
    const joined = source.join({
      'jointable1': {
        on: {mytable_id: 'id'}
      },
      'jointable2': {
        on: {jointable1_id: 'jointable1.id'}
      }
    });

    const conjunction = joined.predicate(
      {jointable1_id: 'jointable1.id'},
      0,
      joined.forJoin,
      'jointable2'
    );

    assert.equal(conjunction.predicates, '"jointable2"."jointable1_id" = "jointable1"."id"');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('should return a safe value for fully-empty criteria', function () {
    const result = source.predicate({}, 0, source.forWhere);
    assert.equal(result.predicate, 'TRUE');
    assert.equal(result.params.length, 0);
  });

  it('should return a safe value for empty table criteria', function () {
    const result = source.predicate({}, 0);
    assert.equal(result.predicate, 'TRUE');
    assert.equal(result.params.length, 0);
  });

  it('should create basic criteria', function () {
    const result = source.predicate({field: 'value'}, 0, source.forWhere);
    assert.equal(result.predicate, '"field" = $1');
    assert.equal(result.params.length, 1);
    assert.equal(result.params[0], 'value');
  });

  it.skip('tests against columns', function () {
    // TODO sourceKey.path isn't always fully qualified
    const result = source.predicate({'col1 >': 'col2'}, 0, source.forWhere);
    assert.equal(result.predicate, '"col1" > "col2"');
    assert.equal(result.params.length, 0);
  });

  it('should AND together predicates', function () {
    const result = source.predicate({field1: 'value1', field2: 'value2'}, 0, source.forWhere);
    assert.equal(result.predicate, '"field1" = $1 AND "field2" = $2');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value1');
    assert.equal(result.params[1], 'value2');
  });

  it('should accommodate pre-built predicates', function () {
    const result = source.predicate({
      conditions: '"field2" @@ lower($1)',
      params: ['value2'],
      where: {field1: 'value1'}
    }, 0, source.forWhere);

    assert.equal(result.predicate, '"field2" @@ lower($1) AND "field1" = $2');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value2');
    assert.equal(result.params[1], 'value1');
  });

  it('should accommodate pre-built document predicates with isDocument', function () {
    const result = source.predicate({
      conditions: '"field2" @@ lower($1)',
      params: ['value2'],
      where: {field1: 'value1'},
      isDocument: true
    }, 0, source.forWhere);

    assert.equal(result.predicate, '"field2" @@ lower($1) AND "body" @> $2');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value2');
    assert.equal(result.params[1], JSON.stringify({field1: 'value1'}));
  });

  it('should return predicate and parameters', function () {
    const result = source.predicate({field1: 'value1', field2: 'value2'}, 0, source.forWhere);

    assert.equal(result.predicate, '"field1" = $1 AND "field2" = $2');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value1');
    assert.equal(result.params[1], 'value2');
  });

  it('should keep non-contains stuff separate in document queries', function () {
    const result = source.predicate({
      field: [{one: 'two', three: 'four'}],
      'otherthing >': 123
    }, 0, source.forDoc);

    assert.equal(result.predicate, `"body" @> $1 AND ("body"->>'otherthing')::decimal > 123`);
    assert.lengthOf(result.params, 1);
    assert.equal(result.params[0], '{"field":[{"one":"two","three":"four"}]}');
  });

  describe('JSON value formatting', function () {
    it('should stringify numbers', function () {
      const result = source.predicate({'json.field': 123}, 0, source.forWhere);

      assert.equal(result.predicate, '"json"->>\'field\' = $1');
      assert.lengthOf(result.params, 1);
      assert.equal(result.params[0], '123');
      assert.typeOf(result.params[0], 'string');
    });

    it('should stringify booleans', function () {
      const result = source.predicate({'json.field': true}, 0, source.forWhere);

      assert.equal(result.predicate, '"json"->>\'field\' = $1');
      assert.lengthOf(result.params, 1);
      assert.equal(result.params[0], 'true');
      assert.typeOf(result.params[0], 'string');
    });

    it('should stringify dates', function () {
      const date = new Date();
      const result = source.predicate({'json.field': date}, 0, source.forWhere);

      assert.equal(result.predicate, '"json"->>\'field\' = $1');
      assert.lengthOf(result.params, 1);
      assert.equal(result.params[0], date.toString());
      assert.typeOf(result.params[0], 'string');
    });

    it('should stringify individual items in arrays', function () {
      const result = source.predicate({'json.field': [1, 2, 3]}, 0, source.forWhere);

      assert.equal(result.predicate, '"json"->>\'field\' IN ($1,$2,$3)');
      assert.lengthOf(result.params, 3);
      assert.deepEqual(result.params, ['1', '2', '3']);
      assert.typeOf(result.params[0], 'string');
    });

    it('should not stringify nulls', function () {
      const result = source.predicate({'json.field': null}, 0, source.forWhere);

      assert.equal(result.predicate, '"json"->>\'field\' IS null');
      assert.lengthOf(result.params, 0);
    });
  });

  describe('with disjunction subgroups', function () {
    it('should encapsulate and OR together subgroups', function () {
      const result = source.predicate({
        or: [{
          field1: 'value1'
        }, {
          field2: 'value2', field3: 'value3'
        }, {
          field4: 'value4'
        }]
      }, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1) OR ("field2" = $2 AND "field3" = $3) OR ("field4" = $4))');
      assert.equal(result.params.length, 4);
      assert.equal(result.params[0], 'value1');
      assert.equal(result.params[1], 'value2');
      assert.equal(result.params[2], 'value3');
      assert.equal(result.params[3], 'value4');
    });

    it('should not pollute other fields', function () {
      const result = source.predicate({
        or: [{field1: 'value1'}, {field2: 'value2'}],
        field3: 'value3'
      }, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1) OR ("field2" = $2)) AND "field3" = $3');
      assert.equal(result.params.length, 3);
      assert.equal(result.params[0], 'value1');
      assert.equal(result.params[1], 'value2');
      assert.equal(result.params[2], 'value3');
    });

    it('should return a usable predicate if only given one subgroup', function () {
      const result = source.predicate({or: [{field1: 'value1'}]}, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1))');
      assert.equal(result.params.length, 1);
      assert.equal(result.params[0], 'value1');
    });

    it('recurses', function () {
      const result = source.predicate({
        or: [{
          field1: 'value1',
          or: [{
            field2: 'value4'
          }, {
            field3: 'value5'
          }]
        }, {
          field2: 'value2',
          field3: 'value3'
        }]
      }, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1 AND (("field2" = $2) OR ("field3" = $3))) OR ("field2" = $4 AND "field3" = $5))');
      assert.equal(result.params.length, 5);
      assert.equal(result.params[0], 'value1');
      assert.equal(result.params[1], 'value4');
      assert.equal(result.params[2], 'value5');
      assert.equal(result.params[3], 'value2');
      assert.equal(result.params[4], 'value3');
    });
  });

  describe('with nested conjunction subgroups', function () {
    it('should encapsulate and AND together subgroups', function () {
      const result = source.predicate({
        and: [{
          field1: 'value1'
        }, {
          field2: 'value2', field3: 'value3'
        }, {
          field4: 'value4'
        }]
      }, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1) AND ("field2" = $2 AND "field3" = $3) AND ("field4" = $4))');
      assert.equal(result.params.length, 4);
      assert.equal(result.params[0], 'value1');
      assert.equal(result.params[1], 'value2');
      assert.equal(result.params[2], 'value3');
      assert.equal(result.params[3], 'value4');
    });

    it('should not pollute other fields', function () {
      const result = source.predicate({
        and: [{field1: 'value1'}, {field2: 'value2'}],
        field3: 'value3'
      }, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1) AND ("field2" = $2)) AND "field3" = $3');
      assert.equal(result.params.length, 3);
      assert.equal(result.params[0], 'value1');
      assert.equal(result.params[1], 'value2');
      assert.equal(result.params[2], 'value3');
    });

    it('should return a usable predicate if only given one subgroup', function () {
      const result = source.predicate({and: [{field1: 'value1'}]}, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1))');
      assert.equal(result.params.length, 1);
      assert.equal(result.params[0], 'value1');
    });

    it('recurses', function () {
      const result = source.predicate({
        or: [{
          field1: 'value1',
          and: [{
            field2: 'value4'
          }, {
            field3: 'value5'
          }]
        }, {
          field2: 'value2',
          field3: 'value3'
        }]
      }, 0, source.forWhere);

      assert.equal(result.predicate, '(("field1" = $1 AND (("field2" = $2) AND ("field3" = $3))) OR ("field2" = $4 AND "field3" = $5))');
      assert.equal(result.params.length, 5);
      assert.equal(result.params[0], 'value1');
      assert.equal(result.params[1], 'value4');
      assert.equal(result.params[2], 'value5');
      assert.equal(result.params[3], 'value2');
      assert.equal(result.params[4], 'value3');
    });
  });
});
