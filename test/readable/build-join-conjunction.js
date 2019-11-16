'use strict';

const Writable = require('../../lib/writable');

describe('join', function () {
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

  it('creates join definitions', function () {
    const conjunction = source.join({
      jointable1: {
        on: {mytable_id: 'id'}
      }
    }).solveEtCoagula({mytable_id: 'id'}, 'jointable1');

    assert.equal(conjunction.predicates, '("jointable1"."mytable_id" = "mytable"."id")');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('creates complex criteria', function () {
    const conjunction = source.join({
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
    }).solveEtCoagula({
      mytable_id: 'id',
      or: [{
        a: 'x'
      }, {
        b: 'y',
        c: 'z'
      }]
    }, 'jointable1');

    assert.equal(conjunction.predicates, [
      '("jointable1"."mytable_id" = "mytable"."id" AND ',
      '(("jointable1"."a" = "mytable"."x") OR ',
      '("jointable1"."b" = "mytable"."y" AND ',
      '"jointable1"."c" = "mytable"."z")))'
    ].join(''));
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('uses supplied aliases', function () {
    const conjunction = source.join({
      'jt3': {
        relation: 'myschema.jointable3',
        on: {mytable_id: 'id'}
      }
    }).solveEtCoagula({mytable_id: 'id'}, 'jt3');

    assert.equal(conjunction.predicates, '("jt3"."mytable_id" = "mytable"."id")');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('ignores schemas in favor of aliases', function () {
    const conjunction = source.join({
      'myschema.jointable3': {
        on: {mytable_id: 'id'}
      }
    }).solveEtCoagula({mytable_id: 'id'}, 'jointable3');

    assert.equal(conjunction.predicates, '("jointable3"."mytable_id" = "mytable"."id")');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });

  it('routes join keys to the appropriate relations', function () {
    const conjunction = source.join({
      'jointable1': {
        on: {mytable_id: 'id'}
      },
      'jointable2': {
        on: {jointable1_id: 'jointable1.id'}
      }
    }).solveEtCoagula({jointable1_id: 'jointable1.id'}, 'jointable2');

    assert.equal(conjunction.predicates, '("jointable2"."jointable1_id" = "jointable1"."id")');
    assert.lengthOf(conjunction.params, 0);
    assert.equal(conjunction.offset, 0);
  });
});
