'use strict';

const Writable = require('../../lib/writable');

describe('join', function () {
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

  beforeEach(function () {
    source.db.entityCache = {};
  });

  it('creates join definitions', function () {
    const conjunction = source.join({
      jointable1: {
        on: {mytable_id: 'id'}
      }
    }).buildJoinConjunction({mytable_id: 'id'}, 'jointable1');

    assert.equal(conjunction, '("jointable1"."mytable_id" = "mytable"."id")');
  });

  it('creates complex criteria', function () {
    const conjunction = source.join({
      'jointable1': {
        on: {
          mytable_id: 'id',
          or: [{
            one: 'two'
          }, {
            three: 'four',
            five: 'six'
          }]
        }
      }
    }).buildJoinConjunction({
      mytable_id: 'id',
      or: [{
        one: 'two'
      }, {
        three: 'four',
        five: 'six'
      }]
    }, 'jointable1');

    assert.deepEqual(conjunction, [
      '(("jointable1"."mytable_id" = "mytable"."id") AND ',
      '(("jointable1"."one" = "mytable"."two") OR ',
      '(("jointable1"."three" = "mytable"."four") AND ',
      '("jointable1"."five" = "mytable"."six"))))'
    ].join(''));
  });

  it('uses supplied aliases', function () {
    const conjunction = source.join({
      'jt3': {
        relation: 'myschema.jointable3',
        on: {mytable_id: 'id'}
      }
    }).buildJoinConjunction({mytable_id: 'id'}, 'jt3');

    assert.equal(conjunction, '("jt3"."mytable_id" = "mytable"."id")');
  });

  it('ignores schemas in favor of aliases', function () {
    const conjunction = source.join({
      'myschema.jointable3': {
        on: {mytable_id: 'id'}
      }
    }).buildJoinConjunction({mytable_id: 'id'}, 'jointable3');

    assert.equal(conjunction, '("jointable3"."mytable_id" = "mytable"."id")');
  });

  it('routes join keys to the appropriate relations', function () {
    const conjunction = source.join({
      'jointable1': {
        on: {mytable_id: 'id'}
      },
      'jointable2': {
        on: {jointable1_id: 'jointable1.id'}
      }
    }).buildJoinConjunction({jointable1_id: 'jointable1.id'}, 'jointable2');

    assert.equal(conjunction, '("jointable2"."jointable1_id" = "jointable1"."id")');
  });
});
