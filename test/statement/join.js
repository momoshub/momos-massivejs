'use strict';

const Writable = require('../../lib/writable');
const join = require('../../lib/statement/join');

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
    const joins = join(source.join({
      'jointable1': {
        type: 'INNER',
        on: {mytable_id: 'id'}
      }
    }));

    assert.deepEqual(joins, [{
      type: 'INNER',
      relation: '"jointable1"',
      criteria: '("jointable1"."mytable_id" = "mytable"."id")'
    }]);
  });

  it('creates complex criteria', function () {
    const joins = join(source.join({
      'jointable1': {
        type: 'INNER',
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
    }));

    assert.deepEqual(joins, [{
      type: 'INNER',
      relation: '"jointable1"',
      criteria: [
        '(("jointable1"."mytable_id" = "mytable"."id") AND ',
        '(("jointable1"."one" = "mytable"."two") OR ',
        '(("jointable1"."three" = "mytable"."four") AND ',
        '("jointable1"."five" = "mytable"."six"))))'
      ].join('')
    }]);
  });

  it('accounts for schemas', function () {
    const joinSource = source.join({
      'jointable1': {
        type: 'INNER',
        on: {mytable_id: 'id'}
      },
      'myschema.jointable3': {
        type: 'LEFT OUTER',
        on: {mytable_id: 'id'}
      }
    });

    const joins = join(joinSource);

    assert.deepEqual(joins, [{
      type: 'INNER',
      relation: '"jointable1"',
      criteria: '("jointable1"."mytable_id" = "mytable"."id")'
    }, {
      type: 'LEFT OUTER',
      relation: '"myschema"."jointable3" AS "jointable3"',
      criteria: '("jointable3"."mytable_id" = "mytable"."id")'
    }]);
  });

  it('uses supplied aliases', function () {
    const joinSource = source.join({
      'jointable1': {
        type: 'INNER',
        on: {mytable_id: 'id'}
      },
      'jt3': {
        relation: 'myschema.jointable3',
        type: 'LEFT OUTER',
        on: {mytable_id: 'id'}
      }
    });

    const joins = join(joinSource);

    assert.deepEqual(joins, [{
      type: 'INNER',
      relation: '"jointable1"',
      criteria: '("jointable1"."mytable_id" = "mytable"."id")'
    }, {
      type: 'LEFT OUTER',
      relation: '"myschema"."jointable3" AS "jt3"',
      criteria: '("jt3"."mytable_id" = "mytable"."id")'
    }]);
  });

  it('routes join keys to the appropriate relations', function () {
    const joinSource = source.join({
      'jointable1': {
        type: 'INNER',
        on: {mytable_id: 'id'}
      },
      'jointable2': {
        type: 'INNER',
        on: {jointable1_id: 'jointable1.id'}
      }
    });

    const joins = join(joinSource);

    assert.deepEqual(joins, [{
      type: 'INNER',
      relation: '"jointable1"',
      criteria: '("jointable1"."mytable_id" = "mytable"."id")'
    }, {
      type: 'INNER',
      relation: '"jointable2"',
      criteria: '("jointable2"."jointable1_id" = "jointable1"."id")'
    }]);
  });

  it('does it all at once', function () {
    const joinSource = source.join({
      'jointable1': {
        type: 'INNER',
        on: {mytable_id: 'id'}
      },
      'jt2': {
        type: 'INNER',
        relation: 'jointable2',
        on: {jointable1_id: 'jointable1.id'}
      },
      'myschema.jointable3': {
        type: 'LEFT OUTER',
        on: {mytable_id: 'id'}
      }
    });

    const joins = join(joinSource);

    assert.deepEqual(joins, [{
      type: 'INNER',
      relation: '"jointable1"',
      criteria: '("jointable1"."mytable_id" = "mytable"."id")'
    }, {
      type: 'INNER',
      relation: '"jointable2" AS "jt2"',
      criteria: '("jt2"."jointable1_id" = "jointable1"."id")'
    }, {
      type: 'LEFT OUTER',
      relation: '"myschema"."jointable3" AS "jointable3"',
      criteria: '("jointable3"."mytable_id" = "mytable"."id")'
    }]);
  });
});
