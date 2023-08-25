'use strict';

const decompose = require('../util/decompose');

exports = module.exports = function (db) {
  const file = db.serverAtLeast('9.5') ? 'tables.sql' : 'tables-legacy.sql';

  return db.instance.query(db.loader.queryFiles[file], db.loader).then(tables => decompose({
    pk: ['schema', 'name'],
    columns: ['schema', 'name', 'parent', 'pk', 'columns', 'types', 'is_insertable_into'],
    fks: {
      pk: 'fk',
      columns: {
        fk: 'fk',
        fk_dependent_columns: 'dependent_columns',
        fk_origin_schema: 'origin_schema',
        fk_origin_name: 'origin_name',
        fk_origin_columns: 'origin_columns'
      }
    }
  }, tables));
};
