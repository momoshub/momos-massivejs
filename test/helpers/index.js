'use strict';

const path = require('path');

global._ = require('lodash');
global.pgp = require('pg-promise');
global.assert = require('chai').use(require('chai-as-promised')).assert;
global.massive = require('../../index');
global.host = process.env.POSTGRES_HOST || 'localhost';
global.connectionString = `postgres://postgres@${global.host}/massive`;

global.loader = {
  enhancedFunctions: true
};

global.resetDb = function (schema = 'default') {
  global.loader.scripts = path.join(__dirname, 'scripts', schema);

  return massive(global.connectionString, global.loader).then(db => {
    return db.query(`select schema_name from information_schema.schemata where catalog_name = 'massive' and schema_name not like 'pg_%' and schema_name not like 'information_schema'`)
      .then(schemata =>
        // drop schemas one by one to avoid deadlocks in case of cross-schema dependencies
        schemata.reduce((promise, s) =>
          promise.then(() => db.query(`drop schema ${s.schema_name} cascade`)), Promise.resolve()
        )
      )
      .then(() => db.schema())
      .then(() => db.reload());
  });
};

module.exports = {resetDb};
