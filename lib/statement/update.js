'use strict';

const _ = require('lodash');
const parseKey = require('../util/parse-key');
const where = require('./where');
const prepareParams = require('../util/prepare-params');

/**
 * Represents an UPDATE query.
 *
 * @class
 * @param {Table} source - Database object to query.
 * @param {Object} changes - A map of field names to new values.
 * @param {Object} criteria - A criteria object.
 * @param {Object} [options] - {@link https://massivejs.org/docs/options-objects|Update options}.
 */
const Update = function (source, changes, criteria = {}, options = {}) {
  if (source.isPkSearch(criteria)) {
    options.generator = 'tableGenerator'; // pks are always table columns

    if (!_.isPlainObject(criteria)) {
      // primitive unary pk search
      criteria = _.fromPairs([[source.pk[0], criteria]]);

      options.single = source.loader !== 'join';
    }
  }

  let offset = 0;

  changes = _.pick(changes, source.columnNames);

  this.changes = _.reduce(changes, (acc, value, key) => {
    acc.push(`"${key}" = $${++offset}`);

    return acc;
  }, []);

  const {conditions, params} = where(source, criteria, offset, options.generator);

  this.source = source;

  // options governing query behavior
  this.build = options.build || false;
  this.decompose = options.decompose;
  this.document = options.document || false;
  this.single = options.single || false;
  this.stream = options.stream || false;

  // options governing SQL statement elements, in rough order of appearance:
  this.only = options.only || false;
  this.conditions = conditions;
  this.returning = options.fields ? options.fields.map(f => parseKey(f, source).lhs) : ['*'];
  this.params = prepareParams(_.keys(changes), [changes]).concat(params);
};

/**
 * Format this object into a SQL UPDATE.
 *
 * @return {String} A SQL UPDATE statement.
 */
Update.prototype.format = function () {
  let sql = 'UPDATE ';

  if (this.only) { sql += 'ONLY '; }

  sql += `${this.source.delimitedFullName} `;
  sql += `SET ${this.changes.join(', ')} `;

  if (this.source.loader === 'join') {
    // the first join is b in `UPDATE a SET ... FROM b`
    const target = this.source.joins[0];

    this.conditions = `${target.on} AND (${this.conditions}) `;

    sql += `FROM ${target.relation} `;
    sql += _.tail(this.source.joins).map(j => `${j.type} JOIN ${j.target} ON ${j.on} `).join('');
    sql += `WHERE ${this.conditions} `;
    sql += `RETURNING ${this.source.delimitedFullName}.*`;
  } else {
    sql += `WHERE ${this.conditions} `;
    sql += `RETURNING ${this.returning.join(', ')}`;
  }

  return sql;
};

module.exports = Update;
