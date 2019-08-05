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
  let offset = 0;

  changes = _.pick(changes, source.columnNames);

  this.params = prepareParams(_.keys(changes), [changes]);
  this.changes = _.reduce(changes, (acc, value, key) => {
    acc.push(`"${key}" = $${++offset}`);

    return acc;
  }, []);

  options = _.defaults(options, {
    only: false,
    generator: 'tableGenerator',
    single: false
  });

  this.source = source;
  this.only = options.only;
  this.build = options.build;
  this.decompose = options.decompose;
  this.document = options.document;
  this.generator = options.generator;
  this.single = options.single;
  this.stream = options.stream;

  if (source.loader === 'join' && Object.keys(source.joinSchema).length > 1) {
    throw new Error('Joins involving more than one other relation joined to the origin are not updatable. Try reconfiguring your join schema with only one root-level join relation, or using SQL.');
  }

  // apply field override from options if set, otherwise get everything
  this.fields = options.fields ? options.fields.map(f => parseKey(f, source).lhs) : ['*'];

  if (source.isPkSearch(criteria)) {
    this.generator = 'tableGenerator'; // pks are always table columns

    if (!_.isPlainObject(criteria)) {
      // primitive unary pk search
      criteria = _.fromPairs([[source.pk[0], criteria]]);

      this.single = source.loader !== 'join';
    }
  }

  this.where = where(source, criteria, this.params.length, this.generator);
  this.params = this.params.concat(this.where.params);
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

    this.where.conditions = `${target.on} AND (${this.where.conditions}) `;

    sql += `FROM ${target.relation} `;
    sql += _.tail(this.source.joins).map(j => `${j.type} JOIN ${j.target} ON ${j.on} `).join('');
    sql += `WHERE ${this.where.conditions} `;
    sql += `RETURNING ${this.source.delimitedFullName}.*`;
  } else {
    sql += `WHERE ${this.where.conditions} `;
    sql += `RETURNING ${this.fields.join(', ')}`;
  }

  return sql;
};

module.exports = Update;
