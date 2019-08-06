'use strict';

const _ = require('lodash');
const parseKey = require('../util/parse-key');
const where = require('./where');

/**
 * Represents a DELETE query.
 *
 * @class
 * @param {Table} source - Database object to query.
 * @param {Object|String|Number} [criteria] - A criteria object or primitive pk
 * value.
 * @param {Object} [options] - {@link https://massivejs.org/docs/options-objects|Delete options}.
 */
const Delete = function (source, criteria = {}, options = {}) {
  if (source.isPkSearch(criteria, options)) {
    options.generator = 'tableGenerator'; // pks are always table columns

    if (!_.isPlainObject(criteria)) {
      // primitive unary pk search
      criteria = _.fromPairs([[source.pk[0], criteria]]);

      options.single = source.loader !== 'join';
    }
  }

  const {conditions, params} = where(source, criteria, 0, options.generator);

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
  this.params = params;
};

/**
 * Format this object into a SQL DELETE.
 *
 * @return {String} A SQL DELETE statement.
 */
Delete.prototype.format = function () {
  let sql = 'DELETE FROM ';

  if (this.only) { sql += 'ONLY '; }

  sql += `${this.source.delimitedFullName} `;

  if (this.source.loader === 'join') {
    const target = this.source.joins[0];

    this.conditions = `${target.on} AND (${this.conditions}) `;

    sql += `USING ${target.relation} `;
    sql += _.tail(this.source.joins).map(j => `${j.type} JOIN ${j.target} ON ${j.on} `).join('');
    sql += `WHERE ${this.conditions} `;
    sql += `RETURNING ${this.source.delimitedFullName}.*`;
  } else {
    sql += `WHERE ${this.conditions} `;
    sql += `RETURNING ${this.returning.join(', ')}`;
  }

  return sql;
};

module.exports = Delete;
