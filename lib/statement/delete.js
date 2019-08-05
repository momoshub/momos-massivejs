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

  // get fields to return from options
  this.fields = options.fields ? options.fields.map(f => parseKey(f, source).lhs) : ['*'];

  if (source.isPkSearch(criteria, options)) {
    if (_.isPlainObject(criteria)) {
      // id:val search
      this.where = where(source, criteria);
    } else {
      // primitive unary pk search
      this.where = where(source, _.fromPairs([[source.pk[0], criteria]]));
      this.single = true;
    }
  } else {
    this.where = where(source, criteria, 0, this.generator);
  }

  this.params = this.where.params;
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

    this.where.conditions = `${target.on} AND (${this.where.conditions}) `;

    sql += `USING ${target.relation} `;
    sql += _.tail(this.source.joins).map(j => `${j.type} JOIN ${j.target} ON ${j.on} `).join('');
    sql += `WHERE ${this.where.conditions} `;
    sql += `RETURNING ${this.source.delimitedFullName}.*`;
  } else {
    sql += `WHERE ${this.where.conditions} `;
    sql += `RETURNING ${this.fields.join(', ')}`;
  }

  return sql;
};

module.exports = Delete;
