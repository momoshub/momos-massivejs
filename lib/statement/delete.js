'use strict';

const _ = require('lodash');
const parseKey = require('../util/parse-key');
const join = require('./join');
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

  if (source.loader === 'join') {
    this.join = join(source);
  }

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
    const tableCriteria = {};

    if (source.schema === source.db.currentSchema) {
      tableCriteria[source.name] = criteria;
    } else {
      tableCriteria[source.schema] = {};
      tableCriteria[source.schema][source.name] = criteria;
    }

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

  if (this.join) {
    const target = this.join.shift();

    this.where.conditions = `${target.criteria} AND (${this.where.conditions}) `;

    sql += `USING ${target.relation} `;
    sql += this.join.map(j => `${j.type} JOIN ${j.relation} ON ${j.criteria} `);
  }

  sql += `WHERE ${this.where.conditions} `;

  if (this.join) {
    sql += `RETURNING ${this.source.delimitedFullName}.*`;
  } else {
    sql += `RETURNING ${this.fields.join(', ')}`;
  }

  return sql;
};

module.exports = Delete;
