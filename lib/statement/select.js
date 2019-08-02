'use strict';

const _ = require('lodash');
const parseKey = require('../util/parse-key');
const join = require('./join');
const where = require('./where');
const orderBy = require('./order-by');

/**
 * Represents a SELECT query.
 *
 * @class
 * @param {Readable} source - Database object to query.
 * @param {Object|String|UUID|Number} criteria - A criteria object, prebuilt
 * predicate, or primitive pk value.
 * @param {Object} [options] - {@link https://massivejs.org/docs/options-objects|Select options}.
 */
const Select = function (source, criteria = {}, options = {}) {
  options = _.defaults(options, {
    only: false,
    orderBody: false,
    generator: 'tableGenerator',
    single: false
  });

  this.source = source;
  this.only = options.only;
  this.offset = options.offset;
  this.limit = options.limit;
  this.build = options.build;
  this.document = options.document;
  this.decompose = options.decompose;
  this.generator = options.generator;
  this.pageLength = options.pageLength;
  this.single = options.single;
  this.stream = options.stream;
  this.forUpdate = options.forUpdate;
  this.forShare = options.forShare;

  this.fields = [];

  // add user-defined fields
  this.fields = _.castArray(options.fields || []).reduce((all, field) => {
    if (options.document) {
      // document fields need to be aliased
      all.push({
        fullName: parseKey(`body.${field}`, source).lhs,
        alias: field
      });
    } else {
      all.push(parseKey(field, source).lhs);
    }

    return all;
  }, this.fields);

  // interpolate unsafe user-defined expressions
  _.forEach(options.exprs || [], (expr, name) => {
    this.fields.push({
      fullName: expr,
      alias: name
    });
  });

  if (!this.fields.length) {
    if (!options.fields && !options.exprs) {
      // nothing specified, select all
      if (source.loader === 'join') {
        this.fields = source.columns;
      } else {
        this.fields.push('*');
      }
    } else {
      // explicit empty array, error state
      this.error = 'The fields array cannot be empty';
    }
  } else if (options.document) {
    // if the user *did* specify something, but we're querying a document table
    // and so require the id field in addition to whatever they're after
    this.fields.push('id');
  }

  if (!!source.isPkSearch && source.isPkSearch(criteria)) {
    this.generator = 'tableGenerator'; // pks are always table columns

    if (!_.isPlainObject(criteria)) {
      // primitive unary pk search
      criteria = _.fromPairs([[source.pk[0], criteria]]);

      this.single = source.loader !== 'join';
    }
  }

  this.where = where(source, criteria, 0, this.generator);
  this.params = this.where.params;

  if (options.order) {
    this.order = orderBy(options.order, source, options.orderBody);
  }

  // with pageLength set for keyset pagination, add last values of ordering
  // fields to criteria
  if (this.pageLength) {
    if (!this.order) {
      this.error = 'Keyset paging with pageLength requires an explicit order directive';
    } else if (
      Object.prototype.hasOwnProperty.call(options, 'offset') ||
      Object.prototype.hasOwnProperty.call(options, 'limit')
    ) {
      this.error = 'Keyset paging cannot be used with offset and limit';
    } else if (Object.hasOwnProperty.call(options.order[0], 'last')) {
      const paginationColumns = options.order.map(o => orderBy.fullAttribute(o, source)).join(',');
      const placeholders = options.order.map((o, idx) => `$${idx + this.params.length + 1}`).join(',');
      const comparison = options.order[0].direction && options.order[0].direction.toLowerCase() === 'desc' ? '<' : '>';

      this.params = this.params.concat(options.order.map(o => o.last));
      this.pagination = `(${paginationColumns}) ${comparison} (${placeholders})`;
    }
  }
};

/**
 * Format this object into a SQL SELECT.
 *
 * @return {String} A SQL SELECT statement.
 */
Select.prototype.format = function () {
  if (this.error) {
    throw new Error(this.error);
  }

  const selectList = this.fields.map(f => {
    if (_.isPlainObject(f)) {
      // aliased definitions for document fields
      return `${f.fullName} AS "${f.alias}"`;
    }

    return f;
  });

  let sql = `SELECT ${selectList.join(',')} FROM `;

  if (this.only) { sql += 'ONLY '; }

  sql += `${this.source.delimitedFullName} `;

  if (this.source.loader === 'join') {
    sql = join(this.source).reduce((str, j) => `${str}${j.type} JOIN ${j.relation} ON ${j.criteria} `, sql);
  }

  sql += `WHERE ${this.where.conditions}`;

  if (this.pagination) { sql += ` AND ${this.pagination}`; }
  if (this.order) { sql += ` ${this.order}`; }
  if (this.forUpdate) { sql += ' FOR UPDATE'; }
  if (this.forShare) { sql += ' FOR SHARE'; }
  if (this.pageLength) { sql += ` FETCH FIRST ${this.pageLength} ROWS ONLY`; }
  if (this.offset) { sql += ` OFFSET ${this.offset}`; }
  if (this.single) {
    sql += ' LIMIT 1';
  } else if (this.limit) {
    sql += ` LIMIT ${this.limit}`;
  }

  return sql;
};

module.exports = Select;
