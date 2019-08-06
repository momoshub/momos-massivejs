'use strict';

const _ = require('lodash');
const parseKey = require('../util/parse-key');
const where = require('./where');

/**
 * Represents a SELECT query.
 *
 * @class
 * @param {Readable} source - Database object to query.
 * @param {Object|String|UUID|Number} [criteria] - A criteria object, prebuilt
 * predicate, or primitive pk value.
 * @param {Object} [options] - {@link https://massivejs.org/docs/options-objects|Select options}.
 */
const Select = function (source, criteria = {}, options = {}) {
  if (!!source.isPkSearch && source.isPkSearch(criteria)) {
    options.generator = 'tableGenerator'; // pks are always table columns

    if (!_.isPlainObject(criteria)) {
      // primitive unary pk search
      criteria = _.fromPairs([[source.pk[0], criteria]]);

      options.single = source.loader !== 'join';
    }
  }

  const {conditions, params} = where(source, criteria, 0, options.generator);

  this.source = source;

  // options governing query behavior; some also affect statement generation,
  // such as options.document with buildSelectList.
  this.build = options.build || false;
  this.document = options.document || false;
  this.decompose = options.decompose;
  this.single = options.single || false;
  this.stream = options.stream || false;

  // options governing SQL statement elements, in rough order of appearance:
  this.only = options.only || false;
  this.selectList = this.buildSelectList(options.fields, options.exprs);
  this.conditions = conditions;
  this.order = _.reduce(options.order, (acc, val) => {
    const direction = val.direction && val.direction.toLowerCase() === 'desc' ? ' DESC' : ' ASC';
    const nulls = val.nulls ? ` NULLS ${val.nulls === 'first' ? 'FIRST' : 'LAST'}` : '';

    acc.push(this.buildOrderExpression(val, options.orderBody) + direction + nulls);

    return acc;
  }, []);
  this.offset = options.offset;
  this.limit = options.limit;
  this.pageLength = options.pageLength;
  this.forUpdate = options.forUpdate || false;
  this.forShare = options.forShare || false;
  this.params = params;

  // with pageLength set for keyset pagination, add last values of ordering
  // fields to criteria
  if (this.pageLength) {
    if (!options.order) {
      throw new Error('Keyset paging with pageLength requires an explicit order directive');
    } else if (
      Object.prototype.hasOwnProperty.call(options, 'offset') ||
      Object.prototype.hasOwnProperty.call(options, 'limit')
    ) {
      throw new Error('Keyset paging cannot be used with offset and limit');
    } else if (Object.hasOwnProperty.call(options.order[0], 'last')) {
      const paginationColumns = options.order.map(o => this.buildOrderExpression(o)).join(',');
      const placeholders = options.order.map((o, idx) => `$${idx + this.params.length + 1}`).join(',');
      const comparison = options.order[0].direction && options.order[0].direction.toLowerCase() === 'desc' ? '<' : '>';

      this.params = this.params.concat(options.order.map(o => o.last));
      this.pagination = `(${paginationColumns}) ${comparison} (${placeholders})`;
    }
  }
};

/**
 * Build a list of strings comprising fields (plus aliases, for document
 * tables and joined or compound Readables) and expressions to be retrieved
 * from the source Readable.
 *
 * @param {Array} fields - A list of field names.
 * @param {Object} exprs - A map of expression aliases to values. Values are
 * interpolated directly into the SQL emitted and are thus a potential vector
 * for SQL injection attacks if used carelessly.
 * @return {Array} The complete array of select expressions.
 */
Select.prototype.buildSelectList = function (fields, exprs) {
  const selectList = _.castArray(fields || []).reduceRight((all, field) => {
    if (this.document) {
      // document fields need to alias a JSON traversal expression
      const documentField = `body.${field}`;

      all.unshift(`${parseKey(documentField, this.source).lhs} AS "${field}"`);
    } else {
      all.unshift(parseKey(field, this.source).lhs);
    }

    return all;
  }, _.map(exprs || {}, (expr, name) => {
    // interpolate unsafe user-defined expressions
    return `${expr} AS "${name}"`;
  }));

  if (!selectList.length) {
    if (!fields && !exprs) {
      // nothing specified, select all
      if (this.source.loader === 'join') {
        return this.source.columns.map(c => `${c.fullName} AS "${c.alias}"`);
      }

      return ['*'];
    }

    // we got nothing *explicitly*, error state
    throw new Error('At least one of fields or exprs must be supplied and must define a field or expression to select.');
  } else if (this.document) {
    // if the user *did* specify something, but we're querying a document table
    // and so require the id field in addition to whatever they're after
    selectList.unshift('"id"');
  }

  return selectList;
};

/**
 * Build a single expression for an ORDER BY list.
 *
 * @param {Object} orderObj - An object representing an ORDER BY list element.
 * @param {String} [orderObj.field] - The name of a field in the target
 * relation. May include JSON traversal or an implicit cast in the Postgres
 * 'x::y' format. Either field or expr must be supplied.
 * @param {String} [orderObj.expr] - An expression to be interpolated into the
 * ORDER BY clause directly.
 * @param {String} [orderObj.direction] - "ASC" or "DESC"; not used here, but
 * included for completeness.
 * @param {String} [orderObj.type] - An explicit cast type. If specified for a
 * JSON traversal expression, the value will be retrieved as text before it is
 * cast to the target type.
 * @param {Boolean} useBody - True to treat orderObj.field as an element in the
 * document body instead of a column on the target relation.
 * @return {String} A single expression to be included in an ORDER BY clause.
 */
Select.prototype.buildOrderExpression = function (orderObj, useBody = false) {
  const jsonAsText = !!orderObj.type; // Explicit casts must use as-text operators
  let field;

  if (orderObj.expr) {
    field = orderObj.expr;
  } else if (useBody) {
    field = `"body"${jsonAsText ? '->>' : '->'}'${orderObj.field}'`;
  } else if (orderObj.field) {
    field = parseKey(orderObj.field, this.source, jsonAsText).lhs;
  } else {
    throw new Error('Missing order field or expr.');
  }

  if (orderObj.type) {
    return `(${field})::${orderObj.type}`;
  }

  return field;
};

/**
 * Format this object into a SQL SELECT.
 *
 * @return {String} A SQL SELECT statement.
 */
Select.prototype.format = function () {
  let sql = `SELECT ${this.selectList.join(',')} FROM `;

  if (this.only) { sql += 'ONLY '; }

  sql += `${this.source.delimitedFullName} `;

  if (this.source.loader === 'join') {
    sql += this.source.joins.map(j => `${j.type} JOIN ${j.target} ON ${j.on} `).join('');
  }

  sql += `WHERE ${this.conditions}`;

  if (this.pagination) { sql += ` AND ${this.pagination}`; }
  if (this.order.length) { sql += ` ORDER BY ${this.order.join(',')}`; }
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
