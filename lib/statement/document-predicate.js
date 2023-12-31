'use strict';

const _ = require('lodash');
const stringify = require('../util/stringify');

/**
 * Generate a predicate for a document query.
 *
 * @module documentPredicate
 * @param {Object} condition - A condition object (generated by parseKey).
 * @param {String} key - The key corresponding to the condition in the criteria object.
 * @return {Object} A predicate object.
 */
exports = module.exports = function (condition, key) {
  const isArray = _.isArray(condition.value);

  // Contains queries using the @> operator can take advantage of a GIN index
  // on JSONB fields. This gives document queries a dramatic performance boost.
  if (
    /*
     * type one: array of objects
     *
     * Here, the criteria {key: [{matchMe: true}, ...]} search for a
     * corresponding document {key: [..., {matchMe: true}, ...]}. At least one
     * object in the document array must match all conditions of the object(s)
     * in the criteria array. Multiple such objects are effectively merged.
     */
    (isArray && _.isPlainObject(condition.value[0])) ||
    /*
     * type two: shallow equality against an object
     *
     * Criteria in the format {matchMe: true} yield documents containing
     * {matchMe: true}.
     */
    (condition.appended.operator === '=' &&
      condition.jsonElements.length === 1 &&
      !isArray)
  ) {
    const param = _.set({}, key, condition.value);

    condition.lhs = condition.path;
    condition.appended.operator = '@>';
    condition.params.push(JSON.stringify(param));
    condition.value = `$${condition.offset}`;
  } else if (condition.appended.operator !== 'IS' && condition.appended.operator !== 'IS NOT') {
    /*
     * We're querying a key on the document body! `IS` operations need no
     * further treatment. Comparisons use an as-text operator, so we need to
     * convert the value coming out of the JSON/JSONB field to the correct type
     * first.
     */
    let cast = '';

    if (_.isBoolean(condition.value)) {
      cast = '::boolean';
    } else if (_.isNumber(condition.value)) {
      cast = '::decimal';
    } else if (_.isDate(condition.value)) {
      cast = '::timestamptz';
      condition.params.push(condition.value);
      condition.value = `$${condition.offset}`;
    } else if (condition.appended.mutator) {
      condition = condition.appended.mutator(condition);
    } else {
      condition.params.push(stringify(condition.value));
      condition.value = `$${condition.offset}`;
    }

    condition.lhs = `(${condition.lhs})${cast || ''}`;
  }

  return condition;
};
