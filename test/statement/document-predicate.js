'use strict';

const Readable = require('../../lib/readable');
const documentPredicate = require('../../lib/statement/document-predicate');
const ops = require('../../lib/statement/operations');
const parseKey = require('../../lib/util/parse-key');

describe('documentPredicate', function () {
  const source = new Readable({
    name: 'testsource',
    schema: 'public',
    columns: ['field', 'col1', 'col2', 'body'],
    db: {
      currentSchema: 'public'
    }
  });

  /**
   * Helper for generating conditions in WHERE clause testing.
   *
   * @param {String} key - The key and optional operation.
   * @param {Any} value - The value being tested in the predicate.
   * @param {Integer} offset - The condition offset.
   * @returns {Object} A condition object.
   */
  function getCondition (key, value, offset) {
    const condition = parseKey.withAppendix(key, source, ops, value, offset);

    return condition;
  }

  it('should build deep traversals', function () {
    const obj = {field: [{one: 'two'}]};
    const condition = getCondition('body.field', [{one: 'two'}], 1);
    const result = documentPredicate(condition, 'field', obj);
    assert.equal(result.lhs, '"body"');
    assert.equal(result.appended.operator, '@>');
    assert.equal(result.value, '$1');
    assert.equal(result.params.length, 1);
    assert.equal(result.params[0], JSON.stringify(obj));
  });

  it('should omit other keys and create strict hierarchies for deep traversals', function () {
    const obj = {field: [{one: 'two'}], somethingelse: 'hi'};
    const condition = getCondition('body.field', [{one: 'two'}], 1);
    const result = documentPredicate(condition, 'field', obj);
    assert.equal(result.lhs, '"body"');
    assert.equal(result.appended.operator, '@>');
    assert.equal(result.value, '$1');
    assert.equal(result.params.length, 1);
    assert.equal(result.params[0], JSON.stringify({field: [{one: 'two'}]}));
  });

  it('should create an IS comparison predicate', function () {
    const condition = getCondition('body.field is', true, 1);
    const result = documentPredicate(condition, 'field is', {'field is': true});
    assert.equal(result.lhs, '"body"->>\'field\'');
    assert.equal(result.appended.operator, 'IS');
    assert.isTrue(result.value);
    assert.equal(result.params.length, 0);
  });

  it('should build an equality predicate using the JSON contains op', function () {
    const condition = getCondition('body.field', 'value', 1);
    const result = documentPredicate(condition, 'field', {field: 'value'});
    assert.equal(result.lhs, '"body"');
    assert.equal(result.appended.operator, '@>');
    assert.equal(result.value, '$1');
    assert.equal(result.params.length, 1);
    assert.equal(result.params[0], JSON.stringify({field: 'value'}));
  });

  it('should build a non-equality predicate', function () {
    const condition = getCondition('body.field <>', 'value', 1);
    const result = documentPredicate(condition, 'field <>', {'field <>': 'value'});
    assert.equal(result.lhs, '("body"->>\'field\')');
    assert.equal(result.appended.operator, '<>');
    assert.equal(result.value, '$1');
    assert.equal(result.params.length, 1);
    assert.equal(result.params[0], 'value');
  });

  it('should cast booleans in non-equality predicates', function () {
    const condition = getCondition('body.field <>', true, 1);
    const result = documentPredicate(condition, 'field <>', {'field <>': true});
    assert.equal(result.lhs, '("body"->>\'field\')::boolean');
    assert.equal(result.appended.operator, '<>');
    assert.isTrue(result.value);
    assert.equal(result.params.length, 0);
  });

  it('should cast numbers in non-equality predicates', function () {
    const condition = getCondition('body.field <>', 123.45, 1);
    const result = documentPredicate(condition, 'field <>', {'field <>': 123.45});
    assert.equal(result.lhs, '("body"->>\'field\')::decimal');
    assert.equal(result.appended.operator, '<>');
    assert.equal(result.value, '123.45');
    assert.equal(result.params.length, 0);
  });

  it('should cast dates in non-equality predicates', function () {
    const date = new Date();
    const condition = getCondition('body.field <>', date, 1);
    const result = documentPredicate(condition, 'field <>', {'field <>': date});
    assert.equal(result.lhs, '("body"->>\'field\')::timestamptz');
    assert.equal(result.appended.operator, '<>');
    assert.equal(result.value, '$1');
    assert.equal(result.params.length, 1);
    assert.equal(result.params[0], date);
  });

  it('should create IN clauses for array parameters', function () {
    const condition = getCondition('body.field', ['value1', 'value2'], 1);
    const result = documentPredicate(condition, 'field <>', {field: ['value1', 'value2']});
    assert.equal(result.lhs, '("body"->>\'field\')');
    assert.equal(result.appended.operator, 'IN');
    assert.equal(result.value, '($1,$2)');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value1');
    assert.equal(result.params[1], 'value2');
  });

  it('should traverse JSON with ->>', function () {
    const condition = getCondition('body.field', ['value1', 'value2'], 1);
    const result = documentPredicate(condition, 'field <>', {field: ['value1', 'value2']});
    assert.equal(result.lhs, '("body"->>\'field\')');
    assert.equal(result.appended.operator, 'IN');
    assert.equal(result.value, '($1,$2)');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value1');
    assert.equal(result.params[1], 'value2');
  });

  it('should use pathing operator #>> for nested values', function () {
    const condition = getCondition('body.field.one.two', ['value1', 'value2'], 1);
    const result = documentPredicate(condition, 'field <>', {field: ['value1', 'value2']});
    assert.equal(result.lhs, '("body"#>>\'{field,one,two}\')');
    assert.equal(result.appended.operator, 'IN');
    assert.equal(result.value, '($1,$2)');
    assert.equal(result.params.length, 2);
    assert.equal(result.params[0], 'value1');
    assert.equal(result.params[1], 'value2');
  });
});
