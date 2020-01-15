'use strict';

/**
 * Parse a query explicit locking options, supporting legacy `forUpdate` and `forShare`
 * and `forShare` arguments
 * @param {Object} [options] - {@link https://massivejs.org/docs/options-objects|Select options}
 * @return {Object} a lock object
 */
const parseLock = function (options) {
  const {forUpdate, forShare, lock} = options;
  // fail if more than one of those options is not null and not undefined
  if ([forShare, forUpdate, lock].filter((x) => x != null).length > 1) {
    throw new Error('The "forUpdate", "forShare" and "lock" options are mutually exclusive');
  }

  if (forUpdate !== undefined || forShare !== undefined && process.env.NODE_ENV !== 'production') {
    /* eslint-disable-next-line no-console */
    console.log('DEPRECATED: the "forShare" and "forUpdate" options are deprecated and will be removed in a future version, "lock" should be used instead.');
  }

  if (forUpdate) {
    return {strength: 'UPDATE'};
  }

  if (forShare) {
    return {strength: 'SHARE'};
  }

  return options.lock;
};

module.exports = {parseLock};
