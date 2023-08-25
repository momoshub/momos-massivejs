'use strict';

describe('findCandidateJoinKeys', function () {
  let db;

  before(function () {
    return resetDb('foreign-keys').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('finds a single foreign key relationship', function () {
    const candidates = db.alpha.findCandidateJoinKeys(db.beta, 'beta');

    assert.deepEqual(candidates, [['beta_alpha_id_fkey', {id: 'beta.alpha_id'}]]);
  });

  it('applies aliases', function () {
    const candidates = db.alpha.findCandidateJoinKeys(db.beta, 'asdf');

    assert.deepEqual(candidates, [['beta_alpha_id_fkey', {id: 'asdf.alpha_id'}]]);
  });

  it('finds multiple foreign key relationships', function () {
    const candidates = db.alpha.findCandidateJoinKeys(db.gamma, 'gamma');

    assert.sameDeepMembers(candidates, [
      ['gamma_alpha_id_two_fkey', {id: 'gamma.alpha_id_two'}],
      ['gamma_alpha_id_one_fkey', {id: 'gamma.alpha_id_one'}]
    ]);
  });

  it('finds no foreign key relationships', function () {
    const candidates = db.alpha.findCandidateJoinKeys(db.sch.delta, 'delta');

    assert.lengthOf(candidates, 0);
  });
});
