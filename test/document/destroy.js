'use strict';

describe('destroy (document)', function () {
  let db;
  let newDoc = {};

  before(function () {
    return resetDb('singletable')
      .then(instance => db = instance)
      .then(() => {
        return db.saveDoc('docs', {name: 'foo'});
      })
      .then(doc => newDoc = doc);
  });

  after(function () {
    return db.docs.destroy({id: newDoc.id}).then(() => {
      return db.instance.$pool.end();
    });
  });

  it('deletes a document', function () {
    return db.docs.destroy({name: 'foo'}, {document: true}).then(docs => {
      assert.lengthOf(docs, 1);
      assert.equal(docs[0].name, 'foo');
    });
  });
});
