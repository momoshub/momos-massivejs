'use strict';

describe('saveDocs', function () {
  let db;

  before(function () {
    return resetDb('empty').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  describe('invalid inputs', function () {
    before(function () {
      return db.createDocumentTable('doctable');
    });

    it('should return a rejected promise for passing a non-array', async () => {
      try {
        await db.saveDocs('doctable', {foo: 'bar'});
      } catch (e) {
        assert.equal(e.message, 'Please pass in the documents as an array of objects.');
      }
    });

    it('should return a rejected promise for passing an array with a non-object', async () => {
      try {
        await db.saveDocs('doctable', [{foo: 'bar'}, 123]);
      } catch (e) {
        assert.equal(e.message, 'Please pass in valid documents. Include the primary key for an UPDATE.');
      }
    });

    after(function () {
      return db.query('DROP TABLE doctable;');
    });
  });

  describe('with an existing table', function () {
    before(function () {
      return db.createDocumentTable('docs');
    });

    it('saves new documents without an id', async () => {
      const docs = await db.saveDocs('docs', [
        {
          title: 'Alone',
          description: 'yearning in the darkness',
          price: 89.99,
          type: 'book',
          is_good: true,
          created_at: '2015-03-04T09:43:41.643Z'
        }, {
          title: 'Anytime chilli for one',
          type: 'recipe',
          ingredients: 'tears',
          is_good: false
        }
      ]);
      assert.equal(docs[0].title, 'Alone');
      assert.equal(docs[0].type, 'book');
      assert.equal(docs[1].title, 'Anytime chilli for one');
      assert.equal(docs[1].ingredients, 'tears');
      assert.equal(docs[1].type, 'recipe');

      docs.every(doc => assert.isOk(doc.id));
      docs.every(doc => assert.isOk(doc.created_at));
      docs.every(doc => assert.notOk(doc.updated_at));
    });

    it('updates existing documents with ids', async () => {
      const docs = await db.saveDocs('docs', [
        {
          title: 'Alone',
          description: 'yearning in the darkness',
          type: 'book',
          price: 89.99,
          is_good: true,
          created_at: '2015-03-04T09:43:41.643Z'
        }, {
          title: 'Anytime chilli for one',
          type: 'recipe',
          ingredients: 'tears',
          is_good: false
        }
      ]);

      assert.equal(docs[0].title, 'Alone');
      assert.equal(docs[1].title, 'Anytime chilli for one');
      assert.isOk(docs[0].created_at);
      const docIds = docs.map(doc => doc.id);
      docIds.every(assert.isOk);
      docs.map(doc => doc.updated_at).every(assert.notOk);

      docs[0].title = 'Together!';
      docs[1].title = 'Anytime chilli for two';

      const updated = await db.saveDocs('docs', docs);
      assert.deepEqual(updated.map(doc => doc.id), docIds);

      assert.equal(updated[0].title, 'Together!');
      assert.equal(updated[1].title, 'Anytime chilli for two');
      updated.map(doc => doc.created_at).every(assert.isOk);
      updated.map(doc => doc.updated_at).every(assert.isOk);

      const retrieved = await Promise.all(docIds.map(id => db.docs.findDoc(id)));

      assert.equal(retrieved[0].title, 'Together!');
      assert.equal(retrieved[1].title, 'Anytime chilli for two');
    });

    it('pulls metadata off the actual jsonb field', async () => {
      let docs = await db.saveDocs('docs', [
        {foo: 1},
        {foo: 2}
      ]);

      docs.every(doc => assert.isOk(doc.id));
      docs.every(doc => assert.isOk(doc.created_at));
      docs.every(doc => assert.notOk(doc.updated_at));
      assert.equal(docs[0].foo, 1);
      assert.equal(docs[1].foo, 2);

      docs[0].bar = 3;
      docs[1].bar = 4;

      docs = await db.saveDocs('docs', docs);

      docs.every(doc => assert.isOk(doc.id));
      docs.every(doc => assert.isOk(doc.created_at));
      docs.every(doc => assert.isOk(doc.updated_at));
      assert.equal(docs[0].foo, 1);
      assert.equal(docs[1].foo, 2);
      assert.equal(docs[0].bar, 3);
      assert.equal(docs[1].bar, 4);

      const rows = await Promise.all(docs.map(doc => db.docs.findOne(doc.id)));

      assert.deepEqual(rows[0].body, {foo: 1, bar: 3});
      assert.deepEqual(rows[1].body, {foo: 2, bar: 4});
    });
  });

  describe('with an existing table in a schema', function () {
    before(async () => {
      await db.createSchema('myschema');

      return db.createDocumentTable('myschema.docs');
    });

    it('saves new documents without an id', async () => {
      const docs = await db.saveDocs('myschema.docs', [
        {
          title: 'Alone',
          description: 'yearning in the darkness',
          price: 89.99,
          type: 'book',
          is_good: true,
          created_at: '2015-03-04T09:43:41.643Z'
        }, {
          title: 'Anytime chilli for one',
          type: 'recipe',
          ingredients: 'tears',
          is_good: false
        }
      ]);
      assert.equal(docs[0].title, 'Alone');
      assert.equal(docs[0].type, 'book');
      assert.equal(docs[1].title, 'Anytime chilli for one');
      assert.equal(docs[1].ingredients, 'tears');
      assert.equal(docs[1].type, 'recipe');

      docs.every(doc => assert.isOk(doc.id));
      docs.every(doc => assert.isOk(doc.created_at));
      docs.every(doc => assert.notOk(doc.updated_at));
    });

    it('updates existing documents with ids', async () => {
      const docs = await db.saveDocs('myschema.docs', [
        {
          title: 'Alone',
          description: 'yearning in the darkness',
          price: 89.99,
          type: 'book',
          is_good: true,
          created_at: '2015-03-04T09:43:41.643Z'
        }, {
          title: 'Anytime chilli for one',
          type: 'recipe',
          ingredients: 'tears',
          is_good: false
        }
      ]);

      assert.equal(docs[0].title, 'Alone');
      assert.equal(docs[1].title, 'Anytime chilli for one');
      assert.isOk(docs[0].created_at);
      const docIds = docs.map(doc => doc.id);
      docIds.every(assert.isOk);
      docs.map(doc => doc.updated_at).every(assert.notOk);

      docs[0].title = 'Together!';
      docs[1].title = 'Anytime chilli for two';

      const updated = await db.saveDocs('myschema.docs', docs);
      assert.deepEqual(updated.map(doc => doc.id), docIds);

      assert.equal(updated[0].title, 'Together!');
      assert.equal(updated[1].title, 'Anytime chilli for two');
      updated.map(doc => doc.created_at).every(assert.isOk);
      updated.map(doc => doc.updated_at).every(assert.isOk);

      assert.equal((await db.myschema.docs.findDoc(docIds[0])).title, 'Together!');
      assert.equal((await db.myschema.docs.findDoc(docIds[1])).title, 'Anytime chilli for two');
    });

    after(function () {
      return db.dropSchema('myschema', {cascade: true});
    });
  });

  describe('without an existing table', function () {
    it('creates a table if none exists', async () => {
      const dogs = await db.saveDocs('doggies', [
        {name: 'Fido', age: 10},
        {name: 'Bowser', age: 2}
      ]);

      assert.isOk(db.doggies);
      assert.equal(dogs[0].name, 'Fido');
      assert.equal(dogs[1].name, 'Bowser');
    });

    after(function () {
      return db.query('DROP TABLE doggies;');
    });
  });

  describe('without an existing table in a schema', function () {
    before(function () {
      return db.createSchema('myschema');
    });

    it('creates a table if none exists', async () => {
      const dogs = await db.saveDocs('myschema.doggies', [
        {name: 'Fido', age: 10},
        {name: 'Bowser', age: 2}
      ]);

      assert.isOk(db.doggies);
      assert.equal(dogs[0].name, 'Fido');
      assert.equal(dogs[1].name, 'Bowser');
    });

    after(function () {
      return db.dropSchema('myschema', {cascade: true});
    });
  });
});
