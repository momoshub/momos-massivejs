'use strict';

describe('transactions', function () {
  let db;

  before(function () {
    return resetDb('singletable').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  describe('withConnection', function () {
    it('runs queries', function () {
      return db.withConnection(task => {
        let promise = task.products.insert({string: 'alpha'});

        promise = promise.then(record => {
          assert.isOk(record);
          assert.isTrue(record.id > 0);
          assert.equal(record.string, 'alpha');

          return task.products.save({id: record.id, description: 'test'});
        });

        return promise;
      }).then(record => {
        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.string, 'alpha');
        assert.equal(record.description, 'test');

        return db.products.find(record.id).then(persisted => {
          assert.isOk(persisted);
        });
      });
    });

    it('applies options', function () {
      return db.withConnection(task => {
        let promise = task.products.insert({string: 'alpha'});

        promise = promise.then(record => {
          assert.isOk(record);
          assert.isTrue(record.id > 0);
          assert.equal(record.string, 'alpha');

          return task.products.save({id: record.id, description: 'test'});
        });

        assert.equal(task.instance.ctx.tag, 'my task');

        return promise;
      }, {
        tag: 'my task'
      }).then(record => {
        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.string, 'alpha');
        assert.equal(record.description, 'test');

        return db.products.find(record.id).then(persisted => {
          assert.isOk(persisted);
        });
      });
    });

    it('does not roll back', function () {
      let total;

      return db.products.count().then(count => {
        total = count;

        return db.instance.$config.promise.resolve();
      }).then(() => {
        return db.withConnection(task => {
          let promise = task.products.insert({string: 'beta'});

          promise = promise.then(record => {
            assert.isOk(record);
            assert.isTrue(record.id > 0);
            assert.equal(record.string, 'beta');

            return task.products.save({id: 'not an int', description: 'test'});
          });

          return promise;
        }).then(() => {
          assert.fail();
        }).catch(err => {
          assert.isOk(err);
          assert.equal(err.code, '22P02');

          return db.products.count().then(count => {
            assert.notEqual(count, total);
          });
        });
      });
    });
  });

  describe('withTransaction', function () {
    it('runs queries in transactions', function () {
      return db.withTransaction(async tx => {
        const record = await tx.products.insert({string: 'alpha'});

        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.string, 'alpha');

        return tx.products.save({id: record.id, description: 'test'});
      }).then(record => {
        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.string, 'alpha');
        assert.equal(record.description, 'test');
      });
    });

    it('applies options', function () {
      return db.withTransaction(tx => {
        let promise = tx.products.insert({string: 'alpha'});

        promise = promise.then(record => {
          assert.isOk(record);
          assert.isTrue(record.id > 0);
          assert.equal(record.string, 'alpha');

          return tx.products.save({id: record.id, description: 'test'});
        });

        return promise;
      }, {
        mode: new db.pgp.txMode.TransactionMode({
          tiLevel: db.pgp.txMode.isolationLevel.serializable
        })
      }).then(record => {
        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.string, 'alpha');
        assert.equal(record.description, 'test');

        return db.products.find(record.id).then(persisted => {
          assert.isOk(persisted);
        });
      });
    });

    it('selects for update with sorting and limiting', function () {
      return db.withTransaction(tx => {
        let promise = tx.products.insert({string: 'alpha'});

        promise = promise.then(record => {
          assert.isOk(record);
          assert.isTrue(record.id > 0);
          assert.equal(record.string, 'alpha');

          return tx.products.findOne({id: record.id}, {
            order: [{field: 'string', direction: 'desc'}],
            lock: {
              strength: 'UPDATE'
            }
          });
        });

        return promise;
      }, {
        mode: new db.pgp.txMode.TransactionMode({
          tiLevel: db.pgp.txMode.isolationLevel.serializable
        })
      }).then(record => {
        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.string, 'alpha');
      });
    });

    it('reloads and applies DDL', function () {
      return db.withTransaction(async tx => {
        await tx.query('create table test1 (id serial not null primary key, val text not null)');

        tx = await tx.reload();

        const record = await tx.test1.insert({val: 'hi!'});

        assert.isOk(record);
        assert.isTrue(record.id > 0);
        assert.equal(record.val, 'hi!');
      }, {
        mode: new db.pgp.txMode.TransactionMode({
          tiLevel: db.pgp.txMode.isolationLevel.serializable
        })
      });
    });

    it('rolls back DDL', function () {
      return db.withTransaction(async tx => {
        await tx.query('create table test2 (id serial not null primary key, val text not null)');

        tx = await tx.reload();

        await tx.test2.insert({val: null});
      }, {
        mode: new db.pgp.txMode.TransactionMode({
          tiLevel: db.pgp.txMode.isolationLevel.serializable
        })
      })
        .then(() => { assert.fail(); })
        .catch(async err => {
          assert.isOk(err);

          await db.reload();

          assert.notInclude(db.listTables(), 'test2');
        });
    });

    it('rolls back if anything rejects', function () {
      let total;

      return db.products.count().then(count => {
        total = count;

        return db.instance.$config.promise.resolve();
      }).then(() => {
        return db.withTransaction(tx => {
          let promise = tx.products.insert({string: 'beta'});

          promise = promise.then(record => {
            assert.isOk(record);
            assert.isTrue(record.id > 0);
            assert.equal(record.string, 'beta');

            return tx.products.save({id: 'not an int', description: 'test'});
          });

          return promise;
        }).then(() => {
          assert.fail();
        }).catch(err => {
          assert.isOk(err);
          assert.equal(err.code, '22P02');

          return db.products.count().then(count => {
            assert.equal(count, total);
          });
        });
      });
    });

    it('rolls functions back too', function () {
      let total;

      return db.products.count().then(count => {
        total = count;

        return db.instance.$config.promise.resolve();
      }).then(() => {
        return db.withTransaction(tx => {
          assert.notStrictEqual(tx.fn.executable, db.fn.executable);

          let promise = tx.fn({single: true});

          promise = promise.then(record => {
            assert.isOk(record);
            assert.isTrue(record.id > 0);
            assert.equal(record.string, 'beta');

            return tx.products.save({id: 'not an int', description: 'test'});
          });

          return promise;
        }).then(() => {
          assert.fail();
        }).catch(err => {
          assert.isOk(err);
          assert.equal(err.code, '22P02');

          return db.products.count().then(count => {
            assert.equal(count, total);
          });
        });
      });
    });

    it('rejects with expected errors', function () {
      return db.withTransaction(tx => {
        tx.products.save([])
          .then(() => { assert.fail(); })
          .catch(err => {
            assert.equal(err.message, 'Must provide an object with all fields being modified and the primary key if updating');
          });
      });
    });
  });
});
