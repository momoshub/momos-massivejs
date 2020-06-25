'use strict';

const Executable = require('../lib/executable');
const Readable = require('../lib/readable');
const Writable = require('../lib/writable');

describe('connecting', function () {
  let loader;

  before(function () {
    // override the default PG env vars for testing since empty user information
    // will default to the current username otherwise
    process.env.PGUSER = 'postgres';
    process.env.PGDATABASE = 'massive';

    return resetDb('loader').then(db => {
      loader = db.loader;

      return db.instance.$pool.end();
    });
  });

  after(function () {
    delete process.env.PGUSER;
    delete process.env.PGDATABASE;
  });

  it('exposes the Database class from the module', function () {
    assert.isOk(massive.Database);
  });

  it('returns a database connection', function () {
    return massive({connectionString}, loader).then(db => {
      assert.isOk(db);
      assert.isOk(db.loader);
      assert.isOk(db.driverConfig);
      assert.isTrue(db.objects.length > 0);
      assert.isOk(db.t1);

      return db.instance.$pool.end();
    });
  });

  it('connects twice', async function () {
    const first = await massive({connectionString}, loader);
    let second;

    try {
      await first.query('CREATE USER multiball');
      await first.query('GRANT SELECT ON t1 TO multiball');

      second = await massive({
        connectionString: `postgres://multiball@${global.host}/massive`
      }, loader);

      assert.notEqual(first, second);
      assert.isFunction(first.fn);
      assert.isFunction(second.fn);
      assert.isAbove(first.listTables().length, second.listTables().length);
      assert.equal(second.listTables().length, 1);
    } catch (e) {
      assert.fail(e);
    } finally {
      await first.query('REVOKE SELECT ON t1 FROM multiball');
      await first.query('DROP USER multiball');

      first.instance.$pool.end();
      second.instance.$pool.end();
    }
  });

  it('connects twice in parallel', async function () {
    const db = await massive({connectionString}, loader);

    await db.query('CREATE USER multiball');
    await db.query('GRANT SELECT ON t1 TO multiball');

    await db.instance.$pool.end();

    let m1, m2;

    try {
      [m1, m2] = await Promise.all([
        massive({connectionString}),
        massive({
          connectionString: `postgres://multiball@${global.host}/massive`
        })
      ]);

      assert.isOk(m1.serverVersion);
      assert.isOk(m2.serverVersion);
    } catch (e) {
      assert.fail(e);
    } finally {
      await m1.query('REVOKE SELECT ON t1 FROM multiball');
      await m1.query('DROP USER multiball');

      m1.instance.$pool.end();
      m2.instance.$pool.end();
    }
  });

  it('accepts a receive event option on driver config', function () {
    // eslint-disable-next-line require-jsdoc
    function camelizeColumns (data) {
      const tmp = data[0];
      for (const prop in tmp) {
        const camel = pgp.utils.camelize(prop);
        if (!(camel in tmp)) {
          for (let i = 0; i < data.length; i++) {
            const d = data[i];
            d[camel] = d[prop];
            delete d[prop];
          }
        }
      }
    }

    const driverConfig = {
      receive: data => {
        camelizeColumns(data);
      }
    };

    return massive({connectionString}, loader, driverConfig).then(db => {
      assert.isOk(db);
      assert.isOk(db.loader);
      assert.isOk(db.driverConfig);
      assert.isTrue(db.objects.length > 0);
      assert.isOk(db.t1);

      return db.instance.$pool.end();
    });
  });

  describe('variations', function () {
    it('connects with a connectionString property', function () {
      return massive({connectionString}, loader).then(db => {
        assert.isOk(db);
        assert.isOk(db.t1);

        return db.instance.$pool.end();
      });
    });

    it('connects with a connection string literal', function () {
      return massive(connectionString, loader).then(db => {
        assert.isOk(db);
        assert.isOk(db.t1);

        return db.instance.$pool.end();
      });
    });

    it('connects with a property map', function () {
      return massive({host, database: 'massive', user: 'postgres'}, loader).then(db => {
        assert.isOk(db);
        assert.isOk(db.t1);

        return db.instance.$pool.end();
      });
    });

    it('rejects with connection errors', function () {
      return massive({host, database: 'doesntexist', user: 'postgres'}, loader).then(
        () => { assert.fail(); },
        err => {
          assert.equal(err.code, '3D000');
        }
      );
    });

    it.skip('connects with undefined connections using default configuration', function () {
      return massive().then(db => {
        assert.isOk(db);

        return db.instance.$pool.end();
      });
    });

    it.skip('connects with empty connection block using default configuration', function () {
      return massive({}).then(db => {
        assert.isOk(db);

        return db.instance.$pool.end();
      });
    });

    it.skip('connects with empty connection strings using default configuration', function () {
      return massive('').then(db => {
        assert.isOk(db);

        return db.instance.$pool.end();
      });
    });
  });

  describe('configuration', function () {
    it('allows undefined scripts directories', function () {
      const testLoader = _.cloneDeep({}, loader);

      delete testLoader.scripts;

      return massive(connectionString, testLoader).then(db => {
        assert.lengthOf(db.objects.filter(f => f instanceof Executable), 4);
        assert.lengthOf(db.objects.filter(f => f.sql instanceof pgp.QueryFile), 0);

        return db.instance.$pool.end();
      });
    });

    it('exposes driver defaults through pg-promise', function () {
      return massive(connectionString, loader).then(db => {
        assert.isDefined(db.pgp.pg.defaults.parseInputDatesAsUTC);

        return db.instance.$pool.end();
      });
    });
  });

  describe('object loading', function () {
    it('loads non-public schemata as namespace properties', function () {
      return massive({connectionString}, loader).then(db => {
        assert.isOk(db.one);
        assert.isOk(db.two);
        assert.isOk(db.one.t1);
        assert.isOk(db.one.v1);
        assert.isOk(db.one.f1);

        assert.eventually.equal(db.one.t1.count(), 0);

        return db.instance.$pool.end();
      });
    });

    it('loads all tables', function () {
      return massive({connectionString}, loader).then(db => {
        assert.instanceOf(db.t1, Writable);
        assert.instanceOf(db.t2, Writable);
        assert.instanceOf(db.t3, Writable);
        assert.instanceOf(db.tA, Writable);
        assert.instanceOf(db.one.t1, Writable);
        assert.instanceOf(db.one.t2, Writable);
        assert.instanceOf(db.two.t1, Writable);

        return db.instance.$pool.end();
      });
    });

    it('loads all views', function () {
      return massive({connectionString}, loader).then(db => {
        assert.instanceOf(db.v1, Readable);
        assert.instanceOf(db.v2, Readable);
        assert.instanceOf(db.mv1, Readable);
        assert.instanceOf(db.mv2, Readable);
        assert.instanceOf(db.one.v1, Readable);
        assert.instanceOf(db.one.v2, Readable);

        return db.instance.$pool.end();
      });
    });

    it('loads query files and functions', function () {
      const quietLoader = _.defaults({noWarnings: true}, loader);

      return massive(connectionString, quietLoader).then(db => {
        assert.isTrue(db.objects.filter(o => o instanceof Executable).length > 1);
        assert.lengthOf(db.objects.filter(f => f.sql instanceof pgp.QueryFile), 2); // schema.sql, fn.sql

        return db.instance.$pool.end();
      });
    });

    it('loads everything it can by default', function () {
      const quietLoader = _.defaults({noWarnings: true}, loader);

      return massive(connectionString, quietLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isOk(db.two);

        assert.instanceOf(db.t1, Writable);
        assert.instanceOf(db.t2, Writable);
        assert.instanceOf(db.t3, Writable);
        assert.instanceOf(db.tA, Writable);
        assert.instanceOf(db.one.t1, Writable);
        assert.instanceOf(db.one.t2, Writable);
        assert.instanceOf(db.two.t1, Writable);

        assert.instanceOf(db.v1, Readable);
        assert.instanceOf(db.v2, Readable);
        assert.instanceOf(db.mv1, Readable);
        assert.instanceOf(db.mv2, Readable);
        assert.instanceOf(db.one.v1, Readable);
        assert.instanceOf(db.one.v2, Readable);

        assert.isFunction(db.f1, Executable);
        assert.isFunction(db.f2, Executable);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });

    it('excludes materialized views', function () {
      const testLoader = _.defaults({
        noWarnings: true,
        excludeMatViews: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isOk(db.two);

        assert.instanceOf(db.t1, Writable);
        assert.instanceOf(db.t2, Writable);
        assert.instanceOf(db.t3, Writable);
        assert.instanceOf(db.tA, Writable);
        assert.instanceOf(db.one.t1, Writable);
        assert.instanceOf(db.one.t2, Writable);
        assert.instanceOf(db.two.t1, Writable);

        assert.instanceOf(db.v1, Readable);
        assert.instanceOf(db.v2, Readable);
        assert.isUndefined(db.mv1);
        assert.isUndefined(db.mv2);
        assert.instanceOf(db.one.v1, Readable);
        assert.instanceOf(db.one.v2, Readable);

        assert.isFunction(db.f1, Executable);
        assert.isFunction(db.f2, Executable);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });
  });

  describe('schema filters', function () {
    it('applies filters', function () {
      const testLoader = _.defaults({
        allowedSchemas: 'one, two',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isOk(db.two);

        assert.isUndefined(db.t1);
        assert.isUndefined(db.t2);
        assert.isUndefined(db.t3);
        assert.isUndefined(db.tA);
        assert.instanceOf(db.one.t1, Writable);
        assert.instanceOf(db.one.t2, Writable);
        assert.instanceOf(db.two.t1, Writable);

        assert.isUndefined(db.v1);
        assert.isUndefined(db.v2);
        assert.isUndefined(db.mv1);
        assert.isUndefined(db.mv2);
        assert.instanceOf(db.one.v1, Readable);
        assert.instanceOf(db.one.v2, Readable);

        assert.isUndefined(db.f1);
        assert.isUndefined(db.f2);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });

    it('allows exceptions', function () {
      const testLoader = _.defaults({
        allowedSchemas: 'two',
        exceptions: 't1, v1, one.v2',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isOk(db.two);

        assert.instanceOf(db.t1, Writable);
        assert.isUndefined(db.t2);
        assert.isUndefined(db.t3);
        assert.isUndefined(db.tA);
        assert.isUndefined(db.one.t1);
        assert.isUndefined(db.one.t2);
        assert.instanceOf(db.two.t1, Writable);

        assert.instanceOf(db.v1, Readable);
        assert.isUndefined(db.v2);
        assert.isUndefined(db.mv1);
        assert.isUndefined(db.mv2);
        assert.isUndefined(db.one.v1);
        assert.instanceOf(db.one.v2, Readable);

        assert.isUndefined(db.f1);
        assert.isUndefined(db.f2);
        assert.isUndefined(db.one.f1);
        assert.isUndefined(db.one.f2);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });
  });

  describe('table blacklists', function () {
    it('applies blacklists to tables and views', function () {
      const testLoader = _.defaults({
        blacklist: '%1, one.%2',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isUndefined(db.two);

        assert.isUndefined(db.t1);
        assert.instanceOf(db.t2, Writable);
        assert.instanceOf(db.t3, Writable);
        assert.instanceOf(db.tA, Writable);
        assert.isUndefined(db.one.t1);
        assert.isUndefined(db.one.t2);

        assert.isUndefined(db.v1);
        assert.instanceOf(db.v2, Readable);
        assert.isUndefined(db.mv1);
        assert.instanceOf(db.mv2, Readable);
        assert.isUndefined(db.one.v1);
        assert.isUndefined(db.one.v2);

        assert.isFunction(db.f1, Executable);
        assert.isFunction(db.f2, Executable);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });

    it('checks schema names in the pattern', function () {
      const testLoader = _.defaults({
        blacklist: 'one.%1',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isOk(db.two);

        assert.instanceOf(db.t1, Writable);
        assert.instanceOf(db.t2, Writable);
        assert.instanceOf(db.t3, Writable);
        assert.instanceOf(db.tA, Writable);
        assert.isUndefined(db.one.t1);
        assert.instanceOf(db.one.t2, Writable);
        assert.instanceOf(db.two.t1, Writable);

        assert.instanceOf(db.v1, Readable);
        assert.instanceOf(db.v2, Readable);
        assert.instanceOf(db.mv1, Readable);
        assert.instanceOf(db.mv2, Readable);
        assert.isUndefined(db.one.v1);
        assert.instanceOf(db.one.v2, Readable);

        assert.isFunction(db.f1, Executable);
        assert.isFunction(db.f2, Executable);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });

    it('allows exceptions', function () {
      const testLoader = _.defaults({
        blacklist: '%1',
        exceptions: 'one.%1',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isUndefined(db.two);

        assert.isUndefined(db.t1);
        assert.instanceOf(db.t2, Writable);
        assert.instanceOf(db.t3, Writable);
        assert.instanceOf(db.tA, Writable);
        assert.instanceOf(db.one.t1, Writable);
        assert.instanceOf(db.one.t2, Writable);

        assert.isUndefined(db.v1);
        assert.instanceOf(db.v2, Readable);
        assert.isUndefined(db.mv1);
        assert.instanceOf(db.mv2, Readable);
        assert.instanceOf(db.one.v1, Readable);
        assert.instanceOf(db.one.v2, Readable);

        assert.isFunction(db.f1, Executable);
        assert.isFunction(db.f2, Executable);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });
  });

  describe('table whitelists', function () {
    it('applies a whitelist with exact matching', function () {
      const testLoader = _.defaults({
        whitelist: 't1, one.t1',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isUndefined(db.two);

        assert.instanceOf(db.t1, Writable);
        assert.isUndefined(db.t2);
        assert.isUndefined(db.t3);
        assert.isUndefined(db.tA);
        assert.instanceOf(db.one.t1, Writable);
        assert.isUndefined(db.one.t2);

        assert.isUndefined(db.v1);
        assert.isUndefined(db.v2);
        assert.isUndefined(db.mv1);
        assert.isUndefined(db.mv2);
        assert.isUndefined(db.one.v1);
        assert.isUndefined(db.one.v2);

        assert.isFunction(db.f1, Executable);
        assert.isFunction(db.f2, Executable);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });

    it('overrides other filters', function () {
      const testLoader = _.defaults({
        allowedSchemas: 'one',
        blacklist: 't1',
        whitelist: 't1',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.isOk(db);
        assert.isOk(db.one);
        assert.isUndefined(db.two);

        assert.instanceOf(db.t1, Writable);
        assert.isUndefined(db.t2);
        assert.isUndefined(db.t3);
        assert.isUndefined(db.tA);
        assert.isUndefined(db.one.t1);
        assert.isUndefined(db.one.t2);

        assert.isUndefined(db.v1);
        assert.isUndefined(db.v2);
        assert.isUndefined(db.mv1);
        assert.isUndefined(db.mv2);
        assert.isUndefined(db.one.v1);
        assert.isUndefined(db.one.v2);

        assert.isUndefined(db.f1);
        assert.isUndefined(db.f2);
        assert.isFunction(db.one.f1, Executable);
        assert.isFunction(db.one.f2, Executable);

        assert.isFunction(db.schema, Executable);

        return db.instance.$pool.end();
      });
    });
  });

  describe('function exclusion', function () {
    it('skips loading functions when set', function () {
      const testLoader = _.defaults({
        excludeFunctions: true,
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.lengthOf(db.objects.filter(o => o instanceof Executable), 2);
        assert.lengthOf(db.objects.filter(o => o instanceof Executable && o.sql instanceof pgp.QueryFile), 2);

        return db.instance.$pool.end();
      });
    });

    it('loads all functions when false', function () {
      const testLoader = _.defaults({
        excludeFunctions: false,
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert.lengthOf(db.objects.filter(o => o instanceof Executable), 6);
        assert.lengthOf(db.objects.filter(o => o instanceof Executable && o.sql instanceof pgp.QueryFile), 2);

        return db.instance.$pool.end();
      });
    });
  });

  describe('function filtering', function () {
    it('blacklists functions', function () {
      const testLoader = _.defaults({
        functionBlacklist: '%1, one.f2',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert(!db.f1 && !!db.f2);
        assert(!!db.one && !db.one.f1 && !db.one.f2);

        return db.instance.$pool.end();
      });
    });

    it('whitelists functions', function () {
      const testLoader = _.defaults({
        functionWhitelist: '%1, one.f2',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert(!!db.f1 && !db.f2);
        assert(!!db.one && !!db.one.f1 && !!db.one.f2);

        return db.instance.$pool.end();
      });
    });

    it('applies exceptions', function () {
      const testLoader = _.defaults({
        allowedSchemas: 'one',
        functionBlacklist: 'one.%1',
        exceptions: 'one.f2',
        noWarnings: true
      }, loader);

      return massive(connectionString, testLoader).then(db => {
        assert(!db.f1 && !db.f2);
        assert(!!db.one && !db.one.f1 && !!db.one.f2);

        return db.instance.$pool.end();
      });
    });
  });
});
