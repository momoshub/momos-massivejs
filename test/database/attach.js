'use strict';

const Executable = require('../../lib/executable');
const Writable = require('../../lib/writable');

describe('attaching entities', function () {
  let db;

  before(function () {
    return resetDb('loader').then(instance => db = instance);
  });

  beforeEach(async () => {
    await db.reload();
  });

  after(function () {
    return db.instance.$pool.end();
  });

  describe('object merges', function () {
    it('merges schemas and folders', function () {
      db.attach(new Writable({
        schema: 'newschema',
        name: 't1',
        columns: ['val'],
        db
      }));

      db.attach(new Executable({
        name: 'script1',
        path: 'newschema.script1',
        sql: 'select 2 as val',
        arity: 0,
        db
      }));

      assert.isOk(db.newschema.t1);
      assert.isOk(db.newschema.script1);
    });

    it('merges schemas and tables', function () {
      db.attach(new Writable({
        schema: 'schema_or_table',
        name: 't1',
        columns: ['val'],
        db
      }));

      db.attach(new Writable({
        schema: 'public',
        name: 'schema_or_table',
        columns: ['val'],
        db
      }));

      assert.isOk(db.schema_or_table);
      assert.isOk(db.schema_or_table.t1);
      assert.isOk(db.schema_or_table.find);
      assert.isTrue(db.schema_or_table instanceof Writable);
      assert.isTrue(db.schema_or_table.t1 instanceof Writable);
    });

    it('merges schemas and executables', async () => {
      db.attach(new Writable({
        schema: 'schema_or_function',
        name: 't1',
        columns: ['val'],
        db
      }));

      db.attach(new Executable({
        name: 'schema_or_function',
        path: 'schema_or_function',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      assert.isOk(db.schema_or_function);
      assert.isOk(db.schema_or_function.t1);
      assert.isFunction(db.schema_or_function);
      assert.isTrue(db.schema_or_function.t1 instanceof Writable);

      const result = await db.schema_or_function();

      assert.equal(result[0].val, 1);
    });

    it('merges schemas, executables, and tables', async () => {
      db.attach(new Writable({
        schema: 'three_things',
        name: 't1',
        columns: ['val'],
        db
      }));

      db.attach(new Executable({
        name: 'three_things',
        path: 'three_things',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      db.attach(new Writable({
        schema: 'public',
        name: 'three_things',
        columns: ['val'],
        db
      }));

      assert.isOk(db.three_things);
      assert.isOk(db.three_things.t1);
      assert.isFunction(db.three_things);
      assert.isFunction(db.three_things.find);
      assert.isFunction(db.three_things.t1.find);
      assert.isTrue(db.three_things instanceof Writable);
      assert.isTrue(db.three_things.t1 instanceof Writable);

      const result = await db.three_things();

      assert.equal(result[0].val, 1);
    });

    it('merges folders and tables', async () => {
      db.attach(new Executable({
        name: 'script',
        path: 'folder_or_table.script1',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      db.attach(new Writable({
        schema: 'public',
        name: 'folder_or_table',
        columns: ['val'],
        db
      }));

      db.attach(new Executable({
        name: 'script',
        path: 'folder_or_table.subfolder.script2',
        sql: 'select 2 as val',
        isVariadic: false,
        db
      }));

      assert.isFunction(db.folder_or_table.script1);
      assert.isFunction(db.folder_or_table.subfolder.script2);
      assert.isTrue(db.folder_or_table instanceof Writable);
      assert.isFunction(db.folder_or_table.find);

      const result1 = await db.folder_or_table.script1();
      const result2 = await db.folder_or_table.subfolder.script2();

      assert.equal(result1[0].val, 1);
      assert.equal(result2[0].val, 2);
    });

    it('errors if an existing entity method is shadowed', function () {
      db.attach(new Writable({
        schema: 'public',
        name: 'folder_or_table',
        columns: ['val'],
        db
      }));

      assert.throws(() => {
        db.attach(new Executable({
          name: 'script',
          path: 'folder_or_table.findOne',
          sql: 'select 1 as val',
          isVariadic: false,
          db
        }));
      });
    });

    it('errors if a new entity method will shadow an existing node', function () {
      db.attach(new Executable({
        name: 'script',
        path: 'folder_or_table.findOne',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      assert.throws(() => {
        db.attach(new Writable({
          schema: 'public',
          name: 'folder_or_table',
          columns: ['val'],
          db
        }));
      });
    });

    it('merges folders and executables', async () => {
      db.attach(new Executable({
        name: 'folder_or_function',
        path: 'folder_or_function',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      db.attach(new Executable({
        name: 'script',
        path: 'folder_or_function.script',
        sql: 'select 2 as val',
        isVariadic: false,
        db
      }));

      assert.isFunction(db.folder_or_function);
      assert.isFunction(db.folder_or_function.script);

      const result1 = await db.folder_or_function();
      const result2 = await db.folder_or_function.script();

      assert.equal(result1[0].val, 1);
      assert.equal(result2[0].val, 2);
    });

    it('merges tables and executables', async () => {
      db.attach(new Executable({
        name: 'function_or_table',
        path: 'function_or_table',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      db.attach(new Writable({
        schema: 'public',
        name: 'function_or_table',
        columns: ['val'],
        db
      }));

      assert.isFunction(db.function_or_table);
      assert.isTrue(db.function_or_table instanceof Writable);

      const result1 = await db.function_or_table();
      const result2 = await db.function_or_table.find({}, {build: true});

      assert.equal(result1[0].val, 1);
      assert.deepEqual(result2, {
        sql: 'SELECT * FROM "function_or_table" WHERE TRUE',
        params: []
      });
    });

    it('errors if a script overwrites a function', function () {
      db.attach(new Executable({
        name: 'function_or_script',
        path: 'function_or_script',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      assert.throws(() => {
        db.attach(new Executable({
          name: 'function_or_script',
          path: 'function_or_script',
          sql: 'select 2 as val',
          arity: 0,
          db
        }));
      });
    });

    it('errors if a function overwrites a script', function () {
      db.attach(new Executable({
        name: 'function_or_script',
        path: 'function_or_script',
        sql: 'select 1 as val',
        db
      }));

      assert.throws(() => {
        db.attach(new Executable({
          name: 'function_or_script',
          path: 'function_or_script',
          sql: 'select 2 as val',
          arity: 0,
          isVariadic: false,
          db
        }));
      });
    });

    it('allows multiple function signatures', function () {
      db.attach(new Executable({
        name: 'myfunc',
        path: 'myfunc',
        sql: 'select 1 as val',
        isVariadic: false,
        db
      }));

      db.attach(new Executable({
        name: 'myfunc',
        path: 'myfunc',
        sql: 'select 2 as val',
        isVariadic: false,
        db
      }));
    });
  });
});
