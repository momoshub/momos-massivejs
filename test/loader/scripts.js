'use strict';

const path = require('path');
const loader = require('../../lib/loader/scripts');

describe('scripts', function () {
  let db;

  describe('normal operation', function () {
    before(async () => {
      db = await resetDb('empty');
    });

    after(function () {
      return db.instance.$pool.end();
    });

    it('should query for a list of scripts', async function () {
      db.loader = {scripts: path.resolve(__dirname, '../helpers/scripts/loader')};

      const scripts = await loader(db);

      assert.isArray(scripts);
      assert.lengthOf(scripts, 2);
      assert.property(scripts[0], 'name');
      assert.property(scripts[0], 'schema');
      assert.property(scripts[0], 'sql');
      assert.instanceOf(scripts[0].sql, pgp.QueryFile);
    });
  });

  describe('invalid scripts', function () {
    it.skip('throws the QueryFile error', async function () {
      let caught = false;

      await resetDb('invalid-script').catch(() => {
        caught = true;
      });

      assert.isTrue(caught);
    });
  });
});
