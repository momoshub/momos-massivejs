'use strict';

describe('initialization', function () {
  let db;

  before(function () {
    return resetDb('functions').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('wires an invocation function', function () {
    assert.isFunction(db.get_number);
    assert.isFunction(db.proc_no_params);
  });

  it('recognizes variadic functions and procs', function () {
    const fn = db.objects.find(f => f.name === 'single_variadic');
    assert.isTrue(fn.isVariadic);

    const proc = db.objects.find(p => p.name === 'proc_with_variadic_params');
    assert.isTrue(proc.isVariadic);
  });

  it('handles casing and schema', async () => {
    let res;

    assert.isOk(db.get_number);
    res = await db.get_number();
    assert.equal(res, 1);

    assert.isOk(db.GetNumber);
    res = await db.GetNumber(); // eslint-disable-line new-cap
    assert.equal(res, 2);

    assert.isOk(db.one.get_number);
    res = await db.one.get_number();
    assert.equal(res, 3);

    assert.isOk(db.one.GetNumber);
    res = await db.one.GetNumber(); // eslint-disable-line new-cap
    assert.equal(res, 4);
  });

  it('loads script files', function () {
    assert.isOk(db.namedParam);
  });

  it('preserves directory structure', function () {
    assert.isOk(db.one.two.nestedScript);
  });
});
