'use strict';

describe('join', function () {
  let db;

  before(function () {
    return resetDb('foreign-keys').then(instance => db = instance);
  });

  after(function () {
    return db.instance.$pool.end();
  });

  it('joins a relation with a type and keys', function () {
    return db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).find({
      'alpha.id': 3
    }).then(result => {
      assert.deepEqual(result, [{
        id: 3,
        val: 'three',
        beta: [{
          id: 3, alpha_id: 3, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, val: 'alpha three again'
        }]
      }]);
    });
  });

  it('joins a view with an explicit pk', function () {
    return db.alpha.join({
      beta_view: {
        pk: 'id',
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).find({
      'alpha.id': 3
    }).then(result => {
      assert.deepEqual(result, [{
        id: 3,
        val: 'three',
        beta_view: [{
          id: 3, alpha_id: 3, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, val: 'alpha three again'
        }]
      }]);
    });
  });

  it('joins from a view with an explicit pk', function () {
    return db.beta_view.join({
      pk: 'id',
      alpha: {
        pk: 'id',
        type: 'INNER',
        on: {id: 'alpha_id'}
      }
    }).find({
      'alpha.id': 3
    }).then(result => {
      assert.deepEqual(result, [{
        id: 3, alpha_id: 3, val: 'alpha three',
        alpha: [{id: 3, val: 'three'}]
      }, {
        id: 4, alpha_id: 3, val: 'alpha three again',
        alpha: [{id: 3, val: 'three'}]
      }]);
    });
  });

  it('joins a relation in another schema', function () {
    return db.alpha.join({
      epsilon: {
        relation: 'sch.epsilon',
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).find({}).then(result => {
      assert.deepEqual(result, [{
        id: 1, val: 'one',
        epsilon: [{
          id: 1, alpha_id: 1, val: 'alpha one'
        }]
      }]);
    });
  });

  it('joins multiple tables at multiple levels', function () {
    return db.alpha.join({
      beta: {
        on: {alpha_id: 'id'},
        gamma: {
          on: {beta_id: 'beta.id'}
        },
        'sch.delta': {
          on: {beta_id: 'beta.id'}
        }
      },
      'sch.epsilon': {
        on: {alpha_id: 'id'}
      }
    }).find({
      'alpha.id =': 1
    }).then(result => {
      assert.deepEqual(result, [{
        id: 1, val: 'one',
        beta: [{
          id: 1, alpha_id: 1, val: 'alpha one',
          gamma: [{
            id: 1, beta_id: 1, alpha_id_one: 1, alpha_id_two: 1,
            val: 'alpha one alpha one beta one'
          }],
          delta: [{
            id: 1, beta_id: 1, val: 'beta one'
          }]
        }],
        epsilon: [{id: 1, alpha_id: 1, val: 'alpha one'}]
      }]);
    });
  });

  it('can join on any fields', function () {
    return db.beta.join({
      'sch.epsilon': {
        type: 'INNER',
        on: {val: 'val'}
      }
    }).find({}).then(result => {
      assert.deepEqual(result, [{
        id: 1,
        alpha_id: 1,
        val: 'alpha one',
        epsilon: [{
          id: 1,
          alpha_id: 1,
          val: 'alpha one'
        }]
      }]);
    });
  });

  it('changes join types', function () {
    return db.alpha.join({
      beta: {
        type: 'left outer',
        on: {alpha_id: 'id'}
      }
    }).find({
      'alpha.id >': 1
    }).then(result => {
      assert.deepEqual(result, [{
        id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, val: 'alpha two'}]
      }, {
        id: 3, val: 'three', beta: [{
          id: 3, alpha_id: 3, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, val: 'alpha three again'
        }]
      }, {
        id: 4, val: 'four', beta: []
      }]);
    });
  });

  it('decomposes multiple records', function () {
    return db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).find({
      'alpha.id >': 1
    }).then(result => {
      assert.deepEqual(result, [{
        id: 2, val: 'two',
        beta: [{
          id: 2, alpha_id: 2, val: 'alpha two'
        }]
      }, {
        id: 3, val: 'three',
        beta: [{
          id: 3, alpha_id: 3, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, val: 'alpha three again'
        }]
      }]);
    });
  });

  it('joins to a joined relation instead of the origin', function () {
    return db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'},
        gamma: {
          type: 'INNER',
          on: {beta_id: 'beta.id'}
        }
      }
    }).find({
      'alpha.id >': 1
    }).then(result => {
      assert.deepEqual(result, [{
        id: 2, val: 'two',
        beta: [{
          id: 2, alpha_id: 2, val: 'alpha two',
          gamma: [{
            id: 2, beta_id: 2, alpha_id_one: 1, alpha_id_two: 2,
            val: 'alpha two alpha two beta two'
          }, {
            id: 3, beta_id: 2, alpha_id_one: 2, alpha_id_two: 3,
            val: 'alpha two alpha three beta two again'
          }]
        }]
      }, {
        id: 3, val: 'three',
        beta: [{
          id: 3, alpha_id: 3, val: 'alpha three',
          gamma: [{
            id: 4, beta_id: 3, alpha_id_one: 2, alpha_id_two: null,
            val: 'alpha two (alpha null) beta three'
          }]
        }, {
          id: 4, alpha_id: 3, val: 'alpha three again',
          gamma: [{
            id: 5, beta_id: 4, alpha_id_one: 3, alpha_id_two: 1,
            val: 'alpha three alpha one beta four'
          }]
        }]
      }]);
    });
  });

  it('caches readables', function () {
    db.entityCache = {};

    const a = db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    });

    const b = db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    });

    const c = db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'i'}
      }
    });

    assert.equal(a, b);
    assert.notEqual(a, c);
    assert.notEqual(b, c);
    assert.lengthOf(Object.keys(db.entityCache), 2);
  });

  it('errors when the origin name reappears', function () {
    assert.throws(() => db.alpha.join({
      alpha: {
        on: {id: 'id'}
      }
    }), 'Bad join definition: alpha is repeated.');
  });

  it('errors when another relation name reappears', function () {
    assert.throws(() => db.alpha.join({
      beta: {
        on: {alpha_id: 'id'},
        beta: {
          on: {id: 'id'}
        }
      }
    }), 'Bad join definition: beta is repeated.');
  });

  it('errors for invalid explicit relations', function () {
    assert.throws(() => db.alpha.join({
      alias: {
        type: 'INNER',
        relation: 'qwertyuiop',
        on: {alpha_id: 'id'}
      }
    }), 'Bad join definition: unknown database entity qwertyuiop.');
  });

  it('errors for invalid implicit relations', function () {
    assert.throws(() => db.alpha.join({
      qwertyuiop: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }), 'Bad join definition: unknown database entity qwertyuiop.');
  });

  describe('aliasing', function () {
    it('defers to an explicit relation but aliases to the key', function () {
      return db.alpha.join({
        asdf: {
          type: 'INNER',
          relation: 'beta',
          on: {alpha_id: 'id'}
        }
      }).find({
        'alpha.id': 3
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3,
          val: 'three',
          asdf: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('aliases to the table name when processing an implicit relation with a schema', function () {
      return db.alpha.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).find({}, {build: false}).then(result => {
        assert.deepEqual(result, [{
          id: 1, val: 'one',
          epsilon: [{
            id: 1, alpha_id: 1, val: 'alpha one'
          }]
        }]);
      });
    });

    it('joins the same table multiple times under different aliases', function () {
      return db.gamma.join({
        alpha1: {
          type: 'INNER',
          relation: 'alpha',
          on: {id: 'alpha_id_one'}
        },
        alpha2: {
          type: 'INNER',
          relation: 'alpha',
          on: {id: 'alpha_id_two'}
        }
      }).find({
        'alpha1.id': 3
      }).then(result => {
        assert.deepEqual(result, [{
          id: 5,
          beta_id: 4,
          alpha_id_one: 3,
          alpha_id_two: 1,
          val: 'alpha three alpha one beta four',
          alpha1: [{id: 3, val: 'three'}],
          alpha2: [{id: 1, val: 'one'}]
        }]);
      });
    });

    it('allows self joins with an alias', function () {
      return db.alpha.join({
        alpha_again: {
          relation: 'alpha',
          on: {id: 'id'}
        }
      }).find({
        id: 1
      }).then(result => {
        assert.deepEqual(result, [{
          id: 1, val: 'one',
          alpha_again: [{
            id: 1, val: 'one'
          }]
        }]);
      });
    });

    it('processes keys referencing the relation instead of the alias', function () {
      return db.alpha.join({
        epsilon: {
          type: 'INNER',
          relation: 'sch.epsilon',
          on: {alpha_id: 'id'}
        }
      }).find({
        'sch.epsilon.val': 'alpha one'
      }, {build: false}).then(result => {
        assert.deepEqual(result, [{
          id: 1, val: 'one',
          epsilon: [{
            id: 1, alpha_id: 1, val: 'alpha one'
          }]
        }]);
      });
    });
  });

  describe('defaults and shortcuts', function () {
    it('defaults to inner joins', function () {
      return db.alpha.join({
        beta: {
          on: {alpha_id: 'id'}
        }
      }).find({
        'alpha.id >': 1
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, val: 'alpha two'}]
        }, {
          id: 3, val: 'three', beta: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('does a basic inner join with the bare minimum object', function () {
      return db.alpha.join({
        beta: true
      }).find({
        'alpha.id >': 1
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, val: 'alpha two'}]
        }, {
          id: 3, val: 'three', beta: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('does a basic inner join with just a string', function () {
      return db.alpha.join('beta').find({
        'alpha.id >': 1
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, val: 'alpha two'}]
        }, {
          id: 3, val: 'three', beta: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('shortcuts criteria keys without path info to the primary table', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).find({
        id: 3
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3, val: 'three',
          beta: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('shortcuts ordering keys without path info to the primary table', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).find({
        'alpha.id': 3
      }, {
        order: [{field: 'val', direction: 'desc'}],
        build: true
      }).then(result => {
        assert.equal(result.sql, [
          'SELECT "alpha"."id" AS "alpha__id",',
          '"alpha"."val" AS "alpha__val",',
          '"beta"."alpha_id" AS "beta__alpha_id",',
          '"beta"."id" AS "beta__id","beta"."val" AS "beta__val" ',
          'FROM "alpha" ',
          'INNER JOIN "beta" ON ("beta"."alpha_id" = "alpha"."id") ',
          'WHERE "alpha"."id" = $1 ',
          'ORDER BY "alpha"."val" DESC'
        ].join(''));
        assert.deepEqual(result.params, [3]);
      });
    });
  });

  describe('autogenerating based on foreign keys', function () {
    it('autogenerates keys when one possible join fk matches', function () {
      return db.alpha.join({
        beta: {type: 'INNER'}
      }).find({
        'alpha.id': 3
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3,
          val: 'three',
          beta: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('autogenerates keys when one possible origin fk matches', function () {
      return db.beta.join({
        alpha: {type: 'INNER'}
      }).find({
        'alpha.id': 3
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3, alpha_id: 3, val: 'alpha three',
          alpha: [{
            id: 3,
            val: 'three'
          }]
        }, {
          id: 4, alpha_id: 3, val: 'alpha three again',
          alpha: [{
            id: 3,
            val: 'three'
          }]
        }]);
      });
    });

    it('autogenerates keys deeper in the join tree', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          gamma: {
            type: 'INNER'
          }
        }
      }).find({
        'alpha.id >': 1
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2, val: 'two',
          beta: [{
            id: 2, alpha_id: 2, val: 'alpha two',
            gamma: [{
              id: 2, beta_id: 2, alpha_id_one: 1, alpha_id_two: 2,
              val: 'alpha two alpha two beta two'
            }, {
              id: 3, beta_id: 2, alpha_id_one: 2, alpha_id_two: 3,
              val: 'alpha two alpha three beta two again'
            }]
          }]
        }, {
          id: 3, val: 'three',
          beta: [{
            id: 3, alpha_id: 3, val: 'alpha three',
            gamma: [{
              id: 4, beta_id: 3, alpha_id_one: 2, alpha_id_two: null,
              val: 'alpha two (alpha null) beta three'
            }]
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again',
            gamma: [{
              id: 5, beta_id: 4, alpha_id_one: 3, alpha_id_two: 1,
              val: 'alpha three alpha one beta four'
            }]
          }]
        }]);
      });
    });

    it('errors if keys are not specified and there are no possible fks', function () {
      assert.throws(() => db.beta.join({
        'sch.epsilon': {type: 'INNER'}
      }), 'An explicit \'on\' mapping is required for sch.epsilon.');
    });

    it('errors if keys are not specified and multiple possible fks match', function () {
      assert.throws(() => db.gamma.join({
        alpha: {type: 'INNER'}
      }), 'Ambiguous foreign keys for alpha. Define join keys explicitly.');
    });
  });

  describe('options', function () {
    it('applies options', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).find({
        'alpha.id': 3
      }, {build: true}).then(result => {
        assert.equal(result.sql, [
        'SELECT "alpha"."id" AS "alpha__id",',
          '"alpha"."val" AS "alpha__val",',
          '"beta"."alpha_id" AS "beta__alpha_id",',
          '"beta"."id" AS "beta__id","beta"."val" AS "beta__val" ',
          'FROM "alpha" ',
          'INNER JOIN "beta" ON ("beta"."alpha_id" = "alpha"."id") ',
          'WHERE "alpha"."id" = $1'
      ].join(''));
        assert.deepEqual(result.params, [3]);
      });
    });

    it('sorts by fields in a joined relation', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).find({
        'alpha.id >': 1
      }, {
        build: false,
        order: [{field: 'beta.val'}]
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3, val: 'three',
          beta: [{
            id: 3, alpha_id: 3, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, val: 'alpha three again'
          }]
        }, {
          id: 2, val: 'two',
          beta: [{
            id: 2, alpha_id: 2, val: 'alpha two'
          }]
        }]);
      });
    });

    it('changes the decomposition target type', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'},
          decomposeTo: 'object'
        }
      }).find({
        'alpha.id': 2
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2,
          val: 'two',
          beta: {
            id: 2, alpha_id: 2, val: 'alpha two'
          }
        }]);
      });
    });
  });

  describe('inserts', function () {
    it('inserts the origin of a table-only join', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).insert({
        val: 'new and improved'
      }).then(result => {
        assert.deepEqual(result, {
          id: 5,
          val: 'new and improved'
        });
      });
    });

    it('saves (insert version) the origin of a table-only join', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).save({
        val: 'newer and improveder'
      }).then(result => {
        assert.deepEqual(result, {
          id: 6,
          val: 'newer and improveder'
        });
      });
    });

    it('deep inserts', async function () {
      const join = await db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      });

      const result = await join.insert({
        val: 'newer and improveder',
        beta: [{alpha_id: undefined, val: 'asdf'}]
      });

      assert.deepEqual(result, {id: result.id, val: 'newer and improveder'});

      const inserted = await join.find(result.id);

      assert.deepEqual(inserted, [{
        id: result.id,
        val: 'newer and improveder',
        beta: [{
          id: 6,
          alpha_id: result.id,
          val: 'asdf'
        }]
      }]);
    });
  });

  describe('updates', function () {
    it('updates the origin of a table-only join', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).update({
        'beta.id': 3
      }, {
        val: 'something else'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3,
          val: 'something else'
        }]);
      });
    });

    it('saves (update version) the origin of a table-only join', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).save({
        id: 3,
        val: 'something else'
      }).then(result => {
        assert.deepEqual(result, {
          id: 3,
          val: 'something else'
        });
      });
    });

    it('updates the origin of a table-only join based on nested criteria', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'},
          gamma: {
            on: {beta_id: 'beta.id'}
          }
        }
      }).update({
        'gamma.id': 3
      }, {
        val: 'something else'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2,
          val: 'something else'
        }]);
      });
    });

    it('throws if an update-join includes multiple root-level keys', function () {
      assert.throws(() => db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        },
        gamma: {
          on: {alpha_1_id: 'id'}
        }
      }).update({
        'gamma.id': 3
      }, {
        val: 'something else'
      }), 'Joins involving more than one other relation joined to the origin are not updatable. Try reconfiguring your join schema with only one root-level join relation, or using SQL.');
    });
  });

  describe('deletes', function () {
    it('delete-joins', function () {
      return db.gamma.join({
        beta: {
          alpha: {}
        }
      }).destroy({
        'alpha.id': 3
      }).then(result => {
        assert.deepEqual(result, [{
          id: 4,
          alpha_id_one: 2,
          alpha_id_two: null,
          beta_id: 3,
          val: 'alpha two (alpha null) beta three'
        }, {
          id: 5,
          alpha_id_one: 3,
          alpha_id_two: 1,
          beta_id: 4,
          val: 'alpha three alpha one beta four'
        }]);
      });
    });

    it('runtime errors on out-of-bounds references to the FROM table after USING', function* () {
      let err;

      try {
        yield db.beta.join({
          alpha: {
            on: {id: 'alpha_id'},
            gamma: {
              // beta can only be referenced in the WHERE clause
              on: {beta_id: 'beta.id'}
            }
          }
        }).destroy({
          'gamma.id': 1
        });
      } catch (e) {
        err = e;
      } finally {
        assert.equal(err.code, '42P01');
      }
    });
  });

  describe('useless methods', function () {
    it('errors on findOne', function () {
      return db.alpha.join('beta')
        .findOne({})
        .then(() => assert.fail())
        .catch(err => assert.equal(err.message, 'findOne is not supported with compound Readables.'));
    });
  });
});
