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
          id: 3, alpha_id: 3, j: null, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          id: 3, alpha_id: 3, j: null, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
        id: 3, alpha_id: 3, val: 'alpha three', j: null,
        alpha: [{id: 3, val: 'three'}]
      }, {
        id: 4, alpha_id: 3, val: 'alpha three again', j: null,
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
          id: 1, alpha_id: 1, val: 'alpha one', j: null,
          gamma: [{
            id: 1, beta_id: 1, alpha_id_one: 1, alpha_id_two: 1, j: null,
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
        j: null,
        val: 'alpha one',
        epsilon: [{
          id: 1,
          alpha_id: 1,
          val: 'alpha one'
        }]
      }]);
    });
  });

  describe('constants in join conditions', function () {
    it('joins on constants', function () {
      return db.beta.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            val: 'alpha one'
          }
        }
      }).find({
        val: 'alpha three again'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 4,
          alpha_id: 3,
          j: null,
          val: 'alpha three again',
          epsilon: [{
            id: 1,
            alpha_id: 1,
            val: 'alpha one'
          }]
        }]);
      });
    });

    it('joins on multiple constants', function () {
      return db.beta.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            id: 1,
            val: 'alpha one'
          }
        }
      }).find({
        val: 'alpha three again'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 4,
          alpha_id: 3,
          j: null,
          val: 'alpha three again',
          epsilon: [{
            id: 1,
            alpha_id: 1,
            val: 'alpha one'
          }]
        }]);
      });
    });

    it('joins on constants for multiple relations', function () {
      return db.beta.join({
        alpha: {
          type: 'INNER',
          on: {
            val: 'one'
          }
        },
        'sch.epsilon': {
          type: 'INNER',
          on: {
            id: 1,
            val: 'alpha one'
          }
        }
      }).find({
        val: 'alpha three again'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 4,
          alpha_id: 3,
          j: null,
          val: 'alpha three again',
          alpha: [{
            id: 1,
            val: 'one'
          }],
          epsilon: [{
            id: 1,
            alpha_id: 1,
            val: 'alpha one'
          }]
        }]);
      });
    });

    it('mixes keys and constants', function () {
      return db.alpha.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            alpha_id: 'id',
            val: 'alpha one'
          }
        }
      }).find({}).then(result => {
        assert.deepEqual(result, [{
          id: 1,
          val: 'one',
          epsilon: [{
            id: 1,
            alpha_id: 1,
            val: 'alpha one'
          }]
        }]);
      });
    });

    it('mixes dot-pathed keys and constants', function () {
      return db.alpha.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            alpha_id: 'alpha.id',
            val: 'alpha one'
          }
        }
      }).find({}).then(result => {
        assert.deepEqual(result, [{
          id: 1,
          val: 'one',
          epsilon: [{
            id: 1,
            alpha_id: 1,
            val: 'alpha one'
          }]
        }]);
      });
    });

    it('handles simple operations', function () {
      return db.alpha.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            'alpha_id is': null
          }
        }
      }).find({val: 'one'}).then(result => {
        assert.deepEqual(result, [{
          id: 1,
          val: 'one',
          epsilon: [{
            id: 2,
            alpha_id: null,
            val: 'not two'
          }]
        }]);
      });
    });

    it('handles operations with mutators', function () {
      return db.alpha.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            'alpha_id': [1, 2]
          }
        }
      }).find({val: 'one'}).then(result => {
        assert.deepEqual(result, [{
          id: 1,
          val: 'one',
          epsilon: [{
            id: 1,
            alpha_id: 1,
            val: 'alpha one'
          }]
        }]);
      });
    });

    it('catches constants that start with valid keys', async function () {
      await db.sch.epsilon.insert({
        alpha_id: 3,
        val: 'alpha.id but literally the text alpha.id'
      });

      const result = await db.alpha.join({
        'sch.epsilon': {
          type: 'INNER',
          on: {
            alpha_id: 'alpha.id',
            val: 'alpha.id but literally the text alpha.id'
          }
        }
      }).find({});

      await db.sch.epsilon.destroy({
        val: 'alpha.id but literally the text alpha.id'
      });

      assert.deepEqual(result, [{
        id: 3,
        val: 'three',
        epsilon: [{
          alpha_id: 3,
          id: 3,
          val: 'alpha.id but literally the text alpha.id'
        }]
      }]);
    });

    it('does json', function () {
      return db.beta.join({
        gamma: {
          type: 'INNER',
          on: {
            'j.z.a': 'j.x.y'
          }
        }
      }).find({val: 'not five'}).then(result => {
        assert.deepEqual(result, [{
          id: 6,
          alpha_id: null,
          val: 'not five',
          j: {x: {y: 'test'}},
          gamma: [{
            id: 6, alpha_id_one: 4, alpha_id_two: null, beta_id: 5,
            val: 'beta five', j: {z: {a: 'test'}}
          }]
        }]);
      });
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
        id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, j: null, val: 'alpha two'}]
      }, {
        id: 3, val: 'three', beta: [{
          id: 3, alpha_id: 3, j: null, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          id: 2, alpha_id: 2, j: null, val: 'alpha two'
        }]
      }, {
        id: 3, val: 'three',
        beta: [{
          id: 3, alpha_id: 3, j: null, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          id: 2, alpha_id: 2, j: null, val: 'alpha two',
          gamma: [{
            id: 2, beta_id: 2, alpha_id_one: 1, alpha_id_two: 2, j: null,
            val: 'alpha two alpha two beta two'
          }, {
            id: 3, beta_id: 2, alpha_id_one: 2, alpha_id_two: 3, j: null,
            val: 'alpha two alpha three beta two again'
          }]
        }]
      }, {
        id: 3, val: 'three',
        beta: [{
          id: 3, alpha_id: 3, j: null, val: 'alpha three',
          gamma: [{
            id: 4, beta_id: 3, alpha_id_one: 2, alpha_id_two: null, j: null,
            val: 'alpha two (alpha null) beta three'
          }]
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again',
          gamma: [{
            id: 5, beta_id: 4, alpha_id_one: 3, alpha_id_two: 1, j: null,
            val: 'alpha three alpha one beta four'
          }]
        }]
      }]);
    });
  });

  it('omits a relation from the final result', function () {
    return db.alpha.join({
      alpha_zeta: {
        type: 'LEFT OUTER',
        pk: ['alpha_id', 'zeta_id'],
        on: {alpha_id: 'id'},
        omit: true
      },
      zeta: {
        type: 'LEFT OUTER',
        on: {id: 'alpha_zeta.zeta_id'}
      }
    }).find({
      'alpha.id': [1, 3]
    }).then(result => {
      assert.deepEqual(result, [{
        id: 1,
        val: 'one',
        zeta: [{
          id: 1, val: 'alpha one'
        }, {
          id: 2, val: 'alpha one again'
        }]
      }, {
        id: 3,
        val: 'three',
        zeta: []
      }]);
    });
  });

  it('omits a parent relation from the final result', function () {
    return db.alpha.join({
      alpha_zeta: {
        type: 'LEFT OUTER',
        pk: ['alpha_id', 'zeta_id'],
        on: {alpha_id: 'id'},
        omit: true,
        zeta: {
          type: 'LEFT OUTER',
          on: {id: 'alpha_zeta.zeta_id'}
        }
      }
    }).find({
      'alpha.id': [1, 3]
    }).then(result => {
      assert.deepEqual(result, [{
        id: 1,
        val: 'one',
        zeta: [{
          id: 1, val: 'alpha one'
        }, {
          id: 2, val: 'alpha one again'
        }]
      }, {
        id: 3,
        val: 'three',
        zeta: []
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

  it('manages the options param index for where', function () {
    return db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).where('alpha.id < $1', [4], {offset: 2}).then(result => {
      assert.deepEqual(result, [{
        id: 3,
        val: 'three',
        beta: [{
          id: 3, alpha_id: 3, j: null, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again'
        }]
      }]);
    });
  });

  it('allows overriding the decomposition schema', function () {
    return db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).find({
      'alpha.id': 3
    }, {
      // strip the 'val' fields
      decompose: {
        pk: 'alpha__id',
        columns: {alpha__id: 'id'},
        beta: {
          pk: 'beta__id',
          columns: {beta__id: 'id', beta__alpha_id: 'alpha_id'}
        }
      }
    }).then(result => {
      assert.deepEqual(result, [{
        id: 3,
        beta: [{
          id: 3, alpha_id: 3
        }, {
          id: 4, alpha_id: 3
        }]
      }]);
    });
  });

  it('works in tasks or transactions', function () {
    return db.withConnection(task => {
      return task.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).find({
        'alpha.id': 3
      });
    }).then(result => {
      assert.deepEqual(result, [{
        id: 3,
        val: 'three',
        beta: [{
          id: 3, alpha_id: 3, j: null, val: 'alpha three'
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again'
        }]
      }]);
    });
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

  it('errors if a primary key is missing', function () {
    assert.throws(() => db.alpha.join({
      alpha_zeta: {
        type: 'LEFT OUTER',
        on: {alpha_id: 'id'}
      },
      zeta: {
        type: 'LEFT OUTER',
        on: {id: 'alpha_zeta.zeta_id'}
      }
    }), 'Missing explicit pk in join definition for alpha_zeta.');
  });

  it('restricts the resultset with aliased exprs', function () {
    return db.alpha.join({
      beta: {
        type: 'INNER',
        on: {alpha_id: 'id'}
      }
    }).find({'alpha.id': 3}, {
      exprs: {
        'alpha__id': 'alpha.id',
        'beta__id': 'beta.id'
      }
    }).then(result => {
      assert.deepEqual(result, [{
        id: 3,
        beta: [{id: 3}, {id: 4}]
      }]);
    });
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
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          j: null,
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
          id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, j: null, val: 'alpha two'}]
        }, {
          id: 3, val: 'three', beta: [{
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, j: null, val: 'alpha two'}]
        }, {
          id: 3, val: 'three', beta: [{
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
          }]
        }]);
      });
    });

    it('does a basic inner join with just a string', function () {
      return db.alpha.join('beta').find({
        'alpha.id >': 1
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2, val: 'two', beta: [{id: 2, alpha_id: 2, j: null, val: 'alpha two'}]
        }, {
          id: 3, val: 'three', beta: [{
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          '"beta"."id" AS "beta__id","beta"."j" AS "beta__j","beta"."val" AS "beta__val" ',
          'FROM "alpha" ',
          'INNER JOIN "beta" ON "beta"."alpha_id" = "alpha"."id" ',
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
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
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
          id: 3, alpha_id: 3, j: null, val: 'alpha three',
          alpha: [{id: 3, val: 'three'}]
        }, {
          id: 4, alpha_id: 3, j: null, val: 'alpha three again',
          alpha: [{id: 3, val: 'three'}]
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
            id: 2, alpha_id: 2, j: null, val: 'alpha two',
            gamma: [{
              id: 2, beta_id: 2, alpha_id_one: 1, alpha_id_two: 2, j: null,
              val: 'alpha two alpha two beta two'
            }, {
              id: 3, beta_id: 2, alpha_id_one: 2, alpha_id_two: 3, j: null,
              val: 'alpha two alpha three beta two again'
            }]
          }]
        }, {
          id: 3, val: 'three',
          beta: [{
            id: 3, alpha_id: 3, j: null, val: 'alpha three',
            gamma: [{
              id: 4, beta_id: 3, alpha_id_one: 2, alpha_id_two: null, j: null,
              val: 'alpha two (alpha null) beta three'
            }]
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again',
            gamma: [{
              id: 5, beta_id: 4, alpha_id_one: 3, alpha_id_two: 1, j: null,
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
          '"beta"."id" AS "beta__id","beta"."j" AS "beta__j","beta"."val" AS "beta__val" ',
          'FROM "alpha" ',
          'INNER JOIN "beta" ON "beta"."alpha_id" = "alpha"."id" ',
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
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
          }]
        }, {
          id: 2, val: 'two',
          beta: [{
            id: 2, alpha_id: 2, j: null, val: 'alpha two'
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
            id: 2, alpha_id: 2, j: null, val: 'alpha two'
          }
        }]);
      });
    });
  });

  describe('searches', function () {
    it('joins a relation with a type and keys', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      }).search({
        fields: ['alpha.val', 'beta.val'],
        term: 'three'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 3,
          val: 'three',
          beta: [{
            id: 3, alpha_id: 3, j: null, val: 'alpha three'
          }, {
            id: 4, alpha_id: 3, j: null, val: 'alpha three again'
          }]
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
          id: 7,
          alpha_id: result.id,
          j: null,
          val: 'asdf'
        }]
      }]);
    });

    it('deep inserts with save', async function () {
      const join = await db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        }
      });

      const result = await join.save({
        val: 'newer and improveder',
        beta: [{alpha_id: undefined, val: 'asdf'}]
      });

      assert.deepEqual(result, {id: result.id, val: 'newer and improveder'});

      const inserted = await join.find(result.id);

      assert.deepEqual(inserted, [{
        id: result.id,
        val: 'newer and improveder',
        beta: [{
          id: 8,
          alpha_id: result.id,
          j: null,
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

    it.skip('updates a compound readable with constant criteria', function () {
      // TODO offset needs to be stored between join and where for this to work
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {val: 'alpha one'}
        }
      }).update({
        'beta.id': 1
      }, {
        val: 'something else'
      }).then(result => {
        assert.deepEqual(result, ['TODO']);
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

    it('updates the origin of a table-only join with flat criteria and only one origin reference', function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        },
        gamma: {
          on: {beta_id: 'beta.id'}
        }
      }).update({
        'gamma.id': 3
      }, {
        val: 'something further'
      }).then(result => {
        assert.deepEqual(result, [{
          id: 2,
          val: 'something further'
        }]);
      });
    });

    it('throws if an update-join has multiple origin references', async function () {
      return db.alpha.join({
        beta: {
          type: 'INNER',
          on: {alpha_id: 'id'}
        },
        gamma: {
          on: {beta_id: 'alpha.id'}
        }
      }).update({
        'gamma.id': 3
      }, {
        val: 'something else'
      }).then(() => { assert.fail(); })
        .catch(err => { assert.equal(err.code, '42P01'); });
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
          j: null,
          val: 'alpha two (alpha null) beta three'
        }, {
          id: 5,
          alpha_id_one: 3,
          alpha_id_two: 1,
          beta_id: 4,
          j: null,
          val: 'alpha three alpha one beta four'
        }]);
      });
    });

    it('runtime errors on out-of-bounds references to the FROM table after USING', async () => {
      let err;

      try {
        await db.beta.join({
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
