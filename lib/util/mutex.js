'use strict';

const EventEmitter = require('events').EventEmitter;

const Mutex = function ($p) {
  this.$p = $p;
  this.locked = false;
  this.emitter = new EventEmitter();
};

Mutex.prototype.acquire = function () {
  return this.$p(resolve => {
    if (!this.locked) {
      this.locked = true;

      return resolve();
    }

    const attempt = () => {
      if (!this.locked) {
        this.locked = true;
        this.emitter.removeListener('release', attempt);

        return resolve();
      }

      return null;
    };

    this.emitter.on('release', attempt);

    return null;
  });
};

Mutex.prototype.release = function () {
  this.locked = false;
  setImmediate(() => this.emitter.emit('release'));
};

exports = module.exports = Mutex;
