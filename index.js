var inherits          = require('inherits');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var AbstractIterator  = require('abstract-leveldown').AbstractIterator;
var ltgt              = require('ltgt');
function noop(){}
var setImmediate      = require('immediate');
var createRBT = require("functional-red-black-tree");
function t() {return true;}
module.exports = RedBlackDown;
inherits(RedBlackDown, AbstractLevelDOWN)
function RedBlackDown (location) {
  if (!(this instanceof RedBlackDown)) {
    return new RedBlackDown(location);
   }

  AbstractLevelDOWN.call(this, typeof location == 'string' ? location : '');
  this.tree = createRBT();
}



RedBlackDown.prototype._open = function (options, callback) {
  var self = this;
  setImmediate(function () { callback(null, self) });
}

RedBlackDown.prototype._put = function (key, value, options, callback) {
  this.tree = this.tree.remove(key).insert(key, value);
  setImmediate(callback);
}

RedBlackDown.prototype._get = function (key, options, callback) {
  var value = this.tree.get(key);
  if (value === void 0) {
    // 'NotFound' error, consistent with LevelDOWN API
    return setImmediate(function () { callback(new Error('NotFound')) })
  }
  if (options.asBuffer !== false && !Buffer.isBuffer(value))
    value = new Buffer(String(value))
  setImmediate(function () {
    callback(null, value)
  })
}
RedBlackDown.prototype._del = function (key, options, callback) {
  this.tree = this.tree.remove(key);
  setImmediate(callback)
}

RedBlackDown.prototype._batch = function (array, options, callback) {
  var err
    , i = -1
    , key
    , value
    , len = array.length

  while (++i < len) {
    if (array[i]) {
      key = Buffer.isBuffer(array[i].key) ? array[i].key : String(array[i].key)
      err = this._checkKey(key, 'key')
      if (err) return setImmediate(function () { callback(err) })
      if (array[i].type === 'del') {
        this._del(array[i].key, options, noop)
      } else if (array[i].type === 'put') {
        value = Buffer.isBuffer(array[i].value) ? array[i].value : String(array[i].value)
        err = this._checkKey(value, 'value')
        if (err) return setImmediate(function () { callback(err) })
        this._put(key, value, options, noop)
      }
    }
  }
  
  setImmediate(callback)
}
RedBlackDown.prototype._isBuffer = function (obj) {
  return Buffer.isBuffer(obj)
}

RedBlackDown.prototype._iterator = function (options) {
  return new RedBlackIterator(this, options)
}


function RedBlackIterator (db, options) {
  AbstractIterator.call(this, db)
  var self = this;
  this._limit   = options.limit
  if (this._limit === -1) {
    this._limit = Infinity;
  }
  this.keyAsBuffer = options.keyAsBuffer !== false
  this.valueAsBuffer = options.valueAsBuffer !== false
  this._reverse   = options.reverse
  this._options = options
  this._done = 0;
  if (!this._reverse) {
    this._incr = 'next';
    this._check = 'hasNext';
    this._start = ltgt.lowerBound(options);
    this._end = ltgt.upperBound(options);
    this._incStart = ltgt.lowerBoundInclusive(options);
    this._incEnd = ltgt.upperBoundInclusive(options);
    if (typeof this._start === 'undefined') {
      this._tree = db.tree.begin;
    } if (this._incStart) {
      this._tree = db.tree.ge(this._start);
    } else {
      this._tree = db.tree.gt(this._start);
    }
    if (!this._end) {
      this._test = t;
    } else if (this._incEnd) {
      this._test = function (value) {
        return value <= self._end;
      }
    } else {
      this._test = function (value) {
        return value < self._end;
      }
    }
  } else {
    this._incr = 'prev';
    this._check = 'hasPrev';
    this._start = ltgt.upperBound(options);
    this._end = ltgt.lowerBound(options);
    this._incStart = ltgt.upperBoundInclusive(options);
    this._incEnd = ltgt.lowerBoundInclusive(options);
    if (typeof this._start === 'undefined') {
      this._tree = db.tree.end;
    } if (this._incStart) {
      this._tree = db.tree.le(this._start);
    } else {
      this._tree = db.tree.lt(this._start);
    }
    if (!this._end) {
      this._test = t;
    } else if (this._incEnd) {
      this._test = function (value) {
        return value >= self._end;
      }
    } else {
      this._test = function (value) {
        return value > self._end;
      }
    }
  }
}

inherits(RedBlackIterator, AbstractIterator)

RedBlackIterator.prototype._next = function (callback) {
  var self  = this;

  if (self._done++ >= this._limit) {
    return setImmediate(callback);
  }
  if (!this._tree.valid) {
    return setImmediate(callback);
  }

  var key = self._tree.key;
  var value = self._tree.value;
  if (!this._test(key)) {
    return setImmediate(callback);
  }
  if (self.keyAsBuffer) {
    key = new Buffer(key)
  }

  if (self.valueAsBuffer){
    value = new Buffer(value)
  }
  this._tree[this._incr]();
  setImmediate(function () { callback(null, key, value) })
}