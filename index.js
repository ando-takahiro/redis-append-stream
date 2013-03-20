'use strict';

var stream = require('stream'),
    Readable = stream.Readable,
    Writable = stream.Writable,
    util = require('util'),
    DEFAULT_READ_LENGTH = 64 * 1024; // no evidence for this value

exports.createWriteStream = function(options) {
  return new Append(options);
};

exports.createReadStream = function(options) {
  // this is from the document of node 0.10

  var stream = new Readable(options),
      key = new Buffer(options.key),
      client = options.client,
      maxReadLength = options.maxReadLength || DEFAULT_READ_LENGTH,
      busy = false,
      current = 0;

  function fetch(n) {
    if (busy) {
      return;
    }

    n = Math.min(n || maxReadLength, maxReadLength);
    busy = true;
    client.getrange(key, current, current + n - 1, function(err, data) {
      busy = false;
      if (err) {
        stream.emit('error', err);
      } else {
        if (data.length === 0) {
          // finished
          stream.push(null);
        } else {
          current += data.length;
          if (stream.push(data)) {
            // if push() returns false, then we need to stop reading from source
            fetch();
          }
        }
      }
    });
  }

  // _read will be called when the stream wants to pull more data in
  // the advisory size argument is ignored in this case.
  stream._read = function(n) {
    fetch(n);
  };

  return stream;
};

function Append(options) {
  this._client = options.client;
  this._key = options.key;
  Writable.call(this, options);
}

util.inherits(Append, Writable);

Append.prototype._write = function(chunk, encoding, cb) {
  this._client.append(this._key, chunk, cb);
};

// vim: ts=2:sw=2:sts=2:expandtab:
