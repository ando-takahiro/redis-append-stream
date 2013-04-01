'use strict';

var stream = require('stream'),
    Writable = stream.Writable,
    es = require('event-stream'),
    util = require('util'),
    DEFAULT_READ_LENGTH = 64 * 1024; // no evidence for this value

exports.createWriteStream = function(options) {
  return new Append(options);
};

exports.createReadStream = function(options) {
  // this is from the document of node 0.10

  var key = new Buffer(options.key),
      client = options.client,
      maxReadLength = options.maxReadLength || DEFAULT_READ_LENGTH,
      current = 0;

  return es.readable(function(count, callback) {
    var that = this;
    client.getrange(key, current, current + maxReadLength - 1, function(err, data) {
      if (err) {
        that.emit('error', err);
      } else {
        if (data.length === 0) {
          // finished
          that.emit('end');
        } else {
          current += data.length;
          that.emit('data', data);
        }
      }
      callback();
    });
  });
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
