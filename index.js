'use strict';

var es = require('event-stream'),
    util = require('util'),
    DEFAULT_READ_LENGTH = 64 * 1024; // no evidence for this value

exports.createWriteStream = function(options) {
  var client = options.client;
  return es.through(client.append.bind(client, options.key));
};

exports.createReadStream = function(options) {
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

// vim: ts=2:sw=2:sts=2:expandtab:
