'use strict';

/* global describe, beforeEach, afterEach, it */

var expect = require('chai').expect,
    stream = require('stream'),
    redis = require('redis'),
    fs = require('fs'),
    path = require('path'),
    createWriteStream = require('../index.js').createWriteStream,
    createReadStream = require('../index.js').createReadStream,
    es = require('event-stream'),
    KEY = 'redis-append-stream-file';

describe('redis-append-stream-file', function() {
  var client,
      size = 64 * 1024,
      buf = new Buffer(size);

  for (var i = 0; i < size; ++i) {
    buf.writeUInt8(i % 256, i);
  }

  beforeEach(function() {
    // you need 'detect_buffers' if you treat binay file
    client = redis.createClient(null, null, {detect_buffers: true});
    client.del(KEY);
  });

  afterEach(function() {
    client.end();
  });

  it('send "append" command to redis', function(done) {
    // write with WriteStream
    var generator = es.readable(function(count, callback) {
          if (count < 10) {
            this.emit('data', buf);
          } else {
            this.emit('end');
          }
          callback();
        }),
        writeStream = createWriteStream({client: client, key: KEY});

    generator.pipe(writeStream);

    // wait writing
    writeStream.on('end', function() {
      // read with ReadStream
      var cnt = 0,
          readStream = createReadStream({client: client, key: KEY});

      readStream.on('data', function(data) {
        for (var i = 0; i < data.length; ++i) {
          expect(data.readUInt8(i)).to.equal((i + cnt) % 256, 'error at:' + (i + cnt));
        }
        cnt += data.length;
      }).on('error', function(error) {
        throw error;
      }).on('end', done);
    });
  });
});

// vim: ts=2:sw=2:sts=2:expandtab:
