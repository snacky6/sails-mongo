var Adapter = require('../../lib/adapter'),
    Config = require('../support/config'),
    Fixture = require('../support/fixture'),
    assert = require('assert'),
    async = require('async');

describe('adapter', function() {

  before(function(done) {
    var Schema;

    // Register The Collection
    Adapter.registerCollection({
      identity: 'test',
      config: Config,
      definition: Fixture,
      meta: {
        junctionTable: false
      }
    }, done);
  });


  describe('.native()', function() {

    it('should allow direct access to the collection object', function(done) {

      Adapter.native('test', function(err, collection) {
        assert(!err);

        // Attempt to insert a document
        collection.insert({hello: 'world'}, {w:1}, function(err, objects) {
          assert(!err);

          // check that the record was actually inserted
          collection.findOne({ hello: 'world' }, function(err, doc) {
            assert(!err);
            assert(doc.hello === 'world');

            done(err);
          });
        });
      });
    });
  });

});