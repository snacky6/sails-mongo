
/**
 * Module dependencies
 */

var async = require('async'),
    MongoClient = require('mongodb').MongoClient;

/**
 * Manage a connection to a Mongo Server
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = module.exports = function Connection(config, cb) {
  var self = this;

  // Hold the config object
  this.config = config || {};

  // Build Database connection
  this._buildConnection(function(err, db) {
    if(err) return cb(err);
    if(!db) return cb(new Error('no db object'));

    // Store the DB object
    self.db = db;

    // Return the connection
    cb(null, self);
  });
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Create A Collection
 *
 * @param {String} name
 * @param {Object} collection
 * @param {Function} callback
 * @api public
 */

Connection.prototype.createCollection = function createCollection(name, collection, cb) {
  var self = this;

  // Create the Collection
  this.db.createCollection(name, function(err, result) {
    if(err) return cb(err);

    // Create Indexes
    self._ensureIndexes(result, collection.indexes, cb);
  });
};

/**
 * Drop A Collection
 *
 * @param {String} name
 * @param {Function} callback
 * @api public
 */

Connection.prototype.dropCollection = function dropCollection(name, cb) {
  this.db.dropCollection(name, cb);
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Build Server and Database Connection Objects
 *
 * @param {Function} callback
 * @api private
 */

Connection.prototype._buildConnection = function _buildConnection(cb) {

  // Set the configured options
  var connectionOptions = {
    appname: this.config.appname,
    validateOptions: this.config.validateOptions,
    checkServerIdentity: this.config.checkServerIdentity,
    readPreference: this.config.readPreference,
    pkFactory: this.config.pkFactory,
    promiseLibrary: this.config.promiseLibrary,
    readConcern: this.config.readConcern,
    maxStalenessSeconds: this.config.maxStalenessSeconds,
    loggerLevel: this.config.loggerLevel,
    logger: this.config.logger,
    promoteValues: this.config.promoteValues,
    promoteBuffers: this.config.promoteBuffers,
    promoteLongs: this.config.promoteLongs,
    domainsEnabled: this.config.domainsEnabled,
    ssl: this.config.ssl,
    sslValidate: this.config.sslValidate,
    sslCA: this.config.sslCA,
    sslCert: this.config.sslCert,
    ciphers: this.config.ciphers,
    ecdhCurve: this.config.ecdhCurve,
    sslKey: this.config.sslKey,
    sslPass: this.config.sslPass,
    sslCRL: this.config.sslCRL,
    poolSize: this.config.poolSize,
    autoReconnect: this.config.autoReconnect || this.config.auto_reconnect,
    reconnectInterval: this.config.reconnectInterval,
    reconnectTries: this.config.reconnectTries,
    j: this.config.j,
    w: this.config.w,
    wtimeout: this.config.wtimeout,
    native_parser: this.config.nativeParser,
    forceServerObjectId: this.config.forceServerObjectId,
    serializeFunctions: this.config.serializeFunctions,
    ignoreUndefined: this.config.ignoreUndefined,
    raw: this.config.raw,
    bufferMaxEntries: this.config.bufferMaxEntries,
    noDelay: this.config.noDelay,
    keepAlive: this.config.keepAlive,
    keepAliveInitialDelay: this.config.keepAliveInitialDelay,
    connectTimeoutMS: this.config.connectTimeoutMS,
    socketTimeoutMS: this.config.socketTimeoutMS,
    family: this.config.family,
    ha: this.config.ha,
    haInterval: this.config.haInterval,
    replicaSet: this.config.replicaSet,
    secondaryAcceptableLatencyMS: this.config.secondaryAcceptableLatencyMS,
    acceptableLatencyMS: this.config.acceptableLatencyMS,
    connectWithNoPrimary: this.config.connectWithNoPrimary,
    authSource: this.config.authSource
  };

  // Build A Mongo Connection String
  // Use config connection string if available

  var connectionString = '';

  if (this.config.url) {
    const url = require('url');
    const decodedUrl = url.parse(this.config.url, true);
    const queryParams = (decodedUrl || {}).query || {};

    if (Object.keys(queryParams).length !== 0) {
      // Check if SSL is enable in connection URL (Atlas support)
      if (queryParams.ssl) {
        connectionOptions.ssl = true;
      }

      // Check if authSource option is in connection URL (Atlas support)
      if (queryParams.authSource) {
        connectionOptions.authSource = queryParams.authSource;
      }

      // Check if replicaSet option is in connection URL (Atlas support)
      if (queryParams.replicaSet) {
        connectionOptions.replicaSet = queryParams.replicaSet;
      }
    }

    connectionString = this.config.url;
  } else {
    connectionString = 'mongodb://';
    // If auth is used, append it to the connection string
    if (this.config.user && this.config.password) {

      // Ensure a database was set if auth in enabled
      if (!this.config.database) {
        throw new Error('The MongoDB Adapter requires a database config option if authentication is used.');
      }

      connectionString += this.config.user + ':' + this.config.password + '@';
    }

    // Append the host and port
    connectionString += this.config.host + ':' + this.config.port + '/';

    if (this.config.database) {
      connectionString += this.config.database;
    }
  }

  // Open a Connection
  MongoClient.connect(connectionString, connectionOptions, cb);
};

/**
 * Ensure Indexes
 *
 * @param {String} collection
 * @param {Array} indexes
 * @param {Function} callback
 * @api private
 */

Connection.prototype._ensureIndexes = function _ensureIndexes(collection, indexes, cb) {
  var self = this;

  function createIndex(item, next) {
    collection.ensureIndex(item.index, item.options, next);
  }

  async.each(indexes, createIndex, cb);
};
