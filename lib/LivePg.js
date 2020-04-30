// pg-live-select, MIT License
var fs = require('fs');
var path = require('path');

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var _ = require('lodash');
// var pg = require('pg');
var { Client } = require('pg');
var murmurHash   = require('murmurhash-js').murmur3


var querySequence = require('./querySequence');
var SelectHandle = require('./SelectHandle');
var LivePgKeySelector = require('./LivePgKeySelector.js');
var LivePgSelect = require('./LivePgSelect.js');
var QueryCache = require('./QueryCache');
var differ = require('./differ');
var EJSON = require('ejson');

/*
 * Duration (ms) to wait to check for new updates when no updates are
 *  available in current frame
 */
var STAGNANT_TIMEOUT = 100;

var TRIGGER_QUERY_TPL = loadQueryFromFile('./trigger.tpl.sql');
var REFRESH_QUERY_TPL = loadQueryFromFile('./refresh.tpl.sql');

function LivePg(settings, channel) {
  var self = this;
  EventEmitter.call(self);

  self.settings = settings;
  self.channel = channel;
  self.triggerFun = 'livepg_' + channel;
  self.notifyClient = null;
  self.notifyDone = null;
  self.waitingPayloads = {};
  self.waitingToUpdate = [];
  self.selectBuffer    = {};
  self.allTablesUsed   = {};
  self.tablesUsedCache = {};
  self._select = [];
  self._queryCache = {};
  self._schemaCache = {};

  

  self.db = new Client(settings)
  self.query = self.db.query.bind(self.db);
  self.endDbOrPool = function() {
		self.db.end();
	};
  initialConnect = self.db.connect.bind(self.db);
  initialConnect(function(error, client, done) {
    if (error) return self.emit('error', error);
    self.notifyDone = done;
    self.notifyClient = client;
    _.each(self._queryCache, function(cache) {
        cache.invalidate();
    });
    self._initTriggerFun();
    self._initListener();
    self._initUpdateLoop();
  })

  

}

util.inherits(LivePg, EventEmitter);

LivePg.prototype.select = function(query, values, keySelector, triggers, minInterval) {
  var self = this;

  if (!(typeof query === 'string'))
    throw new Error('query must be a string');
  if (!(typeof values === 'object' || values === undefined))
    throw new Error('values must be an object, null, or undefined');
  if (!(keySelector instanceof Function))
    throw new Error('keySelector required');
  if (!(triggers instanceof Array) || triggers.length === 0)
    throw new Error('triggers array required');
  if (!(typeof minInterval === 'number' || minInterval === undefined))
    throw new Error('minInterval must be a number or undefined');
  if(typeof query !== 'string')
    throw new Error('QUERY_STRING_MISSING');
  if(!(values instanceof Array))
    throw new Error('VALUES_ARRAY_MISMATCH');

  // var queryHash = murmurHash(JSON.stringify([ query, params ]));

  // var includeSchema = self._schemaCache;
  // for (var i = 0; i < triggers.length; i++) {
  //   var triggerDatabase = triggers[i].database || self.settings.database;
  //   if (triggerDatabase === undefined) {
  //     throw new Error('no database selected on trigger');
  //   }
  //   if (!(triggerDatabase in includeSchema)) {
  //     includeSchema[triggerDatabase] = [ triggers[i].table ];
  //   } else if (includeSchema[triggerDatabase].indexOf(triggers[i].table) === -1) {
  //     includeSchema[triggerDatabase].push(triggers[i].table);
  //   }
  // }
  
  var queryCacheKey = EJSON.stringify({
    query: query,
    values: values,
    keySelector: LivePgKeySelector.makeTag(keySelector)
  }, {canonical: true});

  var queryCache;
  if (queryCacheKey in self._queryCache) {
    queryCache = self._queryCache[queryCacheKey];
  } else {
    queryCache = new QueryCache(query, values, queryCacheKey, keySelector, minInterval, self);
    self._queryCache[queryCacheKey] = queryCache;
  }


  // var handle = new SelectHandle(self, queryHash);
  var newSelect = new LivePgSelect(queryCache, queryCacheKey, triggers, self);
  self._select.push(newSelect);
  return newSelect;

  // Perform initialization asynchronously
  // self._initSelect(query, params, triggers, queryHash, handle);

  // return handle;
}

LivePg.prototype.cleanup = function(callback) {
  var self = this;
  self.notifyDone && self.notifyDone();

  var queries = Object.keys(self.allTablesUsed).map(function(table) {
    return 'DROP TRIGGER IF EXISTS "' +
      self.channel + '_' + table + '" ON "' + table + '"';
  });

  queries.push('DROP FUNCTION "' + self.triggerFun + '"() CASCADE');
  queries.forEach((q)=>{
    self.query(q);
  })

  // querySequence(self.connStr, queries, callback);
}

LivePg.prototype._initTriggerFun = function() {
  var self = this;
  self.query(
    replaceQueryArgs(TRIGGER_QUERY_TPL,
      { funName: self.triggerFun, channel: self.channel })
  , function(error) {
    if(error) return self.emit('error', error);
  });
}

LivePg.prototype._initListener = function() {
  var self = this;

    self.notifyClient.query('LISTEN "' + self.channel + '"', function(error, result) {
      if(error) return self.emit('error', error);
    });

  self.notifyClient.on('notification', function(info) {
    if(info.channel === self.channel) {
      var payload = self._processNotification(info.payload);

      // Only continue if full notification has arrived
      if(payload === null) return;

      try {
        var payload = JSON.parse(payload);
      } catch(error) {
        return self.emit('error',
          new Error('INVALID_NOTIFICATION ' + payload));
      }

      if(payload.table in self.allTablesUsed) {

        _.each(self._queryCache, function(cache) {
          if ((self.settings.checkConditionWhenQueued
              || cache.updateTimeout === null)
              && cache.matchRowEvent(payload)) {
            cache.invalidate();
          }
        });
        // self.allTablesUsed[payload.table].forEach(function(queryCacheKey) {
        //   var queryBuffer = self.selectBuffer[queryCacheKey];
        //   if((queryBuffer.triggers
        //       // Check for true response from manual trigger
        //       && payload.table in queryBuffer.triggers
        //       && (payload.op === 'UPDATE'
        //         // Rows changed in an UPDATE operation must check old and new
        //         ? queryBuffer.triggers[payload.table](payload.new_data[0])
        //           || queryBuffer.triggers[payload.table](payload.old_data[0])
        //         // Rows changed in INSERT/DELETE operations only check once
        //         : queryBuffer.triggers[payload.table](payload.data[0])))
        //     || (queryBuffer.triggers
        //       // No manual trigger for this table, always refresh
        //       && !(payload.table in  queryBuffer.triggers))
        //     // No manual triggers at all, always refresh
        //     || !queryBuffer.triggers) {

        //     self.waitingToUpdate.push(queryCacheKey);
        //   }
        // });
      }
    }
  })
}

LivePg.prototype._initUpdateLoop = function() {
  var self = this;

  var performNextUpdate = function() {
    if(self.waitingToUpdate.length !== 0) {
      var queriesToUpdate =
        _.uniq(self.waitingToUpdate.splice(0, self.waitingToUpdate.length));
      var updateReturned = 0;

      queriesToUpdate.forEach(function(queryHash) {
        self._updateQuery(queryHash, function(error) {
          updateReturned++;
          if(error) self.emit('error', error);
          if(updateReturned === queriesToUpdate.length) performNextUpdate();
        })
      });
    } else {
      // No queries to update, wait for set duration
      setTimeout(performNextUpdate, STAGNANT_TIMEOUT);
    }
  };

  performNextUpdate();
}

LivePg.prototype._processNotification = function(payload) {
  var self = this;
  var argSep = [];

  // Notification is 4 parts split by colons
  while(argSep.length < 3) {
    var lastPos = argSep.length !== 0 ? argSep[argSep.length - 1] + 1 : 0;
    argSep.push(payload.indexOf(':', lastPos));
  }

  var msgHash   = payload.slice(0, argSep[0]);
  var pageCount = payload.slice(argSep[0] + 1, argSep[1]);
  var curPage   = payload.slice(argSep[1] + 1, argSep[2]);
  var msgPart   = payload.slice(argSep[2] + 1, argSep[3]);
  var fullMsg;

  if(pageCount > 1) {
    // Piece together multi-part messages
    if(!(msgHash in self.waitingPayloads)) {
      self.waitingPayloads[msgHash] =
        _.range(pageCount).map(function() { return null });
    }
    self.waitingPayloads[msgHash][curPage - 1] = msgPart;

    if(self.waitingPayloads[msgHash].indexOf(null) !== -1) {
      return null; // Must wait for full message
    }

    fullMsg = self.waitingPayloads[msgHash].join('');

    delete self.waitingPayloads[msgHash];
  }
  else {
    // Payload small enough to fit in single message
    fullMsg = msgPart;
  }

  return fullMsg;
}

LivePg.prototype._initSelect =
function(query, params, triggers, queryHash, handle) {
  var self = this;
  if(queryHash in self.selectBuffer) {
    // Same query already exists
    // Give a chance for event listener to be added
    process.nextTick(function() {
      var queryBuffer = self.selectBuffer[queryHash];

      queryBuffer.handlers.push(handle);

      // Initial results from cache
      handle.emit('update',
        { removed: null, moved: null, copied: null, added: queryBuffer.data },
        queryBuffer.data);
    });
  } else {
    // Initialize result set cache
    var newBuffer = self.selectBuffer[queryHash] = {
      query         : query,
      params        : params,
      triggers      : triggers,
      data          : [],
      handlers      : [ handle ],
      initialized   : false
    }

    var attachTriggers = function(tablesUsed) {
      var queries = [];

      tablesUsed.forEach(function(table) {
        if(!(table in self.allTablesUsed)) {
          self.allTablesUsed[table] = [ queryHash ];
          var triggerName = self.channel + '_' + table;
          queries.push(
            'DROP TRIGGER IF EXISTS "' + triggerName + '" ON "' + table + '"');
          queries.push(
            'CREATE TRIGGER "' + triggerName + '" ' +
              'AFTER INSERT OR UPDATE OR DELETE ON "' + table + '" ' +
              'FOR EACH ROW EXECUTE PROCEDURE "' + self.triggerFun + '"()');
        } else if(self.allTablesUsed[table].indexOf(queryHash) === -1) {
          self.allTablesUsed[table].push(queryHash);
        }
      });

      if(queries.length !== 0) {
        querySequence(self.connStr, queries, readyToUpdate);
      } else {
        readyToUpdate();
      }
    };

    var readyToUpdate = function(error) {
      if(error) return handle.emit('error', error);
      // Retrieve initial results
      self.waitingToUpdate.push(queryHash)
    };

    // Determine dependent tables, from cache if possible
    if(queryHash in self.tablesUsedCache) {
      attachTriggers(self.tablesUsedCache[queryHash]);
    } else {
      findDependentRelations(self.connStr, query, params,
        function(error, result) {
          if(error) return handle.emit('error', error);
          self.tablesUsedCache[queryHash] = result;
          attachTriggers(result);
        }
      );
    }
  }
}

LivePg.prototype._updateQuery = function(queryHash, callback) {
  var self = this;
  var queryBuffer = self.selectBuffer[queryHash];

  var oldHashes = queryBuffer.data.map(function(row) { return row._hash; });

  pg.connect(self.connStr, function(error, client, done) {
    if(error) return callback && callback(error);
    client.query(
      replaceQueryArgs(REFRESH_QUERY_TPL, {
        query: queryBuffer.query,
        hashParam: queryBuffer.params.length + 1
      }),
      queryBuffer.params.concat([ oldHashes ]),
      function(error, result) {
        done();
        if(error) return callback && callback(error);
        processDiff(result.rows);
      }
    );
  });

  var processDiff = function(result) {
    var diff = differ.generate(oldHashes, result);
    var eventArgs;

    if(diff !== null) {
      var newData = differ.apply(queryBuffer.data, diff);
      queryBuffer.data = newData;

      var eventArgs = [
        'update',
        filterHashProperties(diff),
        filterHashProperties(newData)
      ];

    } else if(queryBuffer.initialized === false) {
      // Initial update with empty data
      var eventArgs = [
        'update',
        { removed: null, moved: null, copied: null, added: [] },
        []
      ];
    }

    if(eventArgs) {
      queryBuffer.handlers.forEach(function(handle) {
        handle.emit.apply(handle, eventArgs);
      });

      queryBuffer.initialized = true
    }

    // Update process finished
    callback && callback();
  }

}

LivePg.prototype._removeSelect = function(select) {
  var self = this;
  var index = self._select.indexOf(select);
  if (index !== -1) {
    // Remove the select object from our list
    self._select.splice(index, 1);

    var queryCache = select.queryCache;
    var queryCacheIndex = queryCache.selects.indexOf(select);
    if (queryCacheIndex !== -1) {
      // Remove the select object from the query cache's list and remove the
      // query cache if no select objects are using it.
      queryCache.selects.splice(queryCacheIndex, 1);
      if (queryCache.selects.length === 0) {
        delete self._queryCache[queryCache.queryCacheKey];
      }
    }

    return true;
  } else {
    return false;
  }
}

LivePg.prototype.end = function() {
  var self = this;
  self.endDbOrPool();
};

function loadQueryFromFile(filename) {
  return fs.readFileSync(path.join(__dirname, filename)).toString();
}

function replaceQueryArgs(query, args) {
  Object.keys(args).forEach(function(argName) {
    query = query.replace(
      new RegExp('\\\$\\\$' + argName + '\\\$\\\$', 'g'), args[argName]);
  });

  return query;
}

function findDependentRelations(connStr, query, params, callback) {
  var nodeWalker = function(tree) {
    var found = [];

    var checkNode = function(node) {
      if('Plans' in node) found = found.concat(nodeWalker(node['Plans']));
      if('Relation Name' in node) found.push(node['Relation Name']);
    }

    if(tree instanceof Array) tree.forEach(checkNode);
    else checkNode(tree);

    return found;
  }

  pg.connect(connStr, function(error, client, done) {
    if(error) return callback && callback(error);
    client.query('EXPLAIN (FORMAT JSON) ' + query, params,
      function(error, result) {
        done();
        if(error) return callback && callback(error);
        callback(undefined, nodeWalker(result.rows[0]['QUERY PLAN'][0]['Plan']));
      }
    );
  });
}

function filterHashProperties(diff) {
  if(diff instanceof Array) {
    return diff.map(function(event) {
      return _.omit(event, '_hash')
    });
  }
  // Otherwise, diff is object with arrays for keys
  _.forOwn(diff, function(rows, key) {
    diff[key] = filterHashProperties(rows)
  });
  return diff;
}

LivePg.LivePgSelect = LivePgSelect;

module.exports = LivePg;