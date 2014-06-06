var MongoDb = require('mongodb')
  , ObjectID = MongoDb.ObjectID
  , MongoClient = MongoDb.MongoClient
  , MongoServer = MongoDb.Server

  , PromiseApi = require('es6-promise')

  , _ = require('underscore');

module.exports = function(config) {
    
    function MongoAPI() {
        this.hasId = true;
        
        this.host = config.mongo ? config.mongo.host || 'localhost' : 'localhost';
        this.port = config.mongo ? config.mongo.port || 27017 : 27017;
        this._mcli = new MongoClient(new MongoServer(this.host, this.port), { native_parser: true });
        
        return new PromiseApi.Promise(function(send,rej) {
            return this.connect(send,rej);
        }.bind(this));
    }
    
    MongoAPI.prototype.wrapDriver = function(wrapper) {
        this.wrap = wrapper;
    };
    
    MongoAPI.prototype.connect = function(send) {
        this._mcli.open(function(err, cli) {

            if(err) {
                var noop = function() {}
                ,  _db = { // noops
                    collection: function() {
                        return {
                            find:       noop,
                            remove:     noop,
                            insert:     noop,
                            update:     noop
                        };
                    },
                    close:      noop
                };
                console.error(' ! db_mongo failed to connect to ' + this.host+':'+this.post);
                this._db = _db;
                return send({open:function(dbname) {
                    
                    this.dbname = dbname;
                    return this;      
                }.bind(this)});
            }

            this._cli = cli;

            /*
             * API interface for the db_mongo app as fulfilled by promise in constructor
             */
            send({
                
                open: function(dbname) {

                    this.dbname = dbname;
                    this._db = cli.db(dbname);
                    return this;
                
                }.bind(this),
                
                close: function(cb) {
                    cli.close(cb);
                }
                
            });

        }.bind(this));
    };
    
    MongoAPI.prototype._forceSelectors = function(selectors) {
        if(selectors && selectors._id) {
            selectors._id = new ObjectID(selectors._id);
        }
        return selectors;
    };
    
    MongoAPI.prototype._finder = function(collection, data, cb) {
        if('_id' in data)
            data._id = new ObjectID(data._id);
            
        var cursor = collection.find(data);
        return cursor.toArray(cb);
    };
    
    MongoAPI.prototype._saver = function(collection,data,cb) {
        if(data.id) {
            var dataset = data.get();
            delete dataset._id;
            collection.update({_id: data.id}, {$set:dataset}, {safe:true}, cb || function() {});
        }
        else {
            collection.insert( _.isArray(data) ? data : ( 'get' in data ? data.get() : data ), {safe:true}, cb || function() {});
        }
    };
    
    MongoAPI.prototype._updater = function(collection,selector,data,cb) {
        var filteredSelectors = this._forceSelectors(selector);
        collection.update(filteredSelectors, data, {safe:true}, cb);
    };
    
    MongoAPI.prototype._remover = function(collection,search,cb) {
        if(!cb || typeof cb !== 'function') cb = function noop() {};
        return collection.remove(search, cb);
    };
    
    MongoAPI.prototype._doHooks = function(op,data) {
        var spec = this.wrap.spec.fields;
        _.each(spec, function(def,key) {
            if(op=='update' && def.onUpdate) {
                data.$set = data.$set || {};
                data.$set[key] = def.onUpdate();
            }
            if(op=='create' && def.onCreate) {
                data[key] = def.onCreate();
            }
        });
        
        return data;
    };
    
    MongoAPI.prototype.insert = function(name) {
        var collection = this._db.collection(name);
        return function(data, cb) {
            data = this._doHooks('create',data);
            
            return this._saver(collection, data, cb);
        }.bind(this);
    };
    
    MongoAPI.prototype.update = function(name) {
        var collection = this._db.collection(name);
          
        return function(selector, data, cb) {
            data = this._doHooks('update',data);
            
            return this._updater(collection, selector, data, cb);
        }.bind(this);
    };
    
    MongoAPI.prototype.find = function(name) {
        var collection = this._db.collection(name);
        return function(data, cb) {
            return this._finder(collection, data, function(err,results) {
                results = _.map(results, function(rec) {
                    if(rec._id && rec._id instanceof ObjectID)
                        rec._id = rec._id.toHexString();
                        
                    return rec;
                });
                cb.apply(null, [].slice.call(arguments,0));
            });
        }.bind(this);
    };
    
    MongoAPI.prototype.remove = function(name) {
        return function(data,cb) {
            return this._remover(this._db.collection(name), cb);
        }.bind(this);
    };
    
    return new MongoAPI();
};