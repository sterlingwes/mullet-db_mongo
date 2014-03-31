var MongoDb = require('mongodb')
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
    
    MongoAPI.prototype.connect = function(send,rej) {
        this._mcli.open(function(err, cli) {

            if(err) {
                var noop = function() {};
                _db = { // noops
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
                console.error(' ! db_mongo failed to connect to ' + config.mongo);
                this._db = _db;
                return send(API);
            }

            this._cli = cli;

            send(function(dbname) {

                this.noproto = true;
                this.dbname = dbname;
                this._db = cli.db(dbname);
                return this;
                
            }.bind(this));

        }.bind(this));
    };
    
    MongoAPI.prototype._finder = function(collection, data, cb) {
        var cursor = collection.find(data);
        return cursor.toArray(cb);
    };
    
    MongoAPI.prototype._saver = function(collection,data,cb) {
        if(data.id) {
            var dataset = data.get();
            delete dataset._id;
            collection.update({_id: data.id}, {$set:dataset}, {safe:true}, cb);
        }
        else {
            collection.insert( _.isArray(data) ? data : ( 'get' in data ? data.get() : data ), {safe:true}, cb);
        }        
    };
    
    MongoAPI.prototype._remover = function(collection,search,cb) {
        if(!cb || typeof cb !== 'function') cb = function noop() {};
        return collection.remove(search, cb);
    };
    
    MongoAPI.prototype.insert = function(name) {
        var collection = this._db.collection(name);
        return function(data, cb) {
            return this._saver(collection, data, cb);
        }.bind(this);
    };
    
    MongoAPI.prototype.find = function(name) {
        var collection = this._db.collection(name);
        return function(data, cb) {
            return this._finder(collection, data, cb);
        }.bind(this);
    };
    
    MongoAPI.prototype.remove = function(name) {
        return function(data,cb) {
            return this._remover(this._db.collection(name), cb);
        }.bind(this);
    };
    
    return new MongoAPI();
};