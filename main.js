var mongo = require('mongodb').MongoClient

  , PromiseApi = require('es6-promise')

  , _ = require('underscore');

module.exports = function(config) {

    var _db;
    
    function finder(collection, data, cb) {
        var cursor = collection.find(data);
        return cursor.toArray(cb);
    }
    
    function save(collection, data, cb) {

        if(data.id) {
            var dataset = data.get();
            delete dataset._id;
            collection.update({_id: data.id}, {$set:dataset}, {safe:true}, cb);
        }
        else {
            collection.insert( _.isArray(data) ? data : ( 'get' in data ? data.get() : data ), {safe:true}, cb);
        }
    }
    
    function remover(collection, search, cb) {
        if(!cb || typeof cb !== 'function') cb = function noop() {};
        return collection.remove(search, cb);
    };
    
    var API = {

        find: function(name) {
            var collection = _db.collection(name);
            return function(data, cb) {
                return finder(collection, data, cb);
            };
        },

        insert: function(name) {
            var collection = _db.collection(name);
            return function(data, cb) {
                return save(collection, data, cb);
            };
        },

        remove: function(name) {
            return function(data,cb) {
                return remover(_db.collection(name), cb);
            };
        },
        
        hasId:  true,
        
        _db:    undefined

    };
    
    return new PromiseApi.Promise(function(send,rej) {

        mongo.connect(config.mongo, function(err, db) {
            
            if(err) return rej(err);
            
            _db = db;
            API._db = db;
            send(API);
            
        });
        
    });
};