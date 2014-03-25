var db_mongo = require('../main.js')
  , _ = require('underscore')
  , mongo = db_mongo({ mongo: 'mongodb://localhost:27017/mullet' })
  , api
  , find
  , insert
  , remove
  , DB
  , collectionName = 'test__'

  , spec = {
      fields: {
          text: {
              type: String,
              safe: true,
              transform: ['toLowerCase']
          },
          list: {
              type: [String]
          },
          value: {
              type: Number,
              safe: true
          },
          flag: {
              type: Boolean,
              safe: true
          },
          data: {
              type: Object,
              safe: true
          },
          user: {
              type: String,
              synonyms: ['username']
          },
          created: {
              type: Date,
              safe: true
          }
      }
  }

  , test
  , testData = {
        text:   'Testing',
        list:   ['Hello', 'World'],
        value:  150,
        flag:   false,
        data:   { something: 'yes' },
        created: new Date()
    }

  , Schema;

describe('DB (Schema-Mongo)', function() {
    
    it('should return a promise', function() {
        expect(mongo.constructor.name).toBe('Promise');
    });
    
    it('should resolve to a DB interface API', function(done) {
        mongo.then(function(interface) {
            api = interface;
            find = api.find('test__');
            insert = api.insert('test__');
            remove = api.remove('test__');
            expect(find).toBeDefined();
            expect(insert).toBeDefined();
            expect(remove).toBeDefined();
            expect(api.hasId).toBe(true);
            done();
        });
    });    

    it('should instantiate', function() {
        
        DB = require('../../db/main.js')(api);
        Schema = DB.schema(collectionName, spec);
        test = new Schema(testData);

        expect(test.spec).toEqual(spec);
        expect(test.name).toEqual(collectionName);
        expect(test.syns).toEqual( [{field:'user',syns:['username']}] );
        expect(test.whitelist).toEqual( ['text', 'value', 'flag', 'data', 'created'] );

    });

    it('should serialize properly', function() {

        var serialized = _.clone(testData);
        serialized.created = {$date: testData.created.valueOf()};
        serialized._id = test.id;
        serialized.text = testData.text.toLowerCase();
        delete serialized.list;

        expect(JSON.parse(test.serialize())).toEqual(serialized);

    });

    it('should respect driver _id generation', function() {
        expect(test.id).toBeUndefined();
    });
    
    var insertId;
    
    it('should insert records', function(done) {
        
        test.save(function(err,res) {
            expect(err).toBeFalsy();
            expect(res).toEqual(test.get());
            expect(_.omit(res,'_id')).toEqual(_.extend({},testData,{text:'testing'}));
            insertId = res._id;
            done(); 
        });
        
    });
    
    it('should update records', function(done) {
        
        var updateText = 'Testing this update';
        test.set('text', updateText);
        test.save(function(err,res) {
            expect(err).toBeFalsy();
            expect(res).toBe(1); // count of updated recs
            expect(test.get('text')).toEqual(updateText);
            // double check
            find({_id: insertId }, function(err, res) {
                if(_.isArray(res))  res = res[0];
                expect(res.text).toBe(updateText);
                done();
            });
        });
        
    });
    
    it('should remove all records', function(done) {
        remove({}, function(err) {
            expect(err).toBeFalsy();
            done();
        });
    });
    
    it('should close', function(done) {
        api._db.close(function(err) {
            expect(err).toBeNull();
            done();
        });
    });

});