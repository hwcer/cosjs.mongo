"use strict"
const mongodb = require('mongodb');
const mongodb_util = require('./util');
const mongodb_multi = require('./multi');
const mongodb_library = require('cosjs.library');
const mongodb_promise = mongodb_library.require('promise');
const dataTypeTransform = ['array','json','object'];


class cosjs_mongodb extends mongodb_util{
    constructor(opts,DBName,CollName) {
        super();
        this.upsert        = arguments[3]||false;
        this.unique        = 0;                              //是否唯一，1：单例模式，整个进程中同一CollName只存在一个实例，0：每次都创建新实例
        //this.setOnInsert  = null;
        this.ErrorReCall  = 2;                              //数据库写入失败后反复尝试的次数
        this.ErrorReTime  = 500;                             //数据库写入失败后下次重新尝试写入时间

        this._opts        = opts;
        this._DBName      = DBName;
        this._CollName    = CollName;

        this._MongoColl  = null;
        this._MultiColl  = null;                            //开启批量模式
    }


    collection(){
        if(this._MongoColl){
            return Promise.resolve(this._MongoColl);
        }
        return mongodb_connect(this._opts).then(client=>{
            let db = client.db(this._DBName);
            this._MongoColl = db.collection(this._CollName);
            return this._MongoColl;
        })
    }
    //bulkWrite
    save(){
        let ErrorReNum = arguments[0]||0;
        if(!this._MultiColl){
            return mongodb_promise.callback(null,'mongodb multi empty');
        }
        if(this._MultiColl._MultiCache.length < 1){
            return mongodb_promise.callback(null,'mongodb multi operations empty');
        }
        ErrorReNum++;
        let options = {};
        return this.collection().then(coll=>{
            return coll.bulkWrite(this._MultiColl._MultiCache,options);
        }).then(ret=>{
            this._MultiColl = null;
            return ret;
        }).catch(err=>{
            if( ErrorReNum >= this.ErrorReCall ){
                mongodb_ErrorLogs('save','',this._MultiColl._MultiCache,err);
                return Promise.resolve(err);
            }
            else{
                return mongodb_promise.timeout.call(this,ErrorReNum * this.ErrorReTime,'save',ErrorReNum);
            }
        })
    }
    //复制新对象防止实例缓存导致开启multi出现各种怪异现象
    multi(){
        if(this._MultiColl){
            return this;
        }
        else if(this.unique){
            let newMongodb = Object.create(this);
            newMongodb['_MultiColl'] = new mongodb_multi(newMongodb);
            return newMongodb;
        }
        else {
            this._MultiColl = new mongodb_multi(this);
            return this;
        }
    }
    //id,key,dataType
    get(id,key,dataType){
        let query  = this.query(id);
        let option = {"multi":this.isMulti(id),"projection":this.fields(key)};
        if(typeof dataType === "object"){
            Object.assign(option,dataType)
            if(!option["dataType"]){
                option["dataType"] = "json";
            }
        }
        else{
            option["dataType"] = dataType||"json";
        }

        return this.collection().then(coll=>{
            if(option["multi"]){
                return coll.find(query, option);
            }
            else{
                return coll.findOne(query, option);
            }
        }).then(cursor=>{
            if(option["multi"]){
                return mongodb_multiResult(option,cursor);
            }
            else{
                return mongodb_singleResult(key,cursor);
            }
        })
    }
    //id,key,val
    set(id,key,val) {
        let query = this.query(id);
        let update = {"$set":this.values(key,val)};
        let option = {"multi":this.isMulti(id)};

        if( !option["multi"] && this.upsert && typeof this.setOnInsert === 'function'){
            update["$setOnInsert"] = this.setOnInsert(id);
        }
        return this.update(query,update,option);
    }
    //删除一条,或者一个字段
    del(id,key) {
        let query  = this.query(id);
        let option = {"multi":this.isMulti(id)};
        if(!key){
            return this.remove(query,option);
        }
        else{
            let update = {"$unset":this.values(key,1)};
            return this.update(query,update,option);
        }
    }

    incr(id, key, val) {
        let query  = this.query(id);
        let update = {"$inc":this.values(key,val)};
        let option = {"multi":this.isMulti(id)};
        if(!option['multi']){
            option['projection'] = this.fields(key);
        }
        if( !option["multi"] && this.upsert && typeof this.setOnInsert === 'function'){
            update["$setOnInsert"] = this.setOnInsert(id);
        }
        return this.update(query,update,option).then(ret=>{
            if(option['projection']){
                return mongodb_singleResult(key,ret);
            }
            else {
                return ret;
            }
        })
    }
    //分页显示
    page(query, page, size, sort, option){
        size = size || 10;
        if(page<1){
            page = 1;
        }
        option['multi'] = true;
        option['sort'] = sort;
        option['projection'] = option['projection'] || option['fields'] || {};
        delete option['fields'];
        let data = {"page":page,"size":size,"total":0,"records":0,"rows":[] };
        let skip  = (page - 1 ) * size ,limit = size ;

        return this.collection().then(coll=>{
            return coll.find(query, option)
        }).then(cursor=>{
            return cursor.count().then(ret=>{ 
                data['records'] = ret;
                return cursor;
            })
        }).then(cursor=>{
            if(data['records'] > 0){
                return cursor.skip(skip).limit(limit).toArray();
            }
        }).then(rows=>{
            data['total'] = Math.ceil(data['records'] / data['size']);
            data['rows'] = rows||[];
            return data;
        })
    }
    //查询
    find(query,option={}) {
        option['projection'] = option['projection'] || option['fields'] || {};
        delete option['fields'];
        return this.collection().then(coll=>{
            if(option["multi"]){
                return coll.find(query, option);
            }
            else{
                return coll.findOne(query, option);
            }
        }).then(cursor=>{
            if(option["multi"]){
                return mongodb_multiResult(option,cursor);
            }
            else{
                return cursor;
            }
        })
    }
    //统计
    count(query,options={}){
        return this.collection().then(coll=>{
            if(typeof coll.countDocuments === 'function'){
                return coll.countDocuments(query,options);
            }
            else{
                return coll.count(query,options);
            }
        })
    }
    //删除
    remove(query) {
        let option = arguments[1]||{};
        if(this._MultiColl){
            return this._MultiColl.delete(query,option);
        }
        let ErrorReNum = arguments[2]||0;
        ErrorReNum++;

        return this.collection().then(coll=>{
            if(option["multi"]){
                return coll.deleteMany(query, option );
            }
            else {
                return coll.deleteOne(query, option);
            }
        }).then(ret=>{
            return ret['result']||ret;
        }).catch(err=>{
            if( String(err['code']) === "11000" || ErrorReNum >= this.ErrorReCall ){
                mongodb_ErrorLogs('remove',query,'',err);
                return Promise.reject(err);
            }
            else{
                return mongodb_promise.timeout.call(this,ErrorReNum * this.ErrorReTime,'remove',query,option,ErrorReNum);
            }
        })
    }
    //data,[option],callback,[ErrorReNum]
    insert(data,option={}) {
        if(Array.isArray(data)){
            for(let v of data){
                mongodb_insert.call(this,v);
            }
        }
        else{
            mongodb_insert.call(this,data);
        }

        if(this._MultiColl){
            return this._MultiColl.insert(data,option);
        }

        let ErrorReNum = arguments[2]||0;
        ErrorReNum++;
        return this.collection().then(coll=>{
            if(Array.isArray(data)){
                return coll.insertMany(data,option);
            }
            else{
                return coll.insertOne(data,option);
            }
        }).then(ret=>{
            return ret['result']||ret;
        }).catch(err=>{
            if( String(err['code']) === "11000" || ErrorReNum >= this.ErrorReCall ){
                mongodb_ErrorLogs('insert','',data,err);
                return Promise.reject(err);
            }
            else{
                return mongodb_promise.timeout.call(this,ErrorReNum * this.ErrorReTime,'insert',data,option,ErrorReNum);
            }
        })
    }
    //update,multi and upsert 不能同时使用,findAndModify 不能使用MULTI
    update(query,update,option) {
        if(!("upsert" in option)){
            option['upsert'] = this.upsert;
        }
        if(this._MultiColl){
            return this._MultiColl.update(query,update,option);
        }

        let ErrorReNum = arguments[3]||0;
        ErrorReNum++;

        if(option['fields']){
            option['projection'] = option['fields'];
            delete option['fields'];
        }

        if( option['multi'] && ( option['upsert'] || option['projection']  ) ){
            return mongodb_promise.callback('MongoError','mongodb.update use multi but set upsert or fields');
        }
        return this.collection().then(coll=>{
            if( option['projection'] ){
                option['returnOriginal'] = false;
                return coll.findOneAndUpdate(query,update,option);
            }
            else if(option['multi']){
                return coll.updateMany(query, update, option);
            }
            else{
                return coll.updateOne(query, update, option);
            }
        }).then(ret=>{
            if(option['projection']){
                return ret['value'];
            }
            else{
                return ret['result']||ret;
            }
        }).catch(err=>{
            if( String(err['code']) === "11000" || ErrorReNum >= this.ErrorReCall ){
                mongodb_ErrorLogs('update',query, update, err);
                return Promise.reject(err);
            }
            else{
                return mongodb_promise.timeout.call(this,ErrorReNum * this.ErrorReTime,'update',query,update,option,ErrorReNum);
            }
        })
    }
    //aggregate
    aggregate(pipeline, options){
        return this.collection().then(coll=>{
            return coll.aggregate(pipeline, options);
        }).then(cursor=>{
            return mongodb_multiResult(options,cursor);
        })
    }
}

function mongodb_insert(data){
    if(!data["_id"]){
        let ObjectID = this.ObjectID || mongodb.ObjectID;
        data["_id"] = ObjectID();
    }
}

function mongodb_connect(opts) {
    let callback = arguments[1] || mongodb_promise.callback;
    let MongoClient = mongodb.MongoClient;
    let MongoOption = {"useNewUrlParser":true};
    if(opts instanceof MongoClient){
        return callback(null,opts);
    }
    let mongodbUrl;
    if(typeof opts === "string"){
        mongodbUrl = opts.substr(0,10)=== 'mongodb://' ? opts : 'mongodb://' + opts;
    }
    else {
        let encodeUrl = mongodb_url_encode(opts);
        mongodbUrl = 'mongodb://' + encodeUrl;
    }
    return MongoClient.connect(mongodbUrl, MongoOption).then(db=>{
        return callback(null,db);
    }).catch(err=>{
        return callback(err);
    })
}

function mongodb_multiResult(opt,cursor){
    let dataType = opt['dataType'] || 'json';
    if( !cursor || dataTypeTransform.indexOf(dataType) < 0){
        return cursor;
    }
    return cursor.toArray().then(val=>{
        if(dataType === 'array'){
            return val;
        }
        let key =  opt["key"] || '_id',rows = {};
        for(let d of val){
            let id = d[key];rows[id] = d;
        }
        return rows;
    })
}

function mongodb_singleResult(key,cursor){
    if( !cursor || !key || typeof key === 'object'){
        return cursor;
    }
    else{
        return cursor[key]||null;
    }
}

//错误日志
function mongodb_ErrorLogs(method,query,update,error){
    let err = error['name']||'MongoError';
    let ret = error['errmsg'] || error['code'];
    console.error(err,method,JSON.stringify(query),JSON.stringify(update),ret);
}

function mongodb_url_encode(config){
    let url,args = [];
    if(config['url']){
        url = Array.isArray(config['url']) ? config['url'].join(','):config['url'];
    }
    else{
        url = [config['host'],config['port']||27017].join(':');
    }
    if(config['database']){
        url +=  ('/'+ config['database']);
    }
    if(config['username'] && config['password']){
        url = config['username'] + ':'+ config['password'] + '@' + url;
    }

    let keys = ['url','host','port','database','username','password'];
    for(let k in config){
        if( keys.indexOf(k) < 0 ){
            args.push(k+'='+config[k]);
        }
    }
    if(args.length>0){
        url += ('?' + args.join('&'));
    }
    return url;
};



module.exports = cosjs_mongodb;
module.exports.connect = mongodb_connect;
module.exports.ObjectID = mongodb_util.ObjectID;