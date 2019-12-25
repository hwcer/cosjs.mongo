"use strict"
//mongoColl multi
function MultiColl(){
    this._MultiCache = [];
}

MultiColl.prototype.insert = function(data){
    if(Array.isArray(data)){
        for(let d of data){
            this._MultiCache.push({ insertOne: { document: d } } );
        }
    }
    else{
        this._MultiCache.push({ insertOne: { document: data } } );
    }
    return Promise.resolve(0);
}

MultiColl.prototype.update = function(query,update,option){
    let upsert = option['upsert'] ? true : false;
    if(!option['multi']){
        this._MultiCache.push({ updateOne: { filter: query , update : update ,upsert : upsert  } } );
    }
    else{
        this._MultiCache.push({ updateMany: { filter: query , update : update ,upsert : upsert  } } );
    }
    return Promise.resolve(0);
}

MultiColl.prototype.delete = function(query,option){
    option = option||{};
    if(!("single" in option)) {
        option["single"] = option["multi"] ? false : true;
    }
    if(option['single']){
        this._MultiCache.push({ deleteOne: { filter: query } } );
    }
    else{
        this._MultiCache.push({ deleteMany: { filter: query } } );
    }
    return Promise.resolve(0);
}


module.exports = MultiColl;
