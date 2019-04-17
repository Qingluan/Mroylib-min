# -*- coding: utf-8 -*-
from qlib.data import Cache,dbobj
import pymongo
from pymongo import MongoClient, UpdateOne, InsertOne, DeleteOne, TEXT
from pymongo.database import Database, Collection
from pymongo.bulk import BulkWriteError
from pymongo.errors import BulkWriteError
from pymongo.cursor import Cursor
import bson
from bson import Code
from bson.objectid import ObjectId
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
import time
import xlrd, xlwt
import re
import os
import logging
import pdb
import json
import zipfile
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
from functools import reduce
import motor
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorDatabase
from motor.motor_asyncio import AsyncIOMotorCursor
from motor.motor_asyncio import AsyncIOMotorCollection
import tqdm
import sys
import asyncio
from termcolor import colored

def json2xlsx(file, res, name="result"):
    workbook = xlwt.Workbook()
        
    fields = []
    # for i in res:
    #     for k in i.keys():
    #         if k == "_id" :continue
    #         fields.add(k)

    table_name = name

    sheet = workbook.add_sheet(table_name , cell_overwrite_ok=True)
        
    # for field in range(0,len(fields)):
    #     sheet.write(0,field,fields[field])   
    
    
    for row,item in enumerate(res):
        row += 1
        for no,name in enumerate(item):
            # print(name)
            if name == '_id': continue
            if name not in fields:
                
                sheet.write(0, len(fields), name)
                fields.append(name)
            col = fields.index(name)
            sheet.write(row,col, item.get(name,''))
        print('finish:',row, end='\r')
    workbook.save(file)

def get_tags(file):
    if file.endswith(".xlsx"):
        sheets = xlrd.open_workbook(file)
        datas = {}
        for table in  sheets.sheets():
            table_name = table.name
            fields = [str(i.value) for i in table.row(0)]
            datas[table_name] = fields
        return datas
    elif file.endswith(".csv"):
        with open(file) as fp:
            keys = fp.readline().split(",")
            return {os.path.basename(file): keys}

class ExCursor(Cursor):

    def __setitem__(self, k,v):
        c = 0
        for di in self:
            self.collection.update(di, {"$set" : {k:v}})
            c += 1
        return c

    def __call__(self, key, rex=None):
        yield from self.fuzzy(key, rex=rex)

    def to_xlsx(self, file, name='result'):
        workbook = xlwt.Workbook()
        
        fields = self.collection.keys
        fields.remove("_id")
        table_name = name

        sheet = workbook.add_sheet(table_name , cell_overwrite_ok=True)
            
        for field in range(0,len(fields)):
            sheet.write(0,field,fields[field])   
        
        
        for row,item in enumerate(self):
            
            row += 1
            for col,name in enumerate(fields):
                v = item.get(name, '')
                sheet.write(row,col, v)
            logging.info('finish: %d' % row)

        workbook.save(file)

    @property
    def to_list(self):
        return list(self)

    def fuzzy(self, key, rex=None):
        if rex:
            cq = rex
        else:
            cq = re.compile(key)
        for item in self:
            tq = str(item)
            if cq.search(tq):
                yield item

class ExDatabase(Database):

    def __getitem__(self, name):
        try:
            return ExCollection(self, name)
        except TypeError as e:
            print(name)
            raise e

    def sync_keys(self):
        with ThreadPoolExecutor(8) as exe:
            for col in self.cols:
                f = exe.submit(self[col].keys)

    def fuzzy(self, search_key):
        for ss in self.cols:
                    # print('in collection:',ss)
            coll = self[ss]

            for res in  coll.fuzzy(str(search_key)):
                yield res
    
    def backup_to(self, backup_data, use_parrell=True):
        res = {}
        exe = None
        if use_parrell:
            exe = ThreadPoolExecutor(4)
        if exe:
            fs = {exe.submit(self[col].backup_to, backup_data[col]): col for col in self.cols if 'system.prof' not in col}
            for futu in concurrent.futures.as_completed(fs):
                res[fs[futu]] = futu.result()
        else:
            for col in self.cols:
                if 'system.prof' in col:
                    continue
                if self[col].backup_to(backup_data[col]):
                    res[col] = True
                else:
                    res[col] = False
        return res
            

    def __call__(self, *keys, **kargs):
        for c in self.cols:
            if c == "system.indexes" : continue
            if (set(keys) | set(kargs.keys())) < set(self[c].keys):
                for r in  self[c](*keys, **kargs):
                    yield r


    @property
    def cols(self):
        return self.collection_names()


class ExCollection(Collection):
    __keys = dict()
    _exe = ThreadPoolExecutor(max_workers=10)

    def __setitem__(self, key, val):
        return self.insert_one({key:val})

    
    def indexs(self,*args, text=False, language="english"):
        if args:
            if text:
                f = [(i, TEXT) for i in args]
                self.create_index(f, default_language=language)
            else:
                self.create_index(args)
        
        return self.index_information()
    
    def backup_to(self, backup_collection, bach_size=10240, find_bach=2048):
        res = []
        try:
            c = self.find()
            if find_bach:
                c = c.batch_size(find_bach)
            process = tqdm.tqdm( desc="collecitons: %s"% self.full_name, total=self.count())
            for data in c:
                res.append(data)
                process.update()
                if len(res) % bach_size == 0 and len(res) > 0:
                    backup_collection.insert_many(res)
                    res = []
            if len(res)> 0:
                backup_collection.insert_many(res)
            return True
        except Exception as e:
            if not hasattr(e, 'details'):
                raise e
            if '_id_ dup key' in str(e.details):
                return True
            else:
                _, _, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                l = exc_tb.tb_lineno
                print(res, e.details)
                raise e
                tqdm.tqdm.write(str(e) + "%s:%d" %(fname,l))
                return False
          



    @property
    def search_index(self):
        return [i for i in self.indexs().keys() if i.endswith('_text')]
    


    def search(self, key):
        return self.find({"$text":{"$search": key}})

    def fuzzy(self, key):
        r = re.compile(key, re.IGNORECASE)
        query_key = {"$or":[{i:r} for i in  self.keys if i != "_id"]}
        logging.debug(str(query_key))
        return self.find(query_key)

    def remove_duplicate(self, keys):
        mat_keys = {i: "$" + i for i in keys if i != "_id" }        
        res = self.aggregate([{
            "$group":{ 
                "_id": mat_keys ,
                "dups": {"$addToSet": "$_id"},
                "count":{"$sum": 1},
                }},{
            "$match": {
                "count": { "$gt": 1 }}
            }])
        req = []
        for v in res:
            it = iter(v['dups'])
            next(it)
            for id in it:
                req.append(DeleteOne({'_id': id}))
        try:
            self.bulk_write(req)
        except BulkWriteError as e:
            raise e

        return len(req)


    def update_all(self, items,  keys, merge_check=False):
        # pdb.set_trace()
        _insert_mode = False
        if not keys:
            _insert_mode = True
        #     raise TypeError("must set a key to upsert !!")
        if isinstance(keys, str) and '+' in keys:
            keys = re.sub(r'(\s+?)\+(\s+)','+', keys)
            keys = keys.split()
        reqs = []
        up_reqs = []
        
        if _insert_mode:
            print("[+] insert mode", end='\r')
            for v in items:
                u = InsertOne(v)
                reqs.append(u) 
        else:
            # print("[+] merge key:", keys)
            for v in items:
                con = {}
                if_ma = False
                
                for k in keys:
                    # print
                    if '+' in k:
                        if_in = False
                        for ii in k.split('+'):
                            if ii not in v:
                                if_in = True
                                break
                            con[ii] = v[ii]
                        if not if_in:
                            # print(con, v)
                            if merge_check:
                                uu = InsertOne(v)
                            else:
                                uu = UpdateOne(con, {'$set': v} , upsert=True)
                            
                            up_reqs.append(uu)
                    else:
                        if k in v:
                            con[k] = v[k]
                            # print(con, v)
                            if merge_check:
                                uu = InsertOne(v)
                            else:
                                uu = UpdateOne(con, {'$set': v} , upsert=True)

                            up_reqs.append(uu)
                            con = {}
                            if_ma = True
                            break
                        

                if not if_ma:
                    u = InsertOne(v)
                    reqs.append(u)

        if len(reqs) == 0 and len(up_reqs) ==0:
            return
        try:
            
            
            if merge_check:
                if len(reqs) > 0:
                    self.bulk_write(reqs, ordered=False)
                if len(up_reqs) == 0:
                    print('no data to merge')
                    return
                m = Mon()
                bcol_name = self._Collection__name
                if bcol_name in m.merge.cols:
                    bcol_name += str(int(time.time()))
                m.merge[bcol_name].bulk_write(up_reqs, ordered=False)
                m.merge.merge_info.insert_one({
                    'merge_from':bcol_name,
                    'merge_to': self._Collection__database._Database__name + "." +self._Collection__name,
                    'merge_keys': keys
                    })
                print("need to merge : %d in db.merge.%s" % (len(up_reqs), self._Collection__name))
            else:
                d_name = self._Collection__database._Database__name
                bcol_name = self._Collection__name
                print('%s.%s in : %d                   ' % (d_name, bcol_name, len(reqs + up_reqs)), end='\r')
                self.bulk_write(reqs + up_reqs, ordered=False)
            self.sync_keys()
        except BulkWriteError as bwe:
            return up_reqs
            print("Error:", bwe)




    @property
    def keys(self):
        if not self._Collection__name in ExCollection.__keys:
            self.sync_keys()
        return ExCollection.__keys[self._Collection__name]

    def sync_keys(self):
        logging.info("sync collection: %s 's key"%self._Collection__name)
        # map = Code("function() { for (var key in this) { emit(key, null); } }")
        # reduce = Code("function(key, stuff) { return null; }")
        # result = self.map_reduce(map, reduce, 'tmp')
        result = reduce(lambda all_keys, rec_keys: all_keys | set(rec_keys), map(lambda d: d.keys(), self.find()), set())
        ExCollection.__keys[self._Collection__name] = list(result)

    def find(self, *args, **kargs):
        return ExCursor(self, *args, **kargs)


    def delete(self, *keys, **kargs):
        con = {
            k: {"$exists":"True"} for k in keys
        }
        con.update(kargs)
        return self.delete_many(con)


    def __call__(self, *keys, **kargs):
        con = {
            k: {"$exists":"True"} for k in keys
        }
        con.update(kargs)

        return self.find(con)


    def __add__(self, v):
        if isinstance(v, list):
            self.insert_many(v)
            return self
        elif isinstance(v, dict):
            self.insert_one(v)
            return self
        else:
            raise TypeError("not suport type:%s" % v)


class Mon(MongoClient):

    def __init__(self, host=None, port=None, if_async=False, **kargs):
        self._host = host
        super().__init__(host=host,port=port, **kargs)
        if if_async:
            self._c = motor.motor_asyncio.AsyncIOMotorClient(host=host, port=port, **kargs)
            self.if_async = True
        else:
            self._c = None
            self.if_async = False

    @property
    def show_dbs(self):
        return {i: self[i].command('dbstats')['storageSize']/ 1024**2  for i in self.dbs }
    
    @property
    async def async_show_dbs(self):
        return {i: (await self[i].command('dbstats'))['storageSize']/ 1024**2  for i in self.dbs }


    @property
    def dbs(self):
        return self.database_names()
    
    def scan_db(self, db):
        coll = self[db]

    def default_fileter(self, db):
        db_size = self.show_dbs[db]
        if db in ('admin', 'config'):
            return False
        if db_size < 100:
            return False
        return True
    
    async def deal_exception(self, f):
        try:
            await f
        except BulkWriteError as e:
            pass
        except pymongo.errors.DuplicateKeyError as e:
            pass
        except pymongo.errors.OperationFailure as e:
            _, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            l = exc_tb.tb_lineno
            tqdm.tqdm.write(str(e)+ " %s:%d"% (fname, l))
        except bson.errors.InvalidDocument:
            for v in res:
                try:
                    await async_back_col.insert_one(v)
                except bson.errors.InvalidDocument:
                    pass
            res = []
        except Exception as e:
            _, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            l = exc_tb.tb_lineno
            tqdm.tqdm.write(str(e)+ " %s:%d"% (fname, l))
            print(colored(str(res), 'red'))
            raise e

    async def  async_backup_to(self, col, async_back_col):
        res = []
        count = await col.count_documents({})
        cc = 0
        async for doc in col.find():
            if len(res) % 500 ==0 and len(res) > 0:
                fu = async_back_col.insert_many(res)
                await self.deal_exception(fu)
                res = []
                cc += 1
            res.append(doc)
          
            if len(res) > 0:
                fu = async_back_col.insert_many(res)
                await self.deal_exception(fu)
        
    @classmethod
    async def async_backup_to_another_hosts(cls, *host_ports, back_host="localhost", back_port=27017,limit=10,**kargs):
        tasks = []
        for host,port in host_ports:
            cs = cls(host=host, port=port, if_async=True)
            tasks.append(asyncio.ensure_future(cs.async_backup_to_another_host(host=back_host, port=back_port, limit=limit, **kargs)))
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="All Process"):
            await f
    
    def show_back_up_progress(self, db,col=None):
        if col:
            if self.if_async:
                loop = asyncio.new_event_loop()
                f = loop.create_task(self[db][col].count())
                c = loop.run_until_complete(f)
                tqdm.tqdm.write("%s.%s : %d" % (db, col))
                

    async def async_backup_to_another_host(self, host='localhost', port=27017, filter_func=None, limit=10,**kargs):
        BackUpTo = Mon(host=host, port=port, if_async=self.if_async,**kargs)
        try:
            for db in self.dbs:
                if db in ['config', 'admin']:continue
                if (await self.async_show_dbs)[db] < limit:continue
                adb = self._c[db]
                tasks = []
                backup_name = (self.address[0] + "_" + db).replace('.','_').lower()
                tasks = []
                for col in await adb.list_collection_names():
                    if 'system.' in col:continue
                    task = asyncio.ensure_future(self.async_backup_to(adb[col], BackUpTo[backup_name][col]))
                    tasks.append(task)
                
                for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="%s.%s -> %s"%(self.address[0], db, host)):
                    await f
        except pymongo.errors.ServerSelectionTimeoutError:
            tqdm.tqdm.write("%s:%d timeout" % (host, port))
    
    def backup_to_another_host(self, host='localhost', port=27017, filter_func=None, **kargs):
        BackUpTo = Mon(host=host, port=port, if_async=self.if_async,**kargs)
        if not filter_func:
            filter_func = self.default_fileter
        res = {}
        for db in tqdm.tqdm(self.dbs, desc="backup %s -> %s"% (self.address[0],host)):
            if filter_func(db):
                db_name = (self.address[0]+"_"+db).replace(".","_").lower()
                r = self[db].backup_to(BackUpTo[db_name])
                res[db] = r
        return res

    def wait_merge_info(self, col_name=None):
        if col_name:
            return next(self.merge.merge_info(merge_from=col_name))
        infos = []
        for i in self.merge.cols:
            if i == "merge_info":
                continue
            info= self.merge.merge_info.find_one({'merge_from':i})
            if not info:continue
            info.pop('_id')
            infos.append(info)
        return infos

    def merge_diff(self, col_name, merge_key_json=None):
        merge_info = self.wait_merge_info(col_name)
        if not merge_info:
            return []
        db,col = merge_info['merge_to'].split(".")
        keys = merge_info['merge_keys']
        datas = []
        if col not in self[db].cols:
            self.merge[col].drop()
            self.merge.merge_info.remove({'merge_from', merge_info['merge_from']})
            print("merge data is not exists!!!, clean merge info")
            return 

        # 比对 泛化键值
        if merge_key_json:
            dkeys = []
            for i in keys:
                if '+' in i:
                    dkeys.append('+'.join([merge_key_json.get(ii,ii) for ii in i.split('+')]))
            keys = [merge_key_json.get(i,i) for i in keys if '+' not in i]
            keys += dkeys

        for v in self.merge[col_name].find():
            d = {}
            to = None
            same = True
            for key in keys:
                if '+' in key:
                    akeys = key.split("+")
                    to = self[db][col].find_one({akey:v[akey] for akey in akeys})
                    if to:
                        same = True
                        break
                else:
                    to = self[db][col].find_one({key:v[key]})
                    if to:
                        same = True
                        break
            
            v.pop('_id')
            if to:
                to.pop('_id')
            if to and v == to: continue
            d['to'] = v
            d['from'] = to

            datas.append(d)
        if len(datas) ==0:
            self.merge.merge_info.remove({'merge_from':merge_info['merge_from']})

        merge_info['diff'] = datas
        return merge_info

        

    def merge_sure(self, merge_info):
        if 'diff' not in merge_info:
            return False
        datas = merge_info['diff']
        req = [UpdateOne(i['from'], {'$set':i['to']}) for i in datas if i['from']]
        in_req = [InsertOne(i['to']) for i in datas if not i['from']]
        req  += in_req
        try:
            db,col = merge_info['merge_to'].split('.')
            handle = self[db][col]
            handle.bulk_write(req, ordered=False)
            self.merge[merge_info['merge_from']].drop()
            self.merge.merge_info.remove({'merge_from': merge_info['merge_from']})
        except Exception as e:
            raise e

        return len(req)

    def from_mongo_zip(self, file, db=None,log=print):
        if not file.endswith(".zip"):
            raise Exception("file is not endswith .zip")
        if not db:
            db = os.path.basename(file).replace(".zip","")
        zf = zipfile.ZipFile(file)
        host = ""
        if self._host != "localhost":
            host = self._host
        for f in zf.filelist:
            if not f.is_dir() and f.filename.endswith(".json"):
                with NamedTemporaryFile('w+b') as fp:
                    fp.write(zf.read(f.filename))
                    col = os.path.basename(f.filename).replace(".json","")
                    res = os.popen("mongoimport --host %s -d %s -c %s  %s " % (host,db, col,fp.name)).read()
                    log(res)


    def export_to_zip(self, db,dir="/tmp",log=print):
        file = os.path.join(dir, db+".zip")
        dbh = self[db]
        host = ""
        if self._host != "localhost":
            host = self._host
        with TemporaryDirectory() as dirname:
            for col in dbh.cols:
                col_path = os.path.join(dirname, col + ".json")
                msg = os.popen("mongoexport --host %s -d %s --collection %s --out  %s "% (host,db, col, col_path)).read()
                log(msg)
            msg = os.popen("python3 -m zipfile -c  %s  %s " % (file, dirname)).read()
            log(msg)


    def from_json(self, file, keys=[], db_na='importdata',col_na='from_json', merge_key_json=None, merge_check=False):
        with open(file) as fp:
            datas = json.load(fp)
            cols_handle = self[db_na][col_na]
            try:
                if isinstance(datas, dict):
                    datas = [datas]

                cols_handle.update_all(datas, keys, merge_check=merge_check)
            except Exception as e:
                raise e

    def from_csv(self, file, keys=[], db_na=None, col_na=None, merge_key_json=None, merge_check=False):
        db_name = 'local'
        datas = []
        try:
            fp = open(file)
            fields = [i.strip() for i in fp.readline().strip().split(",") if i.strip()]
        except UnicodeDecodeError:
            fp = open(file, encoding='gbk')
            fields = [i.strip() for i in fp.readline().strip().split(",") if i.strip()]
        table_name = str(int(time.time()))

        # 比对 泛化键值
        if merge_key_json:
            fields = [merge_key_json.get(i,i) for i in fields]
            dkeys = []
            for i in keys:
                if '+' in i:
                    dkeys.append('+'.join([merge_key_json.get(ii,ii) for ii in i.split('+')]))
            keys = [merge_key_json.get(i,i) for i in keys if '+' not in i]
            keys += dkeys
            # print(merge_key_json)
                 
            cols_name = table_name

            # 装载数据准备入库
        try:
            for l in fp:
                od = {}
                vals = [i.strip() for i in l.split(",") if i.strip()]
                for ii,vv in enumerate(vals):
                    if ii >= len(fields): continue
                    vvv = vv
                    if not vvv:
                        continue
                    if isinstance(vvv, str) and not vvv.strip():
                        continue
                    od[fields[ii]] = vv

                datas.append(od)
        except Exception as e:
            raise e
        if not db_na:
            db_na = db_name
        if not col_na:
            col_na = cols_name
        cols_handle = self[db_na][col_na]
        
        try:
            cols_handle.update_all(datas, keys, merge_check=merge_check)
        except Exception as e:
            raise e
        # print("[+]","insert:",cols_name,'len:', len(datas))



    def from_xlsx(self, file, keys=[], db_na=None, col_na=None, merge_key_json=None, merge_check=False):
        sheets = xlrd.open_workbook(file)
        db_name = 'local'
        datas = []
        for table in  sheets.sheets():
            table_name = table.name
            rows = table.nrows
            cols = table.ncols
            fields = [str(i.value) for i in table.row(0)]

            # 比对 泛化键值
            if merge_key_json:
                fields = [merge_key_json.get(i,i) for i in fields]
                dkeys = []
                for i in keys:
                    if '+' in i:
                        dkeys.append('+'.join([merge_key_json.get(ii,ii) for ii in i.split('+')]))
                keys = [merge_key_json.get(i,i) for i in keys if '+' not in i]
                keys += dkeys
                # print(merge_key_json)
                
            cols_name = table_name

            # 装载数据准备入库
            try:
                for i in range(1, rows):
                    od = {}
                    vals = table.row(i)
                    for ii,vv in enumerate(vals):
                        vvv = vv.value
                        if not vvv:
                            continue
                        if isinstance(vvv, str) and not vvv.strip():
                            continue
                        od[fields[ii]] = vv.value

                    datas.append(od)
            except Exception as e:
                raise e
            if not db_na:
                db_na = db_name
            if not col_na:
                col_na = cols_name
            cols_handle = self[db_na][col_na]
            
            try:
                cols_handle.update_all(datas, keys, merge_check=merge_check)
            except Exception as e:
                raise e
            # print("[+]","insert:",cols_name,'len:', len(datas))

    def __getitem__(self, name):
        if self.if_async:
            return AsyncIOMotorDatabase(self._c, name)
        else:
            return ExDatabase(self, name)

    def from_sql(self, database, keys=[], merge_key_json=None, merge_check=True,tp=None, **kargs):
        """
        if sql is mysql:
            @tp="mysql"
        """
        sql_handle = Cache(database, tp=tp, **kargs)
        # for each table
        with ThreadPoolExecutor(8) as exe:
            for col in sql_handle.ls():
                Obj = type(col, (dbobj,), {})
                items = [i.get_dict() for i in sql_handle.query(Obj)]
                if len(items) == 0:
                    continue
                fields  = items[0].keys()
                if merge_key_json:
                    fields = [merge_key_json.get(i,i) for i in fields]
                    dkeys = []
                    for i in keys:
                        if '+' in i:
                            dkeys.append('+'.join([merge_key_json.get(ii,ii) for ii in i.split('+')]))
                    keys = [merge_key_json.get(i,i) for i in keys if '+' not in i]
                    keys += dkeys
                handle = self[database][col]
                futu = exe.submit(handle.update_all, items, keys, merge_check=merge_check)
                futu.add_done_callback(lambda x: logging.info("%s insert ok" % col))
            
