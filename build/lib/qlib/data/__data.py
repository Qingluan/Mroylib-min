import sqlite3,time, os, logging
import xlwt, xlrd
import csv
import contextlib
from tempfile import TemporaryFile, NamedTemporaryFile
import re
import json
# import requests
DEBUG = False

import requests
import urllib.parse as up
import tqdm
from concurrent.futures.thread import ThreadPoolExecutor
# DEBUG = True
BACK_PATH = "/tmp/back_dirs"
if not os.path.exists(BACK_PATH):
    os.mkdir(BACK_PATH)

try:
    import pymysql
except Exception:
    pass

"""
# example:
# use:
from data import  *
c = Cache("/tmp/test.db")
w = list(c.query(File, "like",file="dat"))

# expend:
# don't overwrite function: '__init__' !!!!
# don't overwrite function: '__init__' !!!!
# don't overwrite function: '__init__' !!!!
create table test22 as select Infos.reg_time, Users.name, Infos.email, Infos.ip from Infos left outer join Users on Infos.uid == Users.uid ;
class XXX(dbobj):
    ...

c = Cache(db_path)

## insert
e = XXX(k=v, k2=v2)
e.save(c)

## query

c.query(XXX, id=2)                  #id=2 's XXX 
c.query(XXX,'<', id=2)              #id < 2 's XXX 


## query by time , if there be , which like:
c.query(XXX, '<', time='2017-12-1 18:37:23')   # time before 2017-12-1 18:37:23's XXX
c.query(XXX, '<', time='18:37:23')   # time before 2017-12-1 18:37:23's XXX
c.query(XXX, '<', time='18:37')   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '~2', time='18:37')   # time before 2017-12-1 18:37:00 +- 2 sec 's XXX

c.query(XXX, '<', time=1512124620.0)   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '>', time=1512124620.0)   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '~1', time=1512124620.0)   # time before 2017-12-1 18:37:00 +- 1 sec 's XXX
c.query(XXX, '~2', time=1512124620.0)   # time before 2017-12-1 18:37:00 +- 2 sec 's XXX


"""


class ESplugin(object):
    Host = 'http://localhost:9200'



    def __init__(self, host=None,**kargs):
        self.kargs = kargs
        self.sess = requests.Session()
        self.sess.headers.update({
            "Content-Type": "application/json;charset=UTF-8",
        })
        if host:
            self.Host = host

    def to_mapping(self, ObjSample, index):
        kind = ObjSample.get_dict()
        table_name = ObjSample.__class__.__name__

        data = {
            "mappings" : {
                table_name:{
                    'properties':{

                    }
                }
            }
        }
        es_format = {
            int: 'long',
            str: 'text',
            float: 'double',
            bool: 'boolean',
        }
        for k,v in kind.items():
            tp = es_format.get(type(v), 'text')

            data['mappings'][table_name]['properties'][k] = {
                'type': tp,
            }
        return self.sess.put(up.urljoin(self.Host, index), data=json.dumps(data)).json()
        # return res

    def ls(self, index='', type=''):
        url = self.Host
        if index:
            url = up.urljoin(url, index)
        if type:
            url = up.urljoin(url, type)
        return requests.get(url).json()

    def search(self, index,  query_key='', type='', **kargs):
        size = 10
        if '_size' in kargs:
            size = kargs['_size']

        if kargs:
            url = os.path.join(self.Host, index)
            _raw_cond = kargs.items()
            cond = [{"match": {k:v}} for k,v in _raw_cond]
            query_condition = {
                "query" : {
                    "bool": {
                        "must": cond
                        },
                    },
                'size': size,
            }
            return self.sess.post(url, data=json.dumps(query_condition)).json()
        else:
            url = os.path.join(self.Host, index)
            if type:
                url = os.path.join(url, type) + '/routing=%s' % query_key
            else:
                url = url + '/_search?routing=%s' % query_key
            return self.sess.get(url).json()

        

    def to_add_document(self, Obj, index):
        data = Obj.get_dict()
        url = os.path.join(os.path.join(self.Host, index), Obj.__class__.__name__)
        url = os.path.join(url, str(Obj.id))
        print(url, data)
        return self.sess.put(url, data=json.dumps(data)).json()




                



class dbobj(object):
    """
# example:
# use:
from data import  *
c = Cache("/tmp/test.db")
w = list(c.query(File, "like",file="dat"))

# expend:
# don't overwrite function: '__init__' !!!!
# don't overwrite function: '__init__' !!!!
# don't overwrite function: '__init__' !!!!

class XXX(dbobj):
    ...

c = Cache(db_path)

## insert
e = XXX(k=v, k2=v2)
e.save(c)

## query

c.query(XXX, id=2)                  #id=2 's XXX 
c.query(XXX,'<', id=2)              #id < 2 's XXX 


## query by time , if there be , which like:
c.query(XXX, '<', time='2017-12-1 18:37:23')   # time before 2017-12-1 18:37:23's XXX
c.query(XXX, '<', time='18:37:23')   # time before 2017-12-1 18:37:23's XXX
c.query(XXX, '<', time='18:37')   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '~2', time='18:37')   # time before 2017-12-1 18:37:00 +- 2 sec 's XXX

c.query(XXX, '<', time=1512124620.0)   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '>', time=1512124620.0)   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '~1', time=1512124620.0)   # time before 2017-12-1 18:37:00 +- 1 sec 's XXX
c.query(XXX, '~2', time=1512124620.0)   # time before 2017-12-1 18:37:00 +- 2 sec 's XXX


    """

    
    def __init__(self, **kargs):
        for k,v in kargs.items():
            setattr(self, k, v)

    def before_save(self):
        """
    do some what you want to, 
    this will run before save
        """
        pass

    def to_do(self, function, *args, **kargs):
        return function(self, *args, **kargs)

    @staticmethod
    def str2time(self, str, format="%Y-%m-%d %H:%M:%S"):
        return time.mktime(time.strptime(str, format))


    @staticmethod
    def flat(data):
        for k,v in data.items():
            if isinstance(v, dict):
                yield from dbobj.flat(v)
            elif type(v) in (int,None,str,):
                yield (k,v)
            else:
                yield (k,str(v))

    def sec2date(self, time_sec):
        return time.gmtime(time_sec)

    def gettimezone(self):
        return self.sec2date(self.time)

    def getTime(self):
        return time.ctime(self.time)

    def show(self):
        __dict = self.__dict__
        return [(k,__dict[k]) for k in __dict]

    def search(self, key):
        if isinstance(key, int):
            key = str(key)

        for v in self.get_dict().values():
            if not isinstance(v, str):
                v = str(v)
            if key in v:
                return self, v

        return '',''

    def update(self,con):
        _change_str = "UPDATE %s SET " % self.__class__.__name__
        __dict = self.__dict__
        id = None
        id_or_uid = 'id'
        if 'id' in __dict:
            id = __dict.pop('id')

        elif 'uid' in __dict:
            id = __dict.pop('uid')
            id_or_uid = 'uid'

        for k in __dict:
            _change_str += "%s=?," %k

        __change_str = _change_str[:-1] + " WHERE %s=? ;" % id_or_uid
        
        if not con:
            return __change_str

        try:
            if DEBUG:
                logging.debug(__change_str)
            vars = list(__dict.values())
            vars.append(id)
            if con.tp == 'mysql':
                __change_str = __change_str.replace("?", "%s")
            con._con.execute(__change_str, vars)
            return 'update'

        except sqlite3.OperationalError as e:
            # print(con._con.execute("select sql from sqlite_master where name=(?)", [self.__class__.__name__]).fetchone()[0])
            raise e
        except Exception:
            __dict.pop('time')
            _change_str = "UPDATE %s SET " % self.__class__.__name__
            for k in __dict:
                _change_str += "%s=?," %k
            __change_str = _change_str[:-1] + " WHERE %s=? ;" % id_or_uid
            __change_str = __change_str.replace("?", "%s")
            vars = list(__dict.values())
            vars.append(id)
            con._con.execute(__change_str, vars)
        finally:
            con._con.commit()


    def get_fields(self):
        return self.__dict__.keys()

    def get_dict(self):
        return self.__dict__ 


    @classmethod
    def from_json(cls, data,simple=False, flat=False):
        if isinstance(data, dict):
            pass
        elif isinstance(data, str):
            data = json.loads(data)
        else:
            raise TypeError("must json str or dict!!")
        if flat:
            if simple:
                res = {  k : v for k,v in cls.flat(data)}
            else:
                res = {  k : v for k,v in cls.flat(data)}
        else:
            res = dict()
            for k,v in data.items():
                if type(v) in (int, str, None,):
                    if simple:
                        res[k] = v
                    else:
                        res[k] = v
                else:
                    if simple:
                        res[k] = json.dumps(v)
                    else:
                        res[k] = json.dumps(v)
                    
        
        return cls(**res)

    def __repr__(self):
        if hasattr(self, 'id'):
            n = str(self.id)
        elif hasattr(self,'uid'):
            n = str(self.uid)
        else:
            n = 'no_id_or_uid'
        return self.__class__.__name__ + "_" + n

    def add(self, con, check_same=False):
        __dict = self.__dict__
        _insert_str = "INSERT INTO %s (%s) VALUES(" % (self.__class__.__name__, ",".join(__dict.keys()))
        for k in __dict:
            v = __dict[k]
            if k == "time":
                _insert_str += "?,"
                continue

            _insert_str += "?,"

        __insert_str = _insert_str[:-1] + ");"
        __values = __dict.values()
        if not con:
            return __insert_str, __values
        if check_same:
            k,v = next(__dict.items())
            _check_str = "SELECT %s FROM %s WHERE %s=(?);" % (k, self.__class__.__name__, k)
            if con.tp == 'mysql':
                c = con._con.execute(_check_str, v)
                if c > 0:
                    return 'already'
            else:
                res = c.fetchone()
                if res:
                    return 'already'

        try:
            if DEBUG:
                logging.debug(__insert_str)
            con._con.execute(__insert_str, list(__values))
            return 'insert'
        except sqlite3.OperationalError as e:
            # print(con._con.execute("select sql from sqlite_master where name=(?)", [self.__class__.__name__]).fetchone()[0])
            raise e
        except Exception as e:
            logging.error("error line:382")
        finally:
            con._con.commit()

    
    def save(self, con=None, tp=None, check_same=False):
        self.before_save()
        if not hasattr(self, 'time'):
            self.time = int(time.time())

        __dict = self.__dict__
        if not hasattr(self, 'time'):
            _create_str = "CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, time DATETIME," % self.__class__.__name__
        elif hasattr(self, 'time'):
            _create_str = "CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT," % self.__class__.__name__
        _insert_str = "INSERT INTO %s (%s) VALUES(" % (self.__class__.__name__, ",".join(['`%s`' % i for i in __dict.keys()]))
        if tp == 'mysql':
            _create_str = "CREATE TABLE %s (" % self.__class__.__name__

        # if con.tp == 'mysql':

        
        for k in __dict:
            v = __dict[k]
            if k == 'id':
                _insert_str += "?,"
                continue

            if isinstance( v, str):
                _create_str += "`%s`" % k + " TEXT,"
            elif isinstance( v, bytes):
                _create_str += "`%s`" % k + " BLOB,"
            elif isinstance( v, (int, float)):
                if 'time' in k:
                    _create_str += "`%s`" % k + " DATETIME,"
                    if tp == 'mysql' or (con and con.tp == 'mysql'):
                        __dict[k] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(__dict[k])))
                    # if con.
                    # __dict[k] = 
                else:
                    if isinstance(v, float):
                        _create_str += "`%s`" % k + " FLOAT,"
                    else:
                        _create_str += "`%s`" % k + " INTEGER,"

            _insert_str += "?,"
        __create_str = _create_str[:-1] + ");"
        __insert_str = _insert_str[:-1] + ");"

        
                    

        __values = __dict.values()
        
        if not con:
            return __create_str, __insert_str, __values
        
        # if has id -> update
        # print("----")
        if hasattr(self, 'id') and con.query_one(self.__class__, id=self.id):
            id = self.id
            
            self.update(con)
            # print(".....")
            self.id = id
            return 'update id: {}'.format(id)
            
        elif hasattr(self, 'uid') and con.query_one(self.__class__, uid=self.uid):
            uid = self.uid
            
            self.update(con)

            self.uid = uid
            return 'update id: {}'.format(uid)

        try:
            if con.tp == 'mysql':
                __create_str = __create_str.replace("AUTOINCREMENT", "AUTO_INCREMENT")
            con._con.execute(__create_str)

        except Exception as e:
            if "duplicate column name: id" in str(e):
                # __create_str = __create_str.replace('id INTEGER,', '')
                # __create_str = __create_str.replace('time DATETIME,', '',1)
                # __insert_str = __insert_str.replace(',time)', ')',1)
                # if __create_str.count()
                # __insert_str = __insert_str.replace(',?)', ')',1)
                # __values = tuple(list(__values)[:-1])
                print(__create_str,__insert_str, list(__values))
                # con._con.execute(__create_str)
                
            
            else:
                # print(e)
                if DEBUG:
                    logging.debug(e)
            
        

        try:
            if DEBUG:
                logging.debug(__create_str,__insert_str,)
            if con.tp == 'mysql':
                __insert_str = __insert_str.replace('?', '%s')

                # __values = __values[:-1]

            con._con.execute(__insert_str, list(__values))

            return 'insert'
        except sqlite3.OperationalError as e:
            
            # print(con._con.execute("select sql from sqlite_master where name=(?)", [self.__class__.__name__]).fetchone()[0])
            raise e
        except Exception as e:
            print(__create_str)
            print(__insert_str)
            print(list(__values))
            __dict.pop('time')
            __back_insert_str = "INSERT INTO %s (%s) VALUES(" % (self.__class__.__name__, ",".join(__dict.keys()))
            for k in __dict:
                __back_insert_str += "%s,"
            
            # print(__back_insert_str)
            con._con.execute(__back_insert_str[:-1] + ");", list(__values))
        finally:
            con._con.commit()



class  File(dbobj):
    """docstring for  FileObj"""
    def bak(self):
        if not self.time:
            self.time = int(time.time())

        if not self.link:
            self.link = os.path.join(BACK_PATH, str(self.time))

        try:
            os.rename(self.file, self.link)
        except IOError as e:
            logging.error (e)

    def set_backup_path(self, newpath):
        global BACK_PATH
        BACK_PATH = newpath
    
    def before_save(self):
        self.time = int(time.time())
        self.link = os.path.join(BACK_PATH, str(time.time()))
        assert(hasattr(self, 'file'))


class NetWork(dbobj):
    pass



class Cache:
    """
# example:
# use:
from data import  *
c = Cache("/tmp/test.db")
w = list(c.query(File, "like",file="dat"))

# expend:
# don't overwrite function: '__init__' !!!!
# don't overwrite function: '__init__' !!!!
# don't overwrite function: '__init__' !!!!

class XXX(dbobj):
    ...

c = Cache(db_path)

## insert
e = XXX(k=v, k2=v2)
e.save(c)

## query

c.query(XXX, id=2)                  #id=2 's XXX 
c.query(XXX,'<', id=2)              #id < 2 's XXX 


## query by time , if there be , which like:
c.query(XXX, '<', time='2017-12-1 18:37:23')   # time before 2017-12-1 18:37:23's XXX
c.query(XXX, '<', time='18:37:23')   # time before 2017-12-1 18:37:23's XXX
c.query(XXX, '<', time='18:37')   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '~2', time='18:37')   # time before 2017-12-1 18:37:00 +- 2 sec 's XXX

c.query(XXX, '<', time=1512124620.0)   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '>', time=1512124620.0)   # time before 2017-12-1 18:37:00's XXX
c.query(XXX, '~1', time=1512124620.0)   # time before 2017-12-1 18:37:00 +- 1 sec 's XXX
c.query(XXX, '~2', time=1512124620.0)   # time before 2017-12-1 18:37:00 +- 2 sec 's XXX


    """

    paths = {}

    def __init__(self, dbpath, driver=sqlite3 ,tp=None,**connect_options):
        self._db_path = dbpath
        self.tp = tp
        if hasattr(driver, 'connect'):
            self._driver = driver
            if dbpath == ":memory:":
                self._con = getattr(driver,'connect')(dbpath, check_same_thread=False)
            elif tp=='mysql':
                default = {
                    'user':'root',
                    'password':'',
                }
                driver = pymysql
                default.update(connect_options)
                self.__con = getattr(driver,'connect')(database=dbpath, charset='utf8',**default)
                self._con = self.__con.cursor()
                if not hasattr(self._con, 'commit'):
                    self._con.commit = self.__mysql_commit
            else:
                self._con = getattr(driver,'connect')(dbpath, **connect_options)
        else:
            return

    def __mysql_commit(self):
        self.__con.commit()

    def like(self, obj, **args):
        ks = {k: "%" + str(v) + "%" for k,v in args.items()}
        return self.query(obj, eq='like', **ks)

    def query(self,obj, eq="=", m='or', timestamp=None,**kargs):
        """
        eq: like
            =
            >
            <

        m:  or
            and
        """

        try:
            if self.tp != "mysql":
                keys = [ ]
                for ii,i in enumerate(self._con.execute("select sql from sqlite_master where name=(?)", [obj.__name__]).fetchone()[0].split(",")):
                    if ii == 0:
                        i = i.split("(")[1]
                    k = i.split()[0]
                    k = re.findall(r'(\w+)',k)[0]
                    if k == 'id':continue

                    if ')' in k:
                        keys.append(k.replace(')',''))
                    else:
                        keys.append(k)
            elif self.tp == 'mysql':
                try:
                    self._con.execute("desc %s" %obj.__name__)
                    
                    keys = [ i[0] for i in  self._con.fetchall()]
                    


                except Exception:
                    return
            else:
                keys = []
            if 'id' not in keys and 'uid' not in keys:
                keys.insert(0,"id")
        except TypeError as e:
            # logging.error(e)
            return 
        
        _query_str = "SELECT * FROM %s WHERE " % obj.__name__
        
        ## 
        # this block is for parse timestamp
        time_sec = None
        if timestamp != None and isinstance(timestamp, str) :
            err_time = 0
            
            while err_time < 3:
                tody = time.gmtime(time.time())
                pre_time = "%d-%d-%d " %(tody.tm_year,tody.tm_mon,tody.tm_mday)
                try:
                    if err_time == 0:
                        time_sec = time.mktime(time.strptime(timestamp.strip(), "%Y-%m-%d %H:%M:%S"))
                    elif err_time == 1:
                        time_sec = time.mktime(time.strptime(pre_time + timestamp.strip(), "%Y-%m-%d %H:%M:%S"))
                    elif err_time == 2:
                        time_sec = time.mktime(time.strptime(pre_time + timestamp.strip()+":0", "%Y-%m-%d %H:%M:%S"))
                    # print(time_sec)
                except ValueError as  e:
                    err_time += 1

                    continue
                
                if time_sec:
                    break
        elif isinstance(timestamp, (int, float,)):
            time_sec = timestamp

        if time_sec:
            kargs['time'] = int(time_sec)

        for k,v  in kargs.items():

            if isinstance(v, int):
                if eq.startswith("~"):
                    may_be = int(eq[1:])
                    ww = ("(" + k + " > %d and " + k + " < %d ) %s") % ( v- may_be,v + may_be, m)
                    
                    _query_str +=   ww
                    continue
                
                _query_str+= k + " %s%d %s " % (eq,v, m)
            elif isinstance(v, str):
                if eq == "like":
                    _query_str+= k + " %s \"%%%s%%\" %s " % (eq,v, m)
                else:
                    _query_str+= k + " %s \"%s\" %s " % (eq,v, m)
        __query_str = _query_str.strip()[:-len(m)] + ";"
        if DEBUG:
            logging.debug(__query_str)
        try:
            res = self._con.execute(__query_str)
            if self.tp =='mysql':
                res = self._con
        except sqlite3.OperationalError:
            logging.error("{}".format(__query_str))


        for i,v in enumerate(res.fetchall()):
            # print(keys, v)
            if len(keys) != len(v):
                v = [i] + [ii for ii in v]
            raw = dict(zip(keys,v))
            try:
                o = obj(**raw)

            except TypeError as e:

                k = re.findall(r'argument \'(\w+)\'',e.args[0])[0]
                v = raw.pop(k)
                o = obj(**raw)
                setattr(o, k, v)

            yield o

    def run(self,*columns,by='',func='', Obj='',cmd='',**condition):
        if not cmd:
            cols = ','.join(columns)
            if func:
                cols = func + "(%s)" %cols
            
            if hasattr(Obj, '__name__'):
                table = Obj.__name__
            else:
                table = Obj

            cons = []

            for k,v in condition.items():
                if  not isinstance(v, str):
                    cons.append("{k}={v}".format(k=k,v=v))
                else:
                    cons.append("{k}=\"{v}\"".format(k=k,v=v))

            C = ' and '.join(cons)
            

            extend = ' '
            if by:
                extend = ' group by %s' % by

            cmd = "SELECT {one} FROM {table} WHERE {condition} {extend};".format(one=cols, table=table, condition=C, extend=extend)
            if not C:
                cmd = cmd.replace('WHERE', '', 1)
        try:
            res = self._con.execute(cmd)
            if self.tp =='mysql':
                res = self._con
            for i,v in enumerate(res.fetchall()):
                yield v
        except sqlite3.OperationalError as e:
            logging.error(cmd)
            logging.error(e)

    def add_columns(self, Obj, **new_default_dcit):
        """
        self.add_columns(SomeDb, new_colun="default_value", new_col2=0)
        """
        objs = self.query(Obj)
        for i in objs:
            for k in new_default_dcit:
                setattr(i, k, new_default_dcit[k])
        c = Cache(self._db_path + ".bak") 
        c.save_all(*objs)
        self.drop(Obj)
        self.save_all(*objs)

    # def execute(self,*args, **kargs): return self._con.execute(*args, **kargs)
    def query_one(self,obj, eq="=", m='or', timestamp=None,**kargs):
        ss = self.query(obj,eq=eq, m=m, timestamp=timestamp,**kargs)

        
        try:
            # print('fuck bi')
            for i in ss:
                # print('fuck you', i)
                return i

        except TypeError as e:
            # raise e
            pass
            
                

    def export_to_es(self, obj, host="http://localhost:9200"):
        """
        export data to elasticsearch :
            @Index you need to special: like database
            @type will use Obje(table)'s name
        """
        index = obj.__name__.lower()
        res = requests.put(host + "/" + index).json()
        if 'error' in res:
            # res['error']['root_cause']
            pass
            # logging.error(str(res))
            # return res
        type_name = obj.__name__
        endpoint = host + "/_bulk"
        res = ""
        c = 0
        print(type(obj), obj.__name__)
        for i,o in enumerate(self.query(obj)):
            # print(o)

            v = json.dumps(o.get_dict())
            res += '{ "index" : { "_index" : "%s", "_type" : "%s", "_id" : "%s" } }\n' % (index, type_name, o.id)
            res += v +"\n"
            c += 1
            print("[merge : %d]" % c, end='\r')
            if i % 50000 == 0:
                requests.post(endpoint, data=res, headers={'content-type':'application/json;charset=UTF-8'}).json()
                res = ""
        # print(res)
        logging.info("prepare finish {c} , data -> {endpoint}".format(c=c,endpoint=endpoint))
        return requests.post(endpoint, data=res, headers={'content-type':'application/json;charset=UTF-8'}).json()

    def export_to_es_all(self, host="http://localhost:9200"):
        # with ThreadPoolExecutor(10) as exe:
        if 1:
            tables = self.ls()
            for table in tables:
                ObjCls = type(table, (dbobj,), {})
                res = self.export_to_es(ObjCls, host=host)
                print(res)
                # exe.submit(self.export_to_es, ObjCls, host=host)



    # def query_to_do(self, Obj, function,eq="=", m='or', timestamp=None,**kargs)

    def remove(self, Obj, **kargs):
        if not kargs:
            self.drop(Obj)
        else:
            _REMOVE_STR = "DELETE FROM " + Obj.__name__ + " WHERE "
            
            for k in kargs:
                _REMOVE_STR += "%s=? and " % k
            _delete_str = _REMOVE_STR[:-4] + ";"         
            vals = kargs.values()
            try:
                self._con.execute(_delete_str, list(vals))
                self._con.commit()
            except sqlite3.OperationalError as e:
                logging.error(_delete_str)
            except Exception as e:
                logging.error("{} {} line:806".format(_delete_str, vals))


    def delete(self, obj):
        __dict = obj.__dict__
        __DELETE_STR = "DELETE FROM " + obj.__class__.__name__ + " WHERE "
        for k in __dict:
            __DELETE_STR += "%s=? and " % k
        _delete_str = __DELETE_STR[:-4] + ";"
        vals = __dict.values()
        try:
            self._con.execute(_delete_str, list(vals))
            self._con.commit()
        except sqlite3.OperationalError as e:
            logging.error("{} line:820".format(_delete_str))
            raise e
        except Exception as e:
            logging.error("{} {} line:823".format(_delete_str, vals))


    def drop(self, Obj):

        _DROP_STR = "DROP TABLE %s ;" % Obj.__name__
        try:
            self._con.execute(_DROP_STR)
            self._con.commit()
        except sqlite3.OperationalError as e:
            logging.error(_DROP_STR)
            raise e
        except Exception as e:
            logging.error(e)


    def backup(self):
        backup = self._db_path + '.bak'
        self.export_xlsx(backup)

    def __del__(self):
        self._con.close()

    def save_all(self, *Obj, check_same=False, merge_index=None):
        
        now_tables = self.ls()
        
        def _merge_test(obj, merge_index):
            if not merge_index:
                return True
            if hasattr(obj, merge_index):
                v = getattr(obj,merge_index)
                if self.query_one(obj.__class__, **{merge_index: v}):
                    return False
                return True
            return True

        create_strs = set()
        insert_strs = {}

        # first obj for create table
        
        # Obj[0].save(self)
        
        for obj in Obj:
            if not _merge_test(obj, merge_index): continue
            if not obj.__class__.__name__ in now_tables:
                # print("----")
                res = obj.save(self)
                # print("++++")
                # print(res)
                now_tables.append(obj.__class__.__name__)
                continue

            create_str, insert_str, values = obj.save(tp=self.tp)
            create_strs.add(create_str)

            if insert_str not in insert_strs:
                insert_strs[insert_str] = [tuple(values)]
            else:
                insert_strs[insert_str].append(tuple(values))
        # print("|||gen Obj")

        for insert_template in insert_strs:
            if not insert_template:continue
            try:
                
                v = insert_strs[insert_template]
                if self.tp == 'mysql':
                    insert_template = insert_template.replace("?", "%s")

                self._con.executemany(insert_template, v)
                self._con.commit()
                # logging.info("insert many -> " + insert_template)
            except Exception as e:
                # print(k,v, e)
                if 'duplicate column name: id' in str(e):

                    v = insert_strs[insert_template]
                    k = insert_template.replace("id,","",1)
                    print(k,v)

                logging.error(e)

    def fuzzy_search(self, Obj, key, printer=print):
        for i in self.query(Obj):
            v,r = i.search(key)
            if r:
                printer(v)
                yield r

    def ls(self):
        if self.tp != 'mysql':
            con = self._con.execute('select name from sqlite_master;')
        else:
            self._con.execute('select table_name from information_schema.tables where table_schema="%s";' % self._db_path)
            con = self._con
        return [i[0] for i in con.fetchall()]

    def from_xlsx(self, xlsx):
        for row_objs in open_xlsx(xlsx):
            self.save_all(*row_objs)

    def from_csv(self, tablename, csv):
        csv_to_sql(tablename, csv)

    # def export_csv(self, csv):
        # with open(csv, 'w', newline='') as csvfile:
            # spamwriter = csv.writer(csvfile, delimiter=' ',
                            # quotechar='|', quoting=csv.QUOTE_MINIMAL)
            # tables = self.ls()
    def export_xlsx(self, outputpath):
        tables = self.ls()
        workbook = xlwt.Workbook()
        for table in tables:
            ObjCls = type(table, (dbobj,), {})
            objs = list(self.query(ObjCls))

            obj = objs[0]
            results = objs

            fields = list(obj.get_fields())
            table_name = obj.__class__.__name__
            
            sheet = workbook.add_sheet(table_name , cell_overwrite_ok=True)
            
            for field in range(0,len(fields)):
                sheet.write(0,field,fields[field])

            
            row = 1
            col = 0
            for row in range(1, len(results)+1):
                for col,name in enumerate(fields):
                    sheet.write(row,col,u'{}'.format(getattr(results[row-1], name)))

        workbook.save(outputpath)

    @classmethod
    def export_to_es_from_db_file(cls, file, host='http://localhost:9200'):
        c = cls(file)
        c.export_to_es_all(host=host)

    @classmethod
    def load_xlsx(cls, xls_files):
        with NamedTemporaryFile('w+t') as f:
            xlsx_to_sql(xls_files, f.name)
            cl = cls(f.name)
            return cl

    # def from_csv(self, csv_file, create_db_file=None):
    #     if not create_db_file:
    #         with NamedTemporaryFile('w+t') as f:
    #             self._con = getattr(self._driver, 'connect')(f.name)

    #             csv_to_sql(Obj, csv_file)


@contextlib.contextmanager 
def connect(database):
    try:
        c = Cache(database)
        yield c
    finally:
        del c

def merges(Obj,*files, new_file='new.db'):
    c_new = Cache(new_file)
    ws = []
    for db_f in files:
        c = Cache(db_f)
        for u in c.query(Obj):
            ws.append(u)

    c_new.save_all(*ws)
    return True






def export_xlsx( objs, outputpath):
    
    

    obj = objs[0]
    results = objs

    
    fields = list(obj.get_fields())
    table_name = obj.__class__.__name__
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet('table_' + table_name , cell_overwrite_ok=True)

    
    for field in range(0,len(fields)):
        sheet.write(0,field,fields[field])

    
    row = 1
    col = 0
    for row in range(1, len(results)+1):
        for col,name in enumerate(fields):
            sheet.write(row,col,u'{}'.format(getattr(results[row-1], name)))

    workbook.save(outputpath)



def csv_to_sql(Obj,csv_file, *fields, cache=None):

    cs = []
    with open(csv_file) as csv_fp:
        for l in csv_fp:
            l = l.strip()
            if l:
                cs.append(Obj(**dict(zip(fields, l.split(",")))))
    if not cache:
        c = Cache(csv_file+".db")
    else:
        c = cache
    c.save_all(*cs)
    return True


def json_to_sql(table_name, json_file,map=None, handler=None,cache=None, **options):
    cs = []
    Obj = type(table_name, (dbobj,), {})
    if not handler:
        with open(json_file, 'rb') as fp:
            lines = fp.readlines()
            for l in tqdm.tqdm(lines):
                o = json.loads(l)
                if map:
                    o = map(o)
                cs.append(Obj.from_json(o))
    else:
        jh = handler(json_file)
        jh.cmdloop()
        for l in tqdm.tqdm(jh.tree.Out):
            cs.append(Obj.from_json(l))
    if cache and isinstance(cache, Cache):
        cache.save_all(*cs)
    elif isinstance(cache, str):
        d = os.path.dirname(cache)
        if os.path.exists(d):
            c = Cache(cache, **options)
            c.save_all(*cs)
    return cs


def open_xlsx(file, granularity=20):
    sheets = xlrd.open_workbook(file)
    for table in  sheets.sheets():
        table_name = table.name
        rows = table.nrows
        cols = table.ncols
        fields = [str(i.value) for i in table.row(0)]
        Obj = type(table_name, (dbobj,), {})
        logging.info(Obj.__name__)
        all_sql_objs = []
        for i in range(1, rows):

            o = Obj(**dict(zip(fields, [i.value for i in table.row(i)])))
            
            all_sql_objs.append(o)
            if len(all_sql_objs) >= granularity:
                yield all_sql_objs
                all_sql_objs = []

        if len(all_sql_objs) != 0:
            yield all_sql_objs

def mysql_to_sqlite(file, database='test4',user='root', password=''):
    my_con = Cache(database, user=user, password=password, tp='mysql')
    c = Cache(file)
    for table in my_con.ls():
        # print(table, '->', 'sqlite3')
        # print(' -------------------------------------------------')
        Obj = type(table, (dbobj,), {})

        datas = [i for i in my_con.query(Obj)]
        if datas:
            c.save_all(*datas)
        print(table, '->', 'sqlite3', 'finish : %d' % len(datas))
        # print(' ---------------- end ----------------------------')


def sqlite_to_mysql(file, database='test4',user='root', password=''):
    c = Cache(database, user=user, password=password, tp='mysql')
    my_con = Cache(file)
    for table in my_con.ls():
        if table == 'sqlite_master':continue
        Obj = type(table, (dbobj,), {})

        datas = [i for i in my_con.query(Obj)]
        if datas:
            c.save_all(*datas)
        print(table, '->', 'mysql', 'finish : %d' % len(datas))



def xlsx_to_sql(excel_file, database):
    with  connect(database) as c:
        for row_objs in  open_xlsx(excel_file):
            c.save_all(*row_objs)
