import time, logging
import datetime
from hashlib import sha256
from functools import partial


def _hash(obj):
    return sha256(obj.encode('utf8')).hexdigest()



class SqlEngine:


    DEFAULT_TABLE = """CREATE TABLE %s (
        ID INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT ,
        CreatedTime TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP, 
        {extends} );"""

    def __init__(self, database=None, type='sqlite' , **connect_options):
        self.Type = type
        self.database = database
        engine = None
        if not database and type == 'sqlite':
            type = 'mysql'
            self.Type = 'mysql'
            database = 'test'

        try:
            if type == 'sqlite':
                self.DEFAULT_TABLE = self.DEFAULT_TABLE.replace('AUTO_INCREMENT', '')
                import sqlite3
                engine = sqlite3
            elif type == 'mysql':
                import pymysql
                engine = pymysql
            elif type == 'postgresql':
                self.DEFAULT_TABLE = self.DEFAULT_TABLE.replace('ID INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT', 'ID SERIAL PRIMARY KEY')
                import psycopg2
                engine = psycopg2
            else:
                raise Exception("un supported sql: %s" % type)

        except ImportError as e:
            print(e)
        if self.Type == 'sqlite':
            self.con = engine.connect(database, **connect_options)
        elif self.Type == 'mysql':
            self.con = engine.connect(database=database, charset='utf8',**connect_options)

        else:

            self.con = engine.connect(database=database, **connect_options)
        self.cu = self.con.cursor()

    def datetime(timeobj):
        if isinstance(timeobj, datetime.datetime):
            return timeobj
        elif isinstance(timeobj, str):
            ss = time.strptime(timeobj, '%Y-%m-%d %H:%M:%S')
            return datetime.datetime.fromtimestamp(time.mktime(ss))


    def create_table(self, table, *extends_columns):
        columns = ',\n\t'.join(extends_columns)
        cmd = self.DEFAULT_TABLE.format(extends=columns) % table
        self.run_cmd(cmd)
        return cmd

    def create(self, table, extend_options={}, **kargs):
        """
        this function can create table smarter:
        example:
            create('User', name=str, login_time=time, passwd=hash, own=1, number=int, id_f=Table)
            will ->  CREATE TABLE User (
                            ID INTEGER PRIMARY KEY NOT NULL [AUTO_INCREMENT] ,
                            CreatedTime TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            name TEXT NOT NULL,
                            login_time TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            passwd text, # hash(str),
                            own INTEGER NOT NULL DEFAULT 1 ,
                            number INTEGER,
                            id_f FOREIGN KEY REFERENCES Table(ID)
            ) 
        """

        columns = {}
        names = []
        foreign_keys = []
        for k in kargs:
            names.append(k)
            v = kargs[k]
            if v is int:
                columns[k] = '%s %s' % (k, 'INTEGER')
            elif v is float:
                columns[k] = '%s %s' % (k, 'FLOAT')
            elif v is str:
                columns[k] = '%s %s' % (k, 'TEXT')
            elif v is time:
                columns[k] = '%s %s' % (k, 'TimeStamp DEFAULT CURRENT_TIMESTAMP')
            elif isinstance(v, int):
                columns[k] = '%s %s' % (k, 'INTEGER NOT NULL DEFAULT %d' % v)
            elif isinstance(v, str):
                columns[k] = '%s %s' % (k, 'VARCHAR(255) NOT NULL DEFAULT \'%s\'' % v)
            
            elif hasattr(v, '_table'):
                columns[k] = '%s %s' % (k, 'INTEGER' )
                foreign_keys.append(' FOREIGN KEY (%s) REFERENCES %s(ID) ' % (k, v.__name__))
            else:
                raise TypeError("not supported Sql Type %s" % str(v))

        # extend some attribute
        for k in extend_options:
            columns[k] += extend_options[k]
        cols = list(columns.values()) + foreign_keys
        return self.create_table(table, *cols)
        

    def drop_table(self, table):
        cmd = 'DROP TABLE %s;' % table
        return self.run_cmd(cmd)

    def close(self):
        try:
            self.cu.close()
            self.con.close()
        except Exception:
            pass
        finally:
            pass

    def __del__(self):
        self.close()

    def _sql(self, k, v):
        if isinstance(v, int):
            return '{}={}'.format(k, v)
        elif isinstance(v, float):
            return '{}={}'.format(k, v)
        elif isinstance(v, str):
            if '"' in v:
                return '{}=\'{}\''.format(k, v)
            elif "'" in v:
                return '{}=\"{}\"'.format(k, v)
            return '{}=\"{}\"'.format(k, v)

        elif isinstance(v, datetime.datetime):
            return '{}=\'{}\''.format(k, v)
        elif hasattr(v, '_table'):
            if hasattr(v, "ID"):
                return '{}={}'.format(k, v.ID)
            return 'NULL'
        else:
            print(k, v)
            raise Exception("what type?")

    def select(self, table, *fields, tail=' ', sum_mode=False, **condition):
        if len(fields) == 0:
            fields = ("*", )
        if sum_mode:
            cmd = "select count({}) from %s ".format(",".join(fields)) % table 
        else:
            cmd = "select {} from %s ".format(",".join(fields)) % table 

        if condition:
            cmd += 'where ' +  ' and '.join([self._sql(*i) for i in condition.items()])
        # print(cmd)
        cmd = cmd + tail + ';'
        try:
            self.cu.execute(cmd)
            for row in self.cu.fetchall():
                yield row
        except Exception as e:
            if hasattr(self.con, 'rollback'):
                self.con.rollback()
            print(cmd)
            raise e


    def _like(self, k ,v):
        if v[0] == ">" :
            return '{} > {}'.format(k, v)
        elif v[0] == "<" :
            return '{} < {}'.format(k, v)
        elif v[0] == "=" :
            return '{} = {}'.format(k, v)
        else:
            return '{} like \"%{}%\"'.format(k, v)


    def search(self, table, *fields, tail=' ', **condition):
        if len(fields) == 0:
            fields = ("*", )

        cmd = "select {} from %s ".format(",".join(fields)) % table 
        if condition:
            cmd += 'where ' +  ' '.join([self._like(*i) for i in condition.items()])
        # print(cmd)
        cmd = cmd + tail + ';'
        try:
            self.cu.execute(cmd)
            for row in self.cu.fetchall():
                yield row
        except Exception as e:
            if hasattr(self.con, 'rollback'):
                self.con.rollback()
            print(cmd)
            raise e



    def first(self, table, *fields,sum_mode=False, **condition):
        if len(fields) == 0:
            fields = ("*", )
        for i in self.select(table, *fields, tail=' limit 1 ', sum_mode=sum_mode, **condition):
            return i

    def last(self, table, *fields, sum_mode=False, **condition):
        if len(fields) == 0:
            fields = ("*", )
        for i in self.select(table, *fields, tail=' order by  ID desc limit 1 ', sum_mode=sum_mode, **condition):
            return i

    def check_table(self, table):
        """
        """
        if self.Type == 'sqlite':
            for name in self.select("sqlite_master", 'name',type='table'):
                if name[0] == table:
                    sql = self.first("sqlite_master", 'sql', name=table)[0]
                    _pr = sql.find('(') + 1
                    _tr = sql.rfind(')')
                    return tuple((tuple(i.split()) for i in sql[_pr:_tr].split(",")))
            return False
        elif self.Type == 'postgresql':
            res = tuple(self.select("information_schema.columns",
                'column_name',
                'data_type',
                'column_default',
                table_name=table))
            if res:
                return res
            else:
                return False
        else:
            try:
                self.run_cmd("desc " + table)
                return self.cu.fetchall()
            except Exception as e:
                if e.args[0] == 1146:
                    return False
                else:
                    raise e

    def table_list(self):
        if self.Type == "postgresql":
            return tuple(self.select('information_schema.tables', 'table_name', table_schema='public'))
        elif self.Type == 'sqlite':
            return tuple(self.select('sqlite_master', 'name'))
        elif self.Type == 'mysql':
            return tuple(self.select('information_schema.tables', 'table_name', table_schema=self.database))
        else:
            raise Exception("not supported type")

    def _sqls(self, i):
        if isinstance(i, int):
            return str(i)
        elif isinstance(i, str):
            return i
        elif isinstance(i, float):
            return i
        elif hasattr(i, '_table'):
            if hasattr(i, "ID"):
                return str(i.ID)
            return 'NULL'
        elif i is time:
            return datetime.datetime.now()
        elif isinstance(i, datetime.datetime):
            return i
        elif isinstance(i, bytes):
            return i.decode("utf8")
        else:
            print(i)
            raise Exception("error insert", i)

    def insert_all(self, table, insert_columns, datas, check=False):
        sept = '?' if self.Type == 'sqlite' else '%s'
        values_tmp = ','.join([sept for i in range(len(datas[0]))])
        cmd = (r'insert into %s ({order}) values ({values});' % table).format(order=','.join(insert_columns), values=values_tmp) 
        if check:
            D = []
            for values in datas:
                D.append([ self._sqls(i) for i in values ])
                # print(cmd, values)
            self.cu.executemany(cmd, D)
            self.con.commit()
        else:
            self.cu.executemany(cmd, datas)
            self.con.commit()

    def insert(self, table, insert_columns, *values, **kargs):
        values = [ self._sqls(i) for i in values ]
        sept = '?' if self.Type == 'sqlite' else '%s'
        # sept = '%s'
        values_tmp = ','.join([sept for i in range(len(values))])
        cmd = (r'insert into %s ({order}) values ({values});' % table).format(order=','.join(insert_columns), values=values_tmp) 
        # print(cmd, values)
        
        self.run_cmd(cmd, *values)
        

    def update(self, table, sets, **condition):
        sept = '?' if self.Type == 'sqlite' else '%s'

        cond = ' '.join([self._sql(*i) for i in condition.items()])
        updated = ','.join([self._sql(*i) for i in sets.items()])
        cmd = '''UPDATE {table}  SET {updated}
        WHERE {condition} ;
        '''.format(table=table, updated=updated, condition=cond)

        # print(cmd)
        return self.run_cmd(cmd)

    def delete(self, table, **condition):
        cond = ''
        if condition:
            cond = 'WHERE ' + ' '.join([self._sql(*i) for i in condition.items()])
        cmd = '''DELETE FROM {table}
        {condition} ;
        '''.format(table=table, condition=cond)

        return self.run_cmd(cmd)

    def alter(self, table, **columns):
        """
        @table: table is table name
        @**columns: this like 'create' funciton. when you add a new 'k=v' will add to table column k,
                    but if v is 'None' the table will remove column from table.
            example:

                In [14]: sql.check_table("tt2")
                Out[14]: 
                 (('ID', 'INTEGER', 'PRIMARY', 'KEY', 'NOT', 'NULL'),
                 ('CreatedTime', 'TimeStamp', 'NOT', 'NULL', 'DEFAULT', 'CURRENT_TIMESTAMP'),
                 ('nn', 'VARCHAR(255)', 'NOT', 'NULL', 'DEFAULT', '"default', 'text"'))

                In [15]: sql.alter("tt2", nn=None)
                Out[15]: True

                Out[16]: 
                 (('ID', 'INTEGER', 'PRIMARY', 'KEY', 'NOT', 'NULL'),
                  ('CreatedTime', 'TimeStamp', 'NOT', 'NULL', 'DEFAULT', 'CURRENT_TIMESTAMP'))
                
                In [17]: sql.alter("tt2", nn='abc')
                Out[17]: True

                In [18]: sql.check_table("tt2")
                Out[18]: 
                (('ID', 'INTEGER', 'PRIMARY', 'KEY', 'NOT', 'NULL'),
                 ('CreatedTime', 'TimeStamp', 'NOT', 'NULL', 'DEFAULT', 'CURRENT_TIMESTAMP'),
                 ('nn', 'VARCHAR(255)', 'NOT', 'NULL', 'DEFAULT', '"abc"'))

                In [8]: sql.alter("tt2", nn=None, name='name', passwd='secret', content=str, 
                   ...: ftime=time)
                Out[8]: True

                In [9]: sql.check_table("tt2")
                Out[9]: 
                (('ID', 'INTEGER', 'PRIMARY', 'KEY', 'NOT', 'NULL'),
                 ('CreatedTime', 'TimeStamp', 'NOT', 'NULL', 'DEFAULT', 'CURRENT_TIMESTAMP'),
                 ('passwd', 'VARCHAR(255)', 'NOT', 'NULL', 'DEFAULT', '"secret"'),
                 ('ftime', 'TimeStamp', 'DEFAULT', 'NNULL'),
                 ('name', 'VARCHAR(255)', 'NOT', 'NULL', 'DEFAULT', '"name"'),
                 ('content', 'TEXT', 'DEFAULT', 'NULL'))



        """
        first_word = lambda x: x.strip().split()[0]
        names = []
        cmd = ''
        table_structure = self.check_table(table)
        sqlitedrop = False

        old_columns  = [i[0] for i in table_structure]
        drop_columns = set()
        if not table_structure:
            raise Exception("no such table")

        for k in columns:
            cmd = ''
            dtype = ''
            v = columns[k]
            if v is int:
                dtype = 'INTEGER DEFAULT NULL'
            elif v is str:
                dtype = 'TEXT DEFAULT NULL'
            elif v is time:
                dtype = 'TimeStamp DEFAULT NULL' if self.Type == 'sqlite' else 'TimeStamp DEFAULT CURRENT_TIMESTAMP'
            elif isinstance(v, int):
                dtype = 'INTEGER NOT NULL DEFAULT %d' % v
            elif isinstance(v, str):
                dtype = 'VARCHAR(255) NOT NULL DEFAULT \'%s\'' % v
            elif hasattr(v, '_table'):
                dtype = 'INTEGER, FOREIGN KEY (%s) REFERENCES %s(ID)' % (k, v.__name__)
            elif v is None:
                pass
            else:
                raise TypeError("not supported Sql Type %s" % str(v))
        
            operator = 'ADD'
            if k in old_columns:
                operator = 'ALTER COLUMN'

            if v is None:
                operator = 'DROP COLUMN'
            
            if self.Type == 'sqlite' and  v == None:
                # just for sqlite
                drop_columns.add(k)
                old_columns.remove(k)
                sqlitedrop = True
            else:
                cmd = '''ALTER  TABLE {table}
            {operator} {column} {dtype}\n\t;
                            
            '''.format(table=table, operator=operator, column=k, dtype=dtype)
            
            if cmd:
                self.run_cmds(cmd)
            
        
        if sqlitedrop:
            create_sql = self.first('sqlite_master', 'sql', name=table)[0].split(",")
            last_change = False
            last = first_word(create_sql[-1])
            new_create = ''
            delete = set()
            for col in create_sql:
                for m in drop_columns:
                    if m == first_word(col):
                        delete.add(col)
                        if m == last:
                            last_change = True
                        break

            for i in delete:
                create_sql.remove(i)

            cmd = '''ALTER TABLE {table} RENAME TO change_table_tmp_name_this_table_could_not_be_create_by_another_way;'''.format(table=table)
            self.run_cmds(cmd)
            cmd = ','.join(create_sql) if not last_change else ','.join(create_sql) + ");"
            self.run_cmds(cmd)
            cmd = 'INSERT INTO {table} ({values}) SELECT {values} FROM change_table_tmp_name_this_table_could_not_be_create_by_another_way;'.format(table=table, values=','.join(old_columns))
            self.run_cmds(cmd)
            cmd = '''DROP TABLE change_table_tmp_name_this_table_could_not_be_create_by_another_way;'''
            self.run_cmds(cmd)
        
        return True

    def run_cmd(self, cmd, *values):
        try:
            if values:
                self.cu.execute(cmd, values)
            else:
                self.cu.execute(cmd)
        except Exception as e:
            if hasattr(self.con, 'rollback'):
                self.con.rollback()
            logging.debug(e)
            logging.debug(values)
            logging.debug(cmd)
            raise e

        return self.con.commit()

    def run_cmds(self, cmd):
        try:
            self.cu.execute(cmd)
        except Exception as e:
            if hasattr(self.con, 'rollback'):
                self.con.rollback()
            logging.debug(e)
            logging.debug(cmd)
            raise e

        return self.con.commit()
    # def gen_table(self, *columns):
        # create_cmd = 'create '

class ForeignObjectIdNotFoundErr(Exception):
    pass

class Table:
    """
    this is sql Table trans to class
    need set_handle(objhandle)
    """
    _table = 0
    _obj_handler = None
    _obj_instances = {}

    def __init__(self, handle=None, **kargs):
        # setattr(self.__class__, '_table', self.__class__.__name__)
        self.ID = None
        self.CreatedTime = None
        self._update = {}

        self._table = self.__class__.__name__
        # for k in self._columns():
        #     if k in kargs:
        #         setattr(self, k, kargs[k])
        #     else:
        #         setattr(self, k, self.__class__.__dict__[k])
        if Table._obj_handler is None:
            if handle:
                Table._obj_handler = handle


        for k in kargs:
            if k in self._columns():
                setattr(self, k, kargs[k])
            elif k in ('ID', 'CreatedTime'):
                setattr(self, k, kargs[k])
            else:
                raise TypeError("table %s no such columns %s " %(self._table, k) )

        # for k in self._obj_columns():
        #     Obj = getattr(self.__class__, k)
        #     v = getattr(self, k)
        #     setattr(self, k, self._obj.find_one(Obj, ID=v))

    def __repr__(self):
        return self.__class__.__name__

    @classmethod
    def _set_handle(cls, handler):
        cls._obj_handler = handler

    @classmethod
    def _columns(cls):
        return [i for i in cls.__dict__ if not i.startswith('__')]

    @classmethod
    def _obj_columns(cls):
        return [i for i in cls._columns() if hasattr(getattr(cls, i), '_table') ]

    def _dict(self):
        return {i:self[i] for i in self.__class__.__dict__ if not i.startswith('__')}

    @classmethod
    def _CREATE_ITEMS(cls):
        return {i:cls.__dict__[i] for i in cls.__dict__ if not i.startswith('__')}

    def __getitem__(self, k):
        v = getattr(self, k)
        if hasattr(getattr(self.__class__, k), '_table'):
            if not getattr(v, 'ID'):
                raise ForeignObjectIdNotFoundErr("not found obj's ID , you need to got from db first.",v)
            self.__class__._obj_instances[k] = v.__class__
        #     vv = getattr(self.__class__, k)
        #     return self._obj_handler.find_one(vv, ID=v)
        return v

    def __setitem__(self, k, v):
        self._update[k] = v
        setattr(self, k, v)

    def _save(self):
        if self.ID is None:
            return self._obj_handler.add(self)
        return self._obj_handler.save(self)

    def _delete(self):
        return self._obj_handler.delete(self)

    @staticmethod
    def hash( obj):
        return _hash(obj)

    @classmethod
    def _all(cls):
        return list(cls._obj_handler.find(cls))



class SqlObjectEngine:

    def __init__(self, **connect_option):
        self.sql = SqlEngine(**connect_option)

    def create(self, Obj):
        columns = Obj._CREATE_ITEMS()
        self.sql.create(Obj.__name__, **columns)

    def add(self, Obj_self):
        keys = []
        values = []
        _d = Obj_self._dict()
        for i in _d:
            # jump id and createdTime
            if not _d[i]:
                continue
            keys.append(i)
            values.append(_d[i])

        self.sql.insert(Obj_self.__class__.__name__, keys, *values)

    def adds(self, *objs):
        for obj in objs:
            self.add(obj)


    def _find(self, obj, sum_mode=False, **condition):
        columns = ['ID','CreatedTime'] +obj._columns()
        selected = ','.join(columns)
        return self.sql.select(obj.__name__, selected, sum_mode=sum_mode, **condition)

    def find_one(self, obj, sum_mode=False, **condition):
        columns = ['ID','CreatedTime'] +obj._columns()
        tmp_dict = {}
        for row in self._find(obj, sum_mode=sum_mode, **condition):
            tmp_dict = dict(zip(columns, row))
        if not tmp_dict:
            return
        for k in tmp_dict:
            if k in obj.__dict__ and hasattr(obj.__dict__[k], '_table'):
                _instance = obj.__dict__[k]
                _instance_id = tmp_dict[k]
                tmp_dict[k] = self.find_one(_instance,ID=_instance_id)

        return obj(**tmp_dict)
            

    def find(self, obj, sum_mode=False, **condition):
        columns = ['ID','CreatedTime'] +obj._columns()
        for row in self._find(obj, sum_mode=sum_mode, **condition):
            yield obj(**dict(zip(columns, row)))

    def last(self, obj, **condition):
        columns = ['ID','CreatedTime'] +obj._columns()
        selected = ','.join(columns)
        l = self.sql.last(obj.__name__, selected, **condition)
        return obj(**dict(zip(columns, l)))

    def close(self):
        self.sql.close()

    def save(self, obj_self):
        columns = ['ID','CreatedTime'] +obj_self._columns()
        if obj_self.ID != None:
            self.sql.update(obj_self._table, obj_self._update, ID=obj_self.ID)
            obj_self._update = {}

    def delete(self, obj_self):
        self.sql.delete(obj_self._table, ID=obj_self.ID)

    def datetime(self, timeobj):
        if isinstance(timeobj, datetime.datetime):
            return timeobj
        elif isinstance(timeobj, str):
            ss = time.strptime(timeobj, '%Y-%m-%d %H:%M:%S')
            return datetime.datetime.fromtimestamp(time.mktime(ss))


