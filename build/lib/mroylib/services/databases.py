import os
from configparser import  ConfigParser
from qlib.data.sql import SqlEngine
from qlib.log import show

if not os.path.exists("/usr/local/etc/local_databases_set_config.conf"):

    with open("/usr/local/etc/local_databases_set_config.conf", "w") as fp:
        home = os.getenv("HOME")
        fp.write("""
[path]
databases_dir_path={HOME}/databases_dir/

            """.format(HOME=home))


config = ConfigParser()
config.read("/usr/local/etc/local_databases_set_config.conf")

DIR_ROOT_PATH = config.get("path","databases_dir_path")
if not os.path.exists(DIR_ROOT_PATH):
    os.mkdir(DIR_ROOT_PATH)

class DBServiceRegister:
    """
DBService only immplemented this can simply use db service.

class NewDB(DBServiceRegister):

    def init(self):
        self._table_name = 'abc'
        self._tables = {
            'user': str,
            'pub_time': time,
            'id': 12
        }
    """


    def __init__(self, name, **kargs):
        self.init()
        self.db_file = os.path.join(DIR_ROOT_PATH, name)
        if 'type' in kargs and kargs['type'] != "sqlite":
            self.sql = SqlEngine(database=name, **kargs)
        else:
            self.sql = SqlEngine(database=self.db_file, **kargs)

        if self._table_name not  in [i[0] for i in self.sql.table_list()]:
            try:
                self.sql.create(self._table_name, **self._tables)
            except:
                pass

    def info(self):
        for tt in self.sql.table_list():
            t = tt[0]
            show(self.sql.check_table(t))
            print()


    def init(self):
        raise NotImplementedError("must set 'self._tables' and 'self._table_name' ")


    def select(self, *args, **kargs):
        return self.sql.select(self._table_name, *args, **kargs)

    def search(self, *args, **kargs):
        return self.sql.search(self._table_name, *args, **kargs)

    def insert(self, *args, **kargs):
        return self.sql.insert(self._table_name, *args, **kargs)

    def delete(self, *args, **kargs):
        return self.sql.delete(self._table_name, *args, **kargs)

    def __del__(self):
        self.sql.close()