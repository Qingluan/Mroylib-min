from argparse import ArgumentParser
from qlib.log import log
from qlib.data import Cache,mysql_to_sqlite,sqlite_to_mysql, json_to_sql
from mroylib.tools.transform import JsonTreeListHandleCmd
from mongoexe.mon import Mon
import logging
from termcolor import colored
import chardet, os, sys
import json
import asyncio


parser = ArgumentParser()

parser.add_argument("file_or_obj", nargs='*', help=" file or some object to handle\n\t can import excel -> elasticsearch\n\t sqlite -> elasticsearch\n\t mysql -> sqlite \n\t file.sql[mysql] / file.db[sqlite] /excel.xlsx[excel] -> elasticsearch")
parser.add_argument("-e","--encoding"  , default=None, help="change encoding: utf , gbk, ...")
parser.add_argument("--to-elasticsearch", action='store_true',default=False, help="import sql files to elastic search ...")
parser.add_argument("--to-sqlite", action='store_true',default=False,  help="files to sqlite ...")
parser.add_argument("--to-mysql", action='store_true',default=False,  help="files to mysql  ...")
parser.add_argument('-bm', "--backup-mongo", action='store_true',default=False,  help="mode to backup mongo. exm: -bm -H remote.ip -P port -p pass -bH backup.ip -bP backup.port -bp backup.pass")
parser.add_argument("-f","--from-type", default=None, help="set from type : mysql, sqlite, json | default: auto-test" )
parser.add_argument("--url", default='http://localhost:9200', help="elastic search url: default-> http://localhost:9200")
parser.add_argument("-bH","--backup-host", default='localhost', help="set backup host default-> localhost")
parser.add_argument("-bP","--backup-port", default=27017,type=int, help="set port")
parser.add_argument("-bp","--backup-passwd", default='', help="set password")
parser.add_argument("-H","--host", default='localhost', help="set host default-> localhost")
parser.add_argument("-P","--port", default=27017,type=int, help="set port")
parser.add_argument("-D","--database", default='test4', help="set database")
parser.add_argument("--user", default='root', help="set user")
parser.add_argument("-p","--passwd", default='', help="set password")
parser.add_argument("-T", "--table-name", default='Host', help="set tablename, default: Host")
parser.add_argument("--from-json-file", default=None, help="from json file , use for -bm")
parser.add_argument("-a","--async-io", action='store_true',default=False,  help="use asyncio mode")

# parser.add_argument("--to-file", default='/tmp/output', help="set export file, use for --to-sqlite default: /tmp/output ")


def check_file_tp(name):
    tp = name.split(".")[-1].strip()
    print("use : ", tp)
    if '.' not in name:
        return 'txt'
    else:
        return {
            'json':'json',
            'xlsx':'xlsx',
            'excel':'xlsx',
            'db':'sqlite',
            'sql':'mysql',
        }.get(tp,'mysql')


@log(logging.INFO)
def main():
    
    args = parser.parse_args()
    e = None
    name_tps = {n: check_file_tp(n) for n in args.file_or_obj}
    if args.file_or_obj:
        _f = args.file_or_obj[0]
        args.from_type = name_tps.get(_f,'mysql')
        print("from-type:", args.from_type)
    for f in args.file_or_obj:
        if os.path.exists(f):
            os.system("file " + f)
            with open(f, "rb") as fp:
                ws = fp.read(2000)
                e = chardet.detect(ws)
                logging.info(str(e))
            # logging.info("wbt")
    if args.encoding:
                        
                
        if not ws or not e:
            logging.error("not load success")
            return 

        with open(args.file_or_obj + ".bak", 'wb') as fp2:
            fp2.write(ws.decode(e['encoding']).encode(args.encoding))
            
        logging.info(e['encoding'] + ' -> ' + args.encoding )

    if args.to_elasticsearch:
        for f in args.file_or_obj:
            if f.endswith(".xlsx"):
                ca = Cache.load_xlsx(f)
                ca.export_to_es_all(args.url)
            elif f.endswith(".db"):
                Cache.export_to_es_from_db_file(f, args.url)
            elif f.endswith(".sql"):
                if not args.passwd:
                    p = ""
                else:
                    p = "-p{}".format(args.passwd)
                try:
                    os.popen("echo create database {} | mysql -u{} {} ".format(args.database,args.user,p)).read()
                except:
                    pass
                finally:
                    os.popen("mysql -u{} {} {} < {}".format(args.user,p,args.database, f)).read()
                    print(colored(" data --->  mysql", 'green'))
                    ca = Cache(args.database, user=args.user, password=args.passwd, tp='mysql')
                    print(colored(" data --->  mysql --> elasticsearch", 'green'))
                    ca.export_to_es_all(args.url)

            print("{} -> {}".format(f, args.url))
        sys.exit(0)
    elif args.to_sqlite:
        if args.from_type == "json":
            from_json, to_sql = args.file_or_obj
            json_to_sql(args.table_name, from_json, cache=to_sql,handler=JsonTreeListHandleCmd)
        elif args.from_type == 'mysql':
            for f in args.file_or_obj:
                mysql_to_sqlite(f, database=args.database, user=args.user, password=args.passwd)
        
            
    elif args.to_mysql:
        for f in args.file_or_obj:
            if args.from_type == 'sqlite':
                sqlite_to_mysql(f, database=args.database, user=args.user, password=args.passwd)
            elif args.from_type == "json":
                pass
    

    if args.backup_mongo:
        if args.async_io:
            if args.from_json_file and os.path.exists(args.from_json_file):
                with open(args.from_json_file) as fp:
                    all_hosts = [tuple(json.loads(i).values()) for i in fp.readlines()]
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(Mon.async_backup_to_another_hosts(*all_hosts, back_host=args.backup_host, back_port=args.backup_port))
            else:
                m = Mon(host=args.host, port=args.port, if_async=args.async_io)
                loop = asyncio.get_event_loop()
                loop.run_until_complete(m.async_backup_to_another_host(host=args.backup_host, port=args.backup_port))

        else:
            m.backup_to_another_host(host=args.backup_host, port=args.backup_port)
