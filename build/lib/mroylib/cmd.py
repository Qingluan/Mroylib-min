from argparse import ArgumentParser
from qlib.log import log
from qlib.data import Cache,mysql_to_sqlite,sqlite_to_mysql
import logging
from termcolor import colored
import chardet, os, sys



parser = ArgumentParser()

parser.add_argument("file_or_obj", nargs='*', help=" file or some object to handle\n\t can import excel -> elasticsearch\n\t sqlite -> elasticsearch\n\t mysql -> sqlite \n\t file.sql[mysql] / file.db[sqlite] /excel.xlsx[excel] -> elasticsearch")
parser.add_argument("-e","--encoding"  , default=None, help="change encoding: utf , gbk, ...")
parser.add_argument("--to-elasticsearch", action='store_true',default=False, help="import sql files to elastic search ...")
parser.add_argument("--to-sqlite", action='store_true',default=False,  help="import mysql files to sqlite ...")
parser.add_argument("--to-mysql", action='store_true',default=False,  help="import sqlite files to mysql  ...")
parser.add_argument("--url", default='http://localhost:9200', help="elastic search url: default-> http://localhost:9200")
parser.add_argument("--database", default='test4', help="set database")
parser.add_argument("--user", default='root', help="set user")
parser.add_argument("--passwd", default='', help="set password")



@log(logging.INFO)
def main():
    
    args = parser.parse_args()
    e = None
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
        for f in args.file_or_obj:
            mysql_to_sqlite(f, database=args.database, user=args.user, password=args.passwd)
    elif args.to_mysql:
        for f in args.file_or_obj:
            sqlite_to_mysql(f, database=args.database, user=args.user, password=args.passwd)
