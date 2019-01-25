from concurrent.futures.thread import  ThreadPoolExecutor
from termcolor import colored
from functools import partial

import importlib
import base64
import pickle
import json
import os
import sys
import zipfile
import logging
import io
import re
from hashlib import md5
logging.basicConfig(level=logging.INFO)

from qlib.data import dbobj, Cache
from qlib.file import ensure_path

import requests

DEFAULT_BASE_DIR = os.path.expanduser("~/DefaultApiDir/plugins")
MODULES_PATH = os.path.join(DEFAULT_BASE_DIR, 'Plugins')
ensure_path(DEFAULT_BASE_DIR)
ensure_path(MODULES_PATH)


class OO:pass
class Repo(dbobj):pass

# def load(name):
#     try:
#         return importlib.import_module("Plugins.%s" % name)
#     except ModuleNotFoundError as e:
#         files = os.listdir(MODULES_PATH)
#         if (name + ".bash") in files:
#             def _run(*args, **kargs):
#                 res = os.popen('bash %s {}'.format(" ".join(['"%s"' % i for i in args])) % os.path.join(MODULES_PATH, name + ".bash")).read()
#                 return res
#             OO.run = _run
#             return OO
#         return str(e)
    
class RepoDeal:


    def __init__(self, base_dir, base_repo):
        self.BASE_DIR = base_dir
        self.BASE_REPO = base_repo
        self.MODULES_PATH = os.path.join(self.BASE_DIR, 'Plugins')
        self.REPO_DB = os.path.join(self.BASE_DIR, 'repo.db')
        self.REPO_NOW_USE = os.path.join(self.BASE_DIR, 'repo.now')

    def _set_repo(self, name,url, path):
        c = Cache(self.REPO_DB)
        if path.endswith("/"):
            path = path[:-1]
        r = Repo(name=name, url=url, path=path)
        if url.startswith("https://") and 'git' in url:
            os.popen("cd %s && git init || git remote add %s  %s" % (r.path.strip(), r.name.strip(), r.url.strip()))
        r.save(c)

    def load_repo(self):
        if os.path.exists(self.REPO_NOW_USE):
            with open(self.REPO_NOW_USE) as fp:
                N = fp.read()
                dname = os.path.dirname(N)
                mname = os.path.basename(N)
                
                if dname not in sys.path:
                    sys.path.insert(0, dname)
                c = Cache(self.REPO_DB)
                n = ""
                r = c.query_one(Repo, path=N.strip())
                if r:
                    n = r.name
                return mname,n
        else:
            
            c = Cache(self.REPO_DB)
            r = c.query_one(Repo)
            if r:
                dname = r.path
                mname = r.name
                logging.error("Use repo: {}".format(dname))
                # dname = os.path.dirname(self.__class__.MODULES_PATH)
                # mname = os.path.basename(self.__class__.MODULES_PATH)
                if dname not in sys.path:
                    sys.path.insert(0, dname)
                return mname, 'origin'
            else:
                logging.error("Init repo")
                return '',''

    def repo_upload(self, name=None,data=None, mac=None):
        if not name or not mac:
            return "must set a name=xxxx data=base64(xxx) mac=xxx"
        if isinstance(data, str):
            data = base64.b64decode(data.encode("utf-8"))

        if md5(data).hexdigest() != mac:
            return "upload file is not correct!"
        b_fp = io.BytesIO(data)
        z = zipfile.ZipFile(b_fp)
        url = "upload://"
        if len(z.filelist) > 0:
            z.extractall(path=self.BASE_DIR)
            dir_name = z.filelist[0].filename.split("/")[0]
            if not re.match(r'^[\w\d]+$', dir_name):
                return "must char and num as dir's name"
            path = os.path.join(self.BASE_DIR, dir_name)
            if os.path.exists(path):
                os.popen("rm -rf %s " % path)
            self._set_repo(name, url, path)
            return "set repo : %s -> %s" % (name, path)
        else:
            return "no file in zip archive !"


    def repo_set(self,name='', url=''):
        if not 'https://' in url:
            return url + " Not valid"

        if url.endswith(".git"):
            path = os.path.join(self.BASE_DIR ,url.split("/")[-1].split(".git")[0])
        else:    
            path = os.path.join(self.BASE_DIR, url.split("/")[-1])
        path = path.replace("-", "_")

        ensure_path(path)
        # print(path)
        if not os.path.exists(path):
            return "path:%s Not found path" % path
        if ' ' in name:
            return name + " Not valid"

        self._set_repo(name, url, path)
        return "repo set : name=%s url=%s path=%s " % (name, url, path)
    
    def repo_use(self, name=None):
        c = Cache(self.REPO_DB)
        r = c.query_one(Repo,name=name)
        # self._switch_repo()
        with open(self.REPO_NOW_USE, 'w') as fp:
            fp.write(r.path)
        self.MODULES_PATH = r.path
        return 'switch to : %s: %s' % (r.name, r.path)

    def repo_update(self, repo_name=None):
        c = Cache(self.REPO_DB)
        r = c.query_one(Repo, name=repo_name)
        if not r:
            r = c.query_one(Repo)
        
        if r:
            return os.popen("cd %s && pwd &&  git fetch --all && git reset --hard %s/master" % (r.path.strip(), r.name)).read()
        else:
            base_repo = self.BASE_REPO
            assert base_repo is not None
            self._set_repo('origin', base_repo, self.MODULES_PATH)
            res = self.repo_update(repo_name)
            return "rebuild... " + res
    
    def repo_files(self):
        
        if os.path.exists(self.REPO_NOW_USE):
            with open(self.REPO_NOW_USE) as fp:
                self.MODULES_PATH = fp.read().strip()
        else:
            c = Cache(self.REPO_DB)
            rep = c.query_one(Repo)
            if rep:
                self.MODULES_PATH = rep.path
        return os.listdir(self.MODULES_PATH)

    def repo_ls(self):
    
        c = Cache(self.REPO_DB)
        rs = c.query(Repo)
        _d = [r.get_dict() for r in rs]
        now,name = self.load_repo()
        _d.insert(0,name)
        return json.dumps(_d)
    
    def repo_help(self):
        
        return """suport:
                Was sagst du?
                    functions_map = {
                        'repo-help': repo.repo_help,
                        'repo-ls':repo.repo_ls,
                        'repo-files':repo.repo_files,
                        'repo-update':repo.repo_update,
                        'repo-set':repo.repo_set,
                        'repo-use':repo.repo_use,
                        'repo-del':repo.repo_del,
                        'repo-clear':repo.repo_db_clear,
                        'repo-upload':repo.repo_upload
                    }

                """
    
    # def repo_update(self):    
    #     res = self.update("")
    #     return res

    def repo_del(self, name=None):
        c = Cache(self.REPO_DB)
        r = c.query_one(Repo, name=name)
        c.delete(r)
        if os.path.exists(r.path) and os.path.isdir(r.path) and r.path.startswith(self.BASE_DIR):
            os.popen("rm -rf %s "% r.path).read()
        return 'db delete: {}'.format(name)

    def repo_db_clear(self):
        c = Cache(self.REPO_DB)
        c.drop(Repo)
        return 'db clear'

    def if_init(self):
        c = Cache(self.REPO_DB)
        r = c.query_one(Repo)
        if not r:
            return False
        return True

class Services:

    def __init__(self, Services_path):
        self.service_path = Services_path

    def list(self):
        return os.popen("ls %s " % self.service_path).read()

    def start(self, name=None):
        return os.popen("supervisorctl start %s " % name).read()

    def stop(self, name=None):
        return os.popen("supervisorctl stop %s " % name).read()

    def restart(self, name=None):
        return os.popen("supervisorctl restart %s " % name).read()

    def _reload(self, repo=None):
        services = [i for i in os.listdir(repo) if i.endswith(".service")]
        now_services = set(self.list().split("\n"))
        check = set([ i.split(".service")[0] + ".conf" for file  in services])
        res = []
        for i in (check - now_services):
            one = os.path.join(repo, i.split(".")[0] + ".service")
            two = os.path.join(self.service_path, i)
            cmd = "cp -v %s %s " %(one,  two)
            os.popen(cmd)
            res.append(cmd)
        return '\n'.join(res)



        


class BaseApi:
    RAW_PATH = sys.path
    BASE_REPO = None
    BASE_DIR = None
    SERVICES_PATH = None

    exes = ThreadPoolExecutor(max_workers=40)

    def __init__(self, name, loop=None, callback=None):
        assert self.__class__.BASE_REPO is not None
        assert self.__class__.BASE_DIR is not None
        self.__class__.MODULES_PATH = os.path.join(self.__class__.BASE_DIR, 'Plugins')
        self.__class__.REPO_DB = os.path.join(self.__class__.BASE_DIR, 'repo.db')
        self.__class__.REPO_NOW_USE = os.path.join(self.__class__.BASE_DIR, 'repo.now')
        self.__class__.SERVICES_PATH = os.path.join(os.path.dirname(self.__class__.BASE_DIR), 'services')
        self.name = name
        self.loop = loop
        self.__callback = callback
        self.Permission = None
        self._Obj = None
        sys.path += [self.BASE_DIR]
        if hasattr(self._Obj, 'Permission'):
            self.Permission = self._Obj.Permission
            logging.info(f'Permission {self.Permission}')
        else:
            if name.startswith('repo-') or name.startswith("service-"):
                self.Permission = 'auth'
            
            if name == 'repo-set':
                repo = RepoDeal(self.BASE_DIR, self.BASE_REPO)
                if not repo.if_init():
                    logging.error("need to init : Permission -> {}".format(self.Permission))
                    self.Permission = None
                

    def set_callback(self, callback):
        self.__callback = callback

    def if_callback(self):
        if self.__callback:
            return True
        return False

    def get_callback(self):
        if self.__callback:
            return self.__callback
        return self._callback


    def rest_path(self):
        sys.path = self.__class__.RAW_PATH



    

    def load(self, name):
            # print(value)
        repo = RepoDeal(self.BASE_DIR, self.BASE_REPO)
        mname,repo_name = repo.load_repo()
        mname = mname.replace("-","_")
        repo_name = repo_name.replace("-", "_")
        try:
            self._Obj = importlib.import_module("%s.%s" % (mname, name))
        except ModuleNotFoundError as e:
            files = repo.repo_files()
            if (name + ".bash") in files:
                OO.run = partial(self.run_bash, name)
                self._Obj = OO
                return OO
            elif (name + ".service") in files:
                # pass
                os.popen("cp %s %s && x-neid-server reload" %(os.path.join(repo.MODULES_PATH, name + ".service"),
                    os.path.join(self.SERVICES_PATH, name) ))
                self._Obj = 'service load and start'
                return self._Obj

            self._Obj = str(e)
            return str(e)
        except TypeError as e:
            self._Obj = str(e)
            return str(e)
    
    def run_bash(self, name, *args, **kargs):
        res = os.popen('bash %s {}'.format(" ".join(['"%s"' % i for i in args])) % os.path.join(self.MODULES_PATH, name + ".bash")).read()
        return res

    def service_reload(self):
        service = Services(self.SERVICES_PATH)
        return service._reload(repo=self.MODULES_PATH)

    def run(self, *args, **kargs):
        repo = RepoDeal(self.BASE_DIR, self.BASE_REPO)
        service = Services(self.SERVICES_PATH)
        functions_map = {
            'repo-help': repo.repo_help,
            'repo-ls':repo.repo_ls,
            'repo-files':repo.repo_files,
            'repo-update':repo.repo_update,
            'repo-set':repo.repo_set,
            'repo-use':repo.repo_use,
            'repo-del':repo.repo_del,
            'repo-clear':repo.repo_db_clear,
            'repo-upload':repo.repo_upload,
            'service-ls': service.list,
            'service-start': service.start,
            'service-stop': service.stop,
            'service-restart': service.restart,
            'service-reload': self.service_reload,
        }

        logging.error(f"{self.name} : args: {args} kargs: {kargs}")
        if self.name in functions_map:
            res = functions_map.get(self.name)(**kargs)
            return res
            
        else:
            self.load(self.name)
            if isinstance(self._Obj, str):
                return self._Obj
            if not self.loop:
                logging.warn("loop is None.")
            
            fff = partial(self._Obj.run, **kargs)
            if 'loop' in self._Obj.run.__code__.co_varnames:
                logging.info("patch with loop")
                
                fff = partial(fff, loop=self.loop)

            futu = self.__class__.exes.submit(fff)
            if hasattr(self._Obj, 'callback'):
                self.__callback = self._Obj.callback
            futu.add_done_callback(self._acallback)

    def _acallback(self, result):
        self.callback(result)
        self.rest_path()

    def _callback(self, r):
        print(colored("[+]",'green'), r)

    def callback(self, result):
        raise NotImplementedError("Not implement!!")



class BaseArgs:

    def __init__(self, handle, tp=None):
        self.handle = handle
        self.args = []
        self.kargs = dict()
        self._tp = tp
        self.parse()


    def get_parameter(self):
        raise NotImplementedError("")

    def get_parameter_keys(self):
        raise NotImplementedError("")

    def finish(self,D):
        raise NotImplementedError("")

    def parse(self):    
        tp = self.get_parameter("type")
        args = self.get_parameter('args')
        self.module = self.get_parameter('module')
        self.type = tp
        self.kwargs = {}

        keys = self.get_parameter_keys()
        for k in keys:
            if k in ['type', 'args', 'module']:
                continue
            self.kwargs[k] = self.get_parameter(k)

        if tp == 'base64':
            if isinstance(args, str):
                args = args.encode('utf8', 'ignore')
            args = json.loads(base64.b64decode(args))
            if isinstance(args, (list, tuple,)):
                self.args = args
            else:
                self.kargs = args
        elif tp =='json':
            args = json.loads(args)
            if isinstance(args, (list, tuple,)):
                self.args = args
            else:
                self.kargs = args
        else:
            self.args = [args]

    def after_dealwith(self, data):
        b_data = {'res':None, 'type':'json'}
        
        if isinstance(data, str) or isinstance(data, (list, dict, tuple, )):
            b_data['res'] = data
        elif isinstance(data, (int,float,bool,)):
            b_data['res'] = data
        else:
            b_data['res'] = base64.b64encode(pickle.dumps(data))
            b_data['type'] = 'pickle'

        D = json.dumps(b_data)
        self.finish(D)
            

def repo_upload_client():
    if len(sys.argv) != 4:
        print("repo-upload  [name] [url] [file_path]")
        return

    file = sys.argv[3]
    url = sys.argv[2]
    name = sys.argv[1]
    data = None
    if not url.startswith("http"):
        print("not valid url !!")
        return

    if file.endswith(".zip"):
        with open(file, 'rb') as fp:
            data = fp.read()
    else:
        os.popen("zip -r %s.zip %s" % (file, file)).read()
        with open(file+".zip", 'rb') as fp:
            data = fp.read()
    if not data:
        print("no dir found !!")
        return
    mac = md5(data).hexdigest()
    res = requests.post(url, data={
            "module":"repo-upload",
            "name":name,
            "data":base64.b64encode(data),
            "mac":mac,
        },verify=False).text
    print(res)
