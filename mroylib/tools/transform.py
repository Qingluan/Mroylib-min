import cmd
import inspect
import re
from termcolor import colored, COLORS
from functools import wraps
from types import MethodType
import json
import tqdm

def _log(*args, tag='[+]', c='red', ca=('bold',), cn='', end='\n'):
    if cn:
        fmt_str = '\033[%dm' % COLORS.get(cn,0)
    else:
        fmt_str = ''
    REST = '\x1b[0m'
    print(colored(tag, c, attrs=list(ca)), fmt_str, *args, REST, end=end)

def parse(args):
    a = args.split()
    args = []
    kargs = {}
    for i in a:
        if not i.startswith('"') or not i.startswith("'"):
            if i.strip() in locals():
                args.append(locals()[i.strip()])
            elif re.match(r'^\d*$', i.strip()):
                args.append(int(i))
            elif re.match(r'^\d*\.\d*$', i.strip()):
                args.append(float(i))
            elif '=' in i:
                k,v = i.split("=", 1)
                kargs[k] = parse(v)[0][0]
            else:
                args.append(i.strip())
        else:
            args.append(i[1:-1])
    return args, kargs



            # self.get_help(func.__name__)
        

class JsonHandleCmd(cmd.Cmd):

    def __init__(self, json_file, *args, **kargs):
        super().__init__(*args, **kargs)
        keys = None
        strage = []
        with open(json_file, 'rb') as fp:
            lines = fp.readlines()
            for l in tqdm.tqdm(lines):
                o = json.loads(l)
                _keys = dict(zip(o.keys(), o.keys()))
                if keys is None:
                    keys = _keys
                else:
                    if set(keys.keys()) ^ set(_keys.keys()):
                        new_keys = set(_keys.keys()) ^ set(keys.keys())
                        keys.update(_keys)
                
        self.prompt = "you say:"
        self.intro = str(keys)
        self.keys = keys

    def do_map(self, args):
        args, kwargs = parse(args)
        if kwargs:
            self.keys.update(kwargs)
        
        _log("\n",str(self.keys))

    def do_exit(self, args):
        return self.keys
    