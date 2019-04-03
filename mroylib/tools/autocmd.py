import cmd
import inspect
import re
from termcolor import colored, COLORS
from functools import wraps
from types import MethodType

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
        

class AutoCmd(cmd.Cmd):
    
    def __init__(self,instance, *args, **kwargs):
        
        self.instance = instance
        self.last = ''
        # self.change_prompt(instance.__class__.__name__)
        super().__init__(*args, **kwargs)
        self.back = self.instance
        self.home = self.instance
        self.cursor = self.instance
        self.names_map = []

        self.refresh()
    
    def wrap_help(self, func):
        @wraps(func)
        def _f():
            if hasattr(func, '__name__'):
                self.get_help(func.__name__)
        return  _f


    def wrap_func(self, func):
        @wraps(func)
        def _f(line):
            # print(["\"{}\"".format(i) for i in args])
            try:
                if not isinstance(func, MethodType):
                    self.last =  func
                else:
                    args, kargs = parse(line)
                    res = func(*args, **kargs)
                    self.last = res
                _log(self.last, cn='green')
            except Exception as e:
                _log(e, cn='red')
                raise e
        return _f

    def change_prompt(self, name):
        self.prompt = colored("%s |" % name,'red', attrs=['underline'])

    def instance_names(self, instance):
        return [i for i in instance.__dir__() if not i.startswith("_")]
    
    def get_help(self, func):
        if isinstance(func, MethodType):
            a = inspect.getfullargspec(func).args
            _log(a)
            code = inspect.getsource(func)
            _log(code)
        
        
    
    def refresh(self):
        if len(self.names_map):
            for n in self.names_map:
                delattr(self, "do_" + n)
                delattr(self, "help_" + n)
            self.names_map = []
        for n in self.instance_names(self.cursor):
            setattr(self, "do_" + n, self.wrap_func( getattr(self.cursor, n)))
            setattr(self, "help_" + n, self.wrap_help( getattr(self.cursor, n)))
             
            self.names_map.append(n)
        if hasattr(self.cursor, '__name__'):
            n = self.cursor.__name__
        else:
            n = str(self.cursor)
        self.change_prompt(n)
    
    def do_go(self, name):
        self.back = self.cursor
        self.cursor = getattr(self.cursor, name.strip())
        self.refresh()
        
    
    def get_sub_attrs(self):
        _maybe = [i for i in dir(self.cursor) if not i.startswith("_")]
        __maybe = [i for i in _maybe if not isinstance(i, MethodType) ]
        return __maybe

    def complete_go(self, text, line, begidx, endidx):
        if text:
            return [ i for i in self.get_sub_attrs() if i.startswith(text.strip())]
        return self.get_sub_attrs()

    def get_names(self):
        names = set(super().get_names()) | set([i for i in dir(self) if not i.startswith("_")])
        return list(names) 
        # return [ colored(i, 'blue', attrs=['underline']) if isinstance(i, MethodType ) else colored(i, 'green') for i in list(names)]

            
    
    def do_back(self):
        self.cursor = self.back
        self.refresh()

    def do_last(self, args):
        _log("Out:\n",self.last)
    
    def do_exit(self, args):
        return  True