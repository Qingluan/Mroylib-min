from queue import Queue, Empty
from functools import partial
from copy import  deepcopy
import ctypes
import pickle
from concurrent.futures import  ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing.connection import  Listener, Client
from functools import partial
from collections import deque
# import asyncio


import grequests as async
from qlib.log import show
err_log = partial(show, color='red', a=['underline'])

def thread_patch(fun,pargs, num=7, type='thread'):
    args = []
    for ar in pargs:
        if isinstance(ar, (tuple,list, )):
            args.append(ar)

        elif isinstance(ar, (str, int,)):
            args.append((ar,))
        else:
            args.append((ar,))

    with ThreadPoolExecutor(num) as exe:
        m = [exe.submit(fun, *arg) for arg in args]
        for i in m:
            yield i.result()


def net_patch(urls, err_handler=err_log, callback=None, **kargs):
    """
    @fun only:
        def xxxfun(response):
            ....
            return result
    """
    result = {}

    def patch_result(response, *args, **kargs):
        res = callback(response)
        url = response.url
        show(response.url, response.status_code, log=True, k='debug')
        result[url] = res
    if callback:
        ars = (async.get(u,hooks={'response': [patch_result]}, **kargs) for u in urls)
    else:
        ars = (async.get(u, **kargs) for u in urls)
    _res = async.map(ars, exception_handler=err_handler)
    if not callback:
        return _res
    return result





class LinkError(Exception):
    pass

class MissionPassError(TypeError):
    pass

class Links:

    def __init__(self, funs, *args, to=None, thread=False, process=False, io=False, net=False, **kargs):
        self.to = to
        self.thread = thread
        self.io = io
        self.process = process
        self.net = net
        self.funs = self.funs
        self.args = args
        self.kargs = kargs

    def link_test(self):
        funs = deepcopy(self.funs)
        last_res = ()
        while funs:
            f = funs.pop()

            res = f(*self.args, **self.kargs)

    def __call__(self):
        if self.to:
            res = self.fun()
            if isinstance(res, tuple):
                pass
            else:
                pass



class RPCHandler:

    def __init__(self):
        self._funcs = {}
        self.regiester_function(self.list)

    def regiester_function(self, func):
        self._funcs[func.__name__] = func

    def list(self):
        return list(self._funcs.keys())

    def handle_connection(self, connections):
        try:
            while 1:
                fun_name, args, kargs = pickle.loads(connections.recv())
                try:
                    r = self._funcs[fun_name](*args, **kargs)
                    connections.send(pickle.dumps(r))
                except Exception as e:
                    connections.send(pickle.dumps(e))

        except EOFError:
            pass



class RPCProxy:

    def __init__(self, connections):
        self._connections = connections

    def __getattr__(self, func_name):
        def __rpc_call(*args, **kargs):
            self._connections.send(pickle.dumps((func_name, args, kargs)))
            res = pickle.loads(self._connections.recv())
            if isinstance(res, Exception):
                raise res
            return result
        return __rpc_call




def build_server(addr, handler, auth='hello'):
    serv = Listener(addr, authkey=auth)
    while 1:
        try:
            cl = serv.accept()
            handler.handle_connection(cl)
        except Exception as e:
            show(e, color='red')

def build_client(addr, proxy, auth='hello'):
    cl = Client(addr, authkey=auth)
    return proxy(cl)


class Pid:
    _funs = set()
    _links = set()

    def __init__(self):
        pass

    def push(self, fn):
        fn = fn
        Pid.push(fn)