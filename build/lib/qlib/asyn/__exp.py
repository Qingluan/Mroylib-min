import concurrent
import time

from functools import wraps, partial
from collections import Iterable
from termcolor import cprint
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent import futures

class Exe:
    Results = []

    def __init__(self, num, **options):
        self.callback = None

        for k in options:
            setattr(self, k, options[k])
        if isinstance(num, Iterable):
            self.thread_num = num[1]
            self.process_num = num[0]
        else:
            self.process_num = 1
            self.thread_num = num

        if "cpu" in options:
            self.exe = ProcessPoolExecutor(max_workers=self.thread_num)
        else:
            self.exe = ThreadPoolExecutor(max_workers=self.thread_num)

    def submit_result(self, funcs, *args, **kargs):
        futures = self.exe.submit(funcs, *args, **kargs)
        # Exe.Results.append( futures.result())
        return futures.result()

    def submit(self, funcs, *args, **kargs):
        futures = self.exe.submit(funcs, *args, **kargs)
        # Exe.Results.append( futures.result())
    
    def done(self, funcs, after_handle, *args, **kargs):
        futures = self.exe.submit(funcs, *args, **kargs)
        done_func = partial(self.on_done, after_handle)
        futures.add_done_callback(done_func)


    def map(self, func, args_iterable):
        for res in self.exe.map(func, *zip(*args_iterable)):
            # print(res)
            yield res

    def __call__(self, timeout=9, callback=None):
        self.timeout = timeout
        if callback:
            self.callback = callback
        else:
            if not self.callback:
                self.callback = self.__callback

        def _wrap(func):

            @wraps(func)
            def __run(*args, **kargs):
                future = self.exe.submit(func, *args, **kargs)
                future.args = {"args": args, "kargs": kargs}
                return future.add_done_callback(self._background)
            return __run
        return _wrap

    def _background(self, future):
        err = future.exception()
        arg = future.args
        return self.callback(err, arg, future.result())

    def on_done(self, callback, future):
        res = future.result()
        if isinstance(res, (str, dict)):
            return callback(res)
        return callback(*res)

    def __callback(self, exp, args, result):
        print(exp, args, result)

    def _t_run(self, seconds, function, *args, **kargs):
        time.sleep(seconds)
        return function(*args, **kargs)

    def timmer(self, time, function, *args, **kargs):
        self.submit(self._t_run, time, function, *args, **kargs)  
        



def __threads(n, func, *nargs):
    futures = set()
    results = set()
    with ThreadPoolExecutor(max_workers=n) as T:
        [futures.add(T.submit(func, arg)) for arg in nargs]
        for future in concurrent.futures.as_completed(futures):
            err = future.exception()
            if err is not None:
                cprint(err, "red")
            results.add(future.result())
    return results


def compute(func, args, process_num=2, thread_num=6, log=True):
    # queue = set()
    def get_args(num, args):
        s = []
        for i in args:
            s.append(i)
            if len(s) == num:
                yield s
                s = []

    futures = set()
    results = set()
    args_num = len(args) / process_num
    print(process_num, thread_num)
    if log:
        cprint('==> start %s [%d]' % (time.asctime(), args_num), 'cyan')

    with ProcessPoolExecutor(process_num) as P:
        for arg in get_args(args_num, args):
            if log:
                print(arg)
            futures.add(P.submit(__threads, thread_num, func, *arg))
        for f in concurrent.futures.as_completed(futures):
            for e in f.result():
                # if generate:
                #     yield e
                results.add(e)
                # pass
            if log:
                cprint('end <== %s' % time.asctime(), 'blue')

        # if not generate:
            return results
