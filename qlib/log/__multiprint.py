import sys
import queue
import threading
import time
import random
from termcolor import colored

class _Printer:

    def __init__(self, name, color="yellow", attrs=[]):
        self.index = 0
        self.name = name
        self._position = 1
        self._process = 0
        self._last_msg = ""
        self._attrs = attrs
        self._color = color
        self._times = 0

    def _print(self):
        if self.index == 0 and self._position > 0:
            sys.stdout.write("\033[%dA\033[K" % self._position)
        elif self.index == 0:
            sys.stdout.write("\033[1A\033[K")
            # sys.stdout.flush()
        if self._color:
            m = colored(self._last_msg, self._color, attrs=self._attrs)
        else:
            m = colored(self._last_msg, attrs=self._attrs)
        print(colored("[%s]" % self.name, "green"), colored("[%d]" % self._times, 'blue'), m)

    def add_position(self):
        self._position += 1

class Printers:
    _Process = {}
    _msg_queue = queue.Queue()
    interval = 1
    _start =  False

    def __iter__(self):
        return  iter(self._Process)
    
    @classmethod
    def regist(cls,  name, color=None,attrs=[]):
        P = _Printer(name, color=color, attrs=attrs)
        P.index = len(cls._Process)
        cls._Process[name] = P
        for p in cls._Process.values():
            p._position = len(cls._Process)
        
        if not cls._start:
            cls.run()

    @classmethod
    def print(cls, name, *args, **kargs):
        msg = " ".join([str(i) for i in args])
        cls._msg_queue.put_nowait({name: msg})

        
            
    @classmethod
    def _run(cls, _msg_queue):
        EXIT = False
        found = False
        while 1:
            
            if  not _msg_queue.empty():
                m = _msg_queue.get_nowait()
                if isinstance(m, str):
                    EXIT= True
                    break
                found = True
                for n,v in m.items():
                    pp = cls._Process.get(n)
                    pp._last_msg = v
                
            else:
                if found == True:
                    for p in cls._Process.values():
                        p._print()
                        p._times += 1
                    found = False
            
                time.sleep(cls.interval)
            if EXIT:
                break
        
        cls._Process = {}
        cls._msg_queue = queue.Queue()
        _start =  False


    @classmethod
    def run(cls):
        t = threading.Thread(target=cls._run, args=(cls._msg_queue,))
        t.start()
        # t.daemon = True
    
    def __del__(self):
        self.__class__._msg_queue.put_nowait("END")
    
    @classmethod
    def stop(cls):
        cls._msg_queue.put_nowait("[END]")
        

def test_printer(name):
    Printers.regist(name)
    for l in range(100):
        Printers.print(name, "hello world", l)
        time.sleep(random.randint(1, 3))
