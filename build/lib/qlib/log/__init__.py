import sys, logging, os
from io import  TextIOWrapper
from termcolor import colored
from functools import wraps
from .__log import LogControl, INFO, OK, ERR, FAIL, WRN, tag_print, L, show


__all__ = [
    "LogControl", "INFO", "OK", "ERR", "FAIL" , "WRN", "tag_print", "L", "show","logging"
]



class MyFormatter(logging.Formatter):
    Ffmts ={
        logging.ERROR : colored("<%(levelname)s | %(filename)s:%(lineno)s> ", "red", attrs=['bold']) + colored("%(asctime)s", 'yellow') + " : %(message)s ",
        logging.INFO : colored("<%(levelname)s> ", "green") + colored("%(asctime)s", 'yellow') + " : %(message)s " ,
        logging.WARN : colored("<%(levelname)s> ", "yellow") + colored("%(asctime)s", 'yellow') + " : %(message)s " ,
    } 

    def format(self, record):
        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._style._fmt
        # Replace the original format with one customized by logging level
        
        if record.levelno in self.Ffmts:
            self._style._fmt = self.Ffmts[record.levelno]
        result = super().format(record)
        # Restore the original format configured by the user
        # self._fmt = format_orig
        self._style._fmt = format_orig
        return result


def load(level=logging.ERROR, stdout=sys.stdout):
    fmt = MyFormatter()
    hdlr = logging.StreamHandler(sys.stdout)
    hdlr.setFormatter(fmt)

    if isinstance(stdout, str):
        hdlr = logging.FileHandler(stdout)
        
    elif isinstance(stdout, TextIOWrapper):
        hdlr = logging.StreamHandler(stdout)
    
    
    
    # logging.root.addHandler(hdlr)
    # if os.getenv("DEBUG"):
    logging.root.setLevel(level)
    logging.root.handlers = [hdlr]
    # logging.warning("load qlib's formater! ")


def log(level, stdout=sys.stdout):
    load(level, stdout)
    def sfunc(func):
        @wraps(func)
        def __wrap(*args, **kargs):
            return func(*args, **kargs)
        return __wrap
    return sfunc

