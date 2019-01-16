import os,re
from qlib.log import LogControl as L

ARGS = """

"""
ROOT_PATH = os.path.dirname(__file__)
FIND_CMD = "grep -nr \"^def \w\"  " + ROOT_PATH
FIND_CLASS_CMD = "grep -nr \"^class \w\"  " + ROOT_PATH

def _show_functions(fuzz=None):

    for l in os.popen(FIND_CMD):
        if l[4] == "_":
            continue
        res = l.split(":")
        if fuzz:
            if re.findall(fuzz,l):
                L.i(res[0][3:],tag=res[2][3:],tag_color='green')    
        else:
            L.i(res[0],tag=res[2][3:], tag_color='green')



def _show_class(fuzz=None):
    for l in os.popen(FIND_CLASS_CMD):
        
        if l[6] == "_":
            continue
        res = l.split(":")
        if fuzz:
            if re.findall(fuzz,l):
                L.i(res[0][5:],tag=res[2][5:],tag_color='blue')    
        else:
            L.i(res[0][5:],tag=res[2][5:],tag_color='blue')

def help(fuzz=None):
    _show_class(fuzz)
    L.ok("---------------- function -----------------")
    _show_functions(fuzz)
