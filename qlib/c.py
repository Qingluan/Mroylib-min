import os
import re
import ctypes
from ctypes import *
from functools import wraps

from termcolor import cprint

j = os.path.join

qlib_c_lib_path = j(os.getenv("HOME"), ".Q/clib/")
uchar_p_size = 1024


P_C_BASETYPE = {
    int: c_int,
    str: c_char_p,
    float: c_float,
}


P_C = {
    str: lambda x:c_char_p(x.encode("utf8", "ignore")),
    int: lambda x:c_long(x) if x> 65535 else c_int(x),
    float: lambda x:c_float(x),
}

extend_table = {
    'c_uchar': c_ubyte,
    'c_wchar_t': c_wchar,
    'c_uu_char': c_ubyte,
    'c_u_char': c_ubyte,
    'c_u_char_p': c_ubyte * uchar_p_size,
    'c_wchar_t_p': c_wchar_p,
    'c_uchar_p': c_ubyte * uchar_p_size,
    'c_void': c_voidp,
}


def filter_decor(c):
    """
    this function for to remove parameter decorator .
        like this 'const char some[100]' 
    """
    return re.sub(r'(const|extern)', '', c)



def get_base_ctype(c_type):
    words = c_type.strip().split()
    c_type = c_type.strip()
    l = len(words)
    c_type_name = None
    
    if l == 1:
        c_type_name = 'c_' + c_type
    else:
        
        # deal with 'char*' 
        if words[-1] == '*' and l==2:
            pre = re.sub(r' ', '', c_type)
            c_type_name = 'c_' + pre
        else:
            # let 'unsigned char *' to 'uchar*'
            pre = re.sub(r'(^\w+?) |( )', '', c_type)
            c_type_name = 'c_' + words[0][0] + pre
    
        # let 'c_uchar*' to 'c_uchar_p'
        c_type_name = c_type_name.replace(r'*', '_p')


    if hasattr(ctypes, c_type_name):
        return getattr(ctypes, c_type_name)
    else:
        try:
            return extend_table[c_type_name]
        except KeyError:
            return c_type


def check_pointer(n):
    """
    this function for seprate name and point 
    like this  'char *' -> ['char', 1] ,
    'char **' -> ['char', 2] 

    if it is not pointer , will return None
    like this 'char' -> ['char', 0]
    """
    w = n.split("*")
    return w[0].strip(), len(w) -1 



def to_c(py):
    try:
        return None, P_C[type(py)](py)
    except KeyError:
        return None, py


def array_to_c(lst):
    if isinstance(lst, list) and not isinstance(lst[0], list):
        struct = P_C_BASETYPE[type(lst[0])] * len(lst)
        vals = [to_c(i) for i in lst]
        return struct, struct(*vals)
    else:
        l = len(lst)
        arr = [ array_to_c(i) for i in lst]
        sub_vals = [i[1] for i in arr]
        sub_struct = arr[0][0]
        struct = sub_struct * l
        val = struct(*sub_vals)
        return struct, val 


def deref(addr, typ):
    if typ:
        return ctypes.cast(addr, ctypes.POINTER(typ)).contents
    return None


def c(func):
    """
    translaste normal py's arguments to c's arguments
    """
    # def _c(func):

    @wraps(func)
    def _wrap(*args):
        instance = args[0]
        c_args = [instance]  # keep self
        c_args += [ii[1] for ii in [ array_to_c(i) if isinstance(i, list) else to_c(i) for i in args[1:]]]
        return func(*c_args)

    return _wrap

    # return _c

def c_restype(cfun, restype):

    def _c(func):

        @wraps(func)
        def _d(*args, **kargs):
            instance = args[0]
            if hasattr(instance, cfun):
                f = getattr(instance, cfun)
                if hasattr(f, 'restype'):
                    f.restype = ctypes.POINTER(restype)

            return func(*args, **kargs)

        return _d
    return _c



def include(path, structs):

    def __include(cls):
        """
        to auto load  c dll lib
        """
        def _include(name):
            f = j(path, name + ".h")
            txt = None
            with open(f) as fp:
                txt = fp.read()
            funlist = re.findall(r'(?:\s)([\*\w ]+?\s\w+?\([\w,\s\*]{0,100}\)+.)' ,txt)
            for i in funlist:
                yield re.findall(r'(.+?\s)(\w+?)\(', i)[0]

        def set_restype(retur):
            r = get_base_ctype(filter_decor(retur.strip()))
            if isinstance(r, str):
                struct,num = check_pointer(r)
                if hasattr(structs, struct):
                    e = getattr(structs, struct)
                    cprint("load:" + struct, "green")
                else:
                    cprint(r + " ignored", "yellow")
                    return None

                if num > 0:
                    return ctypes.POINTER(e)
                return e
            else:
                return r


        def _wrap():

            lib = CDLL(j(path, cls.__name__ + ".so"))
            for retu, func_s in _include(cls.__name__):
                func = getattr(lib, func_s)
                rest = set_restype(retu)
                if rest:
                    func.restype = rest
                setattr(cls, func_s, func)
        _wrap()
        return cls
    return __include


def gen_py_ctype(line, uchar_p_size=1024):
    """
    read lib's header file
    can parse c's parameter , but notion!!!!!
    array must be this 'xxx[12]'  not 'xxx [12]', !!!! no space between them.!!!
    """

    def escape(line):
        """
        this function for to let translate multi-space to single-space.
        like this 'const     char   * ss' -> 'const char * ss'
        """
        return re.sub(r'([ ]{2,})', ' ', line)


    def check_array(n):
        """
        this function for to find a array parameter.
            like this 'char some[100]' 
        """
        d = re.findall("(?:\[)(\d+?)\]", n)
        if d:
            return n.split("[")[0], int(d[0])
        else:
            return n, 1

    words = escape(line.strip()).split()
    name = words.pop()
    c_type = filter_decor(" ".join(words))
    name, arr_num = check_array(name)

    c = get_base_ctype(c_type)

    ## check c , ensure it is not null

    if arr_num > 1:
        c = c * arr_num

    return (name, c)


class C(Structure):
    """
        a interface need to immplementation
    """
    def __getitem__(self,k ):
        if hasattr(self, k):
            return getattr(self, k)
        return None

    def keys(self):
        return [i for i in self.__dir__() if not i.startswith("__")]

    def to_array(self, arr, len):
        return arr[:len]


def structure(u_char_size):
    """
        a auto generate a py structure.
        but !!! this can only use for simple struct ,
        !! not supported 'nested' struct
    """
    _uchar_p_size = u_char_size

    def _struct(cls):
        if hasattr(cls, '_raw'):
            txt = cls._raw
        else:
            raise Exception("Must set a _raw ")
        head, body_tail = txt.split("{")
        body, tail = body_tail.split("}")
        # cls._struct_name = re.findall(r'(?:struct[ ]{1,10})(\w+?)\{', txt)[0]
        all_ele = re.findall(r'(?:\s)([\w \*\[\d\]]{0,20})\;', body)
        
        _fields_ = []
        for l in all_ele:
            _fields_.append(gen_py_ctype(l.strip(), uchar_p_size=_uchar_p_size))
        
        return type(cls.__name__, (C,), {'_fields_': _fields_})
    return _struct

