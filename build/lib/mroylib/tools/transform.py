import cmd
import inspect
import re
from termcolor import colored, COLORS
from functools import wraps
from types import MethodType
import json
import tqdm
import json
import pickle
import struct
import io
import os

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


class JsonTree:
    def __init__(self, d):
        self._keys = {}
        self._vals = d
        self._subs = {}
        
        for k,v in d.items():
            if not isinstance(v, dict):
                self._keys[k] = k
            else:
                s = JsonTree(v)
                self._keys[k] = s.keys
                self._subs[k] = s
    
    @property
    def id(self):
        keys = '|'.join(self.keys.keys())
        return keys

    @property
    def keys(self):
        return self._keys
    
    @property
    def F(self):
        real = {}
        for k in self.keys:
            v= self.keys[k]
            if isinstance(v, str):
                real[k] = self._vals[k]
            else:
                real.update(self._subs[k].F)
        return JsonTree(real)

    def __setitem__(self, key,value):
        
        if key in self._keys:
            if not isinstance(self._keys[key], dict):
                self._keys[key] = value
            else:
                self._keys[value] = self._keys[key].copy()
                self._vals[value] = self._vals[key].copy()
                self._subs[value] = self._subs[key]
                del self._keys[key]
                del self._vals[key]
                self._subs.pop(key)
            return True
        else:
            g = False    
            for v in self._subs.values():
                if v.__setitem__(key,value):
                    g = True
                    break
            return g
    
    def __repr__(self, pre=0):
        s = []
        for i in self._keys:
            v = self._keys[i]
            if isinstance(v, str):
                s.append(' '* pre + i + " --> " + v)
            else:
                s.append(' '*pre + i + "\n" + self._subs[i].__repr__(pre=pre+1))
        return "\n".join(s)

    @property
    def rank(self):
        rank = 0
        for k in self.keys:
            v = self.keys[k]
            if isinstance(v, dict):
                rank += self._subs[k].rank
            else:
                rank += 1
        return rank

    def __eq__(self, jb):
        if self.keys == jb.keys:
            return True
        else:
            return False
    
    def __mul__(self, jb):
        data = self.D
        if isinstance(jb, dict):
            data.update(jb)
            return JsonTree(data)
        elif isinstance(jb, JsonTree):
            data.update(jb.D)
            return JsonTree(data)
        else:
            return self

    
    def __div__(self, jb):
        if self == jb:
            return self
        self_keys = self.keys.keys()
        jb_keys = jb.keys.keys()
        no_keys = set(self_keys) - set(jb_keys)
        for k in no_keys:
            del self._keys[k]

        for k in self._keys:
            sub_self = self._subs[k]
            sub_jb = jb._subs[k]
            self._subs[k] = sub_self / sub_jb
        return self
    
    @property
    def B(self):
        str_buf = pickle.dumps(self.D, 0)
        return len(str_buf),str_buf

    @property
    def D(self):
        new_d = {}
        for k in self._vals:
            kk = self._keys[k]
            if isinstance(kk, str):
                new_d[self._keys[k]] = self._vals[k]
            else:
                new_d[k] = self._subs[k].D
        return new_d
    
    @classmethod
    def from_B(cls, B_buf):
        return cls(pickle.loads(B_buf))

class BinMap:
    """
    Headers:
        [ all_size : Q(8) ] [ header_size : $(H_S) ? ] [ header_len: $(H_L) ? ]
        header : [ key_len : L (4) , fmt_l : L (4) ,  pickle.loads(struct.unpack(fmt_l + "s" , buf[8:key_len+8] ) , body_point: struct.unpack(Q, buf[key_len+8:] ]
        ...
    Bodys:

    """
    END = "!"
    H_S = "L"
    H_L = "L"
    
    def __init__(self, file_or_buf):
        if isinstance(file_or_buf, io.BufferedIOBase):
            self.buf= file_or_buf
        else:
            self.buf = open(file_or_buf, 'rb')
        self.buf.seek(0, os.SEEK_SET)
        self.bin_size = struct.unpack(self.END + "Q", self.buf.read(8))[0]
        self.header_size = struct.unpack(self.END + self.H_S, self.buf.read(struct.calcsize(self.END + self.H_S)))[0]
        self.header_len = struct.unpack(self.END + self.H_L, self.buf.read(struct.calcsize(self.END + self.H_L )))[0]

        self.header_point = struct.calcsize(self.END + self.H_L) + struct.calcsize(self.END +  self.H_S) + 8
        self.body_point = self.header_point + self.header_size
        
        self.headers = {}
        for _ in tqdm.tqdm(range(self.header_size// self.header_len)):
            self.headers.update(self.unpack(True))

    @classmethod
    def pack(cls, datas):
        packed = []
        headers = b''
        packed_l = []
        start = 0
        max_key_len = 0
        _p = []
        header_point = struct.calcsize(cls.END + cls.H_L) + struct.calcsize(cls.END + cls.H_S) + 8
        for no, data in tqdm.tqdm(enumerate(datas)):
            data_buf = pickle.dumps(data, 0)
            if hasattr(data, 'id'):
                key = data.id
            else:
                key = no

            l = len(data_buf)
            data_buf_l = struct.pack(cls.END + "Q", l)
            packed.append(data_buf_l + data_buf)
            packed_l.append(start)
            start += len(packed[no])

            key_buf = pickle.dumps(key, 0)
            key_buf_l = struct.pack(cls.END + "Q", len(key_buf))
            _p.append(key_buf_l + key_buf)

            if len(_p[no]) + 8 > max_key_len:
                max_key_len = len(_p[no]) + 8
        

        for no, bk in enumerate(_p):
            block_point = max_key_len * len(_p)   +   packed_l[no] + header_point
            h = struct.pack(cls.END + "Q", block_point) + bk
            headers += (h + (max_key_len - len(bk)) * b'0x00' )
        all_body = headers + b''.join(packed)
        bin_size = struct.pack(cls.END + "Q" , len(all_body) + header_point)
        head_size = struct.pack(cls.END + cls.H_S , max_key_len * len(_p))
        head_len = struct.pack(cls.END + cls.H_L , max_key_len)

        return bin_size + head_size + head_len + all_body
        

    def unpack(self, header=False):
        next_l = struct.unpack(self.END + "Q", self.buf.read(8))[0]
        if header:
            d_l = struct.unpack(self.END + "Q", self.buf.read(8))[0]
        else:
            d_l = next_l

        # fmt_l = struct.unpack(self.H_L, buf[8:l + 8])
        data = pickle.loads(self.buf.read(d_l))
        if header:
            return {data: next_l}
        return data

    def __getitem__(self, k):
        if k in self.headers:
            self.buf.seek(self.headers[k], os.SEEK_SET)
            return self.unpack()
    
    def to_bin(self):
        return self.__class__.pack(self)

    def __del__(self):
        if hasattr(self,"buf"):
            self.buf.close()
    
    

class JsonTreeList(BinMap):

    def __init__(self, json_file):
        self.data =[]
        self.max_keys = None
        now_max = 0
        self.min_keys = None
        now_min = 0

        with open(json_file, 'rb') as fp:
            lines = fp.readlines()
            for l in tqdm.tqdm(lines):
                jt = JsonTree(json.loads(l))
                if not self.max_keys:
                    self.max_keys = jt.keys
                if not self.min_keys:
                    self.max_keys = jt.keys
                if jt.rank > now_max:
                    self.max_keys = jt.keys
                elif jt.rank < now_min:
                    self.min_keys = jt.keys
                self.data.append(jt)

    def __setitem__(self, key,val):
        for i in tqdm.tqdm([i.__setitem__(key, val) for i in self.data]):
            pass

    def __iter__(self):
        for data in self.data:
            yield data

    def sub_keys(self, d):
        data = []
        for j in self.data:
            data.append(j / d)
        self.data = data

    def add_keys(self, d):
        data = []
        for j in self.data:
            data.append(j * d)
        self.data = data

    @property
    def Out(self):
        return [i.D for i in self.data]


class JsonTreeListHandleCmd(cmd.Cmd):

    def __init__(self, json_file, *args, **kargs):
        super().__init__(*args, **kargs)
        self.tree = JsonTreeList(json_file)
        self.prompt = "you say :"
        self.intro = colored(self.tree.data[0].__repr__(), 'blue')+'\n'+colored(self.tree.data[0].D,'yellow')

    def do_map(self, args):
        k,v = args.replace('=',' ').strip().split(maxsplit= 1)
        self.tree[k] = v
        self.do_show("")
        

    def do_add_default(self, args):
        if '=' in args:
            k,v = args.strip().split('=',1)
        elif ' ' in args:
            k,v = args.strip().split(maxsplit=1)
        elif ':' in args:
            k,v = args.strip().split(':',maxsplit=1)
        else:
            k = args.strip()
            v = input("now key:%s value:" % k).strip()
        d = {k.strip():v.strip()}
        self.tree.add_keys(d)
        self.do_show("")


    def do_show(self, args):
        intro = colored(self.tree.data[0].__repr__(), 'blue')+'\n'+colored(self.tree.data[0].D,'yellow')
        _log(intro)

    def complete_map(self, text, line, start_ix, end_ix):
        res = []
        for k in self.tree.max_keys:
            if text in k:
                res.append(k)
        return [i+"=" for i in res]

    def do_exit(self, args):
        return True
    
