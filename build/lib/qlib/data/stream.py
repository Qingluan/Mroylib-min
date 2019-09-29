from zlib import decompress, compress
from base64 import b64encode, b64decode
import pickle

def check_bal(c):
    if c is str:
        cc = ord(c)
    else:
        cc = c
    bc = bin(cc).split("b")[1]
    b1 = bc.count("1")
    b0 = bc.count("0")
    return abs(b1 - b0)


def lfsr(k, stream):
    last = 0x0
    init_str = b'this is a init xinsd stirng used to general, human will alive or die. it is a interesting question.'
    k_l = len(k)
    s_l = len(stream)
    il = len(init_str)
    sub_flag = 0x55
    for i,v in enumerate(stream):
        v = (v << (s_l % 2)   ^ s_l)  & 0xff
        c_b = k[i % k_l] 
        if check_bal(c_b) < 2:
            last =  c_b ^ last ^ sub_flag
            yield last ^ v
            
        else:
            last =  c_b ^ init_str[i % il] ^ sub_flag
            yield last ^ v

        sub_flag = (last ^  k_l ^ v) & 0xff

def passpack(passwd, payload):
    if isinstance(payload,  bytes):
        payload = payload
    elif isinstance(payload, str):
        payload = payload.encode('utf8')
    # else:
        # payload = pickle.dumps(payload)
    # if not isinstance(payload, bytes):
        # raise Exception("not suport type , only str or bytes")
    


    return b64encode(compress( bytes(lfsr(passwd, pickle.dumps(payload)))))

def reback(payload):
    return pickle.loads(payload)

def passunpack(passwd, pack_payload):
    return pickle.loads(bytes(lfsr(passwd, decompress(b64decode(pack_payload) ) )))
