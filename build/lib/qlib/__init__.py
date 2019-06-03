from .text import __text as text
from .net import __urls as urls
from .asyn import __exp as asyn


__all__ = [
    'text',
	'urls',
	'asyn',
    'gen_random_serise',
]


def gen_random_serise(max_len=7):
    import random
    import os
    count = 0
    u = ''
    while count < max_len + random.randint(3,7):
        c = os.urandom(1).decode('utf8','ignore').strip()
        try:
            ch = ord(c)
        except:
            continue
        if  (ch >= 48 and ch <= 57) or (ch >= 65 and ch <= 90) or (ch >= 97 and ch <= 122):
            # print(c)
            u += c
            count += 1
    return u