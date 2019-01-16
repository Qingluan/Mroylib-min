import pandas as pd
import pickle, time
from functools import wraps

   
def as_series(func):
    @wraps(func)
    def __wrap(*args, **kargs):
        return pd.Series(dict(func(*args, **kargs)))

    return __wrap

def as_dataframe(func):
    @wraps(func)
    def __wrap(*args, **kargs):
        return pd.DataFrame(dict(func(*args, **kargs)))

    return __wrap



@as_dataframe
def exchange_key_val(res, attr):
    d = {}
    for k in res:
        val = getattr(res.get(k),attr)
        if val not in d:
            d[val] = pd.Series([k])
            continue

        ks = d[val]
        d[val].at[len(ks)] = k
    return d

@as_series
def ex_key_val(res, attr):
    d = {}
    for k in res:
        val = getattr(res.get(k),attr)
        if val not in d:
            d[val] = pd.Series([k])
            continue

        ks = d[val]
        d[val].at[len(ks)] = k
    return d



def save(obj, tmp="/tmp/"+ time.asctime().replace(" ","-")):
    bi = pickle.dumps(obj)
    with open(tmp,"wb") as fp:fp.write(bi)
    return True

def load(tmp):
    return pickle.load(open(tmp))