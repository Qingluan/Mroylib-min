import os
from .__file import to_save, call_vim, file_search, fzip, zip_64, unzip_64, file264, b642file

__all__ = [
    'to_save',
    'call_vim',
    'file_search',
    'j',
    'fzip',
    'zip_64',
    'unzip_64',
    'file264',
    'b642file',

]


def j(*paths):
    import os
    from functools import reduce
    return reduce(os.path.join, paths)


def ensure_path(path):
    car, cdr = os.path.split(path)
    if os.path.exists(path):
        return True
    elif os.path.exists(car):
        os.mkdir(path)
    else:
        ensure_path(car)
