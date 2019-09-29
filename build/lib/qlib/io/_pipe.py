import sys
from contextlib import contextmanager


def get_output(console=True):
    blank_count = 0
    while True:

        l = sys.stdin.readline().rstrip() # remove \n
        if not l:
            if blank_count > 100:
                break
            blank_count += 1
            continue
        else:
            if blank_count > 0:
                blank_count -= 1

        if console:
            print(l)
        yield l


@contextmanager
def stdout(fp):
    stdo = sys.__stdout__
    try:
        sys.stdout = fp
        yield
    finally:
        sys.stdout = stdo
