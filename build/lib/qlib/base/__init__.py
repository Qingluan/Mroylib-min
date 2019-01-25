import random
from os import getenv as __getenv
from os.path import join as J

HOME = __getenv("HOME")
SHELL = __getenv("SHELL").split("/").pop()
__all__ = [
    'HOME',
    'J',
    'SHELL',
]


def random_choice(lst, num=None):
  if not num:
      num = len(lst)
  ix = random.randint(0, num-1)
  return lst[ix]

