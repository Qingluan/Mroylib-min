import time
from contextlib import contextmanager
from termcolor import cprint, colored
from qlib.log import LogControl
from qlib.io import input_default
from ._pipe import stdout

_do_some = lambda x: cprint("%s ..." % x, "yellow", end="")
_ok = lambda : cprint("\b\b\b ok", "green", attrs=['bold'])
_err = lambda : cprint("\b\b\b err", "red", attrs=['bold'])


@contextmanager
def just_info(info):
    try:
        _do_some(info)
        with stdout(None):
            yield
    except:
        _err()
    else:
        _ok()




def dict_cmd(dic):

    """
    for every dict do a interact input.
    """
    def _input(name):
        return input_default("[%s]: " %  colored(name,'red', attrs=['bold']), "Uknow")

    m = dic
    with LogControl.jump(LogControl.SIZE[0] - 1, 0):
        print("=" * LogControl.SIZE[1])
        for k in dic:
            v = _input(k)
            if v == "*None*":
                continue
            m[k] = v
        return m


def console_format():
    """
    simply format all
    """
    import sys
    from termcolor import colored
    from qlib.text import mark
    from qlib.io import GeneratorApi


    BLUE='\033[0;34m'
    GREEN='\033[0;32m'
    CYAN='\033[0;35m'
    RED='\033[0;31m'
    YELLOW='\033[0;36m'
    NC='\033[0m'

    args = GeneratorApi({
            'key':'set special key',
            'style':'set style',
        })


    for l in sys.stdin:
        cons = l.split()
        for i,v in enumerate(cons):
            if v.endswith(":"):
                cons[i] = colored(cons[i],"red")
                try:
                    cons[i+1] = GREEN + cons[i+1]
                    cons[-1] += NC
                except IndexError:
                    pass
            if v.startswith("http"):
                cons[i] = colored(cons[i], attrs=['underline'])

            if "@" in v and "." in v:
                cons[i] = colored(cons[i], 'yellow')
        
        line = NC + ' '.join(cons)
        if args.key:
            line = mark(line, args.key, attrs=['bold','blink'])
        print(line)
