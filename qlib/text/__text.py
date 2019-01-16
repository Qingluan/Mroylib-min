import re
import time
from qlib.io import get_output
from termcolor import colored


def text2dict(text):
    dit = {}
    col = True
    last_key = None
    for line in text.split("\n"):
        if not col:
            r_f = line.rfind('"""')
            if r_f != -1:
                dit[last_key] += '\n                 ' + line[:r_f]
                col = True
                continue
            else:
                dit[last_key] += '\n                 ' + line
                continue

        if not line.strip():
            continue

        arg = line.split()
        if len(arg) == 1:
            dit['title'] = arg[0]
            continue

        if arg[1].startswith("[") and arg[-1].endswith("]"):
            content_arg = line[line.find('[')+1: line.rfind(']')].split()
            dit[arg[0]] = content_arg
            continue

        if arg[1].startswith('"""') and col:
            l_f = line.find('"""')
            r_f = line.rfind('"""')
            if l_f == r_f:
                dit[arg[0]] = line[l_f + 4:]
            else:
                dit[arg[0]] = line[l_f + 4: r_f]
            col = False
            last_key = arg[0]
            continue

        dit[arg[0]] = arg[1] if len(arg) == 2 else " ".join(arg[1:])
    return dit


def symbol(c):
    return str(id(c) + time.time())


def no_symbol(text, repl=' '):
    return re.sub(r'[^\w^d^\n\s]', repl, text)


def text_to_tree(dealnode, dealrealtion, sleep=None, console=False):
    l_num = [0]
    t = {0: ['r']}
    ls = []
    for l in get_output(console):
        search_res = re.search(r'\w', l)
        if not search_res:
            continue
        s_n = search_res.span()[0]

        if s_n in l_num:
            pass
        else:
            l_num.append(s_n)
            l_num.sort()
        fathre_index = l_num.index(s_n) - 1
        fathre_index = fathre_index if fathre_index >= 0 else 0
        # print(l_num[fathre_index])
        father = t[l_num[fathre_index]][-1]
        node = (symbol(l), l[s_n:])

        if s_n in t:
            t[s_n].append(node)
        else:
            t[s_n] = [node]
        ls.append((father, node, s_n))
        dealnode(node)
        if sleep:
            time.sleep(sleep)

    for link in ls:
        dealrealtion(*link)
        if sleep:
            time.sleep(sleep)


# def text_to_map(net)

def mark(raw, mark, color='red', on=None, attrs=[]):
    return re.sub(mark, colored(mark, color, on, attrs), raw)
