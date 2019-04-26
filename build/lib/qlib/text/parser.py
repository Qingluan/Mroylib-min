import re, itertools
from string import digits, ascii_letters ,punctuation, whitespace


def extract_ip(string):
    return re.findall(r'\D((?:[1-2]?\d?\d\.){3}[1-2]?\d?\d)', string)

def extract_http(string):

    return re.findall(r'(https?\://[\w\.\%\#\/\&\=\?\-]+)', string)

def extract_host(string):
    return re.findall(r'((?:www\.|mail\.|ftp\.|news\.|blog\.|info\.)?\w[\w\.\-]+(?:\.com|\.net|\.org|\.cn|\.jp|\.uk|\.gov))', string)    

# \-\.\/\;\#\(\)
def extract_dict(string, sep='\n'):
    dicts = {}
    for line in string.split(sep):
        ds = re.findall(r'^\s*([\w\s\-\.\/]+)\s*[\:\=]\s*([\w\S\s]+)\s*$', line )
        if ds  and ds[0][1].strip() and ds[0][1].strip() not in (':','=',):
            dicts[ds[0][0].strip()] = ds[0][1].strip()
    
    return dicts


def extract_fuzzy_regex(string):
    res = ''
    res_k = []
    last = ''
    last_k = 0
    words_re = r'[\s\w\-\_\.\'\"]'
    for k, group in itertools.groupby(string):
        if k in ascii_letters + digits + "-_ '\"\." :
            l, k = words_re, len(list(group))
            if l == last:
                last_k += k
            else:
                # res.append([last, last_k])
                res_k.append(last_k)
                if last == words_re:
                    res +=  "(%s+)" % last
                else:
                    res += last + "+?"
                last_k = k
                last = l

        elif k in punctuation:
            l, k = r'\%s' % k , len(list(group))
            if l == last:
                last_k += k
            else:
                # res.append([last, last_k])
                res_k.append(last_k)
                if last == words_re:
                    res +=  "(%s+)" % last
                else:
                    res += last + "+?"
                last_k = k
                last = l


        else:
            l, k = '.', len(list(group))
            if l == last:
                last_k += k
            else:
                # res.append([last, last_k])
                res_k.append(last_k)
                if last == words_re:
                    res +=  "(%s+)" % last
                else:
                    res += last + "+?"
                last_k = k
                last = l

    # res.append([last, last_k])
    if last == words_re:
        res +=  "(%s+)" % last
    else:
        res += last + "+?"
    return res[2:], res_k[1:]

def extract_regex(string):
    res =[]
    last = ''
    last_k = 0
    for k, group in itertools.groupby(string):
        if k in ascii_letters:
            l, k = '\w', len(list(group))
            if l == last:
                last_k += k
            else:
                res.append([last, last_k])
                last_k = k
                last = l

        elif k in digits:
            l, k = '\d', len(list(group))
            if l == last:
                last_k += k
            else:
                res.append([last, last_k])
                last_k = k
                last = l

        elif k in punctuation:
            l, k = '\%s' % k , len(list(group))
            if l == last:
                last_k += k
            else:
                res.append([last, last_k])
                last_k = k
                last = l

        elif k in whitespace:
            l, k = '\s', len(list(group))
            if l == last:
                last_k += k
            else:
                res.append([last, last_k])
                last_k = k
                last = l
        else:
            l, k = '.', len(list(group))
            if l == last:
                last_k += k
            else:
                res.append([last, last_k])
                last_k = k
                last = l
    res.append([last, last_k])
    return [i[0] for i in res[1:]], [i[1] for i in res[1:]]


def extract_table(string, sep=" |,"):
    last_len = 0
    row = 0
    table = []
    last_re_str = ''
    table_dim = []
    for line in string.split('\n'):
        if not line.strip():continue
        re_str, dim = extract_fuzzy_regex(line)
        # print(re_str)
        if re_str == last_re_str:
            r = re.findall(re_str, line)[0]
            if isinstance(r, str):
                table.append(r)
            else:
                table.append(list(r))
            # print('table', r, re_str)
            
            table_dim.append(dim)
            row += 1
            
        else:
            last_re_str = re_str
            if not table:
                row = 1
                table = []
                table_dim = []
                continue


            
            

            if isinstance(table[0] , str):
                t = [ [i2 for i2 in  re.split(sep, i)  if i2 ] for i in table]
                cols = max([ len(i) for i in t])
                t = [ i for i in t if len(i) == cols]

                if len(t) >1:
                    yield t
            else:
                if table[0]:
                    yield table

            row = 1
            table = []
            table_dim = []

    
    if isinstance(table[0] , str):

        t = [ [i2 for i2 in  re.split(sep, i)  if i2 ] for i in table]
        cols = max([ len(i) for i in t])
        t = [ i for i in t if len(i) == cols]
        if len(t) >1:
            yield t
    else:
        if table[0]:
            yield table        
            




