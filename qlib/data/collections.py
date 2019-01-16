import os
from queue import Queue

class GarbageLogCollections:
    # url ga
    # info ga
    # payload ga
    # result ga
    def gc(n, msg):
        m = n + ' ' + msg
        os.system("echo {} >> {}/{}.log".format(m, GA_DIR, n))


class Table:

    def __init__(self, data):
        self.data = self.xml2list(data)
        self.column = self.data[0]
        self.index = [i[0] for i in self.data]
        self.index_l = len(self.index)
        self.column_l = len(self.column)

    def xml2list(self, rawdata):
        data = []
        trs = rawdata.xpath(".//tr")
        if len(trs) >= 2:
            head_tr = trs[0]
            th = head_tr.xpath("./th")
            if th:
                data_head = [''.join([i2.strip() for i2 in i.itertext()]) for i in th]
                data.append(data_head)

            body_tr = trs[1:]
        else:
            body_tr = trs

        # data_head = [''.join([i2.strip() for i2 in i.itertext()]) for i in rawdata.xpath("./thead//th")]
        

        data_body = [[ ''.join([i2.strip() for i2 in i.itertext()]) for i in ii.xpath("./td") ] for ii in  body_tr]
        data += data_body
        
        return data

    def format(self):
        c = ''
        for l in self.data:
            c += '|'.join([i if i else '-' for i in l]) + '\n'
        for i in  '()':
            if i in c:
                c = c.replace(i, '\\'+i)
        res =  os.popen("echo  '{}' | column -t -s \| ".format(c)).read()
        # t = ""
        # for l in res.split("\n"):
        #     t += "|".join(l.split(":")) + "\n"

        # res =  os.popen("echo -n '{}' ".format(c)).read()
        return res

        

    def __getitem__(self, k):
        if isinstance(k, tuple):
            i,c = k
            i_l, c_l = None,None
            if i in self.index:
                i_l =  self.index.index(i)

            elif isinstance(i, int) and i < self.index_l:
                i_l = i
            elif isinstance(i, slice):
                i_l = i

            else:
                raise IndexError("not such Index",i)

            if c in self.column:
                c_l = self.column.index(c)
            elif isinstance(c, int) and c < self.column_l:
                c_l = c
            elif isinstance(c, slice):
                c_l = c
                
            else:
                raise IndexError("not such column",c)

            if isinstance(i_l, slice):
                return [i[c_l] for i in self.data[i_l]]

            return self.data[i_l][c_l]
        else:
            if k in self.index:
                k = self.index.index(k)
            elif isinstance(k, int) and k < self.index_l:
                return self.data[k]
            else:
                raise IndexError("not such index",k)


class Collections:
    q = Queue()
    url = set()

    def __init__(self, dir='.'):
        self.f = dir

    def add(self,type, content):
        getattr(Collections, type).add(content)

    def save(self, type,f):
        with open(os.path.join(self.f,f), 'w') as fp:
            for c in getattr(Collections, type):
                print(c, file=fp)

    def load(self, type, f):
        try:
            t = getattr(Collections, type)
        except AttributeError as e:
            setattr(Collections, type, set())
            t = getattr(Collections, type)
        with open(os.path.join(self.f,f), 'r') as fp:
            for l in fp:
                t.add(l)

    def ready(self, type):
        t = getattr(Collections, type)
        for c in t:
            Collections.put(c)

    @staticmethod
    def put(content):
        Collections.q.put(content)

    @staticmethod
    def get(timeout=6, block=True):
        return Collections.q.get(timeout, block)

