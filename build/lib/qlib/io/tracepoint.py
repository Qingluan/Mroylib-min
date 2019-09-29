import functools
import requests
import time
import argparse


class TracePoint:
    classes = []
    funcs = []
    flow = []

    @staticmethod
    def clear():
        TracePoint.classes = []
        TracePoint.funcs = []
        TracePoint.flow = []        

    def __init__(self, cls, func, t):
        if cls not in TracePoint.classes:
            TracePoint.classes.append(cls)
        if cls not in TracePoint.funcs:
            TracePoint.funcs.append(func)
        TracePoint.flow.append(",".join([cls,t, func]))

    def render_flow(self):
        first = TracePoint.flow[0]
        recods = set()
        for no,i in enumerate(TracePoint.flow[1:]):
            cls,t, func = i.split(',',2)
            fcls,ft, ffunc = first.split(',', 2)
            fn = func.split("(")[0]
            ffn = ffunc.split("(")[0]
            label = "{l} -> {c}".format(l=ffn, c=fn)
            if label in recods:
                continue
            recods.add(label)
            lc,_ = self.get_color(cls, func)
            yield """{l} -> {c} [label="<span style='color:gray;'>{t}</span>|<span style='font-size:18px;color:red'>{no}</span>" labelType="html"  lineInterpolate=basis arrowheadStyle="fill: {lc}" style="stroke: {lc}; stroke-width: 1px;"];""".format(no=no,l=ffn, c=fn, t=time.ctime(float(t)), lc=lc)
            first = i


    def render_var(self, one):
        cls,t, func = one.strip().split(",", 2)
        color, color_f = self.get_color(cls, func)
        fn = func.split("(")[0]
        tmp = """{func_name} [labelType="html" label="<span style='font-size:28px;color:{color_f}'>{func}</span><span style='color:{color};'>class:{cls}</span>"];""".format(func_name=fn, color=color,color_f=color_f,cls=cls, func=func)
        return tmp

    def get_color(self, cls, func):
        base = 4096 // len(TracePoint.classes)
        base_f = 4096 // len(TracePoint.funcs)
        c = hex(base * TracePoint.classes.index(cls)).replace("0x", "#")
        c_f = hex(base_f * TracePoint.funcs.index(func)).replace("0x", "#")
        if len(c) < 4:
            c = c + '0'* (4- len(c))
        if len(c_f) < 4:
            c_f = c_f + '0'* (4- len(c_f))
        return c,c_f

    def __repr__(self):
        TEMP = """
digraph {
    /* Note: HTML labels do not work in IE, which lacks support for <foreignObject> tags. */
    node [rx=7 ry=7 labelStyle="font: 300 14px 'Helvetica Neue', Helvetica"]
    edge [labelStyle="font: 300 14px 'Helvetica Neue', Helvetica"]
    %s
}
"""
        fcon = "\n\t".join([self.render_var(i) for i in TracePoint.flow])
        lcon = "\n\t".join(self.render_flow())
        return TEMP % (fcon + lcon)

def trace(cls):

    def _func(func):
        @functools.wraps(func)
        def __run(*args, **kargs):
            print(func.__name__, args,"|" ,kargs)
            return func(*args, **kargs)

        return __run
    return _func


def trace_cls(method):
    def _trace_cls(cls):
    # Get the original implementation
        orig_getattribute = cls.__getattribute__

        # Make a new definition
        def new_getattribute(self, name):
            if name in cls.__dict__:
                f = getattr(cls, name)
                args = "(%s)" % ', '.join(f.__code__.co_varnames)
                t = str(time.time())

                if "http://" in method:
                    requests.post("http://localhost:12222/", data={
                        'class':cls.__name__,
                        'fun':name + args,
                        'time':t,
                    })
                else:
                    with open(method, "a+") as fp:
                        s = ",".join([cls.__name__,t,name + args])
                        fp.write(s + "\n")
            return orig_getattribute(self, name)

        # Attach to the class and return
        cls.__getattribute__ = new_getattribute
        return cls
    return _trace_cls

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l","--load",default=None,help="loadfile")
    parser.add_argument("--url", default='http://localhost:12222',help="debug server")

    args = parser.parse_args()
    with open(args.load) as fp:
        for l in fp:
            cls, t, func = l.strip().split(',', 2)
            requests.post(args.url, data={
                'class':cls,
                'fun':func,
                'time':t,
            })

if __name__ == '__main__':
    main()
