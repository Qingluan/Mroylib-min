import os, sys, time
from termcolor import cprint, colored
from contextlib import contextmanager
from qlib.asyn import Exe
import logging

if os.getenv("DEBUG"):
    file = os.getenv('file')
    print(colored("log -> ",'red'), file,'debug start')
    logging.basicConfig(
        level=logging.DEBUG,
        filename=file,
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    )
else:
    file = os.getenv('file')
    logging.basicConfig(
        # level=logging.INFO,
        filename=file,
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    )


INFO = 0x08
ERR = 0x00
OK = 0x04
WRN = 0x02
FAIL = 0x01



def tag_print(tag, *args, tag_color='red', tag_attr=['bold', 'blink'], txt_color='grey', txt_attr=[], **kargs):
    tag = "[%s] " % colored(tag, tag_color, attrs=tag_attr)
    txt = colored(' '.join([str(i) for i in args]), txt_color, attrs=txt_attr)
    print(tag + txt, **kargs)




class LogControl:
    INFO = 0x08
    ERR = 0x00
    OK = 0x04
    WRN = 0x02
    FAIL = 0x01
    LOG_LEVEL = 0x04

    SIZE = tuple([ int(i) for i in os.popen("tput lines && tput cols ").read().split()])

    @staticmethod
    def save(p='civis'):
        """
        civis set will let p hidden.
        cnorm set will let p display.
        """
        os.system("tput sc  && tput " + p)

    @staticmethod
    def load():
        """
        civis set will let p hidden.
        cnorm set will let p display.
        """
        os.system("tput rc  && tput cnorm ")


    @staticmethod
    @contextmanager
    def jump(line, col, cur=True):
        """
        @cur can set cursor display or hidden
        @line, @col: cursor to jump.
        """
        try:
            os.system("tput sc && tput cup %d %d  && tput el " % (line, col))
            if not cur:
                os.system("tput civis")
            yield
        finally:
            os.system("tput rc")
            if not cur:
                os.system("tput cnorm")



    @staticmethod
    def cl(line):
        os.system("tput sc  && tput cnorm  && tput cup %d 0 && tput el  && tput rc && tput cnorm")

    @staticmethod
    def loc(line, col, el=False):
        os.system("tput cup %d %d " % (line, col))
        if el:
            os.system("tput el")

    @staticmethod
    def title(tag, info, time=5, el=True, asyn=False , **options):
        e = Exe(2)
        LogControl.save()
        LogControl.loc(1, 0, el)
        # print("-" * LogControl.SIZE[1], end="\r")
        os.system("tput el")
        LogControl.i(info, tag=tag, tag_color='green', **options)
        # print("-" * LogControl.SIZE[1])
        if asyn:
            e.timmer(time,  LogControl.load)
        else:
            LogControl.load()
        # 

    @staticmethod
    def bar(tag, info, line=0, el=True, **options):
        e = Exe(2)
        LogControl.save()
        LogControl.loc(line, 0, el)
        os.system("tput el")
        LogControl.i(info, tag=tag, tag_color='green', **options)
        LogControl.load()
        # e.timmer(time,  LogControl.load)

    @staticmethod
    def err(*args, txt_color='white', txt_attr=['bold'], **kargs):
        tag_print('err', *args, txt_color=txt_color, txt_attr=txt_attr, **kargs)

    @staticmethod
    def info(*args, txt_color='white', txt_attr=[], **kargs):
        if LogControl.LOG_LEVEL & INFO:
            tag_print('info', *args,  tag_color='cyan', tag_attr=['bold'],  txt_color=txt_color, txt_attr=txt_attr, **kargs)

    @staticmethod
    def wrn(*args, txt_color='white', txt_attr=[], **kargs):
        if LogControl.LOG_LEVEL & WRN:
            tag_print('warning', *args,  tag_color='yellow', tag_attr=['bold'],  txt_color=txt_color, txt_attr=txt_attr, **kargs)

    @staticmethod
    def ok(*args, txt_color='white', txt_attr=[], **kargs):
        if LogControl.LOG_LEVEL & OK:
            tag_print('√', *args,  tag_color='green', tag_attr=['bold'],  txt_color=txt_color, txt_attr=txt_attr, **kargs)

    @staticmethod
    def fail(*args, txt_color='white', txt_attr=[], **kargs):
        if LogControl.LOG_LEVEL & FAIL:
            tag_print('X', *args,  tag_color='red', tag_attr=['bold'],  txt_color=txt_color, txt_attr=txt_attr, **kargs)

    @staticmethod
    def i(*args, txt_color='white', txt_attr=[], tag='+', tag_color='cyan',**kargs):
        tag_print(tag, *args,  tag_color=tag_color, tag_attr=['bold'],  txt_color=txt_color, txt_attr=txt_attr, **kargs)

    @staticmethod
    @contextmanager
    def show_pro(line, col, sta, text):
        try:
            LogControl.save()
            LogControl.loc(0,0, True)
            LogControl.i(text, tag = sta, end='\r', tag_color='green', txt_color='yellow', txt_attr=['underline'])
            yield
        finally:
            print('\r')
            LogControl.load()

# @contextmanager
# def running(msg, interval=1):
#     running = True
#     # tag = ['| ', '/ ', '- ', '\\ ']
#     def _timer():
#         m = 0
#         while running:
#             print(' ...', end='')
#             sys.stdout.flush()
#             time.sleep(interval)

#     e = Exe(2)
#     try:
#         info(msg, end=' ')
#         e.submit(_timer)
#         with stdout(None) :
#             yield

#     except Exception as e:
#         running = False
#         err(e)
#         sys.exit(0)
#     else:
#         running = False
#         print("\b\b\b" , end='')
#         cprint('[%s]' % colored('√', 'green', attrs=['bold']))
#     finally:
#         running = False

def L(*contents, **kargs):
    on = ''
    c = ''
    end = '\n'
    column = ''
    row = ''
    if 'on' in kargs:
        on = kargs['on']
    if 'color' in kargs:
        c = kargs['color']
    if 'end' in kargs:
        end = kargs['end']
    
    if 'c' in kargs:
        column = kargs['c']

    if 'r' in kargs:
        row = kargs['r']

    res = ' '.join([str(i) for i in contents])
    if c:
        if on:
            res = colored(res, c, on)
        else:
            res = colored(res, c)

    if row and column:
        with LogControl.jump(row,column):
            sys.stdout.write(res + end)
            sys.stdout.flush()
            return len(res+end)

    sys.stdout.write(res + end)
    sys.stdout.flush()
    return len(res+end)


def show(*contents, **kargs):
    """
    @log: set True  to use logging module.
    @c: set color [red/yellow/blue/cyan/magenta]
    @a: set attrs '["underline"/"blink"/"bold"]'
    @on: set background [on_red/on_blue]
    """
    if 'log' in kargs and kargs['log'] == True:
        f = getattr(logging, kargs['k'])
        if 'c' in kargs:
            color = kargs['c']
        else:
            color = None

        if 'on' in kargs:
            on = kargs['on']
        else:
            on = None

        if 'a' in kargs:
            a = kargs['a']
        else:
            a = []

        s = colored("[+] ",'blue') + colored(' '.join([str(i) for i in contents]), color, on, attrs=a)
        f(s)

    else:    
        L("[+]",end=' ', color='blue')
        L(*contents, **kargs)


