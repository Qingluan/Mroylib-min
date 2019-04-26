import random
from functools import wraps
from importlib import import_module
from urllib.parse import urlencode

import requests
from .config import RAW_HEADERS
from .agents import AGS, AG_NUM



def random_choice(lst, num=None):
    if not num:
        num = len(lst)
    ix = random.randint(0, num-1)
    return lst[ix]



# _methods = {}

def parameters(**pars):
    return urlencode(pars)


def set_setssion():
    return requests.session()


def session(url):

    def _wrap(func):
        session = requests.Session()

        @wraps(func)
        def run(*args, **kargs):
            session.get(url)
            kargs['session'] = session
            return func(*args, **kargs)
        return run
    return _wrap


def to(url, data=None, ssl=False, method='get', 
    proxy=None, 
    cookie=False, 
    agent=False,
    parser=None,
    **option):
    """
    @cookie [bool]
        can use cookie to scarp cookie , return session, response
    @proxy
        proxy={
            'https': 'socks5://127.0.0.1:1080',
            'http': 'socks5://127.0.0.1:1080'
        }

        ... 
        proxy='socks5://127.0.0.1:1080'
    @ssl [bool]
        can trans 'wwwxxx.xxxx' -> 'https://' xxxx
    @data [dict]
        post's payload
    @agent [bool /str]
        if True:
            will use random agent from {....} [841]
        if str:
            will set User-agent: agent directly
    @parser [str/None] 'bs4/lxml utf8/gbk'
        import it as parser.
    @options:
        @headners
    """
    User_Agent = None
    session = None
    parserlib = None
    encoding = 'utf-8'

    if agent == True:
        User_Agent = random_choice(AGS, AG_NUM)
    elif isinstance(agent, str):
        User_Agent = agent

    if parser in ("lxml", "bs", "bs4",):
        if parser.startswith("b"):
            parserlib = getattr(import_module("bs4"), 'BeautifulSoup')
        else:
            parserlib = getattr(import_module("lxml.etree"), 'HTML')

        if len(parser.split()) ==2:
            encoding = parser.split().pop()


    if not url.startswith("http"):
        if ssl:
            url = 'https://' + url
        else:
            url = 'http://' + url

    headers = RAW_HEADERS
    headers['User-Agent'] = User_Agent

    if 'headers' in option:
        for k in option['headers']:
            headers[k] = option['headers'][k]



    if cookie:
        session = requests.Session()
        m = getattr(session, method)
    else:
        m = getattr(requests, method)

    if 'session' in option:
        m = getattr(option['session'], method)

    if proxy:
        if isinstance(proxy, dict):
            pass
        elif isinstance(proxy, str):
            proxy = {
                'http': proxy,
                'https': proxy,
            }

        res = m(url, data=data, headers=headers, proxies=proxy)
    else:
        res = m(url, data=data, headers=headers)

    if parserlib:
        if res.status_code == 200:
            res = parserlib(res.content.decode(encoding, "ignore"), "html.parser")

    if cookie:
        return session, res
    return res


def network(url, data=None, method="get", **options):
    if "session" in options:
        m = getattr(options['session'], method)
    else:
        m = getattr(requests, method)

    if not data:
        req = m(url, headers=RAW_HEADERS)
    req = m(url, data=data, headers=RAW_HEADERS)

    def _wrap(func):
        func.res = req

        @wraps(func)
        def __call_back(*args, **kargs):
            return func(*args, **kargs)
        return __call_back
    return _wrap
