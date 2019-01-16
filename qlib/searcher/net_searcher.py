# -*- coding: utf8 -*- 
import os, json, sys

from qlib.net import to, parameters
from qlib.net.agents import AGS
from qlib.graphy import random_choice
from qlib.asyn import Exe
from qlib.log import LogControl
from qlib.data.sql import SqlEngine
from qlib.log import LogControl

try:
    from lxml.etree import HTML
except ImportError as e:
    LogControl.err("pip3 install pandas numpy")
    from bs4 import BeautifulSoup as HTML
    raise SyntaxError(0)

from urllib.parse import quote

LogControl.LOG_LEVEL = LogControl.OK
LogControl.LOG_LEVEL |= LogControl.FAIL
LogControl.LOG_LEVEL |= LogControl.INFO

class Searcher:
    """
    @proxy is setting request's proxy 
        sample: {
            'http': 'socks5://127.0.0.1:1080',
            'https': 'socks5://127.0.0.1:1080'
        }
    """
    config = {
        'search_url': '',
        'search_args': {},
    }

    def __init__(self, ssl=True, asyn=True, debug=False, db=False, database=None, proxy=False):
        self.url_pre = 'https://www.' if ssl else  'https//www.'
        self.search_name = self.__class__.__name__.lower()
        self.host = self.url_pre + self.search_name + '.com'
        self.agent = random_choice(AGS)
        self.asyn = None
        self.db = None
        self.debug =debug
        self.proxy = None
        self.grep_html_content = dict()

        if asyn:
            self.asyn = Exe(20)

        if proxy:
            LogControl.info("loading proxy..", end='')
            self.proxy = proxy #Mongo('local').find("setting")[0].get("proxy")
            if self.proxy:
                LogControl.ok("")


        if db:
            self.use_db = db
            db_path = os.path.join(os.getenv("HOME"), '.Search-engine-sqlite3-db.db') if not database else database
            self.db_path = db_path
            self.db = SqlEngine(database=db_path)
            if not self.db.table_list():
                self.db.create(self.search_name, query=str, content=str, type='web')

    def proxy_to(self, url, **kargs):
        if self.proxy:
            return to(url, proxy=self.proxy, **kargs)
        else:
            return to(url, **kargs)

    def xpath(self, html, *tags,exclude=None):
        xhtml = HTML(html)
        exclude = '[not(name()={})]'.format(exclude) if exclude else ''
        LogControl.info("//" + "//".join(tags) + exclude) if self.debug else ''
        for item in xhtml.xpath("//" + "//".join(tags) + exclude):
            yield item

    def ready(self, pre_url, *args, **options):
        pass

    def set_args(self, **kargs):
        return parameters(**kargs)

    def grep(self, url, grep_str, num):
        """
        async to grep url's content by grep_str, then will return (num, [*url]) -> callback

        """

        def _request():
            if  grep_str.lower().find(url.split(".").pop().lower()) != -1:
                return num, []
            res = self.proxy_to(url)
            if res.status_code / 100 ==2:

                encoding = res.encoding if res.encoding else 'utf8'
                LogControl.err(url, res.encoding)
                if res.content:
                    try:
                        h_content = res.content.decode(encoding)
                        return num, [ i.attrib.get('href') for i in HTML(h_content).xpath('//a') if i.attrib.get('href', '').find(grep_str) != -1]
                    except UnicodeError as e:
                        LogControl.err(e,'\n', res.encoding, encoding, url)
                        return num, []
                    except KeyError as e:
                        LogControl.err(e, url)
                        return num, []

                    
            else:
                return num, []
        if self.asyn:
            self.asyn.done(_request, self.save_grep)

    def save_grep(self, num, res):
        self.grep_html_content[num] = res

class Google(Searcher):
    pass


class Baidu(Searcher):
    
    config = {
        'translate_tag': "@class='op_dict_content'",
        'translate_url': "https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=0&rsv_idx=1&tn=baidu&wd={query}\
                            &rsv_pq=be1d9cdc00001f3a&rsv_t=e851oZevI5oKNV9F6W9CwY5wqlPyiK9Pqz0RLr7vdyawQru3NgKGrAcsizA\
                            &rqlang=cn&rsv_enter=1&rsv_sug3=20&rsv_sug2=0&inputT=2703&rsv_sug4=6023"
    }
    

    def translate(self, raw, to_lang='英文'):
        if not self.asyn:
            self.asyn = Exe(10)

        def query(url):
            res = to(url)
            en = res.encoding
            return res.status_code, res.content.decode(en) 

        def display(code, content):
            '''
            display content
            '''
            if int(code / 200) == 1:
                res = '\n'.join([ i.text for i in self.xpath(content, "div[%s]" % Baidu.config['translate_tag'], 'td', '*')])
                LogControl.ok(res)
            else:
                LogControl.fail("not found this")

        data = Baidu.config['translate_url'].format(query=quote(raw + ' ' + to_lang))
        self.asyn.done(query, display, data)



class BaiduTranslate(Searcher):

    def translate(self, raw, from_lan='en', to_lan='zh', detail=False, example=False):
        """
        
        detail : if detail == true will get more information.
        example : if example == true will get some sample.

        """
        self.host = 'fanyi.baidu.com/v2transapi'
        if not self.asyn:
            self.asyn = Exe(10)

        def query(data):
            res = to(self.host, method='post', data=data)
            return res.status_code, res.json()

        def tree_dict(d, func, key=None, lfunc=None):
            if isinstance(d, dict):
                for item in d:
                    tree_dict(d[item], func, key=item)
            elif isinstance(d, list):
                for item in d:
                    if lfunc:
                        tree_dict(item, lfunc)
                    else:
                        tree_dict(item, func)
            else:
                if key:
                    if key == 'dst':
                        LogControl.ok(d, txt_color='green', txt_attr=['underline','bold'])
                    else:
                        func(d, key)
                else:
                    func(d)

        def single_display(val, key=None, level=1):
            if key:
                LogControl.ok('[' + key + ']', '\t', val)
            else:
                LogControl.info('\t\t' + '   ' * level, val)

        def display(code, content):
            '''
            display content
            '''
            if int(code / 200) == 1:
                try:
                    tree_dict(content['trans_result'], single_display, 'base')
                    if detail:
                        tree_dict(content['dict_result'], single_display, 'dict')
                    if example:
                        tree_dict(content['liju_result'], single_display, 'sample')
                except KeyError:
                    LogControl.fail("Only: ", *content.keys())
                    for k in content:
                        LogControl.err(k, content[k])

            else:
                LogControl.fail("not found this")

        data = {
            'from': from_lan,
            'to': to_lan,
            'query': raw,
            'transtype':'translang', 
            'simple_means_flag':'3',
        }
        self.asyn.done(query, display, data)


class DuckDuckGo(Searcher):

    """
    @db=False/True : will use sqlite3 database .
    @database= 'path' # specify a path to save sqlite database file. default is ~/.Search-engine-sqlite3-db.db

    supported web , news, video, images
    """

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.search_url = 'https://duckduckgo.com/{use_js}?q={query}&{options}&vqd={vqd}'
        self.search_pools = {
            'web': 'd.js',
            'news': 'news.js',
            'video': 'v.js',
            'images': 'i.js',
        }

        self.search_args = 'l=wt-wt&p=1&s=0&a=h_&ct=US&ss_mkt=us&o=json' # output: json
        self.result = None
        self.last_search_type = 'web'
        self.next_url = None


    # def search_old(self, query, type='web'):
    #     """
    #     Abandoned function 
    #     """
    #     url_query = quote('+'.join(query.split()))

    #     def get_vqd(query):
    #         sss = to('http://duckduckgo.com/?q={query}&t=h_&ia={type}'.format(query=query, type=type)).content.decode('utf8')
    #         return sss[sss.rfind("vqd"):].split("&").pop(0).split("=").pop()

    #     args = self.search_args
    #     vqd = get_vqd(url_query)
    #     url = self.search_url.format(query=url_query, options=self.search_args, vqd=vqd) + '&sp=1&yhs=1'
    #     LogControl.info(url) if self.debug  else ''
    #     response = to(url, headers={'cookie':'ak=-1'})
    #     return response.status_code, response 

    def search(self, query, type='web', url=None):
        """
        supported web , news, video, images
        """

        def get_vqd(query):
            vqd_url = 'https://duckduckgo.com/?q={query}&t=h_&ia={type}'.format(query=query, type=type)
            LogControl.info(vqd_url) if self.debug else ''
            sss = self.proxy_to(vqd_url).content.decode('utf8')
            return sss[sss.rfind("vqd"):].split("&").pop(0).split("=").pop()


        if url is None:
            url_query = '+'.join(query.split())


            args = self.search_args
            vqd = get_vqd(url_query)
            url = self.search_url.format(use_js=self.search_pools[type], query=quote(query), options=args, vqd=vqd) + '&sp=1&yhs=1'

            

        LogControl.info(url) if self.debug  else ''
        response = self.proxy_to(url, headers={'cookie':'ak=-1'})
        if response.status_code / 100 == 2:
            
            self.last_search_type = type # record successful request's type
            json_content = ''
            try:
                json_content = response.json()
            except Exception:
                LogControl.err(response.content)
                sys.exit(0)

            self.result = json_content.get('results')
            self.deepanswer = json_content.get('deep_answers')
            self.next_url = self.result[-1].get('n')
            
            if self.use_db:
                self.db.insert(self.search_name, ['query', 'content', 'type'], query, json.dumps(json_content), type)

            return json_content.get('results')
        else:
            LogControl.err(response.status_code, 'err')
            return ''

    def next(self):
        """
        see next page 's result simplely.
        """

        if not self.next_url:
            LogControl.fail("no next url found !")
            return ''
        return self.proxy_to(self.host + self.next_url).json()

    def more(self):
        """
        like next
        """
        return self.next()

    def search_images(self, query):
        return self.search(query, type='iamges')

    def search_video(self, query):
        return self.search(query, type='video')

    def search_news(self, query):
        return self.search(query, type='news')
