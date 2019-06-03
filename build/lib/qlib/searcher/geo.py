import os, sys
import random
from urllib.parse import urljoin
BASE_URL = os.getenv("Q_PATH")
sys.path.append(BASE_URL)


from qlib.asyn import Exe
from qlib.net import to
_exe = Exe(6)

_GEO_IP = [
    # 0: "http://ip-api.com/json/",
    "https://freegeoip.net/json/",
]

def check_local(ip):
    if ip == "127.0.0.1":
        return "hangzhou "
    _LOCAL_IP = {
        "10":True,
        "172" : lambda x: int(x.split(".")[1]) > 15,
        "192" : lambda x: int(x.split(".")[1]) == 168,
    }

    head = ip.split(".")

geo_api = _GEO_IP[random.randint(0, len(_GEO_IP)-1)]



def search_ip(*ips, callback=None, asyn=False):

    e = [urljoin(geo_api,ip) for ip in ips]
    # print(e)
    def get_json(url):
        return url, to(url).json()
    if asyn:
        for url in e:
            _exe.done(get_json, callback, url)
        return True

    res = _exe.map(get_json, e)
    if callback:
        callback(res)    
    else:
        return res
    #     return parse()
    # else:
        
    #     @_exe(callback=callback)
    #     def parse():
    #         @network(e, timeout=8)
    #         def _e():
    #             return _e.res.json()
    #         return _e()
    # return parse()
