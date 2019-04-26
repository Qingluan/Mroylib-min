import grequests as async
import time
from qlib.log import show
# If using requests > v0.13.0, use
# from grequests import async

urls = [
    'http://www.baidu.com',
    'http://www.sohu.com',
    'http://www.qq.com',
    'http://www.bilibili.tv'
]

# A simple task to do to each response object
def do_something(response, *args,**kargs):
    # show(args, kargs)
    show(time.asctime(),response.url, len(response.content), color='green', a=['underline'])
    show(kargs)
    # return response.url
    # show(args, kargs)
    

def exception_handler(request, exception):
    show(exception)
    print("Request failed")

# A list to hold our things to do via async
async_list = (async.get(u,hooks={'response': [do_something]}) for u in urls)
    

res = async.map(async_list, exception_handler=exception_handler)