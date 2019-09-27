import concurrent
import asyncio
import time
import json
from termcolor import colored
from qlib.data import Cache, dbobj

import aiohttp

class Aio:
    
    # Pro = concurrent.futures.ProcessPoolExecutor(4)
    Pro = concurrent.futures.ThreadPoolExecutor(10)
    RUN_DEAL_QUEUES = {}
    CALLBACKS = {}

    @property
    def name(self):
        return self.__class__.__name__

    def get_queue(self, name):
        return Aio.RUN_DEAL_QUEUES.get(name)

    def get_handle(self, name):
        return Aio.CALLBACKS.get(name, self.show)

    async def show(self, msg):
        del msg['tp']
        print(msg)

    def loop_callback(self):
        queue = self.get_queue(self.name)
        print("loop-callback:", self.name)
        while 1:
            msg = {}
            if not queue.empty():
                msg = queue.get_nowait()
            else:
                time.sleep(0.3)
                continue
            if isinstance(msg, str):
                print(self.name, "callback: Stop")
                break
            else:
                # print('after:',msg)
                if msg['tp'] == 'todo':
                    self.queue.put_nowait(msg)
                    time.sleep(0.4)
                    continue
                elif msg['tp'] == self.name:
                    self.Exe.submit(self._handel_callback, msg)
                else:
                    other_queue = self.get_queue(msg['tp'])
                    if other_queue:
                        other_queue.put_nowait(msg)
                    else:
                        self.Exe.submit(self._handel_callback, msg)
                        time.sleep(0.1)
    
    def go(self, name, msg):
        q =Aio.RUN_DEAL_QUEUES.get(name)
        if q:
            q.put_nowait(msg)


    async def _handle(self, queue, msg):
        try:
            result = await self.handle(msg)
            if not result:
                return
            if not isinstance(result, dict):
                res = {'tp': "show", "result": result}
            else:
                if not 'tp' in result:
                    result['tp'] = self.name
                res = result
            await queue.put(res)
        except Exception as e:
            self.deal_err(e)
    
    def deal_err(self, err):
        self.queue.put_nowait({"tp":"ErrHandle", "msg":self.name + " | " + str(err) })

    def _handel_callback(self, msg):
        try:
            self.after(msg)
        except Exception as e:
            self.deal_err(e)

    def after(self, msg):
        raise NotImplementedError("handle-callback")

    async def handle(self, msg):
        raise NotImplementedError("handle")        

    @classmethod
    async def loop(cls):
        END_ALL = False
        while 1:
            tasks = []
            for name in Aio.RUN_DEAL_QUEUES:
                queue = Aio.RUN_DEAL_QUEUES[name]
                if not queue.empty():
                    msg = await queue.get()
                    if isinstance(msg, str):
                        queue.put_nowait("End")
                        END_ALL = True
                        continue
                    if msg['tp'] != 'todo':
                        queue.put_nowait(msg)
                    else:
                        handle = Aio.CALLBACKS[name]
                        print(name, "recv:")
                        tasks.append(asyncio.Task(handle(queue, msg)))
                        # await asyncio.sleep(0.1)
            if len(tasks) >0:
                await asyncio.gather(*tasks)
            else:
                time.sleep(0.1)
            if END_ALL:
                break

    @classmethod
    def Stop(cls):
        for name, queue in Aio.RUN_DEAL_QUEUES.items():
            print("Stop :", name)
            queue.put_nowait("Stop")
            queue.put_nowait("Stop")
        Aio.Pro.shutdown()
    # async def loop(self):
    #     queue = self.get_queue(self.name)    
    #     print("loop:", self.name)
    #     msg = {}
    #     while 1:

    #         msg =await  queue.get()
    #         if isinstance(msg, str):
    #             break
    #         print("recv:",msg)
    #         if msg['tp'] != 'todo':
    #             queue.put_nowait(msg)
    #             continue
    #         asyncio.ensure_future(self._handle(queue, msg))
    #         await asyncio.sleep(0.1)
            
    def __init__(self):
        self.Exe = concurrent.futures.ThreadPoolExecutor(15)
        self.queue = asyncio.queues.Queue()
        Aio.RUN_DEAL_QUEUES[self.name] = self.queue
        Aio.CALLBACKS[self.name] = self._handle
    

    @classmethod
    def StartUp(cls, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        Aio.Pro.submit(cls._run, loop)
        
    @classmethod
    def _run(cls, loop):
        if loop.is_running():
            loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(Aio.loop())
        except Exception as e:
            print(e)
    
    @classmethod
    def regist(cls, instance):
        if 'ErrHandle' not in Aio.RUN_DEAL_QUEUES:
            raise Exception("must ensure Err handle")
        cls.Pro.submit(instance.loop_callback)


    @classmethod
    def pendding(cls, name, **kargs):
        if name in Aio.RUN_DEAL_QUEUES:
            print("->", name)
            kargs.update({
                'tp':"todo",
            })
            qu = Aio.RUN_DEAL_QUEUES.get(name)
            qu.put_nowait(kargs)


class ErrHandle(Aio):
    async def handle(self, msg):
        print(colored("[Err]", 'red'), colored(msg['msg'],'red', attrs=['underline']))
    
    def after(self, msg):
        print(colored("[Err]", 'red'), colored(msg['msg'],'red', attrs=['underline']))

def regist(cls, *args, **kargs):
    if 'ErrHandle' not in Aio.RUN_DEAL_QUEUES:
        errH = ErrHandle()
        Aio.regist(errH)
    instance = cls(*args, **kargs)
    Aio.regist(instance)


# Example

class Http(Aio):
    
    async def handle(self, msg):
        url = msg['url']
        headers = msg.get('headers', {'User-Agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'})
        method = msg.get("method", "get")
        if method == 'get':
            return await self.get(url, headers)
        elif method == 'post':
            data = msg["data"]
            return await self.post(url, data, headers)
        else:
            raise Exception("must a method")

    async def get(self, url, headers=None):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.text()

    async def post(self, url, data,headers=None):
        async with aiohttp.ClientSession() as session:
            async with session.post(url,data=data, headers=headers) as response:
                return await response.text()

    def after(self, msg):
        raise NotImplementedError("must how to deal")


class SaveAsJson(Aio):
    
    def __init__(self, save_json_file):
        super().__init__()
        self.save_json_file = save_json_file

    async def handle(self, msg):
        if 'data' in msg:
            data = msg['data']
            assert isinstance(data, dict) is True
            if 'dest' in msg:
                with open(msg['dest'], 'a+') as fp:
                    fp.write(json.dumps(data))
            else:
                with open(self.save_json_file, 'a+') as fp:
                    fp.write(json.dumps(data))
    
    def after(self, msg):
        pass


# class SaveAsSqlite(Aio):
#     def __init__(self, db):
#         super().__init__()
#         self.db = db
        

#     async def handle(self, msg)