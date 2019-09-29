import os, sys, socks, json, time
from qlib.data import dbobj, Cache
from qlib.file import ensure_path
from base64 import b64encode, b64decode
from functools import partial
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError
from telethon.tl.types import  MessageMediaDocument
from telethon.utils import get_display_name
import logging
import asyncio

from hashlib import sha256

USER_DB_PATH = os.path.expanduser("~/.config/user/.tel.sql")
E = os.path.exists
J = os.path.join
Ldir = os.listdir
CuFiles = lambda : Ldir(os.path.expanduser("~/"))

class Token(dbobj):

    async def connect(self, proxy=None, loop=None):
        api_id, api_hash = self.token.split(":")
        api_id = int(api_id)
        client = TelegramClient('user', api_id=api_id, api_hash=api_hash, proxy=proxy)
        await client.connect()
        return client
        

    async def send_code(self,phone=None, client=None, db=None, proxy=None, loop=None):
        if 'user.session' in CuFiles():
            logging.info("clear old session")
            os.remove('user.session')
        if not client:
            client = await self.connect(proxy, loop=loop)

        await client.sign_in(phone=phone)
        now_time = time.time()
        logging.info(f"{client._phone_code_hash} my phone: {phone}")
        return {
            'hash_code':client._phone_code_hash.get(phone),
            'now_time': now_time
        }


    async def login(self, phone, code, client=None, db=None, proxy=None,loop=None):
        # Ensure you're authorized
        if not client:
            client = await self.connect(proxy, loop=loop)
        if not db:
            ensure_path(USER_DB_PATH)
            db = Cache(USER_DB_PATH)
        
        now_time = time.time()
        if now_time - self.time > self.set_timeout:
            return 'retry', client

        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=self.hash_code)
        except ValueError as e:
            return str(e)
        except PhoneCodeExpiredError as e:
            return str(e)
        me = client.get_me()
        if me:
            return self.hash_code,client
        return 'error please retry',client

    @staticmethod
    def set_token(token, phone, client=None):
        ensure_path(USER_DB_PATH)
        c = Cache(USER_DB_PATH)
        if not c.query_one(Token):
            t = Token(tp='tel', token=token, phone=phone, hash_code='0', set_timeout=24*60)
            t.save(c)
        else:
            if client and client.is_user_authorized():
                t = Token(tp='tel', token=token, phone=phone, hash_code='0', set_timeout=24*60)
                t.save(c)


class Authentication:
    

    def __init__(self, db, proxy=None, loop=None):
        if isinstance(db, str):
            self.db = Cache(db)
        else:
            self.db = db
        if proxy:
            _,proxy = proxy.split("//")
            h,p = proxy.split(":")
            proxy = (socks.SOCKS5, h, int(p))
        self.proxy = proxy
        self.loop = loop

    def registe(self, phone, token, client=None):
        phone = self._get_phone_hash(phone)
        Token.set_token(token, phone, client=client)

    def _get_phone_hash(self, phone):
        o = sha256(phone.encode()).hexdigest()
        return sha256((o + phone).encode()).hexdigest()

    def sendcode(self, phone):

        phone_sha = self._get_phone_hash(phone)
        user = self.db.query_one(Token, phone=phone_sha)
        
        def update_user(res):
            user.time = res['now_time']
            user.hash_code = res['hash_code']
            user.save(self.db)
            logging.info("save hash_code: {res}".format(res=res))
        
        if user:

            logging.info("Found in User db: {phone} , {sha}".format(phone=phone,sha=phone_sha))
            f = asyncio.ensure_future(user.send_code(phone=phone,proxy=self.proxy, loop=self.loop))
            # asyncio.get_event_loop().run_until_complete(f)
            f.add_done_callback(lambda x: update_user(x.result()))
        else:
            logging.info("Not Found in User db: {phone} , {sha}".format(phone=phone, sha=phone_sha))


    def login(self, phone, code, callback):
        user = self.db.query_one(Token, phone=self._get_phone_hash(phone))

        def _middle_deal(x):
            w = x.result()
            if w[0] == 'retry':
                self.sendcode(phone)
                callback("token dispired,resend code to device!", w[1])
                
            else:
                callback(*w)

        if user:
            f = asyncio.ensure_future(user.login(phone, code, proxy=self.proxy, loop=self.loop))
            f.add_done_callback(_middle_deal)
            # logging.info(w)
            # = asyncio.get_event_loop().run_until_complete(f)
            # if msg == 'ok':
            # return user.hash_code
            # return False
        else:
            return False


    def if_auth(self, hash_code):
        if not hash_code: return False
        user = self.db.query_one(Token, hash_code=hash_code)
        if user:
            return True
        return False

