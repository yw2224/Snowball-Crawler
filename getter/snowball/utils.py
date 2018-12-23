'''Utilities for xueqiu.com'''
import sys
import os
from requests import Session
from yaml import load
scriptroot = os.path.split(os.path.realpath(__file__))[0]
sys.path.insert(0, os.path.join(scriptroot, "..", ".."))

from lib.log import Log
from lib.fredis import Redis
from lib.blob import AppendBlob, BlockBlob

def openfile(relpath, mode="r", encoding="utf-8"):
    '''open file by relative path'''
    return open(os.path.join(scriptroot, relpath), mode=mode, encoding=encoding)

# config for xueqiu.com
snowballconf = load(openfile("../../config/snowball.yml"))

def newsessionwrap(with_cookie=False):
    '''get a function to create a new session'''
    def newsession():
        session = Session()
        session.headers.update({"User-Agent":snowballconf["ua"]})

        if with_cookie:
            session.get(snowballconf["homepage"])
        
        return session
    
    return newsession