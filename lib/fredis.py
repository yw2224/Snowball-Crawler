import redis
import json
import os
import sys
from functools import wraps
from lib.config import redisconf
scriptroot = os.path.split(os.path.realpath(__file__))[0]


def jsonresult(func):
    '''Decorator to convert json string to dict object

    In VSCode it may not show the python doc correct, please press CTRL
    '''
    @wraps(func)
    def __jsonresultdecorator(*args, **kwargs):
        
        jsonstr = func(*args, **kwargs)
        
        if type(jsonstr) is str and jsonstr is not None:
            return json.loads(jsonstr)
        elif type(jsonstr) is bytes:
            return json.loads(jsonstr.decode('utf-8'))
        else:
            return jsonstr

    return __jsonresultdecorator

def getcontent(filepath):
    '''
    read file to get content
    '''
    with open(os.path.join(scriptroot,filepath), encoding="utf-8") as file:
        return file.read()


# lua script we need
HASH_GET_SCRIPT = getcontent("../redis/hash.get.lua")
HASH_GETALL_SCRIPT = getcontent("../redis/hash.getall.lua")
HASH_INSERT_SCRIPT = getcontent("../redis/hash.insert.lua")
HASH_KEYS_SCRIPT = getcontent("../redis/hash.keys.lua")
QUEUE_PUSH_SCRIPT = getcontent("../redis/queue.lpush.lua")
QUEUE_RANGE_SCRIPT = getcontent("../redis/queue.lrange.lua")
QUEUE_POP_SCRIPT = getcontent("../redis/queue.rpop.lua")
KEY_GET_SCRIPT = getcontent("../redis/keys.get.lua")
KEY_SET_SCRIPT = getcontent("../redis/keys.set.lua")
CLEAR_ALL_SCRIPT = '''return redis.pcall('flushall')'''


class Redis:
    def __init__(self, host=None, port=None, db=0):
        env = os.getenv("PYTHON_ENV", "production")

        if env == "development":
            rconf = redisconf["test"]
        else:
            rconf = redisconf["pro"]

        host = host or rconf["host"]
        port = port or rconf["port"]
        # print("connecting to " + host + ":" + str(port))
        self.__redis = redis.StrictRedis(host=host, port=port, db=db)
        self.__clearall = self.__redis.register_script(CLEAR_ALL_SCRIPT)
        self.hash = Hash(self.__redis)
        self.queue = Queue(self.__redis)
        self.keys = Keys(self.__redis)

    def clear(self):
        '''Clear data of redis'''
        self.__clearall()


class Hash:
    '''
    Create an instance of Hash to provide methods to operate Redis hash
    '''

    def __init__(self, sredis):
        # register lua script
        self.__hash_get = sredis.register_script(HASH_GET_SCRIPT)
        self.__hash_getall = sredis.register_script(HASH_GETALL_SCRIPT)
        self.__hash_insert = sredis.register_script(HASH_INSERT_SCRIPT)
        self.__hash_keys = sredis.register_script(HASH_KEYS_SCRIPT)

    @jsonresult
    def insert(self, hashtable, values):
        '''Insert the value with key.

        Args:
            hashtable: key of the hash item, such as snowball:news.list
            values: list of value to save

        Returns:
            json string contains insert state like:
            Eb'{"nochangeList":{},"updateList":{},"insert":1,"insertList":["1234556"],"update":0,"nochange":0}'

        Note:
            keys of value object must be fixed as following:
            \n{"k": "your key", "v": {your value object}, "t": timestamp, "tls": "tsl:test:t"}

        Examples:
            r = fredis.Redis()
            r.hash.insert("snowball:news.list",
            {
                "k": 123,
                "v": {"last": 12345},
                "t": 12345,
                "tls": ":getter",
                "tlp": "snowball:"
            }
            )
        '''

        if type(values) not in (list, tuple):
            values = [values]

        return self.__hash_insert(keys=[hashtable], args=[json.dumps(values)])

    def keys(self, hashtable):
        '''Get exist keys of hashtable

        Args:
            hashtable: key of the hash item

        Returns:
            list of keys of item, like ["1st key", "2nd key"]
        '''
        return [x.decode("utf-8") for x in self.__hash_keys(keys=[hashtable])]

    @jsonresult
    def getall(self, hashtable):
        '''Get all the items of hashtable

        Args:
            hashtable: key of the hash item
        '''
        return self.__hash_getall(keys=[hashtable])

    @jsonresult
    def get(self, hashtable, field):
        '''Get value of specified field of hash item

        Args:
            hashtable: key of the hash item
            field: name of the value in "v" (see hash.insert)

        Returns:
            hash object like {"value of 'k'": {"field": "value"}}

        Examples:
            r = fredis.Redis()
            result = r.hash.get("snowball:news.list", "last")
            value = result["mykey"]["field"]
        '''
        return self.__hash_getall(keys=[hashtable], args=[field])


class Queue:
    '''
    Create an instance of Queue to provide methods to operate Redis queue
    '''

    def __init__(self, sredis):
        self.__queue_in = sredis.register_script(QUEUE_PUSH_SCRIPT)
        self.__queue_pop = sredis.register_script(QUEUE_POP_SCRIPT)
        self.__queue_range = sredis.register_script(QUEUE_RANGE_SCRIPT)

    @jsonresult
    def push(self, queue, values):
        '''Push items to queue

        Args:
            queue: key of the queue
            values: value(s) to save

        Returns:
            json string of input values

        Examples:
            r = fredis.Redis()
            r.queue.push("myqueue", {"k":"v"})
        '''
        if type(values) not in (list, tuple):
            values = [values]

        return self.__queue_in(keys=[queue], args=[json.dumps(values)])

    @jsonresult
    def pop(self, queue):
        '''Pop an item from queue

        Args:
            queue: key of the queue

        Returns:
            json string of first item in the queue

        Examples:
            r = fredis.Redis()
            v = r.queue.pop("myqueue")
        '''
        return self.__queue_pop(keys=[queue])

    @jsonresult
    def list(self, queue, start, stop):
        '''Get the range of items from queue

        Args:
            queue: key of the queue
            start: start index of the range
            stop: end index of the range

        Returns:
            list of items

        Examples:
            r = fredis.Redis()
            vlist = r.queue.list("myqueue", 2, 5)
        '''
        return self.__queue_range(keys=[queue, start, stop])


class Keys:
    def __init__(self, sredis):
        self.__get_key = sredis.register_script(KEY_GET_SCRIPT)
        self.__set_key = sredis.register_script(KEY_SET_SCRIPT)

    @jsonresult
    def get(self, *args):
        '''get values of specified keys
        
        Args:
            list of key to retrieve value

        Returns:
            dict object contains exist keys

        Examples:
            r = fredis.Redis()
            mydict = r.keys.get("mykey1", "mykey2")
        '''
        
        if type(args) not in (list, tuple):
            args = [args]
        return self.__get_key(args=[json.dumps(args)])

    def set(self, **kvargs):
        '''set key-value pair into redis
        
        Args:
            a dict object to insert

        Returns:
            number that inserted
        
        Examples:
            r = fredis.Redis()
            r.keys.set(mykey="myvalue", mykey2="myvalue2")
        '''
        kv = [{"k":k, "v":v} for k, v in kvargs.items()]
        
        return self.__set_key(args=[json.dumps(kv)])
