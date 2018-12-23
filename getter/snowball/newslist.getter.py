import sys
import json
import time
import datetime
import argparse
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError
from utils import Log, Redis, snowballconf, newsessionwrap

newsession = newsessionwrap(with_cookie=True)
log = Log(tag="stdout")
redis = Redis()
requestcount = 0
maxcount = -1

options = {
    "since_id": -1,
    "max_id": -1,
    "count": 10,
    "category": -1
}

def getfield(tablename, category, key, defaultvalue):
    '''Get last news id(index) for specified category'''
    ret = redis.hash.get(tablename, key)
    return str(category) in ret and ret[str(category)].get(key, defaultvalue) or defaultvalue

def getnewslist(session):
    global requestcount
    reachtarget = False

    try:
        # used for testing to limit the request count
        requestcount = requestcount + 1
        if maxcount > -1 and requestcount > maxcount:
            exit(0)

        lastid = getfield(snowballconf["newslisthashtable"], options["category"], "last_id", -1)
        latestid = getfield(snowballconf["newslisthashtable"], options["category"], "latest_id", -1)

        if lastid > -1:
            #crawl down with max_id, usually this will be the nextmaxid from result
            options["since_id"] = -1
            options["max_id"] = lastid
        else:
            #crawl up with since_id
            options["max_id"] = -1
            options["since_id"] = latestid

        resp = session.get(snowballconf["newslistapi"], **{"params":options})

        if resp.status_code == 403:
            log.warn("Session timeout, re-try. category: [{}], last_id: [{}], latest_id: [{}]".format(options["category"], lastid, latestid))
            session = newsession()
        elif resp.status_code == 200:
            newslist = json.loads(resp.text)
            
            for item in newslist["list"]:
                index = item["id"]

                # stop saving the news if we reach the latest one
                if index == latestid:
                    reachtarget = True
                    break
                
                data = json.loads(item["data"])
                link = '{}{}'.format(snowballconf["homepage"], data["target"])
                timestamp = resp.headers.get("date", time.time())
                latestid = max(latestid, index)

                if lastid > -1:
                    lastid = newslist.get("next_max_id", -1)

                redis.queue.push(snowballconf["newslistqueue"], {
                    "link": link,
                    "id": index,
                    "time": 0, # 0 means new post
                    "category": options["category"]
                })

                log.info('[timestamp]: {},[category]: {}, [id]: {}, [link]: {}'.format(timestamp, options["category"], index, link))

            redis.hash.insert(snowballconf["newslisthashtable"], {
                "k": options["category"],
                "v":{
                    "last_id": lastid,
                    "latest_id": latestid
                },
                "t":time.time(),
                "tls": ":getter",
                "tlp": "snowball:newslist"
            })
    except(ConnectionError, ProtocolError):
        session = newsession()
        log.warn("Connect failed, retry")

    return reachtarget

def startcrawling():
    session = newsession()

    while True:
        isend = getnewslist(session)
        
        if isend:
            log.info("Waiting for next time crawling, category: [{}]".format(options["category"]))
            time.sleep(snowballconf["retrydelay"])


if __name__ == "__main__":
    # arguments definition
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--limit", type=int, default=-1, help="Downloading with count limitation")
    parser.add_argument("-c", "--category", type=int, default=-1, help="-1: 头条, 6:直播, 105:沪深, 111:房产, 102:港股, 104:基金, 101:美股, 110:保险")
    parser.add_argument("-i", "--interval", type=int, default=5*60, help="update interval, in seconds")
    parser.add_argument("--count", type=int,default=10, help="news count of each request")

    args = parser.parse_args()

    options["category"] = args.category
    options["count"] = args.count

    maxcount = args.limit
  
    startcrawling()




    