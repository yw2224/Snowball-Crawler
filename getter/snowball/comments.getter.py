#!/usr/bin/env python3
'''Crawling comments for news from xueqiu.com, comments will be saved to a yaml file
'''
import json
import time
from datetime import datetime
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError
from yaml import dump
from utils import Log, Redis, AppendBlob, openfile, snowballconf, newsessionwrap

homepage = snowballconf["homepage"]
delay = snowballconf["retrydelay"]
comments_queue = snowballconf["commentsqueue"]
ua = snowballconf["ua"]
container = snowballconf["blobcontainer"]
api = snowballconf["commentsapi"]
comments_hash_name = "snowball:newscomments"
comment_update_queue = snowballconf["commentsupdatequeue"]

expired_time = 10*24*60*60 # in seconds
min_update_interval = 10*60 

redis = Redis()
blob = AppendBlob()
log = Log(tag="stdout")

newsession = newsessionwrap(with_cookie=True)

comment_state = dict(
    k = -1,
    v = dict(
        latest_page = 1, # latest page we have crawled. NOTE: we crawling comments by asc order,
        latest_id = -1,
        lastupdate = 0,
        count = 0
    ),
    t = 0,
    tls = ":getter",
    tlp = "snowball:comments:"
)

def updatecomments(session, newsid, page):
    '''crawling and upload the comments until reach the lastid'''
    result = []

    try:
        resp = session.get(api % (newsid, page))
    except(ProtocolError, ConnectionError):
        log.warn("session timeout, restart")
        session = newsession()
        return result

    if resp.status_code == 200:
        commentsinfo = redis.hash.get(comments_hash_name, "latest_id")
        latestid = newsid in commentsinfo and commentsinfo.get("latest_id", -1) or -1 

        data = json.loads(resp.text)

        if len(data["comments"]) == 0:
            log.info("reach the end of api")
            return result # the end of api

        
        for comment in data["comments"]:
            id = comment["id"]
            
            if latestid != -1 and id < latestid:
                continue # avoid dup comments

            log.info("append comment [%s] to news [%s]" % (id, newsid))

            # here we split the comment, and append to the blob, so that we can keep the list of comments without parsing the blob
            result.append("\n---\n%s\n...\n" % dump(comment, allow_unicode=True))
            latestid = max(latestid, id)
            
        # save crawling state
        comment_state["k"]=newsid
        comment_state["v"]["latest_id"]=latestid
        comment_state["v"]["latest_page"]=page
        comment_state["v"]["lastupdate"]=time.time()
        comment_state["v"]["count"]=data["count"]
        comment_state["t"]=int(time.time() * 1000)

        redis.hash.insert(comments_hash_name, comment_state)
    else:
        log.warn("fail to get comments, status code: %s" % resp.status_code)
        # we do nothing here, as we will push this news to the queue again for updating

    return result

if __name__ == "__main__":
    session = newsession()

    try:
        while(True):
            newsid = redis.queue.pop(comments_queue)

            if newsid:
                newsid = str(newsid)
                commentsinfodict = redis.hash.get(comments_hash_name, "latest_id")
            
                if newsid in commentsinfodict:
                    lastupdate = commentsinfodict[newsid]["lastupdate"]
                    latestpage = commentsinfodict[newsid]["latest_page"]
                else:
                    lastupdate = None
                    latestpage = 1

                if lastupdate:
                    delta = time.time() - lastupdate
                    
                    if delta > expired_time:
                        # if a news have no comment for amount time, then skip it
                        log.info("the news [%s] is expired, remove it." % newsid)
                        continue
                    elif delta < min_update_interval:
                        log.info("too frequently for news [%s], wait." % newsid)
                        redis.queue.push(comments_queue, int(newsid))
                        continue   
                
                page = latestpage

                results = []

                while True:
                    ret = updatecomments(session, newsid, page)
                    
                    if ret is None or len(ret) == 0:
                        break
                    
                    results.append("".join(ret))

                    page += 1

                if len(results) > 0:
                    blobname = "%s.yaml" % newsid

                    if not blob.abservice.exists(container, blobname):
                        blob.create(container, blobname)

                    blob.appendText(container, blobname, "".join(results))

                    log.info("{} uploaded.".format(blobname))

                # check if there is any update
                if page > latestpage:
                    redis.queue.push(comment_update_queue, int(newsid))

                # put this news to the end of queue for next time update
                redis.queue.push(comments_queue, int(newsid))
            else:
                log.info("no job found, waiting")
                time.sleep(delay)
    except(KeyboardInterrupt,):
        redis.queue.push(comments_queue, int(newsid))