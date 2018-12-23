#!/usr/bin/env python3
import json
import time
import os
import sys
from multiprocessing import Process
from datetime import datetime
from urllib3.exceptions import ProtocolError
from requests import Request, Session
from requests.exceptions import ConnectionError
from html.parser import HTMLParser
from utils import Redis, BlockBlob, Log, snowballconf, newsessionwrap

delay = snowballconf["retrydelay"]
snowball_news_queue = snowballconf["newsqueue"]
snowball_comments_queue = snowballconf["commentsqueue"]
snowball_news_update_queue = snowballconf["newsupdatequeue"]
ua = snowballconf["ua"]
container = snowballconf["blobcontainer"]

redis = Redis()
blob = BlockBlob()
log = Log(tag="stdout")
newsession = newsessionwrap()

# category of current proccessing  article
category = -1

class SnowballNewsParser(HTMLParser):
    '''parse news page of xueqiu.com to retrieve the news json '''

    def __init__(self, cbfunc):
        super(SnowballNewsParser, self).__init__()
        self.cb = cbfunc

    def handle_data(self, data):
        if data.startswith("window.SNOWMAN_STATUS"):
            self.cb(data)

def news_process(data):
    '''process data json'''
    data = data[24: -53]
    newsinfo = json.loads(data)
    newscreated_timestamp = newsinfo["created_at"]
    newsedit_timestamp = newsinfo["edited_at"]
    newsid = newsinfo["id"]
    userid = newsinfo["user_id"]
    blobname = "%d.json" % newsid

    should_write_content = True
    blobexist = blob.bbservice.exists(container, blobname)

    if blobexist:
        should_write_content = False
        previouse_metadata = blob.bbservice.get_blob_metadata(container, blobname)
        # check if the last modified time to determine if we should update it
        should_write_content = not previouse_metadata or previouse_metadata != newsedit_timestamp

    if should_write_content:
        log.info("write blob for id: " + str(newsid))

        url = blob.writeText(container, blobname, data, metadata={
                "createtime": str(newscreated_timestamp),
                "editetime": str(newsedit_timestamp),
                "newsid": str(newsid),
                "authorid": str(userid),
                "category": str(category),
                "crawltime": str(int(time.time()*1000))
            })
        
        # push it to the update queue, then other can use this to do updating work
        redis.queue.push(snowball_news_update_queue, {
            "container": container,
            "blob": blobname,
            "url": url
        })

        log.info("url of [{}] is: {}".format(newsid, url))

    # after finished the news crawling, we push the id of article to crawl comments
    redis.queue.push(snowball_comments_queue, newsid)

def update_news():
    global category

    # we will use session to do request, so that requests will reuse the connection
    session = newsession()
    newsparser = SnowballNewsParser(news_process)

    while(True):
        job = redis.queue.pop(snowball_news_queue)

        if job:
            if (time.time() - float(job["time"])) > float(snowballconf["newsupdateinterval"]):
                try:
                    log.info("Crawling link: [{}]".format(job["link"]))
                    resp = session.get(job["link"])

                    if resp.status_code == 200:
                        category = job["category"]
                        newsparser.feed(resp.text)
                    else:
                        log.warn("Status code is [{}], push back to queue".format(resp.status_code))
                        
                except(ProtocolError, ConnectionError):
                    log.warn("exception while request, try again")
                    session = newsession()
                finally:
                    # we put it back to queue for next time update
                    job["time"] = time.time()
                    redis.queue.push(snowball_news_queue, job)
            else:
                redis.queue.push(snowball_news_queue, job)
        else:
            log.info("no job found, wait")
            time.sleep(delay)

if __name__ == "__main__":
    update_news()
    # worker_count = 1

    # if len(sys.argv) > 0:
    #     worker_count = int(sys.argv[0])

    # worker_count = min(worker_count, os.cpu_count)

    # workers = []

    # for i in range(0, worker_count):
    #     worker = Process(target=update_news)
    #     workers.append(worker)
    #     worker.start()

    # for woker in workers:
    #     woker.join()

    
    
