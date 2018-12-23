import json
import yaml
import pickle
import time
import datetime
import re
from urllib3.exceptions import ProtocolError
from requests.exceptions import ConnectionError
import jieba
from os import path

scriptroot = path.split(path.dirname(__file__))[0] + "/files/"
jieba.load_userdict(scriptroot + "userdict.txt")

from lib.log import Log
from lib.fredis import Redis
from lib.blob import AppendBlob, BlockBlob


host = "10.172.136.41"
port = "30002"
delay = 30 # in seconds
default_active_window = 604800 # a week

redis_news_hkey = "snowball.newschema"
redis_comments_hkey = "snowball.commentschema"
redis_news_list = "snowball.updatenews"
redis_comments_list = "snowball.updatecomments"

schema_container = "snowballschema"
schema_url = "https://financestore.blob.core.windows.net/snowballschema/"
user_container = "snowballuser"
user_url = "https://financestore.blob.core.windows.net/snowballuser/"
text_container = "snowballtext"
text_url = "https://financestore.blob.core.windows.net/snowballtext/"
source_container = "xueqiu"
source_url = "https://financestore.blob.core.windows.net/xueqiu/"


log = Log(tag="stdout")
redis = Redis(host=host, port=port)
blob = BlockBlob()
index = redis.hash.get(redis_news_hkey, "index")
index = int(index["index"])



def related_code(text):
    code = set()
    with open(scriptroot + "dict.pk", 'rb') as f:
        dict = pickle.load(f)

    words = jieba.cut(text)
    for word in words:
        if word in dict.keys():
            code.add(dict[word])

    return list(code)


def company_dict():
    dict = {}
    with open(scriptroot + "sl.json") as f:
        for i, l in enumerate(f.readlines()):
            js = json.loads(l)
            print(str(i) + ": " + str(js))

            stock_name = js["stock_name"]
            company_name = js["company_name"]
            code = js["code"]

            dict[stock_name] = code
            dict[company_name] = code

    pickle.dump(dict, open(scriptroot + "dict.pk", "wb"))



def valid_date(str):
    try:
        time.strptime(str, "%Y-%m-%d  %H:%M")
        return str
    except:
        if "今天" in str:
            str = str.replace("今天", "")
            t = time.strftime("%Y-%m-%d")
            return t + str
        elif "分钟前" in str:
            str = str.replace("分钟前", "")
            t = int(str)
            return (datetime.datetime.now() - datetime.timedelta(minutes=t)).strftime("%Y-%m-%d %H:%M")
        elif "秒前" in str:
            str = str.replace("秒前", "")
            t = int(str)
            return (datetime.datetime.now() - datetime.timedelta(seconds=t)).strftime("%Y-%m-%d %H:%M")
        return "2018-" + str



def parse_news(id):
    dict = {}

    news_blob = "%s.json" % id
    news_exist = blob.bbservice.exists(source_container, news_blob)
    if not news_exist:
        log.warn("No news [{}] found in {}".format(id, source_container))
        return ""

    news, _ = blob.readText(source_container, news_blob)
    js = json.loads(news)

    dict["id"] = js["id"]
    dict["user_id"] = js["user_id"]
    dict["title"] = js["title"]
    dict["website_url"] = "https://xueqiu.com" + js["target"]

    date = js["created_at"] # timestamp
    dict["created_at"] = None if date == None else time.strftime("%Y-%m-%d %H:%M", time.localtime(int(date / 1000)))
    date = js["edited_at"] # timestamp
    dict["edited_at"] = None if date == None else time.strftime("%Y-%m-%d %H:%M", time.localtime(int(date / 1000)))
    dict["posted_at"] = valid_date(js["timeBefore"]) # string

    dict["abstract"] = js["description"]
    dict["reply_count"] = js["reply_count"]
    dict["retweet_count"] = js["retweet_count"]
    dict["fav_count"] = js["fav_count"]
    dict["like_count"] = js["like_count"]

    dict["reward_count"] = js["reward_count"]
    dict["reward_amount"] = js["reward_amount"]
    dict["reward_user_count"] = js["reward_user_count"]

    dict["comments"], latest_comment = parse_comments(id)

    if latest_comment == "":
        dict["active_window"] = default_active_window
    else:
        post_at = time.mktime(time.strptime(dict["posted_at"], '%Y-%m-%d %H:%M'))
        latest_comment = time.mktime(time.strptime(latest_comment, '%Y-%m-%d %H:%M'))
        dict["active_window"] = latest_comment - post_at

    text_content = re.sub("<.*?>", "", js["text"]).replace("&nbsp;", "")
    dict["related_codes"] = related_code(text_content)
    text = blob.writeText(text_container, "text_%s.txt" % id, text_content)
    dict["text_content"] = text # text content url

    user_url = parse_user(js["user"], "%s%s.json" % (schema_url, id), "n") # user info, news url, type\
    dict["user_url"] = user_url

    print("News %s: %s" % (id, str(dict)))

    schema = json.dumps(dict, ensure_ascii=False)
    return schema



def parse_user(user, news, type): # user info, news url, type = "n"/news or "c"/comments
    user_id = user["id"]
    user_blob = "user_%s.json" % user_id
    user_exist = blob.bbservice.exists(user_container, user_blob)

    user_comments = set()
    user_news = set()

    if user_exist: # load old version
        user_past, _ = blob.readText(user_container, user_blob)
        js = json.loads(user_past)
        user_comments = (set)(js["user_comments"])
        user_news = (set)(js["user_news"])

        log.info("Existed user [{}]".format(user_id))
    else:
        log.info("Creating user [{}]".format(user_id))

    if type == "c": # user gives comment for news #id
        user_comments.add(news)
    elif type == "n":
        user_news.add(news)

    dict = {}
    dict["user_id"] = user["id"]
    dict["screen_name"] = user["screen_name"]
    dict["description"] = user["description"]
    dict["verified_description"] = user["verified_description"]
    dict["gender"] = user["gender"]
    dict["province"] = user["province"]
    dict["city"] = user["city"]
    dict["followers"] = user["followers_count"]
    dict["following"] = user["friends_count"]
    dict["post_count"] = user["status_count"]
    dict["stocks_count"] = user["stocks_count"]
    dict["website_url"] = "https://xueqiu.com/u" + user["profile"]
    dict["user_comments"] = (list)(user_comments)
    dict["user_news"] = (list)(user_news)

    user_schema = json.dumps(dict, ensure_ascii=False)
    ret_url = blob.writeText(user_container, "user_%s.json" % user["id"], user_schema)

    return ret_url



def parse_comments(id):
    log.info("Parsing comments for news [{}]".format(id))

    comments_blob = "%s.yaml" % id
    comments_exist = blob.bbservice.exists(source_container, comments_blob)
    if not comments_exist:
        log.info("No comments found.")
        return [], ""

    comments_list, _ = blob.readText(source_container, comments_blob)
    comments_list = yaml.load_all(comments_list)

    comments = []
    latest = ""
    for i, ym in enumerate(comments_list):
        dict = {}

        if ym == None: continue
        dict["comment_id"] = ym["id"]
        dict["description"] = re.sub("<.*?>", "", ym["description"]).replace("&nbsp;", "")
        dict["text"] = re.sub("<.*?>", "", ym["text"]).replace("&nbsp;", "")

        date = ym["created_at"]
        dict["created_at"] = None if date == None else time.strftime("%Y-%m-%d %H:%M", time.localtime(int(date / 1000)))

        dict["posted_at"] = valid_date(ym["timeBefore"])
        latest = dict["posted_at"] if dict["posted_at"] > latest else latest # latest comment time

        dict["like_count"] = ym["like_count"]
        dict["reward_amount"] = ym["reward_amount"]
        dict["reward_count"] = ym["reward_count"]
        dict["reward_user_count"] = ym["reward_user_count"]
        dict["reply_comment_id"] = None if ym["reply_comment"] == None else ym["reply_comment"]["id"]
        dict["user_id"] = ym["user_id"]

        news_url = "%s%s.json" % (schema_url, id) # comments of this news
        user_url = parse_user(ym["user"], news_url, "c")
        dict["user_url"] = user_url # url to user info

        print("Comment %d for news %s:" % (i, id))
        print(dict)
        comments.append(dict)

    return comments, latest


def get_news():
    global index

    while(True):
        news = redis.queue.list(redis_news_list, index, index)

        if news:
            try:
                news_id = news[0]["blob"].split(".")[0] # news_id, string
                log.info("Parsing news: [{}]".format(news_id))

                news_schema = parse_news(news_id)
                log.info(news_schema)

                news_url = blob.writeText(schema_container, news[0]["blob"], news_schema)
                redis.hash.insert(redis_news_hkey, {"k": news[0]["blob"], "v": news_url})
                log.info("Saving news schema to url [{}]".format(news_url))

                index += 1
            except(ConnectionError, ProtocolError):
                log.warn(news)
                log.warn("Error. Saving index to [{}]".format(redis_comments_hkey))
            finally:
                redis.hash.insert(redis_news_hkey, {"k": "index", "v": index})
        else:
            log.info("No news found, wait")
            time.sleep(delay)


if __name__ == '__main__':
    get_news()

