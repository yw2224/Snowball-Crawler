# SnowballCrawler

A python news crawler for financial website Snowball (https://xueqiu.com).
Crawl news from the website, process and store the .txt data as json schema. The result is for further finance and economics analysis. 
Use Redis, Microsoft Azure for storage.

Results are shown in **/result_sample**. For each news, the following features are extracted and saved in **/result_sample/snowballschema**:
* Title
* Abstract
* Website url
* Article id
* Reply / Retweet / Like / Reward count
* Post time
* Active window: the time window between the post times of the news and its latest comment
* Related code: the stocks referred to in the news
* Text content: saved in a seperate directory **/result_sample/snowballtext**, connected by url)
* Author information: saved in a seperate directory **/result_sample/snowballuser**, connected by url)
    * Screen name
    * User id
    * Description
    * Region
    * Gender
    * Website url
    * Followers / Following
    * Post count
    * News posted
    * Comments posted