import urllib
import requests
import yaml
import os
import sys

from log import logger
from threading import Thread
from Queue import Queue

API_KEY = "AIzaSyCd2LRQsgIXhBRQtnJaFIRXPAN1ir0IwJE"
STAT_URL = "https://www.googleapis.com/youtube/v3/channels"
GPLUS_URL = "https://www.googleapis.com/plus/v1/people/"
PART = "contentDetails"
no_of_requests = 0;
NO_OF_KEYS = 5;
API_KEYS = [ "AIzaSyCd2LRQsgIXhBRQtnJaFIRXPAN1ir0IwJE", "AIzaSyA5RI3AKWYAIA-42WcceHox3emuLoaaCIg"
             "AIzaSyDNfUGGme2COg27EME6gJy5zCZsNG3ad-Q", "AIzaSyAfCfkySIlz25HDCn5VeqzSFsj8nfrswu8"
             "AIzaSyB_AZ4eO4y7lIpjc2cRKByR71GA1TJyDfU" ]



def fetch_gid(sub_id):
    params = urllib.urlencode({"part": PART, "id": sub_id,
                               "fields": "items(contentDetails(googlePlusUserId))",
                               "key": API_KEY})
    # no_of_requests += 1
    try:
        result = requests.get(STAT_URL, params=params)
        if result.status_code == 200:
                return result
    except requests.ConnectionError as e:
        print e
        logger.error("STATISTICS_ERROR "+os.path.basename(file)+" url: %s ",url)
        return None

    return None


def subscriber_gplus_profile(g_id):
    G_URL = GPLUS_URL + str(g_id)
    params = urllib.urlencode({"key": API_KEY})
    result = requests.get(G_URL, params=params)
    if result.status_code == 200:
        return result
    return None

def log_response(response):
    data = {}
    print("response "+ response)
    data["id"] = ""
    data['gender'] = response["gender"]
    # logger.info("GID SUBCSRIBER_DATA : %s", data)



def worker_func(line, url_list, no_of_requests):
    #data = {}
    gender, gplus_data = None, None
    sub_id, g_id = line.split("|")

    # proces g_id for gender
    # g_id = g_url.strip().split("/")[-1]

    #print g_id
    # g_id is not present generate g_id
    if not len(g_id) == 21:
        res = fetch_gid(sub_id)
        if res and res.json().get("items", None):
            g_id = yaml.load(res.text)["items"][0]["contentDetails"]["googlePlusUserId"]

    params = urllib.urlencode({"key": API_KEY})
    #no_of_requests += 1
    # check g_id factors
    if len(g_id) == 21 :
        url_list.append(GPLUS_URL + str(g_id)+ "?" + str(params))
        print len(url_list)


def main_fn(file_path) :
    url_list = []
    # NUM_SESSIONS = 20
    no_of_requests = 0
    # pool = Pool(100)
    with open(file_path) as f:
        count = 0
        for line in f:
            # print count
            worker_func(line, url_list, no_of_requests)
            count += 1
    print len(url_list)


    def fetch():
        while True:
            url = q.get()
            print url
            response = requests.request('GET', url)
            print response.text
            if response.json().get("gender", None):
                logger.info("GENDER SUBCSRIBER_DATA : %s", yaml.load(response.text)["gender"])
            # if response.status_code == 200 :
            #     print "Status: [%s] URL: %s" % (response.text, url)
            q.task_done()
    concurrent = 10
    q = Queue(concurrent * 2)
    for i in range(concurrent):
        t = Thread(target=fetch)
        t.daemon = True
        t.start()
    try:
        for url in url_list:
                q.put(url)
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    dir = "/home/vidooly/Workspace/gp_profiles/splitfiles0/test/"
    # file to be processed
    for i in os.listdir(dir):
        main_fn(dir+i)
        print (i +" count = "+str(no_processed) + " done")
        sys.exit(1)