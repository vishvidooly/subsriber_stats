import urllib
import requests
import yaml
import os
import sys
import threading

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
    try:
        result = requests.get(STAT_URL, params=params)
        if result.status_code == 200:
                return result
    except Exception as e:
        print e
        logger.info("GID_ERROR SUBSCRIBER_DATA url: %s ",url)

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



def worker_func(line, url_list):
    gender, gplus_data = None, None
    sub_id, g_id = line.split("|")
    g_id = g_id.strip()
    # proces g_id for gender
    # g_id = g_url.strip().split("/")[-1]
    # g_id is not present generate g_id
    if not len(g_id) == 21:
        res = fetch_gid(sub_id)
        if res and res.json().get("items", None):
            g_id = yaml.load(res.text)["items"][0]["contentDetails"]["googlePlusUserId"]

    params = urllib.urlencode({"key": API_KEY})
    # check g_id factors
    if len(g_id) == 21 :
        url_list.append(GPLUS_URL + str(g_id)+ "?" + str(params) +"|"+sub_id)


def main_fn(file_path) :
    url_list = []
    with open(file_path) as f:
        for line in f:
            worker_func(line, url_list)
    print len(url_list)
    def fetch():
        while True:
            url , sub_id = q.get().split("|")
            try :
            	response = requests.request('GET', url,timeout = 10)
            	#print ("Response recieved " + str(sub_id) + str(response.status_code))
            	if response.status_code == 200:
                        data = {}
                	#print ("Response_200 recieved " +str(url)+"|"+ str(sub_id))
                	#print ("Response "+str(threading.current_thread()))
                        gender = None
                        try:
            			gender = yaml.load(response.text)["gender"]
        		except:
            			pass
                        data["subscriber_id"] = sub_id
                        data["gender"] = gender
                      
                	logger.info("GENDER_DATA SUBSCRIBER_DATA : %s", data)
                else :
                     logger.info("GENDER_ERROR_RESPONSE_"+os.path.basename(file_path)+" SUBSCRIBER_DATA response : %s url: %s|%s",response.text,url,sub_id)  
                q.task_done()
            except Exception as e:
                print e 
                logger.info("GENDER_ERROR_"+os.path.basename(file_path)+" SUBSCRIBER_DATA url: %s|%s",url,sub_id)
            	q.task_done()
            
    concurrent = 2
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
    dir = "/home/gulshan/subsriber_data/sub_gid_new/temp_0/"
    # file to be processed
    for i in os.listdir(dir):
        main_fn(dir+i)
        print (str(i) + " done")
