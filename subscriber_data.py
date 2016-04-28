import urllib
import requests
import yaml
import os
import sys
from multiprocessing import Pool

from threading import Thread
import threading
from Queue import Queue
from log import logger


API_KEY = "AIzaSyCd2LRQsgIXhBRQtnJaFIRXPAN1ir0IwJE"
STAT_URL = "https://www.googleapis.com/youtube/v3/channels"
GPLUS_URL = "https://www.googleapis.com/plus/v1/people/"
PART = "statistics"
subid_list = []


def subsriber_youtube_stats(sub_id):
    global subid_list
    subid_list.append(sub_id)
    if len(subid_list) == 50 and isinstance(subid_list, list):
        params = urllib.urlencode({"part": PART, "id": ','.join(subid_list),
                                   "key": API_KEY})
        result = requests.get(STAT_URL, params=params)
        subid_list = []
        if result.status_code == 200:
            return result
    return None


def worker_func1(line):
    stats = {}
    sub_id, g_url = line.split("|")
    subs_data = subsriber_youtube_stats(sub_id)
    if subs_data and subs_data.json().get("items", None):
        stats = yaml.load(subs_data.text).get("items", None)
        for subid_data in stats:
            # TODO format according to map reduce
            data = {}
            data[subid_data['id']] = subid_data['statistics']
            logger.info("STATISTICS SUBCSRIBER_DATA : %s", data)

def worker_func(line, url_list ,subid_list1):
    stats = {}
    sub_id, g_url = line.split("|")
    subid_list1.append(sub_id)
    if len(subid_list1) >= 50 :
        params = urllib.urlencode({"part": PART, "id": ','.join(subid_list1),
                                   "key": API_KEY})
        url_list.append(STAT_URL +"?" + str(params))
        subid_list1[:] = []

def main_fn(file):
    url_list = []
    subid_list1 = []
    with open(file) as f:
        for line in f:
            worker_func(line,url_list ,subid_list1)

    if len(subid_list1) > 0 :
        params = urllib.urlencode({"part": PART, "id": ','.join(subid_list1),
                                   "key": API_KEY})
        url_list.append(STAT_URL +"?" + str(params))
        subid_list1[:] = []

    

    def fetch():
        while True:
            # print threading.current_thread()
            url = q.get()
            response = requests.request('GET', url)
            print response.status_code
            if response.status_code == 200 :
                if response.json().get("items", None):
	        	stats = yaml.load(response.text).get("items", None)
        		for subid_data in stats:
            			# TODO format according to map reduce
            			data = {}
            			data[subid_data['id']] = subid_data['statistics']
            			#logger.info("STATISTICS SUBCSRIBER_DATA : %s", data)
            else :
                print "Status: [%s] URL: %s" % (response.text, url)
            q.task_done()            
    concurrent = 20
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

    print ("done")


if __name__ == "__main__":
    #pool = Pool(processes=4)

    # file to be processed
    no_processed = 0;
    for i in os.listdir("/home/vishnu/Workspace/gp_profiles/splitfiles/"):
           	#with open("/home/vishnu/Workspace/gp_profiles/splitfiles/"+i) as f:
        	#pool.map(worker_func1, f)
                main_fn("/home/vishnu/Workspace/gp_profiles/splitfiles/"+i)
                #logger.debug("file done "+i)
                no_processed += 1
                print(i +" count = "+str(no_processed) + " done")
                sys.exit(1)
