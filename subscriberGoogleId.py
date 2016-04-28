import urllib
import requests
import yaml
import os
from multiprocessing import Pool

from log import logger

API_KEY = "AIzaSyCd2LRQsgIXhBRQtnJaFIRXPAN1ir0IwJE"
STAT_URL = "https://www.googleapis.com/youtube/v3/channels"
GPLUS_URL = "https://www.googleapis.com/plus/v1/people/"
PART = "contentDetails"


def fetch_gid(sub_id):
    params = urllib.urlencode({"part": PART, "id": sub_id,
                               "fields": "items(contentDetails(googlePlusUserId))",
                               "key": API_KEY})
    result = requests.get(STAT_URL, params=params)
    if result.status_code == 200:
        return result
    return None


def subscriber_gplus_profile(g_id):
    G_URL = GPLUS_URL + str(g_id)
    params = urllib.urlencode({"key": API_KEY})
    result = requests.get(G_URL, params=params)
    if result.status_code == 200:
        return result
    return None


def worker_func(line):
    data = {}
    gender, gplus_data = None, None
    sub_id, g_url = line.split("|")

    # proces g_id for gender
    g_id = g_url.strip().split("/")[-1]

    # g_id is not present generate g_id
    if not len(g_id) == 21:
        res = fetch_gid(sub_id)
        if res.status_code == 200 and res.json().get("items", None):
            g_id = yaml.load(res.text)["items"][0]["contentDetails"]["googlePlusUserId"]

    # check g_id factors
    if len(g_id) == 21 and g_id.isdigit():
        gplus_data = subscriber_gplus_profile(g_id)
    if gplus_data:
        try:
            gender = yaml.load(gplus_data.text)["gender"]
        except:
            pass
    data["id"] = sub_id
    data['gender'] = gender
    logger.info("GID_F SUBCSRIBER_DATA : %s", data)

if __name__ == "__main__":
    pool = Pool(processes=4)
    # file to be processed
    for i in os.listdir("/home/vishnu/Workspace/gp_profiles/splitfiles/"):
    	with open("/home/vishnu/Workspace/gp_profiles/splitfiles/"+i) as f:
        	pool.map(worker_func, f)
                logger.debug("file done "+i)
                print(i +"done")
