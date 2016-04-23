import urllib
import requests
import yaml
from multiprocessing import Pool

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


def worker_func(line):
    stats = {}
    sub_id, g_url = line.split("|")
    subs_data = subsriber_youtube_stats(sub_id)
    if subs_data and subs_data.json().get("items", None):
        stats = yaml.load(subs_data.text).get("items", None)
        for subid_data in stats:
            data = {}
            data[subid_data['id']] = subid_data['statistics']
            logger.info("STATISTICS SUBCSRIBER_DATA : %s", data)


if __name__ == "__main__":
    pool = Pool(processes=4)
    # file to be processed
    with open("/home/vishnu/Workspace/gp_profiles/test.txt") as f:
        pool.map(worker_func, f)
