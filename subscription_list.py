# 1: Get today date
# key = subscriptions/2016/04/15

import boto
import os
import datetime
import subprocess
# from boto.s3.key import Key

LOCAL_PATH = "/home/vishnu/Workspace/"  # path to dir
AWS_ACCESS_KEY_ID = 'AKIAIS2P5QBYE7KI5FIA'
AWS_SECRET_ACCESS_KEY = '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'
BUCKET_NAME = 'redshift-db'


def get_date():
    _today = datetime.date.today()
    return _today.strftime("%Y/%m/%d")


def gen_key():
    key = "subscriptions/" + get_date()
    return key
# connect to the bucket


def conn_aws():
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(BUCKET_NAME)
    return bucket

# go through the list of files


def download_file():
    bucket = conn_aws()
    bucket_list = bucket.list()
    file_key = "gp_profiles"
    for l in bucket_list:
        if file_key in str(l.key):
            if not os.path.exists(LOCAL_PATH + file_key):
                subprocess.call(["mkdir", "-p", LOCAL_PATH + file_key],
                                shell=False)
            l.get_contents_to_filename(LOCAL_PATH + str(l.key))


download_file()
