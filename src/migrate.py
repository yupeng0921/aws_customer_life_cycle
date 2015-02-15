#!/usr/bin/env python

import os
import yaml
import json
from pymongo import MongoClient

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

mongodb_addr = conf['mongodb_addr']
mongodb_port = conf['mongodb_port']
db_name = conf['db_name']
data_collection_name = conf['data_collection_name']

client = MongoClient(mongodb_addr, mongodb_port)
db = client[db_name]
data_collection = db[data_collection_name]

def insert_metadata(metadata_file):
    with open(metadata_file) as f:
        for eachline in f:
            item = json.loads(eachline.strip())
            account_id = item['account_id']
            del item['account_id']
            if not item:
                continue
            item['_id'] = account_id
            data_collection.insert(item)

def insert_data(data_file):
    d = {}
    with open(data_file) as f:
        for eachline in f:
            item = json.loads(eachline.strip())
            account_id = item['account_id']
            if account_id not in d:
                d[account_id] = []
            d[account_id].append(item)
    def comp(x, y):
        x = int(x['date'])
        y = int(y['date'])
        if x > y:
            return 1
        elif x < y:
            return -1
        else:
            return 0
    for account_id in d:
        items = d[account_id]
        items.sort()
        data = []
        for item in items:
            del item['account_id']
            data.append(item)
        data_collection.update({'_id':account_id}, {'$set': {'data': data}})

if __name__=='__main__':
    import sys
    metadata_file = sys.argv[1]
    data_file = sys.argv[2]
    print('insert_metadata')
    insert_metadata(metadata_file)
    print('insert_data')
    insert_data(data_file)
