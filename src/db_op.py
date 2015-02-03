#!/usr/bin/env python

import logging
import os
import yaml
from pymongo import MongoClient

logger = logging.getLogger(__name__)

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

mongodb_addr = conf['mongodb_addr']
mongodb_port = conf['mongodb_port']
db_name = conf['db_name']
data_history_len = conf['data_history_len']

data_collection_name = conf['data_collection_name']

client = MongoClient(mongodb_addr, mongodb_port)
db = client[db_name]
data_collection = db['data_collection_name']

class Account(object):
    def __init__(self, item):
        self.item = item
        self.account_id = item['_id']
        if 'data' in item:
            self.data = item['data']
        else:
            logger.warning('account without data: %s' % self.account_id)
            self.data = []
        self.count = len(self.data)

    def set_metadata(self, name, value):
        metadata = {name: value}
        self.item.update(metadata)
        ret = data_collection.update({'_id': self.account_id}, metadata)
        if ret['updatedExisting'] is not True:
            logger.error('set metadata failed: %s %s %s' % (self.account_id,  name, value))

    def get_metadata(self, name):
        if name in self.item:
            return self.item[name]
        else:
            return '0'

    def get_data(self, index, name):
        if index > self.count or index < 0:
            raise Exception('out of range: %s %d %d %s' % (self.account_id, self.count, index, name))
        real_index = self.count - index
        data = self.data[real_index]
        if name not in data:
            raise Exception('no such value: %s %d %d %s' % (self.account_id, self.count, index, name))
        return data[name]

def get_accounts():
    items = data_collection.find()
    for item in items:
        print(item)
        yield Account(item)

def set_metadata_by_account(account_id, metadata_name, value):
    primary = {'_id': account_id}
    ret = data_collection.update(primary, {metadata_name: value})
    if ret['updatedExisting'] is False:
        logger.warning('set metadata failed: %s %s %s' % (account_id, metadata_name, value))

def insert_data(account_id, date, data):
    primary = {'_id': account_id}
    data.update({'date': date})
    ret = data_collection.update(primary, {'$push': {'data': {'$each':[data], '$slice':-data_history_len}}})
    if ret['updatedExisting'] is False:
        primary.update({'data': [data]})
        data_collection.insert(primary)
