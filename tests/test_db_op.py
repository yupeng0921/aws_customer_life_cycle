#!/usr/bin/env python

import unittest
import os
import datetime
import time
import yaml
from pymongo import MongoClient
import db_op

base_dir = os.path.split(os.path.realpath(__file__))[0]
conf_path = '%s/../src/conf.yaml' % base_dir
with open(conf_path) as f:
    conf = yaml.safe_load(f)

mongodb_addr = conf['mongodb_addr']
mongodb_port = conf['mongodb_port']
data_history_len = conf['data_history_len']
db_name = 'test_lifecycle_db'
data_collection_name = 'test_lifecycle_collection'
client = MongoClient(mongodb_addr, mongodb_port)
db = client[db_name]
data_collection = db['data_collection_name']

fake_accounts = [
    {
        'account_id': '111',
        'date': '2013/12/25',
        'data': {'ka': 'va1', 'kb': 'vb1'},
    },
    {
        'account_id': '111',
        'date': '2013/12/26',
        'data': {'ka': 'va2', 'kb': 'vb2'},
    },
    {
        'account_id': '111',
        'date': '2013/12/27',
        'data': {'ka': 'va3', 'kb': 'vb3'},
    },
    {
        'account_id': '222',
        'date': '2013/12/28',
        'data': {'ka': 'va4', 'kb': 'vb4'},
    },
]

class DbOpTest(unittest.TestCase):

    def setUp(self):
        data_collection.drop()
        db_op.data_collection = data_collection
        db_op.data_history_len = 2

    def tearDown(self):
        data_collection.drop()

    def test_insert_data(self):
        account0 = fake_accounts[0]
        db_op.insert_data(account0['account_id'], account0['date'], account0['data'])
        results = list(data_collection.find())
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['_id'], account0['account_id'])
        account1 = fake_accounts[1]
        db_op.insert_data(account1['account_id'], account1['date'], account1['data'])
        account2 = fake_accounts[2]
        db_op.insert_data(account2['account_id'], account2['date'], account2['data'])
        account3 = fake_accounts[3]
        db_op.insert_data(account3['account_id'], account3['date'], account3['data'])
        results = list(data_collection.find())
        self.assertEqual(len(results), 2)
        result = results[0]
        self.assertEqual(result['_id'], account0['account_id'])
        self.assertEqual(len(result['data']), 2)
        self.assertEqual(result['data'][1]['date'], account2['date'])
        self.assertEqual(result['data'][1]['ka'], account2['data']['ka'])
        self.assertEqual(result['data'][1]['kb'], account2['data']['kb'])
        self.assertEqual(result['data'][0]['date'], account1['date'])
        self.assertEqual(result['data'][0]['ka'], account1['data']['ka'])
        self.assertEqual(result['data'][0]['kb'], account1['data']['kb'])

    def test_get_accounts(self):
        account0 = fake_accounts[0]
        db_op.insert_data(account0['account_id'], account0['date'], account0['data'])
        account3 = fake_accounts[3]
        db_op.insert_data(account3['account_id'], account3['date'], account3['data'])
        accounts = list(db_op.get_accounts())
        self.assertEqual(len(accounts), 2)
        account = accounts[0]
        self.assertEqual(account.account_id, account0['account_id'])
        self.assertEqual(account.count, 1)
        account = accounts[1]
        self.assertEqual(account.account_id, account3['account_id'])
        self.assertEqual(account.count, 1)
