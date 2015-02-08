#!/usr/bin/env python

import unittest
import os
from mock import Mock, patch
from worker import insert_to_table, delete_from_table

base_dir = os.path.split(os.path.realpath(__file__))[0]
fake_account_file = 'fake_account.csv'
fake_delete_account_file = 'fake_delete_account.csv'
fake_account_fullpath = os.path.join(base_dir, fake_account_file)
fake_delete_account_fullpath = os.path.join(base_dir, fake_delete_account_file)

class WorkerTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('worker.insert_data')
    @patch('os.remove')
    @patch('worker.unlock')
    def test_insert_to_table(self, unlock, os_remove, insert_data):
        r = insert_to_table.apply([fake_account_fullpath, False])
        self.assertEqual('done', r.result[-1])

    @patch('worker.delete_data')
    @patch('os.remove')
    @patch('worker.unlock')
    def test_delete_from_table(self, unlock, os_remove, delete_data):
        r = delete_from_table.apply([fake_delete_account_fullpath])
        self.assertEqual('done', r.result[-1])
