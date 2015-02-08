#!/usr/bin/env python

import unittest
import os
from cStringIO import StringIO
from mock import Mock, patch

current_dir = os.path.split(os.path.realpath(__file__))[0]
os.environ['LIFECYCLE_CONF'] = os.path.join(current_dir, 'test_conf.yaml')
import server
from server import app

username = 'admin'
password = '123'

class ServerTest(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()
        self.clean_upload()
        self.login(username, password)

    def tearDown(self):
        self.logout()
        self.clean_upload()

    def login(self, username, password):
        return self.app.post('/login', data=dict(
            username=username,
            password=password
        ), follow_redirects=True)

    def logout(self):
        return self.app.get('/logout', follow_redirects=True)

    def clean_upload(self):
        upload_dir = os.path.join(current_dir, 'life_cycle/upload')
        for filename in os.listdir(upload_dir):
            file_full_path = os.path.join(upload_dir, filename)
            if os.path.isfile(file_full_path):
                os.remove(file_full_path)

    def test_root(self):
        rv = self.app.get('/')
        self.assertTrue('home' in rv.data)

    def test_insert_get(self):
        rv = self.app.get('/insert')
        self.assertTrue('insert_file' in rv.data)

    @patch('server.lock')
    @patch('server.insert_to_table')
    def test_insert_post(self, insert_to_table, lock):
        insert_to_table.apply_async = Mock(return_value=None)
        rv = self.app.post(
            '/insert',
            data=dict(insert_file=(StringIO("hi everyone"), 'test.txt')),
            follow_redirects=True)
        self.assertTrue('insert_file' in rv.data)

class ServerLoginTest(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        pass

    def test_no_login(self):
        rv = self.app.get('/')
        self.assertTrue('redirected' in rv.data)
