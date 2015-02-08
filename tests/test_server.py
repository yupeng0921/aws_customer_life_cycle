#!/usr/bin/env python

import unittest
import os
from mock import Mock, patch

current_dir = os.path.split(os.path.realpath(__file__))[0]
os.environ['LIFECYCLE_CONF'] = os.path.join(current_dir, 'test_conf.yaml')
import server
from server import app


class ServerTest(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        pass

    def login(self, username, password):
        return self.app.post('/login', data=dict(
            username=username,
            password=password
        ), follow_redirects=True)

    def logout(self):
        return self.app.get('/logout', follow_redirects=True)

    def test_no_login(self):
        rv = self.app.get('/')
        self.assertTrue('redirected' in rv.data)
