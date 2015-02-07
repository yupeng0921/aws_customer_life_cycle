#!/usr/bin/env python

import unittest
import os
import subprocess
import time
import yaml
from mock import Mock, patch
import interpret

base_dir = os.path.split(os.path.realpath(__file__))[0]
interpret_path = '%s/../src/interpret.py' % base_dir
job_directory = '%s/fake_jobs' % base_dir

conf_path = '%s/../src/conf.yaml' % base_dir
with open(conf_path) as f:
    conf = yaml.safe_load(f)

log_file = conf['log_file']

class InterpretTest(unittest.TestCase):

    def setUp(self):
        self.clean_log()

    def tearDown(self):
        self.clean_log()

    def clean_log(self):
        for job_name in os.listdir(job_directory):
            log_path = os.path.join(job_directory, job_name, log_file)
            if os.path.isfile(log_path):
                os.remove(log_path)

    def test_parameter(self):
        job_name = 'test_parameter'
        def run_command(cmd):
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            count = 0
            while p.poll() is None:
                time.sleep(0.5)
                count += 1
                if count >= 6:
                    p.kill()
                    break
        cmd = 'python %s %s %s' % (interpret_path, job_directory, job_name)
        run_command(cmd)
        log_path = os.path.join(job_directory, job_name, log_file)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('hello world' in log)

    def test_plus(self):
        job_name = 'test_plus'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('a = 2' in log)

    def test_minus(self):
        job_name = 'test_minus'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('a = 4' in log)

    def test_times(self):
        job_name = 'test_times'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('a = 6' in log)

    def test_devide(self):
        job_name = 'test_divide'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('a = 2' in log)

    @patch('interpret.get_accounts')
    def test_body(self, get_accounts):
        class Account(object):
            def __init__(self):
                self.count = 1
                self.account_id = '111'
                self.metadata = {}
            def get_data(self, index, name):
                return 'v1'
            def set_metadata(self, name, value):
                self.metadata[name] = value
            def get_metadata(self, name):
                return self.metadata[name]
        def side_effect():
            return [Account()]
        get_accounts.side_effect = side_effect
        job_name = 'test_body'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('account_id method1: 111' in log)
        self.assertTrue('account_id method2: 111' in log)
        self.assertTrue('data count: 1' in log)
        self.assertTrue('ka: v1' in log)
        self.assertTrue('kb: vb1' in log)

    @patch('interpret.set_metadata_by_account')
    def test_set_metadata_by_account(self, set_metadata_by_account):
        job_name = 'test_set_metadata_by_account'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        set_metadata_by_account.assert_called_with('111', 'key1', 'value1')

    def test_number(self):
        job_name = 'test_number'
        log_path = os.path.join(job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('INFO - test1 equal' in log)
        self.assertTrue('INFO - test2 not equal' in log)
        self.assertTrue('INFO - test3 equal' in log)
        self.assertTrue('INFO - test4 not equal' in log)
        self.assertTrue('INFO - test5 equal' in log)
        self.assertTrue('INFO - test6 not equal' in log)
