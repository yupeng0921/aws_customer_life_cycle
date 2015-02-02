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
