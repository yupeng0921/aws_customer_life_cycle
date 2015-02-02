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
        pass

    def tearDown(self):
        pass

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
        log_path = '%s/%s/%s' % (job_directory, job_name, log_file)
        cmd = 'rm -f %s' % log_path
        run_command(cmd)
        cmd = 'python %s %s %s' % (interpret_path, job_directory, job_name)
        run_command(cmd)
        with open(log_path) as f:
            log = f.read()
        cmd = 'rm -f %s' % log_path
        run_command(cmd)
        self.assertTrue('hello world' in log)

    def test_add(self):
        job_name = 'test_add'
        log_path = '%s/%s/%s' % (job_directory, job_name, log_file)
        interpret.do_job(job_directory, job_name)
        log_path = '%s/%s/%s' % (job_directory, job_name, log_file)
        with open(log_path) as f:
            log = f.read()
        self.assertTrue('a = 2' in log)
