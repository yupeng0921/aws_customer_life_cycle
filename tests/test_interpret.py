#!/usr/bin/env python

import unittest
import subprocess
import time
from mock import Mock, patch

interpret_path = '../src/interpret.py'
jobs_dir = 'fake_jobs'

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
        log_path = '%s/%s/__log' % (jobs_dir, job_name)
        cmd = 'rm -f %s' % log_path
        run_command(cmd)
        cmd = 'python %s %s %s' % (interpret_path, jobs_dir, job_name)
        run_command(cmd)
        with open(log_path) as f:
            log = f.read()
        cmd = 'rm -f %s' % log_path
        run_command(cmd)
        self.assertTrue('hello world' in log)
