#! /usr/bin/env python

import sys
import os
import time
import yaml
import logging
import sqlite3
import subprocess
from daemon import runner

with open(u'%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], u'r') as f:
    conf = yaml.safe_load(f)

current_dir = os.path.split(os.path.realpath(__file__))[0]

daemon_log_file = conf[u'daemon_log_file']
daemon_debug_flag = conf[u'daemon_debug_flag']
job_directory = conf[u'job_directory']
interpret_file = conf[u'interpret_file']
task_db = conf[u'task_db']
task_table = conf[u'task_table']
task_magic_string = conf[u'task_magic_string']
daemon_interval = conf[u'daemon_interval']
daemon_stdin_path = conf[u'daemon_stdin_path']
daemon_stdout_path = conf[u'daemon_stdout_path']
daemon_stderr_path = conf[u'daemon_stderr_path']
daemon_pidfile_path = conf[u'daemon_pidfile_path']
daemon_pidfile_timeout = conf[u'daemon_pidfile_timeout']
daemon_interpret_timeout = conf[u'daemon_interpret_timeout']

class RunTask():
        def __init__(self, stdin_path, stdout_path, stderr_path, pidfile_path, pidfile_timeout, \
                         db_path, table_name, magic_string, log_file, daemon_interval):
            self.stdin_path = stdin_path
            self.stdout_path = stdout_path
            self.stderr_path = stderr_path
            self.pidfile_path = pidfile_path
            self.pidfile_timeout = pidfile_timeout
            self.db_path = db_path
            self.table_name = table_name
            self.magic_string = magic_string
            self.log_file = log_file
            self.daemon_interval = daemon_interval
        def run(self):
            format = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
            datefmt='%Y-%m-%d %H:%M:%S'
            if daemon_debug_flag == u'debug':
                level = logging.DEBUG
            else:
                level = logging.INFO
            logging.basicConfig(filename=self.log_file, level=level, format=format, datefmt=datefmt)
            cx = sqlite3.connect(self.db_path)
            cu = cx.cursor()
            cmd = u'create table if not exists %s (' % self.table_name + \
                u'magic_string varchar(10) primary key,' + \
                u'package_name varchar(255),' + \
                u'status varchar(10)' + \
                u')'
            cu.execute(cmd)
            cu.close()
            logging.info(u'come to loop')
            while True:
                cu = cx.cursor()
                cmd = u'select * from %s where magic_string="%s"' % (self.table_name, self.magic_string)
                cu.execute(cmd)
                ret = cu.fetchone()
                if not ret:
                    logging.debug(u'empty, continue to sleep')
                    time.sleep(self.daemon_interval)
                    continue
                (magic_string, package_name, status) = ret
                if status == u'done':
                    logging.debug(u'done, continue to sleep')
                    time.sleep(self.daemon_interval)
                    continue
                logging.info(u'package: %s' % package_name)
                try:
                    cmd = u'python %s/%s %s' % (current_dir, interpret_file, package_name)
                    logging.debug(u'run interpret: %s' % cmd)
                    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    ret = p.wait(timeout=daemon_interpret_timeout)
                except Exception, e:
                    logging.error(u'run interpret failed: %s' % unicode(e))
                cmd = u'''update %s set status="done" where magic_string = "%s"''' % \
                    (self.table_name, self.magic_string)
                logging.debug(u'set to done, cmd: %s' % cmd)
                cu.execute(cmd)
                cx.commit()
                cu.close()

task = RunTask( \
    daemon_stdin_path, daemon_stdout_path, daemon_stderr_path, daemon_pidfile_path, daemon_pidfile_timeout, \
        task_db, task_table, task_magic_string, daemon_log_file, daemon_interval)

daemon_runner = runner.DaemonRunner(task)
daemon_runner.do_action()


    
