#! /usr/bin/env python

import sys
import os
import time
import yaml
import logging
import sqlite3
import subprocess
from daemon import runner

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

current_dir = os.path.split(os.path.realpath(__file__))[0]

daemon_log_file = conf['daemon_log_file']
daemon_debug_flag = conf['daemon_debug_flag']
app_dir = conf['app_dir']
job_directory = conf['job_directory']
interpret_file = conf['interpret_file']
task_db = conf['task_db']
task_table = conf['task_table']
task_magic_string = conf['task_magic_string']
daemon_interval = conf['daemon_interval']
daemon_stdin_path = conf['daemon_stdin_path']
daemon_stdout_path = conf['daemon_stdout_path']
daemon_stderr_path = conf['daemon_stderr_path']
daemon_pidfile_path = conf['daemon_pidfile_path']
daemon_pidfile_timeout = conf['daemon_pidfile_timeout']
daemon_interpret_timeout = conf['daemon_interpret_timeout']
subprocess_stdout = conf['subprocess_stdout']
subprocess_stderr = conf['subprocess_stderr']

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
            if daemon_debug_flag == 'debug':
                level = logging.DEBUG
            else:
                level = logging.INFO
            logging.basicConfig(filename=self.log_file, level=level, format=format, datefmt=datefmt)
            cx = sqlite3.connect(self.db_path)
            cu = cx.cursor()
            cmd = 'create table if not exists %s (' % self.table_name + \
                'magic_string varchar(10) primary key,' + \
                'package_name varchar(255),' + \
                'status varchar(10)' + \
                ')'
            cu.execute(cmd)
            cx.commit()
            cu.close()
            logging.info('come to loop')
            while True:
                cu = cx.cursor()
                cmd = 'select * from %s where magic_string="%s"' % (self.table_name, self.magic_string)
                cu.execute(cmd)
                ret = cu.fetchone()
                if not ret:
                    logging.debug('empty, continue to sleep')
                    time.sleep(self.daemon_interval)
                    continue
                (magic_string, package_name, status) = ret
                if status == 'done':
                    logging.debug('done, continue to sleep')
                    time.sleep(self.daemon_interval)
                    continue
                logging.info('package: %s' % package_name)
                try:
                    job_dir = os.path.join(app_dir, job_directory)
                    cmd = 'python %s/%s %s %s' % (current_dir, interpret_file, job_dir, package_name)
                    logging.debug('run interpret: %s' % cmd)
                    f_stdout = open(subprocess_stdout, 'w')
                    f_stderr = open(subprocess_stderr, 'w')
                    p = subprocess.Popen(cmd, shell=True, stdout=f_stdout, stderr=f_stderr)
                    ret = p.wait(timeout=daemon_interpret_timeout)
                    f_stdout.close()
                    f_stderr.close()
                except Exception, e:
                    logging.error('run interpret failed: %s' % unicode(e))
                cmd = '''update %s set status="done" where magic_string = "%s"''' % \
                    (self.table_name, self.magic_string)
                logging.debug('set to done, cmd: %s' % cmd)
                cu.execute(cmd)
                cx.commit()
                cu.close()

task = RunTask( \
    daemon_stdin_path, daemon_stdout_path, daemon_stderr_path, daemon_pidfile_path, daemon_pidfile_timeout, \
        task_db, task_table, task_magic_string, daemon_log_file, daemon_interval)

daemon_runner = runner.DaemonRunner(task)
daemon_runner.do_action()


    
