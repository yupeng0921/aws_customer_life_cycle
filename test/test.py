#!/usr/bin/env python

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
import unittest
import os
import os.path
import sys
import yaml
import time

import gmail_op

current_path = os.path.split(os.path.realpath(__file__))[0]
with open('%s/test.yaml' % current_path, 'r') as f:
    conf = yaml.safe_load(f)

url = conf['url']
username = unicode(conf['username'])
password = unicode(conf['password'])
data_files = conf['data_files']

class LifeCycleTestCase(unittest.TestCase):
    def setUp(self):
        self.driver = webdriver.Firefox()
        self.driver.implicitly_wait(10)
        self.driver.get(url)
        gmail_op.delete_emails()
        self.clean_all = False
    def tearDown(self):
        if self.clean_all:
            self.driver.close()
            gmail_op.delete_emails()
    def do_login(self):
        elem = self.driver.find_element_by_id('username')
        elem.send_keys(username)
        elem = self.driver.find_element_by_id('password')
        elem.send_keys(password)
        elem = self.driver.find_element_by_id('login_submit')
        elem.click()
    def do_logout(self):
        elem = self.driver.find_element_by_id('logout_tab')
        elem.click()
    def do_insert(self):
        elem = self.driver.find_element_by_id('insert_tab')
        elem.click()
        for data_file in data_files:
            elem = self.driver.find_element_by_id('insert_file')
            data_full_path = os.path.join(current_path, data_file)
            elem.send_keys(data_full_path)
            elem = self.driver.find_element_by_id('submit')
            elem.click()
    def do_delete(self):
        elem = self.driver.find_element_by_id('delete_tab')
        elem.click()
        for data_file in data_files:
            elem = self.driver.find_element_by_id('delete_file')
            data_full_path = os.path.join(current_path, data_file)
            elem.send_keys(data_full_path)
            elem = self.driver.find_element_by_id('submit')
            elem.click()
    def upload_script(self, package_name, run_immediately):
        elem = self.driver.find_element_by_id('script_tab')
        elem.click()
        package_name = '%s.zip' % package_name
        package_full_path = os.path.join(current_path, package_name)
        elem = self.driver.find_element_by_id('script_package')
        elem.send_keys(package_full_path)
        if run_immediately:
            elem = self.driver.find_element_by_id('run_immediately')
            elem.click()
        elem = self.driver.find_element_by_id('submit')
        elem.click()
    def delete_script(self, package_name):
        elem = self.driver.find_element_by_id('script_tab')
        elem.click()
        delete_id = '%s_delete' % package_name
        elem = self.driver.find_element_by_id(delete_id)
        elem.click()
    def get_log(self, package_name):
        elem = self.driver.find_element_by_id('script_tab')
        elem.click()
        package_id = '%s_log' % package_name
        elem = self.driver.find_element_by_id(package_id)
        elem.click()
        elem = self.driver.find_element_by_tag_name('pre')
        return unicode(elem.text)
    def do_script_1(self):
        package_name = 'package1'
        self.upload_script(package_name, True)
        time.sleep(5)
        count = 30
        while count > 0:
            try:
                log = self.get_log(package_name)
                self.assertTrue('stop job' in log)
            except Exception, e:
                count -= 1
            else:
                break
            time.sleep(3)
        if count == 0:
            log = self.get_log(package_name)
            self.assertTrue('stop job' in log)
        self.assertTrue('test_plus=2' in log)
        self.assertTrue('test_multiple=6' in log)
        self.assertTrue('no previous: 555555555555' in log)
        self.assertTrue('test_done' in log)
        results = gmail_op.get_emails()
        self.assertTrue(len(results) == 1)
        for result in results:
            subject = result['subject']
            content = result['content']
            self.assertTrue('test from life cycle' in subject)
            self.assertTrue('your account: 111111111111' in content)
        self.delete_script(package_name)
    def do_script_2(self):
        package_name = 'package2'
        self.upload_script(package_name, True)
        time.sleep(5)
        count = 30
        while count > 0:
            try:
                log = self.get_log(package_name)
                self.assertTrue('stop job' in log)
            except Exception, e:
                count -= 1
            else:
                break
            time.sleep(3)
        if count == 0:
            log = self.get_log(package_name)
            self.assertTrue('stop job' in log)
        self.assertTrue('111111111111 metadata 1' in log)
        self.assertTrue('222222222222 metadata 0' in log)
        self.assertTrue('333333333333 metadata 0' in log)
        self.assertTrue('444444444444 metadata 0' in log)
        self.assertTrue('555555555555 metadata 0' in log)
        self.delete_script(package_name)
    def do_script_3(self):
        package_name = 'package3'
        self.upload_script(package_name, True)
        time.sleep(5)
        count = 30
        while count > 0:
            try:
                log = self.get_log(package_name)
                self.assertTrue('stop job' in log)
            except Exception, e:
                count -= 1
            else:
                break
            time.sleep(3)
        if count == 0:
            log = self.get_log(package_name)
            self.assertTrue('stop job' in log)
        self.assertTrue('111111111111 metadata 0' in log)
        self.assertTrue('222222222222 metadata 0' in log)
        self.assertTrue('333333333333 metadata 0' in log)
        self.assertTrue('444444444444 metadata 0' in log)
        self.assertTrue('555555555555 metadata 0' in log)
        self.delete_script(package_name)
    def do_script_4(self):
        package_name = 'package4'
        self.upload_script(package_name, True)
        time.sleep(5)
        count = 30
        while count > 0:
            try:
                log = self.get_log(package_name)
                self.assertTrue('stop job' in log)
            except Exception, e:
                count -= 1
            else:
                break
            time.sleep(3)
        if count == 0:
            log = self.get_log(package_name)
            self.assertTrue('stop job' in log)
        self.assertTrue('first line' in log)
        self.assertTrue('second line' in log)
        self.assertTrue('third line' in log)
        self.delete_script(package_name)
    def do_script_5(self):
        package_name = 'package5'
        self.upload_script(package_name, False)
        time.sleep(120)
        log = self.get_log(package_name)
        self.assertTrue('stop job' in log)
        self.assertTrue('test schedule' in log)
        self.delete_script(package_name)
    def test_login(self):
        self.clean_all = False
        self.do_login()
        self.do_logout()
        self.clean_all = True
    def test_load_data(self):
        self.clean_all = False
        self.do_login()
        self.do_insert()
        self.do_delete()
        self.do_logout()
        self.clean_all = True
    def test_script_1(self):
        self.clean_all = False
        self.do_login()
        self.do_insert()
        self.do_script_1()
        self.do_delete()
        self.do_logout()
        self.clean_all = True
    def test_script_2(self):
        self.clean_all = False
        self.do_login()
        self.do_insert()
        self.do_script_2()
        self.do_delete()
        self.do_logout()
        self.clean_all = True
    def test_script_3(self):
        self.clean_all = False
        self.do_login()
        self.do_insert()
        self.do_script_3()
        self.do_delete()
        self.do_logout()
        self.clean_all = True
    def test_script_4(self):
        self.clean_all = False
        self.do_login()
        self.do_insert()
        self.do_script_4()
        self.do_delete()
        self.do_logout()
        self.clean_all = True
    def test_script_5(self):
        self.clean_all = False
        self.do_login()
        self.do_insert()
        self.do_script_5()
        self.do_delete()
        self.do_logout()
        self.clean_all = True

if __name__ == '__main__':
    unittest.main()
