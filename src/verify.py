#! /usr/bin/env python

import time
import datetime
import re

p_account_id = re.compile(r'^[0-9]{12}$')
def check_account_id(account_id):
    m = p_account_id.match(account_id)
    if m:
        return True
    else:
        return False

def check_date(date):
    date = date.split('/')
    if len(date) != 3:
        return False
    try:
        date = int(time.mktime(datetime.datetime(int(date[0]),int(date[1]),int(date[2])).timetuple()))
    except Exception:
        return False
    return True

p_email = re.compile(r'^.*@([a-z0-9]*[-_]?[a-z0-9]+)+[\.][a-z]{2,3}([\.][a-z]{2})?$')
def check_email(email):
    m = p_email.match(email)
    if m:
        return True
    else:
        return False

def check_number(number):
    try:
        float(number)
    except Exception:
        return False
    return True

profile = [
    {
        'name': 'email',
        'type': 'email',
    },
    {
        'name': 'onboard',
        'type': 'date',
    },
    {
        'name': 'revenue1',
        'type': 'number',
    },
    {
        'name': 'revenue2',
        'type': 'number',
    },
]

def verify_file(filename):
    with open(filename) as f:
        line_number = 0
        for eachline in f:
            line_number += 1
            eachline = eachline.strip()
            inputs = eachline.split(',')
            account_id = inputs.pop(0).strip()
            if check_account_id(account_id) is False:
                raise Exception('%d account_id: %s' % (line_number, account_id))
            date = inputs.pop(0).strip()
            if check_date(date) is False:
                raise Exception('%d date: %s' % (line_number, date))
            for item in profile:
                name = item['name']
                i_type = item['type']
                value = inputs.pop(0).strip()
                if i_type == 'email':
                    checker = check_email
                elif i_type == 'date':
                    checker = check_date
                elif i_type == 'number':
                    checker = check_number
                else:
                    raise Exception('unknown type: %s' % i_type)
                if checker(value) is False:
                    raise Exception('%d %s %s %s' % (line_number, name, i_type, value))

def verify_delete(filename):
    with open(filename) as f:
        line_number = 0
        for eachline in f:
            line_number += 1
            eachline = eachline.strip()
            inputs = eachline.split(',')
            account_id = inputs.pop(0).strip()
            if check_account_id(account_id) is False:
                raise Exception('%d account_id: %s' % (line_number, acount_id))
            date = inputs.pop(0).strip()
            if check_date(date) is False:
                raise Exception('%d date: %s' % (line_number, date))
            if inputs:
                raise Exception('%d remains: "%s"' % (line_number, inputs))

if __name__ == '__main__':
    import sys
    filename = sys.argv[1]
    verify_file(filename)
