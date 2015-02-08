#!/usr/bin/env python
import os
import datetime
import time
from celery import Celery
from verify import verify_file, verify_delete
from db_op import insert_data, delete_data, lock, unlock

app = Celery('worker', backend='amqp', broker='amqp://')

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

def change_date_to_epoch_number(date):
    date = date.split('/')
    if len(date) != 3:
        raise Exception('invalid date format')
    date = int(time.mktime(datetime.datetime(int(date[0]),int(date[1]),int(date[2])).timetuple()))
    return date

def do_insert(task, filepath, overwrite, result_info):
    report_interval = 100
    with open(filepath) as f:
        line_number = 0
        for eachline in f:
            line_number += 1
            eachline = eachline.strip()
            inputs = eachline.split(',')
            account_id = inputs.pop(0).strip()
            date = inputs.pop(0).strip()
            date = change_date_to_epoch_number(date)
            data = {}
            for item in profile:
                name = item['name']
                i_type = item['type']
                value = inputs.pop(0).strip()
                if i_type == 'date':
                    value = change_date_to_epoch_number(value)
                data.update({name: value})
            insert_data(account_id, date, data, overwrite)
            if (line_number % report_interval) == 0:
                result_info.append('inserted %d' % line_number)
        if line_number % report_interval:
            result_info.append('inserted %d' % line_number)

@app.task(bind=True)
def insert_to_table(self, filepath, overwrite):
    result_info = []
    try:
        lock()
    except Exception as e:
        result_info.append('lock failed: %s' % str(e))
        return result_info
    try:
        verify_file(filepath)
        result_info.append('verified')
        self.update_state(state='PROGRESS', meta = {'result_info': result_info})
        do_insert(self, filepath, overwrite, result_info)
    except Exception as e:
        result_info.append(str(e))
    else:
        result_info.append('done')
    finally:
        unlock()
    os.remove(filepath)
    return result_info

def do_delete(task, filepath, result_info):
    report_interval = 100
    with open(filepath) as f:
        line_number = 0
        for eachline in f:
            inputs = eachline.split(',')
            account_id = inputs.pop(0).strip()
            date = inputs.pop(0).strip()
            date = change_date_to_epoch_number(date)
            delete_data(account_id, date)
            if (line_number % report_interval) == 0:
                result_info.append('deleted %d' % line_number)
        if line_number % report_interval:
            result_info.append('inserted %d' % line_number)

@app.task(bind=True)
def delete_from_table(self, filepath):
    result_info = []
    try:
        verify_delete(filepath)
        result_info.append('verified')
        self.update_state(state='PROGRESS', meta = {'result_info': result_info})
        do_delete(self, filepath, result_info)
    except Exception as e:
        result_info.append(str(e))
    else:
        result_info.append('done')
    finally:
        unlock()
    os.remove(filepath)
    return result_info
