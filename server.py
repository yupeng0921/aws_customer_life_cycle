#! /usr/bin/env python

import os
import time
import sqlite3
import yaml
import logging
from flask import Flask, request, redirect, url_for, render_template, abort
from werkzeug import secure_filename

import boto
from boto.dynamodb2.table import Table

with open(u'%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], u'r') as f:
    conf = yaml.safe_load(f)

data_db_name = conf[u'data_db_name']
metadata_db_name = conf[u'metadata_db_name']
region = conf[u'region']
account_id_table = conf[u'account_id_table']
log_file = conf[u'log_file']
debug_flag = conf[u'debug_flag']

format = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
datefmt='%Y-%m-%d %H:%M:%S'
if debug_flag == u'debug':
    level = logging.DEBUG
else:
    level = logging.INFO
logging.basicConfig(filename = log_file, level = level, format=format, datefmt=datefmt)

upload_folder = u'upload'

app = Flask(__name__)

@app.route(u'/', methods=[u'GET'])
def index():
    return 'hello'

def insert_to_table(insert_filename, overwrite):
    logging.info(u'insert_filename: %s overwrite: %s' % (insert_filename, overwrite))
    conn = boto.dynamodb2.connect_to_region(region)
    data_table = Table(data_db_name, connection=conn)
    metadata_table = Table(metadata_db_name, connection=conn)
    f = open(insert_filename, u'r')
    line_number = 0
    error_lines_no_overwrite = []
    error_lines_overwrite = []
    for eachline in f:
        line_number += 1
        eachline = eachline.strip()
        try:
            (account_id, date, email, revenue) = eachline.split(u',')
        except Exception, e:
            logging.info(unicode(e))
            error_lines_no_overwrite.append(line_number)
            continue
        data = {u'account_id': account_id,
                u'date': date,
                u'email': email,
                u'revenue': revenue}
        try:
            data_table.put_item(data=data, overwrite=overwrite)
        except Exception, e:
            logging.error(unicode(e))
            error_lines_no_overwrite.append(line_number)
            continue

        data = {u'account_id': account_id, u'count': 0}
        try:
            metadata_table.put_item(data=data, overwrite=False)
        except Exception, e:
            pass

        try:
            item = metadata_table.get_item(account_id=account_id)
        except Exception, e:
            logging.error(unicode(e))
            error_lines_overwrite.append(line_number)
            continue

        if not item:
            logging.error(u'no metadata for %s %d' % (account_id, line_number))
            error_lines_overwrite.append(line_number)

        item[u'count'] = item[u'count'] + 1
        try:
            item.partial_save()
        except Exception, e:
            logging.error(unicode(e))
            error_lines_overwrite.append(line_number)
    f.close()
    ret_dict = {}
    if error_lines_no_overwrite or error_lines_overwrite:
        ret_dict[u'no_overwrite_error'] = error_lines_no_overwrite
        ret_dict[u'overwrite_error'] = error_lines_overwrite
    return ret_dict

@app.route(u'/insert', methods=[u'GET', u'POST'])
def insert():
    if request.method == u'POST':
        insert_file = request.files[u'insert_file']
        if not insert_file:
            return u'no insert file'
        filename = secure_filename(insert_file.filename)
        timestamp = u'%f' % time.time()
        insert_filename = u'%s.%s' % (timestamp, filename)
        insert_filename = os.path.join(upload_folder, insert_filename)
        insert_file.save(insert_filename)
        value = request.form.getlist(u'overwrite')
        if u'overwrite' in value:
            overwrite = True
        else:
            overwrite = False
        try:
            ret = insert_to_table(insert_filename, overwrite)
        except Exception, e:
            os.remove(insert_filename)
            return unicode(e)
        if ret:
            return unicode(ret)
        else:
            return redirect(url_for(u'insert'))
    return render_template(u'insert.html')

# def delete_from_table(delete_filename):
#     logging.info(u'delete_filename: %s' % delete_filename)
#     conn = boto.dynamodb2.connect_to_region(region)
#     table = Table(data_db_name, connection=conn)
#     f = open(delete_filename, u'r')
#     line_number = 0
#     error_lines = []
#     for eachline in f:
#         line_number += 1
#         eachline = eachline.strip()
#         try:
#             (account_id, date) = eachline.split(u',')[0:2]
#         except Exception, e:
#             logging.info(unicode(e))
#             error_lines.append(line_number)
#             continue
#         try:
#             table.delete_item(account_id=account_id, date=date)
#         except Exception, e:
#             logging.info(unicode(e))
#             error_lines.append(line_number)
#             continue
#     f.close()
#     return error_lines

@app.route(u'/delete', methods=[u'GET', u'POST'])
def delete():
    if request.method == u'POST':
        delete_file = request.files[u'delete_file']
        if not delete_file:
            return u'no delete file'
        filename = security_filename(delete_file.filename)
        timestamp = u'%f' % time.time()
        delete_filename = u'%s.%s' % (timestamp, filename)
        delete_filename = os.path.join(upload_folder, delete_filename)
        delete_file.save(delete_filename)
        try:
            ret = delete_from_table(delete_filename)
        except Exception, e:
            os.remove(delete_filename)
            return unicode(e)
        if ret:
            return unicode(ret)
        else:
            return redirect(url_for(u'delete'))
    return render_template(u'delete.html')

if __name__ == u'__main__':
    app.debug = True
    app.run(host=u'0.0.0.0', port=80)
