#! /usr/bin/env python

import os
import time
import sqlite3
import yaml
import re
import codecs
import logging
from flask import Flask, request, redirect, url_for, render_template, abort
from werkzeug import secure_filename

import boto
from boto.dynamodb2.table import Table

with open(u'%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], u'r') as f:
    conf = yaml.safe_load(f)

dynamodb_name = conf[u'dynamodb_name']
region = conf[u'region']
sqlitedb_name = conf[u'sqlitedb_name']

upload_folder = u'upload'
account_id_table = u'account_id'

cx = sqlite3.connect(sqlitedb_name)
cu = cx.cursor()
cmd = u'create table if not exists %s (' % account_id_table + \
    u'account_id varchar(24) primary key' + \
    u')'
cu.execute(cmd)
cx.commit()
cu.close()

app = Flask(__name__)

@app.route(u'/', methods=[u'GET'])
def index():
    return 'hello'

def insert_to_table(insert_filename, overwrite):
    conn = boto.dynamodb2.connect_to_region(region)
    table = Table(table_name, connection=conn)
    f = open(insert_filename, u'r')
    line_number = 0
    error_lines = []
    cu = cx.cursor()
    for eachline in f:
        line_number += 1
        eachline = eachline.strip()
        try:
            (account_id, date, email, revenue) = eachline.split(u',')
        except Exception, e:
            error_lines.append(line_number)
            continue
        data = {u'account_id': account_id,
                u'date': date,
                u'email': email,
                u'revenue': revenue}
        try:
            table.put_item(date=data, overwrite=overwrite)
        except Exception, e:
            error_lines.append(line_number)
            continue
        cmd = u'insert into account_id values("%s")' % account_id
        try:
            cu.execute(cmd)
        except Exception, e:
            pass
    cx.commit()
    cu.close()
    return error_lines

@app.route(u'/insert', methods=[u'GET', u'POST'])
def insert():
    if request.method == u'POST':
        insert_file = request.files[u'insert_file']
        if not insert_file:
            return u'no insert file'
        filename = security_filename(insert_file.filename)
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
            return e
        if ret:
            return unicode(ret)
        else:
            return redirect(url_for(u'insert'))
    return render_template(u'insert.html')

# @app.route(u'/delete', methods=[u'GET', u'POST'])
# def delete():
#     if request.method == u'POST':
#         delete_file = request.files[u'delete_file']
#         if not delete_file:
#             return u'no delete file'
#         filename = security_filename(delete_file.filename)
#         timestamp = u'%f' % time.time()
#         delete_filename = u'%s.%s' % (timestamp, filename)
#         delete_filename = os.path.join(upload_folder, delete_filename)
#         delete_file.save(delete_filename)
#         try:
#             delete_from_table(delete_filename)
#         except Exception, e:
#             os.remove(delete_filename)
#             return e
#         return redirect(url_for(u'delete'))
#     return render_template(u'delete.html')

if __name__ == u'__main__':
    app.debug = True
    app.run(host=u'0.0.0.0', port=80)
