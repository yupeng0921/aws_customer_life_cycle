#! /usr/bin/env python

import os
import time
import zipfile
import sqlite3
import yaml
import logging
import shutil
from crontab import CronTab
from flask import Flask, request, redirect, url_for, render_template, abort
from werkzeug import secure_filename

import boto
from boto.dynamodb2.table import Table

with open(u'%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], u'r') as f:
    conf = yaml.safe_load(f)

data_db_name = conf[u'data_db_name']
metadata_db_name = conf[u'metadata_db_name']
region = conf[u'region']
server_log_file = conf[u'server_log_file']
server_debug_flag = conf[u'server_debug_flag']
table_lock_id = conf[u'table_lock_id']
job_directory = conf[u'job_directory']
schedule_name = conf[u'schedule_name']
interpret_file = conf[u'interpret_file']
task_db = conf[u'task_db']
task_table = conf[u'task_table']
task_magic_string = conf[u'task_magic_string']
script_name = conf[u'script_name']
log_file = conf[u'log_file']

format = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
datefmt='%Y-%m-%d %H:%M:%S'
if server_debug_flag == u'debug':
    level = logging.DEBUG
else:
    level = logging.INFO
logging.basicConfig(filename = server_log_file, level = level, format=format, datefmt=datefmt)

upload_folder = u'upload'

app = Flask(__name__)

@app.route(u'/', methods=[u'GET'])
def index():
    return render_template(u'index.html')

def insert_to_table(insert_filename, overwrite):
    logging.info(u'insert_filename: %s overwrite: %s' % (insert_filename, overwrite))
    conn = boto.dynamodb2.connect_to_region(region)
    data_table = Table(data_db_name, connection=conn)
    metadata_table = Table(metadata_db_name, connection=conn)
    lock_item = metadata_table.get_item(account_id=table_lock_id)
    if not lock_item:
        return u'no lock, check metadata db'
    if u'status' not in lock_item:
        return u'invalid lock, check metadata db'
    if lock_item[u'status'] != u'unlock':
        return u'table is locked, try it later'
    lock_item[u'status'] = u'lock'
    try:
        ret = lock_item.save(overwrite=False)
    except Exception, e:
        return u'lock table failed, try it later'
    if not ret:
        return u'lock table failed, try it later'
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
            continue

        if u'count' not in item:
            logging.error(u'invalid metadata %s %d' % (account_id, line_number))
            error_lines_overwrite.append(line_number)
            continue

        item[u'count'] += 1
        try:
            ret = item.partial_save()
        except Exception, e:
            msg = u'partial_save failed, account_id: %s %s' % (account_id, unicode(e))
            logging.error(msg)
            error_lines_overwrite.append(line_number)
            continue
        else:
            if not ret:
                msg = u'partial_save failed, account_id: %s' % account_id
                logging.error(msg)
                error_lines_overwrite.append(line_number)
                continue
    f.close()
    ret_dict = {}
    lock_item[u'status'] = 'unlock'
    try:
        ret = lock_item.save(overwrite=False)
    except Exception, e:
        msg = u'unlock failed %s' % unicode(e)
        logging.error(msg)
        ret_dict[u'lock_msg'] = msg
    else:
        if not ret:
            msg = u'unlock failed'
            logging.error(msg)
            ret_dict[u'lock_msg'] = msg
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

def delete_from_table(delete_filename):
    logging.info(u'delete_filename: %s' % delete_filename)
    conn = boto.dynamodb2.connect_to_region(region)
    data_table = Table(data_db_name, connection=conn)
    metadata_table = Table(metadata_db_name, connection=conn)
    lock_item = metadata_table.get_item(account_id=table_lock_id)
    if not lock_item:
        return u'no lock, check metadata db'
    if u'status' not in lock_item:
        return u'invalid lock, check metadata db'
    if lock_item[u'status'] != u'unlock':
        return u'table is locked, try it later'
    lock_item[u'status'] = u'lock'
    try:
        ret = lock_item.save(overwrite=False)
    except Exception, e:
        return u'lock table failed, try it later'
    if not ret:
        return u'lock table failed, try it later'
    f = open(delete_filename, u'r')
    line_number = 0
    error_lines = []
    for eachline in f:
        line_number += 1
        eachline = eachline.strip()
        try:
            (account_id, date) = eachline.split(u',')[0:2]
        except Exception, e:
            logging.error(unicode(e))
            error_lines.append(line_number)
            continue

        try:
            ret = data_table.get_item(account_id=account_id, date=date)
        except Exception, e:
            msg = 'get_item failed %s' % unicode(e)
            logging.error(msg)
            error_lines.append(line_number)
            continue
        else:
            if not ret:
                msg = 'get_item failed %s' % unicode(e)
                logging.error(msg)
                error_lines.append(line_number)
                continue

        try:
            ret = data_table.delete_item(account_id=account_id, date=date)
        except Exception, e:
            logging.error(unicode(e))
            error_lines.append(line_number)
            continue
        else:
            if not ret:
                logging.error(u'delete failed: account_id=%s date=%s' % (account_id, date))
                error_lines.append(line_number)

        try:
            item = metadata_table.get_item(account_id=account_id)
        except Exception, e:
            logging.error(unicode(e))
            error_lines.append(line_number)
            continue

        if not item:
            logging.error(u'no metadata for %s %d' % (account_id, line_number))
            error_lines.append(line_number)
            continue

        if u'count' not in item:
            logging.error(u'invalid metadata %s %d' % (account_id, line_number))
            error_lines.append((line_number))
            continue

        item[u'count'] -= 1
        try:
            ret = item.partial_save()
        except Exception, e:
            msg = u'partial_save failed, account_id: %s %s' % (account_id, unicode(e))
            logging.error(msg)
            error_lines.append(line_number)
            continue
        else:
            if not ret:
                msg = u'partial_save failed, account_id: %s' % (account_id)
                logging.error(msg)
                error_lines.append(line_number)
    f.close()
    ret_dict = {}
    lock_item[u'status'] = 'unlock'
    try:
        ret = lock_item.save(overwrite=False)
    except Exception, e:
        msg = u'unlock failed %s' % unicode(e)
        logging.error(msg)
        ret_dict[u'lock_msg'] = msg
    else:
        if not ret:
            msg = u'unlock failed'
            logging.error(msg)
            ret_dict[u'lock_msg'] = msg
    if error_lines:
        ret_dict[u'error_lines'] = error_lines
    return ret_dict

@app.route(u'/delete', methods=[u'GET', u'POST'])
def delete():
    if request.method == u'POST':
        delete_file = request.files[u'delete_file']
        if not delete_file:
            return u'no delete file'
        filename = secure_filename(delete_file.filename)
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

def unzip_file(zipfilename, unziptodir):
    if not os.path.exists(unziptodir): os.mkdir(unziptodir, 0777)
    zfobj = zipfile.ZipFile(zipfilename)
    for name in zfobj.namelist():
        name = name.replace('\\','/')
        if name.endswith('/'):
            os.mkdir(os.path.join(unziptodir, name))
        else:
            ext_filename = os.path.join(unziptodir, name)
            ext_dir= os.path.dirname(ext_filename)
            if not os.path.exists(ext_dir) : os.mkdir(ext_dir,0777)
            outfile = open(ext_filename, 'wb')
            outfile.write(zfobj.read(name))
            outfile.close()

def extract_package_and_add_to_cron(package_zip_full_path, run_immediately):
    package_name_zip = package_zip_full_path.split(u'/')[-1]
    if package_name_zip[-4:] != u'.zip':
        raise Exception(u'%s is not zip file' % package_name_zip)
    unzip_file(package_zip_full_path, job_directory)
    package_name = package_name_zip[0:-4]
    if run_immediately:
        cx = sqlite3.connect(task_db)
        cu = cx.cursor()
        cmd = u'delete from %s where magic_string="%s" and status="done"' % \
            (task_table, task_magic_string)
        cu.execute(cmd)
        cx.commit()
        status = u'doing'
        cmd = u'insert into %s values("%s", "%s", "%s")' % \
            (task_table, task_magic_string, package_name, status)
        try:
            cu.execute(cmd)
            cx.commit()
        except Exception, e:
            cu.close()
            cx.close()
            raise Exception(u'submit job failed: %s' % unicode(e))
        cu.close()
        cx.close()
    schedule_file = u'%s/%s/%s' % (job_directory, package_name, schedule_name)
    with open(schedule_file, u'r') as f:
        schedule_policy = f.read().strip()
    command = u'%s/%s %s' % (os.path.split(os.path.realpath(__file__))[0], interpret_file, package_name)
    comment = package_name
    cron = CronTab()
    job = cron.new(command=command, comment=comment)
    job.setall(schedule_policy)
    job.enable(True)
    if not job.is_valid():
        raise Exception(u'schedule policy maybe invalide: %s' % schedule_policy)
    cron.write()
    os.remove(package_zip_full_path)

def delete_package(package_name):
    package_name_zip = u'%s.zip' % package_name
    package_zip_full_path = u'%s/%s' % (job_directory, package_name_zip)
    package_full_path = u'%s/%s' % (job_directory, package_name)
    try:
        os.remove(package_zip_full_path)
    except Exception, e:
        pass
    cron = CronTab()
    cron.remove_all(comment=package_name)
    cron.write()
    try:
        shutil.rmtree(package_full_path)
    except Exception, e:
        logging.warning(u'remove pacakge failed: %s' % unicode(e))
        pass

@app.route(u'/script', methods=[u'GET', u'POST'])
def script():
    if request.method == u'POST':
        action = request.args.get(u'action')
        if action == u'upload':
            script_package = request.files[u'script_package']
            filename = secure_filename(script_package.filename)
            download_filename = os.path.join(job_directory, filename)
            script_package.save(download_filename)
            value = request.form.getlist(u'run_immediately')
            if u'run_immediately' in value:
                run_immediately = True
            else:
                run_immediately = False
            try:
                extract_package_and_add_to_cron(download_filename, run_immediately)
            except Exception, e:
                return unicode(e)
            
        elif action == u'delete':
            package_name = request.args.get(u'name')
            delete_package(package_name)
            try:
                delete_package(package_name)
            except Exception, e:
                return unicode(e)
        return redirect(url_for(u'script'))
    packages = []
    for name in os.listdir(job_directory):
        package_full_path = os.path.join(job_directory, name)
        if os.path.isdir(package_full_path):
            schedule_file = os.path.join(package_full_path, schedule_name)
            try:
                with open(schedule_file, u'r') as f:
                    schedule = f.read().strip()
            except Exception, e:
                schedule = u'NA'
            package = {'name': name,
                       'schedule': schedule}
            packages.append(package)
    return render_template(u'script.html', packages=packages)

@app.route(u'/script/<package_name>', methods=[u'GET'])
def show_package(package_name=None):
    show = request.args.get(u'show')
    if show == u'log':
        file_path = u'%s/%s/%s' % (job_directory, package_name, log_file)
    elif show == u'script':
        file_path = u'%s/%s/%s' % (job_directory, package_name, script_name)
    else:
        return u'un support item: %s' % show
    try:
        with open(file_path, r'r') as f:
            show_string = f.read()
    except Exception, e:
        return u'read %s error: %s' % (file_path, unicode(e))
    return render_template(u'show_package.html', show_string=show_string)

if __name__ == u'__main__':
    app.debug = True
    app.run(host=u'0.0.0.0', port=80)
