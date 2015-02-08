#! /usr/bin/env python

import os
import time
import zipfile
import sqlite3
import datetime
import time
import yaml
import json
import logging
import shutil
from crontab import CronTab
from flask import Flask, request, redirect, url_for, render_template, abort, Response
from werkzeug import secure_filename
from flask.ext.login import LoginManager, login_required, UserMixin, login_user, logout_user
from log import get_log_level
from worker import insert_to_table, delete_from_table

logger = logging.getLogger(__name__)

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

server_log_level = conf['server_log_level']
table_lock_id = conf['table_lock_id']
job_directory = conf['job_directory']
schedule_name = conf['schedule_name']
interpret_file = conf['interpret_file']
task_db = conf['task_db']
task_table = conf['task_table']
task_magic_string = conf['task_magic_string']
script_name = conf['script_name']
log_file = conf['log_file']
login_file = conf['login_file']

log_level = get_log_level(server_log_level)
logger.setLevel(log_level)
fh = logging.FileHandler(log_file)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
datefmt='%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
fh.setFormatter(formatter)
fh.setLevel(log_level)
logger.addHandler(fh)

upload_folder = 'upload'

profile_file_name = 'profile.json'

app = Flask(__name__)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, username, password, userid, active=True):
        self.userid = userid
        self.username = username
        self.password = password
        self.active = active
    def get_id(self):
        return self.userid
    def is_active(self):
        return self.active

class UsersRepository():
    def __init__(self):
        self.users_id = dict()
        self.users_name = dict()
        self.identifier = 0
    def save_user(self, user):
        self.users_id[user.userid] = user
        self.users_name[user.username] = user
    def get_user_by_name(self, username):
        return self.users_name.get(username)
    def get_user_by_id(self, userid):
        return self.users_id.get(userid)
    def next_index(self):
        self.identifier += 1
        return self.identifier

users_repository = UsersRepository()

with open(login_file) as f:
    login_profile = yaml.safe_load(f)

for user in login_profile['users']:
    username = user['username']
    password = user['password']
    new_user = User(username, password, users_repository.next_index())
    users_repository.save_user(new_user)

app.config['SECRET_KEY'] = unicode(login_profile['secret_key'])

@app.route('/', methods=['GET'])
@login_required
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        registeredUser = users_repository.get_user_by_name(username)
        logger.info(registeredUser)
        logger.info(registeredUser.password)
        if registeredUser != None and unicode(registeredUser.password) == unicode(password):
            login_user(registeredUser)
            return redirect(url_for('index'))
        else:
            logger.warning('invalide username or password: %s %s' % (username, password))
            return abort(401)
    else:
        return Response('''
<form actoin="" method="post">
<p><input type=text id=username name=username>
<p><input type=password id=password name=password>
<p><input type=submit id=login_submit name=login_submit value=Login>
</form>
''')

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/insert', methods=['GET', 'POST'])
@login_required
def insert():
    if request.method == 'POST':
        insert_file = request.files['insert_file']
        if not insert_file:
            return 'no insert file'
        filename = secure_filename(insert_file.filename)
        timestamp = '%f' % time.time()
        insert_filename = '%s.%s' % (timestamp, filename)
        insert_filename = os.path.join(upload_folder, insert_filename)
        insert_file.save(insert_filename)
        value = request.form.getlist('overwrite')
        if 'overwrite' in value:
            overwrite = True
        else:
            overwrite = False
        try:
            ret = insert_to_table.apply_async(args=[insert_filename, overwrite])
        except Exception, e:
            os.remove(insert_filename)
            return unicode(e)
        if ret:
            return unicode(ret)
        else:
            return redirect(url_for('insert'))
    return render_template('insert.html')

@app.route('/delete', methods=['GET', 'POST'])
@login_required
def delete():
    if request.method == 'POST':
        delete_file = request.files['delete_file']
        if not delete_file:
            return 'no delete file'
        filename = secure_filename(delete_file.filename)
        timestamp = '%f' % time.time()
        delete_filename = '%s.%s' % (timestamp, filename)
        delete_filename = os.path.join(upload_folder, delete_filename)
        delete_file.save(delete_filename)
        try:
            ret = delete_from_table(delete_filename)
        except Exception, e:
            return unicode(e)
        if ret:
            return unicode(ret)
        else:
            return redirect(url_for('delete'))
    return render_template('delete.html')

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
    package_name_zip = package_zip_full_path.split('/')[-1]
    if package_name_zip[-4:] != '.zip':
        raise Exception('%s is not zip file' % package_name_zip)
    unzip_file(package_zip_full_path, job_directory)
    os.remove(package_zip_full_path)
    package_name = package_name_zip[0:-4]
    if run_immediately:
        cx = sqlite3.connect(task_db)
        cu = cx.cursor()
        cmd = 'delete from %s where magic_string="%s" and status="done"' % \
            (task_table, task_magic_string)
        cu.execute(cmd)
        cx.commit()
        status = 'doing'
        cmd = 'insert into %s values("%s", "%s", "%s")' % \
            (task_table, task_magic_string, package_name, status)
        try:
            cu.execute(cmd)
            cx.commit()
        except Exception, e:
            cu.close()
            cx.close()
            raise Exception('submit job failed: %s' % unicode(e))
        cu.close()
        cx.close()
    try:
        schedule_file = '%s/%s/%s' % (job_directory, package_name, schedule_name)
        with open(schedule_file, 'r') as f:
            schedule_policy = f.read().strip()
    except Exception, e:
        return
    command = '%s/%s %s' % (os.path.split(os.path.realpath(__file__))[0], interpret_file, package_name)
    comment = package_name
    cron = CronTab()
    job = cron.new(command=command, comment=comment)
    job.setall(schedule_policy)
    job.enable(True)
    if not job.is_valid():
        raise Exception('schedule policy maybe invalide: %s' % schedule_policy)
    cron.write()

def delete_package(package_name):
    package_name_zip = '%s.zip' % package_name
    package_zip_full_path = '%s/%s' % (job_directory, package_name_zip)
    package_full_path = '%s/%s' % (job_directory, package_name)
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
        logger.warning('remove pacakge failed: %s' % unicode(e))
        pass

@app.route('/script', methods=['GET', 'POST'])
@login_required
def script():
    if request.method == 'POST':
        action = request.args.get('action')
        if action == 'upload':
            script_package = request.files['script_package']
            filename = secure_filename(script_package.filename)
            download_filename = os.path.join(job_directory, filename)
            script_package.save(download_filename)
            value = request.form.getlist('run_immediately')
            if 'run_immediately' in value:
                run_immediately = True
            else:
                run_immediately = False
            try:
                extract_package_and_add_to_cron(download_filename, run_immediately)
            except Exception, e:
                return unicode(e)
        elif action == 'delete':
            package_name = request.args.get('name')
            delete_package(package_name)
            try:
                delete_package(package_name)
            except Exception, e:
                return unicode(e)
        return redirect(url_for('script'))
    packages = []
    for name in os.listdir(job_directory):
        package_full_path = os.path.join(job_directory, name)
        if os.path.isdir(package_full_path):
            schedule_file = os.path.join(package_full_path, schedule_name)
            try:
                with open(schedule_file, 'r') as f:
                    schedule = f.read().strip()
            except Exception, e:
                schedule = 'NA'
            package = {'name': name,
                       'schedule': schedule}
            packages.append(package)
    return render_template('script.html', packages=packages)

@app.route('/script/<package_name>', methods=['GET'])
@login_required
def show_package(package_name=None):
    show = request.args.get('show')
    if show == 'log':
        file_path = '%s/%s/%s' % (job_directory, package_name, log_file)
    elif show == 'script':
        file_path = '%s/%s/%s' % (job_directory, package_name, script_name)
    else:
        return 'un support item: %s' % show
    try:
        with open(file_path, r'r') as f:
            show_string = f.read()
    except Exception, e:
        return 'read %s error: %s' % (file_path, unicode(e))
    return render_template('show_package.html', show_string=show_string)

@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    if request.method == 'POST':
        profile_file = request.files['profile_file']
        if not profile_file:
            return 'no profilefile'
        profile_filename = os.path.join(upload_folder, profile_file_name)
        profile_file.save(profile_filename)
        redirect(url_for('profile'))
    with open(os.path.join(upload_folder, profile_file_name)) as f:
        profile = json.load(f)
    return render_template('profile.html', profile=profile)
@login_manager.user_loader
def load_user(userid):
    return users_repository.get_user_by_id(userid)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=80)
