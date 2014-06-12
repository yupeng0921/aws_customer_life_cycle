#! /usr/bin/env python

import sys
import os
import logging
import yaml
import types
import codecs
import boto
import boto.ses
from boto.dynamodb2.table import Table
import sqlite3
from pyparsing import *

with open(u'%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], u'r') as f:
    conf = yaml.safe_load(f)
dynamodb_name = conf[u'dynamodb_name']
region = conf[u'region']
sqlitedb_name = conf[u'sqlitedb_name']
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

number = Word(nums + ".")
string = Suppress('"') + Word(alphanums+'_'+'@'+'.'+'/'+'-') + Suppress('"')
variable = Word('$', alphanums+'_'+'@'+'.'+'$')
symbol = number | variable
gt = symbol + ">" + symbol
ge = symbol + ">=" + symbol
lt = symbol + "<" + symbol
le = symbol + "<=" + symbol
eq = symbol + "==" + symbol
condition = gt | ge | lt | le | eq
match = ZeroOrMore(condition + "and") + condition

can_assign = string | number
assign = variable + '=' + can_assign
parameter = string | number | variable
function = variable + "(" + ZeroOrMore(parameter + Suppress(",")) + Optional(parameter) + ")"
action = assign | function

def parse_file(file_name, job_list):
    logging.info('parse_file: %s' % file_name)
    f = open(file_name, 'r')
    line_number = 0
    stage = None
    current_match = None
    current_action = None
    for eachline in f:
        line_number += 1
        eachline = eachline.strip()
        if not eachline:
            continue
        if eachline == 'match:':
            logging.debug('begin match: %d' % line_number)
            if current_match and current_action:
                d = {'match': current_match,
                     'action': current_action}
                job_list.append(d)
            stage = 'parse_match'
            current_match = []
            continue
        elif eachline == 'action:':
            logging.debug('begin action: %d' % line_number)
            stage = 'parse_action'
            current_action = []
            continue
        logging.debug('stage: %s' % stage)
        if stage == 'parse_match':
            try:
                logging.debug('want to parse match: %s' % eachline)
                m = match.parseString(eachline)
                logging.debug('match parse result: %s' % unicode(m))
            except Exception, e:
                logging.warning('parse_match error, %d, %s' % line_number, unicode(e))
                continue
            current_match.append(list(m))
        elif stage == 'parse_action':
            if not current_match:
                continue
            try:
                logging.debug('want to parse action: %s' % eachline)
                a = action.parseString(eachline)
                logging.debug('action parse result: %s' % unicode(a))
            except Exception, e:
                logging.warning('parse_action error, %d, %s' % (line_number, unicode(e)))
                continue
            current_action.append(list(a))
    if current_match and current_action:
        d = {'match': current_match,
             'action': current_action}
        job_list.append(d)
    return job_list

def get_data_from_db(index, name, context):
    logging.debug('get_data_from_db: index: %s name: %s' % \
                      (unicode(index), unicode(name)))
    if index < 1:
        logging.warning('index < 1, %s' % unicode(index))
        return '0'
    items = context['items']
    fetched_items = context['fetched_items']
    if not fetched_items:
        try:
            item = items.next()
        except Exception, e:
            logging.warning('db has no data for account: %s' % \
                                context.account_id)
            return '0'
        fetched_items.append(item)

    item = fetched_items[0]
    if item['date'] != '0':
        index -= 1

    while len(fetched_items) < (index+1):
        try:
            item = items.next()
        except Exception, e:
            logging.warning('db has no enough data, account: %s index: %s' % \
                                contexxt.account_id, unicode(index))
            return '0'
        fetched_items.append(item)

    item = fetched_items[index]
    if name in item:
        return item[name]
    else:
        logging.warning('db has no such attribute, %s' % name)
        return '0'

def get_metadata_from_db(name, context):
    items = context['items']
    fetched_items = context['fetched_items']
    if not fetched_items:
        try:
            item = items.next()
        except Exception, e:
            return '0'
        fetched_items.append(item)
    item = fetched_items[0]
    if item['date'] != '0':
        return '0'
    if name in item:
        return item[name]
    else:
        return '0'
    
def explain(value, context):
    logging.debug('try to explain: %s' % unicode(value))
    if value == '$N':
        return context['count']
    elif value == '$account_id':
        return context['account_id']
    elif len(value) > 2 and value[0:2] == '$$':
        v1 = value[2:]
        return get_metadata_from_db(v1, context)
    elif len(value) > 1 and value[0] == '$':
        v1 = value[1:]
        values = v1.split('.')
        if len(values) != 2:
            logging.warning('invalid variable: %s' % value)
            return '0'
        try:
            index = int(values[0])
        except Exception, e:
            logging.warning('invalid variable: %s' % value)
            return '0'
        return get_data_from_db(index, values[1], context)
    else:
        return value

def do_condition(condition, left, right):
    support_types = [types.IntType,types.StringType,types.UnicodeType]
    if type(left) not in support_types:
        logging.warning('unsupport type, %s, %s' % left, type(left))
        return False
    if type(right) not in support_types:
        logging.warning('unsupport type, %s, %s' % right, type(right))
        return False
    left1 = None
    try:
        left1 = int(left)
    except Exception, e:
        pass
    right1 = None
    try:
        right1 = int(right)
    except Exception, e:
        pass
    if left1 and right1:
        left = left1
        right = right1
        
    if condition == '>':
        return left > right
    elif condition == '>=':
        return left >= right
    elif condition == '<':
        return left < right
    elif condition == '<=':
        return left <= right
    elif condition == '==':
        return left == right
    else:
        logging.warning('unsupport condition: %s' % condition)
        return False

def do_match(context, match):
    match = match[:]
    logging.debug('try to match: %s' % unicode(match))
    while len(match) >= 3:
        logging.debug('going to match: %s' % unicode(match))
        left = match.pop(0)
        condition = match.pop(0)
        right = match.pop(0)
        left = explain(left, context)
        right = explain(right, context)
        logging.debug('condition: %s left: %s right %s' % (condition, left, right))
        ret = do_condition(condition, left, right)
        logging.debug('condition result: %s' % unicode(ret))
        if not ret:
            return False
        if len(match) > 0:
            match.pop(0)
    if match:
        logging.warning('remain match: %s' % unicode(match))
    return True

def set_metadata_to_db(name, value, context):
    logging.info('name: %s value: %s' % (name, value))

def send_mail(parameters):
    if len(parameters) < 4:
        logging.error('send_email has no enough parameters: %s' % unicode(parameters))
        return
    base_dir = '/home/ec2-user/life_cycle/1/job/test1'
    conf_file = parameters.pop(0)
    conf_file = '%s/%s' % (base_dir, conf_file)
    subject_file = parameters.pop(0)
    subject_file = '%s/%s' % (base_dir, subject_file)
    body_file = parameters.pop(0)
    body_file = '%s/%s' % (base_dir, body_file)
    to_addresses = parameters.pop(0)

    try:
        with codecs.open(conf_file, 'r', 'utf-8') as f:
            conf1 = yaml.safe_load(f)
    except Exception, e:
        logging.error('load conf_file error, %s %s' % \
                          (conf_file, unicode(e)))
        return

    aws_access_key_id = conf1['aws_access_key_id']
    aws_secret_access_key = conf1['aws_secret_access_key']
    region = conf1['region']
    source = conf1['email_address']

    with codecs.open(subject_file, 'r', 'utf-8') as f:
        subject = f.read()

    with codecs.open(body_file, 'r', 'utf-8') as f:
        emailbody = f.read()

    conn = boto.ses.connect_to_region(region, aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)

    conn.send_email(source, subject, None, \
                        to_addresses, format='html', reply_addresses=source, \
                        return_path=source, html_body=emailbody)

buildin_func = {}
buildin_func['send_mail'] = send_mail
def call_function(name, parameters, context):
    logging.debug('call function, name: %s parameters: %s' % (name, unicode(parameters)))
    func = buildin_func[name]
    func(parameters)

def do_action(context, action):
    action = action[:]
    if len(action) == 3 and action[0][0:2] == '$$' and action[1] == '=':
        name = action[0][2:]
        value = explain(action[2], context)
        set_metadata_to_db(name, value, context)
    elif len(action) >= 3 and action[1] == '(' and action[-1] == ')':
        name = action[0][1:]
        parameters = []
        for p in action[2:-1]:
            parameters.append(explain(p, context))
        call_function(name, parameters, context)
    else:
        logging.warning('unknow action: %s' % action)

def do_once(context, job):
    match_list = job['match'][:]
    action_list = job['action'][:]
    logging.debug('match and action list: %s %s' % \
                      (unicode(match_list), unicode(action_list)))
    already_match = False
    for match in match_list:
        already_match = do_match(context, match)
        if already_match:
            break
    if not already_match:
        return None
    for action in action_list:
        do_action(context, action)

def run(job_list):
    conn = boto.dynamodb2.connect_to_region(region)
    table = Table(dynamodb_name, connection=conn)
    cx = sqlite3.connect(sqlitedb_name)
    cu = cx.cursor()
    cmd = 'select * from %s' % account_id_table
    cu.execute(cmd)
    ret = cu.fetchone()
    i = 0
    while ret:
        logging.debug('account_id and count: %s' % unicode(ret))
        account_id = ret[0]
        count = ret[1]
        items = table.query(account_id__eq=account_id, reverse=False)
        fetched_items = []
        context = {'account_id': account_id,
                   'count': count,
                   'items': items,
                   'fetched_items': fetched_items,
                   'table': table
                   }
        for job in job_list:
            do_once(context, job)
        ret = cu.fetchone()
    cu.close()
    cx.close()

def do_job(job_dir_list):
    logging.info('start_job')
    job_list = []
    logging.info('job_dir_list: %s' % unicode(job_dir_list))
    for job_dir in job_dir_list:
        query_file = u'%s/run.q' % job_dir
        parse_file(query_file, job_list)
    run(job_list)
    logging.info('stop_job')

if __name__ == '__main__':
    test_dir = sys.argv[1]
    do_job([test_dir])
