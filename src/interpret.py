#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import yaml
import time
import re
import logging
import types
import codecs
import boto
from log import get_log_level
from sqs_op import complaint_queue, bounce_queue
from db_op import get_accounts, set_metadata_by_account

logger = logging.getLogger(__name__)

context = {}

reserved = {
    'if': 'IF',
    'else': 'ELSE',
    'while': 'WHILE',
    'for': 'FOR',
    'in': 'IN',
    }

tokens = [
    'VARIABLE','NUMBER',
    'PLUS','MINUS','TIMES','DIVIDE','EQUAL',
    'GT', 'GE', 'LT', 'LE', 'EQ', 'NE', 'AND', 'OR', 'MOD',
    'LPAREN','RPAREN',
    'LBRACKET', 'RBRACKET',
    'LBRACE', 'RBRACE',
    'STRING',
    'BUILDIN',
    'COMMA',
    ] + list(reserved.values())
# Tokens

t_PLUS     = r'\+'
t_MINUS    = r'-'
t_TIMES    = r'\*'
t_DIVIDE   = r'/'
t_EQUAL    = r'='
t_GT       = r'>'
t_GE       = r'>='
t_LT       = r'<'
t_LE       = r'<='
t_EQ       = r'=='
t_NE       = r'\!='
t_OR       = r'\|\|'
t_AND      = r'&&'
t_MOD      = r'%'
t_LPAREN   = r'\('
t_RPAREN   = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'
t_LBRACE   = r'\{'
t_RBRACE   = r'\}'
t_BUILDIN  = r'\$[a-zA-Z0-9_\.\$]*'
t_COMMA    = r','

def t_NUMBER(t):
    r'\d+(\.\d+)?'
    t.value = float(t.value)
    return t

def t_STRING(t):
    r'\"[a-zA-Z0-9_=<>\/\$\.@\-\: ]*\"'
    t.value = t.value[1:-1]
    return t

def t_VARIABLE(t):
    r'[a-zA-Z_][a-zA-Z0-9_\.]*'
    t.type = reserved.get(t.value,'VARIABLE')
    return t

# Ignored characters
t_ignore = " \t"

def t_newline(t):
    r'(\r?\n)+'
    t.lexer.lineno += t.value.count("\n")

def t_COMMENT(t):
    r'\#.*'
    pass

def t_error(t):
    raise Exception("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)

# Build the lexer
import ply.lex as lex
lex.lex()

# Parsing rules

precedence = (
    ('nonassoc', 'IFX'),
    ('nonassoc', 'ELSE'),
    ('left', 'OR'),
    ('left', 'AND'),
    ('left', 'GT', 'GE', 'LT', 'LE', 'EQ', 'NE'),
    ('left','PLUS','MINUS'),
    ('left','TIMES','DIVIDE'),
)

# dictionary of variables
variables = {}

func0_dict = {}

def get_current_time():
    return ('number', int(time.time()))
func0_dict['get_current_time'] = get_current_time

func1_dict = {}

def write_log(msg):
    logger.info(unicode(msg))
    return ('number', 0)
func1_dict['log'] = write_log

def get_complaints(source_address):
    ret = complaint_queue.get_dests(source_address)
    return ('array', ret)
func1_dict['get_complaints'] = get_complaints

def get_bounces(source_address):
    ret = bounce_queue.get_dests(source_address)
    return ('array', ret)
func1_dict['get_bounces'] = get_bounces

def get_file_to_array(file_name):
    base_dir = '%s/%s' % (context['job_directory'], context['current_job'])
    full_file_path = '%s/%s' % (base_dir, file_name)
    ret_array = []
    with codecs.open(full_file_path, 'r', 'utf-8') as f:
        for eachline in f:
            ret_array.append(eachline.strip())
    return ('array', ret_array)

func1_dict['get_file_to_array'] = get_file_to_array

func2_dict = {}
def add_to_array(array, item):
    if array in variables:
        var = variables[array]
        if type(var) is not types.ListType:
            raise Exception('%s is not list' % array)
        var.append(item)
        return ('number', 0)
    else:
        raise Exception('list %s is not init'  % array)

func2_dict['add'] = add_to_array

def del_from_array(array, item):
    if array in variables:
        var = variables[array]
        if type(var) is not types.ListType:
            raise Exception('%s is not list' % array)
        var.remove(item)
        return ('number', 0)
    else:
        raise Exception('list %s is not init'  % array)
func2_dict['del'] = del_from_array

def join_string(array, sep):
    if array in variables:
        var = variables[array]
        if type(var) is not types.ListType:
            raise Exception('%s is not list' % array)
        ret = ''
        for item in var:
            item = unicode(item)
            ret = '%s%s%s' % (ret, sep, item)
        return ('string', ret)
    else:
        raise Exception('list %s is not init' % array)

func2_dict['join'] = join_string

func3_dict = {}

def write_to_file(file_name, content, option):
    logger.debug('write_to_file: file_name: %s content: %s option: %s' % (file_name, content, option))
    base_dir = '%s/%s' % (context['job_directory'], context['current_job'])
    full_file_path = '%s/%s' % (base_dir, file_name)
    if option == 'append':
        mode = 'a'
    else:
        mode = 'w'
    content = '%s\n' % unicode(content)
    try:
        with codecs.open(full_file_path, mode, 'utf-8') as f:
            f.write(content)
    except Exception, e:
        raise Exception('write file failed, %s %s %s' % \
                            (full_file_path, mode, e))
    return ('number', 0)

func3_dict['write_to_file'] = write_to_file

def set_metadata_by_account_with_check(account_id, metadata_name, value):
    if len(metadata_name) <= 2 or metadata_name[0:2] != '$$':
        msg = 'invalide metadata name, account_id: %s metadata_name: %s' % \
            (account_name, metadata_name)
        raise Exception(msg)
    set_metadata_by_account(account_id, metadata_name, value)
    return ('number', 0)

func3_dict['set_metadata_by_account'] = set_metadata_by_account_with_check

func5_dict = {}

default_pattern_begin = '\{\{'
default_pattern_end = '\}\}'
def send_mail(conf_file, subject_file, body_file, dest_addr, replacements):
    logger.debug('send_mail: conf_file: %s subject_file: %s body_file: %s dest_addr: %s replacements: %s' % \
                     (conf_file, subject_file, body_file, dest_addr, replacements))
    base_dir = '%s/%s' % (context['job_directory'], context['current_job'])
    conf_file = '%s/%s' % (base_dir, conf_file)
    subject_file = '%s/%s' % (base_dir, subject_file)
    body_file = '%s/%s' % (base_dir, body_file)
    to_addresses = dest_addr

    try:
        with codecs.open(conf_file, 'r', 'utf-8') as f:
            conf1 = yaml.safe_load(f)
    except Exception, e:
        raise Exception('read conf file error: %s %s' % (conf_file, unicode(e)))

    if 'aws_access_key_id' in conf1:
        aws_access_key_id = conf1['aws_access_key_id']
    else:
        aws_access_key_id = None

    if 'aws_secret_access_key' in conf1:
        aws_secret_access_key = conf1['aws_secret_access_key']
    else:
        aws_secret_access_key = None

    if 'region' not in conf1:
        raise Exception('no region in %s' % conf_file)
    region = conf1['region']

    if 'source' not in conf1:
        raise Exception('no source in %s' % conf_file)
    source = conf1['source']

    if 'reply_addresses' not in conf1:
        raise Exception('no reply_addresses in %s' % conf_file)
    reply_addresses = conf1['reply_addresses']

    if 'return_path' not in conf1:
        raise Exception('no return_path in %s' % conf_file)
    return_path = conf1['return_path']

    if 'pattern_begin' not in conf1:
        pattern_begin = default_pattern_begin
    else:
        pattern_begin = conf1['pattern_begin']

    if 'pattern_end' not in conf1:
        pattern_end = default_pattern_end
    else:
        pattern_end = conf1['pattern_end']

    try:
        with codecs.open(subject_file, 'r', 'utf-8') as f:
            subject = f.read()
    except Exception, e:
        raise Exception('read subject file error: %s %s' % (subject_file, unicode(e)))

    if body_file[-5:] == '.html':
        format = 'html'
    elif body_file[-4:] == '.txt':
        format = 'text'
    else:
        raise Exception('email body file should be .html or .txt, %s' % body_file)

    try:
        with codecs.open(body_file, 'r', 'utf-8') as f:
            emailbody = f.read()
    except Exception, e:
        raise Exception('read emailbody file error: %s %s' % (body_file, unicode(e)))

    count = 1
    for replacement in replacements:
        m = '%s%s%s' % (pattern_begin, count, pattern_end)
        p = re.compile(m)
        emailbody, n = re.subn(p, replacement, emailbody)
        if n == 0:
            raise Exception('email mismatch: %s %s %d' % \
                                (body_file, replacement, count))
        count += 1

    if aws_access_key_id and aws_secret_access_key:
        conn = boto.ses.connect_to_region(\
            region, aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    else:
        conn = boto.ses.connect_to_region(region)
    if format == 'html':
        ret = conn.send_email(source, subject, None, to_addresses, format=format, \
                            reply_addresses=reply_addresses, return_path=return_path, html_body=emailbody)
        logger.debug(str(ret))
    else:
        ret = conn.send_email(source, subject, None, to_addresses, format=format, \
                            reply_addresses=reply_addresses, return_path=return_path, text_body=emailbody)
        logger.debug(str(ret))
    return ('number', 0)

func5_dict['send_mail'] = send_mail

def get_data_from_db(index, name):
    account = context['account']
    return account.get_data(indx, name)

def get_buildin_variable(name):
    if context['stage'] != 'body':
        raise Exception('can only get buildin variable in body state: %s' % name)
    account = context['account']
    if name == '$N':
        value = account.count
        return ('number', value)
    elif name == '$account_id':
        value = account.account_id
        return ('string', value)
    elif len(name) > 2 and name[0:2] == '$$':
        name = name[2:]
        return account.get_metadata(name)
    else:
        name = name[1:]
        names = name.split('.')
        if len(names) != 2:
            raise Exception('invalid buildin name: %s' % name)
        try:
            index = int(names[0])
        except Exception, e:
            raise Exception('buildin name is not number: %s' % name)
        value = get_data_from_db(index, names[1])
        return ('string', value)

def set_metadata(name, value):
    if context['stage'] != 'body':
        raise Exception('can only set metadata in body state')
    if len(name) <= 2:
        raise Exception('invalid metadata name: %s' % name)
    if name[0:2] != '$$':
        raise Exception('invalid metadata name: %s' % name)
    name = name[2:]
    account = context['account']
    account.set_metadata(name, value)

class Node(object):
    def __init__(self, nodetype, value, subnodes=[]):
        self.nodetype = nodetype
        self.value = value
        self.subnodes = subnodes
    def __str__(self):
        return str('%s %s %s' % (self.nodetype, self.value, self.subnodes))
    def __repr__(self):
        return str('%s %s %s' % (self.nodetype, self.value, self.subnodes))

class ITPTYPE(object):
    def __init__(self, itptype, value):
        self.itptype = itptype
        self.value = value

def number_node(number):
    node = Node('number', number)
    return node

def string_node(string):
    node = Node('string', string)
    return node

def variable_node(variable):
    node = Node('variable', variable)
    return node

def buildin_variable_node(buildin_variable):
    node = Node('buildin_variable', buildin_variable)
    return node

def empty_list_node():
    node = Node('empty_list', None)
    return node

def opr_node(oper, ops):
    node = Node('opr', oper, ops)
    return node

def interpret(node):
    itp = ITPTYPE('number', 0)
    logger.debug('node: %s' % node)
    if node.nodetype == 'number' or node.nodetype == 'string':
        itp.itptype = node.nodetype
        itp.value = node.value
        return itp
    elif node.nodetype == 'variable':
        names = node.value.split('.')
        name = names[0]
        if name in variables:
            if len(names) == 1:
                itp.value = variables[name]
                if type(itp.value) is types.ListType:
                    itp.value = itp.value[:]
                    itp.itptype = 'array'
                elif type(itp.value) is types.DictType:
                    itp.value = itp.value.copy()
                    itp.itptype = 'dict'
            elif len(names) == 2:
                attr = names[1]
                itp.value = unicode(variables[name][attr])
            else:
                raise Exception('invalid variable: %s' % node.value)
        else:
            raise Exception('unknown variable: %s' % name)
        return itp
    elif node.nodetype == 'buildin_variable':
        v = node.value
        (itp.itptype, itp.value) = get_buildin_variable(v)
        return itp
    elif node.nodetype == 'empty_list':
        itp.itptype = 'array'
        itp.value = []
        return itp
    elif node.nodetype == 'opr':
        if node.value == 'EQUAL_VARIABLE':
            itp1 = interpret(node.subnodes[1])
            if type(itp1.value) is types.ListType:
                value = itp1.value[:]
            elif type(itp1.value) is types.DictType:
                value = itp1.value.copy()
            else:
                value = itp1.value
            variables[node.subnodes[0]] = value
            itp.itptype = itp1.itptype
            itp.value = value
            return itp
        elif node.value == 'EQUAL_BUILDIN':
            itp1 = interpret(node.subnodes[1])
            value = unicode(itp1.value)
            buildin_name = node.subnodes[0]
            set_metadata(buildin_name, value)
            itp.value = value
            itp.itptype = 'string'
            return itp
        elif node.value == 'PLUS':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = None
            v2 = None
            try:
                v1 = float(itp1.value)
            except Exception, e:
                pass
            try:
                v2 = float(itp2.value)
            except Exception, e:
                pass
            if v1 and v2:
                itp.itptype = 'number'
                itp.value = v1 + v2
            else:
                itp.itptpe = 'string'
                itp.value = unicode(itp1.value) + unicode(itp2.value)
            return itp
        elif node.value == 'MINUS':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            itp.value = v1 - v2
            return itp
        elif node.value == 'TIMES':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            itp.value = v1 * v2
            return itp
        elif node.value == 'DIVIDE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            itp.value = v1 / v2
            return itp
        elif node.value == 'GT':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            if (v1 > v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'GE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            if (v1 >= v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'LT':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            if (v1 < v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'LE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            if (v1 <= v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'EQ':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            try:
                v1 = float(itp1)
                v2 = float(itp2)
            except Exception:
                v1 = unicode(itp1.value)
                v2 = unicode(itp2.value)
            if (v1 == v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'NE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            try:
                v1 = float(itp1)
                v2 = float(itp2)
            except Exception:
                v1 = unicode(itp1.value)
                v2 = unicode(itp2.value)
            if (v1 != v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'AND':
            itp1 = interpret(node.subnodes[0])
            v1 = float(itp1.value)
            if not v1:
                itp.value = 0
                return itp
            itp2 = interpret(node.subnodes[1])
            v2 = float(itp2.value)
            itp.value = v1 and v2
            return itp
        elif node.value == 'OR':
            itp1 = interpret(node.subnodes[0])
            v1 = float(itp1.value)
            if v1:
                itp.value = 1
                return itp
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            itp.value = v1 or v2
            return itp
        elif node.value == 'MOD':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = float(itp1.value)
            v2 = float(itp2.value)
            itp.value = v1 % v2
            return itp
        elif node.value == 'fun0':
            func_name = node.subnodes[0][1:]
            if func_name not in func0_dict:
                raise Exception('no such function or funchtion is not one parameters: %s' % func_name)
            func = func0_dict[func_name]
            (itp.itptype, itp.value) = func()
            return itp
        elif node.value == 'fun1':
            func_name = node.subnodes[0][1:]
            if func_name not in func1_dict:
                raise Exception('no such function or funchtion is not one parameters: %s' % func_name)
            func = func1_dict[func_name]
            itp1 = interpret(node.subnodes[1])
            (itp.itptype, itp.value) = func(itp1.value)
            return itp
        elif node.value == 'fun2':
            func_name = node.subnodes[0][1:]
            if func_name not in func2_dict:
                raise Exception('no such function or funchtion is not two parameters: %s' % func_name)
            func = func2_dict[func_name]
            if func_name != 'add' and func_name != 'del' and func_name != 'join':
                itp1 = interpret(node.subnodes[1])
                param1 = itp1.value
            else:
                param1 = node.subnodes[1].value
            itp2 = interpret(node.subnodes[2])
            (itp.itptype, itp.value) = func(param1, itp2.value)
            return itp
        elif node.value == 'fun3':
            func_name = node.subnodes[0][1:]
            if func_name not in func3_dict:
                raise Exception('no such function or funchtion is not three parameters: %s' % func_name)
            func = func3_dict[func_name]
            itp1 = interpret(node.subnodes[1])
            itp2 = interpret(node.subnodes[2])
            itp3 = interpret(node.subnodes[3])
            (itp.itptype, itp.value) = func(itp1.value, itp2.value, itp3.value)
            return itp
        elif node.value == 'fun4':
            func_name = node.subnodes[0][1:]
            if func_name not in func4_dict:
                raise Exception('no such function or funchtion is not four parameters: %s' % func_name)
            func = func4_dict[func_name]
            itp1 = interpret(node.subnodes[1])
            itp2 = interpret(node.subnodes[2])
            itp3 = interpret(node.subnodes[3])
            itp4 = interpret(node.subnodes[4])
            (itp.itptype, itp.value) = func(itp1.value, itp2.value, itp3.value, itp4.value)
            return itp
        elif node.value == 'fun5':
            func_name = node.subnodes[0][1:]
            if func_name not in func5_dict:
                raise Exception('no such function or funchtion is not five parameters: %s' % func_name)
            func = func5_dict[func_name]
            itp1 = interpret(node.subnodes[1])
            itp2 = interpret(node.subnodes[2])
            itp3 = interpret(node.subnodes[3])
            itp4 = interpret(node.subnodes[4])
            itp5 = interpret(node.subnodes[5])
            (itp.itptype, itp.value) = func(itp1.value, itp2.value, itp3.value, itp4.value, itp5.value)
            return itp
        elif node.value == 'FOR':
            iter_name = node.subnodes[0]
            iter_node = variable_node(iter_name)
            list_name = node.subnodes[1]
            list_value = variables[list_name]
            if type(list_value) is not types.ListType:
                raise Exception('%s is not list, can not loop' % list_name)
            for item in list_value:
                variables[iter_name] = item
                itp = interpret(node.subnodes[2])
            return itp
        elif node.value == 'IF':
            itp1 = interpret(node.subnodes[0])
            if itp1.value:
                itp = interpret(node.subnodes[1])
            elif len(node.subnodes) == 3:
                itp = interpret(node.subnodes[2])
            return itp
        elif node.value == 'WHILE':
            itp1 = interpret(node.subnodes[0])
            while itp1.value:
                itp2 = interpret(node.subnodes[1])
                itp1 = interpret(node.subnodes[0])
            return itp2
        elif node.value == 'stmtlist':
            itp1 = interpret(node.subnodes[0])
            itp = interpret(node.subnodes[1])
            return itp
        else:
            raise Exception('unknown opr type %s' % node.value)
    else:
        raise Exception('unknown node nodetype %s' % node.nodetype)

def p_parsed_1(t):
    'parsed : '
    pass

def p_parsed_2(t):
    'parsed : parsed stmt'
    interpret(t[2])

def p_stmt_assign_variable(t):
    'stmt : VARIABLE EQUAL expr'
    t[0] = opr_node('EQUAL_VARIABLE', [t[1], t[3]])

def p_stmt_assign_buildin(t):
    'stmt : BUILDIN EQUAL expr'
    t[0] = opr_node('EQUAL_BUILDIN', [t[1], t[3]])

def p_stmt_expr(t):
    'stmt : expr'
    t[0] = t[1]

def p_stmt_for(t):
    'stmt : FOR VARIABLE IN VARIABLE stmt'
    t[0] = opr_node('FOR', [t[2], t[4], t[5]])

def p_stmt_if_1(t):
    'stmt : IF LPAREN expr RPAREN stmt %prec IFX'
    t[0] = opr_node('IF', [t[3], t[5]])

def p_stmt_if_2(t):
    'stmt : IF LPAREN expr RPAREN stmt ELSE stmt'
    t[0] = opr_node('IF', [t[3], t[5], t[7]])

def p_stmt_while(t):
    'stmt : WHILE LPAREN expr RPAREN stmt'
    t[0] = opr_node('WHILE', [t[3], t[5]])

def p_stmt_stmtlist(t):
    'stmt : LBRACE stmtlist RBRACE'
    t[0] = t[2]

def p_stmtlist_1(t):
    'stmtlist : stmt'
    t[0] = t[1]

def p_stmtlist_2(t):
    'stmtlist : stmtlist stmt'
    t[0] = opr_node('stmtlist', [t[1], t[2]])

def p_expr_number(t):
    'expr : NUMBER'
    t[0] = number_node(t[1])

def p_expr_string(t):
    'expr : STRING'
    t[0] = string_node(t[1])

def p_expr_empty_list(t):
    'expr : LBRACKET RBRACKET'
    t[0] = empty_list_node()

def p_expr_variable(t):
    'expr : VARIABLE'
    t[0] = variable_node(t[1])

def p_expr_buildin_variable(t):
    'expr : BUILDIN'
    t[0] = buildin_variable_node(t[1])

def p_expression_plus(t):
    'expr : expr PLUS expr'
    t[0] = opr_node('PLUS', [t[1], t[3]])

def p_expression_minus(t):
    'expr : expr MINUS expr'
    t[0] = opr_node('MINUS', [t[1], t[3]])

def p_expression_times(t):
    'expr : expr TIMES expr'
    t[0] = opr_node('TIMES', [t[1], t[3]])

def p_expression_divide(t):
    'expr : expr DIVIDE expr'
    t[0] = opr_node('DIVIDE', [t[1], t[3]])

def p_expression_gt(t):
    'expr : expr GT expr'
    t[0] = opr_node('GT', [t[1], t[3]])

def p_expression_ge(t):
    'expr : expr GE expr'
    t[0] = opr_node('GE', [t[1], t[3]])

def p_expression_lt(t):
    'expr : expr LT expr'
    t[0] = opr_node('LT', [t[1], t[3]])

def p_expression_le(t):
    'expr : expr LE expr'
    t[0] = opr_node('LE', [t[1], t[3]])

def p_expression_eq(t):
    'expr : expr EQ expr'
    t[0] = opr_node('EQ', [t[1], t[3]])

def p_expression_ne(t):
    'expr : expr NE expr'
    t[0] = opr_node('NE', [t[1], t[3]])

def p_expression_and(t):
    'expr : expr AND expr'
    t[0] = opr_node('AND', [t[1], t[3]])

def p_expression_or(t):
    'expr : expr OR expr'
    t[0] = opr_node('OR', [t[1], t[3]])

def p_expression_mod(t):
    'expr : expr MOD expr'
    t[0] = opr_node('MOD', [t[1], t[3]])

def p_expression_paren(t):
    'expr : LPAREN expr RPAREN'
    t[0] = t[2]

def p_expression_buildin_function0(t):
    'expr : BUILDIN LPAREN RPAREN'
    t[0] = opr_node('fun0', [t[1]])

def p_expression_buildin_function1(t):
    'expr : BUILDIN LPAREN expr RPAREN'
    t[0] = opr_node('fun1', [t[1], t[3]])

def p_expression_buildin_function2(t):
    'expr : BUILDIN LPAREN expr COMMA expr RPAREN'
    t[0] = opr_node('fun2', [t[1], t[3], t[5]])

def p_expression_buildin_function3(t):
    'expr : BUILDIN LPAREN expr COMMA expr COMMA expr RPAREN'
    t[0] = opr_node('fun3', [t[1], t[3], t[5], t[7]])

def p_expression_buildin_function4(t):
    'expr : BUILDIN LPAREN expr COMMA expr COMMA expr COMMA expr RPAREN'
    t[0] = opr_node('fun4', [t[1], t[3], t[5], t[7], t[9]])

def p_expression_buildin_function5(t):
    'expr : BUILDIN LPAREN expr COMMA expr COMMA expr COMMA expr COMMA expr RPAREN'
    t[0] = opr_node('fun5', [t[1], t[3], t[5], t[7], t[9], t[11]])

def p_error(t):
    print("Syntax error at '%s'" % t.value)

import ply.yacc as yacc
yacc.yacc()

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

interpret_log_level = conf['interpret_log_level']
script_name = conf['script_name']
log_file = conf['log_file']

p_begin = re.compile(r'begin\s*\{')
p_body = re.compile(r'\}\s*body\s*\{')
p_end = re.compile(r'\}\s*end\s*\{')
def get_script(script_file):
    with open(script_file, 'r') as f:
        script_string = f.read()
    p = p_begin.search(script_string)
    if not p:
        raise Exception('script no begin field, %s' % script_file)
    begin_start = p.start() + len(p.group())
    p = p_body.search(script_string)
    if not p:
        raise Exception('script no body field, %s' % script_file)
    begin_stop=p.start()
    body_start=p.start() + len(p.group())
    p = p_end.search(script_string)
    if not p:
        raise Exception('script no end field, %s' % script_file)
    body_stop=p.start()
    end_start = p.start() + len(p.group())
    index = len(script_string) - 1
    while index >= 0:
        if script_string[index] == '}':
            break
        index -= 1
    if index < 0:
        raise Exception('script no last paren, %s' % script_file)
    end_stop = index
    begin = script_string[begin_start:begin_stop]
    body = script_string[body_start:body_stop]
    end = script_string[end_start:end_stop]
    return (begin, body, end)

def do_job(job_directory, current_job):
    datefmt='%Y-%m-%d %H:%M:%S'
    log_level = get_log_level(interpret_log_level)
    logger.setLevel(log_level)
    logfile = '%s/%s/%s' % (job_directory, current_job, log_file)
    fh = logging.FileHandler(logfile)
    fmt = ('%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s')
    datefmt='%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    fh.setFormatter(formatter)
    fh.setLevel(log_level)
    logger.addHandler(fh)
    logger.info('start job: %s', current_job)
    script_file = '%s/%s/%s' % (job_directory, current_job, script_name)
    begin, body, end = get_script(script_file)

    context['job_directory'] = job_directory
    context['current_job'] = current_job
    context['script_name'] = script_name
    context['stage'] = 'begin'
    yacc.parse(begin)
    context['stage'] = 'body'
    body = body.strip()
    if body:
        accounts = get_accounts()
        parsed_account = 0
        for account in accounts:
            context['account'] = account
            logger.debug('parsing account: %s', account[account_id])
            if account.count > 0:
                yacc.parse(body)
            parsed_account += 1
            if parsed_account % 100 == 0:
                logging.info('parsed account: %d', parsed_account)
        logger.info('total parsed account: %d', parsed_account)
    context['stage'] = 'end'
    yacc.parse(end)
    complaint_queue.delete_messages()
    bounce_queue.delete_messages()
    logger.info('stop job: %s', current_job)

if __name__ == '__main__':
    job_directory = sys.argv[1]
    current_job = sys.argv[2]
    try:
        do_job(job_directory, current_job)
    except Exception, e:
        logger.exception('run job failed')
