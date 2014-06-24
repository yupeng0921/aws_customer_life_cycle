#! /usr/bin/env python

import sys
import os
import yaml
import re
import logging
import types
import codecs
import boto
import boto.ses
from boto.dynamodb2.table import Table

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
    'GT', 'GE', 'LT', 'LE', 'EQ', 'NE', 'AND', 'OR',
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
t_LPAREN   = r'\('
t_RPAREN   = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'
t_LBRACE   = r'\{'
t_RBRACE   = r'\}'
t_BUILDIN  = r'\$[a-zA-Z0-9_\.\$]*'
t_COMMA    = r','

def t_NUMBER(t):
    r'\d+'
    try:
        t.value = int(t.value)
    except ValueError:
        print("Integer value too large %d", t.value)
        t.value = 0
    return t

def t_STRING(t):
    r'\"[a-zA-Z0-9_\/\.@\-\: ]*\"'
    t.value = t.value[1:-1]
    return t

def t_VARIABLE(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    t.type = reserved.get(t.value,'VARIABLE')
    return t

# Ignored characters
t_ignore = " \t"

def t_newline(t):
# def t_NEWLINE(t):
    r'\n+'
    t.lexer.lineno += t.value.count("\n")

def t_COMMENT(t):
    r'\#.*'
    pass

def t_error(t):
    print("Illegal character '%s'" % t.value[0])
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
    ('right','UMINUS'),
    )

# dictionary of variables
variables = {}

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

func3_dict = {}

def write_to_file(file_name, content, option):
    logging.debug(u'write_to_file: file_name: %s content: %s option: %s' % (file_name, content, option))
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

func5_dict = {}

default_pattern_begin = u'\{\{'
default_pattern_end = u'\}\}'
def send_mail(conf_file, subject_file, body_file, dest_addr, replacements):
    logging.debug(u'send_mail: conf_file: %s subject_file: %s body_file: %s dest_addr: %s replacements: %s' % \
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

    if 'aws_access_key_id' not in conf1:
        raise Exception('no aws_access_key_id in %s' % conf_file)
    aws_access_key_id = conf1['aws_access_key_id']
    if 'aws_secret_access_key' not in conf1:
        raise Exception('no aws_secret_access_key in %s' % conf_file)
    aws_secret_access_key = conf1['aws_secret_access_key']

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
        m = u'%s%s%s' % (pattern_begin, count, pattern_end)
        p = re.compile(m)
        emailbody, n = re.subn(p, replacement, emailbody)
        if n == 0:
            raise Exception('email mismatch: %s %s %d' % \
                                (body_file, replacement, count))
        count += 1

    conn = boto.ses.connect_to_region(region, aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    if format == 'html':
        conn.send_email(source, subject, None, to_addresses, format=format, \
                            reply_addresses=reply_addresses, return_path=return_path, html_body=emailbody)
    else:
        conn.send_email(source, subject, None, to_addresses, format=format, \
                            reply_addresses=reply_addresses, return_path=return_path, text_body=emailbody)
    return ('number', 0)

func5_dict['send_mail'] = send_mail

def get_data_from_db(index, name):
    metadata = context['metadata']
    if index < 0:
        raise Exception('invalide index: %d' % index)
    items = context['items']
    fetched_items = context['fetched_items']
    while len(fetched_items) < (index+1):
        try:
            item = items.next()
        except Exception, e:
            raise Exception('no enough data %s %d' % (metadata['account_id'], index))
        fetched_items.append(item)

    item = fetched_items[index]
    if name in item:
        return item[name]
    else:
        raise Exception('no such field: %s' % name)

def get_buildin_variable(name):
    if context['stage'] != 'body':
        raise Exception('can only get buildin variable in body state')
    metadata = context['metadata']
    if name == '$N':
        value = metadata['count']
        return ('number', value)
    elif name == '$account_id':
        value = metadata['account_id']
        return ('string', value)
    elif len(name) > 2 and name[0:2] == '$$':
        name = name[2:]
        if name not in metadata:
            return ('string', '0')
        else:
            return ('string', metadata[name])
    else:
        name = name[1:]
        names = name.split('.')
        if len(names) != 2:
            raise Exception('invalid buildin name: %s' % name)
        try:
            index = int(names[0])
        except Exception, e:
            raise Exception('buildin name is not number: %s' % name)
        index -= 1
        value = get_data_from_db(index, names[1])
        return ('string', value)

def set_metadata(name, value):
    if context['stage'] != 'body':
        raise Exception('can only set metadata in body state')
    metadata = context['metadata']
    account_id = metadata['account_id']
    if len(name) <= 2:
        raise Exception('invalid metadata name: %s' % name)
    if name[0:2] != '$$':
        raise Exception('invalid metadata name: %s' % name)
    name = name[2:]
    metadata[name] = value
    try:
        ret = metadata.partial_save()
    except Exception, e:
        msg = 'set metadat failed, account_id: %s name: %s %s' % (account_id, name, unicode(e))
        raise Exception(msg)
    else:
        if not ret:
            msg = 'set metadat failed, account_id: %s name: %s' % (account_id, name)
            raise Exception(msg)

class Node():
    def __init__(self, nodetype, value, subnodes=[]):
        self.nodetype = nodetype
        self.value = value
        self.subnodes = subnodes

class ITPTYPE():
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
    if node.nodetype == 'number' or node.nodetype == 'string':
        itp.itptype = node.nodetype
        itp.value = node.value
        return itp
    elif node.nodetype == 'variable':
        if node.value in variables:
            itp.value = variables[node.value]
        else:
            raise Exception('uninit variable: %s' % node.value)
        return itp
    elif node.nodetype == 'buildin_variable':
        v = node.value
        (itp.itptype, itp.value) = get_buildin_variable(v)
        return itp
    elif node.nodetype == 'empty_list':
        itp.value = []
        return itp
    elif node.nodetype == 'opr':
        if node.value == 'EQUAL_VARIABLE':
            itp1 = interpret(node.subnodes[1])
            if type(itp1.value) is types.ListType:
                value = itp1.value[:]
            else:
                value = itp1.value
            variables[node.subnodes[0]] = value
            itp.value = value
            return itp
        elif node.value == u'EQUAL_BUILDIN':
            itp1 = interpret(node.subnodes[1])
            value = unicode(itp1.value)
            buildin_name = node.subnodes[0]
            set_metadata(buildin_name, value)
            itp.value = value
            itp.itptype = 'string'
            return itp
        elif node.value == 'UMINUS':
            itp1 = interpret(node.subnodes[0])
            if itp1.itptype == 'number':
                itp.value = -itp1.value
                return itp
            else:
                raise Exception('unsupport uminus type: %s' % itp1.itptype)
        elif node.value == 'PLUS':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            itp.value = v1 + v2
            return itp
        elif node.value == 'MINUS':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            itp.value = v1 - v2
            return itp
        elif node.value == 'TIMES':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            itp.value = v1 * v2
            return itp
        elif node.value == 'DIVIDE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            itp.value = v1 / v2
            return itp
        elif node.value == 'GT':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            if (v1 > v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'GE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            if (v1 >= v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'LT':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            if (v1 < v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'LE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            if (v1 <= v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'EQ':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
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
            v1 = unicode(itp1.value)
            v2 = unicode(itp2.value)
            if (v1 != v2):
                itp.value = 1
            else:
                itp.value = 0
            return itp
        elif node.value == 'AND':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            itp.value = v1 and v2
            return itp
        elif node.value == 'OR':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            v1 = int(itp1.value)
            v2 = int(itp2.value)
            itp.value = v1 or v2
            return itp
        elif node.value == 'fun2':
            func_name = node.subnodes[0][1:]
            if func_name not in func2_dict:
                raise Exception('no such function or funchtion is not two parameters: %s' % func_name)
            func = func2_dict[func_name]
            if func_name != 'add' and func_name != 'del':
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

def p_expression_uminus(t):
    'expr : MINUS expr %prec UMINUS'
    t[0] = opr_node('UMINUS', [t[2]])

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

with open(u'%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], u'r') as f:
    conf = yaml.safe_load(f)

data_db_name = conf[u'data_db_name']
metadata_db_name = conf[u'metadata_db_name']
region = conf[u'region']
interpret_log_file = conf[u'interpret_log_file']
interpret_debug_flag = conf[u'interpret_debug_flag']
job_directory = conf[u'job_directory']
script_name = conf[u'script_name']
table_lock_id = conf[u'table_lock_id']

format = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
datefmt='%Y-%m-%d %H:%M:%S'
if interpret_debug_flag == u'debug':
    level = logging.DEBUG
else:
    level = logging.INFO
logging.basicConfig(filename = interpret_log_file, level = level, format=format, datefmt=datefmt)

p_begin = re.compile(r'begin\s*\{')
p_body = re.compile(r'\}\s*body\s*\{')
p_end = re.compile(r'\}\s*end\s*\{')
def get_script(script_file):
    with open(script_file, u'r') as f:
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
        if script_string[index] == u'}':
            break
        index -= 1
    if index < 0:
        raise Exception('script no last paren, %s' % script_file)
    end_stop = index
    begin = script_string[begin_start:begin_stop]
    body = script_string[body_start:body_stop]
    end = script_string[end_start:end_stop]
    return (begin, body, end)

def do_job(current_job):
    logging.info('start job: %s' % current_job)
    script_file = u'%s/%s/%s' % (job_directory, current_job, script_name)
    begin, body, end = get_script(script_file)

    conn = boto.dynamodb2.connect_to_region(region)
    data_table = Table(data_db_name, connection=conn)
    metadata_table = Table(metadata_db_name, connection=conn)

    context[u'job_directory'] = job_directory
    context[u'current_job'] = current_job
    context[u'script_name'] = script_name
    context[u'data_table'] = data_table
    context[u'stage'] = u'begin'
    yacc.parse(begin)
    context[u'stage'] = u'body'
    body = body.strip()
    if body:
        metadatas = metadata_table.scan()
        for metadata in metadatas:
            account_id = metadata[u'account_id']
            if account_id == table_lock_id:
                continue
            logging.debug(u'parsing account: %s' % account_id)
            items = data_table.query(account_id__eq=account_id, reverse=False)
            fetched_items = []
            context[u'metadata'] = metadata
            context[u'items'] = items
            context[u'fetched_items'] = []
            yacc.parse(body)
    context[u'stage'] = u'end'
    yacc.parse(end)
    logging.info('stop job: %s' % current_job)

if __name__ == '__main__':
    current_job = sys.argv[1]
    try:
        do_job(current_job)
    except Exception, e:
        msg = 'run job failed, %s' % unicode(e)
        logging.error(msg)
