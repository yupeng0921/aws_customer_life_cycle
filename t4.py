#! /usr/bin/env python

import types
tokens = (
    'FOR', 'IN', 'WHILE', 'IF', 'ELSE',
    'VARIABLE','NUMBER',
    'PLUS','MINUS','TIMES','DIVIDE','EQUAL',
    'LPAREN','RPAREN',
    'LBRACKET', 'RBRACKET',
    'LBRACE', 'RBRACE',
    'STRING',
    'BUILDIN',
    'COMMA',
    )

# Tokens

t_PLUS     = r'\+'
t_MINUS    = r'-'
t_TIMES    = r'\*'
t_DIVIDE   = r'/'
t_EQUAL    = r'='
t_LPAREN   = r'\('
t_RPAREN   = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'
t_LBRACE   = r'\{'
t_RBRACE   = r'\}'
t_BUILDIN  = r'\$[a-zA-Z0-9_\.]*'
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
    r'\"[a-zA-Z0-9_\/\.@]*\"'
    t.value = t.value[1:-1]
    return t

def t_FOR(t):
    r'for'
    return t

def t_IN(t):
    r'in'
    return t

def t_IF(t):
    r'if'
    return t

def t_ELSE(t):
    r'else'
    return t

def t_WHILE(t):
    r'while'
    return t

def t_VARIABLE(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    return t

# Ignored characters
t_ignore = " \t"

def t_newline(t):
# def t_NEWLINE(t):
    r'\n+'
    t.lexer.lineno += t.value.count("\n")

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
        v = node.value[1:]
        try:
            v = int(v)
        except Exception, e:
            pass
        itp.value = v
        return itp
    elif node.nodetype == 'empty_list':
        itp.value = []
        return itp
    elif node.nodetype == 'opr':
        if node.value == 'EQUAL':
            itp1 = interpret(node.subnodes[1])
            if type(itp1.value) is types.ListType:
                value = itp1.value[:]
            else:
                value = itp1.value
            variables[node.subnodes[0]] = value
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
            itp.value = itp1.value + itp2.value
            return itp
        elif node.value == 'MINUS':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            itp.value = itp1.value - itp2.value
            return itp
        elif node.value == 'TIMES':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            itp.value = itp1.value * itp2.value
            return itp
        elif node.value == 'DIVIDE':
            itp1 = interpret(node.subnodes[0])
            itp2 = interpret(node.subnodes[1])
            itp.value = itp1.value / itp2.value
            return itp
        elif node.value == 'fun2':
            func_name = node.subnodes[0][1:]
            func = func2_dict[func_name]
            if func_name != 'add' and func_name != 'del':
                itp1 = interpret(node.subnodes[1])
                param1 = itp1.value
            else:
                param1 = node.subnodes[1].value
            itp2 = interpret(node.subnodes[2])
            (itp.itptype, itp.value) = func(param1, itp2.value)
            return itp
            var = variables[node.subnodes[0].value]
            if type(var) is not types.ListType:
                var = []
            var.append(itp1.value)
            variables[node.subnodes[0]] = var
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

def p_stmt_assign(t):
    'stmt : VARIABLE EQUAL expr'
    t[0] = opr_node('EQUAL', [t[1], t[3]])

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

def p_error(t):
    print("Syntax error at '%s'" % t.value)

import ply.yacc as yacc
yacc.yacc()

s='''
a="strtest"
b=4-3
hello=(b)
world= -hello
c=a
test1 = $4.gmail
array1=[]
$add(array1, 3)
$add(array1, "test5")
$add(array1, $4.gmail)
$add(array1, 2+3*4)
array2=array1
$del(array1, $4.gmail)
'''

s='''
array1=[]
$add(array1,1)
$add(array1,2)
$add(array1,3)
array2=[]
for i in array1 {
$add(array2, i)
}
k=0
l = 0
for i in array1
for j in array2{
k = k+1
l = k + 1
}
'''

s='''
i = 1
a=2
if (i)
a=3
'''
a=yacc.parse(s)
variables = {}
a=yacc.parse(s)
print(variables)
