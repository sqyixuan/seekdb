#!/bin/env python2
import re

import traceback

class NotMatch(Exception):
    def __init__(self, msg=''):
        self.msg = msg
    def __str__(self):
        return self.msg

match_func_list = []
def match_regist(func):
    match_func_list.append(func)
    return func

def match_call(kw):
    for f in match_func_list:
        try:
            res = f(**kw)
            return res
        except NotMatch:
            #print traceback.format_exc()
            pass

def match_eval(expr):
    while isinstance(expr, EvalObj):
        expr = match_call(dict((k, match_eval(v)) for k, v in expr.items()))
    return expr

class EvalObj(dict):
    def __init__(self, __d={}, **kw):
        new_dict = {}
        new_dict.update(__d)
        new_dict.update(kw)
        dict.__init__(self, new_dict)
    def __call__(self):
        return match_eval(self)

E = EvalObj

def match_by_rexp(x, pat):
    if not re.match(pat, x):
        raise NotMatch('x=%s pat=%s'%(x, pat))

if __name__ == '__main__':
    @match_regist
    def prod(op=None, x=0, y=0, **kw):
        match_by_rexp(op, 'prod')
        return x * y

    @match_regist
    def add(op=None, x=0, y=0, **kw):
        match_by_rexp(op, 'add')
        return x + y

    @match_regist
    def sqrt(op=None, x=0, y=0, **kw):
        match_by_rexp(op, 'sqrt')
        return E(op='add', x=E(op='prod', x=x, y=x), y=E(op='prod', x=y,y=y))

    print E(op='sqrt', x=3, y=3)()
