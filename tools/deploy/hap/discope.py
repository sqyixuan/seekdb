import itertools
import parser
import re
import traceback
import time
import copy

def load_file(__path__, __globals__):
    try:
        execfile(__path__, __globals__, __globals__)
    except Exception, __e:
        print __e, traceback.format_exc()
        print 'load file failed!', __path__

id_gen = itertools.count(1)
def add_parent_link(d, parent=None, k='top'):
    '''each dict has only a single __parent__ link, so cyclic link can not exists'''
    if type(d) != dict: return d
    if d.has_key('__parent__'): return d
    d['__parent__'] = parent
    d['__hap_id__'] = id_gen.next()
    d['__name__'] = k
    for (k, v) in d.items():
        if not k.startswith('_'):
            add_parent_link(v, d, k)
    return d

def after_load_hook(d):
    if type(d) != dict: return d
    if d.has_key('__load_done__'): return d
    d['__load_done__'] = time.time()
    hook = d.get('after_load', None)
    if callable(hook):
        hook(d)
    for (k, v) in d.items():
        if not k.startswith('_'):
            after_load_hook(v)
    return d

def load_global_dict(file_list, globals):
    for path in file_list:
        load_file(path, globals)
    return after_load_hook(add_parent_link(globals))

def hap_id(d):
    return type(d) == dict and '%s:%s'%(d.get('__name__', 'hap'), d.get('__hap_id__', -1)) or "hap_id"

def dict_path_signature(d):
    path, node = [], d
    while type(node) == dict:
        path.append(node.get('__name__', '_'))
        node = node.get('__parent__', None)
    return '.'.join(reversed(path))
        
def dict_overview(d, view_map):
    if type(d) != dict: return None
    if id(d) in view_map:
        return view_map[id(d)]
    parent_id = hap_id(d.get('__parent__', None))
    view_map[id(d)] = dict(hap_id=hap_id(d))
    view_map[id(d)].update(**dict((k, dict_overview(v, view_map)) for k, v in d.items() if type(v) == dict))
    #view_map[id(d)].update(__parent__=parent_id)
    return view_map[id(d)]

def dict_layout(*args, **kw):
    return dict_overview(kw, {})

def search_key(d, k):
    '''__parent__ link has no cycle, search will stop eventually'''
    while True:
        if type(d) != dict: return None
        if d.has_key(k): return d
        d = d.get('__parent__', None)

def find(d, path):
    '''find will finish eventually as each search_key() func-call will finish.
    find will return a value or None, no exception should happend.'''
    for key in filter(None, path.split('.')):
        d = search_key(d, key)
        if type(d) != dict:
            return None
        d = d[key]
    return d

def template_substitute(template, env, substitute_handler):
    '''magic marker example: $word ${abc.def} ${{1+2}}'''
    return re.sub('(?s)(?<![$])(?:\${{(.+?)}}|\${(.+?)}|\$(\w+))', lambda m: substitute_handler(env, m.group(1) or m.group(2) or m.group(3), m.group(0)), template)

def handle_callable(func, *args, **env):
    return func
    
def do_sub(template, env):
    def preprocess(expr):
        if re.match('([\w.])+$', expr):
            expr = '$%s'%(expr)
        return re.sub('\$([\w.]+)', r'F("\1")', expr)
    def is_valid_expr(expr):
        try:
            parser.expr(expr)
        except Exception as e:
            return False
        return True
    def substitute_handler(env, expr, orig_expr):
        expr = preprocess(expr)
        try:
            if is_valid_expr(expr):
                ret = eval(expr, globals(), env)
                ret = handle_callable(env, ret, [], {})
                if (type(ret) == list or type(ret) == tuple) and all(type(x) == str for x in ret):
                    return ' '.join(ret)
                if ret == None or type(ret) != str and type(ret) != int and type(ret) != float and type(ret) != long:
                    return orig_expr
                else:
                    return str(ret)
            else:
                exec expr in env
                return ''
        except Exception as e:
            # on_exception(env, '', e)
            return orig_expr
    return template_substitute(template, env, substitute_handler)

def sub(template, env, max_iter=50):
    '''no exception should happen'''
    env.update(F=lambda path: find(env, path))
    old, cur = "", template
    for i in range(max_iter):
        if cur == old: break
        old, cur = cur, do_sub(cur, env)
    return cur

def get_matched_path(prefix, env, globals):
    if not re.match('[.a-zA-Z0-9]+', prefix):
        return []
    def dict_updated(d, **kw):
        new_dict = copy.copy(d)
        new_dict.update(**kw)
        return new_dict
    def split_path(path):
        p = path.rsplit('.', 1)
        return len(p) > 1 and p or [''] + p
    dir_path, attr_name = split_path(prefix)
    if dir_path:
        d = find(env, dir_path)
    else:
        d = dict_updated(env, **globals)
    if type(d) != dict:
        d = {}
    return ['%s%s'%(dir_path and '%s.'%(dir_path) or '', type(v) == dict and '%s.'%(k) or k) for k,v in d.items() if not k.startswith('_')]

def build_dict(*args, **kw):
    def dict_filter(f, d):
        return dict(filter(f, d.items()))
    def dict_updated(*args, **kw):
        new_dict = dict()
        for d in args:
            new_dict.update(d)
        new_dict.update(kw)
        return new_dict
    return dict_filter(lambda (k,v): not k.startswith('__'), dict_updated(*args, **kw))

class ChainBuilder:
    def __init__(self):
        self.chain = []

    def __call__(self, node):
        self.chain.append(node)
        return node

    def build(self, **kw):
        for node in self.chain:
            kw = node(**kw)
        return kw

if __name__ == '__main__':
    a = dict(b=1, c=dict(b=1))
    a = add_parent_link(a)
    print find(a, 'b')
    print find(a, 'c.b')
    print sub('$b ${c.b}', a)
