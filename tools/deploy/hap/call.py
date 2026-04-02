import copy
import re
import inspect


def prepare_func_kwargs(func, args, kw):
    new_kw = copy.copy(kw)
    args_names = inspect.getargspec(func)[0]
    # if len(args_names) < len(args):
    #     raise Exception("too many args %s"%(args_names))
    for arg in args_names[:len(args)]:
        if type(arg) != str:
            raise Exception("not support nest args: %s"%(args_names))
        if arg in new_kw:
            del new_kw[arg]
    return new_kw

def dict_updated(d, kw, **extra):
    new_dict = copy.copy(d)
    new_dict.update(kw, **extra)
    return new_dict

def parse_cmd_args(args):
    return [i for i in args if not re.match('^\w+=', i)], dict(i.split('=', 1) for i in args if re.match('^\w+=', i))

class BaseCall:
    def __init__(self):
        self.handlers = {}

    def __call__(self, handler):
        self.handlers[handler.func_name]=handler
        return handler

    def magic_handler(self, d, str, args, kw):
        m = re.match('!(\w+)(?:\[(.*)\])?:(.*)', str)
        if not m: return str
        handler_name, handler_args, str_content = m.groups()
        handler = self.handlers.get('magic_%s'%(handler_name), None)
        if not callable(handler): return str
        _args, _kw = parse_cmd_args(filter(None, (handler_args or '').split(',')))
        return handler(d, str_content.strip(), (_args + list(args)), dict_updated(_kw, kw))

    def handle_callable(self, d, func, args, kw):
        if type(func) == str:
            func = self.sub(func, d)
        if callable(func):
            result = func(*args, **prepare_func_kwargs(func, args, d))
        elif type(func) == str and re.match('!\w+(\[.*\])?:', func):
            result = self.magic_handler(d, func, args, kw)
        else:
            return func
        if type(result) == str:
            result = self.sub(result, d)
        return result

    def call(self, d, path, args, kw):
        def split_path(path):
            p = path.rsplit('.', 1)
            return len(p) > 1 and p or [''] + p
        if type(d) != dict or type(path) != str or not path:
            raise Exception('call(type(d)=%s, type(path)=%s): invalid argument'% (type(d), type(path)))
        base_path, func_name = split_path(path)
        base = self.find(d, base_path)
        func = self.find(base, func_name)
        if base == None or func == None:
            return None
        call_env = dict_updated(base, kw)
        return self.handle_callable(call_env, func, args, kw)
