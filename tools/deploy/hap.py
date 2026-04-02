#!/bin/env python2
"""
hap.py means 'happy', it aims easy config management and task dispatch. see `readme.org'

* Basic
./hap.py a.b.c ... # run hap command
./hap.py config2.py:a.b.c ... # load config2.py and run hap command
./hap.py a[0,1,2].b   # execute a0.b a1.b a2.b sequentially
./hap.py ... _dryrun_ # print shell comand instead execute it
./hap.py ... _loop_=4 # execute hap cmd 4 times
./hap.py ... _log_=3  # set log level=3, print more debug log
./hap.py ... _log_=0  # set log level=0, print less log

* ish
  ish can interp  python code, hap command, shell command, sql command etc.
./hap.py ish # launch interactive ish console, support readline history, tab complete, multiline editing
./hap.py a.b.ish '...' # execute one ish cmd then exit
./hap.py rf script.py  # run ish script
"""
import sys, os, os.path
_hap_path_ = os.path.dirname(os.path.abspath(__file__))
def hap_file_path(name=''):
    return os.path.join(_hap_path_, name)
for path in '. pylib hap'.split():
    sys.path.insert(0, hap_file_path(path))
import re
import atexit
import itertools
import copy
import traceback
import time
import pprint

name = 'top'
_hap_prefix_ = ''
_error_ = False
_mode_ = 'batch'
_log_ = 1
_dryrun_ = False
_cmd_, _args_, _kw_ = '', [], dict()
_debug_ = False
_use_match_eval_interp_ = True

class Fail(Exception):
    def __init__(self, msg, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return 'Fail: %s %s'%(self.msg, self.obj != None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)

from logger import logger

from call import BaseCall
import discope
from discope import find, sub, build_dict, ChainBuilder
from interp import InterpException, ChainInterp, wrap_pysrc, get_ctx
import repl
import caller_info

Interp = ChainInterp()
class ChainInterpWrapper:
    def __init__(self):
        pass
    def __lshift__(self, cmd):
        ci = caller_info.get_caller_info()
        cmd = sub(cmd.strip(), ci['locals'])
        return Interp.interp(cmd, ci['locals'], ci['globals'], ci['lineno'], ci['filename'], ci['code'], func=ci['function'])

from meval import *
class MatchEvalInterpWrapper:
    def __init__(self):
        pass
    def __lshift__(self, cmd):
        ci = caller_info.get_caller_info()
        if '__parent__' not in ci['locals']:
            ci['locals'].update(__parent__=ci['globals'])
        cmd = sub(cmd.strip(), ci['locals'])
        return E(cmd=cmd, **ci)()

if _use_match_eval_interp_:
    I = MatchEvalInterpWrapper()
else:
    I = ChainInterpWrapper()

class ConsoleInterp(repl.BaseInterp):
    def __init__(self, env):
        repl.BaseInterp.__init__(self)
        self.env = env

    def hap_prefix(self, d):
        path = discope.dict_path_signature(d)
        m = re.match('top(?:[.]([.a-zA-Z0-9_]+))?', path)
        return m and m.group(1) or ''

    def get_prompt(self):
        def get_cwd():
            home = os.path.realpath(os.path.expanduser('~'))
            cwd = os.getcwd()
            if cwd == home:
                ret = '~'
            elif cwd.startswith(home):
                ret = os.path.join('~', cwd[len(home) + 1:])
            else:
                ret = cwd
            return ret
        return '<%s %s>$ '%(discope.dict_path_signature(self.env), get_cwd())

    def preprocess(self, line):
        cmd = wrap_pysrc(line)
        caller_info.cur_file = cmd
        logger.debug('INPUT: %s -> %s'%(line, cmd), opt=self.env)
        return cmd

    def get_env(self):
        if type(self.env) == dict:
            locals = self.env
        else:
            locals = globals()
        return locals, globals()

    def prepare_env(self):
        locals, globals = self.get_env()
        # logger.set_log_level(locals.get('_log_', _log_))
        globals.update(_hap_prefix_=self.hap_prefix(locals))
        prepare_for_ish = locals.get('prepare_for_ish', None)
        if callable(prepare_for_ish):
            prepare_for_ish(locals)
        return locals, globals

    def get_matched(self, prefix, prefix2):
        locals, globals = self.get_env()
        completer = repl.Completer(hap_file_path('hap/sh.rc'), 'disable-reboot _log_ _mode_'.split(), locals, globals)
        try:
            return completer.get_matched(prefix, prefix2) + discope.get_matched_path(prefix, locals, globals)
        except Exception as e:
            # print e
            # print traceback.format_exc()
            pass

def read_src(fd):
    if type(fd) == str:
        return open(fd).read()
    else:
        return fd.read()

def wrap_src(fd, *arg, **kw):
    print wrap_pysrc(read_src(fd))

def run_file(fname, locals_env=None, globals_env=None):
    if globals_env == None:
        globals_env = globals()
    if locals_env == None:
        locals_env = globals_env
    logger.info('run_file: %s'%(fname), opt=locals_env)
    if _dryrun_:
        return 'OK'
    _src = wrap_pysrc(read_src(fname))
    caller_info.cur_file = _src
    try:
        _code = compile(_src, str(fname), 'exec')
        exec _code in globals_env, locals_env
    except Exception as e:
        if isinstance(e, InterpException):
            e.filename = str(fname)
        tracepoint.on_exception(locals_env, fname, locals_env, e)
        logger.info('run script fail: %s %s'%(fname, e), opt=locals_env)
        logger.debug('detail: %s'%(traceback.format_exc()), opt=locals_env)
        raise Fail('run_file fail: %s'%(e))
        return e
    logger.info('run script succ: %s'%(fname), opt=locals_env)
    return 'OK'

def rf(path_list, *args, **kw):
    prepare_for_ish = kw.get('prepare_for_ish', None)
    if callable(prepare_for_ish):
        prepare_for_ish(kw)
    if type(path_list) == str:
        return [(path, run_file(path, globals(), globals())) for path in path_list.split()]
    else:
        return run_file(path_list, globals(), globals())

def ish(*args, **kw):
    cmd = ' '.join(i for i in sys.argv[2:] if not i.startswith('_'))
    def do_repl(**kw):
        interp = ConsoleInterp(kw)
        globals().update(CI = interp)
        return repl.Console(hap_file_path('hap/.hap_history')).repl(interp)
    def handle_one_cmd(cmd, **kw):
        interp = ConsoleInterp(kw)
        return interp.interp(cmd)
    globals().update(_mode_='interactive')
    init_file = hap_file_path('hap/ish.py')
    if os.path.isfile(init_file):
        rf(init_file)
    if cmd:
        return handle_one_cmd(cmd, **kw)
    elif sys.stdin.isatty():
        do_repl(**kw)
    else:
        return rf(sys.stdin)

def load_file(path_list):
    for path in path_list.split(','):
        discope.load_file(hap_file_path(path), globals())

def load_file_vars(__path__, __globals__=globals()):
    try:
        execfile(__path__, __globals__, locals())
    except Exception as __e:
        raise Fail('load file %s failed!'%(__path__), __e)
    return dict((k, v) for k,v in locals().items() if not k.startswith('__'))

class TracePoint:
    def __init__(self):
        pass

    def check(self):
        if _error_:
            raise Fail('request stop')
        return True

    def dpath(self, d):
        return discope.dict_path_signature(d)

    def on_exception(self, d, path, kw, e, fatal=False):
        if d == None:
            d = globals()
        if isinstance(e, KeyboardInterrupt) or fatal:
            if _mode_ == 'batch':
                globals().update(_error_='keybord_interp')
        logger.error('    Exception: %s %s'%(self.dpath(d), path), False, opt=kw)
        logger.debug(traceback.format_exc(), False, opt=kw)
        if _debug_:
            print 'Exception: %s %s'%(self.dpath(d), path)
            print traceback.format_exc()

    def on_call_begin(self, d, path, kw):
        self.check()
        if '_call_depth_' in kw:
            kw.update(_call_depth_=int(kw['_call_depth_']) + 1)
        logger.info('call: %s %s'%(self.dpath(d), path), opt=kw)

    def on_call_end(self, d, path, kw, result):
        def summary(value):
            if value == None:
                return None
            elif type(value) in [int, str, Exception]:
                return str(value)
            elif type(value) == dict:
                return discope.hap_id(value)
            else:
                return type(value)
        logger.debug('call: %s %s => %s'%(self.dpath(d), path, summary(result)), opt=kw)

tracepoint = TracePoint()
class HapCall(BaseCall):
    def __init__(self, find, sub):
        BaseCall.__init__(self)
        self.find, self.sub = find, sub

Call = HapCall(find, sub)
def call(d, path, *args, **kw):
    return CALL(d, path, args, kw)

def CALL(d, path, args, kw):
    tracepoint.on_call_begin(d, path, kw)
    result = None
    try:
        result = Call.call(d, path, args, kw)
    except (Exception, KeyboardInterrupt) as e:
        if os.getenv("DEPLOY_DEBUG"):
            msg = traceback.format_exc()
            sys.stderr.write("invoke failed with backtrace:\n%s" % msg)
        tracepoint.on_exception(d, path, kw, e)
        result = e
        raise e
    finally:
        tracepoint.on_call_end(d, path, kw, result)
    return result

def list_attr(**kw):
    for key in kw.keys():
        if not key.startswith('_'):
            print key
discope.handle_callable = Call.handle_callable

def reload_console(**kw):
    os.execl(sys.argv[0], *sys.argv)

def cd(dir='', **kw):
    if not dir:
        dir = '~'
    d = find(kw, dir)
    if type(d) == dict:
        globals().get('I').env = d
    else:
        os.chdir(os.path.expanduser(dir))

def parse_cmd_args(args):
    return [i for i in args if not re.match('^\w+=', i)], dict(i.split('=', 1) for i in args if re.match('^\w+=', i))

def expand(spec):
    def multiple_expand(str):
        return [''.join(parts) for parts in itertools.product(*[re.split('[ ,]+', i) for i in re.split('\[(.*?)\]', str)])]
    def list_merge(ls):
        return reduce(lambda a,b: list(a)+list(b), ls, [])
    return list_merge(map(multiple_expand, spec.split()))

def __help(**kw):
    print __doc__

def happy_run(argv):
    def ls_by_rexp(dir, pat):
        return sorted(os.path.join(dir, path) for path in os.listdir(dir) if re.match('^%s$'%(pat), path))
    def get_load_file_list(cfg):
        return [hap_file_path('hap/init.py')] + (cfg and [cfg] or ls_by_rexp(hap_file_path('.'), 'config[0-9]*.py')[-1:])

    global _dryrun_, _log_,  _cmd_, _args_, _kw_, _cfg_

    list_args, kw_args = parse_cmd_args(argv)
    list_args or sys.exit(1)
    cmd = list_args[0].split(':', 1)
    if len(cmd) == 2:
        cfg, cmd = cmd
    else:
        cfg, cmd = os.getenv('HAP_CONFIG'), cmd[0]
    _dryrun_ = ('_dryrun_' in list_args)
    _log_ = int(kw_args.get('_log_', '1'))
    _cmd_, _args_, _kw_ = cmd, list_args, kw_args # for compitability

    logger.set_log_level(_log_)
    logger.debug('happy_run: args: %s, kw: %s'%(list_args, kw_args))
    d = discope.load_global_dict(get_load_file_list(cfg), globals())

    loop = int(kw_args.get('_loop_', '0')) or 1
    for i in range(loop):
        for real_cmd in expand(cmd):
            logger.info(pprint.pformat([cmd, call(d, real_cmd, *list_args[1:], **kw_args)]), header=False)

if __name__ == '__main__':
    try:
        happy_run(sys.argv[1:] or ['__help'])
    except Fail as e:
        print e
