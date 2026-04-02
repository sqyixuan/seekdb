from threading import Thread
import subprocess
import shlex
import random
import parser

def load_ext(ext_list, **kw):
    for ext in ext_list.split(','):
        load_file('h/%s.py'%(ext))

def dict_updated(*args, **kw):
    new_dict = dict()
    for d in args:
        new_dict.update(d)
    new_dict.update(kw)
    return new_dict

def dict_slice(d, *args):
    return dict((k, d[k]) for k in args if k in d)

def dict_merge(d1, d2):
    if type(d1) != dict: return d2
    if type(d2) != dict: return d1
    for k,v in d2.items():
        d1[k] = dict_merge(d1.get(k), v)
    return d1

def read(path):
    try:
        with open(path) as f:
            return f.read()
    except IOError as e:
        return ''

def write(path, content):
    with open(path, 'w') as f:
        f.write(content)

def parse_expr(expr):
    try:
        return parser.expr(expr)
    except Exception as e:
        return None

def get_ts_str(ts=None):
    if ts == None: ts = time.time()
    return time.strftime('%m%d-%X', time.localtime(ts))

def echo_args(*args, **kw):
    return kw

def sleep(sleep_time=1, **kw):
    print 'sleep %s'%(str(sleep_time))
    for i in range(int(sleep_time)):
        tracepoint.check()
        time.sleep(1)

def safe_int(x, default_value):
    try:
        return int(x)
    except TypeError as e:
        return default_value

def retry_loop(call, timeout=60, interval=1, msg='', **kw):
    logger.debug('retryloop: timeout=%s interval = %s'%(str(timeout), str(interval)), opt=kw)
    end_time = time.time() + safe_int(timeout, 60)
    while tracepoint.check() and time.time() < end_time:
        logger.info('#### tryloop %s ####'%(msg), opt=kw)
        try:
            result = call()
            logger.info('#### tryloop success ####', opt=kw)
            return 'succ'
        except Exception as e:
            pass
        sys.stdout.flush()
        time.sleep(float(interval))
    raise Fail('retryloop %s timeout'%(msg))

def on_shell_begin(cmd, input, opt):
    logger.debug('shell: %s input: %s'%(cmd, input), opt=opt)

def on_shell_end(cmd, input, output, ret, opt):
    no_exception = ('NoException' in cmd) or (input != None and 'NoException' in input)
    if ret != 0 and not no_exception:
        raise Fail('shell fail: %s input: %s'%(cmd, input))

def on_shell_timeout(cmd, input, timeout, opt):
    logger.info('shell timeout: timeout=%d %s input: %s'%(timeout, cmd, input), opt=opt)

def popen_communicate(p, input, timeout):
    '''return errcode, stdout'''
    end_time = time.time() + ((timeout == None) and 1000 or timeout)
    while p.poll() is None:
        time.sleep(0.1)
        if time.time() > end_time:
            try:
                p.terminate()
            except OSError, e:
                pass
            return -1, ''
    return p.returncode, p.stdout and p.stdout.read()

def direct_popen(cmd, input=None, output=None, timeout=None, **opt):
    on_shell_begin(cmd, input, opt)
    stdin = input and subprocess.PIPE or None
    stdout = output and subprocess.PIPE or None
    timeout = timeout is not None and float(timeout) or None
    if _mode_ == 'interactive' and stdout == None:
        env = dict_updated(os.environ, HAP_PATH=_hap_path_, HAP_PREFIX=_hap_prefix_, BASH_ENV=hap_file_path('hap/sh.rc'), TERM='xterm-256color')
    else:
        env = None
    p = subprocess.Popen(cmd, shell=True, stdin=stdin, stdout=stdout, stderr=subprocess.STDOUT, env=env, executable='/bin/bash')
    try:
        poutput = p.communicate(input=input, timeout=timeout)[0]
        if output:
            output = poutput
        ret = p.returncode
    except subprocess.TimeoutExpired as e:
        p.terminate()
        output = None
        ret = -1
        on_shell_timeout(cmd, input, timeout, opt)
    on_shell_end(cmd, input, output, ret, opt)
    if output == None:
        output = ret
    else:
        output = output.strip()
    return output

def sh(cmd, **kw):
    return direct_popen(cmd, **kw)

def popen(cmd, **kw):
    return direct_popen(cmd, output=True, **kw)

def popen_wrapper(env, cmd, input=None, output=None, **kw):
    if type(cmd) == str:
        cmd = sub(cmd, env)
    if type(input) == str:
        input = sub(input, env)
    if input != None and 'DiscardOutput' in input and logger.get_log_level() < 2:
        cmd += ' >/dev/null 2>/dev/null'
    return _dryrun_ and '%s input:%s'%(cmd, input) or direct_popen(sub(cmd, env), input=input, output=output, **kw)

@Call
def magic_print(d, cmd, args, kw):
    print cmd

@Call
def magic_sh(d, cmd, args, kw):
    return popen_wrapper(dict_updated(d, _rest_=args), cmd, **kw)

@Call
def magic_popen(d, cmd, args, kw):
    return popen_wrapper(dict_updated(d, _rest_=args), cmd, output=True, **kw)

@Call
def magic_tryloop(d, cmd, args, kw):
    return retry_loop(lambda : magic_call(d, cmd, args, kw), msg=cmd, *args, **kw)

@Call
def magic_ssh(d, cmd, args, kw):
    if find(d, 'is_local'):     # use sh instead is local set, by fufeng
        return magic_sh(d, cmd, args, kw)
    else:
        return popen_wrapper(dict_updated(d, _rest_=args), 'ssh -T $usr@$ip', input=cmd)

@Call
def magic_obstart_sshbg(d, cmd, args, kw):
    final_cmd = 'ssh -Tfn $usr@$ip ' + '\"' + cmd + '\"'
    return popen_wrapper(dict_updated(d, _rest_=args), final_cmd)

@Call
def magic_obstart_ssh(d, cmd, args, kw):
    if "gdb --batch" in cmd:
        return magic_obstart_sshbg(d, cmd, args, kw)
    else:
        return magic_ssh(d, cmd, args, kw)

def magic_ssht(d, cmd, args, kw):
    return popen_wrapper(dict_updated(d, _rest_=args), 'ssh -t $usr@$ip %s'%(repr(cmd)))

@Call
def magic_sql(d, cmd, args, kw):
    return popen_wrapper(dict_updated(d, _rest_=args), 'mysql -h $ip -P $mysql_port -uroot', input=cmd)

@Call
def magic_check(d, cmd, args, kw):
    m = re.split(' *# *', cmd)
    if len(m) != 2:
        raise Fail('check cmd should be split by #')
    cmd, msg = m
    try:
        magic_call(d, cmd, args, kw)
        return 'OK'
    except Fail as e:
        raise Fail('check fail: %s'%(msg))

@Call
def magic_call(d, cmd, args, kw):
    cmd_list = shlex.split(cmd)
    _args, _kw = parse_cmd_args(cmd_list[1:])
    return CALL(d, cmd_list[0], (_args + list(args)), dict_updated(_kw, kw))

@Call
def magic_seq(d, cmd_list, args, kw):
    cmd_seq = [(cmd, arg and parse_cmd_args(arg.split(',')) or ([],{})) for cmd, arg in re.findall('([0-9a-zA-Z._]+)(?:\[(.*?)\])?', cmd_list)]
    return [(cmd, CALL(d, cmd, (_args + list(args)), dict_updated(_kw, kw))) for cmd, (_args,_kw) in cmd_seq]

@Call
def magic_filter(d, cond, args, kw):
    return [(k, v) for k,v in sorted(d.items(), key=lambda x:x[0]) if type(v) == dict and not k.startswith('_') and magic_call(v, cond, args, dict_updated(kw, _log_=0))]

@Call
def magic_all(d, cmd, args, kw):
    members, cmd = cmd.split(' ', 1)
    members = magic_call(d, members, args, kw)
    return [('%s.%s'%(k, cmd), magic_call(v, cmd, args, kw)) for k,v in members if not k.startswith('_')]

@Call
def magic_par(d, cmd, args, kw):
    members, cmd = cmd.split(' ', 1)
    members = magic_call(d, members, args, kw)
    result = [('%s.%s'%(k, cmd), Async(magic_call, v, cmd, args, kw)) for k,v in members if not k.startswith('_')]
    timeout = int(kw.get('par_timeout', '3'))
    return [(key, ret.get(timeout)) for key, ret in result]

@Call
def magic_rand(d, cmd, args, kw):
    members, cmd = cmd.split(' ', 1)
    members = magic_call(d, members, args, kw)
    members = [(k, v) for k,v in members if not k.startswith('_')]
    if not members: return 'empty set'
    k, v = random.choice(members)
    return '%s.%s'%(k, cmd), magic_call(v, cmd, args, kw)

@Call
def magic_second(d, cmd, args, kw):
    return magic_call(d, cmd, args, kw)[1]

@Call
def magic_checkall(d, cmd, args, kw):
    fail_list = [(key, ret) for (key, ret) in magic_par(d, cmd, args, kw) if not ret]
    if fail_list:
        raise Fail('check: %s'%(cmd), fail_list)
    return 'OK'

@Call
def magic_uniq(d, cmd, args, kw):
    return list(set(ret for (key, ret) in magic_all(d, cmd, args, dict_updated(kw, _log_=0))))

def is_match(key, pat, *args, **kw):
    attr = kw.get(key, None)
    return type(attr) == str and re.match(pat, attr)

class Async(Thread):
    def __init__(self, func, *args, **kw):
        self.result = None
        def call_and_set():
            self.result = func(*args, **kw)
        Thread.__init__(self, target=call_and_set)
        self.setDaemon(True)
        self.start()

    def get(self, timeout=None):
        self.join(timeout)
        #return self.isAlive() and self.result
        return self.result

def happy_run_cmd(cmd, env, **opt):
    list_args, kw_args = parse_cmd_args(shlex.split(cmd))
    if not list_args: return
    return CALL(env, list_args[0], list_args[1:], dict_updated(kw_args, opt));

@Interp
def interp_magic_str(cmd, locals, globals, head='', **opt):
    if re.match('!(\w+)(?:\[(.*)\])?:(.*)', cmd):
        result = Call.magic_handler(locals, cmd, (), {})
        return dict(result=result)

@Interp
def interp_simple_hap(cmd, locals, globals, head='', **opt): # ob1.reboot etc
    if head == cmd:
        return interp_hap(cmd, locals, globals, head, **opt)

@Interp
def interp_eval(cmd, locals, globals, head='', **opt):
    if parse_expr(cmd):
        result = eval(cmd, globals, locals)
        if callable(result):
            result = result()
        return dict(result = result)

@Interp
def interp_suite(cmd, locals, globals, head='', **opt):
    if head in 'print raise return import'.split():
        try:
            exec cmd in globals, locals
            return dict(result=None)
        except Exception as e:
            pass

@Interp
def interp_hap(cmd, locals, globals, head='', **opt): # ob1.reboot etc
    if '__parent__' not in locals:
        locals.update(__parent__=globals)
    if type(locals) == dict and re.match('^[a-zA-Z][_.a-zA-Z0-9]*$', head) and find(locals, head) != None:
        if '|' in cmd:
            return interp_popen('hap ' + cmd, locals, globals, head='hap', **opt)
        else:
            ret = happy_run_cmd(cmd, locals, **opt)
            return dict(result=ret)

def is_executable_file(path):
    env = dict_updated(os.environ, BASH_ENV=hap_file_path('hap/sh.rc'))
    return subprocess.call('is_valid_shell_cmd %(path)s'%dict(path=path), env=env, shell=True, executable='/bin/bash') == 0

def remove_head(cmd):
    return (cmd + ' ').split(' ', 1)[1]
@Interp
def interp_popen(cmd, locals, globals, head='', ctx='void', **opt): # mkdir -p xxx
    out_list = ctx.startswith('for')
    popen_opt = dict_slice(locals, 'timeout', '_log_')
    if re.match('^[~_./a-zA-Z0-9]+$', head):
        if head in 'alias ulimit export set LD_LIBRARY_PATH LD_PRELOAD CC CXX'.split() or is_executable_file(head):
            if _dryrun_:
                ret = cmd
            elif ctx == 'void':
                try:
                    direct_popen(cmd, output=None, **popen_opt)
                except Exception as e:
                    if _mode_ != 'interactive':
                        raise e
                ret = None
            else:
                ret = direct_popen(cmd, output=True, **popen_opt)
                if out_list:
                    ret = ret.split('\n')
            return dict(result=ret)

def check_ssh_connection(ip, timeout):
    try:
        popen('ssh -T %s -o ConnectTimeout=%d true'%(ip, timeout), timeout=timeout)
        return 0
    except Exception as e:
        return -1

class Reporter:
    def __init__(self, id, interval=1):
        self.id, self.interval = id, interval
        self.count, self.report_count, self.report_time = 0, 0, 0
    def inc(self):
        self.count += 1
        cur_us = time.time()
        if cur_us > self.report_time + self.interval:
            print '%s %f'%(self.id, 1.0 * (self.count - self.report_count)/(cur_us - self.report_time))
            self.report_time, self.report_count = cur_us, self.count

@match_regist
def me_add_desc(cmd='', head=None, code='', locals={}, globals={}, **kw):
    if head != None: raise NotMatch()
    m = re.match('.*\#\((.*)\)$', code)
    attr = {}
    if m:
        try:
            attr = eval('dict(%s)'%(m.group(1)), globals, locals)
        except Exception:
            pass
    return E(kw, head=cmd.split(' ')[0], ctx=get_ctx(code), code=code, cmd=cmd, locals=locals, globals=globals, **attr)

@match_regist
def me_magic_str(cmd='', locals=None, **kw):
    match_by_rexp(cmd, '!(\w+)(?:\[(.*)\])?:(.*)')
    return Call.magic_handler(locals, cmd, (), {})

@match_regist
def me_hap_target(head='', type=None, locals={}, globals={}, **kw):
    if type != None: raise NotMatch()
    if '__parent__' not in locals:
        locals.update(__parent__=globals)
    if re.match('^[a-zA-Z][_.a-zA-Z0-9]*$', head) and find(locals, head) != None:
        return E(kw, type='hap', head=head, locals=locals, globals=globals)
    else:
        raise NotMatch()

@match_regist
def me_exp_target(cmd='', type=None, locals={}, globals={}, **kw):
    if type != None: raise NotMatch()
    if not parse_expr(cmd): raise NotMatch()
    try:
        eval(cmd, globals, locals)
    except Exception:
        raise NotMatch()
    return E(kw, type='exp', cmd=cmd, locals=locals, globals=globals)

@match_regist
def me_suite_target(head='', **kw):
    if type != None: raise NotMatch()
    if head in 'print raise return import'.split():
        return E(kw, type='suite', head=head)
    else:
        raise NotMatch()

@match_regist
def me_popen_target(head='', type=None, **kw):
    if type != None: raise NotMatch()
    match_by_rexp(head, '^[~_./a-zA-Z0-9-]+$')
    if head in 'alias ulimit export set LD_LIBRARY_PATH LD_PRELOAD CC CXX'.split() or is_executable_file(head):
        return E(kw, head=head, type='popen')
    else:
        raise NotMatch('popen: %s'%(head))

@match_regist
def me_exp_execute(cmd='', type='', locals={}, globals={}, **kw):
    match_by_rexp(type, 'exp')
    result = eval(cmd, globals, locals)
    if callable(result):
        result = result()
    return result

@match_regist
def me_suite_execute(cmd='', type='', locals={}, globals={}, **kw):
    match_by_rexp(type, 'suite')
    exec cmd in globals, locals

@match_regist
def me_hap_execute(cmd='', type='', locals={}, globals={}, **kw): # ob1.reboot etc
    match_by_rexp(type, 'hap')
    if '|' in cmd:
        return interp_popen('hap ' + cmd, locals, globals, head='hap', **kw)
    else:
        return happy_run_cmd(cmd, locals, **kw)

@match_regist
def me_popen_execute(cmd='', ctx='void', type='', locals={}, **kw): # mkdir -p xxx
    match_by_rexp(type, 'popen')
    need_output_list = ctx.startswith('for')
    popen_opt = dict_slice(locals, 'timeout', '_log_')
    if _dryrun_:
        ret = cmd
    elif ctx == 'void':
        try:
            direct_popen(cmd, output=None, **popen_opt)
        except Exception as e:
            if _mode_ != 'interactive':
                raise e
        ret = None
    else:
        ret = direct_popen(cmd, output=True, **popen_opt)
        if need_output_list:
            ret = ret.split('\n')
    return ret

# @match_regist
# def me_default(**kw):
#     logger.error(sub('unknown cmd from $filename:$lineno: $cmd', kw))
#     return None
