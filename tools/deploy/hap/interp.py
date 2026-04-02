import sys
import re
import string
import parser
import caller_info

def parse_expr(expr):
    try:
        return parser.expr(expr)
    except Exception as e:
        return None

def wrap_by_parse(text):
    def try_parse(lines):
        try:
            parser.suite('\n'.join(lines))
        except SyntaxError as e:
            #print traceback.format_exc()
            return e.lineno - 1
        return -1
    def mark_line(line):
        return re.sub('^( *)(.+?)((?: +#.*)?)$', r"\1I<<''' \2 '''\3", line)
    lines = text.split('\n')
    last_lineno = -1
    while True:
        lineno = try_parse(lines)
        if lineno < 0:
            break
        elif lineno == last_lineno:
            print '\n'.join(lines)
            raise Exception('parse fail: lineno=%s'%(lineno))
        else:
            lines[lineno] = mark_line(lines[lineno])
            last_lineno = lineno
    return '\n'.join(lines)

wrap_pattern = '''
    print $expr # print
    return $expr # return
    if $expr: # if
    elif $expr: # if
    while $expr: # while
    for $list in $expr: # for:\g<list>
    $list=$expr # assign:\g<list>
    $cmd # void
'''
def pattern2rexp(pat):
    def sub(template, dict):
        return string.Template(template).safe_substitute(dict)
    rexp_def_map = dict(expr='.+', cmd='''[ ./_a-zA-Z0-9|'"-]+''', list='(?P<list>[_a-zA-Z(][_.a-zA-Z0-9,() ]*)')
    return sub(re.sub(r'^(.*)(\$expr|\$cmd)(.*)$',  r'(?P<prefix>\1)(?P<expr>\2)(?P<suffix>\3)$', pat).replace(' ', ' +').replace('=', ' *= *').replace(':', ' *:'), rexp_def_map)

wrap_rexp = [(pattern2rexp(pat.strip()), ctx.strip()) for pat, ctx in re.findall('^ ([^#]+) # ([^ ]+)$', wrap_pattern, re.M)]

def get_ctx(line):
    for rexp, ctx in wrap_rexp:
        m = re.match(rexp, line.strip())
        if m and parse_expr(m.groupdict().get('list', 'True')):
            return m.expand(ctx)
    return 'void'

def wrap_by_pattern(text):
    def wrap_line(line):
        for rexp, ctx in wrap_rexp:
            m = re.match(rexp, line)
            if m and parse_expr(m.groupdict().get('list', 'True')):
                return m.expand("""\g<prefix>I<<''' \g<expr> '''\g<suffix>""")
        return line
    def handle_line(line):
        m = re.match('^( *)(.+?)(( +#.*)?)$', line)
        if not m:
            return line
        else:
            return '%s%s%s'%(m.group(1), wrap_line(m.group(2)), m.group(3))
    lines = [handle_line(line) for line in text.split('\n')]
    return '\n'.join(lines)


def truncate_blank_line(src):
    return re.sub('(?m)^[ \t]+$', '', src)

def wrap_pysrc(src):
    return wrap_by_parse(wrap_by_pattern(truncate_blank_line(src)))

class InterpException(Exception):
    def __init__(self, filename, func, lineno, cmd, exception_list):
        self.filename, self.func, self.lineno, self.cmd, self.exception_list = filename, func, lineno, cmd, exception_list
    def __str__(self):
        return 'InterpException:\n### filename:%s lineno:%d\n### cmd: %s\n%s'%(self.filename, self.lineno + 1, self.cmd, '\n'.join('### [%ld] %s: %s'%(idx, func.func_name, exception) for idx, (func, exception) in enumerate(self.exception_list)))
    def __repr__(self):
        return str(self)

class ChainInterp:
    def __init__(self):
        self.chain = []
    def __call__(self, node):
        self.chain.append(node)
        return node
    def get_chain(self):
        return self.chain
    def interp(self, cmd, locals, globals, lineno=0, filename='<string>', code='void', func='void', **opt):
        ctx = get_ctx(code)
        ret = None
        exception_list = []
        is_expr = ctx != 'void'
        cmd = cmd.replace('\n', '\\n')
        for interp in self.chain:
            try:
                ret = interp(cmd, locals, globals, head=cmd.split(' ')[0], ctx=ctx, **opt)
            except Exception as e:
                exception_list.append((interp, e))
            if ret: break
        if type(ret) == dict:
            result = ret.get('result', None)
        elif exception_list:
            raise InterpException(filename, func, lineno, cmd, exception_list)
        else:
            result = cmd
        return result

if __name__ == '__main__':
    # print wrap_pysrc('obs0.cat |tail')
    print get_ctx("  tid = select table_id from __all_table where table_name='$table_name';")
    # print wrap_pysrc('id')
    # print wrap_pysrc('a = select svr_ip from __all_meta_table where table_id=$mytid and role=1;')
    # Interp = ChainInterp()
    # print Interp.interp('2 + 3', locals(), globals())
