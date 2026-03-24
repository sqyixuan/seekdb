import sys
import os.path
import time
from caller_info import get_caller_info

def get_ts_str(ts=None):
    if ts == None: ts = time.time()
    return time.strftime('%m%d-%X', time.localtime(ts))

class Logger:
    ERROR = 0
    INFO = 1
    TRACE = 2
    DEBUG = 3
    def __init__(self, level, fd=sys.stdout):
        self.level = level
        self.level_stack = []
        self.fd = fd

    def create(self, log_level):
        return Logger(log_level, self.fd)

    def disable_log(self):
        self.fd = None
        # self.set_log_level(3)

    def set_log_file(self, fd):
        if type(fd) == str:
            fd = open(fd, 'w')
        self.fd = fd
        
    def force_info(self, level, msg):
        if self.fd != None:
            self.fd.write('%s\n'%(msg))
            self.fd.flush()
        # if level <= 0:
        #     sys.stderr.write('%s\n'%(msg))

    def push_log_level(self):
        self.level_stack.append(self.level)

    def pop_log_level(self):
        self.level = self.level_stack.pop()

    def set_log_level(self, level):
        self.level = level

    def get_log_level(self):
        return self.level

    def get_indent(self, opt):
        return '    ' * opt.get('_call_depth_', 0)

    def log(self, level, msg, header=True, opt={}, caller_info=None):
        if not caller_info:
            caller_info = get_caller_info()
        if level <= int(opt.get('_log_', self.level)):
            if header:
                self.force_info(level, '%s%s %s:%d %s' % (self.get_indent(opt), time.strftime('%Y-%m-%d %X', time.localtime(time.time())), os.path.basename(caller_info.get('filename')), caller_info.get('lineno'), msg))
            else:
                self.force_info(level, '%s%s'%(self.get_indent(opt), msg))
    def error(self, msg, header=True, opt={}):
        self.log(Logger.ERROR, msg, header, opt, get_caller_info())
    def info(self, msg, header=True, opt={}):
        self.log(Logger.INFO, msg, header, opt, get_caller_info())
    def trace(self, msg, header=True, opt={}):
        self.log(Logger.TRACE, msg, header, opt, get_caller_info())
    def debug(self, msg, header=True, opt={}):
        self.log(Logger.DEBUG, msg, header, opt, get_caller_info())

logger = Logger(1)
if __name__ == '__main__':
    log = Logger(1)
    log.log(Logger.DEBUG, 'this log should not output', False)
    log.log(Logger.ERROR, 'hello log without header', False)
    log.log(Logger.ERROR, 'hello log with header', True)
