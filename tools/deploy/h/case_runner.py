class CaseRunner:
    def __init__(self, d, test_name=''):
        self.pass_list, self.fail_list = [], []
        self.d = d
        self.test_name = test_name
        self.start_time = time.time()

    def pinfo(self, msg, header='-', level=1):
        def expand_header(header):
            if not header: header = ' '
            c = (header[0] in '-=+*') and header[0] or ' '
            return ('%8s' % (header)).replace(' ', c)
        color_code = (level == 0) and '31' or '32'
        print >>sys.stdout, '\033[%sm[ %8s ]\033[0m %s' % (color_code, expand_header(header), msg)
        sys.stdout.flush()

    def prepare(self, path, *args, **kw):
        self.pinfo(path, header="=")
        return self.run('prepare', path, *args, **kw)
        
    def report(self):
        if self.pass_list:
            self.pinfo(','.join(self.pass_list), header="PASSLST")
        if self.fail_list:
            self.pinfo(','.join(self.fail_list), header="FAILED", level=0)
        else:
            self.pinfo("PASS %s TEST"%(self.test_name), header="=")
            print 'PASS %s TEST'%(self.test_name)
            sys.stdout.flush()
        self.pinfo("%fs"%(time.time() - self.start_time), header="UTIME")
    def setup(self, name):
        self.pinfo(name, header="BEGIN   ")

    def teardown(self, name, is_succ, start_time):
        if is_succ:
            self.pass_list.append(name)
            self.pinfo('%s: utime: %fs'%(name, time.time() - start_time), header="PASS")
        else:
            self.fail_list.append(name)
            self.pinfo('%s: utime: %fs'%(name, time.time() - start_time), header="FAIL", level=0)

    def on_exception(self, name, e):
        self.pinfo('%s: %s'%(name, e), header='*error*', level=0)

    def run(self, name, path, *args, **kw):
        start_time = time.time()
        self.setup(name)
        try:
            result = call(self.d, path, *args, **kw)
            self.pinfo(result, header="-RET")
            is_succ = True
        except Exception as e:
            is_succ = False
            self.on_exception(name, e)
        self.teardown(name, is_succ, start_time)
