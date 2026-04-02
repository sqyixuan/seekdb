load_file('h/sql.py,h/case_runner.py')

disable_reboot = False
class Stress:
    def __init__(self, user='root', database='oceanbase', table='t1'):
        self.user, self.database, self.table_name = user, database, table
        self.idx = itertools.count(0)
        self.conn = None
        self.last_succ_time = 0
    def get_conn(self):
        if not self.conn:
            self.conn = get_conn(user=self.user, database=self.database)
        return self.conn

    def on_exception(self, sql, e):
        error("query: %s: %s"%(sql, e))
        time.sleep(1)
        self.conn = None

    def on_begin(self, sql):
        debug("begin_query: %s"%(sql))

    def on_end(self, sql, result):
        debug("end_query: %s: result=%s"%(sql, result != None))
        max_error_duration = 60
        if self.last_succ_time == 0:
            self.last_succ_time = time.time()
        if result != None:
            self.last_succ_time = time.time()
        if time.time() - self.last_succ_time > max_error_duration:
            raise Fail('error occur during last %ds'%(max_error_duration))

    def write(self):
        sql = "insert into t1 values('%d')"%(self.idx.next())
        result = False
        self.on_begin(sql)
        try:
            conn = self.get_conn()
            conn.query(sql)
            result = True
            conn.commit()
        except (MySQLdb.Error, Exception) as e:
            self.on_exception(sql, e)
        finally:
            self.on_end(sql, result)
        

class ThreadRunner(Thread):
    def __init__(self, func, *args, **kw):
        self.result = None
        def thread_func():
            while check_stop():
                try:
                    func(*args, **kw)
                except Exception as e:
                    tracepoint.on_exception({}, 'thread_runner', {}, e, fatal=True)
        Thread.__init__(self, target=thread_func)
        self.setDaemon(True)

def magic_query1(cmd, *args, **kw):
    result = call(kw, 'query', cmd)
    if not result:
        raise Fail('empty result')
    return str(result[0][0])

@ObServerBuilder
def ObServerFT(**__kw):
    def query(sql='', **kw):
        sql = sub(sql, kw)
        try:
            conn = SqlConn(sub(conn_str, kw))
            return conn.query(sql)[1]
        except Exception as e:
            on_sql_error(sql, e)
            return None
    get_table_id = '''!query1: select /*+READ_CONSISTENCY(WEAK) */ table_id from __all_table where table_name='t1' '''
    switch_leader = "!sql: alter system switch replica leader partition_id='0%1@$get_table_id' server='$ip:$port' /* NoException */"
    stop_and_start = '!seq: stop sleep[30] start'
    migrate = "!sql: alter system move replica partition_id='0%1@$get_table_id' source='$source' destination='$ip:$port'; /* NoException */"
    return build_dict(locals(), **__kw)

loop_count=1
@ObInstanceBuilder
def ObInstanceFT(**__kw):
    table_name = "t1"
    rand_switch_leader = "!rand: all_observer switch_leader"
    rand_restart_observer = '!rand: all_observer stop_and_start'
    rand_observer_addr = '!rand: all_observer addr'
    rand_addr = '!second: rand_observer_addr'
    rand_migrate = '!rand: all_observer migrate source=$rand_addr'
    test_prepare = '!call: run_file fault_test/setup.py'
    test_case_list = 'reboot,create_table,major_freeze,#switch_leader,#migrate,#restart,#mix'
    run_case = '!call: run_file fault_test/t/$case.py'
    cleanup_use_popen = '!sh: ./hap.py $ob_name.[force_stop,cleanup] _log_=-1'
    fault_test_use_popen = '!sh: ./hap.py $ob_name.fault_test _log_=3'
    run_fault_test = '!seq: cleanup_use_popen fault_test_use_popen cleanup_use_popen'
    #run_fault_test = 'PASS FAULT TEST # skipped'

    def name_by_addr(addr='', **kw):
        for k, v in get_match_child(kw, '^ob.*').items():
            if sub('$ip:$port', v) == addr:
                return k
    def fault_test(*args, **kw):
        case_runner = CaseRunner(kw, 'FAULT')
        logger.set_log_level(int(kw.get('_log_', '1')))
        globals().update(disable_reboot='disable-reboot' in args, loop_count=int(kw.get('loop_count', "10")))
        case_list = kw.get('testset', test_case_list)
        case_runner.prepare('test_prepare')
        for case in case_list.split(','):
            if case.startswith('#'):
                case_runner.pinfo(case, header='SKIP')
            else:
                case_runner.run(case, 'run_case', case=case)
        case_runner.report()

    def run_file(file_list, *args, **kw):
        globals().update(obi=kw)
        return [(f, rf(f)) for f in file_list.split(',')]

    return build_dict(locals(), **__kw)
