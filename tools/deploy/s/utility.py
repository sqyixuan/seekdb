load('sql.py')
def create_tenant(cpu=4, **kw):
    zone = `obi.obs0.zone`
    drop resource pool if exists testpool;
    drop resource unit if exists testunit;

    CREATE RESOURCE UNIT testunit max_cpu $cpu, memory_size 2147483648;
    create resource pool testpool unit='testunit',unit_num=1,zone_list=('$zone');
    create tenant tt1 replica_num=1,resource_pool_list=('testpool');

def is_bootstrap_ok(**kw):
    try:
        obi.check_bootstrap
        ret = True
    except Exception as e:
        ret = False
    return ret

def reboot_if_not_ok(**kw):
    if `is_bootstrap_ok`:
        logger.info('already OK')
    else:
        logger.info('not bootstrap OK, try reboot')
        obi.reboot

def create_tenant_if_not_exist(**kw):
    if `select * from __all_tenant where tenant_name='tt1'`:
        print 'tenant tt1 already exist'
    else:
        print 'create tenant tt1'
        create_tenant

def drop_table(**kw):
    drop table if exists t1;

def create_table(**kw):
    create table t1 (pk varchar(256) primary key);

def get_table_id(table_name='t1', **kw):
    tid = `select table_id from __all_table where table_name='$table_name';`
    return tid

def migrate(tid, src, dest, **kw):
    print 'migrate $tid $src $dest'
    alter SYSTEM move REPLICA  PARTITION_ID '0%1@$tid' SOURCE='$src' DESTINATION='$dest';

def addr2name(addr='', **kw):
    for k, v in get_match_child(kw, '^ob.*').items():
        if sub('$ip:$port', v) == addr:
            return k

def major_freeze(**kw):
    alter system major freeze #(type='sql')
def minor_freeze(**kw):
    alter system minor freeze #(type='sql')

def is_merge_finish(**kw):
    if list(`select * from __all_virtual_memstore_info where is_active = 0`):
        raise Fail("merge not finish")
    else:
        print 'merge finish'

def wait_merge(**kw):
    sleep 3
    !tryloop[interval=3]: is_merge_finish _log_=-1

def dump_memtable(**kw):
    tid, = `select table_id from __all_table where table_name='t1'`[0]
    rm -rf /tmp/dump_memtable
    obi.obs0.ob_admin dump_memtable $tid:0:0

def randchoice(spec):
    op_list = reduce(lambda x,y: x+y, [[op] * int(cnt) for op, cnt in re.findall('([a-z0-9_]+):(\d+)', spec)], [])
    return random.choice(op_list)

import multiprocessing

class P2:
    def __init__(self, duration):
        self.is_run = multiprocessing.Value('i', 1)
        self.end_time = time.time() + duration
    def can_run(self):
        return self.is_run.value and time.time() < self.end_time
    def stop(self):
        self.is_run.value = 0
    def start(self, func, cnt=1):
        for i in range(cnt):
            p = multiprocessing.Process(target=func, args=(self, ))
            p.daemon = True
            p.start()

class Test:
    def __init__(self, on_fail):
        self.on_fail = on_fail
    def __enter__(self):
        print '==== TestBegin ===='
    def __exit__(self, exc_type, exc_value, traceback):
        if None == exc_type:
            print '==== TestSucc ===='
        else:
            print 'TestFail: %s'%(exc_value)
            self.on_fail()
            print '==== TestFail ===='
            
class TraceLog:
    def __enter__(self):
        alter system set syslog_level='TRACE' #(type='sql')
    def __exit__(self, *args):
        alter system set syslog_level='INFO' #(type='sql')

def dump_memtable(**kw):
    tid, = list(`select max(table_id) from __all_table where table_name not like '\_\_%'`)[0]
    rm -rf /tmp/dump_memtable
    obi.obs0.ob_admin dump_memtable $tid:0:0
