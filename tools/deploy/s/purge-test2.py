load('s/utility.py')
import random

_sql_trace_ = True
table_size = 10000
thread_cnt = 64

def on_exception():
    # global cur_sql_conn #!python
    # cur_sql_conn.close()
    # cur_sql_conn = make_conn('obi.obs0')
    pass

def random_range():
    return random.randrange(table_size),random.randrange(table_size)

def major_freeze(**kw):
    try:
        alter system major freeze #(type='sql')
    except Exception as e:
        on_exception()

def minor_freeze(**kw):
    try:
        alter system minor freeze #(type='sql')
    except Exception as e:
        on_exception()

def load_values(**kw):
    values = ','.join('(%d, 0)'%(i) for i in range(table_size))
    insert into t1 values $values

def random_update(**kw):
    start_key, end_key = random_range()
    update t1 set c1 = mod(c1+3, 7) where pk > $start_key and pk < $end_key

def random_update_limit(**kw):
    start_key, end_key = random_range()
    update t1 set c1 = mod(c1+3, 7) where pk > $start_key limit 1000;

def random_update2(**kw):
    c1 = random.randrange(10)
    update t1 set c1 = mod(c1+3, 7) where c1=$c1

def random_sfu(**kw):
    start_key, end_key = random_range()
    select * from t1 where pk > $start_key and pk < $end_key for update;
    
def trans_rollback(**kw):
    start_key, end_key = random_range()
    begin #(type='sql')
    update t1 set c1 = mod(c1+3, 7) where pk > $start_key and pk < $end_key
    c1 = random.randrange(10)
    update t1 set c1 = mod(c1+3, 7) where c1=$c1
    rollback #(type='sql')

def check_count(**kw):
    a, = list(`select /*+index(t1 c1)*/count(1) from t1`)[0]
    if a != table_size:
        # pstat.value = 0
        raise Fail("check_count fail: index table row count mismatch", a)
def check_idx(**kw):
    q1, q2 = 'select pk,c1 from t1', 'select /*+index(t1 c1)*/pk,c1 from t1'
    a =  list(`$q1 except $q2`)
    if a:
        pstat.value = 0
        raise Fail("check_idx fail: main table has more row", a)
    b =  list(`$q2 except $q1`)
    if b:
        pstat.value = 0
        raise Fail('check_idx fail: index table has more row', b)

def random_list():
    x = range(7)
    random.shuffle(x)
    return x
def check_idx2(**kw):
    key_list = random_list()
    q1, q2 = "select /*+log_level('*.*:trace')*/ pk,c1 from t1", "select /*+log_level('*.*:trace') index(t1 c1)*/pk,c1 from t1 where c1 in (%s) order by c1 desc"%(','.join(str(i) for i in key_list))
    try:
        a =  list(`$q1 except $q2`)
    except mysql.connector.Error as e:
        print 'check sql fail: %s, ignore'%(e)
        a = None
    if a:
        raise Fail("check_idx fail: main table has more row", a)
    try:
        b = list(`$q2 except $q1`)
    except mysql.connector.Error as e:
        print 'check sql fail: %s, ignore'%(e)
        b = None
    if b:
        raise Fail('check_idx fail: index table has more row', b)
    try:
        c = list(`$q2`)
    except mysql.connector.Error as e:
        print 'check sql fail: %s, ignore'%(e)
        c = []
    if len(c) != table_size:
        raise Fail('check_idx fail: index table row_count not right', c)

reboot_if_not_ok
cur_sql_conn = make_conn('obi.obs0')
set autocommit=1 #(type='sql')
drop table if exists t1;
create table if not exists t1 (pk int, c1 int, primary key(pk), key c1(c1));
load_values

### nlp join test
drop table if exists t2;
create table if not exists t2 (pk int primary key);
insert into t2 values (0), (1), (2), (3), (4), (5), (6);
def do_rescan(**kw):
    select /*+ use_nl(a,b) index(b c1) log_level('*.*:info') */ a.pk as c1, b.pk as c2 from t2 a, t1 b where a.pk = b.c1;

op_list = 'major_freeze:1 minor_freeze:1 random_update_limit:100000 random_update:100 random_update2:0 trans_rollback:0 random_sfu:0'
def purge_test(self, **kw):
    global cur_sql_conn #!python
    cur_sql_conn = make_conn('obi.obs0')
    set autocommit=1 #(type='sql')
    while self.can_run():
        op = randchoice(op_list)
        try:
            $op
        except Exception as e:
            cur_sql_conn = make_conn('obi.obs0')
def delete_old_log(self, *args):
    while self.can_run():
        obi.obs0.rm_old_log
        sleep 10

p2 = P2(86400)
def post_test(**kw):
    p2.stop()
    dump_memtable

p2.start(delete_old_log)
p2.start(purge_test, thread_cnt)

with Test(post_test):
    while p2.can_run():
        # do_rescan
        check_count
