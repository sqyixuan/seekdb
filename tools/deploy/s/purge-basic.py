load('s/utility.py')

reboot_if_not_ok
cur_sql_conn = make_conn('obi.obs0')
set autocommit=1 #(type='sql')

table_size=100000

def create_table(**kw):
    drop table if exists t1;
    create table if not exists t1 (pk int primary key);
def load_data(**kw):
    for i in range(0, table_size, 10000):
        print 'load %d'%(i,)
        values = ','.join('(%d)'%(i) for i in range(i, i+10000))
        insert into t1 values $values;
        commit
def delete_row(**kw):
    delete from t1;

def prepare_table(**kw):
    create_table
    load_data
    print 'major_freeze'
    major_freeze
    delete_row
    print 'minor_freeze'
    minor_freeze

def single_range_scan(**kw):
    with StatReport():
        select count(*) from t1;
    with StatReport():
        select count(*) from t1;

def multi_range_scan(**kw):
    with StatReport():
        select count(*) from t1 where pk < 1000 or pk > 2000;
    with StatReport():
        select count(*) from t1 where pk < 1000 or pk > 2000;

def timeout_test(**kw):
    global _sql_trace_
    create_table
    load_data
    _sql_trace_ = True
    set ob_query_timeout=150000 #(type='sql')
    with TraceLog(), StatReport():
        select count(1) from t1;

def teardown(**kw):
    tid, = `select table_id from __all_table where table_name='t1'`[0]
    print `'tid=$tid'`
    print 'dump memtable'
    rm -rf /tmp/dump_memtable
    obi.obs0.ob_admin dump_memtable $tid:0:0
    head -1 /tmp/dump_memtable/*


# timeout_test
prepare_table
with TraceLog():
     single_range_scan

# prepare_table
# with TraceLog():
#     multi_range_scan
teardown
