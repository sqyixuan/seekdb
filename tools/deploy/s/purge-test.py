load('s/utility.py')

_sql_trace_ = True
table_size = 100

import random
def random_range():
    return random.randrange(table_size),random.randrange(table_size)

def major_freeze(**kw):
    try:
        alter system major freeze #(type='sql')
    except Exception as e:
        pass

def minor_freeze(**kw):
    try:
        alter system minor freeze #(type='sql')
    except Exception as e:
        pass

def random_delete(**kw):
    start_key, end_key = random_range()
    delete from t1 where pk > $start_key and pk < $end_key #(type='comp')
    select * from t1 order by pk desc #(type='comp')
    

def random_insert(**kw):
    start_key, end_key = random_range()
    step = idf(start_key) > end_key and -1 or +1
    values = ','.join('(%d)'%(i) for i in range(start_key, end_key, step))
    if idf(values):
        replace into t1 values $values #(type='comp')
        select * from t1 order by pk desc #(type='comp')

def random_select(**kw):
    start_key, end_key = random_range()
    select * from t1 where pk > $start_key and pk < $end_key order by pk desc #(type='comp')

def delete_all(**kw):
    delete from t1 #(type='comp')
    select * from t1 order by pk desc #(type='comp')

def select_all(**kw):
    select count(*) from t1 #(type='comp')

main_sql_conn = make_conn('ob1.obs0')
comp_sql_conn = make_conn('ob2.obs0')

cur_sql_conn = comp_sql_conn
drop table if exists t1;
set autocommit=1 #(type='sql')

cur_sql_conn = main_sql_conn
set autocommit=1 #(type='sql')
drop table if exists t1 #(type='comp')
create table if not exists t1 (pk int primary key) #(type='comp')

op_list = reduce(lambda x,y: x+y, [[op] * int(cnt) for op, cnt in re.findall('([a-z_]+):(\d+)', 'major_freeze:1 minor_freeze:1 random_delete:10 random_insert:10 random_select:10 delete_all:0 select_all:0')], [])

ob1.obs0.kill -41
try:
    while True:
        op = random.choice(op_list)
        print `'do $op'`
        $op
except Exception as e:
    print e
    ob1.obs0.kill -42

