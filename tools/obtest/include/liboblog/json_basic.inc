connection oblog_conn_sys;
alter tenant cdc_mysql set variables max_allowed_packet=1073741824;

connect (conn_obcdc_msyql_lob,$ip,root@cdc_mysql,,oceanbase,$port);

set ob_trx_timeout=100000000;
set ob_query_timeout=100000000;
set ob_default_lob_inrow_threshold = 4096;

create database json_basic_db1;
use json_basic_db1;

# 启动liboblog
ob1.oblog.set_extra_argument -m;
ob1.oblog.gen_conf tb_white_list=sys.oblog*.*|cdc_mysql.*.*|oracle.*.* log_level=DEBUG;
ob1.oblog.restart;

CREATE TABLE t1(pk int primary key, json_col_1 JSON);
ob1.oblog.next;

# inrow
INSERT INTO t1 VALUES(1, '{"name": "john", "age": 100, "likes": [1, true, null]}');
select md5(json_col_1) from t1 where pk = 1;
ob1.oblog.next;

# update inrow -> inrow
update t1 set json_col_1 = '{"name": "john", "age": 100, "likes": ["1", "true", null]}' where pk = 1;
select md5(json_col_1) from t1 where pk = 1;
ob1.oblog.next;

# inrow
INSERT INTO t1 VALUES(2, '{"name": "mike", "age": 10, "likes": ["fish", 1.2, "null"]}');
select md5(json_col_1) from t1 where pk = 2;
ob1.oblog.next;

# update inrow -> outrow
update t1 set json_col_1 = CONCAT('[',REPEAT('"a",',20001),'"a"]') where pk = 2;
select md5(json_col_1) from t1 where pk = 2;
ob1.oblog.next;

# insert outrow
INSERT INTO t1 VALUES(3, CONCAT('[',REPEAT('"a",',20002),'"a"]'));
select md5(json_col_1) from t1 where pk = 3;
ob1.oblog.next;

# update outrow -> outrow
update t1 set json_col_1 = CONCAT('[',REPEAT('"b",',200003),'"b"]') where pk = 3;
select md5(json_col_1) from t1 where pk = 3;
ob1.oblog.next;

# update outrow -> inrow
update t1 set json_col_1 = CONCAT('[',REPEAT('"a",',50),'"a"]') where pk = 3;
select md5(json_col_1) from t1 where pk = 3;
ob1.oblog.next;

delete from t1 where pk = 3;
ob1.oblog.next;

# update rowkey
insert into t1 values(30001, CONCAT('{"K1": "', REPEAT('x', 40960),'"}'));
select md5(json_col_1) from t1 where pk = 30001;
ob1.oblog.next;
update t1 set pk = 30002 where pk = 30001;
ob1.oblog.next;
delete from t1 where pk = 30002;
ob1.oblog.next;

# delete
delete from t1;
ob1.oblog.next;

set log_row_value_options="partial_json"; 
create table json_test(pk int primary key, j json, b longtext) JSON(j) store as (CHUNK '1K');;
ob1.oblog.next;

# lob chunk size
select lob_chunk_size from oceanbase.__all_table t join oceanbase.__all_column c on t.table_id=c.table_id where t.table_name='json_test' and column_name='j';
select lob_chunk_size from oceanbase.__all_table t join oceanbase.__all_column c on t.table_id=c.table_id where t.table_name='json_test' and column_name='b';

insert json_test values(2, json_object('name', 'dark', 'age', 30, 'position', 'software engineer', 'profile', repeat('e', 4096), 'like', json_array('code', 'dream', 'sleep'), 'tags', json_array('sql boy', 'programer', 'summer', 10000), 'money' , json_object('RMB', 1000, 'Dollers', 20000, 'BTC', 12), 'nickname', 'noone'), repeat('y', 5000));
select md5(j), md5(b) from json_test where pk = 2;
ob1.oblog.next;

# partial update
update json_test set j = json_replace(j, '$.name', 'shadow') where pk = 2;
select md5(j), md5(b) from json_test where pk = 2;
select md5('{"version": 1, "diffs": [{"op": "replace", "path": "$.\\"name\\"", "value": "shadow"}]}') as diff_md5;
ob1.oblog.next;

update json_test set j = json_remove(json_replace(j, '$.tags[3]', 2), '$.tags[1]') where pk = 2;
select md5(j), md5(b) from json_test where pk = 2;
select md5('{"version": 1, "diffs": [{"op": "replace", "path": "$.\\"tags\\"[3]", "value": 2}, {"op": "replace", "path": "$.\\"tags\\"", "value": ["sql boy", "summer", 2]}]}') as diff_md5;
ob1.oblog.next;

update json_test set j = json_set(j, '$.position', 'se'), b = repeat('x', 3000) where pk = 2;
select md5(j), md5(b) from json_test where pk = 2;
select md5('{"version": 1, "diffs": [{"op": "replace", "path": "$.\\"position\\"", "value": "se"}]}') as diff_md5;
ob1.oblog.next;

# delete
delete from json_test;
ob1.oblog.next;

set log_row_value_options="";
drop table json_test;
ob1.oblog.next;

ob1.oblog.stop;
