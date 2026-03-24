##############
#owner: bohou.ws
#test points: big trans
##############

connection conn_obcdc_mysql;

set ob_query_timeout=90000000000000;
set ob_trx_timeout=1200000000000000;

create database if not exists obcdc_db;

use obcdc_db;

# 1KB for per col if col_size = 1024.
let $init_col_cnt = 10;
let $col_size=1024;
let $col_cnt = $init_col_cnt;
let $part_cnt = 10;
# create table.
eval create table t_heap(c0 bigint) partition by hash (c0) partitions $part_cnt;
eval create table t_heap2(c0 bigint) partition by hash (c0) partitions $part_cnt;
while ($col_cnt > 0)
{
  eval alter table t_heap add column c$col_cnt varchar($col_size) default "default";
  eval alter table t_heap2 add column c$col_cnt varchar($col_size) default "default";
  dec $col_cnt;
}

# start time.
let $restart_ts0_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);

set ob_org_cluster_id=2147473648;
set autocommit=0;
--disable_query_log
begin;
insert into t_heap2 (c0) values(1);

send insert /*+ enable_parallel_dml parallel(8) */ into t_heap2 select random(977) c0, randstr(1024, 1234) c1,randstr(1024, 1234) c2, randstr(1024, 1234) c3, randstr(1024, 1234) c4, randstr(1024, 1234) c5, randstr(1024, 1234) c6, randstr(1024, 1234) c7, randstr(1024, 1234) c8, randstr(1024, 1234) c9, randstr(1024, 1234) c10 from table(generator(2100000));

connection conn_obcdc_mysql2;
set ob_query_timeout=90000000000000;
set ob_trx_timeout=1200000000000000;
use obcdc_db;
set autocommit=0;
begin;
insert into t_heap (c0) values(1);
insert /*+ enable_parallel_dml parallel(8) */ into t_heap select random(977) c0, randstr(1024, 1234) c1,randstr(1024, 1234) c2, randstr(1024, 1234) c3, randstr(1024, 1234) c4, randstr(1024, 1234) c5, randstr(1024, 1234) c6, randstr(1024, 1234) c7, randstr(1024, 1234) c8, randstr(1024, 1234) c9, randstr(1024, 1234) c10 from table(generator(2097152));

let $restart_ts1_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
update t_heap set c1 = "modify" where c0 = 1 and c1 = 'default';
let $restart_ts2_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
commit;

connection conn_obcdc_mysql;
reap;
commit;
set autocommit=1;
--enable_query_log
