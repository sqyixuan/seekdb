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
eval create table t_small_tx(c0 bigint not null auto_increment, index(c0)) partition by hash (c0) partitions $part_cnt;
eval create table t_big_tx(c0 bigint not null auto_increment, index(c0)) partition by hash (c0) partitions $part_cnt;
while ($col_cnt > 0)
{
  eval alter table t_small_tx add column c$col_cnt varchar($col_size) default "default";
  eval alter table t_big_tx add column c$col_cnt varchar($col_size) default "default";
  dec $col_cnt;
}

# start time.
let $restart_ts0_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
set autocommit=0;
--disable_query_log
let $init_row_cnt = 1024;
begin;
# init data in small _tb;
insert into t_small_tx (c0) values (1);
let $col_cnt = $init_col_cnt;
# 10KB for one row
while ($col_cnt > 0)
{
  eval update t_small_tx set c$col_cnt = repeat('a', $col_size) where c0 = 1;
  dec $col_cnt;
}

let $small_pdml_cnt = 19;
# 2^19 rows in t_small_tx, about 5GB
while ($small_pdml_cnt > 0)
{
  insert /*+ monitor enable_parallel_dml parallel(10) use_px */ into t_small_tx (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)  select /*+use_px,parallel(10)*/ c1, c2, c3, c4, c4, c6, c7, c8, c9, c10 from t_small_tx;
  dec $small_pdml_cnt;
}

# expect 2^20(1048576) reord
select count(*) from t_small_tx;

# commit;
# restart at trans begin;
let $restart_ts1_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
#echo $restart_ts1_usec;
# commit;

let $pdml_round = 4;
let $total_pdml_round = 0;
while($total_pdml_round < $pdml_round)
{
  insert /*+ monitor enable_parallel_dml parallel(10) use_px */ into t_big_tx (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)  select /*+use_px,parallel(100)*/ c1, c2, c3, c4, c4, c6, c7, c8, c9, c10 from t_small_tx;
  inc $total_pdml_round;
}
# commit;

let $restart_ts2_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
#echo $restart_ts2_usec;
let $col_cnt = $init_col_cnt;
while ($col_cnt > 0)
{
  eval update t_big_tx set c$col_cnt = repeat('b', $col_size) where c0 = 1;
  dec $col_cnt;
}

commit;

select count(*) from t_big_tx;

set autocommit=1;
--enable_query_log
