connection conn_admin;
alter system flush plan cache global;
--sleep 3
connection default;
--disable_warnings
drop table if exists cz_t1, cz_t2, cz_t3, cz_t4, src_t, cz_t5, cz_t6;
--enable_warnings

create table cz_t1 (pk int primary key, a int, b int, c int, e int, d int) partition by hash(pk) partitions 2;
create table cz_t2 (pk int primary key, a int, b int, c int, e int, d int);
create table cz_t3 (pk int primary key, a int, b int, c int, e int, d int);
create table cz_t4 (pk int primary key, a int, b int, c int, e int, d int);
create table src_t (pk int primary key, a int, b int, c int, e int, d int) partition by hash(pk) partitions 2;
create table cz_t5 (pk int primary key, a int, b int) partition by hash(pk) partitions 2;
create table cz_t6 (pk int primary key, a int, b int) partition by hash(pk) partitions 2;

create index idx_t1_a on cz_t1(a) global partition by hash(a) partitions 2;
create index idx_t2_a on cz_t2(a) local;
create index idx_t2_b on cz_t2(b) local;

create index idx_t3_a on cz_t3(a) global partition by hash(a) partitions 2;
create index idx_t3_b on cz_t3(b) global partition by hash(b) partitions 2;
create index idx_t4_a on cz_t4(a) local;
create index idx_t5_a on cz_t5(a) global partition by hash(a) partitions 2;
create index idx_t6_a on cz_t6(a) global partition by hash(a) partitions 2;

--source mysql_test/include/check_all_idx_ok.inc

connection conn_admin;
let $cz_t1_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'cz_t1' order by table_id desc limit 1, table_id, 1);
let $cz_t2_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'cz_t2' order by table_id desc limit 1, table_id, 1);
let $cz_t3_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'cz_t3' order by table_id desc limit 1, table_id, 1);
let $czt1_idx_a = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%_idx_t1_a%' order by table_id desc limit 1, table_id, 1);
let $czt3_idx_a = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%_idx_t3_a%' order by table_id desc limit 1, table_id, 1);
let $czt3_idx_b = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%_idx_t3_b%' order by table_id desc limit 1, table_id, 1);
let $cz_t4_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'cz_t4' order by table_id desc limit 1, table_id, 1);
let $cz_t5_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'cz_t5' order by table_id desc limit 1, table_id, 1);
let $cz_t6_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'cz_t6' order by table_id desc limit 1, table_id, 1);
let $czt5_idx_a = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%_idx_t5_a%' order by table_id desc limit 1, table_id, 1);
let $czt6_idx_a = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%_idx_t6_a%' order by table_id desc limit 1, table_id, 1);
let $src_t_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'src_t' order by table_id desc limit 1, table_id, 1);

let $czt1_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t1_id and role = 1, svr_ip, 1);
let $czt1_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t1_id and role = 1, svr_port, 1);
let $czt1_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t1_id and role = 2, svr_ip, 1);
let $czt1_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t1_id and role = 2, svr_port, 1);

let $czt1_idxa_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt1_idx_a and role = 1, svr_ip, 1);
let $czt1_idxa_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt1_idx_a and role = 1, svr_port, 1);
let $czt1_idxa_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt1_idx_a and role = 2, svr_ip, 1);
let $czt1_idxa_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt1_idx_a and role = 2, svr_port, 1);

let $czt2_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t2_id and role = 1, svr_ip, 1);
let $czt2_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t2_id and role = 1, svr_port, 1);
let $czt2_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t2_id and role = 2, svr_ip, 1);
let $czt2_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t2_id and role = 2, svr_port, 1);

let $czt3_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t3_id and role = 1, svr_ip, 1);
let $czt3_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t3_id and role = 1, svr_port, 1);
let $czt3_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t3_id and role = 2, svr_ip, 1);
let $czt3_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t3_id and role = 2, svr_port, 1);

let $czt3_idxa_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_a and role = 1, svr_ip, 1);
let $czt3_idxa_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_a and role = 1, svr_port, 1);
let $czt3_idxa_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_a and role = 2, svr_ip, 1);
let $czt3_idxa_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_a and role = 2, svr_port, 1);

let $czt3_idxb_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_b and role = 1, svr_ip, 1);
let $czt3_idxb_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_b and role = 1, svr_port, 1);
let $czt3_idxb_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_b and role = 2, svr_ip, 1);
let $czt3_idxb_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt3_idx_b and role = 2, svr_port, 1);

let $czt4_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t4_id and role = 1, svr_ip, 1);
let $czt4_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t4_id and role = 1, svr_port, 1);
let $czt4_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t4_id and role = 2, svr_ip, 1);
let $czt4_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t4_id and role = 2, svr_port, 1);

let $czt5_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t5_id and role = 1, svr_ip, 1);
let $czt5_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t5_id and role = 1, svr_port, 1);
let $czt5_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t5_id and role = 2, svr_ip, 1);
let $czt5_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t5_id and role = 2, svr_port, 1);

let $czt5_idxa_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt5_idx_a and role = 1, svr_ip, 1);
let $czt5_idxa_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt5_idx_a and role = 1, svr_port, 1);
let $czt5_idxa_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt5_idx_a and role = 2, svr_ip, 1);
let $czt5_idxa_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt5_idx_a and role = 2, svr_port, 1);

let $czt6_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t6_id and role = 1, svr_ip, 1);
let $czt6_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t6_id and role = 1, svr_port, 1);
let $czt6_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $cz_t6_id and role = 2, svr_ip, 1);
let $czt6_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $cz_t6_id and role = 2, svr_port, 1);

let $czt6_idxa_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt6_idx_a and role = 1, svr_ip, 1);
let $czt6_idxa_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt6_idx_a and role = 1, svr_port, 1);
let $czt6_idxa_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $czt6_idx_a and role = 2, svr_ip, 1);
let $czt6_idxa_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $czt6_idx_a and role = 2, svr_port, 1);

let $czsrc_part1_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $src_t_id and role = 1, svr_ip, 1);
let $czsrc_part1_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $src_t_id and role = 1, svr_port, 1);
let $czsrc_part2_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $src_t_id and role = 2, svr_ip, 1);
let $czsrc_part2_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $src_t_id and role = 2, svr_port, 1);
alter system set _enable_partition_level_retry=True;
--sleep 3

connection default;
--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CASE#1 MULTI PART INSERT
--enable_query_log
insert /*+index(cz_t1 idx_t1_a)*/into cz_t1 values (1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2);
insert /*+index(cz_t1 idx_t1_a)*/into cz_t1 values (3, 3, 3, 3, 3, 3), (4, 4, 4, 4, 4, 4);
--enable_result_log
explain insert /*+index(cz_t1 idx_t1_a)*/into cz_t1 values (3, 3, 3, 3, 3, 3), (4, 4, 4, 4, 4, 4);
--echo // expect hit_count = 1
--disable_warnings
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert /*+index(cz_t1 idx_t1_a)*/into cz_t1 values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)';

connection conn_admin;
--disable_query_log
--echo // switch cz_t1 part1 to backup
eval alter system switch replica leader partition_id = '0%2@$cz_t1_id' server='$czt1_part2_ip:$czt1_part2_port';
--sleep 5
connection default;

--enable_query_log
insert /*+index(cz_t1 idx_t1_a)*/into cz_t1 values (5, 5, 5, 5, 5, 5), (6, 6, 6, 6, 6, 6);
explain /*+index(cz_t1 idx_t1_a)*/insert into cz_t1 values (5, 5, 5, 5, 5, 5), (6, 6, 6, 6, 6, 6);
--echo // expect hit_count = 2
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert /*+index(cz_t1 idx_t1_a)*/into cz_t1 values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)';

delete from cz_t1;

insert into src_t values (2, 2, 2, 2, 2, 2);
insert into cz_t1 (select * from src_t where pk = 2);
delete from src_t;
insert into src_t values (4, 4, 4, 4, 4, 4);
insert into cz_t1 (select * from src_t where pk = 4);

explain insert into cz_t1 (select * from src_t where pk = 4);

--echo // hit_count = 1, type = 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into cz_t1 (select * from src_t where pk = ?)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo // switch src_t part1 to backup
eval alter system switch replica leader partition_id = '0%2@$src_t_id' server='$czsrc_part2_ip:$czsrc_part2_port';
--sleep 5

connection default;
--enable_query_log
--enable_result_log

delete from src_t;
insert into src_t values (6, 6, 6, 6, 6, 6);
insert into cz_t1 (select * from src_t where pk = 6);
delete from src_t;
insert into src_t values (8, 8, 8, 8, 8, 8);
insert into cz_t1 (select * from src_t where pk = 8);

explain insert into cz_t1 (select * from src_t where pk = 8);
--echo // expect hit_count = 1, 1, new plan is generated and hit
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into cz_t1 (select * from src_t where pk = ?)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo // switch src_t part1 to main
eval alter system switch replica leader partition_id = '0%2@$src_t_id' server='$czsrc_part1_ip:$czsrc_part1_port';
--sleep 3
--echo // switch cz_t1 part1 to main
eval alter system switch replica leader partition_id = '0%2@$cz_t1_id' server='$czt1_part1_ip:$czt1_part1_port';
--sleep 3

connection default;
--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CASE#2 OR EXPANSION WITH LOCAL INDEX
--disable_result_log
--disable_query_log
--echo // insert 10000 rows into cz_t2
let $cnt = 1;
while ($cnt <= 10000)
{
  eval insert into cz_t2 values ($cnt, $cnt, $cnt, $cnt, $cnt, $cnt);
  inc $cnt;
}
--enable_result_log
--enable_query_log

select /*+use_concat*/* from cz_t2 where a = 1 or b = 2;
select /*+use_concat*/* from cz_t2 where a = 2 or b = 3;
explain select /*+use_concat*/* from cz_t2 where a = 2 or b = 3;
--echo // hit_count = 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from cz_t2 where a = ? or b = ?';

--disable_result_log
--disable_query_log
connection conn_admin;
--echo // switch cz_t2 part1 to backup
eval alter system switch replica leader partition_id = '0%2@$cz_t2_id' server='$czt2_part2_ip:$czt2_part2_port';
--sleep 3

--enable_result_log
--enable_query_log
connection default;
select /*+use_concat*/* from cz_t2 where a = 3 or b = 4;
select /*+use_concat*/* from cz_t2 where a = 4 or b = 5;
explain select /*+use_concat*/* from cz_t2 where a = 3 or b = 4;

--echo // hit_count = 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from cz_t2 where a = ? or b = ?';

--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CASE#3 OR EXPANSION WITH GLOBAL INDEX
--disable_result_log
--disable_query_log
--echo // insert 10000 rows into cz_t3
let $cnt = 1;
while ($cnt <= 10000)
{
  eval insert into cz_t3 values ($cnt, $cnt, $cnt, $cnt, $cnt, $cnt);
  inc $cnt;
}
--enable_query_log
--enable_result_log
select /*+use_concat*/* from cz_t3 where a = 1 or b = 2;
select /*+use_concat*/* from cz_t3 where a = 2 or b = 3;
explain select /*+use_concat*/* from cz_t3 where a = 2 or b = 3;

--echo // hit_count = 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from cz_t3 where a = ? or b = ?';


--disable_result_log
--disable_query_log
connection conn_admin;
--echo // switch idx_t3_a part1 to backup
eval alter system switch replica leader partition_id = '0%2@$czt3_idx_a' server='$czt3_idxa_part2_ip:$czt3_idxa_part2_port';
--sleep 5

--enable_result_log
--enable_query_log
connection default;
select /*+use_concat*/* from cz_t3 where a = 2 or b = 4;
select /*+use_concat*/* from cz_t3 where a = 4 or b = 6;
select /*+use_concat*/* from cz_t3 where a = 3 or b = 6;
explain select /*+use_concat*/* from cz_t3 where a = 4 or b = 5;
explain select /*+use_concat*/* from cz_t3 where a = 3 or b = 6;

--echo // hit_count = 1, 2
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from cz_t3 where a = ? or b = ?';

--disable_result_log
--disable_query_log
connection conn_admin;
--echo // switch idx_t3_b part1 to backup
eval alter system switch replica leader partition_id = '0%2@$czt3_idx_b' server='$czt3_idxb_part2_ip:$czt3_idxb_part2_port';
--sleep 10
--enable_result_log
--enable_query_log

connection default;
select /*+use_concat*/* from cz_t3 where a = 6 or b = 8;
select /*+use_concat*/* from cz_t3 where a = 8 or b = 10;
select /*+use_concat*/* from cz_t3 where a = 7 or b = 9;
select /*+use_concat*/* from cz_t3 where a = 7 or b = 8;
explain select /*+use_concat*/* from cz_t3 where a = 6 or b = 8;
explain select /*+use_concat*/* from cz_t3 where a = 6 or b = 7;
explain select /*+use_concat*/* from cz_t3 where a = 7 or b = 9;
explain select /*+use_concat*/* from cz_t3 where a = 7 or b = 8;

--echo // hit_count = 0, 1, 2, 3, 
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from cz_t3 where a = ? or b = ?';

--echo //   >>>>>>>>>>>>>>>>>>>>>>> GLOBAL INDEX, LOCAL INDEX Mixed Up
connection default;
--disable_warnings
drop table if exists t31;
--enable_warnings

create table t31 (pk int primary key, a int, b int, c int, d int, e int) partition by hash(pk) partitions 2;
create index idx_a_t31 on t31(a) local;
create index idx_b_t31 on t31(b) global partition by hash(b) partitions 2;

--disable_query_log
--disable_result_log
let $cnt = 1;
while ($cnt <= 10000)
{
  eval insert into t31 values ($cnt, $cnt, $cnt, $cnt, $cnt, $cnt);
  inc $cnt;
}

--enable_query_log
--enable_result_log
select /*+use_concat*/* from t31 where a = 6 or b = 8;
select /*+use_concat*/* from t31 where a = 8 or b = 10;
explain select /*+use_concat*/* from t31 where a = 8 or b = 10;

select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from t31 where a = ? or b = ?';

connection conn_admin;
let $t31_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 't31' order by table_id desc limit 1, table_id, 1);
let $t31_idxb_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%idx_b_t31%' order by table_id desc limit 1, table_id, 1);
let $t31_main_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $t31_id and role = 1 order by partition_id limit 1, svr_ip, 1);
let $t31_backup_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $t31_id and role = 2 order by partition_id limit 1, svr_ip, 1);
let $t31_main_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $t31_id and role = 1 order by partition_id limit 1, svr_port, 1);
let $t31_backup_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $t31_id and role = 2 order by partition_id limit 1, svr_port, 1);
let $t31_idxb_main_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $t31_idxb_id and role = 1 order by partition_id limit 1, svr_ip, 1);
let $t31_idxb_backup_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $t31_idxb_id and role = 2 order by partition_id limit 1, svr_ip, 1);
let $t31_idxb_main_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $t31_idxb_id and role = 1 order by partition_id limit 1, svr_port, 1);
let $t31_idxb_backup_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $t31_idxb_id and role = 2 order by partition_id limit 1, svr_port, 1);

--echo // switch t31 part0 to backup
--disable_query_log
--disable_result_log
eval alter system switch replica leader partition_id='0%2@$t31_id' server='$t31_backup_ip:$t31_backup_port';
--sleep 5

connection default;
--enable_query_log
--enable_result_log
select /*+use_concat*/* from t31 where a = 6 or b = 8;
select /*+use_concat*/* from t31 where a = 8 or b = 10;
explain select /*+use_concat*/* from t31 where a = 8 or b = 10;

select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from t31 where a = ? or b = ?';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo // switch idx_b_t31 part0 to backup
eval alter system switch replica leader partition_id = '0%2@$t31_idxb_id' server='$t31_idxb_backup_ip:$t31_idxb_backup_port';
--sleep 5

connection default;
--enable_query_log
--enable_result_log
select /*+use_concat*/* from t31 where a = 6 or b = 8;
select /*+use_concat*/* from t31 where a = 8 or b = 10;

explain select /*+use_concat*/* from t31 where a = 8 or b = 10;

select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select /*+use_concat*/* from t31 where a = ? or b = ?';

drop table t31;
connection default;
--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CASE#4 LATE MATERIALIZATION
--disable_query_log
--disable_result_log
--echo // insert values into cz_t4
let $cnt = 1;
while ($cnt <= 1000)
{
  eval insert into cz_t4 values ($cnt, $cnt, $cnt, $cnt, $cnt, $cnt);
  inc $cnt;
}

--enable_result_log
--enable_query_log

select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5;
select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5;
explain select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5;

--echo // expect hit_count = 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5';

--disable_result_log
--disable_query_log
connection conn_admin;
--echo // switch cz_t4 part1 to backup
eval alter system switch replica leader partition_id = '0%1@$cz_t4_id' server='$czt4_part2_ip:$czt4_part2_port';
--sleep 3

--enable_result_log
--enable_query_log
connection default;
select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5;
select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5;
explain select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5;

--echo // hit_count = 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+USE_LATE_MATERIALIZATION*/* from cz_t4 order by a limit 5';

connection default;
--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CASE#5 TABLE LOOKUP
--echo // reset cz_t1, and src_t
--disable_result_log
--disable_query_log
connection conn_admin;
eval alter system switch replica leader partition_id = '0%2@$cz_t1_id' server='$czt1_part1_ip:$czt1_part1_port';
eval alter system switch replica leader partition_id = '0%2@$src_t_id' server='$czsrc_part1_ip:$czsrc_part1_port';
eval alter system switch replica leader partition_id = '0%2@$czt1_idx_a' server='$czt1_idxa_part1_ip:$czt1_idxa_part1_port';
--sleep 5
connection default;
delete from cz_t1;
delete from src_t;

--enable_query_log
--enable_result_log

insert into cz_t1 values (1, 1, 1, 1, 1, 1);
insert into cz_t1 values (2, 2, 1, 1, 1, 1);
insert into cz_t1 values (3, 3, 1, 1, 1, 1);
insert into src_t (select * from cz_t1);

--echo // single table select with lookup
select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;
select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;
explain select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;
--echo // hit_count = 1, type = 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = ?';

--disable_result_log
--disable_query_log
connection conn_admin;
--echo // switch cz_t1 part1 to backup
eval alter system switch replica leader partition_id = '0%2@$cz_t1_id' server='$czt1_part2_ip:$czt1_part2_port';
--sleep 5
--enable_query_log
--enable_result_log
connection default;
select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;
explain select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;

--echo // hit_count = 2, type = 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = ?';

--disable_result_log
--disable_query_log
connection conn_admin;
--echo // switch idx_t1_a part1 to backup
eval alter system switch replica leader partition_id = '0%2@$czt1_idx_a' server='$czt1_idxa_part2_ip:$czt1_idxa_part2_port';
--sleep 5
--enable_query_log
--enable_result_log
connection default;
select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;
select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;
explain select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = 2;

--echo // hit_count = 1, 2, type = 3, 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+index(cz_t1 idx_t1_a)*/* from cz_t1 where a = ?';

--echo // join table
--disable_result_log
--disable_query_log
insert into cz_t5 values (1, 1, 1), (2, 2, 2);
insert into cz_t6 (select * from cz_t5);
--enable_query_log
--enable_result_log

select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;
select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;
explain select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;

--echo // expect hit_count = 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = ? and cz_t6.a = ?';


connection conn_admin;
--disable_result_log
--disable_query_log
--echo // switch idx_t5_a part2 to backup
eval alter system switch replica leader partition_id = '1%2@$czt5_idx_a' server='$czt5_idxa_part2_ip:$czt5_idxa_part2_port';
--sleep 5

connection default;
--enable_result_log
--enable_query_log
select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;
select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;
explain select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;

--echo // expect hit_count = 1, 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = ? and cz_t6.a = ?';

connection conn_admin;
--disable_result_log
--disable_query_log
--echo // switch idx_t6_a part2 to backup
eval alter system switch replica leader partition_id = '1%2@$czt6_idx_a' server='$czt6_idxa_part2_ip:$czt6_idxa_part2_port';
--sleep 5

connection default;
--enable_result_log
--enable_query_log
select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;
select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;
explain select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = 1 and cz_t6.a = 1;

--echo // expect hit_count = 1, 1, 1
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'select /*+index(cz_t5 idx_t5_a) index(cz_t6 idx_t6_a)*/cz_t5.b as b1, cz_t6.b as b2 from cz_t5 join cz_t6 on cz_t5.pk = cz_t6.pk where cz_t5.a = ? and cz_t6.a = ?';
--enable_warnings

--enable_query_log
--enable_result_log
--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CASE#6 TABLE LOOKUP WITH PARTITION WISE join
--disable_warnings
drop table if exists t1, t2, t_insert;
--enable_warnings

create table t1 (pk int primary key, a int, b int) partition by hash(pk) partitions 2;
create table t2 (pk int primary key, a int, b int);
create table t_insert (pk int primary key, a int, b int) partition by hash(pk) partitions 2;

create index case_idx_t1 on t1(a) partition by hash(a) partitions 2;
create index case_idx_t_insert on t_insert(a) partition by hash(a) partitions 2;

insert into t1 values (1, 1, 1), (2, 2, 2), (4, 4, 4), (3, 3, 3), (5, 5, 5);
insert into t2 values (1, 1, 1), (2, 2, 2), (4, 4, 4), (3, 3, 3), (5, 5, 5);

insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 5;
explain insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 5;
connection conn_admin;

let $t2_tid = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 't2' order by table_id desc limit 1, table_id, 1);
let $t1_idx_tid = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%case_idx_t1%' order by table_id desc limit 1, table_id, 1);

let $t2_svr_backup_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $t2_tid and role = 2 limit 1, svr_ip, 1);
let $t2_svr_backup_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $t2_tid and role = 2 limit 1, svr_port, 1);

let $t1_idx_backup_ip = query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table where table_id = $t1_idx_tid and role = 2 and partition_id = 0 limit 1, svr_ip, 1);
let $t1_idx_backup_port = query_get_value(select svr_port from oceanbase.__all_virtual_meta_table where table_id = $t1_idx_tid and role = 2 and partition_id = 0 limit 1, svr_port, 1);

--disable_query_log
--disable_result_log
--echo // switch t2 to backup, case_idx_t1's part0 to backup
eval alter system switch replica leader partition_id='0%2@$t1_idx_tid' server='$t1_idx_backup_ip:$t1_idx_backup_port';
--sleep 10
eval alter system switch replica leader partition_id='0%1@$t2_tid' server='$t2_svr_backup_ip:$t2_svr_backup_port';
--sleep 10
--enable_query_log
--enable_result_log

connection default;

insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 2;
insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 4;

--echo // should be a different plan
insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 1;
insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 3;

explain insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 4;

explain insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = 3;
--disable_warnings
select hit_count, type from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t_insert select /*+index(t1 case_idx_t1)*/t1.* from t1 join t2 where t1.a = t2.a and t1.a = ?';
--enable_warnings

drop table t1, t2, t_insert;
--echo // >>>>>>>>>>>>>>>>>>>>>>>>> CLEANUP

connection default;
drop table cz_t1, cz_t2, cz_t3, cz_t4, src_t, cz_t5, cz_t6;
