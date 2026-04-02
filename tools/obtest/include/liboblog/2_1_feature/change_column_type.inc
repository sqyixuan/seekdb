## 支持类型转换

echo "[CASE_BEGIN] [change_column_type.inc]";
connection oblog_conn_sys;
use oceanbase;
let $tenant_id = query_get_value(select tenant_id from __all_tenant where tenant_name="cdc_mysql", tenant_id, 1);
connection conn_obcdc_mysql;
set autocommit=1;

set global max_allowed_packet = 1073741824;

drop database if exists oblog_change_col_db;
create database oblog_change_col_db;
use oblog_change_col_db;

#####################################################################
## 基础类型转换，由小变大，如tinyint -> smallint
## 字符串类型转换
create table t1 (id int primary key, v1 varchar(20), v2 tinytext, v3 text, v4 mediumtext, v5 longtext, v6 char(20));
## 整型转换
create table t2 (id int primary key, v1 tinyint, v2 smallint, v3 mediumint, v4 int, v5 bigint);
## 二进制类型转换
create table t3 (id int primary key, v1 varbinary(50), v2 tinyblob, v3 blob, v4 mediumblob, v5 longblob);

# start liboblog
let $start_tstamp = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_change_col_db.*|cdc*.*.* log_level=INFO sort_trans_participants=1;
ob1.oblog.restart;

let $cnt = 1;
let $val = -50;
eval insert into t1 values ($cnt, 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1');
ob1.oblog.next;

begin;
update t1 set v1 = 'oceanbase_c2', v2 = 'oceanbase_c2', v3 = 'oceanbase_c2', v4 = 'oceanbase_c2', v5 = 'oceanbase_c2' where id = 1;
eval insert into t2 values ($cnt, $val, $val, $val, $val, $val);
commit;
ob1.oblog.next;

begin;
update t1 set v1 = 'oceanbase_c3', v2 = 'oceanbase_c3', v3 = 'oceanbase_c3', v4 = 'oceanbase_c3', v5 = 'oceanbase_c3' where id = 1;
eval insert into t3 values ($cnt, 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1');
commit;
ob1.oblog.next;

######### 类型转换 ###################
# 字符串类型转换
alter table t1 modify v1 varchar(100);
alter table t1 modify v2 text;
#alter table t1 modify v3 mediumtext;
#alter table t1 modify v4 longtext;
alter table t1 modify v6 char(100);

# 整型转换
alter table t2 modify v1 smallint;
alter table t2 modify v2 mediumint;
alter table t2 modify v3 int;
alter table t2 modify v4 bigint;

# 二进制类型转换
alter table t3 modify v1 varbinary(100);
alter table t3 modify v2 blob;
#alter table t3 modify v3 mediumblob;
#alter table t3 modify v4 longblob;

use oceanbase;
if ($is_schema_split_mode != $SCHEMA_SPLIT_MODE)
{
  connection oblog_conn_sys;
}
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

# stop liboblog
ob1.oblog.stop;

connection conn_obcdc_mysql;
set autocommit=1;
use oblog_change_col_db;
if ($is_schema_split_mode != $SCHEMA_SPLIT_MODE)
{
  ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_change_col_db.*|cdc*.*.* log_level=INFO sort_trans_participants=1 global_data_start_schema_version=$max_ddl_schema_version;
}
if ($is_schema_split_mode == $SCHEMA_SPLIT_MODE)
{
  ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_change_col_db.*|cdc*.*.* log_level=INFO sort_trans_participants=1 data_start_schema_version=$tenant_id:$max_ddl_schema_version;
}
ob1.oblog.set_start_tstamp $start_tstamp;
ob1.oblog.restart;

# 回拉历史数据
ob1.oblog.next_dml;
ob1.oblog.next_dml;
ob1.oblog.next_dml;

let $cnt = 2;
eval insert into t1 values ($cnt, 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1');
ob1.oblog.next_dml;

eval insert into t2 values ($cnt, $val, $val, $val, $val, $val);
ob1.oblog.next_dml;

eval insert into t3 values ($cnt, 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1');
ob1.oblog.next_dml;

ob1.oblog.stop;


##########################################################################
# 主键列修改
connection conn_obcdc_mysql;
set autocommit=1;
use oblog_change_col_db;

create table ta1 (id int, name char(5), v1 int, primary key (id));
create table ta2 (id int, name char(5), v1 int, primary key (name));
create table ta3 (id int, name char(5), v1 int, primary key (id, name));
create table ta4 (id int, name varchar(5), v1 int, primary key (name));

ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_change_col_db.*|cdc*.*.* log_level=INFO sort_trans_participants=1;
ob1.oblog.restart;
let $cnt = 1;
eval insert into ta1 values($cnt, repeat('a', $cnt), $cnt);
ob1.oblog.next;
eval insert into ta2 values($cnt, repeat('a', $cnt), $cnt);
ob1.oblog.next;
eval insert into ta3 values($cnt, repeat('a', $cnt), $cnt);
ob1.oblog.next;
eval insert into ta4 values($cnt, repeat('a', $cnt), $cnt);
ob1.oblog.next;

#主键列修改暂时不支持
if ($is_change_column_support == $change_column_SUPPORT)
{
#alter table ta1 modify id bigint;
alter table ta2 modify name char(10);
alter table ta3 modify id bigint;
alter table ta3 modify name char(10);
alter table ta4 modify name varchar(10);
}

use oceanbase;
if ($is_schema_split_mode != $SCHEMA_SPLIT_MODE)
{
  connection oblog_conn_sys;
}
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

# stop liboblog
ob1.oblog.stop;

connection conn_obcdc_mysql;
set autocommit=1;
use oblog_change_col_db;
if ($is_schema_split_mode != $SCHEMA_SPLIT_MODE)
{
  ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_change_col_db.*|cdc*.*.* log_level=INFO global_data_start_schema_version=$max_ddl_schema_version;
}
if ($is_schema_split_mode == $SCHEMA_SPLIT_MODE)
{
 ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_change_col_db.*|cdc*.*.* log_level=INFO data_start_schema_version=$tenant_id:$max_ddl_schema_version;
}
ob1.oblog.restart;

if ($is_change_column_support == $change_column_SUPPORT)
{
update ta1 set id = 2147483647 where id = 1;
ob1.oblog.next_dml;
update ta2 set name = repeat('b', 8) where id = 1;
ob1.oblog.next_dml;
update ta3 set id = 2147483647, name = repeat('b', 8) where id = 1;
ob1.oblog.next_dml;
update ta4 set name = repeat('b', 8) where id = 1;
ob1.oblog.next_dml;
}

if ($is_change_column_support == $change_column_NOT_SUPPORT)
{
update ta1 set id = 2147483647 where id = 1;
ob1.oblog.next_dml;
update ta2 set name = repeat('b', 1) where id = 1;
ob1.oblog.next_dml;
update ta3 set id = 2147483647, name = repeat('b', 1) where id = 1;
ob1.oblog.next_dml;
update ta4 set name = repeat('b', 1) where id = 1;
ob1.oblog.next_dml;
}

# stop liboblog
ob1.oblog.stop;
echo "[CASE_END] [change_column_type.inc]";
