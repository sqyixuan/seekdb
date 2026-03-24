#
# owner:shuning.tsn
#
# description: 测试liboblog对数据类型的支持


--disable_warnings
drop database if exists oblog_db;

create database oblog_db;
use oblog_db;

#####################################################################
# 基础类型转换，由小变大，如tinyint -> smallint

## 字符串类型转换
create table t1 (id int primary key, v1 varchar(20), v2 tinytext, v3 text, v4 mediumtext, v5 longtext, v6 char(20));
## 整型转换
create table t2 (id int primary key, v1 tinyint, v2 smallint, v3 mediumint, v4 int, v5 bigint);
## 二进制类型转换
create table t3 (id int primary key, v1 varbinary(50), v2 tinyblob, v3 blob, v4 mediumblob, v5 longblob);
## 测试大类型转换：1. varchar->text  2. varchar->char
create table t_varchar (id int primary key, v1 varchar(20), v2 varchar(50), v3 varchar(60), v4 varchar(80), v5 char(20));
## (varbinary, tinyblob) -> binary max=256
create table t_binary (id int primary key, v0 binary(40), v1 varbinary(50), v2 tinyblob, v3 blob, v4 mediumblob, v5 longblob);

# start liboblog
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO;
ob1.oblog.restart;

# 准备数据
let $cnt = 1;
while ($cnt < 6)
{
  eval insert into t1 values ($cnt, 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1', 'oceanbase_c1');
  ob1.oblog.next;
  inc $cnt;
}

let $cnt = 1;
let $val = -50;
while ($cnt < 6)
{
  eval insert into t2 values ($cnt, $val, $val, $val, $val, $val);
  ob1.oblog.next;
  inc $cnt;
  inc $val;
}

let $cnt = 1;
while ($cnt < 6)
{
  eval insert into t3 values ($cnt, 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1');
  ob1.oblog.next;
  inc $cnt;
}

let $cnt = 1;
while ($cnt < 3)
{
  eval insert into t_varchar values ($cnt, 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1');
  ob1.oblog.next;
  inc $cnt;
}

let $cnt = 1;
while ($cnt < 3)
{
  eval insert into t_binary values ($cnt, 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1', 'oceanbase_v1');
  ob1.oblog.next;
  inc $cnt;
}

sleep 1;
let $start_tstamp = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);

######### 类型转换 ###################

# 字符串类型转换
alter table t1 modify v1 varchar(100);
alter table t1 modify v2 text;
#Modify large text/lob column not supported
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

# 1. varchar->text  2. varchar->char
alter table t_varchar modify v1 text;
alter table t_varchar modify v2 text;
alter table t_varchar modify v3 char(100);
alter table t_varchar modify v4 char(100);
alter table t_varchar modify v5 char(100);

# TODO test
# binary 注意：22x版本目前不支持把列修改为binary类型 报错ERROR 1235 (0A000): modify column to binary type not supported 20200907
##alter table t_binary modify v1 binary(100);
#alter table t_binary modify v2 binary(256);

connection oblog_conn_sys;
use oceanbase;
let $tenant_id = query_get_value(select tenant_id from __all_tenant where tenant_name="oblog_tt", tenant_id, 1);
connection oblog_conn_tt;
# schema split mode, 在租户本身下面查询
use oceanbase;
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

# stop liboblog
ob1.oblog.stop;

connection oblog_conn_tt;
use oblog_db;
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO data_start_schema_version=$tenant_id:$max_ddl_schema_version;
ob1.oblog.set_start_tstamp $start_tstamp;
ob1.oblog.restart;

insert into t1 values (100, repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100));
ob1.oblog.next_dml;
update t1 set v1 = 'oceanbase_c3', v2 = 'oceanbase_c3', v3 = 'oceanbase_c3', v4 = 'oceanbase_c3', v5 = 'oceanbase_c3' where id = 5;
ob1.oblog.next_dml;
insert into t2 values (100, 128, 32768, 8388608, -5, -5);
ob1.oblog.next_dml;
update t2 set v1 = -15 , v2 = -15, v3 = -15, v4 = -15, v5 = -15 where id = 5;
ob1.oblog.next_dml;
insert into t3 values(100, repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100));
ob1.oblog.next_dml;
update t3 set v1 = 'oceanbase_v3', v2 = 'oceanbase_v3', v3 = 'oceanbase_v3', v4 = 'oceanbase_v3', v5 = 'oceanbase_v3' where id = 5;
ob1.oblog.next_dml;
insert into t_varchar values(100, repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100));
ob1.oblog.next_dml;
update t_varchar set v1 = 'oceanbase_v3', v2 = 'oceanbase_v3', v3 = 'oceanbase_v3', v4 = 'oceanbase_v3', v5 = 'oceanbase_v3' where id = 1;
ob1.oblog.next_dml;
#insert into t_binary values(100, repeat('a', 5), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100));
insert into t_binary values(100, repeat('a', 5), repeat('a', 50), repeat('a', 100), repeat('a', 100), repeat('a', 100), repeat('a', 100));
ob1.oblog.next_dml;
update t_binary set v1 = 'oceanbase_v3', v2 = 'oceanbase_v3', v3 = 'oceanbase_v3', v4 = 'oceanbase_v3', v5 = 'oceanbase_v3' where id = 1;
ob1.oblog.next_dml;

ob1.oblog.stop;

#########################################################################################
# 测试类型大小边界，如tinyint最大为128，测试类型转换tinyint->smallint，插入256(smallint最大值)

--disable_warnings
create table t4 (id int primary key, v1 tinytext, v2 text, v3 mediumtext, v4 longtext);
create table t5 (id int primary key, v1 tinyblob, v2 blob, v3 mediumblob, v4 longblob);
create table t6 (id int primary key, v1 text);
create table t7 (id int primary key, v1 blob);

# md5输出
# start liboblog
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO;
ob1.oblog.set_extra_argument -m;;
ob1.oblog.restart;
# 插入数据
insert into t4 values (1, repeat('a', 255), repeat('b', 65535), repeat('c', 16777215), repeat('d', 16777215));
ob1.oblog.next;
insert into t5 values (1, repeat('a', 255), repeat('b', 65535), repeat('c', 16777215), repeat('d', 16777215));
ob1.oblog.next;
insert into t6 values (1, repeat('x', 65535));
ob1.oblog.next;
insert into t7 values (1, repeat('y', 65535));
ob1.oblog.next;

select md5(v1), md5(v2), md5(v3), md5(v4) from t4 where id = 1;
select md5(v1), md5(v2), md5(v3), md5(v4) from t5 where id = 1;
select md5(v1) from t6 where id = 1;
select md5(v1) from t7 where id = 1;

alter table t4 modify v1 text;
#alter table t4 modify v2 mediumtext;
#alter table t4 modify v3 longtext;
alter table t5 modify v1 blob;
#alter table t5 modify v2 mediumblob;
#alter table t5 modify v3 longblob;
#alter table t6 modify v1 longtext;
#alter table t7 modify v1 longblob;

sleep 1;
let $start_tstamp = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);

connection oblog_conn_sys;
use oceanbase;
let $tenant_id = query_get_value(select tenant_id from __all_tenant where tenant_name="oblog_tt", tenant_id, 1);
connection oblog_conn_tt;
# schema split mode, 在租户本身下面查询
use oceanbase;
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

# stop liboblog
ob1.oblog.stop;

ob1.oblog.set_extra_argument -m;
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO data_start_schema_version=$tenant_id:$max_ddl_schema_version;
ob1.oblog.set_start_tstamp $start_tstamp;
ob1.oblog.restart;

use oblog_db;
update t4 set v1 = repeat('b', 255), v2 = repeat('c', 65535), v3 = repeat('d', 16777215), v4 = repeat('e', 16777215) where id = 1;
ob1.oblog.next_dml;
update t5 set v1 = repeat('b', 255), v2 = repeat('c', 65535), v3 = repeat('d', 16777215), v4 = repeat('e', 16777215) where id = 1;
ob1.oblog.next_dml;
update t6 set v1 = repeat('y', 65535);
ob1.oblog.next_dml;
update t7 set v1 = repeat('z', 65535);
ob1.oblog.next_dml;

select md5(v1), md5(v2), md5(v3), md5(v4) from t4 where id = 1;
select md5(v1), md5(v2), md5(v3), md5(v4) from t5 where id = 1;
select md5(v1) from t6 where id = 1;
select md5(v1) from t7 where id = 1;
ob1.oblog.stop;

##########################################################################
# 主键列修改
connection oblog_conn_tt;
use oblog_db;
create table ta1 (id int, name char(5), v1 int, primary key (id));
create table ta2 (id int, name char(5), v1 int, primary key (name));
create table ta3 (id int, name char(5), v1 int, primary key (id, name));
create table ta4 (id int, name varchar(5), v1 int, primary key (name));
create table ta5 (id int, name varchar(5), v1 int, primary key (id, name));

ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO;
ob1.oblog.restart;
let $cnt = 1;
while ($cnt < 6)
{
  eval insert into ta1 values($cnt, repeat('a', $cnt), $cnt);
  ob1.oblog.next;
  inc $cnt;
}
let $cnt = 1;
while ($cnt < 6)
{
  eval insert into ta2 values($cnt, repeat('a', $cnt), $cnt);
  ob1.oblog.next;
  inc $cnt;
}
let $cnt = 1;
while ($cnt < 6)
{
  eval insert into ta3 values($cnt, repeat('a', $cnt), $cnt);
  ob1.oblog.next;
  inc $cnt;
}
let $cnt = 1;
while ($cnt < 6)
{
  eval insert into ta4 values($cnt, repeat('a', $cnt), $cnt);
  ob1.oblog.next;
  inc $cnt;
}
let $cnt = 1;
while ($cnt < 6)
{
  eval insert into ta5 values($cnt, repeat('a', $cnt), $cnt);
  ob1.oblog.next;
  inc $cnt;
}

select id, name, v1 from ta1 where id = 5;
select id, name, v1 from ta2 where id = 5;
select id, name, v1 from ta3 where id = 5;
select id, name, v1 from ta4 where id = 5;
select id, name, v1 from ta5 where id = 5;

# 主键列修改暂时不支持
# alter table ta1 modify id bigint;
# alter table ta2 modify name char(10);
# alter table ta3 modify id bigint;
# alter table ta3 modify name char(10);
# alter table ta4 modify name varchar(10);
# alter table ta5 modify name varchar(10);

let $start_tstamp = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);
connection oblog_conn_sys;
use oceanbase;
let $tenant_id = query_get_value(select tenant_id from __all_tenant where tenant_name="oblog_tt", tenant_id, 1);
connection oblog_conn_tt;
# schema split mode, 在租户本身下面查询
use oceanbase;
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

# stop liboblog
ob1.oblog.stop;

connection oblog_conn_tt;

ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO data_start_schema_version=$tenant_id:$max_ddl_schema_version;
ob1.oblog.restart;
use oblog_db;

update ta1 set id = 2147483647 where id = 5;
ob1.oblog.next_dml;
update ta2 set name = repeat('b', 5) where id = 5;
ob1.oblog.next_dml;
update ta3 set id = 2147483647, name = repeat('b', 5) where id = 5;
ob1.oblog.next_dml;
update ta4 set name = repeat('b', 5) where id = 5;
ob1.oblog.next_dml;
update ta5 set name = repeat('b', 5) where id = 5;
ob1.oblog.next_dml;
ob1.oblog.stop;

########################################################
# 测试类型转换后ddl解释类型转换前数据
sleep 2;
connection oblog_conn_tt;
use oblog_db;
create table tb1 (id int primary key, v1 varchar(10), v2 char(10), v3 varbinary(10), v4 text, v5 blob, v6 int);

let $start_tstamp = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);
let $cnt = 0;
insert into tb1 values (1, repeat('a', 5), repeat('a', 5), repeat('a', 5), repeat('a', 5), repeat('a', 5), 1);
inc $cnt;
update tb1 set v1 = repeat('b', 5), v2 = repeat('b', 5), v3 = repeat('b', 5), v4 = repeat('b', 5), v5 = repeat('b', 5), v6 = 1 where id = 1;
inc $cnt;
update tb1 set v1 = repeat('c', 5), v2 = repeat('c', 5), v3 = repeat('c', 5), v4 = repeat('c', 5), v5 = repeat('c', 5), v6 = 1 where id = 1;
inc $cnt;
update tb1 set v1 = repeat('d', 5), v2 = repeat('d', 5), v3 = repeat('d', 5), v4 = repeat('d', 5), v5 = repeat('d', 5), v6 = 1 where id = 1;
inc $cnt;

alter table tb1 modify v1 varchar(20);
alter table tb1 modify v2 char(20);
alter table tb1 modify v3 varbinary(20);
#alter table tb1 modify v4 longtext;
#alter table tb1 modify v5 longblob;
#alter table tb1 modify v6 bigint;

sleep 1;
connection oblog_conn_sys;
use oceanbase;
let $tenant_id = query_get_value(select tenant_id from __all_tenant where tenant_name="oblog_tt", tenant_id, 1);
connection oblog_conn_tt;
# schema split mode, 在租户本身下面查询
use oceanbase;
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

connection oblog_conn_tt;
use oblog_db;
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO data_start_schema_version=$tenant_id:$max_ddl_schema_version;
ob1.oblog.set_start_tstamp $start_tstamp;
ob1.oblog.restart;

let $tmp_cnt = 0;
while ($tmp_cnt < $cnt)
{
  ob1.oblog.next_dml;
  inc $tmp_cnt;
}

ob1.oblog.stop;
