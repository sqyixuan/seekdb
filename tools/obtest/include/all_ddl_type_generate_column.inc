#owner: 修铭
#owner group: liboblog
#description: 测试liboblog是否能正常解析生成列schema
#注意：仅测试1470及之后的版本，因为这个case中有LOB类型，1470才支持

echo "[CASE_BEGIN] [all_ddl_type_generate_column.inc]";
connection conn_obcdc_mysql;
drop database if exists oblog_db1;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db1.*|*cdc*.oblog_db1.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

# 创建database oblog_db1
# 后续所有的操作都在这个DB下
create database oblog_db1;
use oblog_db1;

set ob_org_cluster_id=9876;

# store类型的生成列
create table t1(a int primary key, b varchar(10), c varchar(10) generated always as (substring(b, 3)) stored);
insert into t1 (a, b) values(1, 'ab'), (2, 'abc'), (3, 'abcdef');
ob1.oblog.next_dml;
# stored 生成列做unique key需要输出
create unique index t1_uk on t1 (a, c) local;
ob1.oblog.next;
insert into t1 (a, b) values (4, 'abcd');
ob1.oblog.next;
replace into t1 (a, b) values (5, 'abcdef'), (3, 'abcdefg');
ob1.oblog.next;

# virtual类型的生成列
create table t2(a int primary key, b varchar(10), c varchar(10) generated always as (substring(b, 3)) virtual);
insert into t2 (a, b) values(1, 'ab'), (2, 'abc'), (3, 'abcdef');
ob1.oblog.next_dml;

# 隐式生成列：前缀索引
create table t3 (c1 int, c2 varchar(1000));
create index prefix_idx on t3 (c2(3));
replace into t3 values (1, 'abcdefg'), (2, 'defg'), (3, 'x');
ob1.oblog.next_dml;

# 显示生成列：分区键
create table t5(c1 varchar(10), c2 varchar(8) generated always as (substring(c1, 2)) virtual, c3 int) partition by key(c2) partitions 5;
insert into t5 (c1, c3) values ('abcdefg', 100);
ob1.oblog.next_dml;

# 唯一索引验证
create unique index t5_uk on t5 (c1, c2) local;
insert into t5 (c1, c3) values ('xyzabcde', 200);
ob1.oblog.next_dml;
insert into t5 (c1, c3) values ('zhongguo', 200);
ob1.oblog.next_dml;

# 显示生成列：表达式索引
create table t6(pk int primary key, a int);
alter table t6 add column b int as(a+1);
create index idx on t6(b);
insert into t6 (pk, a) values (1, 1), (2, 2), (3, 3);
ob1.oblog.next_dml;

# 索引建立在隐式生成列上
create table t7(pk1 int, pk2 int, first_name varchar(100), last_name varchar(100), primary key(pk1, pk2));
insert into t7 values(1,1, 'first                     1', 'last                    1');
ob1.oblog.next_dml;

create index test_index ON t7(first_name(10));
insert into t7(pk1,pk2,first_name,last_name) values (1, 1, 'first                     1', 'last                    2') on duplicate key update first_name = 'dtew';
ob1.oblog.next_dml;

replace into t7(pk1,pk2,first_name,last_name) values (1, 1, 'first                     1', 'last                    2');
ob1.oblog.next_dml;

## change column场景
# TODO：在1471打开，1470支持下面场景有BUG
#create table jx_t2(c1 int, c2 varchar(20) default 'abc', index idx(c2(2)), index idx2(c2(1)));
#insert into jx_t2 (c1) values (1);
#ob1.oblog.next_dml;
#insert into jx_t2 (c1, c2) values (2, '2222');
#ob1.oblog.next_dml;
#
#alter table jx_t2 change column c2 col2 varchar(20) default 'bcd';
#insert into jx_t2 (c1) values (3);
#ob1.oblog.next_dml;
#insert into jx_t2 (c1, col2) values (4, '4444');
#ob1.oblog.next_dml;
#
#drop index idx on jx_t2;
#insert into jx_t2 (c1, col2) values (5, '5555');
#ob1.oblog.next_dml;
#
#alter table jx_t2 add column c3 char (50) default 'def';
#alter table jx_t2 add index idx3(c3(1));
#alter table jx_t2 change column c3 col3 char(50);
#insert into jx_t2 (c1, col2) values (6, '6666');
#ob1.oblog.next_dml;
#
#drop index idx2 on jx_t2;
#insert into jx_t2 (c1, col2, col3) values (7, '7777', '8888');
#ob1.oblog.next_dml;

# 生成列指定默认值
# 2.0版本schema模块兼容1470及之前的版本有问题导致的，它要求所有的生成列的默认值current和original必须一致，如果不一致就出刷schema失败
create table t8 (c1 int, c2 varchar(1000) default '');
ob1.oblog.next;
create index prefix_idx on t8 (c2(3));
ob1.oblog.next;
insert into t8 values(1, "abc");
ob1.oblog.next;

create table t9(a int primary key, b varchar(10) default 'hello', c varchar(10) generated always as (substring(b, 3)));
ob1.oblog.next;
insert into t9 (a, b) values(2, "abc");
ob1.oblog.next;

create table t10 (`id` bigint(11) UNSIGNED NOT NULL AUTO_INCREMENT, `instance_id` varchar(128) NOT NULL DEFAULT '',`data_id` varchar(512) NOT NULL DEFAULT '' COMMENT 'combine', PRIMARY KEY (`id`), KEY `uniq_instance_data` (`instance_id`, `data_id`(255)));
ob1.oblog.next;
insert into t10 values(1, "2", "3");
ob1.oblog.next;

# 停止liboblog
ob1.oblog.stop;

--disable_warnings
drop database if exists oblog_db1;
set ob_org_cluster_id=1;

echo "[CASE_END] [all_ddl_type_generate_column.inc]";
