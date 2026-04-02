 #
 # owner:千溪
 # owner group:liboblog
 # description:
 # 测试点: 覆盖所有ddl操作
 # 1. 创建租户、创建database、创建table、创建outline
 #    创建用户
 # 2. 权限相关：database权限、table权限
 # 3. 同义词（现在暂时瀚廷只提交了master, 后续补充）
 #
 # 对于1.4.4 还需要迭代 CREATE USER 'ROOT' DDL
 #
 ##

echo "[CASE_BEGIN] [all_ddl_type.inc]";

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

--disable_warnings
drop tenant if exists test_oblog_tt;
--disable_warnings
drop resource pool if exists oblog_pool;
--disable_warnings
drop resource unit if exists oblog_box;
--disable_warnings
drop database if exists oblog_db1;
--disable_warnings
drop table if exists t1, t2;

create resource unit oblog_box max_cpu 1, memory_size='5G';
create resource pool oblog_pool unit = 'oblog_box', unit_num = 1, zone_list = ('z1');
create tenant test_oblog_tt replica_num = 1, primary_zone='z1', resource_pool_list=('oblog_pool');

--disable_warnings
--disable_query_log
--disable_result_log
sleep 3;
--error 0,1064
alter tenant test_oblog_tt set variables ob_tcp_invited_nodes='%';
--enable_warnings
--enable_query_log
--enable_result_log


# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.*.*|oblog*.*.*|*cdc*.*.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

# 1. TENANT_SCHEMA

# 修改租户信息: OB_DDL_ALTER_TENANT
alter tenant test_oblog_tt set comment = 'oblog tenant';
ob1.oblog.next;

# 删除test_oblog_tt租户:
# OB_DDL_DEL_TENANT
drop tenant test_oblog_tt;
ob1.oblog.next;

#if ($is_schema_split_mode == $SCHEMA_SPLIT_MODE)
#{
#  drop tenant test_oblog_tt force;
#  ob1.oblog.next;
#}
connection conn_obcdc_mysql;
# 2. DATABASE_SCHEMA
# OB_DDL_ADD_DATABASE
create database oblog_db1;
ob1.oblog.next;
create database oblog_db2;
ob1.oblog.next;

# 修改database
# OB_DDL_ALTER_DATABASE
alter database oblog_db1 default character set utf8;
ob1.oblog.next;

# 删除database
# OB_DDL_DEL_DATABASE
set recyclebin=off;
drop database oblog_db2;
ob1.oblog.next;

# 重命名DB: OB_DDL_RENAME_DATABASE
# TODO: 不支持

# 3. TABLE_SCHEMA
use oblog_db1;

# 创建表
# OB_DDL_CREATE_TABLE
create table t1 (pk int primary key);
ob1.oblog.next;

create table t2(pk int primary key, c1 int, c2 int) partition by range(pk) (partition p0 values less than(10), partition p1 values less than(20));
ob1.oblog.next;

# 修改表信息
# OB_DDL_ALTER_TABLE
alter table t1 set comment = 'abc';
ob1.oblog.next;

# 加列
# OB_DDL_ADD_COLUMN
alter table t1 add column c1 varchar(10) default 'a';
ob1.oblog.next;

# 修改列信息
# OB_DDL_CHANGE_COLUMN
# OB_DDL_ALTER_COLUMN
# OB_DDL_MODIFY_COLUMN
alter table t1 modify column c1 varchar(100);
ob1.oblog.next;

# 删列
# OB_DDL_DROP_COLUMN
alter table t2 drop c2;
ob1.oblog.next;

# 重命名表
alter table t1 rename to t3;
ob1.oblog.next;

# OB_DDL_TABLE_RENAME
rename table t3 to t1;
ob1.oblog.next;

# 删除表: OB_DDL_DROP_TABLE
set recyclebin=off;
drop table t2;
ob1.oblog.next;

# 创建索引: OB_DDL_CREATE_INDEX
create index c1_idx on t1 (c1);
ob1.oblog.next;

# 删除索引: OB_DDL_DROP_INDEX
set recyclebin=off;
drop index c1_idx on t1;
ob1.oblog.next;

# 创建VIEW: OB_DDL_CREATE_VIEW
create view t1_view (c1) as select c1 from t1;
ob1.oblog.next;

# 删除VIEW: OB_DDL_DROP_VIEW
set recyclebin=off;
drop view t1_view;
ob1.oblog.next;

# truncate table
# OB_DDL_TRUNCATE_TABLE_CREATE
# OB_DDL_TRUNCATE_TABLE_DROP
truncate table t1;
ob1.oblog.next;

# 创建tablegroup
# OB_DDL_ADD_TABLEGROUP
create tablegroup tg1;
ob1.oblog.next;

# tablegroup中添加表
# OB_DDL_ALTER_TABLEGROUP_ADD_TABLE
alter tablegroup tg1 add t1;
ob1.oblog.next;

# tablegroup中删除表
alter table t1 set tablegroup '';
ob1.oblog.next;

# 重命名tablegroup: OB_DDL_RENAME_TABLEGROUP
# TODO: 不支持

# 删除tablegroup
# OB_DDL_DEL_TABLEGROUP
drop tablegroup tg1;
ob1.oblog.next;

# 4. outline
# 创建outline: OB_DDL_CREATE_OUTLINE
create outline otl_1 on select /*+max_concurrent(1)*/ * from t1;

# alter outline
# OB_DDL_ALTER_OUTLINE
alter outline otl_1 add select /*+use_nl(t1)*/ * from t1;

# replace outline
# OB_DDL_REPLACE_OUTLINE
create or replace outline otl_1 on select /*+max_concurrent(2)*/ * from t1;

# drop outline
# OB_DDL_DROP_OUTLINE
drop outline otl_1;

####### 验证数据是否能够迭代到
replace into t1 (pk) values (1), (20), (300);
ob1.oblog.next;

# 停止oblog
ob1.oblog.stop;

# 清理数据
connection oblog_conn_sys;
--disable_warnings
drop resource pool if exists oblog_pool;
--disable_warnings
drop resource unit if exists oblog_box;
--disable_warnings
drop database if exists oblog_db1;

echo "[CASE_END] [all_ddl_type.inc]";
