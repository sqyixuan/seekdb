 #
 # owner:千溪
 # owner group:liboblog
 # description:
 # 测试点: 覆盖所有关于回收站ddl操作
 # 开启和关闭回收站：
 # 1. DROP TABLE/VIEW/DB
 # 2. TRUNCATE
 # 3. PURGE
 # 4. FLASHBACK
 #
 ##

echo "[CASE_BEGIN] [recycle_bin_ddl.inc]";

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

--disable_warnings
drop database if exists oblog_db1;
drop database if exists oblog_db2;
create user if not exists __oceanbase_inner_drc_user;
grant all privileges on *.* to __oceanbase_inner_drc_user;
delete from oceanbase.__all_ddl_id;

# 指定DDL id，必须特定用户登录
sleep 3;
connect (conn_drc,$ip,__oceanbase_inner_drc_user@sys,,*NO-ONE*,$port);
connection conn_drc;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.*.*;
ob1.oblog.restart;

# 1. database相关
# OB_DDL_ADD_DATABASE
set @__oceanbase_ddl_id = '1_1_12345678900';
create database oblog_db1;
ob1.oblog.next;

set @__oceanbase_ddl_id = '1_1_12345678901';
create database oblog_db2;
ob1.oblog.next;

## 开启回收站, 操作如下: 开启回收站drop database -> flashback database -> purge database
# 1.1 开启回收站drop database
set recyclebin=on;
set @__oceanbase_ddl_id = '1_1_12345678903';
drop database oblog_db2;
ob1.oblog.next;
# 查询oblog_db2在回收站的name
let $db2_recycle_dbname1 = __recycle_\$_1_1_12345678903;

# 1.2 FLASHBACK database
set @__oceanbase_ddl_id = '1_1_12345678904';
eval FLASHBACK DATABASE $db2_recycle_dbname1 TO BEFORE DROP;
ob1.oblog.next;

# 1.3 purge database
set @__oceanbase_ddl_id = '1_1_12345678905';
drop database oblog_db2;
ob1.oblog.next;

# 查询oblog_db2在回收站的name
let $db2_recycle_dbname2 = query_get_value(select OBJECT_NAME from oceanbase.__all_recyclebin order by gmt_create desc limit 1, OBJECT_NAME, 1);
set @__oceanbase_ddl_id = '1_1_12345678906';
eval purge database $db2_recycle_dbname2;
ob1.oblog.next;

# 2. table相关
use oblog_db1;
set @__oceanbase_ddl_id = '1_1_12345678907';
create table t1 (pk int primary key);
ob1.oblog.next;
set @__oceanbase_ddl_id = '1_1_12345678908';
create table t2 (pk int primary key);
ob1.oblog.next;

## 开启回收站: drop table -> flashback table -> purge table
# 2.1 开启回收站drop table
set @__oceanbase_ddl_id = '1_1_12345678909';
drop table t2;
ob1.oblog.next;

# 2.2 FLASHBACK table
# 查询t2在回收站的name
let $t2_recycle_table_name1 = __recycle_\$_1_1_12345678909;
set @__oceanbase_ddl_id = '1_1_12345678910';
eval FLASHBACK TABLE $t2_recycle_table_name1 TO BEFORE DROP;
ob1.oblog.next;

# 验证数据是否能够迭代到
replace into t2 (pk) values (1), (20), (300);
ob1.oblog.next;

# 2.3 purge table
set @__oceanbase_ddl_id = '1_1_12345678911';
drop table t2;
ob1.oblog.next;

# 查询t2在回收站的name
let $t2_recycle_table_name2 = query_get_value(select OBJECT_NAME from oceanbase.__all_recyclebin order by gmt_create desc limit 1, OBJECT_NAME, 1);
set @__oceanbase_ddl_id = '1_1_12345678912';
eval purge table $t2_recycle_table_name2;
ob1.oblog.next;

# 3. view
set @__oceanbase_ddl_id = '1_1_12345678913';
create table t3(pk int primary key, c1 int, c2 int);
ob1.oblog.next;
set @__oceanbase_ddl_id = '1_1_12345678914';
create view t3_view_1 (c1) as select c1 from t3;
ob1.oblog.next;
set @__oceanbase_ddl_id = '1_1_12345678915';
create view t3_view_2 (c1) as select c1 from t3;
ob1.oblog.next;

## 开启回收站, 操作如下: 开启回收站drop view -> flashback view -> purge view
# 3.1 开启回收站drop view
#set recyclebin=on;
set @__oceanbase_ddl_id = '1_1_12345678916';
drop view t3_view_2;
ob1.oblog.next;

# 3.2 FLASHBACK view
# 查询view在回收站的name
let $t3_view_recycle_name1 = __recycle_\$_1_1_12345678916;
set @__oceanbase_ddl_id = '1_1_12345678917';
eval FLASHBACK TABLE $t3_view_recycle_name1 TO BEFORE DROP;
ob1.oblog.next;

# 3.3 purge view
set @__oceanbase_ddl_id = '1_1_12345678918';
drop view t3_view_2;
ob1.oblog.next;

let $t3_view_recycle_name2 = query_get_value(select OBJECT_NAME from oceanbase.__all_recyclebin order by gmt_create desc limit 1, OBJECT_NAME, 1);
set @__oceanbase_ddl_id = '1_1_12345678919';
eval purge table $t3_view_recycle_name2;
ob1.oblog.next;

# 4. truncate table
set @__oceanbase_ddl_id = '1_1_12345678920';
create table t4(pk int primary key, c1 int, c2 int);
ob1.oblog.next;

set @__oceanbase_ddl_id = '1_1_12345678921';
create table t5(pk int primary key, c1 int, c2 int);
ob1.oblog.next;

# 4.2 开启回收站truncate
set @__oceanbase_ddl_id = '1_1_12345678922';
truncate table t5;
ob1.oblog.next;

# 查询table在回收站的name
let $t5_recycle_name = __recycle_\$_1_1_12345678922;
set @__oceanbase_ddl_id = '1_1_12345678923';
eval FLASHBACK TABLE $t5_recycle_name TO BEFORE DROP rename to t6;
ob1.oblog.next;

# 验证数据是否能够迭代到
replace into t6 (pk, c1, c2) values (1, 2, 3), (20, 30, 40), (300, 400, 500);
ob1.oblog.next;

# 5. 关闭回收站
# 连接sys租户
connection oblog_conn_sys;
set recyclebin=off;
# 5.1 关闭回收站drop table
use oblog_db1;
drop table t1;
ob1.oblog.next;

# 5.2 关闭回收站drop view
drop view t3_view_1;
ob1.oblog.next;

# 5.3 关闭回收站truncate
truncate table t4;
ob1.oblog.next;

# 5.4 关闭回收站drop database
drop database oblog_db1;
ob1.oblog.next;

# 停止oblog
ob1.oblog.stop;

# 清理数据
--disable_warnings
drop database if exists oblog_db1;
drop database if exists oblog_db2;

echo "[CASE_END] [recycle_bin_ddl.inc]";
