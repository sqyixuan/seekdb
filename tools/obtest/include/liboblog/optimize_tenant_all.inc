#
# owner:修铭
# owner group:liboblog
# description:
# 测试点:
#     sys租户：optimize tenant all
#     mysql: optimize table
#     oracle: alter table shrink table
##

ob_connect(oblog_conn_sys, ob1.proxy0, root@sys, ,oceanbase, , USE_MYSQL_CONNECTOR);
connection oblog_conn_sys;

# 创建多个租户
# oracle租户
create resource unit t_oracle_box max_cpu 2, memory_size='10G';
create resource pool t_oracle_pool unit = 't_oracle_box', unit_num = 1, zone_list = ('z1');
create tenant t_oracle replica_num = 1, primary_zone='z1', resource_pool_list=('t_oracle_pool') set ob_tcp_invited_nodes='%',ob_compatibility_mode='oracle';
ob1.mysql alter tenant t_oracle set variables autocommit='on';

# mysql租户
create resource unit t_mysql_box max_cpu 2, memory_size='10G';
create resource pool t_mysql_pool unit = 't_mysql_box', unit_num = 1, zone_list = ('z1');
create tenant t_mysql replica_num = 1, primary_zone='z1', resource_pool_list=('t_mysql_pool') set ob_tcp_invited_nodes='%',ob_compatibility_mode='mysql';

sleep 2;
ob_connect(conn_t_oracle, ob1.z1.obs0, SYS@t_oracle, ,SYS, , USE_OB_CONNECTOR);
ob_connect(conn_t_mysql, ob1.z1.obs0, root@t_mysql, , oceanbase, , USE_OB_CONNECTOR);

################### case 1:  验证Oracle租户
connection conn_t_oracle;
create user obtest IDENTIFIED BY "obtest";
grant all on *.* to obtest WITH GRANT OPTION;
grant dba to obtest;
ob_connect(conn_t_oracle_obtest,ob1.z1.obs0,OBTEST@t_oracle,obtest,obtest,,USE_OB_CONNECTOR);

connection oblog_conn_sys;
let $start_tstamp_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
ob1.oblog.gen_conf tb_white_list=t_oracle.*.*;
ob1.oblog.restart;

let $COUNT = 0;
connection conn_t_oracle_obtest;

create table t1 (pk int primary key, c1 varchar(100));
ob1.oblog.next;
inc $COUNT;

create table t2 (pk int primary key, c1 varchar(100));
ob1.oblog.next;
inc $COUNT;

begin;
insert into t1 values (1, 'a'), (2, 'aa'), (3, 'aaa');
insert into t2 values (1, 'a'), (2, 'aa'), (3, 'aaa');
commit;
ob1.oblog.next;
inc $COUNT;

###### 执行alter tenant
# 在sys租户下操作
connection oblog_conn_sys;
set ob_query_timeout=10000000000;
set ob_trx_timeout=10000000000;
# sleep一会，保证配置项刷到
sleep 1;
optimize tenant all;

# 因为租户下两张表，能收到两条DDL
# 这里应该能收到 alter table shrink space; 除非有BUG
ob1.oblog.next;
inc $COUNT;
ob1.oblog.next;
inc $COUNT;

connection conn_t_oracle_obtest;

# 针对单个表执行
alter table t1 shrink space;
ob1.oblog.next;
inc $COUNT;
alter table t2 shrink space;
ob1.oblog.next;
inc $COUNT;

delete from t1;
ob1.oblog.next;
inc $COUNT;

ob1.oblog.stop;

# 从旧点回拉
ob1.oblog.set_start_tstamp_usec $start_tstamp_usec;
ob1.oblog.restart;

let $cnt = 0;
while ($cnt < $COUNT)
{
  ob1.oblog.next;
  inc $cnt;
}
ob1.oblog.stop;


################### case 2:  验证mysql租户
connection conn_t_mysql;
create database oblog_db;
use oblog_db;

let $start_tstamp_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
ob1.oblog.gen_conf tb_white_list=t_mysql.*.*;
ob1.oblog.restart;

let $COUNT = 0;

create table t1 (pk int primary key, c1 varchar(100));
ob1.oblog.next;
inc $COUNT;

create table t2 (pk int primary key, c1 varchar(100));
ob1.oblog.next;
inc $COUNT;

begin;
insert into t1 values (1, 'a'), (2, 'aa'), (3, 'aaa');
insert into t2 values (1, 'a'), (2, 'aa'), (3, 'aaa');
commit;
ob1.oblog.next;
inc $COUNT;

###### 执行alter tenant
# 在sys租户下操作
connection oblog_conn_sys;
alter system set enable_sys_table_ddl = true;
set ob_query_timeout=10000000000;
set ob_trx_timeout=10000000000;
# sleep一会，保证配置项刷到
sleep 1;
optimize tenant all;

# 因为租户下两张表，能收到两条ALTER TABLE
ob1.oblog.next;
inc $COUNT;
ob1.oblog.next;
inc $COUNT;

connection conn_t_mysql;
# 针对单张表操作
optimize table t1;
ob1.oblog.next;
inc $COUNT;

optimize table t2;
ob1.oblog.next;
inc $COUNT;

delete from t1;
ob1.oblog.next;
inc $COUNT;

ob1.oblog.stop;

# 从旧点回拉
ob1.oblog.set_start_tstamp_usec $start_tstamp_usec;
ob1.oblog.restart;

let $cnt = 0;
while ($cnt < $COUNT)
{
  ob1.oblog.next;
  inc $cnt;
}
ob1.oblog.stop;

# 删除租户及相关资源
connection oblog_conn_sys;
drop tenant t_oracle force;
drop tenant t_mysql force;
drop resource pool t_oracle_pool;
drop resource pool t_mysql_pool;
drop resource unit t_oracle_box;
drop resource unit t_mysql_box;
