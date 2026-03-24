#
# owner:修铭
# owner group:liboblog
# description:
# 测试点:
#     alter tenant <ALL|tenant_name> set progressive_merge_num = <num>
#     这条DDL会影响所有租户的所有表的progressive_merge_num属性，我们这里要验证的是这条DDL是否会影响schema。
#
#     BUG：
#     它可能会误改普通租户系统表的属性，导致schema不能fallback。
#
#     下面验证多个租户情况下，执行这条DDL，然后回拉，看schema fallback是否成功
#
# 注意：这条DDL实现上是会为每个表生成一个ALTER TABLE语句，然后单独执行各个DDL，整体并不在一个事务内。
#       因此，并不会同步到对应的一条alter tenant DDL，而是多条ALTER TABLE DDL
#
##
echo "[CASE_BEGIN] [alter_tenant_all_mysql.inc]";

ob_connect(oblog_conn_sys, ob1.proxy0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection oblog_conn_sys;
set ob_query_timeout=1000000000;
set ob_trx_timeout=1000000000;
alter system set enable_sys_table_ddl = true;

# mysql租户
create resource unit t_mysql_box max_cpu 2, memory_size='10G';
create resource pool t_mysql_pool unit = 't_mysql_box', unit_num = 1, zone_list = ('z1');
create tenant t_mysql replica_num = 1, primary_zone='z1', resource_pool_list=('t_mysql_pool') set ob_tcp_invited_nodes='%',ob_compatibility_mode='mysql';

sleep 2;
ob_connect(conn_t_mysql, ob1.z1.obs0, root@t_mysql, , oceanbase, , USE_OB_CONNECTOR);

connection conn_t_mysql;
set autocommit=1;
create database oblog_db;
use oblog_db;

let $start_tstamp_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
ob1.oblog.gen_conf tb_white_list=t_mysql.*.* sort_trans_participants=1;
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

# alter tenant all set progressive_merge_num = 50 不能操作回收站的table, 先清理
connection conn_obcdc_mysql;
purge recyclebin;

###### 执行alter tenant
# 在sys租户下操作
connection oblog_conn_sys;
# 等一会，保证配置项刷新
sleep 2;
purge recyclebin;
select count(*) from __all_virtual_table;
alter tenant all set progressive_merge_num = 50;

# 因为租户下两张表，能收到两条ALTER TABLE
ob1.oblog.next;
inc $COUNT;
ob1.oblog.next;
inc $COUNT;

connection conn_t_mysql;
delete from t1;
ob1.oblog.next;
inc $COUNT;

ob1.oblog.stop;

# 从旧点回拉
ob1.oblog.gen_conf tb_white_list=t_mysql.*.* sort_trans_participants=1;
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
drop tenant t_mysql;
if ($is_schema_split_mode == $SCHEMA_SPLIT_MODE)
{
  drop tenant t_mysql force;
}
drop resource pool t_mysql_pool;
drop resource unit t_mysql_box;
echo "[CASE_END] [alter_tenant_all_mysql.inc]";
