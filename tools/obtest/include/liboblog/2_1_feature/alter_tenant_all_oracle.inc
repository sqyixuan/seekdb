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

ob_connect(oblog_conn_sys, ob1.proxy0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection oblog_conn_sys;
alter system set enable_sys_table_ddl = true;
set ob_query_timeout=1000000000;
set ob_trx_timeout=1000000000;

# 创建多个租户
# oracle租户
create resource unit t_oracle_box max_cpu 2, max_memory=2000000000, max_iops 10000, max_disk_size 5000000000, max_session_num 9223372036854775807, MIN_CPU=1, MIN_MEMORY=2000000000, MIN_IOPS=5000;
create resource pool t_oracle_pool unit = 't_oracle_box', unit_num = 1, zone_list = ('z1');
create tenant t_oracle replica_num = 1, primary_zone='z1', resource_pool_list=('t_oracle_pool') set ob_tcp_invited_nodes='%',ob_compatibility_mode='oracle';
ob1.mysql alter tenant t_oracle set variables autocommit='on';

sleep 2;
ob_connect(conn_t_oracle, ob1, SYS@t_oracle, ,SYS, , USE_OB_CONNECTOR);

################### case 1:  验证Oracle租户
connection conn_t_oracle;
create user obtest IDENTIFIED BY "obtest";
grant all on *.* to obtest WITH GRANT OPTION;
ob_connect(conn_t_oracle_obtest,ob1,OBTEST@t_oracle,obtest,obtest,,USE_OB_CONNECTOR);

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
# 等一会，保证刷新
sleep 2;
alter tenant all set progressive_merge_num = 50;

# 因为租户下两张表，能收到两条ALTER TABLE
ob1.oblog.next;
inc $COUNT;
ob1.oblog.next;
inc $COUNT;

connection conn_t_oracle_obtest;
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
drop tenant t_oracle;
drop resource pool t_oracle_pool;
drop resource unit t_oracle_box;
