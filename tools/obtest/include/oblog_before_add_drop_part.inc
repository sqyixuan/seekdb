# 升级前，创建分区

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

--disable_warnings
drop tenant if exists oblog_tt;
--disable_warnings
drop resource pool if exists oblog_pool;
--disable_warnings
drop resource unit if exists oblog_box;
--disable_warnings
drop database if exists oblog_db;

# create tenant, 创建oblog_tt租户
create resource unit oblog_box max_cpu 2, max_memory=2000000000, max_iops 10000, max_disk_size 5000000000, max_session_num 9223372036854775807, MIN_CPU=1, MIN_MEMORY=2000000000, MIN_IOPS=5000;
create resource pool oblog_pool unit = 'oblog_box', unit_num = 1, zone_list = ('z1');
create tenant oblog_tt replica_num = 1, primary_zone='z1', resource_pool_list=('oblog_pool');

--disable_warnings
--disable_query_log
--disable_result_log
sleep 5;
--error 0,1064
alter tenant yts_tt set variables ob_tcp_invited_nodes='%';
--enable_query_log
--enable_result_log

# 在oblog_tt租户下操作
connect (oblog_conn_tt,$ip,root@oblog_tt,,oceanbase,$port);
connection oblog_conn_tt;

# 创建database
create database oblog_db;
use oblog_db;
sleep 2;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=oblog_tt.*.*;
ob1.oblog.restart;

# 插入数据，保证收到
drop table if exists t1;
drop table if exists t2;
# ob1.oblog.next_dml;

set ob_org_cluster_id=1234;

create table t1(c1 int, c2 int, primary key(c1,c2)) partition by range(c1) (partition p0 values less than(10), partition p1 values less than(20));
ob1.oblog.next;
insert into t1 values (1, 1);
ob1.oblog.next;
insert into t1 values (11, 1);
ob1.oblog.next;

create table t2(c1 int, c2 int, primary key(c1,c2)) partition by range(c1) subpartition by hash(c2) subpartitions 3 (partition p0 values less than(10), partition p1 values less than(20));
ob1.oblog.next;
insert into t2 values (1, 1);
ob1.oblog.next;
insert into t2 values (11, 1);
ob1.oblog.next;
