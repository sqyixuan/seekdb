# sys tenant
#connection conn_sys;

# obtest租户
connection conn_obtest;
let $start_tstamp_before_upgrade = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);

create database oblog_db;
use oblog_db;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=mysql.*.*;
ob1.oblog.restart;

# 插入数据，保证收到
drop table if exists oblog_test;
create table oblog_test (pk int primary key, c1 varchar(100));

let $OBLOG_DML_COUNT = 0;

set ob_org_cluster_id=1234;
replace into oblog_test values (1, 1), (2,2), (3,3);
ob1.oblog.next_dml;
inc $OBLOG_DML_COUNT;

# sys tenant, 继续执行升级
connection conn_sys;
