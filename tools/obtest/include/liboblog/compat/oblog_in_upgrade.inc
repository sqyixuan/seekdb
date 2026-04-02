# 连接obtest租户
connection conn_obcdc_z2;
use oblog_db;

# 插入数据，保证收到
set ob_org_cluster_id=1234;
replace into oblog_test values (1, 1), (2,2), (3,3);
ob1.oblog.next_dml;
inc $OBLOG_DML_COUNT;

# sys tenant, 继续执行升级
connection conn_sys;
