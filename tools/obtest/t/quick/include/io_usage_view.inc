#############################################
#owner : wyh329796
#owner group: rs
#description: quick test
# 测试点：验证计费视图, 在共享存储模式下有数据
#############################################

ob_connect(conn_mysql, ob1.z1.obs1, root@mysql, , test, , USE_MYSQL_CONNECTOR);

connection conn_sys;
--disable_query_log
--disable_warnings
drop tenant if exists oracle_tenant1 force;
DROP RESOURCE POOL IF EXISTS oracle_pool1;
DROP RESOURCE UNIT IF EXISTS oracle_unit1;
--enable_warnings

let $cpu = 1;
let $memory = '3G';

eval CREATE RESOURCE UNIT oracle_unit1 MAX_CPU=$cpu, MEMORY_SIZE=$memory;
eval CREATE RESOURCE POOL oracle_pool1 UNIT='oracle_unit1', UNIT_NUM=1;
CREATE TENANT oracle_tenant1 REPLICA_NUM = 3, RESOURCE_POOL_LIST = ('oracle_pool1') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
alter tenant oracle_tenant1 set variables ob_tcp_invited_nodes='%';

ob_connect (conn_oracle, ob1.z1.obs1, sys@oracle_tenant1, ,sys, USE_MYSQL_CONNECTOR);

connection conn_mysql;
use test;
create table test_mysql_io_usage (id int);
insert into test_mysql_io_usage values(1), (2), (3), (4), (5);
commit;

connection conn_oracle;
create table test_oracle_io_usage (id int);
insert into test_oracle_io_usage values(1), (2), (3), (4), (5);
commit;

connection conn_sys;
create table test_sys_io_usage (id int);
insert into test_sys_io_usage values(1), (2), (3), (4), (5);
commit;

# major freeze
connection conn_sys;
# major freeze all tenant
--disable_warnings
alter system major freeze tenant = sys;
alter system major freeze tenant = all_user;
alter system major freeze tenant = all_meta;
--enable_warnings

alter system set ob_compaction_schedule_interval = '10s' tenant sys;
alter system set ob_compaction_schedule_interval = '10s' tenant all_user;
alter system set ob_compaction_schedule_interval = '10s' tenant all_meta;

let $__i__= 1200;
sleep 3;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__frozen_scn__ = query_get_value(select /*+query_timeout(100000000)*/sum(frozen_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    let $__merged_version__ = query_get_value(select /*+query_timeout(100000000)*/sum(last_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    if($__frozen_scn__ == $__merged_version__)
    {
        let $__i__ = -5;
    }
}

if($__i__ != -5)
{
    --echo major freeze failed
    select * from oceanbase.CDB_OB_MAJOR_COMPACTION;
    let $__tenant_id__ = query_get_value(select tenant_id from oceanbase.CDB_OB_MAJOR_COMPACTION where last_scn != frozen_scn, tenant_id, 1);
    let $__frozen_scn__ = query_get_value(select frozen_scn from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $__tenant_id__, frozen_scn, 1);
    eval select * from oceanbase.__all_virtual_server_compaction_event_history where tenant_id = $__tenant_id__ and compaction_scn = $__frozen_scn__;
    select * from oceanbase.__all_virtual_compaction_diagnose_info;
}
sleep 2;

connection conn_sys;
select count(*) >= 4 from oceanbase.CDB_OB_STORAGE_IO_USAGE;
select count(*) >= 2 from oceanbase.DBA_OB_STORAGE_IO_USAGE;

connection conn_mysql;
use oceanbase;
select count(*) >= 2 from oceanbase.DBA_OB_STORAGE_IO_USAGE;

connection conn_oracle;
SELECT 
  CASE 
    WHEN (SELECT COUNT(*) FROM DBA_OB_STORAGE_IO_USAGE) > 1 THEN 1
    ELSE 0 
  END AS result
FROM dual;

connection conn_mysql;
use test;
drop table test_mysql_io_usage;
connection conn_oracle;
drop table test_oracle_io_usage;
connection conn_sys;
drop table test_sys_io_usage;

--enable_query_log