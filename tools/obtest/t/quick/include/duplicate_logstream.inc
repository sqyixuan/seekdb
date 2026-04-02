#############################################
#owner : jingyu.cr
#owner group: rs
#description: quick test
# 测试点：验证广播日志流
#############################################
connection conn_sys;
let $z3_dst_ip2 = deploy_get_value(ob1.z3.obs2.ip);
let $z3_dst_port2 = deploy_get_value(ob1.z3.obs2.port);
let $z3_dst2 = $z3_dst_ip2:$z3_dst_port2;
--enable_query_log
let $mysql_tenant_id= deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'mysql', tenant_id, 1);
let $mysql_meta_tenant_id = math($mysql_tenant_id - 1);

# add server in zone3
--echo =================================================================
--echo                      add server in zone3
--echo =================================================================
SELECT COUNT(*) as server_count FROM DBA_OB_SERVERS WHERE STATUS = "ACTIVE";
SELECT COUNT(*) as server_count FROM GV$OB_SERVERS;
ob1.z3.obs2.set extra_config=$extra_conf;
ob1.z3.obs2.set status=enable;
ob1.z3.obs2.start;
let $tenant_cnt = 3;
let $server_cnt = 7;
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where (a.refreshed_schema_version != b.refreshed_schema_version or a.refreshed_schema_version % 8 != 0) and a.tenant_id = 1, 0, 120;
ob1.z3.obs2.check_log_until_success -c "weak read service thread start";
ob1.z3.obs2.add;
ob1.check_until_timeout SELECT COUNT(*) c FROM DBA_OB_SERVERS WHERE START_SERVICE_TIME > 0, 7, 200;
set @@session.ob_query_timeout = 100000000000;
SELECT COUNT(*) as server_count FROM DBA_OB_SERVERS WHERE STATUS = "ACTIVE";
ob1.check_until_timeout SELECT COUNT(*) as server_count FROM GV\$OB_SERVERS, 7, 8;
ob1.check_until_timeout select IF(count(*) = $tenant_cnt * $server_cnt, 1, 0) as done from oceanbase.__all_virtual_server_schema_info, 1, 120;
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where (a.refreshed_schema_version != b.refreshed_schema_version or a.refreshed_schema_version % 8 != 0) and a.tenant_id <= $mysql_tenant_id, 0, 60;

--echo =================================================================
--echo                    alter unit_num from 1 to 2
--echo =================================================================
# alter unit_num from 1 to 2
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status = "ACTIVE" group by zone order by zone;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id - 1 and status = "ACTIVE" group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id and zone in ("z1", "z2", "z3") and status = "NORMAL" group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id - 1 and zone in ("z1", "z2", "z3") and status = "NORMAL" group by zone order by zone;
eval ALTER RESOURCE TENANT mysql UNIT_NUM = 2;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_ls_status where tenant_id = $mysql_tenant_id and status = 'NORMAL', 5, 50;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status = "ACTIVE" group by zone order by zone;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id - 1 and status = "ACTIVE" group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id and zone in ("z1", "z2", "z3") and status = "NORMAL" group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id - 1 and zone in ("z1", "z2", "z3") and status = "NORMAL" group by zone order by zone;

--echo =================================================================
--echo         create/alter duplicate table under sys/meta tenant
--echo =================================================================
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;
--error 1235
# can not create duplicate table under sys tenant
create table sys_dup_t1(c1 int) duplicate_scope = 'cluster';
# can not alter duplicate_scope under other tenant
--error 1235
alter table oceanbase.__all_tablegroup duplicate_scope = 'cluster';

# can not create duplicate table under meta tenant
eval alter system change tenant tenant_id = $mysql_meta_tenant_id;
--error 1235
create table sys_dup_t1(c1 int) duplicate_scope = 'cluster';
--error 1235
alter table oceanbase.__all_tablegroup duplicate_scope = 'cluster';

--echo =================================================================
--echo   create/alter duplicate table dup_t1 and dup_pt1 and dup_from_t1
--echo =================================================================
ob_connect(conn_mysql, ob1.z1.obs1, root@mysql, , test, , USE_MYSQL_CONNECTOR);
connection conn_mysql;
create tablegroup tg1;
create table table_in_tg(c1 int) tablegroup = 'tg1';
--error 1235
# can not create duplicate table with tablegroup
create table table_with_tg(c1 int) duplicate_scope = 'cluster', tablegroup = 'tg1';
# test create duplicate table
create table dup_t1(c1 int) duplicate_scope = 'cluster';
eval insert into dup_t1 value (1), (2), (3), (4), (5);
select * from dup_t1 order by c1 asc;
--error 1235
# can not add duplicate table to tablegroup
alter tablegroup tg1 add table dup_t1;
--error 1235
# can not change duplicate table's tablegroup
alter table dup_t1 set tablegroup = 'tg1';
# can change table duplicate_scope
alter table dup_t1 set duplicate_scope = 'none';
alter table dup_t1 set duplicate_scope = 'cluster';
# can not change duplicate_scope of table in tablegroup
--error 1235
alter table table_in_tg set duplicate_scope = 'cluster';
# can not change duplicate_scope of sys table
--error 1044
alter table oceanbase.__all_tablegroup duplicate_scope = 'cluster';
connection conn_mysql;
# test create duplicate table with partitions
eval create table dup_pt1(c1 int) duplicate_scope = 'CLUSTER' partition by hash(c1) partitions 4;
eval insert into dup_pt1 value (1), (2), (3), (4), (5);
select * from dup_pt1 order by c1 asc;
# test create table like duplicate table
eval create table dup_from_t1 like dup_t1;
eval insert into dup_from_t1 value (1), (2), (3), (4), (5);
select * from dup_from_t1 order by c1 asc;
# create normal t1 normal_t1 to normal_t5
eval create table normal_t1(c1 int);
eval create table normal_t2(c1 int);
eval create table normal_t3(c1 int);
eval create table normal_t4(c1 int);
eval create table normal_t5(c1 int);


--echo =================================================================
--echo                  check duplicate log stream
--echo =================================================================
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;
ob1.check_until_timeout select count(*) as dup_ls_cnt from oceanbase.CDB_OB_LS where flag like '%duplicate%', 1, 20;
let $dup_ls_id = deploy_get_value(ob1.select select ls_id as dup_ls_id from oceanbase.CDB_OB_LS where flag like '%duplicate%', dup_ls_id, 1);
# check table's duplicate scope is cluster
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name = "dup_t1" and duplicate_scope = 1, 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name = "dup_pt1" and duplicate_scope = 1, 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name = "dup_from_t1" and duplicate_scope = 1, 1, 10;
# check duplicate table is on duplicate log stream
ob1.check_until_timeout select ls_id from oceanbase.CDB_OB_TABLE_LOCATIONS where table_name = "dup_t1" and role = "LEADER" and duplicate_scope = "CLUSTER", $dup_ls_id, 1;
ob1.check_until_timeout select ls_id from oceanbase.CDB_OB_TABLE_LOCATIONS where table_name = "dup_pt1" and role = "LEADER" and duplicate_scope = "CLUSTER", $dup_ls_id, 1;
ob1.check_until_timeout select ls_id from oceanbase.CDB_OB_TABLE_LOCATIONS where table_name = "dup_from_t1" and role = "LEADER" and duplicate_scope = "CLUSTER", $dup_ls_id, 1;
# check duplicate log stream's distribution
ob1.check_until_timeout select count(*) as cnt from oceanbase.CDB_OB_TABLE_LOCATIONS where table_name like "%normal_t%" and role = "LEADER" and ls_id = $dup_ls_id, 0, 1;
ob1.check_until_timeout select count(*) as cnt from oceanbase.CDB_OB_LS_LOCATIONS where tenant_id = $mysql_tenant_id and ls_id = $dup_ls_id and role = "LEADER" and (member_list like "%$z1_dst0%" or member_list like "%$z1_dst1%"), 1, 10;
ob1.check_until_timeout select count(*) as cnt from oceanbase.CDB_OB_LS_LOCATIONS where tenant_id = $mysql_tenant_id and ls_id = $dup_ls_id and role = "LEADER" and (member_list like "%$z3_dst0%" or member_list like "%$z3_dst1%" or member_list like "%$z3_dst2%"), 1, 10;
ob1.check_until_timeout select count(*) as cnt from oceanbase.CDB_OB_LS_LOCATIONS where tenant_id = $mysql_tenant_id and ls_id = $dup_ls_id and role = "LEADER" and (learner_list like "%$z1_dst0%" or learner_list like "%$z1_dst1%"), 1, 10;
ob1.check_until_timeout select count(*) as cnt from oceanbase.CDB_OB_LS_LOCATIONS where tenant_id = $mysql_tenant_id and ls_id = $dup_ls_id and role = "LEADER" and (learner_list like "%$z3_dst0%" or learner_list like "%$z3_dst1%" or learner_list like "%$z3_dst2%"), 1, 10;
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_log_stat where tenant_id = $mysql_tenant_id and ls_id = $dup_ls_id, 4, 10;

--echo =================================================================
--echo                     test create index table
--echo =================================================================
ob_connect(conn_mysql, ob1.z1.obs0, root@mysql, , test, , USE_MYSQL_CONNECTOR);
connection conn_mysql;
# create local and global index on duplicate table
create index dup_t1_idx1 on dup_t1(c1) global;
create index dup_t1_idx2 on dup_t1(c1) local;
create index normal_t1_idx1 on normal_t1(c1) global;
create index normal_t1_idx2 on normal_t1(c1) local;

# check table's duplicate scope is expected
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_TABLE_LOCATIONS where index_name like "%dup_t1_idx1%" and duplicate_scope = "CLUSTER" and role like "%leader%", 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_TABLE_LOCATIONS where index_name like "%dup_t1_idx2%" and duplicate_scope = "CLUSTER" and role like "%leader%", 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_TABLE_LOCATIONS where index_name like "%normal_t1_idx1%" and duplicate_scope = "NONE" and role like "%leader%", 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_TABLE_LOCATIONS where index_name like "%normal_t1_idx2%" and duplicate_scope = "NONE" and role like "%leader%", 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name like "%dup_t1_idx1%" and duplicate_scope = 1, 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name like "%dup_t1_idx2%" and duplicate_scope = 1, 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name like "%normal_t1_idx1%" and duplicate_scope = 0, 1, 10;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_table where table_name like "%normal_t1_idx2%" and duplicate_scope = 0, 1, 10;

--echo =================================================================
--echo                test duplicate log stream is readable
--echo =================================================================
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;
ob1.check_until_timeout select count(distinct svr_ip, svr_port) as cnt from oceanbase.__all_virtual_dup_ls_tablets, 4, 10;
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_dup_ls_tablets where attribute != "READABLE", 0, 10;
