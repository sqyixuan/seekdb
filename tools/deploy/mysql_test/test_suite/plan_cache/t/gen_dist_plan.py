# -*- coding: utf-8 -*-
# USE `python gen_dist_plan.py old` to generate test case for old parallel framework
# USE `python gen_dist_plan.py new` to generate test case for new parallel framework
import sys
import re;

DEBUG = 0

FILE_HEADER = """
#owner: zongmei.zzm
#owner group: sql1
##
## Test Name: plan_cache_dist_plan
##
## Scope: Test plan cache for dist plans
##
## INFO:  if you want to add case; you should read ../README.md;
#         don't use sys tenant, because when parallel run on obfarm, the hit_count can't be expected;
#         so we use new tenant, new plan cache will be  used for this case only;
##
##
## Date: 2018-8-21
##
"""

TEST_SET = """
--disable_info
--disable_metadata
--disable_abort_on_error

--disable_query_log
--error 0,5157
drop tenant xy_tenant_1 force;

alter system set resource_hard_limit = 500;

eval create resource unit if not exists xy_box_1 max_cpu 5, memory_size = 1500000000;
let $zone_name=query_get_value(select zone from oceanbase.__all_zone where zone != '' limit 1, zone, 1);
let $obs_num = query_get_value(select count(1) as cnt from oceanbase.__all_server group by zone asc limit 1,cnt, 1);
eval create resource pool if not exists xy_pool_1 unit = 'xy_box_1', unit_num = $obs_num;
eval create tenant if not exists xy_tenant_1 primary_zone='$zone_name', resource_pool_list('xy_pool_1') set ob_tcp_invited_nodes='%';

--enable_query_log
--sleep 3

connect (conn1,$OBMYSQL_MS0,root@xy_tenant_1,,*NO-ONE*,$OBMYSQL_PORT);
connect (conn_admin,$OBMYSQL_MS0,admin@sys,admin,*NO-ONE*,$OBMYSQL_PORT);

connection conn_admin;

alter system flush plan cache;
alter system set large_query_threshold = '1s';
alter system set enable_auto_leader_switch=false;

--sleep 3
--enable_query_log
--enable_result_log
connection conn1;
select effective_tenant();
--disable_query_log
--disable_result_log

--disable_warnings
drop database if exists xy_db;
--enable_warnings
create database xy_db;
use xy_db;
"""

# for obtest setup
TEST_SET2 = """
ob1=OBI(cluster=1:1, proxy_num=0, init_unit_cpu=0);
ob1.reboot;

ob1.mysql alter system set large_query_threshold = '1s';
ob1.mysql alter system set enable_auto_leader_switch=false;
ob1.mysql alter system set resource_hard_limit = 500;
--disable_info
--disable_metadata
--disable_abort_on_error

--disable_query_log
--error 0,5157
ob1.mysql drop tenant if exists xy_tenant_1 force;

let $cpu = 4;
let $memory = 1500000000;
let $min_memory = 1073741824;
let $disk = 536870912;
let $iops = 100000;
let $session = 64;

ob1.mysql create resource unit if not exists xy_box_1 max_cpu 5, memory_size 1500000000;
let $zone_name=deploy_get_value(ob1.select select zone from oceanbase.__all_zone where zone != '' limit 1, zone, 1);
let $obs_num = deploy_get_value(ob1.select select count(1) as cnt from oceanbase.__all_server group by zone asc limit 1,cnt, 1);
ob1.mysql create resource pool if not exists xy_pool_1 unit = 'xy_box_1', unit_num = $obs_num;
ob1.mysql create tenant if not exists xy_tenant_1 primary_zone='$zone_name', resource_pool_list('xy_pool_1') set ob_tcp_invited_nodes='%';

--enable_query_log
--sleep 3

ob_connect(conn1, ob1.z1.obs0, root@xy_tenant_1,,test,USE_OB_CONNECTOR);
ob_connect(conn_admin, ob1.z1.obs0, root,,test, USE_OB_CONNECTOR);

connection conn_admin;

alter system flush plan cache;

--sleep 3
--enable_query_log
--enable_result_log
connection conn1;
select effective_tenant();
--disable_query_log
--disable_result_log

--disable_warnings
drop database if exists xy_db;
--enable_warnings
create database xy_db;
use xy_db;
"""

TEST_CLEANUP = """
--echo ****************************** cleanup ******************************
--disable_warnings
drop database if exists xy_db;
--enable_warnings
# using admin acount
connection conn_admin;

alter system set large_query_threshold = '100ms';
alter system set enable_auto_leader_switch = true;
drop tenant xy_tenant_1 force;
drop resource pool xy_pool_1;
drop resource unit xy_box_1;
"""

def get_table_metas(**kvargs):
    if DEBUG:
        print """
--echo ************************ get table metas ************************
"""
    print "connection conn_admin;\n"
    case_num = kvargs["case_num"]
    tbl_num = kvargs["tbl_num"]
    tbl_prefix = kvargs["tbl_prefix"]
    part_num = kvargs["part_num"]
    svr_num = kvargs["svr_num"]
    
    tbl_id_fmt = "let $case%d_tbl_id_%d = \
query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = 'case%d_%s_%d', table_id, 1);"
    for i in range(1, tbl_num + 1):
        print tbl_id_fmt % (case_num, i, case_num, tbl_prefix, i)
    # get ips and ports
    tbl_main_ip_fmt = "let $case%d_tbl%d_part%d_main_ip = \
query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table \
where table_id = $case%d_tbl_id_%d and role = 1 order by partition_id, svr_ip, %d);"
    tbl_main_port_fmt = "let $case%d_tbl%d_part%d_main_port = \
query_get_value(select svr_port from oceanbase.__all_virtual_meta_table \
where table_id = $case%d_tbl_id_%d and role = 1 order by partition_id, svr_port, %d);"
    tbl_backup_ip_fmt = "let $case%d_tbl%d_part%d_backup_ip%d = \
query_get_value(select svr_ip from oceanbase.__all_virtual_meta_table \
where table_id = $case%d_tbl_id_%d and role = 2 and partition_id = %d, svr_ip, %d);"
    tbl_backup_port_fmt = "let $case%d_tbl%d_part%d_backup_port%d = \
query_get_value(select svr_port from oceanbase.__all_virtual_meta_table \
where table_id = $case%d_tbl_id_%d and role = 2 and partition_id = %d, svr_port, %d);"

    debug_fmt_head = "--echo ********************* table case%d_%s_%d $case%d_tbl_id_%d"
    debug_fmt_main = "--echo *********************   main $case%d_tbl%d_part%d_main_ip:\
$case%d_tbl%d_part%d_main_port"
    debug_fmt_backup = "--echo *********************   backup $case%d_tbl%d_part%d_backup_ip%d:\
$case%d_tbl%d_part%d_backup_port%d"

    for i in range (1, tbl_num + 1):
        for j in range(1, part_num + 1):
            print tbl_main_ip_fmt % (case_num, i, j, case_num, i, j)
            print tbl_main_port_fmt % (case_num, i, j, case_num, i, j)
            print "# backups"
            for k in range(1, svr_num):
                print tbl_backup_ip_fmt % (case_num, i, j, k, case_num, i, j-1, k)
                print tbl_backup_port_fmt % (case_num, i, j, k, case_num, i, j-1, k)
            print ""

    if DEBUG:
        for i in range(1, tbl_num + 1):
            print debug_fmt_head % (case_num, tbl_prefix, i, case_num, i)
            for j in range(1, part_num + 1):
                print debug_fmt_main % (case_num, i, j, case_num, i, j)
                for k in range(1, svr_num):
                    print debug_fmt_backup % (case_num, i, j, k, case_num, i, j, k)
            

def case1_setup():
    print """--echo **************************************** Case 1 Insert Dist Plans ****************************************
## case #1 insert dist plans
## insert only supports single table insertion, and does not support distributed insert select
"""
    print """
connection conn1;
--disable_warnings
drop table if exists case1_t_1;
--enable_warnings

--disable_query_log
--disable_result_log
create table case1_t_1 (pk1 int primary key, a1 int) partition by hash(pk1) partitions 2;
"""
    
    get_table_metas(case_num=1, tbl_num=1, tbl_prefix='t', part_num=2, svr_num=2)
    
    print """
connection conn1;
--disable_result_log
--enable_query_log
--echo
insert into case1_t_1 values (1, 1), (2, 2), (3, 3);
insert into case1_t_1 values (4, 4), (5, 5), (6, 6);
--echo
--enable_result_log
explain insert into case1_t_1 values (4, 4), (5, 5), (6, 6);

--enable_result_log
--echo
--echo // expect plan to be distributed
select type = 3 from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'insert into case1_t_1 values (?, ?), (?, ?), (?, ?)';
--echo
--echo // hit_count == 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'insert into case1_t_1 values (?, ?), (?, ?), (?, ?)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader, part0 in backup, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case1_tbl_id_1' server='$case1_tbl1_part1_backup_ip1:$case1_tbl1_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
insert into case1_t_1 values (7, 7), (8, 8), (9, 9);
--echo
--enable_result_log
explain insert into case1_t_1 values (7, 7), (8, 8), (9, 9);
# All hit the same dist plan
--enable_result_log
--echo
--echo // hit the same plan
--echo // hit_count == 2
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'insert into case1_t_1 values (?, ?), (?, ?), (?, ?)';

--disable_query_log
drop table case1_t_1;
--echo
"""

def case2_setup():
    print """--echo **************************************** Case 2 Delete Dist Plans ****************************************
## case #2 
## delete distributed plan, delete now only supports single table deletion, but subqueries can have distributed queries
## For delete from t1 where pk in (select pk in t2), if the data to be deleted is cross-partition, then the partitions must be on the same server, otherwise an error will be reported

connection conn1;
--disable_warnings
--disable_query_log
drop table if exists case2_t_1, case2_t_2, case3_t_3;
--enable_warnings

--disable_query_log
--disable_result_log
"""
    create_tbl_fmt = "create table case2_t_%d (pk%d int primary key, a%d int) partition by hash(pk%d) partitions 2;";
    for i in range(1, 4):
        print create_tbl_fmt % (i, i, i, i);

    get_table_metas(case_num=2, tbl_num=3, tbl_prefix='t', part_num=2, svr_num=2)

    print "connection conn1;"
    for i in range(1, 10):
        print "insert into case2_t_1 values (%d, %d);" % (i, i)

    print """
--echo
--echo =============================== single table delete ===============================
--enable_query_log
--echo
delete from case2_t_1 where pk1 in (1, 2);
delete from case2_t_1 where pk1 in (2, 3);
--echo
--enable_result_log
explain delete from case2_t_1 where pk1 in (2, 3);

--echo
--echo // expect to be 1 plan in plan stat
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'delete from case2_t_1 where pk1 in (?, ?)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader, part0 in backup, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case2_tbl_id_1' server='$case2_tbl1_part1_backup_ip1:$case2_tbl1_part1_backup_port1';
--sleep 3

connection conn1;
--echo
--enable_query_log
delete from case2_t_1 where pk1 in (3, 4);
--echo
--enable_result_log
explain delete from case2_t_1 where pk1 in (3, 4);
--echo
--echo // hit the same plan, hit_count == 2
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'delete from case2_t_1 where pk1 in (?, ?)';
--sleep 3

--echo
--echo ============================ delete with subquery  ============================
--echo
--disable_query_log
--disable_result_log
insert into case2_t_2 values (1, 1), (2, 2);
insert into case2_t_3 values (3, 3), (4, 4);

--echo
--enable_query_log
delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
--enable_result_log
--echo
explain delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);

--echo
--echo // expect 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'delete from case2_t_2 where pk2 in (select pk3 from case2_t_3)';

connection conn_admin;
--disable_query_log
--disable_result_log
# Actually there is no deleted data, so it will not error after switching to the main
--echo 
--echo // switch replica leader for case2_t_2, part0 in backup, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case2_tbl_id_2' server = '$case2_tbl2_part1_backup_ip1:$case2_tbl2_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
--enable_result_log
--echo
explain delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);

--echo
--echo // expect 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'delete from case2_t_2 where pk2 in (select pk3 from case2_t_3)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case2_t_3, part0 in backup, part1 in main
--echo // now t3_part0 and t2_part0 in backup, t3_part1 and t2_part1 are in main
eval alter system switch replica leader partition_id = '0%2@$case2_tbl_id_3' server = '$case2_tbl3_part1_backup_ip1:$case2_tbl3_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
--enable_result_log
--echo
explain delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
--echo
--echo // hit the old plan, epect 2, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'delete from case2_t_2 where pk2 in (select pk3 from case2_t_3)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case2_t_2, part0 in backup, part1 in backup
--echo // now t2's partitions are all in backup
eval alter system switch replica leader partition_id = '1%2@$case2_tbl_id_2' server = '$case2_tbl2_part2_backup_ip1:$case2_tbl2_part2_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);
--echo
--enable_result_log
explain delete from case2_t_2 where pk2 in (select pk3 from case2_t_3);

--echo
--echo // expect 2, 2
--enable_result_log
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'delete from case2_t_2 where pk2 in (select pk3 from case2_t_3)';

--disable_query_log
drop table case2_t_1, case2_t_2, case2_t_3;
--echo
"""

def case3_setup():
    print """--echo **************************************** Case 3 Update Dist Plans ****************************************
## case #3
## update distributed plan similar to delete

connection conn1;
--disable_warnings
drop table if exists case3_t_1, case3_t_2, case3_t_3;
--enable_warnings

--disable_query_log
--disable_result_log
"""
    create_sql_fmt = "create table case3_t_%d (pk%d int primary key, a%d int) partition by hash(pk%d) partitions 2;"
    for i in range(1, 4):
        print create_sql_fmt % (i, i, i, i)

    get_table_metas(case_num=3, tbl_num=3, tbl_prefix='t', part_num=2, svr_num=2)

    print """
--echo
--echo ======================= single table update ===========================
connection conn1;
insert into case3_t_1 values (1, 1), (2, 1);


--echo
--enable_query_log
update case3_t_1 set a1 = 2;
update case3_t_1 set a1 = 3;
--echo
--enable_result_log
explain update case3_t_1 set a1 = 3;

--echo
--echo // expect 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'update case3_t_1 set a1 = ?';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case3_tbl_1, part0 in backup, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case3_tbl_id_1' server='$case3_tbl1_part1_backup_ip1:$case3_tbl1_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
update case3_t_1 set a1 = 4;
--echo
--enable_result_log
explain update case3_t_1 set a1 = 4;

--echo
--echo // expect 2
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'update case3_t_1 set a1 = ?';

--echo ======================= update with subquery ==================================
--disable_query_log
--disable_result_log
insert into case3_t_2 values (1, 1), (2, 1);
insert into case3_t_3 values (1, 1), (2, 1);
--echo
--echo // all the partitition of case3_t_2 should stay in the same server
--enable_query_log
update case3_t_2 set a2 = 2 where pk2 in (select pk3 from case3_t_3);
update case3_t_2 set a2 = 2 where pk2 in (select pk3 from case3_t_3);
--echo
--enable_result_log
explain update case3_t_2 set a2 = 2 where pk2 in (select pk3 from case3_t_3);

--echo
--echo // expect 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'update case3_t_2 set a2 = ? where pk2 in (select pk3 from case3_t_3)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case3_t_3, part0 in backup, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case3_tbl_id_3' server='$case3_tbl3_part1_backup_ip1:$case3_tbl3_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
update case3_t_2 set a2 = 3 where pk2 in (select pk3 from case3_t_3);
update case3_t_2 set a2 = 3 where pk2 in (select pk3 from case3_t_3);
--echo
--enable_result_log
explain update case3_t_2 set a2 = 3 where pk2 in (select pk3 from case3_t_3);

--echo // expect 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'update case3_t_2 set a2 = ? where pk2 in (select pk3 from case3_t_3)';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case3_t_3, part0 in main, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case3_tbl_id_3' server='$case3_tbl3_part1_main_ip:$case3_tbl3_part1_main_port';
--sleep 3

connection conn1;
--enable_query_log
--echo
update case3_t_2 set a2 = 5 where pk2 in (select pk3 from case3_t_3);
--echo
--enable_result_log
explain update case3_t_2 set a2 = 5 where pk2 in (select pk3 from case3_t_3);

--echo
--echo // expect 2, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'update case3_t_2 set a2 = ? where pk2 in (select pk3 from case3_t_3)';

--disable_query_log
drop table case3_t_1, case3_t_2, case3_t_3;
"""

def case4_setup():
    print """--echo **************************************** Case 4 Select Dist Plans ****************************************
## case #4
## select distributed plan

connection conn1;
--disable_warnings
--disable_query_log
drop table if exists case4_t_1, case4_t_2, case4_t_3;
--enable_warnings

--disable_query_log
--disable_result_log
"""
    create_sql_fmt = "create table case4_t_%d (pk%d int primary key, a%d int) partition by hash(pk%d) partitions 2;"
    for i in range(1, 7):
        print create_sql_fmt % (i, i, i, i)

    insert_sql_fmt = "insert into case4_t_%d values (%d, %d);"
    for i in range(1, 7):
        for j in range(1,3):
            print insert_sql_fmt % (i, j, j)

    get_table_metas(case_num=4, tbl_num=6, tbl_prefix='t', part_num=2, svr_num=2)

    print """
connection conn1;

--echo
--echo ======================================= select with 2 tables ===========================================
--enable_query_log
--disable_result_log
--echo
select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;
select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;
--echo
--enable_result_log
explain select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;

--echo
--echo // expect 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2';

connection conn_admin;
--disable_result_log
--echo
--echo // switch replica leader for case4_t_1, part0 in backup, part1 in main
--disable_query_log
eval alter system switch replica leader partition_id = '0%2@$case4_tbl_id_1' server='$case4_tbl1_part1_backup_ip1:$case4_tbl1_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;
select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;

--echo
--enable_result_log
explain select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;

--echo
--echo // new plan was generated, count(hit_count) == 2, expect 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica for case4_t_1, part0 in main, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case4_tbl_id_1' server='$case4_tbl1_part1_main_ip:$case4_tbl1_part1_main_port';
--sleep 3

connection conn1;
--echo
--enable_query_log
select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;
--echo
--enable_result_log
explain select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;

--echo
--echo // hit old plan, expect 2, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2';

connection conn_admin;
--disable_result_log
--disable_query_log
--echo
--echo // switch replica leader for case4_t_2, part0 in backup, part1 in main
eval alter system switch replica leader partition_id = '0%2@$case4_tbl_id_2' server='$case4_tbl2_part1_backup_ip1:$case4_tbl2_part1_backup_port1';
--sleep 3

connection conn1;
--echo
--enable_query_log
select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;
--echo
--enable_result_log
explain select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2;

--echo
--echo // hit old plan, expect 2, 2
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_1 join case4_t_2 on case4_t_1.pk1 = case4_t_2.pk2';

--echo
--echo =========================== select xxx from t join select 1, etc ================================
--disable_result_log
--enable_query_log
--echo
select hint_placepk3 from case4_t_3 left join (select 1 v1 from dual) tmp_t on tmp_t.v1 = case4_t_3.pk3;
--echo
--enable_result_log
explain select hint_placepk3 from case4_t_3 left join (select 1 v1 from dual) tmp_t on tmp_t.v1 = case4_t_3.pk3;

--echo
--echo // hit_count == 0
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_placepk3 from case4_t_3 left join (select 1 v1 from dual) tmp_t on tmp_t.v1 = case4_t_3.pk3';

connection conn_admin;
--echo
--echo // switch replica leader for case4_t_3, part0 in backup, part1 in main
--disable_query_log
--disable_result_log
eval alter system switch replica leader partition_id='0%2@$case4_tbl_id_3' server='$case4_tbl3_part1_backup_ip1:$case4_tbl3_part1_backup_port1';
--sleep 3

connection conn1;
--echo
--enable_query_log
select hint_placepk3 from case4_t_3 left join (select 1 v1 from dual) tmp_t on tmp_t.v1 = case4_t_3.pk3;
--echo
--enable_result_log
explain select hint_placepk3 from case4_t_3 left join (select 1 v1 from dual) tmp_t on tmp_t.v1 = case4_t_3.pk3;

--echo
--echo // hit the same plan, expect 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_placepk3 from case4_t_3 left join (select 1 v1 from dual) tmp_t on tmp_t.v1 = case4_t_3.pk3';

--echo
--echo ==================================== select with 3 tables ====================================
--disable_result_log
--enable_query_log
--echo
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
--echo
--enable_result_log
explain select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;

--echo
--enable_result_log
--echo // new plan was genenered, expect 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6';

connection conn_admin;
--disable_result_log
--disable_query_log
--echo
--echo // switch replica leader for case4_t_4, part0 in backup, part1 in main
eval alter system switch replica leader partition_id='0%2@$case4_tbl_id_4' server='$case4_tbl4_part1_backup_ip1:$case4_tbl4_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
--echo
--enable_result_log
explain select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;

--echo
--echo // new plan, expect 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case4_t_5, part0 in backup, part1 in main
eval alter system switch replica leader partition_id='0%2@$case4_tbl_id_5' server='$case4_tbl5_part1_backup_ip1:$case4_tbl5_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
--enable_result_log
--echo
--echo
explain select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;

--echo
--echo // new plan expect 1, 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6';

connection conn_admin;
--disable_query_log
--disable_result_log
--echo
--echo // switch replica leader for case4_t_6, part0 in backup, part1 in main
eval alter system switch replica leader partition_id='0%2@$case4_tbl_id_6' server='$case4_tbl6_part1_backup_ip1:$case4_tbl6_part1_backup_port1';
--sleep 3

connection conn1;
--enable_query_log
--echo
select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;
--enable_result_log
--echo
explain select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6;

--echo
--echo // the very first plan was hit, expect 2, 1, 1
select hit_count from oceanbase.V$OB_PLAN_CACHE_PLAN_STAT where statement = 'select hint_place* from case4_t_4, case4_t_5, case4_t_6 where pk4 = pk5 and pk5 = pk6';

--disable_result_log
--disable_query_log
drop table case4_t_1, case4_t_2, case4_t_3;
"""


if __name__ == "__main__":
    test_type = sys.argv[1]
    if test_type == 'new':
        OUT_FILE = "plan_cache_dist_plan_px.test"
        SUB_STR = '/*+USE_PX parallel(2)*/'
    else:
        OUT_FILE = "plan_cache_dist_plan.test"
        SUB_STR = ''
    with open(OUT_FILE, 'w') as f:
        sys.stdout = f
        print FILE_HEADER
        print TEST_SET
        if test_type == 'old':
            case1_setup()
            case2_setup()
            case3_setup()    
        case4_setup()    
        print TEST_CLEANUP

    content = ""
    with open(OUT_FILE, 'r') as f:        
        content = f.read()
        content = re.sub("hint_place", SUB_STR, content)

    with open(OUT_FILE, 'w') as f:
        f.write(content)
    

