# EN_INITIAL_MIGRATION_TASK_FAILED = 900,
# EN_START_MIGRATION_TASK_FAILED = 901,
# EN_SYS_TABLETS_MIGRATION_TASK_FAILED = 902,
# EN_DATA_TABLETS_MIGRATION_TASK_FAILED = 903,
# EN_TABLET_GROUP_MIGRATION_TASK_FAILED = 904,
# EN_TABLET_MIGRATION_TASK_FAILED = 905,
# EN_MIGRATION_FINISH_TASK_FAILED = 906,

ob1=OBI(cluster=1:1:2,proxy_num=0,init_unit_cpu=0);
let $extra_conf = "cpu_count=32,memory_limit='40G',system_memory='5G',__min_full_resource_pool_memory='1073741824',cache_wash_threshold='1G',enable_async_syslog=false,_private_buffer_size='16K',_fast_commit_callback_count='1'";

ob1.reboot;

ob1.mysql ALTER SYSTEM SET enable_rebalance = 'true';
ob1.mysql ALTER SYSTEM SET default_compress_func = 'none';

let $proxy_ip = deploy_get_value(ob1.z1.obs0.ip);
let $proxy_port = deploy_get_value(ob1.z1.obs0.mysql_port);

connect(sys_conn, $proxy_ip, root@sys,,oceanbase, $proxy_port);

# create new tenant tenant_for_migrate_locality
--disable_all_result_log
ob1.mysql alter resource unit sys_unit_config memory_size='2G';
ob1.mysql set global ob_query_timeout = 100000000;
--enable_all_result_log
ob1.mysql CREATE RESOURCE POOL sys_pool_2 UNIT = "sys_unit_config", UNIT_NUM = 1, ZONE_LIST = ('z1', 'z2', 'z3');
ob1.create_tenant CREATE TENANT tenant_for_migrate_locality RESOURCE_POOL_LIST=('sys_pool_2'), LOCALITY = "F@z1, F@z3", PRIMARY_ZONE = "z1" SET ob_tcp_invited_nodes='%';

let $tenant_id = deploy_get_value(ob1.select SELECT tenant_id FROM oceanbase.__all_tenant WHERE tenant_name = "tenant_for_migrate_locality", tenant_id, 1);
let $resource_pool_id = deploy_get_value(ob1.select SELECT resource_pool_id FROM oceanbase.__all_resource_pool WHERE tenant_id = $tenant_id, resource_pool_id, 1);

# get tenant_for_migrate_locality's unit info in z3
let $unit_id = deploy_get_value(ob1.select SELECT unit_id FROM oceanbase.__all_unit WHERE resource_pool_id = $resource_pool_id AND zone = 'z3', unit_id, 1);
let $ip = deploy_get_value(ob1.select SELECT svr_ip FROM oceanbase.__all_unit WHERE unit_id = $unit_id, svr_ip, 1);
let $port = deploy_get_value(ob1.select SELECT svr_port FROM oceanbase.__all_unit WHERE unit_id = $unit_id, svr_port, 1);
let $z3_cur = $ip:$port;

# get servers' addr in z1 and z2
let $z1_dst_ip0 = deploy_get_value(ob1.z1.obs0.ip);
let $z1_dst_port0 = deploy_get_value(ob1.z1.obs0.port);
let $z1_dst0 = $z1_dst_ip0:$z1_dst_port0;
let $z2_dst_ip0 = deploy_get_value(ob1.z2.obs0.ip);
let $z2_dst_port0 = deploy_get_value(ob1.z2.obs0.port);
let $z2_dst0 = $z2_dst_ip0:$z2_dst_port0;

# get servers' addr in z3
let $z3_dst_ip0 = deploy_get_value(ob1.z3.obs0.ip);
let $z3_dst_ip1 = deploy_get_value(ob1.z3.obs1.ip);
let $z3_dst_port0 = deploy_get_value(ob1.z3.obs0.port);
let $z3_dst_port1 = deploy_get_value(ob1.z3.obs1.port);
let $z3_dst0 = $z3_dst_ip0:$z3_dst_port0;
let $z3_dst1 = $z3_dst_ip1:$z3_dst_port1;

# create table and insert data
ob_connect(conn_mysql, ob1.z1.obs0, root@tenant_for_migrate_locality, , test, , USE_MYSQL_CONNECTOR);
connection conn_mysql;
create table t1_no_pk (c1 int);
insert into t1_no_pk values (1), (2), (3);
select * from t1_no_pk;
update t1_no_pk set c1=c1+10;
select * from t1_no_pk;
create table t1_pk (c1 int primary key);
insert into t1_pk values (1), (2), (3);
select * from t1_pk;
update t1_pk set c1=c1+10;
select * from t1_pk;
create table t1_part (c1 int primary key) partition by hash(c1) partitions 4;
insert into t1_part values (1), (2), (3), (4);
select * from t1_part;
update t1_part set c1=c1+10;
select * from t1_part;
connect(sys_conn, $proxy_ip, root@sys,,oceanbase, $proxy_port);
alter system minor freeze tenant tenant_for_migrate_locality;
ob1.z1.obs0.check_log_until_success -c "succeed to release head data memtable";
ob1.z3.obs1.check_log_until_success -c "succeed to release head data memtable";
sleep 10;
connection conn_mysql;
insert into t1_no_pk values (4), (5), (6);
insert into t1_no_pk values (7), (8), (9);
insert into t1_no_pk values (10), (11), (12);
insert into t1_pk values (4), (5), (6);
insert into t1_pk values (7), (8), (9);
insert into t1_pk values (10);
insert into t1_part values (5), (6), (7), (8);
insert into t1_part values (9), (10), (21), (22);
connect(sys_conn, $proxy_ip, root@sys,,oceanbase, $proxy_port);
alter system minor freeze tenant tenant_for_migrate_locality;
ob1.z1.obs0.check_log_until_success -c "succeed to release head data memtable";
ob1.z3.obs1.check_log_until_success -c "succeed to release head data memtable";
sleep 10;

--echo "prepare finished"
