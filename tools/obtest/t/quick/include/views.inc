#############################################
#owner : @wanhong.wwh
#owner group: rs
#description: quick test
# 测试点：验证views
#############################################

--echo ================== 1. SYS tenant views ==================
connection conn_sys;

# 检查视图schema，要求列是完整的，全部大写
--echo 1.1 check views schema
desc oceanbase.DBA_OB_TENANTS;
desc oceanbase.DBA_OB_UNITS;
desc oceanbase.DBA_OB_UNIT_CONFIGS;
desc oceanbase.DBA_OB_RESOURCE_POOLS;
desc oceanbase.DBA_OB_SERVERS;
desc oceanbase.DBA_OB_ZONES;
desc oceanbase.DBA_OB_ROOTSERVICE_EVENT_HISTORY;
desc oceanbase.DBA_OB_TENANT_JOBS;
desc oceanbase.DBA_OB_UNIT_JOBS;
desc oceanbase.DBA_OB_SERVER_JOBS;

# 验证各个视图语义，从用户角度出发，尽可能验证所有场景的所有输出，特别是CASE WHEN实现的列
--echo 1.2 verify all views
connection conn_sys;

# 不验证CREATE_TIME列
SELECT TENANT_ID, TENANT_NAME, TENANT_TYPE, PRIMARY_ZONE, LOCALITY, PREVIOUS_LOCALITY, COMPATIBILITY_MODE, STATUS, IN_RECYCLEBIN, LOCKED FROM DBA_OB_TENANTS;

# 不验证：CREATE_TIME、SVR_IP、SVR_PORT 列
SELECT UNIT_ID, TENANT_ID, STATUS, RESOURCE_POOL_ID, UNIT_GROUP_ID, ZONE, MIGRATE_FROM_SVR_IP, MIGRATE_FROM_SVR_PORT, MANUAL_MIGRATE, UNIT_CONFIG_ID FROM DBA_OB_UNITS;

# 验证DBA_OB_UNITS里面的值与DBA_OB_UNIT_CONFIGS值一样
select distinct(U.MAX_CPU = C.MAX_CPU) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;
select distinct(U.MIN_CPU = C.MIN_CPU) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;
select distinct(U.MEMORY_SIZE = C.MEMORY_SIZE) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;
select distinct(U.LOG_DISK_SIZE = C.LOG_DISK_SIZE) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;
select distinct(U.MAX_IOPS = C.MAX_IOPS) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;
select distinct(U.MIN_IOPS = C.MIN_IOPS) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;
select distinct(U.IOPS_WEIGHT = C.IOPS_WEIGHT) from DBA_OB_UNIT_CONFIGS C, DBA_OB_UNITS U where C.UNIT_CONFIG_ID = U.UNIT_CONFIG_ID;

# 不验证 CREATE_TIME, MODIFY_TIME
SELECT UNIT_CONFIG_ID, NAME, MAX_CPU, MIN_CPU, MEMORY_SIZE, LOG_DISK_SIZE, MAX_IOPS, MIN_IOPS, IOPS_WEIGHT FROM DBA_OB_UNIT_CONFIGS WHERE UNIT_CONFIG_ID > 1;

# 不验证CREATE_TIME、MODIFY_TIME
SELECT RESOURCE_POOL_ID, NAME, TENANT_ID, UNIT_COUNT, UNIT_CONFIG_ID, ZONE_LIST, REPLICA_TYPE FROM DBA_OB_RESOURCE_POOLS;

# 通过__all_server表验证DBA_OB_SERVERS
SELECT count(*) FROM DBA_OB_SERVERS WHERE (SVR_IP, SVR_PORT, ID, ZONE, SQL_PORT, STATUS, START_SERVICE_TIME, BUILD_VERSION, CREATE_TIME, MODIFY_TIME) in (SELECT SVR_IP, SVR_PORT, ID, ZONE, SQL_PORT, STATUS, usec_to_time(START_SERVICE_TIME), BUILD_VERSION, GMT_CREATE AS CREATE_TIME, GMT_MODIFIED AS MODIFY_TIME  FROM __all_server);
# 期望是列值为NULL
SELECT STOP_TIME FROM DBA_OB_SERVERS;

#group by被转为distinct，distinct与nest_loop_join结合使用时结果不对，待修复
#SELECT ZONE, STATUS, IDC, REGION, TYPE FROM DBA_OB_ZONES;

# 通过__all_rootservice_event_history验证DBA_OB_ROOTSERVICE_EVENT_HISTORY
SELECT view_row_cnt = table_row_cnt
FROM
    (SELECT count(*) as view_row_cnt FROM DBA_OB_ROOTSERVICE_EVENT_HISTORY WHERE (`TIMESTAMP`, MODULE, EVENT, NAME1, VALUE1, NAME2, VALUE2, NAME3, VALUE3, NAME4, VALUE4, NAME5, VALUE5, NAME6, EXTRA_INFO, RS_SVR_IP, RS_SVR_PORT) in (select gmt_create, MODULE, EVENT, NAME1, VALUE1, NAME2, VALUE2, NAME3, VALUE3, NAME4, VALUE4, NAME5, VALUE5, NAME6, EXTRA_INFO, RS_SVR_IP, RS_SVR_PORT from __all_rootservice_event_history)) as V,
    (SELECT count(*) as table_row_cnt FROM __all_rootservice_event_history) as T;

# 通过__all_server_event_history验证DBA_OB_SERVER_EVENT_HISTORY
SELECT view_row_cnt = table_row_cnt
FROM
    (SELECT count(*) as view_row_cnt FROM DBA_OB_SERVER_EVENT_HISTORY WHERE (`TIMESTAMP`, SVR_IP, SVR_PORT, MODULE, EVENT, NAME1, VALUE1, NAME2, VALUE2, NAME3, VALUE3, NAME4, VALUE4, NAME5, VALUE5, NAME6, EXTRA_INFO) in (select gmt_create, SVR_IP, SVR_PORT, MODULE, EVENT, NAME1, VALUE1, NAME2, VALUE2, NAME3, VALUE3, NAME4, VALUE4, NAME5, VALUE5, NAME6, EXTRA_INFO from __all_server_event_history)) as V,
    (SELECT count(*) as table_row_cnt FROM __all_server_event_history) as T;

# 通过__all_rootservice_job来验证DBA_OB_xxx_JOBS
insert into __all_rootservice_job (job_id, job_type, job_status, result_code, progress, tenant_id, unit_id, svr_ip, svr_port, sql_text, extra_info, rs_svr_ip, rs_svr_port) values
(10000, 'ALTER_TENANT_LOCALITY', 'INPROGRESS', 0, 0, 1, 0, '', 0, 'sql_text', 'extra_info', '127.0.0.1', 1000),
(20000, 'MIGRATE_UNIT', 'INPROGRESS', 0, 0, -1, 1001,  '', 0, 'sql_text', 'extra_info', '127.0.0.1', 1000),
(30000, 'MIGRATE_UNIT', 'FAILED', 4002, 0, 1002, 1003,  '', 0, 'sql_text', 'extra_info', '127.0.0.1', 1000),
(40000, 'DELETE_SERVER', 'SUCCESS', 0, 100, -1, 0, '192.168.1.1', 2000,  'sql_text', 'extra_info', '127.0.0.1', 1000);

SELECT JOB_ID, JOB_TYPE, JOB_STATUS, RESULT_CODE, PROGRESS, TENANT_ID, SQL_TEXT, EXTRA_INFO, RS_SVR_IP, RS_SVR_PORT FROM DBA_OB_TENANT_JOBS WHERE JOB_ID >= 10000;

SELECT JOB_ID, JOB_TYPE, JOB_STATUS, RESULT_CODE, PROGRESS, TENANT_ID, UNIT_ID, SQL_TEXT, EXTRA_INFO, RS_SVR_IP, RS_SVR_PORT FROM DBA_OB_UNIT_JOBS WHERE JOB_ID >= 10000;

SELECT JOB_ID, JOB_TYPE, JOB_STATUS, RESULT_CODE, PROGRESS, SVR_IP, SVR_PORT, SQL_TEXT, EXTRA_INFO, RS_SVR_IP, RS_SVR_PORT FROM DBA_OB_SERVER_JOBS WHERE JOB_ID >= 10000;

--echo ================== 2. Tenant space views ==================

--echo ------- sys tenant
connection conn_sys;
desc oceanbase.DBA_OB_LS;
desc oceanbase.CDB_OB_LS;

select ls_id, status, primary_zone, unit_group_id, ls_group_id, create_scn, drop_scn from oceanbase.DBA_OB_LS where ls_id = 1;
select ls_id, status, primary_zone, unit_group_id, ls_group_id, drop_scn from oceanbase.DBA_OB_LS where ls_id != 1;
