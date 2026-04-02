# create oracle tenant

ob1.mysql alter system set resource_hard_limit= 800;
sleep 2;
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);

--disable_warnings
drop tenant if exists oracle;
--disable_warnings
drop resource pool if exists oblog_pool;
--disable_warnings
drop resource unit if exists oblog_box;

### create oracle tenant
ob1.mysql create resource unit oblog_box max_cpu 2, memory_size='20G';
ob1.mysql create resource pool oblog_pool unit = 'oblog_box', unit_num = 1;
ob1.mysql create tenant oracle replica_num = 1, resource_pool_list=('oblog_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
# oracle模式下autocommit值已为off,会引起诸多case出现因事务迟迟未提交导致的各种问题 为了规避此类问题,将mysqltest环境,obtest环境的该变量,在创建租户之后设置为on
ob1.mysql alter tenant oracle set variables autocommit='on';
ob1.mysql alter tenant oracle set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
ob1.mysql alter tenant oracle set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
ob1.mysql alter tenant oracle set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

sleep 2;
ob_connect(conn_oracle, ob1.proxy0, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);

### create user
create user obtest IDENTIFIED BY "obtest";
grant all on *.* to obtest WITH GRANT OPTION;
grant dba to obtest;
--disable_query_log
grant dba to ORAAUDITOR;
create user oblog_db identified by "passwd";
grant dba to oblog_db;
--enable_query_log
ob_connect(conn_oracle_obtest,ob1.z1.obs0,OBTEST@oracle,obtest,obtest,,USE_OB_CONNECTOR);
alter session set current_schema = oblog_db;
sleep 1;

let $oracle_tenant_name = oracle;
let $original_conn = conn_oracle_obtest;

connection conn_sys;
alter system change tenant oracle;
--disable_query_log
--disable_result_log
--source ../default_srs_data_mysql.sql
--enable_result_log
--enable_query_log
