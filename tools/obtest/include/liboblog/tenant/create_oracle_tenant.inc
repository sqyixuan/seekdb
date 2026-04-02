# create oracle tenant

ob1.mysql alter system set resource_hard_limit= 800;
sleep 2;
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);

--disable_warnings
drop tenant if exists oracle;
--disable_warnings
drop resource pool if exists oracle_oblog_pool;
--disable_warnings
drop resource unit if exists oracle_oblog_box;

### create oracle tenant
ob1.mysql create resource unit oracle_oblog_box max_cpu 1, memory_size='5G';
ob1.mysql create resource pool oracle_oblog_pool unit = 'oracle_oblog_box', unit_num = 1;
ob1.mysql create tenant oracle replica_num = 1, resource_pool_list=('oracle_oblog_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
# oracle模式下autocommit值已为off,会引起诸多case出现因事务迟迟未提交导致的各种问题 为了规避此类问题,将mysqltest环境,obtest环境的该变量,在创建租户之后设置为on
ob1.mysql alter tenant oracle set variables autocommit='on';
ob1.mysql alter tenant oracle set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
ob1.mysql alter tenant oracle set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
ob1.mysql alter tenant oracle set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

sleep 2;
ob_connect(conn_oracle, ob1.z1.obs0, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);

### create user
create user obtest IDENTIFIED BY "obtest";
grant all on *.* to obtest WITH GRANT OPTION;
if ($is_equal_or_greater_than_227 == $IS_EQUAL_OR_GREATER_THAN_227)
{
grant dba to obtest;
grant dba to ORAAUDITOR;
}
ob_connect(conn_oracle_obtest,ob1.z1.obs0,OBTEST@oracle,obtest,obtest,,USE_OB_CONNECTOR);
let $oracle_tenant_name = oracle;
let $original_conn = conn_oracle_obtest;
