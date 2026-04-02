connect (sys,$OBMYSQL_MS0,root,,oceanbase,$OBMYSQL_PORT);
connection sys;


--disable_query_log
--disable_warnings
#drop tenant if exists tenant_lower_case FORCE;
#drop resource pool if exists pool_lower_case;
#drop resource unit if exists uint_lower_case;
--enable_warnings

#eval CREATE RESOURCE UNIT uint_lower_case MAX_CPU=1, MEMORY_SIZE='2G';
#eval CREATE RESOURCE POOL pool_lower_case UNIT='uint_lower_case', UNIT_NUM=$unit_num;
#eval CREATE TENANT tenant_lower_case PRIMARY_ZONE = $zone, REPLICA_NUM = 1, RESOURCE_POOL_LIST = ('pool_lower_case') set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql', lower_case_table_names=$name_case;

let $__tenant_name__ = 'sys';
--source mysql_test/include/get_tenant_server.inc
connect (conn_lower_case_root, $TE_SERVER_IP, root,,test, $TE_SERVER_PORT);
connection conn_lower_case_root;
--enable_query_log
--disable_warnings
drop database if exists db1;
--enable_warnings
create database db1;
--disable_abort_on_error
create table `db1`.`t1`(`c1` int);

--result_format 4
#select schema_name from information_schema.schemata where schema_name = 'DB1';
select schema_name from information_schema.schemata where schema_name = 'db1';
#select table_schema, table_name from information_schema.tables where table_schema = 'DB1' order by table_schema, table_name;
#select table_schema, table_name from information_schema.tables where table_schema = 'db1' order by table_schema, table_name;
#select table_schema, table_name from information_schema.tables where table_name = 'T1' order by table_schema, table_name;
#select table_schema, table_name from information_schema.tables where table_name = 't1' order by table_schema, table_name;
#select table_schema, table_name, column_name from information_schema.columns where table_schema = 'DB1' order by table_schema, table_name;
#select table_schema, table_name, column_name from information_schema.columns where table_schema = 'db1' order by table_schema, table_name;
#select table_schema, table_name, column_name from information_schema.columns where table_name = 'T1' order by table_schema, table_name;
#select table_schema, table_name, column_name from information_schema.columns where table_name = 't1' order by table_schema, table_name;

drop database db1;
connection sys;
--disable_query_log
#drop tenant tenant_lower_case force;
#drop resource pool pool_lower_case;
#drop resource unit uint_lower_case;
--enable_query_log
