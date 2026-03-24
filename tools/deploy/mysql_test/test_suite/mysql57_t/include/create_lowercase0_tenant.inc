# 该inc文件通过创建租户来实现ob标识符的区分大小写功能（ob只能通过新建租户的方式来设置区分大小写）

connect (conn0,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn0;

--disable_warnings
#drop tenant if exists lowercase0 force;
#drop resource pool if exists test_resource_pool1;
#drop resource unit if exists test_resource_unit;
--enable_warnings

--disable_query_log
--disable_result_log
--source mysql_test/include/set_memory_size.inc
#eval create resource unit test_resource_unit max_cpu 1, memory_size $memory_size;

let $zone_name=query_get_value(select zone from oceanbase.__all_zone where zone != '' limit 1, zone, 1);
let $unit_name=query_get_value(select c.name from oceanbase.__all_tenant t join oceanbase.__all_resource_pool r on t.tenant_id = r.tenant_id join oceanbase.__all_unit_config c on r.unit_config_id = c.unit_config_id  where t.tenant_name = 'sys' limit 1, name, 1);

####--replace_column 1 create_time 2 modify_time 3 unit_config_id
####select * from oceanbase.__all_unit_config where name='test_resource_unit';

eval create resource pool test_resource_pool1 unit = '$unit_name', unit_num = 1, zone_list=('$zone_name');

####--replace_column 1 create_time 2 modify_time 3 resource_pool_id 6 unit_config_id 7 zone
####select * from oceanbase.__all_resource_pool where name='test_resource_pool1';

eval create tenant lowercase0 replica_num = 1, primary_zone='$zone_name', resource_pool_list=('test_resource_pool1') set lower_case_table_names = 0, ob_tcp_invited_nodes='%';
--source mysql_test/include/check_tenant_sync.inc

####--replace_column 1 create_time 2 modify_time 3 tenant_id 6 zone 7 zone 13 locality 14 logonly_replica_num 15 previous_locality 16
####storage_format_version 17 storage_format_work_version
####select * from oceanbase.__all_tenant where tenant_name='lowercase0';
--enable_query_log
--enable_result_log

##同步 schema的时间
let $__tenant_name__ = 'lowercase0';
--source mysql_test/include/get_tenant_server.inc
connect (lowercase0,$TE_SERVER_IP,root@lowercase0,,*NO-ONE*,$TE_SERVER_PORT);
connection lowercase0;

show variables like 'lower_case_table_names';


