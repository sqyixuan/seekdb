
set ob_query_timeout=10000000000;
select sleep(5);

use oceanbase;
create resource unit box1 max_cpu 4, memory_size='16G', log_disk_size='30G',max_iops=1000000;
alter system set memory_chunk_cache_size = '0M';

delimiter //
drop procedure if exists create_tenant;//
create procedure create_tenant(tenant_name varchar(64))
begin
  declare zone_name varchar(20);
  select primary_zone from oceanbase.DBA_OB_TENANTS where tenant_id = 1 into zone_name;
  set @sql_text = concat("create resource pool pool_", tenant_name, " unit = 'box1', unit_num = ", 1, ", zone_list = ('z1','z2','z3');");
  prepare stmt from @sql_text;
  execute stmt;
  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('pool_", tenant_name, "'), locality = 'F@z1,F@z2,F@z3' set ob_compatibility_mode='mysql', ob_tcp_invited_nodes='%',  parallel_servers_target=100, ob_sql_work_area_percentage=50, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end //
delimiter ;

call create_tenant('mysql_tenant');
select sleep(10);

set global ob_query_timeout= 100000000000;
set global ob_trx_timeout= 100000000000;
set global ob_enable_transmission_checksum=OFF;

set global recyclebin=off;
set global parallel_servers_target=600;
set global max_allowed_packet=67108864;
set global ob_plan_cache_percentage = 1;
set global autocommit=ON;

alter tenant mysql_tenant set variables ob_query_timeout=100000000000;
alter tenant mysql_tenant set variables ob_trx_timeout=100000000000;
