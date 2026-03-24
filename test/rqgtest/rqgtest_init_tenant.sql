
set ob_query_timeout=10000000000;
select sleep(5);

use oceanbase;
create resource unit box1 max_cpu 6, memory_size 25294967296, log_disk_size='30G',max_iops=1000000;
alter system set memory_chunk_cache_size = '0M';

delimiter //
drop procedure if exists create_mysql_tenant;//
create procedure create_mysql_tenant(tenant_name varchar(64))
begin
  declare num int;
  declare zone_name varchar(20);
  select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit 1 into num;
  select primary_zone from oceanbase.DBA_OB_TENANTS where tenant_id = 1 into zone_name;
  set @sql_text = concat("create resource pool pool_", tenant_name, " unit = 'box1', unit_num = ", num, ", zone_list = ('z1','z2','z3');");
  prepare stmt from @sql_text;
  execute stmt;
  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('pool_", tenant_name, "'), locality = 'F@z1,F@z2,F@z3' set ob_compatibility_mode='mysql', ob_tcp_invited_nodes='%',  parallel_servers_target=100, ob_sql_work_area_percentage=50, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end //
delimiter ;


call create_mysql_tenant('tt1');
select sleep(10);

alter system set _pushdown_storage_level=3 tenant=sys;
alter system set _pushdown_storage_level=3 tenant=all_user;
alter system set _pushdown_storage_level=3 tenant=all_meta;
set global ob_query_timeout=10000000000;
alter system set server_permanent_offline_time='20s';
alter system set row_compaction_update_limit=3;
alter system set _private_buffer_size='20K';
set global parallel_servers_target=200;
alter proxyconfig set enable_reroute=true;
set global max_allowed_packet = 219430400;
