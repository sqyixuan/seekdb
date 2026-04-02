
set ob_query_timeout=15000000000;
select sleep(5);

use oceanbase;
create resource unit box1 max_cpu 3, memory_size='25G', log_disk_size='30G',max_iops=1000000;
alter system set memory_chunk_cache_size = '0M';
set global ob_query_timeout=200000000;
set global ob_trx_idle_timeout=200000000;
set global ob_trx_timeout=200000000;


delimiter //
drop procedure if exists create_mysql_tenant;//
create procedure create_mysql_tenant(tenant_name varchar(64))
begin
  declare num int;
  declare zone_name varchar(20);
  select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit 1 into num;
  select primary_zone from oceanbase.DBA_OB_TENANTS where tenant_id = 1 into zone_name;
  set @sql_text = concat("create resource pool pool_", tenant_name, " unit = 'box1', unit_num = ", num, ", zone_list = ('z1','z2');");
  prepare stmt from @sql_text;
  execute stmt;
  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('pool_", tenant_name, "'), locality = 'F@z1,F@z2' set ob_compatibility_mode='mysql', ob_tcp_invited_nodes='%',  parallel_servers_target=100, ob_sql_work_area_percentage=50, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end //
delimiter ;

delimiter //
drop procedure if exists create_oracle_tenant;//
create procedure create_oracle_tenant(tenant_name varchar(64))
begin
  declare num int;
  declare zone_name varchar(20);
  select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit 1 into num;
  select primary_zone from oceanbase.DBA_OB_TENANTS where tenant_id = 1 into zone_name;
  set @sql_text = concat("create resource pool pool_", tenant_name, " unit = 'box1', unit_num = ", num, ", zone_list = ('z1','z2');");
  prepare stmt from @sql_text;
  execute stmt;
  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('pool_", tenant_name, "'), locality = 'F@z1,F@z2' set ob_compatibility_mode='oracle', ob_tcp_invited_nodes='%',  parallel_servers_target=100, ob_sql_work_area_percentage=50, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end //
delimiter ;


call create_mysql_tenant('test_mysql');
call create_oracle_tenant('test_oracle');
select sleep(10);

alter system set _pushdown_storage_level=3 tenant=sys;
alter system set _pushdown_storage_level=3 tenant=all_user;
alter system set _pushdown_storage_level=3 tenant=all_meta;
set global ob_query_timeout=15000000000;
alter tenant test_mysql set variables parallel_servers_target=200;
alter tenant test_oracle set variables parallel_servers_target=200;
alter system set_tp tp_no = 1200, error_code = 4001, frequency = 1;

alter system set _ob_ddl_timeout='1200s';