
set session ob_query_timeout= 100000000000;
set session ob_trx_timeout= 100000000000;
select sleep(5);
use oceanbase;
create resource unit mysql_box max_cpu 6, memory_size 32294967296,log_disk_size='30G';

delimiter //
drop procedure if exists create_tenant;//
create procedure create_tenant(tenant_name varchar(64))
begin
  declare num int;
  declare zone_name varchar(20);
  select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit 1 into num;
  select zone from oceanbase.dba_ob_zones where status='ACTIVE' limit 1 into zone_name;
  set @sql_text = concat("create resource pool mysql_pool unit = 'mysql_box', unit_num = ", num, ", zone_list = ('z1','z2','z3');");
  prepare stmt from @sql_text;
  execute stmt;
  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('mysql_pool'), locality = 'F@z1,F@z2,F@z3' set ob_compatibility_mode='mysql', ob_tcp_invited_nodes='%',  parallel_servers_target=10, ob_sql_work_area_percentage=20, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end //
delimiter ;

call create_tenant('mysql_tenant');

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
