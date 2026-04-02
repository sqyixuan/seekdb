use oceanbase;
create resource unit box1 max_cpu 2, memory_size 32212254720;

delimiter //
drop procedure if exists create_tenant;//
create procedure create_tenant(tenant_name varchar(64))
begin
  declare num int;
  declare zone_name varchar(20);
  select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit 1 into num;
  select primary_zone from oceanbase.DBA_OB_TENANTS where tenant_id = 1 into zone_name;
  set @sql_text = concat("create resource pool pool_", tenant_name, " unit = 'box1', unit_num = ", num, ";");
  prepare stmt from @sql_text;
  execute stmt;
  set @sql_text = concat("create tenant ", tenant_name, " primary_zone='", zone_name, "', resource_pool_list=('pool_", tenant_name, "') set ob_compatibility_mode='mysql', ob_tcp_invited_nodes='%', parallel_servers_target=10, ob_sql_work_area_percentage=20, secure_file_priv = '/';");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
end //
delimiter ;

call create_tenant('test');
alter tenant test set variables ob_query_timeout=100000000000;
alter tenant test set variables ob_trx_timeout=100000000000;
