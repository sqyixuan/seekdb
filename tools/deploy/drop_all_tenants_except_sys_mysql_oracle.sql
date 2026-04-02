-- =========================================================================================
-- author: quanwei.wqw
-- 在跑大、小farm时，经常遇到'zone1' resource not enough to hold 1 unit的问题, 本质是case写的
-- 不规范，创建租户后，case结束后没有将资源完全释放，导致下一个case在创建租户时资源不够。
-- 这个脚本用于case结束后, 删除除了sys, oracle, mysql以外的所有租户资源。

delimiter ;
use oceanbase;
set @@session.ob_query_timeout = 500000000;

delimiter /
drop procedure if exists drop_resource_pool;/
create procedure drop_resource_pool(resource_pool varchar(128))
begin
  declare recyclebin_value int;
  select value from oceanbase.CDB_OB_SYS_VARIABLES where name = 'recyclebin' and tenant_id=1 into recyclebin_value;
  set recyclebin = off;

  -- 清理resource pool
  set @sql_text = concat("drop resource pool if exists ", resource_pool, ";");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;

  set recyclebin = recyclebin_value;
end /


drop procedure if exists drop_recycled_tenant;/
create procedure drop_recycled_tenant(tenant_name varchar(128))
begin
  declare recyclebin_value int;
  declare num int;
  select value from oceanbase.CDB_OB_SYS_VARIABLES where name = 'recyclebin' and tenant_id=1 into recyclebin_value;
  set recyclebin = off;

  select tenant_name;
  -- purge recycled_tenant
  set @sql_text = concat("PURGE TENANT ", tenant_name, ";");
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;

  set recyclebin = recyclebin_value;
end /


drop procedure if exists drop_all_tenants_except_sys_mysql_oracle;/
create procedure drop_all_tenants_except_sys_mysql_oracle()
begin
  declare num int;
  declare num2 int;
  declare tmp_tenant_name varchar(64);
  declare rp_num int;
  declare tmp_rp_name varchar(128);
  declare recycled_num int;
  declare recycled_name varchar(128);
  declare recycled_num2 int;
  declare unused_unit_num int;

  -- drop unused tenant
  -- recycled tenant name maybe like "RECYCLE_$_1_1716604239079824" or "__recycle_$_xxxx"
  select count(*) from  oceanbase.DBA_OB_TENANTS where TENANT_TYPE = 'USER' and tenant_name not in ('oracle', 'mysql') and lower(tenant_name) not like "%recycle_%" into num;
  select num as tenant_name_to_be_dropped;
  WHILE num>0 DO
    select tenant_name from oceanbase.DBA_OB_TENANTS where TENANT_TYPE = 'USER' and tenant_name not in ('oracle', 'mysql') and lower(tenant_name) not like "%recycle_%" limit 1 into tmp_tenant_name;
    select tmp_tenant_name as will_drop_this_tenant;
    call oceanbase.drop_tenant_force(tmp_tenant_name);
    SET num=num-1;
  END WHILE;

  -- purge tenant in RECYCLEBIN
  select count(*) from oceanbase.DBA_OB_TENANTS where lower(tenant_name) like "%recycle_%" and status = "NORMAL" into recycled_num;
  select recycled_num as tenant_name_recycled;
  WHILE recycled_num>0 DO
    select tenant_name from oceanbase.DBA_OB_TENANTS where lower(tenant_name) like "%recycle_%" and status = "NORMAL" limit 1 into recycled_name;
    select recycled_name as tenant_name_recycled;
    call oceanbase.drop_recycled_tenant(recycled_name);
    SET recycled_num=recycled_num-1;
  END WHILE;

  -- wait all recycled_tenant dropped
  select count(*) from oceanbase.DBA_OB_TENANTS where lower(tenant_name) like "%recycle_%" into recycled_num2;
  IF recycled_num2>0 THEN
    set num2=400;
    WHILE num2>0 DO
      select count(*) from oceanbase.DBA_OB_TENANTS where lower(tenant_name) like "%recycle_%" into recycled_num2;
      select recycled_num2;
      IF recycled_num2=0 THEN
       set num2=0;
      END IF;
      select sleep(1);
      SET num2=num2-1;
    END WHILE;
  END IF;

  -- drop unused resource pool
  select count(*) from  oceanbase.DBA_OB_RESOURCE_POOLS where name not in ('sys_pool', 'pool_for_tenant_oracle', 'pool_for_tenant_mysql') into rp_num;
  select rp_num as resource_pool_to_be_dropped;
  WHILE rp_num>0 DO
    select name from oceanbase.DBA_OB_RESOURCE_POOLS where name not in ('sys_pool', 'pool_for_tenant_oracle', 'pool_for_tenant_mysql') limit 1 into tmp_rp_name;
    select tmp_rp_name as will_drop_this_resource_pool;
    call oceanbase.drop_resource_pool(tmp_rp_name);
    SET rp_num=rp_num-1;
  END WHILE;

  -- wait all unused resource pool dropped
  select count(*) from oceanbase.GV$OB_UNITS where tenant_id not in (1, 1001, 1002, 1003, 1004) into unused_unit_num;
  IF unused_unit_num>0 THEN
    set num2=400;
    WHILE num2>0 DO
      select count(*) from oceanbase.GV$OB_UNITS where tenant_id not in (1, 1001, 1002, 1003, 1004) into unused_unit_num;
      select unused_unit_num;
      IF unused_unit_num=0 THEN
       set num2=0;
      END IF;
      select sleep(1);
      SET num2=num2-1;
    END WHILE;
  END IF;

end /

delimiter ;

-- use oceanbase;
-- call oceanbase.drop_all_tenants_except_sys_mysql_oracle();
