#创建新用户
##创建mysql租户
connection proxy_conn;
eval CREATE RESOURCE UNIT unit_new  MEMORY_SIZE = '3G',MAX_CPU = 1, MIN_CPU = 1,LOG_DISK_SIZE = '6G',MAX_IOPS = 10000, MIN_IOPS = 10000, IOPS_WEIGHT=1;
obdocker.check_until_timeout select count(*) FROM oceanbase.DBA_OB_UNIT_CONFIGS WHERE NAME = "unit_new", 1, 120;
#创建资源池
eval CREATE RESOURCE POOL pool_new UNIT= "unit_new" ,UNIT_NUM=1, ZONE_LIST=('z1','z2','z3');
obdocker.check_until_timeout select count(*) FROM DBA_OB_RESOURCE_POOLS WHERE NAME = "pool_new",1, 120;
#创建租户
eval CREATE TENANT IF NOT EXISTS tent_new PRIMARY_ZONE='z1',RESOURCE_POOL_LIST=("pool_new") SET OB_TCP_INVITED_NODES='%';
obdocker.check_until_timeout select count(*) FROM DBA_OB_TENANTS WHERE TENANT_NAME = "tent_new",1,120;
let $tenant_new_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'tent_new', tenant_id, 1);
#插入数据
ob_connect (sqlnew_conn,$proxyip,root@tent_new,,,$proxyport);
#ddl
create database test_new;
create table test_new.t1_after(pk int primary key, c1 int);
create table test_new.t2_after(c1 int, c2 int) partition by hash(c1) partitions 8;
#dml
let $cnt=1;
while($cnt <= 100)
{
  eval insert into test_new.t1_after (pk,c1) values ($cnt, $cnt);
  eval insert into test_new.t2_after (c1,c2) values ($cnt, $cnt);
  inc $cnt;
}
update test_new.t1_after set c1=222 where pk=11;
update test_new.t2_after set c2=111 where c1=11;
delete from test_new.t1_after where pk=12;
delete from test_new.t2_after where c1=12;
