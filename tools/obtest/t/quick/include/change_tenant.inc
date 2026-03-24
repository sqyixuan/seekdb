#############################################
#owner : zhaoziqian.zzq
#owner group: rs
#description: quick test
# 测试点：验证切换租户
#############################################
ob_connect(conn_sys_z1_obs0, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
if ($is_standalone != 1)
{
ob_connect(conn_sys_z1_obs1, ob1.z1.obs1, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
}

# z1.obs0 has sys、mysql、META$1002
connection conn_sys_z1_obs0;
# change tenant by tenant_id
select effective_tenant_id();
alter system change tenant tenant_id = 1001;
select effective_tenant_id();
select count(*) > 0 from oceanbase.__all_ls_meta_table where tenant_id = 1002;
alter system change tenant tenant_id = 1002;
select effective_tenant_id();
--error 5157
alter system change tenant tenant_id = 1003;
select effective_tenant_id();
alter system change tenant tenant_id = 1;
select effective_tenant_id();
# change tenant by tenant_name
alter system change tenant META$1002;
select effective_tenant_id();
select count(*) > 0 from oceanbase.__all_ls_meta_table where tenant_id = 1002;
alter system change tenant mysql;
select effective_tenant_id();
--error 5160
alter system change tenant META_TENANT;
select effective_tenant_id();
alter system change tenant sys;
select effective_tenant_id();
if ($is_standalone != 1)
{
# z1.obs1 has sys、mysql、META$1002
connection conn_sys_z1_obs1;
# change tenant by tenant_id
select effective_tenant_id();
alter system change tenant tenant_id = 1001;
select effective_tenant_id();
select count(*) > 0 from oceanbase.__all_ls_meta_table where tenant_id = 1002;
alter system change tenant tenant_id = 1002;
select effective_tenant_id();
--error 5157
alter system change tenant tenant_id = 1003;
select effective_tenant_id();
alter system change tenant tenant_id = 1;
select effective_tenant_id();
# change tenant by tenant_name
alter system change tenant META$1002;
select effective_tenant_id();
select count(*) > 0 from oceanbase.__all_ls_meta_table where tenant_id = 1002;
alter system change tenant mysql;
select effective_tenant_id();
--error 5160
alter system change tenant META_TENANT;
select effective_tenant_id();
alter system change tenant sys;
select effective_tenant_id();
}
#TODO:(zhaoziqian.zzq) check unit dropped/migration alter change tenant
#TODO:(zhaoziqian.zzq) run change tenant cmd through proxy
