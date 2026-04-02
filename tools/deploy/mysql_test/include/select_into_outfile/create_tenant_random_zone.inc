connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;

--disable_query_log

--disable_warnings
call oceanbase.drop_tenant_force('tenant_select_into_test');
--enable_warnings

eval call oceanbase.create_mysql_tenant_1c2g_with_arg('tenant_select_into_test', ''); 
alter tenant tenant_select_into_test set primary_zone='RANDOM';
sleep 10;

select * from oceanbase.dba_ob_tenants;

--enable_query_log


disconnect obsys;
