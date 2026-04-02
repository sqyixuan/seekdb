connection sys;
--disable_query_log
--disable_warnings
call oceanbase.drop_tenant_force('tenant_stat_test');
--enable_warnings
--enable_query_log
