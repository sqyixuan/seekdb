connection sys;
--disable_query_log
#drop tenant tenant_stat_test_mysql force;
#call oceanbase.drop_tenant_force('tenant_stat_test_mysql');
--enable_query_log
