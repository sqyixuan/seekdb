connection sys;
--disable_query_log
call oceanbase.drop_tenant_force('tenant_charset_test');
purge recyclebin;
--enable_query_log
