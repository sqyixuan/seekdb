connection sys;
--disable_query_log
drop tenant tenant_compat_test force;
drop resource pool pool_compat_test;
drop resource unit uint_compat_test;
--enable_query_log
