connection sys;
--disable_query_log
drop tenant tenant_charset_test force;
--disable_warnings
DROP RESOURCE POOL IF EXISTS pool_charset_test;
DROP RESOURCE UNIT IF EXISTS uint_charset_test;
--enable_warnings
--enable_query_log
