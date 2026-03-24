connection sys;
--disable_query_log
drop tenant tenant_charset_test_mysql force;
--disable_warnings
DROP RESOURCE POOL IF EXISTS pool_charset_test1;
DROP RESOURCE UNIT IF EXISTS uint_charset_test1;
--enable_warnings
--enable_query_log
