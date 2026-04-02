
connection sys;
--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS tenant_charset_test FORCE;
DROP RESOURCE POOL IF EXISTS pool_charset_test;
DROP RESOURCE UNIT IF EXISTS uint_charset_test;
--enable_warnings
--enable_query_log


