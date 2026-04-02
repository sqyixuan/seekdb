connection sys1;

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS dblink_tnt1 FORCE;
DROP RESOURCE POOL IF EXISTS pool_charset_test1;
DROP RESOURCE UNIT IF EXISTS uint_charset_test1;
--enable_warnings
--enable_query_log