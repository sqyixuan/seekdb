connection sys2;

--disable_query_log
--disable_warnings

DROP TENANT IF EXISTS dblink_tnt2 FORCE;
DROP RESOURCE POOL IF EXISTS pool_charset_test2;
DROP RESOURCE UNIT IF EXISTS uint_charset_test2;

--enable_warnings
--enable_query_log