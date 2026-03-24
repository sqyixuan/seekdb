connection sys;
--disable_query_log
drop tenant IF EXISTS tenant_charset_test force;
drop resource pool IF EXISTS pool_charset_test;
drop resource unit IF EXISTS uint_charset_test;
--enable_query_log
