connection sys;
--disable_query_log
drop tenant tenant_yd force;
drop resource pool if exists pool_yd;
drop resource unit if exists uint_yd;
--enable_query_log
