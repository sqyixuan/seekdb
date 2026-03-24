connection sys_conn;

--disable_query_log
--error 0,5157
drop tenant dblink_tenant1 force;
drop resource pool if exists dblink_pool1;
drop resource unit if exists dblink_unit1;
--enable_query_log
