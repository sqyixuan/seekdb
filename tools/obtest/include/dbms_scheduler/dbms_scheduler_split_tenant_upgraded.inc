connect (conn_dbms_sched_split_tenant_mysql,$ip,root@mysql,,test,$port);
connection conn_dbms_sched_split_tenant_mysql;

--disable_query_log
--disable_result_log
submit job insert into dbms_sched_t values(1);
--enable_query_log
--enable_result_log
sleep 30;
select * from dbms_sched_t;