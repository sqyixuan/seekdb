connect (conn_dbms_sched_split_tenant_z1,$ipz1,root@mysql,,test,$portz1);
connection conn_dbms_sched_split_tenant_z1;
create table dbms_sched_t (id int);

--disable_query_log
--disable_result_log
submit job insert into dbms_sched_t values(1);
--enable_query_log
--enable_result_log
sleep 30;
select * from dbms_sched_t;