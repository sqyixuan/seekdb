--disable_query_log
connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,oceanbase,$OBMYSQL_PORT);
connection obsys;



let $created = query_get_value(select 1 as ok from oceanbase.__all_tenant where tenant_name = 'big_mem_tenant_oracle', ok, 1);

let $expected = 1;
if ($expected != $created) {
  set ob_query_timeout = 100000000;
  # create tenant if needed

  create resource unit big_mem_unit max_cpu 2, memory_size 10737418240, max_iops 100000;
  create resource pool big_mem_pool_mysql unit = 'big_mem_unit', unit_num = 1;
  create tenant big_mem_tenant_mysql replica_num = 1, resource_pool_list=('big_mem_pool_mysql') set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql', parallel_servers_target=10;
  --source mysql_test/include/check_tenant_sync.inc

  create resource pool big_mem_pool_oracle unit = 'big_mem_unit', unit_num = 1;
  create tenant big_mem_tenant_oracle replica_num = 1, resource_pool_list=('big_mem_pool_oracle') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle', parallel_servers_target=10;
  --source mysql_test/include/check_tenant_sync.inc

  let $zone_name = query_get_value(select primary_zone from oceanbase.__all_tenant where tenant_id = 1, primary_zone, 1);
  eval alter tenant big_mem_tenant_mysql  primary_zone = '$zone_name';
  eval alter tenant big_mem_tenant_oracle primary_zone = '$zone_name';

  alter tenant big_mem_tenant_mysql set variables ob_tcp_invited_nodes='%';
  alter tenant big_mem_tenant_oracle set variables ob_tcp_invited_nodes='%';
  alter tenant big_mem_tenant_oracle set variables autocommit='on';
  alter tenant big_mem_tenant_oracle set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
  alter tenant big_mem_tenant_oracle set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
  alter tenant big_mem_tenant_oracle set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

  connect (big_mem_mysql_root,$OBMYSQL_MS0,root@big_mem_tenant_mysql,,test,$OBMYSQL_PORT);
  create user 'admin' IDENTIFIED BY 'admin';
  grant all on *.* to 'admin' WITH GRANT OPTION;
  create database obproxy;

  create user 'proxyro' IDENTIFIED BY '3u^0kCdpE';
  grant select on oceanbase.* to proxyro IDENTIFIED BY '3u^0kCdpE';
  set GLOBAL ob_sql_work_area_percentage = 50;
  disconnect big_mem_mysql_root;

  connect (big_mem_oracle_sys,$OBMYSQL_MS0,SYS@big_mem_tenant_oracle,,SYS,$OBMYSQL_PORT);
  create user root IDENTIFIED BY root;
  grant all on *.* to root WITH GRANT OPTION;

  create user test IDENTIFIED BY test;
  grant all on *.* to test WITH GRANT OPTION;

  create user admin IDENTIFIED BY admin;
  grant all on *.* to admin WITH GRANT OPTION;
  set GLOBAL ob_sql_work_area_percentage = 50;
  disconnect big_mem_oracle_sys;

}

disconnect obsys;
disconnect default;

# connnect to tenant
if ($TENANT == 'mysql') {
  connect (default,$OBMYSQL_MS0,admin@big_mem_tenant_mysql,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
}
if ($TENANT == 'oracle') {
  connect (default,$OBMYSQL_MS0,admin@big_mem_tenant_oracle,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
}
--enable_query_log
