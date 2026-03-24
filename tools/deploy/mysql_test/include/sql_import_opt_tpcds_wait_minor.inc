connect (__conn0__,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,oceanbase,$OBMYSQL_PORT);
--real_sleep 20
let $tenant_id = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name = 'sys', tenant_id, 1);
let $database_id = query_get_value(select database_id from oceanbase.__all_virtual_database where database_name="OPT_TPCDS", database_id, 1);
let $__i__= 300;
while($__i__ > 0)
{
  sleep 1;
  dec $__i__;
  let $mini_cnt = query_get_value("select count(*) as c from oceanbase.__all_virtual_table_mgr where (table_type =11 or table_type = 12) and tenant_id=1 and tablet_id in (select tablet_id from oceanbase.__all_virtual_table where tenant_id=$tenant_id and database_id=$database_id) group by tablet_id,svr_ip,svr_port order by c desc limit 1", c, 1);
  if($mini_cnt == 1)
  {
  let $__i__ = -5;
  }
}
if($__i__ != -5)
{
  --echo minor failed
}
disconnect __conn0__;
