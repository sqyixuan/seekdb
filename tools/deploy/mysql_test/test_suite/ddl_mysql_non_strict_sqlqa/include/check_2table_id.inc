#let $idx1 =query_get_value(select table_name from information_schema.tables where table_schema='ddl_test',table_name,1);
#上面table_schema为当前数据库名

#判断上面获取的是非空值
if($idx1)
{
	--source mysql_test/include/check_schema_sync.inc
	connect (conn_check2,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
	connection conn_check2;

	--disable_result_log
	eval SELECT table_id,table_name FROM oceanbase.__all_virtual_table WHERE table_name=('$idx1');
	--enable_result_log	

	--source mysql_test/include/check_schema_sync.inc
	connection default;

	let $idx2 =query_get_value(select table_name from information_schema.tables where table_schema='ddl_test',table_name,2);
	#判断上面获取的是非空值
	if($idx2)
	{
		--source mysql_test/include/check_schema_sync.inc
		connection conn_check2;

		--disable_result_log
		eval SELECT table_id,table_name FROM oceanbase.__all_virtual_table WHERE table_name=('$idx2');
		--enable_result_log

		--source mysql_test/include/check_schema_sync.inc
		connection default;
	}
	disconnect conn_check2;
}
