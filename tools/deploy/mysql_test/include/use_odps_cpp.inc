connect(sys_conn,$OBMYSQL_MS0,root@sys,,oceanbase,$OBMYSQL_PORT);
connect(mysql_conn,$OBMYSQL_MS0,root,,test,$OBMYSQL_PORT);

connection sys_conn;
alter system set _use_odps_jni_connector = false;
connection mysql_conn;
