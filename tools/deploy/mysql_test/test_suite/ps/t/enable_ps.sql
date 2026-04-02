connect (conn_admin, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin;
alter system set _ob_enable_prepared_statement = true;
--sleep 1
connection default;
