connect (root,$OBMYSQL_MS0,root,,test,$OBMYSQL_PORT);
connection root;
alter system set _enable_defensive_check = '1';
disconnect root;
connection default;
