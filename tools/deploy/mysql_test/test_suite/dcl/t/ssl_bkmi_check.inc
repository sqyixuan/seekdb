connection obsys;

sleep 10;
--exec $mysql_login_cmd -e "select 'Con1 is alive';" 
--exec $mysql_login_cmd -e "drop table if exists t1; create table t1(c1 int);"
alter system set ssl_client_authentication = false;
sleep 10;
--exec $mysql_login_cmd -e "select 'Con1 is alive';" 
--exec $mysql_login_cmd -e "drop table if exists t1; create table t1(c1 int);"

