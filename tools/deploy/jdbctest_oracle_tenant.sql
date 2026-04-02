create user test identified by "test";
grant all on *.* to test with grant option;
grant dba to test;
set global ob_trx_timeout=3600000000;
set global max_allowed_packet=40000000;
set global ob_sql_work_area_percentage=100;
set global parallel_servers_target=600;
alter system set open_cursors = 1000;
set global autocommit=off;
