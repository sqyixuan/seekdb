set global max_allowed_packet=40000000;
set global ob_sql_work_area_percentage=100;
set global parallel_servers_target=600;
set global autocommit=off;
alter user SYS identified by "admin";
