set global parallel_servers_target=600;
set global max_allowed_packet=67108864;
set global ob_plan_cache_percentage = 1;
set global ob_sql_work_area_percentage=70;
alter system set default_table_store_format='column';
create user test identified by "";

grant dba to test;
