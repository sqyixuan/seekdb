disable_query_log;
--require mysql_test/r/true.require
select (support = 'YES' or support = 'DEFAULT') as `TRUE` from information_schema.engines where engine = 'csv';
enable_query_log;
