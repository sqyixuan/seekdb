-- oracle mode run this sql
create user test2 identified by test2;
grant all on *.* to test2;
grant dba to test2;
set global ob_query_timeout=1000000000;
