#
# This test must examine structure of current system database
#
####ob��Щ��û��
-- replace_result Tables_in_mysql Tables_in_db Tables_in_test Tables_in_db
show tables;
show create table db;
show create table user;
show create table func;
--error 1146
show create table tables_priv;
#--error 1146
show create table columns_priv;
#--error 1146
show create table procs_priv;
--error 1146
show create table servers;
show create table proc;
--error 1146
show create table event;
--error 1146
show create table general_log;
--error 1146
show create table slow_log;
