use oceanbase;
create user if not exists 'admin' IDENTIFIED BY 'admin';
grant all on *.* to 'admin' WITH GRANT OPTION;
create database obproxy;

create user if not exists 'proxyro' IDENTIFIED BY '3u^0kCdpE';
grant select on oceanbase.* to proxyro IDENTIFIED BY '3u^0kCdpE';
system sleep 5;
ANALYZE TABLE OCEANBASE.__ALL_VIRTUAL_CORE_ALL_TABLE COMPUTE STATISTICS;
ANALYZE TABLE OCEANBASE.__ALL_VIRTUAL_CORE_COLUMN_TABLE COMPUTE STATISTICS;