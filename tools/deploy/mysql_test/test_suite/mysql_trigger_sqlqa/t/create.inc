
--error 0,1008
drop database cwx_db;
--error 0,1008
drop database cwx_db1;
--error 0,1396
drop user cwx@'%';
--error 0,1396
drop user cwx1@'%';

--disable_warnings
--echo # 创建测试用户
create database cwx_db;
use cwx_db;
create user cwx identified by 'cwx';
grant all privileges on *.* to cwx;

create database cwx_db1;
use cwx_db1;
create user 'cwx1' identified by 'cwx1';
grant all privileges on *.* to 'cwx1';
--enable_warnings
