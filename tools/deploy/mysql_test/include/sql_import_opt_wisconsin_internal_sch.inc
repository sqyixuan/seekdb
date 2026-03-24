if ($db_name == 'opt_wisconsin') {
if ($TENANT =='mysql') {
DROP DATABASE IF EXISTS `OPT_WISCONSIN`;
CREATE DATABASE `OPT_WISCONSIN`;
USE `OPT_WISCONSIN`;
}
if ($TENANT =='oracle') {
CREATE USER opt_wisconsin IDENTIFIED BY OPT_WISCONSIN;
GRANT ALL ON *.* TO opt_wisconsin;
ALTER SESSION SET CURRENT_SCHEMA=opt_wisconsin;
}
}
if ($db_name != 'opt_wisconsin') {
if ($TENANT == 'mysql') {
drop table if exists g_4k  ;
drop table if exists h_4k  ;
drop table if exists t1_1  ;
drop table if exists t1_100;
drop table if exists t2_1  ;
drop table if exists t2_100;
drop table if exists t_10k ;
drop table if exists t_4k  ;
drop table if exists t_5k  ;
drop table if exists t_part;
drop table if exists z_4k  ;
}
if ($TENANT == 'oracle') {
--error 0,942
drop table g_4k  ;
--error 0,942
drop table h_4k  ;
--error 0,942
drop table t1_1  ;
--error 0,942
drop table t1_100;
--error 0,942
drop table t2_1  ;
--error 0,942
drop table t2_100;
--error 0,942
drop table t_10k ;
--error 0,942
drop table t_4k  ;
--error 0,942
drop table t_5k  ;
--error 0,942
drop table t_part;
--error 0,942
drop table z_4k  ;
}
}
