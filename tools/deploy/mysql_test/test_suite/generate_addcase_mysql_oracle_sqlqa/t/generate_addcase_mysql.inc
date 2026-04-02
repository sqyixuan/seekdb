# owner: zuojiao.hzj
# owner group: sql2
# description: 【MYSQL生成列（虚拟列/存储列）】 
# tags: ddl,dml
# author: whh01047778


--echo # 环境清理
purge recyclebin;
--result_format 4

--disable_warnings
--error 0,1396
drop user whh@'%';
drop database if exists whh ;
--enable_warnings

create user whh identified by 'whh';
create database whh ;
grant all on *.* to whh WITH GRANT OPTION;

--source mysql_test/include/check_schema_sync.inc
connect (conn,$OBMYSQL_MS0,whh,whh,whh,$OBMYSQL_PORT);
connection conn;


--echo #############################################################################################
--echo ############ mysql_genrate-1-2 作为分区键
--echo ### key分区
--error 0,1051
drop table virtual_table;
create table virtual_table(
z0_col0 int, 
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1)) 
partition by key(z0_col2) partitions 2;

insert into virtual_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,222),(10,10);
select z0_col0,z0_col1,z0_col2 from virtual_table partition(p0) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from virtual_table partition(p1) order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from virtual_table where z0_col2 = 0 order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from virtual_table where z0_col0+z0_col1 = 0 order by z0_col2;

--error 0,1051
drop table stored_table;
create table stored_table(
z0_col0 int, 
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1) stored) 
partition by key(z0_col2) partitions 3;

insert into stored_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,222),(10,10);
select z0_col0,z0_col1,z0_col2 from stored_table partition(p0) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from stored_table partition(p1) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from stored_table partition(p2) order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from stored_table where z0_col2 = 0 order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from stored_table where z0_col0+z0_col1 = 0 order by z0_col2;


--echo ### COLUMNS分区
--error 0,1051
drop table virtual_table;
create table virtual_table(
z0_col0 int, 
z0_col1 int,
z0_col2 int as (abs(z0_col0-z0_col1)))
partition by range columns(z0_col2) (
partition p0 values less than (10),
partition p1 values less than (20),
partition p2 values less than maxvalue);

insert into virtual_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(5,25),(20,30),(100,10);
select z0_col0,z0_col1,z0_col2 from virtual_table partition(p0) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from virtual_table partition(p1) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from virtual_table partition(p2) order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from virtual_table where z0_col2 = 0 order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from virtual_table where abs(z0_col0-z0_col1) = 0 order by z0_col2;

--error 0,1051
drop table stored_table;
create table stored_table(
z0_col0 int,
z0_col1 int,
z0_col2 int as (abs(z0_col0-z0_col1)) stored)
partition by range columns(z0_col2) (
partition p0 values less than (10),
partition p1 values less than (20),
partition p2 values less than maxvalue);

insert into stored_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(5,25),(20,30),(100,10);
select z0_col0,z0_col1,z0_col2 from stored_table partition(p0) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from stored_table partition(p1) order by z0_col2;
select z0_col0,z0_col1,z0_col2 from stored_table partition(p2) order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from stored_table where z0_col2 = 0 order by z0_col2;
explain select z0_col0,z0_col1,z0_col2 from stored_table where abs(z0_col0-z0_col1) = 0 order by z0_col2;


#--echo ############ mysql_genrate-1-3 DDL
#--echo ### 临时表 
#--error 0,1051
#drop temporary table virtual_table1;
#create temporary table virtual_table1 (
#z0_col0 char(100), 
#z0_col1 varchar(100),
#z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) virtual,
#z0_col3 varchar(100) as (char_length(z0_col1)) virtual);
#
#insert into virtual_table1 (z0_col0,z0_col1) values (0,0),(NULL,NULL),('2022-06-21','ten'),(100,10.00);
#select z0_col0,z0_col1,z0_col2,z0_col3 from virtual_table1 order by z0_col2;
#
#--error 0,1051
#drop temporary table stored_table2;
#create temporary table stored_table2 (
#z0_col0 char(100), 
#z0_col1 varchar(100),
#z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) stored,
#z0_col3 varchar(100) as (char_length(z0_col1)) stored);
#
#insert into stored_table2 (z0_col0,z0_col1) values (0,0),(NULL,NULL),('2022-06-21','ten'),(100,10.00);
#select z0_col0,z0_col1,z0_col2,z0_col3 from stored_table2 order by z0_col2;
#
#drop temporary table virtual_table1;
#drop temporary table stored_table2;

--echo ### 视图
--echo # 单基表 含有虚拟列(VIRTUAL)的表
--error 0,1051
drop table virtual_table;
create table virtual_table(
z0_col0 char(100), 
z0_col1 varchar(100),
z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) virtual);

insert into virtual_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),('2022-06-21','ten'),(100,10.00);
select z0_col0,z0_col1,z0_col2 from virtual_table order by z0_col2;

--error 0,1051
drop view view1;
create view view1 as select z0_col0,z0_col1,z0_col2 from virtual_table where length(z0_col2)>1 order by 1;
select * from view1 order by 1;
insert into view1 (z0_col0,z0_col1) values ('view-','-insert');
select * from view1 order by 1;
--error 3105
update  view1 set z0_col2=0 where 1=1;


--echo # 单基表 含有生成列(STORED)的表
--error 0,1051
drop table stored_table;
create table stored_table (
z0_col0 char(100), 
z0_col1 varchar(100),
z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) stored);

insert into stored_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),('2022-06-21','ten'),(100,10.00);
select z0_col0,z0_col1,z0_col2 from stored_table order by z0_col2;

--error 0,1051
drop view view1;
create view view1 as select z0_col0,z0_col1,z0_col2 from stored_table where length(z0_col2)>1 order by 1;
select * from view1 order by z0_col2;
insert into view1 (z0_col0,z0_col1) values ('view-','-insert');
select * from view1 order by z0_col2;
--error 3105
update  view1 set z0_col2=0 where 1=1;


--echo # 单基表 普通的表，后追加生成列(VIRTUAL/STORED)
--error 0,1051
drop table stored_table;
create table stored_table (
z0_col0 char(100), 
z0_col1 varchar(100));

alter table stored_table add z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) virtual;
alter table stored_table add z0_col3 varchar(100) as (lower(concat(z0_col0,z0_col1))) stored;

insert into stored_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),('2022-06-21','ten'),('WANGHUANHUAN ','CHINA');
select z0_col0,z0_col1,z0_col2,z0_col3 from stored_table order by z0_col1;

--error 0,1051
drop view view1;
create view view1 as select * from stored_table;
select * from view1 order by z0_col2;
--error 3105
update  view1 set z0_col2=0 where 1=1;


--echo # 单基表 两个含有虚拟列(VIRTUAL)的表
--error 0,1051
drop table virtual_table1;
create  table virtual_table1 (
id int,
z0_col0 char(100), 
z0_col1 varchar(100),
z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) virtual);

insert into virtual_table1 (id,z0_col0,z0_col1) values (0,0,0),(1,NULL,NULL),(2,'virtual_table1','one'),(3,100,10.00);
select id,z0_col0,z0_col1,z0_col2 from virtual_table1 order by z0_col2;

--error 0,1051
drop table virtual_table2;
create  table virtual_table2 (
id int,
z1_col0 char(100), 
z1_col1 varchar(100),
z1_col2 varchar(100) as (concat(z1_col0,z1_col1)) virtual);

insert into virtual_table2 (id,z1_col0,z1_col1) values (0,0,0),(1,NULL,NULL),(2,'virtual_table2','two'),(3,200,20.00);
select id,z1_col0,z1_col1,z1_col2 from virtual_table2 order by z1_col2;

--error 0,1051
drop view view1;
create view view1 as select t1.z0_col0,t1.z0_col1,t1.z0_col2,t2.z1_col0,t2.z1_col1,t2.z1_col2 from virtual_table1 t1 inner join virtual_table2 t2 on t1.id=t2.id order by 1;
select * from view1 order by 1;
--error 1393
insert into view1 (z0_col0,z0_col1,z1_col0,z1_col1) values ('view-','-insert1','view-','-insert2');


--echo # 单基表 两个含有生成列(STORED)的表
--error 0,1051
drop table stored_table1;
create  table stored_table1 (
id int,
z0_col0 char(100), 
z0_col1 varchar(100),
z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) stored);

insert into stored_table1 (id,z0_col0,z0_col1) values (0,0,0),(1,NULL,NULL),(2,'stored_table1','one'),(3,100,10.00);
select id,z0_col0,z0_col1,z0_col2 from stored_table1 order by z0_col2;

--error 0,1051
drop table stored_table2;
create  table stored_table2 (
id int,
z1_col0 char(100), 
z1_col1 varchar(100),
z1_col2 varchar(100) as (concat(z1_col0,z1_col1)) stored);

insert into stored_table2 (id,z1_col0,z1_col1) values (0,0,0),(1,NULL,NULL),(2,'stored_table2','two'),(3,200,20.00);
select id,z1_col0,z1_col1,z1_col2 from stored_table2 order by z1_col2;

--error 0,1051
drop view view1;
create view view1 as select t1.z0_col0,t1.z0_col1,t1.z0_col2,t2.z1_col0,t2.z1_col1,t2.z1_col2 from stored_table1 t1 inner join stored_table2 t2 on t1.id=t2.id order by 1;
select * from view1 order by z0_col2;
--error 1393
insert into view1 (z0_col0,z0_col1,z1_col0,z1_col1) values ('view-','-insert1','view-','-insert2');


--echo # 单基表 一个含有虚拟列(VIRTUAL)的表，一个含有生成列(STORED)的表
--error 0,1051
drop table virtual_table1;
create  table virtual_table1 (
id int,
z0_col0 char(100), 
z0_col1 varchar(100),
z0_col2 varchar(100) as (concat(z0_col0,z0_col1)) virtual);

insert into virtual_table1 (id,z0_col0,z0_col1) values (0,0,0),(1,NULL,NULL),(2,'virtual_table1','one'),(3,100,10.00);
select id,z0_col0,z0_col1,z0_col2 from virtual_table1 order by z0_col2;

--error 0,1051
drop table stored_table2;
create  table stored_table2 (
id int,
z1_col0 char(100), 
z1_col1 varchar(100),
z1_col2 varchar(100) as (concat(z1_col0,z1_col1)) stored);

insert into stored_table2 (id,z1_col0,z1_col1) values (0,0,0),(1,NULL,NULL),(2,'stored_table2','two'),(3,200,20.00);
select id,z1_col0,z1_col1,z1_col2 from stored_table2 order by z1_col2;

--error 0,1051
drop view view1;
create view view1 as select t1.z0_col0,t1.z0_col1,t1.z0_col2,t2.z1_col0,t2.z1_col1,t2.z1_col2 from virtual_table1 t1 inner join stored_table2 t2 on t1.id=t2.id order by 1;
select * from view1 order by z0_col2;
--error 1393
insert into view1 (z0_col0,z0_col1,z1_col0,z1_col1) values ('view-','-insert1','view-','-insert2');


--echo ### ALTER TABLE 语句
--echo # RENAME 重命名表
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(floor(z0_col0+z0_col1)) stored
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from test_table order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table order by z0_col2;

--error 0,1051
drop table new_test_table;
alter table test_table rename to new_test_table;
insert into new_test_table (z0_col0,z0_col1) values (100,200);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from new_test_table order by z0_col2;


--echo # RENAME COLUMN 重命名生成列 ob不支持重命名列名，报ERROR 1064.
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as(abs(z0_col0)) virtual,
z0_col3 int as(power(z0_col1,2)) stored
);
insert into test_table1 (z0_col0,z0_col1) values (0,10),(-20,20),(3.50,30); 
select * from test_table1 order by z0_col2;


alter table test_table1 rename column z0_col2 to new_z0_col2;

alter table test_table1 rename column z0_col3 to new_z0_col3;
insert into new_test_table (z0_col0,z0_col1) values (5.5,10.6);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from new_test_table order by z0_col2;


--echo # RENAME COLUMN 重命名生成列引用的普通列
--error 3108
alter table test_table1 rename column z0_col0 to new_z0_col0;
insert into new_test_table (z0_col0,z0_col1) values (-40,5);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from new_test_table order by z0_col2;


--echo # MODIFY 修改生成列 虚拟生成列-》存储生成列
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0/z0_col1) virtual
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 1235, 3106
alter table test_table modify z0_col2 float as(z0_col0/z0_col1) stored;


--echo # MODIFY 修改生成列 虚拟生成列-》普通列
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0/z0_col1) virtual
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 1235, 3106
alter table test_table modify z0_col2 float;


--echo # MODIFY 修改生成列 存储生成列-》虚拟生成列
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0/z0_col1) stored
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;
--error 1235, 3106
alter table test_table modify z0_col2 float as(z0_col0/z0_col1) virtual;
show create table test_table;


--echo # MODIFY 修改生成列 存储生成列-》普通列
--echo # bug1:【Generated Columns】OB Mysql模式，将存储生成列更改为普通列,mysql成功，ob失败。
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0/z0_col1) stored
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 1235,3106
alter table test_table modify z0_col2 float;
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;
show create table test_table;


--echo # MODIFY 修改生成列 普通列-》虚拟生成列
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 1235,3106
alter table test_table modify z0_col2 float as(z0_col0/z0_col1) virtual;
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;
show create table test_table;


--echo # MODIFY 修改生成列 普通列-》存储生成列
--echo # bug2:【Generated Columns】OB Mysql模式，将普通列更改为存储生成列,mysql成功，ob失败。
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1.111,22.2),(10.5,10.9),(-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 1235,3106
alter table test_table modify z0_col2 float as(z0_col0/z0_col1) stored;
insert into test_table (z0_col0,z0_col1) values (10,40);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;
show create table test_table;


--echo # MODIFY 修改生成列的数据类型
--echo # bug3:【Generated Columns】OB Mysql模式，修改生成列的数据类型（modify/change）,mysql成功，ob失败。
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 float,
z0_col1 float,
z0_col2 int as(floor(z0_col0)) virtual,
z0_col3 int as(ceiling(z0_col1)) stored
);
insert into test_table1 (z0_col0,z0_col1) values (1.2,1.8),(2.4,2.6),(3.5,3.3); 
select * from test_table1 order by z0_col2;

alter table test_table1 modify z0_col2 double as(floor(z0_col0)) virtual;
--error 1235,3106
alter table test_table1 modify z0_col3 double as(ceiling(z0_col0)) stored;
insert into test_table1 (z0_col0,z0_col1) values (5.123,5.678); 
select * from test_table1 order by z0_col2;


--echo # MODIFY 修改生成列引用的普通列
alter table test_table1 modify z0_col0 double;
alter table test_table1 modify z0_col1 double;
insert into test_table1 (z0_col0,z0_col1) values (4.123,4.678); 
select * from test_table1 order by z0_col2;


--echo # CHANGE 修改生成列
--echo # bug3:【Generated Columns】OB Mysql模式，修改生成列的数据类型（modify/change）,mysql成功，ob失败。
drop table test_table1;
create table test_table1(
z0_col0 char(200),
z0_col1 char(200),
z0_col2 char(200) as(upper(z0_col0)) virtual,
z0_col3 char(200) as(concat(z0_col0,z0_col1)) stored
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one'),('oracle 11g ',' two'),('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

alter table test_table1 change z0_col2 z0_col22 varchar(200) as(upper(z0_col0)) virtual;
alter table test_table1 change z0_col3 z0_col33 varchar(200) as(concat(z0_col0,z0_col1)) stored;
insert into test_table1 (z0_col0,z0_col1) values (5,5); 
select * from test_table1 order by z0_col22;
show create table test_table1;


--echo # CHANGE 修改生成列引用的普通列
--echo # bug4:【Generated Columns】OB Mysql模式，修改生成列引用的普通列（change）,mysql失败，ob成功。
drop table test_table1;
create table test_table1(
z0_col0 char(200),
z0_col1 char(200),
z0_col2 char(200) as(upper(z0_col0)) virtual,
z0_col3 char(200) as(concat(z0_col0,z0_col1)) stored
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one'),('oracle 11g ',' two'),('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

alter table test_table1 change z0_col0 z0_col00 varchar(200);
alter table test_table1 change z0_col1 z0_col11 varchar(200);
insert into test_table1 (z0_col00,z0_col11) values ('pl/sql ',' add');
select * from test_table1 order by z0_col2; 
show create table test_table1;


--echo # DROP 删除生成列
drop table test_table1;
create table test_table1(
z0_col0 char(200),
z0_col1 char(200),
z0_col2 char(200) as(upper(z0_col0)) virtual,
z0_col3 char(200) as(concat(z0_col0,z0_col1)) stored
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one'),('oracle 11g ',' two'),('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

alter table test_table1 drop z0_col2;
alter table test_table1 drop z0_col3;
insert into test_table1 (z0_col0,z0_col1) values ('pl/sql ',' add');
select * from test_table1 order by z0_col0; 
show create table test_table1;


--echo # DROP 删除生成列引用的普通列
drop table test_table1;
create table test_table1(
z0_col0 char(200),
z0_col1 char(200),
z0_col2 char(200) as(upper(z0_col0)) virtual,
z0_col3 char(200) as(concat(z0_col0,z0_col1)) stored
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one'),('oracle 11g ',' two'),('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

--error 3108
alter table test_table1 drop z0_col0;
--error 3108
alter table test_table1 drop z0_col1;
insert into test_table1 (z0_col0,z0_col1) values ('pl/sql ',' add');
select * from test_table1 order by z0_col0; 
show create table test_table1;


--echo ############ mysql_genrate-1-4 DML
--echo ### SELECT 语句
--echo # LIMIT 子句
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(z0_col0*z0_col1) stored
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(2,9),(10,15),(-100,20);
--error 1064
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table order by z0_col2 limit z0_col2;
--error 1064
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table order by z0_col2 limit z0_col3;
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table order by z0_col2 limit 5;
select z0_col0,z0_col1,abs(z0_col2),z0_col3 from test_table order by z0_col2 limit 5;


--echo # SELECT ... INTO 语句
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(z0_col0*z0_col1) stored
);

insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(2,9),(10,15),(-100,20);
select z0_col2,z0_col3 into @p1,@p2 from test_table where abs(z0_col2) > 50 and z0_col3 != 0 order by z0_col2;
select @p1,@p2;

--echo # select 生成列间运算 from table;
--error 0,1051
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(z0_col0*z0_col1) stored
);

select avg(z0_col2),sum(z0_col3) from test_table;
insert into test_table (z0_col0,z0_col1) values (0,0),(NULL,NULL),(2,9),(10,15),(-100,20);

select z0_col0,z0_col1,z0_col2+2*9-1/2,z0_col3+2*9-1/2 from test_table order by z0_col2;
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table order by z0_col2;
select z0_col0,z0_col1,z0_col2+z0_col3 from test_table order by z0_col2;
select z0_col0,z0_col1+z0_col2,z0_col3 from test_table order by z0_col2;
select avg(z0_col2),sum(z0_col3) from test_table where z0_col2 > 0 order by z0_col2;
select z0_col2 > z0_col3 from test_table order by z0_col2;
select z0_col2 and z0_col3 from test_table order by z0_col2;


--echo # 子查询 作为标量操作数的子查询
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 double, 
z0_col1 double,
z0_col2 double as(z0_col0+z0_col1) virtual,
z0_col3 double as(z0_col0*z0_col1) stored
);

select (select z0_col2 from test_table1);
insert into test_table1 (z0_col0,z0_col1) values (0,0),(NULL,NULL),(2.5,9.9),(-10,15),(1.0E+10,2.0E+10);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;

--error 0,1051
drop table test_table2;
create table test_table2(
z0_col0 char(200), 
z0_col1 char(200), 
z0_col2 char(200) as(concat(z0_col0,z0_col1)) virtual,
z0_col3 char(200) as(upper(concat(z0_col0,z0_col1))) stored
);

select (select z0_col2 from test_table2);
insert into test_table2 (z0_col0,z0_col1) values (0,0),(NULL,NULL),(2022,0621),('2022-06-21 ',' 14:30:00'),('welloe come to ',' beijing');
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table2 order by z0_col2;

select (select z0_col2 from test_table1 where z0_col2 > 20 order by 1);
select (select z0_col3 from test_table1 where z0_col3 between 20 and 30 order by 1);
select (select z0_col2 from test_table2 where z0_col2 like '%to%' order by 1);
select (select z0_col3 from test_table2 where z0_col3 like '%TO%' order by 1);
select (select z0_col2 from test_table1 where z0_col2 > 20) from test_table2 order by z0_col2;
select lower((select z0_col3 from test_table2 where z0_col3 like '%TO%')) from test_table1 order by z0_col2;


--echo # 子查询 使用子查询进行比较
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int, 
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1) virtual,
z0_col3 int as(z0_col0-z0_col1) stored
);
insert into test_table1 (z0_col0,z0_col1) values (0,0),(NULL,NULL),(1,1),(1,2),(42.5,9.9),(-10,15),(2.0E+2,1.0E+2);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;

--error 0,1051
drop table test_table2;
create table test_table2(
z0_col0 int, 
z0_col1 int,
z0_col2 int as(z0_col0*z0_col1) virtual,
z0_col3 int as(z0_col0/z0_col1) stored
);
insert into test_table2 (z0_col0,z0_col1) values (0,0),(0,0),(1,1),(NULL,NULL),(29.5,9.9),(-10,15),(2.0E+2,1.0E+2);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table2 order by z0_col2;

select * from test_table1 where z0_col2 < (select max(z0_col2) from test_table2 where z0_col3 > 2) order by 1;
select * from test_table1 where z0_col2 >= (select max(z0_col2) from test_table2 where z0_col3 > 2) order by 1;
select * from test_table2 where z0_col1 = (select max(z0_col3) from test_table1) order by 1;
select * from test_table2 where z0_col1 != (select min(z0_col3) from test_table1) order by 1;
select * from test_table1 where 2 = (select count(*) from test_table2 where test_table2.z0_col0 = test_table1.z0_col0) order by 1;


--echo # 带有 ANY、IN 或 SOME 的子查询
select * from test_table1 where z0_col2 > any (select z0_col3 from test_table2 where z0_col2 > 0) order by 1;
select * from test_table1 where z0_col2 = any (select z0_col3 from test_table2 where z0_col2 > 0) order by 1;
select * from test_table1 where z0_col2 < any (select z0_col3 from test_table2 where z0_col2 > 0) order by 1;
select * from test_table1 where z0_col2 != any (select z0_col3 from test_table2 where z0_col2 > 0) order by 1;
select * from test_table1 where z0_col2 <> any (select z0_col3 from test_table2 where z0_col2 > 0) order by 1;
select max(z0_col2) from test_table1 where z0_col2 in (select z0_col3 from test_table2 where z0_col2 > 0) order by 1;
select * from test_table1 where z0_col3 >= some (select z0_col3 from test_table2 where z0_col3 > 0) order by 1;
select * from test_table1 where z0_col3 = some (select z0_col3 from test_table2 where z0_col3 > 0) order by 1;
select * from test_table1 where z0_col3 <= some (select z0_col3 from test_table2 where z0_col3 > 0) order by 1;
select * from test_table1 where z0_col3 != some (select z0_col3 from test_table2 where z0_col3 > 0) order by 1;


--echo # ALL 子查询
select z0_col0,z0_col1,abs(z0_col2),abs(z0_col3) from test_table1 where z0_col2 >= all (select z0_col3 from test_table2 where z0_col2 <> 0);
select z0_col0,z0_col1,max(z0_col2),max(z0_col3) from test_table1 where z0_col2 = all (select z0_col3 from test_table2 where z0_col2 <> 0);
select z0_col0,z0_col1,min(z0_col2),min(z0_col3) from test_table1 where z0_col2 <= all (select z0_col3 from test_table2 where z0_col2 <> 0);
select z0_col0,z0_col1,avg(z0_col2),avg(z0_col3) from test_table1 where z0_col2 <> all (select z0_col3 from test_table2 where z0_col2 <> 0);


--echo # 行子查询
select * from test_table1 where (z0_col2,z0_col3) = (select z0_col2, z0_col3 from test_table2 where z0_col2 is null) order by 1;
select * from test_table2 where row(z0_col2,z0_col3) = (select z0_col0, z0_col1 from test_table1 where z0_col2 = 2) order by 1;


--echo # 具有 EXISTS 或 NOT EXISTS 的子查询
select z0_col2,z0_col3 from test_table1 where exists (select * from test_table2 where z0_col3 is not null) order by 1;
select z0_col2,z0_col3 from test_table1 where not exists (select * from test_table2 where z0_col3 > 10) order by 1;
select count(z0_col2),max(z0_col3) from test_table1 where exists (select * from test_table2 where z0_col3 is not null) order by 1;
select distinct z0_col2,z0_col3 from test_table1 where not exists (select * from test_table2 where z0_col3 > 10) order by 1;


--echo # 相关子查询
select * from test_table1 t1 where z0_col2 = any (select z0_col2 from test_table2 t2 where t2.z0_col0 = t1.z0_col0 ) order by 1;
select * from test_table1 t1 where (select z0_col2 from test_table2 t2 where t2.z0_col1 = t1.z0_col1 and t2.z0_col3 = 3 ) > 0 order by 1;


--echo # 派生表
select avg(sum_column1) from (select sum(z0_col2) as sum_column1 from test_table1 group by z0_col3) as t1;
select avg(sum_column1) from (select sum(z0_col3) as sum_column1 from test_table2 group by z0_col2) as t1;


--echo ###  INSERT 语句
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int primary key,
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1) virtual,
z0_col3 int as(mod(z0_col0,z0_col1)) stored,
txt char(200) default 'test_table1'
);

insert into test_table1 (z0_col0,z0_col1) values (0,0),(1,NULL),(2,1),(3,5),(4,9.9),(5,2),(2.0E+2,1.0E+2);
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;

--error 3105
insert into test_table1 (z0_col0,z0_col1) values (5,5) on duplicate key update z0_col0=z0_col0+3,z0_col2=1;
insert into test_table1 (z0_col0,z0_col1) values (5,5) on duplicate key update z0_col0=z0_col0+3,txt = 'on duplicate key update';
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;


--echo ###  UPDATE  语句
update test_table1 set z0_col1 = 10, txt = '1 update z0_col1' where z0_col2 > 100;
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;

update test_table1 set z0_col0 = 41, txt = '2 update z0_col0' where z0_col3 = 4 order by z0_col3 limit 1;
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;

update test_table1 set z0_col0 = 4, txt = '3 update z0_col0' where abs(z0_col2) = 3 order by z0_col3 limit 1;
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;

--error 1064
update test_table1 set z0_col1 = 200 where z0_col2 > 10 order by z0_col3 limit z0_col3;
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;

--error 3105
update test_table1 set z0_col2 = 20 where z0_col3 = 4 order by z0_col3 limit 1;
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;


--echo ###  REPLACE  语句
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;
replace into test_table1 (z0_col0,z0_col1,txt) values (200,21,'1 replace test_table1');
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;

--error 3105
replace into test_table1 values (3,20,30,10,'2 replace test_table1');


--echo ###  TRUNCATE TABLE语句
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int primary key,
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1) virtual,
z0_col3 int as(z0_col0*z0_col1) stored,
txt char(200) default 'test_table1'
);

insert into test_table1 (z0_col0,z0_col1) values (0,0),(1,NULL),(2,1),(3,5);
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto,txt from test_table1 order by z0_col2;

truncate table test_table1;
insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,10),(30,50);
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;
desc test_table1;


--echo ############ mysql_genrate-1-5 PL/SQL
--echo ### 函数
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int primary key,
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1) virtual,
z0_col3 int as(z0_col0*z0_col1) stored
);

insert into test_table1 (z0_col0,z0_col1) values (0,0),(1,1),(2,2),(3,3); 
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;

--error 0,1051
drop table test_table2;
create table test_table2(
z0_col0 int primary key,
z0_col1 int,
z0_col2 int as(z0_col0+z0_col1) virtual,
z0_col3 int as(z0_col0*z0_col1) stored
);

insert into test_table2 (z0_col0,z0_col1) values (10,0),(11,1),(12,2),(13,3);
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table2 order by z0_col2;

DELIMITER //;

--echo # 生成列作为函数参数
--error 0,1305
drop function fun1;//
create function fun1(num2 int,num3 int) returns varchar(100)
begin  
  insert into test_table1 (z0_col0,z0_col1) values (num2,num3); 
  delete from test_table1 where z0_col2 = 0; 
  update test_table1 set z0_col0 = 9 where z0_col2 = 2;
  return 'fun1 ok';
end;
//

select fun1(z0_col2,z0_col3) from test_table2 where z0_col2 < 14 order by z0_col2;//
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;//


--echo # 虚拟列表达式是自定义函数
--error 0,1305
drop function fun1;//
create function fun1() returns int
begin
  return 10;
end;
//

--error 0,1051
drop table test_table222;//
--error 1054
create table test_table222(
z0_col0 int primary key,
z0_col1 int,
z0_col2 int as(fun1()) virtual,
z0_col3 int as(fun1()) stored
);//


--echo ### 存储过程
DROP PROCEDURE IF EXISTS pro1;//
CREATE PROCEDURE pro1(in num1 int,in num2 int)
BEGIN
  alter table test_table2 add z0_col4 varchar(100) as (concat(z0_col0,z0_col1)) virtual;
  alter table test_table2 add z0_col5 varchar(100) as (z0_col0) stored;
  insert into test_table2 (z0_col0,z0_col1) values (num1,num2),(num1+1,num2+1); 
  delete from test_table2 where z0_col2 = num1+num2; 
  update test_table2 set z0_col1 = 0 where z0_col3 = 36;
END;
//

call pro1(5,5);//
select * from test_table2 order by z0_col2;//
desc test_table2;//

DELIMITER ;//


--echo ############ mysql_genrate-1-6 其他场景
--echo ### 生成列：常量
--echo # null
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as (null) virtual,
z0_col3 int as (null) stored
);

insert into test_table1 (z0_col0,z0_col1) values (1,1),(2,2);
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;

--echo # ''/' '
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 char(200) as ('') virtual,
z0_col3 char(200) as (' ') stored
);

insert into test_table1 (z0_col0,z0_col1) values (1,1),(2,2);
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;

--echo # 0，小数，负数，日期
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as (0) virtual,
z0_col3 int as (3.1415926) stored,
z0_col4 int as (-100) virtual,
z0_col5 date as ('2020/06/22') stored
);

select * from test_table1 order by z0_col2;
insert into test_table1 (z0_col0,z0_col1) values (1,1),(2,2);
select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo # 特殊字符 ' ！@#￥%……&*（）-+'
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 char(200) as (' ！@#￥%……&*（）-+') virtual,
z0_col3 char(200) as (',./?;:|[]{}~$%^&') stored
);

select * from test_table1 order by z0_col2;
insert into test_table1 (z0_col0,z0_col1) values (1,1),(2,2);
select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo # 汉字，换行,\n,\t,\r
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 char(200) as ('虚拟列') virtual,
z0_col3 char(200) as ('生成列') stored,
z0_col4 char(200) as ('虚拟列\n\t\r转换符') virtual,
z0_col5 char(200) as ('生成列\n\t\r转换符') stored,
z0_col6 char(200) as ('虚拟列
第二行') virtual,
z0_col7 char(200) as ('生成列
第二行') stored
);

select * from test_table1 order by z0_col2;
insert into test_table1 (z0_col0,z0_col1) values (1,1),(2,2);
select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo ### 生成列：生成列上加默认值
--error 0,1051
drop table test_table1;
--error 1064
create table test_table1(
id int,
z0_col0 int,
z0_col1 int,
z0_col2 int as(z0_col0/z0_col1) virtual default '0',
z0_col3 int as(z0_col1*z0_col1) stored  default '0'
);

--echo ### 生成列：生成列引用生成列     ob不兼容MySQL
--error 0,1051
drop table test_table1;
--error 3106
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as(z0_col1) virtual,
z0_col3 int as(z0_col2) virtual
);
--error 1146
insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,20),(30,30); 
--error 1146
select * from test_table1 order by z0_col2;


--echo ### 表达式：生成列表达式 column / 0
--error 0,1051
drop table test_table1;
create table test_table1(
id int,
z0_col0 char(200) default '1',
z0_col1 char(200) default '2',
z0_col2 int as(z0_col0/0) virtual,
z0_col3 int as(z0_col1) stored
);

insert into test_table1 (id) values (1);
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo ### 表达式：生成列引用的普通列使用默认值
--error 0,1051
drop table test_table1;
create table test_table1(
id int,
z0_col0 int default 30,
z0_col1 int default 5,
z0_col2 int as(z0_col0/z0_col1) virtual,
z0_col3 int as(z0_col1*z0_col1) stored
);

insert into test_table1 (id) values (1);
select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo ### 事务：向表中插入/删除数据，添加生成列等，再回滚
SET @@AUTOCOMMIT=0;

--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as(z0_col0/z0_col1) virtual,
z0_col3 int as(z0_col1*z0_col1) stored
);
insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,20),(30,30); 
select * from test_table1 order by z0_col2;

--echo # 插入数据再回滚
start transaction;
  insert into test_table1 (z0_col0,z0_col1) values (40,40); 
  select * from test_table1 order by z0_col2;
rollback;

select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;

--echo # 删除数据再回滚
start transaction;
  delete from test_table1 where z0_col3 = 900; 
  select * from test_table1 order by z0_col2;
rollback;

select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;

--echo # 添加生成列再回滚
start transaction;
  alter table test_table1 add z0_col4 varchar(100) as (concat(z0_col0,z0_col1)) virtual;
  select * from test_table1 order by z0_col2;
rollback;

select z0_col0,z0_col1,z0_col2 col_vir,z0_col3 col_sto from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


SET @@AUTOCOMMIT=1;

--disable_parsing
--echo ### 预处理语句：向表中插入数据，添加生成列等
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as(z0_col0/z0_col1) virtual,
z0_col3 int as(z0_col1*z0_col1) stored
);
insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,20),(30,30); 
select * from test_table1 order by z0_col2;

prepare stmt1 from 'insert into test_table1 (z0_col0,z0_col1) values (40,40)';
execute stmt1;

prepare stmt1 from 'insert into test_table1 (z0_col0,z0_col1) values (?,?)';
set @p = 50;
execute stmt1 using @p,@p;

prepare stmt1 from 'alter table test_table1 add z0_col4 varchar(100) as (z0_col0) virtual;';
execute stmt1;

prepare stmt1 from 'alter table test_table1 add z0_col5 int as (z0_col0+z0_col1) stored;';
execute stmt1;

select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--error 3105
prepare stmt1 from 'insert into test_table1 (z0_col2,z0_col3) values (0,0)';
--error 1243
execute stmt1;

--enable_parsing

--echo ### 一个表中含有多个虚拟列/存储列(6,7,8...)
--error 0,1051
drop table test_table1;
create table test_table1(
z0_col0 int,
z0_col1 int,
z0_col2 int as(z0_col0*1) virtual,
z0_col3 int as(z0_col0*2) virtual,
z0_col4 int as(z0_col0*3) virtual,
z0_col5 int as(z0_col0*4) virtual,
z0_col6 int as(z0_col0*5) virtual,
z0_col7 int as(z0_col0*6) virtual,
z0_col10 int as(z0_col1*1) stored,
z0_col11 int as(z0_col1*2) stored,
z0_col12 int as(z0_col1*3) stored,
z0_col13 int as(z0_col1*4) stored,
z0_col14 int as(z0_col1*5) stored,
z0_col15 int as(z0_col1*6) stored
);
insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,20),(30,30); 
select * from test_table1 order by z0_col2;

alter table test_table1 add z0_col8 int as (z0_col0*7) virtual;
alter table test_table1 add z0_col9 int as (z0_col0*8) virtual;
alter table test_table1 add z0_col16 int as (z0_col1*7) stored;
alter table test_table1 add z0_col17 int as (z0_col1*8) stored;

insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,20),(30,30); 
select * from test_table1 order by z0_col2;
desc test_table1;

disconnect conn;
connection default;

--disable_warnings
--error 0,1396
drop user whh@'%';
drop database IF EXISTS whh ;
--enable_warnings

PURGE RECYCLEBIN;
