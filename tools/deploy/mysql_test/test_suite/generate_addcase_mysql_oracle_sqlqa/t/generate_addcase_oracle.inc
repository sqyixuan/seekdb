# owner: zuojiao.hzj
# owner group: sql2
# description: 【Oracle 生成列】 
# tags: ddl,dml
# author: whh01047778


--echo # 环境清理
purge recyclebin;
--result_format 4

--disable_warnings
--error 0,600
drop user whh cascade;
--enable_warnings

create user whh identified by whh;
grant all on *.* to whh with grant option;
grant dba to whh;
--source mysql_test/include/check_schema_sync.inc
connect (conn,$OBMYSQL_MS0,whh@oracle,whh,whh,$OBMYSQL_PORT);


--echo #############################################################################################
--echo ############ oracle_genrate-1-1 DDL
--echo ### 视图
--echo # 单基表 含有虚拟列(VIRTUAL)的表
--error 0,00942
drop table virtual_table;
create  table virtual_table (
z0_col0 char(50), 
z0_col1 varchar2(100),
z0_col2 varchar2(200) as (concat(z0_col0,z0_col1)) virtual);

insert into virtual_table (z0_col0,z0_col1) values (0,0);
insert into virtual_table (z0_col0,z0_col1) values (NULL,NULL);
insert into virtual_table (z0_col0,z0_col1) values ('2022-06-21','ten');
insert into virtual_table (z0_col0,z0_col1) values (100,10.00);
select z0_col0,z0_col1,z0_col2 from virtual_table order by z0_col2;

--error 0,00942
drop view view1;
create view view1 as select z0_col0,z0_col1,z0_col2 from virtual_table where length(z0_col2)>1 order by 1;
select * from view1 order by 1;
insert into view1 (z0_col0,z0_col1) values ('view-','-insert');
select * from view1;
--error 54017
update  view1 set z0_col2=0 where 1=1;


--echo # 单基表 普通的表，后追加生成列(VIRTUAL/virtual)
--error 0,00942
drop table virtual_table;
create table virtual_table (
z0_col0 char(50), 
z0_col1 varchar2(200));

alter table virtual_table add z0_col2 varchar2(250) as (concat(z0_col0,z0_col1)) virtual;
alter table virtual_table add z0_col3 varchar2(250) as (lower(concat(z0_col0,z0_col1))) virtual;

insert into virtual_table (z0_col0,z0_col1) values (0,0);
insert into virtual_table (z0_col0,z0_col1) values (NULL,NULL);
insert into virtual_table (z0_col0,z0_col1) values ('2022-06-21','ten');
insert into virtual_table (z0_col0,z0_col1) values ('WANGHUANHUAN ','CHINA');
select z0_col0,z0_col1,z0_col2,z0_col3 from virtual_table order by z0_col1;

--error 0,00942
drop view view1;
create view view1 as select * from virtual_table order by 1;
select * from view1 order by z0_col2;
--error 54017
update  view1 set z0_col2=0 where 1=1;


--echo # 多基表 两个含有虚拟列(VIRTUAL)的表
--error 0,00942
drop table virtual_table1;
create  table virtual_table1 (
id number(10),
z0_col0 char(50), 
z0_col1 varchar2(100),
z0_col2 varchar2(200) as (concat(z0_col0,z0_col1)) virtual);

insert into virtual_table1 (id,z0_col0,z0_col1) values (0,0,0);
insert into virtual_table1 (id,z0_col0,z0_col1) values (1,NULL,NULL);
insert into virtual_table1 (id,z0_col0,z0_col1) values (2,'virtual_table1','one');
insert into virtual_table1 (id,z0_col0,z0_col1) values (3,100,10.00);
select id,z0_col0,z0_col1,z0_col2 from virtual_table1 order by z0_col2;

--error 0,00942
drop table virtual_table2;
create  table virtual_table2 (
id number(10),
z1_col0 char(50), 
z1_col1 varchar2(100),
z1_col2 varchar2(200) as (concat(z1_col0,z1_col1)) virtual);

insert into virtual_table2 (id,z1_col0,z1_col1) values (0,0,0);
insert into virtual_table2 (id,z1_col0,z1_col1) values (1,NULL,NULL);
insert into virtual_table2 (id,z1_col0,z1_col1) values (2,'virtual_table2','two');
insert into virtual_table2 (id,z1_col0,z1_col1) values (3,200,20.00);
select id,z1_col0,z1_col1,z1_col2 from virtual_table2 order by z1_col2;

--error 0,00942
drop view view1;
create view view1 as select t1.z0_col0,t1.z0_col1,t1.z0_col2,t2.z1_col0,t2.z1_col1,t2.z1_col2 from virtual_table1 t1 inner join virtual_table2 t2 on t1.id=t2.id order by 1;
select * from view1 order by 1;
--error 01776
insert into view1 (z0_col0,z0_col1,z1_col0,z1_col1) values ('view-','-insert1','view-','-insert2');


--echo ### ALTER TABLE 语句
--echo # RENAME 重命名表
--error 0,00942
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(floor(z0_col0+z0_col1)) virtual
);

insert into test_table (z0_col0,z0_col1) values (0,0);
insert into test_table (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table (z0_col0,z0_col1) values (1.111,22.2);
insert into test_table (z0_col0,z0_col1) values (10.5,10.9);
insert into test_table (z0_col0,z0_col1) values (-100.5,20.9);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from test_table order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table order by z0_col2;

--error 0,00942
drop table new_test_table;
alter table test_table rename to new_test_table;
insert into new_test_table (z0_col0,z0_col1) values (100,200);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from new_test_table order by z0_col2;


--echo # RENAME COLUMN 重命名生成列
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(abs(z0_col0)) virtual,
z0_col3 number(10) as(power(z0_col1,2)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values (0,10);
insert into test_table1 (z0_col0,z0_col1) values (-20,20);
insert into test_table1 (z0_col0,z0_col1) values (3.50,30); 
select * from test_table1 order by z0_col2;


alter table test_table1 rename column z0_col2 to new_z0_col2;
alter table test_table1 rename column z0_col3 to new_z0_col3;
insert into new_test_table (z0_col0,z0_col1) values (5.5,10.6);
select z0_col0,z0_col1,z0_col2,abs(z0_col3) from new_test_table order by z0_col2;


--echo # RENAME COLUMN 重命名生成列引用的普通列  oracle失败，ob成功
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(abs(z0_col0)) virtual,
z0_col3 number(10) as(power(z0_col1,2)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values (0,10);

alter table test_table1 rename column z0_col0 to new_z0_col0;
insert into test_table1 (new_z0_col0,z0_col1) values (-40,5);
select new_z0_col0,z0_col1,z0_col2,abs(z0_col3) from test_table1 order by z0_col2;


--echo # MODIFY 修改生成列 虚拟生成列-》普通列  oracle成功（实际修改普通列没生效），ob失败
--error 0,00942
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0/z0_col1) virtual
);

insert into test_table (z0_col0,z0_col1) values (0,0);
insert into test_table (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table (z0_col0,z0_col1) values (1.111,22.2);
insert into test_table (z0_col0,z0_col1) values (10.5,10.9);
insert into test_table (z0_col0,z0_col1) values (-100.5,20.9);
--error 01476
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;
delete from test_table where z0_col0 = 0;
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 54027
alter table test_table modify z0_col2 number;
delete from test_table;
insert into test_table (z0_col0,z0_col1) values (100,20);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

SELECT DBMS_METADATA.GET_DDL('TABLE','TEST_TABLE') FROM DUAL;


--echo # MODIFY 修改生成列 普通列-》虚拟生成列
--error 0,00942
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float
);

insert into test_table (z0_col0,z0_col1) values (0,0);
insert into test_table (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table (z0_col0,z0_col1) values (1.111,22.2);
insert into test_table (z0_col0,z0_col1) values (10.5,10.9);
insert into test_table (z0_col0,z0_col1) values (-100.5,20.9);
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;

--error 54026
alter table test_table modify z0_col2 float as(z0_col0/z0_col1) virtual;
select z0_col0,z0_col1,z0_col2 from test_table order by z0_col2;


--echo # MODIFY 修改生成列的数据类型 oracle成功，ob失败
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 float,
z0_col1 float,
z0_col2 number(10) as(floor(z0_col0)) virtual,
z0_col3 number(10) as(ceil(z0_col1)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values (1.2,1.8);
insert into test_table1 (z0_col0,z0_col1) values (2.4,2.6);
insert into test_table1 (z0_col0,z0_col1) values (3.5,3.3); 
select * from test_table1 order by z0_col2;

--error 00600
alter table test_table1 modify z0_col2 decimal(8,2) as(floor(z0_col0)) virtual;
--error 00600
alter table test_table1 modify z0_col3 decimal(8,2) as(ceil(z0_col0)) virtual;
insert into test_table1 (z0_col0,z0_col1) values (5.123,5.678); 
select * from test_table1 order by z0_col2;


--echo # MODIFY 修改生成列引用的普通列
--error 00600
alter table test_table1 modify z0_col0 decimal(8,2);
--error 00600
alter table test_table1 modify z0_col1 decimal(8,2);
insert into test_table1 (z0_col0,z0_col1) values (4.123,4.678); 
select * from test_table1 order by z0_col2;


--echo # CHANGE 修改生成列
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 char(50),
z0_col1 char(50),
z0_col2 char(100) as(upper(z0_col0)) virtual,
z0_col3 char(100) as(concat(z0_col0,z0_col1)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one');
insert into test_table1 (z0_col0,z0_col1) values ('oracle 11g ',' two');
insert into test_table1 (z0_col0,z0_col1) values ('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

--error 00900
alter table test_table1 change z0_col2 z0_col22 varchar2(200) as(upper(z0_col0)) virtual;
--error 00900
alter table test_table1 change z0_col3 z0_col33 varchar2(200) as(concat(z0_col0,z0_col1)) virtual;
insert into test_table1 (z0_col0,z0_col1) values (5,5); 
select * from test_table1 order by z0_col2;


--echo # CHANGE 修改生成列引用的普通列
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 char(50),
z0_col1 char(50),
z0_col2 char(100) as(upper(z0_col0)) virtual,
z0_col3 char(100) as(concat(z0_col0,z0_col1)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one');
insert into test_table1 (z0_col0,z0_col1) values ('oracle 11g ',' two');
insert into test_table1 (z0_col0,z0_col1) values ('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

--error 00900
alter table test_table1 change z0_col0 z0_col00 varchar2(200);
--error 00900
alter table test_table1 change z0_col1 z0_col11 varchar2(200);


--echo # DROP 删除生成列
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 char(50),
z0_col1 char(50),
z0_col2 char(100) as(upper(z0_col0)) virtual,
z0_col3 char(100) as(concat(z0_col0,z0_col1)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one');
insert into test_table1 (z0_col0,z0_col1) values ('oracle 11g ',' two');
insert into test_table1 (z0_col0,z0_col1) values ('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

alter table test_table1 drop column z0_col2;
alter table test_table1 drop column z0_col3;
insert into test_table1 (z0_col0,z0_col1) values ('pl/sql ',' add');
select * from test_table1 order by z0_col0; 


--echo # DROP 删除生成列引用的普通列
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 char(50),
z0_col1 char(50),
z0_col2 char(100) as(upper(z0_col0)) virtual,
z0_col3 char(100) as(concat(z0_col0,z0_col1)) virtual
);
insert into test_table1 (z0_col0,z0_col1) values ('mysql 8.0 ',' one');
insert into test_table1 (z0_col0,z0_col1) values ('oracle 11g ',' two');
insert into test_table1 (z0_col0,z0_col1) values ('oceanbase 4.0 ',' three'); 
select * from test_table1 order by z0_col2;

--error 00600
alter table test_table1 drop column z0_col0;
--error 00600
alter table test_table1 drop column z0_col1;
insert into test_table1 (z0_col0,z0_col1) values ('pl/sql ',' add');
select * from test_table1 order by z0_col0; 


--echo ############ oracle_genrate-1-4 DML
--echo ### SELECT 语句
--echo # where rownum < xxx;
--error 0,00942
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(z0_col0*z0_col1) virtual
);

insert into test_table (z0_col0,z0_col1) values (0,0);
insert into test_table (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table (z0_col0,z0_col1) values (2,9);
insert into test_table (z0_col0,z0_col1) values (10,15);
insert into test_table (z0_col0,z0_col1) values (-100,20);

select z0_col0,z0_col1,z0_col2,z0_col3 from test_table where rownum < z0_col2 order by z0_col2;
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table where rownum < z0_col3  order by z0_col2;
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table where rownum < 5 order by z0_col2;
select z0_col0,z0_col1,abs(z0_col2),z0_col3 from test_table where rownum < 5 order by z0_col2;


--echo # select 生成列间运算 from table;
--error 0,00942
drop table test_table;
create table test_table(
z0_col0 float, 
z0_col1 float,
z0_col2 float as(z0_col0+z0_col1) virtual,
z0_col3 float as(z0_col0*z0_col1) virtual
);

select avg(z0_col2),sum(z0_col3) from test_table;
insert into test_table (z0_col0,z0_col1) values (0,0);
insert into test_table (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table (z0_col0,z0_col1) values (2,9);
insert into test_table (z0_col0,z0_col1) values (10,15);
insert into test_table (z0_col0,z0_col1) values (-100,20);

select z0_col0,z0_col1,z0_col2+2*9-1/2,z0_col3+2*9-1/2 from test_table order by z0_col2;
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table order by z0_col2;
select z0_col0,z0_col1,z0_col2+z0_col3 from test_table order by z0_col2;
select z0_col0,z0_col1+z0_col2,z0_col3 from test_table order by z0_col2;
select avg(z0_col2),sum(z0_col3) from test_table where z0_col2 > 0 order by z0_col2;


--echo # 子查询 
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10), 
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual
);

insert into test_table1 (z0_col0,z0_col1) values (0,0);
insert into test_table1 (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table1 (z0_col0,z0_col1) values (2.5,9.9);
insert into test_table1 (z0_col0,z0_col1) values (-10,15);
insert into test_table1 (z0_col0,z0_col1) values (1.0E+2,2.0E+2);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;

--error 0,00942
drop table test_table2;
create table test_table2(
z0_col0 number(10), 
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual
);

insert into test_table2 (z0_col0,z0_col1) values (10,3);
insert into test_table2 (z0_col0,z0_col1) values (10,20);
insert into test_table2 (z0_col0,z0_col1) values (100,200);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table2 order by z0_col2;

--echo # 单行子查询
select * from test_table1 where z0_col3 = (select z0_col3 from test_table2 where z0_col3 < 100) order by 1;
select * from test_table1 where z0_col2 = (select z0_col2 from test_table2 where z0_col2 > 100) order by 1;
select count(*) from test_table1 where z0_col3 = (select z0_col2 from test_table2 where z0_col3 > 1000) order by 1;
select max(z0_col2) from test_table1 where z0_col2 = (select z0_col2 from test_table2 where z0_col2 > 100) order by 1;


--echo # 多行子查询
select * from test_table1 where z0_col2 in (select z0_col2 from test_table2 ) order by 1;
select * from test_table1 where z0_col2 not in (select z0_col2 from test_table2) order by 1;
select * from test_table2 where z0_col3 in (select z0_col3 from test_table1 ) order by 1;
select * from test_table2 where z0_col3 not in (select z0_col3 from test_table1) order by 1;
select max(z0_col2) from test_table1 where z0_col2 in (select z0_col2 from test_table2 ) order by 1;
select max(z0_col3) from test_table2 where z0_col3 not in (select z0_col3 from test_table1) order by 1;


--echo # 多列子查询
select * from test_table1 where (z0_col2,z0_col3) in (select z0_col2,z0_col3 from test_table2 ) order by 1;
select * from test_table1 where (z0_col3,z0_col2) in (select z0_col3,z0_col2 from test_table2 ) order by 1;
select * from test_table1 where (z0_col2,z0_col3) not in (select z0_col2,z0_col3 from test_table2 ) order by 1;
select * from test_table1 where (z0_col3,z0_col2) not in (select z0_col3,z0_col2 from test_table2 ) order by 1;
select max(z0_col2) from test_table1 where (z0_col3,z0_col2) not in (select z0_col3,z0_col2 from test_table2 ) order by 1;
select max(z0_col3) from test_table1 where (z0_col3,z0_col2) in (select z0_col3,z0_col2 from test_table2 ) order by 1;


--echo # 内联视图子查询
select z0_col2,z0_col3 from (select z0_col2,z0_col3 from test_table2 order by z0_col2);
select z0_col2,z0_col3 from (select z0_col2,z0_col3 from test_table1 order by z0_col2);
select max(z0_col2),min(z0_col3) from (select z0_col2,z0_col3 from test_table1 order by z0_col2);
select z0_col2,z0_col3 from (select z0_col2,z0_col3 from test_table1 order by z0_col2) WHERE rownum<=4;


--echo ###  UPDATE  语句
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10), 
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual,
txt char(50) default 'test_table1'
);
insert into test_table1 (z0_col0,z0_col1) values (0,0);
insert into test_table1 (z0_col0,z0_col1) values (NULL,NULL);
insert into test_table1 (z0_col0,z0_col1) values (10,10);
insert into test_table1 (z0_col0,z0_col1) values (20,20);
insert into test_table1 (z0_col0,z0_col1) values (50,60);
select * from test_table1 order by z0_col2;


update test_table1 set z0_col1 = 30, txt = '1 update z0_col1' where z0_col2 > 100;
select z0_col0,z0_col1,z0_col2,z0_col3,txt from test_table1 order by z0_col2;

update test_table1 set z0_col1 = 5, txt = '2 update z0_col0' where z0_col3 = 100;
select z0_col0,z0_col1,z0_col2,z0_col3,txt from test_table1 order by z0_col2;

--error 54017
update test_table1 set z0_col2 = 200 where z0_col2 > 500;
select z0_col0,z0_col1,z0_col2,z0_col3,txt from test_table1 order by z0_col2;

--error 54017
update test_table1 set z0_col3 = 20 where z0_col3 = 0;
select z0_col0,z0_col1,z0_col2,z0_col3,txt from test_table1 order by z0_col2;


--echo ###  TRUNCATE TABLE语句
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10) primary key,
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual,
txt char(200) default 'test_table1'
);

insert into test_table1 (z0_col0,z0_col1) values (0,0);
insert into test_table1 (z0_col0,z0_col1) values (1,NULL);
insert into test_table1 (z0_col0,z0_col1) values (2,1);
insert into test_table1 (z0_col0,z0_col1) values (3,5);
select z0_col0,z0_col1,z0_col2,z0_col3,txt from test_table1 order by z0_col2;

truncate table test_table1;
insert into test_table1 (z0_col0,z0_col1) values (10,10);
insert into test_table1 (z0_col0,z0_col1) values (20,10);
insert into test_table1 (z0_col0,z0_col1) values (30,50);
select z0_col0,z0_col1,z0_col2 col_add,z0_col3 col_mod,txt from test_table1 order by z0_col2;
desc test_table1;


--echo ############ oracle_genrate-1-5 PL/SQL
--echo ### 函数
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10) primary key,
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual
);

insert into test_table1 (z0_col0,z0_col1) values (0,0);
insert into test_table1 (z0_col0,z0_col1) values (1,1);
insert into test_table1 (z0_col0,z0_col1) values (2,2);
insert into test_table1 (z0_col0,z0_col1) values (3,3); 
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;

--error 0,00942
drop table test_table2;
create table test_table2(
z0_col0 number(10) primary key,
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual
);

insert into test_table2 (z0_col0,z0_col1) values (10,0);
insert into test_table2 (z0_col0,z0_col1) values (11,1);
insert into test_table2 (z0_col0,z0_col1) values (12,2);
insert into test_table2 (z0_col0,z0_col1) values (13,3);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table2 order by z0_col2;

DELIMITER //;

--error 0,00600
drop function fun1;//
create function fun1(num2 in number,num3 in number) return varchar2 is
begin
  insert into test_table1 (z0_col0,z0_col1) values (num2,num3); 
  delete from test_table1 where z0_col2 = 0; 
  update test_table1 set z0_col0 = 9 where z0_col2 = 2;
  return 'fun1 ok';
end;
//

declare
  var varchar2(20);
begin
  var := fun1(4,4);
end;
//
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;


--error 0,00600
drop function fun1;//
create function fun1 return number deterministic is
begin
  return 20;
end;
//

--echo # oracle支持，ob不支持
--error 0,00942
drop table test_table2;//
--error 00600
create table test_table2(
z0_col0 number(10) primary key,
z0_col1 number(10),
z0_col2 number(10) as(fun1()) virtual
);
//

--echo ### 存储过程
--error 0,00942
drop table test_table2;//
create table test_table2(
z0_col0 number(10) primary key,
z0_col1 number(10),
z0_col2 number(10) as(z0_col0+z0_col1) virtual,
z0_col3 number(10) as(z0_col0*z0_col1) virtual
);//

insert into test_table2 (z0_col0,z0_col1) values (10,0);//
insert into test_table2 (z0_col0,z0_col1) values (11,1);//
insert into test_table2 (z0_col0,z0_col1) values (12,2);//
insert into test_table2 (z0_col0,z0_col1) values (13,3);//
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table2 order by z0_col2;//

create or replace procedure pro1(num1 number,num2 number) as
begin
  execute immediate 'alter table test_table2 add z0_col4 varchar2(200) as (concat(z0_col0,z0_col1)) virtual';
  insert into test_table2 (z0_col0,z0_col1) values (num1,num1); 
  insert into test_table2 (z0_col0,z0_col1) values (num2,num2); 
  delete from test_table2 where z0_col2 = num2+num2; 
  update test_table2 set z0_col1 = num1+num2 where z0_col3 = num1+num1;
end;
//

call pro1(15,16);//
select * from test_table2 order by z0_col2;//
desc test_table2;//

DELIMITER ;//


--echo ############ oracle_genrate-1-6 其他场景
--echo ### 生成列：常量
--echo # null
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as (null) virtual
);

insert into test_table1 (z0_col0,z0_col1) values (1,1);
insert into test_table1 (z0_col0,z0_col1) values (2,2);
select z0_col0,z0_col1,z0_col2 from test_table1 order by z0_col2;
select max(z0_col2) from test_table1 order by z0_col2;

--echo # ''/' '
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 char(200) as ('') virtual
);

insert into test_table1 (z0_col0,z0_col1) values (1,1);
insert into test_table1 (z0_col0,z0_col1) values (2,2);
select z0_col0,z0_col1,z0_col2 from test_table1 order by z0_col0;
select max(z0_col2) from test_table1 order by z0_col2;

--echo # 0，小数，负数，日期
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as (0) virtual,
z0_col3 number(10) as (3.1415926) virtual,
z0_col4 number(10) as (-100) virtual
);

select * from test_table1 order by z0_col2;
insert into test_table1 (z0_col0,z0_col1) values (1,1);
insert into test_table1 (z0_col0,z0_col1) values (2,2);
select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo # 特殊字符 ' ！@#￥%……&*（）-+'
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 char(200) as (' ！@#￥%……&*（）-+') virtual,
z0_col3 char(200) as (',./?;:|[]{}~$%^&') virtual
);

select * from test_table1 order by z0_col2;
insert into test_table1 (z0_col0,z0_col1) values (1,1);
insert into test_table1 (z0_col0,z0_col1) values (2,2);
select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo # 汉字，换行,\n,\t,\r
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 char(200) as ('虚拟列') virtual,
z0_col3 char(200) as ('生成列') virtual,
z0_col4 char(200) as ('虚拟列\n\t\r转换符') virtual,
z0_col5 char(200) as ('生成列\n\t\r转换符') virtual,
z0_col6 char(200) as ('虚拟列
第二行') virtual,
z0_col7 char(200) as ('生成列
第二行') virtual
);

select * from test_table1 order by z0_col2;
insert into test_table1 (z0_col0,z0_col1) values (1,1);
insert into test_table1 (z0_col0,z0_col1) values (2,2);
select * from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;


--echo ### 生成列：生成列上加默认值
--error 0,00942
drop table test_table1;
--error 00900
create table test_table1(
id number(10),
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(z0_col0/z0_col1) virtual default '0',
z0_col3 number(10) as(z0_col1*z0_col1) virtual  default '0'
);

--echo ### 生成列：生成列引用生成列     ob不兼容MySQL
--error 0,00942
drop table test_table1;
--error 54016
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(z0_col1) virtual,
z0_col3 number(10) as(z0_col2) virtual
);
--error 00942
insert into test_table1 (z0_col0,z0_col1) values (10,10),(20,20),(30,30); 
--error 00942
select * from test_table1 order by z0_col2;


--echo ### 表达式：生成列表达式 column / 0
--error 0,00942
drop table test_table1;
--error 54016
create table test_table1(
id number(10),
z0_col0 char(200) default '1',
z0_col1 char(200) default '',
z0_col2 number(10) as(z0_col0/0) virtual,
z0_col3 number(10) as(z0_col1) virtual
);


--echo ### 表达式：生成列引用的普通列使用默认值
--error 0,00942
drop table test_table1;
create table test_table1(
id number(10),
z0_col0 number(10) default 30,
z0_col1 number(10) default 5,
z0_col2 number(10) as(z0_col0/z0_col1) virtual,
z0_col3 number(10) as(z0_col1*z0_col1) virtual
);

insert into test_table1 (id) values (1);
select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;

delimiter //;

--echo ### 事务：向表中插入/删除数据，再回滚
--error 0,00942
drop table test_table1;//
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(z0_col0/z0_col1) virtual,
z0_col3 number(10) as(z0_col1*z0_col1) virtual
);//
insert into test_table1 (z0_col0,z0_col1) values (10,10);//
insert into test_table1 (z0_col0,z0_col1) values (20,20);//
insert into test_table1 (z0_col0,z0_col1) values (30,30); //
select * from test_table1 order by z0_col3;//

--echo # 插入数据再回滚
begin
  savepoint tran_seq1;
  insert into test_table1 (z0_col0,z0_col1) values (40,40); 
  rollback to tran_seq1;
end;
//

select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col3;//
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col3;//

--echo # 删除数据再回滚
begin
  savepoint tran_seq2;
  delete from test_table1 where z0_col3 = 900; 
  rollback to tran_seq2;
end;
//

select z0_col0,z0_col1,z0_col2,z0_col3 from test_table1 order by z0_col2;//
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;//


--echo ### 预处理语句：向表中插入数据，添加生成列等
--error 0,00942
drop table test_table1;//
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(z0_col0/z0_col1) virtual,
z0_col3 number(10) as(z0_col1*z0_col1) virtual
);//
insert into test_table1 (z0_col0,z0_col1) values (10,8);//
insert into test_table1 (z0_col0,z0_col1) values (20,4);//
insert into test_table1 (z0_col0,z0_col1) values (30,7); //
select * from test_table1 order by z0_col2;//

begin
 execute immediate 'insert into test_table1 (z0_col0,z0_col1) values (40,6)';
 commit;
end;
//

begin
 execute immediate 'alter table test_table1 add z0_col4 number(10) as (z0_col0+z0_col1) virtual';
 commit;
end;
//

select * from test_table1 order by z0_col2;//
select max(z0_col2),min(z0_col3) from test_table1 order by z0_col2;//


--error 00900
execute immediate 'insert into test_table1 (z0_col2,z0_col3) values (0,0)';//


delimiter ;//

--echo ### 一个表中含有多个虚拟列/存储列(6,7,8...)
--error 0,00942
drop table test_table1;
create table test_table1(
z0_col0 number(10),
z0_col1 number(10),
z0_col2 number(10) as(z0_col0*1) virtual,
z0_col3 number(10) as(z0_col0*2) virtual,
z0_col4 number(10) as(z0_col0*3) virtual,
z0_col5 number(10) as(z0_col0*4) virtual,
z0_col6 number(10) as(z0_col0*5) virtual,
z0_col7 number(10) as(z0_col0*6) virtual,
z0_col10 number(10) as(z0_col1*1) virtual,
z0_col11 number(10) as(z0_col1*2) virtual,
z0_col12 number(10) as(z0_col1*3) virtual,
z0_col13 number(10) as(z0_col1*4) virtual,
z0_col14 number(10) as(z0_col1*5) virtual,
z0_col15 number(10) as(z0_col1*6) virtual
);
insert into test_table1 (z0_col0,z0_col1) values (10,10);
insert into test_table1 (z0_col0,z0_col1) values (20,20);
insert into test_table1 (z0_col0,z0_col1) values (30,30); 
select * from test_table1 order by z0_col2;

alter table test_table1 add z0_col8 number(10) as (z0_col0*7) virtual;
alter table test_table1 add z0_col9 number(10) as (z0_col0*8) virtual;
alter table test_table1 add z0_col16 number(10) as (z0_col1*7) virtual;
alter table test_table1 add z0_col17 number(10) as (z0_col1*8) virtual;

insert into test_table1 (z0_col0,z0_col1) values (40,40);
insert into test_table1 (z0_col0,z0_col1) values (50,50);
insert into test_table1 (z0_col0,z0_col1) values (60,60); 
select * from test_table1 order by z0_col2;
desc test_table1;


disconnect conn;
connection default;

--disable_warnings
--error 0,600
drop user whh cascade;
--enable_warnings

PURGE RECYCLEBIN;