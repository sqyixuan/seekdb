ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0, 942
drop table sourceTable;
--error 0, 942
drop table targetTable;
--error 0, 942
drop view targetView;
--enable_warnings
--result_format 4

create table sourceTable(id int,sales int) partition by hash(id) partitions 4;
create table targetTable(id int,sales int) partition by hash(id) partitions 4;
create view targetView as select * from targetTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

--error 0, 942
drop table t1;
--error 0, 942
drop table t2;
--error 0, 2289
drop sequence s1;

create table t1 (c1 int, c2 int);
create table t2 (c1 int, c2 int);
create sequence s1;

--error 0, 942
drop table t11;
--error 0, 942
drop table t22;

create table t11 (c1 int, c2 int, 
                 c3 decimal generated always as (c1 + c2), 
                 c4 int generated always as ((c1 + c2) * (c1 - c2)), 
                 c5 varchar(64) generated always as (to_char(c1 - c2)),
                 c6 decimal generated always as (c1 + c2 + 0) virtual);
create table t22 (c1 int,c2 decimal);
insert into t22 values (1, 2.5), (2, 6.2), (3, 4.1);

select count(*) from t22;
select count(*) from t11;


--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu1;
--enable_warnings


--echo # 创建表	
create table stu(s1 number, s2 nvarchar2(10));
insert into stu values(1,'a');
insert into stu values(2,'b');
insert into stu values(3,'c');
insert into stu values(4,'d');
insert into stu values(10,'e');


create table stu1(s1 number, s2 nvarchar2(10));
insert into stu1 values(1,'a1');
insert into stu1 values(1,'b1');
insert into stu1 values(3,'c1');
insert into stu1 values(66,'66');


--disable_warnings
--error 0,942
drop table stu222;
--error 0,942
drop table stu333;
--enable_warnings


--echo # 创建表	
create table stu222(s1 nvarchar2(10), s2 nvarchar2(10));
insert into stu222 values(1,'a');
insert into stu222 values(2,'b');
insert into stu222 values(3,'c');
insert into stu222 values(4,'d');
insert into stu222 values(10,'e');


create table stu333(s1 nvarchar2(10), s2 nvarchar2(10));
insert into stu333 values(1,'a1');
insert into stu333 values(2,'b1');
insert into stu333 values(2,'bb');
insert into stu333 values(3,'c1');
insert into stu333 values(66,'66');
insert into stu333 values(100,100);

--disable_warnings
--error 0,942
drop table stu555;
--error 0,942
drop table stu444;
--enable_warnings

--echo # 创建表	
create table stu444(s1 number, s2 nvarchar2(6) unique, s3 nvarchar2(6) default null not null, s4 nvarchar2(6)) partition by hash(s1) partitions 4;
insert into stu444 values(1,1,1,1);
insert into stu444 values(2,2,2,2);
insert into stu444 values(3,3,3,3);

create table stu555(s1 number, s2 nvarchar2(6), s3 nvarchar2(6) default null, s4 nvarchar2(6)) partition by hash(s1) partitions 4;
insert into stu555 values(100,100,100,1);
insert into stu555 values(600,600,600,6);
insert into stu555 values(700,700,700,7);


--disable_warnings
--error 0,942
drop table stu666;
--error 0,942
drop table stu777;
--enable_warnings


--echo # 创建表, 添加数据
create table stu666(s1 number, s2 nvarchar2(6) default 62, s3 nvarchar2(6), s4 nvarchar2(6) default 84) partition by hash(s1) partitions 4;
insert into stu666 values(1,1,1,1);
insert into stu666 values(2,2,2,2);
insert into stu666 values(10,10,10,10);
insert into stu666 values(16,16,16,16);
insert into stu666 values(88,88,88,88);


create table stu777(s1 number, s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6)) partition by range(s1)(
partition p1 values less than(10),
partition p2 values less than(20),
partition p_all values less than(maxvalue)
);
insert into stu777 values(1,12,13,14);
insert into stu777 values(2,22,23,24);
insert into stu777 values(6,62,63,64);
insert into stu777 values(8,82,83,84);
insert into stu777 values(10,102,103,104);
insert into stu777 values(88,882,883,884);
insert into stu777 values(100,1002,1003,1004);


--disable_warnings
--error 0,942
drop table stu999;
--error 0,942
drop table stu888;
--enable_warnings

create table stu888(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6), s5 nvarchar2(6));
create table stu999(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6), s5 nvarchar2(6));

insert into stu888 values('a','b1','c1','d1','e1');
insert into stu888 values('a','b2','c2','d2','e1');
insert into stu888 values('a','b3','c3','d3','e2');
insert into stu888 values('a','b4','c4','d4','e2');
insert into stu888 values('b','b5','c5','d5','e1');
insert into stu999 values('a','bb','cc','dd','e1');



--disable_warnings
--error 0,942
drop table stu2222;
--error 0,942
drop table stu1111;
--enable_warnings


create table stu1111(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6));
create table stu2222(s1 nvarchar2(6) unique, s2 nvarchar2(6), s3 nvarchar2(6));


insert into stu1111(s1,s2) values(1,1);
insert into stu1111(s1,s2) values(2,2);
insert into stu1111 values(3,3,3);
insert into stu2222(s1,s2) values(10,10);
insert into stu2222 values(30,30,3);
insert into stu2222 values(20,20,20);


--disable_warnings
--error 0,942
drop table stu4444;
--error 0,942
drop table stu3333;
--enable_warnings

create table stu3333(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6));
create table stu4444(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6));

insert into stu3333(s1,s2) values(1,1);
insert into stu3333(s1,s2) values(2,2);
insert into stu3333 values(3,3,3);
insert into stu4444 values(1,10,10);
insert into stu4444 values(2,10,10);
insert into stu4444 values(3,30,30);
insert into stu4444 values(3,38,90);
insert into stu4444 values(55,55,20);
insert into stu4444 values(55,10,20);
insert into stu4444 values(55,66,66);
insert into stu4444 values(55,88,88);



--echo ######跨分区的数据提取及写入######
--disable_warnings
--error 0,942
drop table stu6666;
--error 0,942
drop table stu5555;
--enable_warnings


--echo # 创建表, 添加索引, 添加数据
create table stu5555(s1 number, s2 nvarchar2(6) default 62, s3 nvarchar2(6), s4 nvarchar2(6) default 84) partition by hash(s1) partitions 4;

create table stu6666(s1 number, s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6)) 
partition by range(s1)(
partition p1 values less than(10),
partition p2 values less than(20),
partition p_all values less than(maxvalue)
);

insert into stu6666 values(1,1,1,1);
insert into stu6666 values(6,6,6,1);
insert into stu6666 values(8,8,8,2);
insert into stu6666 values(11,11,11,1);
insert into stu6666 values(16,16,16,1);
insert into stu6666 values(18,18,18,2);
insert into stu6666 values(26,26,26,1);
insert into stu6666 values(28,28,28,2);

insert into stu5555 values(106,106,106,1);
insert into stu5555 values(108,108,108,108);
insert into stu5555 values(206,206,206,206);
insert into stu5555 values(208,208,208,2);


--disable_warnings
--error 0,942
drop table stu8888;
--error 0,942
drop table stu7777;
--enable_warnings

create table stu7777(s1 number, s2 nvarchar2(6), s3 nvarchar2(6));
create table stu8888(s1 number, s2 nvarchar2(6));

insert into stu7777 values(10,10,10);
insert into stu8888 values(100,10);
insert into stu8888 values(200,20);



COMMIT;