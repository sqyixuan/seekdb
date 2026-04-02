ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0, 942
drop table test;
--enable_warnings
create table test (c1 int , c2 char(5 char), c22 char(5 byte), c3 timestamp, c4 int, primary key(c1,c4)) partition by hash(c4) partitions 4;

#读多个partition,  更新一个partition
--disable_warnings
--error 0, 942
drop table test2;
--enable_warnings
create table test2(c1 int);
insert into test2 values(1);
insert into test2 values(2);
insert into test2 values(4);

--error 0,942
drop table test33;

create table test33(pk1 int, pk2 varchar(64 char), c1 int , c2 varchar(64 char), primary key(pk1, pk2)); 
insert into test33(pk1,pk2,c1,c2) values(0,'0',0,'0'); 
insert into test33(pk1,pk2,c1,c2) values(1,'1',1,'1');


--error 0,942
drop table t44;
--error 0,1418
drop index i1;
--error 0,1418
drop index i2;

create table t44 (pk int primary key, key1 int, key2 int, v1 varchar(20 char), v12 varchar(20 byte));
###create index i1 on t44(key1) storing(v1, v12);
###create index i2 on t44(key2) storing(pk);
insert into t44 values(1,1,1,'a','a');
insert into t44 values(2,1,1,'b','b');
insert into t44 values(3,2,2,'c','c');
insert into t44 values(4,3,3,'d','d');


--disable_warnings
--error 0, 942
drop table ts1;
--error 0, 942
drop table ts2;
--error 0, 942
drop table ts3;
--error 0, 942
drop table ts4;
--error 0, 942
drop table t1;
--error 0, 942
drop table t2;
--error 0, 942
drop table tp;
--enable_warnings
create table t1(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int);
create table tp(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int) partition by hash(c1) partitions 5;
create table ts1(c1 int, c2 int);
create table ts2(c1 int, c2 int);
create table ts3(c1 int, c2 int);
create table ts4(c1 int, c2 int);

--disable_query_log
insert into t1 select level, level, level, level, level, level, level, level , level from dual connect by level < 20;
insert into tp select * from t1;
insert into ts1 select level, level from dual connect by level < 5;
insert into ts2 select c1*2, c2*2 from ts1;
insert into ts3 select c1*3, c2*3 from ts1;
insert into ts4 select c1*4, c2*4 from ts1;
--enable_query_log


--error 0, 942
drop table t100;

CREATE TABLE t100
 (
 place_id int NOT NULL,
 shows int not null DEFAULT '0',
 ishows int not null DEFAULT '0',
 ushows int not null DEFAULT '0',
 clicks int not null DEFAULT '0',
 iclicks int not null DEFAULT '0',
 uclicks int not null DEFAULT '0',
 ts timestamp,
 PRIMARY KEY (place_id,ts)
 );
 
 
--error 0, 942
drop table t200;
--error 0, 942
drop table t300;
 
create table t200(f1 int, f2 int);
create table t300(f3 int, f4 int);
create index idx333 on t300(f3);

--error 0, 942
drop table t400;
create table t400(f1 int);


# Bug#14186 select datefield is null not updated
--error 0,942
drop table t500;
create table t500 (f1 date not null);

--error 0,942
drop table t600;
# Bug #16510 Updating field named like '*name' caused server crash
create table t600(f1 int, "*f2" int);

--error 0,942
drop table t700;
CREATE TABLE t700 (
    pk int not null,
    primary key (pk)) ;


--disable_warnings
--error 0, 942
drop table t800;
--error 0, 942
drop table t900;
--error 0, 942
drop view v800;
--error 0, 942
drop view v900;
--disable_warnings

create table t800 (c1 int primary key, c2 int, c3 int);
create table t900 (c1 int primary key, c2 int, c3 int);
insert into t800 (c1, c2, c3) values (1, 1, 1);
insert into t900 (c1, c2, c3) values (1, 1, 1);
create view v800 as select * from t800;
create view v900 as select t800.c1 c1, t800.c2 c2, t800.c3 c3, t900.c1 c4, t900.c2 c5, t900.c3 c6 from t800, t900 where t800.c1 = t900.c1;


--error 0, 942
drop table tbl_t1;
CREATE TABLE tbl_t1(id INT NOT NULL,year INT,month INT) 
PARTITION BY RANGE(year) 
(PARTITION Dec2020 VALUES LESS THAN(2021), 
 PARTITION Dec2021 VALUES LESS THAN(2022));
INSERT INTO tbl_t1 VALUES(1,2019,2);
INSERT INTO tbl_t1 VALUES(2,2020,5);
INSERT INTO tbl_t1 VALUES(3,2020,10);
INSERT INTO tbl_t1 VALUES(4,2021,2);
INSERT INTO tbl_t1 VALUES(5,2021,5);
INSERT INTO tbl_t1 VALUES(6,2021,1);
SELECT * FROM tbl_t1;
SELECT * FROM tbl_t1 PARTITION(Dec2020);
CREATE OR REPLACE VIEW info_year AS SELECT * FROM tbl_t1 PARTITION(Dec2020);


COMMIT;