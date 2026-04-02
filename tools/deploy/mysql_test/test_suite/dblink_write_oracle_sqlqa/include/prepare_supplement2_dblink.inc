ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0,942
drop table test_not_null;
--error 0, 942
drop table ta;
--error 0, 942
drop table t11;
--enable_warnings

create table test_not_null (c1 int primary key, c2 int not null, c3 int default 1, c4 char,c5 int);
insert into test_not_null (c1, c2) values (1,2);
insert into test_not_null (c1, c2) values (2,2.3);
insert into test_not_null (c1, c2) values (3.3,3.4);

create table ta (a integer primary key, b integer);
create table t11(c1 int default 12,c2 int default 13);


--disable_warnings
--error 0,942
drop table test;
--error 0,942
drop table test2;
--enable_warnings
######
create table test(c1 int, c2 int) partition by range(c1) (partition p0 values less than (100), partition p1 values less than (200),partition p2 values less than (300));

create table test2(c1 int, c2 int, c3 timestamp, primary key(c1,c2,c3))  partition by hash(c1) subpartition by range(c3) subpartition template (subpartition p0 values less than ('2016-01-01'), subpartition p1 values less than ('2116-01-01')) partitions 5;

--disable_warnings
--error 0, 942
drop table t1;
--error 0, 942
drop table t2;
--error 0, 942
drop table t3;
--enable_warnings

create table t1 (c1 int, c2 int, c3 int);
create table t2 (c1 int, c2 int, c3 int);
create table t3 (c1 int, c2 int, c3 int);

--disable_warnings
--error 0, 942
drop table t1_test1;
--error 0, 942
drop table t1_part;
--error 0, 942
drop table t2_test1;
--error 0, 942
drop table t2_part;
--error 0, 942
drop table t3_test1;
--error 0, 942
drop table t3_part;
--enable_warnings

create table t1_test1(a decimal, b char(10), c varchar(20), d decimal);
create table t1_part(a decimal, b char(10), c varchar(20), d decimal) partition by hash(a) partitions 4;
create table t2_test1(a decimal, b decimal, c decimal);
create table t2_part(a decimal, b decimal, c decimal) partition by hash(a) partitions 4;
create table t3_test1(a decimal, b char(10), c varchar(20));
create table t3_part(a decimal, b char(10) primary key, c varchar(20)) partition by hash(b) partitions 4;

--error 0, 942
drop table test1_test1;
--error 0, 942
drop table test2_test2;

create table test1_test1 (f1 int, f2 char(10), f3 varchar(10));
insert into test1_test1 values(1,'aaa','a1');
insert into test1_test1 values(2,'bbb','b1');
insert into test1_test1 values(null,null,null);
create table test2_test2(f1 int, f2 char(10), f3 varchar(10) default 'admin')
partition by range(f1) (partition p1 values less than(5), partition p2 values less than (maxvalue));

--error 0, 942
drop table t1_0915;
create table t1_0915 (a int, b int);

COMMIT;