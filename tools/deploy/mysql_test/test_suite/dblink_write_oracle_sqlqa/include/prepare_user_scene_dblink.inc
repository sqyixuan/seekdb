ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0,2024
DROP DATABASE link my_link1;
--error 0,2024
DROP DATABASE link my_link2;
--error 0,942
DROP TABLE class333_1 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE stu333_1 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE class333 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE stu333 CASCADE CONSTRAINTS purge;
--enable_warnings
purge recyclebin;

--echo #dblink-9-5   用户实际使用场景
--error 0,942
DROP TABLE class150 CASCADE CONSTRAINTS purge;

CREATE TABLE class150 (id int PRIMARY KEY, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY hash(id) PARTITIONS 2;
INSERT INTO class150 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));


--error 0,942
DROP TABLE stu150 CASCADE CONSTRAINTS purge;
CREATE TABLE stu150 (id int, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias varchar2(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu150 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu150 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu150 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu150 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu150 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu150 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu150 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);


--error 0,942
DROP TABLE class333 CASCADE CONSTRAINTS purge;

CREATE TABLE class333 (id int PRIMARY KEY, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY hash(id) PARTITIONS 2;
INSERT INTO class333 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));


--error 0,942
DROP TABLE stu333 CASCADE CONSTRAINTS purge;
CREATE TABLE stu333 (id int, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias varchar2(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu333 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu333 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu333 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu333 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu333 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu333 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu333 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);


--error 0,942
DROP TABLE testbak1 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak1 AS SELECT * FROM class150;

--error 0,942
DROP TABLE testbak2 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak2 AS SELECT * FROM stu150;

--error 0,942
DROP TABLE testbak3 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak3 AS SELECT * FROM class333;

--error 0,942
DROP TABLE testbak4 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak4 AS SELECT * FROM stu333;

--error 0,942
DROP TABLE testbak5 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak5(c1 varchar2(200));

--error 0,942
DROP TABLE testbak6 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak6(c1 varchar2(200),c2 varchar2(200));

COMMIT;