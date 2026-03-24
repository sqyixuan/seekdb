ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink

--error 0,942
DROP TABLE stu150 CASCADE CONSTRAINTS purge;

CREATE TABLE stu150 (id int PRIMARY KEY, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias varchar2(50), birth DATE, weight binary_double,c_id int)
PARTITION BY RANGE(id)(PARTITION p1 VALUES less than (1004),PARTITION p2 VALUES less than (maxvalue));
INSERT INTO stu150 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu150 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu150 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu150 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu150 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu150 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu150 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE OR REPLACE synonym syn_stu1 FOR stu150;
CREATE OR REPLACE VIEW v_stu150 AS SELECT * FROM   stu150;

--error 0,942
DROP TABLE stu151 CASCADE CONSTRAINTS purge;
CREATE TABLE stu151 AS select * from stu150 where 1=0;

--error 0,942
DROP TABLE stu152 CASCADE CONSTRAINTS purge;
CREATE TABLE stu152 AS 
SELECT id||name basic,birth, trunc(LN(weight),5) other FROM stu150 WHERE id <=1002+p_id AND trunc(id/33,5)<1000 order by 1,2,3;

--error 0, 942
drop table class150;
CREATE TABLE class150 (id int, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY list(id) (PARTITION p1 VALUES(101,102),PARTITION p2 VALUES(DEFAULT));
INSERT INTO class150 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

--error 0, 942
drop table class233;
CREATE TABLE class233 (id int PRIMARY KEY, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY hash(id) PARTITIONS 2;
INSERT INTO class233 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE OR REPLACE synonym syn_class1 FOR class150;
CREATE OR REPLACE synonym syn_class2 FOR class233;


--disable_warnings
--ERROR 0,942
DROP TABLE class1;
--ERROR 0,942
DROP TABLE class2;
--ERROR 0,942
DROP TABLE class3;
--ERROR 0,942
DROP TABLE class4;
--ERROR 0,942
DROP TABLE class5;
--enable_warnings
CREATE TABLE class1 (id int PRIMARY KEY, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class1 VALUES(1, 'tiyu', 'hk', sysdate);
INSERT INTO class1 VALUES(2, 'yinyue', 'hk', sysdate);
CREATE TABLE class2 (id int PRIMARY KEY, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class2 VALUES(1, 'tiyu', 'bj', sysdate);
INSERT INTO class2 VALUES(2, 'yinyue', 'bj', sysdate);
CREATE TABLE class3 (id int);

CREATE TABLE class4 (id int);
INSERT INTO class4 VALUES(1);
INSERT INTO class4 VALUES(2);
INSERT INTO class4 VALUES(3);
INSERT INTO class4 VALUES(2);
CREATE TABLE class5 (id int);

CREATE OR REPLACE synonym syn_class3 FOR class3;

--disable_warnings
--ERROR 0,942
drop table EMP003;
--enable_warnings

create GLOBAL TEMPORARY TABLE EMP003(empno INTEGER,ename VARCHAR2(100),job VARCHAR2(100),mgr NUMBER,hiredate DATE, sal NUMBER(9,2),comm NUMBER(9,2),deptno NUMBER(20),deptnoname char(100)) on commit delete rows;

COMMIT;