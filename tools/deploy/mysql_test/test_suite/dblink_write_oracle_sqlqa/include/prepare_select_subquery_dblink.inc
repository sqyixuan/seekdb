ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink

--disable_warnings
--error 0,942
drop table t_deduce_length CASCADE CONSTRAINTS purge;
--error 0,942
drop table t_deduce_length2 CASCADE CONSTRAINTS purge;
--error 0,01418
drop index idx_1;
--error 0,01418
drop index idx_2;
--error 0,01418
drop index idx_3;
--error 0,01418
drop index idx_4;
--error 0,01418
drop index idx_5;
--error 0,01418
drop index idx_6;
--error 0,01418
drop index idx_7;
--error 0,01418
drop index idx_8;
--error 0,01418
drop index idx_9;
--error 0,01418
drop index idx_10;
--error 0,01418
drop index idx_11;
--error 0,01418
drop index idx_12;
--error 0,01418
drop index idx_13;
--error 0,01418
drop index idx_14;
--error 0,01418
drop index idx_15;
--error 0,01418
drop index idx_16;
--error 0,01418
drop index idx_17;
--error 0,01418
drop index idx_18;
--error 0,01418
drop index idx_19;
--error 0,01418
drop index idx_20;
--error 0,01418
drop index idx_21;
--error 0,01418
drop index idx_22;
--error 0,01418
drop index idx_23;
--error 0,01418
drop index idx_24;
--error 0,01418
drop index idx_25;
--error 0,01418
drop index idx_26;
--error 0,01418
drop index idx_27;
--error 0,01418
drop index idx_28;
--error 0,01418
drop index idx_29;
--error 0,01418
drop index idx_30;
--error 0,01418
drop index idx_31;
--error 0,01418
drop index idx_32;
--error 0,01418
drop index idx_33;
--error 0,01418
drop index idx_34;
--error 0,01418
drop index idx_35;
--error 0,01418
drop index idx_36;
--error 0,01418
drop index idx_37;
--error 0,01418
drop index idx_38;
--error 0,01418
drop index idx_39;
--error 0,01418
drop index idx_40;
--enable_warnings

create table t_deduce_length(
	c_int int,
    c_number number,
	c_number_l number(10,3),
	c_float float(5),
	c_binary_float BINARY_FLOAT,
	c_binary_double BINARY_DOUBLE,
	c_blob varchar2(200 char),
    c_date date,
    c_timestamp timestamp,
    c_raw raw(200),
    c_char char(200 byte),
    c_char_c char(200 char),
    c_varchar2 varchar2(200 byte),
    c_varchar2_c varchar2(200 char),
    c_varchar2_l varchar2(2000 byte),
    c_nchar nchar(200),
    c_nvarchar nvarchar2(200),
    c_nvarchar_l nvarchar2(2000),
    c_clob varchar2(200 char)
);

insert into t_deduce_length values(1,1,1,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(2,2,2,2.02,2.0,2.00,'a',to_date('2002-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试002','a','aa','中文测试001','a','bb','中文测试001','中文测试001');
insert into t_deduce_length values(3,3,3,3.03,3.0,3.00,'a',to_date('2003-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试003','a','bb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(4,4,4,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(5,5,5,5.05,5.0,5.00,'a',to_date('2005-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试005','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(6,6,6,6.06,6.0,6.00,'a',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','CC','中文测试006','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(7,7,7,7.07,7.0,7.00,'a',to_date('2007-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'C','aa','中文测试007','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(8,8,8,8.08,8.0,8.00,'a',to_date('2008-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aaabbb','中文测试008','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(9,9,9,9.09,9.0,9.00,'a',to_date('2009-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试009','a','aaabbb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(10,10,10,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(11,11,11,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(12,12,12,2.02,2.0,2.00,'a',to_date('2002-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试002','a','aa','中文测试001','a','bb','中文测试001','中文测试001');
insert into t_deduce_length values(13,13,13,3.03,3.0,3.00,'a',to_date('2003-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试003','a','bb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(14,14,14,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(15,15,15,5.05,5.0,5.00,'a',to_date('2005-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试005','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(16,16,16,6.06,6.0,6.00,'a',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','CC','中文测试006','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(17,17,17,7.07,7.0,7.00,'a',to_date('2007-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'C','aa','中文测试007','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(18,18,18,8.08,8.0,8.00,'a',to_date('2008-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aaabbb','中文测试008','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(19,19,19,9.09,9.0,9.00,'a',to_date('2009-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试009','a','aaabbb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(20,20,20,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(21,21,21,5.05,5.0,5.00,'a',to_date('2005-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试005','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(22,22,22,6.06,6.0,6.00,'a',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','CC','中文测试006','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(23,23,23,7.07,7.0,7.00,'a',to_date('2007-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'C','aa','中文测试007','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(24,24,24,8.08,8.0,8.00,'a',to_date('2008-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aaabbb','中文测试008','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(25,25,25,9.09,9.0,9.00,'a',to_date('2009-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试009','a','aaabbb','中文测试001','a','aa','中文测试001','中文测试001');

create table t_deduce_length2(
	c_int2 int,
    c_number2 number,
	c_number_l2 number(10,3),
	c_float2 float(5),
	c_binary_float2 BINARY_FLOAT,
	c_binary_double2 BINARY_DOUBLE,
	c_blob2 varchar2(200 char),
    c_date2 date,
    c_timestamp2 timestamp,
    c_raw2 raw(200),
    c_char2 char(200 byte),
    c_char_c2 char(200 char),
    c_varchar22 varchar2(200 byte),
    c_varchar2_c2 varchar2(200 char),
    c_varchar2_l2 varchar2(2000 byte),
    c_nchar2 nchar(200),
    c_nvarchar2 nvarchar2(200),
    c_nvarchar_l2 nvarchar2(2000),
    c_clob2 varchar2(200 char)
);

insert into t_deduce_length2 values(11,11,11,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(12,12,12,2.02,2.0,2.00,'a',to_date('2002-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试002','a','aa','中文测试001','a','bb','中文测试001','中文测试001');
insert into t_deduce_length2 values(13,13,13,3.03,3.0,3.00,'a',to_date('2003-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试003','a','bb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(14,14,14,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(15,15,15,5.05,5.0,5.00,'a',to_date('2005-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试005','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(16,16,16,6.06,6.0,6.00,'a',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','CC','中文测试006','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(17,17,17,7.07,7.0,7.00,'a',to_date('2007-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'C','aa','中文测试007','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(18,18,18,8.08,8.0,8.00,'a',to_date('2008-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aaabbb','中文测试008','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(19,19,19,9.09,9.0,9.00,'a',to_date('2009-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试009','a','aaabbb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(20,20,20,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(21,21,21,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(22,22,22,2.02,2.0,2.00,'a',to_date('2002-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试002','a','aa','中文测试001','a','bb','中文测试001','中文测试001');
insert into t_deduce_length2 values(23,23,23,3.03,3.0,3.00,'a',to_date('2003-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试003','a','bb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(24,24,24,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(25,25,25,5.05,5.0,5.00,'a',to_date('2005-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试005','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(26,26,26,6.06,6.0,6.00,'a',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','CC','中文测试006','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(27,27,27,7.07,7.0,7.00,'a',to_date('2007-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'C','aa','中文测试007','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(28,28,28,8.08,8.0,8.00,'a',to_date('2008-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aaabbb','中文测试008','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(29,29,29,9.09,9.0,9.00,'a',to_date('2009-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试009','a','aaabbb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(30,30,30,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(31,31,31,5.05,5.0,5.00,'a',to_date('2005-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试005','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(32,32,32,6.06,6.0,6.00,'a',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','CC','中文测试006','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(33,33,33,7.07,7.0,7.00,'a',to_date('2007-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'C','aa','中文测试007','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(34,34,34,8.08,8.0,8.00,'a',to_date('2008-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aaabbb','中文测试008','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(35,35,35,9.09,9.0,9.00,'a',to_date('2009-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试009','a','aaabbb','中文测试001','a','aa','中文测试001','中文测试001');


--disable_warnings
--error 0,942
drop table t_deduce_lengthbak1 CASCADE CONSTRAINTS purge;
--error 0,942
drop table t_deduce_lengthbak2 CASCADE CONSTRAINTS purge;
--error 0,942
drop table t_deduce_lengthbak3 CASCADE CONSTRAINTS purge;
--enable_warnings

CREATE TABLE t_deduce_lengthbak1 AS SELECT * FROM t_deduce_length WHERE 1=0;
CREATE TABLE t_deduce_lengthbak2 AS SELECT * FROM t_deduce_length2 WHERE 1=0;
CREATE TABLE t_deduce_lengthbak3(c1 varchar2(200));


--disable_warnings
--error 0,942
drop table EMP;
--error 0,942
drop table DEPT;
--enable_warnings

CREATE TABLE DEPT(DEPTNO NUMBER(2),DNAME VARCHAR2(14),LOC VARCHAR2(13));
CREATE TABLE EMP(EMPNO NUMBER(4) ,ENAME VARCHAR2(10),JOB VARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2));
INSERT INTO DEPT VALUES
	(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES
	(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES
	(40,'OPERATIONS','BOSTON');
INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO EMP VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO EMP VALUES
(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);

--disable_warnings
--error 0,942
drop table TESTBAK;
--enable_warnings

CREATE TABLE TESTBAK(JOB VARCHAR2(200),DEPT10 INT,DEPT20 INT,DEPT30 INT,DEPT40 INT);

--disable_warnings
--error 0,942
drop table EMPBAK;
--enable_warnings

CREATE TABLE EMPBAK AS SELECT * FROM EMP WHERE 1=0;

####基本数据，用于演示的基础###
create or replace view pivoted_emp as
select *
  from (select job,deptno,sal from emp)
  pivot (
          sum(sal)
          for deptno 
          in (10 as dept10,20 as dept20,30 as dept30,40 as dept40)
         );
 
select *from pivoted_emp;

--disable_warnings
--error 0,942
drop table TESTBAK2;
--enable_warnings

CREATE TABLE TESTBAK2(JOB VARCHAR2(200),DEPTNO INT,SAL INT);

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
CREATE TABLE class1 (id int , name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class1 VALUES(1, 'tiyu', 'hk', sysdate);
INSERT INTO class1 VALUES(2, 'yinyue', 'hk', sysdate);
CREATE TABLE class2 (id int , name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class2 VALUES(1, 'tiyu', 'bj', sysdate);
INSERT INTO class2 VALUES(2, 'yinyue', 'bj', sysdate);
CREATE TABLE class3 (id int);

CREATE TABLE class4 (id int);
INSERT INTO class4 VALUES(1);
INSERT INTO class4 VALUES(2);
INSERT INTO class4 VALUES(3);
INSERT INTO class4 VALUES(2);
CREATE TABLE class5 (id int);

--error 0, 942
drop table t1;
create table t1(a number, b varchar2(40));
insert into t1 values(1, 'a');
insert into t1 values(2, 'b');
insert into t1 values(3, null);
insert into t1 values(4, '中国');
commit;

--error 0, 942
drop table t2;
create table t2(a number, b varchar2(40), c TIMESTAMP, d TIMESTAMP WITH TIME ZONE, e TIMESTAMP WITH LOCAL TIME ZONE);
insert into t2 values(2, 'link t2', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
commit;

--error 0, 942
drop table t3;
create table t3(a number, b varchar2(40), c TIMESTAMP, d TIMESTAMP WITH TIME ZONE, e TIMESTAMP WITH LOCAL TIME ZONE, f INTERVAL YEAR TO MONTH, g INTERVAL DAY (6) TO SECOND (5));
insert into t3 values(2, 'link t3', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, INTERVAL '12-2' YEAR(3) TO MONTH, INTERVAL '4 5:12:10.222' DAY TO
SECOND(3));
commit;

--error 0, 942
drop table t4;
create table t4(a number, b varchar2(40), c raw(50));
insert into t4 values(3, 'link t4', hextoraw('127dae98'));
commit;

--error 0, 942
drop table t5;
create table t5(a BINARY_FLOAT, b BINARY_DOUBLE, c FLOAT);
desc t5;
insert into t5 values (3.14159268, 3.141592687, 3.14159268);
commit;

--error 0, 942
drop table t6;
create table t6(a varchar2(60), b nvarchar2(30));
insert into t6 values('varchar2', 'nvarchar2');
insert into t6 values('中国', '外国');
commit;

--error 0, 942
drop table t7;
create table t7(a number, b number);
insert into t7 (a, b) select level, 48 + mod(level, 78) from dual connect by level <= 500;
commit;

--error 0, 942
drop table t11;
--error 0, 942
drop table t12;
--error 0, 942
drop table t13;
--error 0, 942
drop table t14;
--error 0, 942
drop table t15;
--error 0, 942
drop table t16;
--error 0, 942
drop table t17;

create table t11 as select * from t1 where 1 = 0;
create table t12 as select * from t2 where 1 = 0;
create table t13 as select * from t3 where 1 = 0;
create table t14 as select * from t4 where 1 = 0;
create table t15 as select * from t5 where 1 = 0;
create table t16 as select * from t6 where 1 = 0;
create table t17 as select * from t7 where 1 = 0;



--disable_warnings
--error 0,942
DROP TABLE t111 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t222 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t333 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t444 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t555 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t666 CASCADE CONSTRAINTS purge;
--enable_warnings

CREATE TABLE t111(DEPTNO NUMBER(2) PRIMARY KEY,DNAME NVARCHAR2(14),LOC VARCHAR2(13));
INSERT INTO t111 VALUES(1,'AccounTing','New York');
INSERT INTO t111 VALUES (2,'ReSeaRch','DallAs');
INSERT INTO t111 VALUES(3,'SaLeS','ChiCaGo');
INSERT INTO t111 VALUES(4,'OperaTions','BoSton');

CREATE TABLE t222(EMPNO NUMBER(4) UNIQUE,ENAME NVARCHAR2(10) NOT NULL,JOB NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2) REFERENCES t111(DEPTNO),CHECK (sal > 500))
PARTITION BY RANGE(SAL) SUBPARTITION BY LIST(JOB)(PARTITION p1 VALUES less than(2500)(SUBPARTITION p1sub1 VALUES('CLERK'),SUBPARTITION p1sub2 VALUES('SALESMAN')),
PARTITION p2 VALUES less than(5000)(SUBPARTITION p2sub1 VALUES('MANAGER'),SUBPARTITION p2sub2 VALUES('ANALYST'),SUBPARTITION p2sub3 VALUES('PRESIDENT')	));
INSERT INTO t222 VALUES(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,100,2);
INSERT INTO t222 VALUES(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,3);
INSERT INTO t222 VALUES(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,3);
INSERT INTO t222 VALUES(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,358.69,2);
INSERT INTO t222 VALUES(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,3);
INSERT INTO t222 VALUES(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,3);
INSERT INTO t222 VALUES(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),4450,744.9,1);
INSERT INTO t222 VALUES(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,-200,2);
INSERT INTO t222 VALUES(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),4999,NULL,1);
INSERT INTO t222 VALUES(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,3);
INSERT INTO t222 VALUES(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,-99.9,2);
INSERT INTO t222 VALUES(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,-1808,3);
INSERT INTO t222 VALUES(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,753.9,2);
INSERT INTO t222 VALUES(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,729,1);

CREATE TABLE t333(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 NCHAR(1),C4 NUMBER,C5 VARCHAR2(1),C6 VARCHAR2(1),C7 NUMBER,C8 NCHAR(1))PARTITION BY RANGE(c1)SUBPARTITION BY HASH(c2)(PARTITION part10 VALUES LESS THAN(2)(SUBPARTITION p1sub3,SUBPARTITION p1sub4),
PARTITION part11 VALUES LESS THAN(5)(SUBPARTITION p1sub5,SUBPARTITION p1sub6));
INSERT INTO t333 VALUES  (1, 5, 'e', 8, NULL, 't', NULL, NULL) ;
INSERT INTO t333 VALUES  (2, 3, 'x', 2, 'q', 'k', 0, NULL) ;
INSERT INTO t333 VALUES  (2, 4, 'o', 5, 'w', 'u', 8,'n') ;
INSERT INTO t333 VALUES  (3, 4, 'h', 2, 'y', 'z', NULL, NULL) ;
INSERT INTO t333 VALUES  (4, 6, 'j', 2, 'h', 'p', 1,NULL) ;
INSERT INTO t333 VALUES  (4, 4, 'i', 2, 'd', 'x', 0,'f') ;
INSERT INTO t333 VALUES  (3, 9, 'e', 0, 'r', 'v', 6, 'd') ;
INSERT INTO t333 VALUES  (2, 9, 'y', 7, 'h', 'q', 2,'v') ;
INSERT INTO t333 VALUES  (1, 2, 'w', 1, 'c', 'l', 7,'o') ;
INSERT INTO t333 VALUES  (1, 9, 'y', 1, NULL, 's', 5, NULL) ;
INSERT INTO t333 VALUES  (2, 6, 'h', 7, NULL, 'f', 8, NULL) ;
INSERT INTO t333 VALUES  (4, 5, 'i', 2, NULL, 'w', NULL,'x') ;

CREATE TABLE t444(DEPTNO NUMBER(2) PRIMARY KEY,DNAME NVARCHAR2(14),LOC VARCHAR2(13));
INSERT INTO t444 SELECT * FROM t111 WHERE ROWNUM <= 3;

CREATE TABLE t555(EMPNO NUMBER(4) UNIQUE,ENAME NVARCHAR2(10) NOT NULL,JOB NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2),CHECK (sal > 500))PARTITION BY HASH(DEPTNO)(PARTITION part6,PARTITION part7,PARTITION part8);
INSERT INTO t555 SELECT * FROM(SELECT * FROM t222 ORDER BY empno) WHERE ROWNUM <= 6 ;

CREATE TABLE t666(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 NCHAR(1),C4 NUMBER,C5 VARCHAR2(1),C6 VARCHAR2(1),C7 NUMBER,C8 NCHAR(1));
INSERT INTO t666 SELECT * FROM (SELECT * FROM t333 WHERE c3 IN ('w','e','y','x','i','h') ORDER BY 1,2,3,4);




--error 0, 942
drop table test1;
CREATE TABLE test1(f1 NUMBER(10,5) , f2 binary_float, f3 char(20), f4 nvarchar2(20), f5 timestamp(9));
CREATE INDEX index_1 ON test1(f2);
INSERT INTO test1 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test1 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test1 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test1 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));
--error 0, 942
drop table test1_bak;
CREATE TABLE test1_bak AS SELECT * FROM test1 WHERE 1 = 0;

--error 0, 942
drop table test_1;
CREATE TABLE test_1(f1 NUMBER, f2 binary_float, f3 char(20), f4 nvarchar2(20), f5 timestamp(9));
INSERT INTO test_1 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test_1 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test_1 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test_1 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));
--error 0, 942
drop table test_1_bak;
CREATE TABLE test_1_bak AS SELECT * FROM test_1 WHERE 1 = 0;


--error 0, 942
drop table test2;
CREATE TABLE test2(
    f1 NUMBER(10,5) , 
    f2 binary_float, 
    f3 char(20) UNIQUE, 
    f4 nvarchar2(20) NOT NULL, 
    f5 timestamp(9) DEFAULT TO_DATE('25-09-2005 12:01:33','dd-mm-yyyy hh24:mi:ss'),
    f6 int)
PARTITION BY RANGE(f1) 
    (PARTITION p1 VALUES less than (30000),PARTITION p2 VALUES less than (40000),PARTITION p3 VALUES less than (MAXVALUE));
CREATE INDEX index_2 ON test2(f2) LOCAL;
CREATE INDEX index_3 ON test2(f6) GLOBAL PARTITION BY range(f6) (PARTITION p1 VALUES less than (1002),PARTITION p2 VALUES less than (MAXVALUE));
INSERT INTO test2 VALUES(36478.46372, 347.64273, '123456','fhaiu', DEFAULT,1001);
INSERT INTO test2 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_DATE('25-09-2006 12:01:44','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test2 VALUES(12345.67890, 483.35433, '456782','bfdsh', TO_DATE('25-09-2009 12:01:55','dd-mm-yyyy hh24:mi:ss'),1002);
INSERT INTO test2 VALUES(45678.64589, 354.58766, DEFAULT,'gregg', TO_DATE('25-09-2008 12:01:11','dd-mm-yyyy hh24:mi:ss'),1003);
--error 0, 942
drop table test2_bak;
CREATE TABLE test2_bak AS SELECT * FROM test2 WHERE 1 = 0;


--error 0, 942
drop table t1_group purge;
create table t1_group(c1 int, c2 int, c3 int);
insert into t1_group values (1, 2, 3), (2, 3, 4), (4, 5, 6);

--error 0, 942
drop table t1_group_bak purge;
CREATE TABLE t1_group_bak AS
select c1, c2, c3, grouping(c1) gc1, grouping(c2) gc2, grouping(c3) gc3 from t1_group group by rollup(c1, c2), c3;


--disable_warnings
--error 0,942
drop table tbl1;
--enable_warnings
CREATE TABLE tbl1(col1 INT, col2 INT, col3 INT);
INSERT INTO tbl1 VALUES(1, 0, -1);
INSERT INTO tbl1 VALUES(2, 1, -2);
INSERT INTO tbl1 VALUES(4, 2, -4);
INSERT INTO tbl1 VALUES(5, 2, -5);
INSERT INTO tbl1 VALUES(3, 1, -3);
INSERT INTO tbl1 VALUES(6, 3, -6);
INSERT INTO tbl1 VALUES(7, 3, -7);

--disable_warnings
--error 0,942
drop table tbl1_bak;
--enable_warnings

CREATE TABLE tbl1_bak(col1 int,col2 int,C3 VARCHAR2(200),C4 VARCHAR2(200),C5 VARCHAR2(200),C6 VARCHAR2(200),C7 VARCHAR2(200));

--disable_warnings
--error 0,942
drop table tbl2_bak;
--enable_warnings

CREATE TABLE tbl2_bak(col1 int,col2 int,C3 VARCHAR2(200),C4 VARCHAR2(200),C5 VARCHAR2(200),C6 VARCHAR2(200) );


--disable_warnings
--error 0,942
drop table tbl3_bak;
--enable_warnings

CREATE TABLE tbl3_bak(C1 VARCHAR2(200),C2 int,C3 VARCHAR2(200),C4 int,C5 int,C6 VARCHAR2(200) );


--disable_warnings
--error 0, 942
drop table dlt1;
--error 0, 942
drop table dlt2;
--error 0, 942
drop table dlt3;
--error 0, 942
drop table dlt4;
--error 0, 942
drop table dlt6;
--enable_warnings

create table dlt1 (a int , b varchar(10), x varchar(10));
create table dlt2 (c int , d char(10), y char(10));
create table dlt3 (e int            , f char(10), z char(10));
create table dlt4 (id int , p_id int, name varchar2(50), alias varchar2(50), c_id int);
create table dlt6 (id int , p_id int, name nvarchar2(50), alias nvarchar2(50), c_id int);
insert into dlt1 values (1, '123', 'abc');
insert into dlt1 values (2, '234', 'bcd');
insert into dlt1 values (3, '345', 'cde');
insert into dlt1 values (4, '456', 'def');
insert into dlt1 values (100, '000', 'xxx');
insert into dlt1 values (101, '111', 'yyy');
insert into dlt1 values (102, '222', 'zzz');
insert into dlt2 values (1, '123', 'abc');
insert into dlt2 values (2, '234', 'bcd');
insert into dlt2 values (3, '345', 'cde');
insert into dlt2 values (4, '456', 'def');
insert into dlt3 values (100, '000', 'xxx');
insert into dlt3 values (101, '111', 'yyy');
insert into dlt3 values (102, '222', 'zzz');
insert into dlt4 values (1001, null, 'zhangsan', 'zs', 101);
insert into dlt4 values (1002, 1001, 'lisi', 'ls', 101);
insert into dlt4 values (1003, 1001, 'wangwu', 'ww', 102);
insert into dlt4 values (1004, 1002, 'zhaoliu', 'zl', 102);
insert into dlt4 values (1005, 1002, 'zhangsi', 'zs', 103);
insert into dlt4 values (1006, 1002, 'liwu', 'lw', 103);
insert into dlt4 values (1007, 1003, 'wangliu', 'wl', 103);
insert into dlt6 select * from dlt4;
commit;

--disable_warnings
--error 0, 942
drop table dlt5;
--error 0, 942
drop table dlt7;
--enable_warnings

create table dlt5 as select * from dlt1;

create table dlt7 (id int , p_id int, name nvarchar2(50), alias varchar2(50), c_id int);
insert into dlt7 select * from dlt6;
commit;

--disable_warnings
--error 0, 942
drop table dlt4_dup;
--enable_warnings
create table dlt4_dup as
  select dlt3.a c1, dlt4.a c2, dlt4.b c3, dlt4.x c4 from (select a from dlt1 minus select c a from dlt2 minus select 1 a from dual) dlt3, dlt1 dlt4 where dlt3.a = dlt4.a;
  
--disable_warnings
--error 0, 942
drop table dlt1_bak;
--error 0, 942
drop table dlt2_bak;
--error 0, 942
drop table dlt3_bak;
--enable_warnings

create table dlt1_bak (id int);
create table dlt2_bak (id int, name varchar2(200));
create table dlt3_bak (name varchar2(200));


--disable_warnings
--error 0, 942
drop table dlt55;
--enable_warnings

create table dlt55 (
  bf1 binary_float,
  bd1 binary_double,
  n1  int,
  n2  number,
  n3  number(10, -2),
  n4  number(20),
  f1  float,
  v1  varchar2(100),
  c1  char(20),
  r1  raw(100),
  nv1 nvarchar2(100),
  nc1 nchar(20),
  l1  blob,
  l2  clob,
  d1  date,
  tz1 timestamp(1) with time zone,
  tz2 timestamp(2) with local time zone,
  t3  timestamp(3),
  i1  interval year(4) to month,
  i2  interval day(5) to second(6),
  w1  urowid(1000)
);

--disable_warnings
--error 0, 942
drop table t1_spell;
--enable_warnings
create table t1_spell(A int, B varchar(40));
insert into  t1_spell values(1, 'asdg');
insert into  t1_spell values(0, 'asdg');
insert into  t1_spell values(2, 'asdg');


--disable_warnings
--error 0,942
drop table testtable CASCADE CONSTRAINTS purge;
--error 0,942
drop table testtable_bak CASCADE CONSTRAINTS purge;
--enable_warnings
create table testtable
(
c0 int,
c1 varchar2(200),
c2 nchar(200),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 char(200),
c8 raw(200),
c9 number(10,3)
);

insert into testtable values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.2);
insert into testtable values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee',100);
insert into testtable values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff',100.3);
insert into testtable values(4,'aaabbb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc',200);
insert into testtable values(5,'a','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.22);
insert into testtable values(6,'bb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',3000);
insert into testtable values(7,'20060604','20060604','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',2000.33);
insert into testtable values(8,'2006-06-04','aaabbb','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',1000);
insert into testtable values(9,'中文测试009','aaabbb','200','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','200',10000);
insert into testtable values(10,'中文测试010','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','300',999999);
insert into testtable values(11,'中文测试011','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd  hh24:mi:ss'),'a','300',999999);

CREATE TABLE testtable_bak AS SELECT C1 FROM testtable;

--disable_warnings
--error 0,942
drop table t_exists_in CASCADE CONSTRAINTS purge;
--error 0,942
drop table t_exists_in_bak CASCADE CONSTRAINTS purge;
--enable_warnings
CREATE TABLE t_exists_in (c1 varchar2(50));
insert into t_exists_in values('97749');

CREATE TABLE t_exists_in_bak AS SELECT * FROM t_exists_in;



--disable_warnings
--error 0, 942
DROP TABLE DEPT001 CASCADE CONSTRAINTS purge;
--error 0, 942
DROP TABLE EMP001 CASCADE CONSTRAINTS purge;
--enable_warnings
CREATE TABLE DEPT001(DEPTNO NUMBER(2) ,DNAME VARCHAR2(14) ,LOC VARCHAR2(13)) ;
INSERT INTO DEPT001 VALUES(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT001 VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT001 VALUES(30,'SALES','CHICAGO');
INSERT INTO DEPT001 VALUES(40,'OPERATIONS','BOSTON');

create TABLE EMP001(empno NUMBER,ename VARCHAR2(100),job VARCHAR2(100),mgr NUMBER,hiredate DATE,sal NUMBER(9,2),comm NUMBER(9,2),deptno NUMBER(20),comid NUMBER);
INSERT INTO EMP001 (empno, ename, job, mgr, hiredate, sal, comm, deptno,comid)values (1, 'dog', 'code', 7369, to_date('05-06-2018', 'dd-mm-yyyy'), 5000, 5000, 10,9999);
INSERT INTO EMP001 VALUES(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20,9999);
INSERT INTO EMP001 VALUES(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30,9999);
INSERT INTO EMP001 VALUES(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30,9999);
INSERT INTO EMP001 VALUES(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20,9999);
INSERT INTO EMP001 VALUES(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30,9999);
INSERT INTO EMP001 VALUES(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30,9999);
INSERT INTO EMP001 VALUES(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10,9999);
INSERT INTO EMP001 VALUES(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20,9999);
INSERT INTO EMP001 VALUES(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10,9999);
INSERT INTO EMP001 VALUES(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30,9999);
INSERT INTO EMP001 VALUES(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20,9999);
INSERT INTO EMP001 VALUES(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30,9999);
INSERT INTO EMP001 VALUES(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20,9999);
INSERT INTO EMP001 VALUES(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10,9999);

COMMIT;


--error 0,942
DROP TABLE test190;
CREATE TABLE test190(
PK INT,
COL_INT INT,
COL_TS TIMESTAMP,
COL_VARCHAR2_200 VARCHAR2(200),
COL_NUMBER_29_0 NUMBER(29,0),
COL_DATE DATE,
COL_DECIMAL_21_2 DECIMAL(21,2));

delimiter //;
--disable_warnings
create or replace procedure get_data
is
i NUMBER;
j NUMBER;
v_createsql varchar2(200);
v_dropsql varchar2(200);
v_count NUMBER;
begin 
for j in 1 .. 555 loop
insert into test190 values(j,j,(Timestamp '2005-07-26 14:01:31.019751'),'lhqhjxvbudkjsqinonv',0,to_date('2001-08-21','yyyy-mm-dd'),-280625152);
commit;
end loop;
end get_data;
//
delimiter ;//
--enable_warnings

call get_data();

--error 0,942
DROP TABLE test191;

CREATE TABLE test191(num NUMBER(4) UNIQUE,name NVARCHAR2(10) NOT NULL,job NVARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2));
INSERT INTO test191 VALUES(1,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,100);
INSERT INTO test191 VALUES(2,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300);
INSERT INTO test191 VALUES(3,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500);
INSERT INTO test191 VALUES(4,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,358.69);
INSERT INTO test191 VALUES(5,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400);
INSERT INTO test191 VALUES(6,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL);
INSERT INTO test191 VALUES(7,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,744.9);
INSERT INTO test191 VALUES(8,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,-200);
INSERT INTO test191 VALUES(9,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL);
INSERT INTO test191 VALUES(10,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0);
--error 0,942
DROP TABLE test192;
CREATE TABLE test192 (PK INT,COL_INT INT,COL_TS TIMESTAMP,COL_VARCHAR2_200 VARCHAR2(200),COL_NUMBER_29_0 NUMBER(29,0),COL_DATE DATE, COL_DECIMAL_21_2 DECIMAL(21,2))
PARTITION BY RANGE (PK) 
(PARTITION LYQTEST_P0 VALUES LESS THAN(3),
PARTITION LYQTEST_P1 VALUES LESS THAN(5),
PARTITION LYQTEST_P2 VALUES LESS THAN(MAXVALUE));
