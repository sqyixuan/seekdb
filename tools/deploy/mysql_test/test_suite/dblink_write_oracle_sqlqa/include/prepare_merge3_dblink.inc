ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink

--disable_warnings
--error 0,942
drop table stu1;
--error 0,942
drop table stu;
--enable_warnings

create table stu(s1 nvarchar2(6), s2 nvarchar2(6) default 10, s3 nvarchar2(6) default 20, s4 nvarchar2(6) default 30);
create table stu1(s1 nvarchar2(6), s2 nvarchar2(6) default 100, s3 nvarchar2(6) default 200, s4 nvarchar2(6) default 300);

insert into stu values(1,1,1,1);
insert into stu values(2,2,2,2);
insert into stu values(6,6,6,6);
insert into stu1 values(1,11,11,11);
insert into stu1 values(8,8,8,8);

create or replace view stu_view(ss1,ss2,ss3,ss4) as select * from stu;
create or replace view stu1_view(ss1,ss2,ss3,ss4) as select * from stu1;

create or replace synonym my_syn for stu;
create or replace synonym my_syn1 for stu1;


--error 0, 942
DROP TABLE AAA CASCADE CONSTRAINTS purge;
CREATE TABLE AAA (
col_int int,
col_date date,
pk int,
col_number_10_0 number(10,0),
col_char_10 char(10),
col_number_8_2 number(8,2),
col_number_8_3 number(8,3),
col_char_20 char(20),
primary key (pk) ) ; 
INSERT ALL INTO AAA VALUES  (NULL, to_date('2003-03-28 00:01:10','yyyy-mm-dd hh24:mi:ss'), 1, NULL, '', NULL, NULL, 'i') SELECT 1 FROM DUAL;

--error 0, 942
DROP TABLE BBB CASCADE CONSTRAINTS purge;
CREATE TABLE BBB (
col_number_10_0 number(10,0),
col_number_8_3 number(8,3),
col_int int,
pk int,
col_char_20 char(20),
col_number_8_2 number(8,2),
col_date date,
col_char_10 char(10),
primary key (pk) ) ; 
INSERT ALL INTO BBB VALUES  (73, 94, 44, 1, 'him', -9462, to_date('2003-05-26','yyyy-mm-dd'), NULL) INTO BBB VALUES  (9, 7, 29939, 2, 'k', 5, to_date('2004-11-27 14:26:02','yyyy-mm-dd hh24:mi:ss'), 'e') INTO BBB VALUES  (2, 8, -9292, 3, 'v', 5, (Timestamp '2003-02-08 12:27:12.019521'), 'h') INTO BBB VALUES  (NULL, -6645, 15242, 4, '', 86, (Timestamp '2008-08-22 02:24:43.008868'), NULL) INTO BBB VALUES  (-74, -52, -71, 5, 'you''re', 2, (Timestamp '2007-10-09 19:53:04.008332'), '') INTO BBB VALUES  (6, NULL, -2958, 6, NULL, -1547, (Timestamp '2005-12-05 09:18:28.048770'), 'q') INTO BBB VALUES  (NULL, 6174, -91, 7, 'yeah', 0, (Timestamp '2000-10-08 19:46:14.019798'), '') INTO BBB VALUES  (5, NULL, -14, 8, 'w', -21, (Timestamp '2004-04-28 16:56:22.022966'), '') INTO BBB VALUES  (16, NULL, NULL, 9, '', NULL, NULL, 'j') INTO BBB VALUES  (8, 1, NULL, 10, 'b', 80, to_date('2001-08-07 00:54:36','yyyy-mm-dd hh24:mi:ss'), 'g') INTO BBB VALUES  (0, NULL, -97, 11, '', 59, (Timestamp '2003-10-07 20:33:05.051703'), 'go') INTO BBB VALUES  (NULL, NULL, 9445, 12, 'p', -14080, (Timestamp '2008-07-28 23:10:48.034080'), NULL) INTO BBB VALUES  (NULL, NULL, -31067, 13, 'n', 32, (Timestamp '2002-10-13 05:23:41.014522'), NULL) INTO BBB VALUES  (NULL, -124, NULL, 14, 'go', NULL, (Timestamp '2009-02-13 11:04:35.039256'), '') INTO BBB VALUES  (NULL, 29, NULL, 15, '', NULL, (Timestamp '2005-11-24 06:24:29.062784'), '') INTO BBB VALUES  (0, NULL, 1, 16, 'q', 9, (Timestamp '2000-03-20 17:51:52.010486'), 'v') INTO BBB VALUES  (NULL, 0, NULL, 17, 'q', 8, NULL, 'w') INTO BBB VALUES  (4, 60, NULL, 18, 'to', 6, (Timestamp '2005-09-10 06:44:18.007092'), 'a') INTO BBB VALUES  (NULL, NULL, -20124, 19, NULL, NULL, NULL, 'u') INTO BBB VALUES  (NULL, -4792, 9, 20, 'her', -95, NULL, 'didn''t') INTO BBB VALUES  (NULL, 8, -16808, 21, 'I''ll', 56, to_date('2001-10-17 14:10:04','yyyy-mm-dd hh24:mi:ss'), 'w') INTO BBB VALUES  (-72, -35, -4190, 22, 'at', NULL, to_date('2007-11-07','yyyy-mm-dd'), NULL) INTO BBB VALUES  (NULL, NULL, 3, 23, 'a', NULL, (Timestamp '2005-10-08 20:21:22.028610'), 'and') INTO BBB VALUES  (-18339, NULL, 8, 24, 'r', NULL, to_date('2009-11-12 19:35:19','yyyy-mm-dd hh24:mi:ss'), 'yeah') INTO BBB VALUES  (NULL, NULL, 0, 25, NULL, 34, to_date('2000-01-01 07:16:24','yyyy-mm-dd hh24:mi:ss'), '') INTO BBB VALUES  (NULL, 4509, NULL, 26, 'c', 19327, to_date('2000-08-14','yyyy-mm-dd'), 'i') INTO BBB VALUES  (NULL, -29, NULL, 27, 'good', -70, (Timestamp '2004-10-10 18:38:59.055764'), '') INTO BBB VALUES  (NULL, -38, 9256, 28, 'x', 22, to_date('2001-04-04 10:23:28','yyyy-mm-dd hh24:mi:ss'), '') INTO BBB VALUES  (-79, -2097, 126, 29, 'x', NULL, to_date('2009-09-25','yyyy-mm-dd'), NULL) INTO BBB VALUES  (3, -24177, 74, 30, 'q', 8, (Timestamp '2005-09-01 07:29:45.064089'), 'd') SELECT 1 FROM DUAL;



--error 0, 942
DROP TABLE EMPEMP001 CASCADE CONSTRAINTS purge;
create TABLE EMPEMP001(empno   int,ename   VARCHAR2(100),job  VARCHAR2(100),mgr   NUMBER,hiredate date,sal   NUMBER(9,2),comm   NUMBER(9,2),deptno  NUMBER(20),deptnoname   char(100));
INSERT INTO EMPEMP001 VALUES(1,'SMITH001','CLERK',7901,to_date('17-12-1981','dd-mm-yyyy'),800,NULL,20,'分部1');
INSERT INTO EMPEMP001 VALUES(2,'SMITH002','CLERK',7902,to_date('17-12-1982','dd-mm-yyyy'),900,NULL,30,'分部2');
INSERT INTO EMPEMP001 VALUES(3,'SMITH003','CLERK',7903,to_date('17-12-1983','dd-mm-yyyy'),1000,NULL,40,'分部3');


--error 0, 942
DROP TABLE EMPEMP002 CASCADE CONSTRAINTS purge;
create TABLE EMPEMP002(empno   int,ename   VARCHAR2(100),job  VARCHAR2(100),mgr   NUMBER,hiredate date,sal   NUMBER(9,2),comm   NUMBER(9,2),deptno  NUMBER(20),deptnoname   char(100));
INSERT INTO EMPEMP002 VALUES(1,'SMITH001','CLERK',2,to_date('17-12-2981','dd-mm-yyyy'),8000,NULL,20,'分部10');
INSERT INTO EMPEMP002 VALUES(2,'SMITH002','CLERK',4,to_date('17-12-2982','dd-mm-yyyy'),9000,NULL,30,'分部20');
INSERT INTO EMPEMP002 VALUES(3,'SMITH003','CLERK',3,to_date('17-12-2983','dd-mm-yyyy'),2000,NULL,40,'分部30');

--error 0, 942
DROP TABLE    EMP001  CASCADE CONSTRAINTS   purge;
--error 0, 942
DROP VIEW    EMP001_view;

--error 0, 942
DROP TABLE    EMP002  CASCADE CONSTRAINTS   purge;
--error 0, 942
DROP VIEW    EMP002_view;
create table EMP001 as select * from EMPEMP001;
create table EMP002  as select * from EMPEMP002;
create or replace view   EMP001_view AS  SELECT  *  FROM    EMP001;
create or replace view   EMP002_view AS  SELECT  *  FROM    EMP002;

COMMIT;