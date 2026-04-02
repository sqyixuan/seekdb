--disable_warnings
--error 0,942
drop table emp;
--enable_warnings

##数据类型gb18030-4//字符串类型gb18030-4-1//gb18030-4-1-1
##数据类型gb18030-4//字符串类型gb18030-4-1//gb18030-4-1-2
##数据类型gb18030-4//字符串类型gb18030-4-1//gb18030-4-1-3

create table emp (empno nvarchar2(5), ename varchar2(8), mgr varchar2(8),CONSTRAINT PK_test3 PRIMARY KEY (empno,ename));
insert into emp (EMPNO, ENAME, MGR)
values ('1', 'jim', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('2', 'tom', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('3', 'tim', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('4', 'lily', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('5', 'mary', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('6', 'tid', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('7', 'King', '10');
insert into emp (EMPNO, ENAME, MGR)
values ('8', 'kenvin', '10');
insert into emp (EMPNO, ENAME, MGR)
values ('9', 'shema', '8');
insert into emp (EMPNO, ENAME, MGR)
values ('10', 'john', '0');
insert into emp (EMPNO, ENAME, MGR)
values ('0', 'root', '');

select * from emp order by empno;


##数据类型gb18030-4//字符串类型gb18030-4-1//gb18030-4-1-2
##数据类型gb18030-4//数字类型gb18030-4-2//gb18030-4-2-4
##数据类型gb18030-4//时间类型gb18030-4-3//gb18030-4-3-1
##数据类型gb18030-4//时间类型gb18030-4-3//gb18030-4-3-2
##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-4-2
--disable_warnings
--error 0,942
drop table mail4;
--enable_warnings

create table mail4
(
c0 int,
c1 nvarchar2(200),
c2 varchar2(20),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 char(200),
c8 raw(200),
CONSTRAINT PK_test PRIMARY KEY (c1,c2) 
);

insert into mail4 values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee');
insert into mail4 values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff');
insert into mail4 values(4,'aaabbb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc');
insert into mail4 values(5,'a','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(6,'bb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(7,'20060604','20060604','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(8,'2006-06-04','aaabbb','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(9,'中文测试009','aaabbb','200','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','200');
insert into mail4 values(10,'中文测试010','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','300');

select * from mail4 order by c0;

--disable_warnings
--error 0,942
drop table emp;
--enable_warnings

--disable_warnings
--error 0,942
drop table DEPT;
--enable_warnings


##数据类型gb18030-4//数字类型gb18030-4-2//gb18030-4-2-2
CREATE TABLE DEPT
(DEPTNO NUMBER(2) PRIMARY KEY,
	DNAME NVARCHAR2(14),
	LOC VARCHAR2(13) ) ;
CREATE TABLE EMP
       (EMPNO NUMBER(4) PRIMARY KEY,
	ENAME NVARCHAR2(10),
	JOB NVARCHAR2(9),
	MGR NUMBER(4),
	HIREDATE DATE,
	SAL NUMBER(7,2),
	COMM NUMBER(7,2),
	DEPTNO NUMBER(2));
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

select * from EMP order by EMPNO;


--disable_warnings
--error 0,942
drop table DEPT;
--enable_warnings


select null from dual
union
select null from dual;

select 1 from dual
minus
select null from dual;

select 1 from dual
intersect
select null from dual;


--disable_warnings
--error 0,942
drop table testt;
--enable_warnings

create table testt(c1 varchar2(200));
insert into testt values ('ئالىمجان ئابدۇرېشىت');
select * from testt;

select 'ئالىمجان ئابدۇرېشىت' from dual;

--disable_warnings
--error 0,942
drop table testt;
--enable_warnings


--disable_result_log
--echo  ##数据类型gb18030-4//时间类型gb18030-4-3//gb18030-4-3-3
select sysdate - interval '10' day as "10天前",
       sysdate - interval '10' hour as "10小时前",
       sysdate - interval '10' minute as "10分钟前",
       sysdate - interval '10' second as "10秒钟前",
       sysdate - 10 as "10天前",
       sysdate - 10 / 24 as "10小时前",
       sysdate - 10 / (24 * 60) as "10分钟前",
       sysdate - 10 / (24 * 3600) as "10秒钟前"
  from dual;
--enable_result_log

--disable_warnings
--error 0, 942
drop table EMP3 PURGE;
--error 0, 942
drop table emp3_part PURGE;
--enable_warnings

CREATE TABLE EMP3
(
  EMPNO     NUMBER(6),
  SAL       NUMBER(8,2),
  COMM      NUMBER(8,2),
  SAL_PACK  GENERATED ALWAYS AS ( SAL + NVL(COMM,0) ) VIRTUAL
);

insert into EMP3(EMPNO,SAL,COMM) values (1,33.33,44.44);
insert into EMP3(EMPNO,SAL,COMM) values (2,34.34,44.44);
insert into EMP3(EMPNO,SAL,COMM) values (3,35.35,44.44);
insert into EMP3(EMPNO,SAL,COMM) values (4,36.36,44.44);
insert into EMP3(EMPNO,SAL,COMM) values (5,37.37,44.44);

###select table_name,COLUMN_NAME,data_type,data_default,VIRTUAL_COLUMN from user_tab_cols where table_name='EMP3';
alter table emp3 add (sal_total as (sal*12+comm) virtual);

desc emp3;

select * from emp3;
###select table_name,COLUMN_NAME,data_type,data_default,VIRTUAL_COLUMN from user_tab_cols where table_name='EMP3';


--disable_warnings
--error 0, 942
drop table EMP3 PURGE;
--error 0, 942
drop table emp3_part PURGE;
--enable_warnings

--echo  ########虚拟列,数据类型测试
###数字类型转换数字类型

--disable_warnings
--error 0, 942
drop table triangle2;
--enable_warnings

CREATE TABLE triangle2 (
  sidea number(10),
  sideb number(10),
  sidec number(10) AS (SQRT(sidea * sidea + sideb * sideb))
);
INSERT INTO triangle2 (sidea, sideb) VALUES(-128,-128);
INSERT INTO triangle2 (sidea, sideb) VALUES(127,127);
INSERT INTO triangle2 (sidea, sideb) VALUES(60,80);

SELECT * FROM triangle2;

--disable_warnings
--error 0, 942
drop table triangle2;
--enable_warnings

##数据类型gb18030-4//数字类型gb18030-4-2//gb18030-4-2-3
--disable_warnings
--error 0, 942
drop table triangle2;
--enable_warnings

CREATE TABLE triangle2 (
  sidea binary_float,
  sideb binary_float,
  sidec binary_float AS (SQRT(sidea * sidea + sideb * sideb))
);
INSERT INTO triangle2 (sidea, sideb) VALUES(-128,-128);
INSERT INTO triangle2 (sidea, sideb) VALUES(127,127);
INSERT INTO triangle2 (sidea, sideb) VALUES(60,80);

SELECT * FROM triangle2;

--disable_warnings
--error 0, 942
drop table triangle2;
--enable_warnings

###--echo  ##数据类型gb18030-4//数字类型gb18030-4-2//gb18030-4-2-4 暂不支持
###--disable_warnings
###--error 0, 942
###drop table triangle2;
###--enable_warnings
###
###CREATE TABLE triangle2 (
###  sidea long,
###  sideb long,
###  sidec long AS (SQRT(sidea * sidea + sideb * sideb))
###);
###INSERT INTO triangle2 (sidea, sideb) VALUES(-128,-128);
###INSERT INTO triangle2 (sidea, sideb) VALUES(127,127);
###INSERT INTO triangle2 (sidea, sideb) VALUES(60,80);
###
###SELECT * FROM triangle2;
###
###--disable_warnings
###--error 0, 942
###drop table triangle2;
###--enable_warnings





###数字类型转换字符类型
##数据类型gb18030-4//字符串类型gb18030-4-1//gb18030-4-1-1
--disable_warnings
--error 0, 942
drop table triangle10;
--enable_warnings

CREATE TABLE triangle10 (
  sidea number,
  sideb number,
  sidec CHAR(40) AS (SQRT(sidea * sidea + sideb * sideb))
);
INSERT INTO triangle10 (sidea, sideb) VALUES(1,1);
INSERT INTO triangle10 (sidea, sideb) VALUES(3,4);
INSERT INTO triangle10 (sidea, sideb) VALUES(6,8);

SELECT * FROM triangle10;
desc triangle10;

--disable_warnings
--error 0, 942
drop table triangle10;
--enable_warnings

--disable_warnings
--error 0, 942
drop table triangle10;
--enable_warnings

CREATE TABLE triangle10 (
  sidea CHAR(20),
  sideb CHAR(20),
  sidec NCHAR(40) AS (concat(sidea,sideb))
);
INSERT INTO triangle10 (sidea, sideb) VALUES('中文','中文2');
INSERT INTO triangle10 (sidea, sideb) VALUES('中文','中文2');
INSERT INTO triangle10 (sidea, sideb) VALUES('中文','中文2');

SELECT * FROM triangle10;
###desc triangle10;

--disable_warnings
--error 0, 942
drop table triangle10;
--enable_warnings




--disable_warnings
--error 0, 942
drop table triangle11;
--enable_warnings

CREATE TABLE triangle11 (
  sidea number,
  sideb number,
  sidec VARCHAR2(40) AS (SQRT(sidea * sidea + sideb * sideb))
);
INSERT INTO triangle11 (sidea, sideb) VALUES(1,1);
INSERT INTO triangle11 (sidea, sideb) VALUES(3,4);
INSERT INTO triangle11 (sidea, sideb) VALUES(6,8);

SELECT * FROM triangle11;

--disable_warnings
--error 0, 942
drop table triangle11;
--enable_warnings

--disable_warnings
--error 0, 942
drop table triangle12;
--enable_warnings

--error 600,54003
CREATE TABLE triangle12 (
  sidea number,
  sideb number,
  sidec BLOB AS (SQRT(sidea * sidea + sideb * sideb))
);

--disable_warnings
--error 0, 942
drop table triangle12;
--enable_warnings


###--disable_warnings
###--error 0, 942
###drop table triangle13;
###--enable_warnings
###
###CREATE TABLE triangle13 (
###  sidea number,
###  sideb number,
###  sidec CLOB AS (SQRT(sidea * sidea + sideb * sideb))
###);
###INSERT INTO triangle13 (sidea, sideb) VALUES(1,1),(3,4),(6,8);
###
###SELECT * FROM triangle13;
###
###--disable_warnings
###--error 0, 942
###drop table triangle13;
###--enable_warnings


###日期类型转换日期类型

--disable_warnings
--error 0, 942
drop table triangle14;
--enable_warnings

###预期成功无报错，当前OB有bug：34489396- 【generated column】Function to_date as virtual column  report error 54002
--error 0,54002
CREATE TABLE triangle14 (
  sidea DATE,
  sideb DATE,
  sidec DATE AS (to_date(sidea))
);
##INSERT INTO triangle14 (sidea, sideb) VALUES(to_date('1001-01-01'),to_date('1001-01-01'));
##INSERT INTO triangle14 (sidea, sideb) VALUES(to_date('9998-12-31'),to_date('9998-12-31'));
##INSERT INTO triangle14 (sidea, sideb) VALUES(to_date('2020-12-31'),to_date('2020-12-31'));
##
##SELECT * FROM triangle14;
##desc triangle14;

--disable_warnings
--error 0, 942
drop table triangle14;
--enable_warnings


--disable_warnings
--error 0, 942
drop table triangle18;
--enable_warnings

--error 0,54016
CREATE TABLE triangle18 (
  sidea TIMESTAMP,
  sideb TIMESTAMP,
  sidec TIMESTAMP AS (sidea))
;

--disable_warnings
--error 0, 942
drop table triangle18;
--enable_warnings


###日期类型转换字符类型

--disable_warnings
--error 0, 942
drop table triangle19;
--enable_warnings

###预期成功无报错，当前OB有bug：34489396- 【generated column】Function to_date as virtual column  report error 54002
--error 0,54002
CREATE TABLE triangle19 (
  sidea DATE,
  sideb TIMESTAMP,
  sidec VARCHAR2(200) AS (to_date(sideb)))
;

###OB有报错，ORACLE正常。
###INSERT INTO triangle19 (sidea, sideb) VALUES(to_date('1999-01-01','YYYY-MM-DD HH24:MI:SS'),to_date('1999-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'));


###SELECT * FROM triangle19;
###desc triangle19;

--disable_warnings
--error 0, 942
drop table triangle19;
--enable_warnings


--disable_warnings
--error 0, 942
drop table triangle20;
--enable_warnings

###预期成功无报错，当前OB有bug：34489396- 【generated column】Function to_date as virtual column  report error 54002
--error 0,54002
CREATE TABLE triangle20 (
  sidea DATE,
  sideb DATE,
  sidec CHAR(200) AS (to_date(sideb)))
;
###INSERT INTO triangle20 (sidea, sideb) VALUES(to_date('1999-01-01','YYYY-MM-DD HH24:MI:SS'),to_date('1999-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'));

###SELECT * FROM triangle20;

--disable_warnings
--error 0, 942
drop table triangle20;
--enable_warnings

##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-4
##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-4-1
###字符类型转换字符类型

--disable_warnings
--error 0, 942
drop table triangle23;
--enable_warnings

CREATE TABLE triangle23 (
  sidea CHAR(100),
  sideb CHAR(100),
  sidec VARCHAR2(200) AS (CAST(sidea AS CHAR)))
;
INSERT INTO triangle23 (sidea, sideb) VALUES('c','h');
INSERT INTO triangle23 (sidea, sideb) VALUES('中文测试','中文测试');
INSERT INTO triangle23 (sidea, sideb) VALUES('中文混合adfso441','中文混合adfso441');

SELECT * FROM triangle23;

--disable_warnings
--error 0, 942
drop table triangle23;
--enable_warnings

--disable_warnings
--error 0, 942
drop table triangle24;
--enable_warnings

CREATE TABLE triangle24 (
  sidea VARCHAR2(100),
  sideb VARCHAR2(100),
  sidec VARCHAR2(200) AS (CAST(sidea AS CHAR)))
;

INSERT INTO triangle24 (sidea, sideb) VALUES('chaerre','chaerre');
INSERT INTO triangle24 (sidea, sideb) VALUES('中文测试','中文测试');
INSERT INTO triangle24 (sidea, sideb) VALUES('中文混合adfso441','中文混合adfso441');

SELECT * FROM triangle24;

--disable_warnings
--error 0, 942
drop table triangle24;
--enable_warnings

##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-1
--disable_warnings
--error 0, 942
drop table triangle27;
--enable_warnings

CREATE TABLE triangle27 (
  sidea RAW(20),
  sideb RAW(20),
  sidec RAW(20) AS (CAST(sidea AS CHAR)))
;

##OB插入报错，ORACLE正常
##INSERT INTO triangle27 (sidea, sideb) VALUES('abc','abc');
##INSERT INTO triangle27 (sidea, sideb) VALUES('10','10');

SELECT * FROM triangle27;


--disable_warnings
--error 0, 942
drop table triangle27;
--enable_warnings


###字符类型转换日期类型

--disable_warnings
--error 0, 942
drop table triangle33;
--enable_warnings

CREATE TABLE triangle33 (
  sidea VARCHAR2(100),
  sideb VARCHAR2(100),
  sidec DATE AS (to_date(sidea,'YYYY-MM-DD HH24:MI:SS')))
;
INSERT INTO triangle33 (sidea, sideb) VALUES(to_date('1999-01-01','YYYY-MM-DD HH24:MI:SS'),to_date('1999-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'));

###OB有报错，ORACLE正常。
###SELECT * FROM triangle33;

###字符类型转换数字类型

##--disable_warnings
###--error 0, 942
##drop table triangle35;
##--enable_warnings
##
##CREATE TABLE triangle35 (
##  sidea CHAR(100),
##  sideb CHAR(100),
##  sidec NUMBER(38) AS (to_char(sidea)))
##;
##
##INSERT INTO triangle35 (sidea, sideb) VALUES('1001-01-01','1001-01-01');
##INSERT INTO triangle35 (sidea, sideb) VALUES('2020','2020');
##INSERT INTO triangle35 (sidea, sideb) VALUES('99981231','99981231');
##
##SELECT * FROM triangle35;
##
##--disable_warnings
###--error 0, 942
##drop table triangle35;
##--enable_warnings

--disable_warnings
--error 0, 942
drop table triangle36;
--enable_warnings

CREATE TABLE triangle36 (
  sidea VARCHAR2(100),
  sideb VARCHAR2(100),
  sidec NUMBER(38) AS (to_number(sidea)))
;

###INSERT INTO triangle36 (sidea, sideb) VALUES('1001-01-01','1001-01-01');
INSERT INTO triangle36 (sidea, sideb) VALUES('2020','2020');
INSERT INTO triangle36 (sidea, sideb) VALUES('99981231','99981231');

SELECT * FROM triangle36;

--disable_warnings
--error 0, 942
drop table triangle36;
--enable_warnings


###字符类型cast转换数字类型，字段类型设置为字符类型


--disable_warnings
--error 0, 942
drop table triangle37;
--enable_warnings
CREATE TABLE triangle37 (
  sidea CHAR(100),
  sideb CHAR(100),
  sidec CHAR(100) AS (cast(sidea as DATE)))
;
##INSERT INTO triangle37 (sidea, sideb) VALUES('1001-01-01','1001-01-01');
###OB报错，ORACLE正常
##--error 01840
##INSERT INTO triangle37 (sidea, sideb) VALUES('2020','2020');
##INSERT INTO triangle37 (sidea, sideb) VALUES('99981231','99981231');
SELECT * FROM triangle37;
desc triangle37;

--disable_warnings
--error 0, 942
drop table triangle37;
--enable_warnings

--disable_warnings
--error 0, 942
drop table triangle38;
--enable_warnings

CREATE TABLE triangle38 (
  sidea VARCHAR2(100),
  sideb VARCHAR2(100),
  sidec INT AS (cast(sidea as CHAR)))
;

##数据类型gb18030-4//数字类型gb18030-4-2//gb18030-4-2-1
INSERT INTO triangle38 (sidea, sideb) VALUES('1001-01-01','1001-01-01');
INSERT INTO triangle38 (sidea, sideb) VALUES('2020','2020');
INSERT INTO triangle38 (sidea, sideb) VALUES('99981231','99981231');

SELECT * FROM triangle38;

--disable_warnings
--error 0, 942
drop table triangle38;
--enable_warnings


###数字类型cast转换字符类型，字段类型设置为数字类型
--disable_warnings
--error 0, 942
drop table triangle39;
--enable_warnings

CREATE TABLE triangle39 (
  sidea INT,
  sideb INT,
  sidec INT AS (cast(sidea as CHAR)))
;
INSERT INTO triangle39 (sidea, sideb) VALUES(1,1);
INSERT INTO triangle39 (sidea, sideb) VALUES(6,8);
INSERT INTO triangle39 (sidea, sideb) VALUES(3,4);

SELECT * FROM triangle39;
desc triangle39;

--disable_warnings
--error 0, 942
drop table triangle39;
--enable_warnings


###数字类型cast转换字符类型，字段类型设置为字符类型

--disable_warnings
--error 0, 942
drop table triangle40;
--enable_warnings

CREATE TABLE triangle40 (
  sidea INT,
  sideb INT,
  sidec CHAR(10) AS (cast(sidea as CHAR)))
;
INSERT INTO triangle40 (sidea, sideb) VALUES(1,1);
INSERT INTO triangle40 (sidea, sideb) VALUES(3,4);
INSERT INTO triangle40 (sidea, sideb) VALUES(6,8);

SELECT * FROM triangle40;

--disable_warnings
--error 0, 942
drop table triangle40;
--enable_warnings


--echo  ########DML datatypes
--disable_warnings
--error 0, 942
drop table gbk_dml_002;
--enable_warnings

create table gbk_dml_002(c0 raw(255) ,c1 raw(2000) );
--error 01465
insert into gbk_dml_002 values(' 字
龠 ',' 字
龠 ');

insert into gbk_dml_002 values('ABC','EF');

select * from gbk_dml_002;

--disable_warnings
--error 0, 942
drop table gbk_dml_002;
--enable_warnings


create table gbk_dml_002(c0 TIMESTAMP);
##insert into gbk_dml_002(c0) values (now());
##insert into gbk_dml_002(c0) values (CURTIME());

select * from gbk_dml_002;

--disable_warnings
--error 0, 942
drop table gbk_dml_002;
--enable_warnings

--disable_warnings
--error 0, 942
drop table gbk_dml_002;
--error 0, 942
drop table gbk_dml_003;
--error 0, 942
drop table gbk_dml_004;
--enable_warnings

##数据类型gb18030-4//字符串类型gb18030-4-1//gb18030-4-1-3
create table gbk_dml_002(c0 CLOB,c1 blob,c2 char(200) );
insert into gbk_dml_002 values(' 字
龠 ','ABCDEF',' 字
龠 ');

SELECT * FROM gbk_dml_002;

create table gbk_dml_003(gender CLOB);
insert into gbk_dml_003 values('龠');

SELECT * FROM gbk_dml_003;

--disable_warnings
--error 0, 942
drop table gbk_dml_002;
--error 0, 942
drop table gbk_dml_003;
--error 0, 942
drop table gbk_dml_004;
--enable_warnings


--disable_warnings
--error 0, 942
drop table t1;
--error 0, 942
drop table t2;
--enable_warnings

create table t2(c1 number,c2 CLOB,c3 timestamp,c4 date,c5 char(30),c6 varchar2(30),c7 NUMBER,c8 varchar2(200) generated always as (concat(c1,c6)));
create table t1(c1 char(255));
insert into t1 values('先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者。');

insert into t2(c1,c2,c3,c4,c5,c6,c7) select 1,substr(c1,1*20,10),'2020-05-21 01:01:01','2020-05-21 02:02:02',substr(c1,1*20+1,10),substr(c1,1*20+2,10),1*4+1/7 from t1;

select * from t1;
select * from t2;

--disable_warnings
--error 0, 942
drop table t1;
--error 0, 942
drop table t2;
--enable_warnings


##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-2

select Date '2012-12-12' + 123 from dual;
select length('哈嘎嘎') from dual;

##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-3

select Date'2012-12-01' from dual;
select Timestamp '2021-12-01 12:01:02.123' from dual;
select 34e05 from dual;

--disable_warnings
--error 0,942
drop table testtable1;
--enable_warnings

create table testtable1
(
c1 int,
c2 date,
c3 number,
c4 TIMESTAMP,
c5 NVARCHAR2(200)
);

insert into testtable1 values(1,Date'2012-12-01',length('哈嘎嘎'),Timestamp '2021-12-01 12:01:02.123',34e05);
insert into testtable1 values(2,Date'2020-12-01',length('哈嘎嘎aa'),Timestamp '2021-12-01 12:01:02.123','34e05');

select * from testtable1;

--disable_warnings
--error 0,942
drop table testtable1;
--enable_warnings



##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-4-4

--disable_warnings
--error 0,942
drop table testtable1;
--enable_warnings

--error 0,600,54015
create table testtable1
(
c1 int,
c2 varchar2(4000) primary key,
c3 char(2000),
c4 nvarchar2(4000),
c5 nchar(1000),
c6 VARCHAR2(4000) AS (CAST(c2 AS CHAR)),
c7 nvarchar2(4000) AS (CAST(c4 AS CHAR)),
c8 char(2000) AS (CAST(c2 AS CHAR)),
c9 nchar(1000) AS (CAST(c5 AS CHAR))
);


###插入超限制数字
###insert into testtable1(c1,c2,c3,c4,c5) values(1,lpad('中文测试1a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',4000,'好'),lpad('中文测试2a',1000,'好'));
###insert into testtable1(c1,c2,c3,c4,c5) values(2,lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',4000,'好'),lpad('中文测试2a',1000,'好'));
###insert into testtable1(c1,c2,c3,c4,c5) values(3,rpad('中文测试3a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',4000,'好'),rpad('中文测试2a',1000,'好'));
###insert into testtable1(c1,c2,c3,c4,c5) values(4,rpad('中文测试4a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',4000,'好'),rpad('中文测试2a',1000,'好'));
###
###select * from testtable1 order by c1;


##数据类型gb18030-4//其他类型gb18030-4-4//gb18030-4-4-4-5


###OB语法暂不支持
###SELECT CONVERT('? ê í ? ? A B C D E ', 'US7ASCII', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'UTF8', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'AL32UTF8', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'AL32UTF8', 'UTF8') FROM DUAL; 

select ascii('好的') from dual;
select ascii('ΑΒΓΔΕΖΗΘΙΚ∧ΜΝΞΟ∏Ρ∑ΤΥΦΧΨΩ') from dual;
select ascii('▂ ▃ ▄ ▅ ▆ ▇ █ ▉ ▊▋▌▍▎▏■ ▓  回 □ 〓≡') from dual;


--echo ascii/chr
select '33','!',chr('33'),ascii('!') from dual union all
select '33','!',chr('33'),ascii('!') from dual union all
select '34','"',chr('34'),ascii('"') from dual union all
select '35','#',chr('35'),ascii('#') from dual union all
select '36','$',chr('36'),ascii('$') from dual union all
select '37','%',chr('37'),ascii('%') from dual union all
select '38','&',chr('38'),ascii('&') from dual union all
select '39','''',chr('39'),ascii('''') from dual union all
select '40','(',chr('40'),ascii('(') from dual union all
select '41',')',chr('41'),ascii(')') from dual union all
select '42','*',chr('42'),ascii('*') from dual union all
select '43','+',chr('43'),ascii('+') from dual union all
select '44',',',chr('44'),ascii(',') from dual union all
select '45','-',chr('45'),ascii('-') from dual union all
select '46','.',chr('46'),ascii('.') from dual union all
select '47','/',chr('47'),ascii('/') from dual union all
select '48','0',chr('48'),ascii('0') from dual union all
select '49','1',chr('49'),ascii('1') from dual union all
select '50','2',chr('50'),ascii('2') from dual union all
select '51','3',chr('51'),ascii('3') from dual union all
select '52','4',chr('52'),ascii('4') from dual union all
select '53','5',chr('53'),ascii('5') from dual union all
select '54','6',chr('54'),ascii('6') from dual union all
select '55','7',chr('55'),ascii('7') from dual union all
select '56','8',chr('56'),ascii('8') from dual union all
select '57','9',chr('57'),ascii('9') from dual union all
select '58',':',chr('58'),ascii(':') from dual union all
select '59',';',chr('59'),ascii(';') from dual union all
select '60','<',chr('60'),ascii('<') from dual union all
select '61','=',chr('61'),ascii('=') from dual union all
select '62','>',chr('62'),ascii('>') from dual union all
select '63','?',chr('63'),ascii('?') from dual union all
select '64','@',chr('64'),ascii('@') from dual union all
select '65','A',chr('65'),ascii('A') from dual union all
select '66','B',chr('66'),ascii('B') from dual union all
select '67','C',chr('67'),ascii('C') from dual union all
select '68','D',chr('68'),ascii('D') from dual union all
select '69','E',chr('69'),ascii('E') from dual union all
select '70','F',chr('70'),ascii('F') from dual union all
select '71','G',chr('71'),ascii('G') from dual union all
select '72','H',chr('72'),ascii('H') from dual union all
select '73','I',chr('73'),ascii('I') from dual union all
select '74','J',chr('74'),ascii('J') from dual union all
select '75','K',chr('75'),ascii('K') from dual union all
select '76','L',chr('76'),ascii('L') from dual union all
select '77','M',chr('77'),ascii('M') from dual;

select '78','N',chr('78'),ascii('N') from dual union all
select '79','O',chr('79'),ascii('O') from dual union all
select '80','P',chr('80'),ascii('P') from dual union all
select '81','Q',chr('81'),ascii('Q') from dual union all
select '82','R',chr('82'),ascii('R') from dual union all
select '83','S',chr('83'),ascii('S') from dual union all
select '84','T',chr('84'),ascii('T') from dual union all
select '85','U',chr('85'),ascii('U') from dual union all
select '86','V',chr('86'),ascii('V') from dual union all
select '87','W',chr('87'),ascii('W') from dual union all
select '88','X',chr('88'),ascii('X') from dual union all
select '89','Y',chr('89'),ascii('Y') from dual union all
select '90','Z',chr('90'),ascii('Z') from dual union all
select '91','[',chr('91'),ascii('[') from dual union all
select '92','\',chr('92'),ascii('\') from dual union all
select '93',']',chr('93'),ascii(']') from dual union all
select '94','^',chr('94'),ascii('^') from dual union all
select '95','_',chr('95'),ascii('_') from dual union all
select '96','`',chr('96'),ascii('`') from dual union all
select '97','a',chr('97'),ascii('a') from dual union all
select '98','b',chr('98'),ascii('b') from dual union all
select '99','c',chr('99'),ascii('c') from dual union all
select '100','d',chr('100'),ascii('d') from dual union all
select '101','e',chr('101'),ascii('e') from dual union all
select '102','f',chr('102'),ascii('f') from dual union all
select '103','g',chr('103'),ascii('g') from dual union all
select '104','h',chr('104'),ascii('h') from dual union all
select '105','i',chr('105'),ascii('i') from dual union all
select '106','j',chr('106'),ascii('j') from dual union all
select '107','k',chr('107'),ascii('k') from dual union all
select '108','l',chr('108'),ascii('l') from dual union all
select '109','m',chr('109'),ascii('m') from dual union all
select '110','n',chr('110'),ascii('n') from dual union all
select '111','o',chr('111'),ascii('o') from dual union all
select '112','p',chr('112'),ascii('p') from dual union all
select '113','q',chr('113'),ascii('q') from dual union all
select '114','r',chr('114'),ascii('r') from dual union all
select '115','s',chr('115'),ascii('s') from dual union all
select '116','t',chr('116'),ascii('t') from dual union all
select '117','u',chr('117'),ascii('u') from dual union all
select '118','v',chr('118'),ascii('v') from dual union all
select '119','w',chr('119'),ascii('w') from dual union all
select '120','x',chr('120'),ascii('x') from dual union all
select '121','y',chr('121'),ascii('y') from dual union all
select '122','z',chr('122'),ascii('z') from dual union all
select '123','{',chr('123'),ascii('{') from dual union all
select '124','|',chr('124'),ascii('|') from dual union all
select '125','}',chr('125'),ascii('}') from dual union all
select '126','~',chr('126'),ascii('~') from dual union all
select '127','DEL',chr('127'),ascii('DEL') from dual;

###特殊字符补充测试
select substr('524419k0791243374H',9,9) from dual;
select substr('中文测试001临时冻结佛你的说法是',9,9) from dual;
select chr(656) from dual;
