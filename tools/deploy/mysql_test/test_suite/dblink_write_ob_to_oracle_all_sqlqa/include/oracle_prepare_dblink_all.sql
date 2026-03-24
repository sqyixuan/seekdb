ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

drop table testtable1;
create table testtable1
(
c1 int,
srcuser1 varchar2(4000),
srchost1 char(2000),
dstuser1 nvarchar2(2000),
dsthost1 nchar(1000),
size2 int
);

insert into testtable1 values(1,lpad('中文测试1a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(2,lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(3,rpad('中文测试3a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',2000,'好'),rpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(4,rpad('中文测试4a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',2000,'好'),rpad('中文测试2a',1000,'好'),100);

select * from testtable1 order by c1;
commit;


drop table EMP;
drop table DEPT;
drop table BONUS;
drop table BONUS1;
drop table BONUS2;
drop table BONUS3;
drop table SALGRADE;

CREATE TABLE DEPT(DEPTNO NUMBER(2),DNAME VARCHAR2(14),LOC VARCHAR2(13));
CREATE TABLE EMP(EMPNO NUMBER(4) ,ENAME VARCHAR2(20),JOB VARCHAR2(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2));
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


CREATE OR REPLACE VIEW EMP_VIEW1124 AS 
SELECT
    EMPNO,
    ENAME
FROM
    EMP;
	
CREATE OR REPLACE SYNONYM EMP_SYNONYM1124 FOR EMP;

CREATE TABLE BONUS
(
ENAME VARCHAR2(10),  
JOB VARCHAR2(9), 
SAL NUMBER,
COMM NUMBER  
);

CREATE TABLE BONUS1
(
ENAME VARCHAR2(10),  
JOB VARCHAR2(9), 
SAL NUMBER,
COMM NUMBER  
);

CREATE TABLE BONUS2
(
ENAME VARCHAR2(10),  
JOB VARCHAR2(9), 
SAL NUMBER,
COMM NUMBER  
);

CREATE TABLE BONUS3
(
ENAME VARCHAR2(10),  
JOB VARCHAR2(9), 
SAL NUMBER,
COMM NUMBER  
);

CREATE TABLE SALGRADE
( GRADE NUMBER,  
LOSAL NUMBER,  
HISAL NUMBER );

INSERT INTO SALGRADE VALUES (1,700,1200);
INSERT INTO SALGRADE VALUES (2,1201,1400);
INSERT INTO SALGRADE VALUES (3,1401,2000);
INSERT INTO SALGRADE VALUES (4,2001,3000);
INSERT INTO SALGRADE VALUES (5,3001,9999);


DROP TABLE EMP_BAK;
DROP TABLE EMP_BAK2;
DROP TABLE DEPT_BAK;

CREATE TABLE EMP_BAK AS SELECT * FROM EMP;
CREATE TABLE EMP_BAK2 AS SELECT * FROM EMP;
CREATE TABLE DEPT_BAK AS SELECT * FROM DEPT;

DROP TABLE Z0CASE;

CREATE  TABLE Z0CASE (z0_test0 varchar2(100),z0_test1 NUMBER ,z0_test2 binary_float)
PARTITION BY RANGE(z0_test2) (PARTITION p1 VALUES less than(3), PARTITION p2 VALUES less than(5), PARTITION p3 VALUES less than(MAXVALUE));

INSERT INTO Z0CASE VALUES ('ABCabc',0,1.1);
INSERT INTO Z0CASE VALUES ('123456',1,2.2);
INSERT INTO Z0CASE VALUES ('!@#$%^&*',2,3.333);
INSERT INTO Z0CASE VALUES ('中文字符123',3,4.4444);
INSERT INTO Z0CASE VALUES ('  ',4,5.5555);
INSERT INTO Z0CASE VALUES ('',5,66.666);


DROP TABLE Z1CASE;

CREATE  TABLE Z1CASE (z1_test0 varchar2(100),z1_test1 NUMBER ,z1_test2 binary_float)
PARTITION BY RANGE(z1_test2) (PARTITION p1 VALUES less than(5), PARTITION p2 VALUES less than(10), PARTITION p3 VALUES less than(MAXVALUE));
INSERT INTO Z1CASE VALUES ('ABCabc',11,00);
INSERT INTO Z1CASE VALUES ('123456',10,0.11);
INSERT INTO Z1CASE VALUES ('!@#123$%^&*',9,1.11);
INSERT INTO Z1CASE VALUES ('中文字符123',8,2.22);
INSERT INTO Z1CASE VALUES ('  ',7,3.33);
INSERT INTO Z1CASE VALUES ('',6,4.44);
INSERT INTO Z1CASE VALUES ('abcabc',5,5.555);
INSERT INTO Z1CASE VALUES ('11111.1111',4,6.666);
INSERT INTO Z1CASE VALUES ('!@#$%^&*',3,7.777);
INSERT INTO Z1CASE VALUES ('abc中文 字符123',2,8.888);
INSERT INTO Z1CASE VALUES (' ',1,9.999);
INSERT INTO Z1CASE VALUES ('',0,111.111);


DROP TABLE stu150 CASCADE CONSTRAINTS purge;

CREATE TABLE stu150 (id int PRIMARY KEY, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias varchar2(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu150 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu150 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu150 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu150 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu150 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu150 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu150 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE OR REPLACE synonym syn_stu1 FOR stu150;
CREATE OR REPLACE VIEW v_stu150 AS SELECT * FROM   stu150;


DROP TABLE stu151 CASCADE CONSTRAINTS purge;
CREATE TABLE stu151 AS select * from stu150 where 1=0;


DROP TABLE stu152 CASCADE CONSTRAINTS purge;
CREATE TABLE stu152 AS 
SELECT id||name basic,birth, trunc(LN(weight),5) other FROM stu150 WHERE id <=1002+p_id AND trunc(id/33,5)<1000 order by 1,2,3;


drop table class150;
CREATE TABLE class150 (id int, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY list(id) (PARTITION p1 VALUES(101,102),PARTITION p2 VALUES(DEFAULT));
INSERT INTO class150 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class150 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));


drop table class233;
CREATE TABLE class233 (id int PRIMARY KEY, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY hash(id) PARTITIONS 2;
INSERT INTO class233 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class233 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));


drop table class333;
CREATE TABLE class333 (id int PRIMARY KEY, name nvarchar2(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class333 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class333 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE OR REPLACE synonym syn_class1 FOR class150;
CREATE OR REPLACE synonym syn_class2 FOR class233;


DROP TABLE test190;
CREATE TABLE test190(
PK INT,
COL_INT INT,
COL_TS TIMESTAMP,
COL_VARCHAR2_200 VARCHAR2(200),
COL_NUMBER_29_0 NUMBER(29,0),
COL_DATE DATE,
COL_DECIMAL_21_2 DECIMAL(21,2));


create or replace procedure get_data
is
i NUMBER;
j NUMBER;
v_createsql varchar2(200);
v_dropsql varchar2(200);
v_count NUMBER;
begin 
for j in 1 .. 5555 loop
insert into test190 values(j,j,(Timestamp '2005-07-26 14:01:31.019751'),'lhqhjxvbudkjsqinonv',0,to_date('2001-08-21','yyyy-mm-dd'),-280625152);
commit;
end loop;
end get_data;
/


call get_data();


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

DROP TABLE test192;
CREATE TABLE test192 (PK INT,COL_INT INT,COL_TS TIMESTAMP,COL_VARCHAR2_200 VARCHAR2(200),COL_NUMBER_29_0 NUMBER(29,0),COL_DATE DATE, COL_DECIMAL_21_2 DECIMAL(21,2));


drop table class1111;
drop table stu1111;
drop table class2222;
drop table stu2222;
drop table class3333;
drop table stu3333;

CREATE TABLE class1111 (id int, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY list(id) (PARTITION p1 VALUES(101,102),PARTITION p2 VALUES(DEFAULT));
INSERT INTO class1111 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1111 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1111 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1111 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1111 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE TABLE stu1111 (id int PRIMARY KEY, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias char(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu1111 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu1111 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu1111 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu1111 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu1111 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu1111 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu1111 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE TABLE class2222 (id int PRIMARY KEY, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class2222 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2222 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2222 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2222 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2222 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE TABLE stu2222 (id int, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias char(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu2222 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu2222 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu2222 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu2222 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu2222 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu2222 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu2222 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE TABLE class3333 (id int, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class3333 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3333 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3333 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3333 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3333 VALUES(105,'qiqi','yuhang',to_date('20051010','yyyymmdd'));

CREATE TABLE stu3333 (id int, p_id int,name nvarchar2(50) UNIQUE NOT NULL, alias char(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu3333 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu3333 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu3333 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu3333 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu3333 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu3333 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu3333 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

drop table class3333_1;

drop table stu3333_1;

CREATE TABLE class3333_1 AS SELECT * FROM class3333 WHERE 1=0;
CREATE TABLE stu3333_1 AS SELECT * FROM stu3333  WHERE 1=0;
ALTER TABLE stu3333_1 MODIFY name NVARCHAR2(50) NULL;


drop table test_1;
CREATE TABLE test_1(f1 NUMBER, f2 binary_float, f3 char(20), f4 nvarchar2(20), f5 timestamp(9));
INSERT INTO test_1 VALUES(36478.46372, 347.64273, '123456','fhaiu', TO_TIMESTAMP('25-09-2005 12:01:33.33333333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test_1 VALUES(45678.12345, 258.68734, '456789','fndjk', TO_TIMESTAMP('25-09-2006 12:01:44.55555555','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test_1 VALUES(12345.67890, 483.35433, '456789','bfdsh', TO_TIMESTAMP('25-09-2009 12:01:55.55577777','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO test_1 VALUES(45678.64589, 354.58766, '123456','gregg', TO_TIMESTAMP('25-09-2008 12:01:11.33334444','dd-mm-yyyy hh24:mi:ss.ff'));

drop table test_1_bak;
CREATE TABLE test_1_bak AS SELECT * FROM test_1 WHERE 1 = 0;


DROP TABLE t111 CASCADE CONSTRAINTS purge;

DROP TABLE t222 CASCADE CONSTRAINTS purge;

DROP TABLE t333 CASCADE CONSTRAINTS purge;

DROP TABLE t444 CASCADE CONSTRAINTS purge;

DROP TABLE t555 CASCADE CONSTRAINTS purge;

DROP TABLE t666 CASCADE CONSTRAINTS purge;


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


drop table t1_group purge;
create table t1_group(c1 int, c2 int, c3 int);
insert into t1_group values (1, 2, 3);


drop table t1_group_bak purge;
CREATE TABLE t1_group_bak AS
select c1, c2, c3, grouping(c1) gc1, grouping(c2) gc2, grouping(c3) gc3 from t1_group group by rollup(c1, c2), c3;

drop table testtable CASCADE CONSTRAINTS purge;
drop table testtable_bak CASCADE CONSTRAINTS purge;

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

drop table t_exists_in CASCADE CONSTRAINTS purge;

drop table t_exists_in_bak CASCADE CONSTRAINTS purge;

CREATE TABLE t_exists_in (c1 varchar2(50));
insert into t_exists_in values('97749');

CREATE TABLE t_exists_in_bak AS SELECT * FROM t_exists_in;

drop table dlt1;

drop table dlt2;

drop table dlt3;

drop table dlt4;

drop table dlt6;


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

drop table dlt5;
drop table dlt7;
create table dlt5 as select * from dlt1;

create table dlt7 (id int primary key, p_id int, name nvarchar2(50), alias varchar2(50), c_id int);
insert into dlt7 select * from dlt6;
commit;

drop table dlt4_dup;

create table dlt4_dup as
select dlt3.a c1, dlt4.a c2, dlt4.b c3, dlt4.x c4 from (select a from dlt1 minus select c a from dlt2 minus select 1 a from dual) dlt3, dlt1 dlt4 where dlt3.a = dlt4.a;

drop table dlt1_bak;

drop table dlt2_bak;

drop table dlt3_bak;

create table dlt1_bak (id int);
create table dlt2_bak (id int, name varchar2(200));
create table dlt3_bak (name varchar2(200));

drop table dlt55;
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
  nc1 nchar(200),
  l1  blob,
  l2  clob,
  d1  date,
  tz1 timestamp(4) with time zone,
  tz2 timestamp(4) with local time zone,
  t3  timestamp(4),
  i1  interval year(4) to month,
  i2  interval day(5) to second(6),
  w1  urowid(1000)
);

drop table t1_spell;

create table t1_spell(A int, B varchar(40));
insert into  t1_spell values(1, 'asdg');
insert into  t1_spell values(0, 'asdg');
insert into  t1_spell values(2, 'asdg');

drop table t2;
create table t2(a number, b varchar2(40), c TIMESTAMP, d TIMESTAMP WITH TIME ZONE, e TIMESTAMP WITH LOCAL TIME ZONE);
insert into t2 values(2, 'link t2', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
commit;

drop table t3;
create table t3(a number, b varchar2(40), c TIMESTAMP, d TIMESTAMP WITH TIME ZONE, e TIMESTAMP WITH LOCAL TIME ZONE, f INTERVAL YEAR TO MONTH, g INTERVAL DAY (6) TO SECOND (5));
insert into t3 values(2, 'link t3', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, INTERVAL '12-2' YEAR(3) TO MONTH, INTERVAL '4 5:12:10.222' DAY TO
SECOND(3));
commit;

drop table t4;
create table t4(a number, b varchar2(40), c raw(50));
insert into t4 values(3, 'link t4', hextoraw('127dae98'));
commit;

drop table t5;
create table t5(a BINARY_FLOAT, b BINARY_DOUBLE, c FLOAT);
desc t5;
insert into t5 values (3.14159268, 3.141592687, 3.14159268);
commit;

drop table t6;
create table t6(a varchar2(60), b nvarchar2(30));
insert into t6 values('varchar2', 'nvarchar2');
insert into t6 values('中国', '外国');
commit;

drop table t7;
create table t7(a number, b number);
insert into t7 (a, b) select level, 48 + mod(level, 78) from dual connect by level <= 50000;
commit;


drop table t12;

drop table t13;

drop table t14;

drop table t15;

drop table t16;

drop table t17;

create table t12 as select * from t2 where 1 = 0;
create table t13 as select * from t3 where 1 = 0;
create table t14 as select * from t4 where 1 = 0;
create table t15 as select * from t5 where 1 = 0;
create table t16 as select * from t6 where 1 = 0;
create table t17 as select * from t7 where 1 = 0;

COMMIT;


DROP TABLE test1 purge;

DROP TABLE test2 purge;

DROP TABLE test3 purge;


CREATE TABLE test1(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int);
INSERT INTO test1 VALUES(101, 'test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test1 VALUES(102, 'test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test1 VALUES(103, 'test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test1 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test1 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test1 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test1 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test1 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test2(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int);
INSERT INTO test2 VALUES(101, 'test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test2 VALUES(102, 'test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test2 VALUES(103, 'test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test2 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test2 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test2 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test2 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test2 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test3(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int);



drop table DemoOrganization purge;
create table DemoOrganization 
(
OrgID     NUMBER(20,0) ,
OrgName   NVARCHAR2(100),
OrgPath   VARCHAR2(100),
ParentID  INTEGER,
OLevel    INTEGER default 0,
AddTime   DATE);

drop sequence SEQ_DEMOORGANIZATION;
create sequence SEQ_DEMOORGANIZATION minvalue 1 maxvalue 999999999999999 start with 1 increment by 1 cache 201;

DELETE FROM DEMOORGANIZATION;
INSERT INTO DEMOORGANIZATION(ORGNAME,OLEVEL,ORGPATH,PARENTID ,ADDTIME)
select '组织机构1',1,'0',0,to_date('19900101','yyyymmdd') from dual union all
select '组织机构2',1,'0',0,to_date('20000101','yyyymmdd') from dual union all
select '组织机构3',1,'0',0,to_date('20100101','yyyymmdd') from dual union all
select '组织机构4',2,'1',1,to_date('20200101','yyyymmdd') from dual union all
select '组织机构5',2,'2',2,to_date('20300101','yyyymmdd') from dual union all
select '组织机构6',3,'1/4',4,to_date('20400101','yyyymmdd') from dual union all
select '组织机构7',3,'1/4',4,to_date('20500101','yyyymmdd') from dual union all
select '组织机构8',3,'2/5',5,to_date('20600101','yyyymmdd') from dual union all
select '组织机构9',4,'1/4/6',6,to_date('20700101','yyyymmdd') from dual union all
select '组织机构10',4,'1/4/6',6,to_date('20800101','yyyymmdd') from dual union all
select '组织机构11',4,'1/4/6',6,to_date('20900101','yyyymmdd') from dual union all
select '组织机构12',4,'2/5/8',8,to_date('21000101','yyyymmdd') from dual union all
select '组织机构13',4,'2/5/8',8,to_date('21100101','yyyymmdd') from dual;


DROP TABLE test111 purge;

DROP TABLE test222 purge;

DROP TABLE test333 purge;
CREATE TABLE test111(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday));
INSERT INTO test111 VALUES(101, 'test111', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test111 VALUES(102, 'test222', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test111 VALUES(103, 'test333', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test111 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test111 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test111 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test111 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test111 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test222(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday));
INSERT INTO test222 VALUES(101, 'test111', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO test222 VALUES(102, 'test222', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO test222 VALUES(103, 'test333', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO test222 VALUES(104, 'test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO test222 VALUES(105, 'test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO test222 VALUES(106, 'test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO test222 VALUES(107, 'test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO test222 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE test333(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday))
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));

COMMIT;


drop table EMP003;
create GLOBAL TEMPORARY TABLE EMP003(empno INTEGER,ename VARCHAR2(100),job VARCHAR2(100),mgr NUMBER,hiredate DATE, sal NUMBER(9,2),comm NUMBER(9,2),deptno NUMBER(20),deptnoname char(100)) on commit delete rows;

COMMIT;

drop table t_deduce_length CASCADE CONSTRAINTS purge;
drop table t_deduce_length2 CASCADE CONSTRAINTS purge;

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


drop table t_deduce_lengthbak1 CASCADE CONSTRAINTS purge;

drop table t_deduce_lengthbak2 CASCADE CONSTRAINTS purge;

drop table t_deduce_lengthbak3 CASCADE CONSTRAINTS purge;

CREATE TABLE t_deduce_lengthbak1 AS SELECT * FROM t_deduce_length WHERE 1=0;
CREATE TABLE t_deduce_lengthbak2 AS SELECT * FROM t_deduce_length2 WHERE 1=0;
CREATE TABLE t_deduce_lengthbak3(c1 varchar2(200));

drop table TESTBAK;
CREATE TABLE TESTBAK(JOB VARCHAR2(200),DEPT10 INT,DEPT20 INT,DEPT30 INT,DEPT40 INT);
drop table EMPBAK;

CREATE TABLE EMPBAK AS SELECT * FROM EMP WHERE 1=0;
create or replace view pivoted_emp as
select *
  from (select job,deptno,sal from emp)
  pivot (
          sum(sal)
          for deptno 
          in (10 as dept10,20 as dept20,30 as dept30,40 as dept40)
         );
 
select *from pivoted_emp;

drop table TESTBAK2;
CREATE TABLE TESTBAK2(JOB VARCHAR2(200),DEPTNO INT,SAL INT);

DROP TABLE DEPT001 CASCADE CONSTRAINTS purge;

DROP TABLE EMP001 CASCADE CONSTRAINTS purge;

DROP TABLE EMP002 CASCADE CONSTRAINTS purge;

CREATE TABLE DEPT001(DEPTNO NUMBER(2),DNAME VARCHAR2(14) ,LOC VARCHAR2(13)) ;
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

CREATE TABLE EMP002 AS SELECT * FROM EMP001;
COMMIT;

DROP TABLE testbak2 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak2 AS SELECT * FROM stu150;

DROP TABLE testbak3 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak3 AS SELECT * FROM class3333;

DROP TABLE testbak4 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak4 AS SELECT * FROM stu3333;

DROP TABLE testbak5 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak5(c1 varchar2(200));

DROP TABLE testbak6 CASCADE CONSTRAINTS purge;
CREATE TABLE testbak6(c1 varchar2(200),c2 varchar2(200));

COMMIT;

drop table stu1bak;
drop table stu;


create table stu(s1 nvarchar2(6), s2 nvarchar2(6) default 10, s3 nvarchar2(6) default 20, s4 nvarchar2(6) default 30);
create table stu1bak(s1 nvarchar2(6), s2 nvarchar2(6) default 100, s3 nvarchar2(6) default 200, s4 nvarchar2(6) default 300);

insert into stu values(1,1,1,1);
insert into stu values(2,2,2,2);
insert into stu values(6,6,6,6);
insert into stu1bak values(1,11,11,11);
insert into stu1bak values(8,8,8,8);

create or replace view stu_view(ss1,ss2,ss3,ss4) as select * from stu;
create or replace view stu1_view(ss1,ss2,ss3,ss4) as select * from stu1bak;

create or replace synonym my_syn for stu;
create or replace synonym my_syn1 for stu1bak;


DROP TABLE AAA CASCADE CONSTRAINTS purge;
CREATE TABLE AAA (
col_int int,
col_date date,
pk int,
col_number_10_0 number(10,0),
col_char_10 char(10),
col_number_8_2 number(8,2),
col_number_8_3 number(8,3),
col_char_20 char(20) ) ; 
INSERT ALL INTO AAA VALUES  (NULL, to_date('2003-03-28 00:01:10','yyyy-mm-dd hh24:mi:ss'), 1, NULL, '', NULL, NULL, 'i') SELECT 1 FROM DUAL;

DROP TABLE BBB CASCADE CONSTRAINTS purge;
CREATE TABLE BBB (
col_number_10_0 number(10,0),
col_number_8_3 number(8,3),
col_int int,
pk int,
col_char_20 char(20),
col_number_8_2 number(8,2),
col_date date,
col_char_10 char(10) ) ; 
INSERT ALL INTO BBB VALUES  (73, 94, 44, 1, 'him', -9462, to_date('2003-05-26','yyyy-mm-dd'), NULL) INTO BBB VALUES  (9, 7, 29939, 2, 'k', 5, to_date('2004-11-27 14:26:02','yyyy-mm-dd hh24:mi:ss'), 'e') INTO BBB VALUES  (2, 8, -9292, 3, 'v', 5, (Timestamp '2003-02-08 12:27:12.019521'), 'h') INTO BBB VALUES  (NULL, -6645, 15242, 4, '', 86, (Timestamp '2008-08-22 02:24:43.008868'), NULL) INTO BBB VALUES  (-74, -52, -71, 5, 'you''re', 2, (Timestamp '2007-10-09 19:53:04.008332'), '') INTO BBB VALUES  (6, NULL, -2958, 6, NULL, -1547, (Timestamp '2005-12-05 09:18:28.048770'), 'q') INTO BBB VALUES  (NULL, 6174, -91, 7, 'yeah', 0, (Timestamp '2000-10-08 19:46:14.019798'), '') INTO BBB VALUES  (5, NULL, -14, 8, 'w', -21, (Timestamp '2004-04-28 16:56:22.022966'), '') INTO BBB VALUES  (16, NULL, NULL, 9, '', NULL, NULL, 'j') INTO BBB VALUES  (8, 1, NULL, 10, 'b', 80, to_date('2001-08-07 00:54:36','yyyy-mm-dd hh24:mi:ss'), 'g') INTO BBB VALUES  (0, NULL, -97, 11, '', 59, (Timestamp '2003-10-07 20:33:05.051703'), 'go') INTO BBB VALUES  (NULL, NULL, 9445, 12, 'p', -14080, (Timestamp '2008-07-28 23:10:48.034080'), NULL) INTO BBB VALUES  (NULL, NULL, -31067, 13, 'n', 32, (Timestamp '2002-10-13 05:23:41.014522'), NULL) INTO BBB VALUES  (NULL, -124, NULL, 14, 'go', NULL, (Timestamp '2009-02-13 11:04:35.039256'), '') INTO BBB VALUES  (NULL, 29, NULL, 15, '', NULL, (Timestamp '2005-11-24 06:24:29.062784'), '') INTO BBB VALUES  (0, NULL, 1, 16, 'q', 9, (Timestamp '2000-03-20 17:51:52.010486'), 'v') INTO BBB VALUES  (NULL, 0, NULL, 17, 'q', 8, NULL, 'w') INTO BBB VALUES  (4, 60, NULL, 18, 'to', 6, (Timestamp '2005-09-10 06:44:18.007092'), 'a') INTO BBB VALUES  (NULL, NULL, -20124, 19, NULL, NULL, NULL, 'u') INTO BBB VALUES  (NULL, -4792, 9, 20, 'her', -95, NULL, 'didn''t') INTO BBB VALUES  (NULL, 8, -16808, 21, 'I''ll', 56, to_date('2001-10-17 14:10:04','yyyy-mm-dd hh24:mi:ss'), 'w') INTO BBB VALUES  (-72, -35, -4190, 22, 'at', NULL, to_date('2007-11-07','yyyy-mm-dd'), NULL) INTO BBB VALUES  (NULL, NULL, 3, 23, 'a', NULL, (Timestamp '2005-10-08 20:21:22.028610'), 'and') INTO BBB VALUES  (-18339, NULL, 8, 24, 'r', NULL, to_date('2009-11-12 19:35:19','yyyy-mm-dd hh24:mi:ss'), 'yeah') INTO BBB VALUES  (NULL, NULL, 0, 25, NULL, 34, to_date('2000-01-01 07:16:24','yyyy-mm-dd hh24:mi:ss'), '') INTO BBB VALUES  (NULL, 4509, NULL, 26, 'c', 19327, to_date('2000-08-14','yyyy-mm-dd'), 'i') INTO BBB VALUES  (NULL, -29, NULL, 27, 'good', -70, (Timestamp '2004-10-10 18:38:59.055764'), '') INTO BBB VALUES  (NULL, -38, 9256, 28, 'x', 22, to_date('2001-04-04 10:23:28','yyyy-mm-dd hh24:mi:ss'), '') INTO BBB VALUES  (-79, -2097, 126, 29, 'x', NULL, to_date('2009-09-25','yyyy-mm-dd'), NULL) INTO BBB VALUES  (3, -24177, 74, 30, 'q', 8, (Timestamp '2005-09-01 07:29:45.064089'), 'd') SELECT 1 FROM DUAL;


DROP TABLE EMPEMP001 CASCADE CONSTRAINTS purge;
create TABLE EMPEMP001(empno   INTEGER,ename   VARCHAR2(100),job  VARCHAR2(100),mgr   NUMBER,hiredate date,sal   NUMBER(9,2),comm   NUMBER(9,2),deptno  NUMBER(20),deptnoname   char(100));
INSERT INTO EMPEMP001 VALUES(1,'SMITH001','CLERK',7901,to_date('17-12-1981','dd-mm-yyyy'),800,NULL,20,'分部1');
INSERT INTO EMPEMP001 VALUES(2,'SMITH002','CLERK',7902,to_date('17-12-1982','dd-mm-yyyy'),900,NULL,30,'分部2');
INSERT INTO EMPEMP001 VALUES(3,'SMITH003','CLERK',7903,to_date('17-12-1983','dd-mm-yyyy'),1000,NULL,40,'分部3');

DROP TABLE EMPEMP002 CASCADE CONSTRAINTS purge;
create TABLE EMPEMP002(empno   INTEGER,ename   VARCHAR2(100),job  VARCHAR2(100),mgr   NUMBER,hiredate date,sal   NUMBER(9,2),comm   NUMBER(9,2),deptno  NUMBER(20),deptnoname   char(100));
INSERT INTO EMPEMP002 VALUES(1,'SMITH001','CLERK',2,to_date('17-12-2981','dd-mm-yyyy'),8000,NULL,20,'分部10');
INSERT INTO EMPEMP002 VALUES(2,'SMITH002','CLERK',4,to_date('17-12-2982','dd-mm-yyyy'),9000,NULL,30,'分部20');
INSERT INTO EMPEMP002 VALUES(3,'SMITH003','CLERK',3,to_date('17-12-2983','dd-mm-yyyy'),2000,NULL,40,'分部30');

create or replace view   EMP001_view AS  SELECT  *  FROM    EMPEMP001;
create or replace view   EMP002_view AS  SELECT  *  FROM    EMPEMP002;

COMMIT;

drop table sourceTable;

drop table targetTable;

drop view targetView;

create table sourceTable(id int, sales int) partition by hash(id) partitions 4;
create table targetTable(id int, sales int) partition by hash(id) partitions 4;
create view targetView as select * from targetTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

drop table t11;
drop table t22;

create table t11 (c1 int primary key, c2 int, 
                 c3 decimal generated always as (c1 + c2), 
                 c4 int generated always as ((c1 + c2) * (c1 - c2)), 
                 c5 varchar(64) generated always as (to_char(c1 - c2)),
                 c6 decimal generated always as (c1 + c2 + 0) virtual);
create table t22 (c1 int primary key, c2 decimal);
insert into t22 values (1, 2.5);

select count(*) from t22;
select count(*) from t11;

drop table stu222;

drop table stu333;
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

drop table stu555;

drop table stu444;

create table stu444(s1 number, s2 nvarchar2(6) unique, s3 nvarchar2(6) default null not null, s4 nvarchar2(6), constraint stu_pk primary key(s1)) partition by hash(s1) partitions 4;
insert into stu444 values(1,1,1,1);
insert into stu444 values(2,2,2,2);
insert into stu444 values(3,3,3,3);

create table stu555(s1 number, s2 nvarchar2(6), s3 nvarchar2(6) default null, s4 nvarchar2(6), constraint stu1_pk primary key(s1)) partition by hash(s1) partitions 4;
insert into stu555 values(100,100,100,1);
insert into stu555 values(600,600,600,6);
insert into stu555 values(700,700,700,7);

drop table stu666;
drop table stu777;

create table stu666(s1 number, s2 nvarchar2(6) default 62, s3 nvarchar2(6), s4 nvarchar2(6) default 84) ;
insert into stu666 values(1,1,1,1);
insert into stu666 values(2,2,2,2);
insert into stu666 values(10,10,10,10);
insert into stu666 values(16,16,16,16);
insert into stu666 values(88,88,88,88);

create table stu777(s1 number, s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6)) ;
insert into stu777 values(1,12,13,14);
insert into stu777 values(2,22,23,24);
insert into stu777 values(6,62,63,64);
insert into stu777 values(8,82,83,84);
insert into stu777 values(10,102,103,104);
insert into stu777 values(88,882,883,884);
insert into stu777 values(100,1002,1003,1004);

drop table stu999;

drop table stu888;


create table stu888(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6), s5 nvarchar2(6));
create table stu999(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6), s4 nvarchar2(6), s5 nvarchar2(6));

insert into stu888 values('a','b1','c1','d1','e1');
insert into stu888 values('a','b2','c2','d2','e1');
insert into stu888 values('a','b3','c3','d3','e2');
insert into stu888 values('a','b4','c4','d4','e2');
insert into stu888 values('b','b5','c5','d5','e1');
insert into stu999 values('a','bb','cc','dd','e1');

drop table stu2222;
drop table stu1111;

create table stu1111(s1 nvarchar2(6), s2 nvarchar2(6), s3 nvarchar2(6));
create table stu2222(s1 nvarchar2(6) unique, s2 nvarchar2(6), s3 nvarchar2(6));


insert into stu1111(s1,s2) values(1,1);
insert into stu1111(s1,s2) values(2,2);
insert into stu1111 values(3,3,3);
insert into stu2222(s1,s2) values(10,10);
insert into stu2222 values(30,30,3);
insert into stu2222 values(20,20,20);

drop table stu4444;

drop table stu3333;


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

drop table stu6666;
drop table stu5555;

create table stu5555(s1 number, s2 nvarchar2(6) default 62, s3 nvarchar2(6), s4 nvarchar2(6) default 84) ;

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

drop table stu8888;

drop table stu7777;


create table stu7777(s1 number, s2 nvarchar2(6), s3 nvarchar2(6));
create table stu8888(s1 number, s2 nvarchar2(6));

insert into stu7777 values(10,10,10);
insert into stu8888 values(100,10);
insert into stu8888 values(200,20);

COMMIT;

drop table test;

create table test (c1 int , c2 char(5 char), c22 char(5 byte), c3 timestamp, c4 int);


drop table t44;

create table t44 (pk int primary key, key1 int, key2 int, v1 varchar(20 char), v12 varchar(20 byte));
insert into t44 values(1,1,1,'a','a');
insert into t44 values(2,1,1,'b','b');
insert into t44 values(3,2,2,'c','c');
insert into t44 values(4,3,3,'d','d');


drop table ts1;

drop table ts2;

drop table ts3;

drop table ts4;

drop table t1;

drop table t2;

drop table tp;

create table t1(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int);
create table tp(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int) partition by hash(c1) partitions 5;
create table ts1(c1 int, c2 int);
create table ts2(c1 int, c2 int);
create table ts3(c1 int, c2 int);
create table ts4(c1 int, c2 int);

insert into t1 select level, level, level, level, level, level, level, level , level from dual connect by level < 20;
insert into tp select * from t1;
insert into ts1 select level, level from dual connect by level < 5;
insert into ts2 select c1*2, c2*2 from ts1;
insert into ts3 select c1*3, c2*3 from ts1;
insert into ts4 select c1*4, c2*4 from ts1;


drop table t100;

CREATE TABLE t100
 (
 place_id int NOT NULL,
 shows int not null,
 ishows int not null ,
 ushows int not null ,
 clicks int not null ,
 iclicks int not null ,
 uclicks int not null ,
 ts timestamp
 );
 

drop table t200;
drop table t300;
create table t200(f1 int, f2 int);
create table t300(f3 int, f4 int);

drop table t400;
create table t400(f1 int);

drop table t500;
create table t500 (f1 date not null);

drop table t600;
create table t600(f1 int, "*f2" int);

drop table t700;
CREATE TABLE t700 (
    pk int not null,
    primary key (pk)) ;


drop table t800;
drop table t900;
drop view v800;
drop view v900;

create table t800 (c1 int primary key, c2 int, c3 int);
create table t900 (c1 int primary key, c2 int, c3 int);
insert into t800 (c1, c2, c3) values (1, 1, 1);
insert into t900 (c1, c2, c3) values (1, 1, 1);
create view v800 as select * from t800;
create view v900 as select t800.c1 c1, t800.c2 c2, t800.c3 c3, t900.c1 c4, t900.c2 c5, t900.c3 c6 from t800, t900 where t800.c1 = t900.c1;


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

drop table ta;
drop table t11;

create table ta (a integer primary key, b integer);
create table t11(c1 int default 12,c2 int default 13);

drop table t2;
drop table t3;

create table t2 (c1 int, c2 int, c3 int);
create table t3 (c1 int, c2 int, c3 int);

drop table t1_test1;

drop table t1_part;

drop table t2_test1;

drop table t2_part;

drop table t3_test1;

drop table t3_part;


create table t1_test1(a decimal, b char(10), c varchar(20), d decimal);
create table t1_part(a decimal, b char(10), c varchar(20), d decimal) partition by hash(a) partitions 4;
create table t2_test1(a decimal, b decimal, c decimal);
create table t2_part(a decimal, b decimal, c decimal) partition by hash(a) partitions 4;
create table t3_test1(a decimal, b char(10), c varchar(20));
create table t3_part(a decimal, b char(10) primary key, c varchar(20)) partition by hash(b) partitions 4;


drop table test1_test1;

drop table test2_test2;

create table test1_test1 (f1 int, f2 char(10), f3 varchar(10));
insert into test1_test1 values(1,'aaa','a1');
insert into test1_test1 values(2,'bbb','b1');
insert into test1_test1 values(null,null,null);
create table test2_test2(f1 int, f2 char(10), f3 varchar(10) default 'admin')
partition by range(f1) (partition p1 values less than(5), partition p2 values less than (maxvalue));


drop table t1_0915;
create table t1_0915 (a int, b int);

COMMIT;

DROP TABLE INT_GAUSSIAN_100000_T2_GENERAL purge;

DROP TABLE TEST_VARCHAR2 purge;


CREATE TABLE INT_GAUSSIAN_100000_T2_GENERAL(f1 int, f2 int, f3 int,f4 int, f5 int, f6 int);
CREATE TABLE TEST_VARCHAR2(f1 VARCHAR2(200));
COMMIT;

drop table class1;

drop table stu1;

drop table class2;

drop table stu2;

drop table class3;

drop table stu3;


CREATE TABLE class1 (id int, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class1 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE TABLE stu1 (id int PRIMARY KEY, p_id int,name nvarchar2(50) , alias char(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu1 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu1 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu1 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu1 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu1 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu1 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu1 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE TABLE class2 (id int PRIMARY KEY, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5)) ;
INSERT INTO class2 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class2 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE TABLE stu2 (id int, p_id int,name nvarchar2(50) , alias char(50), birth DATE, weight binary_double,c_id int) ;
INSERT INTO stu2 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu2 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu2 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu2 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu2 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu2 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu2 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE TABLE class3 (id int, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5));
INSERT INTO class3 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class3 VALUES(105,'qiqi','yuhang',to_date('20051010','yyyymmdd'));

CREATE TABLE stu3 (id int, p_id int,name nvarchar2(50) , alias char(50), birth DATE, weight binary_double,c_id int);
INSERT INTO stu3 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu3 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu3 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu3 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu3 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu3 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu3 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);


drop table class3_1;

drop table stu3_1;

CREATE TABLE class3_1 AS SELECT * FROM class3 WHERE 1=0;
CREATE TABLE stu3_1 AS SELECT * FROM stu3  WHERE 1=0;

CREATE OR REPLACE synonym  syn_class3  for class3;
drop table test111;
create table test111 (c1 int ,c2 int, c3 varchar(4));
drop table T_GX_DM_ZC_MANAGE;


CREATE TABLE "T_GX_DM_ZC_MANAGE" (
  "PROV_BRANCH_CODE" VARCHAR2(6 CHAR),
  "CITY_BRANCH_CODE" VARCHAR2(20 CHAR),
  "TOWN_BRANCH_CODE" VARCHAR2(20 CHAR),
  "ZCNO" VARCHAR2(20 CHAR),
  "KEY_SNAP_DATE" NUMBER,
  "CZRL" NUMBER,
  "JYRL" NUMBER,
  "YXRL" NUMBER,
  "CXJJRL" NUMBER,
  "XZRL" NUMBER,
  "YINIANNEI_XRCXJJRL" NUMBER,
  "SANJINLV_FZ" NUMBER,
  "SANJINLV_FM" NUMBER,
  "LIUCUNLV13_FZ" NUMBER,
  "LIUCUNLV13_FM" NUMBER,
  "SNQJ_PREM" NUMBER,
  "STD_PREM" NUMBER(12,2),
  "SNQJ_10_PREM" NUMBER(12,2),
  "BAOZHANG_PREM" NUMBER(12,2),
  "XINGJIRENLI" NUMBER,
  "XINGJIHUIYUAN" NUMBER,
  "SNQJ_JS" NUMBER,
  "FYC" NUMBER,
  "CANHUILV_FZ" NUMBER,
  "CANHUILV_FM" NUMBER,
  "JUEDUICANHUILV_FZ" NUMBER,
  "JUEDUICANHUILV_FM" NUMBER,
  "BAFENRENLI" NUMBER,
  "TUTOR_HR" NUMBER,
  "STAMP" DATE,
  "ADDTIME" TIMESTAMP(6),
  "F_FIRST_HR" NUMBER,
  "SYRL_ZC_CHAIFEN" NUMBER,
  "ZCTYPE" VARCHAR2(1 CHAR),
  "SJTYPE" VARCHAR2(1 CHAR),
  "USE_STATUS" VARCHAR2(1 CHAR),
  "YINIAN_XRRL" NUMBER
);

drop table tf67;

drop table tf7;

drop table tf6;

drop table t_constrain1;

drop table t_constrain2;

drop table t_gen_col1;

drop table t_gen_col2;

drop table t_type;

drop table t_type1;

drop table t_type2;

drop table t_part_val;

drop table t_part;

drop table t_part2;

drop table t_part3;

drop table t_local;

drop table t_global;

drop table t_part_global;

drop table t_trigger1;

drop table t_trigger2;

drop table t_temp1;

drop table t_temp2;
drop sequence s1;

create table tf6(a int , b int, c int);
create table tf7(a int, b int, c int);
create table tf67(a int , b int, c int);
create table t_constrain1(c1 int, c2 int, c3 int, constraint pk1 primary key(c1), constraint check_cst1 check(c3 > 3));
create table t_constrain2(c1 int, c2 int, c3 int, constraint pk2 primary key(c1), constraint check_cst2 check(c1 > 2), constraint check_cst3 check(c2 > 4));
create table t_gen_col1(c1 int , c2 int, c3 int as (c1 + c2 + 100) virtual);
create table t_gen_col2(c1 int default 1, c2 int, c3 int as (c1 + c2 + 50) virtual);
create table t_type(c1 int, c2 varchar(20), c3 float);
create table t_type1(c1 int, c2 varchar(20), c3 float);
create table t_type2(c1 int, c2 varchar(20), c3 float);
create table t_part_val(c1 int , c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_part(c1 int , c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_part2(c1 int , c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_part3(c1 int , c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_local (c1 int, c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_global (c1 int, c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_part_global (c1 int, c2 int, c3 int) partition by hash(c1) partitions 4;
create table t_trigger1(c1 int, c2 int, c3 int);
create table t_trigger2(c1 int, c2 int, c3 int);
create global temporary table t_temp1(c1 int, c2 int, c3 int);
create global temporary table t_temp2(c1 int, c2 int, c3 int);
create sequence s1 start with 1;

insert into t_type values (1, 'abc123阿里巴巴', 2.222222);
insert into t_type values (2, '蚂蚁集团123', -1.11111);
insert into t_type values (3, 'abc123', 0.99999);
insert into t_part_val values (1, 2, 3);
insert into t_part_val values (2, 4, 4);
insert into t_part_val values (3, 5, 6);

DROP TABLE test11;
DROP TABLE test13;
CREATE TABLE test11(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test13(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);

DROP TABLE test21;

DROP TABLE test24;
CREATE TABLE test21(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test24(id int, name nchar(20), name_1 varchar2(100) AS (CONCAT(trim(name),'_1')) virtual, birthday DATE, weight binary_float, p_id int);
DROP TABLE test31;
DROP TABLE test33;
DROP TABLE test34;
CREATE TABLE test31(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test33(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test34(id int, name nchar(20), name_1 varchar2(100) AS (CONCAT(trim(name),'_1')) virtual, birthday DATE, weight binary_float, p_id int);

DROP TABLE test41;
DROP TABLE test42;

CREATE TABLE test41(char_ char(20),nchar_ nchar(20),varchar2_ varchar2(20),nvarchar2_ nvarchar2(20),interval_ INTERVAL YEAR TO MONTH);
CREATE TABLE test42(char_ char(20),nchar_ nchar(20),varchar2_ varchar2(20),nvarchar2_ nvarchar2(20),interval_ INTERVAL YEAR TO MONTH);

DROP TABLE test51;
DROP TABLE test52;

CREATE TABLE test51(c1 INT);
CREATE TABLE test52(c1 INT);

COMMIT;

drop table test22 purge;
create table test22(f1 int, f2 char(10), f3 varchar2(10) default 'admin')
partition by range(f1) (partition p1 values less than(5), partition p2 values less than (maxvalue));

drop table test111 purge;

drop table test222 purge;
create table test111 (f1 int, f2 char(10), f3 varchar2(10));
create table test222 (f1 int, f2 char(10), f3 varchar2(10));


drop table test333 purge;

drop table test444 purge;
create table test333 (f1 int, f2 char(10), f3 nvarchar2(10), f4 date);
create table test444 (f1 int, f2 char(10), f3 nvarchar2(10), f4 date);
insert into test333 values(1,'abc','a1',to_date('19700101','yyyymmdd'));
insert into test333 values(2,'bcd','b1',to_date('19800101','yyyymmdd'));
insert into test333 values(3,'cde','c1',to_date('19900101','yyyymmdd'));
insert into test333 values(4,'def','d1',to_date('20000101','yyyymmdd'));
insert into test333 values(5,'efg','e1',to_date('20100101','yyyymmdd'));
insert into test333 values(6,'fgh','f1',to_date('20200101','yyyymmdd'));
insert into test333 values(null,null,NULL,NULL);


drop table test555 purge;

drop table test666 purge;

drop table test777 purge;

create table test555 (id int);
INSERT INTO test555 VALUES(1001);
INSERT INTO test555 VALUES(1002);
INSERT INTO test555 VALUES(1003);

create table test666 (
id int primary key, 
name char(10) , 
name_1 nvarchar2(10) not null, 
birthday DATE DEFAULT to_date('20000101','yyyymmdd'), 
age number(3,0) CHECK(age>0 AND age<150),
p_id int);
INSERT INTO test666 VALUES(1,'stu1','aaa',to_date('19700101','yyyymmdd'),18,1001);
INSERT INTO test666 VALUES(2,'stu2','bbb',to_date('19800101','yyyymmdd'),28,1001);
INSERT INTO test666 VALUES(3,'stu3','ccc',to_date('19900101','yyyymmdd'),38,1002);
INSERT INTO test666 VALUES(4,'stu4','ddd',to_date('20000101','yyyymmdd'),48,1002);
INSERT INTO test666 VALUES(5,'stu5','eee',to_date('20100101','yyyymmdd'),58,1003);
INSERT INTO test666 VALUES(6,NULL,'fff',to_date('20200101','yyyymmdd'),68,1003);

create table test777 (
id int primary key, 
name char(10), 
name_1 nvarchar2(10) not null, 
birthday DATE DEFAULT to_date('20000101','yyyymmdd'), 
age number(3,0) CHECK(age>0 AND age<150),
p_id int);


DROP TABLE t_test1 purge;

DROP TABLE t_test2 purge;

DROP TABLE t_test3 purge;
CREATE TABLE t_test1(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday));
INSERT INTO t_test1 VALUES(101, 't_test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO t_test1 VALUES(102, 't_test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO t_test1 VALUES(103, 't_test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO t_test1 VALUES(104, 't_test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO t_test1 VALUES(105, 't_test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO t_test1 VALUES(106, 't_test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO t_test1 VALUES(107, 't_test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO t_test1 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE t_test2(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday));
INSERT INTO t_test2 VALUES(101, 't_test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO t_test2 VALUES(102, 't_test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO t_test2 VALUES(103, 't_test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO t_test2 VALUES(104, 't_test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO t_test2 VALUES(105, 't_test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO t_test2 VALUES(106, 't_test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO t_test2 VALUES(107, 't_test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO t_test2 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE t_test3(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int, PRIMARY KEY(id,birthday));
DROP SEQUENCE seq;
CREATE SEQUENCE seq MINVALUE 101 MAXVALUE 999999999999999 START WITH 101 INCREMENT BY 1 cache 201;

CREATE OR REPLACE synonym t_test1_syn FOR t_test1;
CREATE OR REPLACE synonym t_test2_syn FOR t_test2;
CREATE OR REPLACE synonym t_test3_syn FOR t_test3;
CREATE OR REPLACE VIEW t_test1_view AS SELECT * FROM t_test1;
CREATE OR REPLACE VIEW t_test2_view AS SELECT * FROM t_test2;
CREATE OR REPLACE VIEW t_test3_view AS SELECT * FROM t_test3;
CREATE OR REPLACE synonym t_test1_view_syn FOR t_test1_view;
CREATE OR REPLACE synonym t_test2_view_syn FOR t_test2_view;
CREATE OR REPLACE synonym t_test3_view_syn FOR t_test3_view;
CREATE OR REPLACE VIEW t_test1_syn_view AS SELECT * FROM t_test1_syn;
CREATE OR REPLACE VIEW t_test2_syn_view AS SELECT * FROM t_test2_syn;
CREATE OR REPLACE VIEW t_test3_syn_view AS SELECT * FROM t_test3_syn;

drop table class4;
CREATE TABLE class4(id int);
INSERT INTO class4 VALUES(1);
INSERT INTO class4 VALUES(2);
INSERT INTO class4 VALUES(3);
INSERT INTO class4 VALUES(2);

drop table class5;
CREATE TABLE class5(id int);
INSERT INTO class5 VALUES(1);
INSERT INTO class5 VALUES(2);
INSERT INTO class5 VALUES(3);
INSERT INTO class5 VALUES(2);

COMMIT;
DROP TABLE stu1215 CASCADE CONSTRAINTS purge;
DROP TABLE stu1216 CASCADE CONSTRAINTS purge;
create table stu1216(s1 varchar2(2), s2 varchar2(40) generated always as(s1+'好的'), constraint s11_ch check(s2<=11));
create table stu1215(s1 varchar2(20), s2 varchar2(40) generated always as(concat(s1,'好的')));
insert into stu1215(s1) values('中文啊1+b');
select * from stu1215 order by s1;

drop table testtable1;
create table testtable1
(
c1 int,
srcuser1 varchar2(4000),
srchost1 char(2000),
dstuser1 nvarchar2(2000),
dsthost1 nchar(1000),
size2 int
);
insert into testtable1 values(1,lpad('中文测试1a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(2,lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(3,rpad('中文测试3a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',2000,'好'),rpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(4,rpad('中文测试4a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',2000,'好'),rpad('中文测试2a',1000,'好'),100);
select * from testtable1 order by c1;
commit;

DROP TABLE stu1217;
create table stu1217(s1 varchar2(2), s2 varchar2(20) generated always as(concat(s1,'好的啊')));
insert into stu1217(s1) values(1);
insert into stu1217(s1) values('');
insert into stu1217(s1) values(null);
insert into stu1217(s1) values('0');
select s2 from stu1217 order by s2 desc;
select s2 from stu1217 order by s2 asc;
commit;

DROP TABLE stu1218;
create table stu1218(s1 varchar2(20));
insert into stu1218(s1) values('测试');
insert into stu1218 values('   ');
insert into stu1218 values('  ');
insert into stu1218 values(null);
insert into stu1218 values(',,,');
insert into stu1218 values('\\\\');
insert into stu1218 values('\\');
insert into stu1218 values('**');
insert into stu1218 values('$$');
insert into stu1218 values('%%');
insert into stu1218 values('''');
insert into stu1218 values('''''');
insert into stu1218 values(123);
insert into stu1218 values('abc');
insert into stu1218 values(''' ');
insert into stu1218 values(' '' ');
insert into stu1218 values('');
commit;

drop table stu1219;
create table stu1219(s1 varchar2(20) , s2 varchar2(20) , s3 varchar2(20));
insert into stu1219 values('韩国', '韓国', '??');
insert into stu1219 values(260, 'abc', '1a?');
insert into stu1219 values(028, '0ab', '01a');
insert into stu1219 values('讋讋讋', '詟詟詟', '詟詟詟');
insert into stu1219 values('???', '???', '???');



