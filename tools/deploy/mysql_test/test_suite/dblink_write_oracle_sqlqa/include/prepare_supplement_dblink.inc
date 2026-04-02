ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0,942
drop table class1;
--error 0,942
drop table stu1;
--error 0,942
drop table class2;
--error 0,942
drop table stu2;
--error 0,942
drop table class3;
--error 0,942
drop table stu3;
--enable_warnings

--echo #dblink-4	dml
CREATE TABLE class1 (id int, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5))
PARTITION BY list(id) (PARTITION p1 VALUES(101,102),PARTITION p2 VALUES(DEFAULT));
INSERT INTO class1 VALUES(101,DEFAULT,'binjiang',to_timestamp('25-09-2005 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(102,'dianzi','xixi',to_timestamp('25-09-2008 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(103,'jisuanji','binjiang',to_timestamp('25-09-2018 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(104,'yishu','xixi',to_timestamp('25-09-2020 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
INSERT INTO class1 VALUES(106,'baba','xiaoshan',to_date('20071010','yyyymmdd'));

CREATE TABLE stu1 (id int PRIMARY KEY, p_id int,name nvarchar2(50) , alias char(50), birth DATE, weight binary_double,c_id int)PARTITION BY RANGE(id) (PARTITION p1 VALUES less than (1004),PARTITION p2 VALUES less than (maxvalue));
INSERT INTO stu1 VALUES(1001,NULL,'zhangsan','zs',to_date('25-09-1991 12:01:33','dd-mm-yyyy hh24:mi:ss'),47.5,101);
INSERT INTO stu1 VALUES(1002,1001,'lisi','ls',to_date('25-09-1992 12:01:33','dd-mm-yyyy hh24:mi:ss'),57.5,101);
INSERT INTO stu1 VALUES(1003,1001,'wangwu','ww',to_date('25-09-1993 12:01:33','dd-mm-yyyy hh24:mi:ss'),67.5,102);
INSERT INTO stu1 VALUES(1004,1002,'zhaoliu','zl',to_date('25-09-1994 12:01:33','dd-mm-yyyy hh24:mi:ss'),77.5,102);
INSERT INTO stu1 VALUES(1005,1002,'zhangsi','zs',to_date('25-09-1995 12:01:33','dd-mm-yyyy hh24:mi:ss'),87.5,103);
INSERT INTO stu1 VALUES(1006,1002,'liwu','lw',to_date('25-09-1996 12:01:33','dd-mm-yyyy hh24:mi:ss'),37.5,103);
INSERT INTO stu1 VALUES(1007,1003,'wangliu','wl',to_date('25-09-1997 12:01:33','dd-mm-yyyy hh24:mi:ss'),97.5,103);

CREATE TABLE class2 (id int PRIMARY KEY, name nchar(50) DEFAULT 'yinyue',location varchar2(50),create_time timestamp(5)) PARTITION BY hash(id) PARTITIONS 2;
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

--disable_warnings
--error 0,942
drop table class3_1;
--error 0,942
drop table stu3_1;
--enable_warnings
CREATE TABLE class3_1 AS SELECT * FROM class3 WHERE 1=0;
CREATE TABLE stu3_1 AS SELECT * FROM stu3  WHERE 1=0;


##value之间存在引用关系时，要做类型转换
--error 0, 942
drop table test11;
create table test11 (c1 int ,c2 int, c3 varchar(4));

--disable_warnings
--error 0, 942
drop table T_GX_DM_ZC_MANAGE;
--enable_warnings

--result_format 4

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


--disable_warnings
--error 0, 942
drop table t1;
--error 0, 942
drop table t2;
--error 0, 942
drop table t3;
--error 0, 942
drop table t4;
--error 0, 942
drop table t5;
--error 0, 942
drop table tf67;
--error 0, 942
drop table tf7;
--error 0, 942
drop table tf6;
--error 0, 942
drop table t7;
--error 0, 942
drop table t6;
--error 0, 942
drop table t_constrain1;
--error 0, 942
drop table t_constrain2;
--error 0, 942
drop table t_gen_col1;
--error 0, 942
drop table t_gen_col2;
--error 0, 942
drop table t_type;
--error 0, 942
drop table t_type1;
--error 0, 942
drop table t_type2;
--error 0, 942
drop table t_part_val;
--error 0, 942
drop table t_part;
--error 0, 942
drop table t_part2;
--error 0, 942
drop table t_part3;
--error 0, 942
drop table t_local;
--error 0, 942
drop table t_global;
--error 0, 942
drop table t_part_global;
--error 0, 942
drop table t_trigger1;
--error 0, 942
drop table t_trigger2;
--error 0, 942
drop table t_temp1;
--error 0, 942
drop table t_temp2;
--error 0, 2289
drop sequence s1;
--enable_warnings

create table t1(c1 int, c2 int, c3 int);
create table t2(c1 int , c2 int, c3 int);
create table t3(c1 int, c2 int, c3 int);
create table t4(c1 int, c2 int, c3 int);
create table t5(c1 int, c2 int, c3 int);
create table t6(a int , b int, c int);
create table tf6(a int , b int, c int);
create table t7(a int , b int, c int);
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

--disable_query_log
insert into t1 values (1, 2, 3);
insert into t1 values (2, 4, 4);
insert into t1 values (3, 5, 6);
insert into t_type values (1, 'abc123阿里巴巴', 2.222222);
insert into t_type values (2, '蚂蚁集团123', -1.11111);
insert into t_type values (3, 'abc123', 0.99999);
insert into t_part_val values (1, 2, 3);
insert into t_part_val values (2, 4, 4);
insert into t_part_val values (3, 5, 6);
--enable_query_log

set autocommit = 1;
--disable_warnings
--error 0, 942
DROP TABLE test1;
--error 0, 942
DROP TABLE test2;
--error 0, 942
DROP TABLE test4;
--enable_warnings

CREATE TABLE test1(id int, name varchar2(10), school nchar(10), birthday date);
CREATE TABLE test2(id int, name varchar2(10), school nchar(10), birthday date);
CREATE TABLE test4(id int, name varchar2(10), school nchar(10), birthday date) PARTITION BY RANGE(id) (
	PARTITION p1 VALUES less than (3),
	PARTITION p2 VALUES less than (5),
	PARTITION p3 VALUES less than (MAXVALUE));

--disable_result_log
--disable_query_log
INSERT INTO test1 VALUES(1,'zhangsan','yucai',to_date('20000101','yyyymmdd'));
INSERT INTO test1 VALUES(2,'lisi','huawang',to_date('20020101','yyyymmdd'));
INSERT INTO test1 VALUES(3,'wangwu','xiwang',to_date('20040101','yyyymmdd'));
INSERT INTO test1 VALUES(4,'zhaoliu','binjiang',to_date('20060101','yyyymmdd'));
INSERT INTO test1 VALUES(5,'yangyang','yuhang',to_date('20080101','yyyymmdd'));
INSERT INTO test1 VALUES(6,'guoguo','xiaoshan',to_date('20100101','yyyymmdd'));
INSERT INTO test1 VALUES(NULL,NULL,NULL,NULL);
select count(*) from test1;
--enable_result_log
--enable_query_log

--disable_warnings
--error 0, 942
DROP TABLE test11;
--error 0, 942
DROP TABLE test13;
--enable_warnings

CREATE TABLE test11(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int)
PARTITION BY RANGE(birthday) (PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE TABLE test13(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int)
PARTITION BY RANGE(birthday) (PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));

--disable_warnings
--error 0, 942
DROP TABLE test21;
--error 0, 942
DROP TABLE test24;
--enable_warnings

CREATE TABLE test21(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test24(id int, name nchar(20), name_1 varchar2(100) AS (CONCAT(trim(name),'_1')) virtual, birthday DATE, weight binary_float, p_id int);

--disable_warnings
--error 0, 942
DROP TABLE test31;
--error 0, 942
DROP TABLE test33;
--error 0, 942
DROP TABLE test34;
--enable_warnings

CREATE TABLE test31(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test33(id int, name nchar(20), name_1 varchar2(100), birthday DATE, weight binary_float, p_id int);
CREATE TABLE test34(id int, name nchar(20), name_1 varchar2(100) AS (CONCAT(trim(name),'_1')) virtual, birthday DATE, weight binary_float, p_id int);

--disable_warnings
--error 0, 942
DROP TABLE test41;
--error 0, 942
DROP TABLE test42;
--enable_warnings

CREATE TABLE test41(char_ char(20),nchar_ nchar(20),varchar2_ varchar2(20),nvarchar2_ nvarchar2(20),interval_ INTERVAL YEAR TO MONTH);
CREATE TABLE test42(char_ char(20),nchar_ nchar(20),varchar2_ varchar2(20),nvarchar2_ nvarchar2(20),interval_ INTERVAL YEAR TO MONTH);

--disable_warnings
--error 0, 942
DROP TABLE test51;
--error 0, 942
DROP TABLE test52;
--enable_warnings

CREATE TABLE test51(c1 INT);
CREATE TABLE test52(c1 INT);

COMMIT;