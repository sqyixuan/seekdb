SET NAMES UTF8;
set autocommit = on;
set ob_query_timeout=99900000000;
set ob_trx_timeout = 1000000000;  

--disable_query_log
--disable_warnings
DROP TABLE IF EXISTS class150;
CREATE TABLE class150 (id int, name varchar(50) DEFAULT 'yinyue',location varchar(50),create_time timestamp(5))
PARTITION BY LIST(id) (
PARTITION p1 VALUES IN (101, 102),
PARTITION p2 VALUES IN (DEFAULT)
);
CREATE INDEX index_11 ON class150(create_time);
INSERT INTO class150 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class150 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class150 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class150 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class150 VALUES(106,'baba','xiaoshan',STR_TO_DATE('20071010','%Y%m%d'));

DROP TABLE IF EXISTS stu150;
CREATE TABLE stu150 (
id int PRIMARY KEY,
p_id int,
name varchar(50),
alias char(50),
birth DATE,
weight double,
c_id int,
CONSTRAINT check1 CHECK (weight BETWEEN 10 AND 100)
)
PARTITION BY RANGE(id) (
PARTITION p1 VALUES LESS THAN (1004),
PARTITION p2 VALUES LESS THAN (MAXVALUE)
);

INSERT INTO stu150 VALUES(1001, NULL, 'zhangsan', 'zs', '1991-09-25', 47.5, 101);
INSERT INTO stu150 VALUES(1002, 1001, 'lisi', 'ls', '1992-09-25', 57.5, 101);
INSERT INTO stu150 VALUES(1003, 1001, 'wangwu', 'ww', '1993-09-25', 67.5, 102);
INSERT INTO stu150 VALUES(1004, 1002, 'zhaoliu', 'zl', '1994-09-25', 77.5, 102);
INSERT INTO stu150 VALUES(1005, 1002, 'zhangsi', 'zs', '1995-09-25', 87.5, 103);
INSERT INTO stu150 VALUES(1006, 1002, 'liwu', 'lw', '1996-09-25', 37.5, 103);
INSERT INTO stu150 VALUES(1007, 1003, 'wangliu', 'wl', '1997-09-25', 97.5, 103);

DROP TABLE IF EXISTS class233;
CREATE TABLE class233 (id int PRIMARY KEY, name char(50) DEFAULT 'yinyue', location varchar(50), create_time timestamp(5))
PARTITION BY HASH(id) PARTITIONS 2;
CREATE INDEX index_21 ON class233(create_time);
INSERT INTO class233 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class233 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class233 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class233 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class233 VALUES(106,'baba','xiaoshan',STR_TO_DATE('10-10-2007','%d-%m-%Y'));

DROP TABLE IF EXISTS stu233;
CREATE TABLE stu233 (
id int,
p_id int,
name varchar(50),
alias char(50),
birth DATE,
weight double,
c_id int,
CONSTRAINT check2 CHECK (weight BETWEEN 10 AND 100)
);
CREATE INDEX index_22 ON stu233(birth);
INSERT INTO stu233 VALUES(1001,NULL,'zhangsan','zs',STR_TO_DATE('25-09-1991 12:01:33','%d-%m-%Y %H:%i:%s'),47.5,101);
INSERT INTO stu233 VALUES(1002,1001,'lisi','ls',STR_TO_DATE('25-09-1992 12:01:33','%d-%m-%Y %H:%i:%s'),57.5,101);
INSERT INTO stu233 VALUES(1003,1001,'wangwu','ww',STR_TO_DATE('25-09-1993 12:01:33','%d-%m-%Y %H:%i:%s'),67.5,102);
INSERT INTO stu233 VALUES(1004,1002,'zhaoliu','zl',STR_TO_DATE('25-09-1994 12:01:33','%d-%m-%Y %H:%i:%s'),77.5,102);
INSERT INTO stu233 VALUES(1005,1002,'zhangsi','zs',STR_TO_DATE('25-09-1995 12:01:33','%d-%m-%Y %H:%i:%s'),87.5,103);
INSERT INTO stu233 VALUES(1006,1002,'liwu','lw',STR_TO_DATE('25-09-1996 12:01:33','%d-%m-%Y %H:%i:%s'),37.5,103);
INSERT INTO stu233 VALUES(1007,1003,'wangliu','wl',STR_TO_DATE('25-09-1997 12:01:33','%d-%m-%Y %H:%i:%s'),97.5,103);

DROP TABLE IF EXISTS class333;
CREATE TABLE class333 (
id int,
name char(50) DEFAULT 'yinyue',
location varchar(50),
create_time timestamp(5)
);

INSERT INTO class333 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class333 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class333 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class333 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class333 VALUES(105,'qiqi','yuhang',STR_TO_DATE('10-10-2005','%d-%m-%Y'));

DROP TABLE IF EXISTS stu333;
CREATE TABLE stu333 (
id int,
p_id int,
name varchar(50) UNIQUE NOT NULL,
alias char(50),
birth DATE,
weight double,
c_id int,
CONSTRAINT check3 CHECK (weight BETWEEN 10 AND 100)
);

INSERT INTO stu333 VALUES(1001,NULL,'zhangsan','zs',STR_TO_DATE('25-09-1991 12:01:33','%d-%m-%Y %H:%i:%s'),47.5,101);
INSERT INTO stu333 VALUES(1002,1001,'lisi','ls',STR_TO_DATE('25-09-1992 12:01:33','%d-%m-%Y %H:%i:%s'),57.5,101);
INSERT INTO stu333 VALUES(1003,1001,'wangwu','ww',STR_TO_DATE('25-09-1993 12:01:33','%d-%m-%Y %H:%i:%s'),67.5,102);
INSERT INTO stu333 VALUES(1004,1002,'zhaoliu','zl',STR_TO_DATE('25-09-1994 12:01:33','%d-%m-%Y %H:%i:%s'),77.5,102);
INSERT INTO stu333 VALUES(1005,1002,'zhangsi','zs',STR_TO_DATE('25-09-1995 12:01:33','%d-%m-%Y %H:%i:%s'),87.5,103);
INSERT INTO stu333 VALUES(1006,1002,'liwu','lw',STR_TO_DATE('25-09-1996 12:01:33','%d-%m-%Y %H:%i:%s'),37.5,103);
INSERT INTO stu333 VALUES(1007,1003,'wangliu','wl',STR_TO_DATE('25-09-1997 12:01:33','%d-%m-%Y %H:%i:%s'),97.5,103);

DROP TABLE IF EXISTS gbktable;
CREATE TABLE gbktable (
c0 INT,
c1 VARCHAR(200),
c2 CHAR(200) CHARACTER SET utf8,
c3 BLOB,
c4 TEXT CHARACTER SET utf8,
c5 DATE,
c6 TIMESTAMP,
c7 CHAR(200) CHARACTER SET utf8,
c8 VARBINARY(200),
c9 DECIMAL(10,3)
);

INSERT INTO gbktable VALUES (1,'中文测试001','中文测试001','aaabbb','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'6262',100.2);
INSERT INTO gbktable VALUES (2,'中文测试002','中文测试002','aaabbb','中文测试002','2006-06-04','2006-06-04 00:00:00','c',X'6565',100);
INSERT INTO gbktable VALUES (3,'中文测试003','中文测试001','aaabbb','中文测试003','2006-06-04','2006-06-04 00:00:00','d',X'6666',100.3);
INSERT INTO gbktable VALUES (4,'aaabbb','中文测试001','aaabbb','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'6363',200);
INSERT INTO gbktable VALUES (5,'a','中文测试001','aaabbb','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'6262',100.22);
INSERT INTO gbktable VALUES (6,'bb','中文测试001','aaabbb','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'6262',3000);
INSERT INTO gbktable VALUES (7,'20060604','20060604','aaabbb','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'6262',2000.33);
INSERT INTO gbktable VALUES (8,'2006-06-04','aaabbb','aaabbb','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'6262',1000);
INSERT INTO gbktable VALUES (9,'中文测试009','aaabbb','200','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'3230',10000);
INSERT INTO gbktable VALUES (10,'中文测试010','aaabbb','300','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'3330',999999);
INSERT INTO gbktable VALUES (11,'中文测试011','aaabbb','300','中文测试1','2006-06-04','2006-06-04 00:00:00','a',X'3330',999999);


DROP TABLE IF EXISTS t_deduce_length;
CREATE TABLE t_deduce_length (
c_int INT,
c_number DECIMAL,
c_number_l DECIMAL(10,3),
c_float FLOAT(5),
c_binary_float FLOAT,
c_binary_double DOUBLE,
c_blob VARCHAR(200),
c_date DATE,
c_timestamp TIMESTAMP,
c_raw BINARY(200),
c_char CHAR(200),
c_char_c CHAR(200),
c_varchar2 VARCHAR(200),
c_varchar2_c VARCHAR(200),
c_varchar2_l VARCHAR(2000),
c_nchar CHAR(200),
c_nvarchar VARCHAR(200),
c_nvarchar_l VARCHAR(2000),
c_clob VARCHAR(200)
);

INSERT INTO t_deduce_length VALUES (1,1,1,1.01,1.0,1.0,'a','2001-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试001','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (2,2,2,2.02,2.0,2.0,'a','2002-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试002','a','中文测试001','中文测试001','a','bb','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (3,3,3,3.03,3.0,3.0,'a','2003-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试003','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (4,4,4,4.04,4.0,4.0,'a','2004-06-04','2006-06-04 00:00:00',UNHEX('61'),'cc','中文测试004','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (5,5,5,5.05,5.0,5.0,'a','2005-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试005','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (6,6,6,6.06,6.0,6.0,'a','2006-06-04','2006-06-04 00:00:00',UNHEX('61'),'CC','中文测试006','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (7,7,7,7.07,7.0,7.0,'a','2007-06-04','2006-06-04 00:00:00',UNHEX('43'),'aa','中文测试007','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (8,8,8,8.08,8.0,8.0,'a','2008-06-04','2006-06-04 00:00:00',UNHEX('61'),'aaabbb','中文测试008','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length VALUES (9,9,9,9.09,9.0,9.0,'a','2009-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试009','a','中文测试001','中文测试001','a','aaabbb','中文测试001','中文测试001');

ALTER TABLE t_deduce_length ADD INDEX idx_1 (c_number) LOCAL;
ALTER TABLE t_deduce_length ADD UNIQUE INDEX idx_2 (c_number_l) ;


DROP TABLE IF EXISTS t_deduce_length2;
CREATE TABLE t_deduce_length2 (
c_int2 INT,
c_number2 DECIMAL,
c_number_l2 DECIMAL(10,3),
c_float2 FLOAT(5),
c_binary_float2 FLOAT,
c_binary_double2 DOUBLE,
c_blob2 VARCHAR(200),
c_date2 DATE,
c_timestamp2 TIMESTAMP,
c_raw2 BINARY(200),
c_char2 CHAR(200),
c_char_c2 CHAR(200),
c_varchar22 VARCHAR(200),
c_varchar2_c2 VARCHAR(200),
c_varchar2_l2 VARCHAR(2000),
c_nchar2 CHAR(200),
c_nvarchar2 VARCHAR(200),
c_nvarchar_l2 VARCHAR(2000),
c_clob2 VARCHAR(200)
);

INSERT INTO t_deduce_length2 VALUES (1,1,1,1.01,1.0,1.0,'a','2001-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试001','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (2,2,2,2.02,2.0,2.0,'a','2002-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试002','a','中文测试001','中文测试001','a','bb','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (3,3,3,3.03,3.0,3.0,'a','2003-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试003','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (4,4,4,4.04,4.0,4.0,'a','2004-06-04','2006-06-04 00:00:00',UNHEX('61'),'cc','中文测试004','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (5,5,5,5.05,5.0,5.0,'a','2005-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试005','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (6,6,6,6.06,6.0,6.0,'a','2006-06-04','2006-06-04 00:00:00',UNHEX('61'),'CC','中文测试006','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (7,7,7,7.07,7.0,7.0,'a','2007-06-04','2006-06-04 00:00:00',UNHEX('43'),'aa','中文测试007','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (8,8,8,8.08,8.0,8.0,'a','2008-06-04','2006-06-04 00:00:00',UNHEX('61'),'aaabbb','中文测试008','a','中文测试001','中文测试001','a','aa','中文测试001','中文测试001');
INSERT INTO t_deduce_length2 VALUES (9,9,9,9.09,9.0,9.0,'a','2009-06-04','2006-06-04 00:00:00',UNHEX('61'),'aa','中文测试009','a','中文测试001','中文测试001','a','aaabbb','中文测试001','中文测试001');

DROP TABLE IF EXISTS bmsql_oorder;
CREATE TABLE bmsql_oorder (
o_w_id INT NOT NULL,
o_d_id INT NOT NULL,
o_id INT NOT NULL,
o_c_id INT DEFAULT NULL,
o_carrier_id INT DEFAULT NULL,
o_ol_cnt DECIMAL(2,0) DEFAULT NULL,
o_all_local DECIMAL(1,0) DEFAULT NULL,
o_entry_d TIMESTAMP DEFAULT NULL
) PARTITION BY HASH(o_w_id) PARTITIONS 2;

INSERT INTO bmsql_oorder(o_w_id,o_d_id,o_id,o_c_id,o_carrier_id) VALUES (36,7,1,169,1);


DROP TABLE IF EXISTS employees;
CREATE TABLE employees(
EMPNO INT(4) PRIMARY KEY,
ENAME VARCHAR(100),
JOB VARCHAR(9),
MGR INT(4),
HIREDATE DATE,
SAL DECIMAL(7,2),
COMM DECIMAL(7,2),
DEPTNO INT(2)
);
INSERT INTO employees VALUES
(7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO employees VALUES
(7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO employees VALUES
(7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO employees VALUES
(7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);

drop table if exists test_table;
CREATE TABLE `test_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tinyint_col` tinyint(4) NOT NULL DEFAULT '0',
  `smallint_col` smallint(6) NOT NULL DEFAULT '0',
  `mediumint_col` mediumint(9) NOT NULL DEFAULT '0',
  `int_col` int(11) NOT NULL DEFAULT '0',
  `bigint_col` bigint(20) NOT NULL DEFAULT '0',
  `float_col` float NOT NULL DEFAULT '0',
  `double_col` double NOT NULL DEFAULT '0',
  `decimal_col` decimal(10,2) NOT NULL DEFAULT '0.00',
  `date_col` date NOT NULL DEFAULT '0000-00-00',
  `time_col` time NOT NULL DEFAULT '00:00:00',
  `year_col` year(4) NOT NULL DEFAULT '0000',
  `datetime_col` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `char_col` char(10) CHARACTER SET gbk COLLATE gbk_chinese_ci NOT NULL DEFAULT '',
  `varchar_col` varchar(255) CHARACTER SET gbk COLLATE gbk_chinese_ci NOT NULL DEFAULT '',
  `tinyblob_col` tinyblob,
  `tinytext_col` tinytext CHARACTER SET gbk COLLATE gbk_chinese_ci,
  `blob_col` blob,
  `text_col` text CHARACTER SET gbk COLLATE gbk_chinese_ci,
  `mediumtext_col` mediumtext CHARACTER SET gbk COLLATE gbk_chinese_ci,
  `longblob_col` longblob,
  `longtext_col` longtext CHARACTER SET gbk COLLATE gbk_chinese_ci,
  `enum_col` enum('A','B','C') CHARACTER SET gbk COLLATE gbk_chinese_ci NOT NULL DEFAULT 'A',
  `set_col` set('red','green','blue') CHARACTER SET gbk COLLATE gbk_chinese_ci NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
);
INSERT INTO `test_table` VALUES (1, 1, 10, 100, 1000, 10000, 3.14, 3.1415926, 1234.56, '2021-01-01', '12:00:00', '2021', '2021-01-01 12:00:00', '2021-01-01 12:00:00', 'Hello', 'World', 'TinyBlob', 'TinyText', 'Blob', 'Text', 'MediumText', 'LongBlob', 'LongText', 'A', 'red,green');





DROP TABLE  IF EXISTS test100 ;
CREATE TABLE test100(f1 int, f2 varchar(10), f3 varchar(10));
INSERT INTO test100 VALUES(1001,'a','12345');
INSERT INTO test100 VALUES(1002,'b','67890');
INSERT INTO test100 VALUES(1003,'c','23456');
commit;

DROP TABLE  IF EXISTS test101 ;
CREATE TABLE test101(f1 NUMBER(5,0) PRIMARY KEY, f2 varchar(10), f3 varchar(10));
INSERT INTO test101 VALUES(1001,'a','12345');
INSERT INTO test101 VALUES(1002,'b','67890');
INSERT INTO test101 VALUES(1003,'c','23456');
INSERT INTO test101 VALUES(1006,'d','23456');

DROP TABLE  IF EXISTS test102 ;
CREATE TABLE test102(f1 NUMBER(5,0) PRIMARY KEY, f2 varchar(10), f3 varchar(10)) ;
INSERT INTO test102 VALUES(1001,'a','12345');
INSERT INTO test102 VALUES(1003,'b','67890');
INSERT INTO test102 VALUES(1004,'c','23456');

DROP TABLE  IF EXISTS test103 ;
CREATE TABLE test103(f1 NUMBER(5,0), f2 varchar(10), f3 varchar(10),PRIMARY KEY(f1,f2)) ;
INSERT INTO test103 VALUES(1001,'a','12345');
INSERT INTO test103 VALUES(1003,'b','67890');
INSERT INTO test103 VALUES(1004,'c','23456');
INSERT INTO test103 VALUES(1006,'d','23456');
INSERT INTO test103 VALUES(1007,'e','23456');
commit;

DROP TABLE  IF EXISTS test104 ;
CREATE TABLE test104(f1 int PRIMARY KEY, f2 varchar(10), f3 varchar(10));
INSERT INTO test104 VALUES(1001,'a','12345');
INSERT INTO test104 VALUES(1003,'b','67890');
INSERT INTO test104 VALUES(1004,'c','23456');
INSERT INTO test104 VALUES(1006,'d','23456');
INSERT INTO test104 VALUES(1007,'e','23456');
commit;

DROP TABLE  IF EXISTS test106 ;
DROP TABLE  IF EXISTS test107 ;
DROP TABLE  IF EXISTS test108 ;
CREATE TABLE test106(f1 NUMBER PRIMARY KEY);
INSERT INTO test106 VALUES(1e1);
INSERT INTO test106 VALUES(1e-1);
INSERT INTO test106 VALUES(5);
CREATE TABLE test107(f1 NUMBER(15));
INSERT INTO test107 VALUES(1e1);
INSERT INTO test107 VALUES(1e-1);
INSERT INTO test107 VALUES(5);
CREATE TABLE test108(f1 NUMBER(15,12));
INSERT INTO test108 VALUES(100);
INSERT INTO test108 VALUES(1e-1);
INSERT INTO test108 VALUES(5.5);

DROP TABLE  IF EXISTS test109 ;
DROP TABLE  IF EXISTS test110 ;
CREATE TABLE test109(f1 float);
INSERT INTO test109 VALUES(1e1);
INSERT INTO test109 VALUES(1e-1);
INSERT INTO test109 VALUES(5);
CREATE TABLE test110(f1 float(15) PRIMARY KEY);
INSERT INTO test110 VALUES(1e1);
INSERT INTO test110 VALUES(1e-1);
INSERT INTO test110 VALUES(5);

DROP TABLE  IF EXISTS test111 ;
CREATE TABLE test111(f1 float);
INSERT INTO test111 VALUES(1e1);
INSERT INTO test111 VALUES(1e-1);
INSERT INTO test111 VALUES(5);

DROP TABLE  IF EXISTS test112 ;
CREATE TABLE test112(f1 double PRIMARY KEY);
INSERT INTO test112 VALUES(1e1);
INSERT INTO test112 VALUES(1e-1);
INSERT INTO test112 VALUES(5);


DROP TABLE  IF EXISTS test113 ;
DROP TABLE  IF EXISTS test114 ;
DROP TABLE  IF EXISTS test115 ;
CREATE TABLE test113(f1 varchar(5));
INSERT INTO test113 VALUES('11');
INSERT INTO test113 VALUES('aa');
INSERT INTO test113 VALUES('33');
CREATE TABLE test114(f1 varchar(5) PRIMARY KEY);
INSERT INTO test114 VALUES('11');
INSERT INTO test114 VALUES('aa');
INSERT INTO test114 VALUES('66');
CREATE TABLE test115(f1 varchar(5) PRIMARY KEY);
INSERT INTO test115 VALUES('111');
INSERT INTO test115 VALUES('aaa');
INSERT INTO test115 VALUES('666');

DROP TABLE  IF EXISTS test116 ;
DROP TABLE  IF EXISTS test117 ;
CREATE TABLE test116(f1 varchar(20) PRIMARY KEY);
INSERT INTO test116 VALUES('1');
INSERT INTO test116 VALUES('a');
INSERT INTO test116 VALUES('3');
CREATE TABLE test117(f1 varchar(20) PRIMARY KEY);
INSERT INTO test117 VALUES('11');
INSERT INTO test117 VALUES('aa');
INSERT INTO test117 VALUES('66');

DROP TABLE  IF EXISTS test118 ;
CREATE TABLE test118(f1 varchar(5) PRIMARY KEY) ;
INSERT INTO test118 VALUES('11');
INSERT INTO test118 VALUES('aa');
INSERT INTO test118 VALUES('66');

DROP TABLE   IF EXISTS  test119;
CREATE TABLE test119(f1 INT, f2 DATETIME);
INSERT INTO test119 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test119 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test119 VALUES(105, '2020-09-25 12:01:00');

DROP TABLE   IF EXISTS  test120;
CREATE TABLE test120(f1 INT, f2 DATETIME);
INSERT INTO test120 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test120 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test120 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test121;
CREATE TABLE test121(f1 INT, f2 DATETIME);
INSERT INTO test121 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test121 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test121 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test122;
CREATE TABLE test122(f1 INT, f2 DATETIME);
INSERT INTO test122 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test122 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test122 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test123;
CREATE TABLE test123(f1 INT, f2 DATETIME);
INSERT INTO test123 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test123 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test123 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test124;
CREATE TABLE test124(f1 INT, f2 DATETIME);
INSERT INTO test124 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test124 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test124 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test125;
CREATE TABLE test125(f1 INT, f2 DATETIME);
INSERT INTO test125 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test125 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test125 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test126;
CREATE TABLE test126(f1 INT, f2 DATETIME);
INSERT INTO test126 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test126 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test126 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test127;
CREATE TABLE test127(f1 INT, f2 DATETIME);
INSERT INTO test127 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test127 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test127 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test128;
CREATE TABLE test128(f1 INT, f2 DATETIME);
INSERT INTO test128 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test128 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test128 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test129;
CREATE TABLE test129(f1 INT, f2 DATETIME);
INSERT INTO test129 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test129 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test129 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test130;
CREATE TABLE test130(f1 INT, f2 DATETIME);
INSERT INTO test130 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test130 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test130 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test131;
CREATE TABLE test131(f1 INT, f2 DATETIME);
INSERT INTO test131 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test131 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test131 VALUES(105, '2020-09-25 12:01:00');

DROP TABLE  IF EXISTS test132 ;
CREATE TABLE test132(f1 INT PRIMARY KEY, f2 LONGTEXT);
INSERT INTO test132 VALUES(123, 'adminadminadmin');
INSERT INTO test132 VALUES(456, 'testtesttest');

DROP TABLE   IF EXISTS  test133;
CREATE TABLE test133(f1 INT, f2 BINARY(20));
INSERT INTO test133 VALUES(101, UNHEX('72617774797065'));
INSERT INTO test133 VALUES(102, UNHEX('31323334353637'));

DROP TABLE   IF EXISTS  test134;
CREATE TABLE test134(f1 INT PRIMARY KEY, f2 LONGBLOB);
INSERT INTO test134 VALUES(101, UNHEX('72617774797065'));
INSERT INTO test134 VALUES(103, UNHEX('31323334353637'));

DROP TABLE   IF EXISTS  test135;
DROP TABLE   IF EXISTS  test136;
DROP TABLE   IF EXISTS  test137;
CREATE TABLE test135(f1 INT, f2 VARBINARY(255), PRIMARY KEY(f2));
INSERT INTO test135 VALUES(1, UNHEX('41414B41646F414145414144466C4141414144'));

CREATE TABLE test136(f1 INT, f2 BINARY(255));
INSERT INTO test136 VALUES(1, UNHEX('41414B41646F414145414144466C4141414142'));

CREATE TABLE test137(f1 INT, f2 BINARY(100), PRIMARY KEY(f2));
INSERT INTO test137 VALUES(1, UNHEX('41414B41646F414145414144466C4141414163'));

DROP TABLE  IF EXISTS test138 ;
CREATE TABLE test138(f1 int PRIMARY KEY,f2 TEXT);
INSERT INTO test138 VALUES(1,'1234ddddddddddddddddddddddddddd');
INSERT INTO test138 VALUES(2,'fal;jg;agopra/;.''po[ragp');
INSERT INTO test138 VALUES(4,'1234ddddddddddddddddddddddddddd');
INSERT INTO test138 VALUES(6,'fal;jg;agopra/;.''po[ragp');

DROP TABLE  IF EXISTS test139 ;
CREATE TABLE test139(f1 int,f2 blob);
INSERT INTO test139 VALUES(1,'1234ddddddddddddddddddddddddddd');
INSERT INTO test139 VALUES(2,'41895fdabccccccccffff87897686aa');
INSERT INTO test139 VALUES(4,'1234ddddddddddddddddddddddddddd');
INSERT INTO test139 VALUES(6,'41895fdabccccccccffff87897686aa');

DROP TABLE  IF EXISTS test140 ;
CREATE TABLE test140(f1 int PRIMARY KEY,f2 TEXT);
INSERT INTO test140 VALUES(1,'AAJDTjAAEAAITskAAA');
INSERT INTO test140 VALUES(2,'AAJDTnAAEAAITtcAAB');

DROP TABLE   IF EXISTS  test141;
CREATE TABLE test141(f1 INT, f2 FLOAT, f3 VARCHAR(20), f4 VARCHAR(20), f5 DATETIME(6));
INSERT INTO test141 VALUES(36478, 347.64273, '123456', 'fhaiu', '2005-09-25 12:01:33.333333');
INSERT INTO test141 VALUES(45678, 258.68734, '456789', 'fndjk', '2006-09-25 12:01:44.555555');
INSERT INTO test141 VALUES(12345, 483.35433, '456789', 'bfdsh', '2009-09-25 12:01:55.555777');
INSERT INTO test141 VALUES(45678, 354.58766, '123456', 'gregg', '2008-09-25 12:01:11.333344');

DROP TABLE  IF EXISTS test142;
DROP TABLE  IF EXISTS test143 ;
CREATE TABLE test142(f1 int PRIMARY KEY);
INSERT INTO test142 VALUES(1001);
INSERT INTO test142 VALUES(1002);
INSERT INTO test142 VALUES(1003);
CREATE TABLE test143(
    f1 DECIMAL(10,0) PRIMARY KEY,
    f2 FLOAT UNIQUE,
    f3 VARCHAR(20) NULL,
    f4 VARCHAR(20) NOT NULL,
    f5 DATETIME DEFAULT '2005-09-25 12:01:33',
    f6 INT
);
INSERT INTO test143 VALUES(36478.46372, 347.64273, '123456', 'fhaiu', DEFAULT, 1001);
INSERT INTO test143 VALUES(45678.12345, 258.68734, '456789', 'fndjk', '2006-09-25 12:01:44', 1002);
INSERT INTO test143 VALUES(12345.67890, 483.35433, '456789', 'bfdsh', '2009-09-25 12:01:55', 1002);
INSERT INTO test143 VALUES(45678.64589, 354.58766, DEFAULT, 'gregg', '2008-09-25 12:01:11', 1003);

DROP TABLE  IF EXISTS test144 ;
DROP TABLE  IF EXISTS test145 ;
DROP TABLE  IF EXISTS test146 ;
DROP TABLE  IF EXISTS test147 ;
CREATE TABLE test144(f1 int PRIMARY KEY);
INSERT INTO test144 VALUES(1001);
INSERT INTO test144 VALUES(1002);
INSERT INTO test144 VALUES(1003);

CREATE TABLE test145(
    f1 INT,
    f2 FLOAT,
    f3 VARCHAR(20),
    f4 VARCHAR(20),
    f5 DATETIME(6)
);
INSERT INTO test145 VALUES(36, 347.64273, '123456', 'fhaiu', '2005-09-25 12:01:33.333333');
INSERT INTO test145 VALUES(45, 258.68734, '456789', 'fndjk', '2006-09-25 12:01:44.555555');
INSERT INTO test145 VALUES(12, 483.35433, '456789', 'bfdsh', '2009-09-25 12:01:55.555777');
INSERT INTO test145 VALUES(45, 354.58766, '123456', 'gregg', '2008-09-25 12:01:11.333344');

CREATE TABLE test146(f1 NUMBER(10) PRIMARY KEY, f2 float, f3 varchar(20), f4 varchar(20), f5 DATETIME(6));
INSERT INTO test146 VALUES(36478.46372, 347.64273, '123456','fhaiu', ('2005-09-25 12:01:33.33333333'));
INSERT INTO test146 VALUES(45678.12345, 258.68734, '456789','fndjk', ('2006-09-25 12:01:44.55555555'));
INSERT INTO test146 VALUES(12345.67890, 483.35433, '456789','bfdsh', ('2009-09-25 12:01:55.55577777'));
INSERT INTO test146 VALUES(45678.64589, 354.58766, '123456','gregg', ('2008-09-25 12:01:11.33334444'));

CREATE TABLE test147(
    f1 DECIMAL(10,0) PRIMARY KEY,
    f2 FLOAT UNIQUE,
    f3 VARCHAR(20) NULL,
    f4 VARCHAR(20) NOT NULL,
    f5 DATETIME DEFAULT '2005-09-25 12:01:33',
    f6 INT
);
INSERT INTO test147 VALUES(36478.46372, 347.64273, '123456', 'fhaiu', DEFAULT, 1001);
INSERT INTO test147 VALUES(45678.12345, 258.68734, '456789', 'fndjk', '2006-09-25 12:01:44', 1002);
INSERT INTO test147 VALUES(12345.67890, 483.35433, '456789', 'bfdsh', '2009-09-25 12:01:55', 1002);
INSERT INTO test147 VALUES(45678.64589, 354.58766, DEFAULT, 'gregg', '2008-09-25 12:01:11', 1003);

DROP TABLE  IF EXISTS test148 ;
CREATE TABLE test148(
    f1 DECIMAL(10,5),
    f2 FLOAT,
    f3 VARCHAR(20),
    f4 VARCHAR(20),
    f5 DATETIME(6)
);
CREATE INDEX index_111 ON test148(f2);
INSERT INTO test148 VALUES(36478.46372, 347.64273, '123456', 'fhaiu', '2005-09-25 12:01:33.333333');
INSERT INTO test148 VALUES(45678.12345, 258.68734, '456789', 'fndjk', '2006-09-25 12:01:44.555555');
INSERT INTO test148 VALUES(12345.67890, 483.35433, '456789', 'bfdsh', '2009-09-25 12:01:55.555777');
INSERT INTO test148 VALUES(45678.64589, 354.58766, '123456', 'gregg', '2008-09-25 12:01:11.333344');
DROP TABLE  IF EXISTS test149 ;
CREATE TABLE test149(
    f1 DECIMAL,
    f2 FLOAT,
    f3 VARCHAR(20),
    f4 VARCHAR(20),
    f5 DATETIME(6),
    f6 TEXT,
    f7 BLOB
);
INSERT INTO test149 VALUES(36478.46372, 347.64273, '123456', 'fhaiu', '2005-09-25 12:01:33.333333', '1234dddddddddd', 0x313233346464646464646464646464646464646464646464);
INSERT INTO test149 VALUES(45678.12345, 258.68734, '456789', 'fndjk', '2006-09-25 12:01:44.555555', 'fal;jg;agopra/;.''po[ragp', 0x66616C3B6A673B61676F7072612F3B272720706F5B72616770);
INSERT INTO test149 VALUES(12345.67890, 483.35433, '456789', 'bfdsh', '2009-09-25 12:01:55.555777', '1234dddddddddd', 0x313233346464646464646464646464646464646464646464);
INSERT INTO test149 VALUES(45678.64589, 354.58766, '123456', 'gregg', '2008-09-25 12:01:11.333344', 'fal;jg;agopra/;.''po[ragp', 0x34313839356664616263636666383738393638366161);


DROP TABLE  IF EXISTS test151 ;
DROP TABLE  IF EXISTS test152 ;
DROP TABLE  IF EXISTS test153 ;
DROP TABLE  IF EXISTS test154 ;

create table test151 (a int);
create table test152 (a int, b int);
create table test153 (a int);
create table test154 (a int not null, b int not null);
insert into test151 values (2);
insert into test152 values (1,7);
insert into test152 values (2,7);
insert into test153 values (6);
insert into test153 values (7);
insert into test153 values (3);
insert into test154 values (4,8);
insert into test154 values (3,8);
insert into test154 values (5,9);

DROP TABLE  IF EXISTS test161 ;
DROP TABLE  IF EXISTS test162 ;
DROP TABLE  IF EXISTS test163 ;
DROP TABLE  IF EXISTS test164 ;
DROP TABLE  IF EXISTS test165 ;
DROP TABLE  IF EXISTS test166 ;
DROP TABLE  IF EXISTS test167 ;
DROP TABLE  IF EXISTS test168 ;
DROP TABLE  IF EXISTS test169 ;
DROP TABLE  IF EXISTS test170 ;
DROP TABLE  IF EXISTS test171 ;
DROP TABLE  IF EXISTS test172 ;
DROP TABLE  IF EXISTS test173 ;
DROP TABLE  IF EXISTS test174 ;
DROP TABLE  IF EXISTS test175 ;
DROP TABLE  IF EXISTS test176 ;
DROP TABLE  IF EXISTS test177 ;

create table test161 (a int, b int);
create table test162 (a int, b int);
create table test163 (a int, b int);
insert into test161 values (0,100);
insert into test161 values (1,2);
insert into test161 values (1,3);
insert into test161 values (2,2);
insert into test161 values (2,7);
insert into test161 values (2,-1);
insert into test161 values (3,10);
insert into test162 values (0,0);
insert into test162 values (1,1);
insert into test162 values (2,1);
insert into test162 values (3,1);
insert into test162 values (4,1);
insert into test163 values (3,3);
insert into test163 values (2,2);
insert into test163 values (1,1);

create table test164 (s1 int);
create table test165 (s1 int);
insert into test164 values (1);
insert into test165 values (1);

CREATE TABLE test166 (id int default NULL,name varchar(20) default NULL);
INSERT INTO test166 VALUES (1,'Tim');
INSERT INTO test166 VALUES (2,'Rebecca');
INSERT INTO test166 VALUES (3,NULL);
CREATE TABLE test167 (id int default NULL, pet varchar(20) default NULL);
INSERT INTO test167 VALUES (1,'Fido');
INSERT INTO test167 VALUES (2,'Spot');
INSERT INTO test167 VALUES (3,'Felix');

create table test168 (i int, j number(10));
insert into test168 values (1, 2);
insert into test168 values (2, 2);
insert into test168 values (3, 2);

CREATE TABLE test169 (a int, b INT, d INT, c varchar(10) NOT NULL, PRIMARY KEY (a, b));
INSERT INTO test169 VALUES (1,1,0,'a');
INSERT INTO test169 VALUES (1,2,0,'b');
INSERT INTO test169 VALUES (1,3,0,'c');
INSERT INTO test169 VALUES (1,4,0,'d');
INSERT INTO test169 VALUES (1,5,0,'e');
INSERT INTO test169 VALUES (2,1,0,'f');
INSERT INTO test169 VALUES (2,2,0,'g');
INSERT INTO test169 VALUES (2,3,0,'h');
INSERT INTO test169 VALUES (3,4,0,'i');
INSERT INTO test169 VALUES (3,3,0,'j');
INSERT INTO test169 VALUES (3,2,0,'k');
INSERT INTO test169 VALUES (3,1,0,'l');
INSERT INTO test169 VALUES (1,9,0,'m');
INSERT INTO test169 VALUES (1,0,10,'n');
INSERT INTO test169 VALUES (2,0,5,'o');
INSERT INTO test169 VALUES (3,0,7,'p');

CREATE TABLE test170 (id int NOT NULL, st varchar(2));
INSERT INTO test170 VALUES (3,'FL');
INSERT INTO test170 VALUES (2,'GA');
INSERT INTO test170 VALUES (4,'FL');
INSERT INTO test170 VALUES (1,'GA');
INSERT INTO test170 VALUES (5,'NY');
INSERT INTO test170 VALUES (7,'FL');
INSERT INTO test170 VALUES (6,'NY');

CREATE TABLE test171 (id int NOT NULL);
INSERT INTO test171 VALUES (7);
INSERT INTO test171 VALUES (5);
INSERT INTO test171 VALUES (1);
INSERT INTO test171 VALUES (3);

create table test172 (a int, b int);
insert into test172 values (0,0);
insert into test172 values (2,2);
insert into test172 values (3,3);

create table test173 (a int, b int);
insert into test173 values (1,1);
insert into test173 values (3,3);
insert into test173 values (NULL,4);

CREATE TABLE test174 (
  pk INT,
  col_varchar varchar(1),
  PRIMARY KEY (pk)
)
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(1,'w')
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(2,'m')
;

CREATE TABLE test175 (c1 varchar(1), c2 INT, c3 INT);
INSERT/*insert trace*/ INTO test175 VALUES ('',6,8);
INSERT/*insert trace*/ INTO test175 VALUES ('',75,NULL);

CREATE TABLE test176 (c1 varchar(1));
INSERT/*insert trace*/ INTO test176 VALUES ('S');
INSERT/*insert trace*/ INTO test176 VALUES ('S');
INSERT/*insert trace*/ INTO test176 VALUES ('S');

CREATE TABLE test177 (i1 INT);
INSERT/*insert trace*/ INTO test177 VALUES (2);
INSERT/*insert trace*/ INTO test177 VALUES (4);
INSERT/*insert trace*/ INTO test177 VALUES (6);
INSERT/*insert trace*/ INTO test177 VALUES (8);


DROP TABLE  IF EXISTS test181 ;
DROP TABLE  IF EXISTS test182 ;
DROP TABLE  IF EXISTS test183 ;
DROP TABLE  IF EXISTS test184 ;
DROP TABLE  IF EXISTS test185 ;

CREATE TABLE test181(
    categoryId INT NOT NULL,
    courseId INT NOT NULL,
    startDate DATE NOT NULL,
    endDate DATE NOT NULL,
    createDate DATE NOT NULL,
    modifyDate DATE NOT NULL,
    attributes VARCHAR(30)
);
INSERT INTO test181 VALUES (1,41,'2004-02-09','2010-01-01','2004-02-09','2004-02-09','');
INSERT INTO test181 VALUES (1,86,'2004-08-16','2004-08-16','2004-08-16','2004-08-16','');
INSERT INTO test181 VALUES (1,87,'2004-08-16','2004-08-16','2004-08-16','2004-08-16','');
INSERT INTO test181 VALUES (2,52,'2004-03-15','2004-10-01','2004-03-15','2004-09-17','');
INSERT INTO test181 VALUES (2,53,'2004-03-16','2004-10-01','2004-03-16','2004-09-17','');
INSERT INTO test181 VALUES (2,88,'2004-08-16','2004-08-16','2004-08-16','2004-08-16','');
INSERT INTO test181 VALUES (2,89,'2004-08-16','2004-08-16','2004-08-16','2004-08-16','');
INSERT INTO test181 VALUES (3,51,'2004-02-09','2010-01-01','2004-02-09','2004-02-09','');
INSERT INTO test181 VALUES (5,12,'2004-02-18','2010-01-01','2004-02-18','2004-02-18','');


CREATE TABLE test182 (
  userId INT NOT NULL,
  courseId INT NOT NULL,
  datett DATE NOT NULL
);
INSERT INTO test182 VALUES (5141,71,'2003-11-18');
INSERT INTO test182 VALUES (5141,72,'2003-11-25');
INSERT INTO test182 VALUES (5141,52,'2004-08-06');
INSERT INTO test182 VALUES (5141,12,'2004-08-06');
INSERT INTO test182 VALUES (5141,87,'2004-10-21');
INSERT INTO test182 VALUES (5141,89,'2004-10-22');
INSERT INTO test182 VALUES (5141,41,'2004-08-06');
INSERT INTO test182 VALUES (5141,53,'2004-08-06');
INSERT INTO test182 VALUES (5141,86,'2004-10-21');
INSERT INTO test182 VALUES (5141,88,'2004-10-21');
INSERT INTO test182 VALUES (5141,51,'2004-10-26');

CREATE TABLE test183 (
  groupId INT NOT NULL,
  parentId INT NOT NULL,
  startDate DATE NOT NULL,
  endDate DATE NOT NULL,
  createDate DATE NOT NULL,
  modifyDate DATE NOT NULL,
  ordering INT
);
INSERT INTO test183 VALUES (12,9,'1000-01-01','3999-12-31','2004-01-29','2004-01-29',NULL);


CREATE TABLE test184 (
  id INT NOT NULL,
  groupTypeId INT NOT NULL,
  groupKey VARCHAR(50) NOT NULL,
  name VARCHAR(50),
  ordering INT,
  description VARCHAR(50),
  createDate DATE NOT NULL,
  modifyDate DATE NOT NULL
);
INSERT INTO test184 VALUES (9,5,'stationer','stationer',0,'Stationer','2004-01-29','2004-01-29');
INSERT INTO test184 VALUES (12,5,'group2','group2',0,'group2','2004-01-29','2004-01-29');


CREATE TABLE test185 (
  userId INT NOT NULL,
  groupId INT NOT NULL,
  createDate DATE NOT NULL,
  modifyDate DATE NOT NULL
);
INSERT INTO test185 VALUES (5141,12,'2004-08-06','2004-08-06');


DROP TABLE  IF EXISTS test551 ;
DROP TABLE  IF EXISTS test552 ;
DROP TABLE  IF EXISTS test553 ;
DROP TABLE  IF EXISTS test554 ;
DROP TABLE  IF EXISTS test555 ;
DROP TABLE  IF EXISTS test556 ;

CREATE TABLE test551(DEPTNO NUMBER(2) PRIMARY KEY,DNAME varchar(14),LOC varchar(13));
INSERT INTO test551 VALUES(1,'AccounTing','New York');
INSERT INTO test551 VALUES (2,'ReSeaRch','DallAs');
INSERT INTO test551 VALUES(3,'SaLeS','ChiCaGo');
INSERT INTO test551 VALUES(4,'OperaTions','BoSton');

CREATE TABLE test552(
EMPNO INT(4) UNIQUE,
ENAME VARCHAR(10) NOT NULL,
JOB VARCHAR(9),
MGR INT(4),
HIREDATE DATE,
SAL DECIMAL(7,2),
COMM DECIMAL(7,2),
DEPTNO INT(2)
);
INSERT INTO test552 VALUES(7369,'SMITH','CLERK',7902,('1980-12-17'),800,100,2);
INSERT INTO test552 VALUES(7499,'ALLEN','SALESMAN',7698,('1981-02-20'),1600,300,3);
INSERT INTO test552 VALUES(7521,'WARD','SALESMAN',7698,('1981-02-22'),1250,500,3);
INSERT INTO test552 VALUES(7566,'JONES','MANAGER',7839,('1981-04-02'),2975,358.69,2);
INSERT INTO test552 VALUES(7654,'MARTIN','SALESMAN',7698,('1981-09-28'),1250,1400,3);
INSERT INTO test552 VALUES(7698,'BLAKE','MANAGER',7839,('1981-05-01'),2850,NULL,3);
INSERT INTO test552 VALUES(7782,'CLARK','MANAGER',7839,('1981-06-09'),4450,744.9,1);
INSERT INTO test552 VALUES(7788,'SCOTT','ANALYST',7566,('1987-04-13'),3000,-200,2);
INSERT INTO test552 VALUES(7839,'KING','PRESIDENT',NULL,('1981-11-17'),4999,NULL,1);
INSERT INTO test552 VALUES(7844,'TURNER','SALESMAN',7698,('1981-09-08'),1500,0,3);
INSERT INTO test552 VALUES(7876,'ADAMS','CLERK',7788,('1987-05-13'),1100,-99.9,2);
INSERT INTO test552 VALUES(7900,'JAMES','CLERK',7698,('1981-12-03'),950,-1808,3);
INSERT INTO test552 VALUES(7902,'FORD','ANALYST',7566,('1981-12-03'),3000,753.9,2);
INSERT INTO test552 VALUES(7934,'MILLER','CLERK',7782,('1982-01-23'),1300,729,1);

CREATE TABLE test553(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 CHAR(1),C4 NUMBER,C5 varchar(1),C6 varchar(1),C7 NUMBER,C8 CHAR(1));
INSERT INTO test553 VALUES  (1, 5, 'e', 8, NULL, 't', NULL, NULL) ;
INSERT INTO test553 VALUES  (2, 3, 'x', 2, 'q', 'k', 0, NULL) ;
INSERT INTO test553 VALUES  (2, 4, 'o', 5, 'w', 'u', 8,'n') ;
INSERT INTO test553 VALUES  (3, 4, 'h', 2, 'y', 'z', NULL, NULL) ;
INSERT INTO test553 VALUES  (4, 6, 'j', 2, 'h', 'p', 1,NULL) ;
INSERT INTO test553 VALUES  (4, 4, 'i', 2, 'd', 'x', 0,'f') ;
INSERT INTO test553 VALUES  (3, 9, 'e', 0, 'r', 'v', 6, 'd') ;
INSERT INTO test553 VALUES  (2, 9, 'y', 7, 'h', 'q', 2,'v') ;
INSERT INTO test553 VALUES  (1, 2, 'w', 1, 'c', 'l', 7,'o') ;
INSERT INTO test553 VALUES  (1, 9, 'y', 1, NULL, 's', 5, NULL) ;
INSERT INTO test553 VALUES  (2, 6, 'h', 7, NULL, 'f', 8, NULL) ;
INSERT INTO test553 VALUES  (4, 5, 'i', 2, NULL, 'w', NULL,'x') ;

CREATE TABLE test554(DEPTNO NUMBER(2) PRIMARY KEY,DNAME varchar(14),LOC varchar(13));
INSERT INTO test554 SELECT * FROM test551 limit 2;

CREATE TABLE test555(EMPNO NUMBER(4) UNIQUE,ENAME varchar(10) NOT NULL,JOB varchar(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2));
INSERT INTO test555 SELECT * FROM(SELECT * FROM test552 ORDER BY empno) limit 5 ;

CREATE TABLE test556(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 CHAR(1),C4 NUMBER,C5 varchar(1),C6 varchar(1),C7 NUMBER,C8 CHAR(1));
INSERT INTO test556 SELECT * FROM (SELECT * FROM test553 WHERE c3 IN ('w','e','y','x','i','h') ORDER BY 1,2,3,4);


DROP TABLE   IF EXISTS  Z0CASE;
CREATE TABLE Z0CASE (
z0_test0 VARCHAR(100),
z0_test1 INT(11),
z0_test2 FLOAT
);
INSERT INTO Z0CASE VALUES ('ABCabc',0,1.1);
INSERT INTO Z0CASE VALUES ('123456',1,2.2);
INSERT INTO Z0CASE VALUES ('!@#$%^&*',2,3.333);
INSERT INTO Z0CASE VALUES ('中文字符123',3,4.4444);
INSERT INTO Z0CASE VALUES (' ',4,5.5555);
INSERT INTO Z0CASE VALUES ('',5,66.666);

DROP TABLE  IF EXISTS Z1CASE;
DROP TABLE   IF EXISTS  Z1CASE;
CREATE TABLE Z1CASE (
z1_test0 VARCHAR(100),
z1_test1 INT(11),
z1_test2 FLOAT
);
INSERT INTO Z1CASE VALUES ('ABCabc',11,00);
INSERT INTO Z1CASE VALUES ('123456',10,0.11);
INSERT INTO Z1CASE VALUES ('!@#123$%^&',9,1.11);
INSERT INTO Z1CASE VALUES ('中文字符123',8,2.22);
INSERT INTO Z1CASE VALUES (' ',7,3.33);
INSERT INTO Z1CASE VALUES ('',6,4.44);
INSERT INTO Z1CASE VALUES ('abcabc',5,5.555);
INSERT INTO Z1CASE VALUES ('11111.1111',4,6.666);
INSERT INTO Z1CASE VALUES ('!@#$%^&',3,7.777);
INSERT INTO Z1CASE VALUES ('abc中文 字符123',2,8.888);
INSERT INTO Z1CASE VALUES (' ',1,9.999);
INSERT INTO Z1CASE VALUES ('',0,111.111);


DROP TABLE  IF EXISTS grouping1;
CREATE TABLE grouping1 (
pk SMALLINT,
col_number DECIMAL,
col_int INT,
col_date DATETIME,
col_varchar2_20 VARCHAR(20),
col_decimal_20_2 DECIMAL(20,2),
PRIMARY KEY (pk)
) PARTITION BY HASH(pk) PARTITIONS 4;
INSERT INTO grouping1 VALUES (1, 868548608, 0, '2008-12-25 01:10:47', '', 0);
INSERT INTO grouping1 VALUES (2, 1387003904, 538902528, '2004-02-15 10:44:52', 'rmmjkovqnoqh', -2114322432);
INSERT INTO grouping1 VALUES (3, -1678966784, 0, '2009-04-06 17:15:12.047025', NULL, 1865613312);


DROP TABLE  IF EXISTS wf_test1;
CREATE TABLE wf_test1 (
col_int INT,
col_decimal_20_2 DECIMAL(20,2),
col_number DECIMAL,
col_varchar2_20 VARCHAR(20),
pk SMALLINT,
col_varchar2_21 VARCHAR(21),
col_date DATE,
col_timestamp DATETIME,
col_decimal_21_2 DECIMAL(21,2),
PRIMARY KEY (pk)
);
INSERT INTO wf_test1 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (-1611857920, -474021888, -566689792, NULL, 1, 'this', '2005-02-05 21:11:43', '2009-07-27 00:00:00', 1225785344);
INSERT INTO wf_test1 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (1588789248, 749535232, 351666176, '', 2, 's', '2006-06-23 06:15:53', '2007-06-26 00:00:00', 0);

DROP TABLE  IF EXISTS grouping11;
CREATE TABLE grouping11(
col_int INT,
col_decimal_20_2 DECIMAL(20,2),
col_number DECIMAL,
col_varchar2_20 VARCHAR(20),
pk SMALLINT,
col_varchar2_21 VARCHAR(21),
col_date DATE,
col_timestamp DATETIME,
col_decimal_21_2 DECIMAL(21,2),
PRIMARY KEY (pk)
) PARTITION BY HASH(pk) PARTITIONS 4;

DROP TABLE  IF EXISTS cume_group;
CREATE TABLE cume_group (
pk SMALLINT,
col_number DECIMAL,
col_int INT,
col_date DATE,
col_varchar2_20 VARCHAR(20),
col_decimal_20_2 DECIMAL(20,2),
PRIMARY KEY (pk)
) PARTITION BY HASH(pk) PARTITIONS 4;
INSERT INTO cume_group VALUES (1, 868548608, 0, '2008-12-25 01:10:47', '', 0);
INSERT INTO cume_group VALUES (2, 1387003904, 538902528, '2004-02-15 10:44:52', 'rmmjkovqnoqh', -2114322432);
INSERT INTO cume_group VALUES (3, -1678966784, 0, '2009-04-06 17:15:12.047025', NULL, 1865613312);
INSERT INTO cume_group VALUES (4, 1488257024, 782434304, '2006-08-12 16:39:33', 'i', -1414922240);
INSERT INTO cume_group VALUES (5, 1508835328, 185335808, '2002-07-12 19:18:28.007325', 'x', 2096758784);
INSERT INTO cume_group VALUES (6, -1587281920, 105971712, '2001-12-19 06:21:34.036297', 'so', 445972480);
INSERT INTO cume_group VALUES (7, 167575552, -752877568, '2006-11-22 15:47:47.010215', 'm', 1246363648);
INSERT INTO cume_group VALUES (8, -1590165504, -970457088, '2008-04-16 03:59:05', '', -429522944);
INSERT INTO cume_group VALUES (9, -513409024, -803995648, '2005-12-22 03:52:43.035922', 'j', 0);
INSERT INTO cume_group VALUES (10, -1531445248, 0, '2001-08-15 21:22:47', '', 0);
INSERT INTO cume_group VALUES (11, -1583284224, 680329216, '2009-12-23 19:40:09.048862', 'z', 0);
INSERT INTO cume_group VALUES (12, -582352896, -1330446336, '2005-09-12 16:35:00.027314', '', -391118848);
INSERT INTO cume_group VALUES (13, 1471021056, 0, '2008-04-11 02:33:14', NULL, 398786560);
INSERT INTO cume_group VALUES (14, -160432128, 622526464, '2008-10-19', NULL, 303431680);
INSERT INTO cume_group VALUES (15, 0, 942473216, NULL, '', 1044905984);


DROP TABLE  IF EXISTS grouping_rollup_t00;
CREATE TABLE grouping_rollup_t00 (
col_int INT,
col_decimal_20_2 DECIMAL(20,2),
col_number DECIMAL,
col_varchar2_20 VARCHAR(20),
pk SMALLINT,
col_varchar2_21 VARCHAR(21),
col_date DATE,
col_timestamp DATETIME,
col_decimal_21_2 DECIMAL(21,2),
PRIMARY KEY (pk)
) PARTITION BY HASH(pk) PARTITIONS 4;
INSERT INTO grouping_rollup_t00 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (-1611857920, -474021888, -566689792, NULL, 1, 'this', '2005-02-05 21:11:43', '2009-07-27', 1225785344);
INSERT INTO grouping_rollup_t00 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (1588789248, 749535232, 351666176, '', 2, 's', '2006-06-23 06:15:53', '2007-06-26', 0);
INSERT INTO grouping_rollup_t00 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (1103495168, 384565248, 1958608896, '', 3, NULL, '2006-03-05 20:18:36.015514', '2003-09-10 04:13:09.010524', 844300288);
INSERT INTO grouping_rollup_t00 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (0, 0, 0, 'q', 4, 'lhit', '2001-11-10', '2000-02-10 21:45:18.021280', -1576271872);
INSERT INTO grouping_rollup_t00 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (-1646985216, 0, 0, 'm', 5, '', '2004-09-25', '2007-05-16 19:15:59.034385', 992542720);
INSERT INTO grouping_rollup_t00 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2) VALUES (NULL, NULL, NULL, NULL, 6, '', NULL, NULL, NULL);


DROP TABLE  IF EXISTS grouping_rollup_t01;
CREATE TABLE grouping_rollup_t01 (
col_int INT,
col_decimal_20_2 DECIMAL(20,2),
col_number DECIMAL,
col_varchar2_20 VARCHAR(20),
pk SMALLINT,
col_varchar2_21 VARCHAR(21),
col_date DATE,
col_timestamp DATETIME,
col_decimal_21_2 DECIMAL(21,2),
PRIMARY KEY (pk)
) PARTITION BY HASH(pk) PARTITIONS 10;
INSERT INTO grouping_rollup_t01 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2)
VALUES (0, 0, 0, '', 8, '', NULL, '2004-03-12 21:25:48.044571', 1668415488);
INSERT INTO grouping_rollup_t01 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2)
VALUES (1455292416, -476250112, -1987248128, NULL, 9, 'she', '2008-10-19', '2008-07-28 19:09:49.001817', 798556160);
INSERT INTO grouping_rollup_t01 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2)
VALUES (-2106064896, 148242432, -430309376, 'j', 10, 'rv', '2005-03-03 16:34:52.001480', '2004-11-23 02:21:32.018522', 0);
INSERT INTO grouping_rollup_t01 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2)
VALUES (0, -1016922112, 1504968704, 'w', 11, NULL, '2007-04-21 16:13:18', '2008-01-28 16:47:00.029273', -718667776);
INSERT INTO grouping_rollup_t01 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2)
VALUES (-759234560, 1424949248, -798031872, 'srvlhitckzgppy', 12, 'w', NULL, '2004-07-26 22:05:43.046113', 0);
INSERT INTO grouping_rollup_t01 (col_int, col_decimal_20_2, col_number, col_varchar2_20, pk, col_varchar2_21, col_date, col_timestamp, col_decimal_21_2)
VALUES (-1652883456, 530972672, 1590362112, 'are', 13, NULL, '2008-07-01', '2005-12-22 07:38:05.023325', 0);

DROP TABLE  IF EXISTS first_last_value1;
CREATE TABLE first_last_value1 (
c0 DECIMAL,
c1 DECIMAL,
c2 VARCHAR(10),
c3 VARCHAR(10),
c4 DATE,
c5 DECIMAL GENERATED ALWAYS AS (c1+c0) STORED,
PRIMARY KEY (c1)
);
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 1, 'oraman', 'dba', '2019-12-10');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 2, 'ross', 'sa', '2019-12-11');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 3, 'james', 'sa', '2019-12-11');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 4, 'terry', 'sa', '2019-12-13');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 5, 'cattiny', 'sa', '2019-12-14');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 8, 'terry', 'cto', '2019-12-12');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 10, 'gc', 'dba', '2019-12-10');
INSERT INTO first_last_value1 (c0, c1, c2, c3, c4) VALUES (2, 11, 'henry', 'dba', '2019-12-10');


DROP TABLE  IF EXISTS lead_lag2;
CREATE TABLE lead_lag2 (
c1 VARCHAR(64) NOT NULL,
c2 VARCHAR(10) NOT NULL,
c3 DATE NOT NULL,
PRIMARY KEY (c1)
);
INSERT INTO lead_lag2 VALUES ('1000001', 'AB7477', '2015-11-30 10:18:12');
INSERT INTO lead_lag2 VALUES ('1000002', 'AB7477', '2015-11-30 10:22:12');
INSERT INTO lead_lag2 VALUES ('1000003', 'AB7477', '2015-11-30 10:28:12');
INSERT INTO lead_lag2 VALUES ('1000004', 'AB7477', '2015-11-30 10:29:12');
INSERT INTO lead_lag2 VALUES ('1000005', 'AB7477', '2015-11-30 10:39:13');
INSERT INTO lead_lag2 VALUES ('1000006', 'AB7477', '2015-11-30 10:45:12');
INSERT INTO lead_lag2 VALUES ('1000007', 'AB7477', '2015-11-30 10:56:12');
INSERT INTO lead_lag2 VALUES ('1000008', 'AB7477', '2015-11-30 10:57:12');
INSERT INTO lead_lag2 VALUES ('1000009', 'AB3808', '2015-11-30 11:00:12');
INSERT INTO lead_lag2 VALUES ('1000010', 'AB3808', '2015-11-30 11:10:13');
INSERT INTO lead_lag2 VALUES ('1000011', 'AB3808', '2015-11-30 11:15:12');
INSERT INTO lead_lag2 VALUES ('1000012', 'AB3808', '2015-11-30 11:26:12');
INSERT INTO lead_lag2 VALUES ('1000013', 'AB3808', '2015-11-30 11:30:12');


DROP TABLE  IF EXISTS wf_test111;
CREATE TABLE wf_test111 (
pk SMALLINT,
col_number DECIMAL,
col_int INT,
col_date DATETIME,
col_varchar2_20 VARCHAR(20),
col_decimal_20_2 DECIMAL(20,2),
PRIMARY KEY (pk)
);
INSERT INTO wf_test111 VALUES (1, 868548608, 0, '2008-12-25 01:10:47.050179', 'there', 1387003904);
INSERT INTO wf_test111 VALUES (2, 538902528, 0, '2002-11-05 16:28:11.056788', 'q', -475725824);
INSERT INTO wf_test111 VALUES (3, -1287520256, -361234432, '2000-07-20 05:30:47.051125', 'back', -778567680);
INSERT INTO wf_test111 VALUES (4, -1103888384, -1961033728, '2004-03-07 14:02:48.037340', 'w', -98959360);
INSERT INTO wf_test111 VALUES (5, 1601961984, -608960512, '2009-01-17 03:03:49.050687', 'd', -868155392);
INSERT INTO wf_test111 VALUES (6, 1662976000, 0, '2001-11-28 00:50:27.051028', 'the', -1465647104);
INSERT INTO wf_test111 VALUES (7, 0, 0, '2002-11-01 20:02:37', 'some', -298057728);
INSERT INTO wf_test111 VALUES (8, 1550123008, 2053308416, '2004-05-07 10:00:03.039599', 'be', -1808990208);
COMMIT;



DROP TABLE  IF EXISTS test200 ;
CREATE TABLE test200(f1 int, f2 varchar(10), f3 varchar(10));
INSERT INTO test200 VALUES(1001,'a','12345');
INSERT INTO test200 VALUES(1002,'b','67890');
INSERT INTO test200 VALUES(1003,'c','23456');
commit;

DROP TABLE  IF EXISTS test201 ;
CREATE TABLE test201(f1 NUMBER(5,0) PRIMARY KEY, f2 varchar(10), f3 varchar(10));
INSERT INTO test201 VALUES(1001,'a','12345');
INSERT INTO test201 VALUES(1002,'b','67890');
INSERT INTO test201 VALUES(1003,'c','23456');
INSERT INTO test201 VALUES(1006,'d','23456');

DROP TABLE  IF EXISTS test202 ;
CREATE TABLE test202(f1 NUMBER(5,0) PRIMARY KEY, f2 varchar(10), f3 varchar(10)) ;
INSERT INTO test202 VALUES(1001,'a','12345');
INSERT INTO test202 VALUES(1003,'b','67890');
INSERT INTO test202 VALUES(1004,'c','23456');

DROP TABLE  IF EXISTS test203 ;
CREATE TABLE test203(f1 NUMBER(5,0), f2 varchar(10), f3 varchar(10),PRIMARY KEY(f1,f2)) ;
INSERT INTO test203 VALUES(1001,'a','12345');
INSERT INTO test203 VALUES(1003,'b','67890');
INSERT INTO test203 VALUES(1004,'c','23456');
INSERT INTO test203 VALUES(1006,'d','23456');
INSERT INTO test203 VALUES(1007,'e','23456');
commit;

DROP TABLE  IF EXISTS test204 ;
CREATE TABLE test204(f1 int PRIMARY KEY, f2 varchar(10), f3 varchar(10));
INSERT INTO test204 VALUES(1001,'a','12345');
INSERT INTO test204 VALUES(1003,'b','67890');
INSERT INTO test204 VALUES(1004,'c','23456');
INSERT INTO test204 VALUES(1006,'d','23456');
INSERT INTO test204 VALUES(1007,'e','23456');
commit;

DROP TABLE  IF EXISTS test206 ;
DROP TABLE  IF EXISTS test207 ;
DROP TABLE  IF EXISTS test208 ;
CREATE TABLE test206(f1 NUMBER PRIMARY KEY);
INSERT INTO test206 VALUES(1e1);
INSERT INTO test206 VALUES(1e-1);
INSERT INTO test206 VALUES(5);
CREATE TABLE test207(f1 NUMBER(15));
INSERT INTO test207 VALUES(1e1);
INSERT INTO test207 VALUES(1e-1);
INSERT INTO test207 VALUES(5);
CREATE TABLE test208(f1 NUMBER(15,12));
INSERT INTO test208 VALUES(100);
INSERT INTO test208 VALUES(1e-1);
INSERT INTO test208 VALUES(5.5);

DROP TABLE  IF EXISTS test209 ;
DROP TABLE  IF EXISTS test210 ;
CREATE TABLE test209(f1 float);
INSERT INTO test209 VALUES(1e1);
INSERT INTO test209 VALUES(1e-1);
INSERT INTO test209 VALUES(5);
CREATE TABLE test210(f1 float(15) PRIMARY KEY);
INSERT INTO test210 VALUES(1e1);
INSERT INTO test210 VALUES(1e-1);
INSERT INTO test210 VALUES(5);

DROP TABLE  IF EXISTS test211 ;
CREATE TABLE test211(f1 float);
INSERT INTO test211 VALUES(1e1);
INSERT INTO test211 VALUES(1e-1);
INSERT INTO test211 VALUES(5);

DROP TABLE  IF EXISTS test212 ;
CREATE TABLE test212(f1 double PRIMARY KEY);
INSERT INTO test212 VALUES(1e1);
INSERT INTO test212 VALUES(1e-1);
INSERT INTO test212 VALUES(5);


DROP TABLE  IF EXISTS test213 ;
DROP TABLE  IF EXISTS test214 ;
DROP TABLE  IF EXISTS test215 ;
CREATE TABLE test213(f1 varchar(5));
INSERT INTO test213 VALUES('11');
INSERT INTO test213 VALUES('aa');
INSERT INTO test213 VALUES('33');
CREATE TABLE test214(f1 varchar(5) PRIMARY KEY);
INSERT INTO test214 VALUES('11');
INSERT INTO test214 VALUES('aa');
INSERT INTO test214 VALUES('66');

CREATE TABLE test215(f1 varchar(5) PRIMARY KEY);
INSERT INTO test215 VALUES('111');
INSERT INTO test215 VALUES('aaa');
INSERT INTO test215 VALUES('666');

DROP TABLE  IF EXISTS test216 ;
DROP TABLE  IF EXISTS test217 ;
CREATE TABLE test216(f1 varchar(20) PRIMARY KEY);
INSERT INTO test216 VALUES('1');
INSERT INTO test216 VALUES('a');
INSERT INTO test216 VALUES('3');
CREATE TABLE test217(f1 varchar(20) PRIMARY KEY);
INSERT INTO test217 VALUES('11');
INSERT INTO test217 VALUES('aa');
INSERT INTO test217 VALUES('66');

DROP TABLE  IF EXISTS test218 ;
CREATE TABLE test218(f1 varchar(5) PRIMARY KEY) ;
INSERT INTO test218 VALUES('11');
INSERT INTO test218 VALUES('aa');
INSERT INTO test218 VALUES('66');

DROP TABLE   IF EXISTS  test219;
CREATE TABLE test219(f1 INT, f2 DATETIME);
INSERT INTO test219 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test219 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test219 VALUES(105, '2020-09-25 12:01:00');


DROP TABLE   IF EXISTS  test220;
CREATE TABLE test220(f1 INT, f2 DATETIME);
INSERT INTO test220 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test220 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test220 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test221;
CREATE TABLE test221(f1 INT, f2 DATETIME);
INSERT INTO test221 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test221 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test221 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test222;
CREATE TABLE test222(f1 INT, f2 DATETIME);
INSERT INTO test222 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test222 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test222 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test223;
CREATE TABLE test223(f1 INT, f2 DATETIME);
INSERT INTO test223 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test223 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test223 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test224;
CREATE TABLE test224(f1 INT, f2 DATETIME);
INSERT INTO test224 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test224 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test224 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test225;
CREATE TABLE test225(f1 INT, f2 DATETIME);
INSERT INTO test225 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test225 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test225 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test226;
CREATE TABLE test226(f1 INT, f2 DATETIME);
INSERT INTO test226 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test226 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test226 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test227;
CREATE TABLE test227(f1 INT, f2 DATETIME);
INSERT INTO test227 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test227 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test227 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test228;
CREATE TABLE test228(f1 INT, f2 DATETIME);
INSERT INTO test228 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test228 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test228 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test229;
CREATE TABLE test229(f1 INT, f2 DATETIME);
INSERT INTO test229 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test229 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test229 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test230;
CREATE TABLE test230(f1 INT, f2 DATETIME);
INSERT INTO test230 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test230 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test230 VALUES(105, '2020-09-25 12:01:00');
DROP TABLE   IF EXISTS  test231;
CREATE TABLE test231(f1 INT, f2 DATETIME);
INSERT INTO test231 VALUES(101, '2005-09-25 12:01:00');
INSERT INTO test231 VALUES(103, '2009-09-25 12:01:00');
INSERT INTO test231 VALUES(105, '2020-09-25 12:01:00');


DROP TABLE  IF EXISTS test232 ;
CREATE TABLE test232(f1 INT PRIMARY KEY, f2 LONGTEXT);
INSERT INTO test232 VALUES(123, 'adminadminadmin');
INSERT INTO test232 VALUES(456, 'testtesttest');

DROP TABLE   IF EXISTS  test233;
CREATE TABLE test233(f1 INT, f2 BINARY(20));
INSERT INTO test233 VALUES(101, UNHEX('72617774797065'));
INSERT INTO test233 VALUES(102, UNHEX('31323334353637'));
COMMIT;

--enable_query_log
--enable_warnings

SELECT 1;