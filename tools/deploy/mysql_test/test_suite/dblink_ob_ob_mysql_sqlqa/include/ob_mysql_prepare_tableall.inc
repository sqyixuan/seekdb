set autocommit = on;
set ob_query_timeout=99900000000;
set ob_trx_timeout = 1000000000;  
SET NAMES UTF8;

--disable_query_log
--disable_warnings
DROP TABLE IF EXISTS class150;
CREATE TABLE class150 (id int, name varchar(50) DEFAULT 'yinyue',location varchar(50),create_time timestamp(5))
PARTITION BY LIST(id) (
PARTITION p1 VALUES IN (101, 102),
PARTITION p2 VALUES IN (DEFAULT)
);
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
c_id int
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
c_id int
);
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
c_id int
);

INSERT INTO stu333 VALUES(1001,NULL,'zhangsan','zs',STR_TO_DATE('25-09-1991 12:01:33','%d-%m-%Y %H:%i:%s'),47.5,101);
INSERT INTO stu333 VALUES(1002,1001,'lisi','ls',STR_TO_DATE('25-09-1992 12:01:33','%d-%m-%Y %H:%i:%s'),57.5,101);
INSERT INTO stu333 VALUES(1003,1001,'wangwu','ww',STR_TO_DATE('25-09-1993 12:01:33','%d-%m-%Y %H:%i:%s'),67.5,102);
INSERT INTO stu333 VALUES(1004,1002,'zhaoliu','zl',STR_TO_DATE('25-09-1994 12:01:33','%d-%m-%Y %H:%i:%s'),77.5,102);
INSERT INTO stu333 VALUES(1005,1002,'zhangsi','zs',STR_TO_DATE('25-09-1995 12:01:33','%d-%m-%Y %H:%i:%s'),87.5,103);
INSERT INTO stu333 VALUES(1006,1002,'liwu','lw',STR_TO_DATE('25-09-1996 12:01:33','%d-%m-%Y %H:%i:%s'),37.5,103);
INSERT INTO stu333 VALUES(1007,1003,'wangliu','wl',STR_TO_DATE('25-09-1997 12:01:33','%d-%m-%Y %H:%i:%s'),97.5,103);


DROP TABLE IF EXISTS class111;
CREATE TABLE class111 (id int, name varchar(50) DEFAULT 'yinyue',location varchar(50),create_time timestamp(5))
PARTITION BY LIST(id) (
PARTITION p1 VALUES IN (101, 102),
PARTITION p2 VALUES IN (DEFAULT)
);
INSERT INTO class111 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class111 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class111 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class111 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class111 VALUES(106,'baba','xiaoshan',STR_TO_DATE('20071010','%Y%m%d'));

DROP TABLE IF EXISTS stu111;
CREATE TABLE stu111 (
id int PRIMARY KEY,
p_id int,
name varchar(50),
alias char(50),
birth DATE,
weight double,
c_id int
)
PARTITION BY RANGE(id) (
PARTITION p1 VALUES LESS THAN (1004),
PARTITION p2 VALUES LESS THAN (MAXVALUE)
);

INSERT INTO stu111 VALUES(1001, NULL, 'zhangsan', 'zs', '1991-09-25', 47.5, 101);
INSERT INTO stu111 VALUES(1002, 1001, 'lisi', 'ls', '1992-09-25', 57.5, 101);
INSERT INTO stu111 VALUES(1003, 1001, 'wangwu', 'ww', '1993-09-25', 67.5, 102);
INSERT INTO stu111 VALUES(1004, 1002, 'zhaoliu', 'zl', '1994-09-25', 77.5, 102);
INSERT INTO stu111 VALUES(1005, 1002, 'zhangsi', 'zs', '1995-09-25', 87.5, 103);
INSERT INTO stu111 VALUES(1006, 1002, 'liwu', 'lw', '1996-09-25', 37.5, 103);
INSERT INTO stu111 VALUES(1007, 1003, 'wangliu', 'wl', '1997-09-25', 97.5, 103);


DROP TABLE IF EXISTS class222;
CREATE TABLE class222 (id int PRIMARY KEY, name char(50) DEFAULT 'yinyue', location varchar(50), create_time timestamp(5))
PARTITION BY HASH(id) PARTITIONS 2;
INSERT INTO class222 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class222 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class222 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class222 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class222 VALUES(106,'baba','xiaoshan',STR_TO_DATE('10-10-2007','%d-%m-%Y'));

DROP TABLE IF EXISTS stu222;
CREATE TABLE stu222 (
id int,
p_id int,
name varchar(50),
alias char(50),
birth DATE,
weight double,
c_id int
);
INSERT INTO stu222 VALUES(1001,NULL,'zhangsan','zs',STR_TO_DATE('25-09-1991 12:01:33','%d-%m-%Y %H:%i:%s'),47.5,101);
INSERT INTO stu222 VALUES(1002,1001,'lisi','ls',STR_TO_DATE('25-09-1992 12:01:33','%d-%m-%Y %H:%i:%s'),57.5,101);
INSERT INTO stu222 VALUES(1003,1001,'wangwu','ww',STR_TO_DATE('25-09-1993 12:01:33','%d-%m-%Y %H:%i:%s'),67.5,102);
INSERT INTO stu222 VALUES(1004,1002,'zhaoliu','zl',STR_TO_DATE('25-09-1994 12:01:33','%d-%m-%Y %H:%i:%s'),77.5,102);
INSERT INTO stu222 VALUES(1005,1002,'zhangsi','zs',STR_TO_DATE('25-09-1995 12:01:33','%d-%m-%Y %H:%i:%s'),87.5,103);
INSERT INTO stu222 VALUES(1006,1002,'liwu','lw',STR_TO_DATE('25-09-1996 12:01:33','%d-%m-%Y %H:%i:%s'),37.5,103);
INSERT INTO stu222 VALUES(1007,1003,'wangliu','wl',STR_TO_DATE('25-09-1997 12:01:33','%d-%m-%Y %H:%i:%s'),97.5,103);

DROP TABLE IF EXISTS class1121;
CREATE TABLE class1121 (id int PRIMARY KEY, name char(50) DEFAULT 'yinyue', location varchar(50), create_time timestamp(5))
PARTITION BY HASH(id) PARTITIONS 2;
INSERT INTO class1121 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class1121 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class1121 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class1121 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class1121 VALUES(106,'baba','xiaoshan',STR_TO_DATE('10-10-2007','%d-%m-%Y'));

DROP TABLE IF EXISTS stu1121;
CREATE TABLE stu1121 (
id int,
p_id int,
name varchar(50),
alias char(50),
birth DATE,
weight double,
c_id int
);
INSERT INTO stu1121 VALUES(1001,NULL,'zhangsan','zs',STR_TO_DATE('25-09-1991 12:01:33','%d-%m-%Y %H:%i:%s'),47.5,101);
INSERT INTO stu1121 VALUES(1002,1001,'lisi','ls',STR_TO_DATE('25-09-1992 12:01:33','%d-%m-%Y %H:%i:%s'),57.5,101);
INSERT INTO stu1121 VALUES(1003,1001,'wangwu','ww',STR_TO_DATE('25-09-1993 12:01:33','%d-%m-%Y %H:%i:%s'),67.5,102);
INSERT INTO stu1121 VALUES(1004,1002,'zhaoliu','zl',STR_TO_DATE('25-09-1994 12:01:33','%d-%m-%Y %H:%i:%s'),77.5,102);
INSERT INTO stu1121 VALUES(1005,1002,'zhangsi','zs',STR_TO_DATE('25-09-1995 12:01:33','%d-%m-%Y %H:%i:%s'),87.5,103);
INSERT INTO stu1121 VALUES(1006,1002,'liwu','lw',STR_TO_DATE('25-09-1996 12:01:33','%d-%m-%Y %H:%i:%s'),37.5,103);
INSERT INTO stu1121 VALUES(1007,1003,'wangliu','wl',STR_TO_DATE('25-09-1997 12:01:33','%d-%m-%Y %H:%i:%s'),97.5,103);

DROP TABLE IF EXISTS class2121;
CREATE TABLE class2121 (id int PRIMARY KEY, name char(50) DEFAULT 'yinyue', location varchar(50), create_time timestamp(5))
PARTITION BY HASH(id) PARTITIONS 2;
INSERT INTO class2121 VALUES(101,DEFAULT,'binjiang',STR_TO_DATE('25-09-2005 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class2121 VALUES(102,'dianzi','xixi',STR_TO_DATE('25-09-2008 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class2121 VALUES(103,'jisuanji','binjiang',STR_TO_DATE('25-09-2018 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class2121 VALUES(104,'yishu','xixi',STR_TO_DATE('25-09-2020 12:01:33.3333','%d-%m-%Y %H:%i:%s.%f'));
INSERT INTO class2121 VALUES(106,'baba','xiaoshan',STR_TO_DATE('10-10-2007','%d-%m-%Y'));

DROP TABLE IF EXISTS stu2121;
CREATE TABLE stu2121 (
id int,
p_id int,
name varchar(50),
alias char(50),
birth DATE,
weight double,
c_id int
);
INSERT INTO stu2121 VALUES(1001,NULL,'zhangsan','zs',STR_TO_DATE('25-09-1991 12:01:33','%d-%m-%Y %H:%i:%s'),47.5,101);
INSERT INTO stu2121 VALUES(1002,1001,'lisi','ls',STR_TO_DATE('25-09-1992 12:01:33','%d-%m-%Y %H:%i:%s'),57.5,101);
INSERT INTO stu2121 VALUES(1003,1001,'wangwu','ww',STR_TO_DATE('25-09-1993 12:01:33','%d-%m-%Y %H:%i:%s'),67.5,102);
INSERT INTO stu2121 VALUES(1004,1002,'zhaoliu','zl',STR_TO_DATE('25-09-1994 12:01:33','%d-%m-%Y %H:%i:%s'),77.5,102);
INSERT INTO stu2121 VALUES(1005,1002,'zhangsi','zs',STR_TO_DATE('25-09-1995 12:01:33','%d-%m-%Y %H:%i:%s'),87.5,103);
INSERT INTO stu2121 VALUES(1006,1002,'liwu','lw',STR_TO_DATE('25-09-1996 12:01:33','%d-%m-%Y %H:%i:%s'),37.5,103);
INSERT INTO stu2121 VALUES(1007,1003,'wangliu','wl',STR_TO_DATE('25-09-1997 12:01:33','%d-%m-%Y %H:%i:%s'),97.5,103);

DROP TABLE IF EXISTS test11111;
DROP TABLE IF EXISTS test11112;
DROP TABLE IF EXISTS test11113;
CREATE TABLE test11111 (c1 VARCHAR(1), c2 INT, c3 INT);
INSERT/*insert trace*/ INTO test11111 VALUES ('',6,8);
INSERT/*insert trace*/ INTO test11111 VALUES ('',75,NULL);

CREATE TABLE test11112 (c1 VARCHAR(1));
INSERT/*insert trace*/ INTO test11112 VALUES ('S');
INSERT/*insert trace*/ INTO test11112 VALUES ('S');
INSERT/*insert trace*/ INTO test11112 VALUES ('S');

CREATE TABLE test11113 (i1 INT);
INSERT/*insert trace*/ INTO test11113 VALUES (2);
INSERT/*insert trace*/ INTO test11113 VALUES (4);
INSERT/*insert trace*/ INTO test11113 VALUES (6);
INSERT/*insert trace*/ INTO test11113 VALUES (8);


DROP TABLE IF EXISTS test33333;
create table test33333(id number(20));
insert into test33333 values(123456789);
insert into test33333 values(1234567890);
insert into test33333 values(12345678901);
insert into test33333 values(12345678901111111);
insert into test33333 values(1234567890122222222);


DROP TABLE IF EXISTS test44444;
create table test44444(id number(20));
insert into test44444 values(123456789);
insert into test44444 values(1234567890);
insert into test44444 values(12345678901);
insert into test44444 values(12345678901111111);
insert into test44444 values(12345678901234567890);


DROP TABLE IF EXISTS test331;
DROP TABLE IF EXISTS test332;
DROP TABLE IF EXISTS test333;
DROP TABLE IF EXISTS test334;
create table test331 (a int);
create table test332 (a int, b int);
create table test333 (a int);
create table test334 (a int not null, b int not null);
insert into test331 values (2);
insert into test332 values (1,7),(2,7);
insert into test333 values (6),(7),(3);
insert into test334 values (4,8),(3,8),(5,9);


DROP TABLE IF EXISTS test161;
DROP TABLE IF EXISTS test162;
DROP TABLE IF EXISTS test163;
DROP TABLE IF EXISTS test164;
DROP TABLE IF EXISTS test165;
DROP TABLE IF EXISTS test166;
DROP TABLE IF EXISTS test167;
DROP TABLE IF EXISTS test168;
DROP TABLE IF EXISTS test169;
DROP TABLE IF EXISTS test170;
DROP TABLE IF EXISTS test171;
DROP TABLE IF EXISTS test172;
DROP TABLE IF EXISTS test173;
DROP TABLE IF EXISTS test174;
DROP TABLE IF EXISTS test175;
DROP TABLE IF EXISTS test176;
DROP TABLE IF EXISTS test177;

create table test161 (a int, b int);
create table test162 (a int, b int);
create table test163 (a int, b int);
insert into test161 values (0,100),(1,2), (1,3), (2,2), (2,7), (2,-1), (3,10);
insert into test162 values (0,0), (1,1), (2,1), (3,1), (4,1);
insert into test163 values (3,3), (2,2), (1,1);

create table test164 (s1 int);
create table test165 (s1 int);
insert into test164 values (1);
insert into test165 values (1);

CREATE TABLE test166 (id int default NULL,name varchar(20) default NULL);
INSERT INTO test166 VALUES (1,'Tim'),(2,'Rebecca'),(3,NULL);
CREATE TABLE test167 (id int default NULL, pet varchar(20) default NULL);
INSERT INTO test167 VALUES (1,'Fido'),(2,'Spot'),(3,'Felix');

create table test168 (i int, j number(10));
insert into test168 values (1, 2), (2, 2), (3, 2);

CREATE TABLE test169 (a int, b INT, d INT, c CHAR(10) NOT NULL, PRIMARY KEY (a, b));
INSERT INTO test169 VALUES (1,1,0,'a'), (1,2,0,'b'), (1,3,0,'c'), (1,4,0,'d'),
(1,5,0,'e'), (2,1,0,'f'), (2,2,0,'g'), (2,3,0,'h'), (3,4,0,'i'), (3,3,0,'j'),
(3,2,0,'k'), (3,1,0,'l'), (1,9,0,'m'), (1,0,10,'n'), (2,0,5,'o'), (3,0,7,'p');

CREATE TABLE test170 (id int NOT NULL, st CHAR(2), INDEX idx(id));
INSERT INTO test170 VALUES
  (3,'FL'), (2,'GA'), (4,'FL'), (1,'GA'), (5,'NY'), (7,'FL'), (6,'NY');
CREATE TABLE test171 (id int NOT NULL, INDEX idx2(id));
INSERT INTO test171 VALUES (7), (5), (1), (3);

create table test172 (a int, b int);
insert into test172 values (0,0), (2,2), (3,3);
create table test173 (a int, b int);
insert into test173 values (1,1), (3,3);
insert into test173 values (NULL,4);

CREATE TABLE test174 (
  pk INT,
  col_varchar varchar(1),
  PRIMARY KEY (pk)
)
;

INSERT INTO test174 (pk,col_varchar)
VALUES
(1,'w'),
(2,'m')
;

CREATE TABLE test175 (c1 varchar(1), c2 INT, c3 INT);
INSERT/*insert trace*/ INTO test175 VALUES ('',6,8), ('',75,NULL);

CREATE TABLE test176 (c1 varchar(1));
INSERT/*insert trace*/ INTO test176 VALUES ('S'), ('S'), ('S');

CREATE TABLE test177 (i1 INT);
INSERT/*insert trace*/ INTO test177 VALUES (2),(4),(6),(8);



DROP TABLE IF EXISTS test181;
DROP TABLE IF EXISTS test182;
DROP TABLE IF EXISTS test183;
DROP TABLE IF EXISTS test184;
DROP TABLE IF EXISTS test185;

CREATE TABLE test181 (
  categoryId int NOT NULL,
  courseId int NOT NULL,
  startDate timestamp NOT NULL,
  endDate timestamp NOT NULL,
  createDate timestamp NOT NULL,
  modifyDate timestamp NOT NULL,
  attributes varchar(30)
);
INSERT INTO test181 VALUES (1,41,'2004-02-09','2010-01-01','2004-02-09','2004-02-09',''),
(1,86,'2004-08-16','2004-08-16','2004-08-16','2004-08-16',''),
(1,87,'2004-08-16','2004-08-16','2004-08-16','2004-08-16',''),
(2,52,'2004-03-15','2004-10-01','2004-03-15','2004-09-17',''),
(2,53,'2004-03-16','2004-10-01','2004-03-16','2004-09-17',''),
(2,88,'2004-08-16','2004-08-16','2004-08-16','2004-08-16',''),
(2,89,'2004-08-16','2004-08-16','2004-08-16','2004-08-16',''),
(3,51,'2004-02-09','2010-01-01','2004-02-09','2004-02-09',''),
(5,12,'2004-02-18','2010-01-01','2004-02-18','2004-02-18','');

CREATE TABLE test182 (
  userId int NOT NULL,
  courseId int NOT NULL,
  datett timestamp NOT NULL
);
INSERT INTO test182 VALUES (5141,71,'2003-11-18'),
(5141,72,'2003-11-25'),(5141,41,'2004-08-06'),
(5141,52,'2004-08-06'),(5141,53,'2004-08-06'),
(5141,12,'2004-08-06'),(5141,86,'2004-10-21'),
(5141,87,'2004-10-21'),(5141,88,'2004-10-21'),
(5141,89,'2004-10-22'),(5141,51,'2004-10-26');


CREATE TABLE test183 (
  groupId int NOT NULL,
  parentId int NOT NULL,
  startDate timestamp NOT NULL,
  endDate timestamp NOT NULL,
  createDate timestamp NOT NULL,
  modifyDate timestamp NOT NULL,
  ordering int
);
INSERT INTO test183 VALUES (12,9,'1000-01-01','3999-12-31','2004-01-29','2004-01-29',NULL);

CREATE TABLE test184 (
  id int NOT NULL,
  groupTypeId int NOT NULL,
  groupKey varchar(50) NOT NULL,
  name varchar(50),
  ordering int,
  description varchar(50),
  createDate timestamp NOT NULL,
  modifyDate timestamp NOT NULL
);
INSERT INTO test184 VALUES (9,5,'stationer','stationer',0,'Stationer','2004-01-29','2004-01-29'),
(12,5,'group2','group2',0,'group2','2004-01-29','2004-01-29');

CREATE TABLE test185 (
  userId int NOT NULL,
  groupId int NOT NULL,
  createDate timestamp NOT NULL,
  modifyDate timestamp NOT NULL
);
INSERT INTO test185 VALUES (5141,12,'2004-08-06','2004-08-06');

DROP TABLE IF EXISTS test551;
DROP TABLE IF EXISTS test552;
DROP TABLE IF EXISTS test553;
DROP TABLE IF EXISTS test554;
DROP TABLE IF EXISTS test555;
DROP TABLE IF EXISTS test556;

CREATE TABLE test551(DEPTNO NUMBER(2) PRIMARY KEY,DNAME VARCHAR(14),LOC VARCHAR(13));
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
SAL FLOAT(7,2),
COMM FLOAT(7,2),
DEPTNO INT(2)
);

INSERT INTO test552 VALUES(7369,'SMITH','CLERK',7902,str_to_date('17-12-1980','%d-%m-%Y'),800,100,2);
INSERT INTO test552 VALUES(7499,'ALLEN','SALESMAN',7698,str_to_date('20-2-1981','%d-%m-%Y'),1600,300,3);
INSERT INTO test552 VALUES(7521,'WARD','SALESMAN',7698,str_to_date('22-2-1981','%d-%m-%Y'),1250,500,3);
INSERT INTO test552 VALUES(7566,'JONES','MANAGER',7839,str_to_date('2-4-1981','%d-%m-%Y'),2975,358.69,2);
INSERT INTO test552 VALUES(7654,'MARTIN','SALESMAN',7698,str_to_date('28-9-1981','%d-%m-%Y'),1250,1400,3);
INSERT INTO test552 VALUES(7698,'BLAKE','MANAGER',7839,str_to_date('1-5-1981','%d-%m-%Y'),2850,NULL,3);
INSERT INTO test552 VALUES(7782,'CLARK','MANAGER',7839,str_to_date('9-6-1981','%d-%m-%Y'),4450,744.9,1);
INSERT INTO test552 VALUES(7788,'SCOTT','ANALYST',7566,str_to_date('13-4-1987','%d-%m-%Y'),3000,-200,2);
INSERT INTO test552 VALUES(7839,'KING','PRESIDENT',NULL,str_to_date('17-11-1981','%d-%m-%Y'),4999,NULL,1);
INSERT INTO test552 VALUES(7844,'TURNER','SALESMAN',7698,str_to_date('8-9-1981','%d-%m-%Y'),1500,0,3);
INSERT INTO test552 VALUES(7876,'ADAMS','CLERK',7788,str_to_date('13-5-1987','%d-%m-%Y'),1100,-99.9,2);
INSERT INTO test552 VALUES(7900,'JAMES','CLERK',7698,str_to_date('3-12-1981','%d-%m-%Y'),950,-1808,3);
INSERT INTO test552 VALUES(7902,'FORD','ANALYST',7566,str_to_date('3-12-1981','%d-%m-%Y'),3000,753.9,2);
INSERT INTO test552 VALUES(7934,'MILLER','CLERK',7782,str_to_date('23-1-1982','%d-%m-%Y'),1300,729,1);

CREATE TABLE test553(
C1 DECIMAL(5,2),
C2 DECIMAL(5,2),
C3 CHAR(1),
C4 INT,
C5 CHAR(1),
C6 CHAR(1),
C7 INT,
C8 CHAR(1)
);

INSERT INTO test553 VALUES(1,5,'e',8,NULL,'t',NULL,NULL);
INSERT INTO test553 VALUES(2,3,'x',2,'q','k',0,NULL);
INSERT INTO test553 VALUES(2,4,'o',5,'w','u',8,'n');
INSERT INTO test553 VALUES(3,4,'h',2,'y','z',NULL,NULL);
INSERT INTO test553 VALUES(4,6,'j',2,'h','p',1,NULL);
INSERT INTO test553 VALUES(4,4,'i',2,'d','x',0,'f');
INSERT INTO test553 VALUES(3,9,'e',0,'r','v',6,'d');
INSERT INTO test553 VALUES(2,9,'y',7,'h','q',2,'v');
INSERT INTO test553 VALUES(1,2,'w',1,'c','l',7,'o');
INSERT INTO test553 VALUES(1,9,'y',1,NULL,'s',5,NULL);
INSERT INTO test553 VALUES(2,6,'h',7,NULL,'f',8,NULL);
INSERT INTO test553 VALUES(4,5,'i',2,NULL,'w',NULL,'x');

CREATE TABLE test554(DEPTNO NUMBER(2) PRIMARY KEY,DNAME VARCHAR(14),LOC VARCHAR(13));
INSERT INTO test554 SELECT * FROM test551 LIMIT 2;

CREATE TABLE test555(EMPNO NUMBER(4) UNIQUE,ENAME VARCHAR(10) NOT NULL,JOB VARCHAR(9),MGR NUMBER(4),HIREDATE DATE,SAL NUMBER(7,2),COMM NUMBER(7,2),DEPTNO NUMBER(2));
INSERT INTO test555 SELECT * FROM(SELECT * FROM test552 ORDER BY empno) LIMIT 5 ;

CREATE TABLE test556(C1 NUMBER(5,2),C2 NUMBER(5,2),C3 CHAR(1),C4 NUMBER,C5 VARCHAR(1),C6 VARCHAR(1),C7 NUMBER,C8 CHAR(1));
CREATE UNIQUE INDEX index_335 ON test556(c6);
CREATE INDEX index_433 ON test556(c1);
INSERT INTO test556 SELECT * FROM (SELECT * FROM test553 WHERE c3 IN ('w','e','y','x','i','h') ORDER BY 1,2,3,4);

CREATE OR REPLACE VIEW t1_view AS SELECT e.deptno,e.dname,e.loc,p.ename,p.job FROM test551 e,test555 p WHERE e.deptno = p.deptno ORDER BY 1,2,3;
CREATE OR REPLACE VIEW t2_view AS SELECT * FROM (SELECT * FROM test552 e WHERE e.sal < 4000 UNION ALL SELECT * FROM test555 p WHERE p.sal BETWEEN 1500 AND 3000 ORDER BY 1,2,3,4);
CREATE OR REPLACE VIEW t3_view AS SELECT DISTINCT e.c1,e.c2,e.c3,e.c4,p.c5,p.c6,p.c7,p.c8 FROM test553 e,test556 p WHERE e.c1 = p.c1 AND e.c7 IS NOT NULL ORDER BY 1,2,3,4;
CREATE OR REPLACE VIEW t4_view AS SELECT e.deptno,SUM(p.sal) ssal FROM test554 e,test552 p WHERE e.deptno = p.deptno GROUP BY e.deptno ORDER BY e.deptno;
CREATE OR REPLACE VIEW t5_view AS SELECT e.deptno,p.sal,c.c6,d.dname,v.job FROM test551 e LEFT JOIN test552 p ON e.deptno = p.deptno INNER JOIN test553 c ON p.deptno = c.c1 RIGHT JOIN test554 d ON c.c1 = d.deptno LEFT JOIN test555 v ON d.deptno = v.deptno; 
CREATE OR REPLACE VIEW t6_view AS SELECT * FROM test552 e WHERE EXISTS (SELECT p.sal FROM test555 p WHERE p.ename = 'ALLEN');
CREATE OR REPLACE VIEW t7_view AS SELECT * FROM test555 e WHERE NOT EXISTS (SELECT * FROM test551 e,test552 p WHERE e.deptno = p.deptno AND p.deptno = 4);


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

DROP TABLE IF EXISTS test190;
CREATE TABLE test190(
PK INT,
COL_INT INT,
COL_TS TIMESTAMP,
COL_VARCHAR2_200 VARCHAR(200),
COL_NUMBER_29_0 NUMERIC(29,0),
COL_DATE DATE,
COL_DECIMAL_21_2 DECIMAL(21,2));
INSERT INTO test190(PK, COL_INT, COL_TS, COL_VARCHAR2_200, COL_NUMBER_29_0, COL_DATE, COL_DECIMAL_21_2)
VALUES (1, 123, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);
INSERT INTO test190(PK, COL_INT, COL_TS, COL_VARCHAR2_200, COL_NUMBER_29_0, COL_DATE, COL_DECIMAL_21_2)
VALUES (2, 456, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);

DROP TABLE IF EXISTS test191;
CREATE TABLE test191(
num INT(4) UNIQUE,
name VARCHAR(10) NOT NULL,
job VARCHAR(9),
MGR INT(4),
HIREDATE DATE,
SAL DECIMAL(7,2),
COMM DECIMAL(7,2)
);

INSERT INTO test191 VALUES(1,'SMITH','CLERK',7902,STR_TO_DATE('17-12-1980','%d-%m-%Y'),800,100);
INSERT INTO test191 VALUES(2,'ALLEN','SALESMAN',7698,STR_TO_DATE('20-2-1981','%d-%m-%Y'),1600,300);
INSERT INTO test191 VALUES(3,'WARD','SALESMAN',7698,STR_TO_DATE('22-2-1981','%d-%m-%Y'),1250,500);
INSERT INTO test191 VALUES(4,'JONES','MANAGER',7839,STR_TO_DATE('2-4-1981','%d-%m-%Y'),2975,358.69);
INSERT INTO test191 VALUES(5,'MARTIN','SALESMAN',7698,STR_TO_DATE('28-9-1981','%d-%m-%Y'),1250,1400);
INSERT INTO test191 VALUES(6,'BLAKE','MANAGER',7839,STR_TO_DATE('1-5-1981','%d-%m-%Y'),2850,NULL);
INSERT INTO test191 VALUES(7,'CLARK','MANAGER',7839,STR_TO_DATE('9-6-1981','%d-%m-%Y'),2450,744.9);
INSERT INTO test191 VALUES(8,'SCOTT','ANALYST',7566,STR_TO_DATE('13-4-1987','%d-%m-%Y'),3000,-200);
INSERT INTO test191 VALUES(9,'KING','PRESIDENT',NULL,STR_TO_DATE('17-11-1981','%d-%m-%Y'),5000,NULL);
INSERT INTO test191 VALUES(10,'TURNER','SALESMAN',7698,STR_TO_DATE('8-9-1981','%d-%m-%Y'),1500,0);

DROP TABLE IF EXISTS test192;
CREATE TABLE test192 (PK INT,COL_INT INT,COL_TS TIMESTAMP,COL_VARCHAR2_200 VARCHAR(200),COL_NUMBER_29_0 NUMERIC(29,0),COL_DATE DATE, COL_DECIMAL_21_2 DECIMAL(21,2));
INSERT INTO test192 (PK,COL_INT,COL_TS,COL_VARCHAR2_200,COL_NUMBER_29_0,COL_DATE,COL_DECIMAL_21_2) VALUES (1, 123, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);
INSERT INTO test192 (PK,COL_INT,COL_TS,COL_VARCHAR2_200,COL_NUMBER_29_0,COL_DATE,COL_DECIMAL_21_2) VALUES (2, 456, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);
INSERT INTO test192 (PK,COL_INT,COL_TS,COL_VARCHAR2_200,COL_NUMBER_29_0,COL_DATE,COL_DECIMAL_21_2) VALUES (3, 789, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);
INSERT INTO test192 (PK,COL_INT,COL_TS,COL_VARCHAR2_200,COL_NUMBER_29_0,COL_DATE,COL_DECIMAL_21_2) VALUES (4, 111, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);
INSERT INTO test192 (PK,COL_INT,COL_TS,COL_VARCHAR2_200,COL_NUMBER_29_0,COL_DATE,COL_DECIMAL_21_2) VALUES (5, 222, '2022-01-01 00:00:00', 'test', 1234567890123456789012345678, '2022-01-01', 123.45);


DROP TABLE IF EXISTS studentdblink;
CREATE TABLE studentdblink(stuid varchar(20), stuname varchar(2000), stuage varchar(2000));
INSERT INTO studentdblink VALUES ('001','张三','十八');
INSERT INTO studentdblink VALUES ('002','黒 匼 ','黒 匼 ');
INSERT INTO studentdblink VALUES ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
INSERT INTO studentdblink VALUES ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

DROP TABLE IF EXISTS sample_table;
CREATE TABLE `sample_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `int_col` int(11) DEFAULT NULL,
  `tinyint_col` tinyint(4) DEFAULT NULL,
  `smallint_col` smallint(6) DEFAULT NULL,
  `mediumint_col` mediumint(9) DEFAULT NULL,
  `bigint_col` bigint(20) DEFAULT NULL,
  `float_col` float DEFAULT NULL,
  `double_col` double DEFAULT NULL,
  `decimal_col` decimal(10,2) DEFAULT NULL,
  `date_col` date DEFAULT NULL,
  `datetime_col` datetime DEFAULT NULL,
  `timestamp_col` timestamp DEFAULT NULL,
  `time_col` time DEFAULT NULL,
  `year_col` year(4) DEFAULT NULL,
  `char_col` char(10) DEFAULT NULL,
  `varchar_col` varchar(20) DEFAULT NULL,
  `binary_col` binary(10) DEFAULT NULL,
  `varbinary_col` varbinary(20) DEFAULT NULL,
  `tinyblob_col` tinyblob,
  `blob_col` blob,
  `mediumblob_col` mediumblob,
  `longblob_col` longblob,
  `tinytext_col` tinytext,
  `text_col` text,
  `mediumtext_col` mediumtext,
  `longtext_col` longtext,
  `enum_col` enum('value1','value2','value3') DEFAULT NULL,
  `set_col` set('value1','value2','value3') DEFAULT NULL,
  PRIMARY KEY (`id`)
) DEFAULT CHARSET=utf8mb4;

INSERT INTO `sample_table` (`int_col`, `tinyint_col`, `smallint_col`, `mediumint_col`, `bigint_col`, `float_col`, `double_col`, `decimal_col`, `date_col`, `datetime_col`, `timestamp_col`, `time_col`, `year_col`, `char_col`, `varchar_col`, `binary_col`, `varbinary_col`, `tinyblob_col`, `blob_col`, `mediumblob_col`, `longblob_col`, `tinytext_col`, `text_col`, `mediumtext_col`, `longtext_col`, `enum_col`, `set_col`)
VALUES (
  123, -- 整数型
  5, -- 小整数型
  32767, -- 短整数型
  8388607, -- 中等整数型
  9223372036854775807, -- 大整数型
  3.1415926, -- 浮点型
  3.141592653589793238462643383279, -- 双精度浮点型
  12345.67, -- 定点数
  '2021-08-01', -- 日期
  '2021-08-01 12:34:56', -- 日期和时间
  '2022-02-22 22:22:22', -- 时间戳
  '12:34:56', -- 时间
  2021, -- 年份
  '你好', -- 定长字符串，包含中文
  'world', -- 可变长度字符串
  'binary', -- 定长二进制数据
  'varbinary', -- 可变长度二进制数据
  'tinyblob', -- 短二进制数据
  'blob', -- 二进制数据
  'mediumblob', -- 中等二进制数据
  'longblob', -- 长二进制数据
  '你好', -- 短文本，包含中文
  'text', -- 文本
  'mediumtext', -- 中等文本
  'longtext', -- 长文本
  'value2', -- 枚举类型
  'value1,value3' -- 集合类型
);


INSERT INTO sample_table (int_col, float_col, decimal_col, date_col, datetime_col, timestamp_col, time_col, year_col)
VALUES (123, 3.14, 12.34, '2022-02-22', '2022-02-22 22:22:22', '2022-02-22 22:22:22', '22:22:22', 2022);

INSERT INTO sample_table (char_col, varchar_col, binary_col, varbinary_col)
VALUES ('abc', 'abcde', 'a', 'abcdefgh');

INSERT INTO sample_table (char_col, varchar_col, binary_col, varbinary_col)
VALUES ('中文字符', '这是一', '二符', '这是一');

INSERT INTO sample_table (enum_col, set_col)
VALUES ('value1', 'value1,value2');

INSERT INTO sample_table (enum_col, set_col)
VALUES (NULL, 'value2');

INSERT INTO sample_table (tinyblob_col, blob_col, mediumblob_col, longblob_col, tinytext_col, text_col, mediumtext_col, longtext_col)
VALUES ('abc', 'abcdefghijklmnopqrstuvwxyz', 'abc', 'abcdefghijklmnopqrstuvwxyz', 'abc', 'abcdefghijklmnopqrstuvwxyz', 'abc', 'abcdefghijklmnopqrstuvwxyz');

INSERT INTO sample_table (tinyblob_col, blob_col, mediumblob_col, longblob_col, tinytext_col, text_col, mediumtext_col, longtext_col)
VALUES ('这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串', '这是一个很长很长很长的字符串');


DROP TABLE IF EXISTS tmp_hash;
--error 0,1146
drop index idx_b on tmp_hash;
--error 0,1146
drop index idx_c on tmp_hash;
create table tmp_hash(a int , b int, c int, d int) partition by hash(a) partitions 4;
create index idx_b on tmp_hash(b);
create index idx_c on tmp_hash(c);
insert into tmp_hash values (1,1,1,1);
select tmp_hash.b from tmp_hash USE INDEX(idx_b); 

DROP TABLE IF EXISTS temp_fee;
--error 0,1146
ALTER TABLE temp_fee DROP INDEX idx_b, DROP INDEX idx_c;
CREATE TABLE temp_fee (
month_id INT,
prov_id VARCHAR(3),
device_number VARCHAR(40)
) PARTITION BY RANGE (month_id)
(
PARTITION part201606 VALUES LESS THAN (201708),
PARTITION part201607 VALUES LESS THAN (201709),
PARTITION part201608 VALUES LESS THAN (201710),
PARTITION part201609 VALUES LESS THAN (201711)
);
CREATE INDEX idx_b ON temp_fee(month_id);
CREATE INDEX idx_c ON temp_fee(prov_id);

INSERT INTO temp_fee VALUES (201710, '101', '1111');
SELECT month_id FROM temp_fee USE INDEX (idx_b);
SELECT month_id FROM temp_fee USE INDEX (idx_c);


DROP TABLE IF EXISTS temp_qwe;
--error 0,1146
ALTER TABLE temp_qwe DROP INDEX idx_b, DROP INDEX idx_c;
CREATE TABLE temp_qwe (
prov_id INT,
d_number VARCHAR(15),
user_id VARCHAR(15)
) PARTITION BY LIST (prov_id)
(
PARTITION part11 VALUES IN (11),
PARTITION part10 VALUES IN (10),
PARTITION part09 VALUES IN (DEFAULT)
);
INSERT INTO temp_qwe VALUES (8,'101','1111');
INSERT INTO temp_qwe VALUES (9,'101','2222');
INSERT INTO temp_qwe VALUES (10,'101','3333');
INSERT INTO temp_qwe VALUES (11,'101','4444');

DROP TABLE IF EXISTS obdept;

CREATE TABLE obdept(  
    deptno           NUMBER(10,0),  
    dname            VARCHAR(15),  
    loc            VARCHAR(20)
  );

DROP TABLE IF EXISTS outer_join_A;
DROP TABLE IF EXISTS outer_join_B;
DROP TABLE IF EXISTS outer_join_C;

CREATE TABLE outer_join_A (
id INT,
varchar_1 varchar(10),
varchar_2 varchar(10)
);
CREATE TABLE outer_join_B (
id INT,
varchar_1 varchar(10),
varchar_2 varchar(10)
);

CREATE TABLE outer_join_C (
id INT,
varchar_1 varchar(10),
varchar_2 varchar(10)
);
INSERT INTO outer_join_A(id,varchar_1,varchar_2) VALUES(1,'A','a');
INSERT INTO outer_join_A(id,varchar_1,varchar_2) VALUES(2,'B','b');
INSERT INTO outer_join_A(id,varchar_1,varchar_2) VALUES(3,'C','c');
INSERT INTO outer_join_A(id,varchar_1,varchar_2) VALUES(4,'D','d');
INSERT INTO outer_join_A(id,varchar_1,varchar_2) VALUES(5,'E','e');
INSERT INTO outer_join_B(id,varchar_1,varchar_2) VALUES(1,'A','aa');
INSERT INTO outer_join_B(id,varchar_1,varchar_2) VALUES(2,'BB','b');
INSERT INTO outer_join_B(id,varchar_1,varchar_2) VALUES(3,'C','c');
INSERT INTO outer_join_B(id,varchar_1,varchar_2) VALUES(4,'DD','d');
INSERT INTO outer_join_B(id,varchar_1,varchar_2) VALUES(5,'E','e');
INSERT INTO outer_join_C(id,varchar_1,varchar_2) VALUES(1,'A%%','aa');
INSERT INTO outer_join_C(id,varchar_1,varchar_2) VALUES(1,'A','b');
INSERT INTO outer_join_C(id,varchar_1,varchar_2) VALUES(1,'A ','c');
INSERT INTO outer_join_C(id,varchar_1,varchar_2) VALUES(2,'A A','d');
INSERT INTO outer_join_C(id,varchar_1,varchar_2) VALUES(3,'BACD','e');

--enable_query_log
--enable_warnings

SELECT 1;
