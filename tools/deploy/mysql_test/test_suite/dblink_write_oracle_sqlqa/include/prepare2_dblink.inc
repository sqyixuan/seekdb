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

--echo #sub-1	基本功能测试
--error 0,942
drop table test1 purge;
--error 0,942
drop table test2 purge;
--error 0,942
drop table test3 purge;
create table test1 (f1 int, f2 char(10), f3 varchar2(10));
create table test2 (f1 int, f2 char(10), f3 varchar2(10));
create table test3 (f1 int);


--echo #向表中指定分区插入数据
--error 0,942
drop table test22 purge;
create table test22(f1 int, f2 char(10), f3 varchar2(10) default 'admin')
partition by range(f1) (partition p1 values less than(5), partition p2 values less than (maxvalue));

--error 0,942
drop table test111 purge;
--error 0,942
drop table test222 purge;
create table test111 (f1 int, f2 char(10), f3 varchar2(10));
create table test222 (f1 int, f2 char(10), f3 varchar2(10));

--error 0,942
drop table test333 purge;
--error 0,942
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

--error 0,942
drop table test555 purge;
--error 0,942
drop table test666 purge;
--error 0,942
drop table test777 purge;

create table test555 (id int primary key);
INSERT INTO test555 VALUES(1001);
INSERT INTO test555 VALUES(1002);
INSERT INTO test555 VALUES(1003);

create table test666 (
id int primary key, 
name char(10) unique, 
name_1 nvarchar2(10) not null, 
birthday DATE DEFAULT to_date('20000101','yyyymmdd'), 
age number(3,0) CHECK(age>0 AND age<150),
p_id int,
foreign KEY(p_id) REFERENCES test555(id));
INSERT INTO test666 VALUES(1,'stu1','aaa',to_date('19700101','yyyymmdd'),18,1001);
INSERT INTO test666 VALUES(2,'stu2','bbb',to_date('19800101','yyyymmdd'),28,1001);
INSERT INTO test666 VALUES(3,'stu3','ccc',to_date('19900101','yyyymmdd'),38,1002);
INSERT INTO test666 VALUES(4,'stu4','ddd',to_date('20000101','yyyymmdd'),48,1002);
INSERT INTO test666 VALUES(5,'stu5','eee',to_date('20100101','yyyymmdd'),58,1003);
INSERT INTO test666 VALUES(6,NULL,'fff',to_date('20200101','yyyymmdd'),68,1003);

create table test777 (
id int primary key, 
name char(10) unique, 
name_1 nvarchar2(10) not null, 
birthday DATE DEFAULT to_date('20000101','yyyymmdd'), 
age number(3,0) CHECK(age>0 AND age<150),
p_id int,
foreign KEY(p_id) REFERENCES test555(id));


--error 0,942
DROP TABLE t_test1 purge;
--error 0,942
DROP TABLE t_test2 purge;
--error 0,942
DROP TABLE t_test3 purge;
CREATE TABLE t_test1(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int)
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE INDEX idx_11111 ON t_test1(id) GLOBAL;
CREATE INDEX idx_12111 ON t_test1(name) GLOBAL;
INSERT INTO t_test1 VALUES(101, 't_test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO t_test1 VALUES(102, 't_test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO t_test1 VALUES(103, 't_test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO t_test1 VALUES(104, 't_test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO t_test1 VALUES(105, 't_test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO t_test1 VALUES(106, 't_test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO t_test1 VALUES(107, 't_test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO t_test1 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE t_test2(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int)
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE INDEX idx_21111 ON t_test2(id) GLOBAL;
CREATE INDEX idx_22111 ON t_test2(name) GLOBAL;
INSERT INTO t_test2 VALUES(101, 't_test1', 'admin1', to_date('19700101','yyyymmdd'), 50.5, NULL);
INSERT INTO t_test2 VALUES(102, 't_test2', 'admin2', to_date('19800101','yyyymmdd'), 55.5, 101);
INSERT INTO t_test2 VALUES(103, 't_test3', 'admin3', to_date('19900101','yyyymmdd'), 60.5, 101);
INSERT INTO t_test2 VALUES(104, 't_test4', 'admin4', to_date('20000101','yyyymmdd'), 65.5, 102);
INSERT INTO t_test2 VALUES(105, 't_test5', 'admin5', to_date('20100101','yyyymmdd'), 70.5, 102);
INSERT INTO t_test2 VALUES(106, 't_test6', 'admin6', to_date('20200101','yyyymmdd'), 75.5, 103);
INSERT INTO t_test2 VALUES(107, 't_test7', 'admin7', to_date('20300101','yyyymmdd'), 80.5, 103);
INSERT INTO t_test2 VALUES(108, NULL, NULL, to_date('20400101','yyyymmdd'), NULL, NULL);

CREATE TABLE t_test3(id int, name nchar(20), name_1 varchar2(20), birthday DATE, weight binary_float, p_id int)
PARTITION BY RANGE(birthday) 
(PARTITION p1 VALUES less than(to_date('19900101','yyyymmdd')), PARTITION p2 VALUES less than(to_date('20100101','yyyymmdd')), PARTITION p3 VALUES less than(MAXVALUE));
CREATE INDEX idx_31111 ON t_test3(id) GLOBAL;
CREATE INDEX idx_32111 ON t_test3(name) GLOBAL;

--error 2289
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

COMMIT;