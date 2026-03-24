--disable_warnings
drop table if exists t1,t2,t3,t4,t5,t6,t7,t8,t9,t10;
--enable_warnings
--enable_info
--result_format 4

create table t1(c1 int, c2 int, c3 int);
insert into t1 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6);
insert into t1 values(7,7,7),(8,8,8),(9,9,9);
select * from t1;

insert into t1 values((10),10,(10)),((11),11,(11)),((12),12,(12));
insert into t1 values((14),14,(14)),((15),15,(15)),((16),16,(16));
select * from t1;

## 查询 plan_cache_plan_stat表，确定是否走优化
--disable_warnings
select hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t1 %' order by hit_count;
--enable_warnings

INSERT into t1 VALUES(20,20,(20)),(21,21,(21)),(22,22,(22));
INSERT into t1 VALUES(23,23,(23)),(24,24,(24)),(25,25,(25));

select * from t1;

## 查询 plan_cache_plan_stat表，确定是否走优化
--disable_warnings
select hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'INSERT into t1 %' order by hit_count;
--enable_warnings

drop table t1;

create table t2(c1 int primary key, c2 int, c3 varchar(50), c4 date, c5 char(20));
insert into t2 values(1,1,'11','2012-1-1','111'),(2,2,'222','2012-1-2','2222');
insert into t2 values(3,3,'33','2012-1-3','33333'),(4,4,'444','2012-1-4','44444444');

insert into t2 values(5,5,'55',str_to_date('05-05-2020', '%m-%d-%Y'),'5555'),(6,6,'66',str_to_date('06-05-2020', '%m-%d-%Y'),'6666'),(7,7,'77',str_to_date('07-05-2020', '%m-%d-%Y'),'777');
insert into t2 values(8,8,'88',str_to_date('08-05-2020', '%m-%d-%Y'),'8888'),(9,9,'99',str_to_date('09-05-2020', '%m-%d-%Y'),'9999'),(10,10,'1010',str_to_date('10-05-2020', '%m-%d-%Y'),'10101');

select * from t2;
--disable_warnings
select hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t2 %' order by hit_count;
--enable_warnings

drop table t2;

## 回退场景

create table t4(c1 int, c2 int, c3 int);
insert into t4 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);
insert into t4 values(6,6,6),(7,7,7),(8,8,8);
create table t3(c1 int primary key, c2 int, c3 int);
insert into t3 select * from t4;
insert into t3 values(11,11,11),((12),(12),(12)),(13,(13),13);
insert into t3 values((14),14,14),(15,(15),15);
select * from t3;

## 主键冲突了
-- error 1062
insert into t3 values(20,20,20),(21,21,21),(22,22,22),(20,20,20);
select * from t3;
insert into t3 values(20,20,20),(21,21,21),(22,22,22),(23,23,23);
--disable_warnings
select type, hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t3 %' order by hit_count;
--enable_warnings

drop table t3;
drop table t4;

## 测试字符串中有括号等场景
create table t5(c1 int primary key, c2 varchar(50), c3 varchar(50));
insert into t5 values(1, '((aa)()aaabb))(()', '999(()))(9)))'),(2, '((s(d)f)gh', 'hh)))(sd'),(3, 'saddd)))))', '((dadas))');
insert into t5 values(4, '(((dsdd)))', '((((aaa))))'),(5, '(((sdsd)))', '((dkz))');
select * from t5;
--disable_warnings
select hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t5 %' order by hit_count;
--enable_warnings

insert into t5 values (6, 'dkz', 'ddd'), (7,'dsr','dsrdsr'), (8, '99()9', 'dsadds');
insert into t5 values (9, 'dkzdkz', 'dddddd'), (10,'dsrdsr','dsrdsr2'), (11, '99(99)9', 'dsadds2');
insert into t5 value(12,'asd','aa'),(13,'bcd','def'),(14,'kkk','lll');

--disable_warnings
select hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t5 %' order by hit_count;
--enable_warnings

drop table t5;

## 测试带hint的场景
create table t6(c1 int, c2 int, c3 int);
insert /*+PARALLEL(5)*/ into t6 values(1,1,1),(2,2,2);
select * from t6;

--disable_warnings
select hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert % into t6 %' order by hit_count;
--enable_warnings
drop table t6;

## 测试insert select不会被改写
create table t7(c1 int, c2 varchar(20), c3 int);
create table t8(c1 int, c2 varchar(20), c3 int);
insert into t7 values(1, '111', 1), (2, '222', 2), (3, '333', 3);
insert into t8 select c1, c2, c3 from t7;
--disable_warnings
select type, hit_count, is_batched_multi_stmt, statement, query_sql from oceanbase.gv$OB_PLAN_CACHE_PLAN_STAT where statement like 'insert into t8 select %' order by hit_count;
--enable_warnings
drop table t7;
drop table t8;

## issue 
CREATE TABLE t10 (`pk` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY);
INSERT INTO t10 VALUES (NULL),(3);
delete from t10;
INSERT INTO t10 VALUES (NULL),(NULL);
INSERT INTO t10 VALUES (NULL),(NULL);
--error 1062
INSERT INTO t10 VALUES ('1'),('1');
select * from t10;
drop table t10;
