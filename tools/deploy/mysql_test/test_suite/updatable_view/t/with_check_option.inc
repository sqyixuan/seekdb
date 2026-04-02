--error 0, 1008
drop database with_check_option;
create database with_check_option;
use with_check_option;
--echo ######################  insert  ##################################
create table t(c1 int, c2 int);
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
create or replace view v as select * from t with check option;
insert into v values(0, 0);
insert into v values(100, 100);

create or replace view v as select * from t where c1 > 2 with check option;
insert into v values(3, 3);
--error 1369
insert into v values(2, 2);
insert into v select 3, 3 from dual;
--error 1369
insert into v select 2, 2 from dual;

create or replace view v as select * from t where c1 + c2 > 5 with check option;
insert into v values(3, 3);
--error 1369
insert into v values(2, 2);
insert into v select 3, 3 from dual;
--error 1369
insert into v select 2, 2 from dual;


## alias name
create or replace view v as select * from t A where c1 > 2 with check option;
insert into v values(3, 3);
--error 1369
insert into v values(2, 2);

##subquery
create or replace view v as select * from (select * from t) A where c1 > 2 with check option;
--error 1471
insert into v values(3, 3);

## always failed
create or replace view v as select * from t where 1 > 2 with check option;
--error 1369
insert into v values(0, 0);
--error 1369
insert into v values(100, 100);



--echo ######################  update  ##################################
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
create or replace view v as select * from t with check option;
update v set c2 = c2 + 1;

create or replace view v as select * from t where c2 < 3 with check option;
select * from t;
--error 1369
update v set c2 = c2 + 1;
select * from t;
update v set c2 = c2 - 1;

create or replace view v as select * from t where c1 + c2 < 5 with check option;
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
--error 1369
update v set c2 = c2 + 1;
update v set c2 = c2 - 1;
select * from t;

## alias name
create or replace view v as select * from t A where c2 < 3 with check option;
--error 1369
update v set c2 = 3;
update v set c2 = 2;

##subquery, not updatable in mysql mode.
--error 1288
update (select * from t) set c2 = 3;
--error 1288
update (select * from t) A set c2 = 2;
--error 1288
update (select * from v) set c2 = 3;
--error 1288
update (select * from v) A set c2 = 3;
--error 1288
update (select * from v A) set c2 = 3;
create or replace view v as select * from (select * from t) A where c2 > 1 with check option;
update v set c2 = 3;
--error 1369
update v set c2 = 0;



--echo ######################  replace  #################################
delete from t;
replace into t values(1, 1);
replace into t values(2, 2);
replace into t values(3, 3);
create or replace view v as select * from t with check option;
replace into v values(0, 0);
replace into v values(100, 100);

create or replace view v as select * from t where c2 > 2 with check option;
replace into v values(3, 3);
--error 1369
replace into v values(2, 2);
replace into v select 3, 3;
--error 1369
replace into v select 2, 2;

create or replace view v as select * from t where c1 + c2 > 5 with check option;
replace into v values(3, 3);
--error 1369
replace into v values(2, 2);
replace into v select 3, 3;
--error 1369
replace into v select 2, 2;

## alias name
create or replace view v as select * from t A where c2 > 2 with check option;
replace into v values(3, 3);
--error 1369
replace into v values(2, 2);

##subquery
create or replace view v as select * from (select * from t) A where c2 > 2 with check option;
--error 1471
replace into v values(3, 3);

## always failed
create or replace view v as select * from t where 1 > 2 with check option;
--error 1369
replace into v values(0, 0);
--error 1369
replace into v values(100, 100);



--echo ######################  generated column  ##############################
drop table t;
create table t(c1 int, c2 int, c3 int generated always as (c1 + c2));
insert into t(c1, c2) values(1, 1);
insert into t(c1, c2) values(2, 2);
insert into t(c1, c2) values(3, 3);
create or replace view v as select * from t where c3 > 3 with check option;
--error 1369
insert into v(c1, c2) values(1, 1);
insert into v(c1, c2) values(2, 2);
--error 1369
update v set c2 = 0;
update v set c2 = 3;
--error 1369
replace into v(c1, c2) values(1, 1);
replace into v(c1, c2) values(2, 2);



--echo ######################  nested view  ###############################
create table t1(a int);
insert into t1 values(0), (1), (2), (3);
CREATE OR REPLACE VIEW v1 AS SELECT * FROM t1 WHERE a < 3;
CREATE OR REPLACE VIEW v2 AS SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION;

CREATE OR REPLACE VIEW v1_1 AS SELECT * FROM v1 WHERE a > 0 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v1_2 AS SELECT * FROM v1 WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3 AS SELECT * FROM v1 WHERE a > 0;
--error 1369
insert into v1_1 values(-1);
insert into v1_1 values(1);
insert into v1_1 values(3);

--error 1369
insert into v1_2 values(-1);
insert into v1_2 values(1);
--error 1369
insert into v1_2 values(3);

insert into v1_3 values(-1);
insert into v1_3 values(1);
insert into v1_3 values(3);

CREATE OR REPLACE VIEW v1_1_1 AS SELECT * FROM v1_1 WHERE a != 1 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v1_1_2 AS SELECT * FROM v1_1 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_1_3 AS SELECT * FROM v1_1 WHERE a != 1;
--error 1369
insert into v1_1_1 values(-1);
--error 1369
insert into v1_1_1 values(1);
insert into v1_1_1 values(3);

--error 1369
insert into v1_1_2 values(-1);
--error 1369
insert into v1_1_2 values(1);
--error 1369
insert into v1_1_2 values(3);

--error 1369
insert into v1_1_3 values(-1);
insert into v1_1_3 values(1);
insert into v1_1_3 values(3);


CREATE OR REPLACE VIEW v1_2_1 AS SELECT * FROM v1_2 WHERE a != 1 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v1_2_2 AS SELECT * FROM v1_2 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_2_3 AS SELECT * FROM v1_2 WHERE a != 1;
--error 1369
insert into v1_2_1 values(-1);
--error 1369
insert into v1_2_1 values(1);
--error 1369
insert into v1_2_1 values(3);

--error 1369
insert into v1_2_2 values(-1);
--error 1369
insert into v1_2_2 values(1);
--error 1369
insert into v1_2_2 values(3);

--error 1369
insert into v1_2_3 values(-1);
insert into v1_2_3 values(1);
--error 1369
insert into v1_2_3 values(3);


CREATE OR REPLACE VIEW v1_3_1 AS SELECT * FROM v1_3 WHERE a != 1 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v1_3_2 AS SELECT * FROM v1_3 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3_3 AS SELECT * FROM v1_3 WHERE a != 1;
insert into v1_3_1 values(-1);
--error 1369
insert into v1_3_1 values(1);
insert into v1_3_1 values(3);

--error 1369
insert into v1_3_2 values(-1);
--error 1369
insert into v1_3_2 values(1);
--error 1369
insert into v1_3_2 values(3);

insert into v1_3_3 values(-1);
insert into v1_3_3 values(1);
insert into v1_3_3 values(3);


CREATE OR REPLACE VIEW v2_1 AS SELECT * FROM v2 WHERE a > 0 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v2_2 AS SELECT * FROM v2 WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3 AS SELECT * FROM v2 WHERE a > 0;
--error 1369
insert into v2_1 values(-1);
insert into v2_1 values(1);
--error 1369
insert into v2_1 values(3);

--error 1369
insert into v2_2 values(-1);
insert into v2_2 values(1);
--error 1369
insert into v2_2 values(3);

insert into v2_3 values(-1);
insert into v2_3 values(1);
--error 1369
insert into v2_3 values(3);


CREATE OR REPLACE VIEW v2_1_1 AS SELECT * FROM v2_1 WHERE a != 1 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v2_1_2 AS SELECT * FROM v2_1 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_1_3 AS SELECT * FROM v2_1 WHERE a != 1;
--error 1369
insert into v2_1_1 values(-1);
--error 1369
insert into v2_1_1 values(1);
--error 1369
insert into v2_1_1 values(3);

--error 1369
insert into v2_1_2 values(-1);
--error 1369
insert into v2_1_2 values(1);
--error 1369
insert into v2_1_2 values(3);

--error 1369
insert into v2_1_3 values(-1);
insert into v2_1_3 values(1);
--error 1369
insert into v2_1_3 values(3);


CREATE OR REPLACE VIEW v2_2_1 AS SELECT * FROM v2_2 WHERE a != 1 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v2_2_2 AS SELECT * FROM v2_2 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_2_3 AS SELECT * FROM v2_2 WHERE a != 1;
--error 1369
insert into v2_2_1 values(-1);
--error 1369
insert into v2_2_1 values(1);
--error 1369
insert into v2_2_1 values(3);

--error 1369
insert into v2_2_2 values(-1);
--error 1369
insert into v2_2_2 values(1);
--error 1369
insert into v2_2_2 values(3);

--error 1369
insert into v2_2_3 values(-1);
insert into v2_2_3 values(1);
--error 1369
insert into v2_2_3 values(3);


CREATE OR REPLACE VIEW v2_3_1 AS SELECT * FROM v2_3 WHERE a != 1 WITH LOCAL CHECK OPTION;
CREATE OR REPLACE VIEW v2_3_2 AS SELECT * FROM v2_3 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3_3 AS SELECT * FROM v2_3 WHERE a != 1;
insert into v2_3_1 values(-1);
--error 1369
insert into v2_3_1 values(1);
--error 1369
insert into v2_3_1 values(3);

--error 1369
insert into v2_3_2 values(-1);
--error 1369
insert into v2_3_2 values(1);
--error 1369
insert into v2_3_2 values(3);

insert into v2_3_3 values(-1);
insert into v2_3_3 values(1);
--error 1369
insert into v2_3_3 values(3);



--echo ######################  non-updatable view  ############################
drop table t1;
create table t1(c1 int, c2 int);
create table t2(c1 int, c2 int);
insert into t1 values(2, 2);
--error 1368
create or replace view v1 as select max(c1) from t1 with check option;
## 预期报错1368， group by后面的with rollup与with check option会冲突，解这个冲突太麻烦了
--error 1064
create or replace view v1 as select c1 from t1 group by c1 with check option;
--error 1368
create or replace view v1 as select * from (select c1 from t1 group by c1) with check option;
--error 1368
create or replace view v1 as select c1 from t1 having c1 > 0 with check option;
--error 1368
create or replace view v1 as select distinct c1 from t1 with check option;
--error 1368
create or replace view v1 as select c1 from t1 union select c1 from t1 with check option;
--error 1368
create or replace view v1 as select c1 from t1 limit 5 with check option;
--error 4179
create or replace view v1 as select t1.c1, t2.c2 from t1, t2 where t1.c1 = t2.c1 with check option;
## where condition
--error 4179
create or replace view v1 as select c1 from t1 where c1 = (select max(c2) from t1) with check option;

## nested view. basic view is not updatable.
create or replace view v1 as select max(c1) from t1;
--error 1368
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select distinct c1 from t1;
--error 1368
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select c1 from t1 union select c1 from t1;
--error 1368
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select c1 from t1 limit 5;
--error 1368
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select t1.c1, t2.c2 from t1, t2 where t1.c1 = t2.c1;
--error 4179
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select c1 from t1 where c1 = (select max(c2) from t1);
--error 4179
create or replace view v2 as select * from v1 with check option;
drop table t1;


--echo ######################  partition table and index ############################
create table t1(c1 int, c2 int, c3 int generated always as (c1 + c2)) partition by hash(c1) partitions 3;
create or replace view v1 as select * from t1 where c1 > 0 and c3 < 5 with check option;
insert into v1(c1, c2) values(1, 2);
--error 1369
insert into v1(c1, c2) values(0, 3);
--error 1369
insert into v1(c1, c2) values(1, 4);

replace into v1(c1, c2) values(1, 2);
--error 1369
replace into v1(c1, c2) values(0, 3);
--error 1369
replace into v1(c1, c2) values(1, 4);

--error 1369
update v1 set c1 = 0;
update v1 set c1 = 2;
--error 1369
update v1 set c1 = 3;
select * from v1;


drop table t1;
create table t1(c1 int, c2 int, c3 int generated always as (c1 + c2));
create index idx_t1_c1 on t1(c1);
create index idx_t1_c2 on t1(c2);
create index idx_t1_c3 on t1(c3);
create or replace view v1 as select * from t1 where c1 > 0 and c3 < 5 with check option;
insert into v1(c1, c2) values(1, 2);
--error 1369
insert into v1(c1, c2) values(0, 3);
--error 1369
insert into v1(c1, c2) values(1, 4);

replace into v1(c1, c2) values(1, 2);
--error 1369
replace into v1(c1, c2) values(0, 3);
--error 1369
replace into v1(c1, c2) values(1, 4);

--error 1369
update v1 set c1 = 0;
update v1 set c1 = 2;
--error 1369
update v1 set c1 = 3;
select * from v1;

create table test1(a1 int primary key AUTO_INCREMENT,name varchar(20),chinese BIGINT,math BIGINT,english INTEGER);
insert into test1(name,chinese,math,english) values('小明',101,115,113);
insert into test1(name,chinese,math,english) values('小王',103,125,115);
insert into test1(name,chinese,math,english) values('小超',107,135,108);
insert into test1(name,chinese,math,english) values('小李',119,145,100);
create or REPLACE view v1 as SELECT * from test1 where chinese<=121 and math>=100 and english<120 WITH CHECK OPTION;
SELECT * from test1 order by a1 asc;
SELECT * from v1 order by a1 asc;
--error 1369
update v1 set english=english*sin(asin(81));

create table test3(id int,chinese BIGINT,math BIGINT,english INTEGER);
create or REPLACE view v3 as SELECT * from test3 where chinese<=10 and math>=15 and english<30 WITH CHECK OPTION;
--error 1369
replace into v3 values(4,SIN(60)+cos(25),ASIN(30)+ACOS(20),TAN(1)+ATAN(2));

drop database with_check_option;