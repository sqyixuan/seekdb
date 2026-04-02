--echo ######################  insert  ##################################
--error 0, 942
drop table t;
create table t(c1 int, c2 int);
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
create or replace view v as select * from t with check option;
insert into v values(0, 0);
insert into (select * from t with check option) values(0, 0);
insert into v values(100, 100);
insert into (select * from t with check option) values(100, 100);

create or replace view v as select * from t where c1 > 2 with check option;
insert into v values(3, 3);
insert into (select * from t where c1 > 2 with check option) values(3, 3);
--error 1402
insert into v values(2, 2);
--error 1402
insert into (select * from t where c1 > 2 with check option) values(2, 2);
insert into v select 3, 3 from dual;
insert into (select * from t where c1 > 2 with check option) select 3, 3 from dual;
--error 1402
insert into v select 2, 2 from dual;
--error 1402
insert into (select * from t where c1 > 2 with check option) select 2, 2 from dual;

create or replace view v as select * from t where c1 + c2 > 5 with check option;
insert into v values(3, 3);
insert into (select * from t where c1 + c2 > 5 with check option) values(3, 3);
--error 1402
insert into v values(2, 2);
--error 1402
insert into (select * from t where c1 + c2 > 5 with check option) values(2, 2);
insert into v select 3, 3 from dual;
--error 1402
insert into v select 2, 2 from dual;
--error 1402
insert into (select * from t where c1 + c2 > 5 with check option) select 2, 2 from dual;


## alias name
create or replace view v as select * from t A where c1 > 2 with check option;
insert into v values(3, 3);
insert into (select * from t A where c1 > 2 with check option) values(3, 3);
--error 1402
insert into v values(2, 2);
--error 1402
insert into (select * from t A where c1 > 2 with check option) values(2, 2);

##subquery
create or replace view v as select * from (select * from t) A where c1 > 2 with check option;
insert into v values(3, 3);
insert into (select * from (select * from t) A where c1 > 2 with check option) values(3, 3);
--error 1402
insert into v values(2, 2);
--error 1402
insert into (select * from (select * from t) A where c1 > 2 with check option) values(2, 2);


## always failed
create or replace view v as select * from t where 1 > 2 with check option;
--error 1402
insert into v values(0, 0);
--error 1402
insert into (select * from t where 1 > 2 with check option) values(0, 0);
--error 1402
insert into v values(100, 100);
--error 1402
insert into (select * from t where 1 > 2 with check option) values(100, 100);



--echo ######################  update  ##################################
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
create or replace view v as select * from t with check option;
update v set c2 = c2 + 1;
update (select * from t with check option) set c2 = c2 + 1;

delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
create or replace view v as select * from t where c2 < 3 with check option;
--error 1402
update v set c2 = c2 + 1;
--error 1402
update (select * from t where c2 < 3 with check option) set c2 = c2 + 1;
select * from t;
update v set c2 = c2 - 1;
update (select * from t where c2 < 3 with check option) set c2 = c2 - 1;
select * from t;

create or replace view v as select * from t where c1 + c2 < 5 with check option;
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
--error 1402
update v set c2 = c2 + 1;
--error 1402
update (select * from t where c1 + c2 < 5 with check option) set c2 = c2 + 1;
update v set c2 = c2 - 1;
update (select * from t where c1 + c2 < 5 with check option) set c2 = c2 - 1;
select * from t;

## alias name
create or replace view v as select * from t A where c2 < 3 with check option;
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
--error 1402
update v set c2 = 3;
--error 1402
update (select * from t A where c2 < 3 with check option) set c2 = 3;
update v set c2 = 2;
update (select * from t A where c2 < 3 with check option) set c2 = 2;


##subquery
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);
update (select * from t) set c2 = 3;
update (select * from t) A set c2 = 3;
update (select * from t) A set c2 = 2;
update (select * from v) set c2 = 1;
update (select * from (select * from t A where c2 < 3 with check option)) set c2 = 2;
update (select * from v where c2 > 1) set c2 = 1;
--error 1402
update (select * from v where c2 > 0 with check option) set c2 = 0;
--error 1402
update (select * from v where c2 > 0 with check option) set c2 = 10;
--error 1402
update (select * from v) set c2 = 3;
--error 1402
update (select * from v) A set c2 = 3;
--error 1402
update (select * from (select * from t A where c2 < 3 with check option)) A set c2 = 3;
update (select * from v A) set c2 = 2;
--error 1402
update (select * from v A) set c2 = 3;
--error 1402
update (select * from (select * from t A where c2 < 3 with check option) A) set c2 = 3;
create or replace view v as select * from (select * from t) A where c2 > 1 with check option;
update v set c2 = 3;
--error 1402
update v set c2 = 0;
--error 1402
update (select * from (select * from t) A where c2 > 1 with check option) set c2 = 0;

create or replace view v as select * from t A where c2 < 3;
delete from t;
insert into t values(1, 1);
insert into t values(2, 2);
insert into t values(3, 3);



--echo ######################  generated column  ##############################
drop table t;
create table t(c1 int, c2 int, c3 int generated always as (c1 + c2));
insert into t(c1, c2) values(1, 1);
insert into t(c1, c2) values(2, 2);
insert into t(c1, c2) values(3, 3);
create or replace view v as select * from t where c3 > 3 with check option;
--error 1402
insert into v(c1, c2) values(1, 1);
insert into v(c1, c2) values(2, 2);
--error 1402
update v set c2 = 0;
update v set c2 = 3;



--echo ######################  nested view  ###############################
--error 0, 942
drop table t1;
create table t1(a int);
insert into t1 values(0), (1), (2), (3);
CREATE OR REPLACE VIEW v1 AS SELECT * FROM t1 WHERE a < 3;
CREATE OR REPLACE VIEW v2 AS SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION;

CREATE OR REPLACE VIEW v1_2 AS SELECT * FROM v1 WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3 AS SELECT * FROM v1 WHERE a > 0;

--error 1402
insert into v1_2 values(-1);
insert into v1_2 values(1);
--error 1402
insert into v1_2 values(3);

insert into v1_3 values(-1);
insert into v1_3 values(1);
insert into v1_3 values(3);


CREATE OR REPLACE VIEW v1_2_2 AS SELECT * FROM v1_2 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_2_3 AS SELECT * FROM v1_2 WHERE a != 1;
--error 1402
insert into v1_2_2 values(-1);
--error 1402
insert into v1_2_2 values(1);
--error 1402
insert into v1_2_2 values(3);

--error 1402
insert into v1_2_3 values(-1);
insert into v1_2_3 values(1);
--error 1402
insert into v1_2_3 values(3);


CREATE OR REPLACE VIEW v1_3_2 AS SELECT * FROM v1_3 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3_3 AS SELECT * FROM v1_3 WHERE a != 1;
--error 1402
insert into v1_3_2 values(-1);
--error 1402
insert into v1_3_2 values(1);
--error 1402
insert into v1_3_2 values(3);

insert into v1_3_3 values(-1);
insert into v1_3_3 values(1);
insert into v1_3_3 values(3);


CREATE OR REPLACE VIEW v2_2 AS SELECT * FROM v2 WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3 AS SELECT * FROM v2 WHERE a > 0;
--error 1402
insert into v2_2 values(-1);
insert into v2_2 values(1);
--error 1402
insert into v2_2 values(3);

insert into v2_3 values(-1);
insert into v2_3 values(1);
--error 1402
insert into v2_3 values(3);


CREATE OR REPLACE VIEW v2_2_2 AS SELECT * FROM v2_2 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_2_3 AS SELECT * FROM v2_2 WHERE a != 1;
--error 1402
insert into v2_2_2 values(-1);
--error 1402
insert into v2_2_2 values(1);
--error 1402
insert into v2_2_2 values(3);

--error 1402
insert into v2_2_3 values(-1);
insert into v2_2_3 values(1);
--error 1402
insert into v2_2_3 values(3);


CREATE OR REPLACE VIEW v2_3_2 AS SELECT * FROM v2_3 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3_3 AS SELECT * FROM v2_3 WHERE a != 1;
--error 1402
insert into v2_3_2 values(-1);
--error 1402
insert into v2_3_2 values(1);
--error 1402
insert into v2_3_2 values(3);

insert into v2_3_3 values(-1);
insert into v2_3_3 values(1);
--error 1402
insert into v2_3_3 values(3);



--echo #################  nested view and subquery (subquery contains view) #######################
drop table t1;
create table t1(a int);
insert into t1 values(0), (1), (2), (3);
CREATE OR REPLACE VIEW v1 AS SELECT * FROM t1 WHERE a < 3;
CREATE OR REPLACE VIEW v2 AS SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION;

CREATE OR REPLACE VIEW v1_2 AS SELECT * FROM v1 WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3 AS SELECT * FROM v1 WHERE a > 0;

--error 1402
insert into (SELECT * FROM v1 WHERE a > 0 WITH CHECK OPTION) values(-1);
insert into (SELECT * FROM v1 WHERE a > 0 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM v1 WHERE a > 0 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM v1 WHERE a > 0) values(-1);
insert into (SELECT * FROM v1 WHERE a > 0) values(1);
insert into (SELECT * FROM v1 WHERE a > 0) values(3);


--error 1402
insert into (SELECT * FROM v1_2 WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM v1_2 WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM v1_2 WHERE a != 1 WITH CHECK OPTION) values(3);

--error 1402
insert into (SELECT * FROM v1_2 WHERE a != 1) values(-1);
insert into (SELECT * FROM v1_2 WHERE a != 1) values(1);
--error 1402
insert into (SELECT * FROM v1_2 WHERE a != 1) values(3);


--error 1402
insert into (SELECT * FROM v1_3 WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM v1_3 WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM v1_3 WHERE a != 1 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM v1_3 WHERE a != 1) values(-1);
insert into (SELECT * FROM v1_3 WHERE a != 1) values(1);
insert into (SELECT * FROM v1_3 WHERE a != 1) values(3);


CREATE OR REPLACE VIEW v2_2 AS SELECT * FROM v2 WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3 AS SELECT * FROM v2 WHERE a > 0;
--error 1402
insert into (SELECT * FROM v2 WHERE a > 0 WITH CHECK OPTION) values(-1);
insert into (SELECT * FROM v2 WHERE a > 0 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM v2 WHERE a > 0 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM v2 WHERE a > 0) values(-1);
insert into (SELECT * FROM v2 WHERE a > 0) values(1);
--error 1402
insert into (SELECT * FROM v2 WHERE a > 0) values(3);


--error 1402
insert into (SELECT * FROM v2_2 WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM v2_2 WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM v2_2 WHERE a != 1 WITH CHECK OPTION) values(3);

--error 1402
insert into (SELECT * FROM v2_2 WHERE a != 1) values(-1);
insert into (SELECT * FROM v2_2 WHERE a != 1) values(1);
--error 1402
insert into (SELECT * FROM v2_2 WHERE a != 1) values(3);


CREATE OR REPLACE VIEW v2_3_2 AS SELECT * FROM v2_3 WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3_3 AS SELECT * FROM v2_3 WHERE a != 1;
--error 1402
insert into (SELECT * FROM v2_3 WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM v2_3 WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM v2_3 WHERE a != 1 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM v2_3 WHERE a != 1) values(-1);
insert into (SELECT * FROM v2_3 WHERE a != 1) values(1);
--error 1402
insert into (SELECT * FROM v2_3 WHERE a != 1) values(3);




--echo #################  nested view and subquery (view contains subquery) #######################
drop table t1;
create table t1(a int);
insert into t1 values(0), (1), (2), (3);
CREATE OR REPLACE VIEW v1_2 AS SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3 AS SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0;

--error 1402
insert into v1_2 values(-1);
insert into v1_2 values(1);
--error 1402
insert into v1_2 values(3);

insert into v1_3 values(-1);
insert into v1_3 values(1);
insert into v1_3 values(3);


CREATE OR REPLACE VIEW v1_2_2 AS SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_2_3 AS SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1;
--error 1402
insert into v1_2_2 values(-1);
--error 1402
insert into v1_2_2 values(1);
--error 1402
insert into v1_2_2 values(3);

--error 1402
insert into v1_2_3 values(-1);
insert into v1_2_3 values(1);
--error 1402
insert into v1_2_3 values(3);


CREATE OR REPLACE VIEW v1_3_2 AS SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3_3 AS SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1;
--error 1402
insert into v1_3_2 values(-1);
--error 1402
insert into v1_3_2 values(1);
--error 1402
insert into v1_3_2 values(3);

insert into v1_3_3 values(-1);
insert into v1_3_3 values(1);
insert into v1_3_3 values(3);


CREATE OR REPLACE VIEW v2_2 AS SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3 AS SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0;
--error 1402
insert into v2_2 values(-1);
insert into v2_2 values(1);
--error 1402
insert into v2_2 values(3);

insert into v2_3 values(-1);
insert into v2_3 values(1);
--error 1402
insert into v2_3 values(3);


CREATE OR REPLACE VIEW v2_2_2 AS SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_2_3 AS SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1;
--error 1402
insert into v2_2_2 values(-1);
--error 1402
insert into v2_2_2 values(1);
--error 1402
insert into v2_2_2 values(3);

--error 1402
insert into v2_2_3 values(-1);
insert into v2_2_3 values(1);
--error 1402
insert into v2_2_3 values(3);


CREATE OR REPLACE VIEW v2_3_2 AS SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v2_3_3 AS SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1;
--error 1402
insert into v2_3_2 values(-1);
--error 1402
insert into v2_3_2 values(1);
--error 1402
insert into v2_3_2 values(3);

insert into v2_3_3 values(-1);
insert into v2_3_3 values(1);
--error 1402
insert into v2_3_3 values(3);




--echo ################################  nested subquery #################################
drop table t1;
create table t1(a int);
insert into t1 values(0), (1), (2), (3);
CREATE OR REPLACE VIEW v1_2 AS SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION;
CREATE OR REPLACE VIEW v1_3 AS SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0;

--error 1402
insert into (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) values(-1);
insert into (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) values(-1);
insert into (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) values(1);
insert into (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) values(3);


--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION) values(3);

--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1) values(-1);
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1) values(3);


--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1) values(-1);
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1) values(1);
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a < 3) WHERE a > 0) WHERE a != 1) values(3);


--error 1402
insert into (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) values(-1);
insert into (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) values(-1);
insert into (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) values(3);


--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1 WITH CHECK OPTION) values(3);

--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1) values(-1);
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0 WITH CHECK OPTION) WHERE a != 1) values(3);


--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION) values(-1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1 WITH CHECK OPTION) values(3);

insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1) values(-1);
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1) values(1);
--error 1402
insert into (SELECT * FROM (SELECT * FROM (SELECT * FROM v1 WHERE a < 3 WITH CHECK OPTION) WHERE a > 0) WHERE a != 1) values(3);



--echo ######################  non-updatable view  ############################
drop table t1;
--error 0, 942
drop table t2;
create table t1(c1 int, c2 int);
create table t2(c1 int, c2 int);
insert into t1 values(2, 2);

create or replace view v1 as select max(c1) c1 from t1 with check option;
--error 1733
insert into v1 values(1);

create or replace view v1(c1, c2) as select distinct c1, c2 from t1 with check option;
--error 1732
insert into v1 values(1, 2);

create or replace view v1(c1, c2) as select c1, c2 from t1 union select c1, c2 from t1 with check option;
--error 1732
insert into v1 values(1, 2);

create or replace view v1 as select c1, c2 from t1 where rownum < 5 with check option;
--error 1732
insert into v1 values(1, 2);

--error 600
create or replace view v1 as select t1.c1, t2.c2 from t1, t2 where t1.c1 = t2.c1 with check option;
--error 600
create or replace view v1 as select c1, c2 from t1 union all select t1.c1, t2.c2 from t1, t2 with check option;
--error 600
create or replace view v1 as select t1.c1, t2.c2 from t1, t2 union all select c1, c2 from t1 with check option;
create or replace view v1 as select t1.c1, t2.c2 from t1, t2 union all select * from (select c1, c2 from t1 with check option);
--error 600
create or replace view v1 as select c1 from t1 where c1 = (select max(c2) from t1) with check option;


## nested view. basic view is not updatable.
create or replace view v1 as select max(c1) c1 from t1 with check option;
create or replace view v2 as select * from v1 with check option;
--error 1733
insert into v1 values(1);

create or replace view v1(c1, c2) as select distinct c1, c2 from t1 with check option;
create or replace view v2 as select * from v1 with check option;
--error 1732
insert into v1 values(1, 2);

create or replace view v1(c1, c2) as select c1, c2 from t1 union select c1, c2 from t1 with check option;
create or replace view v2 as select * from v1 with check option;
--error 1732
insert into v1 values(1, 2);

create or replace view v1 as select c1, c2 from t1 where rownum < 5 with check option;
create or replace view v2 as select * from v1 with check option;
--error 1732
insert into v1 values(1, 2);


create or replace view v1 as select t1.c1, t2.c2 from t1, t2 where t1.c1 = t2.c1;
--error 600
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select c1, c2 from t1 union all select t1.c1, t2.c2 from t1, t2;
--error 600
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select t1.c1, t2.c2 from t1, t2 union all select c1, c2 from t1;
--error 600
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select t1.c1, t2.c2 from t1, t2 union all select * from (select c1, c2 from t1);
--error 600
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select c1 from t1 where c1 = (select max(c2) from t1);
--error 600
create or replace view v2 as select * from v1 with check option;


create or replace view v1 as select t1.c1, t2.c2 from t1, t2 where t1.c1 = t2.c1;
--error 600
create or replace view v2 as select * from v1 with check option;

create or replace view v1 as select c1 from t1 where c1 = (select max(c2) from t1);
--error 600
create or replace view v2 as select * from v1 with check option;
drop table t1;


--echo ######################  partition table and index ############################
create table t1(c1 int, c2 int, c3 int generated always as (c1 + c2)) partition by hash(c1) partitions 3;
alter table t1 enable row movement;
create or replace view v1 as select * from t1 where c1 > 0 and c3 < 5 with check option;
insert into v1(c1, c2) values(1, 2);
--error 1402
insert into v1(c1, c2) values(0, 3);
--error 1402
insert into v1(c1, c2) values(1, 4);

--error 1402
update v1 set c1 = 0;
--error 0, 14402
update v1 set c1 = 2;
--error 1402
update v1 set c1 = 3;
select * from v1;


drop table t1;
create table t1(c1 int, c2 int, c3 int generated always as (c1 + c2));
create index idx_t1_c1 on t1(c1);
create index idx_t1_c2 on t1(c2);
create index idx_t1_c3 on t1(c3);
create or replace view v1 as select * from t1 where c1 > 0 and c3 < 5 with check option;
insert into v1(c1, c2) values(1, 2);
--error 1402
insert into v1(c1, c2) values(0, 3);
--error 1402
insert into v1(c1, c2) values(1, 4);

--error 1402
update v1 set c1 = 0;
update v1 set c1 = 2;
--error 1402
update v1 set c1 = 3;
select * from v1;
