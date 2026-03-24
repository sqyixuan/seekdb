--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings
create table t1(a int primary key, b int) partition by hash(a) partitions 3;
insert into t1 values(1, 1), (2, 2);
update t1 set b=b+1 where a>0;
select * from t1;
drop table t1;

--disable_warnings
DROP TABLE IF EXISTS t1, t2;
--enable_warnings
create table t1(a int primary key, b int) partition by hash(a) partitions 2;
create table t2(a int primary key, b int) partition by hash(a) partitions 2;
insert into t1 values(1, 1);
insert into t2 values(1, 1);
## test update change t2.b, not change t1.b
update t1, t2 set t1.b=1, t2.b=2 where t1.a=t2.a and t1.a=1;
select * from t1;
select * from t2;
update t1, t2 set t1.a=1, t2.a=2 where t1.a=t2.a and t1.a=1;
select * from t1;
select * from t2;
delete t1, t2 from t1, t2;
insert into t1 values(1, 1);
insert into t2 values(1, 1);
## test update change t1.b, not change t2.b
update t1, t2 set t1.b=2, t2.b=1 where t1.a=t2.a and t1.a=1;
select * from t1;
select * from t2;
update t1, t2 set t1.a=2, t2.a=1 where t1.a=t2.a and t1.a=1;
select * from t1;
select * from t2;
delete t1, t2 from t1, t2;
insert into t1 values(1, 1);
insert into t2 values(1, 1);
## test update change t1.b and t2.b
update t1, t2 set t1.b=2, t2.b=2 where t1.a=t2.a and t1.a=1;
select * from t1;
select * from t2;
delete t1, t2 from t1, t2;

create index i1 on t1(b);
create index i1 on t2(b);
insert into t1 values(1, 1);
## test update main table change t1.a across partition but t1.b does not
update t1 set a=a+1, b=b+1 where a=1;
select * from t1;
insert into t2 select * from t1;
## test update t2 change across partition but t1 does not
update t1, t2 set t1.a=2, t2.a=3 where t1.a=2 and t1.a=t2.a;
select * from t1;
select * from t2;
drop table t1, t2;

create table t1(a int primary key, b int, c int, d timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)) partition by hash(a) partitions 2;
create index idx on t1(c, d) global;
insert into t1 values(1, 1, 1, now());
update t1 set b=2, c=1 where a=1;
--replace_column 3 TIMESTAMP
select /*+index(t1 primary)*/ a, c, d from t1;
--replace_column 3 TIMESTAMP
select /*+index(t1 idx)*/ a, c, d from t1;
update t1 set b=2, c=2 where a=1;
--replace_column 3 TIMESTAMP
select /*+index(t1 primary)*/ a, c, d from t1;
--replace_column 3 TIMESTAMP
select /*+index(t1 idx)*/ a, c, d from t1;
drop table t1;

--disable_warnings
DROP TABLE IF EXISTS t1, t2, t3;
--enable_warnings
CREATE TABLE t1 (a int not null,b int not null);
CREATE TABLE t2 (a int not null, b int not null, primary key (a,b));
CREATE TABLE t3 (a int not null, b int not null, primary key (a,b));
insert into t1 values (1,1),(2,1),(1,3);
insert into t2 values (1,1),(2,2),(3,3);
insert into t3 values (1,1),(2,1),(1,3);
delete t2.*,t3.* from t1,t2,t3 where t1.a=t2.a AND t2.b=t3.a and t1.b=t3.b;
drop table t1, t2, t3;

create table t1(a int primary key, b int, c int, d int) partition by hash(a) partitions 2;
create index idx on t1(b, c) partition by hash(b) partitions 2;
insert into t1 values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4);
select /*+index(t1 primary)*/ * from t1;
select /*+index(t1 idx)*/ a, b, c from t1;
## test primary index update the partition key, global index don't update the partition key
--error 0, 1062
## when update row one by one, this case will throw error 1062
update t1 set a=a+1, c=c+1;
select /*+index(t1 primary)*/ * from t1;
select /*+index(t1 idx)*/ a, b, c from t1;
## test primary index don't update part key, global index update part key
update t1 set b=b+1;
select /*+index(t1 primary)*/ * from t1;
select /*+index(t1 idx)*/ a, b, c from t1;
## test primary index update across partitions, global index don't across partitions
--error 0, 1062
## when update row one by one, this case will throw error 1062
update t1 set a=a+5, b=b+6 where a in (2, 4);
select /*+index(t1 primary)*/ * from t1;
select /*+index(t1 idx)*/ a, b, c from t1;
## test primary index update don't across partitions, global index across partition
update t1 set a=a+6, b=b+5 where b in (8, 10);
select /*+index(t1 primary)*/ * from t1;
select /*+index(t1 idx)*/ a, b, c from t1;
delete from t1;
insert into t1 values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4);
## test primary index row not changed
update t1 set b=a;
update t1 set a=b, b=a, c=d;
select /*+index(t1 primary)*/ * from t1;
select /*+index(t1 idx)*/ a, b, c from t1;
drop table t1;
