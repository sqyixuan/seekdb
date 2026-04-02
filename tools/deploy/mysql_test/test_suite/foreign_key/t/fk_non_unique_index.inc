#### owner: fangzhou.yfz
#### owner group: IMF
#### description: foreign key references non-unique index

--disable_warnings
drop table if exists A, B, C, D, E, F, G;
--enable_warnings

# create tables test:

# self-reference primary key: unique reference the primary key, but cases are different.
create table A(a int, a3 int, primary key (a, a3), a2 int, a4 int, foreign key (a2, a4) references A(a, A3));
drop table A;
# self-reference primary key: non-unique reference the primary key 
create table A(a int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, primary key(A, A2, A3), foreign key (a6) references A(a), foreign key(A4, a5) references A(a2, a));
# self-reference unique key, reference other's primary key
create table B(b int, b2 int, b3 int, b4 int, unique key(b, b2), foreign key (b) references A(a), foreign key (b3, b4) references A(a, a2), foreign key (b3) references B(b));
# self-reference index
create table C(c int, c2 int, c3 int, c4 int, c5 int, index Cindex (c, c2, c3), unique key(c3, c4, c5), foreign key (C3, c4) references C(c2, c));
# reference other's index
create table D(d int, d2 int, d3 int, d4 int, foreign key(d3, D4) references C(C2, c), foreign key(d2, D3, d4) references C(C3, c2, c));
# reference other's unique key
create table E(e int, e2 int, E3 int, E4 int, foreign key(E4, e3) references C(C4, c3), foreign key(E, e2, e3) references C(c3, C4, c5), index Eindex (e));
# self-reference unique index
# OB thinks é = e
create table F(f int, f2 int, f3 int, f4 int, f5 int, unique index Findex (f, F2, F3), foreign key (f4) references F(f), foreign key (f5) references E(é));
# reference other's unique index
create table G(g int, g2 int, g3 int, g4 int, foreign key (g, g2) references F(F, f2));
# reference other's index
--error ER_CANNOT_ADD_FOREIGN
create table F1(f int, f2 int, f3 int, f4 int, foreign key(f3, f4) references C(c, c3));
# reference other's primary key
--error ER_CANNOT_ADD_FOREIGN
create table F2(f int, f2 int, f3 int, f4 int, foreign key(f3) references A(a2));
# reference other's unique key
--error ER_CANNOT_ADD_FOREIGN
create table F3(f int, f2 int, f3 int, f4 int, foreign key(f3) references B(b2));
--error ER_CANNOT_ADD_FOREIGN
create table F4(f int, f2 int, f3 int, f4 int, foreign key(f3, f4) references C(c4, c5));
# self-reference primary key
--error ER_CANNOT_ADD_FOREIGN
create table F5(f int, f2 int, f3 int, f4 int, primary key(f, f2), foreign key(f3, f4) references F5(f, f3));
--error ER_CANNOT_ADD_FOREIGN
create table F5(f int, f2 int, f3 int, f4 int, primary key(f, f2), foreign key(f3, f4) references F5(f, f));
# self-reference unique key
--error ER_CANNOT_ADD_FOREIGN
create table F6(f int, f2 int, f3 int, f4 int, f5 int, unique key(f, f2, f3), foreign key(f4, f5) references F6(f, f3));
# self-reference index
--error ER_CANNOT_ADD_FOREIGN
create table F7(f int, f2 int, f3 int, f4 int, f5 int, index Findex(f, f2, f3), foreign key(f3) references F7(f), foreign key(f4, f5) references F7(f, f3));
--error ER_WRONG_FK_DEF
create table F8(f int, f2 int, f3 int, f4 int, f5 int, foreign key(f3, f4) references E(e));
--error ER_WRONG_FK_DEF
create table F9(f int, f2 int, f3 int, f4 int, f5 int, foreign key(f3, f4) references F(f, f2, f3));
drop table A, B, C, D, E, F, G;

# test 1: 2 tables, one non-unique index, insert/update cascade/delete cascade

create table A(a int, msg int);
create index Aindex on A(a);
create table B(a int, FOREIGN KEY(a) REFERENCES A(a) on update cascade on delete cascade);
insert into A values (2, 10), (3, 11), (3, 12), (NULL, 10), (NULL, 11);
insert into B values (2), (2), (2), (3), (3), (3), (NULL);
select * from B;
--error ER_NO_REFERENCED_ROW_2
insert into B values (10);
update A set a = 4 where a = 2;
delete from A where a = 4;
select * from A;
drop table A, B;

# test 2: 2 tables, one non-unique index, insert/non-cascade update/non-delete cascade

create table A(a int, msg int);
create index Aindex on A(a);
create table B(a int, FOREIGN KEY(a) REFERENCES A(a));
insert into A values (2, 10), (3, 11), (3, 12);
insert into B values (2), (2), (2), (3), (3), (3);
--error ER_NO_REFERENCED_ROW_2
insert into B values (10);
select * from B;
--error ER_ROW_IS_REFERENCED_2
update A set a = 4 where a = 2;
--error ER_ROW_IS_REFERENCED_2
delete from A where a = 2;
drop table A, B;

# test 3: 4 tables, one non-unique index of multiple columns.

create table A(b int, a2 int, a3 int, msg int);
create index Aindex on A(b, a2, a3);
create index Ai2 on A(msg);
create index Ai3 on A(a3, msg, a2);
create table B(a int, FOREIGN KEY(a) REFERENCES A(b));
--error ER_CANNOT_ADD_FOREIGN
create table C(a2 int, FOREIGN KEY(a2) REFERENCES A(a2));
--error ER_CANNOT_ADD_FOREIGN
create table C(a2 int, a3 int, FOREIGN KEY(a2, a3) REFERENCES A(a2, a3));
insert into A values (2, 100, 10, NULL), (3, 101, 11, NULL), (3, 101, 12, NULL);
insert into B values (2), (2), (2), (3), (3), (3);
--error ER_NO_REFERENCED_ROW_2
insert into B values (10);
select * from B;
--error ER_ROW_IS_REFERENCED_2
update A set b = 4 where b = 2;
--error ER_ROW_IS_REFERENCED_2
delete from A where b = 2;

create table C(a2 int, b int, FOREIGN KEY(a2, b) REFERENCES A(a2, b));
insert into C values (100, 2), (101, 3), (101, 3), (100, 2);
select * from C;
--error ER_NO_REFERENCED_ROW_2
insert into C values (100, 100);

create table D(a2 int, b int, FOREIGN KEY(b, a2) REFERENCES A(b, a2));
insert into D values (100, 2), (101, 3), (101, 3), (100, 2);
select * from D;

create table E(a3 int, a2 int, b int, FOREIGN KEY(b, a2, a3) REFERENCES A(b, a2, a3));
insert into E values (10, 100, 2), (11, 101, 3), (12, 101, 3), (10, 100, 2);
select * from E;

# Check drop index
--error ER_DROP_INDEX_FK
alter table A drop index Aindex;
alter table A drop index Ai2;
alter table A drop index Ai3;
drop table E;
--error ER_DROP_INDEX_FK
alter table A drop index Aindex;
drop table D;
--error ER_DROP_INDEX_FK
alter table A drop index Aindex;
drop table C;
--error ER_DROP_INDEX_FK
alter table A drop index Aindex;
drop table B;
alter table A drop index Aindex;
drop table A;

# test 4: self-reference

create table A(a int, a2 int, a3 int, a4 int);
create index Aindex on A(a, a2, a3, a4);
alter table A add foreign key (a2) references A(a) on update cascade;
alter table A add foreign key (a3, a4) references A(a, a2) on update cascade;
insert into A values (1, NULL, NULL, NULL);
insert into A values (1, 1, NULL, NULL);
insert into A values (NULL, NULL, 1, 1);
select * from A;
--error ER_DROP_INDEX_FK
alter table A drop index Aindex;
drop table A;

create table A(pk1 int, pk2 int, a int, a2 int, primary key A(a, pk1, pk2), foreign key (a2) references A(a));
insert into A values (0, 0, 1, NULL);
insert into A values (0, 1, 1, NULL);
insert into A values (0, 0, 2, 1);
select * from A;
drop table A;

create table A(a int, a2 int, a3 int, a4 int, index Aindex(a, a2, a3, a4), foreign key (a2) references A(a), foreign key (a4, a3) references A(a, a2));
--error ER_CANNOT_ADD_FOREIGN
create table B(a varchar(20), a2 int, a3 int, a4 int, index Bindex(a, a2, a3, a4), foreign key (a2) references B(a), foreign key (a4, a3) references B(a, a2));
--error ER_CANNOT_ADD_FOREIGN
create table B(a int, a2 int, a3 int, a4 int, index Bindex(a, a2, a4), foreign key (a2) references B(a), foreign key (a3, a4) references B(a4, a2));
--error ER_CANNOT_ADD_FOREIGN
create table B(a int, a2 int, a3 int, a4 int, index Bindex(a4, a2, a), foreign key (a2) references B(a), foreign key (a3, a4) references B(a, a2));
insert into A values (1, NULL, NULL, NULL);
insert into A values (1, 1, NULL, NULL);
insert into A values (NULL, NULL, 1, 1);
select * from A;
drop table A;

create table A(uk1 int, uk2 int, a int, unique key A(uk1, uk2), foreign key (a) references A(uk1));
insert into A values (1, 1, NULL), (1, 2, 1);
select * from A;
--error ER_ROW_IS_REFERENCED_2
replace into A values (1, 1, 1);
update A set a = NULL;
select * from A;
drop table A;

create table A(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, unique key (c1, c2, c3, c4, c5), foreign key(c5, c4, c6) references A(c3, c1, c2));
insert into A values (1, 1, 1, NULL, NULL, NULL), (NULL, NULL, NULL, 1, 1, 1);
select * from A;
drop table A;

create table A(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, primary key (c1, c2, c3, c4, c5), foreign key(c5, c4, c6) references A(c3, c1, c2), foreign key(c6, c5) references A(c2, c1));
insert into A values (1, 1, 1, 1, 1, 1), (1, 2, 1, 1, 1, 1);
--error ER_NO_REFERENCED_ROW_2
replace into A values (1, 2, 1, 1, 1, 3);
select * from A;
drop table A;

# test 5: reference string column

create table A(a varchar(20), a2 varchar(20));
create index Aindex on A(a2, a);
create table B(a varchar(20), foreign key (a) references A(a2));
insert into A values ('a', 'a2'), ('a3', 'a4'), ('a', 'a2'), ('a3', 'a4');
insert into B values ('a2'), ('a2'), ('a4'), ('a4');
select * from B;
select * from A;
--error ER_NO_REFERENCED_ROW_2
insert into B values ('');
drop table A, B;

# Strong type check
--error ER_CANNOT_ADD_FOREIGN
create table A(a varchar(20), b int, c int, unique key(b, c), foreign key(a) references A(b));
--error ER_CANNOT_ADD_FOREIGN
create table A(a varchar(20), b int, c varchar(20), unique key(b, c), foreign key(a) references A(b));
--error ER_CANNOT_ADD_FOREIGN
create table A(a int, b float, c float, unique key(b, c), foreign key(a) references A(b));
create table A(a int, b int, c int);
create index Aindex on A(a, b);
--error ER_CANNOT_ADD_FOREIGN
create table B(a varchar(20), foreign key(a) references A(a));
--error ER_CANNOT_ADD_FOREIGN
create table B(a float, foreign key(a) references A(a));
create table B(a int, foreign key(a) references A(a));
drop table A, B;

# test 6: choose unique index rather than choosing non-unique index

create table A(a int, a2 int);
create index Ai on A(a);
create unique index Aui on A(a);
create index Ai2 on A(a);
create table B(a int, foreign key (a) references A(a));
--error 4714
alter table A drop index Aui;
alter table A drop index Ai;
alter table A drop index Ai2;
insert into A values (1, 10), (2, 20);
insert into B values (1), (1), (2), (2);
select * from A;
select * from B;
drop table A, B;

create table A(a int, a2 int, primary key (a, a2));
create unique index Aui on A(a2);
create table B(a2 int, foreign key (a2) references A(a2));
--error 4714
alter table A drop index Aui;
insert into A values (1, 10), (2, 11);
insert into B values (10), (10), (11), (11);
select * from A;
select * from B;
drop table A, B;

create table A(a int, a2 int, primary key (a));
create unique index Aui on A(a);
create table B(a int, foreign key (a) references A(a));
alter table A drop index Aui;
insert into A values (1, 10), (2, 11);
insert into B values (1), (1), (2), (2);
select * from A;
select * from B;
drop table A, B;

create table A(a int, a2 int);
create index Aindex on A(a);
create table B(a int, foreign key (a) references A(a));
create unique index Aui on A(a);
insert into A values (1, 10), (2, 11);
insert into B values (1), (1), (2), (2);
select * from A;
select * from B;
alter table A drop index Aui;
insert into B values (1), (1), (2), (2);
select * from B;
drop table A, B;

# test 7: replace, update, delete
create table A(a int, a2 int, primary key(a, a2));
create table B(b int primary key, a int, foreign key (a) references A(a));
replace into A values(1, 1), (2, 2), (3, 3), (4, 4);
replace into A values(1, 5), (1, 6), (1, 7), (2, 5), (2, 6), (2, 7), (3, 5), (3, 6), (3, 7);
replace into B values(10, 1), (11, 2), (12, 4), (13, 1), (14, 2), (15, 1);
select * from A;
select * from B;
replace into B values(12, 1);
select * from B;
replace into B values(16, 1), (17, 2), (18, 1), (19, 2);
select * from B;
update B set a = a + 1;
select * from B;
--error ER_ROW_IS_REFERENCED_2
update A set a = 5, a2 = 5 where a = 2 and a2 = 2;
update A set a = 5, a2 = 5 where a = 1 and a2 = 1;
select * from A;
--error ER_ROW_IS_REFERENCED_2
delete from A;
delete from B where a <> 2;
delete from A where a <> 2;
select * from A;
select * from B;
drop table A, B;

# test 8: rewrite correctness
create table A(a int not null, a2 int not null, primary key(a, a2));
create table B(b int primary key, a int not null, foreign key(a) references A(a));
insert into A values(1, 1), (1, 2), (2, 1), (3, 1);
insert into B values(10, 1), (11, 3);
# outer join -> inner join correctness
select * from B left join A on B.a = A.a order by A.a;
# not eliminate join (only eliminate when the foreign key references primary key)
select B.* from B join A on B.a = A.a order by B.b;
drop table A, B;

create table A(a int, index Aindex(a));
create table B(b int primary key, a int, foreign key(a) references A(a));
create table C(c int primary key, a int not null, foreign key(a) references A(a));
insert into A values(1), (1), (2), (3), (NULL);
insert into B values(10, 1), (11, 3);
insert into C values(10, 1), (11, 3);
# outer join -> inner join correctness
select * from B left join A on B.a = A.a order by A.a, B.b;
select * from C left join A on C.a = A.a order by A.a, C.c;
# not eliminate join (only eliminate when the foreign key references primary key)
select B.* from B join A on B.a = A.a order by B.b;
select C.* from C join A on C.a = A.a order by C.c;
drop table A, B, C;

# test 9: insert ignore
--disable_warnings
drop table if exists t3;
drop table if exists t2;
drop table if exists t1;
--enable_warnings
create table t1(id int primary key,c1 int ,index index1(c1));
create table t2(id int primary key,c1 int);
alter table t2 add foreign key(c1) references t1(c1) on update cascade on delete cascade;
insert into t1 values(1,1),(2,1),(3,1),(4,1);
insert ignore into t2 values(1,1),(2,1);
select * from t1 order by id;
select * from t2 order by id;
insert ignore into t2 values(3,1),(1,1);
select * from t1 order by id;
select * from t2 order by id;
-- error 1452
insert ignore into t2 values(4,1),(1,2);
-- error 1452
insert ignore into t2 values(4,2),(5,1);
select * from t1 order by id;
select * from t2 order by id;

# test 10: update ignore with cascade
--disable_warnings
drop table if exists t3;
drop table if exists t2;
drop table if exists t1;
--enable_warnings
create table t1(id int primary key,c1 int ,index index1(c1));
create table t2(id int primary key,c1 int);
alter table t2 add foreign key(c1) references t1(c1) on update cascade on delete cascade;
insert into t1 values(1,1),(2,1),(3,1),(4,1);
insert into t2 values(1,1),(2,1);
select * from t1 order by id;
select * from t2 order by id;
-- error 1452
UPDATE IGNORE t1 SET id = 2,c1 = 100 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
UPDATE IGNORE t1 SET id = 1,c1 = 100 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
UPDATE IGNORE t1 SET c1 = 100 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
UPDATE IGNORE t1 SET c1 = 200 WHERE id IN (1,2,3);
select * from t1 order by id;
select * from t2 order by id;
UPDATE IGNORE t1 SET c1 = 1 WHERE id IN (1,2,3);
select * from t1 order by id;
select * from t2 order by id;
UPDATE IGNORE t2 SET c1 = 1 WHERE id = 1;
-- error 1452
UPDATE IGNORE t2 SET c1 = 200 WHERE id = 1;

--disable_warnings
drop table if exists t3;
drop table if exists t2;
drop table if exists t1;
--enable_warnings
create table t1(id int primary key,c1 int ,index index1(c1));
create table t2(id int primary key,c1 int, index index1(c1));
create table t3(id int primary key,c1 int);
alter table t2 add foreign key(c1) references t1(c1) on update cascade on delete cascade;
alter table t3 add foreign key(c1) references t2(c1) on update cascade on delete cascade;
insert into t1 values(1,1),(2,1),(3,1),(4,1);
insert into t2 values(1,1),(2,1);
insert into t3 values(1,1),(2,1);
-- error 1452
UPDATE IGNORE t1 SET id = 2,c1 = 100 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
-- error 1452
UPDATE IGNORE t2 SET id = 2,c1 = 100 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
-- error 1452
UPDATE IGNORE t3 SET id = 2,c1 = 100 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
-- error 1452
UPDATE IGNORE t2 SET c1 = 100 WHERE id = 1;
-- error 1452
UPDATE IGNORE t3 SET c1 = 100 WHERE id = 1;
select * from t2 order by id;
select * from t3 order by id;
UPDATE IGNORE t1 SET c1 = 101 WHERE id = 1;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
insert into t1 values(5,2),(6,2),(7,3),(8,3);
insert into t2 values(3,2),(4,3);
insert into t3 values(3,2),(4,3);
-- error 1452
UPDATE IGNORE t1 SET id = 1, c1 = 102 WHERE c1 = 2;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
-- error 1452
UPDATE IGNORE t2 SET id = 1, c1 = 1 WHERE c1 = 2;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
UPDATE IGNORE t1 SET c1 = 102 WHERE c1 = 2;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
-- error 1452
UPDATE IGNORE t2 SET id = 1, c1 = 1 WHERE c1 = 3;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
UPDATE IGNORE t2 SET c1 = 1 WHERE c1 = 3;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;

# test 11: delete ignore with cascade
--disable_warnings
drop table if exists t3;
drop table if exists t2;
drop table if exists t1;
--enable_warnings
create table t1(id int primary key,c1 int ,index index1(c1));
create table t2(id int primary key,c1 int);
alter table t2 add foreign key(c1) references t1(c1) on update cascade on delete cascade;
insert into t1 values(1,1),(2,1),(3,1),(4,1);
insert ignore into t2 values(1,1),(2,1);
select * from t1 order by id;
select * from t2 order by id;
delete ignore from t1 where id = 1;
select * from t1 order by id;
select * from t2 order by id;
insert ignore into t2 values(1,1),(2,1);
delete ignore from t1 where id IN (1,2,3,4);
select * from t1 order by id;
select * from t2 order by id;
insert into t1 values(1,1),(2,1),(3,2),(4,2);
insert ignore into t2 values(1,1),(2,2);
delete ignore from t1 where c1 = 1 or c1 = 2;
select * from t1 order by id;
select * from t2 order by id;
insert into t1 values(1,1),(2,1),(3,2),(4,2);
insert ignore into t2 values(1,1),(2,2);
delete ignore from t1 where c1 = 1 or c1 = 3;
select * from t1 order by id;
select * from t2 order by id;

--disable_warnings
drop table if exists t3;
drop table if exists t2;
drop table if exists t1;
--enable_warnings
create table t1(id int primary key,c1 int ,index index1(c1));
create table t2(id int primary key,c1 int, index index1(c1));
create table t3(id int primary key,c1 int);
alter table t2 add foreign key(c1) references t1(c1) on update cascade on delete cascade;
alter table t3 add foreign key(c1) references t2(c1) on update cascade on delete cascade;
insert into t1 values(1,1),(2,1),(3,1),(4,1),(5,2),(6,2),(7,3),(8,3);
insert into t2 values(1,1),(2,1),(3,2),(4,3);
insert into t3 values(1,1),(2,1),(3,2),(4,3);
delete ignore from t1 where id = 1;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
delete ignore from t1 where id = 2;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
delete ignore from t2 where c1 = 2;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
delete ignore from t1 where c1 = 2;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;
delete ignore from t1 where c1 >= 1;
select * from t1 order by id;
select * from t2 order by id;
select * from t3 order by id;

--disable_warnings
drop table if exists t3;
drop table if exists t2;
drop table if exists t1;
--enable_warnings


# test 11: alter table drop index
--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A (a int, b int, c int, primary key(a), index idx_bc(b, c), index idx_b(b));
# reference idx_b
create table B (a int, b int, c int, primary key(a), index idx_bc(b, c), foreign key(b) references A(b));
# reference idx_bc
create table C (a int, b int, c int, primary key(a), foreign key(b) references B(b));
--error 1553
alter table A drop index idx_b;
alter table A drop index idx_bc;
--error 1553
alter table B drop index idx_bc;

--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A(c1 int, c2 int, c3 int);
create index idx1 on A(c1, c2, c3);
# reference idx1
create table B(c1 int, c2 int, foreign key(c1, c2) references A(c1, c2));
create index idx2 on A(c2, c1, c3);
create index idx3 on A(c1, c2, c3);
--error 1553
alter table A drop index idx1;
alter table A drop index idx2;
alter table A drop index idx3;

--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A(c1 int, c2 int, c3 int);
create index idx1 on A(c1, c2, c3);
create unique index idx2 on A(c1, c2);
create unique index idx3 on A(c1, c2, c3);
# reference idx2
create table B(c1 int, c2 int, foreign key(c1, c2) references A(c1, c2));
alter table A drop index idx1;
--error 4714
alter table A drop index idx2;
alter table A drop index idx3;

--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A(c1 int, c2 int, c3 int);
create index idx1 on A(c1, c2);
create index idx2 on A(c1, c2, c3);
# reference idx2
create table B(c1 int, c2 int, foreign key(c1, c2) references A(c1, c2));
alter table A drop index idx1;
--error 1553
alter table A drop index idx2;


# test 12: alter table drop column
--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A (a int, b int, c int, index idx1(a, b, c));
create table B (a int, b int, c int, index idx2(a, b, c), foreign key(a, b) references A(a, b));
create table C (a int, b int, c int, foreign key(a, b) references B(a, b));
-- error 5320
alter table A drop column a;
-- error 5320
alter table A drop column b;
alter table A drop column c;
-- error 5320
alter table B drop column a;
-- error 5320
alter table B drop column b;
alter table B drop column c;
--error 1553
alter table A drop index idx1;
--error 5320
alter table B drop index idx2;
alter table A add column c int;
alter table B add column c int;
--error 1553
alter table A drop index idx1;
--error 5320
alter table B drop index idx2;

--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A(c1 int,c2 int not null,c3 int,c4 int, primary key(c1, c2, c3)); 
create table B(c1 int not null,c2 int,c3 int,c4 int, CONSTRAINT fk1 foreign key(c1,c2) references A(c1,c2));
--error 5320
alter table A drop c1;
--error 5320
alter table A drop c2;
--error 5320
alter table B drop c1;
--error 5320
alter table B drop c2;
--error 1235
alter table A drop c3;
alter table A drop c4;
alter table B drop c3;
alter table B drop c4;
alter table A drop primary key, add primary key(c1, c2);
ALTER TABLE A ADD COLUMN c4 int;
ALTER TABLE B ADD COLUMN c3 int;
ALTER TABLE B ADD COLUMN c4 int;
alter table A drop primary key, add primary key(c1, c2, c3);

--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A(c1 int,c2 int not null,c3 int,c4 int);
create unique index i1 on A(c1,c2,c3); 
create table B(c1 int not null,c2 int,c3 int,c4 int, CONSTRAINT fk1 foreign key(c1,c2) references A(c1,c2));
--error 5320
alter table A drop c1;
--error 5320
alter table A drop c2;
--error 5320
alter table B drop c1;
--error 5320
alter table B drop c2;
--error 4714
alter table A drop index i1;
alter table A drop c3;
alter table A drop c4;
alter table B drop c3;
alter table B drop c4;
--error 4714
alter table A drop index i1;
ALTER TABLE A ADD COLUMN c3 int;
ALTER TABLE A ADD COLUMN c4 int;
ALTER TABLE B ADD COLUMN c3 int;
ALTER TABLE B ADD COLUMN c4 int;
--error 4714
alter table A drop index i1;


# test 13: alter table add primary key
--disable_warnings
drop table if exists A, B, C;
--enable_warnings
create table A (a int, b int, c int, index idx1(a, b, c));
create table B (a int, b int, c int, foreign key(a, b) references A(a, b));
alter table A add primary key(a, b);
-- error 1553
alter table A drop index idx1;
-- error 4714
alter table A drop primary key;


# test 14: alter table rename column
--disable_warnings
drop table if exists A, B;
--enable_warnings
create table A (a int, b int, c int, index idx1(a, b, c));
create table B (a int, b int, c int, foreign key(a, b) references A(a, b));
alter table A rename column a to d;
alter table A rename column b to e;
alter table A rename column c to f;
alter table B rename column a to d;
alter table B rename column b to e;
alter table B rename column c to f;


# test 15: alter table MODIFY column
--disable_warnings
drop table if exists A, B;
--enable_warnings
create table A (a int, b int, index idx_b(b));
create table B (a int, b int, foreign key(b) references A(b));
--error 5320
alter table A modify b double;
--error 5320
alter table A modify b int unsigned;
--error 5320
alter table B modify b double;
--error 5320
alter table B modify b int unsigned;

--disable_warnings
drop table if exists A, B;
--enable_warnings
create table A (a int, b int, primary key(a));
create table B (a int, b int, foreign key(b) references A(a));
--error 5320
alter table A modify a double;
--error 5320
alter table A modify a int unsigned;
--error 5320
alter table B modify b double;
--error 5320
alter table B modify b int unsigned;
# alter table A modify primary key(a, b);


# test 16: alter table partiton
--disable_warnings
drop table if exists A, B;
--enable_warnings
create table A (a int, b int, c int, primary key(a, b, c));
create table B (a int, b int, c int);
insert into A values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9), (10,10,10);
insert into B values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9), (10,10,10);
alter table B add foreign key(a) references A(a) on update cascade on delete cascade;
alter table B add foreign key(a, b) references A(a, b) on update cascade on delete cascade;
alter table B add foreign key(a, b, c) references A(a, b, c) on update cascade on delete cascade;

alter table A partition by range (a) (partition p0 values less than (5), partition p1 values less than (20));
select * from A order by a;
select * from A order by b;
alter table A partition by list (a) (partition p0 values in (1,2,3,4), partition p1 values in (5,6,7,8,9,10));
select * from A order by a;
select * from A order by b;
alter table A partition by hash (a) partitions 5;
select * from A order by a;
select * from A order by b;
alter table A partition by key (a) partitions 5;
update A set a = 11 where a = 1;
update A set b = 11 where b = 1;
select * from A order by a;
select * from A order by b;
alter table A partition by range (b) (partition p0 values less than (5), partition p1 values less than (20));
select * from A order by a;
select * from A order by b;
alter table A partition by list (b) (partition p0 values in (1,2,3,4), partition p1 values in (5,6,7,8,9,10,11,12,13));
select * from A order by a;
select * from A order by b;
alter table A partition by hash (b) partitions 5;
select * from A order by a;
select * from A order by b;
alter table A partition by key (b) partitions 5;
update A set a = 12 where a = 2;
update A set b = 12 where b = 2;
select * from A order by a;
select * from A order by b;
alter table A partition by range (c) (partition p0 values less than (5), partition p1 values less than (20));
select * from A order by a;
select * from A order by b;
alter table A partition by list (c) (partition p0 values in (1,2,3,4), partition p1 values in (5,6,7,8,9,10,11,12,13));
select * from A order by a;
select * from A order by b;
alter table A partition by hash (c) partitions 5;
select * from A order by a;
select * from A order by b;
alter table A partition by key (c) partitions 5;
update A set a = 13 where a = 3;
update A set b = 13 where b = 3;
select * from A order by a;
select * from A order by b;


# test 17: lob primary key/index
--disable_warnings
drop table if exists A, B;
--enable_warnings
create table A (c1 string primary key, c2 string, index idx2(c2));
create table B (c1 string, foreign key (c1) references A(c2) on update cascade on delete cascade);
insert into A values ("pk", "xxx");
insert into B values ("xxx");
insert into B values ("xxx");
select * from A order by c1;
select * from B order by c1;
-- error 1452
insert into B values ("error");
-- error 1452
update B set c1 = "error" where c1 = "xxx";
insert into A values("pk", "insert up stmt") on duplicate key update c2 = values(c2);
select * from A order by c1;
select * from B order by c1;
replace into A values("pk", "replace stmt");
select * from A order by c1;
select * from B order by c1;
insert into B values ("replace stmt");
select * from B order by c1;

--disable_warnings
drop table if exists A, B;
--enable_warnings
create table A (c1 string, c2 string, c3 int, primary key(c1, c2));
create table B (c1 string, foreign key (c1) references A(c1) on update cascade on delete cascade);
insert into A values ("pk", "xxx", 0);
insert into B values ("pk");
insert into B values ("pk");
select * from A order by c1;
select * from B order by c1;
-- error 1452
insert into B values ("error");
-- error 1452
update B set c1 = "error" where c1 = "pk";
insert into A values("pk", "xxx", 1) on duplicate key update c3 = values(c3);
select * from A order by c1;
select * from B order by c1;
replace into A values("pk", "xxx", 2);
select * from A order by c1;
select * from B order by c1;
insert into B values ("pk");
select * from B order by c1;


--disable_warnings
drop table if exists A, B, C;
--enable_warnings