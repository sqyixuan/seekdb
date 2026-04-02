
--disable_warnings
drop table if exists t1, t2, t3;
create table t1(a int, b int, primary key (a)) partition by hash(a) partitions 2;
create table t2(a int, b int, primary key (a)) partition by hash(a) partitions 2;
create table t3(a int, b int, primary key (a)) partition by hash(a) partitions 3;
insert into t1 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15);
insert into t2 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15);
insert into t3 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15);
--enable_warnings

