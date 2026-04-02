drop tablegroup if exists xy_group;
create tablegroup xy_group;
drop table if exists type_t;
create table type_t(int_c INT primary key default 1,
                    tinyint_c TINYINT,
                    bool_c BOOL,
                    smallint_c SMALLINT,
                    mediumint_c MEDIUMINT,
                    integer_c INTEGER,
                    bigint_c BIGINT,
                    float_c FLOAT,
                    double_c DOUBLE,
                    decimal_c DECIMAL(6,3),
                    numeric_c NUMERIC(6,3),
                    char_c CHAR(100),
                    varchar_c VARCHAR(100),
                    binary_c BINARY(100),
                    date_c DATE,
                    datetime_c DATETIME,
                    time_c    TIME,
                    year_c    YEAR
)  tablegroup='xy_group';

drop table if exists t1;
create table t1(c1 int primary key, c2 int)  tablegroup='xy_group';
drop table if exists t2;
create table t2(c1 int primary key, c2 int, c3 varchar(32)) tablegroup='xy_group';
drop table if exists t3;
create table t3(i1 int primary key, f1 float, s1 varchar(32), d1 datetime)  tablegroup='xy_group';
drop table if exists dist_t;
create table dist_t(c1 int primary key, c2 int) partition by hash (c1) partitions 5;
drop table if exists emp;
create table emp (id int, name varchar(20), leaderid int);
drop table if exists acs_test;
create table acs_test(c1 int primary key, c2 int, c3 int, index k1(c2));

INSERT into type_t values(0, 0, 0, 0, 0, 0, 0, 0.1, 0.1, 0.1, 0.1, 'goodman', 'goodman', 'goodman', '2015-01-01', '2015-01-01 01:01:01', '01:01:01', 70);
INSERT into type_t values(1, 1, 1, 1, 1, 1, 1, 0.11, 0.11, 0.11, 0.11, 'good', 'good', 'good', '2015-02-01', '2015-01-01 02:02:02', '02:02:02', 1980);
INSERT into type_t values(2, 2, true, 2, 2, 2, 2, 1.11, 1.11, 1.11, 1.11, 'helloworld', 'helloworld', 'helloworld', '2015-01-06', '2015-01-01 00:00:00', '02:02:02', 1980);
insert into type_t(int_c, bool_c, bigint_c, varchar_c, datetime_c) values(3, false, 10, '2015-01-06','2015-11-11 00:00:00');
insert into type_t(int_c, bigint_c, varchar_c, datetime_c) values(4, -4, '2015-01-01','1970-01-01 08:00:00.000002');
insert into type_t(int_c, bigint_c, varchar_c, datetime_c) values(5, 4, '1','1970-01-01 08:00:00.000001');
insert into type_t(int_c, bigint_c, varchar_c) values(6, 12, '2');
insert into type_t(int_c, varchar_c) values(7, 'helloeveryone');
insert into type_t(int_c, varchar_c) values(8, 'hello');
insert into type_t(int_c, varchar_c) values(9, 'HELLO');
insert into type_t(int_c, varchar_c) values(10, 'HELLOWORLD');
insert into type_t(int_c, varchar_c) values(11, '80');
insert into type_t(int_c, varchar_c) values(12, 'FF');
insert into type_t(int_c, varchar_c) values(13, '0123');
insert into type_t(int_c, varchar_c) values(14, '0.0.0.1');
insert into type_t(int_c, varchar_c) values(-1, '0.0.0.21');
insert into type_t(int_c, varchar_c) values(-2, '1111');
insert into type_t(int_c, varchar_c) values(21, '15');

insert into t1 values(0, 0);
insert into t1 values(1, 1);
insert into t1 values(2, 1);
insert into t1 values(3, 2);
insert into t1 values(4, 2);
insert into t1 values(5, 3);
insert into t1 values(6, 3);
insert into t1 values(7, 4);
insert into t1 values(8, 4);
insert into t1 values(9, 5);
insert into t1 values(10, 10);

insert into t2 values(0, 0, 'hellotestgood');
insert into t2 values(1, 1, 'hellotestgood');
insert into t2 values(2, 1, 'hellotestgood');
insert into t2 values(3, 2, 'hellotestgood');
insert into t2 values(4, 2, 'hellotestgood');
insert into t2 values(5, 3, 'hellotestgood');
insert into t2 values(6, 3, 'hellotestgood');
insert into t2 values(7, 4, 'hellotestgood');

insert into dist_t values(0, 0);
insert into dist_t values(1, 1);
insert into dist_t values(2, 1);
insert into dist_t values(3, 2);
insert into dist_t values(4, 2);
insert into dist_t values(5, 3);
insert into dist_t values(6, 3);
insert into dist_t values(7, 4);
insert into dist_t values(8, 4);
insert into dist_t values(9, 5);

insert into emp values(2, 'AA', '1');
insert into emp values(3, 'AB', '1');
insert into emp values(4, 'ABA', '3');
insert into emp values(5, 'AAA', '2');
insert into emp values(6, 'ABB', '3');
insert into emp values(7, 'AAA', '5');
insert into emp values(8, 'AAA', '7');
insert into emp values(9, 'AAAA', '5');
insert into emp values(10, 'AAAB', '5');
insert into emp values(11, 'AAAC', '5');
insert into emp values(12, 'AAAA', '5');

insert into acs_test values (1,1,1);
insert into acs_test values (2,2,2);
insert into acs_test values (3,3,3);
insert into acs_test values (4,4,4);
insert into acs_test values (5,5,5);
insert into acs_test values (6,6,6);
