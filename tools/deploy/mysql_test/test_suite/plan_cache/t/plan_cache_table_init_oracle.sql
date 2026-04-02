create tablegroup XY_GROUP;
create table type_t(int_c INT primary key default 1,
                    smallint_c SMALLINT,
                    integer_c INTEGER,
                    float_c binary_float,
                    double_c binary_double,
                    decimal_c DECIMAL(6,3),
                    number_float_c float(21),
                    char_c CHAR(100),
                    varchar_c VARCHAR(100),
                    binary_c char(100),
                    date_c DATE,
                    time_c TIMESTAMP,
                    time_tz_c TIMESTAMP WITH TIME ZONE,
                    time_ltz_c TIMESTAMP WITH LOCAL TIME ZONE
)  tablegroup='XY_GROUP';

create table t1(c1 int primary key, c2 int)  tablegroup='XY_GROUP';
create table t2(c1 int primary key, c2 int, c3 varchar(32)) tablegroup='XY_GROUP';
create table t3(i1 int primary key, f1 decimal(10,5), s1 varchar(32), d1 date)  tablegroup='XY_GROUP';
create table dist_t(c1 int primary key, c2 int) partition by hash (c1) partitions 5;
create table emp (id int, name varchar(20), leaderid int);
create table acs_test(c1 int primary key, c2 int, c3 int);
create index k1 on acs_test (c2);

INSERT into type_t values(0, 0, 0, 0.1, 0.1, 0.1, 0.1,'goodman', 'goodman', 'goodman', '2015-02-01 02:02:02', '1970-01-01 08:00:00.000002', '1970-01-01 08:00:00.000002', '1970-01-01 08:00:00.000002');
INSERT into type_t values(1, 1, 1, 0.11, 0.11, 0.11, 0.11,'good', 'good', 'good', '2015-01-01 02:02:02', '2018-01-01 08:00:00.000002', '2018-01-01 08:00:00.000002', '2018-01-01 08:00:00.000002');
INSERT into type_t values(2, 2, 2, 1.11, 1.11, 1.11, 1.11, 'helloworld', 'helloworld', 'helloworld', '2015-01-06 00:00:00', '2019-01-01 08:00:00.000002', '2019-01-01 08:00:00.000002', '2019-01-01 08:00:00.000002');
insert into type_t(int_c, varchar_c, date_c) values(3, '2015-01-06','2015-11-11 00:00:00');
insert into type_t(int_c, varchar_c, time_c) values(4, '2015-01-01','1970-01-01 08:00:00.000002');
insert into type_t(int_c, varchar_c, time_c) values(5, '1','1970-01-01 08:00:00.000001');
insert into type_t(int_c, varchar_c) values(6, '2');
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

CREATE TABLE exterface_invoke_001 (
  id number(20) NOT NULL,
  partner_id varchar(64) NOT NULL,
  target varchar(128) NOT NULL,
  exterface varchar(256) NOT NULL,
  notify_url varchar(400) DEFAULT NULL,
  return_url varchar(400) DEFAULT NULL,
  sign_type varchar(20) DEFAULT NULL,
  charset varchar(40) DEFAULT NULL,
  gmt_invoke DATE NOT NULL,
  gmt_finish DATE NOT NULL,
  target_type varchar(40) NOT NULL,
  gmt_notify DATE DEFAULT NULL,
  gmt_return DATE DEFAULT NULL,
  extra varchar(4000)  DEFAULT NULL,
  partition_id varchar(8) GENERATED ALWAYS AS (substr(target,18,2)) VIRTUAL,
  PRIMARY KEY (target_type, target, partner_id)
)   partition by list (partition_id) (partition pdefault values (default))
;
