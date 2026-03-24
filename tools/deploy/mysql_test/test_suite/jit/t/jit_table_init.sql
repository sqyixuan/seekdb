create table jit_t1(int_c INT default 5,
                    tinyint_c TINYINT default 2,
                    bool_c BOOL default 1,
                    smallint_c SMALLINT default 0,
                    mediumint_c MEDIUMINT default 3,
                    integer_c INTEGER default 4,
                    bigint_c BIGINT default 6,
                    float_c FLOAT default 1.1,
                    double_c DOUBLE default 1.2,
                    decimal_c DECIMAL(6,3) default 11.11,
                    numeric_c NUMERIC(6,3) default 22.22,
                    char_c CHAR(100) default 'ccc',
                    varchar_c VARCHAR(100) default 'sss',
                    binary_c BINARY(100) default 'sss',
                    date_c DATE,
                    datetime_c DATETIME,
                    time_c    TIME,
                    year_c    YEAR);

INSERT into jit_t1 values(0, 0, 0, 0, 0, 0, 0, 0.1, 0.1, 0.1, 0.1, 'goodman', 'goodman', 'goodman', '2015-01-01', '2015-01-01 01:01:01', '01:01:01', 70);
INSERT into jit_t1 values(1, 1, 1, 1, 1, 1, 1, 0.11, 0.11, 0.11, 0.11, 'good', 'good', 'good', '2015-02-01', '2015-01-01 02:02:02', '02:02:02', 1980);
INSERT into jit_t1 values(2, 2, true, 2, 2, 2, 2, 1.11, 1.11, 1.11, 1.11, 'helloworld', 'helloworld', 'helloworld', '2015-01-06', '2015-01-01 00:00:00', '02:02:02', 1980);
insert into jit_t1(int_c, bool_c, bigint_c, varchar_c, datetime_c) values(3, false, 10, '2015-01-06','2015-11-11 00:00:00');
insert into jit_t1(int_c, bigint_c, varchar_c, datetime_c) values(4, -4, '2015-01-01','1970-01-01 08:00:00.000002');
insert into jit_t1(int_c, bigint_c, varchar_c, datetime_c) values(5, 4, '1','1970-01-01 08:00:00.000001');
insert into jit_t1(int_c, bigint_c, varchar_c) values(6, 12, '2');
insert into jit_t1(int_c, varchar_c) values(7, 'helloeveryone');
insert into jit_t1(int_c, varchar_c) values(8, 'hello');
insert into jit_t1(int_c, varchar_c) values(9, 'HELLO');
insert into jit_t1(int_c, varchar_c) values(10, 'HELLOWORLD');
insert into jit_t1(int_c, varchar_c) values(11, '80');
insert into jit_t1(int_c, varchar_c) values(12, 'FF');
insert into jit_t1(int_c, varchar_c) values(13, '0123');
insert into jit_t1(int_c, varchar_c) values(14, '0.0.0.1');
insert into jit_t1(int_c, varchar_c) values(-1, '0.0.0.21');
insert into jit_t1(int_c, varchar_c) values(-2, '1111');
insert into jit_t1(int_c, varchar_c) values(21, '15');
insert into jit_t1 values(23, NULL, NULL, NULL,NULL, NULL, NULL, NULL,NULL, NULL, NULL, NULL,NULL, NULL, NULL, NULL,NULL,NULL);
# dayincome
create table dayincome(pubid bigint, settleamount bigint, settletype tinyint, realamount bigint, transType varchar(100), subType tinyint, earningsource bigint, earningtype tinyint, commision bigint);
insert into dayincome values(40, NULL, 1, NULL, 'CPSA', NULL, 24341404, NULL, 33);
insert into dayincome values(11, 50, 2, 48, 'WAPP', 0, 17534123, 14, 20);
insert into dayincome values(34, 25, 2, 23, 'CPSV', 4, 17825897, 14, 2);
insert into dayincome values(NULL, 25, NULL, 23, NULL, 4, NULL, 14, NULL);
insert into dayincome values(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
