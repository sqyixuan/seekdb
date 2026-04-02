
# clear data.
source mysql_test/test_suite/datatype/t/clear_data.sql;

--disable_warnings

# data table.
create table t (
  id        int auto_increment primary key, 
  col_type  varchar(10),
  col_int   bigint,
  col_uint  bigint unsigned,
  col_flt   float,
  col_dbl   double,
  col_nmb   decimal(30, 6),
  col_dttm  datetime(1),
  col_dt    date,
  col_tm    time(3),
  col_yr    year,
  col_str   varchar(100)
) character set binary;

# data views.
create view v_int as
  select distinct col_type, col_int from t where col_int is not null;
create view v_uint as
  select distinct col_type, col_uint from t where col_uint is not null;
create view v_flt as
  select distinct col_type, col_flt from t where col_flt is not null;
create view v_dbl as
  select distinct col_type, col_dbl from t where col_dbl is not null;
create view v_nmb as
  select distinct col_type, col_nmb from t where col_nmb is not null;
create view v_dttm as
  select distinct col_type, col_dttm from t where col_dttm is not null;
create view v_dt as
  select distinct col_type, col_dt from t where col_dt is not null;
create view v_tm as
  select distinct col_type, col_tm from t where col_tm is not null;
create view v_yr as
  select distinct col_type, col_yr from t where col_yr is not null;
create view v_str as
  select distinct col_type, col_str from t where col_str is not null;

# test data.
set sql_mode='';
insert into t (col_type, col_int) values
  ('int', -9223372036854775808),
  ('int', -9223372036854775807),
  ('int', 2),
  ('int', 3),
  ('int', 5),
  ('int', 8),
  ('int', 1000000000),
  ('int', 9223372036854775806),
  ('int', 9223372036854775807);
insert into t (col_type, col_uint) values
  ('uint', 0),
  ('uint', 1),
  ('uint', 1000000000000000000),
  ('uint', 18446744073709551614),
  ('uint', 18446744073709551615);
insert into t (col_type, col_nmb) values
  ('num', -92233720368547758090.5),
  ('num', -92233720368547758090),
  ('num', -9223372036854775809.5),
  ('num', -9223372036854775809),
  ('num', -9223372036854775808.5),
  ('num', -9223372036854775807.5),
  ('num', 0.5),
  ('num', 1.5),
  ('num', 9223372036854775807.4),
  ('num', 9223372036854775808.4),
  ('num', 9223372036854775809.4),
  ('num', 18446744073709551614.4),
  ('num', 18446744073709551615.4),
  ('num', 18446744073709551616),
  ('num', 18446744073709551616.5),
  ('num', 184467440737095516160),
  ('num', 184467440737095516160.5);
insert into t (col_type, col_dttm) values
  ('dttm', 0), 
  ('dttm', '1000-01-01 00:00:00.0'),
  ('dttm', '1234-12-23 02:15:16.2'),
  ('dttm', '1799-06-18 06:07:31.4'),
  ('dttm', '1970-01-01 12:00:00.0'),
  ('dttm', '2015-10-16 15:34:21.3'),
  ('dttm', '2037-01-01 22:20:17.0'),
  ('dttm', '9999-12-31 23:59:59.4');
insert into t (col_type, col_dt) values
  ('dt', 0),
  ('dt', '1000-01-01'),
  ('dt', '1234-12-23'),
  ('dt', '1799-06-18'),
  ('dt', '1970-01-01'),
  ('dt', '2015-10-16'),
  ('dt', '2037-01-01'),
  ('dt', '9999-12-31');
insert into t (col_type, col_tm) values
  ('tm', '-838:59:59'),
  ('tm', '-256:25:17.19'),
  ('tm', 0), 
  ('tm', '02:15:16.2'),
  ('tm', '06:07:31.4'),
  ('tm', '12:00:00.0'),
  ('tm', '15:34:21.3'),
  ('tm', '22:20:17.0'),
  ('tm', '23:59:59.4'),
  ('tm', '625:53:23.23'),
  ('tm', '838:59:59');
insert into t (col_type, col_yr) values
  ('yr', 0),
  ('yr', 1901),
  ('yr', 1970),
  ('yr', 1999),
  ('yr', 2015),
  ('yr', 2037),
  ('yr', 2155);
insert into t (col_type, col_str) values
  ('str_en', '-9223372036854775808a'),
  ('str_en', '0a'),
  ('str_en', '9223372036854775807a'),
  ('str_en', '18446744073709551615a'),
  ('str_en', '151016153421.8'),
  ('str_en', '151016153421.8a'),
  ('str_en', '370101222017.9'),
  ('str_en', '370101222017.9a'),
  ('str_en', '151016'),
  ('str_en', '151016a'),
  ('str_en', '370101'),
  ('str_en', '370101a'),
  ('str_en', '153421.3'),
  ('str_en', '153421.3a'),
  ('str_en', '222017.0'),
  ('str_en', '222017.0a'),
  ('str_en', '15'),
  ('str_en', '15.5'),
  ('str_en', '15a'),
  ('str_en', 'abc'),
  ('str_cn', '阿里巴巴'),
  ('str_cn', '支付宝');
update t set
  col_uint = col_int
  where col_type = 'int' and col_int >= 0;
update t set
  col_int = col_uint
  where col_type = 'uint' and col_uint <= 9223372036854775807;
update t set
  col_flt = col_int,
  col_dbl = col_int,
  col_nmb = col_int,
  col_str = col_int
  where col_type = 'int';
update t set
  col_flt = col_uint,
  col_dbl = col_uint,
  col_nmb = col_uint,
  col_str = col_uint
  where col_type = 'uint';
update t set
  col_flt = col_nmb,
  col_dbl = col_nmb,
  col_str = col_nmb
  where col_type = 'num';
update t set
  col_int  = col_dttm,
  col_uint = col_dttm,
  col_flt  = col_dttm,
  col_dbl  = col_dttm,
  col_nmb  = col_dttm,
  col_dt   = col_dttm,
  col_tm   = col_dttm,
  col_str  = col_dttm
  where col_type = 'dttm';
update t set
  col_int  = cast(col_dt as signed),
  col_uint = cast(col_dt as unsigned),
  col_flt  = col_dt,
  col_dbl  = col_dt,
  col_nmb  = col_dt,
  col_str  = col_dt
  where col_type = 'dt';
update t set
  col_int  = col_tm,
  col_uint = col_tm,
  col_flt  = col_tm,
  col_dbl  = col_tm,
  col_nmb  = col_tm,
  col_str  = col_tm
  where col_type = 'tm';
update t set
  col_int  = col_yr,
  col_uint = col_yr,
  col_flt  = col_yr,
  col_dbl  = col_yr,
  col_nmb  = col_yr
  where col_type = 'yr';
update t set
  col_str  = col_yr
  where col_type = 'yr' and col_yr > 0;

# check data.
select * from t order by id;

--enable_warnings
