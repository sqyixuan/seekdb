# http://bugs.mysql.com/bug.php?id=79571
# drop table if exists t1;
# create table t1(a bigint);
# insert into t1 values (-9223372036854775808), (-9223372036854775807), (9223372036854775806), (9223372036854775807);
# select ceil(a) from t1 order by 1;
# select distinct ceil(a) from t1 order by 1;
#
# so we add where condition to int / uint / nmb columns.

--disable_warnings
select distinct floor(col_int) from v_int where abs(col_int + 0.0) < power(10, 16) order by 1;
select distinct floor(col_uint) from v_uint where abs(col_uint + 0.0) < power(10, 16) order by 1;
select distinct floor(col_flt) from v_flt where abs(col_flt) < power(10, 16) order by 1;
select distinct floor(col_dbl) from v_dbl where abs(col_dbl) < power(10, 16) order by 1;
select distinct floor(col_nmb) from v_nmb where abs(col_nmb + 0.0) < power(10, 16) order by 1;
select distinct floor(col_dttm) from v_dttm order by 1;
select distinct floor(col_dt) from v_dt order by 1;
select distinct floor(col_tm) from v_tm order by 1;
select distinct floor(col_yr) from v_yr order by 1;
select distinct floor(col_str) from v_str where length(col_str) < 16 order by 1;
--enable_warnings
