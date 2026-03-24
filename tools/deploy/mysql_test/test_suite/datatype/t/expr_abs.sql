
--disable_warnings
select distinct abs(col_int) from v_int where col_int > -9223372036854775808 order by 1;
select distinct abs(col_uint) from v_uint order by 1;
select distinct abs(col_flt) from v_flt order by 1;
select distinct abs(col_dbl) from v_dbl order by 1;
select distinct abs(col_nmb) from v_nmb order by 1;
select distinct abs(col_dttm) from v_dttm order by 1;
select distinct abs(col_dt) from v_dt order by 1;
select distinct abs(col_tm) from v_tm order by 1;
select distinct abs(col_yr) from v_yr order by 1;
select distinct abs(col_str) from v_str order by 1;
--enable_warnings
