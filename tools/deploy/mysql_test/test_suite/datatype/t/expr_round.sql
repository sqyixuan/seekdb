# change where condition to "col_type != 'str_cn'" for col_str
# after replace old cast func with new cast func.

--disable_warnings
select distinct round(col_int) from v_int order by 1;
select distinct round(col_uint) from v_uint order by 1;
select distinct round(col_flt) from v_flt where abs(col_flt) < power(10, 16) order by 1;
select distinct round(col_dbl) from v_dbl where abs(col_dbl) < power(10, 16) order by 1;
select distinct round(col_nmb) from v_nmb order by 1;
select distinct round(col_dttm) from v_dttm order by 1;
select distinct round(col_dt) from v_dt order by 1;
select distinct round(col_tm) from v_tm order by 1;
select distinct round(col_yr) from v_yr order by 1;
select distinct round(col_str) from v_str where length(col_str) < 16 and col_type in ('int', 'uint', 'flt', 'dbl', 'nmb') order by 1;
--enable_warnings
