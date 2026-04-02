
--disable_warnings

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_uint as c2 from v_int, v_uint) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_flt  as c2 from v_int, v_flt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_dbl  as c2 from v_int, v_dbl) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_nmb  as c2 from v_int, v_nmb) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_dttm as c2 from v_int, v_dttm where v_int.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_dt   as c2 from v_int, v_dt   where v_int.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_tm   as c2 from v_int, v_tm   where v_int.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_yr   as c2 from v_int, v_yr) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_int as c1, col_str  as c2 from v_int, v_str where abs(col_int + 0.0) < power(10, 16)) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_int  as c2 from v_uint, v_int) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_flt  as c2 from v_uint, v_flt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_dbl  as c2 from v_uint, v_dbl) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_nmb  as c2 from v_uint, v_nmb) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_dttm as c2 from v_uint, v_dttm where v_uint.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_dt   as c2 from v_uint, v_dt   where v_uint.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_tm   as c2 from v_uint, v_tm   where v_uint.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_yr   as c2 from v_uint, v_yr) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_uint as c1, col_str  as c2 from v_uint, v_str where col_uint < power(10, 16)) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_int  as c2 from v_flt, v_int) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_uint as c2 from v_flt, v_uint) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_dbl  as c2 from v_flt, v_dbl) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_nmb  as c2 from v_flt, v_nmb) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_dttm as c2 from v_flt, v_dttm where v_flt.col_type = 'dttm') as tmp order by c1, c2;
# disable float VS date.
# because 19700100 (day part is 00) is invalid for ob, but valid for mysql/mariadb.
##select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_dt   as c2 from v_flt, v_dt   where v_flt.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_tm   as c2 from v_flt, v_tm   where v_flt.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_yr   as c2 from v_flt, v_yr) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_flt as c1, col_str  as c2 from v_flt, v_str) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_int  as c2 from v_dbl, v_int) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_uint as c2 from v_dbl, v_uint) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_flt  as c2 from v_dbl, v_flt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_nmb  as c2 from v_dbl, v_nmb) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_dttm as c2 from v_dbl, v_dttm where v_dbl.col_type = 'dttm' and col_dbl = ceil(col_dbl)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_dt   as c2 from v_dbl, v_dt   where v_dbl.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_tm   as c2 from v_dbl, v_tm   where v_dbl.col_type = 'tm' and col_dbl = ceil(col_dbl)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_yr   as c2 from v_dbl, v_yr) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dbl as c1, col_str  as c2 from v_dbl, v_str) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_int  as c2 from v_nmb, v_int) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_uint as c2 from v_nmb, v_uint) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_flt  as c2 from v_nmb, v_flt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_dbl  as c2 from v_nmb, v_dbl) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_dttm as c2 from v_nmb, v_dttm where v_nmb.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_dt   as c2 from v_nmb, v_dt   where v_nmb.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_tm   as c2 from v_nmb, v_tm   where v_nmb.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_yr   as c2 from v_nmb, v_yr) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_nmb as c1, col_str  as c2 from v_nmb, v_str where abs(col_nmb) < power(10, 16)) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_int  as c2 from v_dttm, v_int  where v_int.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_uint as c2 from v_dttm, v_uint where v_uint.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_flt  as c2 from v_dttm, v_flt  where v_flt.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_dbl  as c2 from v_dttm, v_dbl  where v_dbl.col_type = 'dttm' and col_dbl = ceil(col_dbl)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_nmb  as c2 from v_dttm, v_nmb  where v_nmb.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_dt   as c2 from v_dttm, v_dt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_tm   as c2 from v_dttm, v_tm) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_yr   as c2 from v_dttm, v_yr   where not (col_dttm = 0 and col_yr = 0)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dttm as c1, col_str  as c2 from v_dttm, v_str  where v_str.col_type = 'dttm') as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_int  as c2 from v_dt, v_int  where v_int.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_uint as c2 from v_dt, v_uint where v_uint.col_type = 'dt') as tmp order by c1, c2;
# disable date VS float.
# because 19700100 (day part is 00) is invalid for ob, but valid for mysql/mariadb.
##select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_flt  as c2 from v_dt, v_flt  where v_flt.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_dbl  as c2 from v_dt, v_dbl  where v_dbl.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_nmb  as c2 from v_dt, v_nmb  where v_nmb.col_type = 'dt') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_dttm as c2 from v_dt, v_dttm) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_tm   as c2 from v_dt, v_tm) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_yr   as c2 from v_dt, v_yr   where not (col_dt = 0 and col_yr = 0)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_dt as c1, col_str  as c2 from v_dt, v_str  where v_str.col_type in ('dttm', 'dt')) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_int  as c2 from v_tm, v_int  where v_int.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_uint as c2 from v_tm, v_uint where v_uint.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_flt  as c2 from v_tm, v_flt  where v_flt.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_dbl  as c2 from v_tm, v_dbl  where v_dbl.col_type = 'tm' and col_dbl = ceil(col_dbl)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_nmb  as c2 from v_tm, v_nmb  where v_nmb.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_dttm as c2 from v_tm, v_dttm) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_dt   as c2 from v_tm, v_dt) as tmp order by c1, c2;
# disable time VS year.
# no reason! does it make any sence???
##select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_yr   as c2 from v_tm, v_yr) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_tm as c1, col_str  as c2 from v_tm, v_str  where v_str.col_type = 'tm') as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_int  as c2 from v_yr, v_int) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_uint as c2 from v_yr, v_uint) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_flt  as c2 from v_yr, v_flt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_dbl  as c2 from v_yr, v_dbl) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_nmb  as c2 from v_yr, v_nmb) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_dttm as c2 from v_yr, v_dttm where not (col_yr = 0 and col_dttm = 0)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_dt   as c2 from v_yr, v_dt   where not (col_yr = 0 and col_dt = 0)) as tmp order by c1, c2;
# disable year VS time.
# no reason! does it make any sence???
##select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_tm   as c2 from v_yr, v_tm) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_yr as c1, col_str  as c2 from v_yr, v_str) as tmp order by c1, c2;

select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_int  as c2 from v_str, v_int where abs(col_int + 0.0) < power(10, 16)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_uint as c2 from v_str, v_uint where col_uint < power(10, 16)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_flt  as c2 from v_str, v_flt) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_dbl  as c2 from v_str, v_dbl) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_nmb  as c2 from v_str, v_nmb where abs(col_nmb) < power(10, 16)) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_dttm as c2 from v_str, v_dttm  where v_str.col_type = 'dttm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_dt   as c2 from v_str, v_dt  where v_str.col_type in ('dttm', 'dt')) as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_tm   as c2 from v_str, v_tm  where v_str.col_type = 'tm') as tmp order by c1, c2;
select c1, c2, c1 < c2, c1 = c2, c1 > c2 from (select distinct col_str as c1, col_yr   as c2 from v_str, v_yr) as tmp order by c1, c2;

--enable_warnings
