#
# Will test from the followe dimension
#
# operation : select/insert/replace/update/delete
# scale : empty, enough, overflow
# timezone input : empty, offset, tz name, tz name + tzd abbr, invalid value
# timezone var : offset, tz name, invalid value
# storage: memtable, sstable

# mysql> select t1.time_zone_id, t1.name, t3.transition_time, from_unixtime(t3.transition_time), t2.offset, t2.is_dst, t2.Transition_type_id, t2.abbreviation  from oceanbase.__all_time_zone_name t1  join oceanbase.__all_time_zone_transition_type t2  on t1.time_zone_id = t2.time_zone_id  left join oceanbase.__all_time_zone_transition t3  on t2.time_zone_id = t3.time_zone_id and t2.transition_type_id=t3.transition_type_id  where t2.Time_zone_id=300 order by time_zone_id, transition_time;
# +--------------+---------------+-----------------+-----------------------------------+--------+--------+--------------------+--------------+
# | time_zone_id | name          | transition_time | from_unixtime(t3.transition_time) | offset | is_dst | Transition_type_id | abbreviation |
# +--------------+---------------+-----------------+-----------------------------------+--------+--------+--------------------+--------------+
# |          300 | Asia/Shanghai |            NULL | NULL                              |  29152 |      0 |                  0 | LMT          |
# |          300 | Asia/Shanghai |     -1325491552 | NULL                              |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |      -933494400 | NULL                              |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |      -923130000 | NULL                              |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |      -908784000 | NULL                              |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |      -891594000 | NULL                              |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |       515520000 | 1986-05-04 00:00:00               |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |       527007600 | 1986-09-13 23:00:00               |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |       545155200 | 1987-04-12 00:00:00               |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |       558457200 | 1987-09-12 23:00:00               |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |       576604800 | 1988-04-10 00:00:00               |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |       589906800 | 1988-09-10 23:00:00               |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |       608659200 | 1989-04-16 00:00:00               |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |       621961200 | 1989-09-16 23:00:00               |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |       640108800 | 1990-04-15 00:00:00               |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |       653410800 | 1990-09-15 23:00:00               |  28800 |      0 |                  2 | CST          |
# |          300 | Asia/Shanghai |       671558400 | 1991-04-14 00:00:00               |  32400 |      1 |                  1 | CDT          |
# |          300 | Asia/Shanghai |       684860400 | 1991-09-14 23:00:00               |  28800 |      0 |                  2 | CST          |
# +--------------+---------------+-----------------+-----------------------------------+--------+--------+--------------------+--------------+
# 18 rows in set (0.02 sec)
# 

connection default;

--disable_query_log
--disable_result_log
--disable_warnings

--error 0, 600
drop user sjhdb cascade;
create user sjhdb identified by sjhdb;

--error 0, 900
grant dba to sjhdb;
--error 0, 600
ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
--error 0, 600
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
--error 0, 600
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

alter session set current_schema = sjhdb;

--echo
--echo
--echo  ######################## prepare ###########################

--error 0, 2289
drop sequence sjhseq;
create sequence sjhseq start with 1 cache 2 minvalue 0 maxvalue 1000000000;
#sjhseq.nextval

create table t110(id int, c1 timestamp with time zone);
create table t210(id int, c1 timestamp with local time zone);
create table t310(id int, c1 timestamp);
create table t40(id int, c1 date);

let $cnt=0;
while ($cnt <= 9)
{
  eval create table t1$cnt(id int, c1 timestamp($cnt) with time zone);
  eval create table t2$cnt(id int, c1 timestamp($cnt) with local time zone);
  eval create table t3$cnt(id int, c1 timestamp($cnt));
  inc $cnt;
}

let $time_str01 = '1991-06-02 08:00:00.0';
let $time_str02 = '1991-06-02 08:00:00.123456';
let $time_str03 = '1991-06-02 08:00:00.123456789';
let $time_str04 = '1991-06-02 08:00:00.123456789';
let $time_str05 = '1991-10-02 08:00:00.123456789';
let $time_str06 = '1900-04-13 08:00:00.123456789';
let $time_str07 = '1991-06-02 08:00:00.123456789 +09:00';
let $time_str08 = '1991-06-02 08:00:00.123456789 -07:10';
let $time_str09 = '1991-06-02 08:00:00.123456789 Asia/Shanghai';
let $time_str010 = '1991-06-02 08:00:00.123456789 Asia/Shanghai CDT';
let $time_str011 = '1991-10-02 08:00:00.123456789 Asia/Shanghai';
let $time_str012 = '1991-10-02 08:00:00.123456789 Asia/Shanghai CST';
let $time_str013 = '1900-04-13 08:00:00.123456789 Asia/Shanghai';
let $time_str014 = '1900-04-13 08:00:00.123456789 Asia/Shanghai LMT';
let $time_str015 = '1991-09-14 23:00:00.123456789 Asia/Shanghai';
let $time_str016 = '1991-09-14 23:00:00.123456789 Asia/Shanghai CST';

let $time_str11 = '1991-06-02 08:00:00.0';
let $time_str12 = '1991-06-02 08:00:00.123456';
let $time_str13 = '1991-06-02 08:00:00.123456789';
let $time_str14 = '1991-06-02 08:00:00.123456789';
let $time_str15 = '1991-10-02 08:00:00.123456789';
let $time_str16 = '1900-04-13 08:00:00.123456789';
let $time_str17 = '1991-06-02 08:00:00.123456789 +09:00';
let $time_str18 = '1991-06-02 08:00:00.123456789 -07:10';
let $time_str19 = '1991-06-02 08:00:00.123456789 Asia/Shanghai';
let $time_str110 = '1991-06-02 08:00:00.123456789 Asia/Shanghai CDT';
let $time_str111 = '1991-10-02 08:00:00.123456789 Asia/Shanghai';
let $time_str112 = '1991-10-02 08:00:00.123456789 Asia/Shanghai CST';
let $time_str113 = '1900-04-13 08:00:00.123456789 Asia/Shanghai';
let $time_str114 = '1900-04-13 08:00:00.123456789 Asia/Shanghai LMT';
let $time_str115 = '1991-09-14 23:00:00.123456789 Asia/Shanghai';
let $time_str116 = '1991-09-14 23:00:00.123456789 Asia/Shanghai CST';

let $time_str21 = Timestamp '1991-06-02 08:00:00.0';
let $time_str22 = Timestamp '1991-06-02 08:00:00.123456';
let $time_str23 = Timestamp'1991-06-02 08:00:00.123456789';
let $time_str24 = Timestamp'1991-06-02 08:00:00.123456789';
let $time_str25 = Timestamp '1991-10-02 08:00:00.123456789';
let $time_str26 = Timestamp '1900-04-13 08:00:00.123456789';
let $time_str27 = Timestamp '1991-06-02 08:00:00.123456789 +09:00';
let $time_str28 = Timestamp'1991-06-02 08:00:00.123456789 -07:10';
let $time_str29 = Timestamp '1991-06-02 08:00:00.123456789 Asia/Shanghai';
let $time_str210 = Timestamp'1991-06-02 08:00:00.123456789 Asia/Shanghai CDT';
let $time_str211 = Timestamp '1991-10-02 08:00:00.123456789 Asia/Shanghai';
let $time_str212 = Timestamp'1991-10-02 08:00:00.123456789 Asia/Shanghai CST';
let $time_str213 = Timestamp '1900-04-13 08:00:00.123456789 Asia/Shanghai';
let $time_str214 = Timestamp'1900-04-13 08:00:00.123456789 Asia/Shanghai LMT';
let $time_str215 = Timestamp '1991-09-14 23:00:00.123456789 Asia/Shanghai';
let $time_str216 = Timestamp'1991-09-14 23:00:00.123456789 Asia/Shanghai CST';

#OBE-01841: (full) year must be between -4713 and +9999, and not be 0
let $time_str101 = '-1991-06-02 08:00:00.123456789 Asia/Shanghai';

#OBE-01882: timezone region not found
let $time_str102 = '1991-06-02 08:00:00.123456789 Asia/Shangha';

#OBE-01857: not a valid time zone
let $time_str103 = '1991-06-02 08:00:00.123456789 Asia/Shanghai PDT';

#OBE-01857: not a valid time zone
let $time_str104 = '1991-06-02 08:00:00.123456789 Asia/Shanghai CST';

#OBE-01878: specified field not found in datetime or interval
let $time_str105 = '1991-04-14 00:00:00.123456789 Asia/Shanghai';

#OBE-01857: not a valid time zone
let $time_str106 = '1991-04-14 00:00:00.123456789 Asia/Shanghai CDT';

#OBE-01873: the leading precision of the interval is too small
let $time_str107 = '1991-04-14 00:00:00.1234567896';

#OBE-01847: day of month must be between 1 and last day of month
let $time_str108 = '0000-00-00 00:00:00';
let $time_str109 = Timestamp '0000-00-00 00:00:00';




let $ymd_ok_cnt=4;
create table t_ymd_ok(id int, c1 varchar(20));
insert into t_ymd_ok values (1,'0001-01-02');
insert into t_ymd_ok values (2,'1970-01-01');
insert into t_ymd_ok values (3,'1972-02-29');
insert into t_ymd_ok values (4,'9999-12-30');

let $hms_ok_cnt=5;
create table t_hms_ok(id int, c1 varchar(20));
insert into t_hms_ok values (1,' 00:00:00');
insert into t_hms_ok values (2,' 23:59:59');
insert into t_hms_ok values (3,' 00:00:01');
insert into t_hms_ok values (4,' 00:01:00');
insert into t_hms_ok values (5,' 01:00:00');

let $ns_ok_cnt=7;
create table t_ns_ok(id int, c1 varchar(20));
insert into t_ns_ok values (1,'.0');
insert into t_ns_ok values (2,'.000000001');
insert into t_ns_ok values (3,'.000001');
insert into t_ns_ok values (4,'.0100');
insert into t_ns_ok values (5,'.499999999');
insert into t_ns_ok values (6,'.999999999');
insert into t_ns_ok values (7,'.123456789');

let $tz_ok_cnt=7;
create table t_tz_ok(id int, c1 varchar(20));
insert into t_tz_ok values (1,' -00:00');
insert into t_tz_ok values (2,' -15:59');
insert into t_tz_ok values (3,' +15:59');
insert into t_tz_ok values (4,' +00:10');
insert into t_tz_ok values (5,' -00:10');
insert into t_tz_ok values (6,' +01:00');
insert into t_tz_ok values (7,' -01:00');

create table t_tz2_ok(id int, c1 varchar(20));
insert into t_tz2_ok values (1,' -00:00');
insert into t_tz2_ok values (2,' -15:59');
insert into t_tz2_ok values (3,' +15:00');
insert into t_tz2_ok values (4,' +00:10');
insert into t_tz2_ok values (5,' -00:10');
insert into t_tz2_ok values (6,' +01:00');
insert into t_tz2_ok values (7,' -01:00');

let $spe_ok_cnt=10;
create table t_spe_ok(id int, c1 varchar(80));
insert into t_spe_ok values (1,'1991-06-02 08:00:00.123456789 Asia/Shanghai');
insert into t_spe_ok values (2,'1991-06-02 08:00:00.123456789 Asia/Shanghai CDT');
insert into t_spe_ok values (3,'1991-10-02 08:00:00.123456789 Asia/Shanghai');
insert into t_spe_ok values (4,'1991-10-02 08:00:00.123456789 Asia/Shanghai CST');
insert into t_spe_ok values (5,'1991-09-14 23:00:00.123456789 Asia/Shanghai');
insert into t_spe_ok values (6,'1991-09-14 23:00:00.123456789 Asia/Shanghai CST');
insert into t_spe_ok values (7,'1900-04-13 08:00:00.123456789 Asia/Shanghai');
insert into t_spe_ok values (8,'1900-04-13 08:00:00.123456789 Asia/Shanghai LMT');
insert into t_spe_ok values (9,'0001-01-01 00:00:00.0 +00:00');
insert into t_spe_ok values (10,'9999-12-31 23:59:59.999999999 +00:00');

commit;

--enable_query_log
--enable_result_log
--enable_warnings
