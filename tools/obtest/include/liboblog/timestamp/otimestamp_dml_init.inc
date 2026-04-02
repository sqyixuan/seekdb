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

# 创建t10, t12, t14, t16, t18, t110
# 创建t20, t22, t24, t26, t28, t210
# 创建t30, t32, t34, t36, t38, t310

create table t110(id int, c1 timestamp with time zone);
ob1.oblog.next;
create table t210(id int, c1 timestamp with local time zone);
ob1.oblog.next;
create table t310(id int, c1 timestamp);
ob1.oblog.next;

let $cnt=0;
while ($cnt <= 9)
{
  eval create table t1$cnt(id int, c1 timestamp($cnt) with time zone);
  ob1.oblog.next;
  eval create table t2$cnt(id int, c1 timestamp($cnt) with local time zone);
  ob1.oblog.next;
  eval create table t3$cnt(id int, c1 timestamp($cnt));
  ob1.oblog.next;

  inc $cnt;
  inc $cnt;
}

let $time_str01 = '1991-06-02 08:00:00';
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

#let $time_str11 = '1991-06-02 08:00:00';
#let $time_str12 = '1991-06-02 08:00:00.123456';
#let $time_str13 = '1991-06-02 08:00:00.123456789';
#let $time_str14 = '1991-06-02 08:00:00.123456789';
#let $time_str15 = '1991-10-02 08:00:00.123456789';
#let $time_str16 = '1900-04-13 08:00:00.123456789';
#let $time_str17 = '1991-06-02 08:00:00.123456789 +09:00';
#let $time_str18 = '1991-06-02 08:00:00.123456789 -07:10';
#let $time_str19 = '1991-06-02 08:00:00.123456789 Asia/Shanghai';
#let $time_str110 = '1991-06-02 08:00:00.123456789 Asia/Shanghai CDT';
#let $time_str111 = '1991-10-02 08:00:00.123456789 Asia/Shanghai';
#let $time_str112 = '1991-10-02 08:00:00.123456789 Asia/Shanghai CST';
#let $time_str113 = '1900-04-13 08:00:00.123456789 Asia/Shanghai';
#let $time_str114 = '1900-04-13 08:00:00.123456789 Asia/Shanghai LMT';
#let $time_str115 = '1991-09-14 23:00:00.123456789 Asia/Shanghai';
#let $time_str116 = '1991-09-14 23:00:00.123456789 Asia/Shanghai CST';

let $time_str21 = Timestamp '1991-06-02 08:00:00';
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
