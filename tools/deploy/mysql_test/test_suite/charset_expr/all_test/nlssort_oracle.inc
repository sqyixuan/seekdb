--disable_query_log
set @@session.explicit_defaults_for_timestamp=off;
--enable_query_log

# owner: jim.wjh
# owner group: SQL2
# description: test nlssort expr
# tags: expr, datatype

--result_format 4

--error 0, 942
drop table t1;
--error 0, 942
drop table  t_nlssort_test;
--error 0, 942
drop view v_nlssort_test;

create table t_nlssort_test (c1 varchar2(100), c2 nvarchar2(100), c3 char(10), c4 nchar(10));
insert into t_nlssort_test(c1) values('各'),('地'),('贯'),('彻'),('十'),('九'),('届'),('六'),('中'),('全'),('会'),('精'),('神'),('纪'),('实');
update t_nlssort_test set c2=c1,c3=c1,c4=c1;

select varchar_bin, varchar_nlssort_bin, varchar_nlssort_pinyin, nvarchar2_bin, nvarchar2_nlssort_bin, nvarchar2_nlssort_pinyin, char_bin, char_nlssort_bin, char_nlssort_pinyin, nchar_bin, nchar_nlssort_bin, nchar_nlssort_pinyin from
(select rownum from t_nlssort_test) temp
join (select rownum, c1 varchar_bin from (select c1 from t_nlssort_test order by nlssort(c1))) p1 on temp.rownum = p1.rownum
join (select rownum, c1 varchar_nlssort_bin from (select c1 from t_nlssort_test order by nlssort(c1))) p2 on temp.rownum = p2.rownum
join (select rownum, c1 varchar_nlssort_pinyin from (select c1 from t_nlssort_test order by nlssort(c1, 'NLS_SORT=UCA0900_SCHINESE_PINYIN'))) p3 on temp.rownum = p3.rownum
join (select rownum, c2 nvarchar2_bin from (select c2 from t_nlssort_test order by nlssort(c2))) p4 on temp.rownum = p4.rownum
join (select rownum, c2 nvarchar2_nlssort_bin from (select c2 from t_nlssort_test order by nlssort(c2))) p5 on temp.rownum = p5.rownum
join (select rownum, c2 nvarchar2_nlssort_pinyin from (select c2 from t_nlssort_test order by nlssort(c2, 'NLS_SORT=UCA0900_SCHINESE_PINYIN'))) p6 on temp.rownum = p6.rownum
join (select rownum, c3 char_bin from (select c3 from t_nlssort_test order by nlssort(c3))) p7 on temp.rownum = p7.rownum
join (select rownum, c3 char_nlssort_bin from (select c3 from t_nlssort_test order by nlssort(c3))) p8 on temp.rownum = p8.rownum
join (select rownum, c3 char_nlssort_pinyin from (select c3 from t_nlssort_test order by nlssort(c3, 'NLS_SORT=UCA0900_SCHINESE_PINYIN'))) p9 on temp.rownum = p9.rownum
join (select rownum, c4 nchar_bin from (select c4 from t_nlssort_test order by nlssort(c4))) p10 on temp.rownum = p10.rownum
join (select rownum, c4 nchar_nlssort_bin from (select c4 from t_nlssort_test order by nlssort(c4))) p11 on temp.rownum = p11.rownum
join (select rownum, c4 nchar_nlssort_pinyin from (select c4 from t_nlssort_test order by nlssort(c4, 'NLS_SORT=UCA0900_SCHINESE_PINYIN'))) p12 on temp.rownum = p12.rownum
;

create view v_nlssort_test as select nlssort(c1, 'NLS_SORT=UCA0900_SCHINESE_PINYIN') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c2, 'NLS_SORT=UCA0900_SCHINESE_PINYIN') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c3, 'NLS_SORT=UCA0900_SCHINESE_PINYIN') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c4, 'NLS_SORT=UCA0900_SCHINESE_PINYIN') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c1, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c2, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c3, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c4, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;

select nlssort(c1, '') from t_nlssort_test;
select nlssort('', '') from dual;

drop table t_nlssort_test;

create table t_nlssort_test(c1 varchar2(1000));
insert into t_nlssort_test values ('不'), ('与'), ('丏'), ('丐'), ('丑'), ('丒'), ('专'), ('中'), ('丮'), ('丯'), ('丰'), ('丹'), ('为'), ('之'), ('乌'), ('尹'), ('乣'), ('乤'), ('乥'), ('书'), ('予'), ('云'), ('互'), ('亓'), ('五'), ('井'), ('亖'), ('亢'), ('亣'), ('什'), ('仁');
select c1, nlssort(c1, 'NLS_SORT=UCA0900_SCHINESE_STROKE') sortkey from t_nlssort_test order by 2;
delete from t_nlssort_test;

insert into t_nlssort_test values ('各'),('地'),('贯'),('彻'),('十'),('九'),('届'),('六'),('中'),('全'),('会'),('精'),('神'),('纪'),('实');
select c1, nlssort(c1, 'NLS_SORT=UCA0900_SCHINESE_RADICAL') sortkey from t_nlssort_test order by 2;
select c1, nlssort(c1, 'NLS_SORT=UCA0900_SCHINESE_STROKE') sortkey from t_nlssort_test order by 2;

drop table t_nlssort_test;
drop view v_nlssort_test;

create table t_nlssort_test (c1 varchar2(100), c2 nvarchar2(100), c3 char(10), c4 nchar(10));
insert into t_nlssort_test(c1) values('各'),('地'),('贯'),('彻'),('十'),('九'),('届'),('六'),('中'),('全'),('会'),('精'),('神'),('纪'),('实');
update t_nlssort_test set c2=c1,c3=c1,c4=c1;
select varchar_bin, varchar_nlssort_bin, varchar_nlssort_pinyin, nvarchar2_bin, nvarchar2_nlssort_bin, nvarchar2_nlssort_pinyin, char_bin, char_nlssort_bin, char_nlssort_pinyin, nchar_bin, nchar_nlssort_bin, nchar_nlssort_pinyin from
(select rownum from t_nlssort_test) temp
join (select rownum, c1 varchar_bin from (select c1 from t_nlssort_test order by nlssort(c1))) p1 on temp.rownum = p1.rownum
join (select rownum, c1 varchar_nlssort_bin from (select c1 from t_nlssort_test order by nlssort(c1))) p2 on temp.rownum = p2.rownum
join (select rownum, c1 varchar_nlssort_pinyin from (select c1 from t_nlssort_test order by nlssort(c1, 'NLS_SORT=SCHINESE_PINYIN_M'))) p3 on temp.rownum = p3.rownum
join (select rownum, c2 nvarchar2_bin from (select c2 from t_nlssort_test order by nlssort(c2))) p4 on temp.rownum = p4.rownum
join (select rownum, c2 nvarchar2_nlssort_bin from (select c2 from t_nlssort_test order by nlssort(c2))) p5 on temp.rownum = p5.rownum
join (select rownum, c2 nvarchar2_nlssort_pinyin from (select c2 from t_nlssort_test order by nlssort(c2, 'NLS_SORT=SCHINESE_PINYIN_M'))) p6 on temp.rownum = p6.rownum
join (select rownum, c3 char_bin from (select c3 from t_nlssort_test order by nlssort(c3))) p7 on temp.rownum = p7.rownum
join (select rownum, c3 char_nlssort_bin from (select c3 from t_nlssort_test order by nlssort(c3))) p8 on temp.rownum = p8.rownum
join (select rownum, c3 char_nlssort_pinyin from (select c3 from t_nlssort_test order by nlssort(c3, 'NLS_SORT=SCHINESE_PINYIN_M'))) p9 on temp.rownum = p9.rownum
join (select rownum, c4 nchar_bin from (select c4 from t_nlssort_test order by nlssort(c4))) p10 on temp.rownum = p10.rownum
join (select rownum, c4 nchar_nlssort_bin from (select c4 from t_nlssort_test order by nlssort(c4))) p11 on temp.rownum = p11.rownum
join (select rownum, c4 nchar_nlssort_pinyin from (select c4 from t_nlssort_test order by nlssort(c4, 'NLS_SORT=SCHINESE_PINYIN_M'))) p12 on temp.rownum = p12.rownum
;
create view v_nlssort_test as select nlssort(c1, 'NLS_SORT=SCHINESE_PINYIN_M') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c2, 'NLS_SORT=SCHINESE_PINYIN_M') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c3, 'NLS_SORT=SCHINESE_PINYIN_M') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c4, 'NLS_SORT=SCHINESE_PINYIN_M') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c1, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c2, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c3, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
create view v_nlssort_test as select nlssort(c4, 'NLS_SORT=BINARY') R1 from t_nlssort_test;
desc v_nlssort_test;
drop view v_nlssort_test;
select nlssort(c1, '') from t_nlssort_test;
select nlssort('', '') from dual;
select nlssort(unistr('\FFFD'), 'NLS_SORT=SCHINESE_PINYIN_M') from dual;
select nlssort(unistr('\FFFF'), 'NLS_SORT=SCHINESE_PINYIN_M') from dual;
select nlssort(chr(to_number('FFFE','xxxx')）, 'NLS_SORT=SCHINESE_PINYIN_M') from dual;

create table t1(c1 char(11), c2 varchar(11), c3 nchar(11), c4 varchar2(11));
insert into t1 values ('abc', 'abc', 'abc', 'abc');
insert into t1 values ('abc ', 'abc ', 'abc ', 'abc ');
insert into t1 values ('a bc ', 'a bc ', 'a bc ', 'a bc ');
insert into t1 values (' abc ', ' abc ', ' abc ', ' abc ');
insert into t1 values ('我', '我', '我', '我');
insert into t1 values ('我 ', '我 ', '我 ', '我 ');
insert into t1 values ('我是OB', '我是OB', '我是OB', '我是OB');
insert into t1 values('问', '问', '问', '问');
insert into t1 values('闯', '闯', '闯', '闯');
insert into t1 values('闖', '闖', '闖', '闖');
insert into t1 values('門', '門', '門', '門');
insert into t1 values('门', '门', '门', '门');
insert into t1 values('门门', '门门', '门门', '门门');
insert into t1 values('A', 'A', 'A', 'A');
insert into t1 values('b', 'b', 'b', 'b');
insert into t1 values('a', 'a', 'a', 'a');
insert into t1 values('B', 'B', 'B', 'B');
select nlssort(c1, 'NLS_SORT=SCHINESE_PINYIN_M') from t1;
select nlssort(c2, 'NLS_SORT=SCHINESE_PINYIN_M') from t1;
select nlssort(c3, 'NLS_SORT=SCHINESE_PINYIN_M') from t1;
select nlssort(c4, 'NLS_SORT=SCHINESE_PINYIN_M') from t1;

select nlssort('a', 'NLS_SORT=SCHINESE_PINYIN_M') from dual;

select c1, nlssort(c1, 'NLS_SORT=SCHINESE_PINYIN2_M') from t1 order by 2;
select c2, nlssort(c2, 'NLS_SORT=SCHINESE_PINYIN2_M') from t1 order by 2;
select c3, nlssort(c3, 'NLS_SORT=SCHINESE_PINYIN2_M') from t1 order by 2;
select c4, nlssort(c4, 'NLS_SORT=SCHINESE_PINYIN2_M') from t1 order by 2;


select c1, nlssort(c1, 'NLS_SORT=SCHINESE_RADICAL2_M') from t1 order by 2;
select c2, nlssort(c2, 'NLS_SORT=SCHINESE_RADICAL2_M') from t1 order by 2;
select c3, nlssort(c3, 'NLS_SORT=SCHINESE_RADICAL2_M') from t1 order by 2;
select c4, nlssort(c4, 'NLS_SORT=SCHINESE_RADICAL2_M') from t1 order by 2;

select c1, nlssort(c1, 'NLS_SORT=SCHINESE_STROKE2_M') from t1 order by 2;
select c2, nlssort(c2, 'NLS_SORT=SCHINESE_STROKE2_M') from t1 order by 2;
select c3, nlssort(c3, 'NLS_SORT=SCHINESE_STROKE2_M') from t1 order by 2;
select c4, nlssort(c4, 'NLS_SORT=SCHINESE_STROKE2_M') from t1 order by 2;

alter session set nls_sort = BINARY;
alter session set nls_sort = binary;
alter session set nls_sort = 'BINARY';

drop table t_nlssort_test;
drop table t1;
