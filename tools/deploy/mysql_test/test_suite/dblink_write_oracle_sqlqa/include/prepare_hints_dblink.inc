ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
SET NAMES UTF8;

--echo #dblink
--disable_warnings
--error 0,942
drop table t_deduce_length CASCADE CONSTRAINTS purge;
--error 0,942
drop table t_deduce_length2 CASCADE CONSTRAINTS purge;
--enable_warnings

create table t_deduce_length(
	c_int int,
    c_number number,
	c_number_l number(10,3),
	c_float float(5),
	c_binary_float BINARY_FLOAT,
	c_binary_double BINARY_DOUBLE,
	c_blob blob,
    c_date date,
    c_timestamp timestamp,
    c_raw raw(200),
    c_char char(200 byte),
    c_char_c char(200 char),
    c_varchar2 varchar2(200 byte),
    c_varchar2_c varchar2(200 char),
    c_varchar2_l varchar2(2000 byte),
    c_nchar nchar(200),
    c_nvarchar nvarchar2(200),
    c_nvarchar_l nvarchar2(2000),
    c_clob clob
)partition by range(c_int)
subpartition by hash(c_varchar2_c) subpartitions 4
(partition part1 values less than(5),
partition part2 values less than(10),
partition part3 values less than(15),
partition part4 values less than(200));

insert into t_deduce_length values(1,1,1,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(2,2,2,2.02,2.0,2.00,'a',to_date('2002-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试002','a','aa','中文测试001','a','bb','中文测试001','中文测试001');
insert into t_deduce_length values(3,3,3,3.03,3.0,3.00,'a',to_date('2003-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试003','a','bb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length values(4,4,4,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');


create table t_deduce_length2(
	c_int2 int,
    c_number2 number,
	c_number_l2 number(10,3),
	c_float2 float(5),
	c_binary_float2 BINARY_FLOAT,
	c_binary_double2 BINARY_DOUBLE,
	c_blob2 blob,
    c_date2 date,
    c_timestamp2 timestamp,
    c_raw2 raw(200),
    c_char2 char(200 byte),
    c_char_c2 char(200 char),
    c_varchar22 varchar2(200 byte),
    c_varchar2_c2 varchar2(200 char),
    c_varchar2_l2 varchar2(2000 byte),
    c_nchar2 nchar(200),
    c_nvarchar2 nvarchar2(200),
    c_nvarchar_l2 nvarchar2(2000),
    c_clob2 clob
)partition by range(c_int2)
subpartition by hash(c_varchar2_c2) subpartitions 4
(partition part1 values less than(5),
partition part2 values less than(10),
partition part3 values less than(15),
partition part4 values less than(200));


insert into t_deduce_length2 values(11,11,11,1.01,1.0,1.00,'a',to_date('2001-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试001','a','aa','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(12,12,12,2.02,2.0,2.00,'a',to_date('2002-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试002','a','aa','中文测试001','a','bb','中文测试001','中文测试001');
insert into t_deduce_length2 values(13,13,13,3.03,3.0,3.00,'a',to_date('2003-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','aa','中文测试003','a','bb','中文测试001','a','aa','中文测试001','中文测试001');
insert into t_deduce_length2 values(14,14,14,4.04,4.0,4.00,'a',to_date('2004-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc','中文测试004','a','aa','中文测试001','a','aa','中文测试001','中文测试001');



COMMIT;