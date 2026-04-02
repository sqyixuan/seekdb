--echo ##################################all bugs verify##################################

--echo ##预期01481##
--error 01481
select to_number(20080320, '.0EEEE') from dual;
--echo ##预期01481##
--error 01481
select to_number(20080320, '.9EEEE') from dual;
--echo ##预期01481##
--error 01481
select to_number(123, '999999V999CEEEE') from dual;
--echo ##十六进制转换中format model中出现小数表达式时，结果无法理解。所以目前禁止出现小数表达式，直接报错##
--error 01481
select to_number(to_char(1234.34,'0000.00X')) from dual;
--echo ##预期01481##
--error 01481
select to_number(485,'99V9CEEEE') from dual;
--echo ##预期01481##
--error 01481
select to_number(-485,'999VCEEEE') from dual;
--echo ##预期无报错##
select to_number(23, '99BU99') from dual;
--echo ##预期01481##
--error 01481
select to_number(100000000, '999,999,V999,,,') from dual;
--echo ##预期01481##
--error 01481
select to_number(100.2356, 'V99999,99') from dual;
--echo ##预期01481##
--error 01481
select to_number(100.2356, '9V,99999999') from dual;
--echo ##预期01481##
--error 01481
select to_number(12345+12345, 'V9,9,9,9,9') from dual;
--echo ##预期01481##
--error 01481
select to_number(12345*5, '9,9,9,V9,9') from dual;
--echo ##预期01481##
--error 01481
select to_number(52345/5, '9,9V,9,9,9') from dual;
--echo ##预期01481##
--error 01481
select to_number(0.00, '9,V9,9,9,9') from dual;
--echo ##预期01481##
--error 01481
select to_number(0+0, '9,9,V9,9,9') from dual;
--echo ##预期01481##
--error 01481
select to_number(0*0, '9,9V,9,9,9') from dual;
--echo ##预期01722##
--echo ##to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错##
--error 01481
select to_number(0.1,'0.0XXXX')  from dual;
--echo ##预期01722##
--error 01722
select to_number(0.1,'0,0XXX')  from dual;
--echo ##预期01722##
--error 01722
select to_number('00000','0,0XXX')  from dual;
--echo ##预期01481##
--error 01481
select to_number(12345, '0,,,0,,V0,0,0') from dual;
--echo ##预期01481##
--error 01481
select to_number(100.2356, '99V999,99') from dual;
--echo ##预期01481##
--error 01481
select to_number(1485,'9V,999') from dual;
--echo ##预期01722##
--error 01722
select to_number(12345,'9G9G9G9G9eeee') from dual;
--echo ##预期01722##
--error 01722
select to_number(123.456, '99g9D999EEEE') from dual;
--echo ##预期01722##
--error 01722
select to_number('1,E+04','9G9G9G9G9eeee') from dual;
--echo ##预期01722##
--error 01722
select to_number('1.235E+02', '99g9D999EEEE') from dual;
--echo ##预期01481##
--error 01481
select to_number(100000000,'999G999G99V9GGG') from dual;
--echo ##预期01481##
--error 01481
select to_number(100000000, '999,$999,99V9,,,') from dual;
--echo ##预期无报错##
--echo ##不支持负数，直接报错
--error 01722
select to_number(-485,'XXX') from dual;
--echo ##预期无报错##
--echo ##to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错##
--error 01481
select to_number(1234.34,'0000.00X') from dual;
--echo ##预期01722##
--error 01722
select to_number(12345, '0000XX') from dual;
--echo ##预期01722##
--error 01722
select to_number(123, '000XX') from dual;
--echo ##预期无报错##
select to_number(0*0, '9CB') from dual;
--echo ##预期无报错##
select to_number(0+0, '9LB') from dual;
--echo ##预期无报错##
select to_number(0, '0UB') from dual;
--echo ##预期无报错##
--echo ##to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错##
--error 01481
select substrb(to_number(1234.34,'0000.00X'),3,5) from dual;

--echo ##预期无报错##
--echo ##to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错##
--error 01481
SELECT to_char(COALESCE(to_char(1234.34,'0000.00X'),to_char(1234.34,'0000.00X'),'3','4','5')) FROM dual;
--echo ##预期无报错##
--echo ##to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错##
--error 01481
select to_char(1234.34,'0000.00X') from dual;
--echo ##预期无报错##
select to_number(to_char(1,'9BEEEE'),'9BEEEE') from dual;
--echo ##预期无报错##
select to_number(to_char(123, '9999999999EEEE'), '9999999999EEEE') from dual;
--echo ##预期无报错##
select to_number('123.123USD', '999.999C') from dual;
--echo ##预期无报错##
select to_number('123.123USD', '999.999c') from dual;
--echo ##预期无报错##
select to_number('23USD00', '99BC99') from dual;
--echo ##预期无报错##
select to_number('23.12USD', '99.99BC') from dual;
--echo ##预期无报错##
select to_number('23.12USD', '99.99BCPR') from dual;
--echo ##预期无报错##
select to_number('23.12USD+', '99.99BCS') from dual;
--echo ##预期无报错##
select to_number('2E+07', '9EEEE') from dual;
--echo ##预期无报错##
select to_number('1E+02', '9999999999EEEE') from dual;
--echo ##预期无报错##
--echo #--echo #to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错
--error 01481
select to_number('04D2','0000.00X') from dual;
--echo ##预期无报错##
--echo #--echo #不支持负数，直接报错
--error 01722
select to_number(-485,'XXX') from dual;
--echo ##预期无报错##
--echo #--echo #不支持负数，直接报错
--error 01722
select to_number(-485,'FMXXX') from dual;
--echo ##预期无报错##
--echo #--echo #to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错
--error 01481
select to_number(1234.34,'0000.00X') from dual;
--echo ##预期01722##
--error 01722
select to_number(100.00, 'FM0XXX') from dual;
--echo ##预期01722##
--error 01722
select to_number(12,'000XX') from dual;
--echo ##预期01722##
--error 01722
select to_number(485,'000XXX') from dual;
--echo ##预期01722##
--error 01722
select to_number(1485,'000XXX') from dual;
--echo ##预期01722##
--error 01722
select to_number(0, '000xxx') from dual;
--echo ##预期01722##
--error 01722
select to_number(0.00, '000xx') from dual;
--echo ##预期01722##
--error 01722
select to_number(0+0, '000xxxxx') from dual;
--echo ##预期01722##
--error 01722
select to_number(0*0, '000xx') from dual;
--echo ##预期正常##
select to_number('.22','FM9,9,9,9.99Mi') from dual;
--echo ##预期01481##
--error 01481
select to_number(+123.456,' 999.999') from dual;
--echo ##预期01481##
--error 01481
select to_number(-123.456,' 999.999') from dual;
--echo ##预期01481##
--error 01481
select to_number(+123.456,' FM999.009') from dual;
--echo ##预期正常，实际丢失连接##
--error 01722
select to_number(12,'9990999.9') from dual;
--echo ##预期01722##
--error 01722
select to_number(32, '0xx') from dual;
--echo ##预期01722##
--error 01722
select to_number(32, '0xX') from dual;
--echo ##预期01722##
--error 01722
select to_number(32, '0XX') from dual;
--echo ##预期正常##
select to_char(12345,'9G9G9G9G9eeee') from dual;
--echo ##预期正常##
select to_char(123.456, '99g9D999EEEE') from dual;
--echo ##预期正常##
select to_char(12345,'9G9G9G9G9eeee') from dual;
--echo ##预期5$E+02##
select to_char(485,'999UEEEE') from dual;
--echo ##预期-5$E+02##
select to_char(-485,'999UEEEE') from dual;
--echo ##预期01481##
--error 01481
select to_char(12345, '9,,,9,,9,9,9LB') from dual;
--echo ##预期0##
select to_char(0.1,'fmXX')  from dual;
--echo ##预期0##
select to_char(0.1,'FMXXX')  from dual;
--echo ##预期正常##
select to_number(to_char(1*0.3, 'FM9B')) from dual;
--echo ##预期1E+0##
select substrb( to_char(123, '9999999999EEEE'),2,5) from dual;
--echo ##预期1##
--echo #REGEXP_COUNT实现的问题，转owner处理
select REGEXP_COUNT(to_char(1234, 'L9999Mi') ,'$',1,'i') from DUAL;
--echo ##预期1##
--echo #REGEXP_COUNT实现的问题，转owner处理
select REGEXP_COUNT( to_char(123, '999999999L') ,'$',1,'i') from DUAL;
--echo ##预期1##
--echo #REGEXP_COUNT实现的问题，转owner处理
select REGEXP_COUNT(to_char(23.12, '99D99BUS') ,'$',1,'i') from DUAL;
--echo ##预期1##
--echo #REGEXP_COUNT实现的问题，转owner处理
select REGEXP_COUNT(to_char(52345/5, '9,9,9,$9,9') ,'$',1,'i') from DUAL;

select to_char(23.12, '99D99BUS') from DUAL;
select to_char(52345/5, '9,9,9,$9,9') from DUAL;
--echo ##预期00904##
--echo #parser的问题，转owner处理
--error 600
select to_char(ABCD.AB,'9,9,9,9.99') from dual;
--echo ##预期.0E+00##
select to_char(-0.0, '9.9EEEE') from dual;
--echo ##预期.0E+00##
select to_char(0.0, '9.9EEEE') from dual;
--echo ##预期0.0E+00##
select to_char(-0.0, '0.0EEEE') from dual;
--echo ##预期2.E+07##
select to_char(20080320, '9.EEEE') from dual;
--echo ##预期4.18E+01##
select to_char(123*0.34,'9999.99EEEE') from dual;
--echo ##预期01481##
--error 01481
select to_char(123, '999999V999CEEEE') from dual;
--echo ##预期01481##
--error 01481
select to_char(485,'99V9CEEEE') from dual;
--echo ##预期01481##
--error 01481
select to_char(-485,'999VCEEEE') from dual;
--echo ##预期左对齐##
select to_char(1234, 'tme') from dual;
--echo ##预期左对齐##
select to_char(1234, 'tm9') from dual;
--echo ##预期-1E-01##
select to_char(-0.1,'tme') from dual;
--echo ##预期1E-01##
select to_char(0.1,'tme')  from dual;
--echo ##预期0E+00##
select to_char(12345-12345, 'tme') from dual;
--echo ##预期0E+00##
select to_char(0, 'tme') from dual;
--echo ##预期0E+00##
select to_char(0.00, 'tme') from dual;
--echo ##预期0E+00##
select to_char(0+0, 'tme') from dual;
--echo ##预期0E+00##
select to_char(0*0, 'tme') from dual;
--echo ##预期200E+00##
--echo #不兼容，直接使用科学计数法表示
select to_char(2, '99v99EEEE') from dual;
--echo ##预期2000E+00##
--echo #不兼容，直接使用科学计数法表示
select to_char(2, '9v999EEEE') from dual;
--echo ##预期01481##
--error 01481
select to_char(100000000, '999,999,V999,,,') from dual;
--echo ##预期0##
select to_char(-0.1,'XX') from dual;
--echo ##预期0##
select to_char(0.1,'XX')  from dual;
--echo ##预期0##
select to_char(0.1,'XXX')  from dual;
--echo ##预期0##
select to_char(12345-12345, 'XXXX') from dual;
--echo ##预期f11d##
select to_char(12345*5, 'xxxXXXX') from dual;
--echo ##预期28e5##
select to_char(52345/5, 'xXXXXx') from dual;
--echo ##预期00000##
select to_char(0.1,'000XX')  from dual;
--echo ##预期04D2##
--echo #--echo #to_number format model 中出现小数表达式时，结果无法理解。所以目前禁止 format model中出现小数表达式，直接报错
--error 01481
select to_char(1234.34,'0000.00X') from dual;
--echo ##预期为空##
select to_char(0,'B90.99') from dual;
--echo ##预期01481##
--error 01481
select to_char(+123.456,' 999.999') from dual;
--echo ##预期01481##
--error 01481
select to_char(-123.456,' 999.999') from dual;
--echo ##预期01481##
--error 01481
select to_char(+123.456,' FM999.009') from dual;
--echo ##预期1E+65##
select to_char(99999999999999999999999999999999999999999999999999999999999999999, 'tme') from dual;
--echo ##预期1E+65##
select to_char(99999999999999999999999999999999999999999999999999999999999999999, 'tm9') from dual;
--echo ##预期1E+64##
select to_char(9999999999999999999999999999999999999999999999999999999999999999, 'tme') from dual;
--echo ##预期1E+64##
select to_char(9999999999999999999999999999999999999999999999999999999999999999, 'tm9') from dual;
--echo ##预期1E+63##
select to_char(999999999999999999999999999999999999999999999999999999999999999, 'tme') from dual;
--echo ##预期1000000000000000000000000000000000000000000000000000000000000000##
select to_char(999999999999999999999999999999999999999999999999999999999999999, 'tm9') from dual;
--echo ##预期OBE-01426: numeric overflow##
--echo #number表示范围的问题，转owner处理
--error 01426
select to_char(999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999990, '9,9999,9999') from dual;
--echo ##预期############
--echo #number溢出问题，暂时不兼容
--error 01426
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '999999V999') from dual;
--echo ##预期1E+125##
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fmtm9') from dual;
--echo ##预期##################
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '99999,99VC') from dual;
--echo ##预期################
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9$Mi') from dual;
--echo ##预期#############
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,9,9,9,9vPR') from dual;
--echo ##预期#######
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fm9999s') from dual;
--echo ##预期###########
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9CB') from dual;
--echo ##预期#####################
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'FM9,9999,9999c') from dual;
--echo ##预期###############
select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9B') from dual;


--disable_warnings
--error 0,942
drop table format_test1;
--enable_warnings
create table format_test1(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test1 values(29989329,1234.34,1234.34,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(29989330,100.2356,100.2356,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
--echo #原因是不支持将 blob cast转换成sting类型，但是对to_char转换了也没有太大的意义。现在只是报错错误码不一致，暂不兼容
--echo ##预期01722##
--error 00932
select to_char(c8, '9,$99999999B') from format_test1;
--echo ##预期01722##
--error 00932
select to_char(c8,  'fmRn') from format_test1;
--echo ##预期01722##
--error 00932
select to_char(c8, '999999V999S') from format_test1;
--disable_warnings
--error 0,942
drop table format_test1;
--enable_warnings


--disable_warnings
--error 0,942
drop table format_test4;
--enable_warnings
create table format_test4(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test4 values(100000000,100.2356,0.00,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test4 values(100000000,0.00,0+0,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

--echo ##预期正常##
ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_char(100000000,'99G9999999v')) VIRTUAL;
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_char(1000,'99G9999999v')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_char(10000000000, '9,9999,9999BL')) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_char(0.000, 'FMRn')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_char(100.2356, '99999,99VC')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_char(1485,'XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_char(12345, '9,,,9,,9,9,9$Mi')) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_char(0*0, '9CB')) VIRTUAL;
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_char(to_char(0.1*5, '9CB'))) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_char(1234, 'fmtm9')) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_char(29989329, '9.99EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_char(123*0.34,'9999.9')) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_char(123, '9999V999999EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c24 varchar2(200) GENERATED ALWAYS AS (replace(to_char(100.00, '9,$99999999B'),'$100','好的好的')) VIRTUAL;
ALTER TABLE format_test4 ADD c25 varchar2(200) GENERATED ALWAYS AS (concat(to_char(1234.34,'0000d00l'), to_char(100000000,'99G9999999v'))) VIRTUAL;
ALTER TABLE format_test4 ADD c26 varchar2(200) GENERATED ALWAYS AS (COALESCE(to_char(0*0, '9CB'),to_char(1485,'XXX'),'3','4','5')) VIRTUAL;
ALTER TABLE format_test4 ADD c27 varchar2(200) GENERATED ALWAYS AS (LTRIM(to_char(485,'XXX'))) VIRTUAL;

--disable_warnings
--error 0,942
drop table format_test4;
--enable_warnings

--error 0,600
drop procedure 中文存储过程1;

--echo ######存储过程-一般赋值运算-字符表达式,NVARCHAR2######
delimiter //;

--disable_warnings
create or replace procedure 中文存储过程1
is
中文列1 NVARCHAR2(200);
中文列3 NVARCHAR2(200);
中文列4 NVARCHAR2(200);
中文列5 NVARCHAR2(200);
中文列6 NVARCHAR2(200);
begin
中文列1:=TRIM(to_char(0*0, '9CB'));
中文列3:=concat( to_char(12345,'9G9G9G9g9'), to_char(100000000,'99G9999999v'));
中文列4:=nvl(null,to_char(12345+12345, 'tM9'));
中文列5:=REGEXP_COUNT(to_char(1234, 'L9999Mi') ,'$',1,'i');
中文列6:=nvl(to_char('', 'fmtme'),to_char('', 'fmtme'));
dbms_output.put_line(中文列1);
dbms_output.put_line(中文列3);
dbms_output.put_line(中文列4);
dbms_output.put_line(中文列5);
dbms_output.put_line(中文列6);
end;
//
delimiter ;//
--enable_warnings
call 中文存储过程1();

--error 0,600
drop procedure 中文存储过程1;