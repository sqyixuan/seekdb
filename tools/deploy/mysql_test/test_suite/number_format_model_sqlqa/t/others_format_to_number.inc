--echo ##################################数据类型-表 number_format_model-2-5##################################

--disable_warnings
--error 0,942
drop table format_test1;
--enable_warnings
create table format_test1(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test1 values(29989329,1234.34,1234.34,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(29989330,100.2356,100.2356,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));


select to_number(c1, '99999999') from format_test1;
select to_number(c1, '99999999B') from format_test1;
###--error 01722,00939
###select to_number(c2, '999.99999B') from format_test1;
--error 01722
select to_number(c1, '9.99EEEE') from format_test1;
--error 01722
select to_number(c1, '0000d00l') from format_test1;
--error 01722
select to_number(c1, '99999,99U') from format_test1;
--error 00939
select to_number(c2,'9,$99999999B') from format_test1;
--error 00939
select to_number(c2,'fmRn') from format_test1;
--error 00939
select to_number(c2,'999999V999S') from format_test1;
--error 00939
select to_number(c3,  'FM0XXX') from format_test1;
--error 00939
select to_number(c3,  '9,,,9,,9,9,9$Mi') from format_test1;
--error 00939
select to_number(c3, 'FM9B') from format_test1;
--error 01722
select to_number(c4, '9.99EEEE') from format_test1;
--error 01722
select to_number(c4, '0000d00l') from format_test1;
--error 01722
select to_number(c4, '99999,99U') from format_test1;
--error 01722
select to_number(c5, '9,$99999999B') from format_test1;
--error 01722
select to_number(c5,  'fmRn') from format_test1;
--error 01722
select to_number(c5, '999999V999S') from format_test1;
--error 01722
select to_number(c6,  'FM0XXX') from format_test1;
--error 01722
select to_number(c6,  '9,,,9,,9,9,9$Mi') from format_test1;
--error 01722
select to_number(c6, 'FM9B') from format_test1;
--error 01722
select to_number(c7, '9.99EEEE') from format_test1;
--error 01722
select to_number(c7, '0000d00l') from format_test1;
--error 01722
select to_number(c7, '99999,99U') from format_test1;
--error 01722,600
select to_number(c8, '9,$99999999B') from format_test1;
--error 01722,600
select to_number(c8,  'fmRn') from format_test1;
--error 01722,600
select to_number(c8, '999999V999S') from format_test1;
--error 01722
select to_number(c9,  'FM0XXX') from format_test1;
--error 01722
select to_number(c9,  '9,,,9,,9,9,9$Mi') from format_test1;
--error 01722
select to_number(c9, 'FM9B') from format_test1;
--error 01722
select to_number(c10,  'FM0XXX') from format_test1;
--error 01722
select to_number(c10,  '9,,,9,,9,9,9$Mi') from format_test1;
--error 01722
select to_number(c10, 'FM9B') from format_test1;


--disable_warnings
--error 0,942
drop table format_test2;
--enable_warnings
create table format_test2(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test2 values(100000000,100.2356,0.00,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test2 values(100000000,0.00,0+0,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

select to_number(to_char(c1, '9.99EEEE')) from format_test2;
--error 01722
select to_number(to_char(c1, '0000d00l')) from format_test2;
--error 01722
select to_number(to_char(c1, '99999,99U')) from format_test2;
--error 01722
select to_number(to_char(c2, '9,$99999999B')) from format_test2;
--error 01722
select to_number(to_char(c2,  'fmRn')) from format_test2;
--error 01722
select to_number(to_char(c2, '999999V999S')) from format_test2;
select to_number(to_char(c3,  'FM0XXX')) from format_test2;
--error 01722
select to_number(to_char(c3,  '9,,,9,,9,9,9$Mi')) from format_test2;
###select to_number(to_char(c3, 'FM9B')) from format_test2;
select to_number(to_char(c4, '9.99EEEE')) from format_test2;
--error 01722
select to_number(to_char(c4, '0000d00l')) from format_test2;
--error 01722
select to_number(to_char(c4, '99999,99U')) from format_test2;
--error 01722
select to_number(to_char(c5, '9,$99999999B')) from format_test2;
--error 01722
select to_number(to_char(c5,  'fmRn')) from format_test2;
--error 01722
select to_number(to_char(c5, '999999V999S')) from format_test2;
select to_number(to_char(c6,  'FM0XXX')) from format_test2;
--error 01722
select to_number(to_char(c6,  '9,,,9,,9,9,9$Mi')) from format_test2;
--error 01722
select to_number(to_char(c6, 'FM9B')) from format_test2;
select to_number(to_char(c7, '9.99EEEE')) from format_test2;
--error 01722
select to_number(to_char(c7, '0000d00l')) from format_test2;
--error 01722
select to_number(to_char(c7, '99999,99U')) from format_test2;


--disable_warnings
--error 0,942
drop table format_test3;
--enable_warnings
create table format_test3(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
--error 00932
insert into format_test3 values(to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_number('1.23E+03','9999.00EEEE'),to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
--error 00932
insert into format_test3 values(to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_number('$1,0,4,6,9', '9,9,9,$9,9'),to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
--error 01722
insert into format_test3(c1) values(to_number(29989329, '9.99EEEE'));
--error 01722
insert into format_test3(c1) values(to_number(123*0.34,'9999.9'));
--error 01722
insert into format_test3(c1) values(to_number(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c1) values(to_number(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c1) values(to_number(100000000, '9,9999,9999BL'));
--error 01722
insert into format_test3(c1) values(to_number(123, '9999V999999EEEE'));
--error 01722
insert into format_test3(c1) values(to_number(0.000, 'FMRn'));
--error 01722
insert into format_test3(c1) values(to_number(100.2356, '99999,99VC'));
--error 01722
insert into format_test3(c1) values(to_number(1485,'XXX'));
--error 01722
insert into format_test3(c1) values(to_number(12345, '9,,,9,,9,9,9$Mi'));
--error 01722
insert into format_test3(c1) values(to_number(1234, 'fmtm9'));


###insert into format_test3(c1) values(to_number(0*0, '9CB'));
insert into format_test3(c1) values(to_number(to_char(12,'FM9990999.9pr')));

--error 01722
insert into format_test3(c2) values(to_number(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c2) values(to_number(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c2) values(to_number(100000000, '9,9999,9999BL'));
--error 01722
insert into format_test3(c2) values(to_number(0.000, 'FMRn'));
--error 01722
insert into format_test3(c2) values(to_number(100.2356, '99999,99VC'));
--error 01722
insert into format_test3(c2) values(to_number(1485,'XXX'));
--error 01722
insert into format_test3(c2) values(to_number(12345, '9,,,9,,9,9,9$Mi'));
--error 01722
insert into format_test3(c2) values(to_number(1234, 'fmtm9'));
--error 01722
insert into format_test3(c2) values(to_number(29989329, '9.99EEEE'));
--error 01722
insert into format_test3(c2) values(to_number(123*0.34,'9999.9'));
--error 01722
insert into format_test3(c2) values(to_number(123, '9999V999999EEEE'));

####insert into format_test3(c2) values(to_number(0*0, '9CB'));
###--error 01722
###insert into format_test3(c2) values(to_number(to_char(12,'FM9990999.9pr')));


--echo ##################################如果将BINARY_FLOAT或BINARY_DOUBLE值转换为CHAR或NCHAR，并且输入是无穷大或NaN（不是数字），则Oracle总是返回井号以替换该值。number_format_model-2-8-5##################################

--error 00904,00984
insert into format_test3(c2) values(to_number(NaN, '9999V999999EEEE'));
--error 00904,00984
insert into format_test3(c3) values(to_number(NaN, '9999V999999EEEE'));
--error 01722
insert into format_test3(c2) values(to_number(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9999V999999EEEE'));
--error 01722
insert into format_test3(c3) values(to_number(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9999V999999EEEE'));
--error 01722
insert into format_test3(c2) values(to_number('∞', '9999V999999EEEE'));
--error 01722
insert into format_test3(c3) values(to_number('∞', '9999V999999EEEE'));
###select * from format_test3;

--error 01722
insert into format_test3(c3) values(to_number(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c3) values(to_number(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c3) values(to_number(100000000, '9,9999,9999BL'));
--error 01722
insert into format_test3(c3) values(to_number(0.000, 'FMRn'));
--error 01722
insert into format_test3(c3) values(to_number(100.2356, '99999,99VC'));
--error 01722
insert into format_test3(c3) values(to_number(1485,'XXX'));
--error 01722
insert into format_test3(c3) values(to_number(12345, '9,,,9,,9,9,9$Mi'));
insert into format_test3(c3) values(to_number(to_char(12,'FM9990999.9pr')));
--error 01722
insert into format_test3(c3) values(to_number(1234, 'fmtm9'));
--error 01722
insert into format_test3(c3) values(to_number(29989329, '9.99EEEE'));
--error 01722
insert into format_test3(c3) values(to_number(123*0.34,'9999.9'));
--error 01722
insert into format_test3(c3) values(to_number(123, '9999V999999EEEE'));

select to_number(to_char(c1, '9.99EEEE'), '9.99EEEE') from format_test2;
select to_number(to_char(c1, '9.99EEEE'), '9.99EEEE') from format_test2;
--error 01722
select to_number(to_char(c1, '99999,99U'), '99999,99U') from format_test2;
###select to_number(to_char(c2, '9,$99999999B'), '9,$99999999B') from format_test2;
--error 01722
select to_number(to_char(c2,  'fmRn'),  'fmRn') from format_test2;
--error 01722
select to_number(to_char(c2, '999999V999S'), '999999V999S')  from format_test2;

select to_number(to_char(c3,  'FM0XXX'),  'FM0XXX') from format_test2;
select to_number(to_char(c3,  '9,,,9,,9,9,9$Mi'),  '9,,,9,,9,9,9$Mi') from format_test2;
###select to_number(to_char(c3, 'FM9B'), 'FM9B') from format_test2;

select to_number(to_char(c4, '9.99EEEE'), '9.99EEEE') from format_test2;
###select to_number(to_char(c4, '0000d00l'), '0000d00l') from format_test2;
###select to_number(to_char(c4, '99999,99U'), '99999,99U') from format_test2;
select to_number(to_char(c5, '9,$99999999B'), '9,$99999999B') from format_test2;
--error 01722
select to_number(to_char(c5,  'fmRn'),  'fmRn') from format_test2;
--error 01722
select to_number(to_char(c5, '999999V999S'), '999999V999S') from format_test2;
select to_number(to_char(c6,  'FM0XXX'),  'FM0XXX') from format_test2;
select to_number(to_char(c6,  '9,,,9,,9,9,9$Mi'),  '9,,,9,,9,9,9$Mi') from format_test2;
--error 01722
select to_number(to_char(c6, 'FM9B'), 'FM9B') from format_test2;
select to_number(to_char(c7, '9.99EEEE'), '9.99EEEE') from format_test2;
###select to_number(to_char(c7, '0000d00l'), '0000d00l') from format_test2;
###select to_number(to_char(c7, '99999,99U'), '99999,99U') from format_test2;

select to_number(to_char(c1, '9.99EEEE')) from format_test2;
select to_number(to_char(c3,  'FM0XXX')) from format_test2;
select to_number(to_char(c4, '9.99EEEE')) from format_test2;
select to_number(to_char(c6,  'FM0XXX')) from format_test2;
select to_number(to_char(c7, '9.99EEEE')) from format_test2;

--error 01722
select to_number(to_char(c1, '0000d00l')) from format_test2;
--error 01722
select to_number(to_char(c1, '99999,99U')) from format_test2;
--error 01722
select to_number(to_char(c2, '9,$99999999B')) from format_test2;
--error 01722
select to_number(to_char(c2,  'fmRn')) from format_test2;
--error 01722
select to_number(to_char(c2, '999999V999S')) from format_test2;
--error 01722
select to_number(to_char(c3,  '9,,,9,,9,9,9$Mi')) from format_test2;
--echo ##oracle无报错，跟oracle保持兼容##
select to_number(to_char(c3, 'FM9B')) from format_test2;
--error 01722
select to_number(to_char(c4, '0000d00l')) from format_test2;
--error 01722
select to_number(to_char(c4, '99999,99U')) from format_test2;
--error 01722
select to_number(to_char(c5, '9,$99999999B')) from format_test2;
--error 01722
select to_number(to_char(c5,  'fmRn')) from format_test2;
--error 01722
select to_number(to_char(c5, '999999V999S')) from format_test2;
--error 01722
select to_number(to_char(c6,  '9,,,9,,9,9,9$Mi')) from format_test2;
--error 01722
select to_number(to_char(c6, 'FM9B')) from format_test2;
--error 01722
select to_number(to_char(c7, '0000d00l')) from format_test2;
--error 01722
select to_number(to_char(c7, '99999,99U')) from format_test2;

--echo ##################################生成列中使用（虚拟列） number_format_model-2-6##################################

--disable_warnings
--error 0,942
drop table format_test4;
--enable_warnings
create table format_test4(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);

insert into format_test4 values(100000000,100.2356,0.00,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test4 values(100000000,0.00,0+0,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_number('1.23E+03','9999.00EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_number('123.00', '9999999D99')) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_number('1,00000000+','9G99999999S')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_number('1234+', 'fm9999s')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_number('0064', 'FM0XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_number(1234.34,'0000.00X')) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_number('', '9,9,9,V9,9MI')) VIRTUAL;
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_number('2.E+07', '9DEEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_number('', 'BC99MI')) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_number('$1,00', '99$999,99b')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_number('$23', 'FMU99')) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_number('$100', '9,$99999999B')) VIRTUAL;
--error 0,01418
alter table format_test4 drop column c11;
--error 0,01418
alter table format_test4 drop column c12;
--error 0,01418
alter table format_test4 drop column c13;
--error 0,01418
alter table format_test4 drop column c14;
--error 0,01418
alter table format_test4 drop column c15;
--error 0,01418
alter table format_test4 drop column c16;
--error 0,01418
alter table format_test4 drop column c17;
--error 0,01418
alter table format_test4 drop column c18;
--error 0,01418
alter table format_test4 drop column c19;
--error 0,01418
alter table format_test4 drop column c20;
--error 0,01418
alter table format_test4 drop column c21;
--error 0,01418
alter table format_test4 drop column c22;
--error 0,01418
alter table format_test4 drop column c23;
ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_number(to_number(123, '9999999.99'))) VIRTUAL;
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_number(to_number('0064', 'FM0XXX'))) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(2000) GENERATED ALWAYS AS (to_number(to_number('', 'FMB9d9'))) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_number(to_number('0012.+','FM9990999.9s'))) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_number(to_char(485,'XXX'),'XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_char(to_number(to_char(20080320.2365, '9.9EEEE')))) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_char(to_number(to_char(100.2356, '9,99999999Mi')))) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(2000) GENERATED ALWAYS AS (to_number(to_char(to_number('1.23E+03','9999.00EEEE')))) VIRTUAL;
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_number(to_char(to_number('1,,,,,,,,,,23,4.34','9,,,,,,,,,,99,9.99PR')))) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (floor(to_number(to_number('1.23E+03','9999.00EEEE')))) VIRTUAL;
###ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (translate(to_number('0064', 'FM0XXX'),'100','英文')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (cast(to_number(-1234, 's9b999') as varchar2(80))) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (RPAD( to_number('$1234.34', '9999d99$'), 2, '$')) VIRTUAL;
--error 0,01418
alter table format_test4 drop column c11;
--error 0,01418
alter table format_test4 drop column c12;
--error 0,01418
alter table format_test4 drop column c13;
--error 0,01418
alter table format_test4 drop column c14;
--error 0,01418
alter table format_test4 drop column c15;
--error 0,01418
alter table format_test4 drop column c16;
--error 0,01418
alter table format_test4 drop column c17;
--error 0,01418
alter table format_test4 drop column c18;
--error 0,01418
alter table format_test4 drop column c19;
--error 0,01418
alter table format_test4 drop column c20;
--error 0,01418
alter table format_test4 drop column c21;
--error 0,01418
alter table format_test4 drop column c22;
--error 0,01418
alter table format_test4 drop column c23;
ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_number(c1,'99G9999999v')) VIRTUAL;
--error 54015
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_number(c1,'99G9999999v')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_number(c1, '9,9999,9999BL')) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_number(c1, 'FMRn')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_number(c1, '99999,99VC')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_number(c1,'XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_number(c1, '9,,,9,,9,9,9$Mi')) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_number(c1, '9CB')) VIRTUAL;
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_number(to_char(c1, '9CB'))) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_number(c1, 'fmtm9')) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_number(c1, '9.99EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_number(c1,'9999.9')) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_number(c1, '9999V999999EEEE')) VIRTUAL;
--error 0,01418
alter table format_test4 drop column c11;
--error 0,01418
alter table format_test4 drop column c12;
--error 0,01418
alter table format_test4 drop column c13;
--error 0,01418
alter table format_test4 drop column c14;
--error 0,01418
alter table format_test4 drop column c15;
--error 0,01418
alter table format_test4 drop column c16;
--error 0,01418
alter table format_test4 drop column c17;
--error 0,01418
alter table format_test4 drop column c18;
--error 0,01418
alter table format_test4 drop column c19;
--error 0,01418
alter table format_test4 drop column c20;
--error 0,01418
alter table format_test4 drop column c21;
--error 0,01418
alter table format_test4 drop column c22;
--error 0,01418
alter table format_test4 drop column c23;

ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_number(c4,'99G9999999v')) VIRTUAL;
--error 54015
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_number(c4,'99G9999999v')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_number(c4, '9,9999,9999BL')) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_number(c4, 'FMRn')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_number(c4, '99999,99VC')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_number(c4,'XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_number(c4, '9,,,9,,9,9,9$Mi')) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_number(c4, '9CB')) VIRTUAL;
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_number(to_char(c4, '9CB'))) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_number(c4, 'fmtm9')) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_number(c4, '9.99EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_number(c4,'9999.9')) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_number(c4, '9999V999999EEEE')) VIRTUAL;
--error 0,01418
alter table format_test4 drop column c11;
--error 0,01418,00904
alter table format_test4 drop column c12;
--error 0,01418
alter table format_test4 drop column c13;
--error 0,01418
alter table format_test4 drop column c14;
--error 0,01418
alter table format_test4 drop column c15;
--error 0,01418
alter table format_test4 drop column c16;
--error 0,01418
alter table format_test4 drop column c17;
--error 0,01418
alter table format_test4 drop column c18;
--error 0,01418
alter table format_test4 drop column c19;
--error 0,01418
alter table format_test4 drop column c20;
--error 0,01418
alter table format_test4 drop column c21;
--error 0,01418
alter table format_test4 drop column c22;
--error 0,01418
alter table format_test4 drop column c23;

--error 0,600
ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_number(c8,'99G9999999v')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_number(c8,'99G9999999v')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_number(c8, '9,9999,9999BL')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_number(c8, 'FMRn')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_number(c8, '99999,99VC')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_number(c8,'XXX')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_number(c8, '9,,,9,,9,9,9$Mi')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_number(c8, '9CB')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_number(to_char(c8, '9CB'))) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_number(c8, 'fmtm9')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_number(c8, '9.99EEEE')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_number(c8,'9999.9')) VIRTUAL;
--error 0,600
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_number(c8, '9999V999999EEEE')) VIRTUAL;

--error 0,01418
alter table format_test4 drop column c11;
--error 0,01418
alter table format_test4 drop column c12;
--error 0,01418
alter table format_test4 drop column c13;
--error 0,01418
alter table format_test4 drop column c14;
--error 0,01418
alter table format_test4 drop column c15;
--error 0,01418
alter table format_test4 drop column c16;
--error 0,01418
alter table format_test4 drop column c17;
--error 0,01418
alter table format_test4 drop column c18;
--error 0,01418
alter table format_test4 drop column c19;
--error 0,01418
alter table format_test4 drop column c20;
--error 0,01418
alter table format_test4 drop column c21;
--error 0,01418
alter table format_test4 drop column c22;
--error 0,01418
alter table format_test4 drop column c23;


ALTER TABLE format_test4 ADD c11 varchar2(32767) GENERATED ALWAYS AS (to_number(c9,'99G9999999v')) VIRTUAL;
--error 54015
ALTER TABLE format_test4 ADD c12 varchar2(32767) GENERATED ALWAYS AS (to_number(c9,'99G9999999v')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, '9,9999,9999BL')) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, 'FMRn')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, '99999,99VC')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(32767) GENERATED ALWAYS AS (to_number(c9,'XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, '9,,,9,,9,9,9$Mi')) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, '9CB')) VIRTUAL;
--error 932
ALTER TABLE format_test4 ADD c19 varchar2(32767) GENERATED ALWAYS AS (to_number(to_char(c9, '9CB'))) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, 'fmtm9')) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, '9.99EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(32767) GENERATED ALWAYS AS (to_number(c9,'9999.9')) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(32767) GENERATED ALWAYS AS (to_number(c9, '9999V999999EEEE')) VIRTUAL;

--disable_warnings
--error 0,942
drop table format_test1;
--error 0,942
drop table format_test2;
--error 0,942
drop table format_test3;
--error 0,942
drop table format_test4;
--enable_warnings

--echo ##################################其他异常情况（存在正确性问题） number_format_model-2-8##################################

select to_char(0.1, 'B999C99') from dual;
select to_char(0.1, 'FMB999C99') from dual;
select to_char(0.1, 'BC99999') from dual;

###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '99999,99VC') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9$Mi') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,9,9,9,9vPR') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fm9999s') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9CB') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'FM9,9999,9999c') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9B') from dual;

--error 01722
select to_number('1USD', '99999,99VC') from dual;
--error 01722
select to_number('$1', '9,,,9,,9,9,9$Mi') from dual;
--error 01722
select to_number('1', '9,9,9,9,9vPR') from dual;

select to_number('1+', 'fm9999s') from dual;
###select to_number('1USD', '9CB') from dual;
select to_number('', 'FM9,9999,9999c') from dual;
select to_number('1', '9,,,9,,9,9,9B') from dual;

--error 01722
select to_number(-1, 'XX') from dual;
--error 01722
select to_number(-15, 'XX') from dual;
--error 01722
select to_number(-12312312, 'XXXXXXXXXXXX') from dual;
--error 01722
select to_number(-12312313, 'XXXXXXXXXXXX') from dual;