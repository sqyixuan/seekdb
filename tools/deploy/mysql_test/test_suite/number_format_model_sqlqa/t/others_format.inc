--echo ##################################数据类型-表 number_format_model-1-5##################################

--disable_warnings
--error 0,942
drop table format_test1;
--enable_warnings
create table format_test1(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test1 values(29989329,1234.34,1234.34,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(29989330,100.2356,100.2356,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

select to_char(c1, '9.99EEEE') from format_test1;
select to_char(c1, '0000d00l') from format_test1;
select to_char(c1, '99999,99U') from format_test1;
select to_char(c2, '9,$99999999B') from format_test1;
select to_char(c2,  'fmRn') from format_test1;
select to_char(c2, '999999V999S') from format_test1;
select to_char(c3,  'FM0XXX') from format_test1;
select to_char(c3,  '9,,,9,,9,9,9$Mi') from format_test1;
select to_char(c3, 'FM9B') from format_test1;
select to_char(c4, '9.99EEEE') from format_test1;
select to_char(c4, '0000d00l') from format_test1;
select to_char(c4, '99999,99U') from format_test1;
select to_char(c5, '9,$99999999B') from format_test1;
select to_char(c5,  'fmRn') from format_test1;
select to_char(c5, '999999V999S') from format_test1;
select to_char(c6,  'FM0XXX') from format_test1;
select to_char(c6,  '9,,,9,,9,9,9$Mi') from format_test1;
select to_char(c6, 'FM9B') from format_test1;
select to_char(c7, '9.99EEEE') from format_test1;
select to_char(c7, '0000d00l') from format_test1;
select to_char(c7, '99999,99U') from format_test1;

--error 01722,00932
select to_char(c8, '9,$99999999B') from format_test1;
--error 01722,00932
select to_char(c8,  'fmRn') from format_test1;
--error 01722,00932
select to_char(c8, '999999V999S') from format_test1;
--error 00932
select to_char(c9,  'FM0XXX') from format_test1;
--error 00932
select to_char(c9,  '9,,,9,,9,9,9$Mi') from format_test1;
--error 00932
select to_char(c9, 'FM9B') from format_test1;
--error 01821
select to_char(c10,  'FM0XXX') from format_test1;
--error 01821
select to_char(c10,  '9,,,9,,9,9,9$Mi') from format_test1;
--error 01821
select to_char(c10, 'FM9B') from format_test1;


--disable_warnings
--error 0,942
drop table format_test2;
--enable_warnings
create table format_test2(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test2 values(100000000,100.2356,0.00,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test2 values(100000000,0.00,0+0,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

select to_char(c1, '9.99EEEE') from format_test2;
select to_char(c1, '0000d00l') from format_test2;
select to_char(c1, '99999,99U') from format_test2;
select to_char(c2, '9,$99999999B') from format_test2;
select to_char(c2,  'fmRn') from format_test2;
###select to_char(c2, '999999V999S') from format_test2;
###select to_char(c3,  'FM0XXX') from format_test2;
select to_char(c3,  '9,,,9,,9,9,9$Mi') from format_test2;
select to_char(c3, 'FM9B') from format_test2;
select to_char(c4, '9.99EEEE') from format_test2;
select to_char(c4, '0000d00l') from format_test2;
select to_char(c4, '99999,99U') from format_test2;
select to_char(c5, '9,$99999999B') from format_test2;
select to_char(c5,  'fmRn') from format_test2;
select to_char(c5, '999999V999S') from format_test2;
select to_char(c6,  'FM0XXX') from format_test2;
select to_char(c6,  '9,,,9,,9,9,9$Mi') from format_test2;
select to_char(c6, 'FM9B') from format_test2;
select to_char(c7, '9.99EEEE') from format_test2;
select to_char(c7, '0000d00l') from format_test2;
select to_char(c7, '99999,99U') from format_test2;

select to_char(to_char(c1, '9.99EEEE')) from format_test2;
select to_char(to_char(c1, '0000d00l')) from format_test2;
select to_char(to_char(c1, '99999,99U')) from format_test2;
select to_char(to_char(c2, '9,$99999999B')) from format_test2;
select to_char(to_char(c2,  'fmRn')) from format_test2;
select to_char(to_char(c2, '999999V999S')) from format_test2;
select to_char(to_char(c3,  'FM0XXX')) from format_test2;
select to_char(to_char(c3,  '9,,,9,,9,9,9$Mi')) from format_test2;
select to_char(to_char(c3, 'FM9B')) from format_test2;
select to_char(to_char(c4, '9.99EEEE')) from format_test2;
select to_char(to_char(c4, '0000d00l')) from format_test2;
select to_char(to_char(c4, '99999,99U')) from format_test2;
select to_char(to_char(c5, '9,$99999999B')) from format_test2;
select to_char(to_char(c5,  'fmRn')) from format_test2;
select to_char(to_char(c5, '999999V999S')) from format_test2;
select to_char(to_char(c6,  'FM0XXX')) from format_test2;
select to_char(to_char(c6,  '9,,,9,,9,9,9$Mi')) from format_test2;
select to_char(to_char(c6, 'FM9B')) from format_test2;
select to_char(to_char(c7, '9.99EEEE')) from format_test2;
select to_char(to_char(c7, '0000d00l')) from format_test2;
select to_char(to_char(c7, '99999,99U')) from format_test2;


--disable_warnings
--error 0,942
drop table format_test3;
--enable_warnings
create table format_test3(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
--error 01722
insert into format_test3 values(to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_char(1234.34,'9,,,,,,,,,,99,9.99'),to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
--error 01465
insert into format_test3 values(to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_char(1234,'999900EEEE'),to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

insert into format_test3(c1) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c1) values(to_char(123*0.34,'9999.9'));
--error 01722
insert into format_test3(c1) values(to_char(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c1) values(to_char(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c1) values(to_char(100000000, '9,9999,9999BL'));
insert into format_test3(c1) values(to_char(123, '9999V999999EEEE'));
--error 01722
insert into format_test3(c1) values(to_char(0.000, 'FMRn'));
--error 01722
insert into format_test3(c1) values(to_char(100.2356, '99999,99VC'));
--error 01722
insert into format_test3(c1) values(to_char(1485,'XXX'));
--error 01722
insert into format_test3(c1) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
--error 01722
insert into format_test3(c1) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c1) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c1) values(to_char(1234, 'fmtm9'));


--error 01722
insert into format_test3(c2) values(to_char(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c2) values(to_char(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c2) values(to_char(100000000, '9,9999,9999BL'));
--error 01722
insert into format_test3(c2) values(to_char(0.000, 'FMRn'));
--error 01722
insert into format_test3(c2) values(to_char(100.2356, '99999,99VC'));
--error 01722
insert into format_test3(c2) values(to_char(1485,'XXX'));
--error 01722
insert into format_test3(c2) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
--error 01722
insert into format_test3(c2) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c2) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c2) values(to_char(1234, 'fmtm9'));
insert into format_test3(c2) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c2) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c2) values(to_char(123, '9999V999999EEEE'));


--echo ##################################如果将BINARY_FLOAT或BINARY_DOUBLE值转换为CHAR或NCHAR，并且输入是无穷大或NaN（不是数字），则Oracle总是返回井号以替换该值。number_format_model-1-8-5##################################
--error 00984,00904
insert into format_test3(c2) values(to_char(NaN, '9999V999999EEEE'));
--error 00984,00904
insert into format_test3(c3) values(to_char(NaN, '9999V999999EEEE'));

###insert into format_test3(c2) values(to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9999V999999EEEE'));
###insert into format_test3(c3) values(to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9999V999999EEEE'));

--error 01722
insert into format_test3(c2) values(to_char('∞', '9999V999999EEEE'));
--error 01722
insert into format_test3(c3) values(to_char('∞', '9999V999999EEEE'));

###select * from format_test3;

--error 01722
insert into format_test3(c3) values(to_char(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c3) values(to_char(100000000,'99G9999999v'));
--error 01722
insert into format_test3(c3) values(to_char(100000000, '9,9999,9999BL'));
--error 01722
insert into format_test3(c3) values(to_char(0.000, 'FMRn'));
--error 01722
insert into format_test3(c3) values(to_char(100.2356, '99999,99VC'));
--error 01722
insert into format_test3(c3) values(to_char(1485,'XXX'));
--error 01722
insert into format_test3(c3) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
--error 01722
insert into format_test3(c3) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c3) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c3) values(to_char(1234, 'fmtm9'));
insert into format_test3(c3) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c3) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c3) values(to_char(123, '9999V999999EEEE'));


insert into format_test3(c4) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c4) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c4) values(to_char(100000000, '9,9999,9999BL'));
insert into format_test3(c4) values(to_char(0.000, 'FMRn'));
insert into format_test3(c4) values(to_char(100.2356, '99999,99VC'));
insert into format_test3(c4) values(to_char(1485,'XXX'));
insert into format_test3(c4) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into format_test3(c4) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c4) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c4) values(to_char(1234, 'fmtm9'));
insert into format_test3(c4) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c4) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c4) values(to_char(123, '9999V999999EEEE'));

insert into format_test3(c5) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c5) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c5) values(to_char(100000000, '9,9999,9999BL'));
insert into format_test3(c5) values(to_char(0.000, 'FMRn'));
insert into format_test3(c5) values(to_char(100.2356, '99999,99VC'));
insert into format_test3(c5) values(to_char(1485,'XXX'));
insert into format_test3(c5) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into format_test3(c5) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c5) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c5) values(to_char(1234, 'fmtm9'));
insert into format_test3(c5) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c5) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c5) values(to_char(123, '9999V999999EEEE'));

insert into format_test3(c6) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c6) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c6) values(to_char(100000000, '9,9999,9999BL'));
insert into format_test3(c6) values(to_char(0.000, 'FMRn'));
insert into format_test3(c6) values(to_char(100.2356, '99999,99VC'));
insert into format_test3(c6) values(to_char(1485,'XXX'));
insert into format_test3(c6) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into format_test3(c6) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c6) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c6) values(to_char(1234, 'fmtm9'));
insert into format_test3(c6) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c6) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c6) values(to_char(123, '9999V999999EEEE'));

insert into format_test3(c7) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c7) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c7) values(to_char(100000000, '9,9999,9999BL'));
insert into format_test3(c7) values(to_char(0.000, 'FMRn'));
insert into format_test3(c7) values(to_char(100.2356, '99999,99VC'));
insert into format_test3(c7) values(to_char(1485,'XXX'));
insert into format_test3(c7) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into format_test3(c7) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c7) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c7) values(to_char(1234, 'fmtm9'));
insert into format_test3(c7) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c7) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c7) values(to_char(123, '9999V999999EEEE'));

--error 01465
insert into format_test3(c8) values(to_char(100000000,'99G9999999v'));
--error 01465
insert into format_test3(c8) values(to_char(100000000,'99G9999999v'));
--error 01465
insert into format_test3(c8) values(to_char(100000000, '9,9999,9999BL'));
--error 01465
insert into format_test3(c8) values(to_char(0.000, 'FMRn'));
--error 01465
insert into format_test3(c8) values(to_char(100.2356, '99999,99VC'));
--error 01465
insert into format_test3(c8) values(to_char(1485,'XXX'));
--error 01465
insert into format_test3(c8) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
--error 01465
insert into format_test3(c8) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c8) values(to_char(to_char(12,'FM9990999.9pr'));
insert into format_test3(c8) values(to_char(1234, 'fmtm9'));
--error 01465
insert into format_test3(c8) values(to_char(29989329, '9.99EEEE'));
--error 01465
insert into format_test3(c8) values(to_char(123*0.34,'9999.9'));
--error 01465
insert into format_test3(c8) values(to_char(123, '9999V999999EEEE'));

insert into format_test3(c9) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c9) values(to_char(100000000,'99G9999999v'));
insert into format_test3(c9) values(to_char(100000000, '9,9999,9999BL'));
insert into format_test3(c9) values(to_char(0.000, 'FMRn'));
insert into format_test3(c9) values(to_char(100.2356, '99999,99VC'));
insert into format_test3(c9) values(to_char(1485,'XXX'));
insert into format_test3(c9) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into format_test3(c9) values(to_char(0*0, '9CB'));
insert into format_test3(c9) values(to_char(1234, 'fmtm9'));
insert into format_test3(c9) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c9) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c9) values(to_char(123, '9999V999999EEEE'));

--error 01843,01847
insert into format_test3(c10) values(to_char(100000000,'99G9999999v'));
--error 01843,01847
insert into format_test3(c10) values(to_char(100000000,'99G9999999v'));
--error 01843,01847
insert into format_test3(c10) values(to_char(100000000, '9,9999,9999BL'));
--error 01858
insert into format_test3(c10) values(to_char(0.000, 'FMRn'));
--error 01843,01847
insert into format_test3(c10) values(to_char(100.2356, '99999,99VC'));
--error 01861,01858
insert into format_test3(c10) values(to_char(1485,'XXX'));
--error 01858
insert into format_test3(c10) values(to_char(12345, '9,,,9,,9,9,9$Mi'));
--error 01847,01840
insert into format_test3(c10) values(to_char(0*0, '9CB'));
--error 00917,900
insert into format_test3(c10) values(to_char(to_char(12,'FM9990999.9pr'));
--error 01861,01840
insert into format_test3(c10) values(to_char(1234, 'fmtm9'));
--error 01843,01847,01858
insert into format_test3(c10) values(to_char(29989329, '9.99EEEE'));
--error 01847,01840
insert into format_test3(c10) values(to_char(123*0.34,'9999.9'));
--error 01861,01858
insert into format_test3(c10) values(to_char(123, '9999V999999EEEE'));

select to_char(c1, '9.99EEEE') from format_test2;
select to_char(c1, '0000d00l') from format_test2;
select to_char(c1, '99999,99U') from format_test2;
select to_char(c2, '9,$99999999B') from format_test2;
select to_char(c2,  'fmRn') from format_test2;
select to_char(c2, '999999V999S') from format_test2;
###select to_char(c3,  'FM0XXX') from format_test2;
select to_char(c3,  '9,,,9,,9,9,9$Mi') from format_test2;
select to_char(c3, 'FM9B') from format_test2;
select to_char(c4, '9.99EEEE') from format_test2;
select to_char(c4, '0000d00l') from format_test2;
select to_char(c4, '99999,99U') from format_test2;
select to_char(c5, '9,$99999999B') from format_test2;
select to_char(c5,  'fmRn') from format_test2;
select to_char(c5, '999999V999S') from format_test2;
select to_char(c6,  'FM0XXX') from format_test2;
select to_char(c6,  '9,,,9,,9,9,9$Mi') from format_test2;
select to_char(c6, 'FM9B') from format_test2;
select to_char(c7, '9.99EEEE') from format_test2;
select to_char(c7, '0000d00l') from format_test2;
select to_char(c7, '99999,99U') from format_test2;

select to_char(to_char(c1, '9.99EEEE')) from format_test2;
select to_char(to_char(c1, '0000d00l')) from format_test2;
select to_char(to_char(c1, '99999,99U')) from format_test2;
select to_char(to_char(c2, '9,$99999999B')) from format_test2;
select to_char(to_char(c2,  'fmRn')) from format_test2;
select to_char(to_char(c2, '999999V999S')) from format_test2;
###select to_char(to_char(c3,  'FM0XXX')) from format_test2;
select to_char(to_char(c3,  '9,,,9,,9,9,9$Mi')) from format_test2;
select to_char(to_char(c3, 'FM9B')) from format_test2;
select to_char(to_char(c4, '9.99EEEE')) from format_test2;
select to_char(to_char(c4, '0000d00l')) from format_test2;
select to_char(to_char(c4, '99999,99U')) from format_test2;
select to_char(to_char(c5, '9,$99999999B')) from format_test2;
select to_char(to_char(c5,  'fmRn')) from format_test2;
select to_char(to_char(c5, '999999V999S')) from format_test2;
select to_char(to_char(c6,  'FM0XXX')) from format_test2;
select to_char(to_char(c6,  '9,,,9,,9,9,9$Mi')) from format_test2;
select to_char(to_char(c6, 'FM9B')) from format_test2;
select to_char(to_char(c7, '9.99EEEE')) from format_test2;
select to_char(to_char(c7, '0000d00l')) from format_test2;
select to_char(to_char(c7, '99999,99U')) from format_test2;

--echo ##################################生成列中使用（虚拟列） number_format_model-1-6##################################

--disable_warnings
--error 0,942
drop table format_test4;
--enable_warnings
create table format_test4(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);

insert into format_test4 values(100000000,100.2356,0.00,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test4 values(100000000,0.00,0+0,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

###ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_char(100000000,'99G9999999v')) VIRTUAL;
###ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_char(1000,'99G9999999v')) VIRTUAL;
###ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_char(10000000000, '9,9999,9999BL')) VIRTUAL;
###ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_char(0.000, 'FMRn')) VIRTUAL;
###ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_char(100.2356, '99999,99VC')) VIRTUAL;
###ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_char(1485,'XXX')) VIRTUAL;
###ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_char(12345, '9,,,9,,9,9,9$Mi')) VIRTUAL;
###ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_char(0*0, '9CB')) VIRTUAL;
###ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_char(to_char(0.1*5, '9CB'))) VIRTUAL;
###ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_char(1234, 'fmtm9')) VIRTUAL;
###ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_char(29989329, '9.99EEEE')) VIRTUAL;
###ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_char(123*0.34,'9999.9')) VIRTUAL;
###ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_char(123, '9999V999999EEEE')) VIRTUAL;
###alter table format_test4 drop column c11;
###alter table format_test4 drop column c12;
###alter table format_test4 drop column c13;
###alter table format_test4 drop column c14;
###alter table format_test4 drop column c15;
###alter table format_test4 drop column c16;
###alter table format_test4 drop column c17;
###alter table format_test4 drop column c18;
###alter table format_test4 drop column c19;
###alter table format_test4 drop column c20;
###alter table format_test4 drop column c21;
###alter table format_test4 drop column c22;
###alter table format_test4 drop column c23;
ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (floor(to_number(to_char(1234,'999900EEEE')))) VIRTUAL;
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (decode(to_char(1234.34,'0000d00l'),to_char(1234.34,'0000d00l'),'好的','1234.34$$','不好','1234.34$$$','再说')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(2000) GENERATED ALWAYS AS (replace(to_char(1234.34,'0000d00l'),'1234.34$','好的好的')) VIRTUAL;
###ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (replace(to_char(100.00, '9,$99999999B'),'$100','好的好的')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (remainder( to_char(1485,'XXX'),2)) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (abs(to_number(to_char(123*0.34,'tM9')))) VIRTUAL;
###ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (concat(to_char(1234.34,'0000d00l'), to_char(100000000,'99G9999999v'))) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(2000) GENERATED ALWAYS AS (LPAD( to_char(23.12, '99.99BCMI'), 200, '$')) VIRTUAL;
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (instrb(  to_char(1,'9BEEEE') ,'00')) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (substrb( to_char(123, '9999999999EEEE'),2,5)) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (round(to_number(to_char(1234, 'fmtm9')))) VIRTUAL;
###ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (COALESCE(to_char(0*0, '9CB'),to_char(1485,'XXX'),'3','4','5')) VIRTUAL;
###ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (LTRIM(to_char(485,'XXX'))) VIRTUAL;
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
###ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_char(c1,'99G9999999v')) VIRTUAL;
###--error 54015
###ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_char(c1,'99G9999999v')) VIRTUAL;
###ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_char(c1, '9,9999,9999BL')) VIRTUAL;
###ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_char(c1, 'FMRn')) VIRTUAL;
###ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_char(c1, '99999,99VC')) VIRTUAL;
###ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_char(c1,'XXX')) VIRTUAL;
###ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_char(c1, '9,,,9,,9,9,9$Mi')) VIRTUAL;
###ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_char(c1, '9CB')) VIRTUAL;
###--error 54015
###ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_char(to_char(c1, '9CB'))) VIRTUAL;
###ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_char(c1, 'fmtm9')) VIRTUAL;
###ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_char(c1, '9.99EEEE')) VIRTUAL;
###ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_char(c1,'9999.9')) VIRTUAL;
###ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_char(c1, '9999V999999EEEE')) VIRTUAL;
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

ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_char(c4,'99G9999999v')) VIRTUAL;
--error 54015
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_char(c4,'99G9999999v')) VIRTUAL;
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_char(c4, '9,9999,9999BL')) VIRTUAL;
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_char(c4, 'FMRn')) VIRTUAL;
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_char(c4, '99999,99VC')) VIRTUAL;
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_char(c4,'XXX')) VIRTUAL;
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_char(c4, '9,,,9,,9,9,9$Mi')) VIRTUAL;
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_char(c4, '9CB')) VIRTUAL;
###--error 54015
###ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_char(to_char(c4, '9CB'))) VIRTUAL;
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_char(c4, 'fmtm9')) VIRTUAL;
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_char(c4, '9.99EEEE')) VIRTUAL;
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_char(c4,'9999.9')) VIRTUAL;
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_char(c4, '9999V999999EEEE')) VIRTUAL;
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

--error 12899,600
ALTER TABLE format_test4 ADD c11 varchar2(200) GENERATED ALWAYS AS (to_char(c8,'99G9999999v')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c12 varchar2(200) GENERATED ALWAYS AS (to_char(c8,'99G9999999v')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c13 varchar2(200) GENERATED ALWAYS AS (to_char(c8, '9,9999,9999BL')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c14 varchar2(200) GENERATED ALWAYS AS (to_char(c8, 'FMRn')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c15 varchar2(200) GENERATED ALWAYS AS (to_char(c8, '99999,99VC')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c16 varchar2(200) GENERATED ALWAYS AS (to_char(c8,'XXX')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c17 varchar2(200) GENERATED ALWAYS AS (to_char(c8, '9,,,9,,9,9,9$Mi')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c18 varchar2(200) GENERATED ALWAYS AS (to_char(c8, '9CB')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c19 varchar2(200) GENERATED ALWAYS AS (to_char(to_char(c8, '9CB'))) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c20 varchar2(200) GENERATED ALWAYS AS (to_char(c8, 'fmtm9')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c21 varchar2(200) GENERATED ALWAYS AS (to_char(c8, '9.99EEEE')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c22 varchar2(200) GENERATED ALWAYS AS (to_char(c8,'9999.9')) VIRTUAL;
--error 12899,600
ALTER TABLE format_test4 ADD c23 varchar2(200) GENERATED ALWAYS AS (to_char(c8, '9999V999999EEEE')) VIRTUAL;
###--error 600
###ALTER TABLE format_test4 ADD c11 varchar2(32767) GENERATED ALWAYS AS (to_char(c8,'99G9999999v')) VIRTUAL;
###--error 54015
###ALTER TABLE format_test4 ADD c12 varchar2(32767) GENERATED ALWAYS AS (to_char(c8,'99G9999999v')) VIRTUAL;
###ALTER TABLE format_test4 ADD c13 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, '9,9999,9999BL')) VIRTUAL;
###ALTER TABLE format_test4 ADD c14 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, 'FMRn')) VIRTUAL;
###ALTER TABLE format_test4 ADD c15 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, '99999,99VC')) VIRTUAL;
###ALTER TABLE format_test4 ADD c16 varchar2(32767) GENERATED ALWAYS AS (to_char(c8,'XXX')) VIRTUAL;
###ALTER TABLE format_test4 ADD c17 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, '9,,,9,,9,9,9$Mi')) VIRTUAL;
###ALTER TABLE format_test4 ADD c18 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, '9CB')) VIRTUAL;
###--error 54015
###ALTER TABLE format_test4 ADD c19 varchar2(32767) GENERATED ALWAYS AS (to_char(to_char(c8, '9CB'))) VIRTUAL;
###ALTER TABLE format_test4 ADD c20 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, 'fmtm9')) VIRTUAL;
###ALTER TABLE format_test4 ADD c21 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, '9.99EEEE')) VIRTUAL;
###ALTER TABLE format_test4 ADD c22 varchar2(32767) GENERATED ALWAYS AS (to_char(c8,'9999.9')) VIRTUAL;
###ALTER TABLE format_test4 ADD c23 varchar2(32767) GENERATED ALWAYS AS (to_char(c8, '9999V999999EEEE')) VIRTUAL;
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

###--error 00932
###ALTER TABLE format_test4 ADD c11 varchar2(32767) GENERATED ALWAYS AS (to_char(c9,'99G9999999v')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c12 varchar2(32767) GENERATED ALWAYS AS (to_char(c9,'99G9999999v')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c13 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, '9,9999,9999BL')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c14 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, 'FMRn')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c15 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, '99999,99VC')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c16 varchar2(32767) GENERATED ALWAYS AS (to_char(c9,'XXX')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c17 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, '9,,,9,,9,9,9$Mi')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c18 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, '9CB')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c19 varchar2(32767) GENERATED ALWAYS AS (to_char(to_char(c9, '9CB'))) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c20 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, 'fmtm9')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c21 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, '9.99EEEE')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c22 varchar2(32767) GENERATED ALWAYS AS (to_char(c9,'9999.9')) VIRTUAL;
###--error 00932
###ALTER TABLE format_test4 ADD c23 varchar2(32767) GENERATED ALWAYS AS (to_char(c9, '9999V999999EEEE')) VIRTUAL;


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

--echo ##################################其他异常情况（存在正确性问题） number_format_model-1-8##################################

select to_char(0.1, 'B999C99') from dual;
select to_char(0.1, 'FMB999C99') from dual;
select to_char(0.1, 'BC99999') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9999999999EEEE') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.1, 'fmRn') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9999999999EEEE') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.1, 'fmRn') from dual;
######--error 01426
######select to_char(999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999990, '9,9999,9999') from dual;
######--error 01426
######select to_char(-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999990, '9,9999,9999') from dual;
###
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'FMRn') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '99999,99VC') from dual;
######select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '999999V999') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9$Mi') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,9,9,9,9vPR') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fm9999s') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9CB') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'FM9,9999,9999c') from dual;
######select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fmtm9') from dual;
###select to_char(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9B') from dual;
###
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'FMRn') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '99999,99VC') from dual;
######select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '999999V999') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9$Mi') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,9,9,9,9vPR') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fm9999s') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9CB') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'FM9,9999,9999c') from dual;
######select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, 'fmtm9') from dual;
###select to_char(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999, '9,,,9,,9,9,9B') from dual;

--error 01722
select to_char('Inf', 'fmtm9') from dual;
--error 01722
select to_char('Inf', '9,,,9,,9,9,9B') from dual;