--echo ##################################DML number_format_model-1-9##################################

--disable_warnings
--error 0,942
drop table format_test1;
--enable_warnings
create table format_test1(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test1 values(100,1234.34,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

--disable_warnings
--error 0,942
drop table format_test2;
--enable_warnings
create table format_test2(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test2 values(100000000,100.2356,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test2 values(100000000,0.00,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test2 values(100,1234.34,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test2 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));


--disable_warnings
--error 0,942
drop table format_test3;
--enable_warnings
create table format_test3(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test3(c1) values(to_char(100, '999'));
insert into format_test3(c2) values(to_char(100, '000'));
insert into format_test3(c2) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c2) values(to_char(123, '9999V999999EEEE'));
insert into format_test3(c3) values(to_char(1234, '0000'));
insert into format_test3(c3) values(to_char(1234, '9999B'));
insert into format_test3(c1,c3) values(100,to_char(1234, '0000'));
insert into format_test3(c1,c3) values(100,to_char(1234, '9999B'));
insert into format_test3(c4) values(to_char(1234, 'fmtm9'));
insert into format_test3(c4) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c4) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c4) values(to_char(123, '9999V999999EEEE'));
insert into format_test3(c5) values(to_char(1234, 'fmtm9'));
insert into format_test3(c5) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c5) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c5) values(to_char(123, '9999V999999EEEE'));
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
insert into format_test3(c7) values(to_char(1234, 'fmtm9'));
insert into format_test3(c7) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c7) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c7) values(to_char(123, '9999V999999EEEE'));
insert into format_test3(c8) values(to_char(1234, 'fmtm9'));
insert into format_test3(c9) values(to_char(1234, 'fmtm9'));
insert into format_test3(c9) values(to_char(29989329, '9.99EEEE'));
insert into format_test3(c9) values(to_char(123*0.34,'9999.9'));
insert into format_test3(c9) values(to_char(123, '9999V999999EEEE'));


--disable_warnings
--error 0,942
drop table format_test4;
--enable_warnings
create table format_test4(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);

insert into format_test4 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test4 values(100,0.00,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

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

###number_format_model-1-9-1

select c1 as 列1 from format_test1;
## select * from table_name name_another
select c1  from format_test1 表1;
select c1 as 列1 from format_test3;

###to_char(c1, '9,$99999999B') = '$100';
###to_char(c2,'0,0,0,0.00') = '1,2,3,4.34';
###to_char(c3, '9999') = '1234';
###to_char(c4, '9,$99999999B') = '$100';
###to_char(c5, '99D99BUS') = '23.12$+';
###to_char(c6, '99999,99C') = '1,00USD';
###to_char(c7, 'L9999Mi') = '$1234';
###to_char(c8, '9B') = '##';

select to_char(c1, '9,$99999999B') as 列1 from format_test1;
select to_char(c2,'0,0,0,0.00') as 列1 from format_test1;
select to_char(c3, '9999') as 列1 from format_test1;
select to_char(c4, '9,$99999999B') as 列1 from format_test1;

select concat(c4,c5) from format_test1;
select concat(to_char(c6, '99999,99C'),to_char(c7, 'L9999Mi')) from format_test1;
###--error 600,12704
###select concat(c7,c8) from format_test1;

select concat(to_char(c1, '9,$99999999B'),c2) from format_test4;
select concat(c6,to_char(c7, 'L9999Mi')) from format_test4;
###--error 600,12704
###select concat(c7,c8) from format_test4;

select distinct(to_char(c7, 'L9999Mi')) from format_test1;
select distinct(c7) from format_test1;
--error 00932
select distinct(c8) from format_test1;
--error 00932
select distinct(c9) from format_test1;
select distinct(c10) from format_test1;
--error 01722
select distinct(to_char(c7, 'L9999Mi')) from format_test3;
select distinct(c7) from format_test3;
###select distinct(c15) from format_test4;

select t1.* from format_test1 t1, format_test3 t2 where to_char(t1.c1, '9,$99999999B') = to_char(t2.c1, '9,$99999999B') and t2.c3 = '1234';
select t1.* from format_test1 t1, format_test2 t2 where to_char(t1.c1, '9,$99999999B') = to_char(t2.c1, '9,$99999999B') and t2.c3 = '1234';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1, '9,$99999999B') = to_char(t2.c1, '9,$99999999B') and t2.c3 = '1234';

###insert into format_test2 values(100000000,100.2356,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
###insert into format_test2 values(100000000,0.00,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
###insert into format_test2 values(100,1234.34,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
###insert into format_test2 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c1, '9,$99999999B') = '$100';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c2,'0,0,0,0.00') = '1,2,3,4.34';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c3, '9999') = '1234';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c4, '9,$99999999B') = '$100';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c5, '99D99BUS') = '23.12$+';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c6, '99999,99C') = '1,00USD';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c7, 'L9999Mi') = '$1234';
--error 00932,01722
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t1.c8, '9B') = '##';
select t1.* from format_test1 t1, format_test4 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(12345, '9,,,9,,9,9,9B') = ' 1,,,2,,3,4,5';

--error 01722
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = t2.c1 and t2.c3 = '中文';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = t2.c1 and t2.c3 = NULL;
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = t2.c1 and t2.c3 != NULL;
--error 01722
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = t2.c1 and t2.c3 = '1234';
####select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = t2.c1 and t2.c3 != '1234';  

select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') >= '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') <= '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') <> '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') > '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') < '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') between '1234' and '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') not between '1234' and '1234';
select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') in ('1234' ,'1235');
select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') not in ('1234' ,'1235');
--error 01722
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = t2.c1 or t2.c3 = '1234';

select * from format_test1 t1,format_test3 t2 where exists (select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') >= '1234');
select * from format_test1 t1,format_test3 t2 where exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') between '1234' and '1234');
select * from format_test1 t1,format_test3 t2 where exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') not between '1234' and '1234');
select * from format_test1 t1,format_test3 t2 where exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') in ('1234' ,'1235'));
select * from format_test1 t1,format_test3 t2 where exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') not in ('1234' ,'1235'));

select * from format_test1 t1,format_test3 t2 where not exists (select * from format_test1 t1,format_test3 t2 where to_char(t1.c1,'9,$99999999B') = to_char(t2.c1, '9,$99999999B') and to_char(t2.c3,'9999') >= '1234');
select * from format_test1 t1,format_test3 t2 where not exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') between '1234' and '1234');
select * from format_test1 t1,format_test3 t2 where not exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') not between '1234' and '1234');
select * from format_test1 t1,format_test3 t2 where not exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') in ('1234' ,'1235'));
select * from format_test1 t1,format_test3 t2 where not exists (select * from format_test1 t1,format_test3 t2 where to_char(t2.c3,'9,$99999999B') not in ('1234' ,'1235'));

select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '1234%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '123%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '123%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') not like '1234%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') not like '123%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') not like '123%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like ' 1234%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like ' 123%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like ' 123%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '——%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '_%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like ' %';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '@%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like ' #%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '@%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like ' *%';
select * from format_test1 t1,format_test3 t2 where to_char(t1.c1) = to_char(t2.c1) or to_char(t2.c3,'9999') like '*%';

--error 01722
select sum(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY t1.C1,t2.C1;
select count(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY t1.C1,t2.C1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY t1.C1,t2.C1;
select min(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY t1.C1,t2.C1;
--error 01722
select avg(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY t1.C1,t2.C1;

select count(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY to_char(t1.c1,'9,$99999999B'),to_char(t2.c1,'9,$99999999B');
select count(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY to_char(t1.c1,'9,$99999999B'),to_char(t2.c1,'9,$99999999B') having count(to_char(t1.c1,'9,$99999999B')) > 5;
select count(to_char(t1.c1,'9,$99999999B')) from format_test1 t1,format_test3 t2 where to_char(t1.c1) = t2.c1 or t2.c3 like '1234%' GROUP BY to_char(t1.c1,'9,$99999999B'),to_char(t2.c1,'9,$99999999B') having count(to_char(t1.c1,'9,$99999999B')) > 5 order by to_char(t1.c1,'9,$99999999B') asc ,to_char(t2.c1,'9,$99999999B') desc;



--disable_warnings
--error 0,942
drop table format_test5;
--enable_warnings
create table format_test5(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test5 values(100000000,100.2356,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test5 values(100000000,0.00,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test5 values(100,1234.34,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test5 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

--disable_warnings
--error 0,942
drop table format_test6;
--enable_warnings
create table format_test6(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test6 values(100000000,100.2356,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test6 values(100000000,0.00,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test6 values(100,1234.34,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test6 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test6 values (to_char(100, '999'),to_char(100, '000'),to_char(1234, '0000'),to_char(1234, 'fmtm9'),to_char(29989329, '9.99EEEE'),to_char(123*0.34,'9999.9'),to_char(0.000, 'FMRn'),to_char(1234, 'fmtm9'),to_char(29989329, '9.99EEEE'),to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test6 values(200,200,200,'200','200','200','200','200','200',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

####单行、多行、关联子查询
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1;
select max(to_char(t2.c1,'9,$99999999B')) from format_test6 t2 where to_char(t2.c1,'9,$99999999B') = (select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_test6 t2 where to_char(t2.c1,'9,$99999999B') in (select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_test6 t2 where to_char(t2.c1,'9,$99999999B') in (select to_char(t1.c1,'9,$99999999B') from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_test6 t2 where to_char(t2.c1,'9,$99999999B') >= any(select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_test6 t2 where to_char(t2.c1,'9,$99999999B') not in (select to_char(t1.c1,'9,$99999999B') from format_test1 t1);

####rownum,rowid
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where rownum =1;
--error 01722
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'9,$99999999B') =1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'999') =1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'000') =1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'fmtme') =1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'fmtm9') =1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'FM9B') =1;
select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rownum,'FM99PR') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'9,$99999999B') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'999') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'000') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'fmtme') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'fmtm9') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'FM9B') =1;
###--error 600,932
###select max(to_char(t1.c1,'9,$99999999B')) from format_test6 t1 where to_char(rowid,'FM99PR') =1;

###select rowid from format_test6 t1 where to_char(rownum,'FM99PR') =1 and rowid = 'AAJomOAALAAAwBdAAA';

###不同类型之间相互比较
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c2;
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c3;
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c4;
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c5;
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c6;
--error 01722
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c7;
--error 00932
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c8;
--error 00932
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c9;
--error 00932
select max(to_char(c1,'9,$99999999B')) from format_test6 where c1 = c10;

select max(to_char(c1,'9,$99999999B')) from format_test6 where to_char(c1,'9,$99999999B') = to_char(c2,'000');
select max(to_char(c1,'9,$99999999B')) from format_test6 where to_char(c1,'fmtme') = to_char(c3,'000');
select max(to_char(c1,'9,$99999999B')) from format_test6 where to_char(c1,'fmtm9') = to_char(c4,'000');
select max(to_char(c1,'9,$99999999B')) from format_test6 where to_char(c1,'FM9B') = to_char(c5,'000');
select max(to_char(c1,'9,$99999999B')) from format_test6 where to_char(c1,'FM99PR') = to_char(c6,'000');

--disable_warnings
--error 0,942
drop view format_testview6;
--enable_warnings
create view format_testview6 as select * from format_test6;

select max(to_char(c1,'9,$99999999B')) from format_testview6 where c1 = c2;
select max(to_char(c1,'9,$99999999B')) from format_testview6 where c1 = c3;
select max(to_char(c1,'9,$99999999B')) from format_testview6 where c1 = c4;
select max(to_char(c1,'9,$99999999B')) from format_testview6 where c1 = c5;
select max(to_char(c1,'9,$99999999B')) from format_testview6 where c1 = c6;
select max(to_char(t1.c1,'9,$99999999B')) from format_testview6 t1;
select max(to_char(t2.c1,'9,$99999999B')) from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') = (select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') in (select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') in (select to_char(t1.c1,'9,$99999999B') from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') >= any(select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1);
select max(to_char(t2.c1,'9,$99999999B')) from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') not in (select to_char(t1.c1,'9,$99999999B') from format_test1 t1);

####DML验证number_format_model-1-9-2/number_format_model-1-9-3/number_format_model-1-9-4

insert into format_test5 select * from format_test6;
insert into format_test5 select * from format_testview6;

update format_test5 t set t.c2 = 300 where to_char(t.c2,'FM99PR') = to_char(t.c1,'FM99PR') and rownum =1;
update format_test5 t set t.c2 = (select e.c2 from format_test6 e where e.c1 = 200) where to_char(t.c2,'FM99PR') = to_char(t.c1,'FM99PR') and rownum =1;
update format_test5 t set t.c2 = 300 where t.c2 in (select e.c2 from format_test6 e where e.c1 = 200);
update (select 
t2.c1 c1old,t2.c2 c2new from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') = (select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1)) set c1old = c2new;
update format_test5 t set (t.c1,t.c2) = (select 
t2.c1 c1old,t2.c2 c2new from format_testview6 t2 where to_char(t2.c1,'9,$99999999B') = (select max(to_char(t1.c1,'9,$99999999B')) from format_test1 t1) and rownum =1);


delete from (select * from format_test5 t where to_char(t.c2,'FM99PR') = to_char(t.c1,'FM99PR'));
delete from format_test6 t where t.c1 in (select e.c2 from format_testview6 e);


--disable_warnings
--error 0,942
drop table format_test1;
--error 0,942
drop table format_test2;
--error 0,942
drop table format_test3;
--error 0,942
drop table format_test4;
--error 0,942
drop table format_test5;
--error 0,942
drop table format_test6;
--error 0,942
drop view format_testview6;
--enable_warnings
