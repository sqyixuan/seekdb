--echo ##################################PL中的使用 number_format_model-2-4##################################

--echo ##################################函数场景测试##################################
delimiter //;
--echo ######函数-一般赋值运算-字符表达式,NVARCHAR2######
create or replace function fun1
return NVARCHAR2
 is
v_char1 NVARCHAR2(20);
v_char3 NVARCHAR2(20);
v_char4 NVARCHAR2(20);
v_char5 NVARCHAR2(20);
v_char6 NVARCHAR2(20);
begin
v_char1:=to_number('1.23E+03','9999.00EEEE');
v_char3:=to_number('-485','999B');
v_char4:=to_number('$1234.34', '9999d99$');
v_char5:=to_number('2.0E+07', '0D0eeee');
v_char6:=to_number(485,'XXX');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun1 from dual;
###--echo # 展示结果
###select println() from dual;


delimiter //;
--echo ######函数-一般赋值运算-字符表达式,CLOB######

delimiter ;//
###select fun111 from dual;
--echo # 展示结果
###select println() from dual;


delimiter //;
--echo ######函数-一般赋值运算-字符表达式,VARCHAR2######
create or replace function fun2
return VARCHAR2
 is
v_char1 VARCHAR2(20);
v_char3 VARCHAR2(20);
v_char4 VARCHAR2(20);
v_char5 VARCHAR2(20);
v_char6 VARCHAR2(20);
begin
v_char1:=to_number('2.E+07', '9DEEEE');
v_char3:=to_number('1234+', 'fm9999s');
v_char4:=to_number('003039', '0000XX');
v_char5:=to_number('2,3', 'FMB9,9');
v_char6:=to_number('0012.+','FM9990999.9s');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun2 from dual;
###--echo # 展示结果
###select println() from dual;


delimiter //;
--echo ######函数-一般赋值运算-字符表达式,VARCHAR2--其他字符######
create or replace function fun2
return VARCHAR2
 is
v_char1 VARCHAR2(200);
v_char3 VARCHAR2(200);
v_char4 VARCHAR2(200);
v_char5 VARCHAR2(2000);
v_char6 VARCHAR2(200);
v_char8 VARCHAR2(200);
v_char9 VARCHAR2(200);
v_char10 VARCHAR2(200);
v_char11 VARCHAR2(200);
v_char12 VARCHAR2(200);
v_char13 VARCHAR2(2000);
begin
v_char1:=to_number('1234+', 'fm9999s');
v_char3:=to_number('.22','FM9,9,9,9.99PR');
v_char4:=to_number('0064', 'FM0XXX');
v_char5:=to_number('1.23E+03','9999.00EEEE');
v_char6:=to_number('1,00000000+','9G99999999S');
v_char8:=to_number(123, '9999999.99'); 
v_char9:=to_number('$1,0,4,6,9', '9,9,9,$9,9'); 
v_char10:=to_number('$23', 'FMU99'); 
v_char11:=to_number('', '9,9,9,V9,9MI'); 
v_char12:=to_number(-0.1,'99.999BEEEE'); 
v_char13:=to_number('2.0E+07', '0D0eeee'); 
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
dbms_output.put_line(v_char8);
dbms_output.put_line(v_char9);
dbms_output.put_line(v_char10);
dbms_output.put_line(v_char11);
dbms_output.put_line(v_char12);
dbms_output.put_line(v_char13);
return v_char1;
end;
//

delimiter ;//
select fun2 from dual;
###--echo # 展示结果
###select println() from dual;


--echo # 函数-赋值语句-数据库select into赋值--正则表达式#
--disable_warnings
--error 0,942
drop table regtest;
--enable_warnings
create table regtest(id varchar(400),value varchar(100));
insert into regtest values('1',to_number(to_number('1.23E+03','9999.00EEEE')));
insert into regtest values('4',to_number(to_number(-0.1,'99.999BEEEE')));

delimiter //;
create or replace function fun5
return varchar2
is
v_replace varchar2(100);
v_replace1 varchar2(100);
v_replace2 varchar2(100);
v_replace4 nvarchar2(100);
v_replace5 nvarchar2(100);
v_replace6 nvarchar2(100);
v_replace7 clob;
v_replace8 clob;
v_replace9 clob;
v_replace10 varchar2(100);
v_replace11 varchar2(100);
v_replace12 varchar2(100);
v_replace13 varchar2(100);
v_replace14 nvarchar2(100);
v_replace15 clob;
v_like varchar2(100);
v_number1 number;
v_number2 number;
begin
    select value into v_like from regtest where value like '1____60';
    dbms_output.put_line(v_like);
    select to_char(to_number(to_char(1234,'999900EEEE'))) into v_replace from dual;
    dbms_output.put_line(v_replace);
	select to_char(to_number('', 'FM9B')) into v_replace1 from dual;
    dbms_output.put_line(v_replace1);
	select to_char(to_number('1.23E+03','9999.00EEEE'),'9999.00EEEE') into v_replace2 from dual;
    dbms_output.put_line(v_replace2);
	select to_char(to_number('$1,00', '99$999,99b'), '99$999,99b') into v_replace4 from dual;
    dbms_output.put_line(v_replace4);
	select to_char(to_number('0064', 'FM0XXX'), 'FM0XXX') into v_replace5 from dual;
    dbms_output.put_line(v_replace5);
	select to_number(to_char(to_number('1.23E+03','9999.00EEEE'))) into v_replace6 from dual;
    dbms_output.put_line(v_replace6);
	select to_number(to_char(to_number('-485','999B'))) into v_replace7 from dual;
    dbms_output.put_line(v_replace7);
	select to_number(to_char(to_number('123.00', '9999999D99'))) into v_replace8 from dual;
    dbms_output.put_line(v_replace8);
	select to_number(to_char(to_number('1,00.', '99999G99BD'))) into v_replace9 from dual;
    dbms_output.put_line(v_replace9);	
	select to_number(to_char(to_number('', 'BC99MI'))) into v_replace10 from dual;
    dbms_output.put_line(v_replace10);
	select to_number(to_char(to_number(-0.1,'99.999BEEEE'))) into v_replace11 from dual;
    dbms_output.put_line(v_replace11);
	select to_number(to_char(to_number('0064', 'FM0XXX'))) into v_replace12 from dual;
    dbms_output.put_line(v_replace12);
	select value into v_replace13 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace13);
	select value into v_replace14 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace14);
	select value into v_replace15 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace15);
    select to_number(to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'))) into v_number1 FROM dual; 
    dbms_output.put_line(v_number1);
    select length(to_number('$100', '9,$99999999B')) into v_number2 from DUAL;
    dbms_output.put_line(v_number2); 
    return v_replace;
end;
//

delimiter ;//
select fun5 from dual;

###--echo # 展示结果
###select println() from dual;



--echo # 函数-赋值语句--批量绑定--select into 中使用bulk collect--在fetch into中使用bulk collect --nvarchar2 #
--disable_warnings
--error 0,942
drop table test_forall;
--error 0,942
drop table mag_user_basic;
--enable_warnings
create table test_forall ( user_id number(10), user_name nvarchar2(200));
create table mag_user_basic ( user_id number(10), user_name nvarchar2(200));
insert into mag_user_basic values(1,translate(to_number('0064', 'FM0XXX'),'100','wo'));
insert into mag_user_basic values(2,lengthb(to_number('1,00000000+','9G99999999S')));
insert into mag_user_basic values(3,abs(to_number('2,3', 'FMB9,9')));

delimiter //;
create or replace function fun34
return number
 is
TYPE table_forall IS TABLE OF test_forall%ROWTYPE;  
  v_table table_forall;
  CURSOR c1 IS  
    SELECT mub.user_id,mub.user_name FROM mag_user_basic mub; 
begin
SELECT mub.user_id,mub.user_name 
         BULK COLLECT INTO v_table
    FROM mag_user_basic mub;
    FORALL idx IN 1..v_table.COUNT
           INSERT INTO test_forall VALUES v_table(idx);
    COMMIT; 
    OPEN c1;  
   
   FETCH c1 BULK COLLECT INTO v_table;  
   FORALL idx IN 1..v_table.COUNT  
         INSERT INTO test_forall VALUES v_table(idx);  
    COMMIT;
return 23;
end;
//

delimiter ;//
###Oracle报错OBE-14551: cannot perform a DML operation inside a query
###select fun34 from dual;

###--echo # 展示结果
###select println() from dual;



--echo ##################################存储过程场景测试##################################
delimiter //;
--echo ######存储过程-一般赋值运算-字符表达式,NVARCHAR2######
create or replace procedure 中文存储过程1
 is
中文列1 NVARCHAR2(200);
中文列3 NVARCHAR2(200);
中文列4 NVARCHAR2(200);
中文列5 NVARCHAR2(200);
中文列6 NVARCHAR2(200);
begin
中文列1:=concat(to_number('123.00', '9999999D99'), to_char(100000000,'99G9999999v'));
中文列3:=RPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$');
中文列4:=RPAD( to_number('1.23E+03','9999.00EEEE'), 123, '$');
中文列5:=substrb(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),1,2);
中文列6:=initcap(to_number('1,00000000+','9G99999999S'));
dbms_output.put_line(中文列1);
dbms_output.put_line(中文列3);
dbms_output.put_line(中文列4);
dbms_output.put_line(中文列5);
dbms_output.put_line(中文列6);
end;
//

delimiter ;//
call 中文存储过程1();
###--echo # 展示结果
###select println() from dual;

delimiter //;
--echo  ##################procedure存储过程中的应用in/out参数 ##################
create or replace procedure MyProcedure(标志 in INTEGER)    --param1形式参数，能接受实际参数传来的值
AS
BEGIN
    DBMS_OUTPUT.PUT_LINE(REGEXP_COUNT(to_char(52345/5, '9,9,9,$9,9') ,'$',1,'i')); 
END;
//

delimiter ;//
####REGEXP_COUNT 在SQL侧结果不一致
###call MyProcedure(10);


--echo # 展示结果
####select println() from dual;


--echo ##################################各种数据类型场景测试##################################
--echo ######oracle复合数据类型 record(pl/sql记录,相当于高级语言中的结构)用法示例  varchar2######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname varchar2(2000),stuage varchar2(2000));
insert into student values ('001',lower( to_number('003039', '0000XX')),upper( to_number('23,', 'FMB99,')));
insert into student values ('002',upper( to_number('23,', 'FMB99,')),lower( to_number('003039', '0000XX'))||RTRIM(to_number('0064', 'FM0XXX')));
insert into student values ('003',length(TRIM(to_number('1234+', 'fm9999s'))),length(LTRIM(to_number(0+0, '9BPR'))));
insert into student values ('004',nvl(null,to_number('$100', '9,$99999999B')),ceil(to_number(-1234, 's9b999')));

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

###--echo # 展示结果
###select println() from dual;

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid nvarchar2(20), stuname nvarchar2(2000),stuage nvarchar2(2000));
insert into student values ('001',lower( to_number('003039', '0000XX')),upper( to_number('23,', 'FMB99,')));
insert into student values ('002',upper( to_number('23,', 'FMB99,')),lower( to_number('003039', '0000XX'))||RTRIM(to_number('0064', 'FM0XXX')));
insert into student values ('003',length(TRIM(to_number('1234+', 'fm9999s'))),length(LTRIM(to_number(0+0, '9BPR'))));
insert into student values ('004',nvl(null,to_number('$100', '9,$99999999B')),ceil(to_number(-1234, 's9b999')));

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

###--echo # 展示结果
###select println() from dual;


--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid nvarchar2(20), stuname nchar(1000),stuage nchar(1000));
insert into student values ('001',lower( to_number('003039', '0000XX')),upper( to_number('23,', 'FMB99,')));
insert into student values ('002',upper( to_number('23,', 'FMB99,')),lower( to_number('003039', '0000XX'))||RTRIM(to_number('0064', 'FM0XXX')));
insert into student values ('003',length(TRIM(to_number('1234+', 'fm9999s'))),length(LTRIM(to_number(0+0, '9BPR'))));
insert into student values ('004',nvl(null,to_number('$100', '9,$99999999B')),ceil(to_number(-1234, 's9b999')));

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

###--echo # 展示结果
###select println() from dual;


###--disable_warnings
###--error 0,942
###drop table student;
###--enable_warnings
###create table student(stuid nvarchar2(200), stuname clob,stuage clob);
###insert into student values ('001',lower( to_number('003039', '0000XX')),upper( to_number('23,', 'FMB99,')));
###insert into student values ('002',upper( to_number('23,', 'FMB99,')),lower( to_number('003039', '0000XX'))||RTRIM(to_number('0064', 'FM0XXX')));
###OB暂不支持CLOB+GBK时，以下函数的执行
###insert into student values ('003',length(TRIM(to_number('1234+', 'fm9999s'))),length(LTRIM(to_number(0+0, '9BPR'))));
###insert into student values ('004',nvl(null,to_number('$100', '9,$99999999B')),ceil(to_number(-1234, 's9b999')));
###
###delimiter //;
###
###declare
###type stu_record_type is record--is是必须的，不能替换为as
###(
###name student.stuname%type,
###age student.stuage%type
###);
###stu_record stu_record_type;
###begin
###select stuname,stuage into stu_record from student where stuid='001';
###dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
###select stuname,stuage into stu_record from student where stuid='002';
###dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
###select stuname,stuage into stu_record from student where stuid='003';
###dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
###select stuname,stuage into stu_record from student where stuid='004';
###dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
###end;
###//
###
###delimiter ;//
###
###--echo # 展示结果
###select println() from dual;

###sinh默认展示结果不一致。
###delimiter //;
###DECLARE
###   SUBTYPE name IS char(200);
###   SUBTYPE message IS varchar2(1000);
###   salutation name;
###   greetings message;
###BEGIN
###   salutation := to_number(sinh(to_number(123, '9999999.99')));
###   greetings := to_number(REGEXP_COUNT(to_number('$0012.0','9990999.$9PR') ,'1',1,'i')) ;
###   dbms_output.put_line('Hello ' || salutation || greetings);
###END;
###//
###delimiter ;//
###--echo # 展示结果
###select println() from dual;


delimiter //;
--echo ######自定义数据类型-一般赋值运算-字符表达式######
CREATE OR REPLACE TYPE type2 AS OBJECT (
    v_number1 number,
    MEMBER PROCEDURE procedure2
);
//
CREATE OR REPLACE TYPE BODY type2 AS
MEMBER PROCEDURE procedure2
is
v_char1 VARCHAR2(100);
v_char3 VARCHAR2(100);
v_char4 VARCHAR2(100);
v_char5 VARCHAR2(100);
v_char6 VARCHAR2(100);
begin
v_char1:=to_number(power(to_number(to_number('1234+', 'fm9999s')),2));
v_char3:=to_number(exp(to_number(to_number('$0012.0','9990999.$9PR'))));
v_char4:=to_number(ln(to_number('0064', 'FM0XXX')));
v_char5:=to_number(to_single_byte(to_number(NULL, '9,9V,9,9,9mi')));
v_char6:=to_number(ceil(to_number(-1234, 's9b999')), 's9b999');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
END;
//

delimiter ;//

###--echo # 展示结果
###select println() from dual;



delimiter //;
create or replace package P_TEST is
  procedure procedure1;
  procedure procedure2;
end P_TEST;
//
create or replace package body P_TEST
 as
--echo ######包-一般赋值运算-数值表达式######

  procedure procedure1 as
v_number1 number;
v_number2 number;
v_number3 number;
v_number4 number;
v_number5 number;
v_number6 number;
v_number7 number;
begin
v_number1:=to_number(sign(to_number('$1,00', '99$999,99b')));
v_number2:=to_number(sqrt(to_number('001E5','000XX')));
v_number3:=to_number(floor(to_number('1234+', 'fm9999s')));
v_number4:=to_number(ceil(to_number('0064', 'FM0XXX')));
v_number5:=to_number(RTRIM(to_number('2,3', 'FMB9,9')));
v_number6:=to_number(nvl(to_number('$23', 'FMU99'),null));
v_number7:=to_number(TRIM(to_number(-1234, 's9b999')));
dbms_output.put_line(v_number1);
dbms_output.put_line(v_number2);
dbms_output.put_line(v_number3);
dbms_output.put_line(v_number4);
dbms_output.put_line(v_number5);
dbms_output.put_line(v_number6);
end;

--echo ######包-一般赋值运算-字符表达式######
procedure procedure2
as
v_char1 VARCHAR2(100);
v_char3 VARCHAR2(100);
v_char4 VARCHAR2(100);
v_char5 VARCHAR2(100);
v_char6 VARCHAR2(100);
begin
v_char1:=to_char(to_number(123, '9999999.99'), '9999999.99');
v_char3:=to_char(to_number('1,00000000+','9G99999999S'),'9G99999999S');
v_char4:=to_char(to_number('$1,00', '99$999,99b'), '99$999,99b');
v_char5:=to_char(to_number('1234.34','0000.00B'),'0000.00B');
v_char6:=to_char(to_number('1.23E+03','9999.00EEEE'),'9999.00EEEE');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
end P_TEST;
//

delimiter ;//
call P_TEST.procedure1();
call P_TEST.procedure2();


###--echo # 展示结果
###select println() from dual;

delimiter //;
--echo ######匿名块-一般赋值运算-字符表达式######
declare
v_char1 VARCHAR2(100);
v_char3 VARCHAR2(100);
v_char4 VARCHAR2(100);
v_char5 VARCHAR2(100);
v_char6 VARCHAR2(100);
begin
v_char1:=to_char(to_number('$23', 'FMU99'), 'FMU99');
v_char3:=to_char(to_number(-0.1,'99.999BEEEE'),'99.999BEEEE');
v_char4:=to_char(to_number('', '9,9,9,V9,9MI'), '9,9,9,V9,9MI');
v_char5:=to_char(to_number(NULL, '9,9V,9,9,9mi'), '9,9V,9,9,9mi');
v_char6:=to_char(to_number(-1234.34+1234.56,'FM9,9,9,9.99PR'),'FM9,9,9,9.99PR');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
//

delimiter ;//

###--echo # 展示结果
###select println() from dual;


delimiter //;

--echo ######varchar2字符设置，无约束######
DECLARE
  SUBTYPE  sub1_varchar2 IS  varchar2(32);
  SUBTYPE  sub2_varchar2 IS  varchar2(200 char);
  v1   sub1_varchar2;
  v2   sub2_varchar2;
  v3   sub2_varchar2;
BEGIN
  v1:=to_char(to_number(0+0, '9BPR'), '9BPR');
  v2:=to_char(to_number('', 'FMB9d9'), 'FMB9d9');
  v3:=to_char(to_number('23,', 'FMB99,'), 'FMB99,');
  dbms_output.put_line(v1);
  dbms_output.put_line(v2);
  dbms_output.put_line(v3);
END;
//

delimiter ;//
###--echo # 展示结果
###select println() from dual;


--echo ##################################复合数据类型##################################
--echo ######not null约束，未赋值，复合预期的异常######
--disable_warnings
--error 0,942
drop table yd2;
--enable_warnings
create table yd2(id number,name3 VARCHAR2(30) not null);

--echo ######%rowtype 无约束######
insert into yd2 values(1,to_char(to_number(0+0, '9BPR'), '9BPR'));
--error 01400
insert into yd2 values(2,to_char(to_number('', 'fmRN'), 'fmRN'));
insert into yd2 values(3,to_char(to_number('0064', 'FM0XXX'), 'FM0XXX'));

delimiter //;
declare
subtype sub is yd2%rowtype;
v sub;
begin
  select * into v from yd2 where id=1;
  dbms_output.put_line(v.id);
  dbms_output.put_line(v.name3);
end;
//

delimiter ;//
###--echo # 展示结果
###select println() from dual;


delimiter //;
###VARRAYs 变量数组
DECLARE TYPE t_name IS VARRAY (2) OF VARCHAR2 (100); --创建 Collection，需要指定最大范围
              i_n1 t_name := t_name(); --创建 instance，需要使用构造函数
      BEGIN
              i_n1.EXTEND(2); --赋值前要分配空间，不能超过最大范围
              i_n1(1):=to_char(to_number('', 'FMB9d9'), 'FMB9d9'); --赋值，row number 最小为 1，最大为 EXTEND 分配的空间数
              i_n1(2):=to_char(to_number('2,3', 'FMB9,9'), 'FMB9,9');
			  DBMS_OUTPUT.put_line (i_n1(1));
			  DBMS_OUTPUT.put_line (i_n1(2));
      END;
	  //
	  
###--echo # 展示结果
###select println() from dual;
delimiter ;//