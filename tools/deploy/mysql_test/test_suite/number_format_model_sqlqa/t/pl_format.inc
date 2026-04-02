--echo ##################################PL中的使用 number_format_model-1-4##################################

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
v_char1:=to_char(1234.34,'9,,,,,,,,,,99,9.99');
v_char3:=to_char(1234.34,'9,,,,,,,,,,99,9.99');
v_char4:=to_char(1234.34,'0000d00l');
v_char5:=to_char(-485,'99$9Mi');
v_char6:=to_char(23.12, '99.99BCMI');
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
create or replace function fun111
return NVARCHAR2
 is
v_char1 NVARCHAR2(20);
v_char3 CLOB;
v_char4 CLOB;
v_char5 CLOB;
v_char6 CLOB;
begin
v_char1:=to_char(1,'9BEEEE');
v_char3:=to_char(20, 'fmRn');
v_char4:=to_char(100.2356, '99999,99VC');
v_char5:=to_char(1485,'XXX');
v_char6:=to_char(12345, '9,,,9,,9,9,9$Mi');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun111 from dual;
###--echo # 展示结果
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
v_char1:=to_char(100.2356, '9,99999999Mi');
v_char3:=to_char(0*0, '9CB');
v_char4:=to_char(100000000, 'FM9,9999,9999c');
v_char5:=to_char(1234, 'fmtm9');
v_char6:=to_char(1234, 'fmtm9');
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
v_char1:=to_char(29989329, '9.99EEEE');
v_char3:=to_char(12,'9990999.9eeee');
v_char4:=to_char(100000000,'99G9999999v');
v_char5:=to_char(100000000,'99G9999999v');
v_char6:=to_char(100000000, '9,9999,9999BL');
v_char8:=to_char(0.000, 'FMRn');
v_char9:=to_char(123, '999999V999S');
v_char10:=to_char(100.2356, 'FM0XXX');
v_char11:=to_char(1234, 'L9999Mi');
v_char12:=to_char(12345, '9,,,9,,9,9,9B');
v_char13:=to_char(12,'FM9990999.9');
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
insert into regtest values('1',to_char(to_char(1234.34,'9,,,,,,,,,,99,9.99')));
insert into regtest values('4',to_char(to_char(12345, '9,,,9,,9,9,9')));

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
    select to_char(to_char(12,'9990999.9eeee')) into v_replace from dual;
    dbms_output.put_line(v_replace);
	select to_char(to_char(-485,'999V')) into v_replace1 from dual;
    dbms_output.put_line(v_replace1);
	select to_char(to_char(100000000,'99G9999999v')) into v_replace2 from dual;
    dbms_output.put_line(v_replace2);
	select to_char(to_char(-485,'99$9Mi')) into v_replace4 from dual;
    dbms_output.put_line(v_replace4);
	select to_char(to_char(100000000, '9,9999,9999BL')) into v_replace5 from dual;
    dbms_output.put_line(v_replace5);
	select to_char(to_char(100.2356, '99999,99C')) into v_replace6 from dual;
    dbms_output.put_line(v_replace6);
	select to_char(to_char(123, '9999V999999EEEE')) into v_replace7 from dual;
    dbms_output.put_line(v_replace7);
	select to_char(to_char(123, '9999999999EEEE')) into v_replace8 from dual;
    dbms_output.put_line(v_replace8);
	select to_char(to_char(100000000, 'FM9,9999,9999c')) into v_replace9 from dual;
    dbms_output.put_line(v_replace9);
	select to_char(to_char(1234, 'fmtm9')) into v_replace10 from dual;
    dbms_output.put_line(v_replace10);
	select to_char(to_number(to_char(1234,'999900EEEE'))) into v_replace11 from dual;
    dbms_output.put_line(v_replace11);
	select to_char(to_number(to_char(100.2356, '9,99999999Mi'))) into v_replace12 from dual;
    dbms_output.put_line(v_replace12);
	select value into v_replace13 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace13);
	select value into v_replace14 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace14);
	select value into v_replace15 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace15);
    select floor(to_number(to_char(1234,'999900EEEE'))) into v_number1 FROM dual;
    dbms_output.put_line(v_number1);
    select abs(to_number(to_char(123*0.34,'tM9'))) into v_number2 from DUAL;
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
insert into mag_user_basic values(1,substrb( to_char(123, '9999999999EEEE'),2,5));
insert into mag_user_basic values(2,initcap(to_char(100.2356, '99999,99VC')));
insert into mag_user_basic values(3,COALESCE(to_char(1485,'XXX'),to_char(1485,'XXX'),'3','4','5'));

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
--disable_warnings
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
####OBE-00600: internal error code, arguments: -4258, Incorrect string value
####call 中文存储过程1();
--echo # 展示结果
####select println() from dual;

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
insert into student values ('001',to_char(2,'9990999.9eeee'),to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into student values ('002',to_nchar(to_number(to_char(2,'9990999.9eeee'))),to_char(to_char(100000000, '9,9999,9999BL')||to_char(485,'999EEEE')));
insert into student values ('003',to_char(to_char(0.000, 'FMRn')||to_char(123, 'RN')),to_char(q'[to_char(1234.34,'0000.00X')]'));
insert into student values ('004',to_char(replace(to_char(3999, 'rN'),'mmmcmxcix','好的好的')),to_char(RPAD( to_char(23.12, '99.99BCMI'), 200, '$')));

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
insert into student values ('001',to_char(to_char(0.000, 'FMRn')||to_char(123, 'RN')),to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into student values ('002',to_nchar(to_number(to_char(2,'9990999.9eeee'))),to_char(to_char(100000000, '9,9999,9999BL')||to_char(485,'999EEEE')));
insert into student values ('003',to_char(to_char(0.000, 'FMRn')||to_char(123, 'RN')),to_char(q'[to_char(1234.34,'0000.00X')]'));
insert into student values ('004',to_char(replace(to_char(3999, 'rN'),'mmmcmxcix','好的好的')),to_char(RPAD( to_char(23.12, '99.99BCMI'), 200, '$')));

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
insert into student values ('001',REGEXP_COUNT( to_char(123, '999999999L') ,'$',1,'i'),to_char(12345, '9,,,9,,9,9,9$Mi'));
insert into student values ('002',to_nchar(to_number(to_char(2,'9990999.9eeee'))),to_char(to_char(100000000, '9,9999,9999BL')||to_char(485,'999EEEE')));
insert into student values ('003',to_char(to_char(0.000, 'FMRn')||to_char(123, 'RN')),to_char(q'[to_char(1234.34,'0000.00X')]'));
insert into student values ('004',to_char(replace(to_char(3999, 'rN'),'mmmcmxcix','好的好的')),to_char(RPAD( to_char(23.12, '99.99BCMI'), 200, '$')));

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
###insert into student values ('001',REGEXP_COUNT( to_char(123, '999999999L') ,'$',1,'i'),to_char(12345, '9,,,9,,9,9,9$Mi'));
###insert into student values ('002',to_nchar(to_number(to_char(2,'9990999.9eeee'))),to_char(to_char(100000000, '9,9999,9999BL')||to_char(485,'999EEEE')));
###insert into student values ('003',to_char(to_char(0.000, 'FMRn')||to_char(123, 'RN')),to_char(q'[to_char(1234.34,'0000.00X')]'));
###insert into student values ('004',to_char(replace(to_char(3999, 'rN'),'mmmcmxcix','好的好的')),to_char(RPAD( to_char(23.12, '99.99BCMI'), 200, '$')));
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


delimiter //;
DECLARE
   SUBTYPE name IS char(200);
   SUBTYPE message IS varchar2(1000);
   salutation name;
   greetings message;
BEGIN
   salutation := to_char(LPAD(  to_char(100000000, '9,9999,9999BL'), 200, '$'));
   greetings := to_char(COALESCE(to_char(100.2356, 'FM0XXX'),to_char(100.2356, 'FM0XXX'),'3','4','5')) ;
   dbms_output.put_line('Hello ' || salutation || greetings);
END;
//
delimiter ;//
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
v_char1:=to_char(123, '9999V999999EEEE');
v_char3:=to_char(100.2356, '99999,99C');
v_char4:=to_char(-485,'99$9Mi');
v_char5:=to_char(1234.34,'0000d00l');
v_char6:=to_char(100.2356, '99999,99U');
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
--disable_warnings
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
v_number1:=to_number(to_char(1234,'999900EEEE'));
v_number2:=to_number(to_char(29989329, '9.99EEEE'));
v_number3:=to_number(to_char(123*0.34,'9999.9'));
v_number4:=to_number(to_char(123, '9999999999EEEE'));
v_number5:=to_number(to_char(20080320.2365, '9.9EEEE'));
v_number6:=to_number(to_char('', 'fmtme'));
v_number7:=to_number(to_char(12345+12345, 'tM9'));
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
v_char1:=to_char(to_number(to_char(20080320.2365, '9.9EEEE')));
v_char3:=to_char(to_number(to_char(123, '999999V999')));
v_char4:=to_char(to_number(to_char(100.2356, '9,99999999Mi')));
v_char5:=to_char(to_number(to_char(12,'FM9990999.9pr')));
v_char6:=to_char(to_number(to_char('', 'fmtme')));
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
end P_TEST;
//
delimiter ;//
--enable_warnings
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
v_char1:=to_char(20, 'fmRn');
v_char3:=to_char(100.2356, '99999,99VC');
v_char4:=to_char(12345, '9,,,9,,9,9,9$Mi');
v_char5:=to_char(23.12, '99.99BCMI');
v_char6:=to_char(12345,'9G9G9G9g9');
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
  v1:=to_char(1234, 'L9999Mi');
  v2:=to_char(12345, '9,,,9,,9,9,9$Mi');
  v3:=to_char(3999, 'rN');
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
insert into yd2 values(1,to_char(1485,'XXX'));
insert into yd2 values(2,to_char(123, 'RN'));
insert into yd2 values(3,to_char(23.12, '99.99BCMI'));

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
              i_n1(1):=to_char(100000000,'99G9999999v'); --赋值，row number 最小为 1，最大为 EXTEND 分配的空间数
              i_n1(2):=to_char(12345, '9,,,9,,9,9,9$Mi');
			  DBMS_OUTPUT.put_line (i_n1(1));
			  DBMS_OUTPUT.put_line (i_n1(2));
      END;
//

###--echo # 展示结果
###select println() from dual;

delimiter ;//