
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
v_char1:=ASCII('中');
v_char3:=LOWER('中文测试001a');
v_char4:=UPPER('中文测试001a');
v_char5:=LTRIM(' 中文测试001a');
v_char6:=RTRIM('中文测试001a ');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
END;
//

delimiter ;//

--echo # 展示结果
select println() from dual;


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
v_number1:=CEIL(9.99999);
v_number2:=FLOOR(9.0000001);
v_number3:=ROUND(4.444,2);
v_number4:=ABS(-2);
v_number5:=POWER(2,5);
v_number6:=SIGN(-2);
v_number7:=SQRT(16);
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
v_char1:=ASCII('中');
v_char3:=LOWER('中文测试001a');
v_char4:=UPPER('中文测试001a');
v_char5:=LTRIM(' 中文测试001a');
v_char6:=RTRIM('中文测试001a ');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
end P_TEST;
//
call P_TEST.procedure1();
call P_TEST.procedure2();
//

delimiter ;//

--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######匿名块-一般赋值运算-字符表达式######
declare
v_char1 VARCHAR2(100);
v_char3 VARCHAR2(100);
v_char4 VARCHAR2(100);
v_char5 VARCHAR2(100);
v_char6 VARCHAR2(100);
begin
v_char1:=ASCII('中');
v_char3:=LOWER('中文测试001a');
v_char4:=UPPER('中文测试001a');
v_char5:=LTRIM(' 中文测试001a');
v_char6:=RTRIM('中文测试001a ');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--一般赋值测试--concat函数与其它函数的嵌套######
declare
v long;
begin
  v:=concat(chr(120),chr(120));
  dbms_output.put_line(v);
  v:=concat(concat(chr(120),'y'),chr(120));
  dbms_output.put_line(v);
  v:=concat(concat(initcap('汉1lo@@@!$...orld'),initcap('汉12@@!$rld,汉elloworld')),initcap('汉121world'));
  dbms_output.put_line(v);
  v:=concat(concat(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111')),concat(trim(trailing '1' from '11123sd12fd111'),trim(' asadd  ')));
  dbms_output.put_line(v);
  v:=concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=concat(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试'));
  dbms_output.put_line(v);
  v:=concat(NLS_UPPER('A1Cd测试'),NLS_UPPER('A1Cd测试'));
  dbms_output.put_line(v);
  v:=concat(replace('abcdefg','abc','测试'),replace('abcdefg','abc','测试'));
  dbms_output.put_line(v);
  v:=concat(lpad('123456',7,0),rpad('123456',7,0));
  dbms_output.put_line(v); 
  v:=concat(concat(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678')),concat(UPPER('asdf'), ASCII('asd')));
  dbms_output.put_line(v);
  v:=concat(concat(concat(instr('helloworld','l'),instr('重庆某软件公司','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc'))));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;




delimiter //;

--echo ######函数嵌套测试--一般赋值测试--INITCAP函数与其它函数的嵌套######
declare
v long;
begin
  v:=INITCAP(chr(120));
  dbms_output.put_line(v);
  v:=INITCAP(concat(chr(120),'y'));
  dbms_output.put_line(v);
  v:=INITCAP(concat(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111')));
  dbms_output.put_line(v);
  v:=INITCAP(concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')));
  dbms_output.put_line(v);
  v:=INITCAP(concat(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=INITCAP(concat(NLS_UPPER('A1Cd测试'),NLS_UPPER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=INITCAP(concat(replace('abcdefg','abc','测试'),replace('abcdefg','abc','测试')));
  dbms_output.put_line(v);
  v:=INITCAP(concat(lpad('123456',7,0),rpad('123456',7,0)));
  dbms_output.put_line(v); 
  v:=INITCAP(concat(concat(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678')),concat(UPPER('asdf'), ASCII('asd'))));
  dbms_output.put_line(v);
  v:=INITCAP(concat(concat(concat(instr('helloworld','l'),instr('重庆某软件公司','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc')))));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;



delimiter //;

--echo ######函数嵌套测试--一般赋值测试--TRIM函数与其它函数的嵌套######
declare
v long;
begin
  v:=TRIM(chr(120));
  dbms_output.put_line(v);
  v:=TRIM(concat(chr(120),'y'));
  dbms_output.put_line(v);
  v:=TRIM(concat(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111')));
  dbms_output.put_line(v);
  v:=TRIM(concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')));
  dbms_output.put_line(v);
  v:=TRIM(concat(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=TRIM(concat(NLS_UPPER('A1Cd测试'),NLS_UPPER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=TRIM(concat(replace('abcdefg','abc','测试'),replace('abcdefg','abc','测试')));
  dbms_output.put_line(v);
  v:=TRIM(concat(lpad('123456',7,0),rpad('123456',7,0)));
  dbms_output.put_line(v); 
  v:=TRIM(concat(concat(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678')),concat(UPPER('asdf'), ASCII('asd'))));
  dbms_output.put_line(v);
  v:=TRIM(concat(concat(concat(instr('helloworld','l'),instr('重庆某软件公司','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc')))));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--一般赋值测试--LTRIM函数与其它函数的嵌套######
declare
v long;
begin
  v:=LTRIM(chr(120));
  dbms_output.put_line(v);
  v:=LTRIM(concat(chr(120),'y'));
  dbms_output.put_line(v);
  v:=LTRIM(concat(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111')));
  dbms_output.put_line(v);
  v:=LTRIM(concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')));
  dbms_output.put_line(v);
  v:=LTRIM(concat(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=LTRIM(concat(NLS_UPPER('A1Cd测试'),NLS_UPPER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=LTRIM(concat(replace('abcdefg','abc','测试'),replace('abcdefg','abc','测试')));
  dbms_output.put_line(v);
  v:=LTRIM(concat(lpad('123456',7,0),rpad('123456',7,0)));
  dbms_output.put_line(v); 
  v:=LTRIM(concat(concat(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678')),concat(UPPER('asdf'), ASCII('asd'))));
  dbms_output.put_line(v);
  v:=LTRIM(concat(concat(concat(instr('helloworld','l'),instr('重庆某软件公司','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc')))));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--一般赋值测试--RTRIM函数与其它函数的嵌套######
declare
v long;
begin
  v:=RTRIM(chr(120));
  dbms_output.put_line(v);
  v:=RTRIM(concat(chr(120),'y'));
  dbms_output.put_line(v);
  v:=RTRIM(concat(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111')));
  dbms_output.put_line(v);
  v:=RTRIM(concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')));
  dbms_output.put_line(v);
  v:=RTRIM(concat(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=RTRIM(concat(NLS_UPPER('A1Cd测试'),NLS_UPPER('A1Cd测试')));
  dbms_output.put_line(v);
  v:=RTRIM(concat(replace('abcdefg','abc','测试'),replace('abcdefg','abc','测试')));
  dbms_output.put_line(v);
  v:=RTRIM(concat(lpad('123456',7,0),rpad('123456',7,0)));
  dbms_output.put_line(v); 
  v:=RTRIM(concat(concat(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678')),concat(UPPER('asdf'), ASCII('asd'))));
  dbms_output.put_line(v);
  v:=RTRIM(concat(concat(concat(instr('helloworld','l'),instr('重庆某软件公司','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc')))));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--一般赋值测试--translate函数与其它函数的嵌套######
declare
v long;
begin
  v:=translate(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111'),ltrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=translate(nls_upper(chr(120)),nls_upper(concat(chr(120),'y')),concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')));
  dbms_output.put_line(v);
  v:=translate(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试'),NLS_UPPER('A1Cd测试'));
  dbms_output.put_line(v);
  v:=translate(replace('abcdefg','abc','测试'),lpad('123456',7,0),rpad('123456',7,0));
  dbms_output.put_line(v);
  v:=translate(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678'),UPPER('asdf'));
  dbms_output.put_line(v);
  v:=translate(ASCII('asd'),instr('helloworld','l'),length('cctv'));
  dbms_output.put_line(v);
  v:=translate(lengthb('string'),nvl('aa','bb'),NULLIF('a', 'b'));
  dbms_output.put_line(v);
  v:=translate(COALESCE(NULL,NULL,'aa','bb','cc'),nvl('aa','bb'),NULLIF('a', 'b'));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--一般赋值测试--LENGTH函数与其它函数的嵌套######
declare
v long;
begin
  v:= LENGTH(chr(120));
  dbms_output.put_line(v);
  v:= LENGTH(concat(chr(120),'y'));
  dbms_output.put_line(v);
  v:= LENGTH(concat(trim(both '1' from '11123sd12fd111'),trim(leading '1' from '11123sd12fd111')));
  dbms_output.put_line(v);
  v:= LENGTH(concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')));
  dbms_output.put_line(v);
  v:= LENGTH(concat(NLS_LOWER('A1Cd测试'),NLS_LOWER('A1Cd测试')));
  dbms_output.put_line(v);
  v:= LENGTH(concat(NLS_UPPER('A1Cd测试'),NLS_UPPER('A1Cd测试')));
  dbms_output.put_line(v);
  v:= LENGTH(concat(replace('abcdefg','abc','测试'),replace('abcdefg','abc','测试')));
  dbms_output.put_line(v);
  v:= LENGTH(concat(lpad('123456',7,0),rpad('123456',7,0)));
  dbms_output.put_line(v); 
  v:= LENGTH(concat(concat(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678')),concat(UPPER('asdf'), ASCII('asd'))));
  dbms_output.put_line(v);
  v:=LENGTH(concat(concat(concat(instr('helloworld','l'),instr('重庆某软件公司','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc')))));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;



delimiter //;

--echo ######函数嵌套测试--一般赋值测试--lpad函数与其它函数的嵌套######
declare
v long;
begin
  v:=lpad(trim(both '1' from '11123sd12fd111'),1,ltrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=lpad(nls_upper(chr(120)),1,rtrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=lpad(NLS_LOWER('A1Cd测试'),1,NLS_UPPER('A1Cd测试'));
  dbms_output.put_line(v);
  v:=lpad(replace('abcdefg','abc','测试'),1,lpad('123456',7,0));
  dbms_output.put_line(v);
  v:=lpad(substr('HelloWorld',0,3),1,translate('kvenchou_zhou','kvenchou','12345678'));
  dbms_output.put_line(v);
  v:=lpad(ASCII('asd'),1,length('cctv'));
  dbms_output.put_line(v);
  v:=lpad(lengthb('string'),1,NULLIF('a', 'b'));
  dbms_output.put_line(v);
  v:=lpad(COALESCE(NULL,NULL,'aa','bb','cc'),1,nvl('aa','bb'));
  dbms_output.put_line(v);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--一般赋值测试--nullif函数与其它函数的嵌套######
declare
v long;
begin
  v:=NULLIF(trim(both '1' from '11123sd12fd111'),ltrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=NULLIF(nls_upper(chr(120)),rtrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=NULLIF(NLS_LOWER('A1Cd测试'),NLS_UPPER('A1Cd测试'));
  dbms_output.put_line(v);
  v:=NULLIF(replace('abcdefg','abc','测试'),lpad('123456',7,0));
  dbms_output.put_line(v);
  v:=NULLIF(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678'));
  dbms_output.put_line(v);
  v:=NULLIF(ASCII('asd'),length('cctv'));
  dbms_output.put_line(v);
  v:=NULLIF(COALESCE(NULL,NULL,'aa','bb','cc'),nvl('aa','bb'));
  dbms_output.put_line(v);
  v:=NULLIF(lengthb('string'),NULLIF(1, 2));
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;



delimiter //;

--echo ######函数嵌套测试--一般赋值测试--coalesce函数与其它函数的嵌套######
declare
v long;
begin
  v:=coalesce(trim(both '1' from '11123sd12fd111'),ltrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=coalesce(nls_upper(chr(120)),rtrim('ccbcminb','cb'));
  dbms_output.put_line(v);
  v:=coalesce(NLS_LOWER('A1Cd测试'),NLS_UPPER('A1Cd测试'));
  dbms_output.put_line(v);
  v:=coalesce(replace('abcdefg','abc','测试'),lpad('123456',7,0));
  dbms_output.put_line(v);
  v:=coalesce(substr('HelloWorld',0,3),translate('kvenchou_zhou','kvenchou','12345678'));
  dbms_output.put_line(v);
  v:=coalesce(ASCII('asd'),length('cctv'));
  dbms_output.put_line(v);
  v:=coalesce(COALESCE(NULL,NULL,'aa','bb','cc'),nvl('aa','bb'));
  dbms_output.put_line(v);
  v:=coalesce(lengthb('string'),NULLIF(1, 2));
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;



delimiter //;

--echo ######函数嵌套测试--包--空函数嵌套######
CREATE OR REPLACE PACKAGE pkg3 AS
  v1 char:=chr(nullif(1,2));
  v2 char:=chr(nvl('','12'));
  v3 char:=chr(coalesce(null,null,121,2,3));
  v4 char:=chr(to_char(''));
  procedure p1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char('')));
  function f1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char(''))) return char;
END;
//
CREATE OR REPLACE PACKAGE BODY pkg3 AS
procedure p1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char(''))) is
  begin
    dbms_output.put_line('v1='||v1);
    dbms_output.put_line('v2='||v2);
    dbms_output.put_line('v3='||v3);
    dbms_output.put_line('v4='||v4);
  end;
function f1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char(''))) return char
  is
  begin
    return v1;
  end;
end;
//

delimiter ;//
--echo # 展示结果
call pkg3.p1();
select println() from dual;



delimiter //;

--echo ######函数嵌套测试--包--concat函数与其它函数的嵌套######
CREATE OR REPLACE PACKAGE pkg4 AS
  v1 varchar2(200):=concat(chr(120),chr(120));
  
  v2 varchar2(200):=concat(concat(chr(120),'y'),chr(120));
 
  v3 varchar2(200):=concat(concat(initcap('汉1lo@@@!$...orld'),initcap('汉1d,汉ell')),initcap('汉1'));
 
  v4 varchar2(200):=concat(concat(trim(both '1' from '11123sd1'),trim(leading '1' from '11121')),concat(trim(trailing '1' from 'd111'),trim(' asadd  ')));
 
  v5 varchar2(200):=concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb'));
 
  v6 varchar2(200):=concat(NLS_LOWER('测试'),NLS_LOWER('A1C'));
 
  v7 varchar2(200):=concat(NLS_UPPER('A1C'),NLS_UPPER('测试'));
 
  v8 varchar2(200):=concat(replace('efg','abc','测试'),replace('efg','abc','测试'));
 
  v9 varchar2(200):=concat(lpad('123',7,0),rpad('1256',7,0));
 
  v10 varchar2(200):=concat(concat(substr('Helld',0,3),translate('kvhu','kvu','12348')),concat(UPPER('asdf'), ASCII('asd')));
 
  v11 varchar2(200):=concat(concat(concat(instr('hd','l'),instr('重庆','某',1,1)),concat( length('cctv'),lengthb('string'))),concat(concat(nvl('aa','bb'),'aaa'),concat(NULLIF('a', 'b'),COALESCE(NULL,NULL,'aa','bb','cc'))));
 
  procedure p1(v1 varchar2:=concat(chr(120),chr(120)),v2 varchar2:=concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')),v3 varchar2:=concat(replace('efg','abc','测试'),replace('efg','abc','测试')),v4 varchar2:=concat(lpad('123',7,0),rpad('1256',7,0)));
  function f1(v1 varchar2:=concat(chr(120),chr(120)),v2 varchar2:=concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')),v3 varchar2:=concat(replace('efg','abc','测试'),replace('efg','abc','测试')),v4 varchar2:=concat(lpad('123',7,0),rpad('1256',7,0))) return varchar2;
END;
//
CREATE OR REPLACE PACKAGE BODY pkg4 AS
procedure p1(v1 varchar2:=concat(chr(120),chr(120)),v2 varchar2:=concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')),v3 varchar2:=concat(replace('efg','abc','测试'),replace('efg','abc','测试')),v4 varchar2:=concat(lpad('123',7,0),rpad('1256',7,0))) is
  begin
    dbms_output.put_line('v1='||v1);
    dbms_output.put_line('v2='||v2);
    dbms_output.put_line('v3='||v3);
    dbms_output.put_line('v4='||v4);
  end;
function f1(v1 varchar2:=concat(chr(120),chr(120)),v2 varchar2:=concat(ltrim('ccbcminb','cb'),rtrim('ccbcminb','cb')),v3 varchar2:=concat(replace('efg','abc','测试'),replace('efg','abc','测试')),v4 varchar2:=concat(lpad('123',7,0),rpad('1256',7,0))) return varchar2
  is
  begin
    return v1;
  end;
end;
//

delimiter ;//
--echo # 展示结果
call pkg4.p1();
select println() from dual;



delimiter //;

--echo ######varchar2字符设置，无约束######
DECLARE
  SUBTYPE  sub1_varchar2 IS  varchar2(32);
  SUBTYPE  sub2_varchar2 IS  varchar2(2 char);
  v1   sub1_varchar2;
  v2   sub2_varchar2;
  v3   sub2_varchar2;
BEGIN
  v1:='hello world';
  v2:='12';
  v3:='测试';
  dbms_output.put_line(v1);
  dbms_output.put_line(v2);
  dbms_output.put_line(v3);
END;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


--echo ##################################复合数据类型##################################
--echo ######not null约束，未赋值，复合预期的异常######
--disable_warnings
--error 0,942
drop table yd2;
--enable_warnings
create table yd2(id number,name3 VARCHAR2(3) not null);

--echo ######%rowtype 无约束######
insert into yd2 values(1,'中');
insert into yd2 values(2,'问');
insert into yd2 values(3,'测');

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
--echo # 展示结果
select println() from dual;


--echo ######%ROWTYPE Variable Does Not Inherit Initial Values or Constraints######
--disable_warnings
--error 0,942
DROP TABLE t1;
--enable_warnings
CREATE TABLE t1 (
  c1 INTEGER DEFAULT 0 NOT NULL,
  c2 INTEGER DEFAULT 1 NOT NULL
);
delimiter //;
DECLARE
  subtype t1_row is t1%ROWTYPE;
  t t1_row;
BEGIN
  DBMS_OUTPUT.PUT(t.c1); 
  DBMS_OUTPUT.PUT(t.c2);
END;
//

delimiter ;//
--echo # 展示结果
select println() from dual;

##delimiter //;
##--echo ######collection(associative array) 无约束######
##DECLARE 
##TYPE t_name IS TABLE OF varchar2(2) INDEX BY PLS_INTEGER;
##subtype mysub is t_name;
##l_row PLS_INTEGER;
##v mysub;
##BEGIN
##v(1):='不';
##END;
##//
##
##--echo ######collection(VARRAY) 无约束######
##DECLARE 
##TYPE t_name IS VARRAY (2) OF VARCHAR2 (3);
##subtype mysub is t_name;
##i_n1 mysub := t_name();
##BEGIN
##i_n1.EXTEND(2);
##i_n1(1):='中';
##i_n1(2):='文';
##END;
##//
##
#####嵌套表
##DECLARE TYPE t_name IS TABLE OF varchar2(10); --创建 Collection，和 Associative arrays 相差一个 INDEX BY
##              i_n1 t_name := t_name(); --创建 instance，需要使用构造函数
##              i_n2 t_name := t_name();
##              i_n3 t_name := t_name();
##      BEGIN
##              i_n1.EXTEND(2); --赋值前要分配空间
##              i_n1(1):='中'; --赋值，row number 最小为 1，最大为 EXTEND 分配的空间数，可以跳过某个行不分配则为 null
##              i_n1(2):='文';
##              i_n2.EXTEND;     --分配一个空间
##              i_n2(1):='好';
##              i_n3 := i_n1 MULTISET EXCEPT i_n2; --10g 开始提供的功能，将 i_n1 中除去 i_n2 的值，赋值给 i_n3
##              FOR l_row IN i_n1.FIRST .. i_n1.LAST --从行首到行尾，依次输出
##              LOOP
##                 DBMS_OUTPUT.put_line (i_n1(l_row));
##              END LOOP;
##      END;
##	  //
##
##delimiter ;//
##--echo # 展示结果
##select println() from dual;


delimiter //;
###VARRAYs 变量数组
DECLARE TYPE t_name IS VARRAY (2) OF VARCHAR2 (10); --创建 Collection，需要指定最大范围
              i_n1 t_name := t_name(); --创建 instance，需要使用构造函数
      BEGIN
              i_n1.EXTEND(2); --赋值前要分配空间，不能超过最大范围
              i_n1(1):='中文'; --赋值，row number 最小为 1，最大为 EXTEND 分配的空间数
              i_n1(2):='中';
			  DBMS_OUTPUT.put_line (i_n1(1));
			  DBMS_OUTPUT.put_line (i_n1(2));
      END;
	  //
	  
delimiter ;//
--echo # 展示结果
select println() from dual;

--disable_warnings
--error 0,942
drop table employees;
--enable_warnings

CREATE TABLE employees(EMPNO NUMBER(4) PRIMARY KEY,
ENAME NVARCHAR2(100),
JOB NVARCHAR2(9),
MGR NUMBER(4),
HIREDATE DATE,
SAL NUMBER(7,2),
COMM NUMBER(7,2),
DEPTNO NUMBER(2));

INSERT INTO employees VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO employees VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO employees VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO employees VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);

delimiter //;
DECLARE
   v_empno employees.EMPNO%TYPE;
   v_sal   employees.SAL%TYPE;
BEGIN
   SELECT SAL INTO v_sal FROM employees WHERE EMPNO = v_empno;
   IF v_sal<=1500 THEN 
        UPDATE employees SET SAL = SAL + 100 WHERE EMPNO=v_empno; 
        DBMS_OUTPUT.PUT_LINE('编码为'||v_empno||'员工工资已更新!');     
   ELSE
        DBMS_OUTPUT.PUT_LINE('编码为'||v_empno||'员工工资已经超过规定值!');
   END IF;
EXCEPTION
   WHEN NO_DATA_FOUND THEN  
      DBMS_OUTPUT.PUT_LINE('数据库中没有编码为'||v_empno||'的员工');
   WHEN TOO_MANY_ROWS THEN
      DBMS_OUTPUT.PUT_LINE('程序运行错误!请使用游标');
   WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(SQLCODE||'---'||SQLERRM);
END; 
//

delimiter ;//
--echo # 展示结果
select println() from dual;