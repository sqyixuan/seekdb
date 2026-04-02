--echo ######自定义数据类型-一般赋值运算-字符表达式######
delimiter //;
CREATE OR REPLACE TYPE 类型2 AS OBJECT (
    v_number1 number,
    MEMBER PROCEDURE procedure2
);
//
CREATE OR REPLACE TYPE BODY 类型2 AS
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
create or replace package P包 is

  procedure 存储过程1;
  procedure procedure2;
end P包;
//
create or replace package body P包
 as
--echo ######包-一般赋值运算-数值表达式######

  procedure 存储过程1 as
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
end P包;
//

delimiter ;//
call P包.存储过程1();
call P包.procedure2();

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
v变量 long;
begin
  v变量:=concat(chr(120),chr(120));
  dbms_output.put_line(v变量);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


delimiter //;

--echo ######函数嵌套测试--包--空函数嵌套######
CREATE OR REPLACE PACKAGE 包3 AS
  v1 char:=chr(nullif(1,2));
  v2 char:=chr(nvl('','12'));
  v3 char:=chr(coalesce(null,null,121,2,3));
  v4 char:=chr(to_char(''));
  procedure 存储过程1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char('')));
  function 函数1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char(''))) return char;
END;
//
CREATE OR REPLACE PACKAGE BODY 包3 AS
procedure 存储过程1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char(''))) is
  begin
    dbms_output.put_line('v1='||v1);
    dbms_output.put_line('v2='||v2);
    dbms_output.put_line('v3='||v3);
    dbms_output.put_line('v4='||v4);
  end;
function 函数1(v1 char:=chr(nullif(1,2)),v2 char:=chr(nvl('','12')),v3 char:=chr(coalesce(null,null,121,2,3)),v4 char:=chr(to_char(''))) return char
  is
  begin
    return v1;
  end;
end;
//

delimiter ;//
--echo # 展示结果
call 包3.存储过程1();
select println() from dual;


delimiter //;
--echo ######varchar2字符设置，无约束######
DECLARE
  SUBTYPE  子类型1 IS  varchar2(32);
  SUBTYPE  子类型2 IS  varchar2(2 char);
  v1   子类型1;
  v2   子类型2;
  v3   子类型2;
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


delimiter //;

--echo ##################################复合数据类型##################################

--echo ######not null约束，未赋值，复合预期的异常######
delimiter ;//
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
##
##delimiter ;//
##--echo # 展示结果
##select println() from dual;


delimiter //;
--error 0,600
###VARRAYs 变量数组
DECLARE TYPE 数组名称 IS VARRAY (2) OF VARCHAR2 (10); --创建 Collection，需要指定最大范围
              i_n1 数组名称 := 数组名称(); --创建 instance，需要使用构造函数
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

##--disable_warnings
##--error 0,942
##drop table employees;
##--enable_warnings
##
##CREATE TABLE employees(EMPNO NUMBER(4) PRIMARY KEY,
##ENAME NVARCHAR2(100),
##JOB NVARCHAR2(9),
##MGR NUMBER(4),
##HIREDATE DATE,
##SAL NUMBER(7,2),
##COMM NUMBER(7,2),
##DEPTNO NUMBER(2));
##
##INSERT INTO employees VALUES
##(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
##INSERT INTO employees VALUES
##(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
##INSERT INTO employees VALUES
##(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
##INSERT INTO employees VALUES
##(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
##
##delimiter //;
##--error 0,600
##DECLARE
##   v_empno号码 employees.EMPNO%TYPE;
##   v_sal工资   employees.SAL%TYPE;
##BEGIN
##   SELECT SAL INTO v_sal工资 FROM employees WHERE EMPNO = v_empno号码;
##   IF v_sal工资<=1500 THEN
##        UPDATE employees SET SAL = SAL + 100 WHERE EMPNO=v_empno号码;
##        DBMS_OUTPUT.PUT_LINE('编码为'||v_empno号码||'员工工资已更新!'); 
##   ELSE
##        DBMS_OUTPUT.PUT_LINE('编码为'||v_empno号码||'员工工资已经超过规定值!');
##   END IF;
##EXCEPTION
##   WHEN NO_DATA_FOUND THEN  
##      DBMS_OUTPUT.PUT_LINE('数据库中没有编码为'||v_empno号码||'的员工');
##   WHEN TOO_MANY_ROWS THEN
##      DBMS_OUTPUT.PUT_LINE('程序运行错误!请使用游标');
##   WHEN OTHERS THEN
##      DBMS_OUTPUT.PUT_LINE(SQLCODE||'---'||SQLERRM);
##END; 
##//
##
##delimiter ;//
##--echo # 展示结果
##select println() from dual;

delimiter //;
delimiter ;//