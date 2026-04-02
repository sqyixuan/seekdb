--#####################################
--2.1、WRAP CLI 工具
--支持六种 PL/SQL 对象的不同语法场景
--#####################################
--##函数
--error 0,00600
drop function f00;
create function f00 return number is
begin
return 1;
end;
/
select f00() from dual;

create or replace function f01(a in int default 1,b in out int) return number is
begin
b:= a*a;
return b;
end;
/
declare
  a int := 0;
  b int;
  c int;
begin
  c := f01(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(c);
end;
/

create or replace function f02(a int := 2,b in out int) return number AUTHID DEFINER  is
c int;
begin
c := a/b;
return c;
end;
/
declare
  a int := 0;
  b int;
  c int;
begin
  c := f02(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(c);
end;
/

create or replace function f02(rand_range  IN INTEGER) return number AUTHID CURRENT_USER  as
  random_value number(24,0);
  random_seed int;
  seed1 int;
  seed2 int;
  BEGIN
    random_seed := to_number(to_char(systimestamp, 'FF6'));
    seed1 := mod(random_seed * 65537 + 55555555, rand_range);
    seed2 := mod(random_seed * 268435457, rand_range);
    random_value := mod(seed1 * 3 + seed2, rand_range);
    return random_value;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
         DBMS_OUTPUT.PUT_LINE('NO_DATA_FOUND');
      WHEN OTHERS THEN 
         DBMS_OUTPUT.PUT_LINE('ERR: '||SQLCODE||': '||SQLERRM);
END;
/
select f02(rand_range=>100) from dual;

--#存储过程
select 'case: pro00' from dual;
--error 0,00600
drop PROCEDURE pro00;
CREATE PROCEDURE pro00 IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('pro00');
END;
/
call pro00();

select 'case: pro01' from dual;
create or replace PROCEDURE pro01(a in int,b in int)  IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('pro01'||(a+b));
END;
/
call pro01(a=>10,b=>20);

select 'case: pro02' from dual;
/*
CREATE OR REPLACE EDITIONABLE PROCEDURE pro02(a in out int,b in out int)  IS
BEGIN
  a := a/b;
  DBMS_OUTPUT.PUT_LINE('pro02');
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro02(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(a);
end;
/
*/

select 'case: pro03' from dual;
/*
CREATE OR REPLACE NONEDITIONABLE PROCEDURE pro03(a out int,b out int) IS
BEGIN
  a := a*b;
  DBMS_OUTPUT.PUT_LINE('pro03');
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro03(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(a);
end;
/
*/

select 'case: pro04' from dual;
CREATE OR REPLACE PROCEDURE pro04(a in out int,b out int) AUTHID DEFINER as
BEGIN
  b := a*a;
  DBMS_OUTPUT.PUT_LINE('pro04'||b);
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro04(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(b);
end;
/

select 'case: pro05' from dual;
CREATE OR REPLACE PROCEDURE pro05(a out int,b out int) AUTHID CURRENT_USER IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('pro05');
  a := a/b;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN 
     DBMS_OUTPUT.PUT_LINE('NO_DATA_FOUND');
  WHEN OTHERS THEN 
     DBMS_OUTPUT.PUT_LINE('ERR: '||SQLCODE||': '||SQLERRM);
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro05(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(a);
end;
/

--#包
--error 0,00942
drop table tab1;
CREATE TABLE tab1 (c1 int, c2 varchar2(4000) DEFAULT 'syn1_table');

--error 0,00600
drop PACKAGE pack00;
CREATE PACKAGE pack00 AS
  a NUMBER;
END pack00;
/

CREATE OR REPLACE PACKAGE pack00 AS
  a NUMBER;
END pack00;
/

CREATE OR REPLACE PACKAGE pack01
AUTHID DEFINER -- 定义权限为调用者，或使用 CURRENT_USER 表示调用者权限
AS
  a varchar2(2000) := 'pack01';
  -- 公有类型声明
  TYPE emp_record IS RECORD (
    emp_id NUMBER,
    emp_name VARCHAR2(100)
  );
  -- 公有游标声明
  CURSOR emp_cursor IS
    SELECT c1, c2 FROM tab1;
  -- 公有变量声明
  total_employees NUMBER := 0;
  -- 函数声明
  FUNCTION get_employee_count RETURN NUMBER;
  -- 过程声明
  PROCEDURE update_employee(emp_id NUMBER, emp_name VARCHAR2);
END pack01;
/
select pack01.a from dual;

create or replace package pack02 
AUTHID CURRENT_USER 
is
  procedure pack_pro;
  function pack_func return number;
end;
/
create or replace package body pack02 is
  procedure pack_pro is
    a tab1%rowtype;
    b tab1.c1%type;
  begin
    DBMS_OUTPUT.PUT_LINE('◆pack02');
  end;

  function pack_func return number is
    a tab1%rowtype;
    b tab1.c1%type;
  begin
    DBMS_OUTPUT.PUT_LINE('◆pack02');
    return 1;
  end;
end;
/
declare
  var1 number;
begin
  pack02.pack_pro;
  var1 := pack02.pack_func;
  DBMS_OUTPUT.PUT_LINE('pack02: ' || var1);
end;
/

create or replace package pack03
AUTHID CURRENT_USER 
is
  procedure pack_pro;
  function pack_func return number;
end;
/
create package body pack03 as
  procedure pack_pro is
  begin
    pack02.pack_pro();
    DBMS_OUTPUT.PUT_LINE('◆pack03');
  end;

  function pack_func return number is
  begin
    DBMS_OUTPUT.PUT_LINE('◆pack03');
    return pack02.pack_func();
  end;
end pack03;
/
declare
  var1 number;
begin
  pack03.pack_pro;
  var1 := pack03.pack_func;
  DBMS_OUTPUT.PUT_LINE('pack03: ' || var1);
end;
/

--error 00201
create or replace package pack04
AUTHID CURRENT_USER 
is
  a number;
end pack04444;
/

--#OBJECT
select 'case: obj00' from dual;
--error 0,00600
drop TYPE obj00;
CREATE TYPE obj00 AS OBJECT (
  a VARCHAR2(50)
);
/
select obj00('obj00') from dual;

select 'case: obj01' from dual;
CREATE or replace TYPE obj01 AS OBJECT (
  a VARCHAR2(50),
  MEMBER FUNCTION get_age RETURN NUMBER
);
/
CREATE TYPE BODY obj01 AS
  MEMBER FUNCTION get_age RETURN NUMBER IS
  BEGIN
    RETURN a;
  END get_age;
END;
/
select obj01('obj01') from dual;

select 'case: obj02' from dual;
CREATE or replace EDITIONABLE TYPE obj02 AS OBJECT (
  a VARCHAR2(50),
  MEMBER FUNCTION get_age RETURN NUMBER
);
/
;
/*
CREATE or replace EDITIONABLE TYPE BODY obj02 AS
  MEMBER FUNCTION get_age RETURN NUMBER IS
  BEGIN
    RETURN a;
  END get_age;
END;
/
*/
select obj02('obj02') from dual;

select 'case: obj03' from dual;
CREATE or replace NONEDITIONABLE TYPE obj03 AS OBJECT (
  a VARCHAR2(50),
  MEMBER FUNCTION get_age RETURN NUMBER
);
/
;
/*
--#CREATE or replace NONEDITIONABLE TYPE BODY obj03 AS
--#  MEMBER FUNCTION get_age RETURN NUMBER IS
--#  BEGIN
--#    RETURN a;
--#  END get_age;
--#END;
--#/
*/
select obj03('obj03') from dual;

select 'case: obj04' from dual;
CREATE or replace NONEDITIONABLE TYPE obj04 force AS OBJECT (
  a VARCHAR2(50),
  MEMBER FUNCTION get_age RETURN NUMBER
);
/
;
select obj04('obj04') from dual;

select 'case: obj05' from dual;
--error 00600
drop type obj05 force;
CREATE TYPE obj05 AS OBJECT (
  name VARCHAR2(50),
  age NUMBER,
  MEMBER FUNCTION get_age RETURN NUMBER);
/
CREATE OR REPLACE TYPE BODY obj05 AS
  MEMBER FUNCTION get_age RETURN NUMBER IS
  BEGIN
    RETURN self.age;
  END get_age;
END;
/
select obj05(1,1) from dual;

select 'case: obj06' from dual;
--error 0,00600
drop type obj06 force;
create or replace type obj06 force as object(
  a1 number,
  a2 varchar(2000),
  a3 date,
  member procedure getvalue(p1 in out number,p2 in out varchar2,p3 in out date));
/
create or replace type body obj06 is
  member procedure getvalue(p1 in out number,p2 in out varchar2,p3 in out date) is
  begin
    p1 := a1;
    p2 := a2;
    p3 := a3;
    DBMS_OUTPUT.PUT_LINE('◆udt_obj1属性值：'||a1||','||a2||','||a3);
  end;
end;
/
select obj06(1,1,null) from dual;

select 'case: obj07' from dual;
create or replace type obj07 as object(
  t1 number,
  t2 varchar(2000),
  t3 date,
  constructor function obj07(t1 number) RETURN SELF as result,
  static procedure type_pro(a varchar,b varchar),
  member function type_func(a varchar,b varchar)return number,
  --MAP MEMBER FUNCTION GET_GRADE RETURN number
  ORDER MEMBER FUNCTION MATCH(arg1 IN obj07)RETURN number
);
/
create or replace type body obj07 is
  constructor function obj07(t1 number) RETURN SELF as result is
  begin
    SELF.t1 := t1 * 2;
    SELF.t2 := t1 * 3;
  return;
  end;
  
  static procedure type_pro(a varchar,b varchar) is
  begin
    DBMS_OUTPUT.PUT_LINE('◆◆◆◆type_proc');
  end;
 
 member function type_func(a varchar,b varchar)return number is
 begin
    DBMS_OUTPUT.PUT_LINE('◆◆◆◆type_func');
    return 'type_func';
 end;

 --MAP MEMBER FUNCTION GET_GRADE RETURN number as
 --BEGIN
 -- RETURN 1;
 --END;

ORDER MEMBER FUNCTION MATCH(arg1 IN obj07)RETURN number as
begin
return t1-arg1.t1;
END;
end;
/
declare
  var1 obj07;
  var2 obj07;
begin
  var1 := obj07(1,'obj07',sysdate);
  var2 := obj07(2,'obj07',sysdate);
  
  dbms_output.put_line(var1.t1 || var1.t2);
  --dbms_output.put_line(var1.GET_GRADE());

  IF var1>var2 THEN
    DBMS_OUTPUT.PUT_LINE(var1.t1);
  ELSIF var1<var2 THEN
    DBMS_OUTPUT.PUT_LINE(var2.t1);
  ELSE
    DBMS_OUTPUT.PUT_LINE('EQUAL');
  END IF;
end;
/

