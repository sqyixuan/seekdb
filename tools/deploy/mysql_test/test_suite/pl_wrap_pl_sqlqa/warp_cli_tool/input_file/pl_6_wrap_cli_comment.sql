--#####################################
--2.1、WRAP CLI 工具
--六种 DDL 语句识别出来并做 wrap 操作，其他的部分原样保留
--#####################################
--注释（有选项控制是否保留注释）、其他DDL/DML等语句、意义不明的字符。
~！@#￥%……&*（）——+~！@#￥%……&*（）——+
   ~！@#￥%……&*（）——+~！@#￥%……&*（）——+

111111111111111111111111111111111111
====================================
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

--#####################################
drop table t1;
create table t1(c0 number);

drop table t10;
create table t10(c0 number, c1 number);
insert into t10 select level,level from dual connect by level<=10;
update t10 set c0=10 where c1 = 1;
delete from t10 where c1 = 2;

begin
for i in 1..10 loop
insert into t10 select level,level from dual connect by level<=10;
end loop;
end;
/

create or replace trigger tri_t10 before insert on t10 for each row
declare
procedure p1(a int default :new.c0)is
v_sql varchar2(500):='begin insert into t1 values(:a);end;';
cur_hdl integer;
v_row number;
begin
cur_hdl := dbms_sql.open_cursor;
dbms_sql.parse(cur_hdl, v_sql, dbms_sql.native);
dbms_sql.bind_variable(cur_hdl, 'a', a);
v_row:=dbms_sql.execute(cur_hdl);
dbms_sql.close_cursor(cur_hdl);
end;
begin
p1();
end;
/
ALTER TRIGGER tri_t10 COMPILE;
insert into t10 values(0,0);
update t10 set c0=10;
drop trigger tri_t10;

declare
v_cursor number:=dbms_sql.open_cursor;
v_number1 dbms_sql.number_table;
lv_i number(10);
begin
dbms_sql.parse(v_cursor,'select 1 from dual',1);
dbms_sql.define_array(v_cursor,1,v_number1,3,-1);
lv_i:=dbms_sql.execute(v_cursor);
lv_i := dbms_sql.fetch_rows(v_cursor);
dbms_sql.column_value(v_cursor,1,v_number1);
for i in v_number1.first..v_number1.last loop
  dbms_output.put_line('i:'||i);
  dbms_output.put_line(v_number1(i));
end loop;
dbms_sql.close_cursor(v_cursor);
end;
/
--#####################################
--##函数
select 'CASE:f00' from dual;
create or replace function f00 return number is -----注释
begin     -----注释
return 1; -----注释
end;      -----注释
/
ALTER FUNCTION f00 COMPILE;
select f00() from dual;

select 'CASE:f01' from dual;
create /* comment1 */or /* comment1 */ replace /* comment1 */ function /* comment1 */ f01(a in int default 1/* comment1 */,b in out int) /* comment1 */ return /* comment1 */ number /* comment1 */is /* comment1 */
begin     /* comment1 */
b:= a*a;  /* comment1 */
return b; /* comment1 */
end;      /* comment1 */
/
;
declare
  a int := 0;
  b int;
  c int;
begin
  c := f01(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(c);
end;
/

select 'CASE:f02' from dual;
create or replace function f02(a int := 2,b in out int) return number AUTHID DEFINER  is

c int;

begin

c := a/b;

return c;

end;
/
declare
  a int := 10;
  b int := 2;
  c int;
begin
  c := f02(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(c);
end;
/

select 'CASE:f03' from dual;
create 
  or 
  replace 
  function 
  f03(rand_range  IN INTEGER) 
  return 
  number 
  AUTHID 
  CURRENT_USER  
  as
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
select f03(rand_range=>100) from dual;

select 'CASE:f04' from dual;
create or replace function f04 return number is
begin
return 1;
end;
/
ALTER FUNCTION f04 COMPILE;
select f04() from dual;

--#存储过程
--select 'case: pro00_CREATE' from dual;
----error 0,00600
--drop PROCEDURE pro00_CREATE;
--CREATE PROCEDURE pro00_CREATE IS
--BEGIN
--  DBMS_OUTPUT.PUT_LINE('pro00_CREATE');
--END;
--/
--call pro00_CREATE();

select 'case: pro01_or' from dual;
create or replace PROCEDURE "pro01_or"(a in int,b in int)  IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('pro01_or'||(a+b));
END;
/
call "pro01_or"(a=>10,b=>20);

select 'case: pro04_replace' from dual;
CREATE OR REPLACE PROCEDURE pro04_replace(a in out int,b out int) AUTHID DEFINER as
BEGIN
  b := a*a;
  DBMS_OUTPUT.PUT_LINE('pro04_replace'||b);
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro04_replace(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(b);
end;
/

select 'case: pro05_PROCEDURE' from dual;
CREATE OR REPLACE PROCEDURE pro05_PROCEDURE(a out int,b out int) AUTHID CURRENT_USER IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('pro05_PROCEDURE');
  a := 10;
  b := 2;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN 
     DBMS_OUTPUT.PUT_LINE('NO_DATA_FOUND');
  WHEN OTHERS THEN 
     DBMS_OUTPUT.PUT_LINE('ERR: '||SQLCODE||': '||SQLERRM);
END;
/
ALTER PROCEDURE pro05_PROCEDURE COMPILE;
declare
  a int := 0;
  b int;
  c int;
begin
  pro05_PROCEDURE(a,b=>b);
  DBMS_OUTPUT.PUT_LINE('pro05_PROCEDURE：'||a||b);
end;
/

select 'case: pro06' from dual;
CREATE OR REPLACE PROCEDURE pro06(a int) AUTHID CURRENT_USER IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('pro06:'||a);
END;
/
ALTER PROCEDURE pro06 COMPILE;
call pro06(10);

--#包
--error 0,00942
drop table tab1;
CREATE TABLE tab1 (c1 int, c2 varchar2(4000) DEFAULT 'syn1_table');

select 'case: pack00_PACKAGE' from dual;
CREATE OR REPLACE PACKAGE pack00_PACKAGE AS
  a NUMBER;
END pack00_PACKAGE;
/
ALTER PACKAGE pack00_PACKAGE COMPILE PACKAGE;

select 'case: pack00_function' from dual;
CREATE OR REPLACE PACKAGE pack00_function AS
  a NUMBER;
END pack00_function;
/

select 'case: pack01_type' from dual;
CREATE OR REPLACE PACKAGE pack01_type
AUTHID DEFINER -- 定义权限为调用者，或使用 CURRENT_USER 表示调用者权限
AS
  a varchar2(2000) := 'pack01_type';
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
END pack01_type;
/
ALTER PACKAGE pack01_type COMPILE PACKAGE;
select pack01_type.a from dual;

select 'case: pack02_wrapped' from dual;
create or replace package pack02_wrapped 
AUTHID CURRENT_USER 
is
  procedure pack_pro;
  function pack_func return number;
end;
/
create or replace package body pack02_wrapped is
  procedure pack_pro is
    a tab1%rowtype;
    b tab1.c1%type;
  begin
    DBMS_OUTPUT.PUT_LINE('pack02_wrapped');
  end;

  function pack_func return number is
    a tab1%rowtype;
    b tab1.c1%type;
  begin
    DBMS_OUTPUT.PUT_LINE('pack02_wrapped');
    return 1;
  end;
end;
/
declare
  var1 number;
begin
  pack02_wrapped.pack_pro;
  var1 := pack02_wrapped.pack_func;
  DBMS_OUTPUT.PUT_LINE('pack02_wrapped: ' || var1);
end;
/

select 'case: pack03_body' from dual;
create or replace package pack03_body
AUTHID CURRENT_USER 
is
  procedure pack_pro;
  function pack_func return number;
end;
/
create or replace package body pack03_body as
  procedure pack_pro is
  begin
    pack02_wrapped.pack_pro();
    DBMS_OUTPUT.PUT_LINE('pack03_body');
  end;

  function pack_func return number is
  begin
    DBMS_OUTPUT.PUT_LINE('pack03_body');
    return pack02_wrapped.pack_func();
  end;
end pack03_body;
/
declare
  var1 number;
begin
  pack03_body.pack_pro();
  var1 := pack03_body.pack_func();
  DBMS_OUTPUT.PUT_LINE('pack03_body: ' || var1);
end;
/

select 'case: pack04_or' from dual;
create or replace package pack04_or
AUTHID CURRENT_USER 
is
  a number;
end;
/

--#OBJECT
select 'case: obj00' from dual;
CREATE or  replace TYPE obj00 AS OBJECT ( --!@#$%^&*()_+
  a VARCHAR2(50)--!@#$%^&*()_+
);              --!@#$%^&*()_+
/
select obj00('obj00') from dual;

select 'case: obj01' from dual;
CREATE  or  replace   TYPE  obj01   AS  OBJECT  (
  a VARCHAR2(50),
  MEMBER FUNCTION get_age RETURN NUMBER
);
/
CREATE or  replace TYPE BODY obj01 AS
  MEMBER FUNCTION get_age RETURN NUMBER IS
  BEGIN
    RETURN a;
  END get_age;
END;
/
select obj01('obj01') from dual;

select 'case: obj02' from dual;
CREATE or replace TYPE obj02 force AS OBJECT (
  a VARCHAR2(50),

  /*
  1
  1
  1
  1
  1
  1
  */

  MEMBER FUNCTION get_age RETURN NUMBER
);
/
select obj02('obj02') from dual;

select 'case: obj03_null' from dual;
create or replace type obj03_null as object(
  a1 number,
  a2 varchar(2000),
  a3 date,
  member procedure getvalue(p1 in out number,p2 in out varchar2,p3 in out date));
/
create or replace type body obj03_null is
  member procedure getvalue(p1 in out number,p2 in out varchar2,p3 in out date) is
  begin
    p1 := a1;
    p2 := a2;
    p3 := a3;
    DBMS_OUTPUT.PUT_LINE('◆udt_obj1属性值：'||a1||','||a2||','||a3);
  end;
end;
/
select obj03_null(1,1,null) from dual;

