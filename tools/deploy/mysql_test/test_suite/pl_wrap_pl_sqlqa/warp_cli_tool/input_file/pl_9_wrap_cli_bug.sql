--#####################################
--异常场景
--#####################################
--case 1
CREATE OR REPLACE TYPE tb IS TABLE OF INTEGER;
/

create or replace type va is array(10) of number not null;
/

CREATE OR REPLACE TYPE obj0 AS OBJECT (c0 INT,c1 INT,
MEMBER FUNCTION f5(self IN obj0) RETURN obj0List PIPELINED);
/

--error 0,600
drop type obj0_57652309 force;

--disable_warnings
delimiter /;
CREATE OR REPLACE TYPE obj0_57652309 AS OBJECT (c0 INT,c1 INT,MEMBER FUNCTION f5(self IN obj0_57652309) RETURN obj0List_57652309 PIPELINED);
/
--enable_warnings

--error 304
CREATE OR REPLACE TYPE BODY obj0_57652309 AS
  MEMBER FUNCTION f5(self IN obj0_57652309) RETURN obj0List_57652309 PIPELINED IS
    a obj0_57652309 := obj0_57652309(self.c0, self.c1);
  BEGIN
    PIPE ROW(a);
  END;  
END;
/
delimiter ;/

drop type obj0_57652309 force;

--error 0, 600
drop type user_type1 force;
--error 0,942
drop table test;
create table test(id int);
delimiter /;
--disable_warnings
create or replace type user_type1 as object( a number, map member function t_proc(self in user_type1%type) return number );
/
select * from all_errors where name = 'USER_TYPE1' and owner='TEST';
/
drop type user_type1 force;
/

--case1 自己引用自己，加密前警告，加密后报4016
--PLS-00206: %%TYPE must be applied to a variable, column, field or attribute, not to other
--error 0, 600
drop type user_type1 force;

create or replace type user_type1 as object( 
   a number, 
   map member function t_proc(self in user_type1%type) return number 
);
/

--case2 加密前警告，加密后报4016
--PLS-00630: pipelined functions must have a supported collection return type
--error 0,600
drop type obj0_57652309 force;

--disable_warnings
CREATE OR REPLACE TYPE obj0_57652309 AS OBJECT (
   c0 INT,
   c1 INT,
   MEMBER FUNCTION f5(self IN obj0_57652309) RETURN obj0List_57652309 PIPELINED);
/
--enable_warnings

--case3 加密前警告，加密后报4016
--PLS-00523: ORDER methods must return an INTEGER，
delimiter /;
--disable_warnings
create or replace type cai_type15 as object(
a number,
order member function t_func_order(v15 in cai_type15) return cai_type15);
/
delimiter ;/

--case1 语法错误 - 缺少
create
/

create or replace package
/

create replace package pkg as end;
/

create or replace body pkg as end;
/

--case2 语法错误 - 顺序
create or replace body package pkg as end;
/

create replace or package body pkg or  as end;
/

--case 4 词法错误
create or replaces packagea pkg as end;
/

create package "pkg"  as /*
/

create package "pkg"  as " 
/

--bug: 2025011000106962451
call dbms_ddl.create_wrapped('
create or replace function fun WRAPPED
100
MhsBkNkegyrW9OgvgFVj+Rm6e216CHuDB4w2e0VgIPVpf3OkfBHiQab1YikUgsEeYR2EPF+v5axc
uHcXkG6UzgvG58mkWjIqDPA=
;');
/

create or replace function func0(p1 date default null) return date is
begin
   return last_day (p1);
END;
/
create or replace function whhtest1.func0(p1 date default null) return date is
begin
   return last_day (p1);
END;
/
create or replace function whhtest1.func1(p1 date default null) return date is
begin
   return last_day (p1);
END;
/

--bug: 2025011000106957985
call pck_create_pro();
select * from pck_create_tab;
