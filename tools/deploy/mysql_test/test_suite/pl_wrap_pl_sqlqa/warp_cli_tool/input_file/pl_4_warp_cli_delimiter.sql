--echo #-----------------------------------------------------------------------------
--echo # 测试场景：1、加密工具 plsql 结束符
--echo #-----------------------------------------------------------------------------

--echo # case1 结束符前有空行
create or replace function f0 return number is
begin
return 1;
end;

/

select f0() from dual;

--echo # case2 结束符前有空行
create or replace function f0 return number is
begin
return 1;
end;


/

select f0() from dual;

--echo # case3 结束符前有注释
create or replace function f0 return number is
begin
return 1;
end;

--结束符前有注释

/

select f0() from dual;


--echo # case4 结束符前有注释
create or replace function f0 return number is
begin
return 1;
end;

--结束符前有注释1
--结束符前有注释2

/

select f0() from dual;

--echo # case5 结束符后有空行，注释
create or replace function f0 return number is
begin
return 1;
end;

/

--结束符后有空行，注释

select f0() from dual;


--echo # case6 多个结束符
create or replace function f1 return number is
begin
return 2;
end;
//

--结束符有空行，注释

//

/


select f1() from dual;

--echo # case7 结束符前有空格
CREATE OR REPLACE PROCEDURE pro2 (p1 number) is
  v1 number;
begin
  v1 := f1()*p1;
  dbms_output.put_line('pro2：'||v1);
end pro2;
/                      
call pro2(p1=>2);

create or replace type obj0 force as object(c0 number);
                  /


--echo # case8 结束符后有空格
create or replace type obj1 force as object(c0 number,c1 obj0);
//              


--echo # case9 结束符后有注释
create or replace package pkg3 as
type rd is record(c0 int,c1 int);
type tb is table of rd index by pls_integer;
procedure p1(r out rd);
procedure p2(c sys_refcursor);
procedure p3(r rd, t out tb);
procedure p4(a obj0, b out obj1);
procedure p5(a obj1, b out int);
end;
//

create or replace package body pkg3 as
procedure p1(r out rd) is
begin
dbms_output.put_line('p1');
end;
procedure p2(c sys_refcursor) is
begin
dbms_output.put_line('p2');
end;
procedure p3(r rd, t out tb) is
begin
dbms_output.put_line('p3');
for i in 1..100 loop
t(i):=r;
end loop;
end;
procedure p4(a obj0, b out obj1) is
begin
dbms_output.put_line('p4');
b:=obj1(a.c0,a);
end;
procedure p5(a obj1, b out int) is
begin
dbms_output.put_line('p5');
b:=a.c0+a.c1.c0;
end;
end;
/      --xxx

--echo # case10 结束符中有注释
CREATE OR REPLACE PROCEDURE pro3 (p1 number) is
  v1 number;
begin
  v1 := f1()*p1;
  dbms_output.put_line('pro3：'||v1);
end pro2;
/      /

call pro3(p1=>2);

--echo # case11 / // 以外的结束符
CREATE OR REPLACE PROCEDURE pro4 (p1 number) is
  v1 number;
begin
  v1 := f1()*p1;
  dbms_output.put_line('pro4'||v1);
end pro2;
|

CREATE OR REPLACE PROCEDURE pro5 (p1 number) is
  v1 number;
begin
  v1 := f1()*p1;
  dbms_output.put_line('pro5'||v1);
end pro2;
$

CREATE OR REPLACE PROCEDURE pro6 (p1 number) is
  v1 number;
begin
  v1 := f1()*p1;
  dbms_output.put_line('pro6'||v1);
end pro2;
$$

call pro6(p1=>2);

--echo # case12 结束符前有注释
CREATE OR REPLACE PROCEDURE pro6 (p1 number) is
  v1 number;
begin
  v1 := f1()*p1;
  dbms_output.put_line('pro6'||v1);
end pro2;
--xx /

call pro6(p1=>2);

