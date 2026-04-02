--#####################################
--1、CREATE WRAPPED DDL
--支持PL/SQL对象(6个)
--#####################################
--case 1
create or replace function f1 return number is
begin
return 1;
end;
/
select f1() from dual;

create or replace function f1 return number is
begin
return 2;
end;
/
select f1() from dual;

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

create or replace type obj1 force as object(c0 number,c1 obj0);
/

create or replace package pkg3 as
type rd is record(c0 int,c1 int);
type tb is table of rd index by pls_integer;
procedure p1(r out rd);
procedure p2(c sys_refcursor);
procedure p3(r rd, t out tb);
procedure p4(a obj0, b out obj1);
procedure p5(a obj1, b out int);
end;
/
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
/

declare
 v1 pkg3.rd;
 v2 pkg3.tb;
 v3 sys_refcursor;
 v4 obj0 := obj0(4);
 v5 obj1 := obj1(5,v4);
 v6 int;
begin
  pkg3.p1(v1);
  pkg3.p2(v3);
  pkg3.p3(v1,v2);
  dbms_output.put_line('v2='||v2.count);
  pkg3.p4(v4,v5);
  dbms_output.put_line('v5.c1.c0='||v5.c1.c0);
  pkg3.p5(v5,v6);
  dbms_output.put_line('v6='||v6);
end;
/




