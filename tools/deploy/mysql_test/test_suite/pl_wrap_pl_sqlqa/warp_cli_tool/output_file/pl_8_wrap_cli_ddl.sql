--#####################################
--1、CREATE WRAPPED DDL
--支持PL/SQL对象(6个)
--#####################################
--case 1
create or replace function f1 WRAPPED
88
Kh4BEDzE+sFaIqNzgFWI26aIlcCe8Q3eMRzndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI66HR3JH
o4daLEJpUP8=
;
/
select f1() from dual;

create or replace function f1 WRAPPED
88
Kh4BmJ5oOmfeFABzgFWI26aIlcCe8Q3eMRzndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI3CHR3JH
o4daLELuUPc=
;
/
select f1() from dual;

CREATE OR REPLACE PROCEDURE pro2 WRAPPED
156
n18BwxzDaUbeSSGHgFVIYkNen9rUuuKD5+eWUeb756Uc+dumBXVM4Rz583LmZOecofxli+ZCMdht
5MV07FdgIWqubGDj/R4OC6JSyAPhi2nhki9thvHAnoguZHEA9hvx8y1qanOdBbysTpOntwzjhCwJ
PU+f
;
/
call pro2(p1=>2);

create or replace type obj0 WRAPPED
76
aK4BTx56VII9Ty+KgFXbEfPeg/n3lnNk3sszdf+D3vPDoeaziEWLuqdDdYez5rRF/GUsw/h6QA==
;
/

create or replace type obj1 WRAPPED
84
Rf8BaZb3CvUPrheVgFXbEfPeg/n3lnMc3sszdf+D3vPDoeaziEWLuqdDdYez5rTdi7oTv8Nq44Qs
vjonig==
;
/

create or replace package pkg3 WRAPPED
224
Ck8BbFTY5eZnncADgFWD8RNcjG16bPtGxMiIgoT50TBDpcN1Y4ZK3teYx6igAGzbIXxUVyF6TOu/
jCjpr7+7tns+07Dk8pPR6ExyTGMLZ8TlynYf7YsZ234EXvCHGnvZYrwrMvU3R2dpgL81iJp5u3tL
/pEXyG7+5PgVbfjhlbGIaF5liqwTF8vA1V0maIBUJCN4KfEwlbKFUI4z2TyAh6ktq5xdHBM=
;
/
create or replace package body pkg3 WRAPPED
288
z+kBIRiToPkoqS8CwFqAVYnxiOycpT7Dl6n/YbhKHb8SPxz4tPVHCwRPglK5kVE4wewMhDpzes1U
1tnLcepFYsTBjQQRbFs5f9ZravqctafXplBkDK+tUL5z1eeh7J1qjrDFXXr3EGMvzp8dRE386Rse
YHViiCBkBOKPl414V6RVxkxpqozyVjdS8ldZ691iw5BsxYM2rM1dVdZ2alLjEFzAs5vSSEGXFEaV
E/V7TjAV+csh/pkCjmpDdePtyVOWLRAwZyQq+DhnI0aqhRyjF1FESVHXGLtd
;
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





