create table oracle_t1 (id char(16), data int);
insert into oracle_t1 values ('远程ORACLE端', 1);
commit;

create or replace function oracle_fun1 return number is
begin
  return 2;
end;
/
create or replace function oracle_fun2(p1 number default 2) return number is
begin
  return oracle_fun1 * p1;
end;
/

create or replace procedure oracle_pro1(p1 number default 2) is
begin
  insert into oracle_t1 values ('oracle_pro1', p1);
end;
/
create or replace procedure oracle_pro2(p1 number default 2) is
begin
  oracle_pro1(p1=>p1);
end;
/

CREATE OR REPLACE PACKAGE oracle_pkg1 IS
  FUNCTION func_30(p_char IN OUT CHAR) RETURN NUMBER;
END oracle_pkg1;
/
CREATE OR REPLACE PACKAGE BODY oracle_pkg1 IS
  FUNCTION func_30(p_char IN OUT CHAR) RETURN NUMBER IS
  BEGIN
    p_char := 'oracle Modified Char';
    RETURN oracle_fun1;
  END func_30;
END oracle_pkg1;
/

create or replace synonym syn_oracle_fun2 for oracle_fun2;
create or replace synonym syn_oracle_pro2 for oracle_pro2;
create or replace synonym syn_oracle_pkg1 for oracle_pkg1;

commit;
