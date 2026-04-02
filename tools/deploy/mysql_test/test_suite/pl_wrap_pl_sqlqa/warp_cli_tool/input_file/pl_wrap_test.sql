create or replace function cli_f1 return number is
begin
return 1;
end;
/
select cli_f1() from dual;

create or replace function cli_f2 return number is
begin
return 2;
end;
/
select cli_f2() from dual;

CREATE OR REPLACE PROCEDURE cli_pro2 (p1 number) is
  v1 number;
begin
  v1 := cli_f1()*p1;
  dbms_output.put_line('cli_pro2：'||v1);
end cli_pro2;
/
call cli_pro2(p1=>2);

create or replace type cli_obj0 force as object(c0 number);
/

create or replace type cli_obj1 force as object(c0 number,c1 cli_obj0);
/