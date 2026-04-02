connection tentora_conn;

-- error 0, 942
drop table t1;
create table t1 (c1 int, c2 int, c3 int);

-- error 0, 942
drop table t1_log;
create table t1_log (c1 int, c2 int, c3 int);

eval declare v1 varchar(20) := 'world'; begin dbms_output.put_line('hello ' || v1); end; 

CREATE OR REPLACE PROCEDURE p1(p_v1 INT) IS v1 INT := 1; BEGIN INSERT INTO t1 VALUES (p_v1, v1, p_v1 + v1); COMMIT; END;
call p1(1);
begin p1(1); end;

select * from t1;

create or replace function f1(p_v1 int) return int is begin return p_v1; end;
select * from t1 where c1 = f1(1);
declare v1 int; begin v1 := f1(100); dbms_output.put_line('v1: ' || v1); end;

create or replace package pkg1 is v1 int; procedure p1(p_v1 int); function f1(p_v1 int) return int; end pkg1;
create or replace package body pkg1 is v2 int; procedure p1(p_v1 int) is begin v1 := p_v1; v2 := p_v1; end; function f1(p_v1 int) return int is begin v1 := p_v1; return v2; end; end;
call pkg1.p1(3);
select pkg1.f1(4) from dual;

#CREATE OR REPLACE FUNCTION print RETURN VARCHAR2 IS BEGIN DBMS_OUTPUT.GET_LINE(r, t); WHILE t != 1 LOOP DBMS_OUTPUT.GET_LINE(r1, t); r := r || ' ' || r1; END LOOP; RETURN r; END;
#CREATE OR REPLACE PROCEDURE clear_output IS r VARCHAR2(32767); t INT; BEGIN LOOP: DBMS_OUTPUT.GET_LINE(r, t); EXIT WHEN t = 1; END LOOP; END;
CREATE OR REPLACE FUNCTION println RETURN VARCHAR2 IS r VARCHAR2(32767); t INT; r1 VARCHAR2(32767);BEGIN DBMS_OUTPUT.GET_LINE(r, t); LOOP DBMS_OUTPUT.GET_LINE(r1, t); EXIT WHEN t = 1; r := r || chr(10) || '  ' || r1; END LOOP; RETURN r; END;
declare v1 int; begin insert into t1(c1) values ('aaa'); exception when others then dbms_output.put_line('exception: ' || sqlerrm); end;
select println() from dual;


declare v1 int; my_err exception; pragma exception_init(my_err, -20001); begin raise my_err; exception when my_err then dbms_output.put_line('exception: ' || sqlerrm); end;
select println() from dual;


create or replace procedure p2(a int, b in out int, c out int) is begin c := a; b := b + 1; end;

declare a int:=0; b int:=2; c int; begin p2(a,b,c); end;
create or replace trigger tri0 before insert on t1 for each row begin insert into t1_log(c1, c2, c3) values (:old.c1, :new.c2, :old.c3 + :new.c3); end;

insert into t1 values(10, 10, 10);
select * from t1;
select * from t1_log;

create or replace type obj_typ1 force as object(c0 int);

create or replace type obj_typ2 force as object (c0 int, constructor function obj_typ2(a1 int, a2 int) return self as result, static function f0 return int, member procedure p1);

create or replace type body obj_typ2 as constructor function obj_typ2(a1 int, a2 int) return self as result is begin c0:= a1 + a2; return; end; static function f0 return int is begin return 0; end; member procedure p1 is begin null; end; end;
declare obj obj_typ2; v1 int; begin obj := obj_typ2(1, 1); select obj_typ2.f0() into v1 from dual; obj.p1(); dbms_output.put_line('v1: ' || v1); end;

#select println() from dual;


create or replace type arr_typ1 force as varray(100) of int;

create or replace type nstb_typ1 force as table of int;

#declare type arr_typ2 is varray(100) of arr_typ1; type nstb_typ2 is table of nstb_typ1; type rec_typ1 is record(c1 int, c2 int); v1 arr_typ1; v2 arr_typ2; v3 nstb_typ1; v4 nstb_typ2; v5 rec_typ1; begin v1 := arr_typ1(1, 2, 3); v2 := arr_typ2(v1, v1, v1); v3 := nstb_typ1(1, 2, 3); v4 := nstb_typ2(v3, v3, v3); v5 := rec_typ1(1, 2); dbms_output.put_line(v1(1)); dbms_output.put_line(v2(2)(1)); dbms_output.put_line(v3(1)); dbms_output.put_line(v4(2)(1)); dbms_output.put_line(v5.c1); end;


-- error 0, 942
drop table tt2;
create table tt2(c1 int);


declare a int; begin insert into tt2 values(0); execute immediate 'insert into tt2 values(1)'; execute immediate 'insert into tt2 values(?)' using a; end;

select * from tt2;

declare cursor cur1(p_v1 int) is select c1 from tt2 where c1 = p_v1; a tt2.c1%type; begin open cur1(1); loop fetch cur1 into a; exit when cur1%notfound; end loop; close cur1; end;

declare type rec_typ1 is record(c1 int); cursor cur1(p_v1 int) return rec_typ1 is select c1 from tt2 where c1 = p_v1; rec rec_typ1; begin open cur1(1); loop fetch cur1 into rec; exit when cur1%notfound; end loop; close cur1; end;

-- error 0, 942
drop table t3;
-- error 0, 942
drop table t4;
create table t3 (c1 int, c2 int);
insert into t3 values(1, 1);
create table t4 (c1 int, c2 int);
insert into t4 values(1, 1);


declare pragma autonomous_transaction; begin insert into t3 values (2,2); commit; end;
insert into t4 values (2, 2);

rollback;
select * from t3;
select * from t4;



create or replace procedure p1(p_v1 int) is begin if p_v1 = 1 then dbms_output.put_line(p_v1); elsif p_v1 = 2 then dbms_output.put_line(p_v1 * p_v1); else dbms_output.put_line(p_v1 + p_v1); end if; end;
call p1(1);
select println() from dual;

call p1(2);
select println() from dual;

call p1(3);
select println() from dual;

declare v1 number := 0; begin while v1 < 3 loop v1 := v1 + 1; dbms_output.put_line(v1); goto label1; end loop; v1 := 0; <<label1>> dbms_output.put_line(v1); end;



-- error 0, 942
drop table t1_1;
create table t1_1(aa varchar2(20));
insert into t1_1 values(1);
insert into t1_1 values(30);


DECLARE TYPE NumList IS VARRAY(20) OF NUMBER; depts NumList := NumList(10, 30, 70); BEGIN FORALL i IN depts.FIRST..depts.LAST DELETE FROM t1_1 WHERE aa = depts(i); END;
select * from t1_1;


-- error 0, 600
drop TYPE my_typ11 ;

CREATE TYPE my_typ11 AS OBJECT (
  attr1 NUMBER,
  attr2 VARCHAR2(100)
);

CREATE OR REPLACE PROCEDURE modify_my_object(p_object IN OUT NOCOPY my_typ11) IS BEGIN p_object.attr1 := p_object.attr1 + 1; p_object.attr2 := 'Modified'; END;
declare obj1 my_typ11 := my_typ11(1, '1'); begin modify_my_object(obj1); end;


-- error 0, 942
drop table t1_2;
create table t1_2(aa varchar2(20));
insert into t1_2 values(1);
insert into t1_2 values(30);

DECLARE v_new_aa NUMBER := 50000; v_old_aa NUMBER; BEGIN UPDATE t1_2 SET aa = v_new_aa WHERE aa = 30 RETURNING aa INTO v_old_aa; END;
select * from t1_2;

-- error 0, 942
drop TABLE employees;
CREATE TABLE employees (
    employee_id NUMBER,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    salary NUMBER
);

BEGIN INSERT INTO employees VALUES (1, 'John', 'Doe', 50000); INSERT INTO employees VALUES (2, 'Jane', 'Doe', 52000); INSERT INTO employees VALUES (3, 'Bill', 'Smith', 55000); COMMIT; END;

DECLARE TYPE t_employee_tab IS TABLE OF employees%ROWTYPE; v_employees t_employee_tab; BEGIN SELECT * BULK COLLECT INTO v_employees FROM employees; FOR indx IN 1..v_employees.COUNT LOOP DBMS_OUTPUT.PUT_LINE('Employee ID: ' || v_employees(indx).employee_id || ', Name: ' || v_employees(indx).first_name || ' ' || v_employees(indx).last_name || ', Salary: ' || v_employees(indx).salary); END LOOP; END;
select println() from dual;


-- error 0, 942
DROP TABLE accounts;
CREATE TABLE accounts (
  account_id  NUMBER(6),
  balance     NUMBER (10,2)
);
set autocommit =0;
INSERT INTO accounts (account_id, balance)
VALUES (7715, 6350.00);
 
INSERT INTO accounts (account_id, balance)
VALUES (7720, 5100.50);

CREATE OR REPLACE PROCEDURE transfer(from_acct NUMBER, to_acct NUMBER, amount NUMBER) AUTHID CURRENT_USER AS BEGIN UPDATE accounts SET balance = balance - amount WHERE account_id = from_acct; UPDATE accounts SET balance = balance + amount WHERE account_id = to_acct; COMMIT; END;
BEGIN transfer(7715, 7720, 250);END;

SELECT * FROM accounts;

-- error 0, 942
drop table t1_3;
create table t1_3(aa varchar2(2));
-- error 0, 942
drop table t_1;
create table t_1(aa varchar2(2000));

DECLARE emp_id NUMBER(6); emp_lastname VARCHAR2(25); emp_salary NUMBER(8,2); emp_jobid VARCHAR2(10); BEGIN insert into t1_3 values(1); insert into t1_3 values(2); insert into t1_3 values(3); insert into t1_3 values(111); EXCEPTION WHEN others THEN ROLLBACK; insert into t_1 values('Inserts were rolled back'); END;
select * from t1_3;
select * from t_1;


-- error 0, 942
drop table t1_4;
create table t1_4(aa varchar2(2));
-- error 0, 942
drop table t_2;
create table t_2(aa varchar2(2000));

DECLARE emp_id NUMBER(6); emp_lastname VARCHAR2(25); emp_salary NUMBER(8,2); emp_jobid VARCHAR2(10); BEGIN insert into t1_4 values(1); insert into t1_4 values(2); SAVEPOINT do_insert; insert into t1_4 values(3); insert into t1_4 values(111); EXCEPTION WHEN others THEN ROLLBACK to do_insert; insert into t_2 values('Inserts were rolled back'); END;
select * from t1_4;
select * from t_2;
