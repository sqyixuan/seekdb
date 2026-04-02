CREATE OR REPLACE PROCEDURE delete_all_objects AS
BEGIN
  -- 删除所有表
  FOR tbl IN (SELECT table_name FROM user_tables) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || tbl.table_name;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all table finish');
  
  -- 删除所有存储过程
  FOR proc IN (select distinct name from user_source where type=upper('procedure')) LOOP
  if proc.name != 'DELETE_ALL_OBJECTS' then  --不删除本procedure本身
    EXECUTE IMMEDIATE 'DROP PROCEDURE ' || proc.name;
  end if;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all procedure finish');
  
  -- 删除所有函数
  FOR func IN (select distinct name from user_source where type=upper('function')) LOOP
    if func.name != '"LUO"' then  --创建了"luo" 函数,不带双引号删不掉
    EXECUTE IMMEDIATE 'DROP FUNCTION ' || func.name;
    end if;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all function finish');

  -- 删除所有视图
  FOR v  IN (SELECT view_name FROM user_views) LOOP
    EXECUTE IMMEDIATE 'DROP VIEW ' || v.view_name;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all view finish');

  -- 删除所有同义词
  FOR synonym IN (SELECT synonym_name FROM user_synonyms) LOOP
    EXECUTE IMMEDIATE 'DROP SYNONYM ' || synonym.synonym_name;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all synonym finish');
  
  -- 删除所有sequence
  FOR seq IN (SELECT sequence_name FROM user_sequences) LOOP
    EXECUTE IMMEDIATE 'DROP SEQUENCE ' || seq.sequence_name;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('drop all sequence finish');
  
  COMMIT;
END;
/

call delete_all_objects();

drop table ttt1;
create table ttt1 (id char(16) not null , data int not null);

drop table t2;
create table t2 (s char(16), i int, d number);

create or replace function e return number is
begin
  return 2.7182818284590452354;
end;
/

create or replace function inc(i int) return int is
begin
  return i+1;
end;
/

create or replace function mul(x int, y int) return int is
begin
  return x*y;
end;
/

create or replace function concat_str(s1 char, s2 char) return char is
begin
  return concat(s1, s2);
end;
/

create or replace function fac(n1 int) return number is
  f number default 1;
  n INT := n1;
begin
  while n > 1 loop
    f := f * n;
    n := n - 1;
  end loop;
  return f;
end;
/

create or replace function fun(d number, i int, u int) return number is
begin
  return mul(inc(i), fac(u)) / e();
end;
/

create or replace function tt1max return int is
  x int;
begin
  select max(data) into x from tt1;
  return x;
end;
/

insert into tt1 values('luo', 3);
insert into tt1 values('bar', 2);
insert into tt1 values('zip', 5);
insert into tt1 values('zap', 1);
commit;

drop table t3_1;
create table t3_1 (
  v char(16) not null primary key,
  c int not null
);

create or replace function getcount(s char) return int is
  x int;
begin
  select count(*) into x from t3_1 where v = s;
  if x = 0 then
    insert into t3_1 values (s, 1);
  else
    update t3_1 set c = c+1 where v = s;
  end if;
  return x;
end;
/


create or replace function bug2772 return char is
begin
  return 'a';
end;
/


create or replace procedure bug2564_1 is
begin
  insert into tt1 values ('luo', 1);
end;
/


create or replace function bug2564_3(x int, y int) return int is
begin
  return x || y;
end;
/


create or replace function bug2564_4(x int, y int) return int is
begin
  return x || y;
end;
/

create or replace function bug3788 return date is begin return cast('2005-03-04' as date);
end;
/


create or replace function bug4487 return char is
  v char;
begin
  return v;
end;
/

create or replace function bug5240 return int is
  x int;
  cursor c is select data from tt1 where rownum = 1;
begin
  open c;
  fetch c into x;
  close c;
  return x;
end;
/

create or replace function bug7648 return char is begin return 'a'; 
end;
/

create or replace function bug8861(v1 int) return int is begin return v1; 
end;
/


create or replace function f_bug11247(param int)
return int is
begin
return param + 1;
end;
/

create or replace procedure p_bug11247(lim int) is
  v int default 0;
begin
  while v < lim loop
    v := f_bug11247(v);
  end loop;
end;
/

create or replace function bug131333
  return int is
begin
  declare
    a int;
  begin
    a := 1;
  end;
  declare 
    b int:=2;
  begin
    return b;
  end;
end;
/


create or replace function bug9048(f2 varchar2 ) return varchar
is
  f1 varchar2(4000) ;
begin
  f1 := f2;
  f1 := concat( 'hello', f1 );
  return f1;
end;
/

drop table t3;
create table t3 (c1 char(1) primary key not null);

create or replace function bug12379
  return integer is
begin
   insert into t3 values('X');
   insert into t3 values('X');
   return 0;
end;
/

create or replace procedure bug12379_1 is
  x INT;
begin
   select bug12379() INTO x from dual;
exception
  when others then select 42 INTO x from dual;
END;
/

create or replace procedure bug12379_2 is
  x INT;
begin
   select bug12379() INTO x from dual;
exception
  when others then null;
end;
/

commit;

create or replace procedure bug12379_3 is
  x INT;
begin
   select bug12379() INTO x from dual;
end;
/

drop table t3_2;
create table t3_2 ( x int unique );

create or replace procedure bug7049_1 is
begin
  insert into t3_2 values (42);
  insert into t3_2 values (42);
end;
/

create or replace procedure bug7049_2 is
  x VARCHAR2(4000);
begin
  bug7049_1;
  select 'Missed it' as Result INTO x from dual;
exception 
  when others then select 'Caught it' as Result INTO x from dual;
end;
/

create or replace procedure bug7049_3 is 
begin
  bug7049_1;
end;
/

create or replace procedure bug7049_4 is
  x VARCHAR2(4000);
begin
  bug7049_3;
  select 'Missed it' as Result INTO x from dual;
exception 
  when others then select 'Caught it' as Result INTO x from dual;
end;
/

create or replace function bug7049_1_1
  return int is
begin
  insert into t3_2 values (42);
  insert into t3_2 values (42);
  return 42;
end;
/

create or replace function bug7049_2_1
  return int is
  x int default 0;
begin
  x := 1;
  x := bug7049_1_1();
  return x;
exception 
  when others then return x;
end;
/


create or replace function bug10100f(prm int) return int is
begin
  if prm > 1 then
    return prm * bug10100f(prm - 1);
  end if;
  return 1;
end;
/

create or replace procedure bug10100p(prm int, res in out int) is
begin
  res := res * prm;
  if prm > 1 then
    bug10100p(prm - 1, res);  
  end if;
end;
/

create or replace procedure bug10100t(prm int) is
  res int;
begin
  res := 1;
  bug10100p(prm, res);
  DBMS_OUTPUT.PUT_LINE(res);
end;
/

drop table t3_3;
create table t3_3 (a int);
insert into t3_3 values (0);

create view v1 as select a from t3_3;

create or replace procedure update_nest(level int, ceiling int) is
  begin
  if level < ceiling then
    update t3_3 set a=level;
    update_nest(level+1, ceiling);
  else
    NULL;
  end if;
end;
/

create or replace procedure bug10100pv(level1 int, lim int) is
  x int;
begin
  if level1 < lim then
    update v1 set a=level1;
    bug10100pv(level1+1, lim);
  else
    select a into x from v1;
  end if;
end;
/

commit;

create or replace procedure bug10100pc(level1 int, lim int) is
  lv int;
  cursor c is select a from t3_3;
begin
  open c;
  if level1 < lim then
    select level1 INTO lv from dual;
    fetch c into lv;
    update t3_3 set a=level1+lv;
    bug10100pc(level1+1, lim);
  else
    NULL;
  end if;
  close c;
end;
/

drop table table_20028;
create table table_20028 (i int);
insert into table_20028 values(1);

create or replace function func_20028_a return integer is
  temp integer;
begin
  select i into temp from table_20028 where rownum = 1;
  return nvl(temp,0);
end;
/

create or replace function func_20028_b return integer is
begin
  return func_20028_a();
end;
/


create or replace procedure proc_20028_a is
  temp integer;
begin
  select i into temp from table_20028 where rownum = 1;
end;
/

create or replace procedure proc_20028_b is
begin
  proc_20028_a;
end;
/

create or replace function pi return varchar is
begin
return 'pie, my favorite desert.';
end;
/

 
 create or replace function f1 RETURN VARCHAR is
 BEGIN RETURN 'X'; 
 END;
 /
 
 create or replace function f2 RETURN CHAR is
 BEGIN RETURN 'X'; 
 END;
 /
 
 create or replace function f3 RETURN VARCHAR is
 BEGIN RETURN NULL; 
 END;
 /
 
 create or replace function f4 RETURN CHAR is
 BEGIN RETURN NULL; 
 END;
 /


create or replace function bug25373(p1 INTEGER) RETURN INTEGER is
begin
-- LANGUAGE SQL
RETURN p1;
end;
/

drop table t3_5;
CREATE TABLE t3_5 (f1 INT, f2 NUMBER);
INSERT INTO t3_5 VALUES (1, 3.4);
INSERT INTO t3_5 VALUES(1, 2);
INSERT INTO t3_5 VALUES(1, 0.9);
INSERT INTO t3_5 VALUES(2, 8);
INSERT INTO t3_5 VALUES(2, 7);


create or replace function bug5274_f1(p1 NVARCHAR2) RETURN VARCHAR2 is
begin
  RETURN CONCAT(p1, p1);
end;
/

create or replace function bug5274_f2 RETURN VARCHAR is
  v1 INT DEFAULT 0;
  v2 VARCHAR(100) DEFAULT 'x';
BEGIN
  WHILE v1 < 6 loop
    v1 := v1 + 1;
    v2 := bug5274_f1(v2);
  END loop;
  RETURN v2;
end;
/


create or replace function metered(a INT) RETURN INT is begin RETURN 12; 
end;
/

drop table tt1_11;
create table tt1_11(f1_11 int);
insert into tt1_11 values(1);
insert into tt1_11 values(2);

create or replace function func30787(p1 int) return int is
begin
  return p1;
end ;
/

drop table tt1_12;
create table tt1_12(c1 INT);

create or replace function f1_12(p1_12 int) return varchar is
begin
  return 'aaa';
end;
/


commit;

--Case 47:

create or replace function f1_13 RETURN VARCHAR is begin RETURN 'Hello'; 
end;
/

create or replace function f2_13 RETURN SMALLINT is begin RETURN 1; 
end;
/


-- Case 55:

create or replace function my_func_14(val1 int, val2 int) return int is begin return val1 + val2; 
end;
/

drop table tt1_14;
drop table t2_14;
create table tt1_14(col1 int);
create table t2_14(col2 int);
insert into tt1_14 values(1);
insert into t2_14 values(1);



-- Case 56: test recursion call


create or replace function my_func_15(val1 int)
return int
is
  val INT := val1;
begin
  if (val > 1) then
    val := my_func_15(val - 1);
  end if;
  return val;
end;
/



create or replace function f1_16 RETURN INT is
BEGIN
  BEGIN
    BEGIN
     RETURN f1_16();
     EXCEPTION
       WHEN OTHERS THEN return f1_16();
    END;
  EXCEPTION
    WHEN OTHERS THEN return f1_16();
  END;
  RETURN 1;
EXCEPTION
  WHEN OTHERS THEN RETURN NULL;
END;
/


-- Case 60: procedure function dependency test


create or replace procedure proc_17(v1 int,j out number) is
v INT := v1; 
begin 
j:= v;
end;
/

create or replace function func_17 return int is begin 
return 10; 
end;
/

create or replace procedure my_proc_17 is 
j number;
begin 
proc_17(func_17(),j); 
end;
/




--Case 61: bug#16099253

drop table a_18;
create table a_18(a1 number);

insert into a_18 values(-1);

create or replace procedure p(x1 number) is
  TYPE tab_type IS TABLE OF number;
  abs tab_type;
  cursor c is select a1 from a_18;
  x number := x1;
begin
  open c;
  fetch c bulk collect into abs limit 1000;
  x := abs(1);
  insert into a_18 values(x);
  close c;
end;
/



--case 62 function as a param

create or replace function f1_19 return integer is 
begin
  return 1000;
end;
/

create or replace function f2_19(i int) return int is
begin
  return i;
end;
/


create or replace function f3_19(j SMALLINT) return int is
begin
  return j;
end;
/

drop table c_table;
drop table c_res;

create table c_table(id number);
insert into c_table values(2);
create table c_res(res number);


create or replace function c_fun(a number) return number is
begin
  return a + 3;
end;
/

create or replace package c_pack_20 is
function c_fun(a number) return number;
end;
/

create or replace package body c_pack_20 is
function c_fun(a number) return number is
  begin
    return a + 9;
  end;
end;
/

create or replace package c_pack_21 is
procedure c_proc;
function c_fun(a number) return number;
procedure c_proc1;
end;
/

create or replace package body c_pack_21 is
function c_fun(a number) return number is
  begin
    return a;
  end;
procedure c_proc is
v_value number;
begin
select c_fun(id) into v_value from c_table where c_fun(id) is not null;
insert into c_res values(v_value);
DBMS_OUTPUT.PUT_LINE('v: ' || v_value);
end;
procedure c_proc1 is
begin
DBMS_OUTPUT.PUT_LINE('proc1');
end;
end;
/


create or replace package c_pack_21 is
procedure c_proc;
function c_fun(a number) return number;
procedure c_proc1;
end;
/

create or replace package body c_pack_21 is
function c_fun(a number) return number is
  begin
    return a;
  end;
procedure c_proc is
v_value number;
begin
select c_pack_20.c_fun(id) into v_value from c_table where c_pack_20.c_fun(id) is not null;
insert into c_res values(v_value);
DBMS_OUTPUT.PUT_LINE('v: ' || v_value);
end;
procedure c_proc1 is
begin
DBMS_OUTPUT.PUT_LINE('proc1');
end;
end;
/

drop table t_22;
drop table t_23;

create table t_22(a int, b int);
create table t_23(a int primary key, b int);
insert into t_23 values(1, 1);
insert into t_23 values(2, 3);
insert into t_23 values(4, 4);

create or replace function func_22(a int) return number DETERMINISTIC is
c int := 0;
begin
insert into t_22 values(a,a+1) returning a into c;
return c;
end;
/

--嵌套表中为record类型
CREATE OR REPLACE PACKAGE pkg_23 IS
  TYPE record_type IS RECORD (
    field1 NUMBER,
    field2 DATE,
    field3 VARCHAR2(100)
  );
  
  TYPE table_type IS TABLE OF record_type;
  
  FUNCTION func_23 RETURN table_type;
END pkg_23;
/

CREATE OR REPLACE PACKAGE BODY pkg_23 IS
  FUNCTION func_23 RETURN table_type IS
    -- 嵌套表变量
    table_variable table_type;
    -- record变量
    record_variable record_type;
  BEGIN
    table_variable := table_type();
    
    -- record赋值
    record_variable.field1 := 123;
    record_variable.field2 := TO_DATE('2022-01-01', 'YYYY-MM-DD');
    record_variable.field3 := 'Hello';
    
    --嵌套表赋值
    table_variable.EXTEND;
    table_variable(1) := record_variable;
    
    RETURN table_variable;
  END func_23;
END pkg_23;
/

--嵌套表中为字符类型
CREATE OR REPLACE PACKAGE pkg_24 IS
  TYPE nested_table_type IS TABLE OF VARCHAR2(100);
  FUNCTION func_24 RETURN nested_table_type;
END pkg_24;
/


CREATE OR REPLACE PACKAGE BODY pkg_24 IS
  FUNCTION func_24 RETURN nested_table_type IS
    nested_table nested_table_type := nested_table_type('hello world');
  BEGIN
 
    RETURN nested_table; 
  END func_24;
END pkg_24;
/  

CREATE OR REPLACE PACKAGE pkg_25 IS
  TYPE nested_table_type IS TABLE OF NUMBER;
  
  PROCEDURE proc_25(p_values IN nested_table_type);
END pkg_25;
/

CREATE OR REPLACE PACKAGE BODY pkg_25 IS
  PROCEDURE proc_25(p_values IN nested_table_type) IS
  BEGIN
    FOR i IN 1..p_values.COUNT LOOP
      DBMS_OUTPUT.PUT_LINE(p_values(i));
    END LOOP;
  END proc_25;
END pkg_25;
/

CREATE OR REPLACE PACKAGE pkg_26 IS
  -- 声明函数
  FUNCTION func_26(
    p_char IN OUT CHAR,
    p_varchar IN OUT VARCHAR2,
    p_number IN OUT NUMBER,
    p_decimal IN OUT DECIMAL,
    p_float IN OUT FLOAT,
    p_double IN OUT number,
    p_int IN OUT INT,
    p_integer IN OUT INTEGER,
    p_smallint IN OUT SMALLINT,
    p_real IN OUT REAL,
    p_date IN OUT DATE,
    p_timestamp IN OUT TIMESTAMP
  ) RETURN NUMBER;
END pkg_26;
/

CREATE OR REPLACE PACKAGE BODY pkg_26 IS
  -- 实现函数
  FUNCTION func_26(
    p_char IN OUT CHAR,
    p_varchar IN OUT VARCHAR2,
    p_number IN OUT NUMBER,
    p_decimal IN OUT DECIMAL,
    p_float IN OUT FLOAT,
    p_double IN OUT number,
    p_int IN OUT INT,
    p_integer IN OUT INTEGER,
    p_smallint IN OUT SMALLINT,
    p_real IN OUT REAL,
    p_date IN OUT DATE,
    p_timestamp IN OUT TIMESTAMP
  ) RETURN NUMBER IS
  BEGIN
    -- 在函数中对参数进行操作
    p_char := 'Modified Char';
    p_varchar := 'Modified Varchar2';
    p_number := p_number + 1;
    p_decimal := p_decimal * 2;
    p_float := p_float + 1.5;
    p_double := p_double + 2.5;
    p_int := p_int + 1;
    p_integer := p_integer + 1;
    p_smallint := p_smallint + 1;
    p_real := p_real + 1.5;
    p_date := TO_DATE('2022-01-01', 'YYYY-MM-DD');
    p_timestamp := TO_TIMESTAMP('2022-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
    
    -- 返回值
    RETURN 1;
  END func_26;
END pkg_26;
/


CREATE OR REPLACE PACKAGE pkg_27 IS
  FUNCTION func_27(
    p_date IN OUT DATE,
    p_timestamp IN OUT TIMESTAMP
  ) RETURN NUMBER;
END pkg_27;
/

CREATE OR REPLACE PACKAGE BODY pkg_27 IS
  FUNCTION func_27(
    p_date IN OUT DATE,
    p_timestamp IN OUT TIMESTAMP
  ) RETURN NUMBER IS
  BEGIN
    p_date := TO_DATE('2022-01-01', 'YYYY-MM-DD');
    p_timestamp := TO_TIMESTAMP('2022-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
    
    RETURN 1;
  END func_27;
END pkg_27;
/

CREATE OR REPLACE PACKAGE pkg_28 IS
  PROCEDURE proc_28(
    p_date IN OUT DATE,
    p_timestamp IN OUT TIMESTAMP
  );
END pkg_28;
/

CREATE OR REPLACE PACKAGE BODY pkg_28 IS
  PROCEDURE proc_28(
    p_date IN OUT DATE,
    p_timestamp IN OUT TIMESTAMP
  ) IS
  BEGIN
    p_date := TO_DATE('2022-01-01', 'YYYY-MM-DD');
    p_timestamp := TO_TIMESTAMP('2022-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  END proc_28;
END pkg_28;
/

CREATE OR REPLACE PACKAGE pkg_29 IS
  TYPE nested_table_type IS TABLE OF VARCHAR2(100);
  
  FUNCTION func_29(p_input IN VARCHAR2) RETURN nested_table_type;
END pkg_29;
/

CREATE OR REPLACE PACKAGE BODY pkg_29 IS
  FUNCTION func_29(p_input IN VARCHAR2) RETURN nested_table_type IS
    nested_table nested_table_type := nested_table_type(p_input);
  BEGIN
    RETURN nested_table; 
  END func_29;
END pkg_29;
/


CREATE OR REPLACE FUNCTION hello_world
RETURN VARCHAR2
IS
BEGIN
RETURN 'Hello, World!';
END;
/


CREATE OR REPLACE PROCEDURE hello_world_pro 
authid current_user
AS
message VARCHAR2(20) := 'Hello, World!';
BEGIN
  DBMS_OUTPUT.PUT_LINE(message);
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
END;
/


--出参为number
CREATE OR REPLACE PROCEDURE my_proc(p_in_out IN OUT NUMBER)
AS
BEGIN
  p_in_out := p_in_out * 2; 
END;
/

create synonym my_proc_syn for my_proc;

--出参为varchar2
CREATE OR REPLACE PROCEDURE my_proc_1(p_in1 IN VARCHAR2, p_in2 IN NUMBER, p_out OUT VARCHAR2)
AS
  v_num NUMBER;
  v_str VARCHAR2(100);
BEGIN
  -- 执行一些操作
  v_num := p_in2 * 2; -- 将p_in2的值乘以2
  v_str := p_in1 || ' ' || TO_CHAR(v_num); -- 将p_in1和v_num连接成一个字符串
  DBMS_OUTPUT.PUT_LINE('v_str is: ' || v_str);
  p_out := v_str; -- 将v_str的值赋值给p_out
END;
/




CREATE OR REPLACE PROCEDURE my_proc_only_in(p_in IN  NUMBER)
AS
 p_in_out NUMBER;
BEGIN
  p_in_out := p_in * 2; 
  dbms_output.put_line('output is' || p_in_out);
END;
/




--参数
CREATE OR REPLACE PROCEDURE MY_PROCEDURE (
  p_outt1 OUT NUMBER,
  p_out2 OUT VARCHAR2,
  p_out3 OUT DATE,
  p_out4 OUT NUMBER,
  p_out5 OUT VARCHAR2,
  p_out6 OUT DATE,
  p_out7 OUT NUMBER,
  p_out8 OUT VARCHAR2,
  p_out9 OUT DATE,
  p_outt10 OUT NUMBER
) AS
BEGIN
  p_outt1 := 123;
  p_out2 := 'Test';
  p_out3 := TO_DATE('2022-01-01', 'YYYY-MM-DD');
  p_out4 := 456;
  p_out5 := 'Test2';
  p_out6 := TO_DATE('2022-01-01', 'YYYY-MM-DD');
  p_out7 := 789;
  p_out8 := 'Test3';
  p_out9 := TO_DATE('2022-01-01', 'YYYY-MM-DD');
  p_outt10 := 101112;
END;
/

--MY_PROCEDURE_1 
CREATE OR REPLACE PROCEDURE MY_PROCEDURE_1(p_in IN VARCHAR2 DEFAULT 'Test',p_out OUT varchar2) 
AS
BEGIN
  p_out := 'abcdefg';
  DBMS_OUTPUT.PUT_LINE('Input Parameter: ' || p_in);
  DBMS_OUTPUT.PUT_LINE('Output Parameter: ' || p_out);
END;
/


--MY_PROCEDURE_2 
CREATE OR REPLACE PROCEDURE MY_PROCEDURE_2 (
  p_out_char OUT CHAR,
  p_out_varchar OUT VARCHAR,
  p_out_varchar2 OUT VARCHAR2,
  p_out_number OUT NUMBER,
  p_out_int OUT INT,
  p_out_integer OUT INTEGER,
  p_out_float OUT FLOAT,
 -- p_out_double OUT DOUBLE PRECISION,
  p_out_decimal OUT DECIMAL,
  p_out_pls_integer OUT PLS_INTEGER
) AS
BEGIN
  p_out_char := 'A';
  p_out_varchar := 'C';
  p_out_varchar2 := 'D';
  p_out_number := 123;
  p_out_int := 456;
  p_out_integer := 789;
  p_out_float := 1.23;
  --p_out_double := 4.56;
  p_out_decimal := 7.89;
  p_out_pls_integer := 101112;
END;
/

--MY_PROCEDURE_4 
CREATE OR REPLACE PROCEDURE MY_PROCEDURE_4 (
  p_in_varchar IN VARCHAR DEFAULT 'Test_p_in_varchar',
  p_inout_default IN OUT VARCHAR2 ,
  p_in_varchar2 IN VARCHAR2 DEFAULT 'Test_p_in_varchar2',
  p_in_number IN NUMBER DEFAULT 123,
  p_in_int IN INT DEFAULT 456,
  p_in_integer IN INTEGER DEFAULT 789,
  p_in_float IN FLOAT DEFAULT 1.23,
  p_in_decimal IN DECIMAL DEFAULT 4.56,
  p_in_binary_float IN BINARY_FLOAT DEFAULT 7.89,
  p_in_binary_double IN BINARY_DOUBLE DEFAULT 10.11,
  p_in_pls_integer IN PLS_INTEGER DEFAULT 121314,
  p_in_date IN DATE DEFAULT TO_DATE('2022-01-01', 'YYYY-MM-DD'),
  p_in_timestamp IN TIMESTAMP DEFAULT TO_TIMESTAMP('2022-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
) AS
BEGIN
  DBMS_OUTPUT.PUT_LINE('Input Parameters: ');
  DBMS_OUTPUT.PUT_LINE(p_in_varchar);
  DBMS_OUTPUT.PUT_LINE(p_in_varchar2);
  DBMS_OUTPUT.PUT_LINE(p_in_number);
  DBMS_OUTPUT.PUT_LINE(p_in_int);
  DBMS_OUTPUT.PUT_LINE(p_in_integer);
  DBMS_OUTPUT.PUT_LINE(p_in_float);
  DBMS_OUTPUT.PUT_LINE(p_in_decimal);
  DBMS_OUTPUT.PUT_LINE(p_in_binary_float);
  DBMS_OUTPUT.PUT_LINE(p_in_binary_double);
  DBMS_OUTPUT.PUT_LINE(p_in_pls_integer);
  DBMS_OUTPUT.PUT_LINE(p_in_date);
  DBMS_OUTPUT.PUT_LINE(p_in_timestamp);
  
  DBMS_OUTPUT.PUT_LINE('Input/Output Parameter: ' || p_inout_default);
  
  p_inout_default := 'New Value';
  
END;
/

--my_procedure_6
CREATE OR REPLACE PROCEDURE MY_PROCEDURE_6(p_in char, p_out OUT char) AS
BEGIN
  p_out := p_in || RPAD(' ',32167, '*');
END;
/



--my_package 
CREATE OR REPLACE PACKAGE my_package AS
  TYPE my_type IS RECORD (
    id NUMBER,
    name VARCHAR2(50),
    age NUMBER
  );
  
  PROCEDURE my_procedure(p_in_data IN my_type, p_out_message OUT VARCHAR2);
END my_package;
/

-- 实现my_procedure
CREATE OR REPLACE PACKAGE BODY my_package AS
  PROCEDURE my_procedure(p_in_data IN my_type, p_out_message OUT VARCHAR2) IS
  BEGIN
    p_out_message := 'Hello, ' || p_in_data.name || '! Your age is ' || p_in_data.age || '.';
  END my_procedure;
END my_package;
/

--MY_PACKAGE_2
CREATE OR REPLACE PACKAGE MY_PACKAGE_2 AS
  TYPE MY_RECORD IS RECORD (
    ID NUMBER,
    NAME VARCHAR2(100),
    AGE NUMBER
  );
  
  PROCEDURE MY_PROCEDURE(p_in_data IN MY_RECORD, p_out_data OUT MY_RECORD);
END MY_PACKAGE_2;
/

CREATE OR REPLACE PACKAGE BODY MY_PACKAGE_2 AS
  PROCEDURE MY_PROCEDURE(p_in_data IN MY_RECORD, p_out_data OUT MY_RECORD) IS
  BEGIN
    p_out_data := p_in_data;
    p_out_data.NAME := UPPER(p_in_data.NAME);
  END MY_PROCEDURE;
END MY_PACKAGE_2;
/



--my_package_3
drop table departments;
CREATE TABLE departments (
  id NUMBER,
  name VARCHAR2(100)
);

INSERT INTO departments (id, name) VALUES (1, 'Sales');

INSERT INTO departments (id, name) VALUES (2, 'Marketing');

drop table employees;
CREATE TABLE employees (
  id NUMBER,
  name VARCHAR2(100),
  age NUMBER,
  department_id NUMBER
);

INSERT INTO employees (id, name, age, department_id) VALUES (1, 'John', 30, 1);

INSERT INTO employees (id, name, age, department_id) VALUES (2, 'Jane', 25, 1);

INSERT INTO employees (id, name, age, department_id) VALUES (3, 'Bob', 40, 2);

INSERT INTO employees (id, name, age, department_id) VALUES (4, 'Alice', 35, 2);

CREATE OR REPLACE PACKAGE my_package_3 AS
  type my_record is record(  
  id NUMBER,
  name VARCHAR2(100),
  age NUMBER,
  department_id NUMBER);

  TYPE emp_list_type IS TABLE OF my_record;

 --out出参使用table of 
  PROCEDURE get_employee_list(
    dept_id IN NUMBER,
    emp_list OUT emp_list_type
  );
END my_package_3;
/

CREATE OR REPLACE PACKAGE BODY my_package_3 AS
  PROCEDURE get_employee_list(
    dept_id IN NUMBER,
    emp_list OUT emp_list_type
  ) IS
  BEGIN
   
    SELECT *
    BULK COLLECT INTO emp_list
    FROM employees
    WHERE department_id = dept_id;
  END;
END my_package_3;
/


--my_package_4
CREATE OR REPLACE PACKAGE my_package_4 AS
  TYPE emp_type IS RECORD (
    id NUMBER,
    name VARCHAR2(100),
    age NUMBER,
    department_id NUMBER
  );

  TYPE emp_list_type IS TABLE OF emp_type INDEX BY PLS_INTEGER;

  PROCEDURE process_employee_list(
    emp_list IN OUT emp_list_type
  );

  PROCEDURE process_employee_age(
    emp_list IN OUT emp_list_type
  );

END my_package_4;
/

CREATE OR REPLACE PACKAGE BODY my_package_4 AS
  PROCEDURE process_employee_list(
    emp_list IN OUT emp_list_type
  ) IS
    BEGIN
      FOR i IN 1..emp_list.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('ID: ' || emp_list(i).id || ', Name: ' || emp_list(i).name || ', Age: ' || emp_list(i).age || ', Department ID: ' || emp_list(i).department_id);
      END LOOP;
    END;

  PROCEDURE process_employee_age(
    emp_list IN OUT emp_list_type
  ) IS
    -- 遍历输入的Nested Table，将其中员工的年龄自增5
    BEGIN
      FOR i IN 1..emp_list.COUNT LOOP
        emp_list(i).age := emp_list(i).age + 5;
      END LOOP;
    END;

END my_package_4;
/


CREATE OR REPLACE PACKAGE pkg_30 IS

  FUNCTION func_30(
    p_char IN OUT CHAR
  ) RETURN NUMBER;
END pkg_30;
/

CREATE OR REPLACE PACKAGE BODY pkg_30 IS
  FUNCTION func_30(
    p_char IN OUT CHAR
  ) RETURN NUMBER IS
  BEGIN
    p_char := 'Modified Char';
    
    RETURN 1;
  END func_30;
END pkg_30;
/

commit;
