--#####################################
--异常场景
--#####################################
--case 1
CREATE OR REPLACE TYPE tb IS TABLE OF INTEGER;
/

create or replace type va is array(10) of number not null;
/

CREATE OR REPLACE TYPE obj0 WRAPPED
140
TtcB+Fgy9ITf38jwgFVIN3rPg/n3lnNkz3rFjV5D4tpc+vvexmSN3Eix0MYIDYQHrAf/rBPsmQEb
tuDZfBvsVzghshGwV/3Qt/8FiW1W7/ttYt/WMNKY4Hsxey7fQvEu6EJqSCy6ViGy
;
/

--error 0,600
drop type obj0_57652309 force;

--disable_warnings
delimiter /;
CREATE OR REPLACE TYPE obj0_57652309 WRAPPED
152
ocMBBh1OhL1geVy9gFVIN3rPg/n3lnM5UWNRMWNzxrccz3rFjV5D4tpc+vvexmSN3Eix0MYIDQes
B/+sE+yZARu24Nl8G+xXOCGyEbBX/bAcFkUMaYSd5JxtTW8Asu1iMKv4q4Sp+AKEIkX8ZSyNl23f
;
/
--enable_warnings

--error 304
CREATE OR REPLACE TYPE BODY obj0_57652309 WRAPPED
208
6H4B1M8lWQQDz3iegFVIN3rPg89DXjcc+feWczlRY1ExY3PGtxzPenLmZI3aLdriuubPSC2HXKHc
9g3eMS7n/6bQDY3cnYP8ET71IZIhCFAtwPOVE/IWl2iX6RVdhelNl9Rz+hPgUKY92yYcWre6vsDD
ROw+YAE4Y2Tqs/S4xMqjqPn9iW2bz4GYt4z06rRraa2UOVKtLJpFMZ8=
;
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
create or replace type user_type1 WRAPPED
140
ecwBLYV3YdyYzuk7gFUFnhnsGG2nSs1GxMhJ/QFHZdNQu9sgzrsK2ncJiGZmsBpCsbhQStbCjjvT
S6DNC6DDW/h1bTRkGp0BVwvX+ocooLzmWF3d7Tp8TONwSOAGucmtpKNbaHqK4A==
;
/
select * from all_errors where name = 'USER_TYPE1' and owner='TEST';
/
drop type user_type1 force;
/

--case1 自己引用自己，加密前警告，加密后报4016
--PLS-00206: %%TYPE must be applied to a variable, column, field or attribute, not to other
--error 0, 600
drop type user_type1 force;

create or replace type user_type1 WRAPPED
144
n18B+toqzusBCd2HgFUFkCnse20+bL33oWoY5GpOsJCgNu8+WXBWPTXx06dXYADr3CqMsdCTfXRt
3U7SDeZIq20YfzdP2CbtRae+Ic7P4fxzTlERwJ9Ec8Z8/DjTXOREjCW6kCambKJIips=
;
/

--case2 加密前警告，加密后报4016
--PLS-00630: pipelined functions must have a supported collection return type
--error 0,600
drop type obj0_57652309 force;

--disable_warnings
CREATE OR REPLACE TYPE obj0_57652309 WRAPPED
160
Ji4BMqSN2faoP/vTgFVIN3rPg/n3lnM5UWNRMWNzxrccz3rFjV5D4tpc+vuX5mRk3sZkjdxIsVox
us2CzFnM1lZtbDVkqIU+926oiUUNgVJOVUV5+KpXauwTrOABEz766p5SkowvWtlarG/ZG6yVauOE
LE70PWw=
;
/
--enable_warnings

--case3 加密前警告，加密后报4016
--PLS-00523: ORDER methods must return an INTEGER，
delimiter /;
--disable_warnings
create or replace type cai_type15 WRAPPED
136
xWMBbpP0u4fwbRDHgFXbEfPeg97/npAzWnN6BQyStAxDszLms4hF0pIMQ3WHs+a03dJDtObmtKfi
DRP4iRLhk/WLngP4iYuAPjFb8xH6w04PANM6izjk+jvQV1Du72OULDgWilI=
;
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

