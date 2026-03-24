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
create or replace function f00 WRAPPED
112
ZO0BNPcOKw/m7iUqgFWI26aIlcCe8Q3eMaVk53VM/kyWDfnbpgV1TOb588X7bB61D1mTKktSdXUF
8Z4NLFo6+qDBnbynsU6Tp7xsemssZg/WgQ==
;
/
ALTER FUNCTION f00 COMPILE;
select f00() from dual;

select 'CASE:f01' from dual;
create /* comment1 */or /* comment1 */ replace /* comment1 */ function /* comment1 */ f01 WRAPPED
168
BBoBNw2fJFwibjpUgFUkgRPsfqV6CEvOslsNFt6p/F0QH5iVYK7DWY5uv3dHdbLZn9X6xX0LRY/p
t2uuN7J+/PleqH+nbzx4/n+pSANLZtAQ+xyPd+u6X5hUssuuFtpC1QT+OJhnDS+2ad8WsbLPWsle
/qP9k3EDDQYlbtDf
;
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
create or replace function f02 WRAPPED
160
M8UBZiwVKxThC+TigFWI26aIlcCe8Q3eMaXh3hz5ntv6tzKDpeHQrXMMQ4iI/T1q7OgR7UfozuzO
R2XQEejshgHgW9mV7JWsmdkbrBPs7ACyhITAbYnjhITQEV0Azm2V+w6YAk95YhzHc+8Aakm/pmws
U6LgsQ==
;
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
  f03 WRAPPED
444
SZUBvbsGdSc2W/oCvK2AVYHvBcdp/T5gS3zuZVrVRcGYIwUnYvSLd79J7QkknSgkBexFGkVXuQRe
WeM5N9P/rwAAXupe+StqGszKtCvDLNN2/EnQ2XPCGuIYLLTTTQw9YA6ImlFBU799OsqTDgAjQtqX
y5601BPyMtHkCDo8EU88H+6QAorJulnymYGmdmEl7gyF/ivYrnQ6fE4zNogZ/fQstEtMxr0eF6+W
ozpUZwtO9CWqb+uTqrMf3wgkZVVmj86XssxkcHbpqVjggivf2bmr0R2C0uVNfyb2V7xD+HRfGTFG
nD/zE7E/BHrXwRbxdPbSmc2pkxb7uNOfRWK4qSyT/FDDK3PJR5UmYGGkHpbVi+jItCQUufLlv47n
mnTUpeFrI5gNDbsi5yve3KjKmPl6EnSBmlGad1h0LpVHZHS5lPBub9FafbRnlg==
;
/
select f03(rand_range=>100) from dual;

select 'CASE:f04' from dual;
create or replace function f04 WRAPPED
88
mbIBaCzkryHGPyIxgFWI26aIlcCe8Q3eMaX653VM/kyWDfnbpgV1TOb583J1dQXxnhVp7COuh0dy
R6OHWizJSVAe
;
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
create or replace PROCEDURE "pro01_or" WRAPPED
140
zUoBEOKxD3G2jRi/gFVIYkNen9rUuuKDZB3nllGlkPEz5uHeHPmeq02SX0/nnQ0T1zHyy8AuP0IE
iBPXssBR0X2oRU9tRZgnORWp+AKEY07wQC68O0WSa7NFY/xlhAKENmUsaWAjcg==
;
/
call "pro01_or"(a=>10,b=>20);

select 'case: pro04_replace' from dual;
CREATE OR REPLACE PROCEDURE pro04_replace WRAPPED
176
U40BjuZYji8pkK2JgFVIYkNen9rUuuKD5+eWUaU3M3VM+dCC/y7eHPmeDfkzTFrm7UTQpdU4aRBm
OdPEtGm09A/E6rzv5L9hSbP0uMTKSWnki2m7/ORNxU1j7wlS9SMrS8ohkj4hTXEUoW/9siHp58ag
Vk5PV66HZY5lo4daLO9a1qM=
;
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
CREATE OR REPLACE PROCEDURE pro05_PROCEDURE WRAPPED
264
n6gBNGTk2UqAOKMCRVqAVYkFuOy5pac/VAxevEg9g88R0RLALsNAT3IemQE/hDbeP0OhinVxbMLS
z7YvMxEIEzsMuQlvwSPWtdktpW+K0oTEsqVYKx28IUoeo0fvI7VjHa9EEaF7gK39Cv8p8IES62i2
a5AQ1RtjxFPGo73nXrcdPSs9hH+MtWnhpVC4/7ril7mxlhyZM/PEr1Lb3o28XX58fc4uOe8cjm5b
ux3gLZrKJj3nvuq2Recid53/NcAWl8fmgQ==
;
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
CREATE OR REPLACE PROCEDURE pro06 WRAPPED
148
oQwBeEqOsARqu+6agFVIYkNen9rUuuKD5+eWUaVC3hz5ntuxHM96uqGN3frPXLrs4i1INye6n7rm
jXpy4uLa9twV5mTPwkO6hvYnutRiurGrUoDK96hZutq31MXpY2M3auOErBuV44QspF4hEw==
;
/
ALTER PROCEDURE pro06 COMPILE;
call pro06(10);

--#包
--error 0,00942
drop table tab1;
CREATE TABLE tab1 (c1 int, c2 varchar2(4000) DEFAULT 'syn1_table');

select 'case: pack00_PACKAGE' from dual;
CREATE OR REPLACE PACKAGE pack00_PACKAGE WRAPPED
80
aU8BzmF4ZSMjOQxEgFVIz92kn91eg+fegrbGpTknGOIGnGWnp5IMA+SpX4ScNmWEAoT7HKOHWizx
Sic7
;
/
ALTER PACKAGE pack00_PACKAGE COMPILE PACKAGE;

select 'case: pack00_function' from dual;
CREATE OR REPLACE PACKAGE pack00_function WRAPPED
88
OdkBrQav1Qapagr4gFVIz92kn91eg+fegrbGpTn326aIlcCe8Q3PenLmZN4cjUgt2uK6MnLiLYcT
Wb+mbCz+vZKa
;
/

select 'case: pack01_type' from dual;
CREATE OR REPLACE PACKAGE pack01_type WRAPPED
528
iakBzvGcVQwzbsUCPK2AVSiLpv+5ZD6J9viyHxgIi3SIN7RAmoznTGUkBYRja0U7CEAOv7m2jD5P
IcIBw4EhgZWjIJwpA+CTegLwQduAE/M+SV7rB4fr/5nZQcZ42gjVOMpkEulO8zy5HeKKy6sE4XBa
8N6K16lGaEY1k7iOyI4aTptivtqbTSMrYwcl/0YV9LGv3tl7/iA8E8UMN/3XEFZu2Sd8TVDF7ixm
z6A0jtnrMWFgTaDRNjbk3deoaj2dFkKPtF6RQ8lvP1QW9hGF2rbeIn22PKV11TsjF1eUbdtaSlU5
zjl2lYImVGw+DqIhuTe3HON1wyoZ8su38jexFLsSUohKmiVLL0WdPOKBT1L+NlkJ7lcW9BFVjMZQ
1pz/1v21bYGyK9FAXEYLQXQpFespTuKCf9uqmKpm43pYc5GZ4lEt02NxfSgzCOHue8Bw+hgiftSw
g1H0KNNV29uj8leeja943LjY70Oj9T5nrwiD9HmwSudQJoaFbkVPkk+NtCXgDFNaY1jhHA==
;
/
ALTER PACKAGE pack01_type COMPILE PACKAGE;
select pack01_type.a from dual;

select 'case: pack02_wrapped' from dual;
create or replace package pack02_wrapped WRAPPED
156
xWMB6Loy1dUAjRTHgFXb3oK2/4L3g+cs4cNKoUHoN2xsEf7shIYB4FvZley2ARMTrBvgoQHXrBPs
hACyhOzsbOhTwBH+R+g+L6H9rGNkRfxFM2VPMagnPWJS/qdH1Q7BQf71u3KKR8Fwh0dyR6OHWizB
S+3C
;
/
create or replace package body pack02_wrapped WRAPPED
232
mycBFvy6zq0+PU8CUVqAVWuGpuy5pT6MS6nuRg7pRyBnRjdbCJt7pCtDMNVtivAF56m9Qg7sbxjT
PygH8ACh5C/oBG1fW9fWGTRDHoz2oNeTSCz3/wGGjDRB0LYr/f0+i7v1YKW+KnOzAC7uu0izeCMP
RgLlGoVWrEbaJF9g6oZJfqqOpjbjQieuP6ddb02D19Epf/fsYp1PSMYHT+GbD3uS+QdGQ44sNvLM
qw==
;
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
create or replace package pack03_body WRAPPED
152
tY8BRdgvbqY3JS28gFXb3oK2/4L3g+cs4cOPodBT/ruEhgHgW9mV7LYBExOsG+ChAdesE+yEALKE
7Oxs6FPAEf5H6D4tof2sY2RF/EUzZU8xqCc9YlL+p0fVDsFB/vW7copHwXCHR3JHo4daLBroT7o=
;
/
create or replace package body pack03_body WRAPPED
228
GUYBXYYobJnA1ZYCPVqAVdvegrb/gveD3pb3ERznLOJ6kBXXHxS/lWyJaU0D9U4O4U0ctxz47/Tv
1JL3dQXxnqvh7D5MSqFB6DdsbBH+vqUoIWrjr1eU1smXgJv77xj7Pi9thvah3IcuZH8yfwY2pWMz
smpI3sS7cu1JOnKFEzaroafu5oh1tEOnQ3WHs+a0+yTW4YBs8v54hEgy6KvN7G0eK1zjhCwfeIRs
;
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
create or replace package pack04_or WRAPPED
104
76ABgAKIpq2WtrUpgFXb3oK2/4L3g+cs4cOKoVPohIYB4FvZley2ARMTrBvgoQHXrBPshACyhOzs
N+zOR2XQEejjhBHO/uOELDHHUC0=
;
/

--#OBJECT
select 'case: obj00' from dual;
CREATE or  replace TYPE obj00 WRAPPED
112
uIIB3RiA5SwyQ4A6gFVIN3rPg/n3lnOlZM96xY1eQ+LaXPr7ZPsug91kxfqBDULh+5BWcuZk3hxi
h3ripM96c+GlY/uYs+MSxf1aZuEsDcMW2A==
;
/
select obj00('obj00') from dual;

select 'case: obj01' from dual;
CREATE  or  replace   TYPE  obj01 WRAPPED
140
Qf8Bt8mWBnrRAEBwgFVIN3rPg2T595ZzpRxkZM96xWSNXkPi2lz6ZPuX5mTeHGKHeuKkz3pz4aVj
+05a7sgXjkRlw/7SiY6w04COVP4LxNUU1jjEHQhlYIzDU/7IiWkLZUX8ZSyWXsFM
;
/
CREATE or  replace TYPE BODY obj01 WRAPPED
140
Lz8BVlXQWZn1dYBmgFVIN3rPg89DXjcc+feWc6Ucz3py5mSN2i3a4rrmz0gth1yh3PYN3vdMN/eC
94Ni4rrUukMNjUhsiL1pDkRlF72OWlds8jpNY++F6YWtQjqmbPWjh1osNckjIg==
;
/
select obj01('obj01') from dual;

select 'case: obj02' from dual;
CREATE or replace TYPE obj02 WRAPPED
160
OaUBblESL9+HuU4bgFVIN3rPg/n3lnOl5t7LM3X/g896xY1eQ+LaXPr7l+Zk3hxih3ripM96c+Gl
Y/tOza3sXbxa1J0ro4jKCxRWLlbAY97XTE7xsBRCyffgL2tPb2Evg+D5Y09F+ffXM6dPpkdXpmws
U8MjUw==
;
/
select obj02('obj02') from dual;

select 'case: obj03_null' from dual;
create or replace type obj03_null WRAPPED
172
SdQBNnfQjR8NPrIAgFVjkBPsfm0+6PawsswdOvDdNH4ON2hAw1icBqtGqaVOP24E8ULqpZYXuI2e
TO37NRZ/Yxb9SOIpxayHcQSzOxoNY32wtceUt3bi6nH2PAvV+Sb0kJQK0BEZdmzqkOHdFBQnoSeO
cLlkRrUJEBcsX8WlAw==
;
/
create or replace type body obj03_null WRAPPED
280
eDoB1EXdoCkPIkHcgFXbEfPeg96W9xEc+feWc6WG8dumwPr583LmZPkFpgV1TObn55b3/wX+THWD
3vdM/oie/gUu56Uc+Z4N+TNM+vnbbBGx/qVp1A7twe21T8FoXacgfozmkojmY+BKikc4tXJaV2xs
eVDFdOw3YOM+2VDytQhBaJzOjz7ZtLPHz5NuZt79ZjlbPt9UgI5lO6Coan3FauaSJ90wzUKB2Nlj
825At4UC2XDpY2M3YGNj6URtYFCQ8DbUcx1jM7JqSIIsLFoG990=
;
/
select obj03_null(1,1,null) from dual;


