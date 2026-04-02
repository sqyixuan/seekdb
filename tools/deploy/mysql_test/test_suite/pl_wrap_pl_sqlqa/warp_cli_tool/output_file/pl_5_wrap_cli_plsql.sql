--#####################################
--2.1、WRAP CLI 工具
--支持六种 PL/SQL 对象的不同语法场景
--#####################################
--##函数
--error 0,00600
drop function f00;
create function f00 WRAPPED
88
mbIBaI3tKXrrIa4xgFWI26aIlcCe8Q3eMaVk53VM/kyWDfnbpgV1TOb583J1dQXxnhVp7COuh0dy
R6OHWix99lCp
;
/
select f00() from dual;

create or replace function f01 WRAPPED
132
jjsBekjudEIjSdoigFVMwGHsGKV6/bEvxPmgUq9ZtNPQRbsYOGSzVLwTyEJU7Spe/GAo0KsBQEkV
4IlS4oIxXqrNX38qUrF6TJnVYI4syxHDk5neFijIqLU+KXOTyyBb9A==
;
/
declare
  a int := 0;
  b int;
  c int;
begin
  c := f01(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(c);
end;
/

create or replace function f02 WRAPPED
156
BAQBtya0/VR0gYQkgFWI26aIlcCe8Q3eMaXh3hz5ntv6tzKDpeHQrXMMQ4iI/T1q7OgR7UfozuzO
R2XQEejshgHgW9mV7JWsmdkbrBPs7ACyhMBt2uOE0BFdAM79XPs5mAJPvJWgBzq8rE6Tp7yVLJmF
4Oo=
;
/
declare
  a int := 0;
  b int;
  c int;
begin
  c := f02(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(c);
end;
/

create or replace function f02 WRAPPED
432
xyoBG2TjaAkQjsACcK2AVYHvJsdppaffyQKnhbrdOsJtF2akeWQOmLTb7ZEmfguZT6vBlYjVjc1M
5j1PbzZdQYSOVOqkVfTcHI9w7IURrHLwIWEZqeIslLTQscF2YfWTNkOn4R1Tl7A2qxeHJLet5D44
841QWmK0UP1+tdRUuiXblcf0joIPhlk1OtfOCMKuxgGPG+kpvoUq++OyUiu/y5TnMYqJidqO7Idr
yNuW0c3oaIqoIeeGMVmPVmlfB3mL8+ZU5BbyDzgZGHGlQfuXpFNb/MnELVPYnqW1clJcxmZpC5HT
gns+DGcy9tJ0wKmTBXleSAYRCmsCLICR4Hp4EKRqrAAVbHjDQIw6rnq9h40AlPsQ/cqyv72UPINo
be42HUH4Af0Dm11t5NV5TmubFzX4Iy5r4WU9hz4XYpH0sWxUaqy6
;
/
select f02(rand_range=>100) from dual;

--#存储过程
select 'case: pro00' from dual;
--error 0,00600
drop PROCEDURE pro00;
CREATE PROCEDURE pro00 WRAPPED
104
K2YB9BurtF6qUhuogFVIYkNen9rUuuKD5+eWUaVkjXpy4uLa9twV5mTPwkO6hvYnutRiurGrUoDK
96hZutq3c9oFvJVZqJS8lSz+Dj6y
;
/
call pro00();

select 'case: pro01' from dual;
create or replace PROCEDURE pro01 WRAPPED
132
C3YB6zHIYHhA0YyugFVIYkNen9rUuuKD5+eWUaVO3hz5nqtNkl9P550NE9cx8svALj9CBIgT17LA
UdF9qEVPbUWYJzkVqfgChGNO164uvDtFkmuzRWP8ZYQChDZlLFV/aD0=
;
/
call pro01(a=>10,b=>20);

select 'case: pro02' from dual;
/*
CREATE OR REPLACE EDITIONABLE PROCEDURE pro02(a in out int,b in out int)  IS
BEGIN
  a := a/b;
  DBMS_OUTPUT.PUT_LINE('pro02');
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro02(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(a);
end;
/
*/

select 'case: pro03' from dual;
/*
CREATE OR REPLACE NONEDITIONABLE PROCEDURE pro03(a out int,b out int) IS
BEGIN
  a := a*b;
  DBMS_OUTPUT.PUT_LINE('pro03');
END;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro03(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(a);
end;
/
*/

select 'case: pro04' from dual;
CREATE OR REPLACE PROCEDURE pro04 WRAPPED
164
XpwBdmzkED46bqragFVIYkNen9rUuuKD5+eWUaWx3hz5ng35M0xa5u1E0KXVOGkQZjnTxLRptPQP
xOq87+S/YUmz9LjEyklp5Itpu/zkTcVNY+8JUvUjK0vKIZI+IU1xFKFv/bIh6a+zLrw7i0X8ZYQC
hDZlLC65bVI=
;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro04(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(b);
end;
/

select 'case: pro05' from dual;
CREATE OR REPLACE PROCEDURE pro05 WRAPPED
252
XqcBBAGSPHy7VFgClFqAVYmQLydppT5gXhuyJIkUoLRIFFqRDWr9tFbGF2O3ZTlh0i3fJqV6XI/N
i/Z9BMTtpvrR5ID40wmVCa+Agc+4BPQb+mgOtsXmLF6YoDZew9QL0FyAix1aPtOPgAkqCh9E2WdH
uQ6m+NUMGfsTB9OZQvchZysm2y1Fkxyg/VczdSaOWJ91GF82INczH1kCnOphRJXAA8k8+lzzCIes
ZcEgrSZ98kV203WnwCPpZ4hv
;
/
declare
  a int := 0;
  b int;
  c int;
begin
  pro05(a,b=>b);
  DBMS_OUTPUT.PUT_LINE(a);
end;
/

--#包
--error 0,00942
drop table tab1;
CREATE TABLE tab1 (c1 int, c2 varchar2(4000) DEFAULT 'syn1_table');

--error 0,00600
drop PACKAGE pack00;
CREATE PACKAGE pack00 WRAPPED
76
Rr4BE9QBnW0I1ogdgFVIz92kn91eg+fegrbGpWTPenLmZN4cjUgt2uK6MnLiLYcelqOHWiwJJexO
;
/

CREATE OR REPLACE PACKAGE pack00 WRAPPED
76
Rr4BE9QBnW0I1ogdgFVIz92kn91eg+fegrbGpWTPenLmZN4cjUgt2uK6MnLiLYcelqOHWiwJJexO
;
/

CREATE OR REPLACE PACKAGE pack01 WRAPPED
520
0soBhVspzeCVA30Cp62AVSiLpv+5/aeJ9qnuuKuS3kCV7ZU7KWL6rnMkBYRja0U7CHV1YcK15N7t
IcIBw4EhgZWjIJwpA+CTehIoQdvR/UzglYXZU7YAvXNshsmX41Ac2hUY6+f/gzKwWggOwDdinR1s
FTWRsX/LuMeCmZVKAvF5Vo0zuNfC2IVUnillW7pg9WKT7mGSDAW71Kf42gJJsrKwdcJzbHl+AZkj
vRemj3BqOBa3MuwZzt3zBdxXmtguuLxTS8wV00Khh/EKVbNlMiL9r9X0aiQh1SKMVXogoZLBbf8c
8tBU6D4uAyG5wGYcscuKKsLy8eLygTFP7943R21nyfZbP+c58z2IgMMj2rjrNE8769ZYBRTbr6XQ
paP5r/4nhgd+886BkV83kYaTK8uxiCEZHXB/OHBIzFEiAHx+oh5eLrQYkB1Ddf+S8FbdPf8xNr6y
4AodHcGdBD4nYKzJIg4ce5IECAw8oFm0FxBKwaFBvnxZnR0TTT58HkUtSE+Ey6E=
;
/
select pack01.a from dual;

create or replace package pack02 WRAPPED
148
AWMBlPDmTgbh4Xa1gFXb3oK2/4L3g+cs4cNK7ISGAeBb2ZXstgETE6wb4KEB16wT7IQAsoTs7Gzo
U8AR/kfoPmuh/axjZEX8RTNlTzGoJz1iUv6nR9UOwUH+9btyikfBcIdHckejh1os0lY9yg==
;
/
create or replace package body pack02 WRAPPED
228
3XsBrTBP6QkGzTUC7VqAVdvegrb/gveD3pb3ERznLOJ6kKdztGWnp+zuQ7MNDXW05tdNof2VmIQT
4PoBT2HAZGvnJZMLMm0uIfPVCMFCiMYciTftRzi1ch5BhFUanBUCnZ2t5J3cGNF930Lxg50CsdzG
Odrpam1BEc7+44T9RPxFM2VPMagnv9s5xGQOruauDsTvWWoPDq6YDX/nex9zjCnUgiwscCFwSA==
;
/
declare
  var1 number;
begin
  pack02.pack_pro;
  var1 := pack02.pack_func;
  DBMS_OUTPUT.PUT_LINE('pack02: ' || var1);
end;
/

create or replace package pack03 WRAPPED
144
wo0BHeE+2aOzYDrrgFXb3oK2/4L3g+cs4cOPhIYB4FvZley2ARMTrBvgoQHXrBPshACyhOzsbOhT
wBH+R+g+R6H9rGNkRfxFM2VPMagnPWJS/qdH1Q7BQf71u3KKR8Fwh0dyR6OHWiwRPT0s
;
/
create package body pack03 WRAPPED
220
48YBDgSig5acLrYCCFqAVdvegrb/gveD3pb3ERznLOJ6kAyStGWnp+zuQ7MNDXW05tdNof2s5Ire
+P5jLzEn3tv9HCNd1wtjRfzXuZX/B9ehfAHgWgHgvv0rSxVdhaoBfGa+IxTd2rcyTpOnvJVtX8Vq
SYSLAOpxGQgUDed1TP5Mlg3526YFdUzkNbPnfgyv6224Pjp0w7mZbVotrGOULALI+ts=
;
/
declare
  var1 number;
begin
  pack03.pack_pro;
  var1 := pack03.pack_func;
  DBMS_OUTPUT.PUT_LINE('pack03: ' || var1);
end;
/

--error 00201
create or replace package pack04 WRAPPED
104
F/wBtHUn+50RXNvkgFXb3oK2/4L3g+cs4cOKhIYB4FvZley2ARMTrBvgoQHXrBPshACyhOzsN+zO
R2XQEejjhBHO/mRB/a+mbCzE9hbd
;
/

--#OBJECT
select 'case: obj00' from dual;
--error 0,00600
drop TYPE obj00;
CREATE TYPE obj00 WRAPPED
84
RPABGCyILTLqZM3bgFVIN3rPg/n3lnOlZM96xY1eQ+LaXPr7l+Zk3hxih3ripM96c+GlY/v14RJy
rSxDc+yr
;
/
select obj00('obj00') from dual;

select 'case: obj01' from dual;
CREATE or replace TYPE obj01 WRAPPED
136
BXgBWZkMu16n2UAcgFVIN3rPg/n3lnOlHM96xY1eQ+LaXPr7l+Zk3hxih3ripM96c+GlY/tOWu7I
F45EZcP+0omOsNOAjlT+C8TVFNY4xB0IZWCMw1P+yIlpC2VF/GUs7mUMgQ==
;
/
CREATE TYPE BODY obj01 WRAPPED
140
Lz8BVlXQWZn1dYBmgFVIN3rPg89DXjcc+feWc6Ucz3py5mSN2i3a4rrmz0gth1yh3PYN3vdMN/eC
94Ni4rrUukMNjUhsiL1pDkRlF72OWlds8jpNY++F6YWtQjqmbPWjh1osNckjIg==
;
/
select obj01('obj01') from dual;

select 'case: obj02' from dual;
CREATE or replace EDITIONABLE TYPE obj02 WRAPPED
136
BXgBxtZN6qa2SgUcgFVIN3rPg/n3lnOl5s96xY1eQ+LaXPr7l+Zk3hxih3ripM96c+GlY/tOWu7I
F45EZcP+0omOsNOAjlT+C8TVFNY4xB0IZWCMw1P+yIlpC2VF/GUstAgMhg==
;
/
;
/*
CREATE or replace EDITIONABLE TYPE BODY obj02 AS
  MEMBER FUNCTION get_age RETURN NUMBER IS
  BEGIN
    RETURN a;
  END get_age;
END;
/
*/
select obj02('obj02') from dual;

select 'case: obj03' from dual;
CREATE or replace NONEDITIONABLE TYPE obj03 WRAPPED
136
BXgBrSjet7RwswscgFVIN3rPg/n3lnOlxc96xY1eQ+LaXPr7l+Zk3hxih3ripM96c+GlY/tOWu7I
F45EZcP+0omOsNOAjlT+C8TVFNY4xB0IZWCMw1P+yIlpC2VF/GUstPcMBA==
;
/
;
/*
--#CREATE or replace NONEDITIONABLE TYPE BODY obj03 AS
--#  MEMBER FUNCTION get_age RETURN NUMBER IS
--#  BEGIN
--#    RETURN a;
--#  END get_age;
--#END;
--#/
*/
select obj03('obj03') from dual;

select 'case: obj04' from dual;
CREATE or replace NONEDITIONABLE TYPE obj04 WRAPPED
144
C3YB5yDv8tNnoIaugFVIN3rPg/n3lnOl+t7LM3X/g896xY1eQ+LaXPr7l+Zk3hxih3ripM96c+Gl
Y/tOWu7IF45EZcP+0omOsNOAjlT+C8TVFNY4xB0IZWCMw1P+yIlpC2VF/GUsSkqUJQ==
;
/
;
select obj04('obj04') from dual;

select 'case: obj05' from dual;
--error 00600
drop type obj05 force;
CREATE TYPE obj05 WRAPPED
144
uIIBFQTbJ22n4i06gFVIN3rPg/n3lnOlg896xY1eQ+LaXPr7l+Zk+YieBYNih3ripM96c+GlY/tO
WuyKOMQdyImORGXDHoAHrG3VaSag6oc5xCXq5L470Pje6O+83mbvyhwBOGOULMpoIQ0=
;
/
CREATE OR REPLACE TYPE BODY obj05 WRAPPED
148
xWMBrd7JPImHwIzHgFVIN3rPg89DXjcc+feWc6WDz3py5mSN2i3a4rrmz0gth1yh3PYN3vdMN/eC
94Ni4rrUukMNjUhsiL1pDkRlF72OWlds8jrCO/9qW2QZY2T7LvFsOFZqSPkZcq0s1SJbbA==
;
/
select obj05(1,1) from dual;

select 'case: obj06' from dual;
--error 0,00600
drop type obj06 force;
create or replace type obj06 WRAPPED
172
uJIBiJ9sJ70bai1qgFVjkIjsfqV6bPawsiZA74kV/VEvgVKgqGqjoOJtNTMdLeWmjSApifpJWReX
xWuldxX5srks+4xGJEgKd+/C79VxV/n+R8iHMcVzpGUZyuCuSGcFaF50nH2BnIKA5aCmvoLilJuv
Wyjllr1hmtMzrAFhpw==
;
/
create or replace type body obj06 WRAPPED
272
BZUBNHKmGAxbUeBLgFXbEfPeg96W9xEc+feWc6UN+fNy5mT5BaYFdUzm5+eW9/8F/kx1g973TP6I
nv4FLuelHPmeDfkzTPr522wRsf6ladQO7cHttU/BaF2nIH6M5pKI5mPgSopHOLVyWldsbHlQxXTs
N2DjPtlQ8rUIQWiczo8+2bSzx8+Tbmbe/WY5Wz7fVICOZTugqGp9xWrmkifdrM1CgdjZY/NuQLeF
Atlw6WNjN2BjY+lEbWBQkPA21HMdYzOyakiCLCx64AUi
;
/
select obj06(1,1,null) from dual;

select 'case: obj07' from dual;
create or replace type obj07 WRAPPED
308
3CwB4pxkFC87iHICzFqAVST3wJm5pT5G2AyH3U/vrS/c+w1t63ZqZGXNkCsxKh3GapDbofYUN7mK
mUuBfcYasDzSQyBuaayt/X5NcXudt3wcPS2raNclijBWT0rCK3+mCavGamH9FSqy9bopafw1YCe2
dABSwRYei7J1QY3bJcQB7rdDbisfDLxvINGzMdKhjHstONoFieU7VCjTauUWYPm9WRBiH/yc7y80
017DnPNTHoLrEBGvF5bWsr8AfoLBCgB15jRMJLprGxHhrsiq8Yu/c5Rqn52j9Qm4sTAxjukiWt/s
x1s=
;
/
create or replace type body obj07 WRAPPED
448
zGQB/XN1mqqBI18CzK2AVbt2P/+cpaffS87uh3EgcDqa8t6TeErTT1jzeqzQXekU0jIOvzKUjQwa
qOoUdHt0gseC7C/KCZsD/wT4WyAcpqRiBS2f9izavFr7YDXm3RzGrZpE/yg/jW9L3sqANWfyz+Cd
znbweHkukt5IaWs/uB9sHXT0ZUDXOtbp4RUWoJu6gpy0jdynB1bw1zrW0nQgyekF/MWScA3ROWAy
JNYg3myEHVbgjRMIqe1SdCFM9he+0ffGKK71Cj3qoGeEsiCwpPZWhVeB4k6f8YlMeDE/FWXOh4K9
btUJ19PTYXqbSvtAL7nIXK/qFh2Z+fONkzp0YAjthw7XMnu0+P9hmOczWCrBTcLnq+xXLYGw7r82
gWEKcUCd0RzuV8EdR7zXOtUz2QY9n28U0spGVIgXx+J5uwfEBPRwcdX6xnuo8M8HEA==
;
/
declare
  var1 obj07;
  var2 obj07;
begin
  var1 := obj07(1,'obj07',sysdate);
  var2 := obj07(2,'obj07',sysdate);
  
  dbms_output.put_line(var1.t1 || var1.t2);
  --dbms_output.put_line(var1.GET_GRADE());

  IF var1>var2 THEN
    DBMS_OUTPUT.PUT_LINE(var1.t1);
  ELSIF var1<var2 THEN
    DBMS_OUTPUT.PUT_LINE(var2.t1);
  ELSE
    DBMS_OUTPUT.PUT_LINE('EQUAL');
  END IF;
end;
/


