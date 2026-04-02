--echo #-----------------------------------------------------------------------------
--echo # 测试场景：1、加密工具 plsql 结束符
--echo #-----------------------------------------------------------------------------

--echo # case1 结束符前有空行
create or replace function f0 WRAPPED
88
mbIBy7qbSgryhL8xgFWI26aIlcCe8Q3eMWTndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI66HR3JH
o4fCWiz1GVBZ
;
/

select f0() from dual;

--echo # case2 结束符前有空行
create or replace function f0 WRAPPED
92
olkBh6vFaQxWwAtFgFWI26aIlcCe8Q3eMWTndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI66HR3JH
o4fCwlospJJQ8A==
;
/

select f0() from dual;

--echo # case3 结束符前有注释
create or replace function f0 WRAPPED
124
4ZYBz9TZbFCOWLqCgFWI26aIlcCe8Q3eMWTndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI66HR3JH
o4fCXelvyS9TNKJT8VbzvUA0VCr/hTCm25p0XsLCWiwQj23V
;
/

select f0() from dual;


--echo # case4 结束符前有注释
create or replace function f0 WRAPPED
132
wo0BtWHbH4FA3o3rgFWI26aIlcCe8Q3eMWTndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI66HR3JH
o4fCXelvyS9TNKJT8VbzvUA0VCr/hTCm25p0XmhSi6VSUq0sDjallQ==
;
/

select f0() from dual;

--echo # case5 结束符后有空行，注释
create or replace function f0 WRAPPED
88
mbIBy7qbSgryhL8xgFWI26aIlcCe8Q3eMWTndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI66HR3JH
o4fCWiz1GVBZ
;
/

--结束符后有空行，注释

select f0() from dual;


--echo # case6 多个结束符
create or replace function f1 WRAPPED
88
Kh4BmJ5oOmfeFABzgFWI26aIlcCe8Q3eMRzndUz+TJYN+dumBXVM5vnzcnV1BfGeFWnsI3CHR3JH
o4daLELuUPc=
;
//

--结束符有空行，注释

//

/


select f1() from dual;

--echo # case7 结束符前有空格
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


--echo # case8 结束符后有空格
create or replace type obj1 WRAPPED
84
Rf8BaZb3CvUPrheVgFXbEfPeg/n3lnMc3sszdf+D3vPDoeaziEWLuqdDdYez5rTdi7oTv8Nq44Qs
vjonig==
;
//              


--echo # case9 结束符后有注释
create or replace package pkg3 WRAPPED
224
Ck8BbFTY5eZnncADgFWD8RNcjG16bPtGxMiIgoT50TBDpcN1Y4ZK3teYx6igAGzbIXxUVyF6TOu/
jCjpr7+7tns+07Dk8pPR6ExyTGMLZ8TlynYf7YsZ234EXvCHGnvZYrwrMvU3R2dpgL81iJp5u3tL
/pEXyG7+5PgVbfjhlbGIaF5liqwTF8vA1V0maIBUJCN4KfEwlbKFUI4z2TyAh6ktq5xdHBM=
;
//

create or replace package body pkg3 WRAPPED
616
r48BVAqnyfeW1csCTGyAVUazy3V+/adg9tincadqO5E64ctNR2lOHwjtdxf1c/wZJOvyT+YBiBOn
hNSV7lJKsomsomKO0UaXllnPi3SuKoiEp6sAT9PNVJuohXoxNmD61q/mVCCjWNlsfp2QDbR4WtZ7
txQew2/PKdSG04LmXoQ6D0xnHarzjTFSrMYGSd7UYBbAkj2sImrDoItsfRlZQQvrbKJY+NlYthU6
iVCZxHsEH8jr+IojvzsOnKuGSNGz1+6tBXVoQ+3WcPPY3k+qnpZBT6ugzqALe5AIsimGSZ6Ji+sZ
K+XVkJBHC5Cw+kM8xtm3BlYEf4Xfd+8XlcHz2RDdEvk+Il3duD9K51+K41qJDLd6BK9sYMdJnA+R
zUKdSy3Oz6nGgM4D0S9/DCKRokKvf3EikT99Nma39EscEvypIqvNUUiSfpfDTLOQF8PPRVOVgT9K
R79oO6N1rGGEkolsWghL2EYPd92GjXfqjJLpNcJo4Mtmxjwi4CZ9ZLKszLCl+W3L+724S8kiurjW
yRFSi/XVVU9RI1icUoL1SSo9qLy/Y+OY8MCK7IeCNnwSMregu+nx3qTxlXz4i5uvqPgTQkLRDM1I
TOiqWQ==
;

