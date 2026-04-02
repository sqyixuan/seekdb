#owner: webber.wb
#owner group: 技术部-SQL-存储过程
#description: OB-oracle到OB-oracle PL dblink 复杂数据类型测试
#tags: pl
#author:whh01047778

--source mysql_test/test_suite/ob_ob_dblink2_oracle_pl_sqlqa/include/output_print_enable.inc

--echo #-----------------------------------------------------------------------------
--echo # 远程OB端 dblink_tenant1租户下
--echo #-----------------------------------------------------------------------------

--error 0,00942
drop table ori_tab1;
CREATE TABLE ori_tab1 (id int, c1 char(2000) DEFAULT 'ori_tab1',c2 varchar2(4000) DEFAULT '远程OB端');
INSERT INTO ori_tab1 VALUES(1,DEFAULT,DEFAULT);

--echo #### case1
#record类型-出参
delimiter /;
create or replace package remo_pkg1 is
  type rd_ty is record (
    c1 number(8,3),
    c2 date,
    c3 varchar2(100));
  procedure p0;
  procedure p1(a out rd_ty,b in out rd_ty);
  function f1(a in out rd_ty,b out rd_ty) return rd_ty;
end remo_pkg1;
/

create or replace package body remo_pkg1 is
  procedure p0 is
  begin
    null;
  end;
  procedure p1(a out rd_ty,b in out rd_ty) is
  begin
   a := b;
   b.c1 := 1.23456789;
   b.c2 := to_date('2024-01-01', 'yyyy-mm-dd');
   b.c3 := upper(a.c3);  
  end;
  function f1(a in out rd_ty,b out rd_ty) return rd_ty is
    rd_v1 rd_ty;
  begin
    b := a;
    rd_v1.c1 := 1.56789012;
    rd_v1.c2 := to_date('2024-01-01', 'yyyy-mm-dd');
    rd_v1.c3 := lower('varchar2');
    a := rd_v1;
    return rd_v1;
  end f1;
end remo_pkg1;
/
delimiter ;/
call remo_pkg1.p0();

--echo #### case2
#record类型-参数个数 20个
delimiter /;
create or replace package remo_pkg2 is
  type rd_ty is record (
    c1 number(8,3),
    c2 date,
    c3 varchar2(100));
  procedure p0;
  procedure p1(a1 in rd_ty,a2 in rd_ty,a3 in rd_ty,a4 in rd_ty,a5 in rd_ty,a6 in rd_ty,a7 in rd_ty,a8 in rd_ty,a9 in rd_ty,a10 in rd_ty,a11 in rd_ty,a12 in rd_ty,a13 in rd_ty,a14 in rd_ty,a15 in rd_ty,a16 in rd_ty,a17 in rd_ty,a18 in rd_ty,a19 in rd_ty,a20 in rd_ty);
  function f1(a1 in out rd_ty,a2 in out rd_ty,a3 in out rd_ty,a4 in out rd_ty,a5 in out rd_ty,a6 in out rd_ty,a7 in out rd_ty,a8 in out rd_ty,a9 in out rd_ty,a10 in out rd_ty,a11 in out rd_ty,a12 in out rd_ty,a13 in out rd_ty,a14 in out rd_ty,a15 in out rd_ty,a16 in out rd_ty,a17 in out rd_ty,a18 in out rd_ty,a19 in out rd_ty,a20 in out rd_ty) return rd_ty;
  function f2(a1 out rd_ty,a2 out rd_ty,a3 out rd_ty,a4 out rd_ty,a5 out rd_ty,a6 out rd_ty,a7 out rd_ty,a8 out rd_ty,a9 out rd_ty,a10 out rd_ty,a11 out rd_ty,a12 out rd_ty,a13 out rd_ty,a14 out rd_ty,a15 out rd_ty,a16 out rd_ty,a17 out rd_ty,a18 out rd_ty,a19 out rd_ty,a20 out rd_ty) return rd_ty;
end remo_pkg2;
/

create or replace package body remo_pkg2 is
  procedure p0 is
  begin
    null;
  end;
  procedure p1(a1 in rd_ty,a2 in rd_ty,a3 in rd_ty,a4 in rd_ty,a5 in rd_ty,a6 in rd_ty,a7 in rd_ty,a8 in rd_ty,a9 in rd_ty,a10 in rd_ty,a11 in rd_ty,a12 in rd_ty,a13 in rd_ty,a14 in rd_ty,a15 in rd_ty,a16 in rd_ty,a17 in rd_ty,a18 in rd_ty,a19 in rd_ty,a20 in rd_ty) is
  begin
   null;  
  end;
  function f1(a1 in out rd_ty,a2 in out rd_ty,a3 in out rd_ty,a4 in out rd_ty,a5 in out rd_ty,a6 in out rd_ty,a7 in out rd_ty,a8 in out rd_ty,a9 in out rd_ty,a10 in out rd_ty,a11 in out rd_ty,a12 in out rd_ty,a13 in out rd_ty,a14 in out rd_ty,a15 in out rd_ty,a16 in out rd_ty,a17 in out rd_ty,a18 in out rd_ty,a19 in out rd_ty,a20 in out rd_ty) return rd_ty is
    rd_v1 rd_ty;
  begin
    null;
    return rd_v1;
  end f1;
  function f2(a1 out rd_ty,a2 out rd_ty,a3 out rd_ty,a4 out rd_ty,a5 out rd_ty,a6 out rd_ty,a7 out rd_ty,a8 out rd_ty,a9 out rd_ty,a10 out rd_ty,a11 out rd_ty,a12 out rd_ty,a13 out rd_ty,a14 out rd_ty,a15 out rd_ty,a16 out rd_ty,a17 out rd_ty,a18 out rd_ty,a19 out rd_ty,a20 out rd_ty) return rd_ty is
    rd_v1 rd_ty;
  begin
    null;
    return rd_v1;
  end f2;
end remo_pkg2;
/
delimiter ;/
call remo_pkg2.p0();

--echo #### case3
#嵌套表类型 array(char(n))-出参
delimiter /;
create or replace package remo_pkg3 is
  TYPE tb_ty1 IS TABLE OF char(150);
  TYPE tb_ty2 IS TABLE OF varchar2(200);
  procedure p0;
  procedure p1(a out tb_ty1,b in out tb_ty2);
  function f1(a in out tb_ty1,b in out tb_ty2) return tb_ty1;
  function f2(a out tb_ty1,b out tb_ty2) return tb_ty2;
end;
/

create or replace package body remo_pkg3 is
  procedure p0 is
  begin
    null;
  end;
  procedure p1(a out tb_ty1,b in out tb_ty2) is
    v1 tb_ty1 := tb_ty1(null,'','tb_ty1');
    v2 tb_ty2 := tb_ty2(null,'','tb_ty2');
  begin
    select '' bulk collect into a from dual;
    select 'null' bulk collect into b from dual;
    a.extend;
    a(1):=v1(1);
    b.extend(3);
    b(1):=v2(1);
    b(2):=v2(2);
    b(3):=v2(3);
  end;
  function f1(a in out tb_ty1,b in out tb_ty2) return tb_ty1 is
    v1 tb_ty1 := tb_ty1(null,'','tb_ty1');
    v2 tb_ty2 := tb_ty2(null,'','tb_ty2');
  begin
    a := v1;
    b := v2;
    return v1;
  end;
  function f2(a out tb_ty1,b out tb_ty2) return tb_ty2 is
    v1 tb_ty1 := tb_ty1(null,'','tb_ty1');
    v2 tb_ty2 := tb_ty2(null,'','tb_ty2');
  begin
    a := tb_ty1(null);
    a.extend;
    a(1):='out tb_ty1';
    b := tb_ty2(null);
    b.extend;
    b(1):='out tb_ty2';
    return v2;
  end;
end;
/
delimiter ;/
call remo_pkg3.p0();

--echo #### case4
#嵌套表类型array(number)-出参
delimiter /;
create or replace package remo_pkg4 is
  TYPE tb_ty1 IS TABLE OF number(8,3);
  TYPE tb_ty2 IS TABLE OF BINARY_DOUBLE;
  TYPE tb_ty3 IS TABLE OF binary_float;
  procedure p0;
  procedure p1(a out tb_ty1,b out tb_ty2,c out tb_ty3);
  function f1(a in out tb_ty1,b in out tb_ty2,c in out tb_ty3,d number default 1) return tb_ty1;
  function f2(a out tb_ty1,b in out tb_ty2,c in out tb_ty3,d float default 1.4) return tb_ty2;
  function f3(a out tb_ty1,b out tb_ty2,c in out tb_ty3 ,d binary_float default 1.5) return tb_ty3;
end;
/

create or replace package body remo_pkg4 is
  procedure p0 is
  begin
    null;
  end;
  procedure p1(a out tb_ty1,b out tb_ty2,c out tb_ty3) is
    v1 tb_ty1 := tb_ty1(null,1.234567);
    v2 tb_ty2 := tb_ty2(null,1.567895);
    v3 tb_ty3 := tb_ty3(null,1.789456);
  begin
    select 0 bulk collect into a from dual;
    select 0 bulk collect into b from dual;
    select 0 bulk collect into c from dual;
    a.extend(2);
    a(1):=v1(1);
    a(2):=v1(2);
    b.extend(2);
    b(1):=v2(1)+1;
    b(2):=v2(2)+1;
    c.extend(2);
    c(1):=v3(1)*2;
    c(2):=v3(2)*2;
  end;
  function f1(a in out tb_ty1,b in out tb_ty2,c in out tb_ty3,d number default 1) return tb_ty1 is
    v1 tb_ty1 := tb_ty1(null,1.234567);
    v2 tb_ty2 := tb_ty2(null,1.567895);
    v3 tb_ty3 := tb_ty3(null,1.789456);
  begin
    a := v1;
    b := v2;
    c := v3;
    c.extend;
    c(3):=d;
    return v1;
  end;
  function f2(a out tb_ty1,b in out tb_ty2,c in out tb_ty3,d float default 1.4) return tb_ty2 is
    v1 tb_ty1 := tb_ty1(null,1.234567);
    v2 tb_ty2 := tb_ty2(null,1.567895);
    v3 tb_ty3 := tb_ty3(null,1.789456);
  begin
    a := tb_ty1(d);
    b := tb_ty2(d);
    c := tb_ty3(d);
    a.extend;
    a(1):=v1(1)+v1(2);
    b.extend;
    b(1):=v2(1)+v2(2);
    c.extend;
    c(1):=v3(1)+v3(2);
    return v2;
  end;
  function f3(a out tb_ty1,b out tb_ty2,c in out tb_ty3 ,d binary_float default 1.5) return tb_ty3 is
    v1 tb_ty1 := tb_ty1(1.234567);
    v2 tb_ty2 := tb_ty2(1.567895);
    v3 tb_ty3 := tb_ty3(1.789456);
  begin
    a := tb_ty1(d);
    b := tb_ty2(d);
    c := tb_ty3(d);
    a.extend;
    a(1):=v1(1);
    b.extend;
    b(1):=v2(1);
    c.extend;
    c(1):=v3(1);
    return v3;
  end;  
end;
/
delimiter ;/
call remo_pkg4.p0();

--echo #### case5
#嵌套表类型 array(date)-出参
delimiter /;
create or replace package remo_pkg5 is
  TYPE tb_ty1 IS TABLE OF date;
  a tb_ty1 := tb_ty1(to_date('2024-08-07','yyyy-mm-dd'));
  procedure p0;
  procedure p1(p1 out tb_ty1,p2 date default null);
  function f1(p1 in out tb_ty1,p2 out tb_ty1) return tb_ty1;
end;
/

create or replace package body remo_pkg5 is
  procedure p0 is
  begin
    null;
  end;
  procedure p1(p1 out tb_ty1,p2 date default null) is
  begin
    p1 := tb_ty1(p2);
  end;
  function f1(p1 in out tb_ty1,p2 out tb_ty1) return tb_ty1 is
  begin
    p2 := a;
    p1 := tb_ty1(a(1));
    p1.extend;
    p1(2) := Next_day(a(1),'MONDAY');
    return p2;
  end;
end;
/
delimiter ;/
call remo_pkg5.p0();

--echo #### case6
#record类型-参数个数 30个
delimiter /;
create or replace package remo_pkg6 is
  TYPE tb_ty1 IS TABLE OF varchar2(100);
  procedure p0;
  procedure p1(a1 in tb_ty1,a2 in tb_ty1,a3 in tb_ty1,a4 in tb_ty1,a5 in tb_ty1,a6 in tb_ty1,a7 in tb_ty1,a8 in tb_ty1,a9 in tb_ty1,a10 in tb_ty1,a11 in tb_ty1,a12 in tb_ty1,a13 in tb_ty1,a14 in tb_ty1,a15 in tb_ty1,a16 in tb_ty1,a17 in tb_ty1,a18 in tb_ty1,a19 in tb_ty1,a20 in tb_ty1,a21 in tb_ty1,a22 in tb_ty1,a23 in tb_ty1,a24 in tb_ty1,a25 in tb_ty1,a26 in tb_ty1,a27 in tb_ty1,a28 in tb_ty1,a29 in tb_ty1,a30 in tb_ty1);
  function f1(a1 in out tb_ty1,a2 in out tb_ty1,a3 in out tb_ty1,a4 in out tb_ty1,a5 in out tb_ty1,a6 in out tb_ty1,a7 in out tb_ty1,a8 in out tb_ty1,a9 in out tb_ty1,a10 in out tb_ty1,a11 in out tb_ty1,a12 in out tb_ty1,a13 in out tb_ty1,a14 in out tb_ty1,a15 in out tb_ty1,a16 in out tb_ty1,a17 in out tb_ty1,a18 in out tb_ty1,a19 in out tb_ty1,a20 in out tb_ty1,a21 in out tb_ty1,a22 in out tb_ty1,a23 in out tb_ty1,a24 in out tb_ty1,a25 in out tb_ty1,a26 in out tb_ty1,a27 in out tb_ty1,a28 in out tb_ty1,a29 in out tb_ty1,a30 in out tb_ty1) return tb_ty1;
  function f2(a1 out tb_ty1,a2 out tb_ty1,a3 out tb_ty1,a4 out tb_ty1,a5 out tb_ty1,a6 out tb_ty1,a7 out tb_ty1,a8 out tb_ty1,a9 out tb_ty1,a10 out tb_ty1,a11 out tb_ty1,a12 out tb_ty1,a13 out tb_ty1,a14 out tb_ty1,a15 out tb_ty1,a16 out tb_ty1,a17 out tb_ty1,a18 out tb_ty1,a19 out tb_ty1,a20 out tb_ty1,a21 out tb_ty1,a22 out tb_ty1,a23 out tb_ty1,a24 out tb_ty1,a25 out tb_ty1,a26 out tb_ty1,a27 out tb_ty1,a28 out tb_ty1,a29 out tb_ty1,a30 out tb_ty1) return tb_ty1;
end remo_pkg6;
/

create or replace package body remo_pkg6 is
  procedure p0 is
  begin
    null;
  end;
  procedure p1(a1 in tb_ty1,a2 in tb_ty1,a3 in tb_ty1,a4 in tb_ty1,a5 in tb_ty1,a6 in tb_ty1,a7 in tb_ty1,a8 in tb_ty1,a9 in tb_ty1,a10 in tb_ty1,a11 in tb_ty1,a12 in tb_ty1,a13 in tb_ty1,a14 in tb_ty1,a15 in tb_ty1,a16 in tb_ty1,a17 in tb_ty1,a18 in tb_ty1,a19 in tb_ty1,a20 in tb_ty1,a21 in tb_ty1,a22 in tb_ty1,a23 in tb_ty1,a24 in tb_ty1,a25 in tb_ty1,a26 in tb_ty1,a27 in tb_ty1,a28 in tb_ty1,a29 in tb_ty1,a30 in tb_ty1) is
  begin
   null;  
  end;
  function f1(a1 in out tb_ty1,a2 in out tb_ty1,a3 in out tb_ty1,a4 in out tb_ty1,a5 in out tb_ty1,a6 in out tb_ty1,a7 in out tb_ty1,a8 in out tb_ty1,a9 in out tb_ty1,a10 in out tb_ty1,a11 in out tb_ty1,a12 in out tb_ty1,a13 in out tb_ty1,a14 in out tb_ty1,a15 in out tb_ty1,a16 in out tb_ty1,a17 in out tb_ty1,a18 in out tb_ty1,a19 in out tb_ty1,a20 in out tb_ty1,a21 in out tb_ty1,a22 in out tb_ty1,a23 in out tb_ty1,a24 in out tb_ty1,a25 in out tb_ty1,a26 in out tb_ty1,a27 in out tb_ty1,a28 in out tb_ty1,a29 in out tb_ty1,a30 in out tb_ty1) return tb_ty1 is
  begin
    return a1;
  end f1;
  function f2(a1 out tb_ty1,a2 out tb_ty1,a3 out tb_ty1,a4 out tb_ty1,a5 out tb_ty1,a6 out tb_ty1,a7 out tb_ty1,a8 out tb_ty1,a9 out tb_ty1,a10 out tb_ty1,a11 out tb_ty1,a12 out tb_ty1,a13 out tb_ty1,a14 out tb_ty1,a15 out tb_ty1,a16 out tb_ty1,a17 out tb_ty1,a18 out tb_ty1,a19 out tb_ty1,a20 out tb_ty1,a21 out tb_ty1,a22 out tb_ty1,a23 out tb_ty1,a24 out tb_ty1,a25 out tb_ty1,a26 out tb_ty1,a27 out tb_ty1,a28 out tb_ty1,a29 out tb_ty1,a30 out tb_ty1) return tb_ty1 is
  begin
    return a1;
  end f2;
end remo_pkg6;
/
delimiter ;/
call remo_pkg6.p0();

--echo #### case7
#包中方法重载
delimiter /;
create or replace package remo_pkg7 as
type rd_ty1 is record(c0 int);
TYPE tb_ty2 IS TABLE OF char(100);
TYPE tb_rd_ty3 IS TABLE OF rd_ty1;
a1 rd_ty1;
a2 tb_ty2;
a3 tb_rd_ty3;
a4 varchar2(200);
procedure p0;
procedure p1;
procedure p1(a rd_ty1);
procedure p1(a tb_ty2);
procedure p1(a tb_rd_ty3);
procedure p1(a rd_ty1,b tb_ty2,c tb_rd_ty3);
function f1 return varchar2;
end;
/

create or replace package body remo_pkg7 as
procedure p0 is
begin
  null;
end;
procedure p1 is
begin
a4 := 'p1';
end;
procedure p1(a rd_ty1) is
begin
a4 := 'p1(a rd_ty1)';
end;
procedure p1(a tb_ty2) is
begin
a4 := 'p1(a tb_ty2)';
end;
procedure p1(a tb_rd_ty3) is
begin
a4 := 'p1(a tb_rd_ty3)';
end;
procedure p1(a rd_ty1,b tb_ty2,c tb_rd_ty3) is
begin
a4 := 'p1(a rd_ty1,b tb_ty2,c tb_rd_ty3)';
end;
function f1 return varchar2
is
begin
return a4;
end;
end;
/
delimiter ;/
call remo_pkg7.p0();

CREATE OR REPLACE SYNONYM syn_remo_pkg7 for remo_pkg7;

--source mysql_test/test_suite/ob_ob_dblink2_oracle_pl_sqlqa/include/output_print_disable.inc
