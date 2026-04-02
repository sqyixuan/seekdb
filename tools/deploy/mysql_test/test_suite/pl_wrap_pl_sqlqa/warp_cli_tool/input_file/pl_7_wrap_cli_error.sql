--#####################################
--2.1、WRAP CLI 工具
--待加密的 PL/SQL 对象是异常场景
--#####################################
--case 1 正常语法
create or replace package pkg1 as end;
/

create or replace package body pkg1 as end;
/

create or replace type typ3 as object (a number);
/

create or replace type body typ4 as end;
/

create or replace function fun5 return number is begin return 1; end;
/

create or replace procedure proc6 is begin null; end;
/

--case 2 语法不完整
create package pkg7
/

create package pkg8 as
/

create or replace package as
/

create or replace package body
/

create or replace package body as
/

create or replace type body
/

create or replace type body as
/

--create or replace package
--/

--create replace package pkg as end;
--/

--create replace;
--/

create package pkg9 la.,jas,/s/a//adoo657235&^(^*@#^alsugh)
/

create package "pkg10" la.,jas,/s/a//adoo657235&^(^*@#^alsugh)
/

--case 3 语法顺序
--create or replace body package pkg11 as end;
--/

--replace or create type typ12 as object (a number);
--/

--create replace or type body typ13 as null; end;
--/

create fun14 function return number is begin return 1; end;
/

create procedure proc15 or replace as begin null; end;
/

--case 4 词法错误
creates or replace package pkg16 as end;
/

create ors replace package pkg17 as end;
/

--create or replaces packagea pkg18 as end;
--/
--

create or replace packages pkg19 as end;
/

create o r replace packages pkg20 as end;
/

create and replace packages pkg21 as end;
/

create or replace pack_age pkg22 as end;
/

create or replace pack_age pkg23 ass end;
/

create package "pkg24 la.,jas,/s/a//adoo657235&^(^*@#^alsugh)
/

create package "pkg25la.,jas,/s/a//adoo657235&^(^*@#^alsugh)"
/

create package "pkg26la.,jas,/s/a//adoo657235&^(^*@#^alsugh)"''
/

create package "pkg27la.,jas,/s/a//adoo657235&^(^*@#^alsugh)"/**/
/

create package "pkg28la.,jas,/s/a//adoo657235&^(^*@#^alsugh)"/*
/

create package "pkg29@#$%^&*()_
/

create package /**/pkg30
/

create package pkg31/**/
/

create package "pkg32"'''' 
/

create package "pkg33"  '' 
/

create package "pkg34"  as '' 
/

create package "pkg35"  as '''' 
/

--create package "pkg36"  as /*
--/
--
create package "pkg37"  as -- 
/

create package "pkg38"  as "" 
/

--create package "pkg39"  as " 
--/


--case 5 WRAPPED DDL
--case 1 正常语法
create or replace package pkg1 WRAPPED
56
MFEBQohqIV3+gq0MgFXb3oK2/4L3g+f5/1Ec3vPF3qaIGXKtLOZlw40=
;
/

create or replace package body pkg1 WRAPPED
64
tXEBgGXKgaVPcRwhgFXb3oK2/4L3g96W9xEc5/n/URze88XepogZcq0sGxJiBw==
;
/

create or replace type typ3 WRAPPED
72
CvkBzb56QCGgO0ltgFXbEfPeg+cR86XF3vPF+feWdQWV+vveHPnbpgV1TOEScq0suPHsBw==
;
/

create or replace type body typ4 WRAPPED
60
DmYBoPSRfz9o/BCUgFXbEfPeg96W9xEc5xHzpfre88XepogZcq0sKDdx4Q==
;
/

create or replace function fun5 WRAPPED
88
olkB0lO27sLPG29FgFWI26aIlcCe8Q3e26Yxg+d1TP5Mlg3526YFdUzm+fPF3nUF8Z6c1COu2wty
R6OHWiwIWJJe
;
/

create or replace procedure proc6 WRAPPED
72
5B4BLotu8SXwAspPgFXb55b3/wX+THWD5ywynqdztAyL5kIx2KdDdYfiNgzm2OY2ZSzIEnrE
;
/






