--#####################################
--2.1、WRAP CLI 工具
--待加密的 PL/SQL 对象是异常场景
--#####################################
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

--case 2 语法不完整
create package pkg7 WRAPPED
68
HNcBy4vxiWqVDffWgFXb3oK2/4L3g+f5/1GF4YVSdZV1BfNa4lC5wew3soQsrQJ69A==
;
/

create or replace package as WRAPPED
80
kjgBbVE9T0GDsdlMgFXb3oK2/4L3g97zcuGFUnWVdQXz0IP5M+bndUz50IL/g+cYltZBxBziWiwm
Jied
;
/

create or replace package body as WRAPPED
84
xuMBwkfU0u1GOiMzgFXb3oK2/4L3g96W9xEc3vNy4YVSdZV1BfPQg/kz5ud1TPnQgv+D5xHzGKCX
rSwYhD5x
;
/

create or replace type body as WRAPPED
176
YtcBjpw63c4YPYusgFXbEfPeg96W9xEc3vNy4YVS4S4FlXUF89CD+TPm53VM+dCC/4Pn3oK2/4L3
bJbg2/2zg+f5/3FqAe9qSb+BIu0cIB05C6bm4Nb699bbGaN/JaOo+syoYe2oyZ5Oovl4HEz/PP/L
E7CVYcTSRS+ds0gslqFjdw==
;
/

create package "pkg10" WRAPPED
104
P6IBp+sVmvaNROzRgFXb3oK2/4L3g2Qd+f9RRWTm+dBOQsB18+OxM+P3Tlb3gsDxUTFjUXPGg7RC
OULiZIaInv6VBfH79a0sXQg+eQ==
;
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

create procedure proc15 WRAPPED
88
5+kBMQMDyJJyUbmlgFXb55b3/wX+THWD5ywyegUMQ7Sn7uZI4pKzDQyStAyL5kIx2KdDdYfiNgzm
2OY2ZSys8D6e
;
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

create package " WRAPPED
172
pKoBEbINhH1FhkYC61qAVYGeiOwYbacse0bE7x/bsZx19CLWYVvtSsPQ4wKRdFD4e5B+Lk2xRcLj
Ei8vGREn3GOA/SyirIjuAQmYrHWytTiN3Gc0262MNjWPfw32J5gL9+QekLruPRx6GWzPXAE6kPfk
skUwRQ2wM/SJGj8wZsg=
;
/

create package /**/pkg30 WRAPPED
76
Rf8BUiUMgjGo/KSVgFXb3oK2/4L3g/tW4eEz+f9RxpfhhVJ1lXUF89CD5xiWHhS6E+zSLNDJuoc=
;
/

create package "pkg32" WRAPPED
52
MFEBj2YazSLxzBsMgFXb3oK2/4L3g2Qd+f9Rxubmca1slSyIlWzZ
;
/

create package "pkg33" WRAPPED
56
MFEBsAPbe8+wTsQMgFXb3oK2/4L3g2Qd+f9RxsXmZGRZWZetLHVdbHY=
;
/

create package "pkg34" WRAPPED
60
STgBu8Z9N875hQ8ggFXb3oK2/4L3g2Qd+f9RxvrmZN7zxWRZWZetLK/Rw2A=
;
/

create package "pkg35" WRAPPED
60
tXEBHtJZUkestOchgFXb3oK2/4L3g2Qd+f9RxoPmZN7zxWRxrWyVLJlcwy4=
;
/

--create package "pkg36"  as /*
--/
--
create package "pkg37" WRAPPED
60
STgBZpM5B2wW02sggFXb3oK2/4L3g2Qd+f9RxlnmZN7zxfsug5etLK+cw7s=
;
/

create package "pkg38" WRAPPED
60
STgBtPIUs3+MYtMggFXb3oK2/4L3g2Qd+f9RdGTmZN7zxWTm5petLK8Twxg=
;
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







