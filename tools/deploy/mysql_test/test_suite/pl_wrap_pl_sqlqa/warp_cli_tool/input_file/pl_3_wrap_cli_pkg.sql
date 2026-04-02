CREATE TABLE EBANK_CREDIT_CARD (EBUSER_NO VARCHAR2(20) NOT NULL, CREDIT_NO VARCHAR2(32) NOT NULL, CREDIT_TYPE VARCHAR2(10) NOT NULL, CREDIT_ANAME VARCHAR2(40), CCY_TYPE VARCHAR2(3), IS_DEFL CHAR(1) NOT NULL, HANG_DATE VARCHAR2(10), HANG_TIME VARCHAR2(10), HANG_ORG VARCHAR2(8), HANG_TELER VARCHAR2(8), HANG_CHAN VARCHAR2(3), OPEN_ORG VARCHAR2(3), PMT_DUE_DATE VARCHAR2(20), BILLING_DATE VARCHAR2(20), PUSHFLAG VARCHAR2(1), CREDIT_CVN2 VARCHAR2(3), CREDIT_EXPIRED VARCHAR2(10), PAR VARCHAR2(32) NOT NULL, INIT_MARK VARCHAR2(2), CONSTRAINT PK_EBANK_CREDIT_CARD PRIMARY KEY (EBUSER_NO, CREDIT_NO));

CREATE TABLE ECERT_INFO (EBUSER_NO VARCHAR2(20) NOT NULL, OPER_ID VARCHAR2(20) NOT NULL, CERT_KEYID VARCHAR2(20) NOT NULL, APPLY_DATE VARCHAR2(10) NOT NULL, IS_USE CHAR(1) NOT NULL, USBKEY_NO VARCHAR2(30), PROF_DOWN_CODE VARCHAR2(50), OPER_CERT_STATE VARCHAR2(2) NOT NULL, STATE_UPD_DATE VARCHAR2(10) NOT NULL, CONSTRAINT PK_ECERT_INFO PRIMARY KEY (EBUSER_NO, OPER_ID, CERT_KEYID));

CREATE TABLE EUSER_INFO (EBUSER_NO VARCHAR2(20) NOT NULL, INCORP_NO VARCHAR2(10) NOT NULL, CIF_NO VARCHAR2(20), CUST_NAME VARCHAR2(150) NOT NULL, OPEN_CHAN VARCHAR2(3) NOT NULL, EBANK_OPEN_DATE VARCHAR2(10) NOT NULL, CIF_OPEN_DATE VARCHAR2(10), CIF_OPEN_ORG VARCHAR2(8), EBUSER_TYP VARCHAR2(2) NOT NULL, SIGN_ORG VARCHAR2(8) NOT NULL, SIGN_TELER VARCHAR2(8) NOT NULL, SIGN_RETELER VARCHAR2(8) NOT NULL, OPEN_CHANNEL VARCHAR2(10) NOT NULL, EBUSER_STAT VARCHAR2(2) NOT NULL, FREEZE_DATE VARCHAR2(10), FREEZE_RES VARCHAR2(2), PREM_INFO VARCHAR2(60), TMP_ISTRANS VARCHAR2(1), BLEVEL VARCHAR2(1), ELEVEL VARCHAR2(1), CONSTRAINT PK_EUSER_INFO PRIMARY KEY (EBUSER_NO));

CREATE INDEX IDX_EUSER_INFO_CIF_NO ON EUSER_INFO (CIF_NO);

CREATE TABLE E_ASSO_INFO (EBUSER_NO VARCHAR2(20) NOT NULL, ASSO_ID VARCHAR2(10) NOT NULL, ASSO_CATA VARCHAR2(2) NOT NULL, TRAN_WAY VARCHAR2(2) NOT NULL, ASSO_NAME VARCHAR2(150) NOT NULL, ASSO_ACCT VARCHAR2(32) NOT NULL, ASSO_BKNK_NAME VARCHAR2(210), ASSO_BANK_CODE VARCHAR2(12), SUPER_BANK_CODE VARCHAR2(12), CSHNM_BANK_CODE VARCHAR2(20), MOBILE VARCHAR2(12), IMG_FALG VARCHAR2(2), IMG VARCHAR2(100), USE_RATE INTEGER, ASSO_GR_ID VARCHAR2(10), UPD_OPER_ID VARCHAR2(10), CONSTRAINT PK_E_ASSO_INFO PRIMARY KEY (EBUSER_NO, ASSO_ID));

CREATE TABLE E_BASE_FINANCIAL_TR (TRAN_SEQ VARCHAR2(20) NOT NULL, EBUSER_NO VARCHAR2(20) NOT NULL, TRANS_TYPE VARCHAR2(20) NOT NULL, CURRENT_ACCT VARCHAR2(20), CURRENT_ACCT_NAME VARCHAR2(150), DEPOSIT_KIND VARCHAR2(2), REGULAR_ACCT VARCHAR2(20), REGULAT_ACCT_NAME VARCHAR2(150), VALUE_DATE VARCHAR2(18), EXPIRY_DATE VARCHAR2(18), TRAN_AMT NUMBER(18,4) NOT NULL, RATE NUMBER(18,4), AUTO_SAVE VARCHAR2(1), PRE_TIME VARCHAR2(20), DRAW_TIME VARCHAR2(20), SUBMIT_TIME VARCHAR2(20) NOT NULL, SEND_TIME VARCHAR2(20), HOST_TIME VARCHAR2(20), HOST_SEQ VARCHAR2(50), RET_CODE VARCHAR2(100), RET_MSG VARCHAR2(200), TRAN_STATE VARCHAR2(2), MENU_ID VARCHAR2(20) NOT NULL, SUBMIT_ID VARCHAR2(20), SUBMIT_NAME VARCHAR2(300), CONSTRAINT PK_E_BASE_FINANCIAL_TR PRIMARY KEY (TRAN_SEQ));

CREATE TABLE E_BATCH_TR (BATCH_SEQ_NO VARCHAR2(20) NOT NULL, EBUSER_NO VARCHAR2(30) NOT NULL, BUSI_TYPE VARCHAR2(2) NOT NULL, SUBMIT_ID VARCHAR2(20) NOT NULL, SUBMIT_NAME VARCHAR2(20), PAY_ACC VARCHAR2(20) NOT NULL, PAY_ACCNAME VARCHAR2(100) NOT NULL, PAY_ACCNOTE VARCHAR2(20), SUM_NUM INTEGER NOT NULL, SUM_AMT NUMBER(18,4) NOT NULL, FEE1 NUMBER(18,4), FEE2 NUMBER(18,4), SUBMIT_TIME VARCHAR2(20) NOT NULL, PRE_TIME VARCHAR2(20), SEND_TIME VARCHAR2(20), COMPLATE_TIME VARCHAR2(20), BANK_REMARK VARCHAR2(100), RET_CODE VARCHAR2(100), RET_MSG VARCHAR2(200), STATE VARCHAR2(2) NOT NULL, CURRENCY VARCHAR2(5), INPUT_FILENAME VARCHAR2(200), SUCCESS_NUM INTEGER, SUCCESS_AMT NUMBER(18,4), FAIL_NUM INTEGER, FAIL_AMT NUMBER(18,4), ERROR_MSG VARCHAR2(100), HOST_SEQ_NO VARCHAR2(50), HOST_BATCH_NO VARCHAR2(20), SEND_FILENAME VARCHAR2(200), RECEIVE_FILENAME VARCHAR2(20), TRAN_WAY VARCHAR2(2), PER_COUNT VARCHAR2(10), FILE_SIGN VARCHAR2(50), CONSTRAINT PK_E_BATCH_TR PRIMARY KEY (BATCH_SEQ_NO));

CREATE TABLE USER_CHANNEL (EBUSER_NO VARCHAR2(20) NOT NULL, CHAN_TYP VARCHAR2(3) NOT NULL, CHANNEL_ID VARCHAR2(30) NOT NULL, OPEN_WAY VARCHAR2(2) NOT NULL, OPEN_DATE VARCHAR2(10) NOT NULL, CLOSE_DATE VARCHAR2(10), FREEZE_DATE VARCHAR2(10), FREEZE_RES VARCHAR2(2), SIGN_ORG VARCHAR2(8) NOT NULL, SIGN_TELER VARCHAR2(8) NOT NULL, SIGN_RETELER VARCHAR2(8) NOT NULL, STAT VARCHAR2(2) NOT NULL, GUIDING_NO VARCHAR2(30), SIGN_MSG VARCHAR2(40), FUNC_GR_ID VARCHAR2(12), IS_UPLOAD VARCHAR2(1), EDIT_NUM VARCHAR2(2), IS_AGREEMENT VARCHAR2(2), CONSTRAINT PK_USER_CHANNEL PRIMARY KEY (EBUSER_NO, CHAN_TYP, CHANNEL_ID));

CREATE INDEX N_USER_CHANNEL_IDX1 ON USER_CHANNEL (EBUSER_NO, CHAN_TYP, GUIDING_NO);

CREATE TABLE P_SEC_AUTH (EBUSER_NO VARCHAR2(20) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, SEC_VALUE VARCHAR2(60) NOT NULL, SEC_DATE VARCHAR2(10) NOT NULL, SEC_STATE VARCHAR2(2) NOT NULL, CHAN_TYP VARCHAR2(3) NOT NULL, CONSTRAINT PK_P_SEC_AUTH PRIMARY KEY (EBUSER_NO, SEC_WAY, CHAN_TYP));

CREATE TABLE CUST_CH_AU_SELF_LIM (EBUSER_NO VARCHAR2(20) NOT NULL, CHAN_TYP VARCHAR2(3) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, CCY VARCHAR2(3) NOT NULL, ONCE_LIMIT NUMBER(18,4) NOT NULL, DAY_LIMIT NUMBER(18,4) NOT NULL, UP_DATE VARCHAR2(10) NOT NULL, UP_TELER VARCHAR2(10) NOT NULL, DAY_TOTAL INTEGER, YEAR_LIMIT NUMBER(18,4), CONSTRAINT PK_CUST_CH_AU_SELF_LIM PRIMARY KEY (EBUSER_NO, CHAN_TYP, SEC_WAY, CCY));

CREATE TABLE EBANK_CREDIT_CARD (EBUSER_NO VARCHAR2(20) NOT NULL, CREDIT_NO VARCHAR2(32) NOT NULL, CREDIT_TYPE VARCHAR2(10) NOT NULL, CREDIT_ANAME VARCHAR2(40), CCY_TYPE VARCHAR2(3), IS_DEFL CHAR(1) NOT NULL, HANG_DATE VARCHAR2(10), HANG_TIME VARCHAR2(10), HANG_ORG VARCHAR2(8), HANG_TELER VARCHAR2(8), HANG_CHAN VARCHAR2(3), OPEN_ORG VARCHAR2(3), PMT_DUE_DATE VARCHAR2(20), BILLING_DATE VARCHAR2(20), PUSHFLAG VARCHAR2(1), CREDIT_CVN2 VARCHAR2(3), CREDIT_EXPIRED VARCHAR2(10), PAR VARCHAR2(32) NOT NULL, INIT_MARK VARCHAR2(2), CONSTRAINT PK_EBANK_CREDIT_CARD PRIMARY KEY (EBUSER_NO, CREDIT_NO));

CREATE TABLE EBANK_ACCT (EBUSER_NO VARCHAR2(20) NOT NULL, ACCT_NO VARCHAR2(32) NOT NULL, SUB_ACCT_NO VARCHAR2(10) NOT NULL, ACC_ALIAS VARCHAR2(20), CCY_TYPE VARCHAR2(3), ACCT_TYP VARCHAR2(10) NOT NULL, IS_DEFL CHAR(1), HANG_DATE VARCHAR2(10), HANG_TIME VARCHAR2(10), HANG_ORG VARCHAR2(8), HANG_TELER VARCHAR2(8), HANG_CHAN VARCHAR2(8), CARD_FLAG VARCHAR2(2) NOT NULL, CARD_KIND VARCHAR2(4), VOUR_TYPE VARCHAR2(10), OPEN_ORG VARCHAR2(10), PAR VARCHAR2(32) NOT NULL, CARD_LEVELS VARCHAR2(3), CARD_BIND_ACC_NO VARCHAR2(22), OPEN_CHANNEL VARCHAR2(3), FACE_SIGN_FLAG VARCHAR2(2), ISSUERS_FLAG VARCHAR2(2), CONSTRAINT PK_EBANK_ACCT PRIMARY KEY (EBUSER_NO, ACCT_NO));

CREATE INDEX IDX_EBANK_ACCT_NO ON EBANK_ACCT (ACCT_NO);

CREATE TABLE CUST_CHAN_AUTH_LIM_USE (EBUSER_NO VARCHAR2(20) NOT NULL, CHAN_TYP VARCHAR2(3) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, CCY VARCHAR2(3) NOT NULL, DAY_SUM_LIMIT NUMBER(18,4) NOT NULL, LAST_TRAN_DATE VARCHAR2(10) NOT NULL, DAY_TOTAL INTEGER, YEAR_SUM_LIMIT NUMBER(18,4), LAST_YEAR VARCHAR2(4), CONSTRAINT PK_CUST_CHAN_AUTH_LIM_USE PRIMARY KEY (EBUSER_NO, CHAN_TYP, SEC_WAY, CCY));

CREATE TABLE SPECI_TRANS_LIMIT (TRANS_TYPE VARCHAR2(6) NOT NULL, ONCE_LIMIT NUMBER(18,4) NOT NULL, DAY_LIMIT NUMBER(18,4) NOT NULL, IS_AUDI VARCHAR2(1), AUDI_ID VARCHAR2(30), CONSTRAINT PK_SPECI_TRANS_LIMIT PRIMARY KEY (TRANS_TYPE));

CREATE TABLE EBANK_ACCT (EBUSER_NO VARCHAR2(20) NOT NULL, ACCT_NO VARCHAR2(32) NOT NULL, SUB_ACCT_NO VARCHAR2(10) NOT NULL, ACC_ALIAS VARCHAR2(20), CCY_TYPE VARCHAR2(3), ACCT_TYP VARCHAR2(10) NOT NULL, IS_DEFL CHAR(1), HANG_DATE VARCHAR2(10), HANG_TIME VARCHAR2(10), HANG_ORG VARCHAR2(8), HANG_TELER VARCHAR2(8), HANG_CHAN VARCHAR2(8), CARD_FLAG VARCHAR2(2) NOT NULL, CARD_KIND VARCHAR2(4), VOUR_TYPE VARCHAR2(10), OPEN_ORG VARCHAR2(10), PAR VARCHAR2(32) NOT NULL, CARD_LEVELS VARCHAR2(3), CARD_BIND_ACC_NO VARCHAR2(22), OPEN_CHANNEL VARCHAR2(3), FACE_SIGN_FLAG VARCHAR2(2), ISSUERS_FLAG VARCHAR2(2), CONSTRAINT PK_EBANK_ACCT PRIMARY KEY (EBUSER_NO, ACCT_NO));

CREATE INDEX IDX_EBANK_ACCT_NO ON EBANK_ACCT (ACCT_NO);

CREATE TABLE CUST_PAY_AUTH_SET_LIMT (EBUSER_NO VARCHAR2(20) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, CCY VARCHAR2(3) NOT NULL, ONCE_LIMIT NUMBER(18,4) NOT NULL, DAY_LIMIT NUMBER(18,4) NOT NULL, UP_DATE VARCHAR2(10) NOT NULL, UP_CHAN VARCHAR2(10) NOT NULL, CONSTRAINT PK_CUST_PAY_AUTH_SET_LIMT PRIMARY KEY (EBUSER_NO, SEC_WAY, CCY));

CREATE TABLE CUST_SPCI_LIM_USE (EBUSER_NO VARCHAR2(20) NOT NULL, TRANS_TYPE VARCHAR2(6) NOT NULL, DAY_SUM_LIMIT NUMBER(18,4) NOT NULL, LAST_TRAN_DATE VARCHAR2(10), CONSTRAINT PK_CUST_SPCI_LIM_USE PRIMARY KEY (EBUSER_NO, TRANS_TYPE));

CREATE TABLE SOTP_SPECI_TRANS_LIMIT (TRANS_TYPE VARCHAR2(3) NOT NULL, ONCE_LIMIT NUMBER(18,4) NOT NULL, DAY_LIMIT NUMBER(18,4) NOT NULL, STATUS VARCHAR2(1) NOT NULL, UPD_DATE VARCHAR2(10) NOT NULL, UPD_TIME VARCHAR2(8) NOT NULL, UPD_TELLER VARCHAR2(30) NOT NULL, IS_AUDI VARCHAR2(1), AUDI_ID VARCHAR2(30), CONSTRAINT PK_SOTP_SPECI_TRANS_LIMIT PRIMARY KEY (TRANS_TYPE));

CREATE TABLE CUST_PAY_AUTH_LIM_USE (EBUSER_NO VARCHAR2(20) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, CCY VARCHAR2(3) NOT NULL, DAY_SUM_LIMIT NUMBER(18,4) NOT NULL, LAST_TRAN_DATE VARCHAR2(10) NOT NULL, CONSTRAINT PK_CUST_PAY_AUTH_LIM_USE PRIMARY KEY (EBUSER_NO, SEC_WAY, CCY));

CREATE TABLE CUST_PAY_AUTH_SET_LIMT (EBUSER_NO VARCHAR2(20) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, CCY VARCHAR2(3) NOT NULL, ONCE_LIMIT NUMBER(18,4) NOT NULL, DAY_LIMIT NUMBER(18,4) NOT NULL, UP_DATE VARCHAR2(10) NOT NULL, UP_CHAN VARCHAR2(10) NOT NULL, CONSTRAINT PK_CUST_PAY_AUTH_SET_LIMT PRIMARY KEY (EBUSER_NO, SEC_WAY, CCY));

CREATE TABLE CUST_CHAN_LIM_USE (EBUSER_NO VARCHAR2(20) NOT NULL, CHAN_TYP VARCHAR2(3) NOT NULL, CCY VARCHAR2(3) NOT NULL, DAY_SUM_LIMIT NUMBER(18,4) NOT NULL, LAST_TRAN_DATE VARCHAR2(10) NOT NULL, DAY_TOTAL INTEGER, YEAR_SUM_LIMIT NUMBER(18,4), LAST_YEAR VARCHAR2(4), NOTICE_FLAG VARCHAR2(1), CONSTRAINT PK_CUST_CHAN_LIM_USE PRIMARY KEY (EBUSER_NO, CHAN_TYP, CCY));

CREATE TABLE CHAN_AUTH_LIM_DEF (CHAN_TYP VARCHAR2(3) NOT NULL, SEC_WAY VARCHAR2(2) NOT NULL, CCY VARCHAR2(3) NOT NULL, ONCE_LIMIT NUMBER(18,4) NOT NULL, DAY_LIMIT NUMBER(18,4) NOT NULL, UP_DATE VARCHAR2(10) NOT NULL, UP_TELER VARCHAR2(50), IS_AUDI VARCHAR2(1), AUDI_ID VARCHAR2(30), DAY_TOTAL INTEGER, YEAR_LIMIT NUMBER(18,4), OPEN_WAY VARCHAR2(2) NOT NULL, CONSTRAINT PK_CHAN_AUTH_LIM_DEF PRIMARY KEY (CHAN_TYP, SEC_WAY, CCY, OPEN_WAY));

create or replace package pkg50964133 as
type rd is record(c0 int,c1 int);
type tb is table of rd index by pls_integer;
procedure p1(r out rd);
procedure p2(c sys_refcursor);
procedure p3(r rd, t out tb);
procedure p4(a obj0, b out obj1);
procedure p5(a obj1, b out int);
end;
/
create or replace package body pkg50964133 as
procedure p1(r out rd) is
begin
null;
end;
procedure p2(c sys_refcursor) is
begin
null;
end;
procedure p3(r rd, t out tb) is
begin
for i in 1..100 loop
t(i):=r;
end loop;
end;
procedure p4(a obj0, b out obj1) is
begin
b:=obj1(a.c0,a);
end;
procedure p5(a obj1, b out int) is
begin
b:=a.c0+a.c1.c0;
end;
end;
/

create or replace package pkg_aa_rd as
type rd is record(c0 int,c1 int,c2 int,c3 int,c4 int,c5 int,c6 int,c7 int,c8 int);
type tb is table of rd index by pls_integer;
t tb;
procedure p1;
end;
/
create or replace package body pkg_aa_rd as
function f1 return int is begin return 1;end;
procedure p1 is
begin
for i in t.count..t.count+10 loop
t(i).c0:=1;
t(i).c1:=1;
t(i).c2:=1;
t(i).c3:=1;
end loop;
end;
end;
/


create or replace package pkg_aa_rd_rd as
type rd0 is record(c0 int,c1 int);
type rd is record(c0 int,c1 rd0);
type tb is table of rd index by pls_integer;
t tb;
procedure p1;
procedure p2;
procedure p3;
function f1 return int parallel_enable;
end;
/
create or replace package body pkg_aa_rd_rd as
procedure p1 is
begin
t(1).c0:=1;
t(1).c1.c0:=1;
t(1).c1.c1:=1;
t.delete(1);
t(1).c0:=10;
t(1).c1.c0:=10;
t(1).c1.c1:=10;
end;
procedure p2 is
r rd;
begin
for i in 1..1000 loop
  r.c0:=i;
  r.c1:=null;
  t(i):=r;
  if mod(i,2) =0 then
     t.delete(i);
  end if;
end loop;
end;
procedure p3 is
begin
for i in t.first..t.last loop
   if not t.exists(i) then
      dbms_output.put_line('i is:'||i);
      t(i).c0:=i;
      t(i).c1.c0:=i;
      t(i).c1.c1:=i;
   else
       t(i).c1.c0:=i;
       t(i).c1.c1:=i;
   end if;
end loop;
end;
function f1 return int parallel_enable is
begin
if t.count!=999 then
raise_application_error(-20001,'pkg_aa_rd_rd');
return 0;
end if;
for i in t.first..t.last loop
  if t(i).c0!=i or t(i).c1.c0!=i or t(i).c1.c1!=i then
      return 0;
  end if;
end loop;
return 1;
end;
end;
/
--3.NT[rd]
create or replace package pkg_nt_rd as
type tb is table of t0%rowtype;
t tb:=tb();
procedure p1(a t0%rowtype);
procedure p1(a t0%rowtype, i int);
procedure p2;
procedure p2(i int);
end;
/
create or replace package body pkg_nt_rd as
procedure p1(a t0%rowtype) is
begin
   t.extend();
   t(t.count):=a;
end;
procedure p1(a t0%rowtype, i int) is
begin
   t.extend();
   t(i):=a;
end;
procedure p2 is
begin
t.delete(t.count);
end;
procedure p2(i int) is
begin
t.delete(i);
end;
end;
/
--4.NT[record(record)]
create or replace package pkg_nt_rd_rd as
type rd0 is record(c0 int,c1 int);
type rd is record(c0 int,c1 rd0);
type tb is table of rd;
t tb:=tb();
procedure p1;
procedure p2;
procedure p3;
function f1 return int parallel_enable;
end;
/
create or replace package body pkg_nt_rd_rd as
procedure p1 is
begin
t.extend();
t(1):=null;
t(1).c0:=1;
t(1).c1.c0:=1;
t(1).c1.c1:=1;
t.delete(1);
t(1).c0:=10;
t(1).c1.c0:=10;
t(1).c1.c1:=10;
end;
procedure p2 is
r rd;
begin
for i in 1..1000 loop
  t.extend();
  r.c0:=i;
  r.c1:=null;
  t(i):=r;
  if mod(i,2) =0 then
     t.delete(i);
  end if;
end loop;
end;
procedure p3 is
begin
for i in t.first..t.last loop
   if not t.exists(i) then
      dbms_output.put_line('i is:'||i);
      t(i).c0:=i;
      t(i).c1.c0:=i;
      t(i).c1.c1:=i;
   else
       t(i).c1.c0:=i;
       t(i).c1.c1:=i;
   end if;
end loop;
end;
function f1 return int parallel_enable is
begin
if t.count!=2001 then
raise_application_error(-20001,'pkg_aa_rd_rd');
end if;
for i in t.first..t.last loop
  if t(i).c0!=i or t(i).c1.c0!=i or t(i).c1.c1!=i then
      return 0;
  end if;
end loop;
return 1;
end;
end;
/

create or replace package pkg_nt_aa_rd as
type tb is table of t0%rowtype index by varchar2(10);
type ttb is table of tb;
t ttb:=ttb();
procedure p1(a t0%rowtype, b t0%rowtype);
procedure p2;
procedure p3;
end;
/
create or replace package body pkg_nt_aa_rd as
procedure p1(a t0%rowtype, b t0%rowtype) is
v tb;
begin
v('old'):=a;
v('new'):=b;
t.extend();
t(t.count):=v;
end;
procedure p2 is
begin
t.delete(t.count);
end;
procedure p3 is
begin
for i in 1..t.count loop
  if mod(i,2)=1 then
     t(i).delete('old');
  end if;
end loop;
end;
end;
/
--6.NT[NT]
create or replace package pkg_nt_nt as
type tb is table of int;
type ttb is table of tb;
t ttb:=ttb(tb(1),tb(1),tb(1));
procedure p1;
function f1(a int) return int parallel_enable;
end;
/

create or replace package body pkg_nt_nt as
procedure p1 is
begin
for i in 1..2000 loop
   t.extend();
   t(t.count):=tb(1);
   if i<1000 then
      t.delete(i);
   end if;
end loop;
end;
function f1(a int) return int is
begin
if t.count!=a then
raise_application_error(-20001,'pkg_nt_nt');
end if;
for i in t.first..t.last loop
  if t(i).count!=1 then
     return 0;
  end if;
  if t(i)(1)!=1 then
      return 0;
  end if;
end loop;
return 1;
end;
end;
/
--7.NT[NT[record]]
create or replace package pkg_nt_nt_rd as
type rd is record(c0 int,c1 int,c2 int, c3 int,c4 int,c5 varchar2(10), c6 int ,c7 varchar2(10) default 'a');
type tb is table of rd index by pls_integer;
type ttb is table of tb index by pls_integer;
t ttb;
procedure p1(a rd);
procedure p2;
procedure p3;
end;
/
create or replace package body pkg_nt_nt_rd as
procedure p1(a rd) is
v tb;
begin
for i in 1..10 loop
v(i):=a;
end loop;
t(t.count):=v;
end;
procedure p2 is
begin
t.delete(t.count);
end;
procedure p3 is
begin
for i in 1..t.count loop
  if mod(i,2)=1 then
     t(i).delete(t(i).count/2);
  end if;
end loop;
end;
end;
/
--8.AA[record[AA]]
create or replace package pkg_aa_rd_aa as
type tb is table of t0%rowtype index by varchar2(10);
type rd is record(c0 tb, c1 tb);
type ttb is table of rd index by pls_integer;
t ttb;
v tb;
procedure p1(a t0%rowtype, b t0%rowtype);
procedure p2;
procedure p3;
end;
/
create or replace package body pkg_aa_rd_aa  as
procedure p1(a t0%rowtype, b t0%rowtype) is
r rd;
type tb is table of varchar2(10);
c tb:=tb('a','b','c','d','e','f','g','h');
begin
v('old'):=a;
v('new'):=b;
for i in c.first..c.last loop
   v(c(i)):=a;
end loop;
r.c0:=v;
r.c1:=v;
t(t.count-100):=r;
end;
procedure p2 is
begin
t.delete(t.count);
end;
procedure p3 is
begin
for i in t.first..t.last loop
  if mod(i,2)=1 or mod(i,2)=-1 then
    t(i).c0.delete('a','h');
  end if;
end loop;
end;
end;
/

--9.obj[nt]
create or replace type obj_nt force as object(c0 int, c1 tb,constructor function obj_nt(c0 int)return self as result,
member procedure p1(self in out obj_nt),
member procedure p2(self in out obj_nt),
member procedure p3(self in out obj_nt)
);
/
create or replace package pkg_aa_obj as
type tb is table of obj_nt index by pls_integer;
t tb;
id int:=0;
procedure p1(a obj_nt);
end;
/
create or replace package body pkg_aa_obj as
procedure p1(a obj_nt) is
begin
t(id):=a;
id:=id+1;
end;
end;
/
create or replace type body obj_nt as constructor function obj_nt(c0 int)return self as result is
begin
self.c0:=c0;
self.c1:=tb(obj0(c0,c0));
self.c1.extend(10,1);
pkg_aa_obj.p1(self);
return;
end;
member procedure p1(self in out obj_nt) is
begin
self.c1.extend(1,1);
end;
member procedure p2(self in out obj_nt) is
begin
for i in self.c1.first..self.c1.last loop
    if mod(i,2) = 0 then
        self.c1.delete(i);
    end if;
end loop;
end;
member procedure p3(self in out obj_nt) is
begin
for i in self.c1.first..self.c1.last loop
    self.c1.delete(i);
end loop;
end;
end;
/
drop package pkg_obj_nt;
create or replace package pkg_obj_nt as
a obj_nt:=obj_nt(1);
b obj_nt;
procedure p1;
procedure p2;
function f1 return int parallel_enable;
end;
/
create or replace package body pkg_obj_nt as
procedure p1 is
begin
for i in 1..2000 loop
a.p1();
end loop;
for i in 1..2000 loop
if i >500 and i<=1500 then
    a.c1.delete(i);
end if;
end loop;
end;
procedure p2 is
begin
a.p1();
b:=a;
b.p2();
end;
function f1 return int is
n int :=0;
begin
for i in a.c1.first..a.c1.last loop
    if not a.c1.exists(i) then continue;end if;
    n := n +a.c1(i).c0;
end loop;
return n; 
end;
end;
/
--pkg_aa
create or replace package pkg_aa as
type tb is table of int index by pls_integer;
t tb;
id int;
procedure p1;
procedure p2;
end;
/
create or replace package body pkg_aa as
procedure p1 is
begin
t.delete();
for i in 1..10000 loop
   t(10000-i+1):=10000-i+1;
end loop;
for i in 1..10000 loop
   if not mod(i,3)=1 then
      t.delete(i);
   end if;
end loop;
id:=0;
for i in 1..3333 loop
id := t.next(id);
if id != (i-1)*3+1 then
   raise_application_error(-20001,'结果不正确.expect:'||((i-1)*3+1)||'actual:'||id);
end if;
end loop;
end;
procedure p2 is
begin
t.delete();
for i in 1..100 loop
  t(100+i):=100+i;
end loop;
for i in 1..100 loop
  t(i):=i;
end loop;
end;
end;
/
create or replace package pkg_aa_aa as
type tb is table of pkg_aa.tb index by pls_integer;
t tb;
procedure p1;
end;
/
create or replace package body pkg_aa_aa as
procedure p1 is
begin
t.delete();
for i in 1..10 loop
   t(10+i):=pkg_aa.t;
end loop;
for i in 1..10 loop
   t(i):=pkg_aa.t;
end loop;
end;
end;
/
create or replace package pkg_aa_aa_aa as
type tb is table of int index by pls_integer;
type ttb is table of tb index by pls_integer;
type tttb is table of ttb index by pls_integer;
t1 tb;
t2 ttb;
t3 tttb;
cursor c is select level,level from dual connect by level <100;
procedure p1;
procedure p2;
procedure p3;
end;
/
create or replace package body pkg_aa_aa_aa as
procedure p1 is
begin
for i in 1..20 loop
   if mod(i,2)=1 then t1(i):=i;end if;
end loop;
for i in 1..50 loop
   if mod(i,2)=1 then t2(i):=t1;end if;
end loop;
end;
procedure p2 is
begin
for i in 1..1000 loop
 if mod(i,4)=1 then t3(i):=t2;end if;
end loop;
end;
procedure p3 is
begin
open c ;
fetch c bulk collect into t2(1),t2(2);
close c;
for i in 1..100 loop
t3(i+100):=t2;
end loop;
end;
end;
/

create or replace function f2024071200103693517 return int
is
  cnt int;
begin
  select count(*) into cnt from t1;
  insert into t1 values(0,0);
  return 1;
end;
/

create or replace package pkg2024071200103693517 is
  v1 int := f2024071200103693517();
  v2 int;
  procedure p1;
end;
/

create or replace package body pkg2024071200103693517 as
  procedure p1
  is
  begin
    select count(*) into v2 from t0;
  end;
end;
/
create or replace package pkg54042289 as
a obj0:=obj0(1);
procedure p1;
end;
/

create or replace package body pkg54042289 as
procedure p1 is
begin
a.c1:=10;
end;
end;
/
--echo 2024070900103611543
create or replace package pkg2024070900103611543 as
cursor c is select * from t0;
procedure p1;
end;
/
create or replace package body pkg2024070900103611543 as
procedure p1 is
begin
for rec in c loop
null;
end loop;
end;
end;
/
create or replace package pkg0 as 
a varray_int;
type tb_varray is table of varray_int index by pls_integer;
function f1(n int) return varray_int;
procedure p1(a in out varray_int);
end;
/ 

create or replace package body pkg0 as 
function f1(n int) return varray_int is begin a:=varray_int();a.extend(n);for i in 1..n loop a(i):=i;end loop; return a;end;
procedure p1(a in out varray_int)is
begin 
a:=pkg0.a;
end;
end;
/

create or replace package pkg1 as
t pkg0.tb_varray;
type rd is record(c0 pkg0.tb_varray, c1 varray_int default pkg0.f1(100), c2 varray_int default pkg0.f1(1000));  
r rd;
procedure p1;
end;
/
create or replace package body pkg1 as
procedure p1 is
begin
select pkg1.r.c1 bulk collect into pkg1.r.c0 from dual;
for i in 1..pkg1.r.c1.count loop
   pkg0.p1(r.c0(i));
end loop;
end;
end;
/
create or replace package pkg2 as
type subtb is table of int;
type tb is table of subtb;
t tb := tb(subtb(1,3,4));
cursor c1 is select c0 from t0;
cursor c2(t tb1) is select * from table(t(t.count).f1);
procedure p1;
procedure p2;
end;
/
create or replace package body pkg2 as
procedure p0(a obj0, b out tb)is begin b:=tb(null); select a.c0 bulk collect into b(1) from dual; end;
function f0(a in out subtb, b out tb)return int is
begin
  open c1;
  fetch c1 bulk collect into a;
  close c1;
  return 1;
end;
procedure p1 is
a int;
begin
a:=f0(t(1), t);
p0(obj0(0),t);
end;
procedure p2 is
procedure p1 is
begin
for rec in c2(tb1(obj1(1),obj1(2),obj1(3),obj1(4),obj1(5))) loop
    p0(obj0(rec.c0), t);
end loop;
end;
begin
p1();
end;
end;
/

create or replace package pkg52838400_1 as
type rd is record(c0 int, c1 obj0 default obj0(0,0));
r rd;
procedure p1(a rd default r);
end;
/
create or replace package body pkg52838400_1 as
procedure p1(a rd default r)is
b obj0;
begin
b:=a.c1;
end;
end;
/

create or replace package pkg52838400_2 as
type rd is record(c0 int, c1 obj0 default obj0(0,0));
r rd;
procedure p1;
end;
/

create or replace package body pkg52838400_2 as
procedure p1 is
b obj0;
begin
b:=pkg52838400_1.r.c1.f0(r.c1);
end;
end;
/

create or replace package pkg52838400_3 as
type rd is record(c0 int, c1 anydata default anydata.convertobject(obj0(0,0)));
r rd;
procedure p1(a rd default r);
end;
/
create or replace package body pkg52838400_3 as
procedure p1(a rd default r)is
b anydata;
begin
b:=a.c1;
end;
end;
/

create or replace package pkg52838400_4 as
type rd is record(c0 int, c1 obj0 default obj0(10, 10));
r rd;
b obj0:=obj0(0,1);
a obj0;
procedure p1;
end;
/

create or replace package body pkg52838400_4 as
procedure p1 is
begin
b.c0:=pkg52838400_3.r.c1.getobject(a);
b:=a.f0(r.c1);
end;
end;
/

create or replace package pkg3 as
a varray_int:=varray_int(1);
type rd0 is record(c0 int,c1 varray_int default a);
r rd0;
type rd is record(c0 int,c1 rd0 default r);
type tb is table of rd ;
t tb:=tb();
procedure p1;
end;
/
create or replace package body pkg3 as
procedure p1 is
begin
for i in case when t.count>0 then t.count else 1 end .. case when t.count>0 then t.count else 1 end +1000 loop
t.extend();
t(i).c0:=1;
t(i).c1.c0:=1;
if mod(i,10)=0 then
a.extend();
end if;
t(i).c1.c1:=a;
end loop;
for i in t.first..t.first+100 loop
  if mod(i,2)=0 then
     t.delete(i);
  end if;
  end loop;
for i in case when t.count>0 then t.count else 1 end .. case when t.count>0 then t.count else 1 end +1000 loop
t.extend();
t(i).c0:=1;
t(i).c1.c0:=10;
t(i).c1.c1:=a;
end loop;
end;
end;
/
--53517978
create or replace package pkg53517978 as
type rd is record(c0 int,c1 tb_int default tb_int(1,1));
r rd;
type ttb is table of rd index by pls_integer;
t ttb;
procedure p1;
procedure p0(a in out rd);
end;
/

create or replace package body pkg53517978 as
procedure p1 is
begin
for i in 1..1000 loop
  for j in (select 1 from dual) loop
     p0(r);
     t(i):=r;
  end loop;
  t:=null;
end loop;
end;
procedure p0(a in out rd) is
begin
a.c1.extend(1,1);
end;
end;
/

--55463093
create or replace package pkg55463093 as
a int:=10;
type rd is record(c0 int, c1 int default a);
function f1 return int;
end;
/
create or replace package body pkg55463093 as
r rd;
function f1 return int is
begin
return r.c1;
end;
end;
/
-- 54031117
create or replace package pkg54031117 as
type tb is table of int;
type ttb is table of tb;
t ttb:=ttb();
procedure p1;
end;
/
create or replace package body pkg54031117 as
procedure p1 is
begin
t.extend();
t(1):=tb(1,2,3);
t.extend(10,1);
end;
end;
/
create or replace function f54031117  return int parallel_enable is
begin
return pkg54031117.t.count;
end;
/
CREATE or replace PACKAGE PKGH_BANKBIND IS
  PROCEDURE COMMON_BIND_ROLLBACK(V_EBUSER_NO IN VARCHAR2, --电子银行客户号
                                 V_CHAN_TYP IN VARCHAR2);--电子渠道号
END PKGH_BANKBIND;
/
SHOW ERRORS;

CREATE or replace PACKAGE PKGH_LIMIT IS
  ------------------------------------------------------------------------
  -- ORACLE 包
  -- 渠道整合--限额
  -- 游标定义：
  --
  -- 存储过程定义：
  -- 主要动帐类交易的限额操作
  --
  -- 最后修改人： 费晓波
  -- 最后修改日期：2014.10.13
  ------------------------------------------------------------------------
  --智能转账限额校验
  PROCEDURE TRANS_LMT_CTL(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_SEC_WAY    IN VARCHAR2,--安全认证方式
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单笔累计限额
                          --3：大于系统默认单笔限额 4：大于系统默认单笔累计限额
                          --5: 大于客户单笔落地限额 6：大于客户单日累计落地限额
                          --7：大于全行客户单笔限额 8：大于全行客户单日累计限额 9999异常情况
                          LMT_CTL_FLAG OUT VARCHAR2,
                          LMT_VALUE OUT VARCHAR2);--单笔限额/剩余额度
   PROCEDURE TRANS_LMT_UPDATE(V_CHAN_TYP   IN VARCHAR2,
                          V_EBUSER_NO  IN VARCHAR2,
                          V_SEC_WAY    IN VARCHAR2,
                          V_TRANS_AMT  IN NUMBER,
                          V_TRANS_DATE IN VARCHAR2,
                          OUT_STATUS OUT VARCHAR2); --SUCCESS 成功
   PROCEDURE TRANS_LMT_ROLLBACK(V_CHAN_TYP   IN VARCHAR2,
                          V_EBUSER_NO  IN VARCHAR2,
                          V_SEC_WAY    IN VARCHAR2,
                          V_TRANS_AMT  IN NUMBER,
                          V_TRANS_DATE IN VARCHAR2,
                          OUT_STATUS OUT VARCHAR2); --SUCCESS
END PKGH_LIMIT;

/
SHOW ERRORS;

CREATE or replace PACKAGE PKGH_QUERY_LIMIT IS
  ------------------------------------------------------------------------
  --费晓波
  ------------------------------------------------------------------------
  --智能转账限额查询
  TYPE SEC_WAY_DATA_TYPE IS RECORD(SEC_WAY VARCHAR2(2),SEC_WAY_NAME VARCHAR2(20));
  TYPE LIMIT_DATA_TYPE IS RECORD(
       SEC_WAY VARCHAR2(2),
       SEC_WAY_NAME VARCHAR2(20),
       ONCE_LIMIT NUMBER(18,4),
       AVAIL_DAY_LIMIT NUMBER(18,4), --可用单日累计额度
       DAY_LIMIT NUMBER(18,4)
  );
  --返回程序的游标
  TYPE LIMIT_OUT_TYPE IS REF CURSOR RETURN LIMIT_DATA_TYPE;
  --个人安全认证方式的游标
  TYPE SEC_TYPE IS REF CURSOR RETURN SEC_WAY_DATA_TYPE;
  PROCEDURE SMART_LIMIT_QUERY(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          V_SMART_LIMIT_LIST OUT LIMIT_OUT_TYPE);--已使用额度
  PROCEDURE PAY_LIMIT_QUERY(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          V_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE);--已使用额度
  --访客支付限额
  PROCEDURE VISIT_PAY_LIMIT_QUERY(V_ACC_NO   IN VARCHAR2,--付款账号
                          V_VISIT_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE);
END PKGH_QUERY_LIMIT;

/
SHOW ERRORS;

CREATE or replace PACKAGE PKGH_SOTP_LIMIT_CHECK IS

  --MODIFY BY FEIXIAOBO --增加日累计笔数、年累计限额校验

  --智能转账限额校验
  PROCEDURE SOTP_TRANS_LMT_CTL(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_SEC_WAY    IN VARCHAR2,--安全认证方式
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                         -- V_IS_CHECK_CHAN_LIMIT IN VARCHAR2,--是否校验渠道累计限额
                          --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单日累计限额
                          --3：大于系统默认单笔限额 4：大于系统默认单日累计限额
                          --5: 大于客户单笔落地限额 6：大于客户单日累计落地限额
                          --7：大于全行客户单笔限额 8：大于全行客户单日累计限额 9999异常情况
                          LMT_CTL_FLAG OUT VARCHAR2,
                          LMT_VALUE OUT VARCHAR2);--单笔限额/单日累计剩余额度/单日剩余笔数/年累计剩余额度
  PROCEDURE SOTP_TRANS_LMT_UPDATE(V_CHAN_TYP   IN VARCHAR2,
                          V_EBUSER_NO  IN VARCHAR2,
                          V_SEC_WAY    IN VARCHAR2,
                          V_TRANS_AMT  IN NUMBER,
                          V_TRANS_DATE IN VARCHAR2,
                          OUT_STATUS OUT VARCHAR2); --SUCCESS 成功
  PROCEDURE SOTP_TRANS_LMT_ROLLBACK(V_CHAN_TYP   IN VARCHAR2,
                          V_EBUSER_NO  IN VARCHAR2,
                          V_SEC_WAY    IN VARCHAR2,
                          V_TRANS_AMT  IN NUMBER,
                          V_TRANS_DATE IN VARCHAR2,
                          OUT_STATUS OUT VARCHAR2); --SUCCESS
END PKGH_SOTP_LIMIT_CHECK;

/
SHOW ERRORS;

CREATE or replace PACKAGE PKGH_SOTP_PAY_LIMIT_CHECK IS

  --支付限额校验
  PROCEDURE SOTP_PAY_LMT_CTL(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_SEC_WAY    IN VARCHAR2,--安全认证方式
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单日累计限额
                          --3：大于系统默认单笔限额 4：大于系统默认单日累计限额
                          --5: 大于客户单笔落地限额 6：大于客户单日累计落地限额
                          --7：大于全行客户单笔限额 8：大于全行客户单日累计限额 9999异常情况
                          LMT_CTL_FLAG OUT VARCHAR2,
                          LMT_VALUE OUT VARCHAR2);--单笔限额/剩余额度
   --支付限额累计
  PROCEDURE SOTP_PAY_LMT_UPDATE( V_EBUSER_NO  IN VARCHAR2,
                                  V_SEC_WAY    IN VARCHAR2,
                                  V_TRANS_AMT  IN NUMBER,
                                  V_TRANS_DATE IN VARCHAR2,
                                  OUT_STATUS   OUT VARCHAR2); --SUCCESS 成功
  --支付限额回滚
  PROCEDURE SOTP_PAY_LMT_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,
                                    V_SEC_WAY    IN VARCHAR2,
                                    V_TRANS_AMT  IN NUMBER,
                                    V_TRANS_DATE IN VARCHAR2,
                                    OUT_STATUS   OUT VARCHAR2); --SUCCESS
END PKGH_SOTP_PAY_LIMIT_CHECK;

/
SHOW ERRORS;

CREATE or replace PACKAGE PKGH_SOTP_QUERY_LIMIT IS
  ------------------------------------------------------------------------
  --费晓波
  ------------------------------------------------------------------------
  --智能转账限额查询
  TYPE SEC_WAY_DATA_TYPE IS RECORD(
    SEC_WAY      VARCHAR2(2),
    SEC_WAY_NAME VARCHAR2(20));
  TYPE LIMIT_DATA_TYPE IS RECORD(
    SEC_WAY         VARCHAR2(2),
    SEC_WAY_NAME    VARCHAR2(20),
    ONCE_LIMIT      NUMBER(18, 4),
    AVAIL_DAY_LIMIT NUMBER(18, 4), --可用单日累计额度
    DAY_LIMIT       NUMBER(18, 4)
    );
    --ADD-FEIXIAOBO-2016-11-10-增加单日转账次数、单年转账累计金额
    TYPE LIMIT_DATA_TYPE_NEW IS RECORD(
         SEC_WAY         VARCHAR2(2),
         SEC_WAY_NAME    VARCHAR2(20),
         ONCE_LIMIT      NUMBER(18, 4),
         AVAIL_DAY_LIMIT NUMBER(18, 4), --可用单日累计额度
         DAY_LIMIT       NUMBER(18, 4),
         DAY_TOTAL       INTEGER, --单日转账笔数
         AVAIL_DAY_TOTAL  INTEGER,--单日可用转账笔数                                                     ,
         YEAR_LIMIT                NUMBER(18,4),--单年累计额度                                                                     ,
         AVAIL_YEAR_LIMIT          NUMBER(18,4)  --单年可用累计额度
    );
    --ADD-FEIXIAOBO-2016-11-10-增加单日转账次数、单年转账累计金额
  --返回程序的游标
  TYPE LIMIT_OUT_TYPE IS REF CURSOR RETURN LIMIT_DATA_TYPE;

  --返回程序的游标--ADD-FEIXIAOBO-2016-11-10-增加单日转账次数、单年转账累计金额
  TYPE LIMIT_OUT_TYPE_NEW IS REF CURSOR RETURN LIMIT_DATA_TYPE_NEW;
  --ADD-FEIXIAOBO-2016-11-10-增加单日转账次数、单年转账累计金额
  --个人安全认证方式的游标
  TYPE SEC_TYPE IS REF CURSOR RETURN SEC_WAY_DATA_TYPE;
  PROCEDURE SOTP_SMART_LIMIT_QUERY(V_CHAN_TYP         IN VARCHAR2, --渠道类型
                                   V_EBUSER_NO        IN VARCHAR2, --电子银行客户号
                                   V_TRANS_DATE       IN VARCHAR2, --转账日期
                                   V_SMART_LIMIT_LIST OUT LIMIT_OUT_TYPE_NEW, --已使用额度
                                   V_CHAN_LIMIT       OUT NUMBER,--渠道大额限额
                                   V_AVAIL_CHAN_LIMIT OUT NUMBER,--渠道大额可用限额
                                   IS_OPEN_SOTP       OUT VARCHAR2, --是否开通sotp 1-开通  0-不开通
                                   NOTICE_FLAG        OUT VARCHAR2); -- 大额提示 Y-当天提示  N-当天不提示
  PROCEDURE SOTP_PAY_LIMIT_QUERY(V_CHAN_TYP       IN VARCHAR2, --渠道类型
                                 V_EBUSER_NO      IN VARCHAR2, --电子银行客户号
                                 V_TRANS_DATE     IN VARCHAR2, --转账日期
                                 V_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE, --已使用额度
                                 IS_OPEN_SOTP     OUT VARCHAR2); --是否开通sotp 1-开通  0-不开通
  --访客支付限额
  PROCEDURE SOTP_VISIT_PAY_LIMIT_QUERY(V_ACC_NO               IN VARCHAR2, --付款账号
                                       V_VISIT_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE);

END PKGH_SOTP_QUERY_LIMIT;

/
SHOW ERRORS;
CREATE or replace PACKAGE PKGH_SOTP_SPEC_LIMIT IS
  ------------------------------------------------------------------------
  --特色转账限额控制
  --费晓波
  --2016-07-30
  ------------------------------------------------------------------------
  PROCEDURE SOTP_SPEC_TRANS_LMT_CTL(V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_CHAN_TYP     IN VARCHAR2,--渠道类型
                          V_TRANS_TYPE  IN VARCHAR2,--转账类型
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单笔累计限额
                          OUT_STATUS OUT VARCHAR2,
                          V_LIMIT_VALUE OUT NUMBER, --单笔或单日已用限额
                          V_DAY_LIMIT   OUT NUMBER);--单日总限额
   PROCEDURE SOTP_SPEC_TRANS_LMT_UPDATE(V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_TYPE  IN VARCHAR2,--转账类型
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          OUT_STATUS OUT VARCHAR2); --SUCCESS 成功
   PROCEDURE SOTP_SPEC_TRANS_LMT_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_TYPE  IN VARCHAR2,--转账类型
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          OUT_STATUS OUT VARCHAR2); --SUCCESS
END PKGH_SOTP_SPEC_LIMIT;

/
SHOW ERRORS;

CREATE or replace  PACKAGE PKGH_SPEC_LIMIT IS
  ------------------------------------------------------------------------
  --特色转账限额控制
  --费晓波
  --2014-11-13
  ------------------------------------------------------------------------
  --智能转账限额校验
  PROCEDURE SPEC_TRANS_LMT_CTL(V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_TYPE  IN VARCHAR2,--转账类型
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单笔累计限额
                          OUT_STATUS OUT VARCHAR2);
   PROCEDURE SPEC_TRANS_LMT_UPDATE(V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_TYPE  IN VARCHAR2,--转账类型
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          OUT_STATUS OUT VARCHAR2); --SUCCESS 成功
   PROCEDURE SPEC_TRANS_LMT_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_TYPE  IN VARCHAR2,--转账类型
                          V_TRANS_AMT  IN NUMBER,--转账金额
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          OUT_STATUS OUT VARCHAR2); --SUCCESS
END PKGH_SPEC_LIMIT;

/
SHOW ERRORS;

CREATE or replace PACKAGE PKGH_TEST IS
  PROCEDURE SIGN_TEST(V_EBUSER_NO  IN VARCHAR2,
                      V_CHAN_TYP IN VARCHAR2);      --电子银行客户号
END PKGH_TEST;

/
SHOW ERRORS;


CREATE or replace PACKAGE BODY PKGH_BANKBIND IS
  PROCEDURE COMMON_BIND_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,
                                 V_CHAN_TYP IN VARCHAR2)
   IS
   BEGIN
    --回滚点
  SAVEPOINT ROLLBACKPOINT;

  BEGIN
    DELETE FROM EBANK_CREDIT_CARD ECC WHERE ECC.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM EBANK_ACCT EA WHERE EA.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM CUST_CH_AU_SELF_LIM CCASL WHERE CCASL.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM CUST_PAY_AUTH_SET_LIMT CPASL WHERE CPASL.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM MB_ASSO_INFO MAI WHERE MAI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM ASSO_INFO AI WHERE AI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM P_SEC_AUTH PSA WHERE PSA.EBUSER_NO =  V_EBUSER_NO;
    DELETE FROM P_OPERATOR PO WHERE PO.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM P_LGN_INFO PLI WHERE PLI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM PCERT_INFO PCI WHERE PCI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM P_CUST_CHAN_IMG PCCI WHERE PCCI.EBUSER_NO = V_EBUSER_NO;

  -- add-by feixiaobo--删除指纹数据-20160818
  DELETE FROM CUST_MB_FINGER F WHERE F.EBUSER_NO = V_EBUSER_NO AND CHAN_TYP = V_CHAN_TYP;
  -- add-by feixiaobo--删除指纹数据-20160818
    UPDATE EUSER_INFO
      SET EBUSER_STAT = 'C'
    WHERE EBUSER_NO = V_EBUSER_NO;

    UPDATE USER_CHANNEL
      SET STAT = 'C',CLOSE_DATE=TO_CHAR(SYSDATE,'YYYY-MM-DD')
      WHERE EBUSER_NO = V_EBUSER_NO AND CHAN_TYP = V_CHAN_TYP;
    --WHERE EBUSER_NO = V_EBUSER_NO;
    --IF '058' = V_CHAN_TYP THEN
    --   DELETE FROM WEB_CHAN_INFO WCI WHERE WCI.EBUSER_NO = V_EBUSER_NO;
    --ELSIF '059' = V_CHAN_TYP THEN
    --   DELETE FROM MB_CHAN_INFO MCI WHERE MCI.EBUSER_NO = V_EBUSER_NO;
    --ELSIF '060' = V_CHAN_TYP THEN
     --  DELETE FROM IE_CHAN_INFO ICI WHERE ICI.EBUSER_NO = V_EBUSER_NO;
    --END IF;
  COMMIT;
  --异常处理机制
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK TO SAVEPOINT ROLLBACKPOINT;
  COMMIT;

  END;
END;
END PKGH_BANKBIND;

/
SHOW ERRORS;

CREATE or replace PACKAGE BODY PKGH_LIMIT IS
  PROCEDURE TRANS_LMT_CTL(V_CHAN_TYP     IN VARCHAR2,
                          V_EBUSER_NO    IN VARCHAR2,
                          V_SEC_WAY      IN VARCHAR2,
                          V_TRANS_AMT    IN NUMBER,
                          V_TRANS_DATE   IN VARCHAR2,
                          LMT_CTL_FLAG OUT VARCHAR2,
                          LMT_VALUE OUT VARCHAR2)--单笔限额/剩余额度
  --渠道类型，电子客户号，认证方式，转账金额，转账日期
   IS
    CASL_ONCE_LIMIT    NUMBER(18, 4); --用户自设单笔限额
    CASL_DAY_LIMIT     NUMBER(18, 4); --用户自设单日累计限额
    CALD_ONCE_LIMIT    NUMBER(18, 4); --渠道认证方式默认单笔限额
    CALD_DAY_LIMIT     NUMBER(18, 4); --渠道认证方式默认单日累计限额
    CALU_DAY_SUM_LIMIT NUMBER(18, 4); --客户认证方式限额使用情况
    V_EXCEPTION EXCEPTION; --自定义异常类型
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    P_BLD_ONCE_LIMIT       NUMBER(18, 4); --个人客户单笔落地限额
    P_BLD_DAY_LIMIT        NUMBER(18, 4); --个人客户单日累计落地限额
    CLU_DAY_SUM_LIMIT      NUMBER(18, 4); --个人客户当日已使用限额
    CLU_DAY_SUM_LIMIT_NEW  NUMBER(18, 4); --个人客户当日已使用限额+ 当前转账金额
    B_BLD_ONCE_LIMIT       NUMBER(18, 4); --全行客户单笔限额
    B_BLD_DAY_LIMIT        NUMBER(18, 4); --全行客户单日累计限额
    IS_CUST_AUTH_LIMIT     VARCHAR2(1); --是否有客户自设限额 0--没有 1--有
    IS_BANK_LIMIT          VARCHAR2(1); --是否有全行客户限额控制 0--没有 1--有
    V_CCY                    VARCHAR2(3); --币别
  BEGIN
    --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单笔累计限额
    --3：大于系统默认单笔限额 4：大于系统默认单笔累计限额
    --5: 大于客户单笔落地限额 6：大于客户单日累计落地限额
    --7：大于全行客户单笔限额 8：大于全行客户单日累计限额 9999异常情况
    LMT_CTL_FLAG := '9999';
    v_CCY          := 'CNY';
    LMT_VALUE := '';--默认为空
    --回滚点
    SAVEPOINT PT1;
    --渠道认证方式限额校验
    BEGIN
      BEGIN
        BEGIN
          --查询客户渠道认证方式自设限额
          BEGIN
            SELECT CASL.ONCE_LIMIT, CASL.DAY_LIMIT
              INTO CASL_ONCE_LIMIT, CASL_DAY_LIMIT
              FROM CUST_CH_AU_SELF_LIM CASL
             WHERE CASL.EBUSER_NO = V_EBUSER_NO
               AND CASL.CHAN_TYP = V_CHAN_TYP
               AND CASL.SEC_WAY = V_SEC_WAY
               AND CASL.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              IS_CUST_AUTH_LIMIT := 0;
          END;
          DBMS_OUTPUT.PUT_LINE('客户是否自设限额：'||IS_CUST_AUTH_LIMIT);
          --查询客户渠道认证方式当日已累计限额
          BEGIN
            SELECT CALU.DAY_SUM_LIMIT
              INTO CALU_DAY_SUM_LIMIT
              FROM CUST_CHAN_AUTH_LIM_USE CALU
             WHERE CALU.EBUSER_NO = V_EBUSER_NO
               AND CALU.CHAN_TYP = V_CHAN_TYP
               AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
               AND CALU.SEC_WAY = V_SEC_WAY
               AND CALU.CCY =V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              CALU_DAY_SUM_LIMIT := 0;
          END;
          --当前转账金额+客户渠道认证方式当日已累计金额
          CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT + V_TRANS_AMT;
           DBMS_OUTPUT.PUT_LINE('客户渠道认证方式当日已累计限额：'||CALU_DAY_SUM_LIMIT_NEW);
        END;
        --当客户渠道认证方式自设限额无值时
        IF IS_CUST_AUTH_LIMIT = '0' THEN
          --查询渠道认证方式默认限额
          BEGIN
            SELECT CALD.ONCE_LIMIT, CALD.DAY_LIMIT
              INTO CALD_ONCE_LIMIT, CALD_DAY_LIMIT
              FROM CHAN_AUTH_LIM_DEF CALD
             WHERE CALD.CHAN_TYP = V_CHAN_TYP
               AND CALD.SEC_WAY = V_SEC_WAY
               AND CALD.CCY = V_CCY
        and CALD.OPEN_WAY= '02';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              RAISE V_EXCEPTION;
          END;
          DBMS_OUTPUT.PUT_LINE('渠道认证方式默认限额：'||CALD_ONCE_LIMIT||','||CALD_DAY_LIMIT);
          --判断转账金额 是否 大于 系统渠道认证默认单笔限额
          IF V_TRANS_AMT > CALD_ONCE_LIMIT THEN
            LMT_CTL_FLAG := '3';
            LMT_VALUE := CAST(CALD_ONCE_LIMIT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
            --判断当前转账金额+客户渠道认证方式当日已累计金额 > 系统渠道认证默认当日转账金额限额
          ELSIF CALU_DAY_SUM_LIMIT_NEW > CALD_DAY_LIMIT THEN
            LMT_CTL_FLAG := '4';
            LMT_VALUE := CAST(CALD_DAY_LIMIT - V_TRANS_AMT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
          ELSE
            LMT_CTL_FLAG := '0';
          END IF;
          --判断转账金额 是否 大于 客户渠道认证自设单笔限额
        ELSIF V_TRANS_AMT > CASL_ONCE_LIMIT THEN
          LMT_CTL_FLAG := '1';
          LMT_VALUE := CAST(CASL_ONCE_LIMIT AS VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
          --判断当前转账金额+客户渠道认证方式当日已累计金额 > 客户渠道认证方式当日转账累计限额
        ELSIF CALU_DAY_SUM_LIMIT_NEW > CASL_DAY_LIMIT THEN
          LMT_CTL_FLAG := '2';
          LMT_VALUE := CAST(CASL_DAY_LIMIT - V_TRANS_AMT AS VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
        ELSE
          LMT_CTL_FLAG := '0';
        END IF;
      END;
      --落地限额校验
      BEGIN
        --客户已使用限额
        BEGIN
          SELECT CLU.DAY_SUM_LIMIT
            INTO CLU_DAY_SUM_LIMIT
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
    --         AND CLU.CHAN_TYP = V_CHAN_TYP
             AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
             AND CLU.CCY = V_CCY;
        EXCEPTION
          --无数据时，默认为0
          WHEN NO_DATA_FOUND THEN
            CLU_DAY_SUM_LIMIT := 0;
        END;
        CLU_DAY_SUM_LIMIT_NEW := CLU_DAY_SUM_LIMIT + V_TRANS_AMT;
        --网银,手机银行渠道校验落地限额
        IF V_CHAN_TYP = '060' OR V_CHAN_TYP = '059' THEN
          DBMS_OUTPUT.PUT_LINE('进入落地校验：'||V_CHAN_TYP);
          --查询落地限额；
          BEGIN
            SELECT BLD.ONCE_LIMIT, BLD.DAY_LIMIT
              INTO P_BLD_ONCE_LIMIT, P_BLD_DAY_LIMIT
              FROM BANK_LIMIT_DEF BLD
             WHERE BLD.BANK_LIMIT_TY = 'P'
               AND BLD.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              LMT_CTL_FLAG := '9999';
              RAISE V_EXCEPTION;
          END;
          --判断转账金额 是否大于 单笔落地限额
          IF V_TRANS_AMT > P_BLD_ONCE_LIMIT THEN
            LMT_CTL_FLAG := '5';
            LMT_VALUE := CAST(P_BLD_ONCE_LIMIT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
            --判断转账金额+当日已使用限额 是否大于 单日累计落地限额
          ELSIF CLU_DAY_SUM_LIMIT_NEW > P_BLD_DAY_LIMIT THEN
            LMT_CTL_FLAG := '6';
            LMT_VALUE := CAST(P_BLD_DAY_LIMIT - V_TRANS_AMT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
          ELSE
            LMT_CTL_FLAG := '0';
          END IF;
        END IF;
      END;
      --全行客户限额校验
      BEGIN
        --查询全行客户限额
        BEGIN
          SELECT BLD.ONCE_LIMIT, BLD.DAY_LIMIT
            INTO B_BLD_ONCE_LIMIT, B_BLD_DAY_LIMIT
            FROM BANK_LIMIT_DEF BLD
           WHERE BLD.BANK_LIMIT_TY = 'B'
             AND BLD.CCY = V_CCY;
        EXCEPTION
          --没有数据也算正常
          WHEN NO_DATA_FOUND THEN
            IS_BANK_LIMIT := '0';
        END;
        --转账金额 大于 全行客户单笔限额
        IF IS_BANK_LIMIT <> '0' AND V_TRANS_AMT > B_BLD_ONCE_LIMIT THEN
          LMT_CTL_FLAG := '7';
          LMT_VALUE := CAST(B_BLD_ONCE_LIMIT AS VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
        ELSIF IS_BANK_LIMIT <> '0' AND
              CLU_DAY_SUM_LIMIT_NEW > B_BLD_DAY_LIMIT THEN
          LMT_CTL_FLAG := '8';
          LMT_VALUE := CAST(B_BLD_DAY_LIMIT - V_TRANS_AMT AS VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
        ELSE
          LMT_CTL_FLAG := '0';
        END IF;
      END;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('限额校验状态'||LMT_CTL_FLAG);
      -- LMT_CTL_FLAG <> '0',即认为验证失败,触发异常,事务回滚到回滚点
      IF LMT_CTL_FLAG <> '0' THEN
       DBMS_OUTPUT.PUT_LINE('V_EXCEPTION START');
        RAISE V_EXCEPTION;
      END IF;
      --异常处理机制
    EXCEPTION
      WHEN V_EXCEPTION THEN
       DBMS_OUTPUT.PUT_LINE('EXCEPTION START');
       DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
       DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
        ROLLBACK TO SAVEPOINT PT1;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
    END;
  END;
  --转账限额累计
  PROCEDURE TRANS_LMT_UPDATE(V_CHAN_TYP   IN VARCHAR2,
                             V_EBUSER_NO  IN VARCHAR2,
                             V_SEC_WAY    IN VARCHAR2,
                             V_TRANS_AMT  IN NUMBER,
                             V_TRANS_DATE IN VARCHAR2,
                             OUT_STATUS OUT VARCHAR2) IS
    CALU_DAY_SUM_LIMIT NUMBER(18, 4); --客户认证方式限额使用情况
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    CLU_DAY_SUM_LIMIT      NUMBER(18, 4); --个人客户当日已使用限额
    CLU_DAY_SUM_LIMIT_NEW  NUMBER(18, 4); --个人客户当日已使用限额+ 当前转账金额
    v_CCY                    VARCHAR2(3); --币别
    IS_INSERT_CALU VARCHAR2(1); --是否是新增 0：不是 1：是
    IS_INSERT_CLU     VARCHAR2(1);       --是否是新增 0：不是 1：是
  BEGIN
    v_CCY := 'CNY';
    --回滚点
    SAVEPOINT PT1;
    BEGIN
      --查询客户渠道认证方式当日已累计限额
      BEGIN
        SELECT CALU.DAY_SUM_LIMIT
          INTO CALU_DAY_SUM_LIMIT
          FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CALU_DAY_SUM_LIMIT := 0;
          IS_INSERT_CALU := '1';
      DBMS_OUTPUT.PUT_LINE('客户已累计限额：'||CALU_DAY_SUM_LIMIT);
      END;
      --当前转账金额+客户渠道认证方式当日已累计金额
      CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT + V_TRANS_AMT;
      IF IS_INSERT_CALU = '1' THEN
       DBMS_OUTPUT.PUT_LINE('INSERT 客户已累计限额：'||CALU_DAY_SUM_LIMIT_NEW);
       DELETE
            FROM CUST_CHAN_AUTH_LIM_USE CALU
       WHERE
           CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
        --INSERT 客户渠道认证方式当日已累计限额
        INSERT INTO CUST_CHAN_AUTH_LIM_USE
          (EBUSER_NO,
           CHAN_TYP,
           SEC_WAY,
           CCY,
           DAY_SUM_LIMIT,
           LAST_TRAN_DATE)
        VALUES
          (V_EBUSER_NO, V_CHAN_TYP,V_SEC_WAY,v_CCY,CALU_DAY_SUM_LIMIT_NEW,V_TRANS_DATE);
      ELSE
      DBMS_OUTPUT.PUT_LINE('UPDATE 客户已累计限额：'||CALU_DAY_SUM_LIMIT_NEW);
        --更新客户渠道认证方式已使用限额
        UPDATE CUST_CHAN_AUTH_LIM_USE CALU
           SET CALU.DAY_SUM_LIMIT = CALU_DAY_SUM_LIMIT_NEW
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE;
      END IF;
    END;
    BEGIN
      --网银，手机银行渠道
      IF  V_CHAN_TYP = '059' OR V_CHAN_TYP = '060' THEN
       --客户已使用限额
        BEGIN
          SELECT CLU.DAY_SUM_LIMIT
            INTO CLU_DAY_SUM_LIMIT
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
           --  AND CLU.CHAN_TYP = V_CHAN_TYP
             AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
             AND CLU.CCY = V_CCY;
        EXCEPTION
          --无数据时，默认为0
          WHEN NO_DATA_FOUND THEN
            CLU_DAY_SUM_LIMIT := 0;
            IS_INSERT_CLU := '1';
        END;
        CLU_DAY_SUM_LIMIT_NEW := CLU_DAY_SUM_LIMIT + V_TRANS_AMT;
        IF IS_INSERT_CLU = '1' THEN
        --INSERT 客户已使用限额
          DELETE FROM CUST_LIM_USE
          WHERE EBUSER_NO = V_EBUSER_NO;
       -- INSERT INTO CUST_LIM_USE(EBUSER_NO,CHAN_TYP,CCY,DAY_SUM_LIMIT,LAST_TRAN_DATE) VALUES(V_EBUSER_NO,V_CHAN_TYP,CCY,CLU_DAY_SUM_LIMIT_NEW,V_TRANS_DATE);
          INSERT INTO CUST_LIM_USE(EBUSER_NO,CCY,DAY_SUM_LIMIT,LAST_TRAN_DATE) VALUES(V_EBUSER_NO,v_CCY,CLU_DAY_SUM_LIMIT_NEW,V_TRANS_DATE);
        ELSE
        --更新客户已使用限额
        --UPDATE CUST_LIM_USE CLU SET CLU.DAY_SUM_LIMIT = CLU_DAY_SUM_LIMIT_NEW WHERE CLU.EBUSER_NO = V_EBUSER_NO AND CLU.CHAN_TYP = V_CHAN_TYP AND CLU.LAST_TRAN_DATE = V_TRANS_DATE AND CLU.CCY = V_CCY;
        UPDATE CUST_LIM_USE CLU SET CLU.DAY_SUM_LIMIT = CLU_DAY_SUM_LIMIT_NEW WHERE CLU.EBUSER_NO = V_EBUSER_NO AND CLU.LAST_TRAN_DATE = V_TRANS_DATE AND CLU.CCY = V_CCY;
      END IF;
    END IF;
    END;
    DBMS_OUTPUT.PUT_LINE('COMMIT START');
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    OUT_STATUS := 'SUCCESS';
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' ||OUT_STATUS);
    EXCEPTION WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT1;
      OUT_STATUS := sqlerrm;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' ||OUT_STATUS);
  END;
  --转账限额回滚
  PROCEDURE TRANS_LMT_ROLLBACK(V_CHAN_TYP   IN VARCHAR2,
                             V_EBUSER_NO  IN VARCHAR2,
                             V_SEC_WAY    IN VARCHAR2,
                             V_TRANS_AMT  IN NUMBER,
                             V_TRANS_DATE IN VARCHAR2,
                             OUT_STATUS OUT VARCHAR2) IS
    CALU_DAY_SUM_LIMIT NUMBER(18, 4); --客户认证方式限额使用情况
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 - 当前转账金额
    CLU_DAY_SUM_LIMIT      NUMBER(18, 4); --个人客户当日已使用限额
    CLU_DAY_SUM_LIMIT_NEW  NUMBER(18, 4); --个人客户当日已使用限额 - 当前转账金额
    v_CCY                    VARCHAR2(3); --币别
    V_EXCEPTION EXCEPTION; --自定义异常类型
  BEGIN
    v_CCY := 'CNY';
    DBMS_OUTPUT.PUT_LINE('EBUSER_NO='||V_EBUSER_NO||',CHAN_TYP='||V_CHAN_TYP||',SEC_WAY='||V_SEC_WAY
    ||',TRANS_AMT='||V_TRANS_AMT||',TRANS_DATE='||V_TRANS_DATE);
    --回滚点
    SAVEPOINT PT1;
    BEGIN
      --查询客户渠道认证方式当日已累计限额
      BEGIN
        SELECT CALU.DAY_SUM_LIMIT
          INTO CALU_DAY_SUM_LIMIT
          FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('客户渠道认证方式无限额记录');
          OUT_STATUS := 'SUCCESS'; --客户渠道认证方式无限额记录
          RAISE V_EXCEPTION;
          RETURN;
      END;
      DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计限额：'||CALU_DAY_SUM_LIMIT);
      IF CALU_DAY_SUM_LIMIT = 0 THEN
         OUT_STATUS := 'SUCCESS'; --客户渠道认证方式单日累计限额为0
         RAISE V_EXCEPTION;
         RETURN;
      ELSIF CALU_DAY_SUM_LIMIT < V_TRANS_AMT THEN
         OUT_STATUS := 'SUCCESS';
         CALU_DAY_SUM_LIMIT_NEW := 0;
         DBMS_OUTPUT.PUT_LINE('客户渠道认证方式已累计限额小于转账金额--回滚后限额：'||CALU_DAY_SUM_LIMIT_NEW);
      ELSE
       OUT_STATUS := 'SUCCESS';
       CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT - V_TRANS_AMT;--客户任渠道认证方式单日已累计限额 - 转账金额
       DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计回滚后限额：'||CALU_DAY_SUM_LIMIT_NEW);
      END IF;
      --回滚客户渠道认证方式单日已累计限额
      UPDATE CUST_CHAN_AUTH_LIM_USE CALU
           SET DAY_SUM_LIMIT = CALU_DAY_SUM_LIMIT_NEW
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE;
    END;
    BEGIN
      --网银，手机银行渠道
      IF  V_CHAN_TYP = '059' OR V_CHAN_TYP = '060' THEN
       --客户已使用限额
        BEGIN
          SELECT CLU.DAY_SUM_LIMIT
            INTO CLU_DAY_SUM_LIMIT
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
        --     AND CLU.CHAN_TYP = V_CHAN_TYP
             AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
             AND CLU.CCY = V_CCY;
        EXCEPTION
          --无数据时，默认为0
          WHEN NO_DATA_FOUND THEN
             DBMS_OUTPUT.PUT_LINE('客户无限额记录');
             OUT_STATUS := 'SUCCESS'; --客户无限额记录
             RAISE V_EXCEPTION;
             RETURN;
        END;
        DBMS_OUTPUT.PUT_LINE('客户单日已累计限额：'||CLU_DAY_SUM_LIMIT);
        IF CLU_DAY_SUM_LIMIT = 0 THEN
         OUT_STATUS := 'SUCCESS'; --客户单日累计限额为0
         RAISE V_EXCEPTION;
         RETURN;
         ELSIF CLU_DAY_SUM_LIMIT < V_TRANS_AMT THEN
               OUT_STATUS := 'SUCCESS';
         CLU_DAY_SUM_LIMIT_NEW := 0;
         DBMS_OUTPUT.PUT_LINE('客户已累计限额小于转账金额--回滚后限额：'||CLU_DAY_SUM_LIMIT_NEW);
         ELSE
          OUT_STATUS := 'SUCCESS';
          CLU_DAY_SUM_LIMIT_NEW := CLU_DAY_SUM_LIMIT - V_TRANS_AMT;
           DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计回滚后限额：'||CLU_DAY_SUM_LIMIT_NEW);
      END IF;
        --回滚客户已使用限额
        --UPDATE CUST_LIM_USE CLU SET CLU.DAY_SUM_LIMIT = CLU_DAY_SUM_LIMIT_NEW WHERE CLU.EBUSER_NO = V_EBUSER_NO AND CLU.CHAN_TYP = V_CHAN_TYP AND CLU.LAST_TRAN_DATE = V_TRANS_DATE AND CLU.CCY=CCY;
        UPDATE CUST_LIM_USE CLU SET CLU.DAY_SUM_LIMIT = CLU_DAY_SUM_LIMIT_NEW WHERE CLU.EBUSER_NO = V_EBUSER_NO AND CLU.LAST_TRAN_DATE = V_TRANS_DATE AND CLU.CCY=CCY;
    END IF;
    END;
    DBMS_OUTPUT.PUT_LINE('COMMIT START');
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' ||OUT_STATUS);
    EXCEPTION
      WHEN V_EXCEPTION THEN
         ROLLBACK TO SAVEPOINT PT1;
         DBMS_OUTPUT.PUT_LINE('V_EXCEPTION OUT_STATUS : ' ||OUT_STATUS);
         COMMIT;
      WHEN OTHERS THEN
          ROLLBACK TO SAVEPOINT PT1;
          OUT_STATUS := sqlerrm;
          DBMS_OUTPUT.PUT_LINE('sqlcode : ' ||sqlcode);
          DBMS_OUTPUT.PUT_LINE('sqlerrm : ' ||sqlerrm);
          COMMIT;
          DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' ||OUT_STATUS);
  END;
END PKGH_LIMIT;

/
SHOW ERRORS;
CREATE or replace PACKAGE BODY PKGH_QUERY_LIMIT IS
  ------------------------------------------------------------------------
  --费晓波
  ------------------------------------------------------------------------
  --智能转账限额查询
  PROCEDURE SMART_LIMIT_QUERY(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                        --  V_SEC_WAY    IN VARCHAR2,--安全认证方式
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          V_SMART_LIMIT_LIST OUT LIMIT_OUT_TYPE)--已使用额度
   AS
   V_SEC_WAY_LIST SEC_TYPE;--客户认证方式数组游标
   V_SEC_WAY_DATA SEC_WAY_DATA_TYPE;--客户认证方式数组
   V_EXCEPTION EXCEPTION; --自定义异常类型
   V_HAS_SEC_WAY VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
   V_DAY_SUM_LIMIT_USE NUMBER(18,4);--客户单日已使用限额
   V_CCY VARCHAR2(3);
   V_ONCE_LIMIT NUMBER(18,4);--单笔限额
   V_DAY_LIMIT NUMBER(18,4);--单日累计限额
   V_IS_CUST_AUTH_LIMIT VARCHAR2(1);--是否有客户自定义限额 0-没有 1-有
   V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18,4); -- 客户单日剩余额度
  -- V_SEC_WAY VARCHAR2(2);
   V_SMART_LIMIT_DATA LIMIT_DATA_TYPE;--个人客户认证方式限额/剩余限额数据
   V_SMART_LIMIT_LIST_DATA LIMIT_LIST_TYPE := LIMIT_LIST_TYPE();
   BEGIN
       --V_SMART_LIMIT_LIST_DATA LIMIT_TYPE := LIMIT_TYPE();
       -- V_SMART_LIMIT_LIST_DATA := PKGH_QUERY_LIMIT.LIMIT_TYPE();--使用时应实例化，否则报错
        V_HAS_SEC_WAY := '0'; --默认无认证方式
        V_IS_CUST_AUTH_LIMIT := '1';--默认有客户自设认证方式
        V_CCY := 'CNY'; --人民币
        SAVEPOINT PT2; --回滚点
        --当客户自设渠道认证方式限额没有值时，取系统默认认证方式限额
        BEGIN
        --实例
      --  open my_cur for select gsmno,status,price
      --        from gsm_resource
      --      where store_id='SD.JN.01';
     --   fetch my_cur into my_rec;
     --     while my_cur%found loop
     --           dbms_output.put_line(my_rec.gsmno||'#'||my_rec.status||'#'||my_rec.price);
     --           fetch my_cur into my_rec; --让游标指向下一行
     --     end loop;
     --     close my_cur;
      --查询客户的安全认证方式
         OPEN V_SEC_WAY_LIST FOR
               SELECT PSA.SEC_WAY,AT.SEC_WAY_NAME FROM P_SEC_AUTH PSA LEFT JOIN AUTH_TYPE AT
               ON PSA.SEC_WAY = AT.SEC_WAY
              -- WHERE EBUSER_NO = 'P14112000100266' AND SEC_STATE = 'N' AND CHAN_TYP ='059';
              WHERE PSA.EBUSER_NO = V_EBUSER_NO AND PSA.SEC_STATE = 'N' AND PSA.CHAN_TYP = V_CHAN_TYP
              ORDER BY AT.SEC_LEVEL;
         --遍历客户的安全认证方式--row=0时，无数据
         LOOP
         FETCH V_SEC_WAY_LIST INTO V_SEC_WAY_DATA;
               --无数据时，退出
               EXIT WHEN V_SEC_WAY_LIST%NOTFOUND;
               V_HAS_SEC_WAY := '1';--有客户认证方式
         V_IS_CUST_AUTH_LIMIT := '1';
               --查询客户今天当前认证方式已使用累计限额
               BEGIN
                    SELECT CCALU.DAY_SUM_LIMIT INTO V_DAY_SUM_LIMIT_USE
                    FROM CUST_CHAN_AUTH_LIM_USE CCALU
                    WHERE CCALU.EBUSER_NO = V_EBUSER_NO AND CCALU.CHAN_TYP = V_CHAN_TYP
                          AND CCALU.SEC_WAY = V_SEC_WAY_DATA.SEC_WAY AND CCALU.CCY = V_CCY
                          AND CCALU.LAST_TRAN_DATE = V_TRANS_DATE;
                    EXCEPTION
                         WHEN NO_DATA_FOUND THEN
                         V_DAY_SUM_LIMIT_USE := 0;

               END;
               --查询客户当前认证方式的单笔限额和单日累计限额
               BEGIN
                   SELECT CCATT.ONCE_LIMIT,CCATT.DAY_LIMIT INTO V_ONCE_LIMIT,V_DAY_LIMIT
                   FROM CUST_CH_AU_SELF_LIM CCATT
                   WHERE CCATT.EBUSER_NO = V_EBUSER_NO AND CCATT.CHAN_TYP = V_CHAN_TYP
                         AND CCATT.SEC_WAY = V_SEC_WAY_DATA.SEC_WAY AND CCATT.CCY = V_CCY;
                   EXCEPTION
                         WHEN NO_DATA_FOUND THEN
                         V_IS_CUST_AUTH_LIMIT := '0';
               END;
         dbms_output.put_line('V_IS_CUST_AUTH_LIMIT===>'||V_IS_CUST_AUTH_LIMIT);
               --如果客户没有自设限额，则查询渠道系统默认限额
               IF V_IS_CUST_AUTH_LIMIT = '0' THEN
                  SELECT CALD.ONCE_LIMIT,CALD.DAY_LIMIT INTO V_ONCE_LIMIT,V_DAY_LIMIT
                  FROM CHAN_AUTH_LIM_DEF CALD
                  WHERE CALD.CHAN_TYP = V_CHAN_TYP
                  AND CALD.SEC_WAY = V_SEC_WAY_DATA.SEC_WAY AND CALD.CCY = V_CCY;
               END IF;
               --如果客户已使用限额大于 单日累计额度，则剩余额度为0
               --否则剩余= 单日累计限额-客户单日已使用限额
               IF V_DAY_SUM_LIMIT_USE > V_DAY_LIMIT THEN
                  V_DAY_SUM_LIMIT_RESIDUAL := 0;
               ELSE
                  V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT- V_DAY_SUM_LIMIT_USE;
               END IF;
               dbms_output.put_line('客户的安全认证方式row：'||V_SEC_WAY_LIST%ROWCOUNT||'，SEC_WAY：'||V_SEC_WAY_DATA.SEC_WAY);
               dbms_output.put_line('客户认证方式：'||V_SEC_WAY_DATA.SEC_WAY||'，客户单笔限额：'||V_ONCE_LIMIT||'，客户剩余额度：'||V_DAY_SUM_LIMIT_RESIDUAL);
               --为返回数据设置值
               --V_SMART_LIMIT_DATA.SEC_WAY := V_SEC_WAY_DATA.SEC_WAY;--认证方式
               --V_SMART_LIMIT_DATA.ONCE_LIMIT := V_ONCE_LIMIT;--单笔认证限额
               --V_SMART_LIMIT_DATA.DAY_LIMIT := V_DAY_SUM_LIMIT_RESIDUAL;--单日剩余额度
               --V_SMART_LIMIT_LIST_DATA(V_SEC_WAY_LIST%ROWCOUNT) := '123';
               --FOR I IN 1..V_SEC_WAY_LIST%ROWCOUNT LOOP
                 --  dbms_output.put_line('循环：'||I);
                   --IF I = V_SEC_WAY_LIST%ROWCOUNT THEN
                   V_SMART_LIMIT_LIST_DATA.EXTEND;
                   V_SMART_LIMIT_LIST_DATA(V_SMART_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT(V_SEC_WAY_DATA.SEC_WAY,V_SEC_WAY_DATA.SEC_WAY_NAME,V_ONCE_LIMIT,V_DAY_SUM_LIMIT_RESIDUAL,V_DAY_LIMIT);
                   --END IF;
               --END LOOP;
         END LOOP;
         CLOSE V_SEC_WAY_LIST;
         IF V_HAS_SEC_WAY = '0' THEN
             DBMS_OUTPUT.PUT_LINE('无认证方式');
             RAISE V_EXCEPTION;
             RETURN;
         END IF;
       END;
       FOR V_INDEX IN V_SMART_LIMIT_LIST_DATA.FIRST .. V_SMART_LIMIT_LIST_DATA.LAST LOOP
           dbms_output.put_line('V_SMART_LIMIT_LIST_DATA集合的认证方式'||V_SMART_LIMIT_LIST_DATA(V_INDEX).SEC_WAY||
                   '单笔限额'||V_SMART_LIMIT_LIST_DATA(V_INDEX).ONCE_LIMIT||
                   '剩余额度'||V_SMART_LIMIT_LIST_DATA(V_INDEX).AVAIL_DAY_LIMIT ||
                   '累计额度'||V_SMART_LIMIT_LIST_DATA(V_INDEX).DAY_LIMIT);
       END LOOP;
       --循环赋值
       OPEN V_SMART_LIMIT_LIST FOR SELECT SEC_WAY,SEC_WAY_NAME,ONCE_LIMIT,AVAIL_DAY_LIMIT,DAY_LIMIT FROM TABLE(CAST(V_SMART_LIMIT_LIST_DATA AS LIMIT_LIST_TYPE));
       --FOR V_SMART_LIMIT_DATA IN V_SMART_LIMIT_LIST
       --LOOP
       --FETCH V_SMART_LIMIT_LIST INTO V_SMART_LIMIT_DATA;
       --EXIT WHEN V_SMART_LIMIT_LIST%NOTFOUND;
       --DBMS_OUTPUT.put_line('V_SMART_LIMIT_DATA.SEC_WAY = ' || V_SMART_LIMIT_DATA.SEC_WAY ||
       --                 '; V_SMART_LIMIT_DATA.ONCE_LIMIT = ' || V_SMART_LIMIT_DATA.ONCE_LIMIT
       --                  ||'; V_SMART_LIMIT_DATA.DAY_LIMIT = ' || V_SMART_LIMIT_DATA.DAY_LIMIT);
       --END LOOP;
       --CLOSE V_SMART_LIMIT_LIST;
       COMMIT;
       EXCEPTION
            WHEN V_EXCEPTION THEN
               DBMS_OUTPUT.PUT_LINE('sqlcode : ' || sqlcode);
               DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || sqlerrm);
               ROLLBACK TO SAVEPOINT PT2;
               COMMIT;
               DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
     END SMART_LIMIT_QUERY;
     --查询支付限额
     PROCEDURE PAY_LIMIT_QUERY(V_CHAN_TYP   IN VARCHAR2,--渠道类型
                          V_EBUSER_NO  IN VARCHAR2,--电子银行客户号
                          V_TRANS_DATE IN VARCHAR2,--转账日期
                          V_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE)--已使用额度
   AS
   V_SEC_WAY_LIST SEC_TYPE;--客户认证方式数组游标
   V_SEC_WAY_DATA SEC_WAY_DATA_TYPE;--客户认证方式数组
   V_EXCEPTION EXCEPTION; --自定义异常类型
   V_HAS_SEC_WAY VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
   V_DAY_SUM_LIMIT_USE NUMBER(18,4);--客户单日已使用限额
   V_CCY VARCHAR2(3);
   V_ONCE_LIMIT NUMBER(18,4);--单笔限额
   V_DAY_LIMIT NUMBER(18,4);--单日累计限额
   V_IS_CUST_AUTH_LIMIT VARCHAR2(1);--是否有客户自定义限额 0-没有 1-有
   V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18,4); -- 客户单日剩余额度
   V_PAY_LIMIT_LIST_DATA LIMIT_LIST_TYPE := LIMIT_LIST_TYPE();
   BEGIN
        V_HAS_SEC_WAY := '0'; --默认无认证方式
        V_IS_CUST_AUTH_LIMIT := '1';--默认有客户自设认证方式
        V_CCY := 'CNY'; --人民币
        SAVEPOINT PT2; --回滚点
        --当客户自设渠道认证方式限额没有值时，取系统默认认证方式限额
        BEGIN
      --查询客户的安全认证方式
         OPEN V_SEC_WAY_LIST FOR
              SELECT PSA.SEC_WAY,AT.SEC_WAY_NAME FROM P_SEC_AUTH PSA LEFT JOIN AUTH_TYPE AT
               ON PSA.SEC_WAY = AT.SEC_WAY
              WHERE PSA.EBUSER_NO = V_EBUSER_NO AND PSA.SEC_STATE = 'N' AND PSA.CHAN_TYP = V_CHAN_TYP
              ORDER BY AT.SEC_LEVEL;
         --遍历客户的安全认证方式--row=0时，无数据
         LOOP
         FETCH V_SEC_WAY_LIST INTO V_SEC_WAY_DATA;
               --无数据时，退出
               EXIT WHEN V_SEC_WAY_LIST%NOTFOUND;
               V_HAS_SEC_WAY := '1';--有客户认证方式
               --查询客户今天当前认证方式已使用累计限额
               BEGIN
                    SELECT CPALU.DAY_SUM_LIMIT INTO V_DAY_SUM_LIMIT_USE
                    FROM CUST_PAY_AUTH_LIM_USE CPALU
                    WHERE CPALU.EBUSER_NO = V_EBUSER_NO AND CPALU.SEC_WAY = V_SEC_WAY_DATA.SEC_WAY
                          AND CPALU.CCY = V_CCY AND CPALU.LAST_TRAN_DATE = V_TRANS_DATE;
                    EXCEPTION
                         WHEN NO_DATA_FOUND THEN
                          V_DAY_SUM_LIMIT_USE := 0;
               END;
               --查询客户当前认证方式的单笔限额和单日累计限额
               BEGIN
                   SELECT CPASL.ONCE_LIMIT,CPASL.DAY_LIMIT INTO V_ONCE_LIMIT,V_DAY_LIMIT
                   FROM CUST_PAY_AUTH_SET_LIMT CPASL
                   WHERE CPASL.EBUSER_NO = V_EBUSER_NO AND CPASL.SEC_WAY = V_SEC_WAY_DATA.SEC_WAY
                         AND CPASL.CCY = V_CCY;
                   EXCEPTION
                         WHEN NO_DATA_FOUND THEN
                         V_IS_CUST_AUTH_LIMIT := '0';
               END;
               --如果客户没有自设限额，则查询渠道系统默认限额
               IF V_IS_CUST_AUTH_LIMIT = '0' THEN
                  SELECT PALD.ONCE_LIMIT,PALD.DAY_LIMIT INTO V_ONCE_LIMIT,V_DAY_LIMIT
                  FROM PAY_AUTH_LIMT_DEF PALD
                  WHERE PALD.SEC_WAY = V_SEC_WAY_DATA.SEC_WAY AND PALD.CCY = V_CCY;
               END IF;
               --如果客户已使用限额大于 单日累计额度，则剩余额度为0
               --否则剩余= 单日累计限额-客户单日已使用限额
               IF V_DAY_SUM_LIMIT_USE > V_DAY_LIMIT THEN
                  V_DAY_SUM_LIMIT_RESIDUAL := 0;
               ELSE
                  V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT- V_DAY_SUM_LIMIT_USE;
               END IF;
               dbms_output.put_line('客户的安全认证方式row：'||V_SEC_WAY_LIST%ROWCOUNT||'，SEC_WAY：'||V_SEC_WAY_DATA.SEC_WAY);
               dbms_output.put_line('客户认证方式：'||V_SEC_WAY_DATA.SEC_WAY||'，客户单笔限额：'||V_ONCE_LIMIT||'，客户剩余额度：'||V_DAY_SUM_LIMIT_RESIDUAL);
               --为返回数据设置值
               V_PAY_LIMIT_LIST_DATA.EXTEND;
               V_PAY_LIMIT_LIST_DATA(V_PAY_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT(V_SEC_WAY_DATA.SEC_WAY,V_SEC_WAY_DATA.SEC_WAY_NAME,V_ONCE_LIMIT,V_DAY_SUM_LIMIT_RESIDUAL,V_DAY_LIMIT);
         END LOOP;
         CLOSE V_SEC_WAY_LIST;
         IF V_HAS_SEC_WAY = '0' THEN
             DBMS_OUTPUT.PUT_LINE('无认证方式');
             RAISE V_EXCEPTION;
             RETURN;
         END IF;
       END;
       FOR V_INDEX IN V_PAY_LIMIT_LIST_DATA.FIRST .. V_PAY_LIMIT_LIST_DATA.LAST LOOP
           dbms_output.put_line('V_PAY_LIMIT_LIST_DATA集合的认证方式'||V_PAY_LIMIT_LIST_DATA(V_INDEX).SEC_WAY||
                   '单笔限额'||V_PAY_LIMIT_LIST_DATA(V_INDEX).ONCE_LIMIT||
                   '剩余额度'||V_PAY_LIMIT_LIST_DATA(V_INDEX).AVAIL_DAY_LIMIT||
                   '单日累计额度'||V_PAY_LIMIT_LIST_DATA(V_INDEX).DAY_LIMIT);
       END LOOP;
       --循环赋值
       OPEN V_PAY_LIMIT_LIST FOR SELECT SEC_WAY,SEC_WAY_NAME,ONCE_LIMIT,AVAIL_DAY_LIMIT,DAY_LIMIT FROM TABLE(CAST(V_PAY_LIMIT_LIST_DATA AS LIMIT_LIST_TYPE));
       COMMIT;
       EXCEPTION
            WHEN V_EXCEPTION THEN
               DBMS_OUTPUT.PUT_LINE('sqlcode : ' || sqlcode);
               DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || sqlerrm);
               ROLLBACK TO SAVEPOINT PT2;
               COMMIT;
               DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
     END PAY_LIMIT_QUERY;
     --查询访客支付限额
    PROCEDURE VISIT_PAY_LIMIT_QUERY(V_ACC_NO   IN VARCHAR2,--付款账号
                          V_VISIT_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE)
   AS
   V_EXCEPTION EXCEPTION; --自定义异常类型
   V_DAY_SUM_LIMIT_USE NUMBER(18,4);--客户单日已使用限额
   V_CCY VARCHAR2(3);
   V_ONCE_LIMIT NUMBER(18,4);--单笔限额
   V_DAY_LIMIT NUMBER(18,4);--单日累计限额
   V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18,4); -- 客户单日剩余额度
   V_VISIT_PAY_LIMIT_LIST_DATA LIMIT_LIST_TYPE := LIMIT_LIST_TYPE();
   BEGIN
        V_CCY := 'CNY'; --人民币
        SAVEPOINT PT2; --回滚点
        BEGIN
           --查询该账号已使用累计限额
           BEGIN
             SELECT VPALU.DAY_SUM_LIMIT INTO V_DAY_SUM_LIMIT_USE
               FROM VISIT_PAY_AUTH_LIM_USE VPALU
              WHERE VPALU.ACCT_NO = V_ACC_NO
                AND VPALU.LAST_TRAN_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD')
                AND VPALU.CCY = V_CCY;
           EXCEPTION
             WHEN NO_DATA_FOUND THEN
               V_DAY_SUM_LIMIT_USE := 0;
           END;
           --查询访客单笔与单日累计限额
           BEGIN
               SELECT VPLD.ONCE_LIMIT,VPLD.DAY_LIMIT INTO V_ONCE_LIMIT,V_DAY_LIMIT
               FROM VISIT_PAY_LIMT_DEF VPLD WHERE VPLD.CCY = V_CCY;
               EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                    RAISE V_EXCEPTION;
                    RETURN;
           END;
           V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT-V_DAY_SUM_LIMIT_USE;
           --为返回数据设置值
           V_VISIT_PAY_LIMIT_LIST_DATA.EXTEND;
           V_VISIT_PAY_LIMIT_LIST_DATA(V_VISIT_PAY_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT('','',V_ONCE_LIMIT,V_DAY_SUM_LIMIT_RESIDUAL,V_DAY_LIMIT);
       END;
       FOR V_INDEX IN V_VISIT_PAY_LIMIT_LIST_DATA.FIRST .. V_VISIT_PAY_LIMIT_LIST_DATA.LAST LOOP
           dbms_output.put_line('V_VISIT_PAY_LIMIT_LIST_DATA集合'||
                   '单笔限额'||V_VISIT_PAY_LIMIT_LIST_DATA(V_INDEX).ONCE_LIMIT||
                   '剩余额度'||V_VISIT_PAY_LIMIT_LIST_DATA(V_INDEX).AVAIL_DAY_LIMIT||
                   '单日累计额度'||V_VISIT_PAY_LIMIT_LIST_DATA(V_INDEX).DAY_LIMIT);
       END LOOP;
       --循环赋值
       OPEN V_VISIT_PAY_LIMIT_LIST FOR SELECT SEC_WAY,SEC_WAY_NAME,ONCE_LIMIT,AVAIL_DAY_LIMIT,DAY_LIMIT FROM TABLE(CAST(V_VISIT_PAY_LIMIT_LIST_DATA AS LIMIT_LIST_TYPE));
       COMMIT;
       EXCEPTION
            WHEN V_EXCEPTION THEN
               DBMS_OUTPUT.PUT_LINE('sqlcode : ' || sqlcode);
               DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || sqlerrm);
               ROLLBACK TO SAVEPOINT PT2;
               COMMIT;
               DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
     END VISIT_PAY_LIMIT_QUERY;
END PKGH_QUERY_LIMIT;

/
SHOW ERRORS;

CREATE or replace PACKAGE BODY PKGH_SOTP_LIMIT_CHECK IS
  --智能转账限额校验
  --V_CHAN_TYP 渠道号
  --V_EBUSER_NO  电子银行客户号
  --V_SEC_WAY  认证方式
  --V_TRANS_AMT  交易金额
  --V_TRANS_DATE  交易日期
  --LMT_CTL_FLAG  错误代码
  --LMT_VALUE   单笔限额/剩余额度
  PROCEDURE SOTP_TRANS_LMT_CTL(V_CHAN_TYP   IN VARCHAR2,
                               V_EBUSER_NO  IN VARCHAR2,
                               V_SEC_WAY    IN VARCHAR2,
                               V_TRANS_AMT  IN NUMBER,
                               V_TRANS_DATE IN VARCHAR2,
                               --    V_IS_CHECK_CHAN_LIMIT IN VARCHAR2,--是否校验渠道累计限额
                               LMT_CTL_FLAG OUT VARCHAR2,
                               LMT_VALUE    OUT VARCHAR2) --单笔限额/剩余额度
    --渠道类型，电子客户号，认证方式，转账金额，转账日期
   IS
    CASL_ONCE_LIMIT    NUMBER(18, 4); --用户自设单笔限额
    CASL_DAY_LIMIT     NUMBER(18, 4); --用户自设单日累计限额
    CALD_ONCE_LIMIT    NUMBER(18, 4); --渠道认证方式默认单笔限额
    CALD_DAY_LIMIT     NUMBER(18, 4); --渠道认证方式默认单日累计限额
    CALU_DAY_SUM_LIMIT NUMBER(18, 4); --客户认证方式限额使用情况
    V_EXCEPTION EXCEPTION; --自定义异常类型
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    P_BLD_ONCE_LIMIT       NUMBER(18, 4); --个人客户单笔落地限额
    P_BLD_DAY_LIMIT        NUMBER(18, 4); --个人客户单日累计落地限额
    CLU_DAY_SUM_LIMIT      NUMBER(18, 4); --个人客户当日已使用限额
    CLU_DAY_SUM_LIMIT_NEW  NUMBER(18, 4); --个人客户当日已使用限额+ 当前转账金额
    B_BLD_ONCE_LIMIT       NUMBER(18, 4); --全行客户单笔限额
    B_BLD_DAY_LIMIT        NUMBER(18, 4); --全行客户单日累计限额
    IS_CUST_AUTH_LIMIT     VARCHAR2(1); --是否有客户自设限额 0--没有 1--有
    IS_BANK_LIMIT          VARCHAR2(1); --是否有全行客户限额控制 0--没有 1--有
    v_CCY                    VARCHAR2(3); --币别
    V_IS_HAS_SOTP          VARCHAR2(1); --1 ：开通sotp  0 未开通sotp
    V_UNUSE                VARCHAR2(3); --无效值
    --20161111--add-feixiaobo-增加日累计笔数、年累计限额校验、渠道限额限额
    V_DAY_TOTAL           INTEGER; --单日转账次数
    V_DAY_TOTAL_USE       INTEGER; --单日已转账次数
    V_DAY_TOTAL_USE_NEW   INTEGER; --单日最新转账次数
    V_YEAR_LIMIT          NUMBER(18, 4); --单年累计限额
    V_YEAR_LIMIT_USE      NUMBER(18, 4); --单年已使用限额
    V_YEAR_LIMIT_USE_NEW  NUMBER(18, 4); --单年最新使用限额
    V_CHAN_LIMIT          NUMBER(18, 4); --当前渠道限额
    V_CHAN_LIMIT_USE      NUMBER(18, 4); --当前渠道已使用限额
    V_CHAN_LIMIT_USE_NEW  NUMBER(18, 4); --当前渠道最新使用限额
    V_IS_CHECK_CHAN_LIMIT VARCHAR2(1); -- 默认不检查渠道累计限额
    --20161111--add-feixiaobo-增加日累计笔数、年累计限额校验、渠道限额限额
    --add--by--rc--20180630--增加客户版本号判断
    V_OPEN_WAY            VARCHAR2(4);  -- 用户版本类型   01-大众版  02-专业版
    --add--by--rc--20180630--增加非@盾认证方式默认限额表
  BEGIN
    --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单笔累计限额
    --3：大于系统默认单笔限额 4：大于系统默认单笔累计限额
    --5: 大于客户单笔落地限额 6：大于客户单日累计落地限额
    --7：大于全行客户单笔限额 8：大于全行客户单日累计限额 9999异常情况
    LMT_CTL_FLAG          := '9999';
   v_CCY                   := 'CNY';
    LMT_VALUE             := ''; --默认为空
    V_IS_CHECK_CHAN_LIMIT := 'N';
    V_OPEN_WAY := '02';                 --默认专业版
    --回滚点
    SAVEPOINT PT1;

    --add by rc 判断用户是大众版还是专业版
    IF V_CHAN_TYP = '059' THEN
      SELECT G.OPEN_WAY
        INTO V_OPEN_WAY
        FROM USER_CHANNEL G
       WHERE G.EBUSER_NO = V_EBUSER_NO
         AND G.CHAN_TYP = V_CHAN_TYP;
    END IF;
    DBMS_OUTPUT.PUT_LINE('V_OPEN_WAY===>' || V_OPEN_WAY);
    --add by rc 判断用户是大众版还是专业版

    --渠道认证方式限额校验
    BEGIN
      BEGIN
        BEGIN
          --查询客户渠道认证方式自设限额,单日转账次数、年限额
          BEGIN
            SELECT CASL.ONCE_LIMIT,
                   CASL.DAY_LIMIT,
                   CASL.DAY_TOTAL,
                   CASL.YEAR_LIMIT
              INTO CASL_ONCE_LIMIT,
                   CASL_DAY_LIMIT,
                   V_DAY_TOTAL,
                   V_YEAR_LIMIT
              FROM CUST_CH_AU_SELF_LIM CASL
             WHERE CASL.EBUSER_NO = V_EBUSER_NO
               AND CASL.CHAN_TYP = V_CHAN_TYP
               AND CASL.SEC_WAY = V_SEC_WAY
               AND CASL.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              IS_CUST_AUTH_LIMIT := '0';
          END;
          --查询sotp认证方式
          BEGIN
            SELECT SP.SEC_WAY
              INTO V_UNUSE
              FROM P_SEC_AUTH SP
             WHERE SP.SEC_WAY = '00'
               AND SP.CHAN_TYP = V_CHAN_TYP
               AND SP.EBUSER_NO = V_EBUSER_NO
               AND SP.SEC_STATE = 'N';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              V_IS_HAS_SOTP := '0'; --无stop认证方式
          END;
          DBMS_OUTPUT.PUT_LINE('客户是否自设限额：' || IS_CUST_AUTH_LIMIT);
          --查询客户渠道认证方式当日已累计限额，单日转账次数
          BEGIN
            SELECT CALU.DAY_SUM_LIMIT, NVL(CALU.DAY_TOTAL, 0)
              INTO CALU_DAY_SUM_LIMIT, V_DAY_TOTAL_USE
              FROM CUST_CHAN_AUTH_LIM_USE CALU
             WHERE CALU.EBUSER_NO = V_EBUSER_NO
               AND CALU.CHAN_TYP = V_CHAN_TYP
               AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
               AND CALU.SEC_WAY = V_SEC_WAY
               AND CALU.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              CALU_DAY_SUM_LIMIT := 0;
              V_DAY_TOTAL_USE    := 0;
          END;
          --2016-11-11-校验渠道限额，则查询渠道累计限额
          IF V_IS_CHECK_CHAN_LIMIT = 'Y' THEN
            BEGIN
              SELECT CCLU.DAY_SUM_LIMIT
                INTO V_CHAN_LIMIT_USE
                FROM CUST_CHAN_LIM_USE CCLU
               WHERE CCLU.EBUSER_NO = V_EBUSER_NO
                 AND CCLU.CHAN_TYP = V_CHAN_TYP
                 AND CCLU.LAST_TRAN_DATE = V_TRANS_DATE
                 AND CCLU.CCY = V_CCY;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                V_CHAN_LIMIT_USE := 0;
            END;
            --最新渠道已使用限额
            V_CHAN_LIMIT_USE_NEW := V_CHAN_LIMIT_USE + V_TRANS_AMT;

          END IF;

          --2016-11-11-add-feixiaobo--查询渠道认证方式年累计限额
          BEGIN
            SELECT CALU.YEAR_SUM_LIMIT
              INTO V_YEAR_LIMIT_USE
              FROM CUST_CHAN_AUTH_LIM_USE CALU
             WHERE CALU.EBUSER_NO = V_EBUSER_NO
               AND CALU.CHAN_TYP = V_CHAN_TYP
               AND CALU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
               AND CALU.SEC_WAY = V_SEC_WAY
               AND CALU.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              V_YEAR_LIMIT_USE := 0;
          END;
          --最新的日转账次数=当前转账次数+1
          V_DAY_TOTAL_USE_NEW := V_DAY_TOTAL_USE + 1;
          --最新的年累计限额=当前转账金额+已使用限额
          V_YEAR_LIMIT_USE_NEW := V_YEAR_LIMIT_USE + V_TRANS_AMT;

          --2016-11-11-modify-by-feixiaobo--增加转账次数、年累计限额

          --最新的日累计限额=当前转账金额+客户渠道认证方式当日已累计金额
          CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT + V_TRANS_AMT;

          DBMS_OUTPUT.PUT_LINE('客户渠道认证方式当日已累计限额：' ||
                               CALU_DAY_SUM_LIMIT_NEW ||
                               '客户渠道认证方式当日已累计笔数：' || V_DAY_TOTAL_USE_NEW ||
                               '客户渠道认证方式当年已累计限额：' || V_YEAR_LIMIT_USE_NEW);
        END;
        --当客户渠道认证方式自设限额无值时
        IF IS_CUST_AUTH_LIMIT = '0' THEN
          --sotp未开通 或者 网银渠道查询默认
          IF V_IS_HAS_SOTP = '0' OR V_CHAN_TYP = '060' THEN
            --查询渠道认证方式默认限额
            BEGIN
              SELECT CALD.ONCE_LIMIT,
                     CALD.DAY_LIMIT,
                     CALD.DAY_TOTAL,
                     CALD.YEAR_LIMIT
                INTO CALD_ONCE_LIMIT,
                     CALD_DAY_LIMIT,
                     V_DAY_TOTAL,
                     V_YEAR_LIMIT
                FROM CHAN_AUTH_LIM_DEF CALD
               WHERE CALD.CHAN_TYP = V_CHAN_TYP
                 AND CALD.SEC_WAY = V_SEC_WAY
                 AND CALD.CCY = V_CCY
                 -- alter by rc 增加版本号查询
                 AND CALD.OPEN_WAY = V_OPEN_WAY;
                 -- alter by rc 增加版本号查询;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                RAISE V_EXCEPTION;
            END;
          ELSE
            --查询sotp单笔及单日累计限额
            BEGIN
              SELECT SL.ONCE_LIMIT,
                     SL.DAY_LIMIT,
                     SL.DAY_TOTAL,
                     SL.YEAR_LIMIT
                INTO CALD_ONCE_LIMIT,
                     CALD_DAY_LIMIT,
                     V_DAY_TOTAL,
                     V_YEAR_LIMIT
                FROM SOTP_TRAN_AUTH_DEFL_LIMIT SL
               WHERE SL.CHAN_TYP = V_CHAN_TYP
                 AND SL.SEC_WAY = V_SEC_WAY
                 AND SL.CCY = V_CCY
                 AND SL.STATUS = '0' --正常
                 AND SL.TRAN_TYPE = '1'; --转账
            END;
          END IF;
          DBMS_OUTPUT.PUT_LINE('渠道认证方式单笔、单日累计、单日转账、年累计：' ||
                               CALD_ONCE_LIMIT || ',' || CALD_DAY_LIMIT || ',' ||
                               V_DAY_TOTAL || ',' || V_YEAR_LIMIT);
          --判断转账金额 是否 大于 系统渠道认证默认单笔限额
          IF V_TRANS_AMT > CALD_ONCE_LIMIT THEN
            LMT_CTL_FLAG := '3';
            LMT_VALUE    := CAST(CALD_ONCE_LIMIT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
            --判断当前转账金额+客户渠道认证方式当日已累计金额 > 系统渠道认证默认当日转账金额限额
          ELSIF CALU_DAY_SUM_LIMIT_NEW > CALD_DAY_LIMIT THEN
            LMT_CTL_FLAG := '4';
            LMT_VALUE    := CAST(CALD_DAY_LIMIT - CALU_DAY_SUM_LIMIT AS
                                 VARCHAR2); --剩余可用额度
            RAISE V_EXCEPTION;
            RETURN;
            --2016-11-11-add-feixiaobo-增加日转账次数校验
          ELSIF V_DAY_TOTAL_USE_NEW > V_DAY_TOTAL THEN
            LMT_CTL_FLAG := '9';
            LMT_VALUE    := CAST(V_DAY_TOTAL - V_DAY_TOTAL_USE AS VARCHAR2); --当日剩余转账次数
            RAISE V_EXCEPTION;
            RETURN;
            --2016-11-11--add--feixiaobo-增加年累计限额校验
          ELSIF V_YEAR_LIMIT_USE_NEW > V_YEAR_LIMIT THEN
            LMT_CTL_FLAG := '10';
            LMT_VALUE    := CAST(V_YEAR_LIMIT - V_YEAR_LIMIT_USE AS
                                 VARCHAR2); --当年剩余可用额度
            RAISE V_EXCEPTION;
            RETURN;

          ELSE
            LMT_CTL_FLAG := '0';
          END IF;
          --判断转账金额 是否 大于 客户渠道认证自设单笔限额
        ELSIF V_TRANS_AMT > CASL_ONCE_LIMIT THEN
          LMT_CTL_FLAG := '1';
          LMT_VALUE    := CAST(CASL_ONCE_LIMIT AS VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
          --判断当前转账金额+客户渠道认证方式当日已累计金额 > 客户渠道认证方式当日转账累计限额
        ELSIF CALU_DAY_SUM_LIMIT_NEW > CASL_DAY_LIMIT THEN
          LMT_CTL_FLAG := '2';
          LMT_VALUE    := CAST(CASL_DAY_LIMIT - CALU_DAY_SUM_LIMIT AS
                               VARCHAR2); --剩余可用额度
          RAISE V_EXCEPTION;
          RETURN;
        ELSIF V_DAY_TOTAL_USE_NEW > V_DAY_TOTAL THEN
          LMT_CTL_FLAG := '11';
          LMT_VALUE    := CAST(V_DAY_TOTAL - V_DAY_TOTAL_USE AS VARCHAR2); --当日剩余转账次数
          RAISE V_EXCEPTION;
          RETURN;
          --2016-11-11--add--feixiaobo-增加年累计限额校验
        ELSIF V_YEAR_LIMIT_USE_NEW > V_YEAR_LIMIT THEN
          LMT_CTL_FLAG := '12';
          LMT_VALUE    := CAST(V_YEAR_LIMIT - V_YEAR_LIMIT_USE AS VARCHAR2); --当年剩余可用额度
          RAISE V_EXCEPTION;
          RETURN;

        ELSE
          LMT_CTL_FLAG := '0';
        END IF;
      END;
      --落地限额校验
      BEGIN
        --客户已使用限额
        BEGIN
          SELECT CLU.DAY_SUM_LIMIT
            INTO CLU_DAY_SUM_LIMIT
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
                -- AND CLU.CHAN_TYP = V_CHAN_TYP
             AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
             AND CLU.CCY = V_CCY;
        EXCEPTION
          --无数据时，默认为0
          WHEN NO_DATA_FOUND THEN
            CLU_DAY_SUM_LIMIT := 0;
        END;
        CLU_DAY_SUM_LIMIT_NEW := CLU_DAY_SUM_LIMIT + V_TRANS_AMT;
        --网银,手机银行渠道校验落地限额
        IF V_CHAN_TYP = '060' OR V_CHAN_TYP = '059' THEN
          DBMS_OUTPUT.PUT_LINE('进入落地校验：' || V_CHAN_TYP);
          --查询落地限额；
          BEGIN
            SELECT BLD.ONCE_LIMIT, BLD.DAY_LIMIT
              INTO P_BLD_ONCE_LIMIT, P_BLD_DAY_LIMIT
              FROM BANK_LIMIT_DEF BLD
             WHERE BLD.BANK_LIMIT_TY = 'P'
               AND BLD.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              LMT_CTL_FLAG := '9999';
              RAISE V_EXCEPTION;
          END;
          --判断转账金额 是否大于 单笔落地限额
          IF V_TRANS_AMT > P_BLD_ONCE_LIMIT THEN
            LMT_CTL_FLAG := '5';
            LMT_VALUE    := CAST(P_BLD_ONCE_LIMIT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
            --判断转账金额+当日已使用限额 是否大于 单日累计落地限额
          ELSIF CLU_DAY_SUM_LIMIT_NEW > P_BLD_DAY_LIMIT THEN
            LMT_CTL_FLAG := '6';
            LMT_VALUE    := CAST(P_BLD_DAY_LIMIT - CALU_DAY_SUM_LIMIT AS
                                 VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
          ELSE
            LMT_CTL_FLAG := '0';
          END IF;
        END IF;
      END;
      --全行客户限额校验
      BEGIN
        --查询全行客户限额
        BEGIN
          SELECT BLD.ONCE_LIMIT, BLD.DAY_LIMIT
            INTO B_BLD_ONCE_LIMIT, B_BLD_DAY_LIMIT
            FROM BANK_LIMIT_DEF BLD
           WHERE BLD.BANK_LIMIT_TY = 'B'
             AND BLD.CCY = V_CCY;
        EXCEPTION
          --没有数据也算正常
          WHEN NO_DATA_FOUND THEN
            IS_BANK_LIMIT := '0';
        END;
        --转账金额 大于 全行客户单笔限额
        IF IS_BANK_LIMIT <> '0' AND V_TRANS_AMT > B_BLD_ONCE_LIMIT THEN
          LMT_CTL_FLAG := '7';
          LMT_VALUE    := CAST(B_BLD_ONCE_LIMIT AS VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
        ELSIF IS_BANK_LIMIT <> '0' AND
              CLU_DAY_SUM_LIMIT_NEW > B_BLD_DAY_LIMIT THEN
          LMT_CTL_FLAG := '8';
          LMT_VALUE    := CAST(B_BLD_DAY_LIMIT - CALU_DAY_SUM_LIMIT AS
                               VARCHAR2);
          RAISE V_EXCEPTION;
          RETURN;
        ELSE
          LMT_CTL_FLAG := '0';
        END IF;
      END;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('限额校验状态' || LMT_CTL_FLAG);
      -- LMT_CTL_FLAG <> '0',即认为验证失败,触发异常,事务回滚到回滚点
      IF LMT_CTL_FLAG <> '0' THEN
        DBMS_OUTPUT.PUT_LINE('V_EXCEPTION START');
        RAISE V_EXCEPTION;
      END IF;
      --异常处理机制
    EXCEPTION
      WHEN V_EXCEPTION THEN
        DBMS_OUTPUT.PUT_LINE('EXCEPTION START');
        DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
        ROLLBACK TO SAVEPOINT PT1;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
    END;
  END SOTP_TRANS_LMT_CTL;
  --转账限额累计
  PROCEDURE SOTP_TRANS_LMT_UPDATE(V_CHAN_TYP   IN VARCHAR2,
                                  V_EBUSER_NO  IN VARCHAR2,
                                  V_SEC_WAY    IN VARCHAR2,
                                  V_TRANS_AMT  IN NUMBER,
                                  V_TRANS_DATE IN VARCHAR2,
                                  OUT_STATUS   OUT VARCHAR2) IS
    CALU_DAY_SUM_LIMIT     NUMBER(18, 4); --客户认证方式限额使用情况
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    CLU_DAY_SUM_LIMIT      NUMBER(18, 4); --个人客户当日已使用限额
    CLU_DAY_SUM_LIMIT_NEW  NUMBER(18, 4); --个人客户当日已使用限额+ 当前转账金额
    --2016-11-11-ADD-FEIXIAOBO--增加单日转账次数、年累计限额、渠道单日累计、单日转账次数、单年累计限额
    V_DAY_TOTAL_USE           INTEGER; --渠道认证方式对应
    V_DAY_TOTAL_USE_NEW       INTEGER;
    V_YEAR_LIMIT_USE          NUMBER(18, 4);
    V_YEAR_LIMIT_USE_NEW      NUMBER(18, 4);
    V_CHAN_DAY_LIMIT_USE      NUMBER(18, 4); --渠道对应
    V_CHAN_DAY_LIMIT_USE_NEW  NUMBER(18, 4);
    V_CHAN_DAY_TOTAL_USE      INTEGER;
    V_CHAN_DAY_TOTAL_USE_NEW  INTEGER;
    V_CHAN_YEAR_LIMIT_USE     NUMBER(18, 4);
    V_CHAN_YEAR_LIMIT_USE_NEW NUMBER(18, 4);
    IS_HAS_SEC_DAY            VARCHAR2(1); --0:没有数据  1-有数据
    IS_HAS_SEC_YEAR           VARCHAR2(1); --0:没有数据  1-有数据
    IS_HAS_CHAN_DAY           VARCHAR2(1); --0:没有数据  1-有数据 渠道累计限额
    IS_HAS_CHAN_YEAR          VARCHAR2(1); --0:没有数据  1-有数据
    IS_HAS_CUST_DAY           VARCHAR2(1); --0:没有数据  1-有数据 客户累计限额
    IS_HAS_CUST_YEAR          VARCHAR2(1); --0:没有数据  1-有数据
    V_CUST_DAY_TOTAL_USE      INTEGER; --客户单日转账次数
    V_CUST_DAY_TOTAL_USE_NEW  INTEGER; --客户单日最新转账次数
    V_CUST_YEAR_LIMIT_USE     NUMBER(18, 4); --客户当年转账累计限额
    V_CUST_YEAR_LIMIT_USE_NEW NUMBER(18, 4); --客户当年最新累计限额
    --2016-11-11-ADD-FEIXIAOBO--增加单日转账次数、年累计限额、渠道单日累计、单日转账次数、单年累计限额
    v_CCY VARCHAR2(3); --币别
    --IS_INSERT_CALU         VARCHAR2(1); --是否是新增 0：不是 1：是
    --IS_INSERT_CLU          VARCHAR2(1); --是否是新增 0：不是 1：是
  BEGIN
    v_CCY := 'CNY';
    --回滚点
    SAVEPOINT PT1;
    BEGIN
      --查询客户渠道认证方式当日已累计限额、当日已累计笔数
      BEGIN
        SELECT CALU.DAY_SUM_LIMIT, NVL(CALU.DAY_TOTAL, 0)
          INTO CALU_DAY_SUM_LIMIT, V_DAY_TOTAL_USE
          FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CALU_DAY_SUM_LIMIT := 0;
          V_DAY_TOTAL_USE    := 0; --2016-11-11 默认累计为0
          IS_HAS_SEC_DAY     := '0'; --无单日累计限额
          --IS_INSERT_CALU     := '1';
          DBMS_OUTPUT.PUT_LINE('客户已累计限额及笔数：' || CALU_DAY_SUM_LIMIT || ',' ||
                               V_DAY_TOTAL_USE);
      END;
      --查询客户渠道认证方式当年累计限额
      BEGIN
        SELECT CALU.YEAR_SUM_LIMIT
          INTO V_YEAR_LIMIT_USE
          FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_YEAR_LIMIT_USE := 0; --2016-11-11 默认累计为0
          IS_HAS_SEC_YEAR  := '0'; --无单年累计限额
          --IS_INSERT_CALU     := '1';
          DBMS_OUTPUT.PUT_LINE('客户当年已累计限额：' || V_YEAR_LIMIT_USE);
      END;
      --当前转账金额+客户渠道认证方式当日已累计金额
      CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT + V_TRANS_AMT;
      --20161111-ADD-FEIXIAOBO-年累计限额、日累计笔数
      V_YEAR_LIMIT_USE_NEW := V_YEAR_LIMIT_USE + V_TRANS_AMT;
      V_DAY_TOTAL_USE_NEW  := V_DAY_TOTAL_USE + 1;
      DBMS_OUTPUT.PUT_LINE('客户渠道认证方式日累计笔数：' || V_DAY_TOTAL_USE_NEW);

      --无当日累计限额 且 无当年累计限额,则新增数据
      IF IS_HAS_SEC_DAY = '0' AND IS_HAS_SEC_YEAR = '0' THEN
        DBMS_OUTPUT.PUT_LINE('INSERT 客户渠道认证方式当日累计限额、日累计笔数、年累计限额：' ||
                             CALU_DAY_SUM_LIMIT_NEW || ',' ||
                             V_DAY_TOTAL_USE_NEW || ',' ||
                             V_YEAR_LIMIT_USE_NEW);
        DELETE FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
        --INSERT 客户渠道认证方式当日已累计限额、已累计鄙视，年累计限额
        INSERT INTO CUST_CHAN_AUTH_LIM_USE
          (EBUSER_NO,
           CHAN_TYP,
           SEC_WAY,
           CCY,
           DAY_SUM_LIMIT,
           LAST_TRAN_DATE,
           DAY_TOTAL,
           YEAR_SUM_LIMIT,
           LAST_YEAR)
        VALUES
          (V_EBUSER_NO,
           V_CHAN_TYP,
           V_SEC_WAY,
           v_CCY,
           CALU_DAY_SUM_LIMIT_NEW,
           V_TRANS_DATE,
           V_DAY_TOTAL_USE_NEW,
           V_YEAR_LIMIT_USE_NEW,
           SUBSTR(V_TRANS_DATE, 0, 4));
      ELSE
        --IF IS_HAS_SEC_DAY != '0' AND IS_HAS_SEC_YEAR = '0' THEN
        DBMS_OUTPUT.PUT_LINE('UPDATE 客户渠道认证方式当日累计限额、日累计笔数、年累计限额：' ||
                             CALU_DAY_SUM_LIMIT_NEW || ',' ||
                             V_DAY_TOTAL_USE_NEW || ',' ||
                             V_YEAR_LIMIT_USE_NEW);
        --更新客户渠道认证方式已使用限额
        UPDATE CUST_CHAN_AUTH_LIM_USE CALU
           SET CALU.DAY_SUM_LIMIT  = CALU_DAY_SUM_LIMIT_NEW,
               CALU.LAST_TRAN_DATE = V_TRANS_DATE,
               CALU.DAY_TOTAL      = V_DAY_TOTAL_USE_NEW,
               CALU.YEAR_SUM_LIMIT = V_YEAR_LIMIT_USE_NEW,
               CALU.LAST_YEAR      = SUBSTR(V_TRANS_DATE, 0, 4)
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
        -- AND CALU.LAST_TRAN_DATE = V_TRANS_DATE;
        --ELSIF IS_HAS_SEC_DAY = '0' AND IS_HAS_SEC_YEAR != '0' THEN
      END IF;
    END;

    --2016-11-11--ADD-FEIXIAOBO--累计渠道的当日累计限额、当日转账笔数、当年累计限额

    BEGIN
      --查询客户渠道当日已累计限额、当日已累计笔数
      BEGIN
        SELECT CCLU.DAY_SUM_LIMIT, NVL(CCLU.DAY_TOTAL, 0)
          INTO V_CHAN_DAY_LIMIT_USE, V_CHAN_DAY_TOTAL_USE
          FROM CUST_CHAN_LIM_USE CCLU
         WHERE CCLU.EBUSER_NO = V_EBUSER_NO
           AND CCLU.CHAN_TYP = V_CHAN_TYP
           AND CCLU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CCLU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_CHAN_DAY_LIMIT_USE := 0;
          V_CHAN_DAY_TOTAL_USE := 0; --2016-11-11 默认累计为0
          IS_HAS_CHAN_DAY      := '0'; --无单日累计限额
          DBMS_OUTPUT.PUT_LINE('客户渠道已累计限额及笔数：' || V_CHAN_DAY_LIMIT_USE || ',' ||
                               V_CHAN_DAY_TOTAL_USE);
      END;
      --查询客户渠道当年累计限额
      BEGIN
        SELECT CCLU.YEAR_SUM_LIMIT
          INTO V_CHAN_YEAR_LIMIT_USE
          FROM CUST_CHAN_LIM_USE CCLU
         WHERE CCLU.EBUSER_NO = V_EBUSER_NO
           AND CCLU.CHAN_TYP = V_CHAN_TYP
           AND CCLU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
           AND CCLU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_CHAN_YEAR_LIMIT_USE := 0; --2016-11-11 默认累计为0
          IS_HAS_CHAN_YEAR      := '0'; --无单年累计限额
          DBMS_OUTPUT.PUT_LINE('客户渠道当年已累计限额：' || V_CHAN_YEAR_LIMIT_USE);
      END;
      --20161111-ADD-FEIXIAOBO-年累计限额、日累计笔数、日累计笔数
      V_CHAN_DAY_LIMIT_USE_NEW  := V_CHAN_DAY_LIMIT_USE + V_TRANS_AMT;
      V_CHAN_YEAR_LIMIT_USE_NEW := V_CHAN_YEAR_LIMIT_USE + V_TRANS_AMT;
      V_CHAN_DAY_TOTAL_USE_NEW  := V_CHAN_DAY_TOTAL_USE + 1;

      --无当日累计限额 且 无当年累计限额,则新增数据
      IF IS_HAS_CHAN_DAY = '0' AND IS_HAS_CHAN_YEAR = '0' THEN
        DBMS_OUTPUT.PUT_LINE('INSERT 客户渠道当日累计限额、日累计笔数、年累计限额：' ||
                             V_CHAN_DAY_LIMIT_USE_NEW || ',' ||
                             V_CHAN_DAY_TOTAL_USE_NEW || ',' ||
                             V_CHAN_YEAR_LIMIT_USE_NEW);
        DELETE FROM CUST_CHAN_LIM_USE CCLU
         WHERE CCLU.EBUSER_NO = V_EBUSER_NO
           AND CCLU.CHAN_TYP = V_CHAN_TYP
           AND CCLU.CCY = V_CCY;
        --INSERT 客户渠道当日已累计限额、已累计鄙视，年累计限额
        INSERT INTO CUST_CHAN_LIM_USE
          (EBUSER_NO,
           CHAN_TYP,
           CCY,
           DAY_SUM_LIMIT,
           LAST_TRAN_DATE,
           DAY_TOTAL,
           YEAR_SUM_LIMIT,
           LAST_YEAR)
        VALUES
          (V_EBUSER_NO,
           V_CHAN_TYP,
           v_CCY,
           V_CHAN_DAY_LIMIT_USE_NEW,
           V_TRANS_DATE,
           V_CHAN_DAY_TOTAL_USE_NEW,
           V_CHAN_YEAR_LIMIT_USE_NEW,
           SUBSTR(V_TRANS_DATE, 0, 4));
      ELSE

        DBMS_OUTPUT.PUT_LINE('UPDATE 客户渠道当日累计限额、日累计笔数、年累计限额：' ||
                             V_CHAN_DAY_LIMIT_USE_NEW || ',' ||
                             V_CHAN_DAY_TOTAL_USE_NEW || ',' ||
                             V_CHAN_YEAR_LIMIT_USE_NEW);
        --更新客户渠道认证方式已使用限额
        UPDATE CUST_CHAN_LIM_USE CCLU
           SET CCLU.DAY_SUM_LIMIT  = CALU_DAY_SUM_LIMIT_NEW,
               CCLU.LAST_TRAN_DATE = V_TRANS_DATE,
               CCLU.DAY_TOTAL      = V_DAY_TOTAL_USE_NEW,
               CCLU.YEAR_SUM_LIMIT = V_YEAR_LIMIT_USE_NEW,
               CCLU.LAST_YEAR      = SUBSTR(V_TRANS_DATE, 0, 4)
         WHERE CCLU.EBUSER_NO = V_EBUSER_NO
           AND CCLU.CHAN_TYP = V_CHAN_TYP
           AND CCLU.CCY = V_CCY;

      END IF;
    END;

    --2016-11-11--ADD-FEIXIAOBO--累计渠道的当日累计限额、当日转账笔数、当年累计限额
    BEGIN
      --网银，手机银行渠道
      IF V_CHAN_TYP = '059' OR V_CHAN_TYP = '060' THEN
        --客户已使用限额、单日转账次数
        BEGIN
          SELECT CLU.DAY_SUM_LIMIT, NVL(CLU.DAY_TOTAL, 0)
            INTO CLU_DAY_SUM_LIMIT, V_CUST_DAY_TOTAL_USE
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
             AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
             AND CLU.CCY = V_CCY;
        EXCEPTION
          --无数据时，默认为0
          WHEN NO_DATA_FOUND THEN
            CLU_DAY_SUM_LIMIT    := 0;
            V_CUST_DAY_TOTAL_USE := 0;
            IS_HAS_CUST_DAY      := '0';
            -- IS_INSERT_CLU     := '1';
        END;
        --客户当年已使用限额
        BEGIN
          SELECT CLU.YEAR_SUM_LIMIT
            INTO V_CUST_YEAR_LIMIT_USE
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
             AND CLU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
             AND CLU.CCY = V_CCY;
        EXCEPTION
          --无数据时，默认为0
          WHEN NO_DATA_FOUND THEN
            V_CUST_YEAR_LIMIT_USE := 0;
            IS_HAS_CUST_YEAR      := '0';
        END;
        CLU_DAY_SUM_LIMIT_NEW     := CLU_DAY_SUM_LIMIT + V_TRANS_AMT;
        V_CUST_DAY_TOTAL_USE_NEW  := V_CUST_DAY_TOTAL_USE + 1; --客户当日转账次数
        V_CUST_YEAR_LIMIT_USE_NEW := V_CUST_YEAR_LIMIT_USE + V_TRANS_AMT; --客户年累计限额

        IF IS_HAS_CUST_DAY = '0' AND IS_HAS_CUST_YEAR = '0' THEN
          --INSERT 客户已使用限额
          DELETE FROM CUST_LIM_USE WHERE EBUSER_NO = V_EBUSER_NO;
          INSERT INTO CUST_LIM_USE
            (EBUSER_NO,
             CCY,
             DAY_SUM_LIMIT,
             LAST_TRAN_DATE,
             DAY_TOTAL,
             YEAR_SUM_LIMIT,
             LAST_YEAR)
          VALUES
            (V_EBUSER_NO,
             v_CCY,
             CLU_DAY_SUM_LIMIT_NEW,
             V_TRANS_DATE,
             V_CUST_DAY_TOTAL_USE_NEW,
             V_CUST_YEAR_LIMIT_USE_NEW,
             SUBSTR(V_TRANS_DATE,0,4));
        ELSE
          --更新客户已使用限额
          UPDATE CUST_LIM_USE CLU
             SET CLU.DAY_SUM_LIMIT  = CLU_DAY_SUM_LIMIT_NEW,
                 CLU.LAST_TRAN_DATE = V_TRANS_DATE,
                 CLU.DAY_TOTAL      = V_CUST_DAY_TOTAL_USE_NEW,
                 CLU.YEAR_SUM_LIMIT = V_CUST_YEAR_LIMIT_USE_NEW,
                 CLU.LAST_YEAR      = SUBSTR(V_TRANS_DATE,0,4)
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
             AND CLU.CCY = V_CCY;
        END IF;
      END IF;
    END;
    DBMS_OUTPUT.PUT_LINE('COMMIT START');
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    OUT_STATUS := 'SUCCESS';
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT1;
      OUT_STATUS := SQLERRM;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END SOTP_TRANS_LMT_UPDATE;
  --转账限额回滚
  PROCEDURE SOTP_TRANS_LMT_ROLLBACK(V_CHAN_TYP   IN VARCHAR2,
                                    V_EBUSER_NO  IN VARCHAR2,
                                    V_SEC_WAY    IN VARCHAR2,
                                    V_TRANS_AMT  IN NUMBER,
                                    V_TRANS_DATE IN VARCHAR2,
                                    OUT_STATUS   OUT VARCHAR2) IS
    CALU_DAY_SUM_LIMIT     NUMBER(18, 4); --客户认证方式限额使用情况
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 - 当前转账金额
    CLU_DAY_SUM_LIMIT      NUMBER(18, 4); --个人客户当日已使用限额
    CLU_DAY_SUM_LIMIT_NEW  NUMBER(18, 4); --个人客户当日已使用限额 - 当前转账金额
    v_CCY                    VARCHAR2(3); --币别
    --add--20161111-feixiaobo-是否有单日累计限额、累计次数，年累计限额
    IS_HAS_SEC_DAY_LIMIT   VARCHAR2(1); -- 0: 没有  1: 有
    IS_HAS_SEC_YEAR_LIMIT  VARCHAR2(1); -- 0: 没有   1： 有
    IS_HAS_CHAN_DAY_LIMIT  VARCHAR2(1); -- 0: 没有  1: 有
    IS_HAS_CHAN_YEAR_LIMIT VARCHAR2(1); -- 0: 没有   1： 有
    IS_HAS_CUST_DAY_LIMIT  VARCHAR2(1); -- 0: 没有  1: 有
    IS_HAS_CUST_YEAR_LIMIT VARCHAR2(1); -- 0: 没有   1： 有

    V_SEC_DAY_TOTAL_USE     INTEGER; --认证方式单日累计次数
    V_SEC_DAY_TOTAL_USE_NEW INTEGER; --认证方式单日回滚累计次数

    V_CHAN_DAY_TOTAL_USE     INTEGER; --渠道单日累计次数
    V_CHAN_DAY_TOTAL_USE_NEW INTEGER; --渠道单日回滚累计次数

    V_CUST_DAY_TOTAL_USE     INTEGER; --客户单日累计次数
    V_CUST_DAY_TOTAL_USE_NEW INTEGER; --客户单日回滚累计次数

    V_SEC_YEAR_LIMIT_USE     NUMBER(18, 4); --认证方式年累计已使用限额
    V_SEC_YEAR_LIMIT_USE_NEW NUMBER(18, 4); --认证方式年累计回滚后限额

    V_CHAN_YEAR_LIMIT_USE     NUMBER(18, 4); --渠道年累计已使用限额
    V_CHAN_YEAR_LIMIT_USE_NEW NUMBER(18, 4); --渠道年累计回滚后限额

    V_CHAN_DAY_LIMIT_USE     NUMBER(18, 4); --渠道日累计已使用限额
    V_CHAN_DAY_LIMIT_USE_NEW NUMBER(18, 4); --渠道日累计回滚后限额

    V_CUST_YEAR_LIMIT_USE     NUMBER(18, 4); --客户年累计已使用限额
    V_CUST_YEAR_LIMIT_USE_NEW NUMBER(18, 4); --客户年累计回滚后限额

    --add--20161111-feixiaobo-是否有单日累计限额、累计次数，年累计限额
    V_EXCEPTION EXCEPTION; --自定义异常类型
  BEGIN
    v_CCY                    := 'CNY';
    IS_HAS_SEC_DAY_LIMIT   := '1'; -- 0: 没有   1： 有
    IS_HAS_SEC_YEAR_LIMIT  := '1'; -- 0: 没有   1： 有
    IS_HAS_CHAN_DAY_LIMIT  := '1'; -- 0: 没有  1: 有
    IS_HAS_CHAN_YEAR_LIMIT := '1'; -- 0: 没有   1： 有
    IS_HAS_CUST_DAY_LIMIT  := '1'; -- 0: 没有  1: 有
    IS_HAS_CUST_YEAR_LIMIT := '1'; -- 0: 没有   1： 有
    DBMS_OUTPUT.PUT_LINE('EBUSER_NO=' || V_EBUSER_NO || ',CHAN_TYP=' ||
                         V_CHAN_TYP || ',SEC_WAY=' || V_SEC_WAY ||
                         ',TRANS_AMT=' || V_TRANS_AMT || ',TRANS_DATE=' ||
                         V_TRANS_DATE);
    --回滚点
    SAVEPOINT PT1;
    BEGIN
      --查询客户渠道认证方式当日已累计限额
      BEGIN
        SELECT CALU.DAY_SUM_LIMIT, CALU.DAY_TOTAL
          INTO CALU_DAY_SUM_LIMIT, V_SEC_DAY_TOTAL_USE
          FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('客户渠道认证方式无日累计限额记录');
          CALU_DAY_SUM_LIMIT   := 0;
          V_SEC_DAY_TOTAL_USE  := 0;
          IS_HAS_SEC_DAY_LIMIT := '0';
          -- OUT_STATUS := 'SUCCESS'; --客户渠道认证方式无限额记录
        --RAISE V_EXCEPTION;
        -- RETURN;
      END;
      -- DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计限额：' || CALU_DAY_SUM_LIMIT);
      --当认证方式的单日累计限额、累计次数有值时，回滚限额
      IF IS_HAS_SEC_DAY_LIMIT != '0' THEN
        -- 认证方式单日累计限额 小于 转账限额，则最新回滚限额为0
        IF CALU_DAY_SUM_LIMIT <= V_TRANS_AMT THEN
          CALU_DAY_SUM_LIMIT_NEW := 0;
        ELSE
          CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT - V_TRANS_AMT;

        END IF;
        --日累计次数
        IF V_SEC_DAY_TOTAL_USE <= 0 THEN
          V_SEC_DAY_TOTAL_USE_NEW := 0;
        ELSE
          V_SEC_DAY_TOTAL_USE_NEW := V_SEC_DAY_TOTAL_USE - 1;

        END IF;

      END IF;
      --2016-11-11-add-feixiaobo--渠道认证方式年累计限额回滚
      BEGIN
        SELECT CALU.YEAR_SUM_LIMIT
          INTO V_SEC_YEAR_LIMIT_USE
          FROM CUST_CHAN_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.CHAN_TYP = V_CHAN_TYP
           AND CALU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('客户渠道认证方式无年累计限额记录');
          V_SEC_YEAR_LIMIT_USE  := 0;
          IS_HAS_SEC_YEAR_LIMIT := '0';
      END;
      IF IS_HAS_SEC_YEAR_LIMIT != '0' THEN
        -- 认证方式单日累计限额 小于 转账限额，则最新回滚限额为0
        IF V_SEC_YEAR_LIMIT_USE <= V_TRANS_AMT THEN
          V_SEC_YEAR_LIMIT_USE_NEW := 0;
        ELSE
          V_SEC_YEAR_LIMIT_USE_NEW := V_SEC_YEAR_LIMIT_USE - V_TRANS_AMT;

        END IF;

      END IF;

      --回滚客户渠道认证方式单日已累计限额，累计鄙视
      UPDATE CUST_CHAN_AUTH_LIM_USE CALU
         SET CALU.DAY_SUM_LIMIT = CALU_DAY_SUM_LIMIT_NEW,
             CALU.DAY_TOTAL     = V_SEC_DAY_TOTAL_USE_NEW
       WHERE CALU.EBUSER_NO = V_EBUSER_NO
         AND CALU.CHAN_TYP = V_CHAN_TYP
         AND CALU.SEC_WAY = V_SEC_WAY
         AND CALU.CCY = V_CCY
         AND CALU.LAST_TRAN_DATE = V_TRANS_DATE;

      --回滚客户渠道认证方式年累计限额
      UPDATE CUST_CHAN_AUTH_LIM_USE CALU
         SET CALU.YEAR_SUM_LIMIT = V_SEC_YEAR_LIMIT_USE_NEW
       WHERE CALU.EBUSER_NO = V_EBUSER_NO
         AND CALU.CHAN_TYP = V_CHAN_TYP
         AND CALU.SEC_WAY = V_SEC_WAY
         AND CALU.CCY = V_CCY
         AND CALU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4);

    END;
    --2016-11-11-add-feixiaobo--渠道认证方式年累计限额回滚

    BEGIN
      --2016-11-11--add-feixiaobo--渠道日累计、次数限额回滚，年限额回滚

      --查询客户渠道当日已累计限额，累计次数
      BEGIN
        SELECT CCLU.DAY_SUM_LIMIT, CCLU.DAY_TOTAL
          INTO V_CHAN_DAY_LIMIT_USE, V_CHAN_DAY_TOTAL_USE
          FROM CUST_CHAN_LIM_USE CCLU
         WHERE CCLU.EBUSER_NO = V_EBUSER_NO
           AND CCLU.CHAN_TYP = V_CHAN_TYP
           AND CCLU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CCLU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('客户渠道无日累计限额记录');
          V_CHAN_DAY_LIMIT_USE  := 0;
          V_CHAN_DAY_TOTAL_USE  := 0;
          IS_HAS_CHAN_DAY_LIMIT := '0';
          -- OUT_STATUS := 'SUCCESS'; --客户渠道认证方式无限额记录
        --RAISE V_EXCEPTION;
        -- RETURN;
      END;
      -- DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计限额：' || CALU_DAY_SUM_LIMIT);
      --当认证方式的单日累计限额、累计次数有值时，回滚限额
      IF IS_HAS_CHAN_DAY_LIMIT != '0' THEN
        -- 认证方式单日累计限额 小于 转账限额，则最新回滚限额为0
        IF V_CHAN_DAY_LIMIT_USE <= V_TRANS_AMT THEN
          V_CHAN_DAY_LIMIT_USE_NEW := 0;
        ELSE
          V_CHAN_DAY_LIMIT_USE_NEW := V_CHAN_DAY_LIMIT_USE - V_TRANS_AMT;

        END IF;
        --日累计次数
        IF V_CHAN_DAY_TOTAL_USE <= 0 THEN
          V_CHAN_DAY_TOTAL_USE_NEW := 0;
        ELSE
          V_CHAN_DAY_TOTAL_USE_NEW := V_CHAN_DAY_TOTAL_USE - 1;

        END IF;

      END IF;
      --2016-11-11-add-feixiaobo--渠道认证方式年累计限额回滚
      BEGIN
        SELECT CCLU.YEAR_SUM_LIMIT
          INTO V_CHAN_YEAR_LIMIT_USE
          FROM CUST_CHAN_LIM_USE CCLU
         WHERE CCLU.EBUSER_NO = V_EBUSER_NO
           AND CCLU.CHAN_TYP = V_CHAN_TYP
           AND CCLU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
           AND CCLU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('客户渠道无年累计限额记录');
          V_CHAN_YEAR_LIMIT_USE  := 0;
          IS_HAS_CHAN_YEAR_LIMIT := '0';
      END;
      IF IS_HAS_CHAN_YEAR_LIMIT != '0' THEN
        -- 认证方式单日累计限额 小于 转账限额，则最新回滚限额为0
        IF V_CHAN_YEAR_LIMIT_USE <= V_TRANS_AMT THEN
          V_CHAN_YEAR_LIMIT_USE_NEW := 0;
        ELSE
          V_CHAN_YEAR_LIMIT_USE_NEW := V_CHAN_YEAR_LIMIT_USE - V_TRANS_AMT;

        END IF;

      END IF;

      --回滚客户渠道认证方式单日已累计限额，累计鄙视
      UPDATE CUST_CHAN_LIM_USE CCLU
         SET CCLU.DAY_SUM_LIMIT = V_CHAN_DAY_LIMIT_USE_NEW,
             CCLU.DAY_TOTAL     = V_CHAN_DAY_TOTAL_USE_NEW
       WHERE CCLU.EBUSER_NO = V_EBUSER_NO
         AND CCLU.CHAN_TYP = V_CHAN_TYP
         AND CCLU.CCY = V_CCY
         AND CCLU.LAST_TRAN_DATE = V_TRANS_DATE;

      --回滚客户渠道认证方式年累计限额
      UPDATE CUST_CHAN_LIM_USE CCLU
         SET CCLU.YEAR_SUM_LIMIT = V_CHAN_YEAR_LIMIT_USE_NEW
       WHERE CCLU.EBUSER_NO = V_EBUSER_NO
         AND CCLU.CHAN_TYP = V_CHAN_TYP
         AND CCLU.CCY = V_CCY
         AND CCLU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4);

      --2016-11-11-add-feixiaobo--渠道日累计限额、次数回滚，年限额回滚

    END;
    BEGIN
      --网银，手机银行渠道
      IF V_CHAN_TYP = '059' OR V_CHAN_TYP = '060' THEN

        --2016-11-11-ADD-FEIXIAOBO--客户日累计限额、次数回滚，年限额回滚

        --查询客户当日已累计限额，累计次数
        BEGIN
          SELECT CLU.DAY_SUM_LIMIT, CLU.DAY_TOTAL
            INTO CLU_DAY_SUM_LIMIT, V_CUST_DAY_TOTAL_USE
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
             AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
             AND CLU.CCY = V_CCY;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('客户无日累计限额记录');
            CLU_DAY_SUM_LIMIT     := 0;
            V_CUST_DAY_TOTAL_USE  := 0;
            IS_HAS_CUST_DAY_LIMIT := '0';
            -- OUT_STATUS := 'SUCCESS'; --客户渠道认证方式无限额记录
          --RAISE V_EXCEPTION;
          -- RETURN;
        END;
        -- DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计限额：' || CALU_DAY_SUM_LIMIT);
        --当认证方式的单日累计限额、累计次数有值时，回滚限额
        IF IS_HAS_CUST_DAY_LIMIT != '0' THEN
          -- 认证方式单日累计限额 小于 转账限额，则最新回滚限额为0
          IF CLU_DAY_SUM_LIMIT <= V_TRANS_AMT THEN
            CLU_DAY_SUM_LIMIT_NEW := 0;
          ELSE
            CLU_DAY_SUM_LIMIT_NEW := CLU_DAY_SUM_LIMIT - V_TRANS_AMT;

          END IF;
          --日累计次数
          IF V_CUST_DAY_TOTAL_USE <= 0 THEN
            V_CUST_DAY_TOTAL_USE_NEW := 0;
          ELSE
            V_CUST_DAY_TOTAL_USE_NEW := V_CUST_DAY_TOTAL_USE - 1;
          END IF;
        END IF;
        --2016-11-11-add-feixiaobo--渠道认证方式年累计限额回滚
        BEGIN
          SELECT CLU.YEAR_SUM_LIMIT
            INTO V_CUST_YEAR_LIMIT_USE
            FROM CUST_LIM_USE CLU
           WHERE CLU.EBUSER_NO = V_EBUSER_NO
             AND CLU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
             AND CLU.CCY = V_CCY;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('客户渠道无年累计限额记录');
            V_CUST_YEAR_LIMIT_USE  := 0;
            IS_HAS_CUST_YEAR_LIMIT := '0';
        END;
        IF IS_HAS_CUST_YEAR_LIMIT != '0' THEN
          -- 认证方式单日累计限额 小于 转账限额，则最新回滚限额为0
          IF V_CUST_YEAR_LIMIT_USE <= V_TRANS_AMT THEN
            V_CUST_YEAR_LIMIT_USE_NEW := 0;
          ELSE
            V_CUST_YEAR_LIMIT_USE_NEW := V_CUST_YEAR_LIMIT_USE -
                                         V_TRANS_AMT;

          END IF;

        END IF;
        --回滚客户单日已使用限额、转账次数；年累计限额
        UPDATE CUST_LIM_USE CLU
           SET CLU.DAY_SUM_LIMIT = CLU_DAY_SUM_LIMIT_NEW,
               CLU.DAY_TOTAL     = V_CUST_DAY_TOTAL_USE_NEW
         WHERE CLU.EBUSER_NO = V_EBUSER_NO
           AND CLU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CLU.CCY = V_CCY;

        UPDATE CUST_LIM_USE CLU
           SET CLU.YEAR_SUM_LIMIT = V_CUST_YEAR_LIMIT_USE_NEW
         WHERE CLU.EBUSER_NO = V_EBUSER_NO
           AND CLU.LAST_YEAR = SUBSTR(V_TRANS_DATE, 0, 4)
           AND CLU.CCY = V_CCY;
        --2016-11-11-add-feixiaobo--客户日累计限额、次数回滚，年限额回滚

      END IF;
    END;

    DBMS_OUTPUT.PUT_LINE('COMMIT START');
    COMMIT;
    OUT_STATUS := 'SUCCESS';
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN V_EXCEPTION THEN
      ROLLBACK TO SAVEPOINT PT1;
      OUT_STATUS := 'ERROR';
      DBMS_OUTPUT.PUT_LINE('V_EXCEPTION OUT_STATUS : ' || OUT_STATUS);
      COMMIT;
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT1;
      OUT_STATUS := SQLERRM;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END SOTP_TRANS_LMT_ROLLBACK;
END PKGH_SOTP_LIMIT_CHECK;
/
SHOW ERRORS;

CREATE or replace PACKAGE BODY PKGH_SOTP_PAY_LIMIT_CHECK IS
  PROCEDURE SOTP_PAY_LMT_CTL(V_CHAN_TYP   IN VARCHAR2,
                             V_EBUSER_NO  IN VARCHAR2,
                             V_SEC_WAY    IN VARCHAR2,
                             V_TRANS_AMT  IN NUMBER,
                             V_TRANS_DATE IN VARCHAR2,
                             LMT_CTL_FLAG OUT VARCHAR2,
                             LMT_VALUE    OUT VARCHAR2) --单笔限额/剩余额度
   IS
    CASL_ONCE_LIMIT     NUMBER(18, 4); --用户自设单笔限额
    CASL_DAY_LIMIT      NUMBER(18, 4); --用户自设单日累计限额
    CALD_ONCE_LIMIT     NUMBER(18, 4); --渠道认证方式默认单笔限额
    CALD_DAY_LIMIT      NUMBER(18, 4); --渠道认证方式默认单日累计限额
    V_DAY_SUM_LIMIT_USE NUMBER(18, 4); --客户认证方式限额使用情况
    V_EXCEPTION EXCEPTION; --自定义异常类型
    V_DAY_SUM_LIMIT_USE_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    V_IS_CUST_AUTH_LIMIT    VARCHAR2(1); --是否有客户自设限额 0--没有 1--有
    V_CCY                   VARCHAR2(3); --币别
    V_IS_HAS_SOTP           VARCHAR2(1); --1 ：开通sotp  0 未开通sotp
    V_UNUSE                 VARCHAR2(3); --无效值
  BEGIN
    --限额控制默认失败 0：限额校验成功 1：大于客户自设单笔限额 2：大于客户自设单笔累计限额
    --3：大于系统默认单笔限额 4：大于系统默认单笔累计限额 9999异常情况
    LMT_CTL_FLAG := '9999';
    V_CCY        := 'CNY';
    LMT_VALUE    := ''; --默认为空
    --回滚点
    SAVEPOINT PT1;
    --渠道认证方式限额校验
    BEGIN
      BEGIN
        BEGIN
          --查询sotp认证方式
          BEGIN
            SELECT SP.SEC_WAY
              INTO V_UNUSE
              FROM P_SEC_AUTH SP
             WHERE SP.SEC_WAY = '00'
               AND SP.CHAN_TYP = V_CHAN_TYP
               AND SP.EBUSER_NO = V_EBUSER_NO
               AND SP.SEC_STATE = 'N';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              V_IS_HAS_SOTP := '0'; --无stop认证方式
          END;
          --查询客户今天当前认证方式已使用累计限额
          BEGIN
            BEGIN
              SELECT CPALU.DAY_SUM_LIMIT
                INTO V_DAY_SUM_LIMIT_USE
                FROM CUST_PAY_AUTH_LIM_USE CPALU
               WHERE CPALU.EBUSER_NO = V_EBUSER_NO
                 AND CPALU.SEC_WAY = V_SEC_WAY
                 AND CPALU.CCY = V_CCY
                 AND CPALU.LAST_TRAN_DATE = V_TRANS_DATE;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                V_DAY_SUM_LIMIT_USE := 0;
            END;
            --当前支付金额+客户渠道认证方式当日已累计金额
            V_DAY_SUM_LIMIT_USE_NEW := V_DAY_SUM_LIMIT_USE + V_TRANS_AMT;
            DBMS_OUTPUT.PUT_LINE('客户渠道认证方式当日已累计限额：' ||
                                 V_DAY_SUM_LIMIT_USE_NEW);
          END;

          --查询客户当前认证方式的单笔限额和单日累计限额
          BEGIN
            SELECT CPASL.ONCE_LIMIT, CPASL.DAY_LIMIT
              INTO CASL_ONCE_LIMIT, CASL_DAY_LIMIT
              FROM CUST_PAY_AUTH_SET_LIMT CPASL
             WHERE CPASL.EBUSER_NO = V_EBUSER_NO
               AND CPASL.SEC_WAY = V_SEC_WAY
               AND CPASL.CCY = V_CCY;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              V_IS_CUST_AUTH_LIMIT := '0';
          END;
          DBMS_OUTPUT.PUT_LINE('客户是否自设限额：' || V_IS_CUST_AUTH_LIMIT);
          --当客户渠道认证方式自设限额无值时
          IF V_IS_CUST_AUTH_LIMIT = '0' THEN
          --sotp未开通 或者 网银渠道查询默认
            IF V_IS_HAS_SOTP = '0' OR V_CHAN_TYP = '060' THEN
              --查询渠道认证方式默认限额
              BEGIN
                SELECT CALD.ONCE_LIMIT, CALD.DAY_LIMIT
                  INTO CALD_ONCE_LIMIT, CALD_DAY_LIMIT
                  FROM PAY_AUTH_LIMT_DEF CALD
                 WHERE CALD.SEC_WAY = V_SEC_WAY
                   AND CALD.CCY = V_CCY;
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  RAISE V_EXCEPTION;
              END;
            ELSE
              --查询sotp单笔及单日累计限额
              BEGIN
                SELECT SL.ONCE_LIMIT, SL.DAY_LIMIT
                  INTO CALD_ONCE_LIMIT, CALD_DAY_LIMIT
                  FROM SOTP_TRAN_AUTH_DEFL_LIMIT SL
                 WHERE SL.CHAN_TYP = 'ALL'
                   AND SL.SEC_WAY = V_SEC_WAY
                   AND SL.CCY = V_CCY
                   AND SL.STATUS = '0' --正常
                   AND SL.TRAN_TYPE = '2'; --支付
              END;
            END IF;
            DBMS_OUTPUT.PUT_LINE('渠道认证方式默认限额：' || CALD_ONCE_LIMIT || ',' ||
                                 CALD_DAY_LIMIT);
            --判断支付金额 是否 大于 系统渠道认证默认单笔限额
            IF V_TRANS_AMT > CALD_ONCE_LIMIT THEN
              LMT_CTL_FLAG := '3';
              LMT_VALUE    := CAST(CALD_ONCE_LIMIT AS VARCHAR2);
              RAISE V_EXCEPTION;
              RETURN;
              --判断当前转账金额+客户渠道认证方式当日已累计金额 > 系统渠道认证默认当日转账金额限额
            ELSIF V_DAY_SUM_LIMIT_USE_NEW > CALD_DAY_LIMIT THEN
              LMT_CTL_FLAG := '4';
              LMT_VALUE    := CAST(CALD_DAY_LIMIT - V_DAY_SUM_LIMIT_USE AS
                                   VARCHAR2); --剩余可用额度
              RAISE V_EXCEPTION;
              RETURN;
            ELSE
              LMT_CTL_FLAG := '0';
            END IF;
            --判断转账金额 是否 大于 客户渠道认证自设单笔限额
          ELSIF V_TRANS_AMT > CASL_ONCE_LIMIT THEN
            LMT_CTL_FLAG := '1';
            LMT_VALUE    := CAST(CASL_ONCE_LIMIT AS VARCHAR2);
            RAISE V_EXCEPTION;
            RETURN;
            --判断当前转账金额+客户渠道认证方式当日已累计金额 > 客户渠道认证方式当日转账累计限额
          ELSIF V_DAY_SUM_LIMIT_USE_NEW > CASL_DAY_LIMIT THEN
            LMT_CTL_FLAG := '2';
            LMT_VALUE    := CAST(CASL_DAY_LIMIT - V_DAY_SUM_LIMIT_USE AS
                                 VARCHAR2); --剩余可用额度
            RAISE V_EXCEPTION;
            RETURN;
          ELSE
            LMT_CTL_FLAG := '0';
          END IF;
        END;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('限额校验状态' || LMT_CTL_FLAG);
        -- LMT_CTL_FLAG <> '0',即认为验证失败,触发异常,事务回滚到回滚点
        IF LMT_CTL_FLAG <> '0' THEN
          DBMS_OUTPUT.PUT_LINE('V_EXCEPTION START');
          RAISE V_EXCEPTION;
        END IF;
        --异常处理机制
      EXCEPTION
        WHEN V_EXCEPTION THEN
          DBMS_OUTPUT.PUT_LINE('EXCEPTION START');
          DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
          DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
          ROLLBACK TO SAVEPOINT PT1;
          COMMIT;
          DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
      END;
    END;
  END SOTP_PAY_LMT_CTL;

  --支付限额累计
  PROCEDURE SOTP_PAY_LMT_UPDATE(V_EBUSER_NO  IN VARCHAR2,
                                V_SEC_WAY    IN VARCHAR2,
                                V_TRANS_AMT  IN NUMBER,
                                V_TRANS_DATE IN VARCHAR2,
                                OUT_STATUS   OUT VARCHAR2) IS
    CALU_DAY_SUM_LIMIT     NUMBER(18, 4); --客户认证方式限额使用情况
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    v_CCY                    VARCHAR2(3); --币别
    IS_INSERT_CALU         VARCHAR2(1); --是否是新增 0：不是 1：是
  BEGIN
    v_CCY := 'CNY';
    --回滚点
    SAVEPOINT PT1;
    BEGIN
      --查询客户渠道认证方式当日已累计限额
      BEGIN
        SELECT CALU.DAY_SUM_LIMIT
          INTO CALU_DAY_SUM_LIMIT
          FROM CUST_PAY_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CALU_DAY_SUM_LIMIT := 0;
          IS_INSERT_CALU     := '1';
          DBMS_OUTPUT.PUT_LINE('客户已累计限额：' || CALU_DAY_SUM_LIMIT);
      END;
      --当前转账金额+客户渠道认证方式当日已累计金额
      CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT + V_TRANS_AMT;
      IF IS_INSERT_CALU = '1' THEN
        DBMS_OUTPUT.PUT_LINE('INSERT 客户已累计限额：' || CALU_DAY_SUM_LIMIT_NEW);
        DELETE FROM CUST_PAY_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
        --INSERT 客户渠道认证方式当日已累计限额
        INSERT INTO CUST_PAY_AUTH_LIM_USE
          (EBUSER_NO, SEC_WAY, CCY, DAY_SUM_LIMIT, LAST_TRAN_DATE)
        VALUES
          (V_EBUSER_NO,
           V_SEC_WAY,
           v_CCY,
           CALU_DAY_SUM_LIMIT_NEW,
           V_TRANS_DATE);
      ELSE
        DBMS_OUTPUT.PUT_LINE('UPDATE 客户已累计限额：' || CALU_DAY_SUM_LIMIT_NEW);
        --更新客户渠道认证方式已使用限额
        UPDATE CUST_PAY_AUTH_LIM_USE CALU
           SET CALU.DAY_SUM_LIMIT = CALU_DAY_SUM_LIMIT_NEW
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE;
      END IF;
    END;
    DBMS_OUTPUT.PUT_LINE('COMMIT START');
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    OUT_STATUS := 'SUCCESS';
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT1;
      OUT_STATUS := SQLERRM;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END SOTP_PAY_LMT_UPDATE;

  --转账限额回滚
  PROCEDURE SOTP_PAY_LMT_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,
                                  V_SEC_WAY    IN VARCHAR2,
                                  V_TRANS_AMT  IN NUMBER,
                                  V_TRANS_DATE IN VARCHAR2,
                                  OUT_STATUS   OUT VARCHAR2) IS
    CALU_DAY_SUM_LIMIT     NUMBER(18, 4); --客户认证方式限额使用情况
    CALU_DAY_SUM_LIMIT_NEW NUMBER(18, 4); --客户认证方式限额使用情况 - 当前转账金额
    v_CCY                    VARCHAR2(3); --币别
    V_EXCEPTION EXCEPTION; --自定义异常类型
  BEGIN
    v_CCY := 'CNY';
    DBMS_OUTPUT.PUT_LINE('EBUSER_NO=' || V_EBUSER_NO || ',SEC_WAY=' ||
                         V_SEC_WAY || ',TRANS_AMT=' || V_TRANS_AMT ||
                         ',TRANS_DATE=' || V_TRANS_DATE);
    --回滚点
    SAVEPOINT PT1;
    BEGIN
      --查询客户渠道认证方式当日已累计限额
      BEGIN
        SELECT CALU.DAY_SUM_LIMIT
          INTO CALU_DAY_SUM_LIMIT
          FROM CUST_PAY_AUTH_LIM_USE CALU
         WHERE CALU.EBUSER_NO = V_EBUSER_NO
           AND CALU.LAST_TRAN_DATE = V_TRANS_DATE
           AND CALU.SEC_WAY = V_SEC_WAY
           AND CALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          DBMS_OUTPUT.PUT_LINE('客户渠道认证方式无限额记录');
          OUT_STATUS := 'SUCCESS'; --客户渠道认证方式无限额记录
          RAISE V_EXCEPTION;
          RETURN;
      END;
      DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计限额：' || CALU_DAY_SUM_LIMIT);
      IF CALU_DAY_SUM_LIMIT = 0 THEN
        OUT_STATUS := 'SUCCESS'; --客户渠道认证方式单日累计限额为0
        RAISE V_EXCEPTION;
        RETURN;
      ELSIF CALU_DAY_SUM_LIMIT < V_TRANS_AMT THEN
        OUT_STATUS             := 'SUCCESS';
        CALU_DAY_SUM_LIMIT_NEW := 0;
        DBMS_OUTPUT.PUT_LINE('客户渠道认证方式已累计限额小于转账金额--回滚后限额：' ||
                             CALU_DAY_SUM_LIMIT_NEW);
      ELSE
        OUT_STATUS             := 'SUCCESS';
        CALU_DAY_SUM_LIMIT_NEW := CALU_DAY_SUM_LIMIT - V_TRANS_AMT; --客户任渠道认证方式单日已累计限额 - 转账金额
        DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计回滚后限额：' ||
                             CALU_DAY_SUM_LIMIT_NEW);
      END IF;
      --回滚客户渠道认证方式单日已累计限额
      UPDATE CUST_PAY_AUTH_LIM_USE CALU
         SET DAY_SUM_LIMIT = CALU_DAY_SUM_LIMIT_NEW
       WHERE CALU.EBUSER_NO = V_EBUSER_NO
         AND CALU.SEC_WAY = V_SEC_WAY
         AND CALU.CCY = V_CCY
         AND CALU.LAST_TRAN_DATE = V_TRANS_DATE;
    END;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN V_EXCEPTION THEN
      ROLLBACK TO SAVEPOINT PT1;
      DBMS_OUTPUT.PUT_LINE('V_EXCEPTION OUT_STATUS : ' || OUT_STATUS);
      COMMIT;
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT1;
      OUT_STATUS := SQLERRM;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END SOTP_PAY_LMT_ROLLBACK;

END PKGH_SOTP_PAY_LIMIT_CHECK;
/
SHOW ERRORS;

CREATE or replace PACKAGE BODY PKGH_SOTP_QUERY_LIMIT IS
  ------------------------------------------------------------------------
  --智能转账限额查询
  PROCEDURE SOTP_SMART_LIMIT_QUERY(V_CHAN_TYP         IN VARCHAR2, --渠道类型
                                   V_EBUSER_NO        IN VARCHAR2, --电子银行客户号
                                   V_TRANS_DATE       IN VARCHAR2, --转账日期
                                   V_SMART_LIMIT_LIST OUT LIMIT_OUT_TYPE_NEW, --已使用额度
                                   V_CHAN_LIMIT       OUT NUMBER,--渠道大额限额
                                   V_AVAIL_CHAN_LIMIT OUT NUMBER,--渠道大额可用限额
                                   IS_OPEN_SOTP       OUT VARCHAR2,--是否开通sotp 1-开通  0-不开通
                                   NOTICE_FLAG        OUT VARCHAR2) --大额提示 Y-当天提示  N-当天不提示
   AS

    V_EXCEPTION EXCEPTION; --自定义异常类型
    V_HAS_SEC_WAY           VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
    V_CCY                   VARCHAR2(3);
    V_SEC_WAY_LIST          SEC_TYPE; --客户认证方式数组游标
    V_SEC_WAY_DATA          SEC_WAY_DATA_TYPE; --客户认证方式数组
    V_SMART_LIMIT_LIST_DATA LIMIT_LIST_TYPE_NEW := LIMIT_LIST_TYPE_NEW();
    V_IS_HAS_SOTP           VARCHAR2(1); -- 1:代表有sotp认证方式  0:代表没有sotp认证方式
    V_UNUSE                 VARCHAR2(2); --无效结果
    --V_SEC_WAY_NAME          VARCHAR2(30); --认证方式名称
    --20161110-add-feixiaobo-增加渠道限额及可用限额
    V_CHAN_LIMIT_USE        NUMBER(18,4);
    --20161110-add-feixiaobo-增加渠道限额及可用限额
  BEGIN
    V_HAS_SEC_WAY := '0'; --默认无认证方式
    V_CCY         := 'CNY'; --人民币
    V_IS_HAS_SOTP := '1'; --默认有sotp认证方式
    SAVEPOINT PT2; --回滚点
    BEGIN

      --查询sotp认证方式
      BEGIN
        SELECT SP.SEC_WAY
          INTO V_UNUSE
          FROM P_SEC_AUTH SP
         WHERE SP.SEC_WAY = '00'
           AND SP.CHAN_TYP = V_CHAN_TYP
           AND SP.EBUSER_NO = V_EBUSER_NO
           AND SP.SEC_STATE = 'N';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_IS_HAS_SOTP := '0'; --无stop认证方式
      END;
      --查询渠道日累计限额
      BEGIN
         SELECT CLD.DAY_LIMIT INTO V_CHAN_LIMIT FROM CHAN_LIM_DEF CLD
         WHERE CLD.CHAN_TYP = V_CHAN_TYP
               AND CLD.CCY = V_CCY;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             V_CHAN_LIMIT := 0; --无渠道单日累计限额
      END;

       --查询渠道日累计已用限额,提示标志
      BEGIN
        SELECT CCLU.DAY_SUM_LIMIT,NVL(CCLU.NOTICE_FLAG,'Y')
               INTO V_CHAN_LIMIT_USE ,NOTICE_FLAG
        FROM CUST_CHAN_LIM_USE CCLU
        WHERE CCLU.EBUSER_NO =V_EBUSER_NO
        AND CCLU.CHAN_TYP = V_CHAN_TYP
        AND CCLU.CCY = V_CCY
        AND CCLU.LAST_TRAN_DATE = V_TRANS_DATE;
       EXCEPTION
           WHEN NO_DATA_FOUND THEN
             V_CHAN_LIMIT_USE := 0; --无渠道单日累计限额
             NOTICE_FLAG := 'Y';
      END;
      --计算渠道当日可用限额
     -- IF V_CHAN_LIMIT_USE >= V_CHAN_LIMIT THEN
        V_AVAIL_CHAN_LIMIT := 0;
     -- ELSE
     --   V_AVAIL_CHAN_LIMIT :=  V_CHAN_LIMIT - V_CHAN_LIMIT_USE;
      --END IF;
      V_AVAIL_CHAN_LIMIT := V_CHAN_LIMIT_USE;--返回当日已用
      DBMS_OUTPUT.PUT_LINE('V_CHAN_LIMIT ：' || V_CHAN_LIMIT || '，V_CHAN_LIMIT_USE：' ||V_CHAN_LIMIT_USE || '，V_AVAIL_CHAN_LIMIT：' ||V_AVAIL_CHAN_LIMIT);

      DBMS_OUTPUT.PUT_LINE('V_IS_HAS_SOTP ：' || V_IS_HAS_SOTP);
      --判断是否开通sotp，如果开通查询06小额免密认证方式限额，放到V_SMART_LIMIT_LIST_DATA数据中
      --IF V_IS_HAS_SOTP = '1' AND V_CHAN_TYP != '060' THEN
      --赋值认证方式名称
      --  SELECT A.SEC_WAY_NAME
      --     INTO V_SEC_WAY_NAME
      --    FROM AUTH_TYPE A
      --   WHERE A.SEC_WAY = '06';

      --  V_HAS_SEC_WAY := CHAN_LIMIT_QUERY(V_EBUSER_NO,
      --                                    V_CHAN_TYP,
      --                                   '06',
      --                                    V_SEC_WAY_NAME,
      --                                    V_CCY,
      --                                    V_TRANS_DATE,
      --                                    V_IS_HAS_SOTP,
      --                                    V_SMART_LIMIT_LIST_DATA);

      -- END IF;
      --查询客户的安全认证方式
      OPEN V_SEC_WAY_LIST FOR
        SELECT PSA.SEC_WAY, AT.SEC_WAY_NAME
          FROM P_SEC_AUTH PSA
          LEFT JOIN AUTH_TYPE AT ON PSA.SEC_WAY = AT.SEC_WAY
         WHERE PSA.EBUSER_NO = V_EBUSER_NO
           AND PSA.SEC_STATE = 'N'
           AND PSA.CHAN_TYP = V_CHAN_TYP
         ORDER BY AT.SEC_LEVEL;
      --遍历客户的安全认证方式--row=0时，无数据
      LOOP
        FETCH V_SEC_WAY_LIST
          INTO V_SEC_WAY_DATA;
        --无数据时，退出
        EXIT WHEN V_SEC_WAY_LIST%NOTFOUND;
    --开通了sotp且认证方式为01，则过滤
    IF V_SEC_WAY_DATA.SEC_WAY = '01' AND V_IS_HAS_SOTP = '1' THEN
      CONTINUE;
    END IF;

        --获取是否返回认证方式
        V_HAS_SEC_WAY := CHAN_LIMIT_QUERY(V_EBUSER_NO,
                                          V_CHAN_TYP,
                                          V_SEC_WAY_DATA.SEC_WAY,
                                          V_SEC_WAY_DATA.SEC_WAY_NAME,
                                          V_CCY,
                                          V_TRANS_DATE,
                                          V_IS_HAS_SOTP,
                                          V_SMART_LIMIT_LIST_DATA);
      END LOOP;
      CLOSE V_SEC_WAY_LIST;

      IF V_HAS_SEC_WAY = '0' THEN
        DBMS_OUTPUT.PUT_LINE('无认证方式');
        RAISE V_EXCEPTION;
        RETURN;
      END IF;
    END;
    FOR V_INDEX IN V_SMART_LIMIT_LIST_DATA.FIRST .. V_SMART_LIMIT_LIST_DATA.LAST LOOP
      DBMS_OUTPUT.PUT_LINE('V_SMART_LIMIT_LIST_DATA集合的认证方式' ||
                           V_SMART_LIMIT_LIST_DATA(V_INDEX)
                           .SEC_WAY || '单笔限额' ||
                           V_SMART_LIMIT_LIST_DATA(V_INDEX)
                           .ONCE_LIMIT || '单日剩余额度' ||
                           V_SMART_LIMIT_LIST_DATA(V_INDEX)
                           .AVAIL_DAY_LIMIT || '单日累计额度' ||
                           V_SMART_LIMIT_LIST_DATA(V_INDEX).DAY_LIMIT
                           || '单日转账次数'
                           ||V_SMART_LIMIT_LIST_DATA(V_INDEX).DAY_TOTAL
                             || '单日剩余转账次数'
                            ||V_SMART_LIMIT_LIST_DATA(V_INDEX).AVAIL_DAY_TOTAL
                             || '单年转账额度'
                             ||V_SMART_LIMIT_LIST_DATA(V_INDEX).YEAR_LIMIT
                               || '单年转账剩余额度'
                              ||V_SMART_LIMIT_LIST_DATA(V_INDEX).AVAIL_YEAR_LIMIT);
    END LOOP;
    --循环赋值
    OPEN V_SMART_LIMIT_LIST FOR
      SELECT SEC_WAY, SEC_WAY_NAME, ONCE_LIMIT, AVAIL_DAY_LIMIT, DAY_LIMIT,DAY_TOTAL,AVAIL_DAY_TOTAL,YEAR_LIMIT,AVAIL_YEAR_LIMIT
        FROM TABLE(CAST(V_SMART_LIMIT_LIST_DATA AS LIMIT_LIST_TYPE_NEW));
    COMMIT;
    IS_OPEN_SOTP := V_IS_HAS_SOTP;

  EXCEPTION
    WHEN V_EXCEPTION THEN
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      ROLLBACK TO SAVEPOINT PT2;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
  END SOTP_SMART_LIMIT_QUERY;

  --支付认证方式限额
  PROCEDURE SOTP_PAY_LIMIT_QUERY(V_CHAN_TYP       IN VARCHAR2, --渠道类型
                                 V_EBUSER_NO      IN VARCHAR2, --电子银行客户号
                                 V_TRANS_DATE     IN VARCHAR2, --转账日期
                                 V_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE, --已使用额度
                                 IS_OPEN_SOTP     OUT VARCHAR2) --是否开通sotp 1-开通  0-不开通
   AS
    V_SEC_WAY_LIST SEC_TYPE; --客户认证方式数组游标
    V_SEC_WAY_DATA SEC_WAY_DATA_TYPE; --客户认证方式数组
    V_EXCEPTION EXCEPTION; --自定义异常类型
    V_HAS_SEC_WAY         VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
    V_CCY                 VARCHAR2(3);
    V_PAY_LIMIT_LIST_DATA LIMIT_LIST_TYPE := LIMIT_LIST_TYPE();
    V_IS_HAS_SOTP         VARCHAR2(1); -- 1:代表有sotp认证方式  0:代表没有sotp认证方式
    V_UNUSE               VARCHAR2(2); --无效结果
    V_SEC_WAY_NAME        VARCHAR2(30); --认证方式名称
  BEGIN
    V_HAS_SEC_WAY := '0'; --默认无认证方式
    V_CCY         := 'CNY'; --人民币
    V_IS_HAS_SOTP := '1'; --默认有sotp认证方式
    SAVEPOINT PT2; --回滚点
    --当客户自设渠道认证方式限额没有值时，取系统默认认证方式限额
    BEGIN
      --查询sotp认证方式
      BEGIN
        SELECT SP.SEC_WAY
          INTO V_UNUSE
          FROM P_SEC_AUTH SP
         WHERE SP.SEC_WAY = '00'
           AND SP.CHAN_TYP = V_CHAN_TYP
           AND SP.EBUSER_NO = V_EBUSER_NO
           AND SP.SEC_STATE = 'N';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_IS_HAS_SOTP := '0'; --无stop认证方式
      END;
      --判断是否开通sotp，如果开通查询06小额免密认证方式限额，放到V_PAY_LIMIT_LIST_DATA数据中
     -- IF V_IS_HAS_SOTP = '1' AND V_CHAN_TYP != '060' THEN
        --赋值认证方式名称
     --   SELECT A.SEC_WAY_NAME
      --    INTO V_SEC_WAY_NAME
      --    FROM AUTH_TYPE A
     --    WHERE A.SEC_WAY = '06';

      --  V_HAS_SEC_WAY := CHAN_PAY_QUERY(V_EBUSER_NO,
      --                                  V_CHAN_TYP,
      --                                  '06',
      --                                  V_SEC_WAY_NAME,
      --                                  V_CCY,
      --                                  V_TRANS_DATE,
      --                                  V_IS_HAS_SOTP,
      --                                  V_PAY_LIMIT_LIST_DATA);
      --END IF;

      --查询客户的安全认证方式
      OPEN V_SEC_WAY_LIST FOR
        SELECT PSA.SEC_WAY, AT.SEC_WAY_NAME
          FROM P_SEC_AUTH PSA
          LEFT JOIN AUTH_TYPE AT ON PSA.SEC_WAY = AT.SEC_WAY
         WHERE PSA.EBUSER_NO = V_EBUSER_NO
           AND PSA.SEC_STATE = 'N'
           AND PSA.CHAN_TYP = V_CHAN_TYP
         ORDER BY AT.SEC_LEVEL;
      --遍历客户的安全认证方式--row=0时，无数据
      LOOP
        FETCH V_SEC_WAY_LIST
          INTO V_SEC_WAY_DATA;
        --无数据时，退出
        EXIT WHEN V_SEC_WAY_LIST%NOTFOUND;
    --开通了sotp且认证方式为01，则过滤
    IF V_SEC_WAY_DATA.SEC_WAY = '01' AND V_IS_HAS_SOTP = '1' THEN
      CONTINUE;
    END IF;
        V_HAS_SEC_WAY := CHAN_PAY_QUERY(V_EBUSER_NO,
                                        V_CHAN_TYP,
                                        V_SEC_WAY_DATA.SEC_WAY,
                                        V_SEC_WAY_DATA.SEC_WAY_NAME,
                                        V_CCY,
                                        V_TRANS_DATE,
                                        V_IS_HAS_SOTP,
                                        V_PAY_LIMIT_LIST_DATA);
      END LOOP;
      -- END;
      CLOSE V_SEC_WAY_LIST;
      IF V_HAS_SEC_WAY = '0' THEN
        DBMS_OUTPUT.PUT_LINE('无认证方式');
        RAISE V_EXCEPTION;
        RETURN;
      END IF;
    END;
    FOR V_INDEX IN V_PAY_LIMIT_LIST_DATA.FIRST .. V_PAY_LIMIT_LIST_DATA.LAST LOOP
      DBMS_OUTPUT.PUT_LINE('V_PAY_LIMIT_LIST_DATA集合的认证方式' ||
                           V_PAY_LIMIT_LIST_DATA(V_INDEX)
                           .SEC_WAY || '单笔限额' ||
                           V_PAY_LIMIT_LIST_DATA(V_INDEX)
                           .ONCE_LIMIT || '剩余额度' ||
                           V_PAY_LIMIT_LIST_DATA(V_INDEX)
                           .AVAIL_DAY_LIMIT || '单日累计额度' ||
                           V_PAY_LIMIT_LIST_DATA(V_INDEX).DAY_LIMIT);
    END LOOP;
    --循环赋值
    OPEN V_PAY_LIMIT_LIST FOR
      SELECT SEC_WAY, SEC_WAY_NAME, ONCE_LIMIT, AVAIL_DAY_LIMIT, DAY_LIMIT
        FROM TABLE(CAST(V_PAY_LIMIT_LIST_DATA AS LIMIT_LIST_TYPE));
    COMMIT;
    IS_OPEN_SOTP := V_IS_HAS_SOTP;
  EXCEPTION
    WHEN V_EXCEPTION THEN
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      ROLLBACK TO SAVEPOINT PT2;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
  END SOTP_PAY_LIMIT_QUERY;

  --访客支付类限额
  PROCEDURE SOTP_VISIT_PAY_LIMIT_QUERY(V_ACC_NO               IN VARCHAR2, --付款账号
                                       V_VISIT_PAY_LIMIT_LIST OUT LIMIT_OUT_TYPE) AS
    V_EXCEPTION EXCEPTION; --自定义异常类型
    V_DAY_SUM_LIMIT_USE         NUMBER(18, 4); --客户单日已使用限额
    V_CCY                       VARCHAR2(3);
    V_ONCE_LIMIT                NUMBER(18, 4); --单笔限额
    V_DAY_LIMIT                 NUMBER(18, 4); --单日累计限额
    V_DAY_SUM_LIMIT_RESIDUAL    NUMBER(18, 4); -- 客户单日剩余额度
    V_VISIT_PAY_LIMIT_LIST_DATA LIMIT_LIST_TYPE := LIMIT_LIST_TYPE();
  BEGIN
    V_CCY := 'CNY'; --人民币
    SAVEPOINT PT2; --回滚点
    BEGIN
      --查询该账号已使用累计限额
      BEGIN
        SELECT VPALU.DAY_SUM_LIMIT
          INTO V_DAY_SUM_LIMIT_USE
          FROM VISIT_PAY_AUTH_LIM_USE VPALU
         WHERE VPALU.ACCT_NO = V_ACC_NO
           AND VPALU.LAST_TRAN_DATE = TO_CHAR(SYSDATE, 'YYYY-MM-DD')
           AND VPALU.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_DAY_SUM_LIMIT_USE := 0;
      END;
      --查询访客单笔与单日累计限额
      BEGIN
        SELECT VPLD.ONCE_LIMIT, VPLD.DAY_LIMIT
          INTO V_ONCE_LIMIT, V_DAY_LIMIT
          FROM VISIT_PAY_LIMT_DEF VPLD
         WHERE VPLD.CCY = V_CCY;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RAISE V_EXCEPTION;
          RETURN;
      END;

      --如果账号已使用限额大于 单日累计额度，则剩余额度为0
      --否则剩余= 单日累计限额-客户单日已使用限额
      IF V_DAY_SUM_LIMIT_USE > V_DAY_LIMIT THEN
        V_DAY_SUM_LIMIT_RESIDUAL := 0;
      ELSE
        V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT - V_DAY_SUM_LIMIT_USE;
      END IF;
      --为返回数据设置值
      V_VISIT_PAY_LIMIT_LIST_DATA.EXTEND;
      V_VISIT_PAY_LIMIT_LIST_DATA(V_VISIT_PAY_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT('',
                                                                                         '',
                                                                                         V_ONCE_LIMIT,
                                                                                         V_DAY_SUM_LIMIT_RESIDUAL,
                                                                                         V_DAY_LIMIT);
    END;
    FOR V_INDEX IN V_VISIT_PAY_LIMIT_LIST_DATA.FIRST .. V_VISIT_PAY_LIMIT_LIST_DATA.LAST LOOP
      DBMS_OUTPUT.PUT_LINE('V_VISIT_PAY_LIMIT_LIST_DATA集合' || '单笔限额' ||
                           V_VISIT_PAY_LIMIT_LIST_DATA(V_INDEX)
                           .ONCE_LIMIT || '剩余额度' ||
                           V_VISIT_PAY_LIMIT_LIST_DATA(V_INDEX)
                           .AVAIL_DAY_LIMIT || '单日累计额度' ||
                           V_VISIT_PAY_LIMIT_LIST_DATA(V_INDEX).DAY_LIMIT);
    END LOOP;
    --循环赋值
    OPEN V_VISIT_PAY_LIMIT_LIST FOR
      SELECT SEC_WAY, SEC_WAY_NAME, ONCE_LIMIT, AVAIL_DAY_LIMIT, DAY_LIMIT
        FROM TABLE(CAST(V_VISIT_PAY_LIMIT_LIST_DATA AS LIMIT_LIST_TYPE));
    COMMIT;
  EXCEPTION
    WHEN V_EXCEPTION THEN
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      ROLLBACK TO SAVEPOINT PT2;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
  END SOTP_VISIT_PAY_LIMIT_QUERY;

END PKGH_SOTP_QUERY_LIMIT;

/
SHOW ERRORS;
CREATE or replace PACKAGE BODY PKGH_SOTP_SPEC_LIMIT IS
  PROCEDURE SOTP_SPEC_TRANS_LMT_CTL(V_EBUSER_NO   IN VARCHAR2,
                                    V_CHAN_TYP    IN VARCHAR2,
                                    V_TRANS_TYPE  IN VARCHAR2,
                                    V_TRANS_AMT   IN NUMBER,
                                    V_TRANS_DATE  IN VARCHAR2,
                                    OUT_STATUS    OUT VARCHAR2,
                                    V_LIMIT_VALUE OUT NUMBER,
                                    V_DAY_LIMIT   OUT NUMBER)
  --电子客户号，转账类型，转账金额，转账日期
   IS
    ONCE_LIMIT       NUMBER(18, 4); --特色转账单笔限额
    DAY_LIMIT        NUMBER(18, 4); --特色转账单日累计限额
    CUST_DAY_USE     NUMBER(18, 4); --客户特色转账单日已使用限额
    CUST_DAY_USE_NEW NUMBER(18, 4); --客户特色转账单日已使用限额+转账金额
    V_EXCEPTION EXCEPTION; --自定义异常类型
    V_IS_HAS_SOTP VARCHAR2(1); -- 1：开通sotp   0：未开通sotp
    V_UNUSE       VARCHAR2(3); --无效字段
  BEGIN
    --限额控制默认失败 0：限额校验成功 1：大于单笔限额 2：大于单日累计限额
    --9999异常情况
    OUT_STATUS    := '9999';
    V_IS_HAS_SOTP := '1'; -- 默认有SOTP认证方式
    --回滚点
    SAVEPOINT PT2;
    --特色转账限额校验
    BEGIN
      BEGIN
        BEGIN
          --查询sotp认证方式
          BEGIN
            SELECT SP.SEC_WAY
              INTO V_UNUSE
              FROM P_SEC_AUTH SP
             WHERE SP.SEC_WAY = '00'
               AND SP.CHAN_TYP = V_CHAN_TYP
               AND SP.EBUSER_NO = V_EBUSER_NO
               AND SP.SEC_STATE = 'N';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              V_IS_HAS_SOTP := '0'; --无stop认证方式
          END;
          --查询特色转账单笔限额
          DBMS_OUTPUT.PUT_LINE('查询特色转账单笔限额' || '是否有sotp认证方式：' ||
                               V_IS_HAS_SOTP);
          BEGIN
            --无sotp认证方式
            IF V_IS_HAS_SOTP = '0' THEN
              BEGIN
                SELECT STL.ONCE_LIMIT
                  INTO ONCE_LIMIT
                  FROM SPECI_TRANS_LIMIT STL
                 WHERE STL.TRANS_TYPE = V_TRANS_TYPE;
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  DBMS_OUTPUT.PUT_LINE('无' || V_TRANS_TYPE || '类型单笔限额数据');
                  RAISE V_EXCEPTION;
                  RETURN;
              END;
            ELSE
              --有sotp认证方式
              BEGIN
                SELECT SSTL.ONCE_LIMIT
                  INTO ONCE_LIMIT
                  FROM SOTP_SPECI_TRANS_LIMIT SSTL
                 WHERE SSTL.TRANS_TYPE = V_TRANS_TYPE
                   AND SSTL.STATUS = '0';
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  DBMS_OUTPUT.PUT_LINE('无' || V_TRANS_TYPE || '类型单笔限额数据');
                  RAISE V_EXCEPTION;
                  RETURN;
              END;
            END IF;
          END;
          --查询特色转账单日累计限额
          DBMS_OUTPUT.PUT_LINE('查询特色转账单日累计限额');

          BEGIN
            IF V_IS_HAS_SOTP = '0' THEN
              BEGIN
                SELECT STL.DAY_LIMIT
                  INTO DAY_LIMIT
                  FROM SPECI_TRANS_LIMIT STL
                 WHERE STL.TRANS_TYPE = 'ALL';
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  DBMS_OUTPUT.PUT_LINE('无特色转账单日累计限额数据');
                  RAISE V_EXCEPTION;
                  RETURN;
              END;

            ELSE
              BEGIN
                SELECT SSTL.DAY_LIMIT
                  INTO DAY_LIMIT
                  FROM SOTP_SPECI_TRANS_LIMIT SSTL
                 WHERE SSTL.TRANS_TYPE = 'ALL'
                   AND SSTL.STATUS = '0';
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  DBMS_OUTPUT.PUT_LINE('无特色转账单日累计限额数据');
                  RAISE V_EXCEPTION;
                  RETURN;
              END;
            END IF;
          END;

          --查询客户特色转账已使用限额
          DBMS_OUTPUT.PUT_LINE('查询客户特色转账已使用限额');
          BEGIN
            SELECT SUM(CSLU.DAY_SUM_LIMIT)
              INTO CUST_DAY_USE
              FROM CUST_SPCI_LIM_USE CSLU
             WHERE CSLU.EBUSER_NO = V_EBUSER_NO
               AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              CUST_DAY_USE := 0;
              DBMS_OUTPUT.PUT_LINE('客户无特色转账单日累计限额数据');
          END;
          CUST_DAY_USE_NEW := CUST_DAY_USE + V_TRANS_AMT;
          DBMS_OUTPUT.PUT_LINE('客户特色转账单日已累计限额数据' || CUST_DAY_USE_NEW);
        END;
        DBMS_OUTPUT.PUT_LINE('特色转账单笔限额=' || ONCE_LIMIT || ';特色转账单日限额=' ||
                             DAY_LIMIT);
        --判断转账金额 是否 大于 特色转账单笔限额
        IF V_TRANS_AMT > ONCE_LIMIT THEN
          OUT_STATUS    := '1';
          V_LIMIT_VALUE := ONCE_LIMIT;
          DBMS_OUTPUT.PUT_LINE('客户转账金额大于单笔转账限额');
          RAISE V_EXCEPTION;
          RETURN;
          --判断转账金额+单日已累计 是否 大于 特色转账单日累计限额
        ELSIF CUST_DAY_USE_NEW > DAY_LIMIT THEN
          DBMS_OUTPUT.PUT_LINE('客户转账已累计大于单日累计转账限额');
          OUT_STATUS    := '2';
          V_LIMIT_VALUE := CUST_DAY_USE_NEW;
          V_DAY_LIMIT   := DAY_LIMIT;
          RAISE V_EXCEPTION;
          RETURN;
        ELSE
          DBMS_OUTPUT.PUT_LINE('客户转账已累计小于单日累计转账限额');
          OUT_STATUS := '0';
        END IF;
      END;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('限额校验状态' || OUT_STATUS);
      -- OUT_STATUS <> '0',即认为验证失败,触发异常,事务回滚到回滚点
      IF OUT_STATUS <> '0' THEN
        RAISE V_EXCEPTION;
      END IF;
      --异常处理机制
    EXCEPTION
      WHEN V_EXCEPTION THEN
        DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
        ROLLBACK TO SAVEPOINT PT2;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
    END;
  END SOTP_SPEC_TRANS_LMT_CTL;
  --特色转账限额累计
  PROCEDURE SOTP_SPEC_TRANS_LMT_UPDATE(V_EBUSER_NO  IN VARCHAR2,
                                       V_TRANS_TYPE IN VARCHAR2,
                                       V_TRANS_AMT  IN NUMBER,
                                       V_TRANS_DATE IN VARCHAR2,
                                       OUT_STATUS   OUT VARCHAR2) IS
    CUST_DAY_USE     NUMBER(18, 4); --客户认证方式限额使用情况
    CUST_DAY_USE_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    IS_INSERT_CSLU   NUMBER(18, 4); --是否是新增 0：不是 1：是
  BEGIN
    --回滚点
    SAVEPOINT PT2;
    BEGIN
      --查询客户特色转账已使用限额
      DBMS_OUTPUT.PUT_LINE('查询客户特色转账已使用限额');
      BEGIN
        SELECT CSLU.DAY_SUM_LIMIT
          INTO CUST_DAY_USE
          FROM CUST_SPCI_LIM_USE CSLU
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO
           AND CSLU.TRANS_TYPE = V_TRANS_TYPE
           AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CUST_DAY_USE   := 0;
          IS_INSERT_CSLU := 1;
          DBMS_OUTPUT.PUT_LINE('客户无特色转账单日累计限额数据');
      END;
      CUST_DAY_USE_NEW := CUST_DAY_USE + V_TRANS_AMT;
      DBMS_OUTPUT.PUT_LINE('客户特色转账单日已累计限额数据' || CUST_DAY_USE_NEW);
      --新增客户已使用限额
      IF IS_INSERT_CSLU = 1 THEN
        DELETE FROM CUST_SPCI_LIM_USE CSLU
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO;

        INSERT INTO CUST_SPCI_LIM_USE
          (EBUSER_NO, TRANS_TYPE, DAY_SUM_LIMIT, LAST_TRAN_DATE)
        VALUES
          (V_EBUSER_NO, V_TRANS_TYPE, CUST_DAY_USE_NEW, V_TRANS_DATE);
      ELSE
        UPDATE CUST_SPCI_LIM_USE CSLU
           SET CSLU.DAY_SUM_LIMIT = CUST_DAY_USE_NEW
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO
           AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
      END IF;
    END;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    OUT_STATUS := 'SUCCESS';
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT2;
      OUT_STATUS := SQLERRM;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END SOTP_SPEC_TRANS_LMT_UPDATE;
  --特色转账限额回滚
  PROCEDURE SOTP_SPEC_TRANS_LMT_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,
                                         V_TRANS_TYPE IN VARCHAR2,
                                         V_TRANS_AMT  IN NUMBER,
                                         V_TRANS_DATE IN VARCHAR2,
                                         OUT_STATUS   OUT VARCHAR2) IS
    CUST_DAY_USE     NUMBER(18, 4); --特殊已累计限额
    CUST_DAY_USE_NEW NUMBER(18, 4); --特殊已累计限额 - 当前转账金额
    V_EXCEPTION EXCEPTION; --自定义异常类型
  BEGIN
    --回滚点
    SAVEPOINT PT2;
    BEGIN
      --查询客户特色转账已使用限额
      DBMS_OUTPUT.PUT_LINE('查询客户特色转账已使用限额');
      BEGIN
        SELECT CSLU.DAY_SUM_LIMIT
          INTO CUST_DAY_USE
          FROM CUST_SPCI_LIM_USE CSLU
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO
           AND CSLU.TRANS_TYPE = V_TRANS_TYPE
           AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CUST_DAY_USE := 0;
          DBMS_OUTPUT.PUT_LINE('客户无特色转账单日累计限额数据');
      END;
      DBMS_OUTPUT.PUT_LINE('客户特色转账单日已累计限额数据' || CUST_DAY_USE);
      IF CUST_DAY_USE = 0 THEN
        OUT_STATUS := 'SUCCESS'; --客户渠道认证方式单日累计限额为0
        RAISE V_EXCEPTION;
        RETURN;
      ELSIF CUST_DAY_USE < V_TRANS_AMT THEN
        OUT_STATUS       := 'SUCCESS';
        CUST_DAY_USE_NEW := 0;
      ELSE
        OUT_STATUS       := 'SUCCESS';
        CUST_DAY_USE_NEW := CUST_DAY_USE - V_TRANS_AMT; --客户任渠道认证方式单日已累计限额 - 转账金额
      END IF;
      DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计回滚后限额：' || CUST_DAY_USE_NEW);
      --回滚客户渠道认证方式单日已累计限额
      UPDATE CUST_SPCI_LIM_USE CSLU
         SET DAY_SUM_LIMIT = CUST_DAY_USE_NEW
       WHERE CSLU.EBUSER_NO = V_EBUSER_NO
         AND CSLU.TRANS_TYPE = V_TRANS_TYPE
         AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
    END;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN V_EXCEPTION THEN
      ROLLBACK TO SAVEPOINT PT2;
      DBMS_OUTPUT.PUT_LINE('V_EXCEPTION OUT_STATUS : ' || OUT_STATUS);
      COMMIT;
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT2;
      OUT_STATUS := SQLERRM;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END SOTP_SPEC_TRANS_LMT_ROLLBACK;
END PKGH_SOTP_SPEC_LIMIT;

/
SHOW ERRORS;
CREATE or replace PACKAGE BODY PKGH_SPEC_LIMIT IS
  PROCEDURE SPEC_TRANS_LMT_CTL(V_EBUSER_NO  IN VARCHAR2,
                               V_TRANS_TYPE IN VARCHAR2,
                               V_TRANS_AMT  IN NUMBER,
                               V_TRANS_DATE IN VARCHAR2,
                               OUT_STATUS   OUT VARCHAR2)
  --电子客户号，转账类型，转账金额，转账日期
   IS
    ONCE_LIMIT       NUMBER(18, 4); --特色转账单笔限额
    DAY_LIMIT        NUMBER(18, 4); --特色转账单日累计限额
    CUST_DAY_USE     NUMBER(18, 4); --客户特色转账单日已使用限额
    CUST_DAY_USE_NEW NUMBER(18, 4); --客户特色转账单日已使用限额+转账金额
    V_EXCEPTION EXCEPTION; --自定义异常类型
    v_CCY VARCHAR2(3); --币别
  BEGIN
    --限额控制默认失败 0：限额校验成功 1：大于单笔限额 2：大于单日累计限额
    --9999异常情况
    OUT_STATUS := '9999';
    v_CCY        := 'CNY';
    --回滚点
    SAVEPOINT PT2;
    --特色转账限额校验
    BEGIN
      BEGIN
        BEGIN
          --查询特色转账单笔限额
          DBMS_OUTPUT.PUT_LINE('查询特色转账单笔限额');
          BEGIN
            SELECT STL.ONCE_LIMIT
              INTO ONCE_LIMIT
              FROM SPECI_TRANS_LIMIT STL
             WHERE STL.TRANS_TYPE = V_TRANS_TYPE;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              DBMS_OUTPUT.PUT_LINE('无' || V_TRANS_TYPE || '类型单笔限额数据');
              RAISE V_EXCEPTION;
              RETURN;
          END;
          --查询特色转账单日累计限额
          DBMS_OUTPUT.PUT_LINE('查询特色转账单日累计限额');
          BEGIN
            SELECT STL.DAY_LIMIT
              INTO DAY_LIMIT
              FROM SPECI_TRANS_LIMIT STL
             WHERE STL.TRANS_TYPE = 'ALL';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              DBMS_OUTPUT.PUT_LINE('无特色转账单日累计限额数据');
              RAISE V_EXCEPTION;
              RETURN;
          END;
          --查询客户特色转账已使用限额
          DBMS_OUTPUT.PUT_LINE('查询客户特色转账已使用限额');
          BEGIN
            SELECT SUM(CSLU.DAY_SUM_LIMIT)
              INTO CUST_DAY_USE
              FROM CUST_SPCI_LIM_USE CSLU
             WHERE CSLU.EBUSER_NO = V_EBUSER_NO
               AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              CUST_DAY_USE := 0;
              DBMS_OUTPUT.PUT_LINE('客户无特色转账单日累计限额数据');
          END;
          CUST_DAY_USE_NEW := CUST_DAY_USE + V_TRANS_AMT;
          DBMS_OUTPUT.PUT_LINE('客户特色转账单日已累计限额数据' || CUST_DAY_USE_NEW);
        END;
        DBMS_OUTPUT.PUT_LINE('特色转账单笔限额=' || ONCE_LIMIT || ';特色转账单日限额=' ||
                             DAY_LIMIT);
        --判断转账金额 是否 大于 特色转账单笔限额
        IF V_TRANS_AMT > ONCE_LIMIT THEN
          OUT_STATUS := '1';
          DBMS_OUTPUT.PUT_LINE('客户转账金额大于单笔转账限额');
          RAISE V_EXCEPTION;
          RETURN;
          --判断转账金额+单日已累计 是否 大于 特色转账单日累计限额
        ELSIF CUST_DAY_USE_NEW > DAY_LIMIT THEN
          DBMS_OUTPUT.PUT_LINE('客户转账已累计大于单日累计转账限额');
          OUT_STATUS := '2';
          RAISE V_EXCEPTION;
          RETURN;
        ELSE
          DBMS_OUTPUT.PUT_LINE('客户转账已累计大于单日累计转账限额');
          OUT_STATUS := '0';
        END IF;
      END;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('限额校验状态' || OUT_STATUS);
      -- OUT_STATUS <> '0',即认为验证失败,触发异常,事务回滚到回滚点
      IF OUT_STATUS <> '0' THEN
        RAISE V_EXCEPTION;
      END IF;
      --异常处理机制
    EXCEPTION
      WHEN V_EXCEPTION THEN
        DBMS_OUTPUT.PUT_LINE('sqlcode : ' || sqlcode);
        DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || sqlerrm);
        ROLLBACK TO SAVEPOINT PT2;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('EXCEPTION END');
    END;
  END;
  --特色转账限额累计
  PROCEDURE SPEC_TRANS_LMT_UPDATE(V_EBUSER_NO  IN VARCHAR2,
                                  V_TRANS_TYPE IN VARCHAR2,
                                  V_TRANS_AMT  IN NUMBER,
                                  V_TRANS_DATE IN VARCHAR2,
                                  OUT_STATUS   OUT VARCHAR2) IS
    CUST_DAY_USE     NUMBER(18, 4); --客户认证方式限额使用情况
    CUST_DAY_USE_NEW NUMBER(18, 4); --客户认证方式限额使用情况 + 当前转账金额
    IS_INSERT_CSLU   NUMBER(18, 4); --是否是新增 0：不是 1：是
    v_CCY              VARCHAR2(3); --币别
  BEGIN
    v_CCY := 'CNY';
    --回滚点
    SAVEPOINT PT2;
    BEGIN
      --查询客户特色转账已使用限额
      DBMS_OUTPUT.PUT_LINE('查询客户特色转账已使用限额');
      BEGIN
        SELECT CSLU.DAY_SUM_LIMIT
          INTO CUST_DAY_USE
          FROM CUST_SPCI_LIM_USE CSLU
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO
           AND CSLU.TRANS_TYPE = V_TRANS_TYPE
           AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CUST_DAY_USE   := 0;
          IS_INSERT_CSLU := 1;
          DBMS_OUTPUT.PUT_LINE('客户无特色转账单日累计限额数据');
      END;
      CUST_DAY_USE_NEW := CUST_DAY_USE + V_TRANS_AMT;
      DBMS_OUTPUT.PUT_LINE('客户特色转账单日已累计限额数据' || CUST_DAY_USE_NEW);
      --新增客户已使用限额
      IF IS_INSERT_CSLU = 1 THEN
        DELETE FROM CUST_SPCI_LIM_USE CSLU
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO;

        INSERT INTO CUST_SPCI_LIM_USE
          (EBUSER_NO, TRANS_TYPE, DAY_SUM_LIMIT, LAST_TRAN_DATE)
        VALUES
          (V_EBUSER_NO, V_TRANS_TYPE, CUST_DAY_USE_NEW, V_TRANS_DATE);
      ELSE
        UPDATE CUST_SPCI_LIM_USE CSLU
           SET CSLU.DAY_SUM_LIMIT = CUST_DAY_USE_NEW
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO
           AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
      END IF;
    END;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    OUT_STATUS := 'SUCCESS';
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT2;
      OUT_STATUS := sqlerrm;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || sqlcode);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || sqlerrm);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END;
  --特色转账限额回滚
  PROCEDURE SPEC_TRANS_LMT_ROLLBACK(V_EBUSER_NO  IN VARCHAR2,
                                    V_TRANS_TYPE IN VARCHAR2,
                                    V_TRANS_AMT  IN NUMBER,
                                    V_TRANS_DATE IN VARCHAR2,
                                    OUT_STATUS   OUT VARCHAR2) IS
    CUST_DAY_USE     NUMBER(18, 4); --特殊已累计限额
    CUST_DAY_USE_NEW NUMBER(18, 4); --特殊已累计限额 - 当前转账金额
    V_CCY              VARCHAR2(3); --币别
    V_EXCEPTION EXCEPTION; --自定义异常类型
  BEGIN
    V_CCY := 'CNY';
    --回滚点
    SAVEPOINT PT2;
    BEGIN
      --查询客户特色转账已使用限额
      DBMS_OUTPUT.PUT_LINE('查询客户特色转账已使用限额');
      BEGIN
        SELECT CSLU.DAY_SUM_LIMIT
          INTO CUST_DAY_USE
          FROM CUST_SPCI_LIM_USE CSLU
         WHERE CSLU.EBUSER_NO = V_EBUSER_NO
           AND CSLU.TRANS_TYPE = V_TRANS_TYPE
           AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          CUST_DAY_USE := 0;
          DBMS_OUTPUT.PUT_LINE('客户无特色转账单日累计限额数据');
      END;
      DBMS_OUTPUT.PUT_LINE('客户特色转账单日已累计限额数据' || CUST_DAY_USE);
      IF CUST_DAY_USE = 0 THEN
        OUT_STATUS := 'SUCCESS'; --客户渠道认证方式单日累计限额为0
        RAISE V_EXCEPTION;
        RETURN;
      ELSIF CUST_DAY_USE < V_TRANS_AMT THEN
        OUT_STATUS       := 'SUCCESS';
        CUST_DAY_USE_NEW := 0;
      ELSE
        OUT_STATUS       := 'SUCCESS';
        CUST_DAY_USE_NEW := CUST_DAY_USE - V_TRANS_AMT; --客户任渠道认证方式单日已累计限额 - 转账金额
      END IF;
      DBMS_OUTPUT.PUT_LINE('客户渠道认证方式单日已累计回滚后限额：' || CUST_DAY_USE_NEW);
      --回滚客户渠道认证方式单日已累计限额
      UPDATE CUST_SPCI_LIM_USE CSLU
         SET DAY_SUM_LIMIT = CUST_DAY_USE_NEW
       WHERE CSLU.EBUSER_NO = V_EBUSER_NO
         AND CSLU.TRANS_TYPE = V_TRANS_TYPE
         AND CSLU.LAST_TRAN_DATE = V_TRANS_DATE;
    END;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('COMMIT END');
    DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  EXCEPTION
    WHEN V_EXCEPTION THEN
      ROLLBACK TO SAVEPOINT PT2;
      DBMS_OUTPUT.PUT_LINE('V_EXCEPTION OUT_STATUS : ' || OUT_STATUS);
      COMMIT;
    WHEN OTHERS THEN
      ROLLBACK TO SAVEPOINT PT2;
      OUT_STATUS := sqlerrm;
      DBMS_OUTPUT.PUT_LINE('sqlcode : ' || sqlcode);
      DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || sqlerrm);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('OUT_STATUS : ' || OUT_STATUS);
  END;
END PKGH_SPEC_LIMIT;

/
SHOW ERRORS;


CREATE OR REPLACE PACKAGE BODY PKGH_TEST IS
  PROCEDURE SIGN_TEST(V_EBUSER_NO  IN VARCHAR2,V_CHAN_TYP IN VARCHAR2)
   IS
  BEGIN
   DELETE FROM EBANK_CREDIT_CARD ECC WHERE ECC.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM EBANK_ACCT EA WHERE EA.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM CUST_CH_AU_SELF_LIM CCASL WHERE CCASL.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM CUST_PAY_AUTH_SET_LIMT CPASL WHERE CPASL.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM MB_ASSO_INFO MAI WHERE MAI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM ASSO_INFO AI WHERE AI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM P_SEC_AUTH PSA WHERE PSA.EBUSER_NO =  V_EBUSER_NO;
    DELETE FROM P_OPERATOR PO WHERE PO.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM P_LGN_INFO PLI WHERE PLI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM PCERT_INFO PCI WHERE PCI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM P_CUST_CHAN_IMG PCCI WHERE PCCI.EBUSER_NO = V_EBUSER_NO;
    DELETE FROM EUSER_INFO WHERE EBUSER_NO = V_EBUSER_NO;
    DELETE FROM USER_CHANNEL WHERE EBUSER_NO = V_EBUSER_NO;
    IF '058' = V_CHAN_TYP THEN
       DELETE FROM WEB_CHAN_INFO WCI WHERE WCI.EBUSER_NO = V_EBUSER_NO;
    ELSIF '059' = V_CHAN_TYP THEN
       DELETE FROM MB_CHAN_INFO MCI WHERE MCI.EBUSER_NO = V_EBUSER_NO;
    ELSIF '060' = V_CHAN_TYP THEN
       DELETE FROM IE_CHAN_INFO ICI WHERE ICI.EBUSER_NO = V_EBUSER_NO;
    ELSE
       DELETE FROM WEB_CHAN_INFO WCI WHERE WCI.EBUSER_NO = V_EBUSER_NO;
       DELETE FROM MB_CHAN_INFO MCI WHERE MCI.EBUSER_NO = V_EBUSER_NO;
       DELETE FROM IE_CHAN_INFO ICI WHERE ICI.EBUSER_NO = V_EBUSER_NO;
    END IF;
    COMMIT;
  END;
END PKGH_TEST;

/
SHOW ERRORS;
SHOW ERRORS;

