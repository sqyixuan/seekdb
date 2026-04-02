CREATE OR REPLACE TYPE "PPS"."COLOR_NT" AS TABLE OF VARCHAR2 (30);


CREATE OR REPLACE TYPE "PPS"."COLOR_VAT" AS VARRAY (16) OF VARCHAR2 (30);
CREATE OR REPLACE TYPE "PPS"."CV_PAIR" AS OBJECT(cn VARCHAR2(10),cv VARCHAR2(10));
CREATE OR REPLACE TYPE "PPS"."CV_VARR" AS VARRAY(8) OF cv_pair;
CREATE OR REPLACE TYPE "PPS"."OPER_ORG" as object(
  operno   VARCHAR2(20),
  orgid1   VARCHAR2(24),
  orgname1 VARCHAR2(240),
  orgid2   VARCHAR2(24),
  orgname2 VARCHAR2(240),
  orgid3   VARCHAR2(24),
  orgname3 VARCHAR2(240)
);
CREATE OR REPLACE TYPE "PPS"."SPLIT_TYPE" as object
       (
         s_id integer,
         s_value varchar2(1000)
       );
CREATE OR REPLACE TYPE "PPS"."TICKERTYPE" AS OBJECT (
   ticker      VARCHAR2 (20)
 , pricedate   DATE
 , pricetype   VARCHAR2 (1)
 , price       NUMBER
);

CREATE OR REPLACE TYPE "PPS"."T_OPER_ORG" is table of OPER_ORG;
CREATE OR REPLACE TYPE "PPS"."SPLIT_TABLE" IS TABLE OF Split_Type;

CREATE SEQUENCE  "PPS"."LOG_SEQ"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 10647 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_JOB_IDS_LOG_SNO"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000280 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_JOB_M_DAILYLOG_ID"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000180 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_JOB_T_LOG"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000700 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_RID"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 81 CACHE 20 NOORDER  NOCYCLE;
CREATE SEQUENCE  "PPS"."SEQ_T_QRTZ_TRIGGER_GROUP"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000060 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_T_QRTZ_TRIGGER_INFO"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000760 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_T_QRTZ_TRIGGER_LOG"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 2195359 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_T_QRTZ_TRIGGER_LOGGLUE"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000060 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."SEQ_T_QRTZ_TRIGGER_REGISTRY"  MINVALUE 1 MAXVALUE 99999999 INCREMENT BY 1 START WITH 1000060 CACHE 20 NOORDER  CYCLE;
CREATE SEQUENCE  "PPS"."TEMP1_SEQ"  MINVALUE 400 MAXVALUE 999 INCREMENT BY 1 START WITH 460 CACHE 20 ORDER  NOCYCLE;
CREATE SEQUENCE  "PPS"."TEMP_SEQ"  MINVALUE 1 MAXVALUE 999 INCREMENT BY 1 START WITH 913 NOCACHE  ORDER  NOCYCLE;

CREATE TABLE "TEST20210708" 
( SNO NUMBER, 
 NAME VARCHAR2(100) );
 
create table PPS.A01_RISKCON
(
  policyno             CHAR(15) not null,
  polist               CHAR(1),
  npaylen              NUMBER(20,6),
  yearnum              INTEGER,
  ttime                INTEGER,
  tmount               NUMBER(16,2),
  pieces               NUMBER(13,3),
  classcode            VARCHAR2(8) not null,
  empno                VARCHAR2(20),
  begdate              DATE,
  contno               CHAR(8),
  fgsno                CHAR(3) not null,
  paycode              CHAR(2),
  delcode              CHAR(1),
  prelname             CHAR(6),
  pedate               DATE,
  pid                  CHAR(30),
  job                  CHAR(20),
  apid                 CHAR(30),
  operno               VARCHAR2(20),
  stopdat              DATE,
  appdate              DATE,
  delage               INTEGER,
  appno                VARCHAR2(20),
  dbdate               DATE,
  appf                 VARCHAR2(2),
  recaddr              VARCHAR2(508),
  deptno               VARCHAR2(20),
  nextdate             DATE,
  stopage              INTEGER,
  payseq               VARCHAR2(2048),
  unstdrate            NUMBER(10,2),
  reczip               VARCHAR2(12),
  losttime             INTEGER,
  rate                 NUMBER(4,2),
  stdrate              NUMBER(10,2),
  currcode             VARCHAR2(2),
  deltlag              VARCHAR2(2),
  debist               VARCHAR2(2),
  del                  VARCHAR2(2),
  paystatus            VARCHAR2(2),
  delstat              VARCHAR2(2),
  rectele              VARCHAR2(30),
  saleattr             VARCHAR2(2),
  stopdate             DATE,
  gpolicyno            VARCHAR2(15),
  etl_time             DATE,
  payfmage             INTEGER,
  nretdate             DATE,
  renewid              CHAR(1),
  bankflag             CHAR(2),
  payfrmage            NUMBER(13),
  idtype               CHAR(2),
  aidtype              CHAR(2),
  aperson_id           INTEGER,
  benparam             INTEGER,
  comnum               INTEGER,
  csr_id               INTEGER,
  currency             CHAR(3),
  dcdm                 CHAR(2),
  del_date             CHAR(8),
  del_type             INTEGER,
  deskpay              CHAR(1),
  discount             NUMBER(20,6),
  empno_id             INTEGER,
  gcon_id              INTEGER,
  iscard               CHAR(1),
  opdate               DATE,
  oper_id              INTEGER,
  person_id            INTEGER,
  reason               VARCHAR2(200),
  reg_code             VARCHAR2(20),
  renewdate            DATE,
  riskcon_id           INTEGER,
  specagr              VARCHAR2(4000),
  sno                  INTEGER,
  typeid               INTEGER,
  sharetype            CHAR(3),
  branch               CHAR(14),
  begtime              DATE,
  endtime              DATE,
  sour_sys             VARCHAR2(20),
  src_sys              VARCHAR2(20),
  sale_prod_code       VARCHAR2(8),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  workno               VARCHAR2(20),
  comb_policy_no       VARCHAR2(20),
  app_age              NUMBER(3),
  sub_agt_no           VARCHAR2(20),
  o_classcode          VARCHAR2(8),
  cross_sale_ind       VARCHAR2(1),
  prem_rate_level      VARCHAR2(2),
  ill_score            NUMBER(10,2),
  group_no             VARCHAR2(2),
  digital_sign_ind     VARCHAR2(1),
  supply_input_ind     VARCHAR2(1),
  ini_invoice_status   VARCHAR2(2),
  uni_pay_ind          VARCHAR2(1),
  appointed_opdate_ind VARCHAR2(1),
  contcode             VARCHAR2(8)
);







create table PPS.D01_RISKCON
(
  policyno             CHAR(15) not null,
  polist               CHAR(1),
  npaylen              NUMBER(20,6),
  yearnum              INTEGER,
  ttime                INTEGER,
  tmount               NUMBER(16,2),
  pieces               NUMBER(13,3),
  classcode            VARCHAR2(8) not null,
  empno                VARCHAR2(20),
  begdate              DATE,
  contno               VARCHAR2(20),
  fgsno                CHAR(3) not null,
  paycode              CHAR(2),
  delcode              CHAR(1),
  prelname             CHAR(6),
  pedate               DATE,
  pid                  CHAR(30),
  job                  CHAR(20),
  apid                 CHAR(30),
  operno               VARCHAR2(20),
  stopdat              DATE,
  appdate              DATE,
  delage               INTEGER,
  appno                VARCHAR2(20),
  dbdate               DATE,
  appf                 VARCHAR2(2),
  recaddr              VARCHAR2(508),
  deptno               VARCHAR2(20),
  nextdate             DATE,
  stopage              INTEGER,
  payseq               VARCHAR2(2048),
  unstdrate            NUMBER(10,2),
  reczip               VARCHAR2(12),
  losttime             INTEGER,
  rate                 NUMBER(4,2),
  stdrate              NUMBER(10,2),
  currcode             VARCHAR2(2),
  deltlag              VARCHAR2(2),
  debist               VARCHAR2(2),
  del                  VARCHAR2(2),
  paystatus            VARCHAR2(2),
  delstat              VARCHAR2(2),
  rectele              VARCHAR2(30),
  saleattr             VARCHAR2(2),
  stopdate             DATE,
  gpolicyno            VARCHAR2(15),
  etl_time             DATE,
  payfmage             INTEGER,
  nretdate             DATE,
  renewid              CHAR(1),
  bankflag             CHAR(2),
  payfrmage            NUMBER(13),
  idtype               CHAR(2),
  aidtype              CHAR(2),
  aperson_id           INTEGER,
  benparam             INTEGER,
  comnum               INTEGER,
  csr_id               INTEGER,
  currency             CHAR(3),
  dcdm                 CHAR(2),
  del_date             CHAR(8),
  del_type             INTEGER,
  deskpay              CHAR(1),
  discount             NUMBER(20,6),
  empno_id             INTEGER,
  gcon_id              INTEGER,
  iscard               CHAR(1),
  opdate               DATE,
  oper_id              INTEGER,
  person_id            INTEGER,
  reason               VARCHAR2(200),
  reg_code             VARCHAR2(20),
  renewdate            DATE,
  riskcon_id           INTEGER,
  specagr              VARCHAR2(4000),
  sno                  INTEGER,
  typeid               INTEGER,
  sharetype            CHAR(3),
  branch               CHAR(14),
  begtime              DATE,
  endtime              DATE,
  sour_sys             VARCHAR2(20),
  src_sys              VARCHAR2(20),
  sale_prod_code       VARCHAR2(8),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  workno               VARCHAR2(20),
  comb_policy_no       VARCHAR2(20),
  app_age              NUMBER(3),
  sub_agt_no           VARCHAR2(20),
  o_classcode          VARCHAR2(8),
  cross_sale_ind       VARCHAR2(1),
  prem_rate_level      VARCHAR2(2),
  ill_score            NUMBER(10,2),
  group_no             VARCHAR2(2),
  digital_sign_ind     VARCHAR2(1),
  supply_input_ind     VARCHAR2(1),
  ini_invoice_status   VARCHAR2(2),
  uni_pay_ind          VARCHAR2(1),
  appointed_opdate_ind VARCHAR2(1),
  contcode             VARCHAR2(8)
);




create table PPS.D01_RISKCON_2020BK
(
  policyno             CHAR(15) not null,
  polist               CHAR(1),
  npaylen              NUMBER(20,6),
  yearnum              INTEGER,
  ttime                INTEGER,
  tmount               NUMBER(16,2),
  pieces               NUMBER(13,3),
  classcode            VARCHAR2(8) not null,
  empno                VARCHAR2(20),
  begdate              DATE,
  contno               VARCHAR2(20),
  fgsno                CHAR(3) not null,
  paycode              CHAR(2),
  delcode              CHAR(1),
  prelname             CHAR(6),
  pedate               DATE,
  pid                  CHAR(30),
  job                  CHAR(20),
  apid                 CHAR(30),
  operno               VARCHAR2(20),
  stopdat              DATE,
  appdate              DATE,
  delage               INTEGER,
  appno                VARCHAR2(20),
  dbdate               DATE,
  appf                 VARCHAR2(2),
  recaddr              VARCHAR2(508),
  deptno               VARCHAR2(20),
  nextdate             DATE,
  stopage              INTEGER,
  payseq               VARCHAR2(2048),
  unstdrate            NUMBER(10,2),
  reczip               VARCHAR2(12),
  losttime             INTEGER,
  rate                 NUMBER(4,2),
  stdrate              NUMBER(10,2),
  currcode             VARCHAR2(2),
  deltlag              VARCHAR2(2),
  debist               VARCHAR2(2),
  del                  VARCHAR2(2),
  paystatus            VARCHAR2(2),
  delstat              VARCHAR2(2),
  rectele              VARCHAR2(30),
  saleattr             VARCHAR2(2),
  stopdate             DATE,
  gpolicyno            VARCHAR2(15),
  etl_time             DATE,
  payfmage             INTEGER,
  nretdate             DATE,
  renewid              CHAR(1),
  bankflag             CHAR(2),
  payfrmage            NUMBER(13),
  idtype               CHAR(2),
  aidtype              CHAR(2),
  aperson_id           INTEGER,
  benparam             INTEGER,
  comnum               INTEGER,
  csr_id               INTEGER,
  currency             CHAR(3),
  dcdm                 CHAR(2),
  del_date             CHAR(8),
  del_type             INTEGER,
  deskpay              CHAR(1),
  discount             NUMBER(20,6),
  empno_id             INTEGER,
  gcon_id              INTEGER,
  iscard               CHAR(1),
  opdate               DATE,
  oper_id              INTEGER,
  person_id            INTEGER,
  reason               VARCHAR2(200),
  reg_code             VARCHAR2(20),
  renewdate            DATE,
  riskcon_id           INTEGER,
  specagr              VARCHAR2(4000),
  sno                  INTEGER,
  typeid               INTEGER,
  sharetype            CHAR(3),
  branch               CHAR(14),
  begtime              DATE,
  endtime              DATE,
  sour_sys             VARCHAR2(20),
  src_sys              VARCHAR2(20),
  sale_prod_code       VARCHAR2(8),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  workno               VARCHAR2(20),
  comb_policy_no       VARCHAR2(20),
  app_age              NUMBER(3),
  sub_agt_no           VARCHAR2(20),
  o_classcode          VARCHAR2(8),
  cross_sale_ind       VARCHAR2(1),
  prem_rate_level      VARCHAR2(2),
  ill_score            NUMBER(10,2),
  group_no             VARCHAR2(2),
  digital_sign_ind     VARCHAR2(1),
  supply_input_ind     VARCHAR2(1),
  ini_invoice_status   VARCHAR2(2),
  uni_pay_ind          VARCHAR2(1),
  appointed_opdate_ind VARCHAR2(1),
  contcode             VARCHAR2(8)
)



;




create table PPS.JOB_EXCEPTION
(
  sp_name   VARCHAR2(30),
  exp_name  VARCHAR2(200),
  exp_des   VARCHAR2(512),
  row_count NUMBER,
  exp_time  DATE
)



;





create table PPS.JOB_IDS_LOG
(
  sno       NUMBER(22),
  sp_name   VARCHAR2(30),
  line      NUMBER(22),
  tab_name  VARCHAR2(40),
  branch    VARCHAR2(14),
  log_des   VARCHAR2(200),
  row_count NUMBER(22),
  log_time  DATE default sysdate
)



;




create table PPS.JOB_M_DAILYLOG
(
  dailylogid NUMBER default 0,
  trandate   CHAR(8),
  branchid   CHAR(7),
  trantype   CHAR(2),
  errinfor   VARCHAR2(4000),
  tranflag   CHAR(1),
  trantime   DATE,
  typeflag   CHAR(2),
  doneway    VARCHAR2(255),
  explain    VARCHAR2(255),
  memo       VARCHAR2(255)
)



;






create table PPS.JOB_PAYDETAILED
(
  policyno   VARCHAR2(20),
  classcode  VARCHAR2(8),
  delcode    VARCHAR2(8),
  typeno     VARCHAR2(8),
  delnum     VARCHAR2(4),
  nxt_pay_dt VARCHAR2(10),
  branchid   VARCHAR2(14),
  compid     VARCHAR2(14),
  utime      DATE,
  updatetime DATE,
  state      VARCHAR2(2),
  srcsys     VARCHAR2(20),
  choice     NUMBER default 0
)



;






create table PPS.JOB_PAYDIFFERENCE
(
  policyno     VARCHAR2(20),
  classcode    VARCHAR2(8),
  delcode      VARCHAR2(3),
  typeno       VARCHAR2(2),
  delnum       VARCHAR2(4),
  nxt_pay_dt   VARCHAR2(10),
  branchid     VARCHAR2(14),
  compid       VARCHAR2(14),
  utime        DATE,
  updatetime   DATE,
  status       VARCHAR2(2),
  reminderdate VARCHAR2(8),
  srcsys       VARCHAR2(20),
  choice       NUMBER default 0
)



;






create table PPS.JOB_PAYDIFFERENCE_2020BK
(
  policyno     VARCHAR2(20),
  classcode    VARCHAR2(8),
  delcode      VARCHAR2(3),
  typeno       VARCHAR2(2),
  delnum       VARCHAR2(4),
  nxt_pay_dt   VARCHAR2(10),
  branchid     VARCHAR2(14),
  compid       VARCHAR2(14),
  utime        DATE,
  updatetime   DATE,
  status       VARCHAR2(2),
  reminderdate VARCHAR2(8),
  srcsys       VARCHAR2(20),
  choice       NUMBER
)



;




create table PPS.JOB_PAYMENTCHECK
(
  policyno              VARCHAR2(20) not null,
  classcode             VARCHAR2(8) not null,
  delcode               VARCHAR2(3) not null,
  typeno                VARCHAR2(2) not null,
  delnum                VARCHAR2(4) not null,
  nxt_pay_dt            VARCHAR2(10),
  branchid              VARCHAR2(14),
  compid                VARCHAR2(14),
  utime                 DATE,
  updatetime            DATE,
  status                VARCHAR2(2),
  reminderdate          VARCHAR2(8),
  effective             VARCHAR2(1),
  check_flg             VARCHAR2(1),
  surv_flg              VARCHAR2(1),
  del_flg               VARCHAR2(1),
  pay_type              VARCHAR2(1),
  send_flag             VARCHAR2(1) default '0',
  is_payment            VARCHAR2(1),
  responsibility_system VARCHAR2(1),
  problem_type          VARCHAR2(1),
  leakage_reason        VARCHAR2(200),
  problem_colse_reason  VARCHAR2(200),
  duty_status           VARCHAR2(1) default '1'
);





create table PPS.JOB_POLICYNOPAY
(
  policyno        VARCHAR2(15) not null,
  classcode       VARCHAR2(8) not null,
  delcode         VARCHAR2(8) not null,
  typeno          VARCHAR2(8) not null,
  nretdate        DATE,
  nxt_pay_dt      VARCHAR2(10),
  nxt_pay_num     VARCHAR2(4),
  nxt_due_dt      VARCHAR2(10),
  stopdate        DATE,
  begdate         DATE,
  branchid        VARCHAR2(14),
  compid          VARCHAR2(14),
  utime           DATE,
  updatetime      DATE,
  state           VARCHAR2(2),
  delfrm          VARCHAR2(4),
  fixmon          NUMBER,
  policy_table_id VARCHAR2(3),
  deldateday      NUMBER,
  begno           NUMBER,
  endno           NUMBER,
  begage          NUMBER,
  endage          NUMBER,
  fixage          NUMBER,
  diestatus       NUMBER,
  curcalcage      NUMBER,
  choice          NUMBER default 0
);



create table PPS.JOB_POLICYNOPAY_2020BK
(
  policyno        VARCHAR2(15),
  classcode       VARCHAR2(8),
  delcode         VARCHAR2(8),
  typeno          VARCHAR2(8),
  nretdate        DATE,
  nxt_pay_dt      VARCHAR2(10),
  nxt_pay_num     VARCHAR2(4),
  nxt_due_dt      VARCHAR2(10),
  stopdate        DATE,
  begdate         DATE,
  branchid        VARCHAR2(14),
  compid          VARCHAR2(14),
  utime           DATE,
  updatetime      DATE,
  state           VARCHAR2(2),
  delfrm          VARCHAR2(4),
  fixmon          NUMBER,
  policy_table_id VARCHAR2(3),
  deldateday      NUMBER,
  begno           NUMBER,
  endno           NUMBER,
  begage          NUMBER,
  endage          NUMBER,
  fixage          NUMBER,
  diestatus       NUMBER,
  curcalcage      NUMBER,
  choice          NUMBER
);




create table PPS.JOB_T_LOG
(
  sp_name   VARCHAR2(30),
  log_num   INTEGER,
  log_des   VARCHAR2(200),
  row_count INTEGER,
  log_time  DATE
);



create table PPS.STAT_ERROR_LOG
(
  work_date    VARCHAR2(8),
  proc_type    VARCHAR2(10),
  proc_name    VARCHAR2(200),
  step_no      VARCHAR2(20),
  step_desc    VARCHAR2(4000),
  row_num      NUMBER,
  sql_code     VARCHAR2(20),
  sql_errm     VARCHAR2(2000),
  execute_flag VARCHAR2(10),
  begin_time   DATE,
  end_time     DATE
);




create table PPS.TBL_BENEFIT_CHK_INFO
(
  policyno_id      VARCHAR2(20) not null,
  class_code       CHAR(8) not null,
  del_code         CHAR(3) not null,
  type_no          CHAR(2) not null,
  del_num          CHAR(4) not null,
  load_date        CHAR(8),
  del_date         CHAR(8),
  del_amt          VARCHAR2(21),
  del_fre          CHAR(1),
  pay_type         CHAR(1),
  noti_type        CHAR(2),
  app_date         CHAR(8),
  gpolicyno_id     VARCHAR2(20),
  pieces           VARCHAR2(21),
  risk_sta         CHAR(1),
  xsqd             CHAR(2),
  pay_len          VARCHAR2(21),
  dividend_balance VARCHAR2(21),
  pre_dividend_amt VARCHAR2(21),
  cvrg_app_date    CHAR(8),
  pid_id           VARCHAR2(40),
  cust_name        VARCHAR2(120),
  payee_id         VARCHAR2(40),
  payee_name       VARCHAR2(120),
  apid_id          VARCHAR2(40),
  acust_name       VARCHAR2(120),
  asex_flg         CHAR(1),
  abirth_date      CHAR(8),
  health_sta       CHAR(2),
  reason           VARCHAR2(200),
  bank_code        VARCHAR2(20),
  bank_name        VARCHAR2(120),
  del_bkno         VARCHAR2(60),
  bkno_name        VARCHAR2(60),
  branch_id        CHAR(14) not null,
  comp_id          CHAR(14),
  src_sys          CHAR(20),
  chk_last_amt     CHAR(1),
  chk_last_date    CHAR(1),
  chk_last_bkno    CHAR(1),
  last_del_date    CHAR(8),
  last_del_amt     CHAR(21),
  last_del_bkno    VARCHAR2(80),
  check_flg        CHAR(1),
  cl_chkflag       CHAR(1),
  check_opr        CHAR(8),
  check_date       CHAR(8),
  check_rea        CHAR(2),
  check_rea2       CHAR(2),
  sub_pkey_cd1     VARCHAR2(30),
  sub_pkey_cd2     VARCHAR2(30),
  sub_pkey_cd3     VARCHAR2(30),
  misc_tx          VARCHAR2(256),
  cvrg_app_age     CHAR(4),
  first_ben_date   CHAR(8),
  first_del_amt    CHAR(21)
);



create table PPS.TBL_BENEFIT_CHK_INFO_2020BK
(
  policyno_id      VARCHAR2(20) not null,
  class_code       CHAR(8) not null,
  del_code         CHAR(3) not null,
  type_no          CHAR(2) not null,
  del_num          CHAR(4) not null,
  load_date        CHAR(8),
  del_date         CHAR(8),
  del_amt          VARCHAR2(21),
  del_fre          CHAR(1),
  pay_type         CHAR(1),
  noti_type        CHAR(2),
  app_date         CHAR(8),
  gpolicyno_id     VARCHAR2(20),
  pieces           VARCHAR2(21),
  risk_sta         CHAR(1),
  xsqd             CHAR(2),
  pay_len          VARCHAR2(21),
  dividend_balance VARCHAR2(21),
  pre_dividend_amt VARCHAR2(21),
  cvrg_app_date    CHAR(8),
  pid_id           VARCHAR2(40),
  cust_name        VARCHAR2(120),
  payee_id         VARCHAR2(40),
  payee_name       VARCHAR2(120),
  apid_id          VARCHAR2(40),
  acust_name       VARCHAR2(120),
  asex_flg         CHAR(1),
  abirth_date      CHAR(8),
  health_sta       CHAR(2),
  reason           VARCHAR2(200),
  bank_code        VARCHAR2(20),
  bank_name        VARCHAR2(120),
  del_bkno         VARCHAR2(60),
  bkno_name        VARCHAR2(60),
  branch_id        CHAR(14) not null,
  comp_id          CHAR(14),
  src_sys          CHAR(20),
  chk_last_amt     CHAR(1),
  chk_last_date    CHAR(1),
  chk_last_bkno    CHAR(1),
  last_del_date    CHAR(8),
  last_del_amt     CHAR(21),
  last_del_bkno    VARCHAR2(80),
  check_flg        CHAR(1),
  cl_chkflag       CHAR(1),
  check_opr        CHAR(8),
  check_date       CHAR(8),
  check_rea        CHAR(2),
  check_rea2       CHAR(2),
  sub_pkey_cd1     VARCHAR2(30),
  sub_pkey_cd2     VARCHAR2(30),
  sub_pkey_cd3     VARCHAR2(30),
  misc_tx          VARCHAR2(256),
  cvrg_app_age     CHAR(4),
  first_ben_date   CHAR(8),
  first_del_amt    CHAR(21)
)



;



 

create table PPS.TBL_BENEFIT_INFO
(
  policyno_id          VARCHAR2(20) not null,
  class_code           CHAR(8) not null,
  del_code             CHAR(3) not null,
  type_no              CHAR(2) not null,
  del_num              CHAR(4) not null,
  load_date            CHAR(8),
  del_date             CHAR(8),
  del_amt              VARCHAR2(21),
  del_fre              CHAR(1),
  pay_type             CHAR(1),
  noti_type            CHAR(2),
  app_date             CHAR(8),
  gpolicyno_id         VARCHAR2(20),
  pieces               VARCHAR2(21),
  risk_sta             CHAR(1),
  xsqd                 CHAR(2),
  pay_len              VARCHAR2(21),
  tmount               VARCHAR2(21),
  dividend_balance     VARCHAR2(21),
  pre_dividend_amt     VARCHAR2(21),
  empno                VARCHAR2(20),
  operno               VARCHAR2(20),
  deptno               VARCHAR2(20),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  payee_source_id      VARCHAR2(20),
  owner_insured_rlship CHAR(3),
  cvrg_app_date        CHAR(8),
  cvrg_app_age         CHAR(4),
  reason               VARCHAR2(200),
  pid_id               VARCHAR2(40),
  cust_name            VARCHAR2(120),
  sex_flg              CHAR(1),
  birth_date           CHAR(8),
  addr                 VARCHAR2(500),
  zip                  VARCHAR2(20),
  telphone             VARCHAR2(100),
  mobphone             VARCHAR2(20),
  email                VARCHAR2(200),
  payee_id             VARCHAR2(40),
  payee_name           VARCHAR2(120),
  pesex_flg            CHAR(1),
  pebirth_date         CHAR(8),
  peaddr               VARCHAR2(500),
  pezip                VARCHAR2(20),
  petelphone           VARCHAR2(100),
  pemobphone           VARCHAR2(20),
  peemail              VARCHAR2(200),
  apid_id              VARCHAR2(40),
  acust_name           VARCHAR2(120),
  asex_flg             CHAR(1),
  abirth_date          CHAR(8),
  aaddr                VARCHAR2(500),
  azip                 VARCHAR2(20),
  atelphone            VARCHAR2(100),
  amobphone            VARCHAR2(20),
  aemail               VARCHAR2(200),
  health_sta           CHAR(2),
  surv_date            CHAR(8),
  surv_fre             CHAR(4),
  ben_id               VARCHAR2(40),
  bank_code            VARCHAR2(20),
  bank_name            VARCHAR2(80),
  del_bkno             VARCHAR2(60),
  bkno_name            VARCHAR2(60),
  comp_id              CHAR(14),
  branch_id            CHAR(14),
  src_sys              CHAR(20),
  source_bp_id         VARCHAR2(60),
  chk_last_amt         CHAR(1),
  chk_last_date        CHAR(1),
  chk_last_bkno        CHAR(1),
  last_del_date        CHAR(8),
  last_del_amt         CHAR(21),
  last_del_bkno        VARCHAR2(80),
  check_flg            CHAR(1),
  check_opr            CHAR(8),
  check_date           CHAR(8),
  check_rea2           CHAR(2),
  check_rea            VARCHAR2(100),
  surv_flg             CHAR(1),
  task_id              VARCHAR2(15),
  noti_id              CHAR(1),
  ret_flg              CHAR(1),
  proc_flg             VARCHAR2(10),
  del_flg              CHAR(1),
  sub_pkey_cd1         VARCHAR2(30),
  sub_pkey_cd2         VARCHAR2(30),
  sub_pkey_cd3         VARCHAR2(30),
  misc_tx              VARCHAR2(256),
  last_upd_opr_id      CHAR(10),
  last_upd_txn_id      CHAR(10),
  last_upd_ts          CHAR(14),
  csr_no               VARCHAR2(20),
  hf_flg               CHAR(1),
  revisit_date         CHAR(8),
  revisit_reason       CHAR(2),
  revisit_empno        VARCHAR2(20),
  cl_chkdate           CHAR(8),
  cl_chkflag           CHAR(1),
  cl_auto_reason       CHAR(2),
  cl_manu_reason       CHAR(2),
  effective            CHAR(1) default ' ',
  pre_payment_ind      VARCHAR2(1) default '0',
  cntstart             CHAR(11),
  premamt              VARCHAR2(21),
  bpoption             VARCHAR2(21),
  cover                VARCHAR2(21),
  paycode              CHAR(2),
  hf_flg2              CHAR(1),
  regamt               VARCHAR2(21),
  cupnamt              VARCHAR2(21)
);



 

create table PPS.TBL_BENEFIT_INFO_COUNT
(
  policyno_id VARCHAR2(20) not null,
  class_code  CHAR(8) not null,
  del_code    CHAR(3) not null,
  type_no     CHAR(3) not null,
  del_num     CHAR(4) not null,
  load_date   CHAR(8),
  del_date    CHAR(8),
  del_flg     CHAR(1),
  del_amt     VARCHAR2(21),
  branch_id   CHAR(14),
  comp_id     CHAR(14),
  channel     VARCHAR2(4)
);



create table PPS.TBL_BENEFIT_INFO_COUNT_2020BK
(
  policyno_id VARCHAR2(20),
  class_code  CHAR(8),
  del_code    CHAR(3),
  type_no     CHAR(3),
  del_num     CHAR(4),
  load_date   CHAR(8),
  del_date    CHAR(8),
  del_flg     CHAR(1),
  del_amt     VARCHAR2(21),
  branch_id   CHAR(14),
  comp_id     CHAR(14),
  channel     VARCHAR2(4)
)



;





create table PPS.TBL_BEN_NOTI_INFO
(
  policyno_id      VARCHAR2(20) not null,
  class_code       CHAR(8) not null,
  del_code         CHAR(3) not null,
  type_no          CHAR(2) not null,
  del_num          CHAR(4) not null,
  noti_type        CHAR(2) not null,
  mod_type         CHAR(2),
  type_name        VARCHAR2(100),
  del_name         VARCHAR2(50),
  del_amt          CHAR(16),
  del_date         CHAR(8),
  risk_sta         CHAR(1),
  xsqd             CHAR(2),
  cvrg_app_date    CHAR(8),
  pieces           VARCHAR2(21),
  dividend_balance VARCHAR2(21),
  pre_dividend_amt VARCHAR2(21),
  cust_name        VARCHAR2(120),
  acust_name       VARCHAR2(120),
  azip             VARCHAR2(20),
  aaddr            VARCHAR2(500),
  atelphone        VARCHAR2(100),
  payee_id         VARCHAR2(40),
  payee_name       VARCHAR2(120),
  cust_add         VARCHAR2(500),
  add_post         VARCHAR2(20),
  phone_num        VARCHAR2(100),
  mob_num          VARCHAR2(20),
  email            VARCHAR2(200),
  bank_code        VARCHAR2(20),
  bank_name        VARCHAR2(80),
  del_bkno         VARCHAR2(60),
  bkno_name        VARCHAR2(60),
  comp_id          CHAR(14),
  branch_id        CHAR(14),
  send_date        CHAR(8),
  send_sta         CHAR(2),
  sub_pkey_cd1     VARCHAR2(30),
  sub_pkey_cd2     VARCHAR2(30),
  sub_pkey_cd3     VARCHAR2(30),
  misc_tx          VARCHAR2(256),
  last_upd_opr_id  CHAR(10),
  last_upd_txn_id  CHAR(10),
  last_upd_ts      CHAR(14),
  send_object      CHAR(2),
  app_materials    CHAR(2),
  wechat           VARCHAR2(10),
  wcresult         CHAR(2),
  wcstime          DATE,
  message          VARCHAR2(50),
  meresult         CHAR(2),
  mestime          DATE,
  emails           VARCHAR2(10),
  emresult         CHAR(2),
  emstime          DATE,
  app              VARCHAR2(10),
  appresult        CHAR(2),
  appstime         DATE
);




create table PPS.TBL_BEN_NOTI_INFO_2020BK
(
  policyno_id      VARCHAR2(20) not null,
  class_code       CHAR(8) not null,
  del_code         CHAR(3) not null,
  type_no          CHAR(2) not null,
  del_num          CHAR(4) not null,
  noti_type        CHAR(2) not null,
  mod_type         CHAR(2),
  type_name        VARCHAR2(100),
  del_name         VARCHAR2(50),
  del_amt          CHAR(16),
  del_date         CHAR(8),
  risk_sta         CHAR(1),
  xsqd             CHAR(2),
  cvrg_app_date    CHAR(8),
  pieces           VARCHAR2(21),
  dividend_balance VARCHAR2(21),
  pre_dividend_amt VARCHAR2(21),
  cust_name        VARCHAR2(120),
  acust_name       VARCHAR2(120),
  azip             VARCHAR2(20),
  aaddr            VARCHAR2(500),
  atelphone        VARCHAR2(100),
  payee_id         VARCHAR2(40),
  payee_name       VARCHAR2(120),
  cust_add         VARCHAR2(500),
  add_post         VARCHAR2(20),
  phone_num        VARCHAR2(100),
  mob_num          VARCHAR2(20),
  email            VARCHAR2(200),
  bank_code        VARCHAR2(20),
  bank_name        VARCHAR2(80),
  del_bkno         VARCHAR2(60),
  bkno_name        VARCHAR2(60),
  comp_id          CHAR(14),
  branch_id        CHAR(14),
  send_date        CHAR(8),
  send_sta         CHAR(2),
  sub_pkey_cd1     VARCHAR2(30),
  sub_pkey_cd2     VARCHAR2(30),
  sub_pkey_cd3     VARCHAR2(30),
  misc_tx          VARCHAR2(256),
  last_upd_opr_id  CHAR(10),
  last_upd_txn_id  CHAR(10),
  last_upd_ts      CHAR(14),
  send_object      CHAR(2),
  app_materials    CHAR(2),
  wechat           VARCHAR2(10),
  wcresult         CHAR(2),
  wcstime          DATE,
  message          VARCHAR2(50),
  meresult         CHAR(2),
  mestime          DATE,
  emails           VARCHAR2(10),
  emresult         CHAR(2),
  emstime          DATE,
  app              VARCHAR2(10),
  appresult        CHAR(2),
  appstime         DATE
)



;




create table PPS.TBL_CUST_BEN_INFO
(
  pid_id          VARCHAR2(40) not null,
  policyno_id     VARCHAR2(20) not null,
  class_code      CHAR(8) not null,
  del_code        CHAR(3) not null,
  type_no         CHAR(2) not null,
  first_ben_date  CHAR(8),
  first_del_amt   CHAR(21),
  last_ben_date   CHAR(8),
  last_del_amt    CHAR(21),
  last_del_num    CHAR(4),
  bank_code       VARCHAR2(20),
  bank_name       VARCHAR2(80),
  del_bkno        VARCHAR2(60),
  bkno_name       VARCHAR2(60),
  branch_id       CHAR(14),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14)
)



;




create table PPS.TBL_CUST_BEN_INFO_2020BK
(
  pid_id          VARCHAR2(40) not null,
  policyno_id     VARCHAR2(20) not null,
  class_code      CHAR(8) not null,
  del_code        CHAR(3) not null,
  type_no         CHAR(2) not null,
  first_ben_date  CHAR(8),
  first_del_amt   CHAR(21),
  last_ben_date   CHAR(8),
  last_del_amt    CHAR(21),
  last_del_num    CHAR(4),
  bank_code       VARCHAR2(20),
  bank_name       VARCHAR2(80),
  del_bkno        VARCHAR2(60),
  bkno_name       VARCHAR2(60),
  branch_id       CHAR(14),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14)
)



;




create table PPS.TBL_CUST_INFO
(
  cust_id         VARCHAR2(20) not null,
  pid_id          CHAR(40) not null,
  add_id          CHAR(40),
  id_type         CHAR(2),
  cust_name       VARCHAR2(120),
  sex_flg         CHAR(1),
  birth_date      CHAR(8),
  telphone        VARCHAR2(100),
  vip_type        CHAR(2),
  health_sta      CHAR(2),
  surv_date       CHAR(8),
  surv_fre        CHAR(4),
  branch_id       CHAR(14),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14)
)
;


create table PPS.TBL_FAIL_BENEFIT_INFO
(
  pre_cdelrec_id       CHAR(20),
  sno                  CHAR(20),
  source_bp_id         VARCHAR2(60) not null,
  policyno_id          VARCHAR2(20),
  class_code           CHAR(8),
  del_code             CHAR(3),
  type_no              CHAR(2),
  del_num              CHAR(20),
  del_fre              CHAR(1),
  del_amt              CHAR(21),
  setcode              CHAR(1),
  del_flg              CHAR(1),
  checkflag            CHAR(1),
  noti_type            CHAR(2),
  appdate              CHAR(10),
  check_date           CHAR(10),
  deldate              CHAR(10),
  bcode                VARCHAR2(20),
  bank_name            VARCHAR2(80),
  delbkno              VARCHAR2(60),
  bkno_name            VARCHAR2(60),
  payee_id             VARCHAR2(40),
  operno               VARCHAR2(20),
  checkno              VARCHAR2(20),
  comp_id              CHAR(14),
  gpolicyno_id         VARCHAR2(20),
  pid_id               VARCHAR2(40),
  apid_id              VARCHAR2(40),
  pieces               CHAR(21),
  risk_sta             CHAR(1),
  xsqd                 CHAR(2),
  pay_len              CHAR(21),
  tmount               VARCHAR2(21),
  dividend_balance     VARCHAR2(21),
  pre_dividend_amt     VARCHAR2(21),
  empno                VARCHAR2(20),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  payee_source_id      VARCHAR2(20),
  owner_insured_rlship CHAR(3),
  reason               VARCHAR2(200),
  cvrg_app_date        CHAR(8),
  cvrg_app_age         CHAR(4),
  begtime              CHAR(11),
  endtime              CHAR(11),
  branch_id            CHAR(14),
  etl_time             VARCHAR2(20),
  src_sys              VARCHAR2(20),
  fail_reason          CHAR(1),
  proc_sta             CHAR(1),
  sub_pkey_cd1         VARCHAR2(30),
  sub_pkey_cd2         VARCHAR2(30),
  sub_pkey_cd3         VARCHAR2(30),
  misc_tx              VARCHAR2(256),
  last_upd_opr_id      CHAR(10),
  last_upd_txn_id      CHAR(10),
  last_upd_ts          CHAR(14),
  o_classcode          CHAR(8),
  ff_cvrg_status       VARCHAR2(20),
  csr_no               VARCHAR2(20),
  pre_payment_ind      VARCHAR2(1) default 0,
  cntstart             CHAR(11),
  premamt              VARCHAR2(21),
  bpoption             VARCHAR2(21),
  cover                VARCHAR2(21),
  paycode              CHAR(2),
  eventnum             CHAR(20)
)



;






create table PPS.TBL_FAIL_BENEFIT_INFO_2020BK
(
  pre_cdelrec_id       CHAR(20),
  sno                  CHAR(20),
  source_bp_id         VARCHAR2(60) not null,
  policyno_id          VARCHAR2(20),
  class_code           CHAR(8),
  del_code             CHAR(3),
  type_no              CHAR(2),
  del_num              CHAR(20),
  del_fre              CHAR(1),
  del_amt              CHAR(21),
  setcode              CHAR(1),
  del_flg              CHAR(1),
  checkflag            CHAR(1),
  noti_type            CHAR(2),
  appdate              CHAR(10),
  check_date           CHAR(10),
  deldate              CHAR(10),
  bcode                VARCHAR2(20),
  bank_name            VARCHAR2(80),
  delbkno              VARCHAR2(60),
  bkno_name            VARCHAR2(60),
  payee_id             VARCHAR2(40),
  operno               VARCHAR2(20),
  checkno              VARCHAR2(20),
  comp_id              CHAR(14),
  gpolicyno_id         VARCHAR2(20),
  pid_id               VARCHAR2(40),
  apid_id              VARCHAR2(40),
  pieces               CHAR(21),
  risk_sta             CHAR(1),
  xsqd                 CHAR(2),
  pay_len              CHAR(21),
  tmount               VARCHAR2(21),
  dividend_balance     VARCHAR2(21),
  pre_dividend_amt     VARCHAR2(21),
  empno                VARCHAR2(20),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  payee_source_id      VARCHAR2(20),
  owner_insured_rlship CHAR(3),
  reason               VARCHAR2(200),
  cvrg_app_date        CHAR(8),
  cvrg_app_age         CHAR(4),
  begtime              CHAR(11),
  endtime              CHAR(11),
  branch_id            CHAR(14),
  etl_time             VARCHAR2(20),
  src_sys              VARCHAR2(20),
  fail_reason          CHAR(1),
  proc_sta             CHAR(1),
  sub_pkey_cd1         VARCHAR2(30),
  sub_pkey_cd2         VARCHAR2(30),
  sub_pkey_cd3         VARCHAR2(30),
  misc_tx              VARCHAR2(256),
  last_upd_opr_id      CHAR(10),
  last_upd_txn_id      CHAR(10),
  last_upd_ts          CHAR(14),
  o_classcode          CHAR(8),
  ff_cvrg_status       VARCHAR2(20),
  csr_no               VARCHAR2(20),
  pre_payment_ind      VARCHAR2(1),
  cntstart             CHAR(11),
  premamt              VARCHAR2(21),
  bpoption             VARCHAR2(21),
  cover                VARCHAR2(21),
  paycode              CHAR(2),
  eventnum             CHAR(20)
)



;




create table PPS.TBL_IDS_ACCOUNT
(
  sno        VARCHAR2(20),
  typeid     VARCHAR2(7),
  account_id VARCHAR2(20),
  acco_no    VARCHAR2(120),
  usetype    CHAR(1),
  regdate    VARCHAR2(10),
  open_date  VARCHAR2(10),
  status     CHAR(1),
  s_date     VARCHAR2(10),
  s_reason   CHAR(2),
  currency   CHAR(3),
  bankcode   VARCHAR2(20),
  bankname   VARCHAR2(80),
  policyno   VARCHAR2(20),
  ownerid    VARCHAR2(40),
  oper_no    VARCHAR2(20),
  begtime    VARCHAR2(20),
  endtime    VARCHAR2(20),
  branch     CHAR(14),
  etl_time   VARCHAR2(21),
  src_sys    VARCHAR2(20)
)



;





create table PPS.TBL_IDS_ADDRESS
(
  sno              VARCHAR2(20),
  typeid           VARCHAR2(7),
  country          VARCHAR2(20),
  city             VARCHAR2(60),
  region           VARCHAR2(60),
  subregion        VARCHAR2(80),
  street           VARCHAR2(500),
  subaddr          VARCHAR2(500),
  add_post         CHAR(20),
  cust_add         VARCHAR2(500),
  usage            CHAR(1) not null,
  person_id        VARCHAR2(20),
  pid              VARCHAR2(40),
  idtype           CHAR(2),
  purpose          CHAR(1),
  seq              VARCHAR2(10) not null,
  source_person_id VARCHAR2(20) not null,
  id15             VARCHAR2(40),
  begtime          VARCHAR2(20),
  endtime          VARCHAR2(20),
  branch_id        CHAR(14),
  etl_time         VARCHAR2(21),
  src_sys          VARCHAR2(20),
  busi_usage       VARCHAR2(1),
  src_admin_div_id VARCHAR2(18)
)
;


create table PPS.TBL_IDS_CDELREC
(
  sno               NUMBER(20),
  typeid            NUMBER(7) not null,
  con_id            NUMBER(20),
  policyno          VARCHAR2(20) not null,
  classcode         VARCHAR2(8) not null,
  amount            NUMBER(20,4) not null,
  currency          VARCHAR2(3) not null,
  gendate           DATE not null,
  delfrm            VARCHAR2(1) not null,
  setcode           VARCHAR2(1) not null,
  status            VARCHAR2(1) not null,
  movewhy           VARCHAR2(120),
  delcode           VARCHAR2(3) not null,
  typeno            VARCHAR2(2) not null,
  paydate           DATE,
  oper_id           NUMBER(20),
  operno            VARCHAR2(20) not null,
  deptno            VARCHAR2(20) not null,
  docpro            VARCHAR2(1),
  docno             VARCHAR2(20),
  person_id         NUMBER(20),
  pid               VARCHAR2(40),
  idtype            VARCHAR2(2),
  paytime           NUMBER(20) not null,
  appdate           DATE,
  bankstat          VARCHAR2(1) not null,
  account_id        NUMBER(20),
  acco_no           VARCHAR2(120),
  delcoca           DATE,
  seqnc             VARCHAR2(60) not null,
  subsequent_bp_ind VARCHAR2(1),
  bank_code         VARCHAR2(20),
  insrnc_rate       NUMBER(7,4),
  begtime           NUMBER(11) not null,
  endtime           NUMBER(11) not null,
  branch            CHAR(14) not null,
  etl_time          DATE not null,
  src_sys           VARCHAR2(20) not null
)



;




create table PPS.TBL_IDS_CHGCON
(
  sno           VARCHAR2(20),
  typeid        VARCHAR2(7),
  changeid      CHAR(2),
  oldstr        VARCHAR2(2048),
  newstr        VARCHAR2(2048),
  oldval        VARCHAR2(22),
  newval        VARCHAR2(22),
  modyname      VARCHAR2(80) not null,
  apppos_id     VARCHAR2(20),
  modino        VARCHAR2(20) not null,
  con_id        VARCHAR2(20),
  policyno      VARCHAR2(20) not null,
  classcode     CHAR(8) not null,
  begtime       VARCHAR2(20),
  endtime       VARCHAR2(20),
  branch_id     CHAR(14),
  etl_time      VARCHAR2(21),
  src_sys       VARCHAR2(20),
  ff_event_id   VARCHAR2(20),
  ff_event_type VARCHAR2(60)
)



;




create table PPS.TBL_IDS_DEBITREC
(
  policyno         VARCHAR2(20),
  loandate         DATE,
  classcode        CHAR(8),
  currcode         CHAR(3),
  operno           VARCHAR2(20),
  loanrate         NUMBER(6,4),
  id               VARCHAR2(40),
  lid              VARCHAR2(40),
  amount           NUMBER(10,2),
  recptno          VARCHAR2(20),
  verno            VARCHAR2(20),
  retdate          DATE,
  sign             CHAR(1),
  retopr           VARCHAR2(20),
  retrate          NUMBER(10,5),
  retanct          NUMBER(15,4),
  due_interest     NUMBER(15,4),
  overdue_interest NUMBER(15,4),
  extend_ind       CHAR(1),
  continued_ind    CHAR(1),
  full_repay_ind   CHAR(1),
  begtime          DATE,
  endtime          DATE,
  branch           CHAR(14),
  etl_time         TIMESTAMP(6),
  src_sys          VARCHAR2(20),
  pos_no           VARCHAR2(20),
  compound_ind     VARCHAR2(1),
  policydept       VARCHAR2(14),
  bankcode         VARCHAR2(12),
  acctno           VARCHAR2(32)
);




create table PPS.TBL_IDS_EMAIL
(
  sno              VARCHAR2(20),
  typeid           VARCHAR2(7),
  domain           VARCHAR2(20),
  node             VARCHAR2(20),
  userid           VARCHAR2(20),
  etype            CHAR(4),
  eaddr            VARCHAR2(200),
  usage            CHAR(1) not null,
  person_id        VARCHAR2(20),
  pid              VARCHAR2(40),
  idtype           CHAR(2),
  purpose          CHAR(1),
  source_person_id VARCHAR2(20) not null,
  id15             VARCHAR2(40),
  begtime          VARCHAR2(20),
  endtime          VARCHAR2(20),
  branch_id        CHAR(14),
  etl_time         VARCHAR2(21),
  src_sys          VARCHAR2(20),
  busi_usage       VARCHAR2(1)
);


create table PPS.TBL_IDS_EMPNO
(
  sno              VARCHAR2(20),
  typeid           VARCHAR2(7),
  empno_id         VARCHAR2(20),
  person_id        VARCHAR2(20),
  empno            VARCHAR2(20),
  status           CHAR(2),
  name             VARCHAR2(120),
  degreeno         CHAR(2),
  indate           VARCHAR2(10),
  outdate          VARCHAR2(10),
  regdate          VARCHAR2(10),
  degdate          VARCHAR2(10),
  pid              VARCHAR2(40),
  begdate          VARCHAR2(10),
  enddate          VARCHAR2(10),
  edu_level        CHAR(2),
  c_agt_ind        CHAR(1),
  source_person_id VARCHAR2(20),
  agt_sub_type     CHAR(1),
  begtime          VARCHAR2(20),
  endtime          VARCHAR2(20),
  branch           CHAR(14),
  etl_time         VARCHAR2(21),
  src_sys          VARCHAR2(20),
  agt_sale_qualify VARCHAR2(20),
  ext_agt_no       VARCHAR2(20),
  sale_cvrg_type   VARCHAR2(20),
  regu_attr        VARCHAR2(20),
  sale_mng_channel VARCHAR2(20),
  dept_name        VARCHAR2(200)
);



create table PPS.TBL_IDS_MONEYSCH
(
  sno               VARCHAR2(20),
  typeid            VARCHAR2(7),
  con_id            VARCHAR2(20),
  policyno          VARCHAR2(20) not null,
  classcode         CHAR(8) not null,
  paysch            CHAR(2),
  delsch            CHAR(1),
  setcode           CHAR(1),
  delfrm            CHAR(1),
  begdate           CHAR(10),
  nretdate          CHAR(10),
  pbdate            CHAR(10),
  pedate            CHAR(10),
  nextdate          CHAR(10),
  benefit_inc       CHAR(1),
  reczip            VARCHAR2(20),
  recaddr           VARCHAR2(500),
  rectele           VARCHAR2(30),
  appointed_bp_date CHAR(10),
  prem_term         VARCHAR2(20),
  bp_option         VARCHAR2(20),
  begtime           VARCHAR2(20),
  endtime           VARCHAR2(20),
  branch_id         CHAR(14),
  etl_time          VARCHAR2(21),
  src_sys           VARCHAR2(20),
  o_classcode       VARCHAR2(8),
  recmobile         VARCHAR2(30),
  recemail          VARCHAR2(120)
);



create table PPS.TBL_IDS_PADREC
(
  policyno  VARCHAR2(20) not null,
  padtime   NUMBER(20) not null,
  preacct   NUMBER(10,2),
  stdrate   NUMBER(10,2),
  unstdrate NUMBER(10,2),
  rate      NUMBER(6,3),
  amount    NUMBER(10,2),
  operdate  DATE not null,
  begtime   NUMBER(11),
  endtime   NUMBER(11),
  branch    CHAR(14),
  etl_time  DATE,
  src_sys   VARCHAR2(20)
);

create table PPS.TBL_IDS_PERSON
(
  sno                  VARCHAR2(20),
  typeid               VARCHAR2(7),
  person_id            VARCHAR2(20),
  persontype           VARCHAR2(20),
  bthdate              VARCHAR2(20),
  diedate              VARCHAR2(20),
  edulevel             CHAR(2),
  sex                  CHAR(1),
  income               VARCHAR2(22),
  comp_name            VARCHAR2(180),
  marriage             CHAR(3),
  sm0ke_flg            CHAR(1),
  blood_type           CHAR(20),
  ethnicity            CHAR(2),
  hdesp                CHAR(1),
  customerid           VARCHAR2(40),
  name                 VARCHAR2(120),
  jodid                CHAR(7),
  drvcard              VARCHAR2(2),
  country              CHAR(2),
  residence            VARCHAR2(90),
  id                   VARCHAR2(40),
  id15                 VARCHAR2(40),
  idtype               CHAR(2),
  cust_id05            VARCHAR2(20),
  source_person_id     VARCHAR2(20) not null,
  reg_date             VARCHAR2(20),
  begtime              VARCHAR2(20),
  endtime              VARCHAR2(20),
  branch_id            CHAR(14),
  etl_time             VARCHAR2(21),
  src_sys              VARCHAR2(20),
  vehicle_ind          VARCHAR2(1),
  child_ind            VARCHAR2(1),
  identity_chk_ind     CHAR(2),
  identity_chk_desc    VARCHAR2(200),
  owner_yearly_income  NUMBER(20,6),
  owner_local          VARCHAR2(2),
  family_yearly_income NUMBER(20,6)
);



create table PPS.TBL_IDS_PHONE
(
  sno                VARCHAR2(20),
  typeid             VARCHAR2(7),
  areacode           VARCHAR2(20),
  countrycode        VARCHAR2(20),
  tel                VARCHAR2(20),
  ext                VARCHAR2(20),
  type               CHAR(1),
  telnumber          VARCHAR2(100),
  usage              CHAR(1) not null,
  person_id          VARCHAR2(20),
  pid                VARCHAR2(40),
  idtype             CHAR(2),
  purpose            CHAR(1),
  seq                VARCHAR2(10) not null,
  source_person_id   VARCHAR2(20) not null,
  id15               VARCHAR2(40),
  begtime            VARCHAR2(20),
  endtime            VARCHAR2(20),
  branch_id          CHAR(14),
  etl_time           VARCHAR2(21),
  src_sys            VARCHAR2(20),
  workday_cntct_time VARCHAR2(17),
  weekend_cntct_time VARCHAR2(17),
  busi_usage         VARCHAR2(1),
  contacted_ind      VARCHAR2(1),
  authen_type        VARCHAR2(120)
);




create table PPS.TBL_IDS_PLC_ACCT
(
  plc_bp_trans_acct_id  VARCHAR2(20),
  sno                   VARCHAR2(20),
  policy_no             VARCHAR2(20),
  classcode             VARCHAR2(8),
  delcode               VARCHAR2(3),
  typeno                VARCHAR2(2),
  beneficiary_source_id VARCHAR2(40),
  acct_usage            VARCHAR2(1),
  beneficiary_id        VARCHAR2(40),
  beneficiary_name      VARCHAR2(120),
  acct_owner_id         VARCHAR2(40),
  acct_owner_name       VARCHAR2(120),
  acct_no               VARCHAR2(120),
  bank_code             VARCHAR2(20),
  bank_name             VARCHAR2(80),
  currency              VARCHAR2(3),
  create_date           VARCHAR2(10),
  oper_no               VARCHAR2(20),
  begtime               VARCHAR2(20),
  endtime               VARCHAR2(20),
  branch                CHAR(14),
  etl_time              VARCHAR2(21),
  src_sys               VARCHAR2(20)
);




create table PPS.TBL_IDS_REALPAYRC
(
  sno                     CHAR(20),
  typeid                  CHAR(7),
  con_id                  CHAR(20),
  policyno                VARCHAR2(20) not null,
  classcode               CHAR(8) not null,
  check_id                CHAR(20),
  checkno                 CHAR(40),
  amount                  CHAR(24),
  currency                CHAR(3),
  gendate                 CHAR(10),
  delfrm                  CHAR(1),
  setcode                 CHAR(1),
  movewhy                 CHAR(120),
  delcode                 CHAR(3) not null,
  typeno                  CHAR(2) not null,
  regdate                 CHAR(10),
  agent_id                CHAR(20),
  agentno                 CHAR(20),
  oper_id                 CHAR(20),
  operno                  CHAR(20),
  deptno                  CHAR(20),
  docpro                  CHAR(1),
  docno                   CHAR(20),
  person_id               CHAR(20),
  pid                     CHAR(40),
  idtype                  CHAR(2),
  paytime                 CHAR(20) not null,
  seckey                  CHAR(20),
  source_payout_actvty_id CHAR(35),
  sale_mode               CHAR(4),
  the_thrdprt             CHAR(4),
  empno                   CHAR(20),
  csrno                   CHAR(20),
  job                     CHAR(8),
  busi_branch             CHAR(14),
  fin_proc_type           CHAR(7),
  insrnc_rate             CHAR(15),
  policy_no_type          CHAR(1),
  setno                   CHAR(20),
  stra_pay_ind            CHAR(1),
  for_corp_ind            CHAR(1),
  claim_batch_no          CHAR(20),
  begtime                 CHAR(11),
  endtime                 CHAR(11),
  branch                  CHAR(14),
  etl_time                CHAR(20),
  src_sys                 CHAR(20)
);




create table PPS.TBL_ORG_REVOLUTION
(
  org3_id       CHAR(14),
  org3_name     VARCHAR2(50),
  org3_id_new   CHAR(14),
  org3_name_new VARCHAR2(50),
  org2_id       CHAR(14),
  org2_name     VARCHAR2(50),
  org2_id_new   CHAR(14),
  org2_name_new VARCHAR2(50)
);



 


create table PPS.TBL_PRE_BENEFIT_DELETE
(
  delete_tm            CHAR(14),
  policyno_id          VARCHAR2(20) not null,
  class_code           CHAR(8) not null,
  del_code             CHAR(3) not null,
  type_no              CHAR(2) not null,
  del_num              CHAR(4) not null,
  load_date            CHAR(8),
  del_date             CHAR(8),
  del_amt              VARCHAR2(21),
  del_fre              CHAR(1),
  pay_type             CHAR(1),
  noti_type            CHAR(2),
  app_date             CHAR(8),
  gpolicyno_id         VARCHAR2(20),
  pieces               VARCHAR2(21),
  risk_sta             CHAR(1),
  xsqd                 CHAR(2),
  pay_len              VARCHAR2(21),
  tmount               VARCHAR2(21),
  dividend_balance     VARCHAR2(21),
  pre_dividend_amt     VARCHAR2(21),
  empno                VARCHAR2(20),
  operno               VARCHAR2(20),
  deptno               VARCHAR2(20),
  owner_source_id      VARCHAR2(20),
  insured_source_id    VARCHAR2(20),
  payee_source_id      VARCHAR2(20),
  owner_insured_rlship CHAR(3),
  cvrg_app_date        CHAR(8),
  cvrg_app_age         CHAR(4),
  reason               VARCHAR2(200),
  pid_id               VARCHAR2(40),
  cust_name            VARCHAR2(120),
  sex_flg              CHAR(1),
  birth_date           CHAR(8),
  addr                 VARCHAR2(500),
  zip                  VARCHAR2(20),
  telphone             VARCHAR2(100),
  mobphone             VARCHAR2(20),
  email                VARCHAR2(200),
  payee_id             VARCHAR2(40),
  payee_name           VARCHAR2(120),
  pesex_flg            CHAR(1),
  pebirth_date         CHAR(8),
  peaddr               VARCHAR2(500),
  pezip                VARCHAR2(20),
  petelphone           VARCHAR2(100),
  pemobphone           VARCHAR2(20),
  peemail              VARCHAR2(200),
  apid_id              VARCHAR2(40),
  acust_name           VARCHAR2(120),
  asex_flg             CHAR(1),
  abirth_date          CHAR(8),
  aaddr                VARCHAR2(500),
  azip                 VARCHAR2(20),
  atelphone            VARCHAR2(100),
  amobphone            VARCHAR2(20),
  aemail               VARCHAR2(200),
  health_sta           CHAR(2),
  surv_date            CHAR(8),
  surv_fre             CHAR(4),
  ben_id               VARCHAR2(40),
  bank_code            VARCHAR2(20),
  bank_name            VARCHAR2(80),
  del_bkno             VARCHAR2(60),
  bkno_name            VARCHAR2(60),
  csr_no               VARCHAR2(20),
  comp_id              CHAR(14),
  branch_id            CHAR(14),
  src_sys              CHAR(20),
  source_bp_id         VARCHAR2(60),
  chk_last_amt         CHAR(1),
  chk_last_date        CHAR(1),
  chk_last_bkno        CHAR(1),
  last_del_date        CHAR(8),
  last_del_amt         CHAR(21),
  last_del_bkno        VARCHAR2(80),
  check_flg            CHAR(1),
  check_opr            CHAR(8),
  check_date           CHAR(8),
  check_rea2           CHAR(2),
  check_rea            VARCHAR2(100),
  surv_flg             CHAR(1),
  task_id              VARCHAR2(15),
  noti_id              CHAR(1),
  ret_flg              CHAR(1),
  proc_flg             VARCHAR2(10),
  del_flg              CHAR(1),
  sub_pkey_cd1         VARCHAR2(30),
  sub_pkey_cd2         VARCHAR2(30),
  sub_pkey_cd3         VARCHAR2(30),
  misc_tx              VARCHAR2(256),
  last_upd_opr_id      CHAR(10),
  last_upd_txn_id      CHAR(10),
  last_upd_ts          CHAR(14)
);




create table PPS.TBL_RPT_ACTUARY_STATISTICS
(
  policyno_id       VARCHAR2(20) not null,
  class_code        CHAR(8) not null,
  del_code          CHAR(3) not null,
  type_no           CHAR(2) not null,
  del_num           CHAR(4) not null,
  del_date          CHAR(8),
  del_amt           VARCHAR2(21),
  branch_id         CHAR(14),
  comp_id           CHAR(14),
  apid_id           VARCHAR2(40),
  acust_name        VARCHAR2(120),
  import_start_date CHAR(8),
  last_upd_txn_id   CHAR(10),
  last_upd_ts       CHAR(19)
);



create table PPS.TBL_SURV_DATA_INFO
(
  take_date       CHAR(8),
  app_date        CHAR(8),
  task_id         VARCHAR2(15),
  apid_id         VARCHAR2(40),
  apid_name       VARCHAR2(120),
  pid_id          VARCHAR2(40),
  pid_name        VARCHAR2(120),
  task_type       CHAR(1),
  sub_type        CHAR(2),
  spe_type        CHAR(2),
  opr_id          CHAR(8),
  task_kind       CHAR(1),
  rea_id          CHAR(2),
  chk_obj         CHAR(1),
  class_type      CHAR(1),
  cust_tel        VARCHAR2(100),
  comp_id         CHAR(14),
  branch_id       CHAR(14),
  check_result    CHAR(1),
  id_status       CHAR(1),
  regist_reason   CHAR(1),
  regist_date     VARCHAR2(10),
  regist_address  VARCHAR2(500),
  load_date       VARCHAR2(10),
  surv_src        CHAR(1),
  surv_sta        CHAR(1),
  surv_res        VARCHAR2(300),
  die_date        CHAR(8),
  surv_flg        CHAR(1),
  add_res1        CHAR(1),
  add_res2        VARCHAR2(300),
  res_date        CHAR(8),
  health_sta      CHAR(2),
  ret_flg1        CHAR(1),
  ret_flg2        CHAR(1),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14),
  policyno_id     VARCHAR2(20),
  class_code      CHAR(8),
  del_code        CHAR(3),
  type_no         CHAR(2),
  del_num         CHAR(4),
  trigger_type    CHAR(1),
  surv_date       CHAR(8)
);





create table PPS.TBL_SURV_DATA_INFO_ONE
(
  take_date       CHAR(8),
  app_date        CHAR(8),
  task_id         VARCHAR2(15),
  apid_id         VARCHAR2(40),
  apid_name       VARCHAR2(120),
  pid_id          VARCHAR2(40),
  pid_name        VARCHAR2(120),
  task_type       CHAR(1),
  sub_type        CHAR(2),
  spe_type        CHAR(2),
  opr_id          CHAR(8),
  task_kind       CHAR(1),
  rea_id          CHAR(2),
  chk_obj         CHAR(1),
  class_type      CHAR(1),
  cust_tel        VARCHAR2(100),
  comp_id         CHAR(14),
  branch_id       CHAR(14),
  check_result    CHAR(1),
  id_status       CHAR(1),
  regist_reason   CHAR(1),
  regist_date     VARCHAR2(10),
  regist_address  VARCHAR2(500),
  load_date       VARCHAR2(10),
  surv_src        CHAR(1),
  surv_sta        CHAR(1),
  surv_res        VARCHAR2(300),
  die_date        CHAR(8),
  surv_flg        CHAR(1),
  add_res1        CHAR(1),
  add_res2        VARCHAR2(300),
  res_date        CHAR(8),
  health_sta      CHAR(2),
  ret_flg1        CHAR(1),
  ret_flg2        CHAR(1),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14)
);




create table PPS.TBL_SURV_DATA_INFO_TMP
(
  take_date       CHAR(8),
  app_date        CHAR(8),
  task_id         CHAR(10),
  apid_id         VARCHAR2(40),
  apid_name       VARCHAR2(120),
  pid_id          VARCHAR2(40),
  pid_name        VARCHAR2(120),
  task_type       CHAR(1),
  sub_type        CHAR(2),
  spe_type        CHAR(2),
  opr_id          CHAR(8),
  task_kind       CHAR(1),
  rea_id          CHAR(2),
  chk_obj         CHAR(1),
  class_type      CHAR(1),
  cust_tel        VARCHAR2(100),
  comp_id         CHAR(14),
  branch_id       CHAR(14),
  check_result    CHAR(1),
  id_status       CHAR(1),
  regist_reason   CHAR(1),
  regist_date     VARCHAR2(10),
  regist_address  VARCHAR2(500),
  load_date       VARCHAR2(10),
  surv_src        CHAR(1),
  surv_sta        CHAR(1),
  surv_res        VARCHAR2(300),
  die_date        CHAR(8),
  surv_flg        CHAR(1),
  add_res1        CHAR(1),
  add_res2        VARCHAR2(300),
  res_date        CHAR(8),
  health_sta      CHAR(2),
  ret_flg1        CHAR(1),
  ret_flg2        CHAR(1),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14),
  policyno_id     VARCHAR2(20),
  class_code      CHAR(8),
  del_code        CHAR(3),
  type_no         CHAR(2),
  del_num         CHAR(4)
);




create table PPS.TBL_SURV_DATA_INFO_TMP2
(
  policyno_id     VARCHAR2(20) not null,
  class_code      CHAR(8) not null,
  del_code        CHAR(3) not null,
  type_no         CHAR(2) not null,
  del_num         CHAR(4) not null,
  take_date       CHAR(8),
  app_date        CHAR(8),
  task_id         CHAR(10),
  apid_id         VARCHAR2(40),
  apid_name       VARCHAR2(120),
  pid_id          VARCHAR2(40),
  pid_name        VARCHAR2(120),
  task_type       CHAR(1),
  sub_type        CHAR(2),
  spe_type        CHAR(2),
  opr_id          CHAR(8),
  task_kind       CHAR(1),
  rea_id          CHAR(2),
  chk_obj         CHAR(1),
  class_type      CHAR(1),
  cust_tel        VARCHAR2(100),
  comp_id         CHAR(14),
  branch_id       CHAR(14),
  check_result    CHAR(1),
  id_status       CHAR(1),
  regist_reason   CHAR(1),
  regist_date     VARCHAR2(10),
  regist_address  VARCHAR2(500),
  load_date       VARCHAR2(10),
  surv_src        CHAR(1),
  surv_sta        CHAR(1),
  surv_res        VARCHAR2(300),
  die_date        CHAR(8),
  surv_flg        CHAR(1),
  add_res1        CHAR(1),
  add_res2        VARCHAR2(300),
  res_date        CHAR(8),
  health_sta      CHAR(2),
  ret_flg1        CHAR(1),
  ret_flg2        CHAR(1),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14),
  trigger_type    CHAR(1)
);



create table PPS.TBL_SURV_TASK_INFO
(
  app_date        CHAR(8) not null,
  pid_id          VARCHAR2(40) not null,
  surv_type       CHAR(1),
  task_id         CHAR(10),
  pid_name        VARCHAR2(120),
  task_type       CHAR(1),
  sub_type        CHAR(2),
  spe_type        CHAR(2),
  opr_id          CHAR(8),
  task_kind       CHAR(1),
  rea_id          CHAR(2),
  chk_obj         CHAR(1),
  class_type      CHAR(1),
  comp_id         CHAR(14),
  branch_id       CHAR(14),
  apid_id         VARCHAR2(40),
  apid_name       VARCHAR2(120),
  cust_tel        VARCHAR2(100),
  surv_sta        CHAR(1),
  surv_res        VARCHAR2(300),
  die_date        CHAR(8),
  surv_flg        CHAR(1),
  add_res1        CHAR(1),
  add_res2        VARCHAR2(300),
  res_date        CHAR(8),
  sub_pkey_cd1    VARCHAR2(30),
  sub_pkey_cd2    VARCHAR2(30),
  sub_pkey_cd3    VARCHAR2(30),
  misc_tx         VARCHAR2(256),
  last_upd_opr_id CHAR(10),
  last_upd_txn_id CHAR(10),
  last_upd_ts     CHAR(14)
);







create table PPS.TBL_YUGU_PAY
(
  policyno_id VARCHAR2(20) not null,
  class_code  CHAR(8) not null,
  del_code    CHAR(3) not null,
  type_no     CHAR(2) not null,
  del_num     VARCHAR2(4) not null,
  del_date    CHAR(11),
  del_amt     VARCHAR2(21),
  apid_id     VARCHAR2(40),
  del_fre     CHAR(1),
  xsqd        CHAR(5),
  comp_id     CHAR(14),
  branch_id   CHAR(14),
  max_num     VARCHAR2(6),
  life_amt    CHAR(1),
  paifa       CHAR(1),
  risk_sta    CHAR(1),
  reason      VARCHAR2(40),
  src_sys     CHAR(7) not null
);





create table PPS.TEST20210707
(
  sno  NUMBER,
  name VARCHAR2(100)
);




create table PPS.TMP_DIV_POLICYNO
(
  policyno    VARCHAR2(20),
  appno       VARCHAR2(20),
  cardno      VARCHAR2(20),
  branch      VARCHAR2(14),
  company     VARCHAR2(14),
  empno       VARCHAR2(10),
  chatype     VARCHAR2(10),
  branch_new  VARCHAR2(14),
  company_new VARCHAR2(14),
  empno_new   VARCHAR2(10)
);


CREATE TABLE "PPS"."JOB_TEMP_RISKCON" 
   ( "POLICYNO" VARCHAR2(15), 
 "POLIST" CHAR(1), 
 "NPAYLEN" NUMBER(20,6), 
 "YEARNUM" NUMBER(*,0), 
 "TTIME" NUMBER(*,0), 
 "TMOUNT" NUMBER(16,2), 
 "PIECES" NUMBER(13,3), 
 "CLASSCODE" VARCHAR2(8), 
 "EMPNO" VARCHAR2(20), 
 "BEGDATE" DATE, 
 "CONTNO" VARCHAR2(20), 
 "FGSNO" CHAR(3), 
 "PAYCODE" CHAR(2), 
 "DELCODE" CHAR(1), 
 "PRELNAME" CHAR(6), 
 "PEDATE" DATE, 
 "PID" CHAR(30), 
 "JOB" CHAR(20), 
 "APID" CHAR(30), 
 "OPERNO" CHAR(8), 
 "STOPDAT" DATE, 
 "APPDATE" DATE, 
 "APPF" CHAR(1), 
 "APPNO" VARCHAR2(20), 
 "DELAGE" NUMBER(*,0), 
 "EFFECTDT" DATE, 
 "STDRATE" NUMBER(10,2), 
 "UNSTDRATE" NUMBER(10,2), 
 "GPOLICYNO" CHAR(15), 
 "SALEATTR" VARCHAR2(2), 
 "STOPDATE" DATE, 
 "ETL_TIME" DATE, 
 "SNO" NUMBER(*,0), 
 "PAYSEQ" VARCHAR2(2048), 
 "RECADDR" VARCHAR2(508), 
 "RECTELE" VARCHAR2(100), 
 "PAYFMAGE" NUMBER(*,0), 
 "NRETDATE" DATE, 
 "NEXTDATE" DATE, 
 "RENEWID" CHAR(1), 
 "BANKFLAG" CHAR(2), 
 "IDTYPE" CHAR(2), 
 "AIDTYPE" CHAR(2), 
 "APERSON_ID" NUMBER(*,0), 
 "BENPARAM" NUMBER(*,0), 
 "COMNUM" NUMBER(*,0), 
 "CSR_ID" NUMBER(*,0), 
 "CURRENCY" CHAR(3), 
 "DCDM" CHAR(2), 
 "DEL_DATE" CHAR(8), 
 "DEL_TYPE" NUMBER(*,0), 
 "DESKPAY" CHAR(1), 
 "DISCOUNT" NUMBER(20,6), 
 "EMPNO_ID" NUMBER(*,0), 
 "GCON_ID" NUMBER(*,0), 
 "ISCARD" CHAR(1), 
 "OPDATE" DATE, 
 "OPER_ID" NUMBER(*,0), 
 "PERSON_ID" NUMBER(*,0), 
 "REASON" VARCHAR2(200), 
 "REG_CODE" VARCHAR2(20), 
 "RENEWDATE" DATE, 
 "RISKCON_ID" NUMBER(*,0), 
 "SPECAGR" VARCHAR2(2048), 
 "TYPEID" NUMBER(*,0), 
 "SHARETYPE" CHAR(3), 
 "BRANCH" CHAR(14), 
 "BEGTIME" DATE, 
 "ENDTIME" DATE, 
 "SOUR_SYS" VARCHAR2(20), 
 "SRC_SYS" VARCHAR2(20), 
 "SALE_PROD_CODE" VARCHAR2(8), 
 "OWNER_SOURCE_ID" VARCHAR2(20), 
 "INSURED_SOURCE_ID" VARCHAR2(20), 
 "WORKNO" VARCHAR2(20), 
 "COMB_POLICY_NO" VARCHAR2(20), 
 "APP_AGE" NUMBER(3,0), 
 "CSRNO" VARCHAR2(20), 
 "CONTCODE" VARCHAR2(8), 
 "APPOINTED_OPDATE_IND" VARCHAR2(1), 
 "UNI_PAY_IND" VARCHAR2(1), 
 "INI_INVOICE_STATUS" VARCHAR2(2), 
 "SUPPLY_INPUT_IND" VARCHAR2(1), 
 "DIGITAL_SIGN_IND" VARCHAR2(1), 
 "GROUP_NO" VARCHAR2(2), 
 "PREM_RATE_LEVEL" VARCHAR2(2), 
 "ILL_SCORE" NUMBER(10,2), 
 "O_CLASSCODE" VARCHAR2(8), 
 "CROSS_SALE_IND" VARCHAR2(1), 
 "SUB_AGT_NO" VARCHAR2(20)
   );
 
CREATE TABLE "PPS"."JOB_IDS_RISKCON" 
   ( "SNO" NUMBER(*,0), 
 "TYPEID" NUMBER(*,0), 
 "APERSON_ID" NUMBER(*,0), 
 "APID" VARCHAR2(40), 
 "AIDTYPE" CHAR(2), 
 "RISKCON_ID" NUMBER(*,0), 
 "GCON_ID" NUMBER(*,0), 
 "GPOLICYNO" VARCHAR2(20), 
 "DESKPAY" CHAR(1), 
 "SALEATTR" CHAR(3), 
 "RENEWID" CHAR(1), 
 "RENEWDATE" DATE, 
 "BEGDATE" DATE, 
 "STOPDATE" DATE, 
 "POLIST" CHAR(1), 
 "REASON" VARCHAR2(200), 
 "POLICYNO" VARCHAR2(20), 
 "APPNO" VARCHAR2(20), 
 "CURRENCY" CHAR(3), 
 "PIECES" NUMBER(20,6), 
 "APPF" CHAR(1), 
 "OPDATE" DATE, 
 "TMOUNT" NUMBER(16,2), 
 "OPER_ID" NUMBER(*,0), 
 "OPERNO" VARCHAR2(20), 
 "EMPNO_ID" NUMBER(*,0), 
 "EMPNO" VARCHAR2(20), 
 "CSR_ID" NUMBER(*,0), 
 "CSRNO" VARCHAR2(20), 
 "CLASSCODE" VARCHAR2(8), 
 "APPDATE" DATE, 
 "COMNUM" NUMBER(*,0), 
 "PERSON_ID" NUMBER(*,0), 
 "IDTYPE" CHAR(2), 
 "PID" VARCHAR2(40), 
 "JOB" CHAR(8), 
 "SHARETYPE" CHAR(3), 
 "SPECAGR" VARCHAR2(2048), 
 "DISCOUNT" NUMBER(20,6), 
 "ISCARD" CHAR(1), 
 "NPAYLEN" NUMBER(20,6), 
 "STDRATE" NUMBER(10,2), 
 "UNSTDRATE" NUMBER(10,2), 
 "DCDM" CHAR(2), 
 "PRELNAME" CHAR(3), 
 "BANKFLAG" CHAR(2), 
 "PAYSEQ" VARCHAR2(2048), 
 "BENPARAM" NUMBER(*,0), 
 "BEGTIME" DATE, 
 "ENDTIME" DATE, 
 "BRANCH" CHAR(14), 
 "ETL_TIME" TIMESTAMP (6), 
 "SRC_SYS" VARCHAR2(20), 
 "DEL_DATE" CHAR(8), 
 "REG_CODE" VARCHAR2(20), 
 "DEL_TYPE" NUMBER(*,0), 
 "SOUR_SYS" VARCHAR2(20), 
 "SALE_PROD_CODE" VARCHAR2(8), 
 "OWNER_SOURCE_ID" VARCHAR2(20), 
 "INSURED_SOURCE_ID" VARCHAR2(20), 
 "WORKNO" VARCHAR2(20), 
 "COMB_POLICY_NO" VARCHAR2(20), 
 "APP_AGE" NUMBER(3,0), 
 "SUB_AGT_NO" VARCHAR2(20), 
 "YEARNUM" NUMBER(*,0), 
 "PEDATE" DATE, 
 "PAYCODE" CHAR(2), 
 "DELCODE" CHAR(1), 
 "RECADDR" VARCHAR2(508), 
 "NEXTDATE" DATE, 
 "NRETDATE" DATE, 
 "RECTELE" VARCHAR2(100), 
 "FGSNO" CHAR(3), 
 "O_CLASSCODE" VARCHAR2(8), 
 "CROSS_SALE_IND" VARCHAR2(1), 
 "PREM_RATE_LEVEL" VARCHAR2(2), 
 "ILL_SCORE" NUMBER(10,2), 
 "GROUP_NO" VARCHAR2(2), 
 "DIGITAL_SIGN_IND" VARCHAR2(1), 
 "SUPPLY_INPUT_IND" VARCHAR2(1), 
 "INI_INVOICE_STATUS" VARCHAR2(2), 
 "UNI_PAY_IND" VARCHAR2(1), 
 "APPOINTED_OPDATE_IND" VARCHAR2(1), 
 "CONTCODE" VARCHAR2(8)
   );
   
create table PPS.TMP_POLICY
(
  policyno_id VARCHAR2(20)
);
