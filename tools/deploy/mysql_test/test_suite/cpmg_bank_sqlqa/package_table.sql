
create table CPMG.CAP_ECTOTAL_DATA_TMP
(
  datadate VARCHAR2(6 CHAR) not null,
  zoneno   VARCHAR2(4 CHAR) not null,
  brno     VARCHAR2(4 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  value    NUMBER(17,2),
  diccode  VARCHAR2(3 CHAR) not null
)
;

create table CPMG.CAP_EC_DATA
(
  datadate  VARCHAR2(8 CHAR) not null,
  organcode VARCHAR2(8 CHAR) not null,
  currency  VARCHAR2(3 CHAR) not null,
  itemcode  VARCHAR2(20 CHAR) not null,
  kcdqvalue NUMBER(17,2),
  xsvalue   NUMBER(9,6),
  zcyevalue NUMBER(17,2)
)
;
  

create table CPMG.CAP_EC_DATA_TMP
(
  data_date     VARCHAR2(8) not null,
  zoneno        VARCHAR2(4 CHAR) not null,
  brno          VARCHAR2(4 CHAR) not null,
  currtype      VARCHAR2(3 CHAR) not null,
  diccode       VARCHAR2(20 CHAR) not null,
  ec            NUMBER(17,2),
  ec_minus      NUMBER(17,2),
  balance       NUMBER(17,2),
  balance_minus NUMBER(17,2),
  bankflag      VARCHAR2(1 CHAR)
)
;
  

 

create table CPMG.CAP_EC_DATA_TMP2
(
  data_date     VARCHAR2(8) not null,
  zoneno        VARCHAR2(4 CHAR) not null,
  brno          VARCHAR2(4 CHAR) not null,
  currtype      VARCHAR2(3 CHAR) not null,
  diccode       VARCHAR2(20 CHAR) not null,
  ec            NUMBER(17,2),
  ec_minus      NUMBER(17,2),
  balance       NUMBER(17,2),
  balance_minus NUMBER(17,2),
  bankflag      VARCHAR2(1 CHAR)
)
;

create table CPMG.CAP_EC_FORECAST
(
  plandate   VARCHAR2(8 CHAR) not null,
  organcode  VARCHAR2(8 CHAR) not null,
  currency   VARCHAR2(3 CHAR) not null,
  itemcode   VARCHAR2(20 CHAR) not null,
  kcdqvalue  NUMBER(17,2),
  xsvalue    NUMBER(9,6),
  zcyevalue  NUMBER(17,2),
  newzcvalue NUMBER(17,2),
  zyzlvalue  NUMBER(17,2),
  zbzyvalue  NUMBER(17,2),
  zblimit    NUMBER(17,2),
  jdvalue    NUMBER(9,6)
)
;
  

create table CPMG.CAP_FILE_RECORDS
(
  job_no     VARCHAR2(10),
  app_id     VARCHAR2(20) not null,
  table_name VARCHAR2(500),
  bin_name   VARCHAR2(500) not null,
  count_yb   NUMBER(20),
  workdate   VARCHAR2(20) not null,
  field1     VARCHAR2(300),
  field2     VARCHAR2(300)
)
;
  

create table CPMG.CAP_FORECAST_DATA
(
  forecastdate VARCHAR2(8 CHAR) not null,
  organcode    VARCHAR2(8 CHAR) not null,
  userid       VARCHAR2(9 CHAR),
  currency     VARCHAR2(3 CHAR) not null,
  itemcode     VARCHAR2(20 CHAR) not null,
  value        NUMBER(17,2)
)
;

create table CPMG.CAP_LIMIT_APPRMAIL
(
  data_date       VARCHAR2(14 CHAR) not null,
  receiver_id     VARCHAR2(20 CHAR) not null,
  receiver_name   VARCHAR2(50 CHAR),
  mail_title      VARCHAR2(200 CHAR),
  mail_address    VARCHAR2(50 CHAR),
  mail_content    VARCHAR2(1000 CHAR),
  receiver_branch VARCHAR2(20 CHAR),
  send_date       VARCHAR2(14 CHAR),
  send_status     VARCHAR2(1 CHAR),
  log_id          VARCHAR2(20 CHAR) not null
)
;
  

create table CPMG.CAP_PLAN_DATA
(
  year      VARCHAR2(4 CHAR) not null,
  organcode VARCHAR2(8 CHAR) not null,
  xyfxlimit NUMBER(17,2),
  rmblimit  NUMBER(17,2),
  czfxlimit NUMBER(17,2),
  startdate VARCHAR2(8 CHAR) not null,
  enddate   VARCHAR2(8 CHAR),
  userid    VARCHAR2(9 CHAR)
)
;
  

create table CPMG.CAP_PLAN_DATA_TMP
(
  seq       VARCHAR2(10 CHAR),
  rownums   NUMBER(5),
  organcode VARCHAR2(8 CHAR),
  organname VARCHAR2(100 CHAR),
  xyfxlimit NUMBER(17,2),
  rmblimit  NUMBER(17,2),
  czfxlimit NUMBER(17,2)
)
;
  

create table CPMG.CAP_REAL_INCREMENT
(
  inc_branch    VARCHAR2(20 CHAR) not null,
  inc_branch_lv VARCHAR2(1 CHAR) not null,
  inc_year      VARCHAR2(4 CHAR) not null,
  inc_type      VARCHAR2(1 CHAR) not null,
  inc_count     NUMBER(22,2) not null,
  creater       VARCHAR2(20 CHAR),
  create_time   DATE,
  create_branch VARCHAR2(20 CHAR)
)
;
  

create table CPMG.CAP_TRANSLIMIT_STAT_TMP
(
  branchid      VARCHAR2(20 CHAR),
  year          VARCHAR2(4 CHAR),
  quarter       VARCHAR2(1 CHAR),
  uplimit       NUMBER(22,2) default 0,
  uplimitinc    NUMBER(22,2) default 0,
  buycnt        NUMBER(22,2) default 0,
  buycalamt     NUMBER(22,2) default 0,
  sellcnt       NUMBER(22,2) default 0,
  sellcalamt    NUMBER(22,2) default 0,
  transtolamt   NUMBER(22,2) default 0,
  quartereva    NUMBER(22,2) default 0,
  reallimt      NUMBER(22,2) default 0,
  realinc       NUMBER(22,2) default 0,
  limit_min     NUMBER(22,2) default 0,
  limit_init    NUMBER(22,2) default 0,
  uplimitincamt NUMBER(22,2) default 0
)
;
  

create table CPMG.CAP_TRANS_APPLY
(
  trans_id            VARCHAR2(20 CHAR),
  trans_initiate      VARCHAR2(20 CHAR),
  trans_accept_branch VARCHAR2(20 CHAR),
  trans_type          VARCHAR2(1 CHAR),
  trans_variety       VARCHAR2(1 CHAR),
  trans_price         NUMBER(22,2),
  trans_count         NUMBER(22,2),
  trans_status        VARCHAR2(1 CHAR),
  price_relea_date    VARCHAR2(8 CHAR),
  trans_time          DATE,
  trans_quarter       VARCHAR2(1 CHAR),
  trans_year          VARCHAR2(4 CHAR),
  trans_offer_seq     VARCHAR2(20 CHAR)
)
;
  

create table CPMG.CAP_TRANS_BRANCH_SORT
(
  trans_branch_id   VARCHAR2(20 CHAR) not null,
  trans_branch_name VARCHAR2(80 CHAR),
  sort_no           NUMBER
)
;
  

create table CPMG.CAP_TRANS_LIMIT
(
  limit_branch    VARCHAR2(20 CHAR),
  limit_branch_lv VARCHAR2(1 CHAR),
  limit_year      VARCHAR2(4 CHAR),
  limit_type      VARCHAR2(1 CHAR),
  limit_min       NUMBER(22,2),
  creater         VARCHAR2(20 CHAR),
  create_time     DATE,
  create_branch   VARCHAR2(20 CHAR),
  limit_init      NUMBER(22,2)
)
;
  

create table CPMG.CAP_TRANS_LOG
(
  log_id      VARCHAR2(20 CHAR) not null,
  trans_id    VARCHAR2(20 CHAR),
  oper_type   VARCHAR2(1 CHAR),
  oper_msg    VARCHAR2(2000 CHAR),
  operater    VARCHAR2(20 CHAR),
  oper_time   DATE,
  oper_dept   VARCHAR2(20 CHAR),
  oper_branch VARCHAR2(20 CHAR)
)
;
  

create table CPMG.CAP_TRANS_OFFER
(
  trans_branch         VARCHAR2(20 CHAR) not null,
  trans_year           VARCHAR2(4 CHAR) not null,
  buy_price            NUMBER(22,2),
  sell_price           NUMBER(22,2),
  trans_varieties      VARCHAR2(3 CHAR) not null,
  confirm              VARCHAR2(1 CHAR),
  in_trading_varieties VARCHAR2(1 CHAR),
  trans_quarter        VARCHAR2(1 CHAR) not null,
  price_relea_date     VARCHAR2(8 CHAR) not null,
  trans_start_time     VARCHAR2(8 CHAR),
  trans_end_time       VARCHAR2(8 CHAR),
  in_trading           VARCHAR2(1 CHAR),
  history_record       VARCHAR2(1 CHAR),
  create_time          DATE,
  trans_offer_seq      VARCHAR2(20 CHAR) not null,
  trans_level          VARCHAR2(1 CHAR)
)
;
  

create table CPMG.CPMG_QRTZ_HISTORY
(
  seq_no            VARCHAR2(12) not null,
  exector           VARCHAR2(20) not null,
  logdate           VARCHAR2(8) not null,
  datatime          VARCHAR2(8) not null,
  jobname           VARCHAR2(200) not null,
  jobgroup          VARCHAR2(200),
  triggername       VARCHAR2(200),
  triggergroup      VARCHAR2(200),
  previousfiredtime VARCHAR2(20),
  nextfiredtime     VARCHAR2(20),
  action            VARCHAR2(200) not null,
  message           VARCHAR2(2000)
)
;
  

create table CPMG.CTP_ARM_CONFIG
(
  hostname   VARCHAR2(50 CHAR) not null,
  opname     VARCHAR2(50 CHAR) not null,
  paramtype  VARCHAR2(2 CHAR),
  paramvalue NUMBER,
  modifydate DATE
)
;
  

create table CPMG.CTP_BRANCH
(
  id              VARCHAR2(20 CHAR) not null,
  name            VARCHAR2(100 CHAR) not null,
  description     VARCHAR2(500 CHAR),
  status          VARCHAR2(2 CHAR) default 1 not null,
  region_id       VARCHAR2(20 CHAR),
  net_terminal    VARCHAR2(20 CHAR),
  parent_id       VARCHAR2(20 CHAR),
  branch_level    VARCHAR2(3 CHAR) default 1 not null,
  modi_user       VARCHAR2(20 CHAR),
  modi_time       VARCHAR2(8 CHAR),
  short_name      VARCHAR2(80 CHAR),
  branch_category VARCHAR2(1 CHAR),
  finace_id       VARCHAR2(40 CHAR),
  addr            VARCHAR2(120 CHAR),
  zipcode         VARCHAR2(6 CHAR),
  phone           VARCHAR2(40 CHAR),
  sign            VARCHAR2(12 CHAR),
  admin_level     VARCHAR2(4 CHAR),
  open_time       VARCHAR2(6 CHAR),
  last_alt_type   VARCHAR2(16 CHAR),
  balance_id      VARCHAR2(36 CHAR),
  codecert_id     VARCHAR2(36 CHAR),
  revoke_time     VARCHAR2(6 CHAR)
)
;
  

create table CPMG.CTP_ITEM
(
  id             VARCHAR2(8 CHAR) not null,
  name           VARCHAR2(50 CHAR) not null,
  description    VARCHAR2(500 CHAR),
  default_label  VARCHAR2(40 CHAR) not null,
  status         VARCHAR2(2 CHAR) default 0 not null,
  url            VARCHAR2(1024 CHAR) not null,
  item_type      VARCHAR2(30 CHAR),
  item_level     VARCHAR2(100 CHAR),
  item_branch_id VARCHAR2(20 CHAR)
)
;
  

create table CPMG.CTP_MENU
(
  id             VARCHAR2(8 CHAR) not null,
  name           VARCHAR2(50 CHAR) not null,
  description    VARCHAR2(500 CHAR),
  parent_id      VARCHAR2(8 CHAR),
  default_label  VARCHAR2(40 CHAR) not null,
  status         VARCHAR2(2 CHAR) default 0 not null,
  serialno       NUMBER(3) default 0 not null,
  menu_level     VARCHAR2(100 CHAR),
  menu_branch_id VARCHAR2(20 CHAR)
)
;
  

create table CPMG.CTP_MENU_ITEM_REL
(
  menu_id  VARCHAR2(8 CHAR) not null,
  item_id  VARCHAR2(8 CHAR) not null,
  serialno NUMBER(3) default 0 not null
)
;
  

create table CPMG.CTP_MX_APP_INFO
(
  appname_en VARCHAR2(50 CHAR) not null,
  appname_zh VARCHAR2(50 CHAR),
  version    VARCHAR2(100 CHAR),
  type       VARCHAR2(2 CHAR)
)
;
  

create table CPMG.CTP_MX_MONITOR_FLAG
(
  hostname    VARCHAR2(50 CHAR) not null,
  switchtype  VARCHAR2(2 CHAR) not null,
  channeltype VARCHAR2(2 CHAR) not null,
  action      VARCHAR2(2 CHAR),
  modifyuser  VARCHAR2(50 CHAR),
  midifydate  DATE
)
;
  

create table CPMG.CTP_MX_RESOURCE_INFO
(
  resourcecode VARCHAR2(50 CHAR) not null,
  resourcename VARCHAR2(100 CHAR),
  resourcerate NUMBER,
  type         VARCHAR2(2 CHAR),
  monitor      VARCHAR2(200 CHAR),
  ismonitor    CHAR(1)
)
;
  

create table CPMG.CTP_MX_RESOURCE_LEVEL_DEFINE
(
  resourcecode VARCHAR2(50 CHAR) not null,
  eventlevel   VARCHAR2(2 CHAR) not null,
  occurtime    NUMBER
)
;
  

 

create table CPMG.CTP_MX_SERVER_IP
(
  hostname VARCHAR2(50 CHAR) not null,
  ip       VARCHAR2(20 CHAR)
)
;
  

create table CPMG.CTP_MX_SERVER_PORT
(
  hostname VARCHAR2(50 CHAR) not null,
  port     NUMBER not null
)
;
  

create table CPMG.CTP_MX_STATISTICS_PERIOD
(
  name   VARCHAR2(100 CHAR) not null,
  period VARCHAR2(50 CHAR) not null
)
;
  

create table CPMG.CTP_MX_STATISTICS_VALVE
(
  name      VARCHAR2(100 CHAR) not null,
  trancode  VARCHAR2(100 CHAR) not null,
  valve     VARCHAR2(20 CHAR) not null,
  begintime VARCHAR2(20 CHAR) not null,
  endtime   VARCHAR2(20 CHAR) not null
)
;
  

create table CPMG.CTP_MX_STAT_OPAVGTIME_INFO
(
  hostname  VARCHAR2(50 CHAR) not null,
  indextype VARCHAR2(4 CHAR) not null,
  opname    VARCHAR2(100 CHAR) not null,
  statdate  DATE not null,
  optime    NUMBER,
  opcount   NUMBER
)
;
  

create table CPMG.CTP_MX_STAT_PEAK_INFO
(
  hostname  VARCHAR2(50 CHAR) not null,
  indextype VARCHAR2(4 CHAR) not null,
  statdate  DATE not null,
  peakvalue NUMBER
)
;
  

create table CPMG.CTP_MX_TRANS_ERROR_INFO
(
  eventcode VARCHAR2(50 CHAR) not null,
  eventname VARCHAR2(100 CHAR),
  ismonitor CHAR(1)
)
;
  

create table CPMG.CTP_MX_TRANS_ERROR_LEVEL
(
  eventcode  VARCHAR2(50 CHAR) not null,
  trancode   VARCHAR2(50 CHAR) not null,
  eventlevel VARCHAR2(2 CHAR) not null,
  occurtime  NUMBER
)
;
  

create table CPMG.CTP_MX_USABILITY_INFO
(
  name          VARCHAR2(50 CHAR) not null,
  modulecode    VARCHAR2(50 CHAR),
  submodulecode VARCHAR2(50 CHAR),
  checker       VARCHAR2(200 CHAR),
  status        VARCHAR2(2 CHAR),
  msg           VARCHAR2(200 CHAR),
  ismonitor     CHAR(1)
)
;
  

create table CPMG.CTP_PROC_LOG
(
  porc_name VARCHAR2(100 CHAR),
  log_time  VARCHAR2(100 CHAR),
  log_info  CLOB
)
;
  

create table CPMG.CTP_ROLE
(
  id               VARCHAR2(20 CHAR) not null,
  name             VARCHAR2(50 CHAR) not null,
  lst_modi_time    VARCHAR2(8 CHAR),
  description      VARCHAR2(500 CHAR),
  role_level       VARCHAR2(100 CHAR),
  branch_id        VARCHAR2(20 CHAR),
  role_level_admin VARCHAR2(100 CHAR),
  branch_id_admin  VARCHAR2(20 CHAR),
  role_category    VARCHAR2(1 CHAR) default 1 not null,
  lst_modi_user_id VARCHAR2(20 CHAR),
  privilege_all    VARCHAR2(1 CHAR),
  privilege_self   VARCHAR2(1 CHAR),
  privilege_other  VARCHAR2(2 CHAR),
  privilege        VARCHAR2(1 CHAR)
)
;
  

create table CPMG.CTP_ROLE_ITEM_REL
(
  role_id   VARCHAR2(20 CHAR) not null,
  item_id   VARCHAR2(8 CHAR) not null,
  privilege VARCHAR2(5 CHAR) not null
)
;
  

create table CPMG.CTP_ROLE_MENU_REL
(
  role_id   VARCHAR2(20 CHAR) not null,
  menu_id   VARCHAR2(8 CHAR) not null,
  privilege VARCHAR2(5 CHAR) not null
)
;
  

create table CPMG.CTP_ROLE_USER_REL
(
  role_id      VARCHAR2(20 CHAR) not null,
  user_id      VARCHAR2(20 CHAR) not null,
  menuchg_flag CHAR(1) default 1,
  menustr      CLOB
)
;
  

create table CPMG.CTP_TEMP_ITEM_TABLE
(
  id VARCHAR2(8 CHAR)
)
;
  

create table CPMG.CTP_USER
(
  id               VARCHAR2(20 CHAR) not null,
  branch_id        VARCHAR2(20 CHAR) not null,
  name             VARCHAR2(50 CHAR) not null,
  description      VARCHAR2(500 CHAR),
  status           VARCHAR2(2 CHAR) default 0 not null,
  password         VARCHAR2(200 CHAR),
  user_level       VARCHAR2(2 CHAR),
  phone_no         VARCHAR2(20 CHAR),
  email            VARCHAR2(50 CHAR),
  address          VARCHAR2(500 CHAR),
  postcode         VARCHAR2(6 CHAR),
  cert_type        VARCHAR2(30 CHAR),
  cert_no          VARCHAR2(30 CHAR),
  freeze_date      VARCHAR2(8 CHAR),
  fail_num         VARCHAR2(2 CHAR),
  mobile           VARCHAR2(13 CHAR),
  lst_modi_time    VARCHAR2(8 CHAR),
  lst_modi_user_id VARCHAR2(20 CHAR),
  privilege_all    VARCHAR2(1 CHAR),
  privilege_self   VARCHAR2(1 CHAR),
  privilege_other  VARCHAR2(2 CHAR),
  user_category    VARCHAR2(1 CHAR) default 1
)
;
  

create table CPMG.CTP_USER_ITEM_REL
(
  user_id   VARCHAR2(20 CHAR) not null,
  item_id   VARCHAR2(8 CHAR) not null,
  privilege VARCHAR2(5 CHAR) not null
)
;
  

create table CPMG.CTP_USER_MENU_REL
(
  user_id   VARCHAR2(20 CHAR) not null,
  menu_id   VARCHAR2(8 CHAR) not null,
  privilege VARCHAR2(5 CHAR) not null
)
;
  

create table CPMG.CTP_USER_SHORTCUT_MENU
(
  user_id VARCHAR2(20 CHAR) not null,
  role_id VARCHAR2(20 CHAR),
  item_id VARCHAR2(8 CHAR) not null
)
;
  

create table CPMG.DIC_ALL_KIND
(
  operation_kind VARCHAR2(30 CHAR) not null,
  kind_id        VARCHAR2(30 CHAR) not null,
  kind_name      VARCHAR2(200 CHAR),
  remark         VARCHAR2(200 CHAR)
)
;
  

create table CPMG.DIC_G4A_ITEM
(
  item_id      VARCHAR2(60 CHAR) not null,
  item_desc    VARCHAR2(200 CHAR),
  item_class   VARCHAR2(2 CHAR),
  item_formula VARCHAR2(2000 CHAR),
  isvalue      VARCHAR2(1 CHAR)
)
;
  

create table CPMG.DIC_INDEX_RELATION_G4A
(
  item_id    VARCHAR2(60 CHAR) not null,
  item_code  VARCHAR2(20 CHAR) not null,
  field_name VARCHAR2(60 CHAR) not null,
  rid        VARCHAR2(60 CHAR) not null,
  item_desc  VARCHAR2(200 CHAR),
  valid_date VARCHAR2(8 CHAR) not null
)
;
  

create table CPMG.DM_CS_CZFX
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag CHAR(1) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodcode VARCHAR2(10 CHAR) not null,
  ec       NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_PRODUCT
(
  subject_code VARCHAR2(8 CHAR) not null,
  product_id   VARCHAR2(10 CHAR) not null,
  product_name VARCHAR2(100 CHAR) not null,
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.DM_CS_QTFX
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag CHAR(1) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2),
  value05  NUMBER(26,2),
  value06  NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_YHK
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag CHAR(1) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_YHK_UG
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag VARCHAR2(1 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_ZBWZCDB
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag VARCHAR2(1 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_ZFRDK
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  bankflag  VARCHAR2(1 CHAR) not null,
  qx        VARCHAR2(10 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  cresort   VARCHAR2(20 CHAR) not null,
  prodid    VARCHAR2(10 CHAR) not null,
  loantype  VARCHAR2(5 CHAR) not null,
  hydm      VARCHAR2(7 CHAR) not null,
  corpgrade VARCHAR2(6 CHAR) not null,
  value01   NUMBER(26,2),
  value02   NUMBER(26,2),
  value03   NUMBER(26,2),
  value04   NUMBER(26,2),
  value05   NUMBER(26,2),
  value06   NUMBER(26,2),
  value07   NUMBER(26,2),
  value08   NUMBER(26,2),
  value09   NUMBER(26,2),
  value010  NUMBER(26,2),
  smallcorp VARCHAR2(1 CHAR) not null,
  zone_par  VARCHAR2(2),
  lgd_level VARCHAR2(10 CHAR)
)
;
  

create table CPMG.DM_CS_ZGRDK
(
  zoneno          VARCHAR2(5 CHAR) not null,
  brno            VARCHAR2(5 CHAR) not null,
  bankflag        VARCHAR2(1 CHAR) not null,
  qx              VARCHAR2(10 CHAR) not null,
  prodid          VARCHAR2(10 CHAR) not null,
  currtype        VARCHAR2(3 CHAR) not null,
  loanshape       VARCHAR2(3 CHAR) not null,
  creditrank      VARCHAR2(30 CHAR) not null,
  value01         NUMBER(26,2),
  value02         NUMBER(26,2),
  value03         NUMBER(26,2),
  value04         NUMBER(26,2),
  value05         NUMBER(26,2),
  value06         NUMBER(26,2),
  value07         NUMBER(26,2),
  value08         NUMBER(26,2),
  value09         NUMBER(26,2),
  value010        NUMBER(26,2),
  value011        NUMBER(26,2),
  value012        NUMBER(26,2),
  zoneno_for_part VARCHAR2(2) not null
)
;
  

create table CPMG.DM_CS_ZGRDK_UG
(
  zoneno          VARCHAR2(5 CHAR) not null,
  brno            VARCHAR2(5 CHAR) not null,
  bankflag        VARCHAR2(1 CHAR) not null,
  qx              VARCHAR2(10 CHAR) not null,
  prodid          VARCHAR2(10 CHAR) not null,
  currtype        VARCHAR2(3 CHAR) not null,
  loanshape       VARCHAR2(3 CHAR) not null,
  creditrank      VARCHAR2(30 CHAR) not null,
  value01         NUMBER(26,2),
  value02         NUMBER(26,2),
  value03         NUMBER(26,2),
  value04         NUMBER(26,2),
  value05         NUMBER(26,2),
  value06         NUMBER(26,2),
  value07         NUMBER(26,2),
  value08         NUMBER(26,2),
  value09         NUMBER(26,2),
  value010        NUMBER(26,2),
  value011        NUMBER(26,2),
  value012        NUMBER(26,2),
  zoneno_for_part VARCHAR2(2)
)
;
  

create table CPMG.DM_CS_ZPJTX
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  bankflag  VARCHAR2(1 CHAR) not null,
  qx        VARCHAR2(10 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  prodcode  VARCHAR2(10 CHAR) not null,
  loanshape VARCHAR2(3 CHAR) not null,
  value01   NUMBER(26,2),
  value02   NUMBER(26,2),
  value03   NUMBER(26,2),
  value04   NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_ZWXZC
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag VARCHAR2(1 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_ZZJYE
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag VARCHAR2(1 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2)
)
;
  

create table CPMG.DM_CS_ZZQTZ
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodid   VARCHAR2(10 CHAR) not null,
  value01  NUMBER(26,2),
  value02  NUMBER(26,2),
  value03  NUMBER(26,2),
  value04  NUMBER(26,2)
)
;
  

 

create table CPMG.DW_BW_CAP_DETAIL
(
  data_date   VARCHAR2(8 CHAR),
  ta280362001 VARCHAR2(20 CHAR),
  ta280362002 VARCHAR2(60 CHAR),
  ta280362003 VARCHAR2(45 CHAR),
  bom_id      VARCHAR2(10 CHAR),
  curr_type   VARCHAR2(5 CHAR),
  balance     NUMBER(22,2),
  db_bal      NUMBER(22,2),
  ecost       NUMBER(24,2)
)
;
  

create table CPMG.DW_CAP_GRKH_SUM
(
  data_date VARCHAR2(8 CHAR),
  cisno     VARCHAR2(30 CHAR),
  cap_now   NUMBER(22,2),
  cap_12mon NUMBER(22,2)
)
;
  

 

create table CPMG.DW_DAT_DATA_LOAD
(
  table_id    VARCHAR2(20 CHAR) not null,
  data_date   VARCHAR2(8) not null,
  load_time   VARCHAR2(20 CHAR),
  data_cont   NUMBER(18),
  modi_cont   NUMBER(18),
  insert_cont NUMBER(18)
)
;
  

create table CPMG.DW_DAT_HFCAEXOA
(
  zoneno    VARCHAR2(5 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  currsmbl  VARCHAR2(5 CHAR),
  uscvrate  NUMBER(17),
  lxsrate   NUMBER(17),
  yearrate  NUMBER(17),
  data_date VARCHAR2(10) not null
)
;
  

create table CPMG.DW_DAT_HNFPASBJ
(
  zoneno    VARCHAR2(5 CHAR) not null,
  subcode   VARCHAR2(7 CHAR) not null,
  subnatue  VARCHAR2(1 CHAR),
  subclass  VARCHAR2(9 CHAR),
  subno     VARCHAR2(7 CHAR),
  notes     VARCHAR2(80 CHAR),
  intdiff   VARCHAR2(3 CHAR),
  appno     VARCHAR2(3 CHAR),
  data_date VARCHAR2(10) not null
)
;
  

create table CPMG.DW_DAT_NFGNLED
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  subcode   VARCHAR2(7 CHAR) not null,
  subclass  VARCHAR2(9 CHAR),
  subnatue  VARCHAR2(1 CHAR),
  lydrbal   NUMBER(18),
  lycrbal   NUMBER(18),
  tydramt   NUMBER(18),
  tycramt   NUMBER(18),
  tydrino   NUMBER(9),
  tycrino   NUMBER(9),
  lmdrbal   NUMBER(18),
  lmcrbal   NUMBER(18),
  tmdramt   NUMBER(18),
  tmcramt   NUMBER(18),
  tmdrino   NUMBER(9),
  tmcrino   NUMBER(9),
  lddrbal   NUMBER(18),
  ldcrbal   NUMBER(18),
  tddramt   NUMBER(18),
  tdcramt   NUMBER(18),
  tddrbal   NUMBER(18),
  tdcrbal   NUMBER(18),
  tddrino   NUMBER(9),
  tdcrino   NUMBER(9),
  dracm     NUMBER(18),
  cracm     NUMBER(18),
  acccont   NUMBER(9),
  data_date VARCHAR2(10) not null
)
;
  

create table CPMG.DW_DAT_NFPABRP
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  mbrno     VARCHAR2(5 CHAR) not null,
  brtype    VARCHAR2(1 CHAR),
  notes     VARCHAR2(40 CHAR),
  status    VARCHAR2(1 CHAR),
  rsfflag   VARCHAR2(1 CHAR),
  rsflhhh   VARCHAR2(5 CHAR),
  rsffqh    VARCHAR2(3 CHAR),
  rsflhhbs  VARCHAR2(7 CHAR),
  rsflhhm   VARCHAR2(60 CHAR),
  actbrno   VARCHAR2(5 CHAR),
  data_date VARCHAR2(10) not null
)
;
  

create table CPMG.DW_DAT_NFWKPRF
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  prftype   VARCHAR2(1 CHAR),
  subno     VARCHAR2(7 CHAR) not null,
  prtcol    VARCHAR2(1 CHAR),
  prtname   VARCHAR2(40 CHAR),
  amount    NUMBER(18),
  flag      VARCHAR2(1 CHAR),
  data_date VARCHAR2(10) not null
)
;
  

create table CPMG.DW_DAT_STATNFWKPRF
(
  data_date VARCHAR2(8) not null,
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  bankflag  VARCHAR2(1 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  subno     VARCHAR2(7 CHAR) not null,
  amount    NUMBER(22,2)
)
;
  

create table CPMG.DW_G4A_ITEM
(
  org       VARCHAR2(24 CHAR) not null,
  scale     VARCHAR2(5 CHAR) not null,
  item_id   VARCHAR2(60 CHAR) not null,
  value     VARCHAR2(200 CHAR),
  data_date VARCHAR2(8) not null
)
;
  

create table CPMG.DW_G4A_ITEM_TMP
(
  org       VARCHAR2(24 CHAR) not null,
  scale     VARCHAR2(5 CHAR) not null,
  item_id   VARCHAR2(60 CHAR) not null,
  value     VARCHAR2(200 CHAR),
  data_date VARCHAR2(8) not null,
  check_seq VARCHAR2(10 CHAR) not null
)
;
  

create table CPMG.DW_G4A_REPORT_DATA
(
  data_date VARCHAR2(8) not null,
  rid       VARCHAR2(60 CHAR) not null,
  scale     VARCHAR2(5 CHAR) not null,
  item_code VARCHAR2(60 CHAR) not null,
  org       VARCHAR2(24 CHAR) not null,
  value1    VARCHAR2(200 CHAR),
  value2    VARCHAR2(200 CHAR),
  value3    VARCHAR2(200 CHAR),
  value4    VARCHAR2(200 CHAR),
  value5    VARCHAR2(200 CHAR),
  value6    VARCHAR2(200 CHAR),
  value7    VARCHAR2(200 CHAR),
  value8    VARCHAR2(200 CHAR),
  value9    VARCHAR2(200 CHAR),
  value10   VARCHAR2(200 CHAR),
  value11   VARCHAR2(200 CHAR),
  value12   VARCHAR2(200 CHAR),
  value13   VARCHAR2(200 CHAR),
  value14   VARCHAR2(200 CHAR),
  value15   VARCHAR2(200 CHAR),
  value16   VARCHAR2(200 CHAR),
  value17   VARCHAR2(200 CHAR),
  value18   VARCHAR2(200 CHAR),
  value19   VARCHAR2(200 CHAR),
  value20   VARCHAR2(200 CHAR),
  value21   VARCHAR2(200 CHAR),
  value22   VARCHAR2(200 CHAR),
  value23   VARCHAR2(200 CHAR),
  value24   VARCHAR2(200 CHAR),
  value25   VARCHAR2(200 CHAR),
  value26   VARCHAR2(200 CHAR),
  value27   VARCHAR2(200 CHAR),
  value28   VARCHAR2(200 CHAR),
  value29   VARCHAR2(200 CHAR),
  value30   VARCHAR2(200 CHAR),
  value31   VARCHAR2(200 CHAR),
  value32   VARCHAR2(200 CHAR),
  value33   VARCHAR2(200 CHAR),
  value34   VARCHAR2(200 CHAR),
  value35   VARCHAR2(200 CHAR),
  value36   VARCHAR2(200 CHAR),
  value37   VARCHAR2(200 CHAR),
  value38   VARCHAR2(200 CHAR),
  value39   VARCHAR2(200 CHAR),
  value40   VARCHAR2(200 CHAR),
  value41   VARCHAR2(200 CHAR),
  value42   VARCHAR2(200 CHAR),
  value43   VARCHAR2(200 CHAR),
  value44   VARCHAR2(200 CHAR),
  value45   VARCHAR2(200 CHAR),
  value46   VARCHAR2(200 CHAR),
  value47   VARCHAR2(200 CHAR),
  value48   VARCHAR2(200 CHAR),
  value49   VARCHAR2(200 CHAR),
  value50   VARCHAR2(200 CHAR)
)
;
  

create table CPMG.DW_G4A_REPORT_DATA_TMP
(
  data_date VARCHAR2(8) not null,
  rid       VARCHAR2(60 CHAR) not null,
  scale     VARCHAR2(5 CHAR) not null,
  item_code VARCHAR2(60 CHAR) not null,
  org       VARCHAR2(24 CHAR) not null,
  check_seq VARCHAR2(10 CHAR) not null,
  value1    VARCHAR2(200 CHAR),
  value2    VARCHAR2(200 CHAR),
  value3    VARCHAR2(200 CHAR),
  value4    VARCHAR2(200 CHAR),
  value5    VARCHAR2(200 CHAR),
  value6    VARCHAR2(200 CHAR),
  value7    VARCHAR2(200 CHAR),
  value8    VARCHAR2(200 CHAR),
  value9    VARCHAR2(200 CHAR),
  value10   VARCHAR2(200 CHAR),
  value11   VARCHAR2(200 CHAR),
  value12   VARCHAR2(200 CHAR),
  value13   VARCHAR2(200 CHAR),
  value14   VARCHAR2(200 CHAR),
  value15   VARCHAR2(200 CHAR),
  value16   VARCHAR2(200 CHAR),
  value17   VARCHAR2(200 CHAR),
  value18   VARCHAR2(200 CHAR),
  value19   VARCHAR2(200 CHAR),
  value20   VARCHAR2(200 CHAR),
  value21   VARCHAR2(200 CHAR),
  value22   VARCHAR2(200 CHAR),
  value23   VARCHAR2(200 CHAR),
  value24   VARCHAR2(200 CHAR),
  value25   VARCHAR2(200 CHAR),
  value26   VARCHAR2(200 CHAR),
  value27   VARCHAR2(200 CHAR),
  value28   VARCHAR2(200 CHAR),
  value29   VARCHAR2(200 CHAR),
  value30   VARCHAR2(200 CHAR),
  value31   VARCHAR2(200 CHAR),
  value32   VARCHAR2(200 CHAR),
  value33   VARCHAR2(200 CHAR),
  value34   VARCHAR2(200 CHAR),
  value35   VARCHAR2(200 CHAR),
  value36   VARCHAR2(200 CHAR),
  value37   VARCHAR2(200 CHAR),
  value38   VARCHAR2(200 CHAR),
  value39   VARCHAR2(200 CHAR),
  value40   VARCHAR2(200 CHAR),
  value41   VARCHAR2(200 CHAR),
  value42   VARCHAR2(200 CHAR),
  value43   VARCHAR2(200 CHAR),
  value44   VARCHAR2(200 CHAR),
  value45   VARCHAR2(200 CHAR),
  value46   VARCHAR2(200 CHAR),
  value47   VARCHAR2(200 CHAR),
  value48   VARCHAR2(200 CHAR),
  value49   VARCHAR2(200 CHAR),
  value50   VARCHAR2(200 CHAR)
)
;
  

create table CPMG.DW_GCMS_ZTM280362
(
  data_date       VARCHAR2(10 CHAR) not null,
  business_id     VARCHAR2(45 CHAR) not null,
  organization_id VARCHAR2(10 CHAR),
  zoneno          VARCHAR2(10 CHAR),
  branchno        VARCHAR2(10 CHAR),
  bankflag        VARCHAR2(1 CHAR),
  cust_id         VARCHAR2(20 CHAR) not null,
  cust_name       VARCHAR2(60 CHAR),
  contract_id     VARCHAR2(45 CHAR),
  loan_kind       VARCHAR2(5 CHAR),
  business_kind   VARCHAR2(5 CHAR),
  approve_amt     NUMBER(16,2),
  promiss_amt     NUMBER(16,2),
  currency        VARCHAR2(3 CHAR) not null,
  industry        VARCHAR2(10 CHAR),
  subject         VARCHAR2(10 CHAR),
  promiss_flag    NUMBER(1)
)
;
  

create table CPMG.DW_MOD_BMSBILL
(
  zone         VARCHAR2(5),
  brno         VARCHAR2(5),
  upsaveinst   VARCHAR2(10),
  innerno      VARCHAR2(60) not null,
  billno       VARCHAR2(30),
  subcode      VARCHAR2(8),
  amount       NUMBER(24,2),
  billtype     VARCHAR2(4),
  buytype      VARCHAR2(4),
  firsttype    VARCHAR2(4),
  acceptbank   VARCHAR2(30),
  holder       VARCHAR2(30),
  discountdate VARCHAR2(8),
  realmaturity VARCHAR2(8),
  retmaturity  VARCHAR2(8),
  openoper     VARCHAR2(30),
  product      VARCHAR2(10),
  final_flag   VARCHAR2(6),
  credit_level VARCHAR2(6),
  preprop      NUMBER(5,2),
  preamount    NUMBER(28,4),
  netamount    NUMBER(28,4),
  quotiety     NUMBER(22,6),
  capital      NUMBER(28,4),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8)
)
;
  

create table CPMG.DW_MOD_BMSBILL_WB
(
  zoneno      VARCHAR2(5 CHAR) not null,
  brno        VARCHAR2(5 CHAR) not null,
  currtype    VARCHAR2(3 CHAR) not null,
  product     VARCHAR2(10 CHAR) not null,
  balance     NUMBER(28,4),
  avg_balance NUMBER(28,4),
  quotiety    NUMBER(9,6),
  capital     NUMBER(28,4),
  avg_capital NUMBER(28,4),
  data_date   VARCHAR2(8) not null,
  bankflag    VARCHAR2(1 CHAR) default 2 not null
)
;
  

create table CPMG.DW_MOD_FRDK
(
  loanno         VARCHAR2(17 CHAR) not null,
  loansqno       VARCHAR2(3 CHAR) not null,
  accno          VARCHAR2(17 CHAR) not null,
  zoneno         VARCHAR2(5 CHAR),
  brno           VARCHAR2(5 CHAR),
  currtype       VARCHAR2(4 CHAR) not null,
  subcode        VARCHAR2(8 CHAR),
  balance        NUMBER(20,2),
  valueday       VARCHAR2(10 CHAR),
  exhmatud       VARCHAR2(10 CHAR),
  ta490461016    NUMBER(18,2),
  ta490461017    NUMBER(9,2),
  ta200261001    VARCHAR2(30 CHAR),
  ta200261002    VARCHAR2(60 CHAR),
  ta200261004    VARCHAR2(30 CHAR),
  ta200261037    VARCHAR2(6 CHAR),
  ta200251013    VARCHAR2(6 CHAR),
  ta200251011    VARCHAR2(6 CHAR),
  ta200251020    VARCHAR2(30 CHAR),
  ta200011014    VARCHAR2(7 CHAR),
  ta200011040    VARCHAR2(6 CHAR),
  ta200011070    VARCHAR2(6 CHAR),
  ta200011005    VARCHAR2(60 CHAR),
  ta200011016    VARCHAR2(5 CHAR),
  ta200011034    VARCHAR2(30 CHAR),
  ta340362013    NUMBER(10,3),
  ta200268004    VARCHAR2(12 CHAR),
  rzzd_fl        VARCHAR2(5 CHAR),
  product_id3    VARCHAR2(10 CHAR),
  product_name3  VARCHAR2(100 CHAR),
  xyvalue        VARCHAR2(6 CHAR),
  xyname         VARCHAR2(100 CHAR),
  dbdc_id        VARCHAR2(10 CHAR),
  dbdc_name      VARCHAR2(40 CHAR),
  dbfs_id        VARCHAR2(5 CHAR),
  dbfs_lb        VARCHAR2(50 CHAR),
  qxdc_id1       VARCHAR2(10 CHAR),
  qxdc_name1     VARCHAR2(40 CHAR),
  qxdc_id2       VARCHAR2(10 CHAR),
  qxdc_name2     VARCHAR2(40 CHAR),
  xs031          NUMBER(7,4),
  xs034          NUMBER(7,4),
  xs033          NUMBER(7,4),
  xs032          NUMBER(7,4),
  xs035          NUMBER(7,4),
  xs036          NUMBER(7,4),
  gjrz_xs        NUMBER(7,4),
  jjzb_xs        NUMBER(20,4),
  ytbbbl         NUMBER(24,2),
  ytbbj          NUMBER(24,2),
  dkamount       NUMBER(24,2),
  ecost          NUMBER(24,2),
  start_date     VARCHAR2(8) not null,
  end_date       VARCHAR2(8 CHAR),
  xs_product     NUMBER(7,4),
  ywzl           VARCHAR2(6 CHAR),
  htsfsr         NUMBER(18,2),
  rate_today     NUMBER(9),
  pd             NUMBER(9,6),
  lgd            NUMBER(9,6),
  balance_cm     NUMBER(20,2),
  ta490461016_cm NUMBER(18,2),
  ytbbj_cm       NUMBER(24,2),
  ecost_cm       NUMBER(24,2),
  rate_base      NUMBER(9),
  small_corp     VARCHAR2(1 CHAR) default 0 not null,
  dubil_std_rate NUMBER(9),
  rate_chg_mode  NUMBER(1),
  corp_name      VARCHAR2(60 CHAR),
  croup_name     VARCHAR2(60 CHAR),
  zone_par       VARCHAR2(2),
  lgd_level      VARCHAR2(10 CHAR),
  bnkrate        NUMBER(20,6),
  cust_exe_rate  NUMBER(20,9),
  nloan_tjxs     NUMBER(8,4)
)
;
  

create table CPMG.DW_MOD_GRDK
(
  accno           VARCHAR2(17 CHAR) not null,
  lncode          VARCHAR2(13 CHAR) not null,
  memno           VARCHAR2(5 CHAR) not null,
  lnno            VARCHAR2(7 CHAR) not null,
  zoneno          VARCHAR2(5 CHAR),
  actbrno         VARCHAR2(5 CHAR),
  subcode         VARCHAR2(8 CHAR),
  currtype        VARCHAR2(4 CHAR) not null,
  balance         NUMBER(20,2),
  ta500261001     VARCHAR2(60 CHAR),
  product_id3     VARCHAR2(10 CHAR),
  product_name3   VARCHAR2(100 CHAR),
  ta500261003     VARCHAR2(30 CHAR),
  ta500261004     VARCHAR2(30 CHAR),
  ta500261008     VARCHAR2(4 CHAR),
  ta500261013     VARCHAR2(8 CHAR),
  ta500261018     VARCHAR2(6 CHAR),
  ta500261016     VARCHAR2(8 CHAR),
  ta500251012     NUMBER(18,2),
  ta500251018     VARCHAR2(50 CHAR),
  ta500251014     NUMBER(9,6),
  ta500251013     NUMBER(6),
  ta500271009     NUMBER(18,2),
  ta500281016     NUMBER(18,2),
  ta500291012     NUMBER(18,2),
  ta500294008     NUMBER(18,2),
  other_amount    NUMBER(18,2),
  invaild_amount  NUMBER(20,2),
  xyamount        NUMBER(20,2),
  ta500251054     VARCHAR2(5 CHAR),
  totalamount     NUMBER(20,2),
  ta290001004     VARCHAR2(100 CHAR),
  ta290001005     VARCHAR2(4 CHAR),
  ta290001006     VARCHAR2(60 CHAR),
  ta290008004     VARCHAR2(100 CHAR),
  ta290008009     VARCHAR2(60 CHAR),
  ta500251097     VARCHAR2(30 CHAR),
  dkyq_year       NUMBER(17,4),
  bbprecent       NUMBER(5,2),
  yybb            NUMBER(22,4),
  sybb            NUMBER(22,4),
  zcamount        NUMBER(22,4),
  jjzb_xs         NUMBER(22,6),
  ecost           NUMBER(22,4),
  start_date      VARCHAR2(8) not null,
  end_date        VARCHAR2(8 CHAR),
  dkyq_id         VARCHAR2(10 CHAR),
  ecost_rj        NUMBER(22,4),
  bal_rj          NUMBER(22,4),
  status          VARCHAR2(6 CHAR),
  zoneno_for_part VARCHAR2(2) not null
)
;
  

create table CPMG.DW_MOD_GRDK_UG
(
  zoneno          VARCHAR2(5 CHAR),
  phybrno         VARCHAR2(5 CHAR),
  pcaccno         VARCHAR2(17 CHAR) not null,
  prodcode        VARCHAR2(13 CHAR) not null,
  memno           VARCHAR2(5 CHAR) not null,
  lnno            VARCHAR2(7 CHAR) not null,
  ta500251054     VARCHAR2(5 CHAR),
  relatedno       VARCHAR2(60 CHAR),
  currtype        VARCHAR2(3 CHAR) not null,
  subcode         VARCHAR2(6 CHAR),
  loantype        VARCHAR2(4 CHAR),
  levelfive       VARCHAR2(6 CHAR),
  lnamount        NUMBER(18,2),
  lnbalance       NUMBER(18,2),
  loanterm        NUMBER(6),
  avchtype        VARCHAR2(50 CHAR),
  surveyor        VARCHAR2(30 CHAR),
  loandate        VARCHAR2(8 CHAR),
  duedate         VARCHAR2(8 CHAR),
  custid          VARCHAR2(30 CHAR),
  custcis         VARCHAR2(30 CHAR),
  custname        VARCHAR2(60 CHAR),
  regtype         VARCHAR2(4 CHAR),
  regno           VARCHAR2(60 CHAR),
  creditlevel     VARCHAR2(6 CHAR),
  ecusflag        VARCHAR2(2 CHAR),
  avchamt         NUMBER(18,2),
  mgamt           NUMBER(18,2),
  mpamt           NUMBER(18,2),
  assureamt       NUMBER(18,2),
  otheramt        NUMBER(18,2),
  invalidamt      NUMBER(18,2),
  creditamt       NUMBER(18,2),
  totalamt        NUMBER(18,2),
  status          VARCHAR2(6 CHAR),
  fbflag          VARCHAR2(9 CHAR),
  pd_value        NUMBER(12,2),
  lgd_value       NUMBER(12,2),
  r_value         NUMBER(12,2),
  deflt_flag      VARCHAR2(9 CHAR),
  ovdue_cnt       VARCHAR2(11 CHAR),
  product_id3     VARCHAR2(10 CHAR),
  product_name3   VARCHAR2(60 CHAR),
  maturity_days   NUMBER(9),
  term_level_no   VARCHAR2(2 CHAR),
  term_level_name VARCHAR2(50 CHAR),
  k_value         NUMBER(12,2),
  ec_coef         NUMBER(22,6),
  zcamount        NUMBER(22,6),
  ecost           NUMBER(22,6),
  data_date       VARCHAR2(8) not null,
  rate            NUMBER(20,9),
  nloan_tjxs      NUMBER(8,4)
)
;
  

create table CPMG.DW_MOD_PJTX
(
  zoneno       VARCHAR2(5 CHAR),
  actbrno      VARCHAR2(5 CHAR),
  innerno      VARCHAR2(60 CHAR) not null,
  billno       VARCHAR2(30 CHAR),
  subcode      VARCHAR2(6 CHAR),
  billamt      NUMBER(18,2),
  billamt_cm   NUMBER(18,2),
  billtype     VARCHAR2(4 CHAR),
  buytype      VARCHAR2(4 CHAR),
  firsttype    VARCHAR2(4 CHAR),
  drawerbank   VARCHAR2(30 CHAR),
  holder       VARCHAR2(30 CHAR),
  holdertype   VARCHAR2(4 CHAR),
  creditlevel  VARCHAR2(6 CHAR),
  managerno    VARCHAR2(30 CHAR),
  levelfive    VARCHAR2(6 CHAR),
  discountdate VARCHAR2(8 CHAR),
  realmaturity VARCHAR2(8 CHAR),
  retmaturity  VARCHAR2(8 CHAR),
  capcost      NUMBER(17,2),
  intincome    NUMBER(13,2),
  product      VARCHAR2(10 CHAR),
  qx           VARCHAR2(6 CHAR),
  xspj1        NUMBER(7,4),
  xspj2        NUMBER(7,4),
  xspj3        NUMBER(7,4),
  xs           NUMBER(9,6),
  ec           NUMBER(24,2),
  ec_cm        NUMBER(24,2),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR),
  bearer_name  VARCHAR2(60 CHAR),
  bearer_type  VARCHAR2(4 CHAR),
  croup_num    VARCHAR2(30 CHAR),
  croup_name   VARCHAR2(60 CHAR)
)
;
  

create table CPMG.DW_MOD_PVMSDATA
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
)
;
  

create table CPMG.DW_MOD_PVMSDATA_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
)
;
  

create table CPMG.DW_MOD_PVMSDATA_11_TMP
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
)
;
  

create table CPMG.DW_MOD_QTFX
(
  zoneno     VARCHAR2(5 CHAR) not null,
  brno       VARCHAR2(5 CHAR) not null,
  currtype   VARCHAR2(3 CHAR) not null,
  subcode    VARCHAR2(6 CHAR) not null,
  balance    NUMBER(20,2),
  balance_cm NUMBER(20,2),
  data_date  VARCHAR2(8) not null,
  product    VARCHAR2(10 CHAR) not null,
  xsqt1      NUMBER(5,2),
  ec         NUMBER(24,2),
  ec_cm      NUMBER(24,2),
  bal_aveacm NUMBER(24,2),
  ec_aveacm  NUMBER(24,2)
)
;
  

create table CPMG.DW_MOD_YHK
(
  zoneno     VARCHAR2(5 CHAR) not null,
  brno       VARCHAR2(5 CHAR) not null,
  data_date  VARCHAR2(8) not null,
  currtype   VARCHAR2(3 CHAR) not null,
  sjcp       VARCHAR2(10 CHAR) not null,
  sjcpje     NUMBER(24,2),
  fpxs       NUMBER(9,6),
  jjzbzye    NUMBER(24,2),
  sumsjcpje  NUMBER(24,2),
  sumjjzbzye NUMBER(24,2),
  bankflag   CHAR(1) not null
)
;
  

create table CPMG.DW_MOD_YHK_UG
(
  accno         VARCHAR2(17 CHAR) not null,
  mdcardno      VARCHAR2(19 CHAR) not null,
  cardkind      VARCHAR2(2 CHAR),
  corperf       VARCHAR2(2 CHAR),
  currtype      VARCHAR2(3 CHAR),
  cino          VARCHAR2(15 CHAR),
  openzono      VARCHAR2(5 CHAR),
  openbrno      VARCHAR2(5 CHAR),
  opendate      VARCHAR2(10 CHAR),
  subcode       VARCHAR2(8 CHAR) not null,
  credit        NUMBER(20,2),
  fivicode      VARCHAR2(3 CHAR),
  custname      VARCHAR2(200 CHAR),
  custsort      VARCHAR2(3 CHAR),
  custcode      VARCHAR2(20 CHAR),
  fbflag        VARCHAR2(1 CHAR),
  pd_value      NUMBER(12,2),
  lgd_value     NUMBER(12,2),
  r_value       NUMBER(12,2),
  k_value       NUMBER(12,2),
  deflt_flag    VARCHAR2(1 CHAR),
  product_id3   VARCHAR2(10 CHAR),
  product_name3 VARCHAR2(60 CHAR),
  zcamount      NUMBER(26,2),
  ecost         NUMBER(26,2),
  ec_coef       NUMBER(22,6),
  data_date     VARCHAR2(8) not null
)
;
  

create table CPMG.DW_QUE_ZF_CHECK
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  bankflag  VARCHAR2(1 CHAR) not null,
  subcode   VARCHAR2(8 CHAR) not null,
  currency  VARCHAR2(3 CHAR) not null,
  balance_z NUMBER(18,2),
  balance_f NUMBER(18,2),
  amount    NUMBER(18,2),
  data_date VARCHAR2(10) not null
)
;
  

create table CPMG.DW_ZJZJRJYE
(
  data_date    VARCHAR2(8 CHAR) not null,
  zoneno       VARCHAR2(5 CHAR) not null,
  brno         VARCHAR2(5 CHAR) not null,
  branch_level VARCHAR2(1 CHAR) not null,
  currency     VARCHAR2(3 CHAR) not null,
  term         VARCHAR2(3 CHAR) not null,
  ad_balance   NUMBER(32,10)
)
;
  

create table CPMG.EXP_CAP_ASA_A0001
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  module_id VARCHAR2(20 CHAR),
  cnt       NUMBER(18),
  datadate  VARCHAR2(6 CHAR)
)
;
  

create table CPMG.EXP_CAP_ASA_A0002
(
  zoneno     VARCHAR2(5 CHAR),
  brno       VARCHAR2(5 CHAR),
  user_level VARCHAR2(1 CHAR),
  cnt        NUMBER(18),
  cnt_total  NUMBER(18),
  datadate   VARCHAR2(6 CHAR)
)
;
  

create table CPMG.EXP_CAP_ASA_A0003
(
  module_id   VARCHAR2(20 CHAR),
  module_name VARCHAR2(100 CHAR),
  cnt         NUMBER(18),
  datadate    VARCHAR2(6 CHAR)
)
;
  

create table CPMG.LOG_APP_OPERATION
(
  op_type    VARCHAR2(2 CHAR) not null,
  user_id    VARCHAR2(200 CHAR) not null,
  op_time    TIMESTAMP(6) not null,
  op_target  VARCHAR2(200 CHAR),
  extra_info VARCHAR2(1000 CHAR)
)
;
  

create table CPMG.LOG_USERLOG
(
  logdate       VARCHAR2(8 CHAR) not null,
  logtime       VARCHAR2(6 CHAR) not null,
  session_id    VARCHAR2(50 CHAR),
  user_id       VARCHAR2(20 CHAR) not null,
  branch_id     VARCHAR2(9 CHAR),
  user_ip       VARCHAR2(15 CHAR),
  role_id       VARCHAR2(20 CHAR),
  funcmodule_id VARCHAR2(20 CHAR),
  detail        VARCHAR2(300 CHAR)
)
;
  

create table CPMG.L_GETDATA_CALL
(
  proc_name  VARCHAR2(100 CHAR),
  log_level  VARCHAR2(5 CHAR),
  begin_time VARCHAR2(30 CHAR),
  end_time   VARCHAR2(30 CHAR),
  deal_flag  VARCHAR2(10 CHAR)
)
;
  

create table CPMG.L_GETDATA_ERR
(
  proc_name  VARCHAR2(100 CHAR),
  log_level  VARCHAR2(5 CHAR),
  step_no    VARCHAR2(10 CHAR),
  sql_txt    VARCHAR2(4000 CHAR),
  err_code   VARCHAR2(100 CHAR),
  err_txt    VARCHAR2(4000 CHAR),
  begin_time VARCHAR2(30 CHAR),
  end_time   VARCHAR2(30 CHAR),
  deal_flag  VARCHAR2(10 CHAR)
)
;
  

create table CPMG.ODS_ERR_FRDK
(
  loanno       VARCHAR2(22 CHAR),
  loansqno     VARCHAR2(8 CHAR),
  accno        VARCHAR2(22 CHAR),
  zoneno       VARCHAR2(10 CHAR),
  actbrno      VARCHAR2(10 CHAR),
  currtype     VARCHAR2(9 CHAR),
  subcode      VARCHAR2(13 CHAR),
  balance      VARCHAR2(23 CHAR),
  exhmatud     VARCHAR2(13 CHAR),
  ta490461016  VARCHAR2(23 CHAR),
  ta490461017  VARCHAR2(14 CHAR),
  ta200011014  VARCHAR2(11 CHAR),
  ta200011040  VARCHAR2(11 CHAR),
  ta200011070  VARCHAR2(11 CHAR),
  ta340362013  VARCHAR2(14 CHAR),
  ta200261037  VARCHAR2(11 CHAR),
  ta200251013  VARCHAR2(11 CHAR),
  ta200261004  VARCHAR2(22 CHAR),
  ta200261001  VARCHAR2(35 CHAR),
  ta200011005  VARCHAR2(65 CHAR),
  ta200261002  VARCHAR2(65 CHAR),
  ta200011034  VARCHAR2(35 CHAR),
  ta200251020  VARCHAR2(35 CHAR),
  valueday     VARCHAR2(13 CHAR),
  ta200251011  VARCHAR2(11 CHAR),
  ta200268004  VARCHAR2(17 CHAR),
  ta200011016  VARCHAR2(9 CHAR),
  credit_level VARCHAR2(11 CHAR),
  err_code     VARCHAR2(30 CHAR) not null,
  check_date   VARCHAR2(8 CHAR) not null,
  status       VARCHAR2(6 CHAR),
  etl_tx_dt    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.ODS_ERR_FRDK2
(
  zoneno         VARCHAR2(10 CHAR),
  phybrno        VARCHAR2(10 CHAR),
  loanno         VARCHAR2(22 CHAR),
  loansqno       VARCHAR2(8 CHAR),
  accno          VARCHAR2(22 CHAR),
  lnmemno        VARCHAR2(65 CHAR),
  restamount     VARCHAR2(23 CHAR),
  currtype       VARCHAR2(8 CHAR),
  subcode        VARCHAR2(11 CHAR),
  buskind        VARCHAR2(10 CHAR),
  lnudflag       VARCHAR2(65 CHAR),
  loanway        VARCHAR2(11 CHAR),
  loandirt       VARCHAR2(17 CHAR),
  leveltwl       VARCHAR2(11 CHAR),
  levelfive      VARCHAR2(11 CHAR),
  avchcof        VARCHAR2(14 CHAR),
  realboamt      VARCHAR2(23 CHAR),
  surveyor       VARCHAR2(35 CHAR),
  loandate       VARCHAR2(13 CHAR),
  duedate        VARCHAR2(13 CHAR),
  custcode       VARCHAR2(35 CHAR),
  custname       VARCHAR2(65 CHAR),
  entrsize       VARCHAR2(9 CHAR),
  creditlevel    VARCHAR2(11 CHAR),
  groupcode      VARCHAR2(35 CHAR),
  groupname      VARCHAR2(65 CHAR),
  industrycode   VARCHAR2(11 CHAR),
  small_corp     VARCHAR2(6 CHAR),
  dubil_std_rate VARCHAR2(14 CHAR),
  rate_chg_mode  VARCHAR2(6 CHAR),
  configrate     VARCHAR2(14 CHAR),
  cust_exe_rate  NUMBER(20,9),
  income         VARCHAR2(23 CHAR),
  pd             VARCHAR2(14 CHAR),
  lgd            VARCHAR2(14 CHAR),
  status         VARCHAR2(11 CHAR),
  err_code       VARCHAR2(34 CHAR) not null,
  check_date     VARCHAR2(8 CHAR) not null,
  bnkrate        NUMBER(20,9)
)
;
  

create table CPMG.ODS_ERR_GRDK
(
  accno             VARCHAR2(22 CHAR),
  lncode            VARCHAR2(8 CHAR),
  memno             VARCHAR2(10 CHAR),
  lnno              VARCHAR2(8 CHAR),
  zoneno            VARCHAR2(10 CHAR),
  actbrno           VARCHAR2(10 CHAR),
  subcode           VARCHAR2(13 CHAR),
  currtype          VARCHAR2(9 CHAR),
  balance           VARCHAR2(23 CHAR),
  balance_cm        VARCHAR2(23 CHAR),
  ta500261001       VARCHAR2(65 CHAR),
  ta500261003       VARCHAR2(35 CHAR),
  ta500261004       VARCHAR2(35 CHAR),
  ta500261008       VARCHAR2(9 CHAR),
  ta500261013       VARCHAR2(13 CHAR),
  ta500261018       VARCHAR2(11 CHAR),
  ta500261016       VARCHAR2(13 CHAR),
  ta500251012       VARCHAR2(23 CHAR),
  ta500251012_cm    VARCHAR2(23 CHAR),
  ta500251018       VARCHAR2(8 CHAR),
  ta500251014       VARCHAR2(14 CHAR),
  ta500251013       NUMBER(6),
  ta500251097       VARCHAR2(35 CHAR),
  ta500251054       VARCHAR2(10 CHAR),
  ta290001004       VARCHAR2(105 CHAR),
  ta290001005       VARCHAR2(9 CHAR),
  ta290001006       VARCHAR2(65 CHAR),
  ta290008004       VARCHAR2(11 CHAR),
  ta290008009       VARCHAR2(7 CHAR),
  ta500271009       VARCHAR2(23 CHAR),
  ta500271009_cm    VARCHAR2(23 CHAR),
  ta500281016       VARCHAR2(23 CHAR),
  ta500281016_cm    VARCHAR2(23 CHAR),
  ta500291012       VARCHAR2(23 CHAR),
  ta500291012_cm    VARCHAR2(23 CHAR),
  ta500294008       VARCHAR2(23 CHAR),
  ta500294008_cm    VARCHAR2(23 CHAR),
  other_amount      VARCHAR2(23 CHAR),
  other_amount_cm   VARCHAR2(23 CHAR),
  invaild_amount    VARCHAR2(23 CHAR),
  invaild_amount_cm VARCHAR2(23 CHAR),
  xyamount          VARCHAR2(23 CHAR),
  totalamount       VARCHAR2(23 CHAR),
  err_code          VARCHAR2(45 CHAR) not null,
  check_date        VARCHAR2(8 CHAR) not null,
  status            VARCHAR2(6 CHAR),
  etl_tx_dt         VARCHAR2(8 CHAR)
)
;
  

create table CPMG.ODS_ERR_GRDK2
(
  zoneno      VARCHAR2(10 CHAR),
  phybrno     VARCHAR2(10 CHAR),
  pcaccno     VARCHAR2(22 CHAR),
  prodcode    VARCHAR2(8 CHAR),
  memno       VARCHAR2(10 CHAR),
  lnno        VARCHAR2(8 CHAR),
  ta500251054 VARCHAR2(10 CHAR),
  relatedno   VARCHAR2(65 CHAR),
  currtype    VARCHAR2(8 CHAR),
  subcode     VARCHAR2(11 CHAR),
  loantype    VARCHAR2(9 CHAR),
  levelfive   VARCHAR2(11 CHAR),
  lnamount    VARCHAR2(23 CHAR),
  lnbalance   VARCHAR2(23 CHAR),
  rate        NUMBER(20,9),
  loanterm    VARCHAR2(9 CHAR),
  avchtype    VARCHAR2(10 CHAR),
  surveyor    VARCHAR2(35 CHAR),
  loandate    VARCHAR2(13 CHAR),
  duedate     VARCHAR2(13 CHAR),
  custid      VARCHAR2(35 CHAR),
  custcis     VARCHAR2(35 CHAR),
  custname    VARCHAR2(65 CHAR),
  regtype     VARCHAR2(9 CHAR),
  regno       VARCHAR2(65 CHAR),
  creditlevel VARCHAR2(11 CHAR),
  ecusflag    VARCHAR2(7 CHAR),
  avchamt     VARCHAR2(23 CHAR),
  mgamt       VARCHAR2(23 CHAR),
  mpamt       VARCHAR2(23 CHAR),
  assureamt   VARCHAR2(23 CHAR),
  otheramt    VARCHAR2(23 CHAR),
  invalidamt  VARCHAR2(23 CHAR),
  creditamt   VARCHAR2(23 CHAR),
  totalamt    VARCHAR2(23 CHAR),
  err_code    VARCHAR2(35 CHAR) not null,
  check_date  VARCHAR2(8 CHAR) not null,
  status      VARCHAR2(12 CHAR)
)
;
  

create table CPMG.ODS_ERR_PJTX
(
  zoneno       VARCHAR2(10 CHAR),
  actbrno      VARCHAR2(10 CHAR),
  innerno      VARCHAR2(65 CHAR),
  billno       VARCHAR2(35 CHAR),
  subcode      VARCHAR2(11 CHAR),
  billamt      VARCHAR2(23 CHAR),
  billtype     VARCHAR2(9 CHAR),
  buytype      VARCHAR2(9 CHAR),
  firsttype    VARCHAR2(9 CHAR),
  drawerbank   VARCHAR2(35 CHAR),
  holder       VARCHAR2(35 CHAR),
  holdertype   VARCHAR2(9 CHAR),
  creditlevel  VARCHAR2(11 CHAR),
  managerno    VARCHAR2(35 CHAR),
  levelfive    VARCHAR2(11 CHAR),
  discountdate VARCHAR2(13 CHAR),
  realmaturity VARCHAR2(13 CHAR),
  retmaturity  VARCHAR2(13 CHAR),
  capcost      VARCHAR2(22 CHAR),
  intincome    VARCHAR2(18 CHAR),
  err_code     VARCHAR2(20 CHAR) not null,
  check_date   VARCHAR2(8 CHAR) not null
)
;
  

create table CPMG.ODS_TMP_BILL
(
  zoneno       VARCHAR2(5 CHAR) not null,
  brno         VARCHAR2(5 CHAR) not null,
  upsaveinst   VARCHAR2(10 CHAR),
  innerno      VARCHAR2(60 CHAR) not null,
  billno       VARCHAR2(30 CHAR),
  subcode      VARCHAR2(8 CHAR),
  amount       NUMBER(18,2),
  billtype     VARCHAR2(4 CHAR),
  buytype      VARCHAR2(4 CHAR),
  overdue      VARCHAR2(2 CHAR),
  firsttype    VARCHAR2(4 CHAR),
  acceptbank   VARCHAR2(30 CHAR),
  holder       VARCHAR2(30 CHAR),
  holdertype   VARCHAR2(4 CHAR),
  discountdate VARCHAR2(8 CHAR),
  realmaturity VARCHAR2(8 CHAR),
  retmaturity  VARCHAR2(8 CHAR),
  final_flag   VARCHAR2(6 CHAR),
  openoper     VARCHAR2(30 CHAR),
  credit_level VARCHAR2(6 CHAR)
)
;
  

create table CPMG.ODS_TMP_BWZC_DB
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  datadate VARCHAR2(8 CHAR),
  ywbh     VARCHAR2(60 CHAR) not null,
  ywzlbh   VARCHAR2(60 CHAR),
  htbh1    VARCHAR2(60 CHAR) not null,
  zjglh    VARCHAR2(60 CHAR) not null,
  bz       VARCHAR2(4 CHAR) not null,
  subcode  VARCHAR2(8 CHAR) not null,
  htje     NUMBER(18,2),
  htje_cm  NUMBER(18,2),
  sejfl    VARCHAR2(6 CHAR),
  ncxydj   VARCHAR2(6 CHAR),
  bzxydj   VARCHAR2(6 CHAR),
  dbfs     VARCHAR2(6 CHAR),
  jtdbfs   VARCHAR2(6 CHAR),
  dbxs     NUMBER(13,6),
  bzjje    NUMBER(18,2),
  bzjje_cm NUMBER(18,2),
  qydm     VARCHAR2(30 CHAR) not null,
  qymc     VARCHAR2(100 CHAR),
  khjldm   VARCHAR2(30 CHAR),
  khjlxm   VARCHAR2(100 CHAR)
)
;
  

create table CPMG.ODS_TMP_CARD
(
  accno     VARCHAR2(60 CHAR) not null,
  openzone  VARCHAR2(5 CHAR) not null,
  openbrno  VARCHAR2(5 CHAR),
  cino      VARCHAR2(30 CHAR),
  corperf   VARCHAR2(4 CHAR),
  mdcardn   VARCHAR2(60 CHAR),
  cardtype  VARCHAR2(4 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(4 CHAR) not null,
  ovrbal    NUMBER(18,2),
  ovrbal_cm NUMBER(18,2),
  thestage  VARCHAR2(4 CHAR)
)
;
  

create table CPMG.ODS_TMP_DA200011014
(
  dict_code VARCHAR2(7 CHAR) not null,
  dict_name VARCHAR2(100 CHAR)
)
;
  

create table CPMG.ODS_TMP_DA200011040
(
  dict_code             VARCHAR2(5 CHAR) not null,
  dict_name             VARCHAR2(100 CHAR),
  dict_order            NUMBER(3),
  dict_max              NUMBER(10),
  dict_min              NUMBER(10),
  dict_ratio            NUMBER(10,2),
  dict_code_small       VARCHAR2(5 CHAR),
  bail_limit_down_small NUMBER(5,2)
)
;
  

create table CPMG.ODS_TMP_DA200251012
(
  dict_code     VARCHAR2(5 CHAR) not null,
  dict_name     VARCHAR2(100 CHAR),
  dict_order    NUMBER(3),
  dict_max      NUMBER(10),
  dict_min      NUMBER(10),
  dict_ratio    NUMBER(10,2),
  dict_type     VARCHAR2(1 CHAR),
  dict_ratio_pj NUMBER(10,2)
)
;
  

create table CPMG.ODS_TMP_DA200251013
(
  dict_code VARCHAR2(5 CHAR) not null,
  dict_name VARCHAR2(100 CHAR)
)
;
  

create table CPMG.ODS_TMP_FRDK
(
  loanno       VARCHAR2(17 CHAR) not null,
  loansqno     VARCHAR2(3 CHAR) not null,
  accno        VARCHAR2(17 CHAR) not null,
  zoneno       VARCHAR2(5 CHAR) not null,
  actbrno      VARCHAR2(5 CHAR) not null,
  currtype     VARCHAR2(4 CHAR),
  subcode      VARCHAR2(8 CHAR),
  balance      NUMBER(18,2),
  exhmatud     VARCHAR2(8 CHAR),
  ta490461016  NUMBER(18,2),
  ta490461017  NUMBER(9,2),
  ta200011014  VARCHAR2(6 CHAR),
  ta200011040  VARCHAR2(6 CHAR),
  ta200011070  VARCHAR2(6 CHAR),
  ta340362013  NUMBER(13,6),
  ta200261037  VARCHAR2(6 CHAR),
  ta200251013  VARCHAR2(6 CHAR),
  ta200261004  VARCHAR2(17 CHAR),
  ta200261001  VARCHAR2(30 CHAR),
  ta200011005  VARCHAR2(60 CHAR),
  ta200261002  VARCHAR2(60 CHAR),
  ta200011034  VARCHAR2(30 CHAR),
  ta200251020  VARCHAR2(30 CHAR),
  valueday     VARCHAR2(8 CHAR),
  ta200251011  VARCHAR2(6 CHAR),
  ta200268004  VARCHAR2(12 CHAR),
  ta200011016  VARCHAR2(4 CHAR),
  credit_level VARCHAR2(6 CHAR),
  status       VARCHAR2(6 CHAR),
  etl_tx_dt    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.ODS_TMP_FRDK2
(
  zoneno         VARCHAR2(5 CHAR),
  phybrno        VARCHAR2(5 CHAR),
  loanno         VARCHAR2(17 CHAR),
  loansqno       VARCHAR2(3 CHAR),
  accno          VARCHAR2(17 CHAR),
  lnmemno        VARCHAR2(60 CHAR),
  restamount     NUMBER(18,2),
  currtype       VARCHAR2(3 CHAR),
  subcode        VARCHAR2(6 CHAR),
  buskind        VARCHAR2(5 CHAR),
  lnudflag       VARCHAR2(60 CHAR),
  loanway        VARCHAR2(6 CHAR),
  loandirt       VARCHAR2(12 CHAR),
  leveltwl       VARCHAR2(6 CHAR),
  levelfive      VARCHAR2(6 CHAR),
  avchcof        NUMBER(9,6),
  realboamt      NUMBER(18,2),
  surveyor       VARCHAR2(30 CHAR),
  loandate       VARCHAR2(8 CHAR),
  duedate        VARCHAR2(8 CHAR),
  custcode       VARCHAR2(30 CHAR),
  custname       VARCHAR2(60 CHAR),
  entrsize       VARCHAR2(5 CHAR),
  creditlevel    VARCHAR2(6 CHAR),
  groupcode      VARCHAR2(30 CHAR),
  groupname      VARCHAR2(60 CHAR),
  industrycode   VARCHAR2(6 CHAR),
  small_corp     VARCHAR2(1 CHAR) default 0 not null,
  dubil_std_rate NUMBER(9),
  rate_chg_mode  NUMBER(1),
  configrate     NUMBER(9),
  cust_exe_rate  NUMBER(20,9),
  income         NUMBER(18,2),
  pd             NUMBER(9,6),
  lgd            NUMBER(9,6),
  status         VARCHAR2(6 CHAR),
  zone_par       VARCHAR2(2),
  bnkrate        NUMBER(20,9)
)
;
  

create table CPMG.ODS_TMP_FTPSHIBOR
(
  dic_code    VARCHAR2(6 CHAR),
  dic_desc    VARCHAR2(60 CHAR),
  dic_subcode VARCHAR2(12 CHAR),
  dic_subdesc VARCHAR2(60 CHAR),
  dic_comment VARCHAR2(60 CHAR)
)
;
  

create table CPMG.ODS_TMP_GRDK
(
  accno             VARCHAR2(17 CHAR) not null,
  lncode            VARCHAR2(3 CHAR) not null,
  memno             VARCHAR2(5 CHAR) not null,
  lnno              VARCHAR2(3 CHAR) not null,
  zoneno            VARCHAR2(5 CHAR) not null,
  actbrno           VARCHAR2(5 CHAR) not null,
  subcode           VARCHAR2(8 CHAR),
  currtype          VARCHAR2(4 CHAR),
  balance           NUMBER(18,2),
  balance_cm        NUMBER(18,2),
  ta500261001       VARCHAR2(60 CHAR),
  ta500261003       VARCHAR2(30 CHAR),
  ta500261004       VARCHAR2(30 CHAR),
  ta500261008       VARCHAR2(4 CHAR),
  ta500261013       VARCHAR2(8 CHAR),
  ta500261016       VARCHAR2(8 CHAR),
  ta500261018       VARCHAR2(6 CHAR),
  ta500251012       NUMBER(18,2),
  ta500251012_cm    NUMBER(18,2),
  ta500251018       VARCHAR2(8 CHAR),
  ta500251014       NUMBER(9,6),
  ta500251013       NUMBER(6),
  ta500251097       VARCHAR2(30 CHAR),
  ta500251054       VARCHAR2(5 CHAR),
  ta290001004       VARCHAR2(100 CHAR),
  ta290001005       VARCHAR2(4 CHAR),
  ta290001006       VARCHAR2(60 CHAR),
  ta290008004       VARCHAR2(6 CHAR),
  ta290008009       VARCHAR2(2 CHAR),
  ta500271009       NUMBER(18,2),
  ta500271009_cm    NUMBER(18,2),
  ta500281016       NUMBER(18,2),
  ta500281016_cm    NUMBER(18,2),
  ta500291012       NUMBER(18,2),
  ta500291012_cm    NUMBER(18,2),
  ta500294008       NUMBER(18,2),
  ta500294008_cm    NUMBER(18,2),
  other_amount      NUMBER(18,2),
  other_amount_cm   NUMBER(18,2),
  invaild_amount    NUMBER(18,2),
  invaild_amount_cm NUMBER(18,2),
  xyamount          NUMBER(18,2),
  totalamount       NUMBER(18,2),
  status            VARCHAR2(6 CHAR),
  etl_tx_dt         VARCHAR2(8 CHAR)
)
;
  

create table CPMG.ODS_TMP_GRDK2
(
  zoneno          VARCHAR2(5 CHAR),
  phybrno         VARCHAR2(5 CHAR),
  pcaccno         VARCHAR2(17 CHAR),
  prodcode        VARCHAR2(13 CHAR),
  memno           VARCHAR2(5 CHAR),
  lnno            VARCHAR2(7 CHAR),
  ta500251054     VARCHAR2(5 CHAR),
  relatedno       VARCHAR2(60 CHAR),
  currtype        VARCHAR2(3 CHAR),
  subcode         VARCHAR2(6 CHAR),
  loantype        VARCHAR2(4 CHAR),
  levelfive       VARCHAR2(6 CHAR),
  lnamount        NUMBER(18,2),
  lnbalance       NUMBER(18,2),
  rate            NUMBER(20,9),
  loanterm        NUMBER(6),
  avchtype        VARCHAR2(50 CHAR),
  surveyor        VARCHAR2(30 CHAR),
  loandate        VARCHAR2(8 CHAR),
  duedate         VARCHAR2(8 CHAR),
  custid          VARCHAR2(30 CHAR),
  custcis         VARCHAR2(30 CHAR),
  custname        VARCHAR2(60 CHAR),
  regtype         VARCHAR2(4 CHAR),
  regno           VARCHAR2(60 CHAR),
  creditlevel     VARCHAR2(6 CHAR),
  ecusflag        VARCHAR2(2 CHAR),
  avchamt         NUMBER(18,2),
  mgamt           NUMBER(18,2),
  mpamt           NUMBER(18,2),
  assureamt       NUMBER(18,2),
  otheramt        NUMBER(18,2),
  invalidamt      NUMBER(18,2),
  creditamt       NUMBER(18,2),
  totalamt        NUMBER(18,2),
  status          VARCHAR2(6 CHAR),
  zoneno_for_part VARCHAR2(2) not null,
  fbflag          VARCHAR2(9 CHAR),
  pd_value        NUMBER(12,2),
  lgd_value       NUMBER(12,2),
  r_value         NUMBER(12,2),
  k_value         NUMBER(12,2),
  deflt_flag      VARCHAR2(9 CHAR),
  ovdue_cnt       VARCHAR2(11 CHAR)
)
;
  

 

create table CPMG.ODS_TMP_HFCAEXOA
(
  zoneno   VARCHAR2(5 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  currsmbl VARCHAR2(5 CHAR),
  uscvrate NUMBER(17),
  lxsrate  NUMBER(17),
  yearrate NUMBER(17)
)
;
  

 

create table CPMG.ODS_TMP_HNFPASBJ
(
  zoneno   VARCHAR2(5 CHAR) not null,
  subcode  VARCHAR2(7 CHAR) not null,
  subnatue VARCHAR2(1 CHAR),
  subclass VARCHAR2(9 CHAR),
  subno    VARCHAR2(7 CHAR),
  notes    VARCHAR2(80 CHAR),
  intdiff  VARCHAR2(3 CHAR),
  appno    VARCHAR2(3 CHAR)
)
;
  

create table CPMG.ODS_TMP_NFDLZON
(
  zoneno  VARCHAR2(5 CHAR),
  rsflhhh VARCHAR2(5 CHAR),
  hsysid  VARCHAR2(4 CHAR),
  plrflag VARCHAR2(1 CHAR),
  plaarno VARCHAR2(5 CHAR),
  plstime VARCHAR2(8 CHAR),
  pletime VARCHAR2(8 CHAR),
  notes   VARCHAR2(40 CHAR),
  cntmode VARCHAR2(1 CHAR),
  status  VARCHAR2(1 CHAR),
  lstmodd VARCHAR2(10 CHAR),
  bakdec1 VARCHAR2(17 CHAR),
  bakchar VARCHAR2(20 CHAR),
  filler  VARCHAR2(875 CHAR)
)
;
  

create table CPMG.ODS_TMP_NFGNLED
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  currtype VARCHAR2(3 CHAR) not null,
  subcode  VARCHAR2(7 CHAR) not null,
  subclass VARCHAR2(9 CHAR),
  subnatue VARCHAR2(1 CHAR),
  lydrbal  NUMBER(18),
  lycrbal  NUMBER(18),
  tydramt  NUMBER(18),
  tycramt  NUMBER(18),
  tydrino  NUMBER(9),
  tycrino  NUMBER(9),
  lmdrbal  NUMBER(18),
  lmcrbal  NUMBER(18),
  tmdramt  NUMBER(18),
  tmcramt  NUMBER(18),
  tmdrino  NUMBER(9),
  tmcrino  NUMBER(9),
  lddrbal  NUMBER(18),
  ldcrbal  NUMBER(18),
  tddramt  NUMBER(18),
  tdcramt  NUMBER(18),
  tddrbal  NUMBER(18),
  tdcrbal  NUMBER(18),
  tddrino  NUMBER(9),
  tdcrino  NUMBER(9),
  dracm    NUMBER(18),
  cracm    NUMBER(18),
  acccont  NUMBER(9)
)
;
  

create table CPMG.ODS_TMP_NFPABRP
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  mbrno    VARCHAR2(5 CHAR) not null,
  brtype   VARCHAR2(1 CHAR),
  notes    VARCHAR2(40 CHAR),
  status   VARCHAR2(1 CHAR),
  rsfflag  VARCHAR2(1 CHAR),
  rsflhhh  VARCHAR2(5 CHAR),
  rsffqh   VARCHAR2(3 CHAR),
  rsflhhbs VARCHAR2(7 CHAR),
  rsflhhm  VARCHAR2(60 CHAR),
  rmbrno   VARCHAR2(5 CHAR),
  address  VARCHAR2(50 CHAR),
  zip      VARCHAR2(6 CHAR),
  phone1   VARCHAR2(22 CHAR),
  shortnm  VARCHAR2(10 CHAR),
  actbrno  VARCHAR2(5 CHAR),
  brstatus VARCHAR2(1 CHAR),
  lstmodd  VARCHAR2(10 CHAR),
  bakdec1  VARCHAR2(17 CHAR),
  bakchar  VARCHAR2(20 CHAR)
)
;
  

create table CPMG.ODS_TMP_PJTX
(
  zoneno       VARCHAR2(5 CHAR),
  actbrno      VARCHAR2(5 CHAR),
  innerno      VARCHAR2(60 CHAR) not null,
  billno       VARCHAR2(30 CHAR),
  subcode      VARCHAR2(6 CHAR),
  billamt      NUMBER(18,2),
  billtype     VARCHAR2(4 CHAR),
  buytype      VARCHAR2(4 CHAR),
  firsttype    VARCHAR2(4 CHAR),
  drawerbank   VARCHAR2(30 CHAR),
  holder       VARCHAR2(30 CHAR),
  bearer_name  VARCHAR2(60 CHAR),
  holdertype   VARCHAR2(4 CHAR),
  croup_num    VARCHAR2(30 CHAR),
  croup_name   VARCHAR2(60 CHAR),
  creditlevel  VARCHAR2(6 CHAR),
  managerno    VARCHAR2(30 CHAR),
  levelfive    VARCHAR2(6 CHAR),
  discountdate VARCHAR2(8 CHAR),
  realmaturity VARCHAR2(8 CHAR),
  retmaturity  VARCHAR2(8 CHAR),
  capcost      NUMBER(17,2),
  intincome    NUMBER(13,2)
)
;
  

create table CPMG.ODS_TMP_STAT_NFWKPRF
(
  data_date VARCHAR2(8) not null,
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  bankflag  VARCHAR2(1 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  subno     VARCHAR2(7 CHAR) not null,
  amount    NUMBER(22,2)
)
;
  

 

create table CPMG.ODS_TMP_ZJZJRJYE
(
  data_date    VARCHAR2(8 CHAR),
  zoneno       VARCHAR2(5 CHAR),
  brno         VARCHAR2(5 CHAR),
  branch_level VARCHAR2(1 CHAR),
  currency     VARCHAR2(3 CHAR),
  term         VARCHAR2(3 CHAR),
  ad_balance   NUMBER(32,10)
)
;
  

create table CPMG.OLAP_CONTROL
(
  data_date VARCHAR2(8) not null
)
;
  

create table CPMG.OLAP_DIM_ASSET_QUALITY
(
  asset_quality_id   VARCHAR2(2 CHAR) not null,
  asset_quality_name VARCHAR2(4 CHAR) not null,
  start_date         VARCHAR2(8),
  end_date           VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_ASSURANCE
(
  assurance_id   VARCHAR2(10 CHAR) not null,
  assurance_name VARCHAR2(40 CHAR) not null,
  start_date     VARCHAR2(8),
  end_date       VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_BUSINESS
(
  business_id   VARCHAR2(10 CHAR) not null,
  business_name VARCHAR2(50 CHAR) not null,
  start_date    VARCHAR2(8),
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_CREDIT_LVL
(
  credit_lvl_id   VARCHAR2(5 CHAR) not null,
  credit_lvl_name VARCHAR2(100 CHAR) not null,
  start_date      VARCHAR2(8),
  end_date        VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_CURRENCY
(
  currency_id   VARCHAR2(3 CHAR) not null,
  currency_name VARCHAR2(14 CHAR) not null,
  start_date    VARCHAR2(8),
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_DEADLINE
(
  deadline_id   VARCHAR2(6 CHAR) not null,
  deadline_name VARCHAR2(100 CHAR) not null,
  start_date    VARCHAR2(8),
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_IS_SMALL
(
  is_small_id   VARCHAR2(1 CHAR) not null,
  is_small_name VARCHAR2(2 CHAR) not null,
  start_date    VARCHAR2(8),
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_LGD
(
  lgd_id     VARCHAR2(2 CHAR) not null,
  lgd_name   VARCHAR2(10 CHAR) not null,
  start_date VARCHAR2(8),
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_PD
(
  pd_id      VARCHAR2(2 CHAR) not null,
  pd_name    VARCHAR2(10 CHAR) not null,
  start_date VARCHAR2(8),
  end_date   VARCHAR2(8 CHAR)
)
;
  

 

create table CPMG.OLAP_DIM_PRODUCT
(
  product_id   VARCHAR2(10 CHAR) not null,
  product_name VARCHAR2(100 CHAR) not null,
  start_date   VARCHAR2(8),
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_DIM_ZONE
(
  zone_id    VARCHAR2(8 CHAR) not null,
  zone_name  VARCHAR2(60 CHAR) not null,
  start_date VARCHAR2(8),
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.OLAP_FACT_ALL
(
  data_date     VARCHAR2(8) not null,
  branch1       VARCHAR2(8 CHAR),
  branch2       VARCHAR2(8 CHAR),
  currency      VARCHAR2(3 CHAR),
  credit_lvl    VARCHAR2(5 CHAR),
  assurance     VARCHAR2(10 CHAR),
  asset_quality VARCHAR2(2 CHAR),
  business      VARCHAR2(10 CHAR),
  product1      VARCHAR2(10 CHAR),
  product2      VARCHAR2(10 CHAR),
  product3      VARCHAR2(10 CHAR),
  balance       NUMBER(18,2),
  ec            NUMBER(18,2)
)
;
  

create table CPMG.OLAP_FACT_FAREN
(
  data_date     VARCHAR2(8) not null,
  branch1       VARCHAR2(8 CHAR),
  branch2       VARCHAR2(8 CHAR),
  currency      VARCHAR2(3 CHAR),
  credit_lvl    VARCHAR2(5 CHAR),
  assurance     VARCHAR2(10 CHAR),
  asset_quality VARCHAR2(2 CHAR),
  deadline      VARCHAR2(6 CHAR),
  business      VARCHAR2(10 CHAR),
  product1      VARCHAR2(10 CHAR),
  product2      VARCHAR2(10 CHAR),
  product3      VARCHAR2(10 CHAR),
  pd            VARCHAR2(2 CHAR),
  lgd           VARCHAR2(2 CHAR),
  is_small      VARCHAR2(1 CHAR),
  balance       NUMBER(18,2),
  ec            NUMBER(18,2)
)
;
  

create table CPMG.PRM_CHECK
(
  check_id   VARCHAR2(10 CHAR) not null,
  prm_table  VARCHAR2(50 CHAR),
  modi_user  VARCHAR2(10 CHAR),
  app_date   VARCHAR2(8 CHAR),
  check_user VARCHAR2(10 CHAR),
  check_date VARCHAR2(8 CHAR),
  check_view VARCHAR2(200 CHAR),
  status     VARCHAR2(1 CHAR)
)
;
  

create table CPMG.PRM_CHECK_SUBCODE
(
  subcode VARCHAR2(8 CHAR),
  zf_flag VARCHAR2(1 CHAR)
)
;
  

create table CPMG.PRM_CS031
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_type VARCHAR2(1 CHAR),
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_app  NUMBER(2),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS032
(
  dbfs_id    VARCHAR2(5 CHAR) not null,
  dbfs_lb    VARCHAR2(50 CHAR),
  load_type  VARCHAR2(5 CHAR) not null,
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS035
(
  dbdc_id     VARCHAR2(10 CHAR) not null,
  dbdc_name   VARCHAR2(40 CHAR),
  left_value  NUMBER(5,2),
  right_value NUMBER(5,2),
  left_in     VARCHAR2(1 CHAR),
  right_in    VARCHAR2(1 CHAR),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS036
(
  qxdc_id     VARCHAR2(10 CHAR) not null,
  qxdc_name   VARCHAR2(40 CHAR),
  left_value  NUMBER(9),
  right_value NUMBER(9),
  left_in     VARCHAR2(1 CHAR),
  right_in    VARCHAR2(1 CHAR),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS037
(
  fl12_id    VARCHAR2(5 CHAR) not null,
  fl12_name  VARCHAR2(50 CHAR),
  bbprecent  NUMBER(5,2),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS042
(
  fl5_id     VARCHAR2(5 CHAR) not null,
  fl5_name   VARCHAR2(50 CHAR),
  bbprecent  NUMBER(5,2),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS044
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_type VARCHAR2(1 CHAR),
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_app  NUMBER(2),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS05A
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_type VARCHAR2(1 CHAR),
  subject_app  NUMBER(2),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS05B
(
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR),
  product_id   VARCHAR2(10 CHAR) not null,
  subject_type VARCHAR2(10 CHAR),
  subject_code VARCHAR2(7 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_app  NUMBER(2)
)
;
  

create table CPMG.PRM_CS05C
(
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR),
  product_id   VARCHAR2(10 CHAR) not null,
  subject_type VARCHAR2(10 CHAR),
  subject_code VARCHAR2(7 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_app  NUMBER(2),
  data_type    VARCHAR2(1 CHAR)
)
;
  

create table CPMG.PRM_CS061
(
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR),
  product_id   VARCHAR2(10 CHAR) not null,
  subject_type VARCHAR2(10 CHAR),
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  dmlx         VARCHAR2(1 CHAR) not null,
  subject_app  NUMBER(2)
)
;
  

create table CPMG.PRM_CS071
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  subject_app  NUMBER(2),
  subject_name VARCHAR2(50 CHAR),
  subject_type VARCHAR2(1 CHAR),
  buytype      VARCHAR2(1 CHAR) not null,
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS081
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_app  NUMBER(2),
  subject_type VARCHAR2(1 CHAR),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CS10
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  subject_app  NUMBER(2),
  subject_type VARCHAR2(1 CHAR),
  subject_name VARCHAR2(50 CHAR),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR),
  flag         VARCHAR2(1 CHAR)
)
;
  

create table CPMG.PRM_CSFR5
(
  seg_seq    VARCHAR2(10 CHAR) not null,
  lower      NUMBER(9,6),
  is_default VARCHAR2(1 CHAR),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  seg_name   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CSPJ2
(
  qxdc_id     VARCHAR2(10 CHAR) not null,
  qxdc_name   VARCHAR2(40 CHAR),
  left_value  NUMBER(5),
  right_value NUMBER(5),
  left_in     VARCHAR2(1 CHAR),
  right_in    VARCHAR2(1 CHAR),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_CURRENCY_RATE
(
  currency VARCHAR2(3 CHAR) not null,
  uscvrate NUMBER(17,6)
)
;
  

create table CPMG.PRM_DA200011014
(
  dict_code  VARCHAR2(7 CHAR) not null,
  dict_name  VARCHAR2(100 CHAR),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_DA200011040
(
  dict_code             VARCHAR2(5 CHAR) not null,
  dict_name             VARCHAR2(100 CHAR),
  dict_order            NUMBER(3),
  dict_max              NUMBER(10),
  dict_min              NUMBER(10),
  dict_ratio            NUMBER(10,2),
  dict_code_small       VARCHAR2(5 CHAR),
  bail_limit_down_small NUMBER(5,2),
  start_date            VARCHAR2(8) not null,
  end_date              VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_DA200251012
(
  dict_code     VARCHAR2(5 CHAR) not null,
  dict_name     VARCHAR2(100 CHAR),
  dict_order    NUMBER(3),
  dict_max      NUMBER(10),
  dict_min      NUMBER(10),
  dict_ratio    NUMBER(10,2),
  dict_type     VARCHAR2(1 CHAR),
  dict_ratio_pj NUMBER(10,2),
  start_date    VARCHAR2(8) not null,
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_DA200251013
(
  dict_code  VARCHAR2(5 CHAR) not null,
  dict_name  VARCHAR2(100 CHAR),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_EXCHANGE_RATE
(
  zone_no    VARCHAR2(5 CHAR) not null,
  currency   VARCHAR2(3 CHAR) not null,
  uscvrate   NUMBER(17,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  currsmbl   VARCHAR2(6 CHAR)
)
;
  

create table CPMG.PRM_FTPSHIBOR
(
  dic_code    VARCHAR2(6 CHAR) not null,
  dic_desc    VARCHAR2(60 CHAR),
  dic_subcode VARCHAR2(12 CHAR) not null,
  dic_subdesc VARCHAR2(60 CHAR),
  dic_comment VARCHAR2(60 CHAR)
)
;
  

create table CPMG.PRM_HYDM
(
  hydm       VARCHAR2(7 CHAR) not null,
  hymc       VARCHAR2(100 CHAR) not null,
  hydl       VARCHAR2(50 CHAR) not null,
  hyflag     VARCHAR2(10 CHAR) not null,
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR) not null
)
;
  

create table CPMG.PRM_IRA_ORGAN
(
  zone_no          VARCHAR2(5 CHAR) not null,
  br_no            VARCHAR2(5 CHAR) not null,
  act_br_no        VARCHAR2(5 CHAR),
  bank_flag        VARCHAR2(1 CHAR) not null,
  organ_name       VARCHAR2(60 CHAR),
  belong_zoneno    VARCHAR2(5 CHAR),
  belong_brno      VARCHAR2(5 CHAR),
  belong_bank_flag VARCHAR2(1 CHAR),
  br_type          VARCHAR2(1 CHAR),
  start_date       VARCHAR2(8) not null,
  end_date         VARCHAR2(8 CHAR),
  is_sales         VARCHAR2(1 CHAR)
)
;
  

create table CPMG.PRM_MA380001
(
  dict_code  VARCHAR2(5 CHAR) not null,
  dict_name  VARCHAR2(100 CHAR),
  start_date VARCHAR2(10) not null,
  end_date   VARCHAR2(10 CHAR)
)
;
  

create table CPMG.PRM_ORGANMAP
(
  zoneno      VARCHAR2(5 CHAR),
  brno        VARCHAR2(5 CHAR),
  bank_flag   VARCHAR2(1 CHAR),
  zoneno_m    VARCHAR2(5 CHAR),
  brno_m      VARCHAR2(5 CHAR),
  bank_flag_m VARCHAR2(1 CHAR)
)
;
  

create table CPMG.PRM_PRODUCT
(
  product_id    VARCHAR2(10 CHAR) not null,
  product_level VARCHAR2(1 CHAR),
  product_name  VARCHAR2(100 CHAR),
  parent_id     VARCHAR2(10 CHAR),
  start_date    VARCHAR2(8) not null,
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_PRODUCT_SUBJECT
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  product_name VARCHAR2(50 CHAR) not null,
  pro_name     VARCHAR2(100 CHAR)
)
;
  

create table CPMG.PRM_SALE
(
  zone_no   VARCHAR2(5 CHAR) not null,
  br_no     VARCHAR2(5 CHAR) not null,
  bank_flag VARCHAR2(1 CHAR) not null
)
;
  

create table CPMG.PRM_SG_BOND_D
(
  organ    VARCHAR2(10 CHAR) not null,
  statcode VARCHAR2(10 CHAR) not null,
  balance  NUMBER(20,2),
  datadate VARCHAR2(10 CHAR) not null
)
;
  

create table CPMG.PRM_SG_BOND_RMB
(
  organ    VARCHAR2(10 CHAR) not null,
  statcode VARCHAR2(10 CHAR) not null,
  balance  NUMBER(20,2),
  datadate VARCHAR2(10 CHAR) not null
)
;
  

create table CPMG.PRM_SUBCODE_STATCODE
(
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  subcode    VARCHAR2(4 CHAR) not null,
  statcode   VARCHAR2(5 CHAR)
)
;
  

create table CPMG.PRM_SUBJECT
(
  zoneno     VARCHAR2(5 CHAR) not null,
  subcode    VARCHAR2(8 CHAR) not null,
  subnatue   VARCHAR2(1 CHAR),
  subclass   VARCHAR2(10 CHAR),
  subno      VARCHAR2(8 CHAR),
  notes      VARCHAR2(80 CHAR),
  intdiff    VARCHAR2(3 CHAR),
  appno      VARCHAR2(3 CHAR),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS031
(
  product_id VARCHAR2(10 CHAR) not null,
  xyvalue    VARCHAR2(5 CHAR) not null,
  dbdc_id    VARCHAR2(10 CHAR) not null,
  xs_value   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS032
(
  fl12_id    VARCHAR2(5 CHAR) not null,
  xs_value   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS034
(
  lineno     NUMBER(8),
  field_code VARCHAR2(8 CHAR) not null,
  field_name VARCHAR2(100 CHAR),
  area_code  VARCHAR2(5 CHAR) not null,
  xs_value   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS041
(
  product_id VARCHAR2(10 CHAR) not null,
  bzxs_value NUMBER(9,6),
  dyxs_value NUMBER(9,6),
  zyxs_value NUMBER(9,6),
  bdxs_value NUMBER(9,6),
  qtxs_value NUMBER(9,6),
  wxxs_value NUMBER(9,6),
  xyxs_value NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS042
(
  qclass     VARCHAR2(5 CHAR) not null,
  quotiety   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS043
(
  qxdc_id     VARCHAR2(10 CHAR) not null,
  dbdc_name   VARCHAR2(40 CHAR),
  left_value  NUMBER(9),
  right_value NUMBER(9),
  left_in     VARCHAR2(1 CHAR),
  right_in    VARCHAR2(1 CHAR),
  xs_value    NUMBER(9,6),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS051
(
  product_name3 VARCHAR2(10 CHAR) not null,
  xs_value      NUMBER(9,6),
  start_date    VARCHAR2(8) not null,
  end_date      VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS053
(
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  product_id VARCHAR2(10 CHAR) not null,
  fxxs       NUMBER(9,6)
)
;
  

create table CPMG.PRM_XS061
(
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  product_id VARCHAR2(10 CHAR) not null,
  pzxs       NUMBER(9,6)
)
;
  

create table CPMG.PRM_XS071
(
  product_id VARCHAR2(10 CHAR) not null,
  qxdc_id    VARCHAR2(10 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS073
(
  qclass     VARCHAR2(8 CHAR) not null,
  quotiety   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS074
(
  qclass     VARCHAR2(5 CHAR) not null,
  bquotiety  NUMBER(9,6),
  equotiety  NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS081
(
  thproduct  VARCHAR2(10 CHAR) not null,
  quotiety   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XS10
(
  thproduct  VARCHAR2(10 CHAR) not null,
  quotiety   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSCZ2
(
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSFR1
(
  product_id  VARCHAR2(10 CHAR) not null,
  credit_code VARCHAR2(5 CHAR) not null,
  lgd_level   VARCHAR2(5 CHAR) not null,
  par_coef    NUMBER(9,6),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSFR2
(
  product_id  VARCHAR2(10 CHAR),
  ywpz_name   VARCHAR2(50 CHAR),
  subject_all VARCHAR2(200 CHAR) not null,
  ywpz_all    VARCHAR2(200 CHAR) not null,
  app_type    VARCHAR2(1 CHAR),
  par_coef    NUMBER(9,6),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR),
  seg_seq     VARCHAR2(5 CHAR)
)
;
  

create table CPMG.PRM_XSFR2_TMP
(
  product_id   VARCHAR2(10 CHAR),
  subject_code VARCHAR2(200 CHAR) not null,
  ywpz_code    VARCHAR2(200 CHAR) not null,
  app_type     VARCHAR2(1 CHAR),
  par_coef     NUMBER(9,6)
)
;
  

create table CPMG.PRM_XSFR3
(
  qxdc_id     VARCHAR2(10 CHAR) not null,
  credit_code VARCHAR2(5 CHAR) not null,
  par_coef    NUMBER(9,6),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSFRA
(
  product_id VARCHAR2(10 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSXJ1
(
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSXJ2
(
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.PRM_XSZJ1
(
  product_id VARCHAR2(10 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
)
;
  

create table CPMG.RMB_CURRENCY_RATE
(
  currency  VARCHAR2(3 CHAR) not null,
  rmbcvrate NUMBER(17,6),
  data_date VARCHAR2(8 CHAR) not null
)
;
  

create table CPMG.STG_BAK_FRDK
(
  zoneno        VARCHAR2(5 CHAR),
  phybrno       VARCHAR2(5 CHAR),
  loanno        VARCHAR2(17 CHAR) not null,
  loansqno      VARCHAR2(3 CHAR) not null,
  accno         VARCHAR2(17 CHAR) not null,
  lnmemno       VARCHAR2(60 CHAR),
  restamount    NUMBER(18,2),
  restamount_cm NUMBER(18,2),
  currtype      VARCHAR2(3 CHAR),
  subcode       VARCHAR2(6 CHAR),
  buskind       VARCHAR2(5 CHAR),
  lnudflag      VARCHAR2(60 CHAR),
  loanway       VARCHAR2(6 CHAR),
  loandirt      VARCHAR2(12 CHAR),
  leveltwl      VARCHAR2(6 CHAR),
  levelfive     VARCHAR2(6 CHAR),
  avchcof       NUMBER(9,6),
  realboamt     NUMBER(18,2),
  realboamt_cm  NUMBER(18,2),
  surveyor      VARCHAR2(30 CHAR),
  loandate      VARCHAR2(8 CHAR),
  duedate       VARCHAR2(8 CHAR),
  custcode      VARCHAR2(30 CHAR),
  custname      VARCHAR2(60 CHAR),
  entrsize      VARCHAR2(5 CHAR),
  creditlevel   VARCHAR2(6 CHAR),
  groupcode     VARCHAR2(30 CHAR),
  groupname     VARCHAR2(60 CHAR),
  industrycode  VARCHAR2(7 CHAR),
  configrate    NUMBER(9),
  income        NUMBER(18,2),
  pd            NUMBER(16,6),
  lgd           NUMBER(9,6),
  status        VARCHAR2(6 CHAR),
  modify_date   VARCHAR2(8 CHAR),
  data_date     VARCHAR2(8) not null,
  product_id    VARCHAR2(10 CHAR),
  smallcorp     VARCHAR2(1 CHAR),
  dubilstdrate  NUMBER(9),
  ratechgmode   NUMBER(1),
  zone_par      VARCHAR2(2),
  bnkrate       NUMBER(20,6),
  cust_exe_rate NUMBER(20,9)
)
;
  

create table CPMG.STG_BAK_GRDK
(
  accno             VARCHAR2(17 CHAR) not null,
  lncode            VARCHAR2(13 CHAR) not null,
  memno             VARCHAR2(5 CHAR) not null,
  lnno              VARCHAR2(7 CHAR) not null,
  zoneno            VARCHAR2(5 CHAR),
  actbrno           VARCHAR2(5 CHAR),
  subcode           VARCHAR2(8 CHAR),
  currtype          VARCHAR2(4 CHAR),
  balance           NUMBER(20,2),
  bacm              NUMBER(24,2),
  ta500261001       VARCHAR2(60 CHAR),
  ta500261003       VARCHAR2(30 CHAR),
  ta500261004       VARCHAR2(30 CHAR),
  ta500261008       VARCHAR2(4 CHAR),
  ta500261013       VARCHAR2(8 CHAR),
  ta500261014       VARCHAR2(8 CHAR),
  ta500261018       VARCHAR2(6 CHAR),
  ta500261016       VARCHAR2(8 CHAR),
  ta500251012       NUMBER(18,2),
  avg_ta500251012   NUMBER(18,2),
  ta500251018       VARCHAR2(50 CHAR),
  ta500251014       NUMBER(9,6),
  ta500251013       NUMBER(6),
  ta500251097       VARCHAR2(30 CHAR),
  ta500251054       VARCHAR2(5 CHAR),
  ta500270002       VARCHAR2(300 CHAR),
  ta290001004       VARCHAR2(100 CHAR),
  ta290001005       VARCHAR2(4 CHAR),
  ta290001006       VARCHAR2(60 CHAR),
  ta290008004       VARCHAR2(100 CHAR),
  ta290008009       VARCHAR2(60 CHAR),
  ta500271009       NUMBER(18,2),
  ta500271009_cm    NUMBER(20,2),
  ta500281016       NUMBER(18,2),
  ta500281016_cm    NUMBER(20,2),
  ta500291012       NUMBER(18,2),
  ta500291012_cm    NUMBER(20,2),
  ta500294008       NUMBER(18,2),
  ta500294008_cm    NUMBER(20,2),
  other_amount      NUMBER(18,2),
  other_amount_cm   NUMBER(20,2),
  invaild_amount    NUMBER(20,2),
  invaild_amount_cm NUMBER(24,2),
  xyamount          NUMBER(20,2),
  totalamount       NUMBER(20,2),
  modify_date       VARCHAR2(8 CHAR),
  data_date         VARCHAR2(8) not null,
  status            VARCHAR2(6 CHAR),
  etl_tx_dt         VARCHAR2(8 CHAR),
  xyamount_cm       NUMBER(20,2),
  totalamount_cm    NUMBER(20,2),
  product_id        VARCHAR2(10 CHAR),
  zoneno_for_part   VARCHAR2(2) not null
)
;
  

create table CPMG.STG_BAK_PJTX
(
  zoneno       VARCHAR2(5 CHAR),
  actbrno      VARCHAR2(5 CHAR),
  innerno      VARCHAR2(60 CHAR) not null,
  billno       VARCHAR2(30 CHAR),
  subcode      VARCHAR2(6 CHAR),
  billamt      NUMBER(18,2),
  billamt_cm   NUMBER(18,2),
  billtype     VARCHAR2(4 CHAR),
  buytype      VARCHAR2(4 CHAR),
  firsttype    VARCHAR2(4 CHAR),
  drawerbank   VARCHAR2(30 CHAR),
  holder       VARCHAR2(30 CHAR),
  holdertype   VARCHAR2(4 CHAR),
  creditlevel  VARCHAR2(6 CHAR),
  managerno    VARCHAR2(30 CHAR),
  levelfive    VARCHAR2(6 CHAR),
  discountdate VARCHAR2(8 CHAR),
  realmaturity VARCHAR2(8 CHAR),
  retmaturity  VARCHAR2(8 CHAR),
  capcost      NUMBER(17,2),
  intincome    NUMBER(13,2),
  modify_date  VARCHAR2(8 CHAR),
  data_date    VARCHAR2(8) not null,
  qxdc_id      VARCHAR2(10 CHAR),
  bearer_name  VARCHAR2(60 CHAR),
  croup_num    VARCHAR2(30 CHAR),
  croup_name   VARCHAR2(60 CHAR)
)
;
  

create table CPMG.STG_DAT_FRDK
(
  zoneno        VARCHAR2(5 CHAR),
  phybrno       VARCHAR2(5 CHAR),
  loanno        VARCHAR2(17 CHAR) not null,
  loansqno      VARCHAR2(3 CHAR) not null,
  accno         VARCHAR2(17 CHAR) not null,
  lnmemno       VARCHAR2(60 CHAR),
  restamount    NUMBER(18,2),
  restamount_cm NUMBER(18,2),
  currtype      VARCHAR2(3 CHAR),
  subcode       VARCHAR2(6 CHAR),
  buskind       VARCHAR2(5 CHAR),
  lnudflag      VARCHAR2(60 CHAR),
  loanway       VARCHAR2(6 CHAR),
  loandirt      VARCHAR2(12 CHAR),
  leveltwl      VARCHAR2(6 CHAR),
  levelfive     VARCHAR2(6 CHAR),
  avchcof       NUMBER(9,6),
  realboamt     NUMBER(18,2),
  realboamt_cm  NUMBER(18,2),
  surveyor      VARCHAR2(30 CHAR),
  loandate      VARCHAR2(8 CHAR),
  duedate       VARCHAR2(8 CHAR),
  custcode      VARCHAR2(30 CHAR),
  custname      VARCHAR2(60 CHAR),
  entrsize      VARCHAR2(5 CHAR),
  creditlevel   VARCHAR2(6 CHAR),
  groupcode     VARCHAR2(30 CHAR),
  groupname     VARCHAR2(60 CHAR),
  industrycode  VARCHAR2(7 CHAR),
  configrate    NUMBER(9),
  income        NUMBER(18,2),
  pd            NUMBER(17,6),
  lgd           NUMBER(9,6),
  status        VARCHAR2(6 CHAR),
  modify_date   VARCHAR2(8 CHAR),
  product_id    VARCHAR2(10 CHAR),
  smallcorp     VARCHAR2(1 CHAR),
  dubilstdrate  NUMBER(9),
  ratechgmode   NUMBER(1),
  zone_par      VARCHAR2(2),
  bnkrate       NUMBER(20,6),
  cust_exe_rate NUMBER(20,9)
)
;
  

create table CPMG.STG_DAT_GRDK
(
  accno             VARCHAR2(17 CHAR) not null,
  lncode            VARCHAR2(13 CHAR) not null,
  memno             VARCHAR2(5 CHAR) not null,
  lnno              VARCHAR2(7 CHAR) not null,
  zoneno            VARCHAR2(5 CHAR),
  actbrno           VARCHAR2(5 CHAR),
  subcode           VARCHAR2(8 CHAR),
  currtype          VARCHAR2(4 CHAR),
  balance           NUMBER(20,2),
  bacm              NUMBER(24,2),
  ta500261001       VARCHAR2(60 CHAR),
  ta500261003       VARCHAR2(30 CHAR),
  ta500261004       VARCHAR2(30 CHAR),
  ta500261008       VARCHAR2(4 CHAR),
  ta500261013       VARCHAR2(8 CHAR),
  ta500261018       VARCHAR2(6 CHAR),
  ta500261016       VARCHAR2(8 CHAR),
  ta500251012       NUMBER(18,2),
  avg_ta500251012   NUMBER(18,2),
  ta500251018       VARCHAR2(50 CHAR),
  ta500251014       NUMBER(9,6),
  ta500251013       NUMBER(6),
  ta500251097       VARCHAR2(30 CHAR),
  ta500251054       VARCHAR2(5 CHAR),
  ta290001004       VARCHAR2(100 CHAR),
  ta290001005       VARCHAR2(4 CHAR),
  ta290001006       VARCHAR2(60 CHAR),
  ta290008004       VARCHAR2(100 CHAR),
  ta290008009       VARCHAR2(60 CHAR),
  ta500271009       NUMBER(18,2),
  ta500271009_cm    NUMBER(20,2),
  ta500281016       NUMBER(18,2),
  ta500281016_cm    NUMBER(20,2),
  ta500291012       NUMBER(18,2),
  ta500291012_cm    NUMBER(20,2),
  ta500294008       NUMBER(18,2),
  ta500294008_cm    NUMBER(20,2),
  other_amount      NUMBER(18,2),
  other_amount_cm   NUMBER(20,2),
  invaild_amount    NUMBER(20,2),
  invaild_amount_cm NUMBER(24,2),
  xyamount          NUMBER(20,2),
  totalamount       NUMBER(20,2),
  modify_date       VARCHAR2(8 CHAR),
  status            VARCHAR2(6 CHAR),
  etl_tx_dt         VARCHAR2(8 CHAR),
  xyamount_cm       NUMBER(20,2),
  totalamount_cm    NUMBER(20,2),
  product_id        VARCHAR2(10 CHAR),
  zoneno_for_part   VARCHAR2(2)
)
;
  

create table CPMG.STG_DAT_NFGNLED
(
  zoneno    VARCHAR2(5 CHAR) not null,
  brno      VARCHAR2(5 CHAR) not null,
  bankflag  VARCHAR2(1 CHAR) not null,
  currtype  VARCHAR2(3 CHAR) not null,
  subcode   VARCHAR2(7 CHAR) not null,
  tddrbal   NUMBER(24,2),
  tdcrbal   NUMBER(24,2),
  sumdrbal  NUMBER(24,2),
  sumcrbal  NUMBER(24,2),
  data_date VARCHAR2(8) not null
)
;
  

create table CPMG.STG_DAT_PJTX
(
  zoneno       VARCHAR2(5 CHAR),
  actbrno      VARCHAR2(5 CHAR),
  innerno      VARCHAR2(60 CHAR) not null,
  billno       VARCHAR2(30 CHAR),
  subcode      VARCHAR2(6 CHAR),
  billamt      NUMBER(18,2),
  billamt_cm   NUMBER(18,2),
  billtype     VARCHAR2(4 CHAR),
  buytype      VARCHAR2(4 CHAR),
  firsttype    VARCHAR2(4 CHAR),
  drawerbank   VARCHAR2(30 CHAR),
  holder       VARCHAR2(30 CHAR),
  holdertype   VARCHAR2(4 CHAR),
  creditlevel  VARCHAR2(6 CHAR),
  managerno    VARCHAR2(30 CHAR),
  levelfive    VARCHAR2(6 CHAR),
  discountdate VARCHAR2(8 CHAR),
  realmaturity VARCHAR2(8 CHAR),
  retmaturity  VARCHAR2(8 CHAR),
  capcost      NUMBER(17,2),
  intincome    NUMBER(13,2),
  modify_date  VARCHAR2(8 CHAR),
  qxdc_id      VARCHAR2(10 CHAR),
  bearer_name  VARCHAR2(60 CHAR),
  croup_num    VARCHAR2(30 CHAR),
  croup_name   VARCHAR2(60 CHAR)
);

create table CPMG.STG_TMP_BWZC
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_BWZC_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_BWZC_11_TMP
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_FD
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_FRDK
(
  loanno         VARCHAR2(17 CHAR) not null,
  loansqno       VARCHAR2(3 CHAR) not null,
  accno          VARCHAR2(17 CHAR) not null,
  zoneno         VARCHAR2(5 CHAR),
  brno           VARCHAR2(5 CHAR),
  currtype       VARCHAR2(3 CHAR) not null,
  subcode        VARCHAR2(8 CHAR),
  balance        NUMBER(20,2),
  balance_cm     NUMBER(20,2),
  valueday       VARCHAR2(10 CHAR),
  exhmatud       VARCHAR2(10 CHAR),
  ta490461016    NUMBER(18,2),
  ta490461016_cm NUMBER(18,2),
  ta490461017    NUMBER(9,2),
  ta200261001    VARCHAR2(30 CHAR),
  ta200261002    VARCHAR2(60 CHAR),
  ta200261004    VARCHAR2(30 CHAR),
  ta200261037    VARCHAR2(6 CHAR),
  ta200251013    VARCHAR2(6 CHAR),
  ta200251011    VARCHAR2(6 CHAR),
  ta200251020    VARCHAR2(30 CHAR),
  ta200011014    VARCHAR2(7 CHAR),
  ta200011040    VARCHAR2(6 CHAR),
  ta200011070    VARCHAR2(6 CHAR),
  ta200011005    VARCHAR2(60 CHAR),
  ta200011016    VARCHAR2(5 CHAR),
  ta200011034    VARCHAR2(30 CHAR),
  ta340362013    NUMBER(10,3),
  ta200268004    VARCHAR2(12 CHAR),
  rzzd_fl        VARCHAR2(5 CHAR),
  product_id3    VARCHAR2(10 CHAR),
  product_name3  VARCHAR2(100 CHAR),
  xyvalue        VARCHAR2(6 CHAR),
  xyname         VARCHAR2(100 CHAR),
  dbdc_id        VARCHAR2(10 CHAR),
  dbdc_name      VARCHAR2(40 CHAR),
  dbfs_id        VARCHAR2(5 CHAR),
  dbfs_lb        VARCHAR2(50 CHAR),
  qxdc_id1       VARCHAR2(10 CHAR),
  qxdc_name1     VARCHAR2(40 CHAR),
  qxdc_id2       VARCHAR2(10 CHAR),
  qxdc_name2     VARCHAR2(40 CHAR),
  xs031          NUMBER(5,2),
  xs034          NUMBER(5,2),
  xs033          NUMBER(5,2),
  xs032          NUMBER(5,2),
  xs035          NUMBER(5,2),
  xs036          NUMBER(5,2),
  gjrz_xs        NUMBER(5,2),
  jjzb_xs        NUMBER(20,4),
  ytbbbl         NUMBER(24,2),
  ytbbj          NUMBER(24,2),
  ytbbj_cm       NUMBER(24,2),
  dkamount       NUMBER(24,2),
  ecost          NUMBER(24,2),
  ecost_cm       NUMBER(24,2),
  start_date     VARCHAR2(8),
  end_date       VARCHAR2(8 CHAR),
  xs_product     NUMBER(5,2),
  ywzl           VARCHAR2(6 CHAR),
  htsfsr         NUMBER(18,2),
  rate_today     NUMBER(9),
  cust_exe_rate  NUMBER(9),
  pd             NUMBER(9,6),
  lgd            NUMBER(9,6),
  status         VARCHAR2(6 CHAR),
  smallcorp      VARCHAR2(1 CHAR) default 0 not null,
  dubilstdrate   NUMBER(9),
  ratechgmode    NUMBER(1),
  custname       VARCHAR2(60 CHAR),
  groupname      VARCHAR2(60 CHAR)
);

create table CPMG.STG_TMP_FRDK_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_GRDK
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_GRDKLS
(
  accno          VARCHAR2(17) not null,
  lncode         VARCHAR2(3) not null,
  memno          VARCHAR2(5) not null,
  lnno           VARCHAR2(3) not null,
  zoneno         VARCHAR2(5),
  actbrno        VARCHAR2(5),
  subcode        VARCHAR2(8),
  currtype       VARCHAR2(4) not null,
  balance        NUMBER(20,2),
  ta500261001    VARCHAR2(60),
  product_id3    VARCHAR2(10),
  product_name3  VARCHAR2(100),
  ta500261003    VARCHAR2(30),
  ta500261004    VARCHAR2(30),
  ta500261008    VARCHAR2(4),
  ta500261013    VARCHAR2(8),
  ta500261014    VARCHAR2(8),
  ta500261018    VARCHAR2(6),
  ta500261016    VARCHAR2(8),
  ta500251012    NUMBER(18,2),
  ta500251018    VARCHAR2(8),
  ta500251014    NUMBER(9,6),
  ta500251013    NUMBER(6),
  ta500271009    NUMBER(18,2),
  ta500281016    NUMBER(18,2),
  ta500291012    NUMBER(18,2),
  ta500294008    NUMBER(18,2),
  other_amount   NUMBER(18,2),
  invaild_amount NUMBER(20,2),
  xyamount       NUMBER(20,2),
  ta500251054    VARCHAR2(5),
  ta500270002    VARCHAR2(200),
  totalamount    NUMBER(20,2),
  ta290001004    VARCHAR2(100),
  ta290001005    VARCHAR2(4),
  ta290001006    VARCHAR2(60),
  ta290008004    VARCHAR2(100),
  ta290008009    VARCHAR2(60),
  ta500251097    VARCHAR2(30),
  dkyq_year      NUMBER(17,4),
  bbprecent      NUMBER(5,2),
  yybb           NUMBER(22,4),
  sybb           NUMBER(22,4),
  zcamount       NUMBER(22,4),
  jjzb_xs        NUMBER(22,6),
  ecost          NUMBER(22,4),
  start_date     VARCHAR2(8) not null,
  end_date       VARCHAR2(8),
  dkyq_id        VARCHAR2(10),
  ecost_rj       NUMBER(22,4),
  bal_rj         NUMBER(22,4),
  jjzb_xs_rj     NUMBER(22,6),
  status         VARCHAR2(6)
);

create table CPMG.STG_TMP_GRDK_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_PJTX
(
  zoneno       VARCHAR2(5 CHAR),
  actbrno      VARCHAR2(5 CHAR),
  innerno      VARCHAR2(60 CHAR) not null,
  billno       VARCHAR2(30 CHAR),
  subcode      VARCHAR2(6 CHAR),
  billamt      NUMBER(18,2),
  billamt_cm   NUMBER(18,2),
  billtype     VARCHAR2(4 CHAR),
  buytype      VARCHAR2(4 CHAR),
  firsttype    VARCHAR2(4 CHAR),
  drawerbank   VARCHAR2(30 CHAR),
  holder       VARCHAR2(30 CHAR),
  holdertype   VARCHAR2(4 CHAR),
  creditlevel  VARCHAR2(6 CHAR),
  managerno    VARCHAR2(30 CHAR),
  levelfive    VARCHAR2(6 CHAR),
  discountdate VARCHAR2(8 CHAR),
  realmaturity VARCHAR2(8 CHAR),
  retmaturity  VARCHAR2(8 CHAR),
  capcost      NUMBER(17,2),
  intincome    NUMBER(13,2),
  product      VARCHAR2(10 CHAR),
  qx           VARCHAR2(6 CHAR),
  xspj1        NUMBER(5,2),
  xspj2        NUMBER(5,2),
  xspj3        NUMBER(5,2),
  xs           NUMBER(9,6),
  ec           NUMBER(24,2),
  ec_cm        NUMBER(24,2),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR),
  bearer_name  VARCHAR2(60 CHAR),
  croup_num    VARCHAR2(30 CHAR),
  croup_name   VARCHAR2(60 CHAR)
);

create table CPMG.STG_TMP_PJTXBB
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_PJTXBB_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_PJTXWB
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_PJTXWB_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_QTFX
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_QTFX_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_WXZC
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_WXZC_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_YHK
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_YHK_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_ZJYW
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_ZJYW_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_ZQTZ
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.STG_TMP_ZQTZ_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);
 

create table CPMG.STG_TMP_ZQTZ_TMP
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  subcode   VARCHAR2(8 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28,4),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

CREATE TABLE "CPMG"."CAP_ECTOTAL_DATA" (
	"DATADATE" VARCHAR2(6 CHAR) NOT NULL,
	"ORGANCODE" VARCHAR2(8 CHAR) NOT NULL,
	"EC" NUMBER(17,2),
	"DICCODE" VARCHAR2(1 CHAR) NOT NULL
	
);

CREATE TABLE "CPMG"."CAP_BRANCHS_TRANS_OFFER" (
	"OFFER_BRANCH" VARCHAR2(20 CHAR) NOT NULL,
	"OFFER_YEAR" VARCHAR2(4 CHAR) NOT NULL,
	"OFFER_DATE" VARCHAR2(8 CHAR) NOT NULL,
	"OFFER_QUARTER" VARCHAR2(1 CHAR) NOT NULL,
	"OFFER_VARIETIES" VARCHAR2(1 CHAR),
	"BUY_PRICE" NUMBER(22,2),
	"SELL_PRICE" NUMBER(22,2),
	"PRICE_CREATER" VARCHAR2(20 CHAR),
	"CREATE_TIME" DATE,
	"CREATE_BRANCH" VARCHAR2(20 CHAR)
);

CREATE TABLE "CPMG"."CAP_APPROVE_TYPE" (
	"BRANCH_ID" VARCHAR2(20 CHAR) NOT NULL,
	"APPR_TYPE" VARCHAR2(1 CHAR),
	"START_DATE" VARCHAR2(8 CHAR) NOT NULL,
	"END_DATE" VARCHAR2(8 CHAR)

);

CREATE OR REPLACE TYPE "CTP_TYPE_ARRAYTYPE"                                          IS TABLE OF VARCHAR2(100);

create sequence CPMG.SEQ_LOG_QUARTZ_NO
minvalue 1
maxvalue 999999999
start with 923001
increment by 1
cache 500;

CREATE SEQUENCE "CPMG"."SEQ_TANSLOG" START WITH 426 INCREMENT BY 1 MINVALUE 1 MAXVALUE 99999 CYCLE CACHE 500 NOORDER;

CREATE SEQUENCE "CPMG"."SEQ_TANSAPPR" START WITH 163 INCREMENT BY 1 MINVALUE 1 MAXVALUE 99999 CYCLE CACHE 500 NOORDER;

