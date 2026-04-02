create table ZJCG.ACP_BATCH_DETAIL_TR
(
  ctp_id        VARCHAR2(10) not null,
  tran_seq      VARCHAR2(60) not null,
  seq_no        VARCHAR2(5) not null,
  tran_amt      NUMBER(18,4),
  rcv_typ       VARCHAR2(5),
  rcv_no        VARCHAR2(30),
  rcv_name      VARCHAR2(200),
  id_typ        VARCHAR2(5),
  id_no         VARCHAR2(60),
  rcv_bank_name VARCHAR2(1000),
  rcv_bank_no   VARCHAR2(30),
  remark        VARCHAR2(30),
  tran_stat     VARCHAR2(30),
  trans_seq     VARCHAR2(60),
  constraint PK_ACP_BATCH_DETAIL_TR primary key (CTP_ID, TRAN_SEQ, SEQ_NO)
);
desc ZJCG.ACP_BATCH_DETAIL_TR;



create table ZJCG.ACP_BATCH_TR
(
  ctp_id        VARCHAR2(10) not null,
  tran_seq      VARCHAR2(60) not null,
  tot_amt       NUMBER(18,4),
  tran_stat     VARCHAR2(30),
  tot_num       NUMBER,
  submit_time   VARCHAR2(30),
  complete_time VARCHAR2(30),
  succ_amt      NUMBER(18,4),
  succ_num      NUMBER,
  fail_amt      NUMBER(18,4),
  fail_num      NUMBER,
  constraint PK_ACP_BATCH_TR_TRAN_SEQ primary key (CTP_ID, TRAN_SEQ)
);
desc ZJCG.ACP_BATCH_TR;



create table ZJCG.ACP_TRANS_TR
(
  tran_seq    VARCHAR2(60) not null,
  tran_seq_no VARCHAR2(60),
  ctp_id      VARCHAR2(10) not null,
  ctp_typ     VARCHAR2(10),
  rcv_acct    VARCHAR2(30),
  pay_acct    VARCHAR2(30),
  tran_amt    NUMBER(18,4) not null,
  pass_type   VARCHAR2(10) not null,
  tran_stat   VARCHAR2(5) not null,
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  upd_date    VARCHAR2(10),
  upd_time    VARCHAR2(8),
  pay_name    VARCHAR2(300),
  rcv_name    VARCHAR2(300),
  ctp_seq     VARCHAR2(60),
  trans_flag  VARCHAR2(5) default '0',
  id_no       VARCHAR2(30),
  id_typ      VARCHAR2(5)
);

create index ZJCG.INDEX_ACP_TRANS_TR on ZJCG.ACP_TRANS_TR (TRAN_SEQ);
desc ZJCG.ACP_TRANS_TR;



create table ZJCG.CHAN_CHECK_CONTROL
(
  audi_id        VARCHAR2(30) not null,
  audi_menu_name VARCHAR2(100),
  op_act         VARCHAR2(1),
  audi_stat      VARCHAR2(1),
  commit_teller  VARCHAR2(20),
  commit_org     VARCHAR2(10),
  commit_date    VARCHAR2(10),
  commit_time    VARCHAR2(10),
  audi_teller    VARCHAR2(20),
  audi_org       VARCHAR2(10),
  audi_date      VARCHAR2(10),
  audi_time      VARCHAR2(10),
  constraint PK_CHAN_CHECK_CONTROL primary key (AUDI_ID)
);
desc ZJCG.CHAN_CHECK_CONTROL;



create table ZJCG.CHAN_CHECK_OP
(
  audi_id         VARCHAR2(30) not null,
  audi_menu_name  VARCHAR2(100),
  op_act          VARCHAR2(1),
  audi_stat       VARCHAR2(1),
  audi_table_name VARCHAR2(50),
  audi_field_name VARCHAR2(2000),
  commit_teller   VARCHAR2(50),
  commit_date     VARCHAR2(10),
  commit_time     VARCHAR2(10),
  audi_teller     VARCHAR2(50),
  audi_date       VARCHAR2(10),
  audi_time       VARCHAR2(10),
  commit_org      VARCHAR2(10),
  audi_org        VARCHAR2(10),
  audi_info       VARCHAR2(200),
  prop_book       CLOB,
  old_detail      VARCHAR2(2000),
  old_prop_book   CLOB,
  audi_group_id   VARCHAR2(50),
  commit_detail   CLOB
);
desc ZJCG.CHAN_CHECK_OP;


create table ZJCG.CHAN_CODE
(
  chan_typ       VARCHAR2(3) not null,
  chan_name      VARCHAR2(50) not null,
  chan_state     VARCHAR2(1) not null,
  chan_star_date VARCHAR2(10),
  chan_end_date  VARCHAR2(10),
  chan_star_time VARCHAR2(10),
  chan_end_time  VARCHAR2(10),
  up_date        VARCHAR2(10),
  up_teler       VARCHAR2(50),
  is_audi        VARCHAR2(1),
  audi_id        VARCHAR2(30),
  constraint PK_CHAN_CODE primary key (CHAN_TYP)
);
desc ZJCG.CHAN_CODE;


create table ZJCG.CHAN_EXCEPTION_MSG_PRARM
(
  local_lan      VARCHAR2(10) not null,
  exception_code VARCHAR2(100) not null,
  exception_msg  VARCHAR2(200) not null,
  remark         VARCHAR2(30) not null,
  sort           VARCHAR2(10),
  constraint PK_CHAN_EXCEPTION_MSG_PRARM primary key (LOCAL_LAN, EXCEPTION_CODE)
);

desc ZJCG.CHAN_EXCEPTION_MSG_PRARM;



create table ZJCG.CHAN_FUNC_DEF
(
  func_gr_id    VARCHAR2(12) not null,
  chan_typ      VARCHAR2(3) not null,
  func_gr_name  VARCHAR2(60) not null,
  func_gr_desc  VARCHAR2(120),
  func_gr_state CHAR(1) not null,
  up_date       VARCHAR2(10) not null,
  up_tell       VARCHAR2(10) not null,
  constraint PK_CHAN_FUNC_DEF primary key (FUNC_GR_ID)
);
desc ZJCG.CHAN_FUNC_DEF;



create table ZJCG.CHAN_TRANS_PUB_CONF
(
  conf_type  VARCHAR2(30) not null,
  trans_code VARCHAR2(50) not null,
  trans_conf VARCHAR2(50) not null,
  remark     VARCHAR2(300) not null,
  constraint PK_CHAN_TRANS_PUB_CONF primary key (CONF_TYPE, TRANS_CODE)
);

desc ZJCG.CHAN_TRANS_PUB_CONF;



create table ZJCG.CTP_ACCT
(
  ctp_id        VARCHAR2(10) not null,
  ctp_acct      VARCHAR2(30) not null,
  ctp_acct_name VARCHAR2(150) not null,
  ctp_open_org  VARCHAR2(8) not null,
  bank_no       VARCHAR2(30),
  bankname      VARCHAR2(150),
  acct_state    VARCHAR2(2) not null,
  acct_right    VARCHAR2(5),
  pre_no        VARCHAR2(10),
  take_pre_no   VARCHAR2(20),
  status        VARCHAR2(1)
);

desc ZJCG.CTP_ACCT;


create table ZJCG.CTP_ACCT_BAL
(
  cg_account VARCHAR2(40),
  acct_no    VARCHAR2(40),
  bal        VARCHAR2(24),
  zhifujine  VARCHAR2(24)
);
desc ZJCG.CTP_ACCT_BAL;


create table ZJCG.CTP_ACCT_LMT_CT
(
  ctp_id   VARCHAR2(10) not null,
  acct_no  VARCHAR2(40) not null,
  once_lmt NUMBER(18,4) not null,
  day_lmt  NUMBER(18,4) not null,
  cre_date VARCHAR2(10),
  cre_time VARCHAR2(8),
  cre_id   VARCHAR2(25),
  auth_id  VARCHAR2(25),
  upd_date VARCHAR2(10),
  upd_time VARCHAR2(8),
  upd_id   VARCHAR2(25),
  uauto_id VARCHAR2(25),
  stat     VARCHAR2(5) not null,
  constraint PK_CTP_ACCT_LMT_CT primary key (ACCT_NO)
);

desc ZJCG.CTP_ACCT_LMT_CT;



create table ZJCG.CTP_ACCT_LMT_USED
(
  ctp_id    VARCHAR2(10) not null,
  acct_no   VARCHAR2(40) not null,
  used_lmt  NUMBER(18,4) not null,
  used_date VARCHAR2(10) not null,
  constraint PK_CTP_ACCT_LMT_USED primary key (ACCT_NO)
);

desc ZJCG.CTP_ACCT_LMT_USED;



create table ZJCG.CTP_ACCT_REBATE_TR
(
  tran_seq         VARCHAR2(60) not null,
  ctp_id           VARCHAR2(10) not null,
  ctp_seq          VARCHAR2(60),
  tran_stat        VARCHAR2(5),
  tran_amt         NUMBER(18,4),
  tran_date        VARCHAR2(10),
  tran_time        VARCHAR2(8),
  comple_date      VARCHAR2(10),
  comple_time      VARCHAR2(8),
  rebate_acct_no   VARCHAR2(40),
  rebate_acct_name VARCHAR2(300),
  query_rate       NUMBER(5) default 0,
  rebate_type      VARCHAR2(5),
  ctp_acct_name    VARCHAR2(30),
  ctp_acct         VARCHAR2(30),
  tran_type        VARCHAR2(5),
  constraint PK_CTP_ACCT_REBATE primary key (TRAN_SEQ)
);

desc ZJCG.CTP_ACCT_REBATE_TR;



create table ZJCG.CTP_AUTO_DELAY_DATE_INFO
(
  trans_seq    VARCHAR2(60) not null,
  ctp_id       VARCHAR2(10) not null,
  id_typ       VARCHAR2(2),
  id_name      VARCHAR2(300),
  cust_role    VARCHAR2(60) default '0',
  sub_acct     VARCHAR2(60),
  package_id   VARCHAR2(200),
  max_date     VARCHAR2(60),
  now_time     VARCHAR2(20),
  now_date     VARCHAR2(20),
  stat         VARCHAR2(1),
  old_max_date VARCHAR2(20),
  id_no        VARCHAR2(20),
  ebuser_no    VARCHAR2(20),
  ctp_seq      VARCHAR2(60),
  constraint CTP_AUTO_DELAY_DATE_INFO primary key (TRANS_SEQ)
);

alter table ZJCG.CTP_AUTO_DELAY_DATE_INFO add constraint PK_CTP_AUTO_DELAY_DATE_INFO unique (CTP_ID, CTP_SEQ);
desc ZJCG.CTP_AUTO_DELAY_DATE_INFO;



create table ZJCG.CTP_BATCH_ACTIVE_TR
(
  trans_seq        VARCHAR2(60) not null,
  ctp_seq          VARCHAR2(60),
  ctp_id           VARCHAR2(10) not null,
  acct_no          VARCHAR2(60),
  mobile_no        VARCHAR2(20),
  id_name          VARCHAR2(300),
  id_no            VARCHAR2(60),
  id_typ           VARCHAR2(10),
  synz_url         VARCHAR2(600),
  back_url         VARCHAR2(600),
  ele_acct_no      VARCHAR2(60),
  trans_date       VARCHAR2(600),
  trans_time       VARCHAR2(600),
  synz_active_rate VARCHAR2(60) default 0,
  is_active_synz   VARCHAR2(60) default 'N',
  is_active_stat   VARCHAR2(60) default 'N',
  ebuser_no        VARCHAR2(60),
  cust_role        VARCHAR2(60) default 'A',
  ebuser_typ       VARCHAR2(1) default 'P',
  constraint PK_CTP_BATCH_ACTIVE_TR primary key (TRANS_SEQ)
);

desc ZJCG.CTP_BATCH_ACTIVE_TR;



create table ZJCG.CTP_BATCH_ADJUST_TR
(
  seq_no        VARCHAR2(5) not null,
  ctp_id        VARCHAR2(8) not null,
  batch_no      VARCHAR2(50) not null,
  tran_seq      VARCHAR2(50),
  pay_acct      VARCHAR2(20),
  pay_acct_name VARCHAR2(100),
  rcv_acct      VARCHAR2(20),
  rcv_acct_name VARCHAR2(100),
  tran_amt      NUMBER(18,4),
  tran_stat     VARCHAR2(2),
  host_code     VARCHAR2(10),
  host_msg      VARCHAR2(200),
  tran_date     VARCHAR2(60),
  tran_time     VARCHAR2(60),
  comple_date   VARCHAR2(60),
  comple_time   VARCHAR2(60),
  reseve1       VARCHAR2(60),
  reseve2       VARCHAR2(60),
  host_seq      VARCHAR2(60),
  host_date     VARCHAR2(60),
  gg_teller_no  VARCHAR2(10),
  constraint PK_ADJUST primary key (SEQ_NO, CTP_ID, BATCH_NO)
);

desc ZJCG.CTP_BATCH_ACTIVE_TR;



create table ZJCG.CTP_BATCH_AUTOBID_TR
(
  seq_no       VARCHAR2(10) not null,
  ctp_id       VARCHAR2(10) not null,
  ctp_seq      VARCHAR2(60) not null,
  pro_id       VARCHAR2(60),
  detail_seq   VARCHAR2(20),
  stat         VARCHAR2(5),
  bid_seq      VARCHAR2(60),
  id_typ       VARCHAR2(5),
  id_no        VARCHAR2(60),
  id_name      VARCHAR2(300),
  bid_no       VARCHAR2(60),
  pay_acct     VARCHAR2(40),
  rcv_acct     VARCHAR2(40),
  tran_amt     NUMBER(18,4),
  active_acct1 VARCHAR2(40),
  active_amt1  NUMBER(18,4),
  active_acct2 VARCHAR2(40),
  active_amt2  NUMBER(18,4),
  active_acct3 VARCHAR2(40),
  active_amt3  NUMBER(18,4),
  tran_date    VARCHAR2(10),
  tran_time    VARCHAR2(8),
  comple_date  VARCHAR2(10),
  comple_time  VARCHAR2(8),
  host_code    VARCHAR2(500),
  host_msg     VARCHAR2(500),
  extend1      VARCHAR2(50),
  extend2      VARCHAR2(100),
  constraint PK_CTP_BATCH_AUTOBID_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_BATCH_AUTOBID_TR;



create table ZJCG.CTP_BATCH_AUTOBUY_TR
(
  seq_no        VARCHAR2(30) not null,
  tran_seq      VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10),
  ctp_seq       VARCHAR2(60),
  pro_id        VARCHAR2(60),
  detail_seq    VARCHAR2(60),
  stat          VARCHAR2(5),
  bid_seq       VARCHAR2(60),
  old_bid_seq   VARCHAR2(60),
  id_typ        VARCHAR2(5),
  id_no         VARCHAR2(30),
  id_name       VARCHAR2(60),
  buy_ebuser_no VARCHAR2(30),
  zr_ebuser_no  VARCHAR2(30),
  buy_acct      VARCHAR2(30),
  zr_acct       VARCHAR2(30),
  zr_name       VARCHAR2(60),
  bid_amt       VARCHAR2(60),
  tran_amt      VARCHAR2(60),
  fee_acct1     VARCHAR2(30),
  fee_name1     VARCHAR2(60),
  fee_amt1      VARCHAR2(60),
  fee_acct2     VARCHAR2(30),
  fee_name2     VARCHAR2(60),
  fee_amt2      VARCHAR2(60),
  fee_acct3     VARCHAR2(30),
  fee_name3     VARCHAR2(60),
  fee_amt3      VARCHAR2(60),
  tran_date     VARCHAR2(60),
  tran_time     VARCHAR2(60),
  comple_date   VARCHAR2(60),
  comple_time   VARCHAR2(60),
  host_code     VARCHAR2(60),
  host_msg      VARCHAR2(60),
  ori_reg_no    VARCHAR2(60),
  zr_reg_no     VARCHAR2(60),
  buy_reg_no    VARCHAR2(60),
  zr_bid_stat   VARCHAR2(6),
  buy_bid_stat  VARCHAR2(6),
  constraint PK_CTP_BATCH_AUTOBUY_TR primary key (SEQ_NO, TRAN_SEQ)
);
desc ZJCG.CTP_BATCH_AUTOBUY_TR;



create table ZJCG.CTP_BATCH_AUTOCREDIT_TR
(
  seq_no          VARCHAR2(10) not null,
  ctp_id          VARCHAR2(10) not null,
  ctp_seq         VARCHAR2(60) not null,
  pro_id          VARCHAR2(60),
  detail_seq      VARCHAR2(20),
  stat            VARCHAR2(5),
  bid_seq_no      VARCHAR2(60),
  id_typ          VARCHAR2(5),
  id_no           VARCHAR2(60),
  id_name         VARCHAR2(300),
  sale_sub_acc_no VARCHAR2(60),
  tran_date       VARCHAR2(10),
  host_code       VARCHAR2(500),
  host_msg        VARCHAR2(500),
  buy_bid_stat    VARCHAR2(2),
  constraint PK_CTP_BATCH_AUTOCREDIT_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_BATCH_AUTOCREDIT_TR;



create table ZJCG.CTP_BATCH_CANCEL_TR
(
  seq_no      VARCHAR2(10) not null,
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  pro_id      VARCHAR2(60),
  bid_seq_no  VARCHAR2(60),
  detail_seq  VARCHAR2(20),
  stat        VARCHAR2(5),
  id_typ      VARCHAR2(5),
  id_no       VARCHAR2(60),
  id_name     VARCHAR2(300),
  ebuser_no   VARCHAR2(60),
  acct_no     VARCHAR2(40),
  acct_name   VARCHAR2(300),
  tran_amt    NUMBER(18,4),
  real_amt    NUMBER(18,4),
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  comple_date VARCHAR2(10),
  comple_time VARCHAR2(8),
  host_code   VARCHAR2(500),
  host_msg    VARCHAR2(500),
  constraint PK_CTP_BATCH_CANCEL_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_BATCH_CANCEL_TR;



create table ZJCG.CTP_BATCH_COMPENT_TR
(
  ctp_id        VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(30) not null,
  ctp_seq       VARCHAR2(60) not null,
  tran_seq      VARCHAR2(50) not null,
  seq_no        VARCHAR2(5) not null,
  compent_no    VARCHAR2(120) not null,
  pro_id        VARCHAR2(60),
  pro_typ       VARCHAR2(10),
  id_name       VARCHAR2(90),
  id_no         VARCHAR2(60),
  id_typ        VARCHAR2(5),
  ebuser_no2    VARCHAR2(30),
  amt           NUMBER(18,4),
  tran_stat     VARCHAR2(5),
  host_code     VARCHAR2(20),
  host_msg      VARCHAR2(100),
  host_seq      VARCHAR2(10),
  stat          VARCHAR2(5),
  extend1       VARCHAR2(100),
  extend2       VARCHAR2(200),
  trans_seq     VARCHAR2(60),
  tran_date     VARCHAR2(30),
  tran_time     VARCHAR2(30),
  use_amt       NUMBER(18,4),
  upd_time      VARCHAR2(30),
  upd_date      VARCHAR2(30),
  is_data_trans VARCHAR2(5) default 'N',
  constraint PK_CTP_BATCH_COMPENT_TR primary key (COMPENT_NO)
);
desc ZJCG.CTP_BATCH_COMPENT_TR;



create table ZJCG.CTP_BATCH_FEE_TR
(
  seq_no         VARCHAR2(60) not null,
  tran_seq       VARCHAR2(60) not null,
  ctp_id         VARCHAR2(20),
  ctp_seq        VARCHAR2(30),
  old_batch_no   VARCHAR2(30),
  stat           VARCHAR2(10),
  pay_acct_no    VARCHAR2(30),
  pay_acct_name  VARCHAR2(60),
  tran_amt       VARCHAR2(30),
  fee_acct       VARCHAR2(30),
  fee_name       VARCHAR2(60),
  fee_amt        VARCHAR2(30),
  detail_seq     VARCHAR2(30),
  old_detail_seq VARCHAR2(30),
  tran_date      VARCHAR2(30),
  tran_time      VARCHAR2(30),
  comple_date    VARCHAR2(30),
  comple_time    VARCHAR2(30),
  host_code      VARCHAR2(60),
  host_msg       VARCHAR2(60),
  constraint PK_CTP_BATCH_FEE_TR primary key (SEQ_NO, TRAN_SEQ)
);
desc ZJCG.CTP_BATCH_FEE_TR;



create table ZJCG.CTP_BATCH_HOST_TR
(
  seq_no             VARCHAR2(10) not null,
  tran_seq           VARCHAR2(60) not null,
  ctp_id             VARCHAR2(10),
  ctp_seq            VARCHAR2(60),
  detail_seq         VARCHAR2(20),
  tran_type          VARCHAR2(10),
  pro_id             VARCHAR2(60),
  jd_flag            VARCHAR2(5),
  acct_no            VARCHAR2(40),
  acct_name          VARCHAR2(300),
  tran_amt           NUMBER(18,4),
  real_amt           NUMBER(18,4),
  stat               VARCHAR2(5),
  host_code          VARCHAR2(500),
  host_msg           VARCHAR2(500),
  host_date          VARCHAR2(10),
  host_seq           VARCHAR2(60),
  tran_date          VARCHAR2(10),
  tran_time          VARCHAR2(8),
  comple_date        VARCHAR2(10),
  comple_time        VARCHAR2(8),
  original_acct      VARCHAR2(30),
  original_acct_name VARCHAR2(60),
  mid_fee_type       VARCHAR2(5),
  trans_flag         VARCHAR2(5) default '0',
  constraint PK_CTP_BATCH_HOST_TR primary key (SEQ_NO, TRAN_SEQ)
);
desc ZJCG.CTP_BATCH_HOST_TR;

create index ZJCG.INDEX_CTP_BATCH_HOST_TR on ZJCG.CTP_BATCH_HOST_TR (TRAN_SEQ, CTP_ID);
create index ZJCG.NQ_CTP_BATCH on ZJCG.CTP_BATCH_HOST_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_BATCH_HOST_TR1
(
  seq_no             VARCHAR2(10) not null,
  tran_seq           VARCHAR2(60) not null,
  ctp_id             VARCHAR2(10),
  ctp_seq            VARCHAR2(60),
  detail_seq         VARCHAR2(20),
  tran_type          VARCHAR2(10),
  pro_id             VARCHAR2(60),
  jd_flag            VARCHAR2(5),
  acct_no            VARCHAR2(40),
  acct_name          VARCHAR2(300),
  tran_amt           NUMBER(18,4),
  real_amt           NUMBER(18,4),
  stat               VARCHAR2(5),
  host_code          VARCHAR2(500),
  host_msg           VARCHAR2(500),
  host_date          VARCHAR2(10),
  host_seq           VARCHAR2(60),
  tran_date          VARCHAR2(10),
  tran_time          VARCHAR2(8),
  comple_date        VARCHAR2(10),
  comple_time        VARCHAR2(8),
  original_acct      VARCHAR2(30),
  original_acct_name VARCHAR2(60),
  mid_fee_type       VARCHAR2(5),
  trans_flag         VARCHAR2(5) default '0',
  constraint PK_CTP_BATCH_HOST_TR1 primary key (SEQ_NO, TRAN_SEQ)
);
desc ZJCG.CTP_BATCH_HOST_TR1;



create table ZJCG.CTP_BATCH_HOST_TR_BAK
(
  seq_no             VARCHAR2(10) not null,
  tran_seq           VARCHAR2(60) not null,
  ctp_id             VARCHAR2(10),
  ctp_seq            VARCHAR2(60),
  detail_seq         VARCHAR2(20),
  tran_type          VARCHAR2(10),
  pro_id             VARCHAR2(60),
  jd_flag            VARCHAR2(5),
  acct_no            VARCHAR2(40),
  acct_name          VARCHAR2(300),
  tran_amt           NUMBER(18,4),
  real_amt           NUMBER(18,4),
  stat               VARCHAR2(5),
  host_code          VARCHAR2(500),
  host_msg           VARCHAR2(500),
  host_date          VARCHAR2(10),
  host_seq           VARCHAR2(60),
  tran_date          VARCHAR2(10),
  tran_time          VARCHAR2(8),
  comple_date        VARCHAR2(10),
  comple_time        VARCHAR2(8),
  original_acct      VARCHAR2(30),
  original_acct_name VARCHAR2(60),
  mid_fee_type       VARCHAR2(5),
  trans_flag         VARCHAR2(5)
);
desc ZJCG.CTP_BATCH_HOST_TR_BAK;



create table ZJCG.CTP_BATCH_PP_ADD_TR
(
  tran_seq       VARCHAR2(60) not null,
  ctp_seq        VARCHAR2(60) not null,
  ctp_id         VARCHAR2(10) not null,
  pro_id         VARCHAR2(60) not null,
  pro_name       VARCHAR2(500),
  pro_typ        VARCHAR2(10),
  pro_amt        NUMBER(18,4) not null,
  risk_lev       VARCHAR2(30),
  pro_desc       VARCHAR2(2000),
  cre_date       VARCHAR2(10),
  cre_time       VARCHAR2(8),
  star_buy_time  VARCHAR2(20),
  end_buy_time   VARCHAR2(20),
  star_date      VARCHAR2(20),
  pro_term       VARCHAR2(5),
  expinc_rate    NUMBER(18,4),
  end_date       VARCHAR2(20),
  mj_name        VARCHAR2(300) not null,
  mj_id_typ      VARCHAR2(10) not null,
  mj_id_no       VARCHAR2(60) not null,
  mj_no          VARCHAR2(60),
  stat           VARCHAR2(10) not null,
  conf_date      VARCHAR2(10),
  conf_time      VARCHAR2(8),
  conf_result    VARCHAR2(5),
  zr_flag        VARCHAR2(5),
  dc_flag        VARCHAR2(5),
  is_synz        VARCHAR2(5),
  synz_rate      NUMBER,
  synz_url       VARCHAR2(200),
  back_url       VARCHAR2(200),
  last_synz_time VARCHAR2(20),
  fee_per_top    NUMBER(18,4),
  ctp_send_fee   NUMBER(18,4),
  pro_min_amt    NUMBER(18,4),
  constraint PK_CTP_BATCH_PP_ADD_TR primary key (CTP_SEQ, PRO_ID)
);
desc ZJCG.CTP_BATCH_PP_ADD_TR;



create table ZJCG.CTP_BATCH_REBATE_TR
(
  seq_no           VARCHAR2(10) not null,
  ctp_id           VARCHAR2(10) not null,
  ctp_seq          VARCHAR2(60) not null,
  detail_seq       VARCHAR2(20),
  stat             VARCHAR2(5),
  id_typ           VARCHAR2(5),
  id_no            VARCHAR2(60),
  id_name          VARCHAR2(300),
  ebuser_no        VARCHAR2(60),
  acct_no          VARCHAR2(40),
  acct_name        VARCHAR2(300),
  tran_amt         NUMBER(18,4),
  real_amt         NUMBER(18,4),
  tran_date        VARCHAR2(10),
  tran_time        VARCHAR2(8),
  comple_date      VARCHAR2(10),
  comple_time      VARCHAR2(8),
  host_code        VARCHAR2(500),
  host_msg         VARCHAR2(500),
  rebate_acct_no   VARCHAR2(40),
  rebate_acct_name VARCHAR2(300),
  constraint PK_CTP_BATCH_REBATE_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_BATCH_REBATE_TR;




create table ZJCG.CTP_BATCH_REPAY_TR
(
  seq_no      VARCHAR2(10) not null,
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  detail_seq  VARCHAR2(20),
  stat        VARCHAR2(5),
  jd_flag     VARCHAR2(5) not null,
  acct_no     VARCHAR2(40),
  acct_name   VARCHAR2(300),
  tran_amt    NUMBER(18,4),
  real_amt    NUMBER(18,4),
  back_flag   VARCHAR2(5),
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  comple_date VARCHAR2(10),
  comple_time VARCHAR2(8),
  host_code   VARCHAR2(500),
  host_msg    VARCHAR2(500),
  dc_flag     VARCHAR2(5),
  fee_flag    VARCHAR2(5),
  pro_id      VARCHAR2(60),
  trans_flag  VARCHAR2(5) default 'N',
  compent_no  VARCHAR2(60),
  corpus      NUMBER(18,4),
  accrual     NUMBER(18,4),
  bid_seq_no  VARCHAR2(60),
  first_flag  VARCHAR2(5),
  diff_amt    NUMBER(18,4),
  ctp_date    VARCHAR2(10),
  pro_name    VARCHAR2(500),
  constraint PK_CTP_BATCH_REPAY_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_BATCH_REPAY_TR;


create index ZJCG.INDEX_CTP_BATCH_REPAY_TR on ZJCG.CTP_BATCH_REPAY_TR (CTP_ID, CTP_SEQ);
create index ZJCG.N_CTP_BATCH_REPAY_TR on ZJCG.CTP_BATCH_REPAY_TR (PRO_ID, BID_SEQ_NO, STAT, ACCT_NO);
create index ZJCG.N_TRAN_DAT_REPAY on ZJCG.CTP_BATCH_REPAY_TR (CTP_DATE);


create table ZJCG.CTP_BATCH_REPAY_TR1
(
  seq_no      VARCHAR2(10) not null,
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  detail_seq  VARCHAR2(20),
  stat        VARCHAR2(5),
  jd_flag     VARCHAR2(5) not null,
  acct_no     VARCHAR2(40),
  acct_name   VARCHAR2(300),
  tran_amt    NUMBER(18,4),
  real_amt    NUMBER(18,4),
  back_flag   VARCHAR2(5),
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  comple_date VARCHAR2(10),
  comple_time VARCHAR2(8),
  host_code   VARCHAR2(500),
  host_msg    VARCHAR2(500),
  dc_flag     VARCHAR2(5),
  fee_flag    VARCHAR2(5),
  pro_id      VARCHAR2(60),
  trans_flag  VARCHAR2(5) default 'N',
  compent_no  VARCHAR2(60),
  corpus      NUMBER(18,4),
  accrual     NUMBER(18,4),
  constraint PK_CTP_BATCH_REPAY_TR1 primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_BATCH_REPAY_TR1;

create index ZJCG.INDEX_CTP_BATCH_REPAY_TR1 on ZJCG.CTP_BATCH_REPAY_TR1 (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_BATCH_REPAY_TR_BAK
(
  seq_no      VARCHAR2(10) not null,
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  detail_seq  VARCHAR2(20),
  stat        VARCHAR2(5),
  jd_flag     VARCHAR2(5) not null,
  acct_no     VARCHAR2(40),
  acct_name   VARCHAR2(300),
  tran_amt    NUMBER(18,4),
  real_amt    NUMBER(18,4),
  back_flag   VARCHAR2(5),
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  comple_date VARCHAR2(10),
  comple_time VARCHAR2(8),
  host_code   VARCHAR2(500),
  host_msg    VARCHAR2(500),
  dc_flag     VARCHAR2(5),
  fee_flag    VARCHAR2(5),
  pro_id      VARCHAR2(60),
  trans_flag  VARCHAR2(5),
  compent_no  VARCHAR2(60),
  corpus      NUMBER(18,4),
  accrual     NUMBER(18,4)
);
desc ZJCG.CTP_BATCH_REPAY_TR_BAK;


create table ZJCG.CTP_BATCH_TR
(
  tran_seq         VARCHAR2(60) not null,
  ctp_id           VARCHAR2(10) not null,
  ctp_seq          VARCHAR2(60) not null,
  first_batch_no   VARCHAR2(60),
  host_batch_no    VARCHAR2(60),
  pro_id           VARCHAR2(60),
  tran_type        VARCHAR2(5),
  stat             VARCHAR2(5),
  tran_count       VARCHAR2(10),
  next_batch_no    VARCHAR2(60),
  acct_no          VARCHAR2(40),
  acct_name        VARCHAR2(300),
  tran_amt         NUMBER(18,4),
  tot_num          NUMBER,
  succ_num         NUMBER,
  succ_amt         NUMBER(18,4),
  fail_num         NUMBER,
  fail_amt         NUMBER(18,4),
  tran_date        VARCHAR2(10),
  tran_time        VARCHAR2(8),
  comple_date      VARCHAR2(10),
  comple_time      VARCHAR2(8),
  host_code        VARCHAR2(500),
  host_msg         VARCHAR2(500),
  local_file       VARCHAR2(100),
  host_file        VARCHAR2(100),
  front_code       VARCHAR2(20),
  is_synz          VARCHAR2(5),
  synz_rate        NUMBER,
  synz_url         VARCHAR2(200),
  last_synz_time   VARCHAR2(20),
  ohter_synz       VARCHAR2(5) default 'N',
  ohter_synz_rate  NUMBER default 0,
  batch_query_rate NUMBER default 0,
  ctp_date         VARCHAR2(30),
  first_flag       VARCHAR2(5),
  tran_flag        VARCHAR2(5) default '0',
  constraint PK_CTP_BATCH_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_BATCH_TR;

create unique index ZJCG.CTP_SEQ on ZJCG.CTP_BATCH_TR (CTP_ID, CTP_SEQ);
create index ZJCG.FADSFADFAD on ZJCG.CTP_BATCH_TR (CTP_ID, TRAN_TYPE, TRAN_DATE, TRAN_TIME, PRO_ID);


create table ZJCG.CTP_BIND_CARD_EXCHANG_TR
(
  tran_seq      VARCHAR2(60),
  ctp_seq       VARCHAR2(60) not null,
  old_bind_card VARCHAR2(40),
  new_bind_card VARCHAR2(40),
  old_mobile    VARCHAR2(20),
  new_mobile    VARCHAR2(20),
  reg_amt       VARCHAR2(20),
  old_bank_code VARCHAR2(30),
  old_bank_name VARCHAR2(200),
  new_bank_code VARCHAR2(30),
  new_bank_name VARCHAR2(200),
  ele_acct_no   VARCHAR2(30),
  tran_stat     VARCHAR2(2),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(10),
  ref_reason    VARCHAR2(400),
  ctp_id        VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(30),
  back_url      VARCHAR2(200),
  synz_url      VARCHAR2(200),
  synz_rate     VARCHAR2(2) default 0,
  id_typ        VARCHAR2(5),
  id_no         VARCHAR2(50),
  id_name       VARCHAR2(100),
  cust_role     VARCHAR2(2),
  is_synz       VARCHAR2(2) default 'N',
  constraint PK_CTP primary key (CTP_SEQ, CTP_ID)
);
alter table ZJCG.CTP_BIND_CARD_EXCHANG_TR  add constraint PK_TRAN unique (TRAN_SEQ);
desc ZJCG.CTP_BIND_CARD_EXCHANG_TR;



create table ZJCG.CTP_BROKERAGE_TR
(
  tran_seq   VARCHAR2(60) not null,
  seq_no     VARCHAR2(5) not null,
  check_date VARCHAR2(10) not null,
  org_no     VARCHAR2(8),
  pay_acct   VARCHAR2(30),
  pay_name   VARCHAR2(300),
  pay_org    VARCHAR2(8),
  rcv_acct   VARCHAR2(30),
  rcv_name   VARCHAR2(300),
  rcv_org    VARCHAR2(8),
  tran_amt   NUMBER(18,4),
  tran_date  VARCHAR2(10),
  tran_time  VARCHAR2(8),
  bro_tot    NUMBER(18,4),
  bro_per    NUMBER(18,4),
  tran_type  VARCHAR2(5),
  chan_typ   VARCHAR2(5),
  oper_type  VARCHAR2(5),
  pass_type  VARCHAR2(10),
  tran_state VARCHAR2(5) not null
);
desc ZJCG.CTP_BROKERAGE_TR;



create table ZJCG.CTP_BT_ORDER_INFO
(
  ctp_id       VARCHAR2(10) not null,
  order_no     VARCHAR2(60) not null,
  oper_type    VARCHAR2(10) not null,
  pro_id       VARCHAR2(60) not null,
  seller_no    VARCHAR2(60) not null,
  buyer_no     VARCHAR2(60) not null,
  tran_amt     NUMBER(18,4) not null,
  pro_fee      NUMBER(18,4),
  stat         VARCHAR2(2) not null,
  cre_date     VARCHAR2(10),
  cre_time     VARCHAR2(8),
  upd_date     VARCHAR2(10),
  upd_time     VARCHAR2(8),
  back_url     VARCHAR2(200),
  synz_url     VARCHAR2(200),
  is_synz      VARCHAR2(5),
  synz_rate    NUMBER,
  ori_order_no VARCHAR2(60),
  tot_num      NUMBER,
  unit_meat    VARCHAR2(20),
  fee_acct     VARCHAR2(40),
  fee_name     VARCHAR2(300),
  ctp_date     VARCHAR2(10),
  ctp_seq      VARCHAR2(60),
  constraint PK_CTP_BT_ORDER_INFO primary key (CTP_ID, ORDER_NO)
);
desc ZJCG.CTP_BT_ORDER_INFO;




create table ZJCG.CTP_BT_TYP_CONF
(
  ctp_id        VARCHAR2(10) not null,
  pro_typ       VARCHAR2(10) not null,
  zf_flag       VARCHAR2(1),
  zjh_flag      VARCHAR2(1),
  sq_flag       VARCHAR2(1),
  jg_acct       VARCHAR2(30),
  cd_flag       VARCHAR2(1),
  fee_acct      VARCHAR2(30),
  pro_typ_name  VARCHAR2(100),
  kk_flag       VARCHAR2(5),
  fee_max_level VARCHAR2(25)
);
desc ZJCG.CTP_BT_TYP_CONF;



create table ZJCG.CTP_CAP_BATCH_ROLL_TR
(
  seq_no      VARCHAR2(10) not null,
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  detail_seq  VARCHAR2(20),
  stat        VARCHAR2(5),
  id_typ      VARCHAR2(5),
  id_no       VARCHAR2(60),
  id_name     VARCHAR2(300),
  ebuser_no   VARCHAR2(60),
  acct_no     VARCHAR2(40),
  acct_name   VARCHAR2(300),
  tran_amt    NUMBER(18,4),
  real_amt    NUMBER(18,4),
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  comple_date VARCHAR2(10),
  comple_time VARCHAR2(8),
  host_code   VARCHAR2(500),
  host_msg    VARCHAR2(500),
  tran_type   VARCHAR2(6),
  user_id     VARCHAR2(60),
  req_ext1    VARCHAR2(60),
  req_ext2    VARCHAR2(60),
  pass_id     VARCHAR2(60),
  constraint PK_CTP_CAP_BATCH_ROLL_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_CAP_BATCH_ROLL_TR;




create table ZJCG.CTP_CAP_CHAN_TR
(
  pass_id         VARCHAR2(50),
  ctp_id          VARCHAR2(10),
  tran_seq        VARCHAR2(60) not null,
  pro_id          VARCHAR2(60),
  acct_no         VARCHAR2(32),
  acct_name       VARCHAR2(50),
  card_no         VARCHAR2(32),
  card_name       VARCHAR2(50),
  trans_chan_type VARCHAR2(20),
  oper_type       VARCHAR2(2),
  oper_amt        NUMBER(18,4),
  tran_state      VARCHAR2(2),
  tran_date       VARCHAR2(20),
  tran_time       VARCHAR2(20),
  host_code       VARCHAR2(60),
  host_msg        VARCHAR2(100),
  synz_rate       NUMBER(22),
  is_synz         VARCHAR2(5),
  host_time       VARCHAR2(60),
  user_id         VARCHAR2(20),
  tran_typ        VARCHAR2(10),
  ctp_seq         VARCHAR2(60),
  synz_url        VARCHAR2(200)
);
desc ZJCG.CTP_CAP_CHAN_TR;



create table ZJCG.CTP_CAP_CONF_TR
(
  pass_id   VARCHAR2(20) not null,
  ctp_id1   VARCHAR2(20),
  ctp_id2   VARCHAR2(20),
  acct_no   VARCHAR2(30),
  acct_name VARCHAR2(60),
  card_no   VARCHAR2(30),
  card_name VARCHAR2(60),
  cif_no    VARCHAR2(60),
  bank_code VARCHAR2(60),
  bank_name VARCHAR2(60),
  constraint PK_CTP_CAP_CONF_TR primary key (PASS_ID)
);
alter table ZJCG.CTP_CAP_CONF_TR  add constraint PK_CTP_CAP_CONF_TR2 unique (CTP_ID1, CTP_ID2);
desc ZJCG.CTP_CAP_CONF_TR;



create table ZJCG.CTP_CAP_SYNZ_CONF
(
  pass_id   VARCHAR2(200) not null,
  ctp_id1   VARCHAR2(10),
  ctp_id2   VARCHAR2(10),
  synz_type VARCHAR2(20),
  synz_url1 VARCHAR2(200),
  synz_url2 VARCHAR2(200),
  synz_kind VARCHAR2(5),
  constraint PK_CTP_CAP_SYNZ_CONF_PASS_ID primary key (PASS_ID)
);
desc ZJCG.CTP_CAP_SYNZ_CONF;



create table ZJCG.CTP_CAP_SYNZ_TR
(
  pass_id       VARCHAR2(60),
  ctp_id        VARCHAR2(20),
  ctp_seq       VARCHAR2(60),
  id_name       VARCHAR2(200),
  pro_id        VARCHAR2(60),
  tran_amt      VARCHAR2(60),
  tran_stat     VARCHAR2(5),
  synz_date     VARCHAR2(60),
  synz_time     VARCHAR2(60),
  is_synz       VARCHAR2(10) default 'N',
  synz_rate     VARCHAR2(10) default '0',
  tran_type     VARCHAR2(10),
  ori_trans_seq VARCHAR2(60),
  ctp_id2       VARCHAR2(20),
  synz_url      VARCHAR2(200)
);
desc ZJCG.CTP_CAP_SYNZ_TR;


create table ZJCG.CTP_CBIN_CONF
(
  bank_code VARCHAR2(30),
  org_code  VARCHAR2(30),
  bank_name VARCHAR2(200)
);
desc ZJCG.CTP_CBIN_CONF;

create table ZJCG.CTP_CHANGE_CARD_TR
(
  tran_seq   VARCHAR2(60) not null,
  ctp_id     VARCHAR2(10),
  ebuser_no  VARCHAR2(20),
  tran_date  VARCHAR2(10),
  tran_time  VARCHAR2(8),
  tran_stat  VARCHAR2(2),
  id_name    VARCHAR2(200),
  ebuser_typ VARCHAR2(1),
  id_typ     VARCHAR2(2),
  id_no      VARCHAR2(30),
  eacct_no   VARCHAR2(30),
  acct_no    VARCHAR2(30),
  bank_code  VARCHAR2(20),
  bank_name  VARCHAR2(150),
  synz_url   VARCHAR2(200),
  is_synz    VARCHAR2(5),
  synz_rate  NUMBER(22),
  reg_amt    NUMBER(22),
  constraint CTP_CHANGE_CARD_TR_PK primary key (TRAN_SEQ)
);
desc ZJCG.CTP_CHANGE_CARD_TR;


create table ZJCG.CTP_CHEAR_INFO
(
  pass_type   VARCHAR2(20) not null,
  check_date  VARCHAR2(10) not null,
  check_state VARCHAR2(5) not null,
  clear_state VARCHAR2(5) not null,
  clear_date  VARCHAR2(10)
);
alter table ZJCG.CTP_CHEAR_INFO  add constraint PRIMARY_KEY unique (PASS_TYPE, CHECK_DATE);
desc ZJCG.CTP_CHEAR_INFO;


create table ZJCG.CTP_CHECK_FILE_TR
(
  seq_no       VARCHAR2(20) not null,
  trans_seq    VARCHAR2(60),
  busi_typ     VARCHAR2(20),
  service_type VARCHAR2(20),
  tran_amt     VARCHAR2(40),
  tran_stat    VARCHAR2(10),
  jf_acct_no   VARCHAR2(30),
  jf_org       VARCHAR2(30),
  org_no       VARCHAR2(30),
  org_agent    VARCHAR2(30),
  df_org       VARCHAR2(30),
  df_acct_no   VARCHAR2(30),
  czrq         VARCHAR2(30),
  czls         VARCHAR2(30),
  qz_date      VARCHAR2(30),
  qz_seq       VARCHAR2(30),
  host_date    VARCHAR2(30),
  host_seq     VARCHAR2(30),
  tran_date    VARCHAR2(30),
  channelcode  VARCHAR2(30),
  is_check     VARCHAR2(10) default 0,
  is_center    VARCHAR2(10) default 1,
  check_date   VARCHAR2(30),
  check_time   VARCHAR2(30),
  update_time  VARCHAR2(30),
  is_have      VARCHAR2(30) default 0,
  reseve       VARCHAR2(30),
  tran_typ     VARCHAR2(10)
);
desc ZJCG.CTP_CHECK_FILE_TR;

create index ZJCG.N_CHECK_TR on ZJCG.CTP_CHECK_FILE_TR (TRANS_SEQ, SEQ_NO);
create index ZJCG.N_CTP_CHECK on ZJCG.CTP_CHECK_FILE_TR (TRAN_DATE);
create index ZJCG.N_CTP_CHECK_FILE on ZJCG.CTP_CHECK_FILE_TR (HOST_SEQ);
create index ZJCG.N_UPD_DATE on ZJCG.CTP_CHECK_FILE_TR (UPDATE_TIME, TRAN_TYP, IS_CHECK);


create table ZJCG.CTP_CHECK_KEY_SQL_INFO
(
  open_type            VARCHAR2(20) not null,
  local_key            VARCHAR2(400),
  lical_query_sql      VARCHAR2(2000),
  lical_querycount_sql VARCHAR2(2000),
  constraint PK_OPEN_TYPE primary key (OPEN_TYPE)
);
desc ZJCG.CTP_CHECK_KEY_SQL_INFO;


create table ZJCG.CTP_CHECK_MSG
(
  tran_date VARCHAR2(10),
  tran_msg  VARCHAR2(50),
  stat      VARCHAR2(5),
  trans_seq VARCHAR2(50)
);
desc ZJCG.CTP_CHECK_MSG;


create table ZJCG.CTP_CHECK_ZIP_INFO
(
  ctp_id              VARCHAR2(10) not null,
  checking_oper_types VARCHAR2(200) not null,
  locla_path          VARCHAR2(100) not null,
  in_max              VARCHAR2(10),
  aim_ip              VARCHAR2(20),
  aim_pass            VARCHAR2(20),
  aim_user            VARCHAR2(40),
  aim_port            VARCHAR2(10),
  aim_encod           VARCHAR2(10),
  aim_path            VARCHAR2(200),
  stat                VARCHAR2(5),
  direction           VARCHAR2(10),
  account             VARCHAR2(32),
  report_path         VARCHAR2(200),
  sep_flag            VARCHAR2(5) default '1',
  constraint PK_ID primary key (CTP_ID)
);
desc ZJCG.CTP_CHECK_ZIP_INFO;


create table ZJCG.CTP_CLEAR_TRAN_TYP_POOL
(
  tran_typ      VARCHAR2(5) not null,
  fee_key       VARCHAR2(5) not null,
  fee_type      VARCHAR2(20),
  fee_type_name VARCHAR2(20),
  busi_type     VARCHAR2(20),
  sub_busi_type VARCHAR2(20),
  dr_act_submry VARCHAR2(20),
  cr_act_submry VARCHAR2(20),
  constraint PK_POLL primary key (TRAN_TYP, FEE_KEY)
);
desc ZJCG.CTP_CLEAR_TRAN_TYP_POOL;


create table ZJCG.CTP_COMMON_TR
(
  tran_seq            VARCHAR2(60) not null,
  ctp_seq             VARCHAR2(60),
  ctp_id              VARCHAR2(10),
  ctp_date            VARCHAR2(10),
  trans_code          VARCHAR2(60),
  device_no           VARCHAR2(10),
  org_no              VARCHAR2(10),
  tell_no             VARCHAR2(10),
  legal_no            VARCHAR2(20),
  table_id            VARCHAR2(200),
  stat                VARCHAR2(5),
  ebuser_no           VARCHAR2(60),
  ebuser_typ          VARCHAR2(5),
  app_type            VARCHAR2(20),
  tran_date           VARCHAR2(10),
  tran_time           VARCHAR2(10),
  tran_time_consuming INTEGER,
  trans_url           VARCHAR2(200),
  h_rsp_code          VARCHAR2(200),
  h_rsp_msg           VARCHAR2(1000),
  constraint PPPP_CTP_COMMON_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_COMMON_TR;
create index ZJCG.PK_CTP_COMMON_TR on ZJCG.CTP_COMMON_TR (CTP_SEQ, APP_TYPE, TRANS_CODE, CTP_DATE);


create table ZJCG.CTP_COMPENT_TR
(
  ctp_id    VARCHAR2(10) not null,
  tran_seq  VARCHAR2(30) not null,
  ctp_seq   VARCHAR2(60),
  ebuser_no VARCHAR2(20) not null,
  id_no     VARCHAR2(30),
  id_typ    VARCHAR2(5),
  id_name   VARCHAR2(90),
  tot_num   VARCHAR2(22),
  tot_amt   NUMBER(18,4),
  synz_url  VARCHAR2(200),
  back_url  VARCHAR2(200),
  host_seq  VARCHAR2(20),
  host_code VARCHAR2(60),
  host_msg  VARCHAR2(200),
  tran_stat VARCHAR2(5),
  tran_date VARCHAR2(20),
  tran_time VARCHAR2(20),
  upd_date  VARCHAR2(20),
  upd_time  VARCHAR2(20),
  is_synz   VARCHAR2(5),
  synz_rate VARCHAR2(5),
  ctp_date  VARCHAR2(30),
  constraint PK_CTP_COMPENT_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_COMPENT_TR;
create index ZJCG.N_CTP_COMPENT_TR on ZJCG.CTP_COMPENT_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_COMP_QUAIL_INFO
(
  uni_no                   VARCHAR2(60) not null,
  comp_name                VARCHAR2(300) not null,
  id_typ                   VARCHAR2(5) not null,
  id_no                    VARCHAR2(50) not null,
  comp_phone               VARCHAR2(20) not null,
  indu_cate                VARCHAR2(200),
  reg_amt                  VARCHAR2(22),
  comp_prop                VARCHAR2(200),
  legal_name               VARCHAR2(300),
  legal_id_typ             VARCHAR2(5),
  legal_id_no              VARCHAR2(30),
  legal_phone              VARCHAR2(20),
  legal_path               VARCHAR2(200),
  comp_org                 VARCHAR2(50),
  comp_org_path            VARCHAR2(200),
  comp_licen               VARCHAR2(50),
  comp_licen_path          VARCHAR2(200),
  irs_regist               VARCHAR2(50),
  irs_id_no                VARCHAR2(50),
  irs_path                 VARCHAR2(200),
  land_tax_regist          VARCHAR2(50),
  land_tax_id_no           VARCHAR2(50),
  land_tax_path            VARCHAR2(200),
  app_no                   VARCHAR2(50),
  app_path                 VARCHAR2(200),
  inst_credit              VARCHAR2(50),
  inst_credit_path         VARCHAR2(200),
  inst_credit_date         VARCHAR2(8),
  inst_credit_end_date     VARCHAR2(8),
  is_eacct_sign            VARCHAR2(5),
  synz_url                 VARCHAR2(200),
  is_synz                  VARCHAR2(5),
  synz_rate                NUMBER,
  ctp_tran_seq             VARCHAR2(60),
  submit_teller            VARCHAR2(20),
  submit_date              VARCHAR2(10),
  submit_time              VARCHAR2(8),
  upd_teller               VARCHAR2(20),
  upd_date                 VARCHAR2(10),
  upd_time                 VARCHAR2(8),
  org_no                   VARCHAR2(10) not null,
  ctp_id                   VARCHAR2(10) not null,
  uni_stat                 VARCHAR2(5) not null,
  is_eacct_certify         VARCHAR2(5),
  eacct_no                 VARCHAR2(32),
  eacct_type               VARCHAR2(5),
  customerid               VARCHAR2(20),
  back_url                 VARCHAR2(200),
  is_active_sync           VARCHAR2(5),
  synz_active_rate         NUMBER,
  is_open_sync             VARCHAR2(5),
  synz_open_rate           NUMBER,
  audit_opinion            VARCHAR2(200),
  three_in_one             VARCHAR2(5),
  trans_url                VARCHAR2(200),
  is_inner_cust            VARCHAR2(5),
  tran_seq                 VARCHAR2(100),
  is_par_comp              VARCHAR2(5),
  par_comp_name            VARCHAR2(300),
  par_id_typ               VARCHAR2(5),
  par_id_no                VARCHAR2(50),
  par_comp_phone           VARCHAR2(20),
  par_indu_cate            VARCHAR2(6),
  par_reg_amt              VARCHAR2(16),
  par_legal_name           VARCHAR2(300),
  par_legal_id_typ         VARCHAR2(5),
  par_legal_id_no          VARCHAR2(30),
  par_legal_phone          VARCHAR2(20),
  par_legal_path           VARCHAR2(200),
  par_comp_org             VARCHAR2(50),
  par_comp_org_path        VARCHAR2(200),
  par_comp_licen           VARCHAR2(50),
  par_comp_licen_path      VARCHAR2(200),
  par_irs_regist           VARCHAR2(50),
  par_irs_path             VARCHAR2(200),
  par_land_tax_regist      VARCHAR2(50),
  par_land_tax_path        VARCHAR2(200),
  par_app_no               VARCHAR2(50),
  par_app_path             VARCHAR2(200),
  par_inst_credit          VARCHAR2(50),
  par_inst_credit_path     VARCHAR2(200),
  par_three_in_one         VARCHAR2(5),
  par_inst_credit_date     VARCHAR2(8),
  par_inst_credit_end_date VARCHAR2(8),
  acct_no                  VARCHAR2(32),
  pass_id                  VARCHAR2(60),
  ori_trans_seq            VARCHAR2(60),
  is_update                VARCHAR2(5) default 'N',
  is_confirm_sync          VARCHAR2(5),
  sync_confirm_rate        NUMBER,
  user_id                  VARCHAR2(200),
  sl_open_sync             VARCHAR2(5),
  sl_open_date             NUMBER,
  cust_role                VARCHAR2(5),
  ctp_date                 VARCHAR2(18),
  is_cancel                VARCHAR2(5) default '0',
  cancel_date              VARCHAR2(10),
  cancel_time              VARCHAR2(10),
  do_flag                  VARCHAR2(10) default 'N' not null,
  mobile                   VARCHAR2(20), 
  constraint PK_CTP_COMP_QUAIL_INFO primary key (UNI_NO)
);
desc ZJCG.CTP_COMP_QUAIL_INFO;



create table ZJCG.CTP_COMP_UPDATE_INFO
(
  uni_no                   VARCHAR2(60) not null,
  comp_name                VARCHAR2(300) not null,
  id_typ                   VARCHAR2(5) not null,
  id_no                    VARCHAR2(50) not null,
  comp_phone               VARCHAR2(20) not null,
  indu_cate                VARCHAR2(200) not null,
  reg_amt                  VARCHAR2(16),
  comp_prop                VARCHAR2(200),
  legal_name               VARCHAR2(300),
  legal_id_typ             VARCHAR2(5),
  legal_id_no              VARCHAR2(30),
  legal_phone              VARCHAR2(20),
  legal_path               VARCHAR2(200),
  comp_org                 VARCHAR2(50),
  comp_org_path            VARCHAR2(200),
  comp_licen               VARCHAR2(50),
  comp_licen_path          VARCHAR2(200),
  irs_regist               VARCHAR2(50),
  irs_id_no                VARCHAR2(50),
  irs_path                 VARCHAR2(200),
  land_tax_regist          VARCHAR2(50),
  land_tax_id_no           VARCHAR2(50),
  land_tax_path            VARCHAR2(200),
  app_no                   VARCHAR2(50),
  app_path                 VARCHAR2(200),
  inst_credit              VARCHAR2(50),
  inst_credit_path         VARCHAR2(200),
  inst_credit_date         VARCHAR2(8),
  inst_credit_end_date     VARCHAR2(8),
  is_eacct_sign            VARCHAR2(5),
  synz_url                 VARCHAR2(200),
  is_synz                  VARCHAR2(5),
  synz_rate                NUMBER,
  ctp_tran_seq             VARCHAR2(60),
  submit_teller            VARCHAR2(20),
  submit_date              VARCHAR2(10),
  submit_time              VARCHAR2(8),
  upd_teller               VARCHAR2(20),
  upd_date                 VARCHAR2(10),
  upd_time                 VARCHAR2(8),
  org_no                   VARCHAR2(10) not null,
  ctp_id                   VARCHAR2(10) not null,
  uni_stat                 VARCHAR2(5) not null,
  is_eacct_certify         VARCHAR2(5),
  eacct_no                 VARCHAR2(32),
  eacct_type               VARCHAR2(5),
  customerid               VARCHAR2(20),
  back_url                 VARCHAR2(200),
  is_active_sync           VARCHAR2(5),
  synz_active_rate         NUMBER,
  is_open_sync             VARCHAR2(5),
  synz_open_rate           NUMBER,
  audit_opinion            VARCHAR2(200),
  three_in_one             VARCHAR2(5),
  trans_url                VARCHAR2(200),
  is_inner_cust            VARCHAR2(5),
  tran_seq                 VARCHAR2(100),
  is_par_comp              VARCHAR2(5),
  par_comp_name            VARCHAR2(300),
  par_id_typ               VARCHAR2(5),
  par_id_no                VARCHAR2(50),
  par_comp_phone           VARCHAR2(20),
  par_indu_cate            VARCHAR2(6),
  par_reg_amt              VARCHAR2(16),
  par_legal_name           VARCHAR2(300),
  par_legal_id_typ         VARCHAR2(5),
  par_legal_id_no          VARCHAR2(30),
  par_legal_phone          VARCHAR2(20),
  par_legal_path           VARCHAR2(200),
  par_comp_org             VARCHAR2(50),
  par_comp_org_path        VARCHAR2(200),
  par_comp_licen           VARCHAR2(50),
  par_comp_licen_path      VARCHAR2(200),
  par_irs_regist           VARCHAR2(50),
  par_irs_path             VARCHAR2(200),
  par_land_tax_regist      VARCHAR2(50),
  par_land_tax_path        VARCHAR2(200),
  par_app_no               VARCHAR2(50),
  par_app_path             VARCHAR2(200),
  par_inst_credit          VARCHAR2(50),
  par_inst_credit_path     VARCHAR2(200),
  par_three_in_one         VARCHAR2(5),
  par_inst_credit_date     VARCHAR2(8),
  par_inst_credit_end_date VARCHAR2(8),
  acct_no                  VARCHAR2(32),
  pass_id                  VARCHAR2(60),
  old_comp_name            VARCHAR2(300),
  old_id_typ               VARCHAR2(5),
  old_id_no                VARCHAR2(50)
);
desc ZJCG.CTP_COMP_UPDATE_INFO;


create table ZJCG.CTP_CONFIG_TR
(
  ctp_id    VARCHAR2(10) not null,
  ctp_name  VARCHAR2(60) not null,
  trans_url VARCHAR2(220) not null,
  flag      VARCHAR2(5) default '0' not null,
  constraint PK_CTP_CONFIG_TR primary key (CTP_ID, FLAG)
);
desc ZJCG.CTP_CONFIG_TR;


create table ZJCG.CTP_DF_TYP_CONF
(
  ctp_id          VARCHAR2(10) not null,
  ctp_typ         VARCHAR2(5) not null,
  ctp_typ_desc    VARCHAR2(300),
  stat            VARCHAR2(5),
  acct_no         VARCHAR2(60) not null,
  acct_name       VARCHAR2(300) not null,
  left_amt        NUMBER(18,4) not null,
  ctp_acct        VARCHAR2(60),
  ctp_acct_name   VARCHAR2(300),
  xabank_rec_acct VARCHAR2(60),
  xabank_rec_name VARCHAR2(300),
  tran_time       VARCHAR2(10),
  constraint PK_CTP_DF_CONF_TYP primary key (CTP_ID, CTP_TYP)
);
desc ZJCG.CTP_DF_TYP_CONF;


create table ZJCG.CTP_DYNAMIC_CODE
(
  chan_typ  VARCHAR2(8) not null,
  ebuser_no VARCHAR2(60) not null,
  ctp_id    VARCHAR2(10),
  ctp_seq   VARCHAR2(60),
  mobile_no VARCHAR2(11) not null,
  challenge VARCHAR2(100) not null,
  stat      VARCHAR2(5),
  cre_date  VARCHAR2(10),
  cre_time  VARCHAR2(8),
  constraint PK_CTP_DYNAMIC_CODE primary key (EBUSER_NO)
);
desc ZJCG.CTP_DYNAMIC_CODE;


create table ZJCG.CTP_EACCT_CHECK_BALANCE
(
  acct_no    VARCHAR2(32),
  acct_name  VARCHAR2(300),
  acct_bal   VARCHAR2(20),
  trans_date VARCHAR2(10)
);
desc ZJCG.CTP_EACCT_CHECK_BALANCE;


create table ZJCG.CTP_EACCT_TRANS_TR
(
  tran_seq   VARCHAR2(60) not null,
  ctp_seq_no VARCHAR2(60),
  host_no    VARCHAR2(60),
  host_no2   VARCHAR2(60),
  busi_type  VARCHAR2(5),
  ctp_id     VARCHAR2(10),
  ebuser_no  VARCHAR2(20),
  eacct_no   VARCHAR2(30),
  eacct_name VARCHAR2(150),
  tran_amt   NUMBER(18,4),
  tran_type  VARCHAR2(5),
  acct_no    VARCHAR2(30),
  acct_name  VARCHAR2(150),
  tran_date  VARCHAR2(10),
  tran_time  VARCHAR2(8),
  tran_state VARCHAR2(2),
  constraint PK_CTP_EACCT_TRANS_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_EACCT_TRANS_TR;


create table ZJCG.CTP_EXCH_CLEAR_DATE
(
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  clear_date  VARCHAR2(10) not null,
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  comple_date VARCHAR2(10),
  comple_time VARCHAR2(8),
  is_valid    VARCHAR2(5),
  constraint PK_CTP_EXCH_CLEAR_DATE primary key (CTP_ID, CTP_SEQ, CLEAR_DATE)
);
desc ZJCG.CTP_EXCH_CLEAR_DATE;


create table ZJCG.CTP_EXCH_DETAIL_TR
(
  seq_no        VARCHAR2(10) not null,
  tran_seq      VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10) not null,
  ctp_seq       VARCHAR2(60) not null,
  detail_seq    VARCHAR2(20),
  ebuser_no     VARCHAR2(20),
  mid_acct      VARCHAR2(32),
  mid_name      VARCHAR2(300),
  ele_acct_no   VARCHAR2(32),
  ele_acct_name VARCHAR2(300),
  stat          VARCHAR2(5),
  jd_flag       VARCHAR2(5),
  tran_amt      NUMBER(18,4),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(8),
  comple_date   VARCHAR2(10),
  comple_time   VARCHAR2(8),
  host_code     VARCHAR2(500),
  host_msg      VARCHAR2(500),
  constraint PK_CTP_EXCH_DETAIL_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_EXCH_DETAIL_TR;



create table ZJCG.CTP_EXCH_INTER_TR
(
  seq_no        VARCHAR2(10) not null,
  tran_seq      VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10) not null,
  ctp_seq       VARCHAR2(60) not null,
  detail_seq    VARCHAR2(20),
  ebuser_no     VARCHAR2(20),
  mid_acct      VARCHAR2(32),
  mid_name      VARCHAR2(300),
  ctp_acct      VARCHAR2(32),
  ctp_acct_name VARCHAR2(300),
  stat          VARCHAR2(5),
  tran_amt      NUMBER(18,4),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(8),
  comple_date   VARCHAR2(10),
  comple_time   VARCHAR2(8),
  host_code     VARCHAR2(500),
  host_msg      VARCHAR2(500),
  ctp_open_org  VARCHAR2(30),
  ctp_open_name VARCHAR2(150),
  bank_no       VARCHAR2(30),
  bankname      VARCHAR2(150),
  constraint PK_CTP_EXCH_INTER_TR primary key (SEQ_NO, CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_EXCH_INTER_TR;



create table ZJCG.CTP_EXCH_SINGLE_TR
(
  tran_seq      VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10) not null,
  ctp_seq       VARCHAR2(60) not null,
  sel_ebuser_no VARCHAR2(32),
  sel_acct      VARCHAR2(32),
  sel_name      VARCHAR2(32),
  sel_type      VARCHAR2(32),
  buy_ebuser_no VARCHAR2(32),
  buy_acct      VARCHAR2(32),
  buy_name      VARCHAR2(32),
  buy_type      VARCHAR2(32),
  stat          VARCHAR2(5),
  tran_amt      NUMBER(18,4),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(8),
  comple_date   VARCHAR2(10),
  comple_time   VARCHAR2(8),
  host_code     VARCHAR2(500),
  host_msg      VARCHAR2(500),
  constraint PK_CTP_EXCH_SINGLE_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_EXCH_SINGLE_TR;


create table ZJCG.CTP_EXCH_TYP_CONF
(
  ctp_id       VARCHAR2(10) not null,
  advance_acct VARCHAR2(30) not null,
  advance_name VARCHAR2(60),
  mid_acct     VARCHAR2(30) not null,
  mid_name     VARCHAR2(60),
  is_valid     VARCHAR2(5) default '0' not null,
  constraint PK_CTP_EXCH_TYP_CONF primary key (CTP_ID)
);
desc ZJCG.CTP_EXCH_TYP_CONF;


create table ZJCG.CTP_FEE_CLEAR
(
  ctp_id    VARCHAR2(10) not null,
  tran_typ  VARCHAR2(10) not null,
  type_one  NUMBER(18,4),
  org_one   VARCHAR2(8),
  type_two  NUMBER(18,4),
  org_two   VARCHAR2(8),
  acct_one  VARCHAR2(30),
  name_one  VARCHAR2(300),
  acct_two  VARCHAR2(30),
  name_two  VARCHAR2(300),
  type_thr  NUMBER(18,4),
  org_thr   VARCHAR2(8),
  acct_thr  VARCHAR2(30),
  name_thr  VARCHAR2(300),
  type_four NUMBER(18,4),
  org_four  VARCHAR2(8),
  acct_four VARCHAR2(30),
  name_four VARCHAR2(300)
);
desc ZJCG.CTP_FEE_CLEAR;


create table ZJCG.CTP_FEE_CONF
(
  ctp_id       VARCHAR2(10) not null,
  tran_type    VARCHAR2(3) not null,
  star_amt     NUMBER(18,4) not null,
  end_amt      NUMBER(18,4),
  fee_type     VARCHAR2(3),
  fee_amt      NUMBER(18,4),
  fee_bottom   NUMBER(18,4),
  fee_top      NUMBER(18,4),
  df_flag      VARCHAR2(2),
  df_acct      VARCHAR2(30),
  df_amt       NUMBER(18,4),
  df_acct_org  VARCHAR2(8),
  df_acct_name VARCHAR2(300),
  df_acct_typ  VARCHAR2(5)
);
desc ZJCG.CTP_FEE_CONF;


create table ZJCG.CTP_FEE_SHARE_CONF
(
  ctp_id     VARCHAR2(10) not null,
  pro_typ    VARCHAR2(10),
  ctp_typ    VARCHAR2(10),
  pro_id     VARCHAR2(60) not null,
  stat       VARCHAR2(5),
  fee_type   VARCHAR2(5) not null,
  acct_typ1  VARCHAR2(5),
  acct_no1   VARCHAR2(40),
  acct_name1 VARCHAR2(300),
  org_no1    VARCHAR2(10),
  fee_per1   NUMBER(18,4),
  acct_typ2  VARCHAR2(5),
  acct_no2   VARCHAR2(40),
  acct_name2 VARCHAR2(300),
  org_no2    VARCHAR2(10),
  fee_per2   NUMBER(18,4),
  acct_typ3  VARCHAR2(5),
  acct_no3   VARCHAR2(40),
  acct_name3 VARCHAR2(300),
  org_no3    VARCHAR2(10),
  fee_per3   NUMBER(18,4),
  constraint PK_CTP_FEE_SHARE_CONF primary key (CTP_ID, PRO_ID, FEE_TYPE)
);
desc ZJCG.CTP_FEE_SHARE_CONF;


create table ZJCG.CTP_FINAN_PLAN_INFO
(
  ctp_id          VARCHAR2(10) not null,
  ebuser_no       VARCHAR2(60) not null,
  sign_date       VARCHAR2(10),
  sign_time       VARCHAR2(10),
  end_date        VARCHAR2(10),
  package_id      VARCHAR2(100) not null,
  package_name    VARCHAR2(500),
  amt             NUMBER(18,4),
  fee_top         NUMBER(18,4),
  sub_acct_no     VARCHAR2(32),
  stat            VARCHAR2(5),
  max_date_stat   VARCHAR2(1),
  is_data_trans   VARCHAR2(5),
  data_trans_time VARCHAR2(30),
  constraint PK_CTP_FINAN_PLAN_INFO primary key (EBUSER_NO, CTP_ID, PACKAGE_ID)
);
desc ZJCG.CTP_FINAN_PLAN_INFO;


create table ZJCG.CTP_FINAN_PLAN_TR
(
  trans_seq    VARCHAR2(60),
  ctp_id       VARCHAR2(10) not null,
  ebuser_no    VARCHAR2(60) not null,
  sign_type    VARCHAR2(5),
  tran_date    VARCHAR2(10),
  tran_time    VARCHAR2(10),
  end_date     VARCHAR2(10),
  package_id   VARCHAR2(100) not null,
  package_name VARCHAR2(500),
  amt          NUMBER(18,4),
  tran_amt     NUMBER(18,4),
  fee_top      NUMBER(18,4),
  sub_acct_no  VARCHAR2(32),
  stat         VARCHAR2(5),
  ctp_seq      VARCHAR2(60),
  back_url     VARCHAR2(250),
  synz_url     VARCHAR2(250),
  is_synz      VARCHAR2(5),
  active_desc1 VARCHAR2(2000),
  active_acct1 VARCHAR2(60),
  active_name1 VARCHAR2(300),
  active_amt1  NUMBER(18,4),
  active_desc2 VARCHAR2(2000),
  active_acct2 VARCHAR2(60),
  active_name2 VARCHAR2(300),
  active_amt2  NUMBER(18,4),
  active_desc3 VARCHAR2(2000),
  active_acct3 VARCHAR2(60),
  active_name3 VARCHAR2(300),
  active_amt3  NUMBER(18,4),
  ac_stat1     VARCHAR2(10) default '0',
  ac_stat2     VARCHAR2(10) default '0',
  ac_stat3     VARCHAR2(10) default '0',
  check_date   VARCHAR2(50),
  check_time   VARCHAR2(50),
  max_date     VARCHAR2(10),
  query_num    VARCHAR2(5) default 0,
  query_stat   VARCHAR2(5) default 'N',
  comp_date    VARCHAR2(10),
  comp_time    VARCHAR2(10),
  ctp_date     VARCHAR2(10)
);
desc ZJCG.CTP_FINAN_PLAN_TR;
create index ZJCG.N_CTP_DATE_FINAL on ZJCG.CTP_FINAN_PLAN_TR (CTP_ID, CTP_DATE);
create index ZJCG.N_CTP_FINAL_TR on ZJCG.CTP_FINAN_PLAN_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_FIN_BATCH_ACTIVE_TR
(
  trans_seq        VARCHAR2(60) not null,
  ctp_seq          VARCHAR2(60),
  ctp_id           VARCHAR2(10) not null,
  acct_no          VARCHAR2(60),
  mobile_no        VARCHAR2(20),
  id_name          VARCHAR2(300),
  id_no            VARCHAR2(60),
  id_typ           VARCHAR2(10),
  synz_url         VARCHAR2(600),
  back_url         VARCHAR2(600),
  ele_acct_no      VARCHAR2(60),
  trans_date       VARCHAR2(600),
  trans_time       VARCHAR2(600),
  synz_active_rate VARCHAR2(60) default 0,
  is_active_synz   VARCHAR2(60) default 'N',
  is_active_stat   VARCHAR2(60) default 'N',
  ebuser_no        VARCHAR2(60),
  cust_role        VARCHAR2(60) default 'A',
  ebuser_typ       VARCHAR2(1) default 'P'
);
desc ZJCG.CTP_FIN_BATCH_ACTIVE_TR;


create table ZJCG.CTP_GM_TRANS_IN_TR
(
  tran_seq    VARCHAR2(60) not null,
  gm_seq_no   VARCHAR2(60),
  tran_date   VARCHAR2(10) not null,
  tran_time   VARCHAR2(8) not null,
  h_org       VARCHAR2(10) not null,
  h_teller    VARCHAR2(10) not null,
  ebuser_no   VARCHAR2(50),
  tran_state  VARCHAR2(5) not null,
  acct_no     VARCHAR2(30),
  cif_no      VARCHAR2(20),
  ele_acct_no VARCHAR2(30),
  cust_amt    NUMBER(18,4),
  cust_fee    NUMBER(18,4),
  seq_no      VARCHAR2(60) not null,
  oper_type   VARCHAR2(1),
  ogi_stat    VARCHAR2(5) not null,
  now_stat    VARCHAR2(5)
);
desc ZJCG.CTP_GM_TRANS_IN_TR;


create table ZJCG.CTP_HOST_DATE
(
  host_date   VARCHAR2(10) not null,
  update_date VARCHAR2(10),
  update_time VARCHAR2(10)
);
desc ZJCG.CTP_HOST_DATE;


create table ZJCG.CTP_INDE_TR
(
  inde_tran_seq     VARCHAR2(60) not null,
  inde_tran_host_no VARCHAR2(60),
  inde_trans_code   VARCHAR2(100),
  inde_pay_acct     VARCHAR2(30),
  inde_pay_name     VARCHAR2(300),
  inde_rcv_acct     VARCHAR2(30),
  inde_rcv_name     VARCHAR2(300),
  inde_tran_amt     VARCHAR2(20),
  inde_tran_stat    VARCHAR2(5),
  inde_tran_date    VARCHAR2(10),
  inde_tran_time    VARCHAR2(10),
  inde_qz_date      VARCHAR2(10),
  inde_qz_seq       VARCHAR2(60),
  inde_host_date    VARCHAR2(10),
  inde_qz_stat      VARCHAR2(5)
);
desc ZJCG.CTP_INDE_TR;


create table ZJCG.CTP_INFO
(
  ctp_id           VARCHAR2(10) not null,
  ctp_name         VARCHAR2(150) not null,
  ctp_logo_b       VARCHAR2(150),
  ctp_logo_s       VARCHAR2(150),
  open_org         VARCHAR2(8),
  stat             VARCHAR2(2) not null,
  cif_no           VARCHAR2(20),
  id_no            VARCHAR2(50),
  id_type          VARCHAR2(5),
  is_own_temp      VARCHAR2(5),
  is_acct_notice   VARCHAR2(5),
  synz_url         VARCHAR2(200),
  clear_typ        VARCHAR2(5),
  ser_fee_acct     VARCHAR2(60),
  ser_fee_name     VARCHAR2(300),
  df_acct          VARCHAR2(60),
  is_random        VARCHAR2(2),
  open_name        VARCHAR2(200),
  check_type       VARCHAR2(2),
  card_type        VARCHAR2(5) default '01',
  account          VARCHAR2(40),
  is_mobile        VARCHAR2(5),
  mobile           VARCHAR2(30),
  is_has_admin     VARCHAR2(5),
  middle_acct_no   VARCHAR2(60),
  middle_acct_name VARCHAR2(300),
  ctp_back_url     VARCHAR2(100),
  is_auth          VARCHAR2(5),
  echeck_type      VARCHAR2(2),
  account_name     VARCHAR2(300),
  mobile_typ       VARCHAR2(1) default 'Y',
  trans_money      NUMBER(18,2),
  rebate_acct_no   VARCHAR2(40),
  rebate_acct_name VARCHAR2(300),
  is_ps_notice     VARCHAR2(5) default 'N',
  is_sign_mobile   VARCHAR2(10) default 'N' not null,
  constraint PK_CTP_INFO primary key (CTP_ID)
);
desc ZJCG.CTP_INFO;


create table ZJCG.CTP_INFORM_CONF
(
  ctp_id     VARCHAR2(10),
  trans_type VARCHAR2(2),
  synz_url   VARCHAR2(200),
  state      VARCHAR2(2)
);
desc ZJCG.CTP_INFORM_CONF;


create table ZJCG.CTP_INNER_ACCT_NUM
(
  tran_acct_no       VARCHAR2(30) not null,
  tran_date          VARCHAR2(30),
  tran_amt           VARCHAR2(30),
  note               VARCHAR2(30),
  trans_no           VARCHAR2(30),
  cust_no            VARCHAR2(30),
  trans_org          VARCHAR2(100),
  trans_teller       VARCHAR2(30),
  acc_bal            VARCHAR2(30),
  trans_time         VARCHAR2(30),
  trans_canal        VARCHAR2(30),
  enemy_acc_no       VARCHAR2(30),
  enemy_acc_name     VARCHAR2(100),
  enemy_bank_no      VARCHAR2(30),
  enemy_bank_name    VARCHAR2(100),
  trans_summary      VARCHAR2(100),
  trans_canal_name   VARCHAR2(100),
  trans_summary_name VARCHAR2(100),
  jd_flag            VARCHAR2(4)
);
desc ZJCG.CTP_INFORM_CONF;


create table ZJCG.CTP_INNER_ACCT_SUM
(
  tran_acct_no VARCHAR2(30) not null,
  tran_date    VARCHAR2(30),
  tot_amt      VARCHAR2(30),
  num_t        VARCHAR2(30),
  jd_flag      VARCHAR2(4)
);
desc ZJCG.CTP_INNER_ACCT_SUM;


create table ZJCG.CTP_INTER_ACCT
(
  ctp_acct      VARCHAR2(32) not null,
  ctp_acct_name VARCHAR2(150),
  ctp_open_org  VARCHAR2(30),
  ctp_open_name VARCHAR2(150),
  bank_no       VARCHAR2(30),
  bankname      VARCHAR2(150),
  acct_state    VARCHAR2(2),
  acct_right    VARCHAR2(5),
  pre_no        VARCHAR2(10),
  ctp_id        VARCHAR2(10),
  constraint PK_CTP_INTER_ACCT primary key (CTP_ACCT)
);
desc ZJCG.CTP_INTER_ACCT;


create table ZJCG.CTP_JF_TR
(
  ctp_id      VARCHAR2(10) not null,
  tran_seq    VARCHAR2(50) not null,
  recv_acct   VARCHAR2(50),
  tran_amt    VARCHAR2(50),
  ebuser_no   VARCHAR2(30),
  eacct_no    VARCHAR2(50),
  pro_typ     VARCHAR2(30),
  qd_url      VARCHAR2(100),
  hd_url      VARCHAR2(100),
  stat        VARCHAR2(100),
  recv_name   VARCHAR2(100),
  eacct_name  VARCHAR2(100),
  synz_url    VARCHAR2(200),
  order_no    VARCHAR2(50),
  trans_seq   VARCHAR2(50),
  cre_date    VARCHAR2(10),
  cre_time    VARCHAR2(10),
  is_sync     VARCHAR2(5),
  synz_rate   VARCHAR2(200),
  is_check    VARCHAR2(5) default '0',
  check_date  VARCHAR2(20),
  check_time  VARCHAR2(20),
  trans_url   VARCHAR2(200),
  tran_seq_no VARCHAR2(50),
  ctp_date    VARCHAR2(10),
  comp_date   VARCHAR2(10),
  comp_time   VARCHAR2(10),
  constraint PK_CTP_JF_TR primary key (CTP_ID, TRAN_SEQ)
);
desc ZJCG.CTP_JF_TR;
create index ZJCG.N_CTP_JF_TR on ZJCG.CTP_JF_TR (EBUSER_NO);
create index ZJCG.N_JF_DATE on ZJCG.CTP_JF_TR (CTP_ID, CTP_DATE);


create table ZJCG.CTP_JF_TYP_CONF
(
  ctp_id    VARCHAR2(10),
  ctp_typ   VARCHAR2(5),
  jf_desc   VARCHAR2(500),
  acct_no   VARCHAR2(30),
  acct_name VARCHAR2(100)
);
desc ZJCG.CTP_JF_TYP_CONF;


create table ZJCG.CTP_MER_AUTH_DATA
(
  trade_code VARCHAR2(50) not null,
  trade_name VARCHAR2(60) not null,
  trade_date VARCHAR2(15) not null,
  trade_time VARCHAR2(15) not null,
  trade_amt  NUMBER(18,4),
  trade_con  CLOB,
  sum_mer_id VARCHAR2(20) not null,
  rev_mer_id VARCHAR2(20),
  rev_result VARCHAR2(1),
  rev_date   VARCHAR2(15),
  rev_time   VARCHAR2(15),
  ref_reason VARCHAR2(300),
  trade_no   VARCHAR2(60),
  tran_seq   VARCHAR2(50) not null,
  oper_name  VARCHAR2(30),
  ctp_id     VARCHAR2(15),
  review_no  VARCHAR2(20),
  teller     VARCHAR2(20),
  org_no     VARCHAR2(20),
  trans_seq  VARCHAR2(60),
  constraint PK_CTP_MER_AUTH_DATA primary key (TRAN_SEQ)
);
desc ZJCG.CTP_MER_AUTH_DATA;


create table ZJCG.CTP_MIDDLE_TRANSFER_TR
(
  tran_seq      VARCHAR2(20) not null,
  ctp_id        VARCHAR2(20) not null,
  chan_typ      VARCHAR2(3),
  oper_type     VARCHAR2(2),
  fee_type      VARCHAR2(10) not null,
  pay_acct      VARCHAR2(20) not null,
  rec_acct      VARCHAR2(20) not null,
  total_fee     NUMBER(18,4) not null,
  host_code     VARCHAR2(100),
  host_msg      VARCHAR2(800),
  tran_date     VARCHAR2(10) not null,
  tran_time     VARCHAR2(10) not null,
  tran_state    VARCHAR2(2) not null,
  gg_branch_no  VARCHAR2(10) not null,
  seq_no        VARCHAR2(20) not null,
  order_date    VARCHAR2(10) not null,
  pro_id        VARCHAR2(80),
  rec_acct_name VARCHAR2(180),
  auth_tran_seq VARCHAR2(80),
  constraint PK_CTP_MIDDLE_TRANSFER_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_MIDDLE_TRANSFER_TR;


create table ZJCG.CTP_MOBILE_JUMP_INFO
(
  tran_seq          VARCHAR2(60) not null,
  ctp_id            VARCHAR2(10) not null,
  appversion        VARCHAR2(50),
  deviceid          VARCHAR2(50),
  phonedisplay      VARCHAR2(20),
  phoneip           VARCHAR2(20),
  phonemanufacturer VARCHAR2(50),
  phonemodel        VARCHAR2(20),
  providersname     VARCHAR2(30),
  systemversion     VARCHAR2(15),
  phone_flag        VARCHAR2(10),
  jump_date         VARCHAR2(10),
  jump_time         VARCHAR2(8),
  constraint PK_CTP_MOBILE_JUMP_INFO primary key (TRAN_SEQ)
);
desc ZJCG.CTP_MOBILE_JUMP_INFO;


create table ZJCG.CTP_NO_LOAN_TR
(
  trans_seq        VARCHAR2(50) not null,
  ctp_seq          VARCHAR2(50),
  ctp_id           VARCHAR2(10),
  refuse_msg       VARCHAR2(100),
  ebuser_typ       VARCHAR2(5),
  trans_date       VARCHAR2(10),
  trans_time       VARCHAR2(10),
  id_name          VARCHAR2(100),
  id_typ           VARCHAR2(5),
  id_no            VARCHAR2(100),
  pro_amt_new      VARCHAR2(20),
  balance_amt      VARCHAR2(20),
  history_loan_amt VARCHAR2(20)
);
desc ZJCG.CTP_NO_LOAN_TR;


create table ZJCG.CTP_ONLINE_CHECK_TR
(
  tran_seq   VARCHAR2(60) not null,
  ctp_id     VARCHAR2(10),
  org_no     VARCHAR2(10),
  cust_name  VARCHAR2(150),
  ret_name   VARCHAR2(150),
  id_typ     VARCHAR2(2),
  id_no      VARCHAR2(30),
  tran_date  VARCHAR2(10) not null,
  tran_time  VARCHAR2(8) not null,
  tran_state VARCHAR2(5) not null,
  ret_code   VARCHAR2(100),
  ret_msg    VARCHAR2(300)
);
desc ZJCG.CTP_ONLINE_CHECK_TR;


create table ZJCG.CTP_ONLINE_INFO
(
  ctp_id              VARCHAR2(10) not null,
  ctp_name            VARCHAR2(150) not null,
  ctp_online_date     VARCHAR2(20),
  ctp_online_time     VARCHAR2(20),
  contract_begin_date VARCHAR2(20),
  contract_begin_time VARCHAR2(20),
  contract_end_date   VARCHAR2(20),
  contract_end_time   VARCHAR2(20),
  comp_full_name      VARCHAR2(100),
  constraint PK_CTP_ONLINE_INFO primary key (CTP_ID)
);
desc ZJCG.CTP_ONLINE_INFO;


create table ZJCG.CTP_OPEN_MESS
(
  ctp_id           VARCHAR2(10) not null,
  id_typ           VARCHAR2(2) not null,
  id_no            VARCHAR2(30) not null,
  cust_name        VARCHAR2(200) not null,
  acct_no          VARCHAR2(32),
  mobile           VARCHAR2(30),
  currency         VARCHAR2(5),
  account_stat     VARCHAR2(2),
  err_mess         VARCHAR2(2000),
  export_data_date VARCHAR2(30),
  open_date        VARCHAR2(30),
  active_date      VARCHAR2(30),
  eacct_no         VARCHAR2(50),
  ebuser_no        VARCHAR2(50),
  ctp_uni_no       VARCHAR2(50),
  cif_no           VARCHAR2(50),
  constraint PK_CTP_OPEN_MESS primary key (ID_NO, ID_TYP, CUST_NAME)
);
alter table ZJCG.CTP_OPEN_MESS  add constraint UNQ_CTP_OPEN_MESS unique (CTP_UNI_NO);
desc ZJCG.CTP_OPEN_MESS;



create table ZJCG.CTP_OPEN_MESS_BAK
(
  ctp_id           VARCHAR2(10) not null,
  id_typ           VARCHAR2(2) not null,
  id_no            VARCHAR2(30) not null,
  cust_name        VARCHAR2(200) not null,
  acct_no          VARCHAR2(32),
  mobile           VARCHAR2(30),
  currency         VARCHAR2(5),
  account_stat     VARCHAR2(2),
  err_mess         VARCHAR2(2000),
  export_data_date VARCHAR2(30),
  open_date        VARCHAR2(30),
  active_date      VARCHAR2(30),
  eacct_no         VARCHAR2(50),
  ebuser_no        VARCHAR2(50),
  ctp_uni_no       VARCHAR2(50)
);
desc ZJCG.CTP_OPEN_MESS_BAK;


create table ZJCG.CTP_OPERATOR
(
  ctp_id          VARCHAR2(10) not null,
  oper_id         VARCHAR2(20) not null,
  lgn_id          VARCHAR2(50),
  lgn_pwd         VARCHAR2(50) not null,
  lgn_id_typ      VARCHAR2(5) not null,
  lgn_id_no       VARCHAR2(30) not null,
  oper_name       VARCHAR2(300) not null,
  oper_stat       VARCHAR2(5) not null,
  oper_mobile     VARCHAR2(30),
  today_err_count VARCHAR2(10),
  tot_err_cout    INTEGER,
  last_lgn_time   VARCHAR2(25),
  last_lgn_sess   VARCHAR2(500),
  freeze_date     VARCHAR2(10),
  freeze_res      VARCHAR2(10),
  user_type       VARCHAR2(4),
  is_admin        VARCHAR2(5) default 0,
  constraint PK_CTP_OPERATOR primary key (CTP_ID, OPER_ID)
);
desc ZJCG.CTP_OPERATOR;


create table ZJCG.CTP_OPERATOR_BAK
(
  ctp_id          VARCHAR2(10) not null,
  oper_id         VARCHAR2(20) not null,
  lgn_id          VARCHAR2(50),
  lgn_pwd         VARCHAR2(50) not null,
  lgn_id_typ      VARCHAR2(5) not null,
  lgn_id_no       VARCHAR2(30) not null,
  oper_name       VARCHAR2(300) not null,
  oper_stat       VARCHAR2(5) not null,
  oper_mobile     VARCHAR2(30),
  today_err_count VARCHAR2(10),
  tot_err_cout    INTEGER,
  last_lgn_time   VARCHAR2(25),
  last_lgn_sess   VARCHAR2(500),
  freeze_date     VARCHAR2(10),
  freeze_res      VARCHAR2(10)
);
desc ZJCG.CTP_OPERATOR_BAK;


create table ZJCG.CTP_OPER_LGN_TR
(
  tran_seq   VARCHAR2(60) not null,
  chan_typ   VARCHAR2(5) not null,
  ctp_id     VARCHAR2(10) not null,
  oper_id    VARCHAR2(20),
  lgn_ip     VARCHAR2(500),
  lgn_date   VARCHAR2(10) not null,
  lgn_time   VARCHAR2(8) not null,
  tran_state VARCHAR2(5) not null,
  session_id VARCHAR2(500),
  constraint PK_CTP_OPER_LGN_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_OPER_LGN_TR;


create table ZJCG.CTP_P2P_TYP_CONF
(
  ctp_id         VARCHAR2(10) not null,
  pro_typ        VARCHAR2(10) not null,
  pro_typ_name   VARCHAR2(100),
  db_flag        VARCHAR2(5),
  hk_flag        VARCHAR2(5),
  inter_top      NUMBER(18,4),
  fee_per_top    NUMBER(18,4),
  bargain_top    NUMBER(18,4),
  bargain_bottom NUMBER(18,4),
  valid_time     NUMBER(18),
  zr_valid       NUMBER(18),
  pro_typ_flag   VARCHAR2(5),
  constraint PK_CTP_P2P_TYP_CONF primary key (CTP_ID, PRO_TYP)
);
desc ZJCG.CTP_P2P_TYP_CONF;


create table ZJCG.CTP_PASS_TYPE_TR
(
  pass_type   VARCHAR2(20),
  update_date VARCHAR2(60),
  update_time VARCHAR2(60),
  check_flag  VARCHAR2(10) default 'N',
  clear_flag  VARCHAR2(2) default 'N',
  tran_typ    VARCHAR2(10),
  is_check    VARCHAR2(10) default 'N'
);
desc ZJCG.CTP_PASS_TYPE_TR;
create index ZJCG.N_CTP_PASS on ZJCG.CTP_PASS_TYPE_TR (PASS_TYPE, UPDATE_DATE, TRAN_TYP);


create table ZJCG.CTP_PP_ADD_TR
(
  ctp_seq             VARCHAR2(60) not null,
  ctp_id              VARCHAR2(10) not null,
  tran_seq            VARCHAR2(60) not null,
  pro_id              VARCHAR2(60) not null,
  pro_name            VARCHAR2(500),
  pro_typ             VARCHAR2(10),
  pro_amt             NUMBER(18,4) not null,
  risk_lev            VARCHAR2(200),
  pro_desc            VARCHAR2(2000),
  cre_date            VARCHAR2(10),
  cre_time            VARCHAR2(8),
  star_buy_time       VARCHAR2(20),
  end_buy_time        VARCHAR2(20),
  star_date           VARCHAR2(20),
  pro_term            VARCHAR2(5),
  expinc_rate         NUMBER(18,4),
  end_date            VARCHAR2(20),
  mj_name             VARCHAR2(300) not null,
  mj_id_typ           VARCHAR2(10) not null,
  mj_id_no            VARCHAR2(60) not null,
  mj_no               VARCHAR2(60),
  stat                VARCHAR2(10) not null,
  conf_date           VARCHAR2(10),
  conf_time           VARCHAR2(8),
  conf_result         VARCHAR2(5),
  zr_flag             VARCHAR2(5),
  dc_flag             VARCHAR2(5),
  is_synz             VARCHAR2(5),
  synz_rate           NUMBER,
  synz_url            VARCHAR2(200),
  back_url            VARCHAR2(200),
  last_synz_time      VARCHAR2(20),
  fee_per_top         NUMBER(18,4),
  ctp_send_fee        NUMBER(18,4),
  pro_min_amt         NUMBER(18,4),
  is_auto_bid         VARCHAR2(5),
  tot_fee             NUMBER(18,4),
  auto_repay_count    VARCHAR2(5),
  auto_repay_quota    NUMBER(18,4),
  auto_repay_end_date VARCHAR2(10),
  ctp_date            VARCHAR2(30),
  constraint PK_CTP_PP_ADD_TR primary key (CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_PP_ADD_TR;


create table ZJCG.CTP_PP_ADD_TR_RECV
(
  ctp_id      VARCHAR2(10) not null,
  ctp_seq     VARCHAR2(60) not null,
  pro_id      VARCHAR2(60),
  rcv_id_typ  VARCHAR2(10),
  rcv_id_no   VARCHAR2(60),
  rcv_name    VARCHAR2(300),
  rcv_no      VARCHAR2(60),
  fee_type1   VARCHAR2(2),
  fee_id_typ1 VARCHAR2(10),
  fee_id_no1  VARCHAR2(60),
  fee_name1   VARCHAR2(300),
  fee_no1     VARCHAR2(60),
  fee_acct1   VARCHAR2(32),
  fee_type2   VARCHAR2(2),
  fee_id_typ2 VARCHAR2(10),
  fee_id_no2  VARCHAR2(60),
  fee_name2   VARCHAR2(300),
  fee_no2     VARCHAR2(60),
  fee_acct2   VARCHAR2(32),
  fee_type3   VARCHAR2(2),
  fee_id_typ3 VARCHAR2(10),
  fee_id_no3  VARCHAR2(60),
  fee_name3   VARCHAR2(300),
  fee_no3     VARCHAR2(60),
  fee_acct3   VARCHAR2(32),
  amt         VARCHAR2(60),
  primary key (CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_PP_ADD_TR_RECV;


create table ZJCG.CTP_PP_AUTO_BID_INFO
(
  ctp_id        VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(60) not null,
  sign_date     VARCHAR2(10),
  sign_time     VARCHAR2(8),
  end_date      VARCHAR2(10),
  agree_no      VARCHAR2(20) not null,
  stat          VARCHAR2(5) not null,
  max_date      VARCHAR2(10),
  max_amt       VARCHAR2(24),
  max_count     VARCHAR2(24),
  use_count     VARCHAR2(24),
  is_data_trans VARCHAR2(5),
  constraint PK_CTP_PP_AUTO_BID_INFO primary key (EBUSER_NO)
);
desc ZJCG.CTP_PP_AUTO_BID_INFO;


create table ZJCG.CTP_PP_AUTO_BID_TR
(
  trans_seq VARCHAR2(60) not null,
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_typ  VARCHAR2(5) not null,
  tran_date VARCHAR2(10),
  tran_time VARCHAR2(8),
  end_time  VARCHAR2(8),
  agree_no  VARCHAR2(20),
  stat      VARCHAR2(5) not null,
  ctp_seq   VARCHAR2(60),
  ctp_date  VARCHAR2(30),
  constraint PK_CTP_PP_AUTO_BID_TR primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_AUTO_BID_TR;

create index ZJCG.AFFDSFE on ZJCG.CTP_PP_AUTO_BID_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_PP_AUTO_COMPENT_INFO
(
  ctp_id              VARCHAR2(10),
  pro_id              VARCHAR2(60),
  ebuser_no           VARCHAR2(60),
  auto_repay_count    VARCHAR2(5),
  auto_repay_quota    NUMBER(18,4),
  auto_repay_end_date VARCHAR2(10),
  stat                VARCHAR2(5),
  tran_date           VARCHAR2(10),
  tran_time           VARCHAR2(10),
  end_date            VARCHAR2(30),
  user_count          VARCHAR2(5),
  pro_typ             VARCHAR2(10),
  is_data_trans       VARCHAR2(10)
);
desc ZJCG.CTP_PP_AUTO_COMPENT_INFO;
create index ZJCG.FSAFASDFASDFASD on ZJCG.CTP_PP_AUTO_COMPENT_INFO (CTP_ID, PRO_ID, EBUSER_NO, PRO_TYP, STAT);


create table ZJCG.CTP_PP_AUTO_COMPENT_TR
(
  tran_seq            VARCHAR2(60) not null,
  ctp_seq             VARCHAR2(60) not null,
  ctp_id              VARCHAR2(10) not null,
  pro_id              VARCHAR2(60) not null,
  pro_typ             VARCHAR2(10) not null,
  ebuser_no           VARCHAR2(60) not null,
  oper_type           VARCHAR2(5) not null,
  auto_repay_count    VARCHAR2(5),
  auto_repay_quota    NUMBER(18,4),
  auto_repay_end_date VARCHAR2(10),
  tran_stat           VARCHAR2(5) not null,
  back_url            VARCHAR2(200),
  synz_url            VARCHAR2(200),
  is_synz             VARCHAR2(2),
  synz_rate           VARCHAR2(2),
  tran_date           VARCHAR2(10),
  tran_time           VARCHAR2(10),
  ctp_date            VARCHAR2(30),
  upd_date            VARCHAR2(10),
  upd_time            VARCHAR2(10),
  constraint PK_CTP_PP_AUTO_COMPENT_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_PP_AUTO_COMPENT_TR;
create index ZJCG.N_CTP_PP_AUTO_COMPENT_TR on ZJCG.CTP_PP_AUTO_COMPENT_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_PP_AUTO_CREDIT_INFO
(
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_date VARCHAR2(10),
  sign_time VARCHAR2(10),
  end_date  VARCHAR2(10),
  agree_no  VARCHAR2(20) not null,
  stat      VARCHAR2(5) not null,
  max_date  VARCHAR2(10),
  max_amt   VARCHAR2(24),
  max_count VARCHAR2(24),
  primary key (EBUSER_NO)
);
desc ZJCG.CTP_PP_AUTO_CREDIT_INFO;


create table ZJCG.CTP_PP_AUTO_CREDIT_TR
(
  trans_seq VARCHAR2(60) not null,
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_typ  VARCHAR2(5) not null,
  tran_date VARCHAR2(10),
  tran_time VARCHAR2(8),
  end_time  VARCHAR2(8),
  agree_no  VARCHAR2(20),
  stat      VARCHAR2(5) not null,
  ctp_seq   VARCHAR2(60),
  primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_AUTO_CREDIT_TR;


create table ZJCG.CTP_PP_AUTO_CREDIT_TRANSF_INFO
(
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_date VARCHAR2(10),
  sign_time VARCHAR2(10),
  end_date  VARCHAR2(10),
  agree_no  VARCHAR2(20) not null,
  stat      VARCHAR2(5) not null,
  primary key (EBUSER_NO)
);
desc ZJCG.CTP_PP_AUTO_CREDIT_TRANSF_INFO;


create table ZJCG.CTP_PP_AUTO_CREDIT_TRANSF_TR
(
  trans_seq VARCHAR2(60) not null,
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_typ  VARCHAR2(5) not null,
  tran_date VARCHAR2(10),
  tran_time VARCHAR2(8),
  end_time  VARCHAR2(8),
  agree_no  VARCHAR2(20),
  stat      VARCHAR2(5) not null,
  ctp_seq   VARCHAR2(60),
  primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_AUTO_CREDIT_TRANSF_TR;


create table ZJCG.CTP_PP_AUTO_TRANSF_INFO
(
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_date VARCHAR2(10),
  sign_time VARCHAR2(10),
  end_date  VARCHAR2(10),
  agree_no  VARCHAR2(20) not null,
  stat      VARCHAR2(5) not null,
  max_date  VARCHAR2(10),
  max_amt   VARCHAR2(24),
  max_count VARCHAR2(24),
  primary key (EBUSER_NO)
);
desc ZJCG.CTP_PP_AUTO_TRANSF_INFO;


create table ZJCG.CTP_PP_AUTO_TRANSF_TR
(
  trans_seq VARCHAR2(60) not null,
  ctp_id    VARCHAR2(10) not null,
  ebuser_no VARCHAR2(60) not null,
  sign_typ  VARCHAR2(5) not null,
  tran_date VARCHAR2(10),
  tran_time VARCHAR2(8),
  end_time  VARCHAR2(8),
  agree_no  VARCHAR2(20),
  stat      VARCHAR2(5) not null,
  ctp_seq   VARCHAR2(60),
  primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_AUTO_TRANSF_TR;


create table ZJCG.CTP_PP_BID_TR
(
  trans_seq   VARCHAR2(60) not null,
  ctp_id      VARCHAR2(10) not null,
  pro_id      VARCHAR2(60) not null,
  buy_no      VARCHAR2(60) not null,
  buy_acct    VARCHAR2(60) not null,
  buy_amt     NUMBER(18,4) not null,
  ret_code    VARCHAR2(500),
  ret_msg     VARCHAR2(500),
  submit_time VARCHAR2(20),
  comp_time   VARCHAR2(20),
  stat        VARCHAR2(5) not null,
  host_seq    VARCHAR2(60),
  is_check    VARCHAR2(5) default '0',
  check_date  VARCHAR2(20),
  check_time  VARCHAR2(20),
  ctp_seq     VARCHAR2(60),
  bid_amt     NUMBER(18,4),
  tran_typ    VARCHAR2(5),
  bid_seq_no  VARCHAR2(60),
  ctp_date    VARCHAR2(30),
  query_rate  NUMBER(5) default 0,
  constraint PK_CTP_PP_BID_TR primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_BID_TR;
create index ZJCG.N_PP_BID on ZJCG.CTP_PP_BID_TR (CTP_ID, CTP_DATE, STAT, SUBMIT_TIME);
create index ZJCG.N_USER_INFO on ZJCG.CTP_PP_BID_TR (BUY_NO);


create table ZJCG.CTP_PP_CANCEL_TR
(
  tran_seq      VARCHAR2(60),
  ctp_id        VARCHAR2(12) not null,
  ctp_seq       VARCHAR2(60) not null,
  pro_id        VARCHAR2(60),
  pro_typ       VARCHAR2(5),
  id_name       VARCHAR2(60),
  id_no         VARCHAR2(50),
  id_typ        VARCHAR2(5),
  cust_role     VARCHAR2(2),
  bid_seq_no    VARCHAR2(60),
  tran_stat     VARCHAR2(10),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(10),
  complete_date VARCHAR2(10),
  complete_time VARCHAR2(10),
  ctp_date      VARCHAR2(10),
  constraint PK_CANCEL_SEQ primary key (CTP_ID, CTP_SEQ)
);
alter table ZJCG.CTP_PP_CANCEL_TR  add constraint PK_CANCEL_TR unique (TRAN_SEQ);
desc ZJCG.CTP_PP_CANCEL_TR;


create table ZJCG.CTP_PP_CONFIRM_TR
(
  trans_seq     VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10) not null,
  bid_seq_no    VARCHAR2(60) not null,
  pro_id        VARCHAR2(60) not null,
  bider_no      VARCHAR2(60) not null,
  bider_acct    VARCHAR2(60) not null,
  bid_amt       NUMBER(18,4) not null,
  bid_stat      VARCHAR2(5) not null,
  bid_date      VARCHAR2(10),
  bid_time      VARCHAR2(8),
  last_date     VARCHAR2(10),
  last_time     VARCHAR2(8),
  bid_typ       VARCHAR2(5) not null,
  pay_seq       VARCHAR2(60),
  auto_seq      VARCHAR2(20),
  auto_stat     VARCHAR2(5),
  conf_seq      VARCHAR2(20),
  conf_stat     VARCHAR2(5),
  df_seq        VARCHAR2(20),
  df_stat       VARCHAR2(5),
  zr_seq        VARCHAR2(60),
  lock_flag     VARCHAR2(5) not null,
  canc_seq      VARCHAR2(60),
  host_seq      VARCHAR2(60),
  canc_typ      VARCHAR2(5),
  is_data_trans VARCHAR2(1),
  is_zr_stat    VARCHAR2(5) default '2',
  is_check      VARCHAR2(5) default '0',
  check_date    VARCHAR2(20),
  check_time    VARCHAR2(20),
  canc_check    VARCHAR2(20),
  ctp_date      VARCHAR2(30),
  hk_amt        NUMBER(18,4) default 0,
  constraint PK_CTP_PP_CONFIRM_TR primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_CONFIRM_TR;
create index ZJCG.INDEX_CTP_PP_CONFIRM_TR_NOTIFY on ZJCG.CTP_PP_CONFIRM_TR (CTP_ID, PRO_ID, BIDER_NO, BID_SEQ_NO);
create index ZJCG.N_CONF_PP_CONFIRM on ZJCG.CTP_PP_CONFIRM_TR (CONF_SEQ);
create index ZJCG.N_PP_CONFIRM on ZJCG.CTP_PP_CONFIRM_TR (DF_SEQ);


create table ZJCG.CTP_PP_CONFIRM_TR_BAK
(
  trans_seq     VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10) not null,
  bid_seq_no    VARCHAR2(60) not null,
  pro_id        VARCHAR2(60) not null,
  bider_no      VARCHAR2(60) not null,
  bider_acct    VARCHAR2(60) not null,
  bid_amt       NUMBER(18,4) not null,
  bid_stat      VARCHAR2(5) not null,
  bid_date      VARCHAR2(10),
  bid_time      VARCHAR2(8),
  last_date     VARCHAR2(10),
  last_time     VARCHAR2(8),
  bid_typ       VARCHAR2(5) not null,
  pay_seq       VARCHAR2(60),
  auto_seq      VARCHAR2(20),
  auto_stat     VARCHAR2(5),
  conf_seq      VARCHAR2(20),
  conf_stat     VARCHAR2(5),
  df_seq        VARCHAR2(20),
  df_stat       VARCHAR2(5),
  zr_seq        VARCHAR2(60),
  lock_flag     VARCHAR2(5) not null,
  canc_seq      VARCHAR2(60),
  host_seq      VARCHAR2(60),
  canc_typ      VARCHAR2(5),
  is_data_trans VARCHAR2(1),
  is_zr_stat    VARCHAR2(5),
  is_check      VARCHAR2(5),
  check_date    VARCHAR2(20),
  check_time    VARCHAR2(20),
  canc_check    VARCHAR2(20),
  ctp_date      VARCHAR2(30)
);
desc ZJCG.CTP_PP_CONFIRM_TR_BAK;


create table ZJCG.CTP_PP_INFO
(
  ctp_id              VARCHAR2(10) not null,
  pro_id              VARCHAR2(60) not null,
  pro_name            VARCHAR2(500) not null,
  pro_typ             VARCHAR2(10),
  pro_amt             NUMBER(18,4) not null,
  risk_lev            VARCHAR2(200),
  pro_desc            VARCHAR2(2000),
  cre_date            VARCHAR2(10),
  cre_time            VARCHAR2(8),
  star_buy_time       VARCHAR2(20),
  end_buy_time        VARCHAR2(20),
  star_date           VARCHAR2(20),
  end_date            VARCHAR2(20),
  pro_term            VARCHAR2(10),
  expinc_rate         NUMBER(18,4),
  mj_no               VARCHAR2(60) not null,
  mj_name             VARCHAR2(300) not null,
  mj_id_typ           VARCHAR2(10) not null,
  mj_id_no            VARCHAR2(60) not null,
  hk_flag             VARCHAR2(5),
  zr_flag             VARCHAR2(5),
  dc_flag             VARCHAR2(5),
  is_dc_flag          VARCHAR2(5),
  dc_amt              NUMBER(18,4),
  df_date             VARCHAR2(10),
  df_time             VARCHAR2(8),
  fee_per_top         NUMBER(18,4),
  buy_acct            VARCHAR2(60),
  buy_name            VARCHAR2(300),
  df_acct             VARCHAR2(60),
  df_name             VARCHAR2(300),
  stat                VARCHAR2(5) not null,
  fx_per_top          NUMBER(18,4),
  tot_fx_amt          NUMBER(18,4),
  agree_no            VARCHAR2(60),
  db_flag             VARCHAR2(5),
  left_amt            NUMBER(18,4),
  tot_num             NUMBER(18),
  conf_seq            VARCHAR2(60),
  conf_stat           VARCHAR2(5),
  df_seq              VARCHAR2(60),
  df_stat             VARCHAR2(5),
  fee_acct1           VARCHAR2(40),
  fee_name1           VARCHAR2(300),
  fee_amt1            NUMBER(18,4),
  fee_acct2           VARCHAR2(40),
  fee_name2           VARCHAR2(300),
  fee_amt2            NUMBER(18,4),
  fee_acct3           VARCHAR2(40),
  fee_name3           VARCHAR2(300),
  fee_amt3            NUMBER(18,4),
  cancel_seq          VARCHAR2(60),
  zr_fee_acct         VARCHAR2(32),
  db_amt              NUMBER(18,4),
  ctp_s_time          VARCHAR2(20),
  ctp_f_time          VARCHAR2(20),
  ctp_repay_time      VARCHAR2(20),
  ctp_cash_time       VARCHAR2(20),
  is_data_trans       VARCHAR2(1),
  pro_min_amt         NUMBER(18,4),
  fee_acct4           VARCHAR2(40),
  fee_name4           VARCHAR2(300),
  fee_amt4            NUMBER(18,4),
  fee_acct5           VARCHAR2(40),
  fee_name5           VARCHAR2(300),
  fee_amt5            NUMBER(18,4),
  lock_flag           VARCHAR2(5) default '0',
  err_msg             VARCHAR2(512),
  hk_amt              NUMBER(18,4) default 0,
  is_auto_bid         VARCHAR2(5),
  tot_fee             NUMBER(18,4),
  auto_repay_count    VARCHAR2(5),
  auto_repay_quota    NUMBER(18,4),
  auto_repay_end_date VARCHAR2(10),
  hk_corpus           NUMBER(18,4),
  hk_accrual          NUMBER(18,4),
  dcch_amt            NUMBER(18,4),
  constraint PK_CTP_PP_INFO primary key (CTP_ID, PRO_ID)
);
desc ZJCG.CTP_PP_INFO;
create index ZJCG.ADFADFADF on ZJCG.CTP_PP_INFO (CTP_ID, STAT);
create index ZJCG.AFDAFADFASDF on ZJCG.CTP_PP_INFO (MJ_NO);


create table ZJCG.CTP_PP_INFO_RECV
(
  ctp_id       VARCHAR2(10) not null,
  pro_id       VARCHAR2(60) not null,
  rcv_id_typ1  VARCHAR2(10),
  rcv_id_no1   VARCHAR2(60),
  rcv_name1    VARCHAR2(300),
  rcv_amt1     NUMBER(18,4),
  rcv_no1      VARCHAR2(60),
  rcv_id_typ2  VARCHAR2(10),
  rcv_id_no2   VARCHAR2(60),
  rcv_name2    VARCHAR2(300),
  rcv_no2      VARCHAR2(60),
  rcv_amt2     NUMBER(18,4),
  rcv_id_typ3  VARCHAR2(10),
  rcv_id_no3   VARCHAR2(60),
  rcv_name3    VARCHAR2(300),
  rcv_no3      VARCHAR2(60),
  rcv_amt3     NUMBER(18,4),
  rcv_id_typ4  VARCHAR2(10),
  rcv_id_no4   VARCHAR2(60),
  rcv_name4    VARCHAR2(300),
  rcv_no4      VARCHAR2(60),
  rcv_amt4     NUMBER(18,4),
  rcv_status1  VARCHAR2(5),
  rcv_status2  VARCHAR2(5),
  rcv_status3  VARCHAR2(5),
  rcv_status4  VARCHAR2(5),
  extend_field VARCHAR2(200),
  primary key (CTP_ID, PRO_ID)
);
desc ZJCG.CTP_PP_INFO_RECV;


create table ZJCG.CTP_PP_ORDER_TR
(
  ctp_id         VARCHAR2(10) not null,
  tran_seq       VARCHAR2(60),
  ctp_seq        VARCHAR2(60) not null,
  bid_seq_no     VARCHAR2(60) not null,
  bider_no       VARCHAR2(60) not null,
  bider_name     VARCHAR2(300) not null,
  bider_id_typ   VARCHAR2(10) not null,
  bider_id_no    VARCHAR2(60) not null,
  bider_acct     VARCHAR2(60),
  pro_id         VARCHAR2(60) not null,
  bid_amt        NUMBER(18,4) not null,
  tran_amt       NUMBER(18,4),
  active_desc1   VARCHAR2(2000),
  active_acct1   VARCHAR2(60),
  active_name1   VARCHAR2(300),
  active_amt1    NUMBER(18,4),
  active_desc2   VARCHAR2(2000),
  active_acct2   VARCHAR2(60),
  active_name2   VARCHAR2(300),
  active_amt2    NUMBER(18,4),
  active_desc3   VARCHAR2(2000),
  active_acct3   VARCHAR2(60),
  active_name3   VARCHAR2(300),
  active_amt3    NUMBER(18,4),
  is_synz        VARCHAR2(5),
  synz_rate      NUMBER,
  synz_url       VARCHAR2(200),
  back_url       VARCHAR2(200),
  last_synz_time VARCHAR2(20),
  stat           VARCHAR2(5),
  cre_date       VARCHAR2(10),
  cre_time       VARCHAR2(8),
  ac_stat1       VARCHAR2(8) default '0',
  ac_stat2       VARCHAR2(8) default '0',
  ac_stat3       VARCHAR2(8) default '0',
  ctp_date       VARCHAR2(30),
  constraint PK_CTP_PP_ORDER_TR primary key (CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_PP_ORDER_TR;
create index ZJCG.ASDFG on ZJCG.CTP_PP_ORDER_TR (CTP_ID, BID_SEQ_NO, PRO_ID);
create index ZJCG.N_DATE_ORDER on ZJCG.CTP_PP_ORDER_TR (CTP_ID, CTP_DATE);


create table ZJCG.CTP_PP_REPAY_REQ_TR
(
  tran_seq      VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10) not null,
  pro_id        VARCHAR2(60) not null,
  pro_typ       VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(30) not null,
  is_last_term  VARCHAR2(2) not null,
  id_name       VARCHAR2(60),
  id_typ        VARCHAR2(5),
  id_no         VARCHAR2(30),
  cust_role     VARCHAR2(5),
  compen_back   VARCHAR2(2),
  seq_no        VARCHAR2(8),
  tot_num       VARCHAR2(22) not null,
  tot_amt       NUMBER(18,4) not null,
  file_content  CLOB,
  fee_tot_num   VARCHAR2(22),
  fee_tot_amt   NUMBER(18,4),
  file_content1 CLOB,
  synz_url      VARCHAR2(200),
  back_url      VARCHAR2(200),
  ctp_date      VARCHAR2(30),
  ctp_seq       VARCHAR2(60),
  tran_stat     VARCHAR2(5),
  tran_date     VARCHAR2(30),
  constraint PK_CTP_PP_REPAY_REQ_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_PP_REPAY_REQ_TR;



create table ZJCG.CTP_PP_REPAY_TR
(
  tran_seq        VARCHAR2(60) not null,
  ctp_id          VARCHAR2(10) not null,
  pro_id          VARCHAR2(60) not null,
  pay_no          VARCHAR2(60),
  pay_acct        VARCHAR2(40),
  pay_name        VARCHAR2(300),
  tran_amt        NUMBER(18,4),
  rcv_no          VARCHAR2(60),
  rcv_acct        VARCHAR2(40),
  rcv_name        VARCHAR2(300),
  stat            VARCHAR2(5) not null,
  host_code       VARCHAR2(500),
  host_msg        VARCHAR2(500),
  is_check        VARCHAR2(5) default '0',
  check_date      VARCHAR2(20),
  check_time      VARCHAR2(20),
  error_msg       VARCHAR2(500),
  ctp_seq         VARCHAR2(60),
  ctp_date        VARCHAR2(30),
  tran_date       VARCHAR2(10),
  tran_time       VARCHAR2(10),
  query_rate      VARCHAR2(10) default 0,
  comp_date       VARCHAR2(10),
  comp_time       VARCHAR2(10),
  compen_capital  VARCHAR2(22),
  compen_interest VARCHAR2(22),
  compen_fee      VARCHAR2(22),
  fee_acct        VARCHAR2(40),
  fee_name        VARCHAR2(60),
  fee_is_check    VARCHAR2(5) default '0',
  fee_check_date  VARCHAR2(20),
  fee_check_time  VARCHAR2(20),
  constraint PK_CTP_PP_REPAY_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_PP_REPAY_TR;


create table ZJCG.CTP_PP_RETURN_TR
(
  tran_seq  VARCHAR2(60),
  ctp_id    VARCHAR2(20),
  pro_id    VARCHAR2(60),
  pro_typ   VARCHAR2(10),
  acct_no   VARCHAR2(30),
  acct_name VARCHAR2(100),
  tran_amt  NUMBER(18,4),
  tran_stat VARCHAR2(10),
  ctp_seq   VARCHAR2(60),
  tran_date VARCHAR2(30),
  tran_time VARCHAR2(30),
  fee_typ   VARCHAR2(30)
);
desc ZJCG.CTP_PP_RETURN_TR;


create table ZJCG.CTP_PP_ZR_ORDER_TR
(
  trans_seq      VARCHAR2(60) not null,
  ctp_id         VARCHAR2(10) not null,
  ctp_seq        VARCHAR2(60),
  pro_id         VARCHAR2(60) not null,
  ori_bid_seq_no VARCHAR2(60) not null,
  bid_seq_no     VARCHAR2(60),
  zr_ebuser_no   VARCHAR2(60),
  zr_acct        VARCHAR2(60),
  zr_name        VARCHAR2(300),
  zr_id_typ      VARCHAR2(10),
  zr_id_no       VARCHAR2(60),
  bid_amt        NUMBER(18,4),
  tran_amt       NUMBER(18,4),
  tot_amt        NUMBER(18,4),
  zr_date        VARCHAR2(10),
  zr_time        VARCHAR2(8),
  zr_typ         VARCHAR2(5),
  stat           VARCHAR2(5),
  buy_ebuser_no  VARCHAR2(60),
  buy_acct       VARCHAR2(60),
  buy_name       VARCHAR2(300),
  buy_id_typ     VARCHAR2(10),
  buy_id_no      VARCHAR2(60),
  ret_code       VARCHAR2(500),
  ret_msg        VARCHAR2(500),
  host_seq       VARCHAR2(60),
  host_seq1      VARCHAR2(60),
  host_seq2      VARCHAR2(60),
  active_acct1   VARCHAR2(60),
  active_name1   VARCHAR2(300),
  active_amt1    NUMBER(18,4),
  active_acct2   VARCHAR2(60),
  active_name2   VARCHAR2(300),
  active_amt2    NUMBER(18,4),
  is_synz        VARCHAR2(5),
  synz_url       VARCHAR2(200),
  back_url       VARCHAR2(200),
  synz_rate      NUMBER,
  last_synz_time VARCHAR2(20),
  fee_acct1      VARCHAR2(40),
  fee_name1      VARCHAR2(300),
  fee_amt1       NUMBER(18,4),
  fee_acct2      VARCHAR2(40),
  fee_name2      VARCHAR2(300),
  fee_amt2       NUMBER(18,4),
  fee_acct3      VARCHAR2(40),
  fee_name3      VARCHAR2(300),
  fee_amt3       NUMBER(18,4),
  is_check       VARCHAR2(5) default '0',
  check_date     VARCHAR2(20),
  check_time     VARCHAR2(20),
  fee_stat1      VARCHAR2(5) default '0',
  fee_stat2      VARCHAR2(5) default '0',
  fee_stat3      VARCHAR2(5) default '0',
  zr_perc        VARCHAR2(20),
  ac_stat1       VARCHAR2(5) default '0',
  ac_stat2       VARCHAR2(5) default '0',
  ctp_date       VARCHAR2(30),
  comp_date      VARCHAR2(10),
  comp_time      VARCHAR2(10),
  pro_typ        VARCHAR2(10),
  bid_typ        VARCHAR2(10),
  constraint PK_CTP_PP_ZR_ORDER_TR primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_ZR_ORDER_TR;
create index ZJCG.ASDFDGSD on ZJCG.CTP_PP_ZR_ORDER_TR (CTP_ID, CTP_SEQ);
create index ZJCG.N_PP_ZR_ORDER on ZJCG.CTP_PP_ZR_ORDER_TR (CTP_ID, CTP_DATE);


create table ZJCG.CTP_PP_ZR_TR
(
  trans_seq      VARCHAR2(60) not null,
  ctp_seq        VARCHAR2(60) not null,
  ctp_id         VARCHAR2(10) not null,
  pro_id         VARCHAR2(60) not null,
  ori_bid_seq_no VARCHAR2(60),
  bid_seq_no     VARCHAR2(60),
  ori_reg_no     VARCHAR2(60),
  zr_reg_no      VARCHAR2(60),
  buy_reg_no     VARCHAR2(60),
  zr_ebuser_no   VARCHAR2(60) not null,
  zr_acct        VARCHAR2(60) not null,
  bid_amt        NUMBER(18,4) not null,
  tran_amt       NUMBER(18,4) not null,
  zr_date        VARCHAR2(10),
  zr_time        VARCHAR2(8),
  zr_typ         VARCHAR2(5),
  stat           VARCHAR2(5) not null,
  buy_ebuser_no  VARCHAR2(60) not null,
  buy_acct       VARCHAR2(60) not null,
  ret_code       VARCHAR2(500),
  ret_msg        VARCHAR2(500),
  is_check       VARCHAR2(5) default '0',
  check_date     VARCHAR2(20),
  check_time     VARCHAR2(20),
  ctp_date       VARCHAR2(30),
  query_rate     NUMBER(20) default 0,
  tran_type      VARCHAR2(5),
  constraint PK_CTP_PP_ZR_TR primary key (TRANS_SEQ)
);
desc ZJCG.CTP_PP_ZR_TR;
create index ZJCG.ASFDSFDSF on ZJCG.CTP_PP_ZR_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_PRO_CONFIRM_TR
(
  tran_seq    VARCHAR2(60),
  tran_date   VARCHAR2(10),
  tran_time   VARCHAR2(8),
  conf_seq    VARCHAR2(60),
  conf_date   VARCHAR2(10),
  conf_time   VARCHAR2(8),
  retu_seq    VARCHAR2(60),
  retu_date   VARCHAR2(10),
  retu_time   VARCHAR2(8),
  canc_seq    VARCHAR2(60),
  canc_date   VARCHAR2(10),
  canc_time   VARCHAR2(8),
  pro_id      VARCHAR2(60) not null,
  sell_ebuser VARCHAR2(30) not null,
  rcv_acct    VARCHAR2(40),
  rcv_name    VARCHAR2(300),
  buy_ebuser  VARCHAR2(30) not null,
  unit_price  NUMBER(18,4),
  tot_num     VARCHAR2(18),
  tran_amt    NUMBER(18,4),
  pay_acct    VARCHAR2(40),
  pay_name    VARCHAR2(300),
  tran_fee    NUMBER(18,4) not null,
  stat        VARCHAR2(5) not null,
  ctp_id      VARCHAR2(10) not null,
  ctp_typ     VARCHAR2(10),
  pro_typ     VARCHAR2(10),
  order_no    VARCHAR2(60) not null,
  cre_date    VARCHAR2(10),
  cre_time    VARCHAR2(8),
  fee_acct    VARCHAR2(40),
  fee_name    VARCHAR2(300),
  ctp_date    VARCHAR2(10),
  ctp_seq     VARCHAR2(60)
);
desc ZJCG.CTP_PRO_CONFIRM_TR;


create table ZJCG.CTP_PRO_FEE_CONF
(
  id         VARCHAR2(60) not null,
  ctp_id     VARCHAR2(10) not null,
  pro_typ    VARCHAR2(10),
  pro_id     VARCHAR2(60),
  star_amt   NUMBER(18,4) not null,
  end_amt    NUMBER(18,4) not null,
  fee_type   VARCHAR2(5) not null,
  fee_amt    NUMBER(18,4) not null,
  fee_bottom NUMBER(18,4),
  fee_top    NUMBER(18,4)
);
desc ZJCG.CTP_PRO_FEE_CONF;


create table ZJCG.CTP_PRO_INFO
(
  pro_id      VARCHAR2(60) not null,
  pro_name    VARCHAR2(60),
  ctp_typ     VARCHAR2(5),
  pro_typ     VARCHAR2(10),
  goods_id    VARCHAR2(60),
  goods_name  VARCHAR2(60),
  goods_desc  VARCHAR2(4000),
  channel_id  VARCHAR2(5),
  ebuser_no   VARCHAR2(30),
  stat        VARCHAR2(5),
  cre_date    VARCHAR2(10),
  cre_time    VARCHAR2(8),
  upd_date    VARCHAR2(10),
  upd_time    VARCHAR2(8),
  acct_middle VARCHAR2(60),
  kk_flag     VARCHAR2(5),
  ctp_id      VARCHAR2(60)
);
desc ZJCG.CTP_PRO_INFO;


create table ZJCG.CTP_PRO_TRANS_TR
(
  tran_seq  VARCHAR2(60) not null,
  tran_type VARCHAR2(5) not null,
  pro_id    VARCHAR2(60) not null,
  tran_amt  NUMBER(18,4) not null,
  stat      VARCHAR2(5) not null,
  fee_amt   NUMBER(18,4),
  ctp_id    VARCHAR2(10),
  ctp_typ   VARCHAR2(10),
  pro_typ   VARCHAR2(10),
  order_no  VARCHAR2(60),
  pay_acct  VARCHAR2(30),
  rcv_acct  VARCHAR2(30),
  pay_name  VARCHAR2(300),
  rcv_name  VARCHAR2(300)
);
desc ZJCG.CTP_PRO_TRANS_TR;


create table ZJCG.CTP_PRO_TYP_CONF
(
  ctp_id       VARCHAR2(10) not null,
  ctp_typ      VARCHAR2(5) not null,
  pro_typ      VARCHAR2(10) not null,
  pro_typ_desc VARCHAR2(300),
  stat         VARCHAR2(5)
);
desc ZJCG.CTP_PRO_TYP_CONF;


create table ZJCG.CTP_PWD_RESET
(
  ebuser_no  VARCHAR2(20),
  id_type    VARCHAR2(2),
  id_no      VARCHAR2(30),
  cust_name  VARCHAR2(100),
  acct_no    VARCHAR2(30),
  pay_acct   VARCHAR2(30),
  rcv_acct   VARCHAR2(30),
  tran_amt   NUMBER(18,4),
  new_mobile VARCHAR2(30),
  tran_date  VARCHAR2(30),
  tran_state VARCHAR2(2),
  tran_type  VARCHAR2(5),
  order_no   VARCHAR2(50),
  tran_time  VARCHAR2(30)
);
desc ZJCG.CTP_PWD_RESET;


create table ZJCG.CTP_QUAIL_CONF
(
  ctp_id     VARCHAR2(10) not null,
  ip         VARCHAR2(20),
  port       VARCHAR2(10),
  uname      VARCHAR2(20),
  pwd        VARCHAR2(20),
  file_path  VARCHAR2(300),
  encoding   VARCHAR2(20),
  upd_teller VARCHAR2(10),
  upd_date   VARCHAR2(10),
  upd_time   VARCHAR2(8),
  sec_type   VARCHAR2(1),
  constraint PK_CTP_QUAIL_CONF primary key (CTP_ID)
);
desc ZJCG.CTP_QUAIL_CONF;


create table ZJCG.CTP_REBATE_ACCT_INFO
(
  ctp_id            VARCHAR2(10) not null,
  ctp_name          VARCHAR2(150),
  rebate_acct_no1   VARCHAR2(40),
  rebate_acct_name1 VARCHAR2(300),
  rebate_acct_no2   VARCHAR2(40),
  rebate_acct_name2 VARCHAR2(300),
  constraint PK_CTP_REBATE_ACCT_INFO primary key (CTP_ID)
);
desc ZJCG.CTP_REBATE_ACCT_INFO;


create table ZJCG.CTP_REBATE_SIGN_INFO
(
  ctp_id           VARCHAR2(10) not null,
  ctp_acct         VARCHAR2(30) not null,
  rebate_acct_name VARCHAR2(300),
  rebate_acct_no   VARCHAR2(40) not null,
  rebate_type      VARCHAR2(5),
  pro_id           VARCHAR2(50),
  sign_date        VARCHAR2(10),
  sign_time        VARCHAR2(10),
  ctp_acct_name    VARCHAR2(150),
  stat             VARCHAR2(5),
  tran_seq         VARCHAR2(30),
  constraint PK_CTP_REBATE_SIGN_INFO primary key (CTP_ID, CTP_ACCT, REBATE_ACCT_NO)
);
desc ZJCG.CTP_REBATE_SIGN_INFO;


create table ZJCG.CTP_REGISTER_TR
(
  tran_seq         VARCHAR2(60) not null,
  ctp_seq_no       VARCHAR2(60),
  ctp_id           VARCHAR2(10),
  ebuser_no        VARCHAR2(20),
  busi_id          VARCHAR2(30),
  tran_date        VARCHAR2(10) not null,
  tran_time        VARCHAR2(8) not null,
  tran_state       VARCHAR2(2) not null,
  cust_name        VARCHAR2(200),
  ebuser_typ       VARCHAR2(1),
  id_typ           VARCHAR2(2),
  id_no            VARCHAR2(30),
  cif_no           VARCHAR2(20),
  eacct_no         VARCHAR2(30),
  acct_no          VARCHAR2(30),
  card_lev         VARCHAR2(10),
  bank_code        VARCHAR2(20),
  bank_name        VARCHAR2(150),
  ip               VARCHAR2(100),
  point_y          VARCHAR2(45),
  point_x          VARCHAR2(45),
  imei             VARCHAR2(100),
  open_flag        VARCHAR2(5),
  synz_url         VARCHAR2(200),
  is_synz          VARCHAR2(5),
  synz_rate        NUMBER(22),
  is_active_synz   VARCHAR2(5),
  synz_active_rate NUMBER(22),
  mobile           VARCHAR2(20)
);
desc ZJCG.CTP_REGISTER_TR;

create index ZJCG.N_CTP_REGISTER_TR on ZJCG.CTP_REGISTER_TR (CTP_SEQ_NO, CTP_ID);
alter table ZJCG.CTP_REGISTER_TR  add constraint UN_CTP_REGISTER_TR unique (TRAN_SEQ);


create table ZJCG.CTP_REQ_LOG_TR
(
  ctp_id     VARCHAR2(10) not null,
  ctp_seq    VARCHAR2(60) not null,
  inte_seq   VARCHAR2(60),
  trans_code VARCHAR2(100) not null,
  ctp_date   VARCHAR2(10) not null,
  req_msg    CLOB,
  req_date   VARCHAR2(10),
  req_time   VARCHAR2(8),
  res_body   CLOB,
  res_date   VARCHAR2(10),
  res_time   VARCHAR2(8),
  mer_pri    VARCHAR2(500),
  constraint PK_CTP_REQ_LOG_TR primary key (CTP_ID, CTP_SEQ)
);
desc ZJCG.CTP_REQ_LOG_TR;


create table ZJCG.CTP_SERVICE_DETAIL_REGISTER
(
  tran_seq   VARCHAR2(60) not null,
  ctp_id     VARCHAR2(10) not null,
  tran_type  VARCHAR2(5) not null,
  clear_typ  VARCHAR2(5) not null,
  tran_date  VARCHAR2(10) not null,
  fee_amt    NUMBER(18,4),
  cust_fee   NUMBER(18,4),
  ctp_fee    NUMBER(18,4),
  trans_flag VARCHAR2(5) not null,
  trans_seq  VARCHAR2(60),
  trans_time VARCHAR2(20),
  ser_fee    NUMBER(18,4) not null,
  clear_flag VARCHAR2(5) not null,
  clear_seq  VARCHAR2(60),
  clear_time VARCHAR2(20),
  pass_type  VARCHAR2(20) not null,
  cost_fee   NUMBER(18,4) not null,
  cost_flag  VARCHAR2(5) not null,
  cost_seq   VARCHAR2(60),
  cost_time  VARCHAR2(20),
  constraint PK_CTP_SERVICE_DETAIL_REGISTER primary key (TRAN_SEQ)
);
desc ZJCG.CTP_SERVICE_DETAIL_REGISTER;


create table ZJCG.CTP_SERVICE_REGISTER
(
  ctp_id    VARCHAR2(10) not null,
  tran_type VARCHAR2(20) not null,
  fee_date  VARCHAR2(10) not null,
  fee_amt   NUMBER(18,4) not null,
  stat      VARCHAR2(5) not null,
  pass_type VARCHAR2(20) not null,
  constraint PK_CTP_SERVICE_REGISTER primary key (CTP_ID, TRAN_TYPE, FEE_DATE, PASS_TYPE)
);
desc ZJCG.CTP_SERVICE_REGISTER;


create table ZJCG.CTP_SERVICE_TYPE_CONF
(
  busi_typ     VARCHAR2(10),
  service_type VARCHAR2(5),
  busi_desc    VARCHAR2(200),
  tran_typ     VARCHAR2(5),
  upd_date     VARCHAR2(10),
  file_flag    VARCHAR2(5),
  cfsq_flag    VARCHAR2(5)
);
alter table ZJCG.CTP_SERVICE_TYPE_CONF  add constraint UN_CTP_SERVICE_TYP unique (BUSI_TYP, SERVICE_TYPE);
desc ZJCG.CTP_SERVICE_TYPE_CONF;


create table ZJCG.CTP_SER_FEE_CONF
(
  ctp_id          VARCHAR2(10) not null,
  tran_type       VARCHAR2(5) not null,
  star_amt        NUMBER(18,4) not null,
  end_amt         NUMBER(18,4) not null,
  fee_type        VARCHAR2(5) not null,
  fee_amt         NUMBER(18,4) not null,
  fee_bottom      NUMBER(18,4),
  fee_top         NUMBER(18,4),
  cust_fee_top    NUMBER(18,4),
  cust_fee_bottom NUMBER(18,4),
  constraint PK_CTP_SER_FEE_CONF primary key (CTP_ID, TRAN_TYPE, STAR_AMT)
);
desc ZJCG.CTP_SER_FEE_CONF;


create table ZJCG.CTP_SIGN_PAYMENT
(
  review_no         VARCHAR2(60) not null,
  sign_type         VARCHAR2(2) not null,
  name              VARCHAR2(200) not null,
  id_typ            VARCHAR2(5),
  id_no             VARCHAR2(50),
  bind_acct         VARCHAR2(20) not null,
  bind_acct_name    VARCHAR2(20) not null,
  mobile            VARCHAR2(11),
  id_card_face_path VARCHAR2(200),
  id_card_back_path VARCHAR2(200),
  protocol_path     VARCHAR2(200),
  bind_acct_path    VARCHAR2(200),
  bus_license_path  VARCHAR2(200),
  status            VARCHAR2(2),
  review_msg        VARCHAR2(500),
  submit_teller     VARCHAR2(20),
  submit_date       VARCHAR2(20),
  submit_time       VARCHAR2(20),
  ctp_id            VARCHAR2(20) not null,
  pro_typ           VARCHAR2(20),
  take_pre_no       VARCHAR2(20),
  pb_protocol_no    VARCHAR2(30),
  acct_type         VARCHAR2(2),
  bank_no           VARCHAR2(30),
  constraint PK_CTP_SIGN_PAYMENT primary key (REVIEW_NO)
);
desc ZJCG.CTP_SIGN_PAYMENT;


create table ZJCG.CTP_STOCK_TRANSFER_INFO
(
  tran_seq       VARCHAR2(50) not null,
  ctp_id         VARCHAR2(10) not null,
  stat           VARCHAR2(100),
  id_typ         VARCHAR2(2),
  id_no          VARCHAR2(300),
  id_name        VARCHAR2(60),
  seller_id_typ  VARCHAR2(2),
  seller_id_no   VARCHAR2(300),
  seller_id_name VARCHAR2(60),
  tran_amt       NUMBER(22),
  transfer_desc  VARCHAR2(600),
  cre_date       VARCHAR2(10),
  cre_time       VARCHAR2(10),
  confirm_date   VARCHAR2(10),
  confirm_time   VARCHAR2(10),
  check_date     VARCHAR2(20),
  check_time     VARCHAR2(20),
  fee_acct1      VARCHAR2(40),
  fee_name1      VARCHAR2(300),
  cust_fee1      NUMBER(22),
  fee_acct2      VARCHAR2(40),
  fee_name2      VARCHAR2(300),
  cust_fee2      NUMBER(22),
  fee_acct3      VARCHAR2(40),
  fee_name3      VARCHAR2(300),
  cust_fee3      NUMBER(22),
  fee_acct4      VARCHAR2(40),
  fee_name4      VARCHAR2(300),
  cust_fee4      NUMBER(22),
  synz_url       VARCHAR2(200),
  back_url       VARCHAR2(200),
  is_sync        VARCHAR2(5),
  synz_rate      VARCHAR2(200),
  valid_time     VARCHAR2(20),
  trans_url      VARCHAR2(200),
  fee_flag1      VARCHAR2(5),
  fee_flag2      VARCHAR2(5),
  fee_flag3      VARCHAR2(5),
  fee_flag4      VARCHAR2(5),
  tran_msg       VARCHAR2(50),
  ctp_seq        VARCHAR2(30),
  tran_stat      VARCHAR2(10),
  trans_seq      VARCHAR2(50),
  fee_stat1      VARCHAR2(5) default '0',
  fee_stat2      VARCHAR2(5) default '0',
  fee_stat3      VARCHAR2(5) default '0',
  fee_stat4      VARCHAR2(5) default '0',
  is_check       VARCHAR2(5) default '0',
  primary key (TRAN_SEQ)
);
desc ZJCG.CTP_STOCK_TRANSFER_INFO;


create table ZJCG.CTP_SUB_ACCT
(
  ebuser_no     VARCHAR2(20) not null,
  acct_no       VARCHAR2(32) not null,
  acct_name     VARCHAR2(100),
  sub_acct_no   VARCHAR2(32) not null,
  open_date     VARCHAR2(10),
  open_time     VARCHAR2(10),
  ctp_id        VARCHAR2(10),
  acc_type      VARCHAR2(1),
  stat          VARCHAR2(1),
  close_date    VARCHAR2(10),
  close_time    VARCHAR2(10),
  is_data_trans VARCHAR2(10),
  constraint PK_CTP_SUB_ACCT primary key (EBUSER_NO, ACCT_NO, SUB_ACCT_NO)
);
desc ZJCG.CTP_SUB_ACCT;


create table ZJCG.CTP_SUB_AUTO_BID_INFO
(
  ctp_id        VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(60) not null,
  sign_date     VARCHAR2(10),
  sign_time     VARCHAR2(8),
  end_date      VARCHAR2(10),
  agree_no      VARCHAR2(20) not null,
  stat          VARCHAR2(5) not null,
  max_date      VARCHAR2(10),
  sub_acct_no   VARCHAR2(30) not null,
  is_data_trans VARCHAR2(5),
  constraint PK_CTP_SUB_AUTO_BID_INFO primary key (SUB_ACCT_NO)
);
desc ZJCG.CTP_SUB_AUTO_BID_INFO;


create table ZJCG.CTP_SUB_AUTO_CREDIT_INFO
(
  ctp_id        VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(60) not null,
  sign_date     VARCHAR2(10),
  sign_time     VARCHAR2(10),
  end_date      VARCHAR2(10),
  agree_no      VARCHAR2(20) not null,
  stat          VARCHAR2(5) not null,
  max_date      VARCHAR2(10),
  sub_acct_no   VARCHAR2(30) not null,
  is_data_trans VARCHAR2(5),
  primary key (SUB_ACCT_NO)
);
desc ZJCG.CTP_SUB_AUTO_CREDIT_INFO;


create table ZJCG.CTP_SUB_AUTO_TRANSF_INFO
(
  ctp_id        VARCHAR2(10) not null,
  ebuser_no     VARCHAR2(60) not null,
  sign_date     VARCHAR2(10),
  sign_time     VARCHAR2(10),
  end_date      VARCHAR2(10),
  agree_no      VARCHAR2(20) not null,
  stat          VARCHAR2(5) not null,
  max_date      VARCHAR2(10),
  sub_acct_no   VARCHAR2(30) not null,
  is_data_trans VARCHAR2(5),
  primary key (SUB_ACCT_NO)
);
desc ZJCG.CTP_SUB_AUTO_TRANSF_INFO;


create table ZJCG.CTP_SUB_TRANSFER_TR
(
  ctp_id         VARCHAR2(10) not null,
  ctp_trans_seq  VARCHAR2(150) not null,
  ebuser_no      VARCHAR2(20),
  tran_amt       NUMBER(18,4),
  cust_fee       NUMBER(18,4),
  ele_acct_no    VARCHAR2(32) not null,
  sub_account    VARCHAR2(32) not null,
  tran_stat      VARCHAR2(1),
  ebuser_type    VARCHAR2(1),
  tran_date      VARCHAR2(10),
  tran_time      VARCHAR2(10),
  tran_flag      VARCHAR2(1),
  tran_seq       VARCHAR2(150),
  check_date     VARCHAR2(20),
  check_time     VARCHAR2(20),
  fee_acct       VARCHAR2(100),
  fee_name       VARCHAR2(60),
  fee_acct1      VARCHAR2(60),
  fee_name1      VARCHAR2(100),
  cust_fee1      NUMBER(18,4),
  fee_acct2      VARCHAR2(60),
  fee_name2      VARCHAR2(100),
  cust_fee2      NUMBER(18,4),
  fee_acct3      VARCHAR2(60),
  fee_name3      VARCHAR2(100),
  cust_fee_tot   NUMBER(18,4),
  cust_fee3      NUMBER(18,4),
  cf_check_flag  VARCHAR2(5) default '0',
  ca_check_flag  VARCHAR2(5) default '0',
  cf_check_flag1 VARCHAR2(5) default '0',
  cf_check_flag2 VARCHAR2(5) default '0',
  cf_check_flag3 VARCHAR2(5) default '0',
  query_rate     VARCHAR2(10) default 0,
  ctp_date       VARCHAR2(10),
  comp_date      VARCHAR2(10),
  comp_time      VARCHAR2(10),
  constraint CTP_SUB_TRANSFER_TR_PK primary key (CTP_ID, CTP_TRANS_SEQ, ELE_ACCT_NO, SUB_ACCOUNT)
);
desc ZJCG.CTP_SUB_TRANSFER_TR;
create index ZJCG.N_CTP_DATE_SUBTRANS on ZJCG.CTP_SUB_TRANSFER_TR (CTP_ID, CTP_DATE);


create table ZJCG.CTP_SUPAY_TR
(
  tran_seq         VARCHAR2(60),
  busi_code        VARCHAR2(60),
  pass_type        VARCHAR2(10),
  tran_amt         VARCHAR2(60),
  tran_stat        VARCHAR2(60),
  tran_date        VARCHAR2(60),
  branch           VARCHAR2(60),
  acct_branch      VARCHAR2(60),
  h_rsp_code       VARCHAR2(60),
  h_rsp_msg        VARCHAR2(200),
  host_seq         VARCHAR2(30),
  busi_type        VARCHAR2(30),
  acct_no          VARCHAR2(30),
  acct_name        VARCHAR2(60),
  card_no          VARCHAR2(30),
  card_name        VARCHAR2(60),
  msg_seq_no       VARCHAR2(60),
  send_bank_code   VARCHAR2(60),
  send_settle_bank VARCHAR2(60),
  rcv_bank_code    VARCHAR2(60),
  rcv_settle_bank  VARCHAR2(60),
  is_check         VARCHAR2(10),
  check_date       VARCHAR2(60),
  tran_class       VARCHAR2(10),
  tran_type        VARCHAR2(60),
  service_seq      VARCHAR2(60)
);
desc ZJCG.CTP_SUPAY_TR;
create index ZJCG.N_CHECK on ZJCG.CTP_SUPAY_TR (TRAN_DATE, PASS_TYPE, IS_CHECK);
create index ZJCG.N_SUPAY_TR on ZJCG.CTP_SUPAY_TR (TRAN_SEQ);


create table ZJCG.CTP_SYSTEM_CONTROL
(
  host_date VARCHAR2(10),
  update_time VARCHAR2(10)
);
desc ZJCG.CTP_SYSTEM_CONTROL;


create table ZJCG.CTP_SYS_CONT
(
  id   VARCHAR2(5) not null,
  stat VARCHAR2(5),
  constraint PK primary key (ID)
);
desc ZJCG.CTP_SYS_CONT;


create table ZJCG.CTP_SYS_MENU
(
  chan_typ       VARCHAR2(3) not null,
  query_typ      VARCHAR2(1) not null,
  servie_typ     VARCHAR2(10) not null,
  menu_id        VARCHAR2(12) not null,
  menu_name      VARCHAR2(60) not null,
  par_menu_id    VARCHAR2(12),
  is_child       CHAR(1) not null,
  menu_url       VARCHAR2(100),
  menu_icon_path VARCHAR2(100),
  menu_sort      INTEGER
);
desc ZJCG.CTP_SYS_MENU;

create table ZJCG.CTP_TASK_EXECUTE_TR
(
  exe_seq_no     VARCHAR2(50) not null,
  trigger_name   VARCHAR2(100),
  job_name       VARCHAR2(100),
  exe_start_time VARCHAR2(30),
  exe_end_time   VARCHAR2(30),
  exe_use_time   VARCHAR2(20),
  exe_result     VARCHAR2(10),
  retcode        VARCHAR2(20),
  retmsg         VARCHAR2(2000),
  return_value   VARCHAR2(2000),
  constraint PK_CTP_TASK_TR primary key (EXE_SEQ_NO)
);
desc ZJCG.CTP_TASK_EXECUTE_TR;


create table ZJCG.CTP_TASK_JOB_INFO
(
  job_name       VARCHAR2(200) not null,
  job_desc       VARCHAR2(2000),
  execute_class  VARCHAR2(200),
  execute_method VARCHAR2(200),
  job_type       VARCHAR2(1),
  ip_poor_flag   VARCHAR2(10) default 4004,
  constraint PK_CTP_JOB_INFO primary key (JOB_NAME)
);
desc ZJCG.CTP_TASK_JOB_INFO;


create table ZJCG.CTP_TASK_THREAD_INFO
(
  trigger_name   VARCHAR2(100) not null,
  job_name       VARCHAR2(100),
  thread_type    VARCHAR2(1),
  cronexpression VARCHAR2(100),
  starttime      VARCHAR2(20),
  endtime        VARCHAR2(20),
  repeatecount   VARCHAR2(10),
  paramjsonstr   VARCHAR2(2000),
  state          VARCHAR2(1),
  trigger_desc   VARCHAR2(300),
  pass_id        VARCHAR2(100),
  max_exe_num    VARCHAR2(10),
  constraint PK_THREAD_INFO primary key (TRIGGER_NAME)
);
desc ZJCG.CTP_TASK_THREAD_INFO;


create table ZJCG.CTP_THIRD_SYNZ_TR
(
  pass_id   VARCHAR2(60),
  ctp_id    VARCHAR2(20),
  ctp_seq   VARCHAR2(60),
  id_name   VARCHAR2(60),
  pro_id    VARCHAR2(60),
  pro_amt   VARCHAR2(60),
  tran_stat VARCHAR2(5)
);
desc ZJCG.CTP_THIRD_SYNZ_TR;


create table ZJCG.CTP_TIO_TYP_CONF
(
  ctp_id             VARCHAR2(10) not null,
  pro_typ            VARCHAR2(10) not null,
  pro_typ_name       VARCHAR2(100),
  mer_o_type         VARCHAR2(1),
  mer_rec_acc_no     VARCHAR2(30),
  mer_rec_acc_name   VARCHAR2(100),
  per_need_reg       VARCHAR2(2),
  cor_need_reg       VARCHAR2(2),
  cor_pub_name       VARCHAR2(500),
  mer_inner_acc_no   VARCHAR2(30),
  mer_inner_acc_name VARCHAR2(100),
  invalid_term       VARCHAR2(10),
  trans_count        VARCHAR2(10),
  constraint PK_CTP_TIO_TYP_CONF primary key (CTP_ID, PRO_TYP)
);
desc ZJCG.CTP_TIO_TYP_CONF;


create table ZJCG.CTP_TRANSFER_TRANS_TR
(
  ctp_seq       VARCHAR2(50) not null,
  pay_id_typ    VARCHAR2(2),
  pay_id_no     VARCHAR2(20),
  pay_acct_name VARCHAR2(50),
  pay_acct_no   VARCHAR2(30),
  pay_mobile    VARCHAR2(11),
  tran_amt      NUMBER(18,4),
  pay_cust_fee  NUMBER(18,4),
  pay_acct1     VARCHAR2(30),
  fee_type1     VARCHAR2(2),
  tran_amt1     NUMBER(18,4),
  pay_acct2     VARCHAR2(30),
  fee_type2     VARCHAR2(2),
  tran_amt2     NUMBER(18,4),
  rcv_id_typ    VARCHAR2(2),
  rcv_id_no     VARCHAR2(20),
  rcv_acct_name VARCHAR2(50),
  rcv_acct_no   VARCHAR2(30),
  rcv_mobile    VARCHAR2(11),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(10),
  tran_state    VARCHAR2(5),
  ctp_id        VARCHAR2(10) not null,
  constraint PK_TPP_TRANSFER_TRANS_TR primary key (CTP_SEQ, CTP_ID)
);
desc ZJCG.CTP_TRANSFER_TRANS_TR;



create table ZJCG.CTP_TRANS_CODE_QX
(
  trans_code_qx   VARCHAR2(10) not null,
  trans_code_desc VARCHAR2(200),
  constraint PK_TRANS_CODE primary key (TRANS_CODE_QX)
);
desc ZJCG.CTP_TRANS_CODE_QX;


create table ZJCG.CTP_TRANS_CONF
(
  trans_code   VARCHAR2(100) not null,
  ctp_id       VARCHAR2(10) not null,
  fee_type     VARCHAR2(10) not null,
  ctp_typ_desc VARCHAR2(500),
  pay_type     VARCHAR2(5) not null,
  account_no   VARCHAR2(30),
  constraint PK_CTP_TRANS_CONF primary key (TRANS_CODE, CTP_ID, FEE_TYPE)
);
desc ZJCG.CTP_TRANS_CONF;


create table ZJCG.CTP_TRANS_ORDER_INFO
(
  ctp_id    VARCHAR2(10) not null,
  order_no  VARCHAR2(60) not null,
  oper_type VARCHAR2(2) not null,
  ebuser_no VARCHAR2(60),
  acct_no   VARCHAR2(30) not null,
  tran_amt  NUMBER(18,4) not null,
  cust_fee  NUMBER(18,4) not null,
  fee_list  CLOB,
  state     VARCHAR2(5) not null,
  cre_date  VARCHAR2(10),
  cre_time  VARCHAR2(8),
  upd_date  VARCHAR2(10),
  upd_time  VARCHAR2(8),
  back_url  VARCHAR2(200),
  synz_url  VARCHAR2(200),
  is_synz   VARCHAR2(5),
  synz_rate NUMBER default 0,
  ctp_seq   VARCHAR2(60),
  ctp_date  VARCHAR2(10)
);
desc ZJCG.CTP_TRANS_ORDER_INFO;
create index ZJCG.NIN_CTP_ORDER on ZJCG.CTP_TRANS_ORDER_INFO (CTP_ID, ORDER_NO);
create index ZJCG.N_CTP_TRANS_ORDER_INFO on ZJCG.CTP_TRANS_ORDER_INFO (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_TRANS_SERVICE
(
  ctp_id     VARCHAR2(10) not null,
  trans_code VARCHAR2(100) not null,
  stat       VARCHAR2(5) not null,
  trans_desc VARCHAR2(200),
  constraint PK_CTP_TRANS_SERVICE primary key (CTP_ID, TRANS_CODE)
);
desc ZJCG.CTP_TRANS_SERVICE;


create table ZJCG.CTP_TRANS_TR
(
  tran_seq           VARCHAR2(60) not null,
  tran_seq_no        VARCHAR2(60),
  tran_host_no       VARCHAR2(60),
  check_seq_no       VARCHAR2(60),
  check_host_no      VARCHAR2(60),
  ctp_id             VARCHAR2(10) not null,
  trans_code         VARCHAR2(100) not null,
  ctp_typ            VARCHAR2(10),
  rcv_acct           VARCHAR2(30),
  pay_acct           VARCHAR2(30),
  tran_amt           NUMBER(18,4) not null,
  pay_type           VARCHAR2(5) not null,
  tran_stat          VARCHAR2(5) not null,
  tran_date          VARCHAR2(10),
  tran_time          VARCHAR2(8),
  upd_date           VARCHAR2(10),
  upd_time           VARCHAR2(8),
  pay_name           VARCHAR2(300),
  pay_org            VARCHAR2(8),
  rcv_name           VARCHAR2(300),
  rcv_org            VARCHAR2(8),
  pay_method         VARCHAR2(5),
  rev_flag           VARCHAR2(5),
  fee_type           VARCHAR2(5),
  seq                VARCHAR2(60),
  host_date          VARCHAR2(10),
  qz_seq             VARCHAR2(60),
  qz_date            VARCHAR2(10),
  host_check         VARCHAR2(10) default '0',
  host_check_time    VARCHAR2(60),
  roll_check         VARCHAR2(60) default '0',
  roll_check_time    VARCHAR2(60),
  ctp_seq            VARCHAR2(60),
  original_acct      VARCHAR2(30),
  original_acct_name VARCHAR2(60),
  mid_fee_type       VARCHAR2(5),
  trans_flag         VARCHAR2(5) default '0'
);
desc ZJCG.CTP_TRANS_TR;
create index ZJCG.INDEX_CTP_TRANS_TR on ZJCG.CTP_TRANS_TR (TRAN_SEQ);
create index ZJCG.N_CTP_TRANS_TR on ZJCG.CTP_TRANS_TR (TRAN_DATE, TRAN_STAT, FEE_TYPE);


create table ZJCG.CTP_TRANS_UN_TR
(
  tran_seq  VARCHAR2(60) not null,
  ctp_id    VARCHAR2(10) not null,
  rcv_acct  VARCHAR2(30),
  pay_acct  VARCHAR2(30),
  tran_amt  NUMBER(18,4) not null,
  tran_stat VARCHAR2(5) not null,
  tran_date VARCHAR2(10),
  tran_time VARCHAR2(8),
  upd_date  VARCHAR2(10),
  upd_time  VARCHAR2(8),
  pay_name  VARCHAR2(300),
  rcv_name  VARCHAR2(300),
  ctp_seq   VARCHAR2(60),
  que_num   VARCHAR2(5) default 0,
  synz_url  VARCHAR2(300)
);
desc ZJCG.CTP_TRANS_UN_TR;
create index ZJCG.INDEX_CTP_TRANS_UN_TR on ZJCG.CTP_TRANS_UN_TR (TRAN_SEQ);
create index ZJCG.N_CTP_TRANS_UN_TR on ZJCG.CTP_TRANS_UN_TR (QUE_NUM, TRAN_STAT, TRAN_DATE, TRAN_TIME);
create index ZJCG.S_CTP_TRANS_UN_TR on ZJCG.CTP_TRANS_UN_TR (CTP_ID, CTP_SEQ);


create table ZJCG.CTP_TRANS_URL_CONF
(
  tran_type  VARCHAR2(10) not null,
  trans_url  VARCHAR2(500) not null,
  url_parm   VARCHAR2(500),
  trans_desc VARCHAR2(500)
);
desc ZJCG.CTP_TRANS_URL_CONF;


create table ZJCG.CTP_TRAN_INFO
(
  ctp_seq      VARCHAR2(60),
  pro_id       VARCHAR2(60) not null,
  ctp_id       VARCHAR2(10) not null,
  tran_date    VARCHAR2(20),
  tran_amt     NUMBER(18,4),
  host_sn      VARCHAR2(60) not null,
  rcv_acct     VARCHAR2(60),
  rcv_name     VARCHAR2(60),
  pay_acct     VARCHAR2(60),
  pay_name     VARCHAR2(60),
  tran_time    VARCHAR2(20),
  resc_ract_no VARCHAR2(200),
  constraint PK_CTP_TRAN_INFO primary key (PRO_ID, CTP_ID, HOST_SN)
);
desc ZJCG.CTP_TRAN_INFO;


create table ZJCG.CTP_TYP_CONF
(
  ctp_id       VARCHAR2(10) not null,
  ctp_typ      VARCHAR2(5) not null,
  ctp_typ_desc VARCHAR2(300),
  stat         VARCHAR2(5)
);
desc ZJCG.CTP_TYP_CONF;


create table ZJCG.CTP_URL_TR
(
  ctp_id      VARCHAR2(10) not null,
  tran_seq    VARCHAR2(50) not null,
  ebuser_no   VARCHAR2(30),
  back_url    VARCHAR2(230),
  tran_date   VARCHAR2(130),
  stat        VARCHAR2(10),
  data_field  VARCHAR2(100),
  tran_type   VARCHAR2(100),
  synz_url    VARCHAR2(200),
  ctp_seq     VARCHAR2(60),
  is_synz     VARCHAR2(5),
  synz_rate   NUMBER(22),
  is_realname VARCHAR2(5),
  max_date    VARCHAR2(10),
  max_amt     VARCHAR2(24),
  max_count   VARCHAR2(24),
  ctp_date    VARCHAR2(30),
  cust_role   VARCHAR2(5) default 'A',
  sub_acct_no VARCHAR2(30),
  is_cancel   VARCHAR2(2) default '0',
  cancel_date VARCHAR2(18),
  cancel_time VARCHAR2(18),
  constraint PK_CTP_URL_TR primary key (TRAN_SEQ)
);
desc ZJCG.CTP_URL_TR;


create table ZJCG.CTP_USER_INFO
(
  ctp_id         VARCHAR2(10) not null,
  ebuser_no      VARCHAR2(20) not null,
  cust_name      VARCHAR2(200) not null,
  cust_ename     VARCHAR2(200),
  ebuser_typ     VARCHAR2(1) not null,
  open_date      VARCHAR2(10) not null,
  open_time      VARCHAR2(8) not null,
  mobile         VARCHAR2(30) not null,
  id_typ         VARCHAR2(2) not null,
  id_no          VARCHAR2(30) not null,
  open_teller    VARCHAR2(20),
  cif_no         VARCHAR2(20) not null,
  ctp_nick_name  VARCHAR2(200),
  ctp_login_name VARCHAR2(200),
  comp_org_no    VARCHAR2(50),
  comp_licen     VARCHAR2(50),
  comp_add       VARCHAR2(300),
  comp_zipcode   VARCHAR2(10),
  comp_email     VARCHAR2(50),
  comp_fax       VARCHAR2(20),
  legal_idt      VARCHAR2(2),
  legal_idt_no   VARCHAR2(30),
  legal_phone_no VARCHAR2(18),
  legal_add      VARCHAR2(300),
  user_stat      VARCHAR2(1),
  legal_name     VARCHAR2(150),
  gg_branch_no   VARCHAR2(8),
  mac            VARCHAR2(100),
  is_data_trans  VARCHAR2(1) default 'N',
  update_date    VARCHAR2(10) default to_char(Timestamp '2012-12-01 12:01:02.123','yyyy-mm-dd'),
  update_time    VARCHAR2(8) default to_char(Timestamp '2012-12-01 12:01:02.123','hh24:Mi:SS'),
  update_type    VARCHAR2(2) default 0,
  cust_role      VARCHAR2(5) default 'A',
  is_cancel      VARCHAR2(5) default '0',
  cancel_date    VARCHAR2(18),
  cancel_time    VARCHAR2(18),
  cancel_count   NUMBER(5) default 0,
  constraint PK_CTP_USER_ID primary key (EBUSER_NO)
);
desc ZJCG.CTP_USER_INFO;

# oracle 模式下不允许在主键列上建索引
# create unique index ZJCG.UNI_USER_INFO_EBUSERNO on ZJCG.CTP_USER_INFO (EBUSER_NO);
create unique index ZJCG.UNQ_CTP_USER_ID on ZJCG.CTP_USER_INFO (CTP_ID, ID_TYP, ID_NO, CUST_NAME, CUST_ROLE, IS_CANCEL, CANCEL_COUNT);


create table ZJCG.CTP_USER_MENU
(
  ctp_id  VARCHAR2(10),
  oper_id VARCHAR2(20),
  menu_id VARCHAR2(12)
);
desc ZJCG.CTP_USER_MENU;


create table ZJCG.CTP_USER_ROLE_SYSMENU
(
  role_id VARCHAR2(20) not null,
  menu_id VARCHAR2(20) not null,
  constraint PK_CTP_USER_ROLE_SYSMENU primary key (ROLE_ID, MENU_ID)
);
desc ZJCG.CTP_USER_ROLE_SYSMENU;


create table ZJCG.CTP_WORK_SIGN
(
  ctp_id       VARCHAR2(10) not null,
  device_no    VARCHAR2(16),
  pk_q_p       VARCHAR2(80),
  pkq_upddate  VARCHAR2(10),
  pk_r_x       VARCHAR2(80),
  pk_r_y       VARCHAR2(80),
  pkr_upddate  VARCHAR2(10),
  mack         VARCHAR2(80),
  mack_upddate VARCHAR2(10),
  constraint PK_CTP_WORK_SIGN primary key (CTP_ID)
);
desc ZJCG.CTP_WORK_SIGN;


create table ZJCG.CUPS_CARDPARAM
(
  bankid    VARCHAR2(6) not null,
  bankname  VARCHAR2(100),
  cardname  VARCHAR2(50),
  cardid    VARCHAR2(1),
  offset1   VARCHAR2(2),
  length1   VARCHAR2(2),
  offset2   VARCHAR2(2),
  length2   VARCHAR2(2),
  string2   VARCHAR2(20),
  offset3   VARCHAR2(2),
  length3   VARCHAR2(2),
  string3   VARCHAR2(20),
  offset4   VARCHAR2(2),
  length4   VARCHAR2(2),
  string4   VARCHAR2(20),
  offset5   VARCHAR2(2),
  length5   VARCHAR2(2),
  string5   VARCHAR2(20),
  panoffset VARCHAR2(2),
  panlen    VARCHAR2(2),
  pinlen    VARCHAR2(1),
  cardflag  VARCHAR2(8),
  string1   VARCHAR2(20),
  is_audi   VARCHAR2(1),
  audi_id   VARCHAR2(30)
);
desc ZJCG.CUPS_CARDPARAM;

create index ZJCG.CUPS_CARDPARAM_IDX on ZJCG.CUPS_CARDPARAM (STRING1);


create table ZJCG.CUPS_CARDPARAM_TMP
(
  bankid    VARCHAR2(6) not null,
  bankname  VARCHAR2(100),
  cardname  VARCHAR2(50),
  cardid    VARCHAR2(1),
  offset1   VARCHAR2(2),
  length1   VARCHAR2(2),
  offset2   VARCHAR2(2),
  length2   VARCHAR2(2),
  string2   VARCHAR2(20),
  offset3   VARCHAR2(2),
  length3   VARCHAR2(2),
  string3   VARCHAR2(20),
  offset4   VARCHAR2(2),
  length4   VARCHAR2(2),
  string4   VARCHAR2(20),
  offset5   VARCHAR2(2),
  length5   VARCHAR2(2),
  string5   VARCHAR2(20),
  panoffset VARCHAR2(2),
  panlen    VARCHAR2(2),
  pinlen    VARCHAR2(1),
  cardflag  VARCHAR2(8),
  string1   VARCHAR2(20)
);
desc ZJCG.CUPS_CARDPARAM_TMP;
create index ZJCG.CUPS_CARDPARAM_IDX2 on ZJCG.CUPS_CARDPARAM_TMP (STRING1);


create table ZJCG.CURRENCY_INFO
(
  curr_code VARCHAR2(10) not null,
  curr_name VARCHAR2(30) not null,
  constraint PK_CURRENCY_INFO primary key (CURR_CODE)
);
desc ZJCG.CURRENCY_INFO;


create table ZJCG.DTB_ROLL_TR
(
  tran_seq        VARCHAR2(50) not null,
  ebuser_no       VARCHAR2(20),
  acct_no         VARCHAR2(32),
  trans_chan_type VARCHAR2(20),
  batch_no        VARCHAR2(20),
  oper_type       VARCHAR2(2),
  oper_amt        NUMBER(18,4),
  card_no         VARCHAR2(30),
  card_name       VARCHAR2(50),
  bank_no         VARCHAR2(50),
  bank_name       VARCHAR2(60),
  tran_state      VARCHAR2(2),
  host_amt        VARCHAR2(20),
  host_time       VARCHAR2(20),
  host_balance    VARCHAR2(20),
  host_result     VARCHAR2(2),
  host_code       VARCHAR2(500),
  host_msg        VARCHAR2(500),
  host_query_seq  VARCHAR2(20),
  remark          VARCHAR2(200),
  tran_date       VARCHAR2(10),
  tran_time       VARCHAR2(10),
  recharge_seq    VARCHAR2(50),
  cancel_seq      VARCHAR2(50),
  front_seq       VARCHAR2(50),
  front_date      VARCHAR2(20),
  need_check      VARCHAR2(1),
  check_way       VARCHAR2(2),
  check_time      VARCHAR2(20),
  check_teller    VARCHAR2(20),
  check_result    VARCHAR2(20),
  is_batch        VARCHAR2(1),
  chan_typ        VARCHAR2(3) not null,
  gg_branch_no    VARCHAR2(10) not null,
  gg_teller_no    VARCHAR2(10),
  terminalid      VARCHAR2(10),
  sysid           VARCHAR2(10),
  ctp_id          VARCHAR2(10),
  tz_seq          VARCHAR2(60),
  tz_date         VARCHAR2(10),
  cz_date         VARCHAR2(10),
  cz_seq          VARCHAR2(60),
  tran_fee        NUMBER(18,4),
  cust_fee        NUMBER(18,4),
  zj_fee          NUMBER(18,4),
  ctp_seq_no      VARCHAR2(60),
  hy_flag         VARCHAR2(1),
  clear_flag      VARCHAR2(1),
  pro_typ         VARCHAR2(20),
  ca_check_flag   VARCHAR2(20) default '0',
  ctp_check_time  VARCHAR2(60),
  cf_check_flag   VARCHAR2(60) default '0',
  tran_check_flag VARCHAR2(60) default '0',
  cancel_check    VARCHAR2(20),
  check_date      VARCHAR2(20),
  buc_check_flag  VARCHAR2(20),
  acct_tp         VARCHAR2(20),
  ctp_date        VARCHAR2(30),
  ori_ctp_seq     VARCHAR2(60),
  constraint PK_DTB_ROLL_TR primary key (TRAN_SEQ)
);
desc ZJCG.DTB_ROLL_TR;

create index ZJCG.NO_DTB_ROLL on ZJCG.DTB_ROLL_TR (CTP_ID, CTP_DATE);
create index ZJCG.N_DTB_ROLL_TR on ZJCG.DTB_ROLL_TR (CTP_ID, CTP_SEQ_NO);
create index ZJCG.QRY_DTB_ROLL_TR_PASS_TP on ZJCG.DTB_ROLL_TR (TRAN_DATE, CARD_NO, OPER_TYPE, TRAN_STATE, CTP_ID, CTP_SEQ_NO);


create table ZJCG.DTB_UPAY_LIST
(
  tran_seq            VARCHAR2(50) not null,
  ebuser_no           VARCHAR2(20),
  card_no             VARCHAR2(22),
  card_name           VARCHAR2(100),
  oper_amt            NUMBER(18,4),
  oper_type           VARCHAR2(2),
  bank_name           VARCHAR2(50),
  bank_no             VARCHAR2(20),
  submit_time         VARCHAR2(20),
  upay_time           VARCHAR2(20),
  upay_result         VARCHAR2(2),
  upay_state          VARCHAR2(2) not null,
  upay_order_no       VARCHAR2(20),
  upay_code           VARCHAR2(100),
  upay_msg            VARCHAR2(500),
  upay_query_seq      VARCHAR2(50),
  remark              VARCHAR2(200),
  check_amt           NUMBER(18,4),
  check_time          VARCHAR2(20),
  check_result        VARCHAR2(5),
  pass_type           VARCHAR2(20),
  acc_type            VARCHAR2(2),
  validity            VARCHAR2(10),
  cvn2                VARCHAR2(2),
  branch_bank         VARCHAR2(100),
  provice             VARCHAR2(50),
  city                VARCHAR2(50),
  cert_type           VARCHAR2(2),
  cert_no             VARCHAR2(30),
  prot_no             VARCHAR2(30),
  mobile_no           VARCHAR2(30),
  email               VARCHAR2(50),
  is_batch            VARCHAR2(2),
  is_today_check      VARCHAR2(1),
  chan_typ            VARCHAR2(3),
  ctp_id              VARCHAR2(10),
  ctp_user_id         VARCHAR2(60),
  tran_amt            NUMBER(18,4),
  tran_fee            NUMBER(18,4),
  flag                VARCHAR2(10),
  end_flag            VARCHAR2(10),
  sys_trace_audit_num VARCHAR2(6),
  constraint PK_DTB_UPAY_LIST primary key (TRAN_SEQ)
);
desc ZJCG.DTB_UPAY_LIST;
create index ZJCG.N_DTB_UPAY_LIST on ZJCG.DTB_UPAY_LIST (PASS_TYPE, UPAY_TIME, UPAY_STATE);


create table ZJCG.PASS_CTP_CONFIG
(
  ctp_id     VARCHAR2(10) not null,
  pass_type  VARCHAR2(10) not null,
  pass_level VARCHAR2(2),
  busi_type  VARCHAR2(2) not null,
  ebuser_typ VARCHAR2(1) not null,
  constraint PK_PASS_CTP_CONF primary key (CTP_ID, PASS_TYPE, BUSI_TYPE, EBUSER_TYP)
);
desc ZJCG.PASS_CTP_CONFIG;


create table ZJCG.PASS_DETAIL_CONF
(
  pass_id       VARCHAR2(100) not null,
  bank_code     VARCHAR2(20) not null,
  start_amt     NUMBER(18,4) not null,
  end_amt       NUMBER(18,4) not null,
  busi_type     VARCHAR2(10) not null,
  pass_type     VARCHAR2(10) not null,
  is_batch      VARCHAR2(2) not null,
  is_audi       VARCHAR2(1),
  audi_id       VARCHAR2(30),
  begin_time    NUMBER(4),
  end_time      NUMBER(4),
  support_day   VARCHAR2(7) default 1111111,
  day_amt       NUMBER(18,4),
  ebuser_typ    VARCHAR2(2),
  total_err_num VARCHAR2(10),
  month_amt     NUMBER(18,4),
  constraint PK_PASS_DETAIL_CONF primary key (PASS_ID)
);
desc ZJCG.PASS_DETAIL_CONF;



create table ZJCG.PASS_FEE_CONF
(
  pass_type   VARCHAR2(10),
  pass_name   VARCHAR2(20),
  oper_type   VARCHAR2(2),
  lower_limit NUMBER(18,4),
  upper_limit NUMBER(18,4),
  charge_type VARCHAR2(2),
  fee_value   NUMBER(18,4)
);
desc ZJCG.PASS_FEE_CONF;


create table ZJCG.PASS_INF
(
  pass_type   VARCHAR2(10) not null,
  pass_name   VARCHAR2(20) not null,
  pass_url    VARCHAR2(100) not null,
  fee_in_amt  NUMBER(18,4),
  fee_out_amt NUMBER(18,4),
  check_type  VARCHAR2(2),
  constraint PK_PASS_INF primary key (PASS_TYPE)
);
desc ZJCG.PASS_INF;



create table ZJCG.PASS_INF_XYHF
(
  pass_type VARCHAR2(10) not null,
  pass_name VARCHAR2(50) not null,
  pass_url  VARCHAR2(100) not null,
  p_name    VARCHAR2(30) not null,
  constraint PK_PASS_INF_XYHF primary key (PASS_TYPE)
);
desc ZJCG.PASS_INF_XYHF;


create table ZJCG.PAY_AUTH_LIMT_DEF
(
  sec_way    VARCHAR2(2) not null,
  ccy        VARCHAR2(3) not null,
  once_limit NUMBER(18,4) not null,
  day_limit  NUMBER(18,4) not null,
  up_date    VARCHAR2(10) not null,
  up_teler   VARCHAR2(50),
  is_audi    VARCHAR2(1),
  audi_id    VARCHAR2(30),
  constraint PK_PAY_AUTH_LIMT_DEF primary key (SEC_WAY, CCY)
);
desc ZJCG.PAY_AUTH_LIMT_DEF;


create table ZJCG.PAY_FEE_TRANS_TR
(
  tran_seq    VARCHAR2(20) not null,
  ebuser_no   VARCHAR2(30),
  pay_acc     VARCHAR2(20),
  pay_name    VARCHAR2(200),
  org_no      VARCHAR2(20),
  amt         NUMBER(18,4),
  amt_flag    VARCHAR2(20),
  fee_type    VARCHAR2(20),
  tran_state  VARCHAR2(20),
  tran_date   VARCHAR2(20),
  tran_time   VARCHAR2(20),
  host_seq_no VARCHAR2(20),
  chan_type   VARCHAR2(20),
  ebuser_typ  VARCHAR2(2),
  oper_id     VARCHAR2(8),
  primary key (TRAN_SEQ)
);
desc ZJCG.PAY_FEE_TRANS_TR;


create table ZJCG.SIGN_PROTOCL_INFO
(
  pro_id         VARCHAR2(50) not null,
  ebuser_no      VARCHAR2(60) not null,
  pro_cont       CLOB not null,
  bus_no         VARCHAR2(32),
  inte_chan      VARCHAR2(50) not null,
  sign_date      VARCHAR2(10),
  sign_time      VARCHAR2(10),
  pro_cont_md5   VARCHAR2(100),
  pro_type       VARCHAR2(2) not null,
  pro_tran_msg   CLOB,
  upd_date       VARCHAR2(10),
  upd_time       VARCHAR2(10),
  invalid_date   VARCHAR2(20),
  acct_no        VARCHAR2(30),
  pb_protocol    VARCHAR2(50),
  acct_name      VARCHAR2(100),
  protocol_state VARCHAR2(100),
  trans_count    VARCHAR2(10),
  constraint PK_SIGN_PROTOCL_INFO primary key (PRO_ID)
);
desc ZJCG.SIGN_PROTOCL_INFO;
create index ZJCG.N_SIGN_PROT on ZJCG.SIGN_PROTOCL_INFO (BUS_NO, ACCT_NO);


create table ZJCG.SMS_CUST_INFO
(
  id_name   VARCHAR2(60),
  tran_type VARCHAR2(200),
  mobile    VARCHAR2(60) not null,
  stat      VARCHAR2(2)
);
desc ZJCG.SMS_CUST_INFO;


create table ZJCG.SMS_TEMP
(
  sms_temp_id    VARCHAR2(20) not null,
  sms_temp_name  VARCHAR2(60) not null,
  sms_temp_cont  VARCHAR2(600) not null,
  sms_temp_state VARCHAR2(2) not null,
  up_date        VARCHAR2(10) not null,
  up_teler       VARCHAR2(50) not null,
  constraint PK_SMS_TEMP primary key (SMS_TEMP_ID)
);
desc ZJCG.SMS_TEMP;



create sequence ZJCG.ASSO_GR_ID_SEQ
minvalue 1
maxvalue 99999999
start with 301643
increment by 1
cache 20
cycle;

create sequence ZJCG.ASSO_ID_SEQ
minvalue 1
maxvalue 1999999999
start with 103141
increment by 1
cache 20
cycle;

create sequence ZJCG.COMMON_SEQ
minvalue 1
maxvalue 1999999999
start with 1254
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_DETAIL_SEQ
minvalue 1
maxvalue 1999999999
start with 2423897
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_INTE_SEQ
minvalue 10000001
maxvalue 99999999
start with 10001901
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_P2P_COMPENT_SEQ
minvalue 1000000001
maxvalue 1999999999
start with 1000001811
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_P2P_REGIST_SEQ
minvalue 1000001
maxvalue 9999999
start with 2138893
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_PRO_SEQ
minvalue 1
maxvalue 1999999999
start with 100461
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_SIGN_SEQ
minvalue 1
maxvalue 99999999
start with 101
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_TASK_SEQ_NO
minvalue 1000001
maxvalue 9999999
start with 9752653
increment by 1
cache 20
cycle;

create sequence ZJCG.CTP_UNPAY_SEQ
minvalue 1000
maxvalue 9999
start with 1061
increment by 1
cache 20
cycle;

create sequence ZJCG.CUST_CHAN_ID_SEQ
minvalue 1
maxvalue 1999999999
start with 103236
increment by 1
cache 20
cycle;

create sequence ZJCG.CUST_ID_SEQ
minvalue 1
maxvalue 1999999999
start with 415337
increment by 1
cache 20
cycle;

create sequence ZJCG.CUST_LOGIN_ID_SEQ
minvalue 1
maxvalue 1999999999
start with 236763
increment by 1
cache 20
cycle;


create table ZJCG.BANK_COFG_CODE
(
  bank_code        VARCHAR2(18) not null,
  bank_name        VARCHAR2(100) not null,
  order_letter     VARCHAR2(10) not null,
  order_seq        INTEGER not null,
  update_date      VARCHAR2(10),
  update_teller_no VARCHAR2(50),
  update_time      VARCHAR2(8),
  update_org       VARCHAR2(10),
  cls_code         VARCHAR2(3),
  is_audi          VARCHAR2(1),
  audi_id          VARCHAR2(30),
  constraint PK_BANK_COFG_CODE primary key (BANK_CODE)
);
desc ZJCG.BANK_COFG_CODE;


create table ZJCG.BANK_EXT_INF
(
  bank_code         VARCHAR2(20) not null,
  bank_name         VARCHAR2(200),
  bank_logo_url     VARCHAR2(100),
  bank_bg_url       VARCHAR2(100),
  is_audi           VARCHAR2(2),
  audi_id           VARCHAR2(50),
  dk_limit          NUMBER(18,4),
  df_limit          NUMBER(18,4),
  certify_pass_type VARCHAR2(20),
  is_check_mobile   VARCHAR2(2),
  is_check_pwd      VARCHAR2(2),
  balance_query     VARCHAR2(1),
  dk_day_limit      NUMBER(18,4),
  df_day_limit      NUMBER(18,4),
  ctp_id            VARCHAR2(20) not null,
  constraint PK_BANK_EXT_INF primary key (BANK_CODE, CTP_ID)
);
desc ZJCG.BANK_EXT_INF;


create table ZJCG.BANK_LIMIT_DEF
(
  bank_limit_ty VARCHAR2(2) not null,
  ccy           VARCHAR2(3) not null,
  once_limit    NUMBER(18,4) not null,
  day_limit     NUMBER(18,4) not null,
  up_date       VARCHAR2(10) not null,
  up_tell       VARCHAR2(10) not null,
  day_total     INTEGER,
  year_limit    NUMBER(18,4),
  constraint PK_BANK_LIMIT_DEF primary key (BANK_LIMIT_TY, CCY)
);
desc ZJCG.BANK_LIMIT_DEF;


create table ZJCG.CARDPARAM_CLS_INFO
(
  string1          VARCHAR2(20) not null,
  bank_code        VARCHAR2(18) not null,
  update_date      VARCHAR2(10),
  update_teller_no VARCHAR2(50),
  update_time      VARCHAR2(8),
  update_org       VARCHAR2(10),
  is_audi          VARCHAR2(1),
  audi_id          VARCHAR2(30),
  bank_name        VARCHAR2(200),
  card_len         VARCHAR2(10) not null,
  org_code         VARCHAR2(30),
  org_name         VARCHAR2(200),
  card_type        VARCHAR2(2),
  constraint PK1_CARDPARAM_CLS_INFO primary key (STRING1, CARD_LEN)
);
desc ZJCG.CARDPARAM_CLS_INFO;


create table ZJCG.CHAN_INTE_AUTH_CTL
(
  chan_typ   VARCHAR2(3) not null,
  tran_code  VARCHAR2(100) not null,
  open_way   VARCHAR2(3) not null,
  ebuser_typ VARCHAR2(3) not null,
  incorp_no  VARCHAR2(10) not null,
  oper_type  VARCHAR2(3) not null,
  remark     VARCHAR2(200),
  constraint PK_CHAN_INTE_AUTH_CTL primary key (TRAN_CODE, CHAN_TYP, OPEN_WAY, EBUSER_TYP, INCORP_NO)
);
desc ZJCG.CHAN_INTE_AUTH_CTL;


create table ZJCG.CHAN_SERVICE_FREQ_CL
(
  chan_typ        VARCHAR2(3) not null,
  tran_code       VARCHAR2(50) not null,
  tran_value_name VARCHAR2(20) not null,
  freq_type       VARCHAR2(1) not null,
  per_time        INTEGER not null,
  per_count       INTEGER not null,
  fre_time        INTEGER not null,
  ext1            VARCHAR2(10) default 'ALL' not null,
  ext2            VARCHAR2(10) default 'ALL' not null
);
desc ZJCG.CHAN_SERVICE_FREQ_CL;


create table ZJCG.DEFILEQZTRANS
(
  tran_code VARCHAR2(60),
  stat      VARCHAR2(2),
  tran_desc VARCHAR2(200)
);
desc ZJCG.DEFILEQZTRANS;


create table ZJCG.DTB_ACCT
(
  ebuser_no       VARCHAR2(20) not null,
  acct_index      VARCHAR2(5),
  acct_no         VARCHAR2(32) not null,
  acct_name       VARCHAR2(100),
  acc_alias       VARCHAR2(50),
  ccy_type        VARCHAR2(3),
  acct_tp         VARCHAR2(10) not null,
  hang_date       VARCHAR2(10),
  hang_time       VARCHAR2(10),
  hang_org        VARCHAR2(8),
  hang_teler      VARCHAR2(8),
  hang_chan       VARCHAR2(8),
  bank_no         VARCHAR2(20),
  bank_name       VARCHAR2(100),
  is_active       VARCHAR2(10),
  is_sign_mobile  VARCHAR2(10),
  pre_no          VARCHAR2(30),
  tranin_amt      NUMBER(18,4),
  open_org        VARCHAR2(8),
  acc_type        VARCHAR2(1),
  bind_type       VARCHAR2(1),
  ctp_id          VARCHAR2(10),
  branch_no       VARCHAR2(20),
  channel_id      VARCHAR2(5),
  reg_amt         NUMBER(18,4),
  is_random       VARCHAR2(2),
  dk_pro_id       VARCHAR2(50),
  dk_pro_state    VARCHAR2(1),
  is_dk           VARCHAR2(2),
  is_data_trans   VARCHAR2(1),
  cust_role       VARCHAR2(5) default 'A',
  acct_no_encrypt VARCHAR2(32),
  is_cancel       VARCHAR2(5) default '0',
  cancel_date     VARCHAR2(18),
  cancel_time     VARCHAR2(18),
  cancel_count    NUMBER(5) default 0,
  constraint PK_DTB_ACCT primary key (EBUSER_NO, ACCT_NO)
);
desc ZJCG.DTB_ACCT;

create unique index ZJCG.UNIK_DTB_ACCT on ZJCG.DTB_ACCT (CTP_ID, ACCT_NO, CUST_ROLE, IS_CANCEL, CANCEL_COUNT);


create table ZJCG.DTB_ACCT_EXTEND
(
  ebuser_no          VARCHAR2(20) not null,
  acct_no            VARCHAR2(32),
  acct_name          VARCHAR2(100),
  sub_acct_no        VARCHAR2(32),
  sub_acct_no_second VARCHAR2(32),
  open_date          VARCHAR2(10),
  open_time          VARCHAR2(10),
  ctp_id             VARCHAR2(10) not null,
  acc_type           VARCHAR2(1) not null,
  cust_role          VARCHAR2(5) default 'A' not null,
  pend_pro_id        VARCHAR2(60),
  is_cancel          VARCHAR2(5) default '0',
  cancel_date        VARCHAR2(18),
  cancel_time        VARCHAR2(18),
  stat               VARCHAR2(1) default '0' not null,
  is_data_trans      VARCHAR2(1) default 'N',
  constraint PK_DTB_ACCT_EXTEND primary key (CTP_ID, EBUSER_NO, CUST_ROLE, ACC_TYPE, STAT)
);
desc ZJCG.DTB_ACCT_EXTEND;
create index ZJCG.N_DTB_ACCT_EXTEND on ZJCG.DTB_ACCT_EXTEND (CTP_ID, OPEN_DATE, IS_DATA_TRANS);


create table ZJCG.DTB_BUSINESS_FEE
(
  tran_seq        VARCHAR2(20) not null,
  trans_chan_type VARCHAR2(20) not null,
  chan_typ        VARCHAR2(3),
  oper_type       VARCHAR2(2),
  fee_type        VARCHAR2(3) not null,
  pay_acct        VARCHAR2(20) not null,
  rec_acct        VARCHAR2(20) not null,
  total_fee       NUMBER(18,4) not null,
  host_code       VARCHAR2(100),
  host_msg        VARCHAR2(800),
  load_date       VARCHAR2(20) not null,
  tran_date       VARCHAR2(10) not null,
  tran_time       VARCHAR2(10) not null,
  tran_state      VARCHAR2(2) not null,
  gg_branch_no    VARCHAR2(10) not null,
  seq_no          VARCHAR2(20) not null,
  order_date      VARCHAR2(10) not null,
  trans_flag      VARCHAR2(2) default '0',
  constraint PK_DTB_BUSINESS_FEE primary key (TRAN_SEQ, SEQ_NO)
);
desc ZJCG.DTB_BUSINESS_FEE;


create table ZJCG.DTB_RECHARGE_TR
(
  recharge_seq   VARCHAR2(50) not null,
  tran_seq       VARCHAR2(50),
  ebuser_no      VARCHAR2(20),
  acct_no        VARCHAR2(32),
  batch_no       VARCHAR2(20),
  host_amt       VARCHAR2(20),
  host_time      VARCHAR2(20),
  host_balance   VARCHAR2(20),
  host_result    VARCHAR2(2),
  host_code      VARCHAR2(500),
  host_msg       VARCHAR2(500),
  host_query_seq VARCHAR2(20),
  remark         VARCHAR2(200),
  tran_date      VARCHAR2(10),
  tran_time      VARCHAR2(10),
  tran_state     VARCHAR2(2),
  oper_amt       VARCHAR2(20),
  ctp_id         VARCHAR2(10),
  ctp_user_id    VARCHAR2(50),
  chan_typ       VARCHAR2(5),
  fee_amt        NUMBER(18,4),
  fee_acct       VARCHAR2(30),
  constraint PK_DTB_RECHARGE_TR primary key (RECHARGE_SEQ)
);
desc ZJCG.DTB_RECHARGE_TR;


create table ZJCG.DTB_TRANIN_CONF
(
  tran_seq     VARCHAR2(50),
  chan_typ     VARCHAR2(3) not null,
  once_min_amt NUMBER(18,4),
  max_tran_num VARCHAR2(4),
  upd_date     VARCHAR2(20),
  upd_teller   VARCHAR2(20),
  is_audi      VARCHAR2(1),
  audi_id      VARCHAR2(30)
);
desc ZJCG.DTB_TRANIN_CONF;


create table ZJCG.DTB_TRANS_LIMIT_USE
(
  ebuser_no  VARCHAR2(30) not null,
  chan_typ   VARCHAR2(10) not null,
  tran_date  VARCHAR2(20) not null,
  tran_amt   NUMBER(18,4) not null,
  tran_times NUMBER,
  constraint PK_DTB_TRANS_LIMIT_USE primary key (EBUSER_NO, CHAN_TYP)
);
desc ZJCG.DTB_TRANS_LIMIT_USE;


create table ZJCG.DTB_UPAY_ACCT_CHECK
(
  tran_seq       VARCHAR2(30) not null,
  pay_card_type  VARCHAR2(10),
  tran_amt       VARCHAR2(50),
  calc_amount    VARCHAR2(50),
  business_fee   VARCHAR2(50),
  calc_profit    VARCHAR2(50),
  stag_payfee    VARCHAR2(20),
  mer_catcode    VARCHAR2(5),
  mer_id         VARCHAR2(60),
  trade_code     VARCHAR2(50),
  term_type      VARCHAR2(10),
  termid         VARCHAR2(100),
  send_orgcode   VARCHAR2(50),
  acq_inscode    VARCHAR2(50),
  payment        VARCHAR2(10),
  trace_no       VARCHAR2(30),
  tran_time      VARCHAR2(20),
  load_date      VARCHAR2(20),
  oper_type      VARCHAR2(1),
  is_check       VARCHAR2(5),
  check_result   VARCHAR2(5),
  pass_type      VARCHAR2(20) not null,
  payment_amount VARCHAR2(50),
  order_date     VARCHAR2(10) not null,
  constraint PK_DTB_UPAY_ACCT_CHECK primary key (TRAN_SEQ)
);
desc ZJCG.DTB_UPAY_ACCT_CHECK;

create index ZJCG.N_DTB_UPAY_ACCT_CHECK on ZJCG.DTB_UPAY_ACCT_CHECK (CHECK_RESULT, PASS_TYPE, ORDER_DATE);


create table ZJCG.MARKET_ACTIVITY
(
  activity_id      VARCHAR2(20) not null,
  image_url        VARCHAR2(100),
  activity_content VARCHAR2(2000),
  activity_name    VARCHAR2(40),
  begin_time       VARCHAR2(20),
  end_time         VARCHAR2(20),
  state            VARCHAR2(2),
  order_index      VARCHAR2(10),
  is_audi          VARCHAR2(10),
  audi_id          VARCHAR2(50),
  constraint PK_MARKET_ACTIVITY primary key (ACTIVITY_ID)
);
desc ZJCG.MARKET_ACTIVITY;


create table ZJCG.MENU_TRANS_CODE_RELATION
(
  page_id      VARCHAR2(100),
  trans_code   VARCHAR2(100),
  is_main_menu VARCHAR2(5),
  root_menu    VARCHAR2(1000)
);
desc ZJCG.MENU_TRANS_CODE_RELATION;


create table ZJCG.MONEYFUND_RATE_HISTORY
(
  product_id     VARCHAR2(20) not null,
  update_date    VARCHAR2(20) not null,
  rate_year      NUMBER(20,4),
  rate_year_week NUMBER(20,4),
  rate_thousand  NUMBER(20,4),
  constraint PK_MONEYFUND_RATE_HISTORY primary key (PRODUCT_ID, UPDATE_DATE)
);
desc ZJCG.MONEYFUND_RATE_HISTORY;


create table ZJCG.P2P_TRANS_URL_CONF
(
  tran_type  VARCHAR2(10) not null,
  trans_url  VARCHAR2(500) not null,
  url_parm   VARCHAR2(500),
  trans_desc VARCHAR2(500)
);
desc ZJCG.P2P_TRANS_URL_CONF;


create table ZJCG.PARM_CODE
(
  code_type VARCHAR2(16) not null,
  code_no   VARCHAR2(100) not null,
  code_name VARCHAR2(300) not null,
  code_sort INTEGER,
  remark    VARCHAR2(120),
  is_audi   VARCHAR2(1),
  audi_id   VARCHAR2(30),
  constraint PK_PARM_CODE primary key (CODE_TYPE, CODE_NO)
);
desc ZJCG.PARM_CODE;


create table ZJCG.PAYMENT_CHECK_TR
(
  inte_seq       VARCHAR2(30) not null,
  trans_date     VARCHAR2(8),
  trans_seq      VARCHAR2(8) not null,
  num            VARCHAR2(8) not null,
  trans_code     VARCHAR2(4),
  chan_type      VARCHAR2(4),
  msg_type       VARCHAR2(4),
  cif_no         VARCHAR2(10),
  main_acct      VARCHAR2(18),
  acct_no        VARCHAR2(32),
  acct_num       VARCHAR2(4),
  sum_info       VARCHAR2(4),
  agent_name     VARCHAR2(30),
  teller         VARCHAR2(6),
  trans_amt      VARCHAR2(16),
  balance        VARCHAR2(16),
  end_balance    VARCHAR2(16),
  rcv_name       VARCHAR2(140),
  rcv_acct       VARCHAR2(32),
  rcv_bank       VARCHAR2(100),
  state          VARCHAR2(20),
  end_date       VARCHAR2(8),
  trans_time     VARCHAR2(6),
  back_result    VARCHAR2(20),
  load_date      VARCHAR2(20),
  load_time      VARCHAR2(20),
  is_check       VARCHAR2(2),
  h_rsp_code     VARCHAR2(50),
  h_rsp_msg      VARCHAR2(200),
  qd_seq         VARCHAR2(20),
  qd_date        VARCHAR2(8),
  oper_flag      VARCHAR2(4),
  is_synz        VARCHAR2(2),
  synz_rate      NUMBER,
  last_synz_time VARCHAR2(20),
  is_dz_synz     VARCHAR2(2),
  dz_synz_rate   NUMBER,
  lz_ctp_seq     VARCHAR2(30),
  lz_is_synz     VARCHAR2(2),
  lz_synz_rate   NUMBER,
  df_is_synz     VARCHAR2(2) default '0',
  ps             VARCHAR2(500),
  constraint PK_PAYMENT_TR primary key (INTE_SEQ)
);
desc ZJCG.PAYMENT_CHECK_TR;

create unique index ZJCG.UNK_PAYMENT_CHECK_TR on ZJCG.PAYMENT_CHECK_TR (TRANS_DATE, TRANS_SEQ, NUM);


create table ZJCG.PER_BORR_CUST_ACCOUNT_CONFIRM
(
  ebuser_no           VARCHAR2(50) not null,
  dc_flag             VARCHAR2(5) not null,
  ctp_send_fee        VARCHAR2(60),
  auto_repay_count    VARCHAR2(10) not null,
  auto_repay_quota    VARCHAR2(60) not null,
  auto_repay_end_date VARCHAR2(10) not null,
  ctp_id              VARCHAR2(20) not null,
  constraint PK_E primary key (EBUSER_NO)
);
desc ZJCG.PER_BORR_CUST_ACCOUNT_CONFIRM;


create table ZJCG.PER_BORR_CUST_ACCOUNT_INFO
(
  dc_flag             VARCHAR2(5) not null,
  ctp_send_fee        VARCHAR2(60),
  auto_repay_count    VARCHAR2(10) not null,
  auto_repay_quota    VARCHAR2(60) not null,
  auto_repay_end_date VARCHAR2(10) not null,
  tran_seq            VARCHAR2(60) not null,
  ctp_id              VARCHAR2(20) not null,
  constraint PK_T_C primary key (TRAN_SEQ, CTP_ID)
);
desc ZJCG.PER_BORR_CUST_ACCOUNT_INFO;


create table ZJCG.PER_CUST_ACCOUNT_APPLY
(
  tran_seq       VARCHAR2(60) not null,
  id_name        VARCHAR2(60) not null,
  id_typ         VARCHAR2(2) not null,
  id_no          VARCHAR2(30) not null,
  mobile         VARCHAR2(11) not null,
  ctp_login_name VARCHAR2(20),
  ctp_nick_name  VARCHAR2(20),
  acct_no        VARCHAR2(30),
  ccy_type       VARCHAR2(10) not null,
  acc_alias      VARCHAR2(20),
  cust_ename     VARCHAR2(20),
  ebuser_typ     VARCHAR2(1) not null,
  open_teller    VARCHAR2(20),
  serial_no      VARCHAR2(30),
  chk_code       VARCHAR2(20),
  synz_url       VARCHAR2(100),
  tran_date      VARCHAR2(20) not null,
  tran_time      VARCHAR2(20) not null,
  ctp_id         VARCHAR2(20) not null,
  back_url       VARCHAR2(200),
  stat           VARCHAR2(10),
  trans_seq      VARCHAR2(50),
  cust_role      VARCHAR2(5),
  ctp_date       VARCHAR2(18),
  constraint PK_PER_CUST_ACCOUNT_APPLY primary key (TRAN_SEQ, CTP_ID)
);
desc ZJCG.PER_CUST_ACCOUNT_APPLY;


create table ZJCG.PROMPT_MSG_TRANSLATE
(
  prompt_type               VARCHAR2(40) not null,
  prompt_trans_code         VARCHAR2(40) not null,
  prompt_host_trans_code    VARCHAR2(40),
  prompt_msg_code           VARCHAR2(100) not null,
  prompt_previous_msg_value VARCHAR2(150),
  prompt_msg_value          VARCHAR2(150),
  prompt_trans_name         VARCHAR2(100),
  constraint PK_PROMPT_MSG_TRANSLATE primary key (PROMPT_MSG_CODE, PROMPT_TYPE, PROMPT_TRANS_CODE)
);
desc ZJCG.PROMPT_MSG_TRANSLATE;


create table ZJCG.PROT_TEMP
(
  prot_temp_id   VARCHAR2(30) not null,
  prot_temp_name VARCHAR2(300) not null,
  prot_temp_cont CLOB not null,
  up_date        VARCHAR2(32) not null,
  up_teler       VARCHAR2(50) not null,
  prot_temp_type VARCHAR2(100),
  prot_status    VARCHAR2(1),
  is_audi        VARCHAR2(1),
  audi_id        VARCHAR2(30),
  file_type      VARCHAR2(10),
  constraint PK_PROT_TEMP primary key (PROT_TEMP_ID)
);
desc ZJCG.PROT_TEMP;


create table ZJCG.PT_BANK_CODE
(
  msg_type         VARCHAR2(10) not null,
  bank_code        VARCHAR2(18) not null,
  category         VARCHAR2(5),
  cls_code         VARCHAR2(4),
  connmode         VARCHAR2(1),
  frontcode        VARCHAR2(4),
  state_code       VARCHAR2(4),
  city_code        VARCHAR2(6),
  node_code        VARCHAR2(4),
  drec_code        VARCHAR2(18),
  finance_org_code VARCHAR2(14),
  lname            VARCHAR2(200),
  sname            VARCHAR2(100),
  addr             VARCHAR2(100),
  postcode         VARCHAR2(6),
  tel              VARCHAR2(50),
  email            VARCHAR2(30),
  linkman          VARCHAR2(16),
  status           VARCHAR2(50),
  effdate          VARCHAR2(10),
  invdate          VARCHAR2(10),
  alt_type         VARCHAR2(4),
  alt_issno        VARCHAR2(8),
  alt_date_time    VARCHAR2(26),
  remark           VARCHAR2(140),
  legal_person     VARCHAR2(35),
  higher_bank_code VARCHAR2(70),
  accept_bank_code VARCHAR2(14),
  manage_bank_code VARCHAR2(14),
  is_audi          VARCHAR2(1),
  audi_id          VARCHAR2(30),
  constraint PK_PT_BANK_CODE primary key (MSG_TYPE, BANK_CODE)
);
desc ZJCG.PT_BANK_CODE;

create index ZJCG.PT_BANK_CODE_INDEX2 on ZJCG.PT_BANK_CODE (MSG_TYPE, BANK_CODE, CITY_CODE);

create index ZJCG.PT_BANK_CODE_INDEX3 on ZJCG.PT_BANK_CODE (MSG_TYPE, DREC_CODE);

create index ZJCG.PT_BANK_CODE_IX1 on ZJCG.PT_BANK_CODE (MSG_TYPE, CLS_CODE, CITY_CODE);

create unique index ZJCG.PT_BANK_CODE_PK on ZJCG.PT_BANK_CODE (BANK_CODE, MSG_TYPE);


create table ZJCG.PT_CITY
(
  city          VARCHAR2(6) not null,
  city_desc     VARCHAR2(35),
  city_type     VARCHAR2(8),
  city_node     VARCHAR2(4),
  state         VARCHAR2(2),
  remark        VARCHAR2(60),
  outbound_flag VARCHAR2(1),
  update_date   VARCHAR2(8),
  msg_type      VARCHAR2(10) not null,
  province_code VARCHAR2(10),
  constraint PK_PT_CITY primary key (CITY, MSG_TYPE)
);
desc ZJCG.PT_CITY;


create table ZJCG.PT_PROVINCE
(
  province_code VARCHAR2(4) not null,
  province_name VARCHAR2(100),
  province_type VARCHAR2(1),
  constraint PK_PT_PROVINCE primary key (PROVINCE_CODE)
);
desc ZJCG.PT_PROVINCE;


create table ZJCG.PUB_NOTICE
(
  id           VARCHAR2(20) not null,
  catg_code    VARCHAR2(50) not null,
  title        VARCHAR2(120) not null,
  content      CLOB not null,
  expire_time  VARCHAR2(10),
  cre_date     VARCHAR2(200),
  cre_time     VARCHAR2(10),
  up_date      VARCHAR2(10),
  up_tell      VARCHAR2(50),
  use_time     VARCHAR2(10),
  is_audi      VARCHAR2(1),
  audi_id      VARCHAR2(30),
  is_alert     VARCHAR2(1),
  is_must_read VARCHAR2(1),
  constraint PK_PUB_NOTICE primary key (ID)
);
desc ZJCG.PUB_NOTICE;


create table ZJCG.QR_TRANS_VOCHER
(
  id           VARCHAR2(40) not null,
  cifno        VARCHAR2(30) not null,
  acc_no       VARCHAR2(50) not null,
  amount       NUMBER(18,4) not null,
  status       VARCHAR2(2) not null,
  order_no     VARCHAR2(40) not null,
  createdate   VARCHAR2(10),
  createtime   VARCHAR2(10),
  updatedate   VARCHAR2(10),
  trans_log_id VARCHAR2(40),
  rcv_acc_no   VARCHAR2(50),
  constraint PK_QR_TRANS_VOCHER primary key (ID)
);
desc ZJCG.QR_TRANS_VOCHER;


create table ZJCG.REALNAME_CERTIFY_TR
(
  order_id        VARCHAR2(50) not null,
  txn_time        VARCHAR2(20),
  acc_type        VARCHAR2(10),
  acc_no          VARCHAR2(32),
  req_chanel      VARCHAR2(3),
  pass_type       VARCHAR2(20),
  cert_type       VARCHAR2(2),
  cert_no         VARCHAR2(32),
  acc_name        VARCHAR2(50),
  bank_code       VARCHAR2(50),
  bank_name       VARCHAR2(50),
  branch_bank     VARCHAR2(50),
  is_check_mobile VARCHAR2(2),
  mobile_no       VARCHAR2(20),
  is_check_pwd    VARCHAR2(2),
  res_code        VARCHAR2(100),
  res_msg         VARCHAR2(500),
  validity        VARCHAR2(20),
  email           VARCHAR2(50),
  cvn2            VARCHAR2(10),
  remark          VARCHAR2(150),
  prot_no         VARCHAR2(20),
  is_charge       VARCHAR2(2),
  gg_branch_no    VARCHAR2(10) not null,
  ctp_id          VARCHAR2(10),
  ebuser_no       VARCHAR2(20),
  ctp_seq_no      VARCHAR2(60),
  fee_amt         NUMBER(18,4),
  thirdpayseqno   VARCHAR2(30),
  constraint PK_REALNAME_CERTIFY_TR primary key (ORDER_ID)
);
desc ZJCG.REALNAME_CERTIFY_TR;


create table ZJCG.RUN_TASK_CONTROL
(
  thread_id   VARCHAR2(30) not null,
  chan_typ    VARCHAR2(10) not null,
  thread_typ  VARCHAR2(10),
  is_open     VARCHAR2(10) not null,
  thread_name VARCHAR2(50),
  thread_desc VARCHAR2(500),
  start_time  VARCHAR2(20),
  end_time    VARCHAR2(20),
  constraint PK_TASK_CONTROL primary key (THREAD_ID, CHAN_TYP)
);
desc ZJCG.RUN_TASK_CONTROL;

#
#alter table ZJCG.RUN_TASK_CONTROL add constraint IS_OPEN_CHECK check (IS_OPEN='O' OR IS_OPEN='C');


create table ZJCG.SYS_FILTER_PARAM
(
  param_type   VARCHAR2(30),
  param_value  VARCHAR2(30),
  param_remark VARCHAR2(100),
  sort         INTEGER
);
desc ZJCG.SYS_FILTER_PARAM;


create table ZJCG.THREAD_CONTROL_TR
(
  thread_code VARCHAR2(30),
  chan_type   VARCHAR2(10),
  is_open     VARCHAR2(2),
  thread_desc VARCHAR2(200)
);
desc ZJCG.THREAD_CONTROL_TR;


create table ZJCG.TPP_BATCH_TR
(
  batch_no        VARCHAR2(60) not null,
  tpp_seq         VARCHAR2(60),
  acct_no         VARCHAR2(30),
  oper_type       VARCHAR2(2),
  trans_chan_type VARCHAR2(10),
  pro_typ         VARCHAR2(10),
  tot_num         NUMBER(10),
  tot_amt         NUMBER(18,4),
  tran_date       VARCHAR2(20),
  tran_time       VARCHAR2(20),
  batch_state     VARCHAR2(2),
  tot_fee         NUMBER(18,4),
  succ_num        NUMBER(10),
  succ_amt        NUMBER(18,4),
  fail_amt        NUMBER(18,4),
  upay_tot_num    NUMBER(10),
  upay_tot_amt    NUMBER(18,4),
  upay_code       VARCHAR2(200),
  upay_msg        VARCHAR2(500),
  seq_date        VARCHAR2(10),
  seq_time        VARCHAR2(10),
  recharge_seq    VARCHAR2(30),
  cancel_seq      VARCHAR2(50),
  front_date      VARCHAR2(20),
  front_seq       VARCHAR2(50),
  host_result     VARCHAR2(10),
  host_code       VARCHAR2(500),
  host_msg        VARCHAR2(500),
  host_query_seq  VARCHAR2(50),
  is_check        VARCHAR2(2),
  is_today_check  VARCHAR2(2),
  chan_typ        VARCHAR2(5),
  gg_branch_no    VARCHAR2(20),
  gg_teller_no    VARCHAR2(20),
  terminalid      VARCHAR2(20),
  sysid           VARCHAR2(20),
  ctp_id          VARCHAR2(20),
  sxf_clear_flag  VARCHAR2(2),
  dkf_clear_flag  VARCHAR2(2),
  clear_date      VARCHAR2(20),
  upay_batch_id   VARCHAR2(30),
  constraint PK_TPP_BATCH_TR primary key (BATCH_NO)
);
desc ZJCG.TPP_BATCH_TR;


create table ZJCG.USER_LOG_CONF
(
  ebuser_no VARCHAR2(20) not null,
  chan_typ  VARCHAR2(3) not null,
  up_date   VARCHAR2(10),
  up_tell   VARCHAR2(10),
  constraint PK_USER_LOG_CONF primary key (EBUSER_NO, CHAN_TYP)
);
desc ZJCG.USER_LOG_CONF;


create table ZJCG.XABANK_INNER_ACCT
(
  trans_chan_type        VARCHAR2(20) not null,
  xabank_pay_acct        VARCHAR2(30) not null,
  xabank_rec_acct        VARCHAR2(30) not null,
  xabank_fee_pay         VARCHAR2(30),
  xabank_fee_rec         VARCHAR2(30),
  df_setm_pay_acct       VARCHAR2(30),
  df_setm_rec_acct       VARCHAR2(30),
  xabank_pay_name        VARCHAR2(300),
  xabank_rec_name        VARCHAR2(300),
  xabank_fee_pay_dk      VARCHAR2(30),
  xabank_fee_rec_name    VARCHAR2(300),
  df_setm_pay_acct_name  VARCHAR2(300),
  df_setm_rec_acct_name  VARCHAR2(300),
  xabank_fee_rec_df      VARCHAR2(30),
  xabank_fee_rec_df_name VARCHAR2(300),
  constraint PK_XABANK_INNER_ACCT primary key (TRANS_CHAN_TYPE)
);
desc ZJCG.XABANK_INNER_ACCT;

create table ZJCG.CHAN_DEFU_FUNC
(
  chan_typ   VARCHAR2(3) not null,
  sing_type  VARCHAR2(2) not null,
  func_gr_id VARCHAR2(12) not null,
  constraint PK_CHAN_DEFU_FUNC primary key (CHAN_TYP, SING_TYPE)
);
desc ZJCG.CHAN_DEFU_FUNC;

create table ZJCG.CHAN_RESOURCE_REL
(
  id        VARCHAR2(20) not null,
  chan_type VARCHAR2(3) not null,
  res_type  VARCHAR2(2) not null,
  up_date   VARCHAR2(10),
  up_tell   VARCHAR2(20),
  is_audi   VARCHAR2(1),
  audi_id   VARCHAR2(30),
  constraint PK_CHAN_RESOURCE_REL primary key (ID, CHAN_TYPE)
);
desc ZJCG.CHAN_RESOURCE_REL;


create table ZJCG.CHAN_TRANS_AUTH_CHECK_CONF
(
  trans_code   VARCHAR2(100) not null,
  check_type   VARCHAR2(1) not null,
  check_chan   VARCHAR2(30) not null,
  check_column VARCHAR2(50) not null,
  is_null_flag VARCHAR2(1) default '1' not null,
  tran_remark  VARCHAR2(500) not null,
  conf_status  VARCHAR2(1) not null,
  up_date      VARCHAR2(20) not null,
  up_teler     VARCHAR2(15) not null,
  is_audi      VARCHAR2(1),
  audi_id      VARCHAR2(30),
  constraint PK_CHAN_TRANS_AUTH_CHECK_CONF primary key (TRANS_CODE)
);
desc ZJCG.CHAN_TRANS_AUTH_CHECK_CONF;



create table ZJCG.E_LMT_PARM_CT
(
  chan_typ   VARCHAR2(3),
  trans_code VARCHAR2(50),
  tran_way   VARCHAR2(3),
  trans_type VARCHAR2(50)
);
desc ZJCG.E_LMT_PARM_CT;


create sequence ZJCG.TRACK_SEQ_NO
minvalue 1
maxvalue 999999
start with 12287
increment by 1
cache 20
cycle;



create sequence ZJCG.QUEST_PPCM_SEQUENCE_100
minvalue 1
maxvalue 9999999999999999
start with 1
increment by 100
nocache;
#maxvalue 9999999999999999999999999999

create sequence ZJCG.QUEST_PPCM_SNAPSHOT_ID_S
minvalue 1
maxvalue 9999999999999999
start with 121
increment by 1
cache 20
order;
#maxvalue 9999999999999999999999999999

create sequence ZJCG.RECHARGE_SEQ
minvalue 1
maxvalue 99999999
start with 487280
increment by 1
cache 20
cycle;

create sequence ZJCG.TPP_BATCH_FLOW_SEQ
minvalue 10000001
maxvalue 99999999
start with 10002321
increment by 1
cache 20
cycle;


create table ZJCG.DTB_BIND_CARD_TR
(
  tran_seq        VARCHAR2(30) not null,
  ebuser_no       VARCHAR2(20) not null,
  acct_no         VARCHAR2(30) not null,
  acct_tp         VARCHAR2(2),
  bank_code       VARCHAR2(20),
  bank_name       VARCHAR2(50),
  oper_type       VARCHAR2(2) not null,
  oper_date       VARCHAR2(10) not null,
  oper_time       VARCHAR2(10) not null,
  oper_chan       VARCHAR2(3) not null,
  oper_result     VARCHAR2(2) not null,
  hang_date       VARCHAR2(10),
  hang_time       VARCHAR2(10),
  order_no        VARCHAR2(50),
  acct_no_encrypt VARCHAR2(32),
  constraint PK_DTB_BIND_CARD_TR primary key (TRAN_SEQ)
);
desc ZJCG.DTB_BIND_CARD_TR;


create table ZJCG.E_AUTH_TEMPLATE
(
  auth_template_id   VARCHAR2(20) not null,
  ebuser_no          VARCHAR2(20) not null,
  set_all            VARCHAR2(2),
  auth_template_name VARCHAR2(40),
  auth_template_desc VARCHAR2(500),
  start_amt          NUMBER(18,4) not null,
  end_amt            NUMBER(18,4) not null,
  one_auth           INTEGER,
  two_auth           INTEGER,
  three_auth         INTEGER,
  four_auth          INTEGER,
  five_auth          INTEGER,
  temp_status        VARCHAR2(2),
  upd_date           VARCHAR2(10),
  upd_time           VARCHAR2(20),
  upd_operator       VARCHAR2(20),
  constraint PK_E_AUTH_TEMPLATE primary key (AUTH_TEMPLATE_ID, START_AMT, END_AMT)
);
desc ZJCG.E_AUTH_TEMPLATE;


create table ZJCG.E_AUTH_TEMPLATE_REL
(
  ebuser_no        VARCHAR2(20) not null,
  auth_template_id VARCHAR2(20) not null,
  chan_typ         VARCHAR2(3) not null,
  menu_gr_id       VARCHAR2(10) not null,
  constraint PK_E_AUTH_TEMPLATE_REL primary key (EBUSER_NO, AUTH_TEMPLATE_ID, CHAN_TYP, MENU_GR_ID)
);
desc ZJCG.E_AUTH_TEMPLATE_REL;


create table ZJCG.E_OPER_MESSAGE_QUEUE
(
  msg_no      VARCHAR2(30) not null,
  msg_type    VARCHAR2(2) not null,
  queue_no    VARCHAR2(30),
  ebuser_no   VARCHAR2(30) not null,
  oper_id     VARCHAR2(20) not null,
  oper_level  VARCHAR2(2) not null,
  tran_seq    VARCHAR2(40) not null,
  menu_id     VARCHAR2(12) not null,
  busi_code   VARCHAR2(50) not null,
  submit_date VARCHAR2(20),
  msg_content VARCHAR2(500),
  msg_title   VARCHAR2(100),
  auth_acc    VARCHAR2(30),
  state       VARCHAR2(2),
  constraint PK_E_OPER_MESSAGE_QUEUE primary key (MSG_NO)
);
desc ZJCG.E_OPER_MESSAGE_QUEUE;


create table ZJCG.REALNAME_CHECK_TR
(
  tran_seq     VARCHAR2(30),
  order_date   VARCHAR2(20),
  auth_mode    VARCHAR2(2),
  tran_state   VARCHAR2(2),
  check_date   VARCHAR2(20),
  check_result VARCHAR2(2),
  pass_type    VARCHAR2(20),
  is_check     VARCHAR2(2),
  load_date    VARCHAR2(20)
);
desc ZJCG.REALNAME_CHECK_TR;


create table ZJCG.TPP_BATCH_WITHHOLD_TR
(
  seq_no        VARCHAR2(10),
  tran_seq      VARCHAR2(60) not null,
  ctp_id        VARCHAR2(10),
  ctp_seq       VARCHAR2(60),
  pro_id        VARCHAR2(60),
  detail_seq    VARCHAR2(20),
  oper_type     VARCHAR2(5),
  fee_seq_no    VARCHAR2(60),
  fee_acct      VARCHAR2(60),
  fee_name      VARCHAR2(60),
  batch_no      VARCHAR2(60),
  ebuser_no     VARCHAR2(20),
  acct_no       VARCHAR2(32),
  acct_name     VARCHAR2(32),
  ele_acct_no   VARCHAR2(32),
  ele_acct_name VARCHAR2(32),
  stat          VARCHAR2(5),
  id_typ        VARCHAR2(5),
  id_no         VARCHAR2(60),
  id_name       VARCHAR2(30),
  jd_flag       VARCHAR2(5),
  tran_amt      NUMBER(18,4),
  fee           NUMBER(18,4),
  fee_stat      VARCHAR2(5),
  tran_date     VARCHAR2(10),
  tran_time     VARCHAR2(8),
  comple_date   VARCHAR2(10),
  comple_time   VARCHAR2(8),
  host_code     VARCHAR2(500),
  host_msg      VARCHAR2(500),
  recharge_seq  VARCHAR2(60),
  is_own_bank   VARCHAR2(5),
  is_batch      VARCHAR2(5),
  bank_code     VARCHAR2(30),
  bank_name     VARCHAR2(30),
  protoclno     VARCHAR2(50),
  pass_type     VARCHAR2(30),
  dk_batch_no   VARCHAR2(30),
  upay_batch_id VARCHAR2(30),
  upay_code     VARCHAR2(500),
  upay_msg      VARCHAR2(500),
  mobile        VARCHAR2(50),
  flag_state    VARCHAR2(5),
  constraint PK_CTP_BATCH_WITHHOLD_TR primary key (TRAN_SEQ)
);
desc ZJCG.TPP_BATCH_WITHHOLD_TR;
