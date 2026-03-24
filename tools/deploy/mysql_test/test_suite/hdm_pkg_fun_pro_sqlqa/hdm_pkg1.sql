
create table HDM.HDM_AOIP_MPS_QUERY
(
  trade_id       VARCHAR2(60),
  accno          VARCHAR2(19),
  acc_type       VARCHAR2(3),
  details_type   VARCHAR2(20),
  currtype       VARCHAR2(3),
  query_b_date   VARCHAR2(10),
  query_e_date   VARCHAR2(10),
  details_flag   VARCHAR2(1),
  serial_number  VARCHAR2(30),
  batch_no       VARCHAR2(20),
  details_reason VARCHAR2(60),
  bak1           VARCHAR2(60)
);

create table HDM.HDM_CODE_PRM
(
  app_name       VARCHAR2(30) not null,
  prmfieldname   VARCHAR2(50) not null,
  prmcode        VARCHAR2(10) not null,
  prmname        VARCHAR2(4000),
  prmcomment     VARCHAR2(40),
  prmdatefrom    VARCHAR2(8),
  prmdateto      VARCHAR2(8),
  prmeffflag     VARCHAR2(1),
  prmfieldnamech VARCHAR2(50)
);

create table HDM.HDM_EMAIL_MAXCOUNT
(
  table_name VARCHAR2(30) not null,
  max_count  NUMBER,
  CONSTRAINT "HDM_EMAIL_MAXCOUNT_PK" PRIMARY KEY ("TABLE_NAME")
);
create table HDM.HDM_LOG_TASK_DETAIL
(
  d_task_date  VARCHAR2(8) not null,
  c_proc_name  VARCHAR2(400) not null,
  c_min_zoneno VARCHAR2(10) not null,
  c_max_zoneno VARCHAR2(10) not null,
  d_start_time TIMESTAMP(6),
  d_end_time   TIMESTAMP(6),
  n_status     NUMBER(2),
  c_message    VARCHAR2(4000)
);

create table HDM.HDM_NTHCABRP_DAT
(
  zoneno   VARCHAR2(4),
  brno     VARCHAR2(4),
  mbrno    VARCHAR2(4),
  brtype   VARCHAR2(1),
  notes    VARCHAR2(40),
  status   VARCHAR2(1),
  rsfflag  VARCHAR2(1),
  rsflhhh  VARCHAR2(5),
  rsffqh   VARCHAR2(3),
  rsflhhbs VARCHAR2(7),
  rsflhhm  VARCHAR2(60),
  rmbrno   VARCHAR2(5),
  address  VARCHAR2(50),
  zip      VARCHAR2(6),
  phone1   VARCHAR2(22),
  shortnm  VARCHAR2(10),
  actbrno  VARCHAR2(4),
  brstatus VARCHAR2(1),
  lstmodd  VARCHAR2(8),
  bakdec1  VARCHAR2(17),
  bakchar  VARCHAR2(20)
);

create table HDM.HDM_NTHCABRP_S_DAT
(
  zoneno   VARCHAR2(4),
  brno     VARCHAR2(4),
  mbrno    VARCHAR2(4),
  brtype   VARCHAR2(1),
  notes    VARCHAR2(40),
  status   VARCHAR2(1),
  rsfflag  VARCHAR2(1),
  rsflhhh  VARCHAR2(5),
  rsffqh   VARCHAR2(3),
  rsflhhbs VARCHAR2(7),
  rsflhhm  VARCHAR2(60),
  rmbrno   VARCHAR2(5),
  address  VARCHAR2(50),
  zip      VARCHAR2(6),
  phone1   VARCHAR2(22),
  shortnm  VARCHAR2(10),
  actbrno  VARCHAR2(4),
  brstatus VARCHAR2(1),
  lstmodd  VARCHAR2(8),
  bakdec1  VARCHAR2(17),
  bakchar  VARCHAR2(20)
);

create table HDM.HDM_NTHPAZON_S_DAT
(
  zoneno  VARCHAR2(4),
  rsflhhh VARCHAR2(5),
  hsysid  VARCHAR2(4),
  plrflag VARCHAR2(1),
  plaarno VARCHAR2(5),
  plstime VARCHAR2(8),
  pletime VARCHAR2(8),
  notes   VARCHAR2(40),
  cntmode VARCHAR2(1),
  status  VARCHAR2(1),
  lstmodd VARCHAR2(8),
  bakdec1 VARCHAR2(17),
  bakchar VARCHAR2(20)
);

create table HDM.HDM_QUERY_DATE_SPLIT
(
  query_id       VARCHAR2(20) not null,
  up_time_start  VARCHAR2(10),
  up_time_end    VARCHAR2(10),
  our_time_start VARCHAR2(10),
  our_time_end   VARCHAR2(10),
  update_date    VARCHAR2(10),
  c_flag         VARCHAR2(1),
  v_p_i_no       VARCHAR2(2)
);

create table HDM.HDM_QUERY_LOG_DAT
(
  qry_id            VARCHAR2(20) not null,
  qry_date_from     VARCHAR2(10) not null,
  qry_date_to       VARCHAR2(10) not null,
  qry_channel       VARCHAR2(2),
  qry_detail_kind   VARCHAR2(3),
  qry_fee_flag      VARCHAR2(1),
  qry_req_kind      VARCHAR2(1) not null,
  qry_table_name    VARCHAR2(30) not null,
  qry_again_flag    VARCHAR2(1),
  qry_time          VARCHAR2(8),
  qry_status        VARCHAR2(1),
  qry_timestmp_from TIMESTAMP(6),
  qry_timestmp_to   TIMESTAMP(6),
  qry_count         VARCHAR2(8),
  qry_offset        VARCHAR2(8),
  cardacc           VARCHAR2(1),
  no                VARCHAR2(20),
  acserno           VARCHAR2(5),
  currtype          VARCHAR2(3),
  querykind         VARCHAR2(2),
  cino              VARCHAR2(15),
  custareacode      VARCHAR2(5),
  ccid              VARCHAR2(20),
  incardno          VARCHAR2(20),
  transchannel      VARCHAR2(1),
  feeaccno          VARCHAR2(28),
  feeno             VARCHAR2(80),
  zoneno            VARCHAR2(5),
  brno              VARCHAR2(5),
  email             VARCHAR2(200),
  mobno             VARCHAR2(65),
  attachment_count  NUMBER(30),
  serialno          VARCHAR2(20),
  inqflag           VARCHAR2(1),
  fundid            VARCHAR2(12),
  applnum           VARCHAR2(24),
  trxflag           VARCHAR2(1),
  trxknd            VARCHAR2(5),
  booksqno          VARCHAR2(24),
  revtranf          VARCHAR2(1),
  tradetyp          VARCHAR2(1),
  prodid            VARCHAR2(20),
  prtoid            VARCHAR2(34),
  opzoneno          VARCHAR2(5),
  currtype1         VARCHAR2(3),
  fundkind          VARCHAR2(1),
  cashexf           VARCHAR2(3)
);

create table HDM.HDM_QUERY_RETURN
(
  table_name VARCHAR2(30) not null,
  id         VARCHAR2(20) not null,
  query_flag VARCHAR2(1) not null
);

create table HDM.HDM_S_EFTZRZDZ_QUERY_DAT
(
  chanesq  VARCHAR2(27),
  mac      VARCHAR2(17),
  ip       VARCHAR2(32),
  workdate VARCHAR2(10)
);

create table HDM.HDM_S_EFTZRZDZ_QUERY_MONTH
(
  chanesq  VARCHAR2(27),
  mac      VARCHAR2(17),
  ip       VARCHAR2(32),
  workdate VARCHAR2(10)
);

create table HDM.HDM_S_EXPORT_PFEPSADL
(
  accno      VARCHAR2(17),
  mdcardno   VARCHAR2(19),
  busidate   VARCHAR2(10),
  timestmp   VARCHAR2(26),
  summflag   VARCHAR2(4000),
  currtype   VARCHAR2(3),
  currtype_c VARCHAR2(4000),
  cashexf    VARCHAR2(1),
  cashexf_c  VARCHAR2(4000),
  drcrf      VARCHAR2(4000),
  amount     NUMBER(30,2),
  balance    NUMBER(30,2),
  servface   VARCHAR2(4000),
  tellerno   VARCHAR2(5),
  brno       VARCHAR2(4000),
  zoneno     VARCHAR2(4000),
  cashnote   VARCHAR2(20),
  trxcode    VARCHAR2(4000),
  workdate   VARCHAR2(10),
  authtlno   VARCHAR2(5),
  recipact   VARCHAR2(34),
  phybrno    VARCHAR2(5),
  termid     VARCHAR2(15),
  trxcurr    VARCHAR2(300),
  trxamt     NUMBER(30,2),
  crycode    VARCHAR2(300),
  serialno   VARCHAR2(11),
  trxsite    VARCHAR2(40),
  recipcn    VARCHAR2(15),
  id         VARCHAR2(20),
  busitime   VARCHAR2(20)
);

create table HDM.HDM_S_EXPORT_PFEPSADL_UP
(
  accno       VARCHAR2(17),
  mdcardno    VARCHAR2(19),
  busidate    VARCHAR2(10),
  timestmp    VARCHAR2(26),
  summflag    VARCHAR2(4000),
  currtype    VARCHAR2(3),
  currtype_c  VARCHAR2(4000),
  cashexf     VARCHAR2(1),
  cashexf_c   VARCHAR2(4000),
  drcrf       VARCHAR2(4000),
  amount      NUMBER(30,2),
  balance     NUMBER(30,2),
  servface    VARCHAR2(4000),
  tellerno    VARCHAR2(5),
  brno        VARCHAR2(4000),
  zoneno      VARCHAR2(4000),
  cashnote    VARCHAR2(20),
  trxcode     VARCHAR2(4000),
  workdate    VARCHAR2(10),
  authtlno    VARCHAR2(5),
  recipact    VARCHAR2(34),
  phybrno     VARCHAR2(5),
  termid      VARCHAR2(15),
  trxcurr     VARCHAR2(300),
  trxamt      NUMBER(30,2),
  crycode     VARCHAR2(300),
  serialno    VARCHAR2(11),
  trxsite     VARCHAR2(40),
  recipcn     VARCHAR2(15),
  id          VARCHAR2(20),
  busitime    VARCHAR2(20),
  zoneno_real VARCHAR2(5),
  brno_real   VARCHAR2(5),
  worktime    VARCHAR2(8),
  ptrxcode    VARCHAR2(5),
  authzone    VARCHAR2(5),
  trxserno    VARCHAR2(23),
  medidtp     VARCHAR2(3),
  agaccno     VARCHAR2(17),
  orgmediumno VARCHAR2(34),
  conftype    VARCHAR2(3),
  orgcode     VARCHAR2(10),
  orgname     VARCHAR2(40),
  busino      VARCHAR2(15),
  resavname   VARCHAR2(60),
  rebankname  VARCHAR2(40),
  prodtype    VARCHAR2(3),
  saprodtid   VARCHAR2(13),
  severcode   VARCHAR2(9),
  orgnotes    VARCHAR2(60),
  notes       VARCHAR2(20)
);

create table HDM.HDM_S_EXPORT_PFQABDTL
(
  accno    VARCHAR2(20),
  serialno VARCHAR2(20),
  currtype VARCHAR2(20),
  cashexf  VARCHAR2(20),
  busidate VARCHAR2(20),
  trxcurr  VARCHAR2(20),
  trxamt   VARCHAR2(20),
  crycode  VARCHAR2(20)
);

create table HDM.HDM_S_PFEPABDL_QUERY_DAT
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  busidate   VARCHAR2(10),
  amount     NUMBER(18),
  mdcardno   VARCHAR2(19),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  servface   VARCHAR2(3),
  trxcurr    VARCHAR2(3),
  trxamt     NUMBER(18),
  crycode    VARCHAR2(3),
  feecashexf VARCHAR2(1),
  feeamt     NUMBER(18),
  feecurr    VARCHAR2(3),
  trxfeeamt  NUMBER(18),
  trxfeecurr VARCHAR2(3),
  cashnote   VARCHAR2(20),
  timestmp   VARCHAR2(26),
  eserialno  VARCHAR2(22),
  workdate   VARCHAR2(10),
  bakfield1  VARCHAR2(100),
  zoneaccno  VARCHAR2(4)
);

create table HDM.HDM_S_PFEPABDL_QUERY_MONTH
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  busidate   VARCHAR2(10),
  amount     NUMBER(18),
  mdcardno   VARCHAR2(19),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  servface   VARCHAR2(3),
  trxcurr    VARCHAR2(3),
  trxamt     NUMBER(18),
  crycode    VARCHAR2(3),
  feecashexf VARCHAR2(1),
  feeamt     NUMBER(18),
  feecurr    VARCHAR2(3),
  trxfeeamt  NUMBER(18),
  trxfeecurr VARCHAR2(3),
  cashnote   VARCHAR2(20),
  timestmp   VARCHAR2(26),
  eserialno  VARCHAR2(22),
  workdate   VARCHAR2(10),
  bakfield1  VARCHAR2(100),
  zoneaccno  VARCHAR2(4)
);

create table HDM.HDM_S_PFEPCBDL_QUERY_DAT
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  savitem    VARCHAR2(3),
  monint     NUMBER(18),
  rate       NUMBER(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  cbprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPCBDL_QUERY_MONTH
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  savitem    VARCHAR2(3),
  monint     NUMBER(18),
  rate       NUMBER(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  cbprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPFODL_QUERY_DAT
(
  accno         VARCHAR2(17),
  serialno      VARCHAR2(11),
  currtype      VARCHAR2(3),
  cashexf       VARCHAR2(1),
  actbrno       VARCHAR2(5),
  phybrno       VARCHAR2(5),
  subcode       VARCHAR2(7),
  cino          VARCHAR2(15),
  busidate      VARCHAR2(10),
  busitime      VARCHAR2(8),
  trxcode       VARCHAR2(5),
  workdate      VARCHAR2(10),
  valueday      VARCHAR2(10),
  closdate      VARCHAR2(10),
  drcrf         VARCHAR2(1),
  vouhno        NUMBER(9),
  amount        NUMBER(18),
  balance       NUMBER(18),
  closint       NUMBER(18),
  keepint       NUMBER(18),
  inttax        NUMBER(18),
  summflag      VARCHAR2(1),
  cashnote      VARCHAR2(20),
  savterm       VARCHAR2(3),
  nextterm      VARCHAR2(3),
  rate          NUMBER(9),
  matudate      VARCHAR2(10),
  zoneno        VARCHAR2(5),
  brno          VARCHAR2(5),
  tellerno      VARCHAR2(5),
  trxsqno       VARCHAR2(5),
  servface      VARCHAR2(3),
  authono       VARCHAR2(5),
  authtlno      VARCHAR2(5),
  termid        VARCHAR2(15),
  timestmp      VARCHAR2(26),
  recipact      VARCHAR2(34),
  mdcardno      VARCHAR2(19),
  acserno       VARCHAR2(5),
  synflag       VARCHAR2(1),
  showflag      VARCHAR2(1),
  macardn       VARCHAR2(19),
  recipcn       VARCHAR2(15),
  catrflag      VARCHAR2(1),
  sellercode    VARCHAR2(20),
  amount1       NUMBER(18),
  amount2       NUMBER(18),
  flag1         VARCHAR2(9),
  date1         VARCHAR2(10),
  clevel        VARCHAR2(3),
  partygroup_no VARCHAR2(10),
  client_asset  NUMBER(18),
  zoneaccno     VARCHAR2(4),
  eserialno     VARCHAR2(27),
  foprodtid     VARCHAR2(13),
  fonxtprodtid  VARCHAR2(13),
  prodtid       VARCHAR2(13)
);

create table HDM.HDM_S_PFEPFODL_QUERY_MONTH
(
  accno         VARCHAR2(17),
  serialno      VARCHAR2(11),
  currtype      VARCHAR2(3),
  cashexf       VARCHAR2(1),
  actbrno       VARCHAR2(5),
  phybrno       VARCHAR2(5),
  subcode       VARCHAR2(7),
  cino          VARCHAR2(15),
  busidate      VARCHAR2(10),
  busitime      VARCHAR2(8),
  trxcode       VARCHAR2(5),
  workdate      VARCHAR2(10),
  valueday      VARCHAR2(10),
  closdate      VARCHAR2(10),
  drcrf         VARCHAR2(1),
  vouhno        NUMBER(9),
  amount        NUMBER(18),
  balance       NUMBER(18),
  closint       NUMBER(18),
  keepint       NUMBER(18),
  inttax        NUMBER(18),
  summflag      VARCHAR2(1),
  cashnote      VARCHAR2(20),
  savterm       VARCHAR2(3),
  nextterm      VARCHAR2(3),
  rate          NUMBER(9),
  matudate      VARCHAR2(10),
  zoneno        VARCHAR2(5),
  brno          VARCHAR2(5),
  tellerno      VARCHAR2(5),
  trxsqno       VARCHAR2(5),
  servface      VARCHAR2(3),
  authono       VARCHAR2(5),
  authtlno      VARCHAR2(5),
  termid        VARCHAR2(15),
  timestmp      VARCHAR2(26),
  recipact      VARCHAR2(34),
  mdcardno      VARCHAR2(19),
  acserno       VARCHAR2(5),
  synflag       VARCHAR2(1),
  showflag      VARCHAR2(1),
  macardn       VARCHAR2(19),
  recipcn       VARCHAR2(15),
  catrflag      VARCHAR2(1),
  sellercode    VARCHAR2(20),
  amount1       NUMBER(18),
  amount2       NUMBER(18),
  flag1         VARCHAR2(9),
  date1         VARCHAR2(10),
  clevel        VARCHAR2(3),
  partygroup_no VARCHAR2(10),
  client_asset  NUMBER(18),
  zoneaccno     VARCHAR2(4),
  eserialno     VARCHAR2(27),
  foprodtid     VARCHAR2(13),
  fonxtprodtid  VARCHAR2(13),
  prodtid       VARCHAR2(13)
);

create table HDM.HDM_S_PFEPGKDL_QUERY_DAT
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  yearkind   VARCHAR2(20),
  rate       NUMBER(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4)
);

create table HDM.HDM_S_PFEPGKDL_QUERY_MONTH
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  yearkind   VARCHAR2(20),
  rate       NUMBER(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4)
);

create table HDM.HDM_S_PFEPNFDL_QUERY_DAT
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  nfprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);
create table HDM.HDM_S_PFEPNFDL_QUERY_MONTH
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  nfprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPOTDL_LIST_HIS
(
  mdcardno   VARCHAR2(19),
  serialno   VARCHAR2(11),
  acappno    VARCHAR2(3),
  acserno    VARCHAR2(5),
  accno      VARCHAR2(17),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  openzone   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  amount     NUMBER(18),
  balance    NUMBER(18),
  backbal    NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  drwterm    VARCHAR2(5),
  finano     VARCHAR2(5),
  fseqno     VARCHAR2(9),
  fincode    VARCHAR2(17),
  matudate   VARCHAR2(10),
  fintype    VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  date2      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20) not null
);
create table HDM.HDM_S_PFEPOTDL_QUERY_DAT
(
  mdcardno   VARCHAR2(19),
  serialno   VARCHAR2(11),
  acappno    VARCHAR2(3),
  acserno    VARCHAR2(5),
  accno      VARCHAR2(17),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  openzone   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  amount     NUMBER(18),
  balance    NUMBER(18),
  backbal    NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  drwterm    VARCHAR2(5),
  finano     VARCHAR2(5),
  fseqno     NUMBER(9),
  fincode    NUMBER(17),
  matudate   VARCHAR2(10),
  fintype    VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  date2      VARCHAR2(10),
  zoneaccno  VARCHAR2(4)
);

create table HDM.HDM_S_PFEPOTDL_QUERY_LIST
(
  mdcardno   VARCHAR2(19),
  serialno   VARCHAR2(11),
  acappno    VARCHAR2(3),
  acserno    VARCHAR2(5),
  accno      VARCHAR2(17),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  openzone   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  amount     NUMBER(18),
  balance    NUMBER(18),
  backbal    NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  drwterm    VARCHAR2(5),
  finano     VARCHAR2(5),
  fseqno     VARCHAR2(9),
  fincode    VARCHAR2(17),
  matudate   VARCHAR2(10),
  fintype    VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  date2      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_PFEPOTDL_QUERY_MONTH
(
  mdcardno   VARCHAR2(19),
  serialno   VARCHAR2(11),
  acappno    VARCHAR2(3),
  acserno    VARCHAR2(5),
  accno      VARCHAR2(17),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  openzone   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  amount     NUMBER(18),
  balance    NUMBER(18),
  backbal    NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  drwterm    VARCHAR2(5),
  finano     VARCHAR2(5),
  fseqno     NUMBER(9),
  fincode    NUMBER(17),
  matudate   VARCHAR2(10),
  fintype    VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  date2      VARCHAR2(10),
  zoneaccno  VARCHAR2(4)
);

create table HDM.HDM_S_PFEPOTDL_RETURN
(
  mdcardno   VARCHAR2(19),
  serialno   VARCHAR2(11),
  acappno    VARCHAR2(3),
  acserno    VARCHAR2(5),
  accno      VARCHAR2(17),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  openzone   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  amount     NUMBER(18),
  balance    NUMBER(18),
  backbal    NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  drwterm    VARCHAR2(5),
  finano     VARCHAR2(5),
  fseqno     VARCHAR2(9),
  fincode    VARCHAR2(17),
  matudate   VARCHAR2(10),
  fintype    VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  date2      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_PFEPOTDL_RETURN_HIS
(
  mdcardno   VARCHAR2(19),
  serialno   VARCHAR2(11),
  acappno    VARCHAR2(3),
  acserno    VARCHAR2(5),
  accno      VARCHAR2(17),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  openzone   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  amount     NUMBER(18),
  balance    NUMBER(18),
  backbal    NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  drwterm    VARCHAR2(5),
  finano     VARCHAR2(5),
  fseqno     VARCHAR2(9),
  fincode    VARCHAR2(17),
  matudate   VARCHAR2(10),
  fintype    VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  date2      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_PFEPSADL_LIST_HIS
(
  accno       VARCHAR2(17),
  serialno    VARCHAR2(11),
  currtype    VARCHAR2(3),
  cashexf     VARCHAR2(1),
  actbrno     VARCHAR2(5),
  phybrno     VARCHAR2(5),
  subcode     VARCHAR2(7),
  cino        VARCHAR2(15),
  accatrbt    VARCHAR2(3),
  busidate    VARCHAR2(10),
  busitime    VARCHAR2(8),
  trxcode     VARCHAR2(5),
  workdate    VARCHAR2(10),
  valueday    VARCHAR2(10),
  closdate    VARCHAR2(10),
  drcrf       VARCHAR2(1),
  vouhno      VARCHAR2(9),
  amount      NUMBER(18),
  balance     NUMBER(18),
  closint     NUMBER(18),
  inttax      NUMBER(18),
  summflag    VARCHAR2(1),
  cashnote    VARCHAR2(20),
  zoneno      VARCHAR2(5),
  brno        VARCHAR2(5),
  tellerno    VARCHAR2(5),
  trxsqno     VARCHAR2(5),
  servface    VARCHAR2(3),
  authono     VARCHAR2(5),
  authtlno    VARCHAR2(5),
  termid      VARCHAR2(15),
  timestmp    VARCHAR2(26),
  recipact    VARCHAR2(34),
  mdcardno    VARCHAR2(19),
  acserno     VARCHAR2(5),
  synflag     VARCHAR2(1),
  showflag    VARCHAR2(1),
  macardn     VARCHAR2(19),
  recipcn     VARCHAR2(15),
  catrflag    VARCHAR2(1),
  sellercode  VARCHAR2(20),
  amount1     NUMBER(18),
  amount2     NUMBER(18),
  flag1       VARCHAR2(9),
  date1       VARCHAR2(10),
  eserialno   VARCHAR2(27),
  trxsite     VARCHAR2(40),
  c_month     NUMBER(2),
  zoneaccno   VARCHAR2(4),
  id          VARCHAR2(20) not null,
  worktime    VARCHAR2(8),
  ptrxcode    VARCHAR2(5),
  authzone    VARCHAR2(5),
  trxserno    VARCHAR2(23),
  medidtp     VARCHAR2(3),
  agaccno     VARCHAR2(17),
  orgmediumno VARCHAR2(34),
  conftype    VARCHAR2(3),
  orgcode     VARCHAR2(10),
  orgname     VARCHAR2(40),
  busino      VARCHAR2(15),
  resavname   VARCHAR2(60),
  rebankname  VARCHAR2(40),
  prodtype    VARCHAR2(3),
  saprodtid   VARCHAR2(13),
  severcode   VARCHAR2(9),
  orgnotes    VARCHAR2(60),
  notes       VARCHAR2(20)
);

create table HDM.HDM_S_PFEPSADL_QUERY_DAT
(
  accno       VARCHAR2(17),
  serialno    VARCHAR2(11),
  currtype    VARCHAR2(3),
  cashexf     VARCHAR2(1),
  actbrno     VARCHAR2(5),
  phybrno     VARCHAR2(5),
  subcode     VARCHAR2(7),
  cino        VARCHAR2(15),
  accatrbt    VARCHAR2(3),
  busidate    VARCHAR2(10),
  busitime    VARCHAR2(8),
  trxcode     VARCHAR2(5),
  workdate    VARCHAR2(10),
  valueday    VARCHAR2(10),
  closdate    VARCHAR2(10),
  drcrf       VARCHAR2(1),
  vouhno      NUMBER(9),
  amount      NUMBER(18),
  balance     NUMBER(18),
  closint     NUMBER(18),
  inttax      NUMBER(18),
  summflag    VARCHAR2(1),
  cashnote    VARCHAR2(20),
  zoneno      VARCHAR2(5),
  brno        VARCHAR2(5),
  tellerno    VARCHAR2(5),
  trxsqno     VARCHAR2(5),
  servface    VARCHAR2(3),
  authono     VARCHAR2(5),
  authtlno    VARCHAR2(5),
  termid      VARCHAR2(15),
  timestmp    VARCHAR2(26),
  recipact    VARCHAR2(34),
  mdcardno    VARCHAR2(19),
  acserno     VARCHAR2(5),
  synflag     VARCHAR2(1),
  showflag    VARCHAR2(1),
  macardn     VARCHAR2(19),
  recipcn     VARCHAR2(15),
  catrflag    VARCHAR2(1),
  sellercode  VARCHAR2(20),
  amount1     NUMBER(18),
  amount2     NUMBER(17),
  flag1       VARCHAR2(9),
  date1       VARCHAR2(10),
  eserialno   VARCHAR2(27),
  trxsite     VARCHAR2(40),
  zoneaccno   VARCHAR2(4),
  worktime    VARCHAR2(8),
  ptrxcode    VARCHAR2(5),
  authzone    VARCHAR2(5),
  trxserno    VARCHAR2(23),
  medidtp     VARCHAR2(3),
  agaccno     VARCHAR2(17),
  orgmediumno VARCHAR2(34),
  conftype    VARCHAR2(3),
  orgcode     VARCHAR2(10),
  orgname     VARCHAR2(40),
  busino      VARCHAR2(15),
  resavname   VARCHAR2(60),
  rebankname  VARCHAR2(40),
  prodtype    VARCHAR2(3),
  saprodtid   VARCHAR2(13),
  severcode   VARCHAR2(9),
  orgnotes    VARCHAR2(60),
  notes       VARCHAR2(20)
);

create table HDM.HDM_S_PFEPSADL_QUERY_LIST
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  accatrbt   VARCHAR2(3),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  eserialno  VARCHAR2(27),
  trxsite    VARCHAR2(40),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20) not null,
  resavname  VARCHAR2(60),
  rebankname VARCHAR2(40)
);

create table HDM.HDM_S_PFEPSADL_QUERY_MONTH
(
  accno       VARCHAR2(17),
  serialno    VARCHAR2(11),
  currtype    VARCHAR2(3),
  cashexf     VARCHAR2(1),
  actbrno     VARCHAR2(5),
  phybrno     VARCHAR2(5),
  subcode     VARCHAR2(7),
  cino        VARCHAR2(15),
  accatrbt    VARCHAR2(3),
  busidate    VARCHAR2(10),
  busitime    VARCHAR2(8),
  trxcode     VARCHAR2(5),
  workdate    VARCHAR2(10),
  valueday    VARCHAR2(10),
  closdate    VARCHAR2(10),
  drcrf       VARCHAR2(1),
  vouhno      NUMBER(9),
  amount      NUMBER(18),
  balance     NUMBER(18),
  closint     NUMBER(18),
  inttax      NUMBER(18),
  summflag    VARCHAR2(1),
  cashnote    VARCHAR2(20),
  zoneno      VARCHAR2(5),
  brno        VARCHAR2(5),
  tellerno    VARCHAR2(5),
  trxsqno     VARCHAR2(5),
  servface    VARCHAR2(3),
  authono     VARCHAR2(5),
  authtlno    VARCHAR2(5),
  termid      VARCHAR2(15),
  timestmp    VARCHAR2(26),
  recipact    VARCHAR2(34),
  mdcardno    VARCHAR2(19),
  acserno     VARCHAR2(5),
  synflag     VARCHAR2(1),
  showflag    VARCHAR2(1),
  macardn     VARCHAR2(19),
  recipcn     VARCHAR2(15),
  catrflag    VARCHAR2(1),
  sellercode  VARCHAR2(20),
  amount1     NUMBER(18),
  amount2     NUMBER(17),
  flag1       VARCHAR2(9),
  date1       VARCHAR2(10),
  eserialno   VARCHAR2(27),
  trxsite     VARCHAR2(40),
  zoneaccno   VARCHAR2(4),
  worktime    VARCHAR2(8),
  ptrxcode    VARCHAR2(5),
  authzone    VARCHAR2(5),
  trxserno    VARCHAR2(23),
  medidtp     VARCHAR2(3),
  agaccno     VARCHAR2(17),
  orgmediumno VARCHAR2(34),
  conftype    VARCHAR2(3),
  orgcode     VARCHAR2(10),
  orgname     VARCHAR2(40),
  busino      VARCHAR2(15),
  resavname   VARCHAR2(60),
  rebankname  VARCHAR2(40),
  prodtype    VARCHAR2(3),
  saprodtid   VARCHAR2(13),
  severcode   VARCHAR2(9),
  orgnotes    VARCHAR2(60),
  notes       VARCHAR2(20)
);
create table HDM.HDM_S_PFEPSADL_RETURN
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  accatrbt   VARCHAR2(3),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  eserialno  VARCHAR2(27),
  trxsite    VARCHAR2(40),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_PFEPSADL_RETURN_HIS
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  accatrbt   VARCHAR2(3),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  eserialno  VARCHAR2(27),
  trxsite    VARCHAR2(40),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_PFEPTZDL_QUERY_DAT
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  prasinff   VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  adprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPTZDL_QUERY_MONTH
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  prasinff   VARCHAR2(3),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  adprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPVMDL_LIST_HIS
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  rate       VARCHAR2(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20) not null,
  eserialno  VARCHAR2(27),
  vmprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPVMDL_QUERY_DAT
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  rate       NUMBER(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  vmprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);


create table HDM.HDM_S_PFEPVMDL_QUERY_MONTH
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     NUMBER(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  rate       NUMBER(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  zoneaccno  VARCHAR2(4),
  eserialno  VARCHAR2(27),
  vmprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPVMDL_RETURN
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  rate       VARCHAR2(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1),
  eserialno  VARCHAR2(27),
  vmprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFEPVMDL_RETURN_HIS
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  rate       VARCHAR2(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1),
  eserialno  VARCHAR2(27),
  vmprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);

create table HDM.HDM_S_PFQABDTL_RETURN
(
  cfzoneo    VARCHAR2(5),
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  busidate   VARCHAR2(10),
  amount     NUMBER(18),
  mdcardno   VARCHAR2(19),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  servface   VARCHAR2(3),
  trxcurr    VARCHAR2(3),
  trxamt     NUMBER(18),
  crycode    VARCHAR2(3),
  feecashexf VARCHAR2(1),
  feeamt     NUMBER(18),
  feecurr    VARCHAR2(3),
  trxfeeamt  NUMBER(18),
  trxfeecurr VARCHAR2(3),
  cashnote   VARCHAR2(20),
  timestmp   VARCHAR2(26),
  eserialno  VARCHAR2(22),
  workdate   VARCHAR2(10),
  bakfield1  VARCHAR2(100),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_PFQABDTL_RETURN_HIS
(
  cfzoneo    VARCHAR2(5),
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  busidate   VARCHAR2(10),
  amount     NUMBER(18),
  mdcardno   VARCHAR2(19),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  servface   VARCHAR2(3),
  trxcurr    VARCHAR2(3),
  trxamt     NUMBER(18),
  crycode    VARCHAR2(3),
  feecashexf VARCHAR2(1),
  feeamt     NUMBER(18),
  feecurr    VARCHAR2(3),
  trxfeeamt  NUMBER(18),
  trxfeecurr VARCHAR2(3),
  cashnote   VARCHAR2(20),
  timestmp   VARCHAR2(26),
  eserialno  VARCHAR2(22),
  workdate   VARCHAR2(10),
  bakfield1  VARCHAR2(100),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1)
);

create table HDM.HDM_S_TB1560_PERSON_DTH
(
  query_id_real   VARCHAR2(30),
  query_id        VARCHAR2(60),
  c_flag_dth      VARCHAR2(2),
  c_lgldocno      VARCHAR2(60),
  cino            VARCHAR2(60),
  accno           VARCHAR2(120),
  app_no          VARCHAR2(2),
  currtype        VARCHAR2(120),
  acserno         VARCHAR2(60),
  opendate        VARCHAR2(20),
  query_date_from VARCHAR2(20),
  query_date_to   VARCHAR2(20),
  openbrno        VARCHAR2(20),
  mdcardno        VARCHAR2(120),
  batch_date      VARCHAR2(15)
);

create table HDM.hdm_s_pfepvmdl_query_list
(
  accno      VARCHAR2(17),
  serialno   VARCHAR2(11),
  currtype   VARCHAR2(3),
  cashexf    VARCHAR2(1),
  actbrno    VARCHAR2(5),
  phybrno    VARCHAR2(5),
  subcode    VARCHAR2(7),
  cino       VARCHAR2(15),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxcode    VARCHAR2(5),
  workdate   VARCHAR2(10),
  valueday   VARCHAR2(10),
  closdate   VARCHAR2(10),
  drcrf      VARCHAR2(1),
  vouhno     VARCHAR2(9),
  amount     NUMBER(18),
  balance    NUMBER(18),
  closint    NUMBER(18),
  keepint    NUMBER(18),
  inttax     NUMBER(18),
  summflag   VARCHAR2(1),
  cashnote   VARCHAR2(20),
  savterm    VARCHAR2(3),
  rate       VARCHAR2(9),
  matudate   VARCHAR2(10),
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  trxsqno    VARCHAR2(5),
  servface   VARCHAR2(3),
  authono    VARCHAR2(5),
  authtlno   VARCHAR2(5),
  termid     VARCHAR2(15),
  timestmp   VARCHAR2(26),
  recipact   VARCHAR2(34),
  mdcardno   VARCHAR2(19),
  acserno    VARCHAR2(5),
  synflag    VARCHAR2(1),
  showflag   VARCHAR2(1),
  macardn    VARCHAR2(19),
  recipcn    VARCHAR2(15),
  catrflag   VARCHAR2(1),
  sellercode VARCHAR2(20),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  flag1      VARCHAR2(9),
  date1      VARCHAR2(10),
  c_month    NUMBER(2),
  zoneaccno  VARCHAR2(4),
  id         VARCHAR2(20),
  query_flag VARCHAR2(1),
  eserialno  VARCHAR2(27),
  vmprodtid  VARCHAR2(13),
  prodtid    VARCHAR2(13)
);
CREATE OR REPLACE  VIEW HDM.HDM_S_EFTZRZDZ_QUERY AS
SELECT CHANESQ,
MAC,
IP,
WORKDATE
FROM HDM_S_EFTZRZDZ_QUERY_DAT
WHERE WORKDATE<='2020-12-31'
UNION ALL
SELECT CHANESQ,
MAC,
IP,
WORKDATE
FROM HDM_S_EFTZRZDZ_QUERY_MONTH
WHERE WORKDATE>='2021-01-01' AND WORKDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPABDL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
BUSIDATE,
AMOUNT,
MDCARDNO,
ZONENO,
BRNO,
TELLERNO,
SERVFACE,
TRXCURR,
TRXAMT,
CRYCODE,
FEECASHEXF,
FEEAMT,
FEECURR,
TRXFEEAMT,
TRXFEECURR,
CASHNOTE,
TIMESTMP,
ESERIALNO,
WORKDATE,
BAKFIELD1,
ZONEACCNO FROM HDM_S_PFEPABDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
BUSIDATE,
AMOUNT,
MDCARDNO,
ZONENO,
BRNO,
TELLERNO,
SERVFACE,
TRXCURR,
TRXAMT,
CRYCODE,
FEECASHEXF,
FEEAMT,
FEECURR,
TRXFEEAMT,
TRXFEECURR,
CASHNOTE,
TIMESTMP,
ESERIALNO,
WORKDATE,
BAKFIELD1,
ZONEACCNO FROM HDM_S_PFEPABDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPCBDL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
SAVITEM,
MONINT,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
CBPRODTID,
PRODTID FROM HDM_S_PFEPCBDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
SAVITEM,
MONINT,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
CBPRODTID,
PRODTID FROM HDM_S_PFEPCBDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPFODL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
NEXTTERM,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
CLEVEL,
PARTYGROUP_NO,
CLIENT_ASSET,
ZONEACCNO,
ESERIALNO,
FOPRODTID,
FONXTPRODTID,
PRODTID FROM HDM_S_PFEPFODL_QUERY_DAT where BUSIDATE<='2020-12-31'
union all
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
NEXTTERM,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
CLEVEL,
PARTYGROUP_NO,
CLIENT_ASSET,
ZONEACCNO,
ESERIALNO,
FOPRODTID,
FONXTPRODTID,
PRODTID FROM HDM_S_PFEPFODL_QUERY_MONTH where BUSIDATE>='2021-01-01' and BUSIDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPGKDL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
YEARKIND,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO FROM HDM_S_PFEPGKDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
YEARKIND,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO FROM HDM_S_PFEPGKDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPNFDL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
NFPRODTID,
PRODTID FROM HDM_S_PFEPNFDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
NFPRODTID,
PRODTID FROM HDM_S_PFEPNFDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPOTDL_QUERY AS
SELECT MDCARDNO,
SERIALNO,
ACAPPNO,
ACSERNO,
ACCNO,
CURRTYPE,
CASHEXF,
OPENZONE,
ACTBRNO,
PHYBRNO,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
AMOUNT,
BALANCE,
BACKBAL,
SUMMFLAG,
CASHNOTE,
DRWTERM,
FINANO,
FSEQNO,
FINCODE,
MATUDATE,
FINTYPE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
DATE2,
ZONEACCNO FROM HDM_S_PFEPOTDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT MDCARDNO,
SERIALNO,
ACAPPNO,
ACSERNO,
ACCNO,
CURRTYPE,
CASHEXF,
OPENZONE,
ACTBRNO,
PHYBRNO,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
AMOUNT,
BALANCE,
BACKBAL,
SUMMFLAG,
CASHNOTE,
DRWTERM,
FINANO,
FSEQNO,
FINCODE,
MATUDATE,
FINTYPE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
DATE2,
ZONEACCNO FROM HDM_S_PFEPOTDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';



CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPSADL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
ACCATRBT,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ESERIALNO,
TRXSITE,
ZONEACCNO,
WORKTIME,
PTRXCODE,
AUTHZONE,
TRXSERNO,
MEDIDTP,
AGACCNO,
ORGMEDIUMNO,
CONFTYPE,
ORGCODE,
ORGNAME,
BUSINO,
RESAVNAME,
REBANKNAME,
PRODTYPE,
SAPRODTID,
SEVERCODE,
ORGNOTES,
NOTES FROM HDM_S_PFEPSADL_QUERY_DAT where BUSIDATE<='2020-12-31'
union all
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
ACCATRBT,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ESERIALNO,
TRXSITE,
ZONEACCNO,
WORKTIME,
PTRXCODE,
AUTHZONE,
TRXSERNO,
MEDIDTP,
AGACCNO,
ORGMEDIUMNO,
CONFTYPE,
ORGCODE,
ORGNAME,
BUSINO,
RESAVNAME,
REBANKNAME,
PRODTYPE,
SAPRODTID,
SEVERCODE,
ORGNOTES,
NOTES FROM HDM_S_PFEPSADL_QUERY_MONTH where BUSIDATE>='2021-01-01' and BUSIDATE<='2021-01-06';

CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPTZDL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
PRASINFF,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
ADPRODTID,
PRODTID FROM HDM_S_PFEPTZDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
PRASINFF,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
ADPRODTID,
PRODTID FROM HDM_S_PFEPTZDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';


CREATE OR REPLACE  VIEW HDM.HDM_S_PFEPVMDL_QUERY AS
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
VMPRODTID,
PRODTID FROM HDM_S_PFEPVMDL_QUERY_DAT WHERE BUSIDATE<='2020-12-31'
UNION ALL
SELECT ACCNO,
SERIALNO,
CURRTYPE,
CASHEXF,
ACTBRNO,
PHYBRNO,
SUBCODE,
CINO,
BUSIDATE,
BUSITIME,
TRXCODE,
WORKDATE,
VALUEDAY,
CLOSDATE,
DRCRF,
VOUHNO,
AMOUNT,
BALANCE,
CLOSINT,
KEEPINT,
INTTAX,
SUMMFLAG,
CASHNOTE,
SAVTERM,
RATE,
MATUDATE,
ZONENO,
BRNO,
TELLERNO,
TRXSQNO,
SERVFACE,
AUTHONO,
AUTHTLNO,
TERMID,
TIMESTMP,
RECIPACT,
MDCARDNO,
ACSERNO,
SYNFLAG,
SHOWFLAG,
MACARDN,
RECIPCN,
CATRFLAG,
SELLERCODE,
AMOUNT1,
AMOUNT2,
FLAG1,
DATE1,
ZONEACCNO,
ESERIALNO,
VMPRODTID,
PRODTID FROM HDM_S_PFEPVMDL_QUERY_MONTH WHERE BUSIDATE>='2021-01-01' AND BUSIDATE<='2021-01-06';

delimiter //;

CREATE OR REPLACE PACKAGE hdm_insert_list
AS

   PROCEDURE hdm_insert_list_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_insert_list_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_insert_list_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

   /******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          KWG
   --时间：          2015年8月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_merge_nthcabrp_dat (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_insert_list;
//


CREATE OR REPLACE PACKAGE hdm_our_export
AS
/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_update_c_flag (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_update_c_flag_2 (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

   /******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          kwg
   --时间：          2015年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_merge_nthcabrp_dat (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_our_export;
//







CREATE OR REPLACE PACKAGE hdm_upour_export
AS
/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_upour_export;
//

CREATE OR REPLACE PACKAGE HDM.PCKG_AOIP_MPS_QUERY
IS
    CN_BATCH_SIZE  constant  pls_integer := 1000;
  PFEPSADL_1   constant  varchar2(2) := '1';
  PERSONAL_2   constant  varchar2(2) := '2';
  BFHCRDBF_3   constant  varchar2(2) := '3';
  BFHSCDBF_4   constant  varchar2(2) := '4';
  NFQCHDTL_4   constant  varchar2(2) := '5';
  INSTANCE_NAME  varchar2(20);

  PFEPFODL_02   constant  varchar2(2) := '02';
    PFEPNFDL_03   constant  varchar2(2) := '03';
    PFEPVMDL_04   constant  varchar2(2) := '04';
    PFEPCBDL_05   constant  varchar2(2) := '05';
    PFEPGKDL_07   constant  varchar2(2) := '07';
    PFEPTZDL_08   constant  varchar2(2) := '08';


    PROCEDURE PROC_EXPORT_PFEPSADL_DATA(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


    PROCEDURE PROC_EXPORT_PERSONAL_DATA(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

END PCKG_AOIP_MPS_QUERY;
//

CREATE OR REPLACE PACKAGE PCKG_EDW_DTH_QUERY
IS
    CN_BATCH_SIZE  constant  pls_integer := 1000;
  PFEPSADL_01   constant  varchar2(2) := '01';
  PFEPFODL_02   constant  varchar2(2) := '02';
  PFEPNFDL_03   constant  varchar2(2) := '03';
  PFEPVMDL_04   constant  varchar2(2) := '04';
  PFEPCBDL_05   constant  varchar2(2) := '05';
  PFEPGKDL_07   constant  varchar2(2) := '07';
  PFEPTZDL_08   constant  varchar2(2) := '08';
  BFHCRDBF_21   constant  varchar2(2) := '21';
  BFHSCDBF_22   constant  varchar2(2) := '22';
  INSTANCE_NAME  varchar2(20);

    PROCEDURE EXPORT_PERSON_CARD_RESULT(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


    PROCEDURE EXPORT_PERSON_PFEPSADL_RESULT(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


    PROCEDURE EXPORT_PERSON_PFQSYDTL_RESULT(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


END PCKG_EDW_DTH_QUERY;
//

CREATE OR REPLACE PACKAGE pckg_hdm_public_util
IS

   PROCEDURE proc_writelog (
      p_i_date         IN   VARCHAR2,                       --(传入参数 日期)
      p_i_proc_name    IN   VARCHAR2,                   --(传入参数 任务描述)
      p_i_min_zoneno   IN   VARCHAR2,                 --(传入参数 最小地区号)
      p_i_max_zoneno   IN   VARCHAR2,                 --(传入参数 最大地区号)
      p_i_status       IN   NUMBER,
      --(传入参数 执行标志：0-'已开始',1-'正常结束',99-'出错未完成')
      p_i_message      IN   VARCHAR2,                   --(传入参数 执行信息)
      p_i_flag         IN   NUMBER    --(传入参数 执行标志,1-首次插入,2-更新)
   );
/******************************************************************************
--存储过程名称：  func_get_log_status
--作者：          周平平
--时间：          2007年03月22日
--使用源表名称：
--使用目标表名称：
--参数说明：      p_i_date                (传入参数 日期)
--                p_i_proc_name           (传入参数 任务描述)
--                p_i_min_zoneno          (传入参数 最小地区号)
--                p_i_max_zoneno          (传入参数 最大地区号)
--功能：          获取任务的执行状态（0-任务已开始未完成,1-任务成功,99-任务出错）
******************************************************************************/
   FUNCTION func_get_log_status (
      p_i_date         IN   VARCHAR2,                       --(传入参数 日期)
      p_i_proc_name    IN   VARCHAR2,                   --(传入参数 任务描述)
      p_i_min_zoneno   IN   VARCHAR2,                 --(传入参数 最小地区号)
      p_i_max_zoneno   IN   VARCHAR2                  --(传入参数 最大地区号)
   )
      RETURN NUMBER;

END pckg_hdm_public_util;
//
create or replace function hdm.func_AccNoTransfer(AccNo in varchar2) return varchar2 is
  nCheck Varchar2(50) := '';
begin
  nCheck := 11 * (substr(accno, 1, 1) - '0') +
            13 * (substr(accno, 2, 1) - '0') +
            17 * (substr(accno, 3, 1) - '0') +
            19 * (substr(accno, 4, 1) - '0') +
            23 * (substr(accno, 5, 1) - '0') +
            29 * (substr(accno, 6, 1) - '0') +
            31 * (substr(accno, 7, 1) - '0') +
            37 * (substr(accno, 8, 1) - '0') +
            41 * (substr(accno, 9, 1) - '0') +
            43 * (substr(accno, 10, 1) - '0') +
            47 * (substr(accno, 11, 1) - '0') +
            53 * (substr(accno, 12, 1) - '0') +
            59 * (substr(accno, 13, 1) - '0') +
            61 * (substr(accno, 14, 1) - '0') +
            67 * (substr(accno, 15, 1) - '0') +
            71 * (substr(accno, 16, 1) - '0') +
            73 * (substr(accno, 17, 1) - '0');
  nCheck := 97 - mod(nCheck, 97);
  nCheck :=trim(to_char(nCheck,'09'));
  nCheck := AccNo || nCheck;
  return(nCheck);
EXCEPTION
  WHEN OTHERS
  THEN
       BEGIN
                 return(AccNo);
       END;
end func_AccNoTransfer;
//


CREATE OR REPLACE FUNCTION func_zonenoTransfer(zoneno in varchar2)
   return varchar2
   is
   normal_zoneno Varchar2(50) := '';
   len int := length(zoneno);
  begin
        if  len = 3 then
            --左补零1位
            normal_zoneno := lpad(zoneno,4, '0');
        elsif  len = 5 then
            --去左一位
             normal_zoneno := substr(zoneno, 2, 4);
        else
            normal_zoneno := zoneno;
        end if;
  return(normal_zoneno);
 EXCEPTION
  WHEN OTHERS
  THEN
       BEGIN
                 return(zoneno);
       END;
end func_zonenoTransfer;
//

CREATE OR REPLACE PACKAGE BODY hdm_insert_list
IS
/******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_insert_list_pfepsadl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_insert_list.hdm_insert_list_PFEPSADL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;
      v_count_all       NUMBER;

    CURSOR cur_query_dat
    IS
    SELECT /*+use_hash(a b) leading(a b)*/
                           a.query_id, our_time_start, our_time_end, NO,
                           currtype, acserno, cardacc
                      FROM hdm_query_date_split a, hdm_query_log_dat b
                     WHERE update_date = p_i_date
                       AND c_flag = '0'
                       AND b.qry_status = '8'
                       AND a.up_time_start IS NOT NULL
                       AND a.up_time_end IS NOT NULL
                       AND a.our_time_start IS NOT NULL
                       AND a.our_time_end IS NOT NULL
                       AND UPPER (b.qry_table_name) = 'HDM_S_PFEPSADL_QUERY'
                       AND a.query_id = b.qry_id;
    rec_query_dat  cur_query_dat%rowtype ;

     CURSOR cur_pfepsadl_query(cardacc varchar2, NO varchar2, our_time_start varchar2, our_time_end varchar2, currtype varchar2)
   IS
   SELECT /*+index(b INX_PFEPSADL_QUERY_dat_1 INX_PFEPSADL_QUERY_month_1 )*/
                   b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                   b.phybrno, b.subcode, b.cino, b.accatrbt, b.busidate,
                   b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                   b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                   b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                   b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                   b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                   b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                   b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                   b.eserialno, b.trxsite, b.zoneaccno
              FROM hdm_s_pfepsadl_query b
             WHERE cardacc = '1'                                 /*accno查*/
               AND NO = b.accno
               AND b.busidate BETWEEN our_time_start AND our_time_end
               AND (currtype = b.currtype or currtype='000');
   rec_pfepsadl_query cur_pfepsadl_query%rowtype;

   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      /*取出需要等待上游回复且需要拼接本库数据的*/

      v_step := '2';

    open cur_query_dat;
    loop
     fetch cur_query_dat into rec_query_dat;
     exit when cur_query_dat%notfound;

         /*支持重跑*/
         DELETE FROM hdm_s_pfepsadl_query_list
               WHERE ID = rec_query_dat.query_id;

         v_step := '3';
     open cur_pfepsadl_query(rec_query_dat.cardacc, rec_query_dat.NO, rec_query_dat.our_time_start, rec_query_dat.our_time_end, rec_query_dat.currtype);
     loop
      fetch cur_pfepsadl_query into rec_pfepsadl_query;
      exit when cur_pfepsadl_query%notfound;

        INSERT INTO hdm_s_pfepsadl_query_list
                     (accno, serialno, currtype, cashexf, actbrno, phybrno,
                      subcode, cino, accatrbt, busidate, busitime, trxcode,
                      workdate, valueday, closdate, drcrf, vouhno, amount,
                      balance, closint, inttax, summflag, cashnote, zoneno,
                      brno, tellerno, trxsqno, servface, authono, authtlno,
                      termid, timestmp, recipact, mdcardno, acserno, synflag,
                      showflag, macardn, recipcn, catrflag, sellercode,
                      amount1, amount2, flag1, date1, eserialno, trxsite,
                      zoneaccno, ID)
      values (
                   rec_pfepsadl_query.accno, rec_pfepsadl_query.serialno, rec_pfepsadl_query.currtype, rec_pfepsadl_query.cashexf, rec_pfepsadl_query.actbrno,
                   rec_pfepsadl_query.phybrno, rec_pfepsadl_query.subcode, rec_pfepsadl_query.cino, rec_pfepsadl_query.accatrbt, rec_pfepsadl_query.busidate,
                   rec_pfepsadl_query.busitime, rec_pfepsadl_query.trxcode, rec_pfepsadl_query.workdate, rec_pfepsadl_query.valueday, rec_pfepsadl_query.closdate,
                   rec_pfepsadl_query.drcrf, rec_pfepsadl_query.vouhno, rec_pfepsadl_query.amount, rec_pfepsadl_query.balance, rec_pfepsadl_query.closint,
                   rec_pfepsadl_query.inttax, rec_pfepsadl_query.summflag, rec_pfepsadl_query.cashnote, rec_pfepsadl_query.zoneno, rec_pfepsadl_query.brno,
                   rec_pfepsadl_query.tellerno, rec_pfepsadl_query.trxsqno, rec_pfepsadl_query.servface, rec_pfepsadl_query.authono, rec_pfepsadl_query.authtlno,
                   rec_pfepsadl_query.termid, rec_pfepsadl_query.timestmp, rec_pfepsadl_query.recipact, rec_pfepsadl_query.mdcardno, rec_pfepsadl_query.acserno,
                   rec_pfepsadl_query.synflag, rec_pfepsadl_query.showflag, rec_pfepsadl_query.macardn, rec_pfepsadl_query.recipcn, rec_pfepsadl_query.catrflag,
                   rec_pfepsadl_query.sellercode, rec_pfepsadl_query.amount1, rec_pfepsadl_query.amount2, rec_pfepsadl_query.flag1, rec_pfepsadl_query.date1,
                   rec_pfepsadl_query.eserialno, rec_pfepsadl_query.trxsite,  rec_pfepsadl_query.zoneaccno,
                   rec_query_dat.query_id);
      end loop;
       close cur_pfepsadl_query;

    end loop;
    close cur_query_dat;

      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_insert_list_pfepsadl;

/******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_insert_list_pfepvmdl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_insert_list.hdm_insert_list_pfepvmdl' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;
      v_count_all       NUMBER;

    CURSOR cur_query_dat
    IS
      SELECT /*+use_hash(a b) leading(a b)*/
                           a.query_id, our_time_start, our_time_end, NO,
                           currtype, acserno, cardacc
                      FROM hdm_query_date_split a, hdm_query_log_dat b
                     WHERE update_date = p_i_date
                       AND c_flag = '0'
                       AND b.qry_status = '8'
                       AND a.up_time_start IS NOT NULL
                       AND a.up_time_end IS NOT NULL
                       AND a.our_time_start IS NOT NULL
                       AND a.our_time_end IS NOT NULL
                       AND UPPER (b.qry_table_name) = 'HDM_S_PFEPVMDL_QUERY'
                       AND a.query_id = b.qry_id;
    rec_query_dat cur_query_dat%rowtype;

    CURSOR cur_pfepvmdl_query(cardacc varchar2, NO varchar2, our_time_start varchar2, our_time_end varchar2, currtype varchar2)
    IS
    SELECT /*+ index(b INX_PFEPVMDL_QUERY_DAT_1 INX_PFEPVMDL_QUERY_MOST_1)*/
                   b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                   b.phybrno, b.subcode, b.cino, b.busidate, b.busitime,
                   b.trxcode, b.workdate, b.valueday, b.closdate, b.drcrf,
                   b.vouhno, b.amount, b.balance, b.closint, b.keepint,
                   b.inttax, b.summflag, b.cashnote, b.savterm, b.rate,
                   b.matudate, b.zoneno, b.brno, b.tellerno, b.trxsqno,
                   b.servface, b.authono, b.authtlno, b.termid, b.timestmp,
                   b.recipact, b.mdcardno, b.acserno, b.synflag, b.showflag,
                   b.macardn, b.recipcn, b.catrflag, b.sellercode, b.amount1,
                   b.amount2, b.flag1, b.date1, b.zoneaccno
              FROM hdm_s_pfepvmdl_query b
             WHERE cardacc = '1'
               AND NO = b.accno
               AND b.busidate BETWEEN our_time_start AND our_time_end
               AND (currtype = b.currtype or currtype='000');
    rec_pfepvmdl_query cur_pfepvmdl_query%rowtype;

   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';


      /*取出需要等待上游回复且需要拼接本库数据的*/

         v_step := '2';

    open cur_query_dat;
    loop

      fetch cur_query_dat into rec_query_dat;
      exit when cur_query_dat%notfound;

         /*支持重跑*/
         DELETE FROM hdm_s_pfepvmdl_query_list
               WHERE ID = rec_query_dat.query_id;

         v_step := '3';

     open cur_pfepvmdl_query(rec_query_dat.cardacc, rec_query_dat.NO, rec_query_dat.our_time_start, rec_query_dat.our_time_end, rec_query_dat.currtype);
     loop

        fetch cur_pfepvmdl_query into rec_pfepvmdl_query;
        exit when cur_pfepvmdl_query%notfound;

         INSERT INTO hdm_s_pfepvmdl_query_list
               (accno, serialno, currtype, cashexf, actbrno, phybrno,
                subcode, cino, busidate, busitime, trxcode, workdate,
                valueday, closdate, drcrf, vouhno, amount, balance,
                closint, keepint, inttax, summflag, cashnote, savterm,
                rate, matudate, zoneno, brno, tellerno, trxsqno,
                servface, authono, authtlno, termid, timestmp, recipact,
                mdcardno, acserno, synflag, showflag, macardn, recipcn,
                catrflag, sellercode, amount1, amount2, flag1, date1,
                zoneaccno, ID)
        values (
             rec_pfepvmdl_query.accno, rec_pfepvmdl_query.serialno, rec_pfepvmdl_query.currtype, rec_pfepvmdl_query.cashexf, rec_pfepvmdl_query.actbrno,
               rec_pfepvmdl_query.phybrno, rec_pfepvmdl_query.subcode, rec_pfepvmdl_query.cino, rec_pfepvmdl_query.busidate, rec_pfepvmdl_query.busitime,
               rec_pfepvmdl_query.trxcode, rec_pfepvmdl_query.workdate, rec_pfepvmdl_query.valueday, rec_pfepvmdl_query.closdate, rec_pfepvmdl_query.drcrf,
               rec_pfepvmdl_query.vouhno, rec_pfepvmdl_query.amount, rec_pfepvmdl_query.balance, rec_pfepvmdl_query.closint, rec_pfepvmdl_query.keepint,
               rec_pfepvmdl_query.inttax, rec_pfepvmdl_query.summflag, rec_pfepvmdl_query.cashnote, rec_pfepvmdl_query.savterm, rec_pfepvmdl_query.rate,
               rec_pfepvmdl_query.matudate, rec_pfepvmdl_query.zoneno, rec_pfepvmdl_query.brno, rec_pfepvmdl_query.tellerno, rec_pfepvmdl_query.trxsqno,
               rec_pfepvmdl_query.servface, rec_pfepvmdl_query.authono, rec_pfepvmdl_query.authtlno, rec_pfepvmdl_query.termid, rec_pfepvmdl_query.timestmp,
               rec_pfepvmdl_query.recipact, rec_pfepvmdl_query.mdcardno, rec_pfepvmdl_query.acserno, rec_pfepvmdl_query.synflag, rec_pfepvmdl_query.showflag,
               rec_pfepvmdl_query.macardn, rec_pfepvmdl_query.recipcn, rec_pfepvmdl_query.catrflag, rec_pfepvmdl_query.sellercode, rec_pfepvmdl_query.amount1,
               rec_pfepvmdl_query.amount2, rec_pfepvmdl_query.flag1, rec_pfepvmdl_query.date1,rec_pfepvmdl_query.zoneaccno,
               rec_query_dat.query_id
            );
        end loop;
          close cur_pfepvmdl_query;
      end loop;
    close cur_query_dat;

      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_insert_list_pfepvmdl;

/******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_insert_list_pfepotdl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_insert_list.hdm_insert_list_PFEPOTDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;
      v_count_all       NUMBER;

    CURSOR cur_query_dat
    IS
    SELECT /*+use_hash(a b) leading(a b)*/
                           a.query_id, our_time_start, our_time_end, NO,
                           currtype, acserno, cardacc
                      FROM hdm_query_date_split a, hdm_query_log_dat b
                     WHERE update_date = p_i_date
                       AND c_flag = '0'
                       AND b.qry_status = '8'
                       AND a.up_time_start IS NOT NULL
                       AND a.up_time_end IS NOT NULL
                       AND a.our_time_start IS NOT NULL
                       AND a.our_time_end IS NOT NULL
                       AND UPPER (b.qry_table_name) = 'HDM_S_PFEPOTDL_QUERY'
                       AND a.query_id = b.qry_id;
    rec_query_dat  cur_query_dat%rowtype;

    CURSOR cur_pfepotdl_query(cardacc varchar2, NO varchar2, our_time_start varchar2, our_time_end varchar2, currtype varchar2)
    IS
    SELECT /*+index(b INX_PFEPOTDL_QUERY_DAT_1 INX_PFEPOTDL_QUERY_MONTH_1)*/
                   b.mdcardno, b.serialno, b.acappno, b.acserno, b.accno,
                   b.currtype, b.cashexf, b.openzone, b.actbrno, b.phybrno,
                   b.cino, b.busidate, b.busitime, b.trxcode, b.workdate,
                   b.valueday, b.closdate, b.drcrf, b.amount, b.balance,
                   b.backbal, b.summflag, b.cashnote, b.drwterm, b.finano,
                   b.fseqno, b.fincode, b.matudate, b.fintype, b.zoneno,
                   b.brno, b.tellerno, b.trxsqno, b.servface, b.authono,
                   b.authtlno, b.termid, b.timestmp, b.recipact, b.synflag,
                   b.showflag, b.macardn, b.recipcn, b.catrflag, b.sellercode,
                   b.amount1, b.amount2, b.flag1, b.date1, b.date2,
                   b.zoneaccno
              FROM hdm_s_pfepotdl_query b
             WHERE cardacc = '1'                                 /*accno查*/
               AND NO = b.accno
               AND b.busidate BETWEEN our_time_start AND our_time_end
               AND currtype = b.currtype;
     rec_pfepotdl_query cur_pfepotdl_query%rowtype;

   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';

      v_step := '2';

    open cur_query_dat;
    loop

    fetch cur_query_dat into rec_query_dat;
    exit when cur_query_dat%notfound;
         /*支持重跑*/
         DELETE FROM hdm_s_pfepotdl_query_list
               WHERE ID = rec_query_dat.query_id;

      v_step := '3';
       open cur_pfepotdl_query(rec_query_dat.cardacc, rec_query_dat.NO, rec_query_dat.our_time_start, rec_query_dat.our_time_end, rec_query_dat.currtype);
       loop
         fetch cur_pfepotdl_query into rec_pfepotdl_query;
         exit when cur_pfepotdl_query%notfound;

         INSERT INTO hdm_s_pfepotdl_query_list
               (mdcardno, serialno, acappno, acserno, accno, currtype,
                cashexf, openzone, actbrno, phybrno, cino, busidate,
                busitime, trxcode, workdate, valueday, closdate, drcrf,
                amount, balance, backbal, summflag, cashnote, drwterm,
                finano, fseqno, fincode, matudate, fintype, zoneno,
                brno, tellerno, trxsqno, servface, authono, authtlno,
                termid, timestmp, recipact, synflag, showflag, macardn,
                recipcn, catrflag, sellercode, amount1, amount2, flag1,
                date1, date2, zoneaccno, ID)
         values (
               rec_pfepotdl_query.mdcardno, rec_pfepotdl_query.serialno, rec_pfepotdl_query.acappno, rec_pfepotdl_query.acserno, rec_pfepotdl_query.accno,
               rec_pfepotdl_query.currtype, rec_pfepotdl_query.cashexf, rec_pfepotdl_query.openzone, rec_pfepotdl_query.actbrno, rec_pfepotdl_query.phybrno,
               rec_pfepotdl_query.cino, rec_pfepotdl_query.busidate, rec_pfepotdl_query.busitime, rec_pfepotdl_query.trxcode, rec_pfepotdl_query.workdate,
               rec_pfepotdl_query.valueday, rec_pfepotdl_query.closdate, rec_pfepotdl_query.drcrf, rec_pfepotdl_query.amount, rec_pfepotdl_query.balance,
               rec_pfepotdl_query.backbal, rec_pfepotdl_query.summflag, rec_pfepotdl_query.cashnote, rec_pfepotdl_query.drwterm, rec_pfepotdl_query.finano,
               rec_pfepotdl_query.fseqno, rec_pfepotdl_query.fincode, rec_pfepotdl_query.matudate, rec_pfepotdl_query.fintype, rec_pfepotdl_query.zoneno,
               rec_pfepotdl_query.brno, rec_pfepotdl_query.tellerno, rec_pfepotdl_query.trxsqno, rec_pfepotdl_query.servface, rec_pfepotdl_query.authono,
               rec_pfepotdl_query.authtlno, rec_pfepotdl_query.termid, rec_pfepotdl_query.timestmp, rec_pfepotdl_query.recipact, rec_pfepotdl_query.synflag,
               rec_pfepotdl_query.showflag, rec_pfepotdl_query.macardn, rec_pfepotdl_query.recipcn, rec_pfepotdl_query.catrflag, rec_pfepotdl_query.sellercode,
               rec_pfepotdl_query.amount1, rec_pfepotdl_query.amount2, rec_pfepotdl_query.flag1, rec_pfepotdl_query.date1, rec_pfepotdl_query.date2,
               rec_pfepotdl_query.zoneaccno, rec_query_dat.query_id);

         end loop;
         close cur_pfepotdl_query;
    end loop;
    close cur_query_dat;

      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_insert_list_pfepotdl;

   /******************************************************************************
   --存储过程名称：  hdm_insert_list
   --作者：          KWG
   --时间：          2015年8月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_merge_nthcabrp_dat (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_insert_list.hdm_merge_nthcabrp_dat' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';

     --更新网点表hdm_nthcabrp_dat，提供前台使用。因hdm_nthcabrp_s_dat表的加载有truncate动作，避免前台查询时表被清空。
  merge into hdm_nthcabrp_dat a
     using hdm_nthcabrp_s_dat b
  on ( a.zoneno = b.zoneno and a.brno = b.brno)
  when matched
   then
     update set  a.MBRNO = b.MBRNO,
            a.BRTYPE = b.BRTYPE,
            a.NOTES = b.NOTES,
            a.STATUS = b.STATUS,
            a.RSFFLAG = b.RSFFLAG,
            a.RSFLHHH = b.RSFLHHH,
            a.RSFFQH = b.RSFFQH,
            a.RSFLHHBS = b.RSFLHHBS,
            a.RSFLHHM = b.RSFLHHM,
            a.RMBRNO = b.RMBRNO,
            a.ADDRESS = b.ADDRESS,
            a.ZIP = b.ZIP,
            a.PHONE1 = b.PHONE1,
            a.SHORTNM = b.SHORTNM,
            a.ACTBRNO = b.ACTBRNO,
            a.BRSTATUS = b.BRSTATUS,
            a.LSTMODD = b.LSTMODD,
            a.BAKDEC1 = b.BAKDEC1,
            a.BAKCHAR = b.BAKCHAR
     where
          NOT ( ((b.MBRNO is not null AND a.MBRNO is not null AND b.MBRNO = a.MBRNO) OR (b.MBRNO is null AND a.MBRNO is null)) AND
                ((b.BRTYPE is not null AND a.BRTYPE is not null AND b.BRTYPE = a.BRTYPE) OR (b.BRTYPE is null AND a.BRTYPE is null)) AND
                ((b.NOTES is not null AND a.NOTES is not null AND b.NOTES = a.NOTES) OR (b.NOTES is null AND a.NOTES is null)) AND
                ((b.STATUS is not null AND a.STATUS is not null AND b.STATUS = a.STATUS) OR (b.STATUS is null AND a.STATUS is null)) AND
                ((b.RSFFLAG is not null AND a.RSFFLAG is not null AND b.RSFFLAG = a.RSFFLAG) OR (b.RSFFLAG is null AND a.RSFFLAG is null)) AND
                ((b.RSFLHHH is not null AND a.RSFLHHH is not null AND b.RSFLHHH = a.RSFLHHH) OR (b.RSFLHHH is null AND a.RSFLHHH is null)) AND
                ((b.RSFFQH is not null AND a.RSFFQH is not null AND b.RSFFQH = a.RSFFQH) OR (b.RSFFQH is null AND a.RSFFQH is null)) AND
                ((b.RSFLHHBS is not null AND a.RSFLHHBS is not null AND b.RSFLHHBS = a.RSFLHHBS) OR (b.RSFLHHBS is null AND a.RSFLHHBS is null)) AND
                ((b.RSFLHHM is not null AND a.RSFLHHM is not null AND b.RSFLHHM = a.RSFLHHM) OR (b.RSFLHHM is null AND a.RSFLHHM is null)) AND
                ((b.RMBRNO is not null AND a.RMBRNO is not null AND b.RMBRNO = a.RMBRNO) OR (b.RMBRNO is null AND a.RMBRNO is null)) AND
                ((b.ADDRESS is not null AND a.ADDRESS is not null AND b.ADDRESS = a.ADDRESS) OR (b.ADDRESS is null AND a.ADDRESS is null)) AND
                ((b.ZIP is not null AND a.ZIP is not null AND b.ZIP = a.ZIP) OR (b.ZIP is null AND a.ZIP is null)) AND
                ((b.PHONE1 is not null AND a.PHONE1 is not null AND b.PHONE1 = a.PHONE1) OR (b.PHONE1 is null AND a.PHONE1 is null)) AND
                ((b.SHORTNM is not null AND a.SHORTNM is not null AND b.SHORTNM = a.SHORTNM) OR (b.SHORTNM is null AND a.SHORTNM is null)) AND
                ((b.ACTBRNO is not null AND a.ACTBRNO is not null AND b.ACTBRNO = a.ACTBRNO) OR (b.ACTBRNO is null AND a.ACTBRNO is null)) AND
                ((b.BRSTATUS is not null AND a.BRSTATUS is not null AND b.BRSTATUS = a.BRSTATUS) OR (b.BRSTATUS is null AND a.BRSTATUS is null)) AND
                ((b.LSTMODD is not null AND a.LSTMODD is not null AND b.LSTMODD = a.LSTMODD) OR (b.LSTMODD is null AND a.LSTMODD is null)) AND
                ((b.BAKDEC1 is not null AND a.BAKDEC1 is not null AND b.BAKDEC1 = a.BAKDEC1) OR (b.BAKDEC1 is null AND a.BAKDEC1 is null)) AND
                ((b.BAKCHAR is not null AND a.BAKCHAR is not null AND b.BAKCHAR = a.BAKCHAR) OR (b.BAKCHAR is null AND a.BAKCHAR is null)) )
  when not matched
     then
       insert ( ZONENO,
        BRNO,
        MBRNO,
        BRTYPE,
        NOTES,
        STATUS,
        RSFFLAG,
        RSFLHHH,
        RSFFQH,
        RSFLHHBS,
        RSFLHHM,
        RMBRNO,
        ADDRESS,
        ZIP,
        PHONE1,
        SHORTNM,
        ACTBRNO,
        BRSTATUS,
        LSTMODD,
        BAKDEC1,
        BAKCHAR )
     values  ( b.ZONENO,
        b.BRNO,
        b.MBRNO,
        b.BRTYPE,
        b.NOTES,
        b.STATUS,
        b.RSFFLAG,
        b.RSFLHHH,
        b.RSFFQH,
        b.RSFLHHBS,
        b.RSFLHHM,
        b.RMBRNO,
        b.ADDRESS,
        b.ZIP,
        b.PHONE1,
        b.SHORTNM,
        b.ACTBRNO,
        b.BRSTATUS,
        b.LSTMODD,
        b.BAKDEC1,
        b.BAKCHAR );

         commit;

     p_o_succeed := '0';
     v_step := '3';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );


     EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_merge_nthcabrp_dat;
END hdm_insert_list;
//

CREATE OR REPLACE PACKAGE BODY HDM.pckg_hdm_public_util
IS
   /******************************************************************************
   --存储过程名称：  PROC_WRITELOG
   --作者：          姚海瑜
   --时间：          2008年09月03日
   --使用源表名称：
   --使用目标表名称：
   --参数说明：      p_i_date                (传入参数 日期)
   --                p_i_proc_name           (传入参数 任务描述)
   --                p_i_min_zoneno          (传入参数 最小地区号)
   --                p_i_max_zoneno          (传入参数 最大地区号)
   --                p_i_status              (传入参数 执行标志 0-'已开始',1-'正常结束',99-'出错未完成')
   --                p_i_flag                (传入参数 执行标志,1-首次插入,2-更新)
   --                p_i_message             (传入参数 执行信息)
   --                p_o_succeed             (传出参数 记日志成功标志 0成功,非0失败)
   --                p_o_message             (传出参数 记日志错误信息)
   --功能：          公共任务记日志模块
   ******************************************************************************/
   PROCEDURE proc_writelog (
      p_i_date         IN   VARCHAR2,                       --(传入参数 日期)
      p_i_proc_name    IN   VARCHAR2,                   --(传入参数 任务描述)
      p_i_min_zoneno   IN   VARCHAR2,                 --(传入参数 最小地区号)
      p_i_max_zoneno   IN   VARCHAR2,                 --(传入参数 最大地区号)
      p_i_status       IN   NUMBER,
      --(传入参数 执行标志：0-'已开始',1-'正常结束',99-'出错未完成')
      p_i_message      IN   VARCHAR2,                   --(传入参数 执行信息)
      p_i_flag         IN   NUMBER    --(传入参数 执行标志,1-首次插入,2-更新)
   )
   AS
      --PRAGMA AUTONOMOUS_TRANSACTION;
      v_min_zoneno   VARCHAR2 (10);
      v_max_zoneno   VARCHAR2 (10);
   BEGIN
      v_min_zoneno := NVL (p_i_min_zoneno, '0');
      v_max_zoneno := NVL (p_i_max_zoneno, '0');

      IF p_i_flag = 1
      THEN
         --首次插入
         DELETE FROM hdm.hdm_log_task_detail
               WHERE d_task_date = p_i_date
                 AND c_proc_name = LOWER (p_i_proc_name)
                 AND c_min_zoneno = v_min_zoneno
                 AND c_max_zoneno = v_max_zoneno;

         INSERT INTO hdm.hdm_log_task_detail
                     (d_task_date, c_proc_name, c_min_zoneno,
                      c_max_zoneno, d_start_time, d_end_time, n_status,
                      c_message
                     )
              VALUES (p_i_date, LOWER (p_i_proc_name), v_min_zoneno,
                      v_max_zoneno, SYSTIMESTAMP, NULL, p_i_status,
                      p_i_message
                     );
      ELSIF p_i_flag = 2
      THEN
         MERGE INTO hdm.hdm_log_task_detail t
            USING (SELECT p_i_date d_task_date,
                          LOWER (p_i_proc_name) c_proc_name,
                          v_min_zoneno c_min_zoneno,
                          v_max_zoneno c_max_zoneno, p_i_status n_status,
                          p_i_message c_message
                     FROM DUAL) s
            ON (    t.d_task_date = s.d_task_date
                AND t.c_proc_name = s.c_proc_name
                AND t.c_min_zoneno = s.c_min_zoneno
                AND t.c_max_zoneno = s.c_max_zoneno)
            WHEN MATCHED THEN
               UPDATE
                  SET t.d_end_time = SYSTIMESTAMP, t.n_status = s.n_status,
                      c_message = s.c_message
            WHEN NOT MATCHED THEN
               INSERT (d_task_date, c_proc_name, c_min_zoneno, c_max_zoneno,
                       d_start_time, d_end_time, n_status, c_message)
               VALUES (s.d_task_date, s.c_proc_name, s.c_min_zoneno,
                       s.c_max_zoneno, NULL, SYSTIMESTAMP, s.n_status,
                       s.c_message);
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         RAISE;
   END proc_writelog;

/******************************************************************************
--存储过程名称：  func_get_log_status
--作者：          姚海瑜
--时间：          2009年03月30日
--使用源表名称：
--使用目标表名称：
--参数说明：      p_i_date                (传入参数 日期)
--                p_i_proc_name           (传入参数 任务描述)
--                p_i_min_zoneno          (传入参数 最小地区号)
--                p_i_max_zoneno          (传入参数 最大地区号)
--功能：          获取任务的执行状态（0-任务已开始未完成,1-任务成功,99-任务出错）
******************************************************************************/
   FUNCTION func_get_log_status (
      p_i_date         IN   VARCHAR2,                        --(传入参数 日期)
      p_i_proc_name    IN   VARCHAR2,                    --(传入参数 任务描述)
      p_i_min_zoneno   IN   VARCHAR2,                  --(传入参数 最小地区号)
      p_i_max_zoneno   IN   VARCHAR2                   --(传入参数 最大地区号)
   )
      RETURN NUMBER
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      v_status       hdm.hdm_log_task_detail.n_status%TYPE;
      v_min_zoneno   VARCHAR2 (10);
      v_max_zoneno   VARCHAR2 (10);
   BEGIN
      v_min_zoneno := NVL (p_i_min_zoneno, '0');
      v_max_zoneno := NVL (p_i_max_zoneno, '0');

      SELECT NVL (MAX (n_status), 99)
        INTO v_status
        FROM hdm.hdm_log_task_detail
       WHERE d_task_date = p_i_date
         AND c_proc_name = LOWER (p_i_proc_name)
         AND c_min_zoneno = v_min_zoneno
         AND c_max_zoneno = v_max_zoneno;

      RETURN v_status;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 99;
   END func_get_log_status;

END pckg_hdm_public_util;
//

CREATE OR REPLACE PACKAGE PCKG_EDW_DTH_QUERY
IS
    CN_BATCH_SIZE  constant  pls_integer := 1000;
  PFEPSADL_01   constant  varchar2(2) := '01';
  PFEPFODL_02   constant  varchar2(2) := '02';
  PFEPNFDL_03   constant  varchar2(2) := '03';
  PFEPVMDL_04   constant  varchar2(2) := '04';
  PFEPCBDL_05   constant  varchar2(2) := '05';
  PFEPGKDL_07   constant  varchar2(2) := '07';
  PFEPTZDL_08   constant  varchar2(2) := '08';
  BFHCRDBF_21   constant  varchar2(2) := '21';
  BFHSCDBF_22   constant  varchar2(2) := '22';
  INSTANCE_NAME  varchar2(20);

    PROCEDURE EXPORT_PERSON_CARD_RESULT(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


    PROCEDURE EXPORT_PERSON_PFEPSADL_RESULT(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


    PROCEDURE EXPORT_PERSON_PFQSYDTL_RESULT(
        p_i_batch IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


END PCKG_EDW_DTH_QUERY;
//

CREATE OR REPLACE PACKAGE BODY PCKG_EDW_DTH_QUERY
IS
  /******************************************************************************
     --存储过程名称： EXPORT_PERSON_CARD_RESULT
     --作者： kwg
     --时间： 2015年10月16日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明： p_i_batch (传入参数,查询批次)
     --p_o_succeed (传出参数 成功返回0，失败返回1)
     --功能： 汇总、排序、去重后导出信用卡联动查询明细。
  ******************************************************************************/
  PROCEDURE EXPORT_PERSON_CARD_RESULT(
    p_i_batch IN VARCHAR2, --(传入参数,查询批次)
    p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
  )
  IS
    p_i_date        VARCHAR2(10) := NULL;
    p_i_batch_no        VARCHAR2(40) := NULL;
      v_proc_err_code     VARCHAR2 (500):= null;
      v_proc_err_txt      VARCHAR2 (500):= null;
      v_proc_name         VARCHAR2 (500):= 'hdm.PCKG_EDW_DTH_QUERY.EXPORT_PERSON_CARD_RESULT';
      v_step              VARCHAR2 (30):= '0';   --步骤号
      v_min_zoneno        VARCHAR2 (10):='0';
      v_max_zoneno        VARCHAR2 (10):='0';
    v_outputfile      UTL_FILE.file_type := NULL;
    v_filename          VARCHAR2 (50);
    v_resultline        VARCHAR2 (32767) := NULL;
    --贷记卡

  BEGIN

    v_step := '0';
    v_proc_name := upper(v_proc_name||p_i_batch||'-'||DBMS_RANDOM.RANDOM());
    p_i_date := REGEXP_SUBSTR(p_i_batch,'\d+',1,1);
    p_i_batch_no := REGEXP_SUBSTR(p_i_batch,'\d+',1,2);


    hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                           v_proc_name,
                                           v_min_zoneno,
                                           v_max_zoneno,
                                           0,
                                           '任务已开始',
                                           1
                                          );

    v_step := '1';


      COMMIT;
     p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                         v_proc_name,
                         v_min_zoneno,
                         v_max_zoneno,
                         1,
                         '任务完成',
                         2
                          );
    EXCEPTION
       WHEN OTHERS
       THEN
       BEGIN
       ROLLBACK;
       v_proc_err_code := SQLCODE;
       v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
       p_o_succeed := '1';
       --任务日志表中记录出错状态
       hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                           v_proc_name,
                           v_min_zoneno,
                           v_max_zoneno,
                           99,
                            '任务出错! '
                           || '出错步骤号为: '
                           || v_step
                           || ' || 错误码为: '
                           || v_proc_err_code
                           || ' || 错误信息为: '
                           || v_proc_err_txt,
                           2
                          );
       RAISE;
      END;
  END EXPORT_PERSON_CARD_RESULT;

  /******************************************************************************
     --存储过程名称： EXPORT_PERSON_PFEPSADL_RESULT
     --作者： kwg
     --时间： 2015年10月16日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明： p_i_batch (传入参数,查询批次)
     --p_o_succeed (传出参数 成功返回0，失败返回1)
     --功能： 汇总、排序、去重后导出活期联动查询明细。
  ******************************************************************************/
  PROCEDURE EXPORT_PERSON_PFEPSADL_RESULT(
    p_i_batch IN VARCHAR2, --(传入参数,查询批次)
    p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
  )
  IS
    p_i_date VARCHAR2(10) := NULL;
            p_i_batch_no        VARCHAR2(40) := NULL;
      v_proc_err_code     VARCHAR2 (500):= null;
      v_proc_err_txt      VARCHAR2 (500):= null;
      v_proc_name         VARCHAR2 (500):= 'hdm.PCKG_EDW_DTH_QUERY.EXPORT_PERSON_PFEPSADL_RESULT';
      v_step              VARCHAR2 (30):= '0';   --步骤号
      v_min_zoneno        VARCHAR2 (10):='0';
      v_max_zoneno        VARCHAR2 (10):='0';

    v_outputfile      UTL_FILE.file_type := NULL;
    v_filename          VARCHAR2 (50);
    v_resultline        VARCHAR2 (32767) := NULL;
    v_rebankname        varchar2(100);
    --活期
    CURSOR cur_query_dth_pfe
      IS
      SELECT QUERY_ID, ACCNO, CURRTYPE,QUERY_DATE_FROM, QUERY_DATE_TO
      FROM HDM_S_TB1560_PERSON_DTH a
      WHERE a.APP_NO = PFEPSADL_01;
    rec_query_dth_pfe   cur_query_dth_pfe%rowtype     := null;

     CURSOR cur_dth_pfe_result (P_ACCNO varchar2, P_CURRTYPE varchar2, P_QUERY_DATE_FROM varchar2, P_QUERY_DATE_TO varchar2 )
      IS
         SELECT /*+ index(b INX_PFEPSADL_QUERY_DAT_1 INX_PFEPSADL_QUERY_MONTH_1)*/
                   b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                   b.phybrno, b.subcode, b.cino, b.accatrbt, b.busidate,
                   b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                   b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                   b.inttax, b.summflag, b.cashnote, func_zonenoTransfer(b.zoneno) as zoneno , func_zonenoTransfer(b.brno) as brno,
                   b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                   b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                   b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                   b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                   b.eserialno, b.trxsite,  b.zoneaccno, b.resavname, b.rebankname
              FROM  hdm_s_pfepsadl_query b
              WHERE zoneaccno = SUBSTR(P_ACCNO, 1, 4)
         AND b.accno = P_ACCNO
               AND b.busidate BETWEEN P_QUERY_DATE_FROM AND P_QUERY_DATE_TO
               AND (P_CURRTYPE = '000' or b.currtype = P_CURRTYPE);

            TYPE typ_dth_pfe_result is table of cur_dth_pfe_result%rowtype;
      tab_dth_pfe_result typ_dth_pfe_result;

  BEGIN

    v_step := '0';
    v_proc_name := upper(v_proc_name||p_i_batch||'-'||DBMS_RANDOM.RANDOM());
    p_i_date := REGEXP_SUBSTR(p_i_batch,'\d+',1,1);
    p_i_batch_no := REGEXP_SUBSTR(p_i_batch,'\d+',1,2);


    hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                           v_proc_name,
                                           v_min_zoneno,
                                           v_max_zoneno,
                                           0,
                                           '任务已开始',
                                           1
                                          );

    v_step := '1';
     --获取实例名
    INSTANCE_NAME := 'hdmpras';


    --导出活期明细
     v_filename := 'result_person_dth_pfe_'||p_i_date||'_'||p_i_batch_no||'_'||INSTANCE_NAME||'.bin';
     v_outputfile :=  UTL_FILE.fopen ('EDW_EXPORT_PFE_RESULT', v_filename, 'w', 32767);
     open cur_query_dth_pfe;
     loop
         fetch cur_query_dth_pfe into rec_query_dth_pfe;
         exit when cur_query_dth_pfe%notfound;
            open cur_dth_pfe_result(rec_query_dth_pfe.ACCNO, rec_query_dth_pfe.CURRTYPE, rec_query_dth_pfe.QUERY_DATE_FROM, rec_query_dth_pfe.QUERY_DATE_TO);
            loop
        fetch cur_dth_pfe_result bulk collect into tab_dth_pfe_result limit CN_BATCH_SIZE;
        for i in 1..tab_dth_pfe_result.count
        loop
           -- 对方开户行名可能包含逗号
           v_rebankname := tab_dth_pfe_result(i).REBANKNAME;
           if instr(v_rebankname,',') > 0 then
             v_rebankname := '"' || v_rebankname || '"';
           end if;
             v_resultline :=
                           rec_query_dth_pfe.QUERY_ID
                           || CHR (27)
                           ||  '0'                       --QUERY_FLAG,'0'有数据返回
                           || CHR (27)
                           || tab_dth_pfe_result(i).ACCNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).SERIALNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).CURRTYPE
                           || CHR (27)
                           || tab_dth_pfe_result(i).CASHEXF
                           || CHR (27)
                           || tab_dth_pfe_result(i).ACTBRNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).PHYBRNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).SUBCODE
                           || CHR (27)
                           || tab_dth_pfe_result(i).CINO
                           || CHR (27)
                           || tab_dth_pfe_result(i).ACCATRBT
                           || CHR (27)
                           || tab_dth_pfe_result(i).BUSIDATE
                           || CHR (27)
                           || tab_dth_pfe_result(i).BUSITIME
                           || CHR (27)
                           || tab_dth_pfe_result(i).TRXCODE
                           || CHR (27)
                           || tab_dth_pfe_result(i).WORKDATE
                           || CHR (27)
                           || tab_dth_pfe_result(i).VALUEDAY
                           || CHR (27)
                           || tab_dth_pfe_result(i).CLOSDATE
                           || CHR (27)
                           || tab_dth_pfe_result(i).DRCRF
                           || CHR (27)
                           || tab_dth_pfe_result(i).VOUHNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).AMOUNT
                           || CHR (27)
                           || tab_dth_pfe_result(i).BALANCE
                           || CHR (27)
                           || tab_dth_pfe_result(i).CLOSINT
                           || CHR (27)
                           || tab_dth_pfe_result(i).INTTAX
                           || CHR (27)
                           || tab_dth_pfe_result(i).SUMMFLAG
                           || CHR (27)
                           || tab_dth_pfe_result(i).CASHNOTE
                           || CHR (27)
                           || tab_dth_pfe_result(i).ZONENO
                           || CHR (27)
                           || tab_dth_pfe_result(i).BRNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).TELLERNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).TRXSQNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).SERVFACE
                           || CHR (27)
                           || tab_dth_pfe_result(i).AUTHONO
                           || CHR (27)
                           || tab_dth_pfe_result(i).AUTHTLNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).TERMID
                           || CHR (27)
                           || tab_dth_pfe_result(i).TIMESTMP
                           || CHR (27)
                           || tab_dth_pfe_result(i).RECIPACT
                           || CHR (27)
                           || tab_dth_pfe_result(i).MDCARDNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).ACSERNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).SYNFLAG
                           || CHR (27)
                           || tab_dth_pfe_result(i).SHOWFLAG
                           || CHR (27)
                           || tab_dth_pfe_result(i).MACARDN
                           || CHR (27)
                           || tab_dth_pfe_result(i).RECIPCN
                           || CHR (27)
                           || tab_dth_pfe_result(i).CATRFLAG
                           || CHR (27)
                           || tab_dth_pfe_result(i).SELLERCODE
                           || CHR (27)
                           || tab_dth_pfe_result(i).AMOUNT1
                           || CHR (27)
                           || tab_dth_pfe_result(i).AMOUNT2
                           || CHR (27)
                           || tab_dth_pfe_result(i).FLAG1
                           || CHR (27)
                           || tab_dth_pfe_result(i).DATE1
                           || CHR (27)
                           || tab_dth_pfe_result(i).ESERIALNO
                           || CHR (27)
                           || tab_dth_pfe_result(i).TRXSITE
               || CHR (27)
                           || tab_dth_pfe_result(i).RESAVNAME
               || CHR (27)
                           || v_rebankname
            ;
                     UTL_FILE.put_line (v_outputfile, v_resultline, FALSE);
                     UTL_FILE.fflush (v_outputfile);
        end loop;
        exit when tab_dth_pfe_result.count < CN_BATCH_SIZE;
      end loop;
      close cur_dth_pfe_result;

      end loop;
      close cur_query_dth_pfe;
      UTL_FILE.fclose (v_outputfile);



      COMMIT;
     p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                         v_proc_name,
                         v_min_zoneno,
                         v_max_zoneno,
                         1,
                         '任务完成',
                         2
                          );
    EXCEPTION
       WHEN OTHERS
       THEN
       BEGIN
       ROLLBACK;
       v_proc_err_code := SQLCODE;
       v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
       p_o_succeed := '1';
       --任务日志表中记录出错状态
       hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                           v_proc_name,
                           v_min_zoneno,
                           v_max_zoneno,
                           99,
                            '任务出错! '
                           || '出错步骤号为: '
                           || v_step
                           || ' || 错误码为: '
                           || v_proc_err_code
                           || ' || 错误信息为: '
                           || v_proc_err_txt,
                           2
                          );
       RAISE;
      END;
  END EXPORT_PERSON_PFEPSADL_RESULT;


  /******************************************************************************
     --存储过程名称： EXPORT_PERSON_PFQSYDTL_RESULT
     --作者： kwg
     --时间： 2015年10月16日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明： p_i_batch (传入参数,查询批次)
     --p_o_succeed (传出参数 成功返回0，失败返回1)
     --功能： 汇总、排序、去重后导出非01个人金融联动查询明细。
  ******************************************************************************/
  PROCEDURE EXPORT_PERSON_PFQSYDTL_RESULT(
    p_i_batch IN VARCHAR2, --(传入参数,查询批次)
    p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
  )
  IS
    p_i_date VARCHAR2(10) := NULL;
            p_i_batch_no        VARCHAR2(40) := NULL;
      v_proc_err_code     VARCHAR2 (500):= null;
      v_proc_err_txt      VARCHAR2 (500):= null;
      v_proc_name         VARCHAR2 (500):= 'hdm.PCKG_EDW_DTH_QUERY.EXPORT_PERSON_PFQSYDTL_RESULT';
      v_step              VARCHAR2 (30):= '0';   --步骤号
      v_min_zoneno        VARCHAR2 (10):='0';
      v_max_zoneno        VARCHAR2 (10):='0';

    v_outputfile      UTL_FILE.file_type := NULL;
    v_filename          VARCHAR2 (50);
    v_resultline        VARCHAR2 (32767) := NULL;

    --非01类个金
    CURSOR cur_query_dth_pfq
      IS
      SELECT QUERY_ID, ACCNO, CURRTYPE,QUERY_DATE_FROM, QUERY_DATE_TO,APP_NO
      FROM HDM_S_TB1560_PERSON_DTH a
      WHERE a.APP_NO IN (
            PFEPFODL_02,
            PFEPNFDL_03,
            PFEPVMDL_04,
            PFEPCBDL_05,
            PFEPGKDL_07,
            PFEPTZDL_08);
    rec_query_dth_pfq   cur_query_dth_pfq%rowtype     := null;

    --查询结果记录
    type typ_record_result is record (
          accno hdm_s_pfepfodl_query_dat.accno%type, serialno hdm_s_pfepfodl_query_dat.serialno%type, currtype hdm_s_pfepfodl_query_dat.currtype%type, cashexf hdm_s_pfepfodl_query_dat.cashexf%type, actbrno hdm_s_pfepfodl_query_dat.actbrno%type,
                  phybrno hdm_s_pfepfodl_query_dat.phybrno%type, subcode hdm_s_pfepfodl_query_dat.subcode%type, cino hdm_s_pfepfodl_query_dat.cino%type, accatrbt  varchar2(10), busidate hdm_s_pfepfodl_query_dat.busidate%type,
                  busitime hdm_s_pfepfodl_query_dat.busitime%type, trxcode hdm_s_pfepfodl_query_dat.trxcode%type, workdate hdm_s_pfepfodl_query_dat.workdate%type, valueday hdm_s_pfepfodl_query_dat.valueday%type, closdate hdm_s_pfepfodl_query_dat.closdate%type,
                  drcrf hdm_s_pfepfodl_query_dat.drcrf%type, vouhno hdm_s_pfepfodl_query_dat.vouhno%type, amount hdm_s_pfepfodl_query_dat.amount%type, balance hdm_s_pfepfodl_query_dat.balance%type, closint hdm_s_pfepfodl_query_dat.closint%type,
                  inttax hdm_s_pfepfodl_query_dat.inttax%type, summflag hdm_s_pfepfodl_query_dat.summflag%type, cashnote hdm_s_pfepfodl_query_dat.cashnote%type, zoneno hdm_s_pfepfodl_query_dat.zoneno%type, brno hdm_s_pfepfodl_query_dat.brno%type,
                  tellerno hdm_s_pfepfodl_query_dat.tellerno%type, trxsqno hdm_s_pfepfodl_query_dat.trxsqno%type, servface hdm_s_pfepfodl_query_dat.servface%type, authono hdm_s_pfepfodl_query_dat.authono%type, authtlno hdm_s_pfepfodl_query_dat.authtlno%type,
                  termid hdm_s_pfepfodl_query_dat.termid%type, timestmp hdm_s_pfepfodl_query_dat.timestmp%type, recipact hdm_s_pfepfodl_query_dat.recipact%type, mdcardno hdm_s_pfepfodl_query_dat.mdcardno%type, acserno hdm_s_pfepfodl_query_dat.acserno%type,
                  synflag hdm_s_pfepfodl_query_dat.synflag%type, showflag hdm_s_pfepfodl_query_dat.showflag%type, macardn hdm_s_pfepfodl_query_dat.macardn%type, recipcn hdm_s_pfepfodl_query_dat.recipcn%type, catrflag hdm_s_pfepfodl_query_dat.catrflag%type,
                  sellercode hdm_s_pfepfodl_query_dat.sellercode%type, amount1 hdm_s_pfepfodl_query_dat.amount1%type, amount2 hdm_s_pfepfodl_query_dat.amount2%type, flag1 hdm_s_pfepfodl_query_dat.flag1%type, date1 hdm_s_pfepfodl_query_dat.date1%type,
                  eserialno hdm_s_pfepfodl_query_dat.eserialno%type,  trxsite  varchar2(60), zoneaccno hdm_s_pfepfodl_query_dat.zoneaccno%type );
    TYPE typ_dth_pfq_result is table of typ_record_result;
    tab_dth_pfq_result typ_dth_pfq_result;
        --定义游标变量
        cur_dth_pfq_result SYS_REFCURSOR;

  BEGIN

    v_step := '0';
    v_proc_name := upper(v_proc_name||p_i_batch||'-'||DBMS_RANDOM.RANDOM());
    p_i_date := REGEXP_SUBSTR(p_i_batch,'\d+',1,1);
    p_i_batch_no := REGEXP_SUBSTR(p_i_batch,'\d+',1,2);


    hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                           v_proc_name,
                                           v_min_zoneno,
                                           v_max_zoneno,
                                           0,
                                           '任务已开始',
                                           1
                                          );

    v_step := '1';
     --获取实例名
    INSTANCE_NAME := 'hdmpras';


    --导出非个金类明细
     v_filename := 'result_person_dth_pfq_'||p_i_date||'_'||p_i_batch_no||'_'||INSTANCE_NAME||'.bin';
     v_outputfile :=  UTL_FILE.fopen ('EDW_EXPORT_PFQ_RESULT', v_filename, 'w', 32767);
     open cur_query_dth_pfq;
     loop
         fetch cur_query_dth_pfq into rec_query_dth_pfq;
         exit when cur_query_dth_pfq%notfound;
        if rec_query_dth_pfq.app_no = PFEPFODL_02 then
          dbms_output.put_line(2);
        open cur_dth_pfq_result for SELECT /*+index(b INX_pfepfodl_QUERY_DAT_1 INX_pfepfodl_QUERY_MONTH_1)*/
                         b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                         b.phybrno, b.subcode, b.cino, null as accatrbt, b.busidate,
                         b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                         b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                         b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                         b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                         b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                         b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                         b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                         b.eserialno, null as trxsite, b.zoneaccno
                      FROM  hdm_s_pfepfodl_query b
                      WHERE substr(rec_query_dth_pfq.ACCNO, 1, 4) = zoneaccno
                       AND  b.accno = rec_query_dth_pfq.ACCNO
                       AND b.busidate BETWEEN rec_query_dth_pfq.QUERY_DATE_FROM AND rec_query_dth_pfq.QUERY_DATE_TO
                       AND (rec_query_dth_pfq.CURRTYPE = '000' or b.currtype = rec_query_dth_pfq.CURRTYPE);
      elsif rec_query_dth_pfq.app_no = PFEPNFDL_03 then
         dbms_output.put_line(3);
        open cur_dth_pfq_result for SELECT /*+ index(b INX_pfepnfdl_QUERY_DAT_1 INX_pfepnfdl_QUERY_MONTH_1)*/
                         b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                         b.phybrno, b.subcode, b.cino, null as accatrbt, b.busidate,
                         b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                         b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                         b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                         b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                         b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                         b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                         b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                         b.eserialno, null as trxsite, b.zoneaccno
                      FROM  hdm_s_pfepnfdl_query b
                      WHERE substr(rec_query_dth_pfq.ACCNO, 1, 4) = zoneaccno
                        AND  b.accno = rec_query_dth_pfq.ACCNO
                        AND b.busidate BETWEEN rec_query_dth_pfq.QUERY_DATE_FROM AND rec_query_dth_pfq.QUERY_DATE_TO
                        AND (rec_query_dth_pfq.CURRTYPE = '000' or b.currtype = rec_query_dth_pfq.CURRTYPE);
      elsif rec_query_dth_pfq.app_no = PFEPVMDL_04 then
         dbms_output.put_line(4);
         open cur_dth_pfq_result for SELECT /*+index(b INX_pfepvmdl_QUERY_DAT_1 INX_pfepvmdl_QUERY_MONTH_1) */
                           b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                           b.phybrno, b.subcode, b.cino, null as accatrbt, b.busidate,
                           b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                           b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                           b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                           b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                           b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                           b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                           b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                           null as eserialno, null as trxsite,  b.zoneaccno
                       FROM  hdm_s_pfepvmdl_query b
                       WHERE substr(rec_query_dth_pfq.ACCNO, 1, 4) = zoneaccno
                        AND  b.accno = rec_query_dth_pfq.ACCNO
                        AND b.busidate BETWEEN rec_query_dth_pfq.QUERY_DATE_FROM AND rec_query_dth_pfq.QUERY_DATE_TO
                        AND (rec_query_dth_pfq.CURRTYPE = '000' or b.currtype = rec_query_dth_pfq.CURRTYPE);
            elsif rec_query_dth_pfq.app_no = PFEPCBDL_05 then
               dbms_output.put_line(5);
         open cur_dth_pfq_result for SELECT /*+index(b INX_pfepcbdl_QUERY_DAT_1 INX_pfepcbdl_QUERY_MONTH_1) */
                           b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                           b.phybrno, b.subcode, b.cino, null as accatrbt, b.busidate,
                           b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                           b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                           b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                           b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                           b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                           b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                           b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                           b.eserialno, null as trxsite,  b.zoneaccno
                        FROM  hdm_s_pfepcbdl_query b
                        WHERE substr(rec_query_dth_pfq.ACCNO, 1, 4) = zoneaccno
                        AND  b.accno = rec_query_dth_pfq.ACCNO
                        AND b.busidate BETWEEN rec_query_dth_pfq.QUERY_DATE_FROM AND rec_query_dth_pfq.QUERY_DATE_TO
                        AND (rec_query_dth_pfq.CURRTYPE = '000' or b.currtype = rec_query_dth_pfq.CURRTYPE);
      elsif rec_query_dth_pfq.app_no = PFEPGKDL_07 then
         dbms_output.put_line(7);
        open cur_dth_pfq_result for SELECT /*+index(b INX_pfepgkdl_QUERY_DAT_1 INX_pfepgkdl_QUERY_MONTH_1) */
                         b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                         b.phybrno, b.subcode, b.cino, null as accatrbt, b.busidate,
                         b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                         b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                         b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                         b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                         b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                         b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                         b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                         null as eserialno, null as trxsite, b.zoneaccno
                        FROM  hdm_s_pfepgkdl_query b
                      WHERE substr(rec_query_dth_pfq.ACCNO, 1, 4) = zoneaccno
                        AND  b.accno = rec_query_dth_pfq.ACCNO
                        AND b.busidate BETWEEN rec_query_dth_pfq.QUERY_DATE_FROM AND rec_query_dth_pfq.QUERY_DATE_TO
                        AND (rec_query_dth_pfq.CURRTYPE = '000' or b.currtype = rec_query_dth_pfq.CURRTYPE);
      elsif rec_query_dth_pfq.app_no = PFEPTZDL_08 then
         dbms_output.put_line(8);
        open cur_dth_pfq_result for SELECT /*+index(b INX_pfeptzdl_QUERY_DAT_1 INX_pfeptzdl_QUERY_MONTH_1)*/
                           b.accno, b.serialno, b.currtype, b.cashexf, b.actbrno,
                           b.phybrno, b.subcode, b.cino,null as accatrbt, b.busidate,
                           b.busitime, b.trxcode, b.workdate, b.valueday, b.closdate,
                           b.drcrf, b.vouhno, b.amount, b.balance, b.closint,
                           b.inttax, b.summflag, b.cashnote, b.zoneno, b.brno,
                           b.tellerno, b.trxsqno, b.servface, b.authono, b.authtlno,
                           b.termid, b.timestmp, b.recipact, b.mdcardno, b.acserno,
                           b.synflag, b.showflag, b.macardn, b.recipcn, b.catrflag,
                           b.sellercode, b.amount1, b.amount2, b.flag1, b.date1,
                           b.eserialno, null as trxsite,  b.zoneaccno
                      FROM  hdm_s_pfeptzdl_query b
                      WHERE substr(rec_query_dth_pfq.ACCNO, 1, 4) = zoneaccno
                        AND  b.accno = rec_query_dth_pfq.ACCNO
                        AND b.busidate BETWEEN rec_query_dth_pfq.QUERY_DATE_FROM AND rec_query_dth_pfq.QUERY_DATE_TO
                        AND (rec_query_dth_pfq.CURRTYPE = '000' or b.currtype = rec_query_dth_pfq.CURRTYPE);
      end if;

            loop
        fetch cur_dth_pfq_result bulk collect into tab_dth_pfq_result limit CN_BATCH_SIZE;
        for i in 1..tab_dth_pfq_result.count
        loop
             v_resultline :=
                           rec_query_dth_pfq.QUERY_ID
                           || CHR (27)
                           ||  '0'                       --QUERY_FLAG,'0'有数据返回
                           || CHR (27)
                           || tab_dth_pfq_result(i).ACCNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).SERIALNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).CURRTYPE
                           || CHR (27)
                           || tab_dth_pfq_result(i).CASHEXF
                           || CHR (27)
                           || tab_dth_pfq_result(i).ACTBRNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).PHYBRNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).SUBCODE
                           || CHR (27)
                           || tab_dth_pfq_result(i).CINO
                           || CHR (27)
                           || tab_dth_pfq_result(i).ACCATRBT
                           || CHR (27)
                           || tab_dth_pfq_result(i).BUSIDATE
                           || CHR (27)
                           || tab_dth_pfq_result(i).BUSITIME
                           || CHR (27)
                           || tab_dth_pfq_result(i).TRXCODE
                           || CHR (27)
                           || tab_dth_pfq_result(i).WORKDATE
                           || CHR (27)
                           || tab_dth_pfq_result(i).VALUEDAY
                           || CHR (27)
                           || tab_dth_pfq_result(i).CLOSDATE
                           || CHR (27)
                           || tab_dth_pfq_result(i).DRCRF
                           || CHR (27)
                           || tab_dth_pfq_result(i).VOUHNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).AMOUNT
                           || CHR (27)
                           || tab_dth_pfq_result(i).BALANCE
                           || CHR (27)
                           || tab_dth_pfq_result(i).CLOSINT
                           || CHR (27)
                           || tab_dth_pfq_result(i).INTTAX
                           || CHR (27)
                           || tab_dth_pfq_result(i).SUMMFLAG
                           || CHR (27)
                           || tab_dth_pfq_result(i).CASHNOTE
                           || CHR (27)
                           || tab_dth_pfq_result(i).ZONENO
                           || CHR (27)
                           || tab_dth_pfq_result(i).BRNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).TELLERNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).TRXSQNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).SERVFACE
                           || CHR (27)
                           || tab_dth_pfq_result(i).AUTHONO
                           || CHR (27)
                           || tab_dth_pfq_result(i).AUTHTLNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).TERMID
                           || CHR (27)
                           || tab_dth_pfq_result(i).TIMESTMP
                           || CHR (27)
                           || tab_dth_pfq_result(i).RECIPACT
                           || CHR (27)
                           || tab_dth_pfq_result(i).MDCARDNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).ACSERNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).SYNFLAG
                           || CHR (27)
                           || tab_dth_pfq_result(i).SHOWFLAG
                           || CHR (27)
                           || tab_dth_pfq_result(i).MACARDN
                           || CHR (27)
                           || tab_dth_pfq_result(i).RECIPCN
                           || CHR (27)
                           || tab_dth_pfq_result(i).CATRFLAG
                           || CHR (27)
                           || tab_dth_pfq_result(i).SELLERCODE
                           || CHR (27)
                           || tab_dth_pfq_result(i).AMOUNT1
                           || CHR (27)
                           || tab_dth_pfq_result(i).AMOUNT2
                           || CHR (27)
                           || tab_dth_pfq_result(i).FLAG1
                           || CHR (27)
                           || tab_dth_pfq_result(i).DATE1
                           || CHR (27)
                           || tab_dth_pfq_result(i).ESERIALNO
                           || CHR (27)
                           || tab_dth_pfq_result(i).TRXSITE;

                     UTL_FILE.put_line (v_outputfile, v_resultline, FALSE);
                     UTL_FILE.fflush (v_outputfile);
        end loop;
        exit when tab_dth_pfq_result.count < CN_BATCH_SIZE;
      end loop;
      if cur_dth_pfq_result%isopen then
        close cur_dth_pfq_result;
      end if;

      end loop;
      close cur_query_dth_pfq;
      UTL_FILE.fclose (v_outputfile);

      COMMIT;
     p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                         v_proc_name,
                         v_min_zoneno,
                         v_max_zoneno,
                         1,
                         '任务完成',
                         2
                          );
    EXCEPTION
       WHEN OTHERS
       THEN
       BEGIN
       ROLLBACK;
       v_proc_err_code := SQLCODE;
       v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
       p_o_succeed := '1';
       --任务日志表中记录出错状态
       hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                           v_proc_name,
                           v_min_zoneno,
                           v_max_zoneno,
                           99,
                            '任务出错! '
                           || '出错步骤号为: '
                           || v_step
                           || ' || 错误码为: '
                           || v_proc_err_code
                           || ' || 错误信息为: '
                           || v_proc_err_txt,
                           2
                          );
       RAISE;
      END;
  END EXPORT_PERSON_PFQSYDTL_RESULT;
END PCKG_EDW_DTH_QUERY;

//

CREATE OR REPLACE PACKAGE hdm_our_export
AS
/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_update_c_flag (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_update_c_flag_2 (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

   /******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          kwg
   --时间：          2015年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_merge_nthcabrp_dat (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_our_export;
//

CREATE OR REPLACE PACKAGE BODY hdm_our_export
IS
/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepsadl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_our_export.hdm_ourdb_export_PFEPSADL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_pfepsadl
      IS
         SELECT   /*+use_hash(a b) leading(a b)*/
                  a.query_id, our_time_start, our_time_end, qry_table_name,
                  cardacc, NO, acserno, currtype, cino, zoneno, brno, email,
                  mobno
             FROM hdm_query_date_split a, hdm_query_log_dat b
            WHERE update_date = p_i_date
              AND UPPER (qry_table_name) = 'HDM_S_PFEPSADL_QUERY'
              AND up_time_start IS NULL
              AND up_time_end IS NULL
              AND c_flag = '0'
              AND b.qry_channel <> '5'
              AND a.query_id = b.qry_id
         ORDER BY query_id;

      CURSOR export_pfepsadl_5
      IS
         SELECT   /*+use_hash(a b) leading(a b)*/
                  a.query_id, our_time_start, our_time_end, qry_table_name,
                  cardacc, NO, acserno, currtype, cino, zoneno, brno, email,
                  mobno
             FROM hdm_query_date_split a, hdm_query_log_dat b
            WHERE update_date = p_i_date
              AND UPPER (qry_table_name) = 'HDM_S_PFEPSADL_QUERY'
              AND up_time_start IS NULL
              AND up_time_end IS NULL
              AND c_flag = '0'
              AND b.qry_channel = '5'
              AND a.query_id = b.qry_id
         ORDER BY query_id;

      rec_cur           export_pfepsadl%ROWTYPE             := NULL;
      rec_cur_5         export_pfepsadl_5%ROWTYPE           := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;

      TYPE t_rec_3 IS RECORD (
         accno      VARCHAR2 (17),
         MDCARDNO   VARCHAR2 (19),
         busidate   VARCHAR2 (10),
         TIMESTMP   VARCHAR2 (26),
         summflag   VARCHAR2 (4000),
         currtype   VARCHAR2 (4000),
         cashexf    VARCHAR2 (4000),
         drcrf      VARCHAR2 (4000),
         amount     NUMBER (17, 2),
         balance    NUMBER (17, 2),
         servface   VARCHAR2 (4000),
         tellerno   VARCHAR2 (5),
         brno       VARCHAR2 (4000),
         zoneno     VARCHAR2 (4000),
         cashnote   VARCHAR2 (20),
         trxcode    VARCHAR2 (4000),
         workdate   VARCHAR2 (10),
         authtlno   VARCHAR2 (5),
         recipact   VARCHAR2 (34),
         phybrno    VARCHAR2 (5),
         termid     VARCHAR2 (15),
         TRXCURR    VARCHAR2 (4000),
         TRXAMT     NUMBER (17, 2),
         CRYCODE    VARCHAR2 (4000),
         RECIPCN    VARCHAR2 (15),
         TRXSITE    VARCHAR2 (40),
         BUSITIME   VARCHAR2 (20),
         brno_real  VARCHAR2(20),
         zoneno_real VARCHAR2(20)
      );                                      --定义记录变量存放担保段初始数据

      TYPE tt_rec_3 IS TABLE OF t_rec_3;

      --记录本次正常报文里有的结算应还款年月
      rec_3             tt_rec_3                            := NULL;
      v_no_all          NUMBER                              := 0;
      v_max_count       NUMBER                              := 0;
      v_instance        VARCHAR2 (100)                      := 'hdmpras';
      v_file_all        NUMBER                              := 0;
      v_count_each      NUMBER                              := 0;
      in_file           VARCHAR2 (32767);
      v_file            UTL_FILE.file_type                  := NULL;
      v_seq             NUMBER                              := 0;


      CURSOR cur_pfepsadl_query(NO varchar2, our_time_start varchar2, our_time_end varchar2, currtype varchar2)
      IS
      select /*+index(a INX_PFEPSADL_QUERY_DAT_1 INX_PFEPSADL_QUERY_MONTH_1)*/
                    a.accno,
                    busidate,
                    TIMESTMP,
                    NVL (c.prmname, a.summflag) summflag,
                    a.currtype,
                    NVL (d.prmname, a.currtype) currtype_c,
                    a.CASHEXF,
                    a.SERIALNO,
                    NVL (e.prmname, a.cashexf) cashexf_c,
                    NVL (f.prmname, a.drcrf) drcrf,
                    ltrim(to_char(amount / 100,'99999999999999999990.99'))  amount,
                    ltrim(to_char(balance / 100,'99999999999999999990.99'))  balance,
                    NVL (k.prmname, a.servface) servface,
                    tellerno,
                    NVL (h.notes, a.brno) brno,
                    NVL (b.notes, a.zoneno) zoneno,
                    cashnote,
                    NVL (g.prmname, a.trxcode) trxcode,
                    workdate,
                    authtlno,
                    recipact,
                    phybrno,
                    termid,
                    RECIPCN,
                    TRXSITE
               from hdm_s_pfepsadl_query a,
                    hdm_nthpazon_s_dat b,
                    (SELECT prmcode, prmname
                       FROM hdm_code_prm
                      WHERE UPPER (app_name) = 'PRAS'
                        AND UPPER (prmfieldname) = 'SUMMFLAG'
                        AND prmeffflag = '1') c,
                    (SELECT prmcode, prmname
                       FROM hdm_code_prm
                      WHERE UPPER (app_name) = 'PRAS'
                        AND UPPER (prmfieldname) = 'CURRTYPE'
                        AND prmeffflag = '1') d,
                    (SELECT prmcode, prmname
                       FROM hdm_code_prm
                      WHERE UPPER (app_name) = 'PRAS'
                        AND UPPER (prmfieldname) = 'CASHEXF'
                        AND prmeffflag = '1') e,
                    (SELECT prmcode, prmname
                       FROM hdm_code_prm
                      WHERE UPPER (app_name) = 'PRAS'
                        AND UPPER (prmfieldname) = 'DRCRF'
                        AND prmeffflag = '1') f,
                    (SELECT prmcode, prmname
                       FROM hdm_code_prm
                      WHERE UPPER (app_name) = 'PRAS'
                        AND UPPER (prmfieldname) = 'TRXCODE'
                        AND prmeffflag = '1') g,
                    hdm_nthcabrp_s_dat h,
                    (SELECT prmcode, prmname
                       FROM hdm_code_prm
                      WHERE UPPER (app_name) = 'PRAS'
                        AND UPPER (prmfieldname) = 'SERVFACE'
                        AND prmeffflag = '1') k
              WHERE a.accno = NO
                AND a.busidate BETWEEN our_time_start
                                   AND our_time_end
                AND (a.currtype = currtype or currtype='000')
                AND a.zoneno = b.zoneno(+)
                AND a.summflag = c.prmcode(+)
                AND a.currtype = d.prmcode(+)
                AND a.cashexf = e.prmcode(+)
                AND a.drcrf = f.prmcode(+)
                AND a.trxcode = g.prmcode(+)
                AND a.brno = h.brno(+)
                AND a.zoneno = h.zoneno(+)
                AND a.servface = k.prmcode(+);
        rec_pfepsadl_query cur_pfepsadl_query%rowtype;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2';

      SELECT max_count
        INTO v_max_count
        FROM hdm_email_maxcount
       WHERE table_name = 'HDM_S_PFEPSADL_QUERY';
      v_step := '3';

      v_step := '4';

      OPEN export_pfepsadl;

      LOOP
         v_step := '5';

         FETCH export_pfepsadl
          INTO rec_cur;

         v_step := '6';
         EXIT WHEN export_pfepsadl%NOTFOUND;
         rec_3 := NULL;

         IF rec_cur.cardacc = '1'
         THEN
            v_step := '7';
            delete from hdm_s_export_pfepsadl;

            v_step := '8';

            open cur_pfepsadl_query(rec_cur.NO, rec_cur.our_time_start, rec_cur.our_time_end, rec_cur.currtype);
            loop
                 fetch cur_pfepsadl_query into rec_pfepsadl_query;
                 exit when cur_pfepsadl_query%notfound;
                 insert into hdm_s_export_pfepsadl
                    (accno,busidate,TIMESTMP,summflag,currtype,currtype_c,CASHEXF,SERIALNO,cashexf_c,drcrf,amount,balance,servface,tellerno,brno,
                     zoneno,cashnote,trxcode,workdate,authtlno,recipact,phybrno,termid,RECIPCN,TRXSITE,ID)
                 values (
                         rec_pfepsadl_query.accno,rec_pfepsadl_query.busidate,rec_pfepsadl_query.TIMESTMP,rec_pfepsadl_query.summflag,
                         rec_pfepsadl_query.currtype,rec_pfepsadl_query.currtype_c,rec_pfepsadl_query.CASHEXF,rec_pfepsadl_query.SERIALNO,
                         rec_pfepsadl_query.cashexf_c,rec_pfepsadl_query.drcrf,rec_pfepsadl_query.amount,rec_pfepsadl_query.balance,rec_pfepsadl_query.servface,
                         rec_pfepsadl_query.tellerno,rec_pfepsadl_query.brno,rec_pfepsadl_query.zoneno,rec_pfepsadl_query.cashnote,
                         rec_pfepsadl_query.trxcode,rec_pfepsadl_query.workdate,rec_pfepsadl_query.authtlno,rec_pfepsadl_query.recipact,
                         rec_pfepsadl_query.phybrno,rec_pfepsadl_query.termid,rec_pfepsadl_query.RECIPCN,rec_pfepsadl_query.TRXSITE
                         ,rec_cur.query_id);
              end loop;
              close cur_pfepsadl_query;

            v_step := '9';
            SELECT a.accno,
                   null as MDCARDNO,
                   a.busidate,
                   a.TIMESTMP,
                   a.summflag,
                   a.currtype_c as currtype,
                   a.cashexf_c as cashexf,
                   a.drcrf,
                   a.amount,
                   a.balance,
                   a.servface,
                   a.tellerno,
                   a.brno,
                   a.zoneno,
                   a.cashnote,
                   a.trxcode,
                   a.workdate,
                   a.authtlno,
                   a.recipact,
                   a.phybrno,
                   a.termid,
                   nvl(b.TRXCURR,a.currtype) as TRXCURR,
                   nvl(b.TRXAMT,a.amount) as TRXAMT,
                   nvl(b.CRYCODE,a.TRXSITE) as CRYCODE,
                   a.RECIPCN,
                   a.TRXSITE,
                   null as BUSITIME,
                   null as brno_real,
                   null as zoneno_real
              BULK COLLECT INTO rec_3
              from hdm_s_export_pfepsadl a,
                   (select /*+ index(a inx_pfepabdl_query_dat_1 inx_pfepabdl_query_month_1) */ accno,
                           SERIALNO,
                           CURRTYPE,
                           CASHEXF,
                           BUSIDATE,
                           NVL (b.prmname, a.TRXCURR) TRXCURR,
                           TRXAMT/100 TRXAMT,
                           NVL (c.prmname, a.CRYCODE) CRYCODE
                      from hdm_s_pfepabdl_query a,
                           (SELECT prmcode, prmname
                              FROM hdm_code_prm
                             WHERE UPPER (app_name) = 'PRAS'
                               AND UPPER (prmfieldname) = 'TRXCURR'
                               AND prmeffflag = '1') b,
                           (SELECT prmcode, prmname
                              FROM hdm_code_prm
                             WHERE UPPER (app_name) = 'PRAS'
                               AND UPPER (prmfieldname) = 'CRYCODE'
                               AND prmeffflag = '1') c
                     where a.accno = rec_cur.NO
                       AND a.busidate BETWEEN rec_cur.our_time_start
                                          AND rec_cur.our_time_end
                       AND (a.currtype = rec_cur.currtype or rec_cur.currtype='000')
                       AND a.TRXCURR = b.prmcode(+)
                       AND a.CRYCODE = c.prmcode(+)) b
             where a.accno=b.accno(+)
               and a.SERIALNO=b.SERIALNO(+)
               and a.currtype=b.currtype(+)
               and a.CASHEXF=b.CASHEXF(+)
               and a.BUSIDATE=b.BUSIDATE(+)
               order by a.busidate,a.TIMESTMP;
         END IF;

         IF rec_3 IS NOT NULL

         THEN
            v_step := '9';
            v_no_all := rec_3.COUNT;
            v_step := '10';

            IF v_no_all > 0
            THEN
               v_file_all := CEIL (v_no_all / v_max_count);
            ELSE
               v_file_all := 1;
            END IF;

            v_step := '11';

            FOR i IN 1 .. v_file_all
            LOOP
               v_step := '12';

               IF i = v_file_all
               THEN
                  v_count_each := v_no_all - (v_file_all - 1) * v_max_count;
               ELSE
                  v_count_each := v_max_count;
               END IF;

               v_step := '13';
               in_file :=
                   rec_cur.query_id || '_' || v_instance || '_' || i || '.csv';
               v_step := '14';
               v_outputfile :=
                      UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
               v_step := '15';
               UTL_FILE.put_line
                  (v_outputfile,
                   'SEQ序列号,BUSIDATE入帐日期,SUMMFLAG零售存取款摘要,CURRTYPE币种,CASHEXF钞汇标志,DRCRF借贷标志,AMOUNT发生额,BALANCE余额,SERVFACE服务界面,TELLERNO柜员号,BRNO交易网点号,ZONENO交易地区号,CASHNOTE注释,TRXCODE交易代码,WORKDATE工作日期,AUTHTLNO授权柜员号,RECIPACT对方帐户,PHYBRNO帐户物理网点号,TERMID终端号,TRXCURR记帐币种,TRXAMT记帐金额,CRYCODE国家简称',
                   FALSE
                  );                                 -- 将报文头写入文件缓存区
               v_step := '16';
               UTL_FILE.fflush (v_outputfile);

               IF v_count_each > 0
               THEN

                  FOR j IN 1 .. v_count_each
                  LOOP
                     v_step := '17';
                     v_seq := (i - 1) * v_max_count + j;
                     v_step := '18';

                     UTL_FILE.put_line (v_outputfile,
                                           CHR (9)
                                        || v_seq
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).busidate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).summflag
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).currtype
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashexf
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).drcrf
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).amount
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).balance
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).servface
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).tellerno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).brno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).zoneno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashnote
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).trxcode
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).workdate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).authtlno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).recipact
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).phybrno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).termid
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).TRXCURR
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).TRXAMT
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).CRYCODE,
                                        FALSE
                                       );            -- 将报文头写入文件缓存区
                     UTL_FILE.fflush (v_outputfile);
                  END LOOP;
               END IF;

               UTL_FILE.fclose (v_outputfile);
            END LOOP;
         --END IF;
         END IF;

         v_step := '19';

         UPDATE hdm_query_log_dat
            SET qry_status = '5',
                attachment_count = NVL (v_file_all, 0)
          WHERE qry_id = rec_cur.query_id;

         v_step := '20';

         UPDATE hdm_query_date_split
            SET c_flag = '3'
          WHERE query_id = rec_cur.query_id;

         /*更新异步查询导出状态*/
         COMMIT;
      END LOOP;

      CLOSE export_pfepsadl;

      v_step := '21';

      OPEN export_pfepsadl_5;

      LOOP
         v_step := '22';

         FETCH export_pfepsadl_5
          INTO rec_cur_5;

         v_step := '23';
         EXIT WHEN export_pfepsadl_5%NOTFOUND;
         rec_3 := NULL;

         IF rec_cur_5.cardacc = '1'
         THEN
            --dbms_output.put_line(rec_cur_5.NO);
            --dbms_output.put_line(rec_cur_5.our_time_start);
            --dbms_output.put_line(rec_cur_5.our_time_end);
            v_step := '24';

            select accno,MDCARDNO,busidate,TIMESTMP,summflag,currtype,cashexf,drcrf,amount,balance,servface,tellerno,
                   brno,zoneno,cashnote,trxcode,workdate,authtlno,recipact,phybrno,termid,TRXCURR,TRXAMT,CRYCODE,RECIPCN,TRXSITE,BUSITIME,brno_real,zoneno_real
              BULK COLLECT INTO rec_3
              from (
            select accno,MDCARDNO,busidate,TIMESTMP,summflag,currtype,cashexf,drcrf,amount,balance,servface,tellerno,
                   brno,zoneno,cashnote,trxcode,workdate,authtlno,recipact,phybrno,termid,TRXCURR,TRXAMT,CRYCODE,RECIPCN,TRXSITE,BUSITIME,brno_real,zoneno_real
              from (
            SELECT /*+index(a INX_PFEPSADL_QUERY_DAT_1 INX_PFEPSADL_QUERY_MONTH_1)*/
                   a.accno,
                   a.MDCARDNO,
                   a.busidate,
                   a.TIMESTMP,
                   NVL (c.prmname, a.summflag) summflag,
                   NVL (d.prmname, a.currtype) currtype,
                   NVL (e.prmname, a.cashexf) cashexf,
                   NVL (f.prmname, a.drcrf) drcrf,
                   ltrim(to_char(a.amount / 100,'99999999999999999990.99'))  amount,
                   ltrim(to_char(a.balance / 100,'99999999999999999990.99'))  balance,
                   NVL (k.prmname, a.servface) servface,
                   a.tellerno,
                   NVL (h.notes, a.brno) brno,
                   NVL (b.notes, a.zoneno) zoneno,
                   a.cashnote,
                   NVL (g.prmname, a.trxcode) trxcode,
                   a.workdate,
                   a.authtlno,
                   a.recipact,
                   a.phybrno,
                   a.termid,
                   null as TRXCURR,
                   null as TRXAMT,
                   null as CRYCODE,
                   a.RECIPCN,
                   a.TRXSITE,
                   a.BUSITIME,
                   a.brno as brno_real,
                   a.zoneno as zoneno_real
              FROM hdm_s_pfepsadl_query a,
                   hdm_nthpazon_s_dat b,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SUMMFLAG'
                       AND prmeffflag = '1') c,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CURRTYPE'
                       AND prmeffflag = '1') d,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CASHEXF'
                       AND prmeffflag = '1') e,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'DRCRF'
                       AND prmeffflag = '1') f,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'TRXCODE'
                       AND prmeffflag = '1') g,
                   hdm_nthcabrp_s_dat h,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SERVFACE'
                       AND prmeffflag = '1') k
             WHERE a.accno = rec_cur_5.NO
               AND a.busidate BETWEEN rec_cur_5.our_time_start
                                  AND rec_cur_5.our_time_end
               AND (a.currtype = rec_cur_5.currtype or rec_cur_5.currtype='000')
               AND a.zoneno = b.zoneno(+)
               AND a.summflag = c.prmcode(+)
               AND a.currtype = d.prmcode(+)
               AND a.cashexf = e.prmcode(+)
               AND a.drcrf = f.prmcode(+)
               AND a.trxcode = g.prmcode(+)
               AND a.brno = h.brno(+)
               AND a.zoneno = h.zoneno(+)
               AND a.servface = k.prmcode(+))
               group by accno,MDCARDNO,busidate,TIMESTMP,summflag,currtype,cashexf,drcrf,amount,balance,servface,tellerno,
                        brno,zoneno,cashnote,trxcode,workdate,authtlno,recipact,phybrno,termid,TRXCURR,TRXAMT,CRYCODE,RECIPCN,TRXSITE,BUSITIME,brno_real,zoneno_real
               order by accno,busidate,TIMESTMP);
         END IF;

         IF rec_3 IS NOT NULL
         THEN

            v_step := '26';
            v_no_all := rec_3.COUNT;
            v_step := '27';

            IF v_no_all > 0
            THEN
               v_file_all := CEIL (v_no_all / v_max_count);
            ELSE
               v_file_all := 1;
            END IF;

            v_step := '28';

            FOR i IN 1 .. v_file_all
            LOOP
               v_step := '29';

               IF i = v_file_all
               THEN
                  v_count_each := v_no_all - (v_file_all - 1) * v_max_count;
               ELSE
                  v_count_each := v_max_count;
               END IF;
  dbms_output.put_line(333333333333333333);
               v_step := '30';
               in_file :=
                  rec_cur_5.query_id || '_' || v_instance || '_' || i
                  || '.csv';
               v_step := '31';
               v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_EMAIL_5', in_file, 'w', 32767);
               v_step := '32';
               UTL_FILE.put_line
                  (v_outputfile,
                      '序列号,账号,币种,卡号,交易时间戳,工作日期,借贷标志,发生额,余额,注释,对方帐户,对方客户编号,交易地区号,交易地区号(原始),交易网点号,交易网点号(原始),帐户物理网点号,柜员号,授权柜员号,交易代码,服务界面,交易日期,记帐时间,零售存取款摘要,钞汇标志,终端号,交易场所简称'
                   || CHR (9),
                   FALSE
                  );                                -- 将报文头写入文件缓存区
               v_step := '33';
               UTL_FILE.fflush (v_outputfile);

               IF v_count_each > 0
               THEN
                  FOR j IN 1 .. v_count_each
                  LOOP
                     v_step := '34';
                     v_seq := (i - 1) * v_max_count + j;
                     v_step := '35';

                     UTL_FILE.put_line (v_outputfile,
                                           CHR (9)
                                        || v_seq
                                        || ','
                                        || CHR (9)
                                        || FUNC_ACCNOTRANSFER(rec_3 (v_seq).ACCNO)
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).currtype
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).MDCARDNO
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).TIMESTMP
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).workdate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).drcrf
                                        || ','
                                        || CHR (9)
                                        || ltrim(to_char(rec_3 (v_seq).amount,'99999999999999999990.99'))
                                        || ','
                                        || CHR (9)
                                        || ltrim(to_char(rec_3 (v_seq).balance,'99999999999999999990.99'))
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashnote
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).recipact
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).RECIPCN
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).zoneno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).zoneno_real
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).brno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).brno_real
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).phybrno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).tellerno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).authtlno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).trxcode
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).servface
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).BUSIDATE
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).BUSITIME
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).summflag
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashexf
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).termid
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).TRXSITE,
                                        FALSE
                                       );            -- 将报文头写入文件缓存区
                     UTL_FILE.fflush (v_outputfile);
                  END LOOP;
               END IF;

               UTL_FILE.fclose (v_outputfile);
            END LOOP;
         --END IF;
         END IF;

         v_step := '36';

         UPDATE hdm_query_log_dat
            SET qry_status = '5',
                attachment_count = NVL (v_file_all, 0)
          WHERE qry_id = rec_cur_5.query_id;

         v_step := '37';

         UPDATE hdm_query_date_split
            SET c_flag = '3'
          WHERE query_id = rec_cur_5.query_id;

         /*更新异步查询导出状态*/
         COMMIT;
      END LOOP;

      CLOSE export_pfepsadl_5;

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_ourdb_export_pfepsadl;

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepvmdl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_our_export.hdm_ourdb_export_PFEPVMDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_pfepvmdl
      IS
         SELECT   /*+use_hash(a b) leading(a b)*/
                  a.query_id, our_time_start, our_time_end, qry_table_name,
                  cardacc, NO, acserno, currtype, cino, zoneno, brno, email,
                  mobno
             FROM hdm_query_date_split a, hdm_query_log_dat b
            WHERE update_date = p_i_date
              AND UPPER (qry_table_name) = 'HDM_S_PFEPVMDL_QUERY'
              AND up_time_start IS NULL
              AND up_time_end IS NULL
              AND c_flag = '0'
              AND a.query_id = b.qry_id
         ORDER BY query_id;

      rec_cur           export_pfepvmdl%ROWTYPE;
      v_outputfile      UTL_FILE.file_type;

      TYPE t_rec_3 IS RECORD (
         busidate   VARCHAR2 (10),
         summflag   VARCHAR2 (4000),
         currtype   VARCHAR2 (4000),
         cashexf    VARCHAR2 (4000),
         drcrf      VARCHAR2 (4000),
         amount     NUMBER (17, 2),
         balance    NUMBER (17, 2),
         servface   VARCHAR2 (4000),
         tellerno   VARCHAR2 (5),
         brno       VARCHAR2 (4000),
         zoneno     VARCHAR2 (4000),
         cashnote   VARCHAR2 (20),
         trxcode    VARCHAR2 (4000),
         workdate   VARCHAR2 (10),
         authtlno   VARCHAR2 (5),
         recipact   VARCHAR2 (34),
         phybrno    VARCHAR2 (5),
         termid     VARCHAR2 (15)
      );                                      --定义记录变量存放担保段初始数据

      TYPE tt_rec_3 IS TABLE OF t_rec_3;

      --记录本次正常报文里有的结算应还款年月
      rec_3             tt_rec_3                            := NULL;
      v_no_all          NUMBER                              := 0;
      v_max_count       NUMBER                              := 0;
      v_instance        VARCHAR2 (100)                      := 'hdmpras';
      v_file_all        NUMBER                              := 0;
      v_count_each      NUMBER                              := 0;
      in_file           VARCHAR2 (32767);
      v_file            UTL_FILE.file_type                  := NULL;
      v_seq             NUMBER                              := 0;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2';

      SELECT max_count
        INTO v_max_count
        FROM hdm_email_maxcount
       WHERE table_name = 'HDM_S_PFEPVMDL_QUERY';

      v_step := '3';

      v_step := '4';

      OPEN export_pfepvmdl;

      LOOP
         v_step := '5';

         FETCH export_pfepvmdl
          INTO rec_cur;

         v_step := '6';
         EXIT WHEN export_pfepvmdl%NOTFOUND;
         rec_3 := NULL;

         IF rec_cur.cardacc = '1'
         THEN
            v_step := '7';

            SELECT /*+index(a INX_PFEPVMDL_QUERY_DAT_1 INX_PFEPVMDL_QUERY_MONTH_1)*/
                   busidate,
                   NVL (c.prmname, a.summflag) summflag,
                   NVL (d.prmname, a.currtype) currtype,
                   NVL (e.prmname, a.cashexf) cashexf,
                   NVL (f.prmname, a.drcrf) drcrf,
                   amount / 100 amount,
                   balance / 100 balance,
                   NVL (k.prmname, a.servface) servface,
                   tellerno,
                   NVL (h.notes, a.brno) brno,
                   NVL (b.notes, a.zoneno) zoneno,
                   cashnote,
                   NVL (g.prmname, a.trxcode) trxcode,
                   workdate,
                   authtlno,
                   recipact,
                   phybrno,
                   termid
            BULK COLLECT INTO rec_3
              FROM hdm_s_pfepvmdl_query a,
                   hdm_nthpazon_s_dat b,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SUMMFLAG'
                       AND prmeffflag = '1') c,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CURRTYPE'
                       AND prmeffflag = '1') d,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CASHEXF'
                       AND prmeffflag = '1') e,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'DRCRF'
                       AND prmeffflag = '1') f,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'TRXCODE'
                       AND prmeffflag = '1') g,
                   hdm_nthcabrp_s_dat h,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SERVFACE'
                       AND prmeffflag = '1') k
             WHERE a.accno = rec_cur.NO
               AND a.busidate BETWEEN rec_cur.our_time_start
                                  AND rec_cur.our_time_end
               AND (a.currtype = rec_cur.currtype or rec_cur.currtype='000')
               AND a.zoneno = b.zoneno(+)
               AND a.summflag = c.prmcode(+)
               AND a.currtype = d.prmcode(+)
               AND a.cashexf = e.prmcode(+)
               AND a.drcrf = f.prmcode(+)
               AND a.trxcode = g.prmcode(+)
               AND a.brno = h.brno(+)
               AND a.zoneno = h.zoneno(+)
               AND a.servface = k.prmcode(+)
               order by a.busidate,a.TIMESTMP;
         ELSE
            v_step := '8';

            SELECT /*+index(a INX_PFEPVMDL_QUERY_DAT_1 INX_PFEPVMDL_QUERY_MONTH_1)*/
                   busidate,
                   NVL (c.prmname, a.summflag) summflag,
                   NVL (d.prmname, a.currtype) currtype,
                   NVL (e.prmname, a.cashexf) cashexf,
                   NVL (f.prmname, a.drcrf) drcrf,
                   amount / 100 amount,
                   balance / 100 balance,
                   NVL (k.prmname, a.servface) servface,
                   tellerno,
                   NVL (h.notes, a.brno) brno,
                   NVL (b.notes, a.zoneno) zoneno,
                   cashnote,
                   NVL (g.prmname, a.trxcode) trxcode,
                   workdate,
                   authtlno,
                   recipact,
                   phybrno,
                   termid
            BULK COLLECT INTO rec_3
              FROM hdm_s_pfepvmdl_query a,
                   hdm_nthpazon_s_dat b,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SUMMFLAG'
                       AND prmeffflag = '1') c,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CURRTYPE'
                       AND prmeffflag = '1') d,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CASHEXF'
                       AND prmeffflag = '1') e,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'DRCRF'
                       AND prmeffflag = '1') f,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'TRXCODE'
                       AND prmeffflag = '1') g,
                   hdm_nthcabrp_s_dat h,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SERVFACE'
                       AND prmeffflag = '1') k
             WHERE a.mdcardno = rec_cur.NO
               AND a.busidate BETWEEN rec_cur.our_time_start
                                  AND rec_cur.our_time_end
               AND (a.currtype = rec_cur.currtype or rec_cur.currtype='000')
               AND DECODE (rec_cur.acserno, NULL, '@@', a.acserno) =
                                                   NVL (rec_cur.acserno, '@@')
               AND a.zoneno = b.zoneno(+)
               AND a.summflag = c.prmcode(+)
               AND a.currtype = d.prmcode(+)
               AND a.cashexf = e.prmcode(+)
               AND a.drcrf = f.prmcode(+)
               AND a.trxcode = g.prmcode(+)
               AND a.brno = h.brno(+)
               AND a.zoneno = h.zoneno(+)
               AND a.servface = k.prmcode(+)
               order by a.busidate,a.TIMESTMP;
         END IF;

         IF rec_3 IS NOT NULL
         THEN
            v_step := '9';
            v_no_all := rec_3.COUNT;
            v_step := '10';

            IF v_no_all > 0
            THEN
               v_file_all := CEIL (v_no_all / v_max_count);
            ELSE
               v_file_all := 1;
            END IF;

            v_step := '11';

            FOR i IN 1 .. v_file_all
            LOOP
               IF i = v_file_all
               THEN
                  v_count_each := v_no_all - (v_file_all - 1) * v_max_count;
               ELSE
                  v_count_each := v_max_count;
               END IF;

               v_step := '12';
               in_file :=
                   rec_cur.query_id || '_' || v_instance || '_' || i || '.csv';
               v_step := '13';
               v_outputfile :=
                      UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
               v_step := '14';
               UTL_FILE.put_line
                  (v_outputfile,
                   'SEQ序列号,BUSIDATE入帐日期,SUMMFLAG零售存取款摘要,CURRTYPE币种,CASHEXF钞汇标志,DRCRF借贷标志,AMOUNT发生额,BALANCE余额,SERVFACE服务界面,TELLERNO柜员号,BRNO交易网点号,ZONENO交易地区号,CASHNOTE注释,TRXCODE交易代码,WORKDATE工作日期,AUTHTLNO授权柜员号,RECIPACT对方帐户客户协,PHYBRNO帐户物理网点号,TERMID终端号',
                   FALSE
                  );                                 -- 将报文头写入文件缓存区
               UTL_FILE.fflush (v_outputfile);

               IF v_count_each > 0
               THEN
                  FOR j IN 1 .. v_count_each
                  LOOP
                     v_step := '15';
                     v_seq := (i - 1) * v_max_count + j;
                     v_step := '16';
                     UTL_FILE.put_line (v_outputfile,
                                           CHR (9)
                                        || v_seq
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).busidate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).summflag
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).currtype
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashexf
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).amount
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).balance
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).servface
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).tellerno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).brno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).zoneno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashnote
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).trxcode
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).workdate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).authtlno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).recipact
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).phybrno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).termid,
                                        FALSE
                                       );            -- 将报文头写入文件缓存区
                     UTL_FILE.fflush (v_outputfile);
                  END LOOP;
               END IF;

               UTL_FILE.fclose (v_outputfile);
            END LOOP;
         --END IF;
         END IF;

         v_step := '17';

         UPDATE hdm_query_log_dat
            SET qry_status = '5',
                attachment_count = NVL (v_file_all, 0)
          WHERE qry_id = rec_cur.query_id;

         v_step := '18';

         UPDATE hdm_query_date_split
            SET c_flag = '3'
          WHERE query_id = rec_cur.query_id;

         /*更新异步查询导出状态*/
         COMMIT;
      END LOOP;

      CLOSE export_pfepvmdl;

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_ourdb_export_pfepvmdl;

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_ourdb_export_pfepotdl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
               := 'hdm_our_export.hdm_ourdb_export_PFEPOTDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_pfepotdl
      IS
         SELECT   /*+use_hash(a b) leading(a b)*/
                  a.query_id, our_time_start, our_time_end, qry_table_name,
                  cardacc, NO, acserno, currtype, cino, zoneno, brno, email,
                  mobno
             FROM hdm_query_date_split a, hdm_query_log_dat b
            WHERE update_date = p_i_date
              AND UPPER (qry_table_name) = 'HDM_S_PFEPOTDL_QUERY'
              AND up_time_start IS NULL
              AND up_time_end IS NULL
              AND c_flag = '0'
              AND a.query_id = b.qry_id
         ORDER BY query_id;

      rec_cur           export_pfepotdl%ROWTYPE;
      v_outputfile      UTL_FILE.file_type;

      TYPE t_rec_3 IS RECORD (
         busidate   VARCHAR2 (10),
         summflag   VARCHAR2 (4000),
         currtype   VARCHAR2 (4000),
         cashexf    VARCHAR2 (4000),
         drcrf      VARCHAR2 (4000),
         amount     NUMBER (17, 2),
         balance    NUMBER (17, 2),
         servface   VARCHAR2 (4000),
         tellerno   VARCHAR2 (5),
         brno       VARCHAR2 (4000),
         zoneno     VARCHAR2 (4000),
         cashnote   VARCHAR2 (20),
         trxcode    VARCHAR2 (4000),
         workdate   VARCHAR2 (10),
         authtlno   VARCHAR2 (5),
         recipact   VARCHAR2 (34),
         phybrno    VARCHAR2 (5),
         termid     VARCHAR2 (15)
      );                                      --定义记录变量存放担保段初始数据

      TYPE tt_rec_3 IS TABLE OF t_rec_3;

      --记录本次正常报文里有的结算应还款年月
      rec_3             tt_rec_3                            := NULL;
      v_no_all          NUMBER                              := 0;
      v_max_count       NUMBER                              := 0;
      v_instance        VARCHAR2 (100)                      := 'hdmpras';
      v_file_all        NUMBER                              := 0;
      v_count_each      NUMBER                              := 0;
      in_file           VARCHAR2 (32767);
      v_file            UTL_FILE.file_type                  := NULL;
      v_seq             NUMBER                              := 0;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2';

      SELECT max_count
        INTO v_max_count
        FROM hdm_email_maxcount
       WHERE table_name = 'HDM_S_PFEPOTDL_QUERY';

      v_step := '3';


      v_step := '4';

      OPEN export_pfepotdl;

      LOOP
         v_step := '5';

         FETCH export_pfepotdl
          INTO rec_cur;

         v_step := '6';
         EXIT WHEN export_pfepotdl%NOTFOUND;
         rec_3 := NULL;

         IF rec_cur.cardacc = '1'
         THEN
            v_step := '7';

            SELECT /*+index(a INX_PFEPOTDL_QUERY_DAT_1 INX_PFEPOTDL_QUERY_MONTH_1)*/
                   busidate,
                   NVL (c.prmname, a.summflag) summflag,
                   NVL (d.prmname, a.currtype) currtype,
                   NVL (e.prmname, a.cashexf) cashexf,
                   NVL (f.prmname, a.drcrf) drcrf,
                   amount / 100 amount,
                   balance / 100 balance,
                   NVL (k.prmname, a.servface) servface,
                   tellerno,
                   NVL (h.notes, a.brno) brno,
                   NVL (b.notes, a.zoneno) zoneno,
                   cashnote,
                   NVL (g.prmname, a.trxcode) trxcode,
                   workdate,
                   authtlno,
                   recipact,
                   phybrno,
                   termid
            BULK COLLECT INTO rec_3
              FROM hdm_s_pfepotdl_query a,
                   hdm_nthpazon_s_dat b,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SUMMFLAG'
                       AND prmeffflag = '1') c,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CURRTYPE'
                       AND prmeffflag = '1') d,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CASHEXF'
                       AND prmeffflag = '1') e,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'DRCRF'
                       AND prmeffflag = '1') f,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'TRXCODE'
                       AND prmeffflag = '1') g,
                   hdm_nthcabrp_s_dat h,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SERVFACE'
                       AND prmeffflag = '1') k
             WHERE accno = rec_cur.NO
               AND busidate BETWEEN rec_cur.our_time_start
                                AND rec_cur.our_time_end
               AND currtype = rec_cur.currtype
               AND a.zoneno = b.zoneno(+)
               AND a.summflag = c.prmcode(+)
               AND a.currtype = d.prmcode(+)
               AND a.cashexf = e.prmcode(+)
               AND a.drcrf = f.prmcode(+)
               AND a.trxcode = g.prmcode(+)
               AND a.brno = h.brno(+)
               AND a.zoneno = h.zoneno(+)
               AND a.servface = k.prmcode(+)
               order by a.busidate,a.TIMESTMP;
         ELSE
            v_step := '8';

            SELECT /*+index(a INX_PFEPOTDL_QUERY_DAT_2 INX_PFEPOTDL_QUERY_MONTH_2)*/
                   busidate,
                   NVL (c.prmname, a.summflag) summflag,
                   NVL (d.prmname, a.currtype) currtype,
                   NVL (e.prmname, a.cashexf) cashexf,
                   NVL (f.prmname, a.drcrf) drcrf,
                   amount / 100 amount,
                   balance / 100 balance,
                   NVL (k.prmname, a.servface) servface,
                   tellerno,
                   NVL (h.notes, a.brno) brno,
                   NVL (b.notes, a.zoneno) zoneno,
                   cashnote,
                   NVL (g.prmname, a.trxcode) trxcode,
                   workdate,
                   authtlno,
                   recipact,
                   phybrno,
                   termid
            BULK COLLECT INTO rec_3
              FROM hdm_s_pfepotdl_query a,
                   hdm_nthpazon_s_dat b,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SUMMFLAG'
                       AND prmeffflag = '1') c,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CURRTYPE'
                       AND prmeffflag = '1') d,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'CASHEXF'
                       AND prmeffflag = '1') e,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'DRCRF'
                       AND prmeffflag = '1') f,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'TRXCODE'
                       AND prmeffflag = '1') g,
                   hdm_nthcabrp_s_dat h,
                   (SELECT prmcode, prmname
                      FROM hdm_code_prm
                     WHERE UPPER (app_name) = 'PRAS'
                       AND UPPER (prmfieldname) = 'SERVFACE'
                       AND prmeffflag = '1') k
             WHERE mdcardno = rec_cur.NO
               AND busidate BETWEEN rec_cur.our_time_start
                                AND rec_cur.our_time_end
               AND currtype = rec_cur.currtype
               AND DECODE (rec_cur.acserno, NULL, '@@', a.acserno) =
                                                   NVL (rec_cur.acserno, '@@')
               AND a.zoneno = b.zoneno(+)
               AND a.summflag = c.prmcode(+)
               AND a.currtype = d.prmcode(+)
               AND a.cashexf = e.prmcode(+)
               AND a.drcrf = f.prmcode(+)
               AND a.trxcode = g.prmcode(+)
               AND a.brno = h.brno(+)
               AND a.zoneno = h.zoneno(+)
               AND a.servface = k.prmcode(+)
               order by a.busidate,a.TIMESTMP;
         END IF;

         IF rec_3 IS NOT NULL
         THEN
            v_step := '9';
            v_no_all := rec_3.COUNT;
            v_step := '10';

            IF v_no_all > 0
            THEN
               v_file_all := CEIL (v_no_all / v_max_count);
            ELSE
               v_file_all := 1;
            END IF;

            FOR i IN 1 .. v_file_all
            LOOP
               v_step := '11';

               IF i = v_file_all
               THEN
                  v_count_each := v_no_all - (v_file_all - 1) * v_max_count;
               ELSE
                  v_count_each := v_max_count;
               END IF;

               v_step := '12';
               in_file :=
                   rec_cur.query_id || '_' || v_instance || '_' || i || '.csv';
               v_step := '13';
               v_outputfile :=
                      UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
               v_step := '14';
               UTL_FILE.put_line
                  (v_outputfile,
                   'SEQ序列号,BUSIDATE入帐日期,SUMMFLAG零售存取款摘要,CURRTYPE币种,CASHEXF钞汇标志,DRCRF借贷标志,AMOUNT发生额,BALANCE余额,SERVFACE服务界面,TELLERNO柜员号,BRNO交易网点号,ZONENO交易地区号,CASHNOTE注释,TRXCODE交易代码,WORKDATE工作日期,AUTHTLNO授权柜员号,RECIPACT对方帐户客户协,PHYBRNO帐户物理网点号,TERMID终端号',
                   FALSE
                  );                                 -- 将报文头写入文件缓存区
               UTL_FILE.fflush (v_outputfile);

               IF v_count_each > 0
               THEN
                  FOR j IN 1 .. v_count_each
                  LOOP
                     v_step := '15';
                     v_seq := (i - 1) * v_max_count + j;
                     v_step := '16';
                     UTL_FILE.put_line (v_outputfile,
                                           CHR (9)
                                        || v_seq
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).busidate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).summflag
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).currtype
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashexf
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).amount
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).balance
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).servface
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).tellerno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).brno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).zoneno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).cashnote
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).trxcode
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).workdate
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).authtlno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).recipact
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).phybrno
                                        || ','
                                        || CHR (9)
                                        || rec_3 (v_seq).termid,
                                        FALSE
                                       );            -- 将报文头写入文件缓存区
                     UTL_FILE.fflush (v_outputfile);
                  END LOOP;
               END IF;

               UTL_FILE.fclose (v_outputfile);
            END LOOP;
         --END IF;
         END IF;

         v_step := '17';

         UPDATE hdm_query_log_dat
            SET qry_status = '5',
                attachment_count = NVL (v_file_all, 0)
          WHERE qry_id = rec_cur.query_id;

         v_step := '18';

         UPDATE hdm_query_date_split
            SET c_flag = '3'
          WHERE query_id = rec_cur.query_id;

         /*更新异步查询导出状态*/
         COMMIT;
      END LOOP;

      CLOSE export_pfepotdl;

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_ourdb_export_pfepotdl;

/******************************************************************************
   --存储过程名称：  hdm_our_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_update_c_flag (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
                      := 'hdm_list_export.hdm_update_c_flag' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      UPDATE hdm_query_log_dat
         SET qry_status = '6'
       WHERE qry_status = '5';

      v_step := '2';
      IF p_i_no <> 'N'
      THEN
         UPDATE hdm_query_log_dat
            SET qry_status = '8'
          WHERE qry_status = '7';
      END IF;

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_update_c_flag;

/******************************************************************************
   --存储过程名称：  hdm_update_c_flag_2
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_update_c_flag_2 (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
                      := 'hdm_list_export.hdm_update_c_flag_2' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      IF p_i_no <> 'N'
      THEN
         v_step := '3';
         merge /*+use_hash(a b) leading(a b)*/into hdm_query_date_split a
              using (select qry_id
                       from hdm_query_log_dat
                      where qry_status = '8') b
                 on (a.query_id = b.qry_id)
               when matched then
                     update set c_flag='1'
                          where a.c_flag = '0'
                            and a.up_time_start IS NOT NULL
                            AND a.up_time_end IS NOT NULL
                            AND a.our_time_start IS NOT NULL
                            AND a.our_time_end IS NOT NULL;
      END IF;

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_update_c_flag_2;


   /******************************************************************************
   --存储过程名称：  hdm_merge_nthcabrp_dat
   --作者：          kwg
   --时间：          2015年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_merge_nthcabrp_dat (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
                      := 'hdm_list_export.hdm_merge_nthcabrp_dat' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';

      --更新网点表hdm_nthcabrp_dat，提供前台使用。因hdm_nthcabrp_s_dat表的加载有truncate动作，避免前台查询时表被清空。
    merge into hdm_nthcabrp_dat a
         using hdm_nthcabrp_s_dat b
    on ( a.zoneno = b.zoneno and a.brno = b.brno)
    when matched
     then
         update set  a.MBRNO = b.MBRNO,
            a.BRTYPE = b.BRTYPE,
            a.NOTES = b.NOTES,
            a.STATUS = b.STATUS,
            a.RSFFLAG = b.RSFFLAG,
            a.RSFLHHH = b.RSFLHHH,
            a.RSFFQH = b.RSFFQH,
            a.RSFLHHBS = b.RSFLHHBS,
            a.RSFLHHM = b.RSFLHHM,
            a.RMBRNO = b.RMBRNO,
            a.ADDRESS = b.ADDRESS,
            a.ZIP = b.ZIP,
            a.PHONE1 = b.PHONE1,
            a.SHORTNM = b.SHORTNM,
            a.ACTBRNO = b.ACTBRNO,
            a.BRSTATUS = b.BRSTATUS,
            a.LSTMODD = b.LSTMODD,
            a.BAKDEC1 = b.BAKDEC1,
            a.BAKCHAR = b.BAKCHAR
     where
          NOT ( ((b.MBRNO is not null AND a.MBRNO is not null AND b.MBRNO = a.MBRNO) OR (b.MBRNO is null AND a.MBRNO is null)) AND
                ((b.BRTYPE is not null AND a.BRTYPE is not null AND b.BRTYPE = a.BRTYPE) OR (b.BRTYPE is null AND a.BRTYPE is null)) AND
                ((b.NOTES is not null AND a.NOTES is not null AND b.NOTES = a.NOTES) OR (b.NOTES is null AND a.NOTES is null)) AND
                ((b.STATUS is not null AND a.STATUS is not null AND b.STATUS = a.STATUS) OR (b.STATUS is null AND a.STATUS is null)) AND
                ((b.RSFFLAG is not null AND a.RSFFLAG is not null AND b.RSFFLAG = a.RSFFLAG) OR (b.RSFFLAG is null AND a.RSFFLAG is null)) AND
                ((b.RSFLHHH is not null AND a.RSFLHHH is not null AND b.RSFLHHH = a.RSFLHHH) OR (b.RSFLHHH is null AND a.RSFLHHH is null)) AND
                ((b.RSFFQH is not null AND a.RSFFQH is not null AND b.RSFFQH = a.RSFFQH) OR (b.RSFFQH is null AND a.RSFFQH is null)) AND
                ((b.RSFLHHBS is not null AND a.RSFLHHBS is not null AND b.RSFLHHBS = a.RSFLHHBS) OR (b.RSFLHHBS is null AND a.RSFLHHBS is null)) AND
                ((b.RSFLHHM is not null AND a.RSFLHHM is not null AND b.RSFLHHM = a.RSFLHHM) OR (b.RSFLHHM is null AND a.RSFLHHM is null)) AND
                ((b.RMBRNO is not null AND a.RMBRNO is not null AND b.RMBRNO = a.RMBRNO) OR (b.RMBRNO is null AND a.RMBRNO is null)) AND
                ((b.ADDRESS is not null AND a.ADDRESS is not null AND b.ADDRESS = a.ADDRESS) OR (b.ADDRESS is null AND a.ADDRESS is null)) AND
                ((b.ZIP is not null AND a.ZIP is not null AND b.ZIP = a.ZIP) OR (b.ZIP is null AND a.ZIP is null)) AND
                ((b.PHONE1 is not null AND a.PHONE1 is not null AND b.PHONE1 = a.PHONE1) OR (b.PHONE1 is null AND a.PHONE1 is null)) AND
                ((b.SHORTNM is not null AND a.SHORTNM is not null AND b.SHORTNM = a.SHORTNM) OR (b.SHORTNM is null AND a.SHORTNM is null)) AND
                ((b.ACTBRNO is not null AND a.ACTBRNO is not null AND b.ACTBRNO = a.ACTBRNO) OR (b.ACTBRNO is null AND a.ACTBRNO is null)) AND
                ((b.BRSTATUS is not null AND a.BRSTATUS is not null AND b.BRSTATUS = a.BRSTATUS) OR (b.BRSTATUS is null AND a.BRSTATUS is null)) AND
                ((b.LSTMODD is not null AND a.LSTMODD is not null AND b.LSTMODD = a.LSTMODD) OR (b.LSTMODD is null AND a.LSTMODD is null)) AND
                ((b.BAKDEC1 is not null AND a.BAKDEC1 is not null AND b.BAKDEC1 = a.BAKDEC1) OR (b.BAKDEC1 is null AND a.BAKDEC1 is null)) AND
                ((b.BAKCHAR is not null AND a.BAKCHAR is not null AND b.BAKCHAR = a.BAKCHAR) OR (b.BAKCHAR is null AND a.BAKCHAR is null)) )
       when not matched
        then
        insert
           ( ZONENO,
             BRNO,
            MBRNO,
            BRTYPE,
            NOTES,
            STATUS,
            RSFFLAG,
            RSFLHHH,
            RSFFQH,
            RSFLHHBS,
            RSFLHHM,
            RMBRNO,
            ADDRESS,
            ZIP,
            PHONE1,
            SHORTNM,
            ACTBRNO,
            BRSTATUS,
            LSTMODD,
            BAKDEC1,
            BAKCHAR )
        values  (
            b.ZONENO,
            b.BRNO,
            b.MBRNO,
            b.BRTYPE,
            b.NOTES,
            b.STATUS,
            b.RSFFLAG,
            b.RSFLHHH,
            b.RSFFQH,
            b.RSFLHHBS,
            b.RSFLHHM,
            b.RMBRNO,
            b.ADDRESS,
            b.ZIP,
            b.PHONE1,
            b.SHORTNM,
            b.ACTBRNO,
            b.BRSTATUS,
            b.LSTMODD,
            b.BAKDEC1,
            b.BAKCHAR );

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_merge_nthcabrp_dat;
END hdm_our_export;
//

CREATE OR REPLACE PACKAGE hdm_upour_export
AS
/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );

/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_upour_export;
//

CREATE OR REPLACE PACKAGE BODY hdm_upour_export
IS
/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepsadl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
             := 'hdm_upour_export.hdm_upour_export_PFEPSADL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_pfepsadl (v_id VARCHAR2)
      IS
         SELECT /*+use_hash(a b)*/
                a.accno,
                a.busidate,
                a.summflag,
                a.currtype_c as currtype,
                a.CASHEXF_c as CASHEXF,
                a.drcrf,
                a.amount,
                a.balance,
                a.servface,
                a.tellerno,
                a.brno,
                a.zoneno,
                a.cashnote,
                a.trxcode,
                a.workdate,
                a.authtlno,
                a.recipact,
                a.phybrno,
                a.termid,
                nvl(b.TRXCURR,'人民币') as TRXCURR,
                nvl(b.TRXAMT/100,a.amount) as TRXAMT,
                NVL (b.CRYCODE, '中国') CRYCODE,
                a.RECIPCN,
                a.TRXSITE,
                a.ID,
                a.TIMESTMP
           FROM (select accno,busidate,summflag,currtype,currtype_c,CASHEXF,SERIALNO,cashexf_c,drcrf,amount,balance,servface,tellerno,brno,
                        zoneno,cashnote,trxcode,workdate,authtlno,recipact,phybrno,termid,RECIPCN,TRXSITE,ID,TIMESTMP
                   from hdm_s_export_pfepsadl_up
                  where id=v_id) a,
                hdm_s_export_PFQABDTL b
          where a.accno=b.accno(+)
            and a.SERIALNO=b.SERIALNO(+)
            and a.currtype=b.currtype(+)
            and a.CASHEXF=b.CASHEXF(+)
            and a.BUSIDATE=b.BUSIDATE(+)
          order by a.busidate, a.timestmp;

      CURSOR export_pfepsadl_5 (v_id VARCHAR2)
      IS
         select accno,busidate,summflag,currtype,cashexf,drcrf,amount,
                balance, servface, tellerno, brno, zoneno, cashnote,
                trxcode, workdate, authtlno, recipact, phybrno, termid, recipcn, trxsite, ID,timestmp,busitime,MDCARDNO,zoneno_real,brno_real
           from (
         SELECT FUNC_ACCNOTRANSFER(ACCNO) as accno,busidate, summflag, currtype_c as currtype, cashexf_c as cashexf, drcrf, amount,
                balance, servface, tellerno, brno, zoneno, cashnote,
                trxcode, workdate, authtlno, recipact, phybrno, termid, recipcn, trxsite, ID,timestmp,busitime,MDCARDNO,zoneno_real,brno_real
           FROM hdm_s_export_pfepsadl_up
          WHERE ID = v_id)
          group by accno,busidate,summflag,currtype,cashexf,drcrf,amount,
                   balance, servface, tellerno, brno, zoneno, cashnote,
                   trxcode, workdate, authtlno, recipact, phybrno, termid, recipcn, trxsite, ID,timestmp,busitime,MDCARDNO,zoneno_real,brno_real
          order by accno,busidate,timestmp;

      rec_cur           export_pfepsadl%ROWTYPE             := NULL;
      rec_cur_5         export_pfepsadl_5%ROWTYPE           := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
      v_this_id_all     NUMBER                              := 0;
      v_max_count       NUMBER                              := 0;
      v_instance        VARCHAR2 (100)                      := 'hdmpras';
      v_file_all        NUMBER                              := 0;
      v_count_each      NUMBER                              := 0;
      in_file           VARCHAR2 (32767)                    := NULL;
      v_file            UTL_FILE.file_type                  := NULL;
      v_seq             NUMBER                              := 0;

      TYPE t_rec_2 IS RECORD (
         v_id   VARCHAR2 (20)
      );                                      --定义记录变量存放担保段初始数据

      TYPE tt_rec_2 IS TABLE OF t_rec_2;

      --记录本次正常报文里有的结算应还款年月
      rec_2             tt_rec_2                            := NULL;
      v_count_id        VARCHAR2 (20)                       := NULL;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2';

      SELECT max_count
        INTO v_max_count
        FROM hdm_email_maxcount
       WHERE table_name = 'HDM_S_PFEPSADL_QUERY';

      v_step := '3';
      v_step := '3A';
      DELETE FROM hdm_s_export_pfepsadl_up;

      v_step := '3C';
      INSERT INTO hdm_s_export_pfepsadl_up
                  (accno,busidate,summflag,currtype,currtype_c,CASHEXF,SERIALNO,cashexf_c,drcrf,amount,balance,servface,tellerno,brno,
                   zoneno,cashnote,trxcode,workdate,authtlno,recipact,phybrno,termid,RECIPCN,TRXSITE,ID,TIMESTMP)
         SELECT /*+use_hash(a m) leading(a m) use_hash(a b) leading(a b) use_hash(a h) leading(a h)*/
                 a.accno,
                 busidate,
                 NVL (c.prmname, a.summflag) summflag,
                 a.currtype,
                 NVL (d.prmname, a.currtype) currtype_c,
                 a.CASHEXF,
                 a.SERIALNO,
                 NVL (e.prmname, a.cashexf) cashexf_c,
                 NVL (f.prmname, a.drcrf) drcrf,
                 ltrim(to_char(a.amount / 100,'99999999999999999990.99')) amount,
                 ltrim(to_char(a.balance / 100,'99999999999999999990.99')) balance,
                 NVL (k.prmname, a.servface) servface,
                 a.tellerno,
                 NVL (h.notes, a.brno) brno,
                 NVL (b.notes, a.zoneno) zoneno,
                 a.cashnote,
                 NVL (g.prmname, a.trxcode) trxcode,
                 a.workdate,
                 a.authtlno,
                 a.recipact,
                 a.phybrno,
                 a.termid,
                 a.RECIPCN,
                 a.TRXSITE,
                 a.id,
                 a.TIMESTMP
           FROM (SELECT accno,busidate, summflag, currtype, cashexf,SERIALNO, drcrf, amount,
                        balance, servface, tellerno, brno, zoneno, cashnote,
                        trxcode, workdate, authtlno, recipact, phybrno,
                        termid,RECIPCN,TRXSITE,ID,TIMESTMP
                   FROM hdm_s_pfepsadl_return
                  WHERE query_flag <> '1'
                 UNION ALL
                 SELECT accno,busidate, summflag, currtype, cashexf,SERIALNO, drcrf, amount,
                        balance, servface, tellerno, brno, zoneno, cashnote,
                        trxcode, workdate, authtlno, recipact, phybrno,
                        termid,RECIPCN,TRXSITE,ID,TIMESTMP
                   FROM hdm_s_pfepsadl_query_list) a,
                (SELECT qry_id, qry_channel
                   FROM hdm_query_log_dat
                  WHERE qry_req_kind = '3') m,
                hdm_nthpazon_s_dat b,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'SUMMFLAG'
                    AND prmeffflag = '1') c,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'CURRTYPE'
                    AND prmeffflag = '1') d,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'CASHEXF'
                    AND prmeffflag = '1') e,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'DRCRF'
                    AND prmeffflag = '1') f,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'TRXCODE'
                    AND prmeffflag = '1') g,
                hdm_nthcabrp_s_dat h,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'SERVFACE'
                    AND prmeffflag = '1') k
          WHERE a.ID = m.qry_id
            AND m.qry_channel <> '5'
            AND a.zoneno = b.zoneno(+)
            AND a.summflag = c.prmcode(+)
            AND a.currtype = d.prmcode(+)
            AND a.cashexf = e.prmcode(+)
            AND a.drcrf = f.prmcode(+)
            AND a.trxcode = g.prmcode(+)
            AND a.brno = h.brno(+)
            AND a.zoneno = h.zoneno(+)
            AND a.servface = k.prmcode(+);

            v_step := '3D';
            insert into hdm_s_export_PFQABDTL
            (accno,SERIALNO,currtype,CASHEXF,BUSIDATE,TRXCURR,TRXAMT,CRYCODE)
            select accno,SERIALNO,currtype,CASHEXF,BUSIDATE,TRXCURR,TRXAMT,CRYCODE
              from (
                       select accno,SERIALNO,currtype,CASHEXF,BUSIDATE,
                              TRXCURR,TRXAMT,CRYCODE,
                              ROW_NUMBER () OVER (PARTITION BY accno,SERIALNO,currtype,CASHEXF,BUSIDATE ORDER BY accno,SERIALNO,currtype,CASHEXF,BUSIDATE)
                                                 rn
                         from    (
                                   select /*+use_hash(a b) leading(a b) index(b inx_pfepabdl_query_dat_1 inx_pfepabdl_query_month_1)*/
                                          b.accno,
                                          b.SERIALNO,
                                          b.currtype,
                                          b.CASHEXF,
                                          b.BUSIDATE,
                                          NVL (c.prmname, b.TRXCURR) TRXCURR,
                                          b.TRXAMT,
                                          NVL (d.prmname, b.CRYCODE) CRYCODE
                                     from (select accno,SERIALNO,currtype,CASHEXF,BUSIDATE,TRXCURR,CRYCODE
                                             from hdm_s_export_pfepsadl_up
                                            group by accno,SERIALNO,currtype,CASHEXF,BUSIDATE,TRXCURR,CRYCODE) a,
                                          hdm_s_pfepabdl_query b,
                                         (SELECT prmcode, prmname
                                            FROM hdm_code_prm
                                           WHERE UPPER (app_name) = 'PRAS'
                                             AND UPPER (prmfieldname) = 'TRXCURR'
                                             AND prmeffflag = '1') c,
                                         (SELECT prmcode, prmname
                                            FROM hdm_code_prm
                                           WHERE UPPER (app_name) = 'PRAS'
                                             AND UPPER (prmfieldname) = 'CRYCODE'
                                             AND prmeffflag = '1') d
                                    where a.accno=b.accno
                                      and a.SERIALNO=b.SERIALNO
                                      and a.currtype=b.currtype
                                      and a.CASHEXF=b.CASHEXF
                                      and a.BUSIDATE=b.BUSIDATE
                                      and b.TRXCURR = c.prmcode(+)
                                      and b.CRYCODE = d.prmcode(+)
                                    union all
                                   select accno,
                                          SERIALNO,
                                          currtype,
                                          CASHEXF,
                                          BUSIDATE,
                                          NVL (c.prmname, a.TRXCURR) TRXCURR,
                                          TRXAMT,
                                          NVL (d.prmname, a.CRYCODE) CRYCODE
                                     from HDM_S_PFQABDTL_RETURN a,
                                          (SELECT prmcode, prmname
                                            FROM hdm_code_prm
                                           WHERE UPPER (app_name) = 'PRAS'
                                             AND UPPER (prmfieldname) = 'TRXCURR'
                                             AND prmeffflag = '1') c,
                                         (SELECT prmcode, prmname
                                            FROM hdm_code_prm
                                           WHERE UPPER (app_name) = 'PRAS'
                                             AND UPPER (prmfieldname) = 'CRYCODE'
                                             AND prmeffflag = '1') d
                                    where a.query_flag = '0'
                                      and a.TRXCURR = c.prmcode(+)
                                      and a.CRYCODE = d.prmcode(+)
                                 )
               )
         where rn=1;

      v_step := '4';
      SELECT   ID
      BULK COLLECT INTO rec_2
          FROM hdm_query_return a, hdm_query_log_dat b
         WHERE table_name = 'HDM_S_PFEPSADL_QUERY_LIST'
           AND a.ID = b.qry_id
           AND b.qry_channel <> '5'
      GROUP BY ID
      ORDER BY ID;

      IF rec_2 IS NOT NULL
      THEN
         v_count_id := rec_2.COUNT;

         IF v_count_id > 0
         THEN
            FOR i IN 1 .. v_count_id
            LOOP
               v_step := '5';

               OPEN export_pfepsadl (rec_2 (i).v_id);

               --LOOP
                  --FETCH export_pfepsadl
                   --INTO rec_cur;
               v_step := '6';

               SELECT COUNT (1)
                 INTO v_this_id_all
                 FROM (SELECT accno
                         FROM hdm_s_pfepsadl_return
                        WHERE ID = rec_2 (i).v_id AND query_flag <> '1'
                       UNION ALL
                       SELECT accno
                         FROM hdm_s_pfepsadl_query_list
                        WHERE ID = rec_2 (i).v_id);

               IF v_this_id_all > 0
               THEN
                  v_file_all := CEIL (v_this_id_all / v_max_count);
               ELSE
                  v_file_all := 1;
               END IF;

               v_step := '7';

               FOR j IN 1 .. v_file_all
               LOOP
                  IF j = v_file_all
                  THEN
                     v_count_each :=
                                v_this_id_all
                                - (v_file_all - 1) * v_max_count;
                  ELSE
                     v_count_each := v_max_count;
                  END IF;

                  IF v_this_id_all = 0
                  THEN
                     v_count_each := 0;
                  END IF;

                  v_step := '8';
                  in_file :=
                     rec_2 (i).v_id || '_' || v_instance || '_' || j || '.csv';
                  v_step := '9';
                  v_outputfile :=
                      UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
                  v_step := '10';
                  UTL_FILE.put_line
                     (v_outputfile,
                      'SEQ序列号,BUSIDATE入帐日期,SUMMFLAG零售存取款摘要,CURRTYPE币种,CASHEXF钞汇标志,DRCRF借贷标志,AMOUNT发生额,BALANCE余额,SERVFACE服务界面,TELLERNO柜员号,BRNO交易网点号,ZONENO交易地区号,CASHNOTE注释,TRXCODE交易代码,WORKDATE工作日期,AUTHTLNO授权柜员号,RECIPACT对方帐户,PHYBRNO帐户物理网点号,TERMID终端号,TRXCURR记帐币种,TRXAMT记帐金额,CRYCODE国家简称',
                      FALSE
                     );                              -- 将报文头写入文件缓存区
                  UTL_FILE.fflush (v_outputfile);

                  IF v_count_each > 0
                  THEN
                     FOR k IN 1 .. v_count_each
                     LOOP
                        v_step := '11';
                        v_seq := (j - 1) * v_max_count + k;
                        v_step := '12';

                        FETCH export_pfepsadl
                         INTO rec_cur;

                        UTL_FILE.put_line (v_outputfile,
                                              CHR (9)
                                           || v_seq
                                           || ','
                                           || CHR (9)
                                           || rec_cur.busidate
                                           || ','
                                           || CHR (9)
                                           || rec_cur.summflag
                                           || ','
                                           || CHR (9)
                                           || rec_cur.currtype
                                           || ','
                                           || CHR (9)
                                           || rec_cur.cashexf
                                           || ','
                                           || CHR (9)
                                           || rec_cur.drcrf
                                           || ','
                                           || CHR (9)
                                           || rec_cur.amount
                                           || ','
                                           || CHR (9)
                                           || rec_cur.balance
                                           || ','
                                           || CHR (9)
                                           || rec_cur.servface
                                           || ','
                                           || CHR (9)
                                           || rec_cur.tellerno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.brno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.zoneno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.cashnote
                                           || ','
                                           || CHR (9)
                                           || rec_cur.trxcode
                                           || ','
                                           || CHR (9)
                                           || rec_cur.workdate
                                           || ','
                                           || CHR (9)
                                           || rec_cur.authtlno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.recipact
                                           || ','
                                           || CHR (9)
                                           || rec_cur.phybrno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.termid
                                           || ','
                                           || CHR (9)
                                           || rec_cur.TRXCURR
                                           || ','
                                           || CHR (9)
                                           || rec_cur.TRXAMT
                                           || ','
                                           || CHR (9)
                                           || rec_cur.CRYCODE,
                                           FALSE
                                          );         -- 将报文头写入文件缓存区
                        UTL_FILE.fflush (v_outputfile);
                        EXIT WHEN export_pfepsadl%NOTFOUND;
                     END LOOP;
                  END IF;

                  UTL_FILE.fclose (v_outputfile);
               END LOOP;

                  --EXIT WHEN export_pfepsadl%NOTFOUND;
               --END LOOP;
               CLOSE export_pfepsadl;

               v_step := '13';

               UPDATE hdm_query_log_dat
                  SET qry_status = '5',
                      attachment_count = NVL (v_file_all, 0)
                WHERE qry_id = rec_2 (i).v_id;

               COMMIT;
            END LOOP;
         END IF;
      END IF;

      v_step := '13A';
      DELETE FROM hdm_s_export_pfepsadl_up;

      v_step := '13B';
      INSERT INTO hdm_s_export_pfepsadl_up
                  (accno,busidate, summflag, currtype_c, cashexf_c, drcrf, amount,
                   balance, servface, tellerno, brno, zoneno, cashnote,
                   trxcode, workdate, authtlno, recipact, phybrno, termid, recipcn, trxsite, ID, timestmp, busitime,MDCARDNO,
                   zoneno_real,brno_real)
         SELECT   /*+use_hash(a m) leading(a m) use_hash(a b) leading(a b) use_hash(a h) leading(a h)*/
                  accno,busidate, NVL (c.prmname, a.summflag) summflag,
                  NVL (d.prmname, a.currtype) currtype_c,
                  NVL (e.prmname, a.cashexf) cashexf_c,
                  NVL (f.prmname, a.drcrf) drcrf, amount, balance,
                  NVL (k.prmname, a.servface) servface, tellerno,
                  NVL (h.notes, a.brno) brno, NVL (b.notes, a.zoneno) zoneno,
                  cashnote, NVL (g.prmname, a.trxcode) trxcode, workdate,
                  authtlno, recipact, phybrno, termid, recipcn,trxsite, a.ID, TIMESTMP, busitime,MDCARDNO,
                  a.zoneno as zoneno_real,
                  a.brno as brno_real
             FROM (SELECT accno,busidate, summflag, currtype, cashexf, drcrf,
                          amount, balance, servface, tellerno, brno, zoneno,
                          cashnote, trxcode, workdate, authtlno, recipact,
                          phybrno, termid, ID, timestmp, recipcn, trxsite, SYNFLAG, busitime,MDCARDNO
                     FROM hdm_s_pfepsadl_return
                    WHERE query_flag <> '1'
                   UNION ALL
                   SELECT accno,busidate, summflag, currtype, cashexf, drcrf,
                          amount, balance, servface, tellerno, brno, zoneno,
                          cashnote, trxcode, workdate, authtlno, recipact,
                          phybrno, termid, ID, timestmp, recipcn, trxsite, SYNFLAG, busitime,MDCARDNO
                     FROM hdm_s_pfepsadl_query_list) a,
                  (SELECT qry_id, qry_channel
                     FROM hdm_query_log_dat
                    WHERE qry_req_kind = '3') m,
                  hdm_nthpazon_s_dat b,
                  (SELECT prmcode, prmname
                     FROM hdm_code_prm
                    WHERE UPPER (app_name) = 'PRAS'
                      AND UPPER (prmfieldname) = 'SUMMFLAG'
                      AND prmeffflag = '1') c,
                  (SELECT prmcode, prmname
                     FROM hdm_code_prm
                    WHERE UPPER (app_name) = 'PRAS'
                      AND UPPER (prmfieldname) = 'CURRTYPE'
                      AND prmeffflag = '1') d,
                  (SELECT prmcode, prmname
                     FROM hdm_code_prm
                    WHERE UPPER (app_name) = 'PRAS'
                      AND UPPER (prmfieldname) = 'CASHEXF'
                      AND prmeffflag = '1') e,
                  (SELECT prmcode, prmname
                     FROM hdm_code_prm
                    WHERE UPPER (app_name) = 'PRAS'
                      AND UPPER (prmfieldname) = 'DRCRF'
                      AND prmeffflag = '1') f,
                  (SELECT prmcode, prmname
                     FROM hdm_code_prm
                    WHERE UPPER (app_name) = 'PRAS'
                      AND UPPER (prmfieldname) = 'TRXCODE'
                      AND prmeffflag = '1') g,
                  hdm_nthcabrp_s_dat h,
                  (SELECT prmcode, prmname
                     FROM hdm_code_prm
                    WHERE UPPER (app_name) = 'PRAS'
                      AND UPPER (prmfieldname) = 'SERVFACE'
                      AND prmeffflag = '1') k
            WHERE a.ID = m.qry_id
              AND m.qry_channel = '5'
              AND a.zoneno = b.zoneno(+)
              AND a.summflag = c.prmcode(+)
              AND a.currtype = d.prmcode(+)
              AND a.cashexf = e.prmcode(+)
              AND a.drcrf = f.prmcode(+)
              AND a.trxcode = g.prmcode(+)
              AND a.brno = h.brno(+)
              AND a.zoneno = h.zoneno(+)
              AND a.servface = k.prmcode(+)
         ORDER BY a.accno,a.busidate, a.timestmp;

      v_step := '14';

      SELECT   ID
      BULK COLLECT INTO rec_2
          FROM hdm_query_return a, hdm_query_log_dat b
         WHERE table_name = 'HDM_S_PFEPSADL_QUERY_LIST'
           AND a.ID = b.qry_id
           AND b.qry_channel = '5'
      GROUP BY ID
      ORDER BY ID;

      IF rec_2 IS NOT NULL
      THEN
         v_count_id := rec_2.COUNT;

         IF v_count_id > 0
         THEN
            FOR i IN 1 .. v_count_id
            LOOP
               v_step := '15';

               OPEN export_pfepsadl_5 (rec_2 (i).v_id);

               v_step := '16';

               SELECT COUNT (1)
                 INTO v_this_id_all
                 FROM (select accno,busidate,summflag,currtype,cashexf,drcrf,amount,
                              balance, servface, tellerno, brno, zoneno, cashnote,
                              trxcode, workdate, authtlno, recipact, phybrno, termid, recipcn, trxsite, ID,timestmp,busitime,MDCARDNO
                         from hdm_s_export_pfepsadl_up
                        group by accno,busidate,summflag,currtype,cashexf,drcrf,amount,
                                 balance, servface, tellerno, brno, zoneno, cashnote,
                                 trxcode, workdate, authtlno, recipact, phybrno, termid, recipcn, trxsite, ID,timestmp,busitime,MDCARDNO
                      )
                WHERE ID = rec_2 (i).v_id;

               IF v_this_id_all > 0
               THEN
                  v_file_all := CEIL (v_this_id_all / v_max_count);
               ELSE
                  v_file_all := 1;
               END IF;

               v_step := '17';

               FOR j IN 1 .. v_file_all
               LOOP
                  IF j = v_file_all
                  THEN
                     v_count_each :=
                                v_this_id_all
                                - (v_file_all - 1) * v_max_count;
                  ELSE
                     v_count_each := v_max_count;
                  END IF;

                  IF v_this_id_all = 0
                  THEN
                     v_count_each := 0;
                  END IF;

                  v_step := '18';
                  in_file :=
                     rec_2 (i).v_id || '_' || v_instance || '_' || j || '.csv';
                  v_step := '19';
                  v_outputfile :=
                     UTL_FILE.fopen ('EXPORT_EMAIL_5', in_file, 'w',
                                         32767);
                  v_step := '20';
                  UTL_FILE.put_line
                     (v_outputfile,
                         '序列号,账号,币种,卡号,交易时间戳,工作日期,借贷标志,发生额,余额,注释,对方帐户,对方客户编号,交易地区号,交易地区号(原始),交易网点号,交易网点号(原始),帐户物理网点号,柜员号,授权柜员号,交易代码,服务界面,交易日期,记帐时间,零售存取款摘要,钞汇标志,终端号,交易场所简称'
                      || CHR (9),
                      FALSE
                     );                              -- 将报文头写入文件缓存区
                  UTL_FILE.fflush (v_outputfile);

                  IF v_count_each > 0
                  THEN
                     FOR k IN 1 .. v_count_each
                     LOOP
                        v_step := '21';
                        v_seq := (j - 1) * v_max_count + k;
                        v_step := '22';

                        FETCH export_pfepsadl_5
                         INTO rec_cur_5;

                        UTL_FILE.put_line (v_outputfile,
                                              CHR (9)
                                           || v_seq
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.accno
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.currtype
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.MDCARDNO
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.TIMESTMP
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.workdate
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.drcrf
                                           || ','
                                           || CHR (9)
                                           || ltrim(to_char(rec_cur_5.amount / 100,'99999999999999999990.99'))
                                           || ','
                                           || CHR (9)
                                           || ltrim(to_char(rec_cur_5.balance / 100,'99999999999999999990.99'))
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.cashnote
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.recipact
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.RECIPCN
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.zoneno
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.zoneno_real
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.brno
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.brno_real
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.phybrno
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.tellerno
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.authtlno
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.trxcode
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.servface
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.BUSIDATE
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.BUSITIME
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.summflag
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.cashexf
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.termid
                                           || ','
                                           || CHR (9)
                                           || rec_cur_5.TRXSITE,
                                           FALSE
                                          );         -- 将报文头写入文件缓存区
                        UTL_FILE.fflush (v_outputfile);
                        EXIT WHEN export_pfepsadl_5%NOTFOUND;
                     END LOOP;
                  END IF;

                  UTL_FILE.fclose (v_outputfile);
               END LOOP;

               CLOSE export_pfepsadl_5;

               v_step := '23';

               UPDATE hdm_query_log_dat
                  SET qry_status = '5',
                      attachment_count = NVL (v_file_all, 0)
                WHERE qry_id = rec_2 (i).v_id;

               /*更新异步查询导出状态*/
               COMMIT;
            END LOOP;
         END IF;
      END IF;

      v_step := '24';

      /*将已经导出的数据在LIST表里清除*/
      INSERT INTO hdm_s_pfepsadl_list_his
                  (accno, serialno, currtype, cashexf, actbrno, phybrno,
                   subcode, cino, accatrbt, busidate, busitime, trxcode,
                   workdate, valueday, closdate, drcrf, vouhno, amount,
                   balance, closint, inttax, summflag, cashnote, zoneno, brno,
                   tellerno, trxsqno, servface, authono, authtlno, termid,
                   timestmp, recipact, mdcardno, acserno, synflag, showflag,
                   macardn, recipcn, catrflag, sellercode, amount1, amount2,
                   flag1, date1, eserialno, trxsite, zoneaccno, ID)
         SELECT accno, serialno, currtype, cashexf, actbrno, phybrno, subcode,
                cino, accatrbt, busidate, busitime, trxcode, workdate,
                valueday, closdate, drcrf, vouhno, amount, balance, closint,
                inttax, summflag, cashnote, zoneno, brno, tellerno, trxsqno,
                servface, authono, authtlno, termid, timestmp, recipact,
                mdcardno, acserno, synflag, showflag, macardn, recipcn,
                catrflag, sellercode, amount1, amount2, flag1, date1,
                eserialno, trxsite, zoneaccno, ID
           FROM hdm_s_pfepsadl_query_list
          WHERE ID IN (SELECT ID
                         FROM hdm_query_return
                        WHERE table_name = 'HDM_S_PFEPSADL_QUERY_LIST');

      DELETE FROM hdm_s_pfepsadl_query_list
            WHERE ID IN (SELECT ID
                           FROM hdm_query_return
                          WHERE table_name = 'HDM_S_PFEPSADL_QUERY_LIST');

      v_step := '25';

      INSERT INTO hdm_s_pfepsadl_return_his
                  (accno, serialno, currtype, cashexf, actbrno, phybrno,
                   subcode, cino, accatrbt, busidate, busitime, trxcode,
                   workdate, valueday, closdate, drcrf, vouhno, amount,
                   balance, closint, inttax, summflag, cashnote, zoneno, brno,
                   tellerno, trxsqno, servface, authono, authtlno, termid,
                   timestmp, recipact, mdcardno, acserno, synflag, showflag,
                   macardn, recipcn, catrflag, sellercode, amount1, amount2,
                   flag1, date1, eserialno, trxsite, zoneaccno, ID,
                   query_flag)
         SELECT accno, serialno, currtype, cashexf, actbrno, phybrno, subcode,
                cino, accatrbt, busidate, busitime, trxcode, workdate,
                valueday, closdate, drcrf, vouhno, amount, balance, closint,
                inttax, summflag, cashnote, zoneno, brno, tellerno, trxsqno,
                servface, authono, authtlno, termid, timestmp, recipact,
                mdcardno, acserno, synflag, showflag, macardn, recipcn,
                catrflag, sellercode, amount1, amount2, flag1, date1,
                eserialno, trxsite,zoneaccno, ID, query_flag
           FROM hdm_s_pfepsadl_return;

      v_step := '26';
      insert into HDM_S_PFQABDTL_RETURN_his
      (ID,query_flag,ACCNO,SERIALNO,CURRTYPE,CASHEXF,BUSIDATE,AMOUNT,
       MDCARDNO,ZONENO,BRNO,TELLERNO,SERVFACE,TRXCURR,TRXAMT,CRYCODE,FEECASHEXF,
       FEEAMT,FEECURR,TRXFEEAMT,TRXFEECURR,CASHNOTE,TIMESTMP,ESERIALNO,WORKDATE,BAKFIELD1)
       select ID,query_flag,ACCNO,SERIALNO,CURRTYPE,CASHEXF,BUSIDATE,AMOUNT,
              MDCARDNO,ZONENO,BRNO,TELLERNO,SERVFACE,TRXCURR,TRXAMT,CRYCODE,FEECASHEXF,
              FEEAMT,FEECURR,TRXFEEAMT,TRXFEECURR,CASHNOTE,TIMESTMP,ESERIALNO,WORKDATE,BAKFIELD1
         from HDM_S_PFQABDTL_RETURN;

      /*将return表清空*/
      DELETE FROM hdm_s_pfepsadl_return;
      DELETE FROM HDM_S_PFQABDTL_RETURN;

      v_step := '27';
      DELETE FROM hdm_s_pfepsadl_return_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      DELETE FROM HDM_S_PFQABDTL_RETURN_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      v_step := '28';
      DELETE FROM hdm_s_pfepsadl_list_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      DELETE FROM hdm_s_pfepsadl_query_list
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_upour_export_pfepsadl;

/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepvmdl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
             := 'hdm_upour_export.hdm_upour_export_PFEPVMDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_pfepvmdl (v_id VARCHAR2)
      IS
         SELECT busidate, NVL (c.prmname, a.summflag) summflag,
                NVL (d.prmname, a.currtype) currtype,
                NVL (e.prmname, a.cashexf) cashexf,
                NVL (f.prmname, a.drcrf) drcrf, amount, balance,
                NVL (k.prmname, a.servface) servface, tellerno,
                NVL (h.notes, a.brno) brno, NVL (b.notes, a.zoneno) zoneno,
                cashnote, NVL (g.prmname, a.trxcode) trxcode, workdate,
                authtlno, recipact, phybrno, termid
           FROM (SELECT busidate, summflag, currtype, cashexf, drcrf, amount,
                        balance, servface, tellerno, brno, zoneno, cashnote,
                        trxcode, workdate, authtlno, recipact, phybrno,
                        termid, ID,timestmp
                   FROM hdm_s_pfepvmdl_return
                  WHERE ID = v_id AND query_flag <> '1'
                 UNION ALL
                 SELECT busidate, summflag, currtype, cashexf, drcrf, amount,
                        balance, servface, tellerno, brno, zoneno, cashnote,
                        trxcode, workdate, authtlno, recipact, phybrno,
                        termid, ID,timestmp
                   FROM hdm_s_pfepvmdl_query_list
                  WHERE ID = v_id) a,
                hdm_nthpazon_s_dat b,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'SUMMFLAG'
                    AND prmeffflag = '1') c,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'CURRTYPE'
                    AND prmeffflag = '1') d,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'CASHEXF'
                    AND prmeffflag = '1') e,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'DRCRF'
                    AND prmeffflag = '1') f,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'TRXCODE'
                    AND prmeffflag = '1') g,
                hdm_nthcabrp_s_dat h,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'SERVFACE'
                    AND prmeffflag = '1') k
          WHERE a.zoneno = b.zoneno(+)
            AND a.summflag = c.prmcode(+)
            AND a.currtype = d.prmcode(+)
            AND a.cashexf = e.prmcode(+)
            AND a.drcrf = f.prmcode(+)
            AND a.trxcode = g.prmcode(+)
            AND a.brno = h.brno(+)
            AND a.zoneno = h.zoneno(+)
            AND a.servface = k.prmcode(+)
          ORDER BY a.busidate, a.timestmp;

      rec_cur           export_pfepvmdl%ROWTYPE             := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
      v_this_id_all     NUMBER                              := NULL;
      v_max_count       NUMBER                              := NULL;
      v_instance        VARCHAR2 (100)                      := 'hdmpras';
      v_file_all        NUMBER                              := NULL;
      v_count_each      NUMBER                              := NULL;
      in_file           VARCHAR2 (32767)                    := NULL;
      v_file            UTL_FILE.file_type                  := NULL;
      v_seq             NUMBER                              := NULL;

      TYPE t_rec_2 IS RECORD (
         v_id   VARCHAR2 (20)
      );                                      --定义记录变量存放担保段初始数据

      TYPE tt_rec_2 IS TABLE OF t_rec_2;

      --记录本次正常报文里有的结算应还款年月
      rec_2             tt_rec_2                            := NULL;
      v_count_id        VARCHAR2 (20)                       := NULL;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2';

      SELECT max_count
        INTO v_max_count
        FROM hdm_email_maxcount
       WHERE table_name = 'HDM_S_PFEPVMDL_QUERY';

      v_step := '3';
      v_step := '4';

      SELECT   ID
      BULK COLLECT INTO rec_2
          FROM hdm_query_return
         WHERE table_name = 'HDM_S_PFEPVMDL_QUERY_LIST'
      GROUP BY ID
      ORDER BY ID;

      IF rec_2 IS NOT NULL
      THEN
         v_step := '5';
         v_count_id := rec_2.COUNT;

         IF v_count_id > 0
         THEN
            FOR i IN 1 .. v_count_id
            LOOP
               v_step := '6';

               OPEN export_pfepvmdl (rec_2 (i).v_id);

               --LOOP
                  --v_step := '7';

               --FETCH export_pfepvmdl
                --INTO rec_cur;
               v_step := '8';

               SELECT COUNT (1)
                 INTO v_this_id_all
                 FROM (SELECT accno
                         FROM hdm_s_pfepvmdl_return
                        WHERE ID = rec_2 (i).v_id AND query_flag <> '1'
                       UNION ALL
                       SELECT accno
                         FROM hdm_s_pfepvmdl_query_list
                        WHERE ID = rec_2 (i).v_id);

               IF v_this_id_all > 0
               THEN
                  v_file_all := CEIL (v_this_id_all / v_max_count);
               ELSE
                  v_file_all := 1;
               END IF;

               FOR j IN 1 .. v_file_all
               LOOP
                  IF j = v_file_all
                  THEN
                     v_count_each :=
                                v_this_id_all
                                - (v_file_all - 1) * v_max_count;
                  ELSE
                     v_count_each := v_max_count;
                  END IF;

                  IF v_this_id_all = 0
                  THEN
                     v_count_each := 0;
                  END IF;

                  v_step := '9';
                  in_file :=
                     rec_2 (i).v_id || '_' || v_instance || '_' || j || '.csv';
                  v_step := '10';
                  v_outputfile :=
                      UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
                  v_step := '11';
                  UTL_FILE.put_line
                     (v_outputfile,
                      'SEQ序列号,BUSIDATE入帐日期,SUMMFLAG零售存取款摘要,CURRTYPE币种,CASHEXF钞汇标志,DRCRF借贷标志,AMOUNT发生额,BALANCE余额,SERVFACE服务界面,TELLERNO柜员号,BRNO交易网点号,ZONENO交易地区号,CASHNOTE注释,TRXCODE交易代码,WORKDATE工作日期,AUTHTLNO授权柜员号,RECIPACT对方帐户客户协,PHYBRNO帐户物理网点号,TERMID终端号',
                      FALSE
                     );                              -- 将报文头写入文件缓存区
                  UTL_FILE.fflush (v_outputfile);

                  IF v_count_each > 0
                  THEN
                     FOR k IN 1 .. v_count_each
                     LOOP
                        v_step := '12';
                        v_seq := (j - 1) * v_max_count + k;
                        v_step := '13';

                        FETCH export_pfepvmdl
                         INTO rec_cur;

                        UTL_FILE.put_line (v_outputfile,
                                              CHR (9)
                                           || v_seq
                                           || ','
                                           || CHR (9)
                                           || rec_cur.busidate
                                           || ','
                                           || CHR (9)
                                           || rec_cur.summflag
                                           || ','
                                           || CHR (9)
                                           || rec_cur.currtype
                                           || ','
                                           || CHR (9)
                                           || rec_cur.cashexf
                                           || ','
                                           || CHR (9)
                                           || rec_cur.amount / 100
                                           || ','
                                           || CHR (9)
                                           || rec_cur.balance / 100
                                           || ','
                                           || CHR (9)
                                           || rec_cur.servface
                                           || ','
                                           || CHR (9)
                                           || rec_cur.tellerno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.brno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.zoneno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.cashnote
                                           || ','
                                           || CHR (9)
                                           || rec_cur.trxcode
                                           || ','
                                           || CHR (9)
                                           || rec_cur.workdate
                                           || ','
                                           || CHR (9)
                                           || rec_cur.authtlno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.recipact
                                           || ','
                                           || CHR (9)
                                           || rec_cur.phybrno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.termid,
                                           FALSE
                                          );         -- 将报文头写入文件缓存区
                        UTL_FILE.fflush (v_outputfile);
                        EXIT WHEN export_pfepvmdl%NOTFOUND;
                     END LOOP;
                  END IF;

                  UTL_FILE.fclose (v_outputfile);
               END LOOP;

                  --EXIT WHEN export_pfepvmdl%NOTFOUND;
               --END LOOP;
               CLOSE export_pfepvmdl;

               v_step := '14';

               UPDATE hdm_query_log_dat
                  SET qry_status = '5',
                      attachment_count = NVL (v_file_all, 0)
                WHERE qry_id = rec_2 (i).v_id;

               /*UPDATE hdm_query_date_split
                  SET c_flag = '1'
                WHERE query_id = rec_2 (i).v_id;*/

               /*更新异步查询导出状态*/
               COMMIT;
            END LOOP;
         END IF;
      END IF;

      /*将已经导出的数据在LIST表里清除*/
      v_step := '15';

      INSERT INTO hdm_s_pfepvmdl_list_his
                  (accno, serialno, currtype, cashexf, actbrno, phybrno,
                   subcode, cino, busidate, busitime, trxcode, workdate,
                   valueday, closdate, drcrf, vouhno, amount, balance,
                   closint, keepint, inttax, summflag, cashnote, savterm,
                   rate, matudate, zoneno, brno, tellerno, trxsqno, servface,
                   authono, authtlno, termid, timestmp, recipact, mdcardno,
                   acserno, synflag, showflag, macardn, recipcn, catrflag,
                   sellercode, amount1, amount2, flag1, date1,
                   zoneaccno, ID)
         SELECT accno, serialno, currtype, cashexf, actbrno, phybrno, subcode,
                cino, busidate, busitime, trxcode, workdate, valueday,
                closdate, drcrf, vouhno, amount, balance, closint, keepint,
                inttax, summflag, cashnote, savterm, rate, matudate, zoneno,
                brno, tellerno, trxsqno, servface, authono, authtlno, termid,
                timestmp, recipact, mdcardno, acserno, synflag, showflag,
                macardn, recipcn, catrflag, sellercode, amount1, amount2,
                flag1, date1, zoneaccno, ID
           FROM hdm_s_pfepvmdl_query_list
          WHERE ID IN (SELECT ID
                         FROM hdm_query_return
                        WHERE table_name = 'HDM_S_PFEPVMDL_QUERY_LIST');

      DELETE FROM hdm_s_pfepvmdl_query_list
            WHERE ID IN (SELECT ID
                           FROM hdm_query_return
                          WHERE table_name = 'HDM_S_PFEPVMDL_QUERY_LIST');

      v_step := '16';

      INSERT INTO hdm_s_pfepvmdl_return_his
                  (accno, serialno, currtype, cashexf, actbrno, phybrno,
                   subcode, cino, busidate, busitime, trxcode, workdate,
                   valueday, closdate, drcrf, vouhno, amount, balance,
                   closint, keepint, inttax, summflag, cashnote, savterm,
                   rate, matudate, zoneno, brno, tellerno, trxsqno, servface,
                   authono, authtlno, termid, timestmp, recipact, mdcardno,
                   acserno, synflag, showflag, macardn, recipcn, catrflag,
                   sellercode, amount1, amount2, flag1, date1,
                   zoneaccno, ID, query_flag)
         SELECT accno, serialno, currtype, cashexf, actbrno, phybrno, subcode,
                cino, busidate, busitime, trxcode, workdate, valueday,
                closdate, drcrf, vouhno, amount, balance, closint, keepint,
                inttax, summflag, cashnote, savterm, rate, matudate, zoneno,
                brno, tellerno, trxsqno, servface, authono, authtlno, termid,
                timestmp, recipact, mdcardno, acserno, synflag, showflag,
                macardn, recipcn, catrflag, sellercode, amount1, amount2,
                flag1, date1, zoneaccno, ID, query_flag
           FROM hdm_s_pfepvmdl_return;

      /*将return表清空*/
      DELETE FROM hdm_s_pfepvmdl_return;

      v_step := '26';

      DELETE FROM hdm_s_pfepvmdl_return_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      v_step := '27';

      DELETE FROM hdm_s_pfepvmdl_list_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      DELETE FROM hdm_s_pfepvmdl_query_list
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_upour_export_pfepvmdl;

/******************************************************************************
   --存储过程名称：  hdm_upour_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_upour_export_pfepotdl (
      p_i_date      IN       VARCHAR2,                       --(传入参数 日期)
      p_i_no        IN       VARCHAR2,          --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2        --(传出参数 成功返回0，失败返回1)
   )
   AS
      v_step            VARCHAR2 (3)                        := '0';
      --步骤号
      v_proc_err_code   VARCHAR2 (200)                      := NULL;
      --错误代码
      v_proc_err_txt    VARCHAR2 (200)                      := NULL;
      --错误信息
      v_proc_name       VARCHAR2 (100)
             := 'hdm_upour_export.hdm_upour_export_PFEPOTDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_pfepotdl (v_id VARCHAR2)
      IS
         SELECT busidate, NVL (c.prmname, a.summflag) summflag,
                NVL (d.prmname, a.currtype) currtype,
                NVL (e.prmname, a.cashexf) cashexf,
                NVL (f.prmname, a.drcrf) drcrf, amount, balance,
                NVL (k.prmname, a.servface) servface, tellerno,
                NVL (h.notes, a.brno) brno, NVL (b.notes, a.zoneno) zoneno,
                cashnote, NVL (g.prmname, a.trxcode) trxcode, workdate,
                authtlno, recipact, phybrno, termid, ID
           FROM (SELECT busidate, summflag, currtype, cashexf, drcrf, amount,
                        balance, servface, tellerno, brno, zoneno, cashnote,
                        trxcode, workdate, authtlno, recipact, phybrno,
                        termid, ID,timestmp
                   FROM hdm_s_pfepotdl_return
                  WHERE ID = v_id AND query_flag <> '1'
                 UNION ALL
                 SELECT busidate, summflag, currtype, cashexf, drcrf, amount,
                        balance, servface, tellerno, brno, zoneno, cashnote,
                        trxcode, workdate, authtlno, recipact, phybrno,
                        termid, ID,timestmp
                   FROM hdm_s_pfepotdl_query_list
                  WHERE ID = v_id) a,
                hdm_nthpazon_s_dat b,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'SUMMFLAG'
                    AND prmeffflag = '1') c,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'CURRTYPE'
                    AND prmeffflag = '1') d,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'CASHEXF'
                    AND prmeffflag = '1') e,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'DRCRF'
                    AND prmeffflag = '1') f,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'TRXCODE'
                    AND prmeffflag = '1') g,
                hdm_nthcabrp_s_dat h,
                (SELECT prmcode, prmname
                   FROM hdm_code_prm
                  WHERE UPPER (app_name) = 'PRAS'
                    AND UPPER (prmfieldname) = 'SERVFACE'
                    AND prmeffflag = '1') k
          WHERE a.zoneno = b.zoneno(+)
            AND a.summflag = c.prmcode(+)
            AND a.currtype = d.prmcode(+)
            AND a.cashexf = e.prmcode(+)
            AND a.drcrf = f.prmcode(+)
            AND a.trxcode = g.prmcode(+)
            AND a.brno = h.brno(+)
            AND a.zoneno = h.zoneno(+)
            AND a.servface = k.prmcode(+)
          ORDER BY a.busidate, a.timestmp;

      rec_cur           export_pfepotdl%ROWTYPE             := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
      v_this_id_all     NUMBER                              := NULL;
      v_max_count       NUMBER                              := NULL;
      v_instance        VARCHAR2 (100)                      := 'hdmpras';
      v_file_all        NUMBER                              := NULL;
      v_count_each      NUMBER                              := NULL;
      in_file           VARCHAR2 (32767)                    := NULL;
      v_file            UTL_FILE.file_type                  := NULL;
      v_seq             NUMBER                              := NULL;

      TYPE t_rec_2 IS RECORD (
         v_id   VARCHAR2 (20)
      );                                      --定义记录变量存放担保段初始数据

      TYPE tt_rec_2 IS TABLE OF t_rec_2;

      --记录本次正常报文里有的结算应还款年月
      rec_2             tt_rec_2                            := NULL;
      v_count_id        VARCHAR2 (20)                       := NULL;
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         hdm.pckg_hdm_public_util.func_get_log_status (p_i_date,
                                                       v_proc_name,
                                                       v_min_zoneno,
                                                       v_max_zoneno
                                                      );

      IF v_status = 1                                          -- 已成功运行过
      THEN
         p_o_succeed := '0';
         COMMIT;
         RETURN;
      END IF;

      v_step := '0';
      --任务日志表中记录开始运行状态
      hdm.pckg_hdm_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2';

      SELECT max_count
        INTO v_max_count
        FROM hdm_email_maxcount
       WHERE table_name = 'HDM_S_PFEPOTDL_QUERY';

      v_step := '3';
      v_step := '4';

      SELECT   ID
      BULK COLLECT INTO rec_2
          FROM hdm_query_return
         WHERE table_name = 'HDM_S_PFEPOTDL_QUERY_LIST'
      GROUP BY ID
      ORDER BY ID;

      IF rec_2 IS NOT NULL
      THEN
         v_step := '5';
         v_count_id := rec_2.COUNT;

         IF v_count_id > 0
         THEN
            FOR i IN 1 .. v_count_id
            LOOP
               v_step := '6';

               OPEN export_pfepotdl (rec_2 (i).v_id);

               --LOOP
                  --v_step := '7';

               --FETCH export_pfepotdl
                --INTO rec_cur;
               v_step := '8';

               SELECT COUNT (1)
                 INTO v_this_id_all
                 FROM (SELECT accno
                         FROM hdm_s_pfepotdl_return
                        WHERE ID = rec_2 (i).v_id AND query_flag <> '1'
                       UNION ALL
                       SELECT accno
                         FROM hdm_s_pfepotdl_query_list
                        WHERE ID = rec_2 (i).v_id);

               IF v_this_id_all > 0
               THEN
                  v_file_all := CEIL (v_this_id_all / v_max_count);
               ELSE
                  v_file_all := 1;
               END IF;

               FOR j IN 1 .. v_file_all
               LOOP
                  IF j = v_file_all
                  THEN
                     v_count_each :=
                                v_this_id_all
                                - (v_file_all - 1) * v_max_count;
                  ELSE
                     v_count_each := v_max_count;
                  END IF;

                  IF v_this_id_all = 0
                  THEN
                     v_count_each := 0;
                  END IF;

                  v_step := '9';
                  in_file :=
                     rec_2 (i).v_id || '_' || v_instance || '_' || j || '.csv';
                  v_step := '10';
                  v_outputfile :=
                      UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
                  v_step := '11';
                  UTL_FILE.put_line
                     (v_outputfile,
                      'SEQ序列号,BUSIDATE入帐日期,SUMMFLAG零售存取款摘要,CURRTYPE币种,CASHEXF钞汇标志,DRCRF借贷标志,AMOUNT发生额,BALANCE余额,SERVFACE服务界面,TELLERNO柜员号,BRNO交易网点号,ZONENO交易地区号,CASHNOTE注释,TRXCODE交易代码,WORKDATE工作日期,AUTHTLNO授权柜员号,RECIPACT对方帐户客户协,PHYBRNO帐户物理网点号,TERMID终端号',
                      FALSE
                     );                              -- 将报文头写入文件缓存区
                  UTL_FILE.fflush (v_outputfile);

                  IF v_count_each > 0
                  THEN
                     FOR k IN 1 .. v_count_each
                     LOOP
                        v_step := '12';
                        v_seq := (j - 1) * v_max_count + k;
                        v_step := '13';

                        FETCH export_pfepotdl
                         INTO rec_cur;

                        UTL_FILE.put_line (v_outputfile,
                                              CHR (9)
                                           || v_seq
                                           || ','
                                           || CHR (9)
                                           || rec_cur.busidate
                                           || ','
                                           || CHR (9)
                                           || rec_cur.summflag
                                           || ','
                                           || CHR (9)
                                           || rec_cur.currtype
                                           || ','
                                           || CHR (9)
                                           || rec_cur.cashexf
                                           || ','
                                           || CHR (9)
                                           || rec_cur.amount / 100
                                           || ','
                                           || CHR (9)
                                           || rec_cur.balance / 100
                                           || ','
                                           || CHR (9)
                                           || rec_cur.servface
                                           || ','
                                           || CHR (9)
                                           || rec_cur.tellerno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.brno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.zoneno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.cashnote
                                           || ','
                                           || CHR (9)
                                           || rec_cur.trxcode
                                           || ','
                                           || CHR (9)
                                           || rec_cur.workdate
                                           || ','
                                           || CHR (9)
                                           || rec_cur.authtlno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.recipact
                                           || ','
                                           || CHR (9)
                                           || rec_cur.phybrno
                                           || ','
                                           || CHR (9)
                                           || rec_cur.termid,
                                           FALSE
                                          );         -- 将报文头写入文件缓存区
                        UTL_FILE.fflush (v_outputfile);
                        EXIT WHEN export_pfepotdl%NOTFOUND;
                     END LOOP;
                  END IF;

                  UTL_FILE.fclose (v_outputfile);
               END LOOP;

                  --EXIT WHEN export_pfepotdl%NOTFOUND;
               --END LOOP;
               CLOSE export_pfepotdl;

               v_step := '14';

               UPDATE hdm_query_log_dat
                  SET qry_status = '5',
                      attachment_count = NVL (v_file_all, 0)
                WHERE qry_id = rec_2 (i).v_id;

               /*UPDATE hdm_query_date_split
                  SET c_flag = '1'
                WHERE query_id = rec_2 (i).v_id;*/

               /*更新异步查询导出状态*/
               COMMIT;
            END LOOP;
         END IF;
      END IF;

      v_step := '15';

      INSERT INTO hdm_s_pfepotdl_list_his
                  (mdcardno, serialno, acappno, acserno, accno, currtype,
                   cashexf, openzone, actbrno, phybrno, cino, busidate,
                   busitime, trxcode, workdate, valueday, closdate, drcrf,
                   amount, balance, backbal, summflag, cashnote, drwterm,
                   finano, fseqno, fincode, matudate, fintype, zoneno, brno,
                   tellerno, trxsqno, servface, authono, authtlno, termid,
                   timestmp, recipact, synflag, showflag, macardn, recipcn,
                   catrflag, sellercode, amount1, amount2, flag1, date1,
                   date2,  zoneaccno, ID)
         SELECT mdcardno, serialno, acappno, acserno, accno, currtype,
                cashexf, openzone, actbrno, phybrno, cino, busidate, busitime,
                trxcode, workdate, valueday, closdate, drcrf, amount, balance,
                backbal, summflag, cashnote, drwterm, finano, fseqno, fincode,
                matudate, fintype, zoneno, brno, tellerno, trxsqno, servface,
                authono, authtlno, termid, timestmp, recipact, synflag,
                showflag, macardn, recipcn, catrflag, sellercode, amount1,
                amount2, flag1, date1, date2,  zoneaccno, ID
           FROM hdm_s_pfepotdl_query_list
          WHERE ID IN (SELECT ID
                         FROM hdm_query_return
                        WHERE table_name = 'HDM_S_PFEPOTDL_QUERY_LIST');

      /*将已经导出的数据在LIST表里清除*/
      DELETE FROM hdm_s_pfepotdl_query_list
            WHERE ID IN (SELECT ID
                           FROM hdm_query_return
                          WHERE table_name = 'HDM_S_PFEPOTDL_QUERY_LIST');

      v_step := '16';

      INSERT INTO hdm_s_pfepotdl_return_his
                  (mdcardno, serialno, acappno, acserno, accno, currtype,
                   cashexf, openzone, actbrno, phybrno, cino, busidate,
                   busitime, trxcode, workdate, valueday, closdate, drcrf,
                   amount, balance, backbal, summflag, cashnote, drwterm,
                   finano, fseqno, fincode, matudate, fintype, zoneno, brno,
                   tellerno, trxsqno, servface, authono, authtlno, termid,
                   timestmp, recipact, synflag, showflag, macardn, recipcn,
                   catrflag, sellercode, amount1, amount2, flag1, date1,
                   date2,  zoneaccno, ID, query_flag)
         SELECT mdcardno, serialno, acappno, acserno, accno, currtype,
                cashexf, openzone, actbrno, phybrno, cino, busidate, busitime,
                trxcode, workdate, valueday, closdate, drcrf, amount, balance,
                backbal, summflag, cashnote, drwterm, finano, fseqno, fincode,
                matudate, fintype, zoneno, brno, tellerno, trxsqno, servface,
                authono, authtlno, termid, timestmp, recipact, synflag,
                showflag, macardn, recipcn, catrflag, sellercode, amount1,
                amount2, flag1, date1, date2,  zoneaccno, ID,
                query_flag
           FROM hdm_s_pfepotdl_return;

      /*将return表清空*/
      DELETE FROM hdm_s_pfepotdl_return;

      v_step := '26';

      DELETE FROM hdm_s_pfepotdl_return_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      v_step := '27';

      DELETE FROM hdm_s_pfepotdl_list_his
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      DELETE FROM hdm_s_pfepotdl_query_list
            WHERE SUBSTR (ID, 1, 8) <
                     TO_CHAR (ADD_MONTHS (LAST_DAY (TO_DATE (p_i_date,
                                                             'YYYYMMDD'
                                                            )
                                                   ),
                                          -6
                                         ),
                              'YYYYMMDD'
                             );

      COMMIT;
      p_o_succeed := '0';
      --任务日志表中记录成功完成状态
      pckg_hdm_public_util.proc_writelog (p_i_date,
                                          v_proc_name,
                                          v_min_zoneno,
                                          v_max_zoneno,
                                          1,
                                          '任务完成',
                                          2
                                         );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_proc_err_code := SQLCODE;
         v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
         p_o_succeed := '1';
         --任务日志表中记录出错状态
         pckg_hdm_public_util.proc_writelog (p_i_date,
                                             v_proc_name,
                                             v_min_zoneno,
                                             v_max_zoneno,
                                             99,
                                                '任务出错! '
                                             || '出错步骤号为: '
                                             || v_step
                                             || ' || 错误码为: '
                                             || v_proc_err_code
                                             || ' || 错误信息为: '
                                             || v_proc_err_txt,
                                             2
                                            );
         RETURN;
   END hdm_upour_export_pfepotdl;
END hdm_upour_export;
//
delimiter ;//