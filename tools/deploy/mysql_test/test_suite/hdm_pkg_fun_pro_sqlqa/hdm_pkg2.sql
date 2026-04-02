CREATE TABLE HDM_S_NFQCLINF_QUERY_DAT( FINFO VARCHAR2(20),
                           BUSTYPNO VARCHAR2(20),
                           BAKCHAR3 VARCHAR2(20),
                            agaccno VARCHAR2(20),
                            ZONEACCNO VARCHAR2(20),
                           busidate VARCHAR2(20),
                            SERIALNO VARCHAR2(20));

CREATE TABLE "HDM_DATASOURCE_CONFIG_DAT" (
	"BUSINESS_TYPE" VARCHAR2(2 BYTE),
	"INSTANCE_YEAR" VARCHAR2(30 BYTE),
	"INSTANCE_ID" VARCHAR2(10 BYTE),
	"DATA_DATE" VARCHAR2(10 BYTE),
	"UPDATE_DATE" TIMESTAMP(6),
	"INSTANCE_NAME" VARCHAR2(30 BYTE),
	"EXT_FIELD" VARCHAR2(30 BYTE)
);

CREATE TABLE "HDM_LOG_TASK_DETAIL" (
	"D_TASK_DATE" VARCHAR2(8 BYTE) NOT NULL,
	"C_PROC_NAME" VARCHAR2(400 BYTE) NOT NULL,
	"C_MIN_ZONENO" VARCHAR2(10 BYTE) NOT NULL,
	"C_MAX_ZONENO" VARCHAR2(10 BYTE) NOT NULL,
	"D_START_TIME" TIMESTAMP(6),
	"D_END_TIME" TIMESTAMP(6),
	"N_STATUS" NUMBER(2,0),
	"C_MESSAGE" VARCHAR2(4000 BYTE)
);

create table HDM_QUERY_LOG_DAT_3
(
  eserialno       VARCHAR2(20),
  agaccno         VARCHAR2(20),
  seqno           VARCHAR2(20),
  query_date_to   VARCHAR2(20),
  acctseqname     VARCHAR2(20),
  corpnmc         VARCHAR2(20),
  bankname        VARCHAR2(20),
  group_id        VARCHAR2(20),
  request_date    VARCHAR2(20),
  request_type    VARCHAR2(20),
  qry_order       VARCHAR2(20),
  qry_where       VARCHAR2(20),
  query_date_from VARCHAR2(20),
  status          VARCHAR2(20),
  exec_start_time VARCHAR2(20),
  exec_end_time   VARCHAR2(20)
);

create table HDM_CMP_GROUP_DAT
(
  trade_id     VARCHAR2(20),
  accno        VARCHAR2(20),
  seqno        VARCHAR2(20),
  query_date   VARCHAR2(20),
  acctseqname  VARCHAR2(20),
  corpnmc      VARCHAR2(20),
  bankname     VARCHAR2(20),
  group_id     VARCHAR2(20),
  request_date VARCHAR2(20),
  request_type VARCHAR2(20),
  status       VARCHAR2(20)
);

create table HDM_S_NFDGPSUB_QUERY_DAT
(
  tagaccno VARCHAR2(20),
  gpagacc  VARCHAR2(20),
  seqno    VARCHAR2(20)
);

create table HDM_S_NFQCLINF_QUERY
(
  finfo     VARCHAR2(20),
  bustypno  VARCHAR2(20),
  agaccno   VARCHAR2(20),
  zoneaccno VARCHAR2(20),
  busidate  VARCHAR2(20),
  serialno  VARCHAR2(20),
  timestmp  VARCHAR2(20)
);

create table HDM_S_NBWKFAR_QUERY_DAT
(
  ensummry  VARCHAR2(20),
  cmpchgf   VARCHAR2(20),
  zoneaccno VARCHAR2(20),
  accno     VARCHAR2(20),
  busidate  VARCHAR2(20),
  busitime  VARCHAR2(20),
  drcrf     VARCHAR2(20),
  amount    VARCHAR2(20),
  vouhtype  VARCHAR2(20),
  vouhno    VARCHAR2(20),
  trxsqnb   VARCHAR2(20),
  trxsqns   VARCHAR2(20),
  brno      VARCHAR2(20),
  tellerno  VARCHAR2(20),
  trxcode   VARCHAR2(20)
);

create table HDM_S_NFQCHDTL_QUERY
(
  drcrf     VARCHAR2(20),
  recipacc  VARCHAR2(20),
  recipnam  VARCHAR2(20),
  recipbna  VARCHAR2(20),
  amount    VARCHAR2(20),
  currtype  VARCHAR2(20),
  summary   VARCHAR2(20),
  opertype  VARCHAR2(20),
  purpose   VARCHAR2(20),
  eventseq  VARCHAR2(20),
  ptrxseq   VARCHAR2(20),
  timestmp  VARCHAR2(20),
  notes     VARCHAR2(20),
  torganno  VARCHAR2(20),
  tellerno  VARCHAR2(20),
  busidate  VARCHAR2(20),
  neagnflg  VARCHAR2(20),
  otactno   VARCHAR2(20),
  otactnam  VARCHAR2(20),
  updtranf  VARCHAR2(20),
  valueday  VARCHAR2(20),
  bakchar   VARCHAR2(20),
  serialno  VARCHAR2(20),
  vouhno    VARCHAR2(20),
  vouhtype  VARCHAR2(20),
  busitime  VARCHAR2(20),
  accno     VARCHAR2(20),
  trxcode   VARCHAR2(20),
  recipac2  VARCHAR2(20),
  recipna2  VARCHAR2(20),
  ref       VARCHAR2(20),
  oref      VARCHAR2(20),
  cardno    VARCHAR2(20),
  agaccno   VARCHAR2(20),
  zoneaccno VARCHAR2(20)
);

create table HDM_QUERY_LOG_DAT_3_HIS
(
  eserialno       VARCHAR2(20),
  agaccno         VARCHAR2(20),
  seqno           VARCHAR2(20),
  query_date_to   VARCHAR2(20),
  acctseqname     VARCHAR2(20),
  corpnmc         VARCHAR2(20),
  bankname        VARCHAR2(20),
  group_id        VARCHAR2(20),
  request_date    VARCHAR2(20),
  request_type    VARCHAR2(20),
  qry_order       VARCHAR2(20),
  qry_where       VARCHAR2(20),
  query_date_from VARCHAR2(20),
  status          VARCHAR2(20),
  exec_start_time VARCHAR2(20),
  exec_end_time   VARCHAR2(20)
);

create table HDM_S_NFQCHDTL_QUERY_DAT(AGACCNO VARCHAR2(20),
                           ZONENO VARCHAR2(20),
                           TORGANNO VARCHAR2(20),
                           TRXCODE VARCHAR2(20),
                           BUSIDATE VARCHAR2(20),
                           BUSITIME VARCHAR2(20),
                           TIMESTMP VARCHAR2(20),
                           DRCRF VARCHAR2(20),
                           CASHF VARCHAR2(20),
                           UPDTRANF VARCHAR2(20),
                           AMOUNT VARCHAR2(20),
                           BALANCE VARCHAR2(20),
                           BAKDEC VARCHAR2(20),
                           CURRTYPE VARCHAR2(20),
                           OPERTYPE VARCHAR2(20),
                           VOUHTYPE VARCHAR2(20),
                           VOUHNO VARCHAR2(20),
                           SUMMARY VARCHAR2(20),
                           PURPOSE VARCHAR2(20),
                           NOTES VARCHAR2(20),
                           SUBACCNO VARCHAR2(20),
                           RECIPACC  VARCHAR2(20),
                           RECIPNAM  VARCHAR2(20),
                           NEAGNFLG VARCHAR2(20),
                           OTACTNO  VARCHAR2(20),
                           OTACTNAM  VARCHAR2(20),
                           RECIPBKN VARCHAR2(20),
                           RECIPBNA VARCHAR2(20),
                           TELLERNO VARCHAR2(20),
                           EVENTSEQ VARCHAR2(20),
                           PTRXSEQ VARCHAR2(20),
                           REF VARCHAR2(20),
                           OREF VARCHAR2(20),
                           SERIALNO VARCHAR2(20),
                           RECIPAC2 VARCHAR2(20),
                           RECIPNA2 VARCHAR2(20),
                           CARDNO VARCHAR2(20),
                           DETAILF VARCHAR2(20),
                           VALUEDAY VARCHAR2(20),
                           ZONEACCNO VARCHAR2(20)
                             );

create table AGT_PFAGPRAS
(
  paydate  VARCHAR2(20),
  accno    VARCHAR2(20),
  amount1  VARCHAR2(20),
  drcrf    VARCHAR2(20),
  ageaccno VARCHAR2(20),
  inmsg    VARCHAR2(20),
  agtname  VARCHAR2(20),
  status   VARCHAR2(20),
  recordf  VARCHAR2(20),
  agentno  VARCHAR2(20)
);
create table AGT_PFAGPRAS_NEW
(
  paydate  VARCHAR2(20),
  accno    VARCHAR2(20),
  amount1  VARCHAR2(20),
  drcrf    VARCHAR2(20),
  ageaccno VARCHAR2(20),
  inmsg    VARCHAR2(20),
  agtname  VARCHAR2(20),
  status   VARCHAR2(20),
  recordf  VARCHAR2(20),
  agentno  VARCHAR2(20)
);
create table BCAS_BTHDRLTD
(
  rmbacc   VARCHAR2(20),
  mdcardno VARCHAR2(20)
);

create table BCAS_BTHDRSYN
(
  savname  VARCHAR2(20),
  mdcardno VARCHAR2(20)
);

create table BCAS_PFHCDMDU
(
  cino     VARCHAR2(20),
  mdcardno VARCHAR2(20),
  savname  VARCHAR2(20)
);

create table BCA_S_BFHINPOS
(
  zoneno     VARCHAR2(5),
  brno       VARCHAR2(5),
  cenbrno    VARCHAR2(5),
  tellerno   VARCHAR2(5),
  workdate   VARCHAR2(10),
  carddeno   VARCHAR2(19),
  carddety   VARCHAR2(3),
  dloref     VARCHAR2(1),
  dsubcode   VARCHAR2(7),
  drcurtyp   VARCHAR2(3),
  draccno    VARCHAR2(17),
  draccseqno VARCHAR2(9),
  cardcrno   VARCHAR2(19),
  cardcrty   VARCHAR2(3),
  cloref     VARCHAR2(1),
  csubcode   VARCHAR2(7),
  crcurtyp   VARCHAR2(3),
  craccno    VARCHAR2(17),
  craccseqno VARCHAR2(9),
  trxcode    VARCHAR2(5),
  consamt    NUMBER(18),
  feeamt     NUMBER(18),
  cfeeamt    NUMBER(18),
  tipamt     NUMBER(18),
  tiprat     VARCHAR2(5),
  parinamt   NUMBER(18),
  parinrat   VARCHAR2(17),
  trxserno   VARCHAR2(23),
  streamno   VARCHAR2(9),
  posno      VARCHAR2(15),
  recbthno   VARCHAR2(7),
  unknownc   VARCHAR2(1),
  autno      VARCHAR2(6),
  tydwname   VARCHAR2(60),
  tydwno     VARCHAR2(13),
  tydwbmno   VARCHAR2(3),
  tydwacc    VARCHAR2(34),
  tydwaccty  VARCHAR2(1),
  summary    VARCHAR2(1),
  openunit   VARCHAR2(40),
  openbrno   VARCHAR2(5),
  actbrno    VARCHAR2(5),
  caccbrno   VARCHAR2(5),
  ortrxamt   NUMBER(18),
  discntamt  NUMBER(18),
  cashin     NUMBER(18),
  bonusin    NUMBER(18),
  bonusout   NUMBER(18),
  trxamt     NUMBER(18),
  emailadd   VARCHAR2(50),
  worktime   VARCHAR2(8),
  incamt     NUMBER(18),
  bankcode   VARCHAR2(14),
  backup1    VARCHAR2(100),
  backup2    VARCHAR2(200),
  trxtype    VARCHAR2(3),
  busitype   VARCHAR2(3),
  merinamt   NUMBER(18),
  mhdinamt   NUMBER(18),
  amount1    NUMBER(18),
  amount2    NUMBER(18),
  amount3    NUMBER(18),
  amount4    NUMBER(18),
  amount5    NUMBER(18),
  amount6    NUMBER(18),
  amount7    NUMBER(18),
  amount8    NUMBER(18),
  amount9    NUMBER(18),
  amount10   NUMBER(18),
  trxzoneno  VARCHAR2(5),
  trxbrno    VARCHAR2(5),
  unitno     VARCHAR2(15),
  busidate   VARCHAR2(10),
  batchdate  VARCHAR2(10)
);

create table BDM_EV_AGTSAL
(
  workdate   VARCHAR2(8),
  trxtype    VARCHAR2(1),
  payacc     VARCHAR2(30),
  paycino    VARCHAR2(30),
  recacc     VARCHAR2(30),
  reccino    VARCHAR2(30),
  amount     VARCHAR2(20),
  paybrno    VARCHAR2(5),
  servface   VARCHAR2(2),
  tellerno   VARCHAR2(2),
  datasource VARCHAR2(50)
);

create table FBEC_TRANSACTION
(
  hostdate   VARCHAR2(20),
  hosttime   VARCHAR2(20),
  tellerno   VARCHAR2(20),
  trxsqnb    VARCHAR2(20),
  drcardno   VARCHAR2(20),
  amount     VARCHAR2(20),
  currrype   VARCHAR2(20),
  drcrf      VARCHAR2(20),
  crcardno   VARCHAR2(20),
  crname     VARCHAR2(20),
  feeamount  VARCHAR2(20),
  changsuo   VARCHAR2(20),
  succflag   VARCHAR2(20),
  apptrxcode VARCHAR2(20),
  drname     VARCHAR2(20)
);

create table CMP_GROUP_DAT
(
  trade_id     VARCHAR2(20),
  accno        VARCHAR2(20),
  seqno        VARCHAR2(20),
  query_date   VARCHAR2(20),
  acctseqname  VARCHAR2(20),
  corpnmc      VARCHAR2(20),
  bankname     VARCHAR2(20),
  group_id     VARCHAR2(20),
  request_date VARCHAR2(20),
  request_type VARCHAR2(20),
  status       VARCHAR2(20)
);

create table DATASOURCE_CONFIG_DAT
(
  business_type VARCHAR2(2),
  instance_year VARCHAR2(30),
  instance_id   VARCHAR2(10),
  data_date     VARCHAR2(10),
  update_date   TIMESTAMP(6),
  instance_name VARCHAR2(30),
  ext_field     VARCHAR2(30)
);

create table LOG_TASK_DETAIL
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

create table QUERY_LOG_DAT_3
(
  eserialno       VARCHAR2(20),
  agaccno         VARCHAR2(20),
  seqno           VARCHAR2(20),
  query_date_to   VARCHAR2(20),
  acctseqname     VARCHAR2(20),
  corpnmc         VARCHAR2(20),
  bankname        VARCHAR2(20),
  group_id        VARCHAR2(20),
  request_date    VARCHAR2(20),
  request_type    VARCHAR2(20),
  qry_order       VARCHAR2(20),
  qry_where       VARCHAR2(20),
  query_date_from VARCHAR2(20),
  status          VARCHAR2(20),
  exec_start_time VARCHAR2(20),
  exec_end_time   VARCHAR2(20)
);

create table QUERY_LOG_DAT_3_HIS
(
  eserialno       VARCHAR2(20),
  agaccno         VARCHAR2(20),
  seqno           VARCHAR2(20),
  query_date_to   VARCHAR2(20),
  acctseqname     VARCHAR2(20),
  corpnmc         VARCHAR2(20),
  bankname        VARCHAR2(20),
  group_id        VARCHAR2(20),
  request_date    VARCHAR2(20),
  request_type    VARCHAR2(20),
  qry_order       VARCHAR2(20),
  qry_where       VARCHAR2(20),
  query_date_from VARCHAR2(20),
  status          VARCHAR2(20),
  exec_start_time VARCHAR2(20),
  exec_end_time   VARCHAR2(20)
);

create table S_NBWKFAR_QUERY_DAT
(
  ensummry  VARCHAR2(20),
  cmpchgf   VARCHAR2(20),
  zoneaccno VARCHAR2(20),
  accno     VARCHAR2(20),
  busidate  VARCHAR2(20),
  busitime  VARCHAR2(20),
  drcrf     VARCHAR2(20),
  amount    VARCHAR2(20),
  vouhtype  VARCHAR2(20),
  vouhno    VARCHAR2(20),
  trxsqnb   VARCHAR2(20),
  trxsqns   VARCHAR2(20),
  brno      VARCHAR2(20),
  tellerno  VARCHAR2(20),
  trxcode   VARCHAR2(20)
);

create table S_NFDGPSUB_QUERY_DAT
(
  tagaccno VARCHAR2(20),
  gpagacc  VARCHAR2(20),
  seqno    VARCHAR2(20)
);

create table S_NFQCHDTL_QUERY
(
  drcrf     VARCHAR2(20),
  recipacc  VARCHAR2(20),
  recipnam  VARCHAR2(20),
  recipbna  VARCHAR2(20),
  amount    VARCHAR2(20),
  currtype  VARCHAR2(20),
  summary   VARCHAR2(20),
  opertype  VARCHAR2(20),
  purpose   VARCHAR2(20),
  eventseq  VARCHAR2(20),
  ptrxseq   VARCHAR2(20),
  timestmp  VARCHAR2(20),
  notes     VARCHAR2(20),
  torganno  VARCHAR2(20),
  tellerno  VARCHAR2(20),
  busidate  VARCHAR2(20),
  neagnflg  VARCHAR2(20),
  otactno   VARCHAR2(20),
  otactnam  VARCHAR2(20),
  updtranf  VARCHAR2(20),
  valueday  VARCHAR2(20),
  bakchar   VARCHAR2(20),
  serialno  VARCHAR2(20),
  vouhno    VARCHAR2(20),
  vouhtype  VARCHAR2(20),
  busitime  VARCHAR2(20),
  accno     VARCHAR2(20),
  trxcode   VARCHAR2(20),
  recipac2  VARCHAR2(20),
  recipna2  VARCHAR2(20),
  ref       VARCHAR2(20),
  oref      VARCHAR2(20),
  cardno    VARCHAR2(20),
  agaccno   VARCHAR2(20),
  zoneaccno VARCHAR2(20)
);

create table S_NFQCLINF_QUERY
(
  finfo     VARCHAR2(20),
  bustypno  VARCHAR2(20),
  agaccno   VARCHAR2(20),
  zoneaccno VARCHAR2(20),
  busidate  VARCHAR2(20),
  serialno  VARCHAR2(20),
  timestmp  VARCHAR2(20)
);

create table IBPS_RCVLOG
(
  rcvdate     DATE,
  worktime    VARCHAR2(20),
  rcvacct     VARCHAR2(20),
  amt         VARCHAR2(20),
  cursign     VARCHAR2(20),
  rcvoperno   VARCHAR2(20),
  sndbnkcode  VARCHAR2(20),
  sndbnkname  VARCHAR2(20),
  payacct     VARCHAR2(20),
  payname     VARCHAR2(20),
  strinfo     VARCHAR2(20),
  hostserbno1 VARCHAR2(20),
  hostsersno1 VARCHAR2(20),
  hdlflag     VARCHAR2(20)
);

create table IBPS_SNDLOG
(
  snddate     VARCHAR2(20),
  worktime    VARCHAR2(20),
  payacct     VARCHAR2(20),
  amt         VARCHAR2(20),
  cursign     VARCHAR2(20),
  entoperno   VARCHAR2(20),
  hostserbno1 VARCHAR2(20),
  rcvbnkcode  VARCHAR2(20),
  rcvbnkname  VARCHAR2(20),
  rcvacct     VARCHAR2(20),
  rcvname     VARCHAR2(20),
  strinfo     VARCHAR2(20),
  abstract    VARCHAR2(20),
  hostsersno1 VARCHAR2(20),
  hdlflag     VARCHAR2(20)
);
create table NASE_NTHCHSUB
(
  accname VARCHAR2(20),
  accno   VARCHAR2(20),
  phybrno VARCHAR2(20),
  cino    VARCHAR2(20)
);
create table NASE_NTHINSUB
(
  accname VARCHAR2(20),
  accno   VARCHAR2(20),
  subcode VARCHAR2(20)
);
create table NASE_NTHPAAGT
(
  accno    VARCHAR2(20),
  amount1  VARCHAR2(20),
  drcrf    VARCHAR2(20),
  ageaccno VARCHAR2(20),
  agesname VARCHAR2(20),
  agtsmnm  VARCHAR2(20),
  inmsg    VARCHAR2(20),
  status   VARCHAR2(20),
  recordf  VARCHAR2(20),
  agentno  VARCHAR2(20)
);

create table NASE_NTHPAZON_ALL
(
  notes  VARCHAR2(20),
  zoneno VARCHAR2(20)
);

create table NCPS_BANKINFO
(
  bank   VARCHAR2(20),
  bankno VARCHAR2(20)
);

create table NCPS_RCVDTL
(
  busidate  VARCHAR2(20),
  timestamp VARCHAR2(20),
  raccno    VARCHAR2(20),
  amount    VARCHAR2(20),
  currtype  VARCHAR2(20),
  teller    VARCHAR2(20),
  pbnkno    VARCHAR2(20),
  pbnkname  VARCHAR2(20),
  paccno    VARCHAR2(20),
  pname     VARCHAR2(20),
  notes     VARCHAR2(20),
  trxsqnob  VARCHAR2(20),
  trxsqnos  VARCHAR2(20),
  step      VARCHAR2(20),
  fee       NUMBER
);


create table NCPS_RCVDTL_BAK
(
  busidate  VARCHAR2(20),
  timestamp VARCHAR2(20),
  raccno    VARCHAR2(20),
  amount    VARCHAR2(20),
  currtype  VARCHAR2(20),
  teller    VARCHAR2(20),
  pbnkno    VARCHAR2(20),
  pbnkname  VARCHAR2(20),
  paccno    VARCHAR2(20),
  pname     VARCHAR2(20),
  notes     VARCHAR2(20),
  trxsqnob  VARCHAR2(20),
  trxsqnos  VARCHAR2(20),
  step      VARCHAR2(20)
);


create table NCPS_RECALLDTL
(
  busidate  VARCHAR2(20),
  timestamp VARCHAR2(20),
  opaccno   VARCHAR2(20),
  amount    VARCHAR2(20),
  currtype  VARCHAR2(20),
  trxtel    VARCHAR2(20),
  trxsqnob  VARCHAR2(20),
  drcrf     VARCHAR2(20),
  orbnkno   VARCHAR2(20),
  oraccno   VARCHAR2(20),
  orname    VARCHAR2(20),
  notes     VARCHAR2(20),
  trxsqnos  VARCHAR2(20),
  step      VARCHAR2(20)
);

create table NCPS_SNDDTL
(
  workdate  VARCHAR2(20),
  timestamp VARCHAR2(20),
  paccno    VARCHAR2(20),
  amount    NUMBER,
  currtype  VARCHAR2(20),
  teller    VARCHAR2(20),
  trxsqnob  VARCHAR2(20),
  rbnkno    VARCHAR2(20),
  rbnkname  VARCHAR2(20),
  raccno    VARCHAR2(20),
  rname     VARCHAR2(20),
  fee       VARCHAR2(20),
  notes     VARCHAR2(20),
  trxsqnos  VARCHAR2(20),
  step      VARCHAR2(20)
);











create table NCPS_SNDDTL_BAK
(
  workdate  VARCHAR2(20),
  timestamp VARCHAR2(20),
  paccno    VARCHAR2(20),
  amount    NUMBER,
  currtype  VARCHAR2(20),
  teller    VARCHAR2(20),
  trxsqnob  VARCHAR2(20),
  rbnkno    VARCHAR2(20),
  rbnkname  VARCHAR2(20),
  raccno    VARCHAR2(20),
  rname     VARCHAR2(20),
  fee       VARCHAR2(20),
  notes     VARCHAR2(20),
  trxsqnos  VARCHAR2(20),
  step      VARCHAR2(20)
);











create table ONLTRANREC_HIS
(
  srvtransdatetime VARCHAR2(20),
  cardno           VARCHAR2(20),
  tranamt          VARCHAR2(20),
  trancurrcode     VARCHAR2(20),
  drcrf            VARCHAR2(20),
  adddata1         VARCHAR2(20),
  addstldata       VARCHAR2(20)
);











create table PRAS_PTHSASUB
(
  savname VARCHAR2(20),
  accno   VARCHAR2(20),
  cino    VARCHAR2(20)
);











create table PRS_S_PFHCDMDU
(
  openzono  VARCHAR2(5),
  hashcode  VARCHAR2(3),
  mdcardno  VARCHAR2(19),
  mediumno  VARCHAR2(19),
  prodid    VARCHAR2(13),
  mamdno    VARCHAR2(19),
  cardkind  VARCHAR2(3),
  cardstat  VARCHAR2(3),
  prestat   VARCHAR2(3),
  stparang  VARCHAR2(1),
  donepin   VARCHAR2(1),
  mdkpwd1   VARCHAR2(8),
  mdkpwd2   VARCHAR2(8),
  allyno    VARCHAR2(9),
  agrunno   VARCHAR2(9),
  exchflag  VARCHAR2(1),
  cardflag  VARCHAR2(1),
  icbcflag  VARCHAR2(1),
  synflag   VARCHAR2(1),
  goldflag  VARCHAR2(1),
  cino      VARCHAR2(15),
  chkpwdno  VARCHAR2(3),
  seclckflg VARCHAR2(3),
  mdkexprd  VARCHAR2(6),
  opendate  VARCHAR2(10),
  actdate   VARCHAR2(10),
  exchdate  VARCHAR2(10),
  closdate  VARCHAR2(10),
  exchnum   VARCHAR2(3),
  openbrno  VARCHAR2(5),
  agnbrno   VARCHAR2(5),
  oldcrdno  VARCHAR2(19),
  mediumtp  VARCHAR2(5),
  cardmedm  VARCHAR2(1),
  medbitmp1 VARCHAR2(50),
  medbitmp2 VARCHAR2(30),
  medbitmp3 VARCHAR2(30),
  lstudate  VARCHAR2(10),
  orgznno   VARCHAR2(5),
  orgdate   VARCHAR2(10),
  cpseq     VARCHAR2(3),
  gxhno     VARCHAR2(4),
  allymbno  VARCHAR2(20),
  savname   VARCHAR2(30),
  lgldoctp  VARCHAR2(4),
  idcode    VARCHAR2(18),
  accnum    VARCHAR2(5),
  accno     VARCHAR2(17),
  hkdacc    VARCHAR2(17),
  usdacc    VARCHAR2(17),
  acprtflg  VARCHAR2(1),
  accbknum  VARCHAR2(3),
  accbklin  VARCHAR2(3),
  saccbklin VARCHAR2(3),
  acseqno   VARCHAR2(5),
  actzono   VARCHAR2(5),
  actbrno   VARCHAR2(5),
  acttlno   VARCHAR2(5),
  nfeeflag  VARCHAR2(1),
  nfeerate  VARCHAR2(9),
  feeflag   VARCHAR2(3),
  feeamt    VARCHAR2(7),
  bfeetime  VARCHAR2(3),
  lsttrand  VARCHAR2(10),
  yfeedate  VARCHAR2(10),
  totcsale  VARCHAR2(5),
  totasale  VARCHAR2(17),
  cdrsalet  VARCHAR2(17),
  cdramtt   VARCHAR2(17),
  tdramtt   VARCHAR2(17),
  cdrsalem  VARCHAR2(17),
  cdramtm   VARCHAR2(17),
  tdramtm   VARCHAR2(17),
  cdsquota  VARCHAR2(17),
  cdtquota  VARCHAR2(17),
  stdramtl  VARCHAR2(17),
  cdrsaletl VARCHAR2(17),
  cdramttl  VARCHAR2(17),
  tdramttl  VARCHAR2(17),
  atmttamt  VARCHAR2(17),
  atmttino  VARCHAR2(3),
  atmdtamt  VARCHAR2(17),
  atmdtino  VARCHAR2(3),
  asintegr  VARCHAR2(11),
  spintegr  VARCHAR2(11),
  allyinte  VARCHAR2(11),
  quota1    VARCHAR2(17),
  quota2    VARCHAR2(17),
  dpusecnl  VARCHAR2(30),
  crdctl    VARCHAR2(9),
  stdyamt1  VARCHAR2(7),
  stdyamt2  VARCHAR2(17),
  stdyamt3  VARCHAR2(17),
  stdycht1  VARCHAR2(50)
);


create table PRS_S_PFSJNL
(
  batzone     VARCHAR2(5),
  brno        VARCHAR2(5),
  tellerno    VARCHAR2(5),
  trxsqnb     VARCHAR2(5),
  trxsqns     VARCHAR2(3),
  trxserno    VARCHAR2(23),
  trxcode     VARCHAR2(5),
  trxtype     VARCHAR2(1),
  workdate    VARCHAR2(10),
  worktime    VARCHAR2(8),
  busidate    VARCHAR2(10),
  busitime    VARCHAR2(8),
  draccno     VARCHAR2(17),
  drcurr      VARCHAR2(3),
  drsbjcod    VARCHAR2(7),
  drcardno    VARCHAR2(19),
  drsynflg    VARCHAR2(1),
  drappno     VARCHAR2(3),
  draccode    VARCHAR2(5),
  craccno     VARCHAR2(17),
  crcurr      VARCHAR2(3),
  crsbjcod    VARCHAR2(7),
  crcardno    VARCHAR2(19),
  crsynflg    VARCHAR2(1),
  crappno     VARCHAR2(3),
  craccode    VARCHAR2(5),
  prooff      VARCHAR2(1),
  vouhtype    VARCHAR2(3),
  vouhno      VARCHAR2(9),
  lgldoctp    VARCHAR2(4),
  idcode      VARCHAR2(18),
  cino        VARCHAR2(15),
  summflag    VARCHAR2(1),
  valueday    VARCHAR2(10),
  sealf       VARCHAR2(1),
  crosf       VARCHAR2(1),
  cashexf     VARCHAR2(1),
  catrflag    VARCHAR2(1),
  accatrbt    VARCHAR2(3),
  savterm     VARCHAR2(3),
  nextterm    VARCHAR2(3),
  yearkind    VARCHAR2(5),
  cashnote    VARCHAR2(20),
  authtlno    VARCHAR2(5),
  authono     VARCHAR2(5),
  servface    VARCHAR2(3),
  termid      VARCHAR2(15),
  revtranf    VARCHAR2(1),
  cashf       VARCHAR2(1),
  intdiff     VARCHAR2(3),
  sepcode     VARCHAR2(3),
  amount1     NUMBER(18),
  amount2     NUMBER(18),
  currtyp3    VARCHAR2(3),
  amount3     NUMBER(18),
  currtyp4    VARCHAR2(3),
  amount4     NUMBER(18),
  currtyp5    VARCHAR2(3),
  amount5     NUMBER(18),
  depuname    VARCHAR2(30),
  deputype    VARCHAR2(4),
  depuno      VARCHAR2(18),
  taxrate     VARCHAR2(9),
  field1      NUMBER(18),
  field2      NUMBER(18),
  field3      NUMBER(18),
  field4      NUMBER(18),
  field5      NUMBER(18),
  field6      NUMBER(18),
  date1       VARCHAR2(10),
  date2       VARCHAR2(10),
  notes       VARCHAR2(40),
  timestmp    VARCHAR2(26),
  field11     VARCHAR2(19),
  field12     NUMBER(18),
  field13     NUMBER(5),
  bractbno    VARCHAR2(10),
  drphybno    VARCHAR2(10),
  dractbno    VARCHAR2(10),
  crphybno    VARCHAR2(10),
  cractbno    VARCHAR2(10),
  currtyp6    VARCHAR2(3),
  amount6     NUMBER(18),
  currtyp7    VARCHAR2(3),
  amount7     NUMBER(18),
  field14     NUMBER(18),
  field15     NUMBER(18),
  cinobak     VARCHAR2(15),
  crossf      VARCHAR2(1),
  drcardty    VARCHAR2(3),
  crcardty    VARCHAR2(3),
  drmacardno  VARCHAR2(19),
  crmacardno  VARCHAR2(19),
  cutlgldoctp VARCHAR2(4),
  cutidcode   VARCHAR2(18),
  osavname    VARCHAR2(30),
  rsavname    VARCHAR2(60),
  recipact    VARCHAR2(34),
  transuse    VARCHAR2(20),
  uniseqno    VARCHAR2(40),
  rate        VARCHAR2(9),
  monint      NUMBER(18),
  prasinff    VARCHAR2(3),
  accvalueday VARCHAR2(10),
  matudate    VARCHAR2(10),
  field17     NUMBER(18),
  notes9      VARCHAR2(20),
  notes10     VARCHAR2(30),
  drprodid    VARCHAR2(13),
  crprodid    VARCHAR2(13),
  trxprodid   VARCHAR2(13),
  chantype    VARCHAR2(3),
  channo      VARCHAR2(5),
  eserialno   VARCHAR2(27),
  mserialn    VARCHAR2(27),
  oserialn    VARCHAR2(27),
  oritrxcode  VARCHAR2(5),
  drprotseno  VARCHAR2(17),
  crprotseno  VARCHAR2(17),
  drprodtype  VARCHAR2(5),
  drcustclas  VARCHAR2(5),
  dramttype   VARCHAR2(3),
  drdealterm  VARCHAR2(5),
  drprodcode1 VARCHAR2(7),
  drprodcode2 VARCHAR2(5),
  drprodcode3 VARCHAR2(5),
  crprodtype  VARCHAR2(5),
  crcustclas  VARCHAR2(5),
  cramttype   VARCHAR2(3),
  crdealterm  VARCHAR2(5),
  crprodcode1 VARCHAR2(7),
  crprodcode2 VARCHAR2(5),
  crprodcode3 VARCHAR2(5),
  otcrycode   VARCHAR2(3),
  sellercode  VARCHAR2(20),
  recommcode  VARCHAR2(20),
  orgno       VARCHAR2(10),
  totamt      NUMBER(18),
  balance     NUMBER(18),
  accinf      VARCHAR2(60),
  intamt      VARCHAR2(17),
  svrcode     VARCHAR2(9),
  bitmap1     VARCHAR2(20),
  bitmap2     VARCHAR2(20),
  bitmap3     VARCHAR2(20),
  bitmap4     VARCHAR2(40)
);



create table RPT_BFHINPOS
(
  workdate DATE,
  worktime VARCHAR2(20),
  carddrno VARCHAR2(20),
  trxamt   VARCHAR2(20),
  tydwacc  VARCHAR2(20),
  tydwname VARCHAR2(20),
  tydwno   VARCHAR2(20)
);


create table RPT_BFHINPOS_NOVA157
(
  workdate DATE,
  worktime VARCHAR2(20),
  carddrno VARCHAR2(20),
  trxamt   VARCHAR2(20),
  tydwacc  VARCHAR2(20),
  tydwname VARCHAR2(20),
  tydwno   VARCHAR2(20)
);


create table RPT_BFHINPOS_NOVA165
(
  workdate DATE,
  worktime VARCHAR2(20),
  carddrno VARCHAR2(20),
  trxamt   VARCHAR2(20),
  tydwacc  VARCHAR2(20),
  tydwname VARCHAR2(20),
  tydwno   VARCHAR2(20)
);


create table RPT_BFHKBDPB
(
  ownname VARCHAR2(20),
  cardno  VARCHAR2(20)
);

create table RPT_SFAWCST
(
  draccno  VARCHAR2(20),
  lstime   VARCHAR2(20),
  craccno  VARCHAR2(20),
  msgamt   VARCHAR2(20),
  typtlno  VARCHAR2(20),
  msgserno VARCHAR2(20),
  sbnkcode VARCHAR2(20),
  rbnkcode VARCHAR2(20),
  saccno   VARCHAR2(20),
  raccno   VARCHAR2(20),
  saccname VARCHAR2(20),
  raccname VARCHAR2(20),
  msgmark  VARCHAR2(20),
  feeamt   VARCHAR2(20),
  tradate  DATE,
  clrdate  DATE
);


create table RPT_SFAWDST
(
  tradate  VARCHAR2(20),
  lstime   VARCHAR2(20),
  draccno  VARCHAR2(20),
  craccno  VARCHAR2(20),
  msgamt   VARCHAR2(20),
  typtlno  VARCHAR2(20),
  msgserno VARCHAR2(20),
  sbnkcode VARCHAR2(20),
  rbnkcode VARCHAR2(20),
  saccno   VARCHAR2(20),
  raccno   VARCHAR2(20),
  saccname VARCHAR2(20),
  raccname VARCHAR2(20),
  msgmark  VARCHAR2(20),
  feeamt   VARCHAR2(20),
  clrdate  VARCHAR2(20)
);


create table RPT_SFFHBPM
(
  workdate   VARCHAR2(20),
  jltimestmp VARCHAR2(20),
  jlsndrevf  VARCHAR2(20),
  lmsgamt    VARCHAR2(20),
  jlcurrtype VARCHAR2(20),
  jltrxcode  VARCHAR2(20),
  jltellerno VARCHAR2(20),
  jltrxsqnb  VARCHAR2(20),
  rgobnkcode VARCHAR2(20),
  rgbankcode VARCHAR2(20),
  jlobnkname VARCHAR2(20),
  jlbankname VARCHAR2(20),
  rgraccno   VARCHAR2(20),
  rgsaccno   VARCHAR2(20),
  rgraccname VARCHAR2(20),
  rgsaccname VARCHAR2(20),
  jlfeeamt   VARCHAR2(20),
  jltrxsqns  VARCHAR2(20),
  jlsummary  VARCHAR2(20),
  jlstatus   VARCHAR2(20),
  jlmsgamt   NUMBER,
  jlrac      VARCHAR2(20),
  jlsac      VARCHAR2(20)
);











create table RPT_SFFHBPMA
(
  workdate   VARCHAR2(20),
  jltimestmp VARCHAR2(20),
  jlsndrevf  VARCHAR2(20),
  lmsgamt    VARCHAR2(20),
  jlcurrtype VARCHAR2(20),
  jltrxcode  VARCHAR2(20),
  jltellerno VARCHAR2(20),
  jltrxsqnb  VARCHAR2(20),
  rgobnkcode VARCHAR2(20),
  rgbankcode VARCHAR2(20),
  jlobnkname VARCHAR2(20),
  jlbankname VARCHAR2(20),
  rgraccno   VARCHAR2(20),
  rgsaccno   VARCHAR2(20),
  rgraccname VARCHAR2(20),
  rgsaccname VARCHAR2(20),
  jlfeeamt   VARCHAR2(20),
  jltrxsqns  VARCHAR2(20),
  jlsummary  VARCHAR2(20),
  jlstatus   VARCHAR2(20),
  jlmsgamt   NUMBER,
  jlrac      VARCHAR2(20),
  jlsac      VARCHAR2(20)
);











create table RPT_SFFHIPM
(
  workdate   VARCHAR2(20),
  jltimestmp VARCHAR2(20),
  jlsndrevf  VARCHAR2(20),
  lmsgamt    VARCHAR2(20),
  jlcurrtype VARCHAR2(20),
  jltrxcode  VARCHAR2(20),
  jltellerno VARCHAR2(20),
  jltrxsqnb  VARCHAR2(20),
  rgobnkcode VARCHAR2(20),
  rgbankcode VARCHAR2(20),
  jlobnkname VARCHAR2(20),
  jlbankname VARCHAR2(20),
  rgraccno   VARCHAR2(20),
  rgsaccno   VARCHAR2(20),
  rgraccname VARCHAR2(20),
  rgsaccname VARCHAR2(20),
  jlfeeamt   VARCHAR2(20),
  jltrxsqns  VARCHAR2(20),
  jlsummary  VARCHAR2(20),
  jlstatus   VARCHAR2(20),
  jlmsgamt   NUMBER,
  jlrac      VARCHAR2(20),
  jlsac      VARCHAR2(20)
);











create table RPT_SFFHIPMA
(
  workdate   VARCHAR2(20),
  jltimestmp VARCHAR2(20),
  jlsndrevf  VARCHAR2(20),
  lmsgamt    VARCHAR2(20),
  jlcurrtype VARCHAR2(20),
  jltrxcode  VARCHAR2(20),
  jltellerno VARCHAR2(20),
  jltrxsqnb  VARCHAR2(20),
  rgobnkcode VARCHAR2(20),
  rgbankcode VARCHAR2(20),
  jlobnkname VARCHAR2(20),
  jlbankname VARCHAR2(20),
  rgraccno   VARCHAR2(20),
  rgsaccno   VARCHAR2(20),
  rgraccname VARCHAR2(20),
  rgsaccname VARCHAR2(20),
  jlfeeamt   VARCHAR2(20),
  jltrxsqns  VARCHAR2(20),
  jlsummary  VARCHAR2(20),
  jlstatus   VARCHAR2(20),
  jlmsgamt   NUMBER,
  jlrac      VARCHAR2(20),
  jlsac      VARCHAR2(20)
);











create table RPT_SFFHMBK
(
  allname  VARCHAR2(20),
  bankcode VARCHAR2(20)
);

create table SADTL_ACCNO_LIST
(
  sn        VARCHAR2(20),
  accno     VARCHAR2(20),
  accnotype VARCHAR2(1)
);

create table SADTL_ACCNO_LIST_19
(
  sn        VARCHAR2(20),
  accno     VARCHAR2(20),
  accnotype VARCHAR2(1)
);

create table SADTL_ACCNO_LIST_DUPACC
(
  dupacc VARCHAR2(20)
);

create table SADTL_ACCNO_LIST_NODUP
(
  sn        VARCHAR2(20),
  accno     VARCHAR2(20),
  accnotype VARCHAR2(1)
);

create table SADTL_CARD_LIST
(
  sn        VARCHAR2(20),
  accno     VARCHAR2(20),
  accnotype VARCHAR2(1)
);

create table SADTL_DTL_TMP
(
  accno               VARCHAR2(17),
  serialno            VARCHAR2(11),
  currtype            VARCHAR2(3),
  cashexf             VARCHAR2(1),
  actbrno             VARCHAR2(5),
  phybrno             VARCHAR2(5),
  subcode             VARCHAR2(7),
  cino                VARCHAR2(15),
  accatrbt            VARCHAR2(3),
  busidate            VARCHAR2(10),
  busitime            VARCHAR2(8),
  trxcode             VARCHAR2(5),
  workdate            VARCHAR2(10),
  valueday            VARCHAR2(10),
  closdate            VARCHAR2(10),
  drcrf               VARCHAR2(1),
  vouhno              NUMBER(9),
  amount              NUMBER(18),
  balance             NUMBER(18),
  closint             NUMBER(18),
  inttax              NUMBER(18),
  summflag            VARCHAR2(1),
  cashnote            VARCHAR2(20),
  zoneno              VARCHAR2(5),
  brno                VARCHAR2(5),
  tellerno            VARCHAR2(5),
  trxsqno             VARCHAR2(5),
  servface            VARCHAR2(3),
  authono             VARCHAR2(5),
  authtlno            VARCHAR2(5),
  termid              VARCHAR2(15),
  timestmp            VARCHAR2(26),
  recipact            VARCHAR2(34),
  mdcardno            VARCHAR2(19),
  acserno             VARCHAR2(5),
  synflag             VARCHAR2(1),
  showflag            VARCHAR2(1),
  macardn             VARCHAR2(19),
  recipcn             VARCHAR2(15),
  catrflag            VARCHAR2(1),
  sellercode          VARCHAR2(20),
  amount1             NUMBER(18),
  amount2             NUMBER(17),
  flag1               VARCHAR2(9),
  date1               VARCHAR2(10),
  eserialno           VARCHAR2(27),
  trxsite             VARCHAR2(40),
  zoneaccno           VARCHAR2(4),
  worktime            VARCHAR2(8),
  ptrxcode            VARCHAR2(5),
  authzone            VARCHAR2(5),
  trxserno            VARCHAR2(23),
  medidtp             VARCHAR2(3),
  agaccno             VARCHAR2(17),
  orgmediumno         VARCHAR2(34),
  conftype            VARCHAR2(3),
  orgcode             VARCHAR2(10),
  orgname             VARCHAR2(40),
  busino              VARCHAR2(15),
  resavname           VARCHAR2(60),
  rebankname          VARCHAR2(40),
  prodtype            VARCHAR2(3),
  saprodtid           VARCHAR2(13),
  severcode           VARCHAR2(9),
  orgnotes            VARCHAR2(60),
  notes               VARCHAR2(20),
  fix_recipact        VARCHAR2(17),
  fix_rsavname        VARCHAR2(60),
  fix_rbnkno          VARCHAR2(12),
  fix_rbnkname        VARCHAR2(60),
  fix_recipdatasource VARCHAR2(100),
  dtl_seq             VARCHAR2(50),
  recip_seq           VARCHAR2(50),
  sn                  VARCHAR2(20),
  fix_bakmsg          VARCHAR2(200),
  fix_inmsg           VARCHAR2(100)
);

create table SADTL_LOG_TABLE
(
  workdate VARCHAR2(8),
  procname VARCHAR2(50),
  loginf   VARCHAR2(1000)
);

create table SADTL_LOG_TABLE_2015
(
  workdate VARCHAR2(20),
  procname VARCHAR2(20),
  loginf   VARCHAR2(20)
);

create table SADTL_RECIPACT_TMP
(
  busidate   VARCHAR2(8),
  busitime   VARCHAR2(8),
  timestmp   VARCHAR2(26),
  accno      VARCHAR2(40),
  amount     NUMBER(20),
  currtype   VARCHAR2(3),
  trxcode    VARCHAR2(5),
  tellerno   VARCHAR2(9),
  trxsqno    VARCHAR2(8),
  drcrf      VARCHAR2(1),
  rbnkno     VARCHAR2(20),
  rbnkname   VARCHAR2(200),
  recipact   VARCHAR2(40),
  rsavname   VARCHAR2(60),
  fee        VARCHAR2(20),
  trxsqnob   VARCHAR2(5),
  trxsqnos   VARCHAR2(3),
  datasource VARCHAR2(100),
  sn         VARCHAR2(20),
  recip_seq  VARCHAR2(20),
  dtl_seq    VARCHAR2(20),
  bakmsg     VARCHAR2(200),
  changsuo   VARCHAR2(100),
  inmsg      VARCHAR2(100)
);

create table SADTL_RECIPACT_TMP_2015
(
  busidate   VARCHAR2(20 byte),
  busitime   VARCHAR2(20 byte),
  accno      VARCHAR2(20 byte),
  amount     VARCHAR2(20 byte),
  currtype   VARCHAR2(20 byte),
  tellerno   VARCHAR2(20 byte),
  trxsqno    VARCHAR2(20 byte),
  drcrf      VARCHAR2(20 byte),
  rbnkno     VARCHAR2(20 byte),
  rbnkname   VARCHAR2(20 byte),
  recipact   VARCHAR2(20 byte),
  rsavname   VARCHAR2(20 byte),
  bakmsg     VARCHAR2(20 byte),
  trxsqnob   VARCHAR2(20 byte),
  trxsqnos   VARCHAR2(20 byte),
  changsuo   VARCHAR2(20 byte),
  inmsg      VARCHAR2(20 byte),
  datasource VARCHAR2(30 byte),
  fee        VARCHAR2(20 byte),
  timestmp   VARCHAR2(20 byte),
  trxocde    VARCHAR2(30 byte)
);

create table SCP_S_SFAWCST
(
  rpzoneno  VARCHAR2(5),
  rpbrno    VARCHAR2(5),
  listno    VARCHAR2(3),
  orgseq    VARCHAR2(1),
  sndrevf   VARCHAR2(1),
  msgserno  VARCHAR2(8),
  msgtype   VARCHAR2(6),
  sbnkcode  VARCHAR2(12),
  scbnkcod  VARCHAR2(12),
  saccno    VARCHAR2(34),
  saccname  VARCHAR2(60),
  rbnkcode  VARCHAR2(12),
  rcbnkcod  VARCHAR2(12),
  raccno    VARCHAR2(34),
  raccname  VARCHAR2(60),
  busstype  VARCHAR2(8),
  status    VARCHAR2(3),
  msgdate   VARCHAR2(10),
  msgamt    NUMBER(18),
  feeamt    NUMBER(18),
  typtlno   VARCHAR2(5),
  chktlno   VARCHAR2(5),
  auttlno   VARCHAR2(5),
  msgmark   VARCHAR2(140),
  draccno   VARCHAR2(34),
  draccname VARCHAR2(60),
  craccno   VARCHAR2(34),
  craccname VARCHAR2(60),
  organno   VARCHAR2(10),
  fmsgtype  VARCHAR2(4),
  fmsgser   VARCHAR2(8),
  fmsgdate  VARCHAR2(10),
  fymsgser  VARCHAR2(8),
  fymsgdate VARCHAR2(10),
  tradate   VARCHAR2(10),
  etrancod  VARCHAR2(35),
  confflag  VARCHAR2(4),
  trainfo   VARCHAR2(256),
  confinf   VARCHAR2(140),
  lstime    VARCHAR2(19),
  clrdate   VARCHAR2(10),
  clrinfo   VARCHAR2(300),
  setclass  VARCHAR2(2)
);

create table SCP_S_SFAWDST
(
  rpzoneno  VARCHAR2(5),
  rpbrno    VARCHAR2(5),
  listno    VARCHAR2(3),
  orgseq    VARCHAR2(1),
  sndrevf   VARCHAR2(1),
  msgserno  VARCHAR2(8),
  msgtype   VARCHAR2(6),
  sbnkcode  VARCHAR2(12),
  scbnkcod  VARCHAR2(12),
  saccno    VARCHAR2(34),
  saccname  VARCHAR2(60),
  rbnkcode  VARCHAR2(12),
  rcbnkcod  VARCHAR2(12),
  raccno    VARCHAR2(34),
  raccname  VARCHAR2(60),
  busstype  VARCHAR2(8),
  status    VARCHAR2(3),
  msgdate   VARCHAR2(10),
  msgamt    NUMBER(18),
  feeamt    NUMBER(18),
  typtlno   VARCHAR2(5),
  chktlno   VARCHAR2(5),
  auttlno   VARCHAR2(5),
  msgmark   VARCHAR2(140),
  draccno   VARCHAR2(34),
  draccname VARCHAR2(60),
  craccno   VARCHAR2(34),
  craccname VARCHAR2(60),
  organno   VARCHAR2(10),
  fmsgtype  VARCHAR2(4),
  fmsgser   VARCHAR2(8),
  fmsgdate  VARCHAR2(10),
  fymsgser  VARCHAR2(8),
  fymsgdate VARCHAR2(10),
  tradate   VARCHAR2(10),
  etrancod  VARCHAR2(35),
  confflag  VARCHAR2(4),
  trainfo   VARCHAR2(256),
  confinf   VARCHAR2(140),
  lstime    VARCHAR2(19),
  clrdate   VARCHAR2(10),
  clrinfo   VARCHAR2(300),
  setclass  VARCHAR2(2)
);
create table SDA_S_EFMERROT
(
  payareacode     VARCHAR2(5),
  recareacode     VARCHAR2(5),
  serialno        VARCHAR2(20),
  mercode         VARCHAR2(20),
  transtime       VARCHAR2(14),
  payamt          VARCHAR2(20),
  fee             VARCHAR2(20),
  curtype         VARCHAR2(3),
  recacctno       VARCHAR2(30),
  recnetcode      VARCHAR2(5),
  payacctno       VARCHAR2(30),
  paynetcode      VARCHAR2(5),
  merflag         VARCHAR2(1),
  ipaddress       VARCHAR2(24),
  submittellerid  VARCHAR2(24),
  authortellerid  VARCHAR2(24),
  authortellerid2 VARCHAR2(24),
  jionflag        VARCHAR2(1),
  mername         VARCHAR2(60),
  tranchannel     VARCHAR2(1),
  orderno         VARCHAR2(30),
  payacctname     VARCHAR2(60),
  gbflag          VARCHAR2(1),
  downloaddate    VARCHAR2(8),
  depflag         VARCHAR2(1),
  depmonth        VARCHAR2(3),
  reflag          VARCHAR2(1),
  sfeetype        VARCHAR2(1),
  feeamt          NUMBER(18),
  proamt          NUMBER(18),
  paytype         VARCHAR2(4),
  paymode         VARCHAR2(4),
  ctrxserno       VARCHAR2(21),
  thbankno        VARCHAR2(10),
  icbcmallf       VARCHAR2(1),
  flag1           VARCHAR2(3),
  flag3           VARCHAR2(3)
);

create table SDA_S_EFRMLGIN
(
  payareacode  VARCHAR2(5),
  recareacode  VARCHAR2(5),
  serialno     VARCHAR2(15),
  payamt       VARCHAR2(20),
  fee          VARCHAR2(20),
  transtime    VARCHAR2(14),
  paycardno    VARCHAR2(20),
  paynetcode   VARCHAR2(5),
  recacctno    VARCHAR2(20),
  recnetcode   VARCHAR2(5),
  states       VARCHAR2(1),
  curtype      VARCHAR2(3),
  certid       VARCHAR2(27),
  payuse       VARCHAR2(40),
  gbflag       VARCHAR2(1),
  downloaddate VARCHAR2(8)
);

create table SDA_S_EFRMLGOT
(
  payareacode  VARCHAR2(5),
  recareacode  VARCHAR2(5),
  serialno     VARCHAR2(15),
  payamt       VARCHAR2(20),
  fee          VARCHAR2(20),
  transtime    VARCHAR2(14),
  paycardno    VARCHAR2(20),
  paynetcode   VARCHAR2(5),
  recacctno    VARCHAR2(20),
  recnetcode   VARCHAR2(5),
  states       VARCHAR2(1),
  curtype      VARCHAR2(3),
  certid       VARCHAR2(27),
  payuse       VARCHAR2(40),
  gbflag       VARCHAR2(1),
  downloaddate VARCHAR2(8)
);

create table SGBILL_BIP_TRAN_DETAIL_V
(
  trxdate VARCHAR2(20),
  trxtime VARCHAR2(20),
  cdid    VARCHAR2(20),
  amt     VARCHAR2(20),
  ccy     VARCHAR2(20),
  drcrf   VARCHAR2(20),
  dbid    VARCHAR2(20),
  dbnm    VARCHAR2(20),
  dbissr  VARCHAR2(20),
  herrno  VARCHAR2(20),
  txtpcd  VARCHAR2(20)
);

create table WY_MER_REPORT_OUT_B2C
(
  transtime    VARCHAR2(20),
  payacctno    VARCHAR2(20),
  payamt       VARCHAR2(20),
  recacctno    VARCHAR2(20),
  mername      VARCHAR2(20),
  pmpfdatadate VARCHAR2(20),
  orderno      VARCHAR2(20)
);

create table WY_PERSON_REMIT_LOG_IN
(
  transtime    VARCHAR2(20),
  rec_acctno   VARCHAR2(20),
  payamt       VARCHAR2(20),
  curtype      VARCHAR2(20),
  pay_cardno   VARCHAR2(20),
  pay_areacode VARCHAR2(20),
  fee          VARCHAR2(20),
  payuse       VARCHAR2(20)
);
create table WY_PERSON_REMIT_LOG_OUT
(
  transtime    VARCHAR2(20),
  pay_cardno   VARCHAR2(20),
  payamt       VARCHAR2(20),
  curtype      VARCHAR2(20),
  payuse       VARCHAR2(20),
  fee          VARCHAR2(20),
  rec_acctno   VARCHAR2(20),
  rec_areacode VARCHAR2(20)
);

create sequence SEQ_SADTL_ACCNO_LIST
minvalue 1
maxvalue 99999999999999999999
start with 1
increment by 1
cache 20;



create sequence SEQ_SADTL_DTL
minvalue 1
maxvalue 99999999999999999999
start with 1
increment by 1
cache 20;


create sequence SEQ_SADTL_RECIP
minvalue 1
maxvalue 99999999999999999999
start with 1
increment by 1
cache 20;



delimiter //;


CREATE OR REPLACE PACKAGE pckg_public_util
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

END pckg_public_util;
//


CREATE OR REPLACE PACKAGE PCKG_TOEBANK_QUERY
IS
    -- 字典值定义
    STATUS_WAITING   CONSTANT  VARCHAR2(2) := '-1';
    STATUS_READY        CONSTANT  VARCHAR2(2) := '0';
    STATUS_RUNNING   CONSTANT  VARCHAR2(2) := '1';
    STATUS_SUCCESSED CONSTANT  VARCHAR2(2) := '2';
    STATUS_FAILED    CONSTANT  VARCHAR2(2) := '3';
    REQUEST_TYPE_EBANK CONSTANT  VARCHAR2(2) := '1';
    REQUEST_TYPE_CMP CONSTANT  VARCHAR2(2) := '2';

    /******************************************************************************
     --存储过程名称： PROC_BATCH_INIT
     --作者： kwg
     --时间： 2017年03月08日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
    PROCEDURE PROC_BATCH_INIT (
            p_i_date            IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2);

    /******************************************************************************
     --存储过程名称： PROC_BATCH_INIT
     --作者： kwg
     --时间： 2017年03月08日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
    PROCEDURE PROC_EXPORT_EBANK (
            p_i_date            IN VARCHAR2,
            p_serial_num          IN VARCHAR2,
            p_request_date     IN VARCHAR2,
            P_file_name        IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2);

    /******************************************************************************
     --函数名称： PROC_EXPORT_CMP
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_EXPORT_CMP (
            p_i_date            IN VARCHAR2,
            p_serial_num          IN VARCHAR2,
            p_request_date          IN VARCHAR2,
            P_file_name        IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2);                          --0：导出完成 1：导出未完成

    /******************************************************************************
     --函数名称： PROC_UPDATE_RUNNING_STATUS
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_UPDATE_RUNNING_STATUS (
            p_i_date            IN VARCHAR2,
            p_serial_num           IN VARCHAR2,
            p_request_date      IN VARCHAR2,
            p_request_type      IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2);

      /******************************************************************************
     --函数名称： PROC_UPDATE_END_STATUS
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_UPDATE_END_STATUS (
            p_i_date            IN VARCHAR2,
            p_serial_num           IN VARCHAR2,
            p_request_date      IN VARCHAR2,
            p_request_type      IN VARCHAR2,
            p_flag              IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2);

    /******************************************************************************
     --函数名称： PROC_BACKUP_LOG_DAT
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_BACKUP_LOG_DAT (
            p_i_date            IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2);                          --0：导出完成 1：导出未完成


    /******************************************************************************
     --函数名称： GET_QUERY_WHERE_STRING
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --p_in_str (传入参数，表名)
     --功能： 模版程序1
     ******************************************************************************/
    FUNCTION GET_QUERY_WHERE_STRING(
        p_in_str IN VARCHAR2,
        p_table_name IN VARCHAR2,
        p_date_from IN VARCHAR2,
        p_date_to IN VARCHAR2
    )
    RETURN VARCHAR2;

    /******************************************************************************
     --函数名称： GET_QUERY_ORDER_STRING
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --p_in_str (传入参数，表名)
     --功能： 模版程序1
     ******************************************************************************/
    FUNCTION GET_QUERY_ORDER_STRING(
        p_in_str IN VARCHAR2,
        p_table_name IN VARCHAR2
    )
    RETURN VARCHAR2;
END PCKG_TOEBANK_QUERY;
//







CREATE OR REPLACE PACKAGE PCK_CREATE_RECIPACT
IS
   -- Author  : KFZX-ZHUJIAO
   -- Created : 2017-08-01 10:10:10
   -- Purpose : 个人活期明细整合  生成交易对手表
   PROCEDURE init_recipact_main (i_workdate IN VARCHAR2);           --yyyymmdd


   --获取各个渠道交易对手
   --网银转出
   PROCEDURE proc_recip_wy_out (i_workdate IN VARCHAR2);

   --超级网银行,网银跨行清算系统支付业务借报清单文件
   PROCEDURE proc_recip_2d_wy (i_workdate IN VARCHAR2);

   --网银转入
   PROCEDURE proc_recip_wy_in (i_workdate IN VARCHAR2);

   --B2C
   PROCEDURE proc_recip_B2C (i_workdate IN VARCHAR2);

    --POS
  procedure proc_recip_POS(i_workdate in varchar2    );

   --代发工资
  procedure proc_recip_salary(i_workdate in varchar2   );


END PCK_CREATE_RECIPACT;
//







create or replace package PCK_CREATE_RECIPACT_2015 is
  -- Author  : SZFH-HEMP
  -- Created : 2015-3-23 15:58:40
  -- Purpose : 个人活期明细整合  生成交易对手表

  procedure init_recipact_main(i_workdate in varchar2); --yyyymmdd


  --获取各个渠道交易对手
  --网银转出
  procedure proc_recip_wy_out(i_workdate in varchar2);

  --超级网银行
  procedure proc_recip_2d_wy(i_workdate in varchar2);


  --网银转入
  procedure proc_recip_wy_in(i_workdate in varchar2    );

  --B2C
  procedure proc_recip_B2C(i_workdate in varchar2    );
   --POS
  procedure proc_recip_POS(i_workdate in varchar2    );

   --代发工资
  procedure proc_recip_salary(i_workdate in varchar2   );

   --代理业务
  procedure proc_recip_dlyy(i_workdate in varchar2   );


   --现代化支付旧
  procedure proc_recip_ibps(i_workdate in varchar2   );

   --现代化支付新
  procedure proc_recip_sffh(i_workdate in varchar2   );

  --新同城
  procedure proc_recip_ncps(i_workdate in varchar2  );

  --自助终端
  procedure proc_recip_fbec(i_workdate in varchar2 );

  --atm
  procedure proc_recip_atm(i_workdate in varchar2    );

  --深港票交
  procedure proc_recip_sgbill(i_workdate in varchar2    );

end PCK_CREATE_RECIPACT_2015;
//







CREATE OR REPLACE PACKAGE PCK_CREATE_SADTL_PUB
IS
   -- 公共模块

   --公共函数 统一日志登记
   PROCEDURE public_log (i_workdate   IN VARCHAR2,                       -- 日期
                         i_pgm        IN VARCHAR2,                      --功能模块
                         i_loginf     IN VARCHAR2                     -- 需记录内容
                                                 );
END PCK_CREATE_SADTL_PUB;
//



create or replace package PCK_CREATE_SADTL_PUB_2015 is

 -- 公共模块


  --公共函数 根据卡号或者帐号获取名称
   Function Get_Accname
  (
        i_Accno In Varchar2
    ) Return Varchar2 ;


  --公共函数 统一日志登记
  procedure public_log
  (
    i_workdate   In varchar2, -- 日期
    i_pgm        in varchar2, --功能模块
    i_loginf    in  varchar2-- 需记录内容
  );


  --根据地区号取名称
   Function Get_zoneno_name
  (
        i_zoneno In Varchar2
    ) Return Varchar2 ;


  --根据人民银行12号取对应的名称
   Function Get_bankcode_name
  (
        i_bankcode In Varchar2
    ) Return Varchar2 ;

end PCK_CREATE_SADTL_PUB_2015;
//


CREATE OR REPLACE PACKAGE PCK_MATCH_SADTL
IS
   PROCEDURE main (i_workdate IN VARCHAR2);

   PROCEDURE ycl;

   PROCEDURE match_js;

   PROCEDURE match_special_13_34024;                       --精确匹配，结算日志13/34024

   PROCEDURE match_special_12_34024;                       --精确匹配，结算日志12/34024

   PROCEDURE match_special_59_34024;                       --精确匹配，结算日志59/34024

   PROCEDURE match_special_12_34009;                       --精确匹配，结算日志12/34009

   PROCEDURE match_special_12_34009_cr;

   PROCEDURE find_rercip_teller_34024 (i_busidate   IN     VARCHAR2,
                                       i_trxcode    IN     VARCHAR2,
                                       i_tellerno   IN     NUMBER,
                                       i_trxsqnb    IN     NUMBER,
                                       i_currtype   IN     NUMBER,
                                       i_amount     IN     NUMBER,
                                       i_draccno    IN     VARCHAR2,
                                       o_code          OUT NUMBER, --0 表示正常，才可以使用后面的 o_recip
                                       o_recip         OUT VARCHAR2,  --日志贷方账号
                                       o_rsavname      OUT VARCHAR2 --日志中的NOTES1
                                                                   );

   PROCEDURE find_rercip_teller12_34009_cr (i_busidate   IN     VARCHAR2,
                                            i_trxcode    IN     VARCHAR2,
                                            i_tellerno   IN     NUMBER,
                                            i_trxsqnb    IN     NUMBER,
                                            i_currtype   IN     NUMBER,
                                            i_amount     IN     NUMBER,
                                            i_craccno    IN     VARCHAR2,
                                            o_code          OUT NUMBER, --0 表示正常，才可以使用后面的 o_recip
                                            o_recip         OUT VARCHAR2 --日志贷方账号
                                                                        --o_rsavname out varchar2 --日志中的NOTES1
                                            );

   PROCEDURE find_rercip_teller12_34009 (i_busidate   IN     VARCHAR2,
                                         i_trxcode    IN     VARCHAR2,
                                         i_tellerno   IN     NUMBER,
                                         i_trxsqnb    IN     NUMBER,
                                         i_currtype   IN     NUMBER,
                                         i_amount     IN     NUMBER,
                                         i_draccno    IN     VARCHAR2,
                                         o_code          OUT NUMBER, --0 表示正常，才可以使用后面的 o_recip
                                         o_recip         OUT VARCHAR2 --日志贷方账号
                                                                     --o_rsavname out varchar2 --日志中的NOTES1
                                         );

   PROCEDURE match_week (i_workdate IN VARCHAR2);

   --2.1 1网银转出对手匹配
   --2.1 2.3 需要关注是否含手续费的问题,现在看起来,好像不需要考虑手续费
   PROCEDURE match_wy_out (i_workdate IN VARCHAR2);

   --2.2 2代网银
   PROCEDURE match_2DWY (i_workdate IN VARCHAR2);

   --2.3 3网银转入对手匹配
   PROCEDURE match_wy_in (i_workdate IN VARCHAR2);
   --2.4 4b2c
   procedure match_b2c (i_workdate in  varchar2);
   --2.5 pos
   procedure match_pos (i_workdate in  varchar2);
   --2.6 代发工资
       procedure match_salary (i_workdate in  varchar2);
END PCK_MATCH_SADTL;
//







create or replace function func_AccNoTransfer(AccNo in varchar2) return varchar2 is
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







CREATE OR REPLACE FUNCTION GET_FULL_ACCNO(ACCNO IN VARCHAR2)
return varchar2 as
  acctno_nbr17   number(17,0);
  acctno_chr17   varchar2(17);
  acctno_nbr19   number(19,0);
  acctno_chr19   varchar2(19);
  idx            number(17,0) := 0;
  summation      number(17,0) := 0;
  remainder      number(17,0) := 0;
  quotient       number(17,0) := 0;
  check_no       number(17,0) := 0;
  acctno_len     number(17,0) := 0;
begin
  acctno_len    := length(trim(accno));
---
--- 输入数据：帐号有效性检验，17位或19位帐号全部位数字，
---                       前4位是地区号，应该不为零，
---                       第5～8位是网点号，应该不为零，
---                       第9～10位是应用号，应该不为零
---
---
  if acctno_len = 17 or acctno_len = 19 then
--
     if acctno_len = 17 then
        acctno_chr17 := trim(accno);
     else
        acctno_chr17 := substr(trim(accno),1,17);
     end if;
--   输入帐号字符合法性检验
     loop
        idx := idx + 1;
        if substr(acctno_chr17,idx,1) > '9' or substr(acctno_chr17,idx,1) < '0' then
           return 'Input accno : ' || trim(accno) || ' is wrong, illegal character is in the accno!';
        end if;
        exit when idx > 17;
     end loop;
--   地区号正确性检验
     if to_number(substr(acctno_chr17,1,4)) = 0 then
        return 'Input accno : ' || trim(accno) || ' is wrong, zoneno is zero!';
     end if;
--   网点号正确性检验
     if to_number(substr(acctno_chr17,5,4)) = 0 then
        return 'Input accno : ' || trim(accno) || ' is wrong, branch no is zero!';
     end if;
--   应用号正确性检验
     if to_number(substr(acctno_chr17,9,2)) = 0 then
        return 'Input accno : ' || trim(accno) || ' is wrong, appno is zero!';
     end if;
--
  else
--
     if accno is null then
        return 'Input accno is null, confirm and rerun please!';
     end if;
--
     if acctno_len < 17 or acctno_len = 18 then
        return 'Input accno : ' || trim(accno) || ' is too shot, confirm and rerun please!';
     end if;
--
     if acctno_len > 19 then
        return 'Input accno : ' || trim(accno) || ' is too long, confirm and rerun please!';
     end if;
--
  end if;
--
  if acctno_len = 17 then
--
     summation := to_number(substr(acctno_chr17,1,1))* 11 + to_number(substr(acctno_chr17,2,1))* 13 + to_number(substr(acctno_chr17,3,1))* 17 + to_number(substr(acctno_chr17,4,1))* 19 + to_number(substr(acctno_chr17,5,1))* 23
                + to_number(substr(acctno_chr17,6,1))* 29 + to_number(substr(acctno_chr17,7,1))* 31 + to_number(substr(acctno_chr17,8,1))* 37 + to_number(substr(acctno_chr17,9,1))* 41 + to_number(substr(acctno_chr17,10,1))* 43
                + to_number(substr(acctno_chr17,11,1))* 47 + to_number(substr(acctno_chr17,12,1))* 53 + to_number(substr(acctno_chr17,13,1))* 59 + to_number(substr(acctno_chr17,14,1))* 61 + to_number(substr(acctno_chr17,15,1))* 67
                + to_number(substr(acctno_chr17,16,1))* 71 + to_number(substr(acctno_chr17,17,1))* 73
                ;
--
     remainder := mod(summation,97);
     check_no  := 97 - remainder;
     acctno_chr19 := acctno_chr17 || trim(to_char(check_no,'09'));
--
     return acctno_chr19;
--
--elsif acctno_len = 19 or acctno_len = 18 then
  elsif acctno_len = 19 then
--
     summation := to_number(substr(acctno_chr17,1,1))* 11 + to_number(substr(acctno_chr17,2,1))* 13 + to_number(substr(acctno_chr17,3,1))* 17 + to_number(substr(acctno_chr17,4,1))* 19 + to_number(substr(acctno_chr17,5,1))* 23
                + to_number(substr(acctno_chr17,6,1))* 29 + to_number(substr(acctno_chr17,7,1))* 31 + to_number(substr(acctno_chr17,8,1))* 37 + to_number(substr(acctno_chr17,9,1))* 41 + to_number(substr(acctno_chr17,10,1))* 43
                + to_number(substr(acctno_chr17,11,1))* 47 + to_number(substr(acctno_chr17,12,1))* 53 + to_number(substr(acctno_chr17,13,1))* 59 + to_number(substr(acctno_chr17,14,1))* 61 + to_number(substr(acctno_chr17,15,1))* 67
                + to_number(substr(acctno_chr17,16,1))* 71 + to_number(substr(acctno_chr17,17,1))* 73
                ;
--
     remainder := mod(summation,97);
     check_no  := 97 - remainder;
     acctno_chr19 := acctno_chr17 || trim(to_char(check_no,'09'));
--
     return acctno_chr19;
  end if;
end;
//


CREATE OR REPLACE PACKAGE BODY pckg_public_util
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
         DELETE FROM log_task_detail
               WHERE d_task_date = p_i_date
                 AND c_proc_name = LOWER (p_i_proc_name)
                 AND c_min_zoneno = v_min_zoneno
                 AND c_max_zoneno = v_max_zoneno;

         INSERT INTO log_task_detail
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
         MERGE INTO log_task_detail t
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
      v_status       log_task_detail.n_status%TYPE;
      v_min_zoneno   VARCHAR2 (10);
      v_max_zoneno   VARCHAR2 (10);
   BEGIN
      v_min_zoneno := NVL (p_i_min_zoneno, '0');
      v_max_zoneno := NVL (p_i_max_zoneno, '0');

      SELECT NVL (MAX (n_status), 99)
        INTO v_status
        FROM log_task_detail
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

END pckg_public_util;
//

CREATE OR REPLACE PACKAGE BODY PCK_CREATE_RECIPACT
IS
   PROCEDURE init_recipact_main (i_workdate IN VARCHAR2)            --yyyymmdd
   IS
   BEGIN
      EXECUTE IMMEDIATE 'Truncate Table SADTL_RECIPACT_TMP';

      EXECUTE IMMEDIATE 'Alter Table SADTL_RECIPACT_TMP nologging';

      --获取各个渠道交易对手
      --网银转出
      proc_recip_wy_out (i_workdate);
      --超级网银行
      proc_recip_2d_wy (i_workdate);
      --网银转入
      proc_recip_wy_in (i_workdate);

      --B2C
      proc_recip_B2C (i_workdate);

      --POS
      proc_recip_POS (i_workdate);

      --代发工资
      proc_recip_salary (i_workdate);


      COMMIT;
   END;

   --网银转出
   PROCEDURE proc_recip_wy_out (i_workdate IN VARCHAR2)
   IS
   BEGIN
      --此对手数据源基本全时段有数据，所以不用特别判断时间
      INSERT INTO SADTL_RECIPACT_TMP (Busidate,
                                      Busitime,
                                      ACCNO,
                                      AMOUNT,
                                      CURRTYPE,
                                      drcrf,
                                      Recipact,
                                      RBNKNO,
                                      RBNKNAME,
                                      RSAVNAME,
                                      fee,
                                      BAKMSG,
                                      CHANGSUO,
                                      DATASOURCE)
         SELECT SUBSTR (transtime, 1, 8),
                   SUBSTR (transtime, 9, 2)
                || '.'
                || SUBSTR (transtime, 11, 2)
                || '.'
                || SUBSTR (transtime, 13, 2),
                a.paycardno,
                a.payamt,
                CASE
                   WHEN a.CURTYPE = 'RMB' THEN 1
                   WHEN a.CURTYPE = 'USD' THEN 14
                   WHEN a.CURTYPE = 'HKD' THEN 13
                   ELSE 0
                END
                   CURRTYPE,
                1 DRCRF,
                recacctno Recipact,
                recAREACODE || '(工行地区号)',
                NULL, --PCK_CREATE_SADTL_PUB.Get_zoneno_name(recAREACODE)||'(工行地区名称)',没有info.nase_nthpazon_all
                NULL, --PCK_CREATE_SADTL_PUB.Get_Accname(recacctno),没有Cmapp.pras_pthsasub
                fee,
                payuse,
                '网银' CHANGSUO,
                'SDA_S_EFRMLGOT' DATASOURCE
           FROM SDA_S_EFRMLGOT a
          WHERE a.transtime BETWEEN i_workdate || '000000'
                                AND i_workdate || '235959';

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'proc_recip_wy_out',
                                          SUBSTR (SQLERRM, 1, 100));
         RAISE;
   END;


   PROCEDURE proc_recip_2d_wy (i_workdate IN VARCHAR2)
   IS
   BEGIN
      IF i_workdate <= '20120929'
      THEN
         RETURN;
      END IF;



      --网银跨行清算系统支付业务借报清单文件
      INSERT INTO SADTL_RECIPACT_TMP (Busidate,
                                      Busitime,
                                      Timestmp,
                                      ACCNO,
                                      AMOUNT,
                                      TELLERNO,
                                      Trxsqno,
                                      drcrf,
                                      Rbnkno,
                                      Recipact,
                                      rsavname,
                                      BAKMSG,
                                      CHANGSUO,
                                      DATASOURCE,
                                      fee)
         SELECT TRADATE,
                SUBSTR (LSTIME, 12, 8),
                LSTIME,
                DECODE (draccno, NULL, craccno, draccno),
                MSGAMT,
                TYPTLNO,
                MSGSERNO,
                DECODE (draccno, NULL, 2, 1),
                DECODE (draccno, NULL, SBNKCODE, RBNKCODE),
                DECODE (draccno, NULL, SACCNO, RACCNO),
                DECODE (draccno, NULL, SACCNAME, RACCNAME),
                SUBSTR (MSGMARK, 1, 50),
                '超级网银借报',
                'SCP_S_SFAWDST',
                FEEAMT
           FROM SCP_S_SFAWDST a
          WHERE     a.TRADATE = TO_DATE (i_workdate, 'yyyymmdd')
                AND a.CLRDATE <> TO_DATE ('19000101', 'yyyymmdd'); --前期调查，这个条件认为是成功的


      --网银跨行清算系统支付业务贷报清单文件
      INSERT INTO SADTL_RECIPACT_TMP (Busidate,
                                      Busitime,
                                      Timestmp,
                                      ACCNO,
                                      AMOUNT,
                                      TELLERNO,
                                      Trxsqno,
                                      drcrf,
                                      Rbnkno,
                                      Recipact,
                                      rsavname,
                                      BAKMSG,
                                      CHANGSUO,
                                      DATASOURCE,
                                      fee)
         SELECT TRADATE,
                SUBSTR (LSTIME, 12, 8),
                LSTIME,
                DECODE (draccno, NULL, craccno, draccno),
                MSGAMT,
                TYPTLNO,
                MSGSERNO,
                DECODE (draccno, NULL, 2, 1),
                DECODE (draccno, NULL, SBNKCODE, RBNKCODE),
                DECODE (draccno, NULL, SACCNO, RACCNO),
                DECODE (draccno, NULL, SACCNAME, RACCNAME),
                SUBSTR (MSGMARK, 1, 50),
                '超级网银贷报',
                'SCP_S_SFAWCST',
                FEEAMT
           FROM SCP_S_SFAWCST a
          WHERE     a.TRADATE = TO_DATE (i_workdate, 'yyyymmdd')
                AND a.CLRDATE <> TO_DATE ('19000101', 'yyyymmdd'); --前期调查，这个条件认为是成功的


      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'proc_recip_2d_wy',
                                          SUBSTR (SQLERRM, 1, 100));

         RAISE;
   END;

   --网银转入
   PROCEDURE proc_recip_wy_in (i_workdate IN VARCHAR2)
   IS
   BEGIN
      INSERT INTO SADTL_RECIPACT_TMP (Busidate,
                                      Busitime,
                                      ACCNO,
                                      AMOUNT,
                                      CURRTYPE,
                                      drcrf,
                                      Recipact,
                                      RBNKNO,
                                      RBNKNAME,
                                      RSAVNAME,
                                      fee,
                                      BAKMSG,
                                      CHANGSUO,
                                      DATASOURCE)
         SELECT SUBSTR (transtime, 1, 8),
                   SUBSTR (transtime, 9, 2)
                || '.'
                || SUBSTR (transtime, 11, 2)
                || '.'
                || SUBSTR (transtime, 13, 2),
                a.recacctno,
                a.payamt,
                CASE
                   WHEN a.CURTYPE = 'RMB' THEN 1
                   WHEN a.CURTYPE = 'USD' THEN 14
                   WHEN a.CURTYPE = 'HKD' THEN 13
                   ELSE 0
                END
                   CURRTYPE,
                2 DRCRF,
                paycardno Recipact,
                PAYAREACODE || '(工行地区号)',
                NULL, --PCK_CREATE_SADTL_PUB.Get_zoneno_name (PAYAREACODE)|| '(工行地区名称)',
                NULL,          --PCK_CREATE_SADTL_PUB.Get_Accname (paycardno),
                fee,
                payuse,
                '网银' CHANGSUO,
                'SDA_S_EFRMLGIN' DATASOURCE
           FROM SDA_S_EFRMLGIN a
          WHERE a.transtime BETWEEN i_workdate || '000000'
                                AND i_workdate || '235959';

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'proc_recip_wy_in',
                                          SUBSTR (SQLERRM, 1, 100));

         RAISE;
   END;

   --B2C
   PROCEDURE proc_recip_B2C (i_workdate IN VARCHAR2)
   IS
   --v_cnt number;
   BEGIN
      IF i_workdate <= '20090101'
      THEN
         RETURN;
      END IF;

      INSERT INTO SADTL_RECIPACT_TMP (busidate,
                                      busitime,
                                      accno,
                                      amount,
                                      currtype,
                                      drcrf,
                                      recipact,
                                      rsavname,
                                      changsuo,
                                      BAKMSG,
                                      datasource)
         SELECT SUBSTR (transtime, 1, 8),
                   SUBSTR (transtime, 9, 2)
                || '.'
                || SUBSTR (transtime, 11, 2)
                || '.'
                || SUBSTR (transtime, 13, 2),
                a.payacctno accno,
                TO_NUMBER (a.PAYAMT) amount,
                1 currtype,
                1 drcrf,
                a.RECACCTNO recipact,
                a.MERNAME rsavname,
                'B2C' changsuo,
                '订单号：' || a.ORDERNO inmsg,
                'SDA_S_EFMERROT' DATASOURCE
           FROM SDA_S_EFMERROT a
          WHERE SUBSTR (transtime, 1, 8) BETWEEN i_workdate AND i_workdate;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'proc_recip_b2c',
                                          SUBSTR (SQLERRM, 1, 100));
         RAISE;
   END;

   --POS 仅有刷我行POS的对手信息，其他待补充
   PROCEDURE proc_recip_POS (i_workdate IN VARCHAR2)
   IS
   BEGIN
      INSERT INTO SADTL_RECIPACT_TMP (busidate,
                                      busitime,
                                      accno,
                                      amount,
                                      currtype,
                                      drcrf,
                                      recipact,
                                      rsavname,
                                      changsuo,
                                      BAKMSG,
                                      datasource)
         SELECT /*+index(a)*/
               TO_CHAR (WORKDATE, 'YYYYMMDD'),
                   SUBSTR (worktime, 9, 2)
                || '.'
                || SUBSTR (worktime, 11, 2)
                || '.'
                || SUBSTR (worktime, 13, 2),
                a.CARDDENO accno,
                TO_NUMBER (a.TRXAMT) amount,
                1 currtype,
                1 drcrf,
                a.TYDWACC recipact,
                a.TYDWNAME rsavname,
                'POS' changsuo,
                '特约单位编号：' || a.TYDWNO inmsg,
                'BCA_S_BFHINPOS' DATASOURCE
           FROM BCA_S_BFHINPOS a
          WHERE a.WORKDATE = TO_DATE (i_workdate, 'yyyymmdd');

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'proc_recip_pos',
                                          SUBSTR (SQLERRM, 1, 100));
         RAISE;
   END;

   --代发工资
   PROCEDURE proc_recip_salary (i_workdate IN VARCHAR2)
   IS
   BEGIN
      INSERT INTO SADTL_RECIPACT_TMP (busidate,
                                      accno,
                                      amount,
                                      currtype,
                                      tellerno,
                                      drcrf,
                                      recipact,
                                      changsuo,
                                      BAKMSG,
                                      datasource)
         SELECT TO_CHAR (workdate, 'yyyymmdd'),
                a.recacc accno,
                TO_NUMBER (a.amount) amount,
                1 currtype,
                tellerno,
                2 drcrf,
                a.payacc recipact,
                '代发工资' changsuo,
                'TRXTYPE:' || TRXTYPE || ',DATASOURCE:' || DATASOURCE inmsg,
                'Bdm_Ev_Agtsal' DATASOURCE
           FROM Bdm_Ev_Agtsal a
          WHERE     A.workdate = TO_DATE (i_workdate, 'yyyymmdd')
                AND A.TRXTYPE = 1;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'proc_recip_salary',
                                          SUBSTR (SQLERRM, 1, 100));
         RAISE;
   END;
END PCK_CREATE_RECIPACT;
//



create or replace package body PCK_CREATE_RECIPACT_2015 is

  procedure init_recipact_main(i_workdate in varchar2) --yyyymmdd
  is
  begin

      Execute Immediate 'Truncate Table SADTL_recipact_tmp_2015';
          Execute Immediate 'Alter Table   SADTL_recipact_tmp_2015 nologging';




  --获取各个渠道交易对手
  --网银转出
   proc_recip_wy_out(i_workdate );

  --超级网银行
   proc_recip_2d_wy(i_workdate );


  --网银转入
   proc_recip_wy_in(i_workdate     );

  --B2C
   proc_recip_B2C(i_workdate     );
   --POS
   proc_recip_POS(i_workdate     );

   --代发工资
   proc_recip_salary(i_workdate    );

   --代理业务
   proc_recip_dlyy(i_workdate    );


   --现代化支付旧
   proc_recip_ibps(i_workdate    );

   --现代化支付新
   proc_recip_sffh(i_workdate    );

  --新同城
   proc_recip_ncps(i_workdate   );

  --自助终端
   proc_recip_fbec(i_workdate  );

  --atm
   proc_recip_atm(i_workdate     );

  --深港票交
   proc_recip_sgbill(i_workdate     );


    commit;
  end;




  --网银转出
  procedure proc_recip_wy_out(i_workdate in varchar2)
   is
   begin

      --此对手数据源基本全时段有数据，所以不用特别判断时间


      insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            ACCNO,
            AMOUNT,
            CURRTYPE,
            drcrf    ,
            Recipact  ,
            RBNKNO  ,
            RBNKNAME ,
            RSAVNAME,
            fee     ,
            BAKMSG,
            CHANGSUO,
            DATASOURCE
      )
       SELECT
           substr(transtime,1,8),
           substr(transtime,9,2)||'.'||substr(transtime,11,2)||'.'||substr(transtime,13,2),
           a.pay_cardno,
           a.payamt,
           case when a.CURTYPE='RMB' THEN 1
                WHEN a.CURTYPE='USD' THEN 14
                WHEN a.CURTYPE='HKD' THEN 13
                ELSE 0
           END CURRTYPE ,
           1 DRCRF,
           rec_acctno Recipact,
           rec_AREACODE||'(工行地区号)',
           PCK_CREATE_SADTL_PUB_2015.Get_zoneno_name(rec_AREACODE)||'(工行地区名称)',

           PCK_CREATE_SADTL_PUB_2015.Get_Accname(rec_acctno),
           fee    ,
           payuse,
           '网银'CHANGSUO,
           'wy_person_remit_log_out' DATASOURCE

           FROM
                   wy_person_remit_log_out   a

             WHERE a.transtime BETWEEN i_workdate||'000000' AND i_workdate||'235959';

     commit;


     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_wy_out',substr( sqlerrm,1,100));
        raise;
   end;

  procedure proc_recip_2d_wy(i_workdate in varchar2)
   is
   --v_cnt number;
   begin

    if i_workdate<='20120929' then
      return;
    end if;



      --网银跨行清算系统支付业务借报清单文件
      insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            Timestmp,
            ACCNO,
            AMOUNT,
            TELLERNO,
            Trxsqno ,
            drcrf ,
            Rbnkno,
            Recipact,
            rsavname,
            BAKMSG   ,
            CHANGSUO,
            DATASOURCE,
            fee


      )
       SELECT
           to_char(TRADATE,'yyyymmdd'),
           SUBSTR(LSTIME,12,8),
           LSTIME,
           decode(draccno,null,craccno,draccno),
           MSGAMT,
           TYPTLNO,
           MSGSERNO,
           decode(draccno,null,2,1),
           decode(draccno,null,SBNKCODE,RBNKCODE),
           decode(draccno,null,SACCNO,RACCNO),
           decode(draccno,null,SACCNAME,RACCNAME),
           SUBSTR(MSGMARK,1,50),
           '超级网银借报',
           'rpt_sfawdst',
           FEEAMT

           FROM
                   rpt_sfawdst  a

             WHERE a.TRADATE =to_date(i_workdate, 'yyyymmdd')
               and a.CLRDATE <> to_date('19000101','yyyymmdd');--前期调查，这个条件认为是成功的


      --网银跨行清算系统支付业务借报清单文件
  /*
      insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            Timestmp,
            ACCNO,
            AMOUNT,
            TELLERNO,
            Trxsqno ,
            drcrf ,
            Rbnkno,
            Recipact,
            rsavname,
            BAKMSG   ,
            CHANGSUO,
            DATASOURCE,
            fee


      )
       SELECT
           to_char(TRADATE,'yyyymmdd'),
           SUBSTR(LSTIME,12,8),
           LSTIME,
           decode(draccno,null,craccno,draccno),
           MSGAMT,
           TYPTLNO,
           MSGSERNO,
           decode(draccno,null,2,1),
           decode(draccno,null,SBNKCODE,RBNKCODE),
           decode(draccno,null,SACCNO,RACCNO),
           decode(draccno,null,SACCNAME,RACCNAME),
           MSGMARK,
           '超级网银借报',
           'rpt_sfawdst',
           FEEAMT

           FROM
                   rpt_sfawdst  a

             WHERE a.TRADATE =to_date(i_workdate, 'yyyymmdd')
               and a.CLRDATE <> to_date('19000101','yyyymmdd');--前期调查，这个条件认为是成功的
    */


      --网银跨行清算系统支付业务贷报清单文件
      insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            Timestmp,
            ACCNO,
            AMOUNT,
            TELLERNO,
            Trxsqno ,
            drcrf ,
            Rbnkno,
            Recipact,
            rsavname,
            BAKMSG   ,
            CHANGSUO,
            DATASOURCE,
            fee


      )
       SELECT
           to_char(TRADATE,'yyyymmdd'),
           SUBSTR(LSTIME,12,8),
           LSTIME,
           decode(draccno,null,craccno,draccno),
           MSGAMT,
           TYPTLNO,
           MSGSERNO,
           decode(draccno,null,2,1),
           decode(draccno,null,SBNKCODE,RBNKCODE),
           decode(draccno,null,SACCNO,RACCNO),
           decode(draccno,null,SACCNAME,RACCNAME),
           SUBSTR(MSGMARK,1,50),
           '超级网银贷报',
           'rpt_sfawcst',
           FEEAMT

           FROM
                   rpt_sfawcst  a
             WHERE a.TRADATE =to_date(i_workdate, 'yyyymmdd')
               and a.CLRDATE <> to_date('19000101','yyyymmdd');--前期调查，这个条件认为是成功的

/*
      insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            Timestmp,
            ACCNO,
            AMOUNT,
            TELLERNO,
            Trxsqno ,
            drcrf ,
            Rbnkno,
            Recipact,
            rsavname,
            BAKMSG   ,
            CHANGSUO,
            DATASOURCE,
            fee


      )
       SELECT
           to_char(TRADATE,'yyyymmdd'),
           SUBSTR(LSTIME,12,8),
           LSTIME,
           decode(draccno,null,craccno,draccno),
           MSGAMT,
           TYPTLNO,
           MSGSERNO,
           decode(draccno,null,2,1),
           decode(draccno,null,SBNKCODE,RBNKCODE),
           decode(draccno,null,SACCNO,RACCNO),
           decode(draccno,null,SACCNAME,RACCNAME),
           MSGMARK,
           '超级网银贷报',
           'rpt_sfawcst',
           FEEAMT

           FROM
                   rpt_sfawcst  a
             WHERE a.TRADATE =to_date(i_workdate, 'yyyymmdd')
               and a.CLRDATE <> to_date('19000101','yyyymmdd');--前期调查，这个条件认为是成功的
*/

     commit;
     EXCEPTION
       WHEN OTHERS THEN

        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_2d_wy',substr( sqlerrm,1,100));

        raise;
   end;

  --网银转入
  procedure proc_recip_wy_in(i_workdate in varchar2   )
   is
   begin


       insert into SADTL_recipact_tmp_2015
      (

            Busidate,
            Busitime,
            ACCNO,
            AMOUNT,
            CURRTYPE,
            drcrf    ,
            Recipact  ,
            RBNKNO  ,
            RBNKNAME ,
            RSAVNAME,
            fee     ,
            BAKMSG ,
            CHANGSUO,
            DATASOURCE
      )
       SELECT
           substr(transtime,1,8),
           substr(transtime,9,2)||'.'||substr(transtime,11,2)||'.'||substr(transtime,13,2),
           a.rec_acctno,
           a.payamt,
           case when a.CURTYPE='RMB' THEN 1
                WHEN a.CURTYPE='USD' THEN 14
                WHEN a.CURTYPE='HKD' THEN 13
                ELSE 0
           END CURRTYPE ,
           2 DRCRF,
           pay_cardno Recipact,
           PAY_AREACODE||'(工行地区号)',
           PCK_CREATE_SADTL_PUB_2015.Get_zoneno_name(PAY_AREACODE)||'(工行地区名称)',
           PCK_CREATE_SADTL_PUB_2015.Get_Accname(pay_cardno),
           fee    ,
           payuse ,
           '网银'CHANGSUO,
           'wy_person_remit_log_in' DATASOURCE

           FROM
                   wy_person_remit_log_in a
             WHERE a.transtime BETWEEN  i_workdate||'000000' AND      i_workdate||'235959';

     commit;


     EXCEPTION
       WHEN OTHERS THEN

        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_wy_in',substr( sqlerrm,1,100));

        raise;
     end;


  --B2C
  procedure proc_recip_B2C(i_workdate in varchar2  )
   is
     --v_cnt number;
   begin

     if i_workdate<='20090101' then
        return;
     end if;

       insert into SADTL_recipact_tmp_2015
      (
            busidate,
            busitime,
            accno,
            amount,
            currtype,
            drcrf,
            recipact,
            rsavname,
            changsuo,
            BAKMSG,
            datasource
      )

       SELECT
           substr(transtime,1,8),
           substr(transtime,9,2)||'.'||substr(transtime,11,2)||'.'||substr(transtime,13,2),
           a.payacctno accno,
           to_number(a.PAYAMT) amount,
           1 currtype,
           1 drcrf,
           a.RECACCTNO recipact,
           a.MERNAME rsavname,
           'B2C' changsuo,
           '订单号：'||a.ORDERNO inmsg,
           'wy_mer_report_out_b2c' DATASOURCE

           FROM
                   wy_mer_report_out_b2c a
             WHERE a.PMPFDATADATE BETWEEN i_workdate  and i_workdate;

     commit;


     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_b2c',substr( sqlerrm,1,100));
        raise;
     end;
  --POS 仅有刷我行POS的对手信息，其他待补充
  procedure proc_recip_POS(i_workdate in varchar2    )
   is
   begin


     insert into SADTL_recipact_tmp_2015
      (
            busidate,
            busitime,
            accno,
            amount,
            currtype,
            drcrf,
            recipact,
            rsavname,
            changsuo,
            BAKMSG,
            datasource
      )

       SELECT/*+index( a RPT_BFHINPOS_INB01)*/
           to_char(WORKDATE,'YYYYMMDD'),
           substr(worktime,9,2)||'.'||substr(worktime,11,2)||'.'||substr(worktime,13,2),
           a.CARDDRNO accno,
           to_number(a.TRXAMT) amount,
           1 currtype,
           1 drcrf,
           a.TYDWACC recipact,
           a.TYDWNAME rsavname,
           'POS' changsuo,
           '特约单位编号：'||a.TYDWNO inmsg,
           'rpt_bfhinpos' DATASOURCE
           FROM
                   Rpt_Bfhinpos_Nova157 a
             WHERE a.WORKDATE   = to_date(i_workdate, 'yyyymmdd') ;


     insert into SADTL_recipact_tmp_2015
      (
            busidate,
            busitime,
            accno,
            amount,
            currtype,
            drcrf,
            recipact,
            rsavname,
            changsuo,
            BAKMSG,
            datasource
      )

       SELECT/*+index( a RPT_BFHINPOS_INDB01)*/
           to_char(WORKDATE,'YYYYMMDD'),
           substr(worktime,9,2)||'.'||substr(worktime,11,2)||'.'||substr(worktime,13,2),
           a.CARDDRNO accno,
           to_number(a.TRXAMT) amount,
           1 currtype,
           1 drcrf,
           a.TYDWACC recipact,
           a.TYDWNAME rsavname,
           'POS' changsuo,
           '特约单位编号：'||a.TYDWNO inmsg,
           'rpt_bfhinpos' DATASOURCE

           FROM
                   Rpt_Bfhinpos_Nova165 a
             WHERE a.WORKDATE = to_date(i_workdate, 'yyyymmdd');




     insert into SADTL_recipact_tmp_2015
      (
            busidate,
            busitime,
            accno,
            amount,
            currtype,
            drcrf,
            recipact,
            rsavname,
            changsuo,
            BAKMSG,
            datasource
      )

       SELECT/*+index( a RPT_BFHINPOS_INDB03)*/
           to_char(WORKDATE,'YYYYMMDD'),
           substr(worktime,9,2)||'.'||substr(worktime,11,2)||'.'||substr(worktime,13,2),
           a.CARDDRNO accno,
           to_number(a.TRXAMT) amount,
           1 currtype,
           1 drcrf,
           a.TYDWACC recipact,
           a.TYDWNAME rsavname,
           'POS' changsuo,
           '特约单位编号：'||a.TYDWNO inmsg,
           'rpt_bfhinpos' DATASOURCE

           FROM
                   Rpt_Bfhinpos a
             WHERE a.WORKDATE = to_date(i_workdate, 'yyyymmdd');



     commit;

     EXCEPTION
       WHEN OTHERS THEN
       PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_pos',substr( sqlerrm,1,100));
       raise;
     end;

   --代发工资
  procedure proc_recip_salary(i_workdate in varchar2  )
  is
  begin

       insert into SADTL_recipact_tmp_2015
      (
            busidate,
            accno,
            amount,
            currtype,
            tellerno,
            drcrf,
            recipact,
            changsuo,
            BAKMSG,
            datasource

      )

       SELECT
           to_char(workdate,'yyyymmdd'),
           a.recacc accno,
           to_number(a.amount) amount,
           1 currtype,
            tellerno,
           2 drcrf,

           a.payacc recipact,

           '代发工资' changsuo,

           'TRXTYPE:' || TRXTYPE  || ',DATASOURCE:' ||DATASOURCE  inmsg,
           'Bdm_Ev_Agtsal' DATASOURCE


           FROM
                   BDM_EV_AGTSAL a
             WHERE A.workdate =    to_date(i_workdate, 'yyyymmdd')
               and A.TRXTYPE=1;
                /*只能取等于1的 ，表示代发工资，实际上，这个表CMMID.BDM_EV_AGTSAL的加工过程是由问题的 ,没有判断代理业务的借贷方向就直接把代理业务的对公帐户放在付方帐号 */



    commit;


     EXCEPTION
       WHEN OTHERS THEN
       PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_salary',substr( sqlerrm,1,100));
       raise;
     end;

   --代理业务
  procedure proc_recip_dlyy(i_workdate in varchar2  )
  is
  begin




       insert into SADTL_recipact_tmp_2015
      (
            busidate,
            accno,
            amount,
            currtype,
            drcrf,
            recipact,
            changsuo,
            BAKMSG,
            datasource

      )

       SELECT

           to_char(Paydate,'yyyymmdd'),
           a.accno accno,
           to_number(a.amount1) amount,
           1 currtype,
           a.drcrf,
          '' recipact,
           '批量代理业务' changsuo,
             AGESNAME  || ',' || AGTNAME  || ',' || AGTSMNM inmsg,
           'agt_pfagpras' DATASOURCE


           FROM
                   Agt_Pfagpras a,

                   Nase_Nthpaagt c
             WHERE A.Paydate = to_date(i_workdate, 'yyyymmdd')
               and a.Status = 2 And a.Recordf = '2' --成功的  明细的
               and  a.Agentno = c.Agentno(+); --取代理业务名称





       insert into SADTL_recipact_tmp_2015
      (
            busidate,
            accno,
            amount,
            currtype,
            drcrf,
            recipact,
            changsuo,
            BAKMSG,
            datasource

      )

       SELECT

           to_char(Paydate,'yyyymmdd'),
           to_char(Paydate,'yyyymmdd'),
           to_number(a.amount1) amount,
           1 currtype,
           a.drcrf,

          '' recipact,

           '批量代理业务' changsuo,

             AGESNAME  || ',' || AGTNAME  || ',' || AGTSMNM inmsg,
           'agt_pfagpras' DATASOURCE


           FROM
                   Agt_Pfagpras_new a,

                   Nase_Nthpaagt c
             WHERE A.Paydate =                   to_date(i_workdate, 'yyyymmdd')
               and a.Status = 2 And a.Recordf = '2' --成功的  明细的
               and  a.Agentno = c.Agentno(+) ; --取代理业务名称



    commit;


     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_dlyy',substr( sqlerrm,1,100));
        raise;
     end;


   --现代化支付旧
  procedure proc_recip_ibps(i_workdate in varchar2    )
  is
  --v_cnt number;
  begin

    if i_workdate<20030421 or 20030421> 20100625 then
      return;
    end if;

      --旧现代化支付发报
      insert into SADTL_recipact_tmp_2015
      (

            Busidate
            ,Busitime
            ,ACCNO
            ,AMOUNT
            ,CURRTYPE
            ,TELLERNO
            ,Trxsqno
            ,drcrf
            --,Ropbnkno
            ,RBNKNO
            ,RBNKNAME
            ,Recipact
            ,rsavname
            ,BAKMSG
            ,trxsqnob
            ,trxsqnos
            --,BAKMSG
            ,changsuo
            ,inMSG
            ,DATASOURCE

      )
       SELECT
          to_char(SNDDATE,'yyyymmdd')
          ,substr(worktime,1,2)||'.'||substr(worktime,3,2)||'.'||substr(worktime,5,2)
          ,a.PAYACCT
          ,amt
          ,case when a.cursign='RMB' THEN 1
                WHEN a.cursign='USD' THEN 14
                WHEN a.cursign='HKD' THEN 13

                WHEN a.cursign='001' THEN 1
                WHEN a.cursign='013' THEN 13
                WHEN a.cursign='014' THEN 14  --这个表不同的时间段记录的币种不一致

                          ELSE 0
                     END CURRTYPE
          ,to_number(entoperno)
          ,to_number(hostserbno1)
          ,1 drcrf
          --,rcvobnkcode
          ,RCVBNKCODE
          ,rcvbnkname
          ,rcvacct
          ,rcvname
          ,strinfo||abstract
          ,hostserbno1
          ,hostsersno1
          --,abstract
          ,'现代化支付系统'
          ,'现代化支付发报'
          ,'ibps_sndlog'

           FROM
                   ibps_sndlog  a
             WHERE a.SNDDATE = to_date(i_workdate,'yyyymmdd')

               AND a.hdlflag='51';


      --旧现代化支付收报
      insert into SADTL_recipact_tmp_2015
      (
            Busidate
            ,Busitime
            ,ACCNO
            ,AMOUNT
            ,CURRTYPE
            ,TELLERNO
            ,Trxsqno
            ,drcrf
            --,Ropbnkno
            ,RBNKNO
            ,RBNKNAME
            ,Recipact
            ,rsavname
            ,BAKMSG
            ,trxsqnob
            ,trxsqnos
            ,changsuo
            ,INMSG
            ,DATASOURCE
     )
       SELECT
          to_char(RCVDATE,'yyyymmdd')
          ,substr(worktime,1,2)||'.'||substr(worktime,3,2)||'.'||substr(worktime,5,2)
          ,rcvacct
          ,amt
          ,case when a.cursign='RMB' THEN 1
                          WHEN a.cursign='USD' THEN 14
                          WHEN a.cursign='HKD' THEN 13

                WHEN a.cursign='001' THEN 1
                WHEN a.cursign='013' THEN 13
                WHEN a.cursign='014' THEN 14


                          ELSE 0
                     END CURRTYPE
          ,to_number(rcvoperno)
          ,to_number(hostserbno1)
          ,2 drcrf
          --,payobnkcode
          ,sndbnkcode
          ,sndbnkname
          ,payacct
          ,payname
          ,strinfo
          ,hostserbno1
          ,hostsersno1
          ,'现代化支付系统'
          ,'现代化支付收报'
          ,'ibps_rcvlog'
           FROM
                   ibps_rcvlog  a
            WHERE a.RCVDATE = to_date(i_workdate,'yyyymmdd')

               AND a.hdlflag='51';

    commit;


     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_ibps',substr( sqlerrm,1,100));

        raise;
     end;

   --现代化支付新
   --
  procedure proc_recip_sffh(i_workdate in varchar2   )
  is
  --v_cnt number;
  begin


     --总行现代化支付大额2010-6-28及以后的数据
     if i_workdate<20100628 then
       return;
     end if;

      --总行现代化支付大额
      insert into SADTL_recipact_tmp_2015
      (
            Busidate
            ,Busitime
            ,Timestmp
            ,ACCNO
            ,AMOUNT
            ,CURRTYPE
            --,Cashexf
            ,TRXOCDE
            ,TELLERNO
            ,Trxsqno
            ,drcrf
            --,Ropbnkno
            ,RBNKNO
            ,RBNKNAME
            ,Recipact
            ,rsavname
            ,fee
            --,BAKMSG
            ,trxsqnob
            ,trxsqnos
            ,BAKMSG
            ,changsuo
            ,INMSG
            ,DATASOURCE

      )
       SELECT
          to_char(workdate,'yyyymmdd')
          ,substr(JLTIMESTMP,12,2)||'.'||substr(JLTIMESTMP,15,2)||'.'||substr(JLTIMESTMP,18,2)
          ,JLTIMESTMP


          ,decode(JLSNDREVF,5,JLSAC,6,JLRAC,'0')

          ,JLMSGAMT
          ,JLCURRTYPE
          --,JLSCASHEXF
          ,JLTRXCODE
          ,JLTELLERNO
          ,JLTRXSQNB


        ,decode(JLSNDREVF,5,1,6,2,0)  --5：发报6：收报
        --,decode(JLSNDREVF,5,RGRBNKCODE,6,RGSBNKCODE,0)
        ,decode(JLSNDREVF,5,RGOBNKCODE,6,RGBANKCODE,0)
        ,decode(JLSNDREVF,5,JLOBNKNAME,6,JLBANKNAME,0)
        ,decode(JLSNDREVF,5,RGRACCNO,6,RGSACCNO,null)
        ,decode(JLSNDREVF,5,substr(RGRACCNAME,1,50),6,substr(RGSACCNAME,1,50),null)


          ,JLFEEAMT
          --,RGMSGMARK
          ,JLTRXSQNB
          ,JLTRXSQNS
          ,JLSUMMARY
          ,'总行现代化支付'
          ,'总行现代化支付大额'
          ,'RPT_SFFHIPM'

           FROM
                   RPT_SFFHIPM  a
             WHERE a.WORKDATE =  to_date(i_workdate, 'yyyymmdd')


               and a.JLSTATUS in (12,38,37,36);





      --20141113 b 修改2代


      --总行现代化支付大额
      insert into SADTL_recipact_tmp_2015
      (
            Busidate
            ,Busitime
            ,Timestmp
            ,ACCNO
            ,AMOUNT
            ,CURRTYPE
            --,Cashexf
            ,TRXOCDE
            ,TELLERNO
            ,Trxsqno
            ,drcrf
            --,Ropbnkno
            ,RBNKNO
            ,RBNKNAME
            ,Recipact
            ,rsavname
            ,fee
            --,BAKMSG
            ,trxsqnob
            ,trxsqnos
            ,BAKMSG
            ,changsuo
            ,INMSG
            ,DATASOURCE

      )
       SELECT
          to_char(workdate,'yyyymmdd')
          ,substr(JLTIMESTMP,12,2)||'.'||substr(JLTIMESTMP,15,2)||'.'||substr(JLTIMESTMP,18,2)
          ,JLTIMESTMP


          ,decode(JLSNDREVF,5,JLSAC,6,JLRAC,'0')

          ,JLMSGAMT
          ,JLCURRTYPE
          --,JLSCASHEXF
          ,JLTRXCODE
          ,JLTELLERNO
          ,JLTRXSQNB


        ,decode(JLSNDREVF,5,1,6,2,0)  --5：发报6：收报
        --,decode(JLSNDREVF,5,RGRBNKCODE,6,RGSBNKCODE,0)
        ,decode(JLSNDREVF,5,RGOBNKCODE,6,RGBANKCODE,0)
        ,decode(JLSNDREVF,5,JLOBNKNAME,6,JLBANKNAME,0)
        ,decode(JLSNDREVF,5,RGRACCNO,6,RGSACCNO,null)
        ,decode(JLSNDREVF,5,substr(RGRACCNAME,1,50),6,substr(RGSACCNAME,1,50),null)



          ,JLFEEAMT
          --,substr(RGMSGMARK,1,50)
          ,JLTRXSQNB
          ,JLTRXSQNS
          ,JLSUMMARY
          ,'总行现代化支付'
          ,'总行现代化支付大额'
          ,'RPT_SFFHIPM'

           FROM
                   rpt_sffhipma  a
             WHERE a.WORKDATE =    to_date(i_workdate, 'yyyymmdd')

               and a.JLSTATUS in (12,38,37,36);



      --20141113 e 修改2代


      --总行现代化支付小额
      insert into SADTL_recipact_tmp_2015
      (
          Busidate
          ,Busitime
          ,Timestmp
          ,ACCNO
          ,AMOUNT
          ,CURRTYPE
         -- ,Cashexf
          ,TRXOCDE
          ,TELLERNO
          ,Trxsqno
          ,drcrf
          --,Ropbnkno
          ,RBNKNO
          ,RBNKNAME
          ,Recipact
          ,rsavname
          ,fee
          --,notes
          ,trxsqnob
          ,trxsqnos
          ,BAKMSG
          ,changsuo
          ,INMSG
          ,DATASOURCE
     )
       SELECT
        to_char(workdate,'yyyymmdd')
        ,substr(JLTIMESTMP,12,2)||'.'||substr(JLTIMESTMP,15,2)||'.'||substr(JLTIMESTMP,18,2)
        ,JLTIMESTMP

        ,decode(JLSNDREVF,5,JLSAC,6,JLRAC,'0')

        ,JLMSGAMT
        ,JLCURRTYPE
       -- ,JLSCASHEXF
        ,JLTRXCODE
        ,JLTELLERNO
        ,JLTRXSQNB

        ,decode(JLSNDREVF,5,1,6,2,0)  --5：发报6：收报
       -- ,decode(JLSNDREVF,5,RGRBNKCODE,6,RGSBNKCODE,0)
        ,decode(JLSNDREVF,5,RGOBNKCODE,6,RGBANKCODE,0)
        ,decode(JLSNDREVF,5,JLOBNKNAME,6,JLBANKNAME,0)
        ,decode(JLSNDREVF,5,RGRACCNO,6,RGSACCNO,null)
        ,decode(JLSNDREVF,5,substr(RGRACCNAME,1,50),6,substr(RGSACCNAME,1,50) ,null)


        ,JLFEEAMT
       -- ,RGMSGMARK
        ,JLTRXSQNB
        ,JLTRXSQNS
        ,JLSUMMARY
        ,'总行现代化支付'
        ,'总行现代化支付小额'
        ,'rpt_sffhbpm'

           FROM
                   rpt_sffhbpm  a
             WHERE a.WORKDATE =   to_date(i_workdate, 'yyyymmdd')

               and a.JLSTATUS in (12,38,37,36,9); --9是小额的成功标志？


      --20141113  b  修改2代

      --总行现代化支付小额
      insert into SADTL_recipact_tmp_2015
      (
          Busidate
          ,Busitime
          ,Timestmp
          ,ACCNO
          ,AMOUNT
          ,CURRTYPE
          --,Cashexf
          ,TRXOCDE
          ,TELLERNO
          ,Trxsqno
          ,drcrf
          --,Ropbnkno
          ,RBNKNO
          ,RBNKNAME
          ,Recipact
          ,rsavname
          ,fee
         -- ,notes
          ,trxsqnob
          ,trxsqnos
          ,BAKMSG
          ,changsuo
          ,INMSG
          ,DATASOURCE
     )
       SELECT
        to_char(workdate,'yyyymmdd')
        ,substr(JLTIMESTMP,12,2)||'.'||substr(JLTIMESTMP,15,2)||'.'||substr(JLTIMESTMP,18,2)
        ,JLTIMESTMP

        ,decode(JLSNDREVF,5,JLSAC,6,JLRAC,'0')

        ,JLMSGAMT
        ,JLCURRTYPE
       -- ,JLSCASHEXF
        ,JLTRXCODE
        ,JLTELLERNO
        ,JLTRXSQNB

        ,decode(JLSNDREVF,5,1,6,2,0)  --5：发报6：收报
        --,decode(JLSNDREVF,5,RGRBNKCODE,6,RGSBNKCODE,0)
        ,decode(JLSNDREVF,5,RGOBNKCODE,6,RGBANKCODE,0)
        ,decode(JLSNDREVF,5,JLOBNKNAME,6,JLBANKNAME,0)
        ,decode(JLSNDREVF,5,RGRACCNO,6,RGSACCNO,null)
        ,decode(JLSNDREVF,5,substr(RGRACCNAME,1,50),6,substr(RGSACCNAME,1,50),null)



        ,JLFEEAMT
       -- ,substr(RGMSGMARK,1,50)
        ,JLTRXSQNB
        ,JLTRXSQNS
        ,JLSUMMARY
        ,'总行现代化支付'
        ,'总行现代化支付小额'
        ,'rpt_sffhbpm'

           FROM
                   rpt_sffhbpma  a
             WHERE a.WORKDATE = to_date(i_workdate, 'yyyymmdd')

               and a.JLSTATUS in (12,38,37,36,9); --9是小额的成功标志？


      --20141113  e  修改2代
    commit;


     EXCEPTION
       WHEN OTHERS THEN
       PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_sffh',substr( sqlerrm,1,100));

        raise;
     end;

  --新同城
  procedure proc_recip_ncps(i_workdate in varchar2  )

  is
   --v_cnt number;
   begin
     --新同城20051127及以后的数据
     if i_workdate<'20051127' then
       return;
     end if;

      --新同城往帐
      insert into SADTL_recipact_tmp_2015
      (
        Busidate
        ,Busitime
        ,Timestmp
        ,ACCNO
        ,AMOUNT
        ,CURRTYPE
        ,TELLERNO
        ,Trxsqno
        ,drcrf
        --,Ropbnkno
        --,Ropbnkname
        ,RBNKNO
        ,RBNKNAME
        ,Recipact
        ,rsavname
        ,fee
        ,BAKMSG
        ,trxsqnob
        ,trxsqnos
        ,changsuo
        ,INMSG
        ,DATASOURCE
      )
       SELECT
          workdate
          ,substr(TIMESTAMP,12,2)||'.'||substr(TIMESTAMP,15,2)||'.'||substr(TIMESTAMP,18,2)
          ,TIMESTAMP
          ,PACCNO
          ,amount
          ,to_number(CURRTYPE)
          ,to_number(teller)
          ,trxsqnob
          ,1 drcrf
         -- ,ROPBNKNO
          --,ROPBNKNAME
          ,RBNKNO
          ,RBNKNAME
          ,RACCNO
          ,RNAME
          ,fee
          ,substr(NOTES,1,50)
          ,trxsqnob
          ,trxsqnos
          ,'新同城'
          ,'新同城往帐'
          ,'ncps_snddtl'
           FROM
                       NCPS_SNDDTL_bak a
             WHERE a.WORKDATE=i_workdate

               and a.step='0000';


      insert into SADTL_recipact_tmp_2015
      (
        Busidate
        ,Busitime
        ,Timestmp
        ,ACCNO
        ,AMOUNT
        ,CURRTYPE
        ,TELLERNO
        ,Trxsqno
        ,drcrf
        --,Ropbnkno
        --,Ropbnkname
        ,RBNKNO
        ,RBNKNAME
        ,Recipact
        ,rsavname
        ,fee
        ,BAKMSG
        ,trxsqnob
        ,trxsqnos
        ,changsuo
        ,INMSG
        ,DATASOURCE
      )
       SELECT
          workdate
          ,substr(TIMESTAMP,12,2)||'.'||substr(TIMESTAMP,15,2)||'.'||substr(TIMESTAMP,18,2)
          ,TIMESTAMP
          ,PACCNO
          ,amount
          ,to_number(CURRTYPE)
          ,to_number(teller)
          ,trxsqnob
          ,1 drcrf
          --,ROPBNKNO
          --,ROPBNKNAME
          ,RBNKNO
          ,RBNKNAME
          ,RACCNO
          ,RNAME
          ,fee
          ,substr(NOTES,1,50)
          ,trxsqnob
          ,trxsqnos
          ,'新同城'
          ,'新同城往帐'
          ,'ncps_snddtl'
           FROM
                   NCPS_SNDDTL  a
             WHERE a.WORKDATE = i_workdate

               and a.step='0000';




      --新同城来帐
      insert into SADTL_recipact_tmp_2015
      (
          Busidate
          ,Busitime
          ,Timestmp
          ,ACCNO
          ,AMOUNT
          ,CURRTYPE
          ,TELLERNO
          ,Trxsqno
          ,drcrf
          --,Ropbnkno
          --,Ropbnkname
          ,RBNKNO
          ,RBNKNAME
          ,Recipact
          ,rsavname
          ,fee
          ,BAKMSG
          ,trxsqnob
          ,trxsqnos
          ,changsuo
          ,INMSG
          ,DATASOURCE
     )
       SELECT
          busidate
          ,substr(TIMESTAMP,12,2)||'.'||substr(TIMESTAMP,15,2)||'.'||substr(TIMESTAMP,18,2)
          ,TIMESTAMP
          ,RACCNO
          ,amount
          ,to_number(CURRTYPE)
          ,to_number(teller)
          ,trxsqnob
          ,2 drcrf
          --,POPBNKNO
          --,POPBNKNAME
          ,PBNKNO
          ,PBNKNAME
          ,PACCNO
          ,PNAME
          ,null fee
          ,substr(NOTES,1,50)
          ,trxsqnob
          ,trxsqnos
          ,'新同城'
          ,'新同城来帐'
          ,'ncps_rcvdtl'
           FROM
                   NCPS_RCVDTL_bak  a
            WHERE a.busidate =i_workdate

               and a.step in (
               '0221' ,  '0201'  ,   '0211'
               ) and length(RACCNO)<=19
               ;


      insert into SADTL_recipact_tmp_2015
      (
          Busidate
          ,Busitime
          ,Timestmp
          ,ACCNO
          ,AMOUNT
          ,CURRTYPE
          ,TELLERNO
          ,Trxsqno
          ,drcrf
          --,Ropbnkno
          --,Ropbnkname
          ,RBNKNO
          ,RBNKNAME
          ,Recipact
          ,rsavname
          ,fee
          ,BAKMSG
          ,trxsqnob
          ,trxsqnos
          ,changsuo
          ,INMSG
          ,DATASOURCE
     )
       SELECT
          busidate
          ,substr(TIMESTAMP,12,2)||'.'||substr(TIMESTAMP,15,2)||'.'||substr(TIMESTAMP,18,2)
          ,TIMESTAMP
          ,RACCNO
          ,amount
          ,to_number(CURRTYPE)
          ,to_number(teller)
          ,trxsqnob
          ,2 drcrf
          --,POPBNKNO
          --,POPBNKNAME
          ,PBNKNO
          ,PBNKNAME
          ,PACCNO
          ,PNAME
          ,fee
          ,substr(NOTES,1,50)
          ,trxsqnob
          ,trxsqnos
          ,'新同城'
          ,'新同城来帐'
          ,'ncps_rcvdtl'
           FROM
                   NCPS_RCVDTL  a
            WHERE a.busidate =i_workdate

               and a.step in (
               '0221' ,  '0201'  ,   '0211'
               )
            and length(RACCNO)<=19
               ;

     commit;


     --退票
           insert into SADTL_recipact_tmp_2015
      (
          Busidate
          ,Busitime
          ,Timestmp
          ,ACCNO
          ,AMOUNT
          ,CURRTYPE
          ,TELLERNO

          ,Trxsqno
          ,drcrf
          --,Ropbnkno

          ,RBNKNO

          ,Recipact
          ,rsavname

          ,BAKMSG
          ,trxsqnob
          ,trxsqnos
          ,changsuo
          ,INMSG
          ,DATASOURCE
     )
       SELECT
          busidate
          ,substr(TIMESTAMP,12,2)||'.'||substr(TIMESTAMP,15,2)||'.'||substr(TIMESTAMP,18,2)
          ,TIMESTAMP
          ,OPaccno
          ,amount
          ,to_number(CURRTYPE)
          ,to_number(TRXTEL)

          ,trxsqnob
          ,2 drcrf
          --
          --,OROPBNKNO

          ,ORBNKNO


          ,ORACCNO
          ,ORNAME

          ,substr(NOTES,1,50)
          ,trxsqnob
          ,trxsqnos
          ,'新同城'
          ,'新同城退票'
          ,'NCPS_recalldtl'
           FROM
                   NCPS_recalldtl   a
            WHERE a.busidate =i_workdate

               and a.step in (
               '0000','0211','0221'
               )
               ;
     --
     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_ncps',substr( sqlerrm,1,100));

        raise;
   end;

  --自助终端
  procedure proc_recip_fbec(i_workdate in varchar2   )

  is
  -- v_cnt number;
   begin

        --自助终端往账

          insert into SADTL_recipact_tmp_2015
              (
                    Busidate,
                    Busitime,
                    tellerno,
                    trxsqno,
                    ACCNO,
                    AMOUNT,
                    CURRTYPE,
                    drcrf    ,
                    Recipact  ,
                    RSAVNAME,
                    fee,
                    CHANGSUO,
                    DATASOURCE
              )
               SELECT
                   to_char(hostdate,'yyyymmdd'),
                   substr(hosttime,1,2)||'.'||substr(hosttime,3,2)||'.'||substr(hosttime,5,2),
                   tellerno,
                   trxsqnb,
                   a.drcardno,
                   a.amount,
                   a.currrype ,
                   1 DRCRF,
                   a.crcardno Recipact,
                   a.crname,
                   --feeamount,
                   replace( feeamount,',',''),
                   '自助终端往账'CHANGSUO,
                   'fbec_transaction' DATASOURCE

                   FROM
                           fbec_transaction a
                     WHERE a.hostdate =to_date( i_workdate,'yyyymmdd')
                       AND a.succflag = '0'
                       and a.APPTRXCODE not in ('ibps','ncps')

                       and a.amount<>0
                       and length(drcardno)<=19;




        --自助终端来账

        insert into SADTL_recipact_tmp_2015
              (
                    Busidate,
                    Busitime,
                    tellerno,
                    trxsqno,
                    ACCNO,
                    AMOUNT,
                    CURRTYPE,
                    drcrf    ,
                    Recipact  ,
                    RSAVNAME,
                    fee,
                    CHANGSUO,
                    DATASOURCE
              )
               SELECT

                   to_char(hostdate,'yyyymmdd'),
                   substr(hosttime,1,2)||'.'||substr(hosttime,3,2)||'.'||substr(hosttime,5,2),
                   tellerno,
                   trxsqnb,
                   a.crcardno,
                   a.amount,
                   a.currrype ,
                   2 DRCRF,
                   a.drcardno Recipact,
                   a.drname,
                   --feeamount,
                   replace( feeamount,',',''),

                   '自助终端来账'CHANGSUO,
                   'fbec_transaction' DATASOURCE

                   FROM
                           fbec_transaction a
                     WHERE  a.hostdate =to_date( i_workdate,'yyyymmdd')
                       AND a.succflag = '0'
                       and a.APPTRXCODE not in ('ibps','ncps')

                       and a.amount<>0
                       and length(crcardno)<=19;



     commit;
     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_fbec',substr( sqlerrm,1,100));

        raise;
   end;

  --ATM
  procedure proc_recip_atm(i_workdate in varchar2   )

  is
  -- v_cnt number;
   begin


/*
  insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            ACCNO,
            AMOUNT,
            CURRTYPE,
            drcrf    ,
            Recipact  ,
            RSAVNAME,

            CHANGSUO,
            DATASOURCE
      )
       SELECT

           substr(srvtransdatetime,1,8),
           substr(srvtransdatetime,9,2)||'.'||substr(srvtransdatetime,11,2)||'.'||substr(srvtransdatetime,13,2),
           a.cardno,
           a.tranamt,
           a.TranCurrCode ,
           1 DRCRF,
           substr(adddata1,1,50) Recipact,
           PCK_CREATE_SADTL_PUB_2015.Get_Accname(adddata1),

           'ATM'CHANGSUO,
           'frnt_onltranrec' DATASOURCE

           FROM
                   frnt_onltranrec a
             WHERE a.srvtransdatetime  BETWEEN i_workdate || '000000' AND i_workdate || '999999'
               AND TRIM(a.adddata1) IS NOT NULL
               AND a.addstldata = '01';
        */

      insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            ACCNO,
            AMOUNT,
            CURRTYPE,
            drcrf    ,
            Recipact  ,
            RSAVNAME,

            CHANGSUO,
            DATASOURCE
      )

        SELECT

           substr(srvtransdatetime,1,8),
           substr(srvtransdatetime,9,2)||'.'||substr(srvtransdatetime,11,2)||'.'||substr(srvtransdatetime,13,2),
           a.cardno,
           a.tranamt,
           a.TranCurrCode ,
           1 DRCRF,
           substr(adddata1,1,50) Recipact,
           PCK_CREATE_SADTL_PUB_2015.Get_Accname(adddata1),

           'ATM'CHANGSUO,
           'onltranrec_his' DATASOURCE

           FROM
                   onltranrec_his a
             WHERE a.srvtransdatetime  BETWEEN i_workdate || '000000' AND i_workdate || '999999'

             AND TRIM(a.adddata1) IS NOT NULL
             AND a.addstldata = '01';

     commit;
     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_amt',substr( sqlerrm,1,100));

        raise;
   end;

  --深港票交
  --sgbill_bip_tran_detail_v 没有导表,比较小,是个CMMID下指向OLTP的视图
  --港币和美元
  --我行是借方
  procedure proc_recip_sgbill(i_workdate in varchar2   )

  is
  -- v_cnt number;
   begin


  insert into SADTL_recipact_tmp_2015
      (
            Busidate,
            Busitime,
            ACCNO,
            AMOUNT,
            CURRTYPE,
            drcrf    ,
            Recipact  ,
            RSAVNAME,

            Rbnkno,
            CHANGSUO,
            DATASOURCE
      )
       SELECT

           TRXDATE,
           trxtime,
           a.CDID,
           a.AMT,
           decode(  a.CCY ,'USD',14,'HKD',13,'CNY',1,0),
           2 DRCRF,
           DBID Recipact,
           DBNM,
            DBISSR,
           '深港票据' CHANGSUO,
           'sgbill' DATASOURCE

           FROM
                   sgbill_bip_tran_detail_v a
             WHERE a.TRXDATE  =i_workdate

               and a.HERRNO   = '0000'
               and a.TXTPCD='112';  --只有112这个业务,这样我行一定是贷方,这样以后即使增加其他业务,前面协定的借贷标志也没有问题



     commit;
     EXCEPTION
       WHEN OTHERS THEN
        PCK_CREATE_SADTL_PUB_2015.public_log(i_workdate ,'proc_recip_2d_sgbill',substr( sqlerrm,1,100));

        raise;
   end;


end PCK_CREATE_RECIPACT_2015;
//







CREATE OR REPLACE PACKAGE BODY PCK_CREATE_SADTL_PUB
IS
  --公共函数 统一日志登记
   PROCEDURE public_log (i_workdate   IN VARCHAR2,                       -- 日期
                         i_pgm        IN VARCHAR2,                      --功能模块
                         i_loginf     IN VARCHAR2                     -- 需记录内容
                                                 )
   IS
   BEGIN
      INSERT INTO sadtl_log_table (workdate, procname, loginf)
           VALUES (i_workdate, i_pgm, i_loginf);

      COMMIT;
   END;
END PCK_CREATE_SADTL_PUB;
//







create or replace package body PCK_CREATE_SADTL_PUB_2015 is



  --公共函数 根据卡号或者帐号获取名称
   Function Get_Accname
  (
    i_Accno In Varchar2
  ) Return Varchar2
  is
  v_len  number;
  o_Name Varchar2(200);
  begin


    select length( regexp_replace( i_Accno, '\d+', '')) into v_len  from dual;

    if v_len<>0 then
      return null;
    end if;


     Select savname
          Into o_Name
          From pras_pthsasub
         Where accno = substr(i_accno,1,17)
           and rownum < 2;
      return o_Name;
  Exception
        When no_data_found Then
          begin



                      Select savname
                    Into o_Name
                    From Bcas_Pfhcdmdu
                   Where mdcardno = i_accno
                     and rownum < 2;


              return o_Name;
          Exception
             When no_data_found Then
              begin
                      Select accname
                    Into o_Name
                    From nase_nthchsub
                   Where accno = substr(i_accno,1,17)
                     and rownum < 2;
                     return o_Name;
              Exception
                When no_data_found Then
                  begin

                   Select ACCNAME
                        Into o_Name
                        From nase_nthinsub
                       Where accno = substr(i_accno,1,17)
                       and subcode not like '4%'  --交换差的不要，这样会把新同城的匹配补上的，行名补充为我们自己
                         and rownum < 2;

                     return o_Name;
                    Exception
                      When no_data_found Then
                        begin

                            Select savname
                          Into o_Name
                          From bcas_bthdrsyn
                         Where mdcardno = i_accno
                           and rownum < 2;
                           return o_Name;

                          Exception
                          When no_data_found Then
                            begin
                                Select Savname
                                Into o_Name
                                From Pras_Pthsasub
                               Where Accno In (Select To_Char(RMBACC)
                                                 From Bcas_Bthdrltd
                                                Where Mdcardno = i_Accno)
                                 and rownum < 2;
                                 return o_Name;
                            Exception
                            When no_data_found Then

                              begin
                                select OWNNAME into  o_Name from RPT_BFHKBDPB
                                where cardno =i_accno and OWNNAME is not null and rownum<2;
                                 return o_Name;
                                 Exception
                                   When no_data_found Then
                                      return null;
                              end;


                             return null;
                            end;


                        end;

                  end;

              end;


          end;

   end;



  --公共函数 统一日志登记
  procedure public_log
  (
    i_workdate   In varchar2, -- 日期
    i_pgm        in varchar2, --功能模块
    i_loginf    in  varchar2-- 需记录内容
  )
  is
  begin
    insert into sadtl_log_table_2015(workdate,procname,loginf)
    values
    (i_workdate,i_pgm,i_loginf);
    commit;
  end;


  --根据地区号取名称

   Function Get_zoneno_name
  (
        i_zoneno In Varchar2
    ) Return Varchar2
  is
  o_Name Varchar2(200);
  begin
    o_Name:=null;
    select replace(notes,' ','') into o_Name  from nase_nthpazon_all where zoneno=i_zoneno and rownum=1;
    return o_Name;
  Exception
        When no_data_found Then
          o_Name:=null;
          return o_Name;
  end;

 --根据人民银行12位行号取对应的名称

   Function Get_bankcode_name
  (
        i_bankcode In Varchar2
    ) Return Varchar2
  is
  o_Name Varchar2(200);
  begin
    o_Name:=null;
    select allname into o_Name  from rpt_sffhmbk where bankcode=i_bankcode and rownum=1;
    return o_Name;
  Exception
        When no_data_found Then
          begin
             select bank into o_Name  from ncps_bankinfo where bankno=i_bankcode and rownum=1;
             return o_Name;

         Exception
         When no_data_found Then
          o_Name:=null;
          return o_Name;
          end;
  end;


end PCK_CREATE_SADTL_PUB_2015;
//







CREATE OR REPLACE PACKAGE BODY PCK_MATCH_SADTL
IS
   PROCEDURE main (i_workdate IN VARCHAR2)
   IS
   BEGIN
      ycl;                                                               --预处理
      match_js;                                                        --强关联匹配
      match_week (i_workdate);                                         --若关联匹配
   END main;

   PROCEDURE ycl
   IS
      v_step   VARCHAR2 (5);
   BEGIN
      v_step := 1;

      EXECUTE IMMEDIATE 'Truncate Table SADTL_ACCNO_list';

      EXECUTE IMMEDIATE 'Truncate Table SADTL_ACCNO_list_19';

      EXECUTE IMMEDIATE 'Truncate Table SADTL_card_list';

      EXECUTE IMMEDIATE 'Truncate Table SADTL_ACCNO_list_dupacc';

      EXECUTE IMMEDIATE 'Truncate Table SADTL_ACCNO_LIST_NODUP';



      v_step := 2;

      --17位帐号
      INSERT INTO SADTL_ACCNO_list (sn, accno, accnotype)
         SELECT Seq_Sadtl_Accno_List.NEXTVAL, accno, '1'
           FROM (SELECT DISTINCT accno
                   FROM SADTL_DTL_tmp t);

      v_step := 3;

      --19位帐号
      INSERT INTO SADTL_ACCNO_list_19 (sn, accno, accnotype)
         SELECT sn, GET_FULL_ACCNO (accno), '2' FROM SADTL_ACCNO_list;

      v_step := 4;

      --卡号
      INSERT INTO SADTL_card_list (sn, accno, accnotype)
         SELECT b.sn, a.mdcardno, '3'
           FROM PRS_S_PFHCDMDU a, SADTL_ACCNO_list b
          WHERE a.accno = b.accno;

      v_step := 5;

      -- 合并
      INSERT INTO SADTL_ACCNO_list
         SELECT DISTINCT Sn, Accno, Accnotype
           FROM (SELECT * FROM SADTL_ACCNO_list_19
                 UNION ALL
                 SELECT * FROM SADTL_card_list);

      COMMIT;

      v_step := 6;

      INSERT INTO SADTL_ACCNO_list_dupacc (Dupacc)
           SELECT accno
             FROM SADTL_ACCNO_LIST t
         GROUP BY accno
           HAVING COUNT (*) > 1;

      COMMIT;

      v_step := 7;

      INSERT INTO SADTL_ACCNO_LIST_NODUP (SN, ACCNO, ACCNOTYPE)
         SELECT x.sn, x.accno, x.accnotype
           FROM SADTL_ACCNO_LIST x, SADTL_ACCNO_list_dupacc y
          WHERE x.accno = y.dupacc(+) AND y.dupacc IS NULL;

      COMMIT;

      v_step := 8;

      --更新明细表seq
      UPDATE SADTL_DTL_tmp a
         SET dtl_seq = Seq_Sadtl_Dtl.NEXTVAL;

      COMMIT;

      --更新对手表seq
      UPDATE SADTL_RECIPACT_TMP
         SET recip_seq = SEQ_SADTL_recip.NEXTVAL;

      COMMIT;

      v_step := 9;

      --更新明细表sn
      UPDATE SADTL_DTL_tmp a
         SET sn =
                (SELECT sn
                   FROM SADTL_ACCNO_LIST_NODUP b
                  WHERE b.accno = a.accno);

      --更新对手表sn
      UPDATE SADTL_RECIPACT_TMP a
         SET sn =
                (SELECT sn
                   FROM SADTL_ACCNO_LIST_NODUP b
                  WHERE b.accno = a.accno);

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (
            'v_step:' || v_step || '|' || SUBSTR (SQLERRM, 1, 200));
         ROLLBACK;
         RAISE;
   END ycl;

   PROCEDURE match_js
   IS
   BEGIN
      match_special_13_34024;                              --精确匹配，结算日志13/34024
      match_special_12_34024;                              --精确匹配，结算日志12/34024
      match_special_59_34024;                              --精确匹配，结算日志59/34024
      match_special_12_34009;                              --精确匹配，结算日志12/34009
      match_special_12_34009_cr;                           --精确匹配，结算日志12/34009
   END match_js;

   PROCEDURE match_special_13_34024
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;

      v_code       NUMBER;
      v_recip      VARCHAR2 (100);
      v_rsavname   VARCHAR2 (100);

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recip_seq IS NULL                               --未匹配对手的
                  AND RECIPACT IS NULL                        --明细中，电话银行异地同行转账
                  AND tellerno = 13
                  AND trxcode = 34024
                  AND drcrf = 1
                  AND cashnote NOT LIKE '%费%'
         ORDER BY accno, timestmp;
   BEGIN
      FOR v_dtl IN c_dtl
      LOOP
         v_code := NULL;
         v_recip := NULL;
         v_rsavname := NULL;

         find_rercip_teller_34024 (v_dtl.busidate,
                                   v_dtl.trxcode,
                                   v_dtl.tellerno,
                                   v_dtl.trxsqno,
                                   v_dtl.currtype,
                                   v_dtl.amount,
                                   SUBSTR (v_dtl.accno, 1, 17),
                                   v_code,
                                   v_recip,
                                   v_rsavname);

         IF v_code = 0
         THEN
            UPDATE SADTL_DTL_tmp a
               SET a.Fix_Recipact =
                      DECODE (a.recipact, NULL, v_recip, a.recipact),
                   a.Fix_Rsavname =
                      DECODE (a.RESAVNAME, NULL, v_rsavname, a.RESAVNAME),
                   a.Fix_Recipdatasource = 'PTHSJNL',
                   a.RECIP_SEQ = -1
             WHERE a.DTL_SEQ = v_dtl.DTL_SEQ;

            COMMIT;
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         COMMIT;
         RAISE;
   END match_special_13_34024;

   PROCEDURE find_rercip_teller_34024 (i_busidate   IN     VARCHAR2,
                                       i_trxcode    IN     VARCHAR2,
                                       i_tellerno   IN     NUMBER,
                                       i_trxsqnb    IN     NUMBER,
                                       i_currtype   IN     NUMBER,
                                       i_amount     IN     NUMBER,
                                       i_draccno    IN     VARCHAR2,
                                       o_code          OUT NUMBER, --0 表示正常，才可以使用后面的 o_recip
                                       o_recip         OUT VARCHAR2,  --日志贷方账号
                                       o_rsavname      OUT VARCHAR2 --日志中的NOTES1
                                                                   )
   IS
      v_cnt   NUMBER;
   BEGIN
      --目前没有这以前的数据
      IF i_busidate < '20020101'
      THEN
         o_code := -1;
         RETURN;
      END IF;

      --先验证自己的汇款
      --因为后面对手匹配的时候，无账号条件
      --所以先验证自己的汇款，这样效率降低了，

      SELECT COUNT (*)
        INTO v_cnt
        FROM PRS_S_PFSJNL a
       WHERE     workdate = TO_DATE (i_busidate, 'yyyymmdd')       --禁止自动使用此索引
             AND tellerno = i_tellerno
             AND trxcode = i_trxcode                                   --34024
             AND trxsqnb = i_trxsqnb
             AND amount1 = i_amount
             AND drcurr = i_currtype
             AND draccno = TO_NUMBER (i_draccno)
             AND trxtype = 1;              --一共有4笔 0 手续费，2笔1  扣本金和手续费  6  汇款出去

      --正常应等于2
      IF v_cnt = 0
      THEN
         o_code := -1;
         RETURN;
      END IF;

      SELECT DECODE (CRACCNO, 0, '', CRACCNO) || ',' || CRCARDNO, NOTES
        INTO o_recip, o_rsavname
        FROM PRS_S_PFSJNL
       WHERE     workdate = TO_DATE (i_busidate, 'yyyymmdd')
             AND tellerno = i_tellerno
             AND trxcode = i_trxcode                                   --34024
             AND trxsqnb = i_trxsqnb
             AND amount2 = i_amount
             AND crcurr = i_currtype
             AND trxtype = 6;              --一共有4笔 0 手续费，2笔1  扣本金和手续费  6  汇款出去

      o_code := 0;
      COMMIT;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_code := -1;
      WHEN OTHERS
      THEN
         o_code := -1;
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
   END find_rercip_teller_34024;

   PROCEDURE match_special_12_34024
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;

      --接受对手存储过程返回
      v_code       NUMBER;
      v_recip      VARCHAR2 (100);
      v_rsavname   VARCHAR2 (100);

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recip_seq IS NULL                               --未匹配对手的
                  AND RECIPACT IS NULL
                  AND tellerno = 12
                  AND trxcode = 34024
                  AND drcrf = 1
                  AND cashnote NOT LIKE '%费%'
         ORDER BY accno, timestmp;
   BEGIN
      --打开明细游标
      FOR v_dtl IN c_dtl
      LOOP
         v_code := NULL;
         v_recip := NULL;
         v_rsavname := NULL;

         find_rercip_teller_34024 (v_dtl.busidate,
                                   v_dtl.trxcode,
                                   v_dtl.tellerno,
                                   v_dtl.trxsqno,
                                   v_dtl.currtype,
                                   v_dtl.amount,
                                   SUBSTR (v_dtl.accno, 1, 17),
                                   v_code,
                                   v_recip,
                                   v_rsavname);

         IF v_code = 0
         THEN
            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET a.Fix_Recipact =
                      DECODE (a.recipact, NULL, v_recip, a.recipact),
                   a.Fix_Rsavname =
                      DECODE (a.RESAVNAME, NULL, v_rsavname, a.RESAVNAME),
                   a.Fix_Recipdatasource = 'PTHSJNL',
                   a.RECIP_SEQ = -1
             WHERE a.DTL_SEQ = v_dtl.DTL_SEQ;

            COMMIT;
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
         COMMIT;
         RAISE;
   END match_special_12_34024;

   PROCEDURE match_special_59_34024
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;

      --接受对手存储过程返回
      v_code       NUMBER;
      v_recip      VARCHAR2 (100);
      v_rsavname   VARCHAR2 (100);

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recip_seq IS NULL                               --未匹配对手的
                  AND RECIPACT IS NULL
                  AND tellerno = 59
                  AND trxcode = 34024
                  AND drcrf = 1
                  AND cashnote NOT LIKE '%费%'
         ORDER BY accno, timestmp;
   BEGIN
      --打开明细游标
      FOR v_dtl IN c_dtl
      LOOP
         v_code := NULL;
         v_recip := NULL;
         v_rsavname := NULL;

         find_rercip_teller_34024 (v_dtl.busidate,
                                   v_dtl.trxcode,
                                   v_dtl.tellerno,
                                   v_dtl.trxsqno,
                                   v_dtl.currtype,
                                   v_dtl.amount,
                                   SUBSTR (v_dtl.accno, 1, 17),
                                   v_code,
                                   v_recip,
                                   v_rsavname);

         IF v_code = 0
         THEN
            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET a.Fix_Recipact =
                      DECODE (a.recipact, NULL, v_recip, a.recipact),
                   a.Fix_Rsavname =
                      DECODE (a.RESAVNAME, NULL, v_rsavname, a.RESAVNAME),
                   a.Fix_Recipdatasource = 'PTHSJNL',
                   a.RECIP_SEQ = -1
             WHERE a.DTL_SEQ = v_dtl.DTL_SEQ;

            COMMIT;
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
         COMMIT;
         RAISE;
   END match_special_59_34024;

   PROCEDURE match_special_12_34009
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;

      --接受对手存储过程返回
      v_code       NUMBER;
      v_recip      VARCHAR2 (100);
      v_rsavname   VARCHAR2 (100);

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recip_seq IS NULL                               --未匹配对手的
                  AND RECIPACT IS NULL                       --明细中， 电话银行异地同行转账
                  AND tellerno = 12
                  AND trxcode = 34009
                  AND drcrf = 1
                  AND cashnote NOT LIKE '%费%'
         ORDER BY accno, timestmp;
   BEGIN
      --打开明细游标
      FOR v_dtl IN c_dtl
      LOOP
         v_code := NULL;
         v_recip := NULL;
         v_rsavname := NULL;

         find_rercip_teller12_34009 (v_dtl.busidate,
                                     v_dtl.trxcode,
                                     v_dtl.tellerno,
                                     v_dtl.trxsqno,
                                     v_dtl.currtype,
                                     v_dtl.amount,
                                     SUBSTR (v_dtl.accno, 1, 17),
                                     v_code,
                                     v_recip                      --v_rsavname
                                            );

         IF v_code = 0
         THEN
            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET a.FIX_RECIPACT =
                      DECODE (a.recipact, NULL, v_recip, a.recipact),
                   a.Fix_Recipdatasource = 'PTHSJNL',
                   a.RECIP_SEQ = -1
             WHERE a.DTL_SEQ = v_dtl.DTL_SEQ;

            COMMIT;
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
         COMMIT;
         RAISE;
   END match_special_12_34009;

   PROCEDURE find_rercip_teller12_34009 (i_busidate   IN     VARCHAR2,
                                         i_trxcode    IN     VARCHAR2,
                                         i_tellerno   IN     NUMBER,
                                         i_trxsqnb    IN     NUMBER,
                                         i_currtype   IN     NUMBER,
                                         i_amount     IN     NUMBER,
                                         i_draccno    IN     VARCHAR2,
                                         o_code          OUT NUMBER, --0 表示正常，才可以使用后面的 o_recip
                                         o_recip         OUT VARCHAR2 --日志贷方账号
                                                                     --o_rsavname out varchar2 --日志中的NOTES1
                                         )
   IS
      v_cnt   NUMBER;
   BEGIN
      IF i_busidate < '20020101'
      THEN
         o_code := -1;
         RETURN;
      END IF;

      --先验证自己的汇款
      --因为后面对手匹配的时候，无账号条件
      --所以先验证自己的汇款，这样效率降低了，

      SELECT COUNT (*)
        INTO v_cnt
        FROM PRS_S_PFSJNL
       WHERE     workdate = TO_DATE (i_busidate, 'yyyymmdd')
             AND tellerno = i_tellerno                                    --12
             AND trxcode = i_trxcode                                   --34009
             AND trxsqnb = i_trxsqnb
             AND amount1 = i_amount
             AND drcurr = i_currtype
             AND draccno = TO_NUMBER (i_draccno)
             AND trxtype = 1;                                    --一共有2笔 类型都是1

      --正常应等于2
      IF v_cnt = 0
      THEN
         o_code := -1;
         RETURN;
      END IF;

      SELECT DECODE (CRACCNO, 0, '', CRACCNO) || ',' || CRCARDNO
        INTO o_recip
        FROM PRS_S_PFSJNL
       WHERE     workdate = TO_DATE (i_busidate, 'yyyymmdd')
             AND tellerno = i_tellerno                                    --12
             AND trxcode = i_trxcode                                   --34009
             AND trxsqnb = i_trxsqnb
             AND amount2 = i_amount
             AND crcurr = i_currtype
             AND trxtype = 1;                                    --一共有2笔 类型都是1

      o_code := 0;
      COMMIT;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_code := -1;
      WHEN OTHERS
      THEN
         o_code := -1;

         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
   END find_rercip_teller12_34009;

   PROCEDURE match_special_12_34009_cr
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;

      --接受对手存储过程返回
      v_code       NUMBER;
      v_recip      VARCHAR2 (100);
      v_rsavname   VARCHAR2 (100);

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recip_seq IS NULL                               --未匹配对手的
                  AND RECIPACT IS NULL                       --明细中， 电话银行异地同行转账
                  AND tellerno = 12
                  AND trxcode = 34009
                  AND drcrf = 2
         ORDER BY accno, timestmp;
   BEGIN
      --打开明细游标
      FOR v_dtl IN c_dtl
      LOOP
         v_code := NULL;
         v_recip := NULL;
         v_rsavname := NULL;

         find_rercip_teller12_34009_cr (v_dtl.busidate,
                                        v_dtl.trxcode,
                                        v_dtl.tellerno,
                                        v_dtl.trxsqno,
                                        v_dtl.currtype,
                                        v_dtl.amount,
                                        SUBSTR (v_dtl.accno, 1, 17),
                                        v_code,
                                        v_recip                   --v_rsavname
                                               );

         IF v_code = 0
         THEN
            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET a.FIX_RECIPACT =
                      DECODE (a.recipact, NULL, v_recip, a.recipact),
                   a.Fix_Recipdatasource = 'PTHSJNL',
                   a.RECIP_SEQ = -1
             WHERE a.DTL_SEQ = v_dtl.DTL_SEQ;

            COMMIT;
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
         COMMIT;
         RAISE;
   END match_special_12_34009_cr;

   PROCEDURE find_rercip_teller12_34009_cr (i_busidate   IN     VARCHAR2,
                                            i_trxcode    IN     VARCHAR2,
                                            i_tellerno   IN     NUMBER,
                                            i_trxsqnb    IN     NUMBER,
                                            i_currtype   IN     NUMBER,
                                            i_amount     IN     NUMBER,
                                            i_craccno    IN     VARCHAR2,
                                            o_code          OUT NUMBER, --0 表示正常，才可以使用后面的 o_recip
                                            o_recip         OUT VARCHAR2 --日志贷方账号
                                                                        --o_rsavname out varchar2 --日志中的NOTES1
                                            )
   IS
      v_cnt   NUMBER;
   BEGIN
      IF i_busidate < '20020101'
      THEN
         o_code := -1;
         RETURN;
      END IF;

      --先验证自己的汇款
      --因为后面对手匹配的时候，无账号条件
      --所以先验证自己的汇款，这样效率降低了，

      SELECT COUNT (*)
        INTO v_cnt
        FROM PRS_S_PFSJNL
       WHERE     workdate = TO_DATE (i_busidate, 'yyyymmdd')
             AND tellerno = i_tellerno                                    --12
             AND trxcode = i_trxcode                                   --34009
             AND trxsqnb = i_trxsqnb
             AND amount2 = i_amount
             AND crcurr = i_currtype
             AND craccno = TO_NUMBER (i_craccno)
             AND trxtype = 1;                                    --一共有2笔 类型都是1

      --正常应等于2
      IF v_cnt = 0
      THEN
         o_code := -1;
         RETURN;
      END IF;

      SELECT DECODE (dRACCNO, 0, '', dRACCNO) || ',' || dRCARDNO
        INTO o_recip
        FROM PRS_S_PFSJNL
       WHERE     workdate = TO_DATE (i_busidate, 'yyyymmdd')
             AND tellerno = i_tellerno                                    --12
             AND trxcode = i_trxcode                                   --34009
             AND trxsqnb = i_trxsqnb
             AND amount1 = i_amount
             AND drcurr = i_currtype
             AND trxtype = 1;                                    --一共有2笔 类型都是1

      o_code := 0;
      COMMIT;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_code := -1;
      WHEN OTHERS
      THEN
         o_code := -1;
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 200));
   END find_rercip_teller12_34009_cr;

   PROCEDURE match_week (i_workdate IN VARCHAR2)
   IS
   BEGIN
      match_wy_out (i_workdate);
      match_2DWY (i_workdate);
      match_wy_in (i_workdate);
      match_b2c(i_workdate);
      match_pos(i_workdate);
      match_salary(i_workdate);
   END match_week;

   PROCEDURE match_wy_out (i_workdate IN VARCHAR2)
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;
      v_recipact   SADTL_RECIPACT_TMP%ROWTYPE;

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recipact IS NULL                                 --对手不为空
                  AND recip_seq IS NULL                               --未匹配对手的
                  AND tellerno = 12                              --明细中，网银转出的条件
                  AND servface = 10
                  AND drcrf = 1
                  AND trxcode IN (34007, 34009, 34024, 2713)
         ORDER BY accno, timestmp;

      --对手游标
      CURSOR c_recipact (
         i_sn          NUMBER,                                    --同查询批次中相同序号
         i_amount      NUMBER,                                          --金额相同
         i_currtype    NUMBER,                                          --币种相同
         i_busidate    VARCHAR2                                          --同一天
                               )
      IS
           SELECT *
             FROM SADTL_RECIPACT_TMP
            WHERE                                                     --本查询批次的
                 dtl_seq IS NULL                                   --未有明细与之匹配的
                  AND datasource = 'SDA_S_EFRMLGOT'              --明细中，网银转出的条件
                  AND sn = i_sn                                         --账号相同
                  AND amount = i_amount
                  AND currtype = i_currtype
                  AND busidate = i_busidate
         ORDER BY busitime;
   BEGIN
      --打开明细游标
      OPEN c_dtl;

      LOOP
         FETCH c_dtl INTO v_dtl;

         EXIT WHEN c_dtl%NOTFOUND;

         --打开对手游标
         OPEN c_recipact (v_dtl.sn,
                          v_dtl.amount,
                          v_dtl.currtype,
                          v_dtl.busidate);

         LOOP
            FETCH c_recipact INTO v_recipact;

            EXIT WHEN c_recipact%NOTFOUND;                --若没有匹配到明细就是NOTFOUND

            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET --FIX_RBNKNO    =     v_recipact.RBNKNO, --原有明细中，没有此字段，此字段全靠补
 --FIX_RBNKNAME  =     decode(a.REbkNAME,null,v_recipact.RBNKNAME ,a.REbkNAME),
                                     --FIX_BAKMSG       =   v_recipact.BAKMSG,
                                       --FIX_INMSG       =   v_recipact.inmsg,
                   FIX_RECIPACT =
                      DECODE (a.recipact,
                              NULL, v_recipact.RECIPACT,
                              a.recipact),
                   --FIX_RSAVNAME  =     decode(a.RESAVNAME,null,v_recipact.RSAVNAME ,a.RESAVNAME),
                   recip_seq = v_recipact.recip_seq
             WHERE a.dtl_seq = v_dtl.dtl_seq;

            --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细
            UPDATE SADTL_RECIPACT_TMP b
               SET b.dtl_seq = v_dtl.dtl_seq
             WHERE b.recip_seq = v_recipact.recip_seq;

            COMMIT;

            EXIT WHEN 1 = 1;                  --匹配完了之后就跳出循环不再继续遍历对手表，继续往下遍历明细表
         END LOOP;

         CLOSE c_recipact;

         COMMIT;
      END LOOP;

      CLOSE c_dtl;

      COMMIT;
   /*EXCEPTION
    WHEN OTHERS THEN
      o_code := -1;
      dbms_output.put_line(substr(sqlerrm, 1, 200));*/

   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'match_wy_out',
                                          SUBSTR (SQLERRM, 1, 100));
         RAISE;
   END;

   --2.2 2代转出对手匹配
   --此模块游标说明 此模块有2组游标，区别在于第2组游标考虑的手续费
   PROCEDURE match_2DWY (i_workdate IN VARCHAR2)
   IS
      v_dtl          SADTL_DTL_tmp%ROWTYPE;
      v_recipact     SADTL_RECIPACT_TMP%ROWTYPE;
      o_code         NUMBER;

      v_dtl_2        SADTL_DTL_tmp%ROWTYPE;
      v_recipact_2   SADTL_RECIPACT_TMP%ROWTYPE;

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE      --and recipact is null --对手不为空 这个交易存在对手不为空,但需要补充对手名称的情况
                 recip_seq IS NULL                                    --未匹配对手的
                  ------明细中，2代网银的条件

                  AND (trxcode IN (52075, 52078))
                  AND tellerno <> 23                                 --排出现代化支付
                  AND currtype = 1                           --对手无币种,所以这里只取人民币
         --and accno ='40000327011001221'
         --and sn=33914900

         ORDER BY accno, timestmp;


      --对手游标
      CURSOR c_recipact (
         i_sn          NUMBER,                                    --同查询批次中相同序号
         i_amount      NUMBER,                                          --金额相同
         --无币种
         i_busidate    VARCHAR2,                                         --同一天
         i_drcrf       NUMBER,                                          --借贷标志
         i_recipacc    VARCHAR2                                           --对手
                               )
      IS
           SELECT *
             FROM SADTL_RECIPACT_TMP
            WHERE     dtl_seq IS NULL                              --未有明细与之匹配的
                  AND datasource IN ('SCP_S_SFAWDST', 'SCP_S_SFAWCST') --对手表中,2代网银的条件
                  AND sn = i_sn
                  AND DECODE (i_recipacc, NULL, '1', RECIPACT) =
                         DECODE (i_recipacc, NULL, '1', i_recipacc)
                  AND amount = i_amount
                  AND drcrf = i_drcrf
                  AND busidate = i_busidate
         ORDER BY timestmp;


      --在莱一组，我们借方，明细是含手续费的，但是对手是2个字段
      --明细游标
      CURSOR c_dtl_2
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE      --and recipact is null --对手不为空 这个交易存在对手不为空,但需要补充对手名称的情况
                 recip_seq IS NULL                                    --未匹配对手的
                  ----明细中，2代网银的条件
                  AND (trxcode IN (52075, 52078))
                  AND tellerno <> 23                                 --排出现代化支付
                  AND currtype = 1                           --对手无币种,所以这里只取人民币
         --and accno ='40000327011001221'
         --and sn=33914900
         ORDER BY accno, timestmp;


      --对手游标
      CURSOR c_recipact_2 (
         i_sn          NUMBER,                                    --同查询批次中相同序号
         i_amount      NUMBER,                                          --金额相同
         --无币种
         i_busidate    VARCHAR2,                                         --同一天
         i_drcrf       NUMBER,                                          --借贷标志
         i_recipacc    VARCHAR2                                           --对手
                               )
      IS
           SELECT *
             FROM SADTL_RECIPACT_TMP
            WHERE     dtl_seq IS NULL                              --未有明细与之匹配的
                  AND datasource IN ('SCP_S_SFAWDST', 'SCP_S_SFAWCST') --对手表中,2代网银的条件
                  AND sn = i_sn
                  --and decode(RECIPACT,null,'1',RECIPACT) =decode(RECIPACT,null,'1',i_recipacc)

                  AND DECODE (i_recipacc, NULL, '1', RECIPACT) =
                         DECODE (i_recipacc, NULL, '1', i_recipacc)
                  AND amount + NVL (fee, 0) = i_amount
                  AND drcrf = i_drcrf
                  AND busidate = i_busidate
         ORDER BY timestmp;
   BEGIN
      --打开明细游标
      OPEN c_dtl_2;

      LOOP
         FETCH c_dtl_2 INTO v_dtl_2;

         EXIT WHEN c_dtl_2%NOTFOUND;

         --打开对手游标
         OPEN c_recipact_2 (v_dtl_2.sn,
                            v_dtl_2.amount,
                            v_dtl_2.busidate,
                            v_dtl_2.drcrf,
                            v_dtl_2.recipact);

         LOOP
            FETCH c_recipact_2 INTO v_recipact_2;

            EXIT WHEN c_recipact_2%NOTFOUND;

            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET                    --FIX_RBNKNO    =   v_recipact_2.RBNKNO,
 --FIX_RBNKNAME  =   decode(a.REbkNAME,null,v_recipact_2.RBNKNAME ,a.REbkNAME),
                                      --FIX_BAKMSG     =  v_recipact_2.BAKMSG,
                                       --FIX_INMSG     =   v_recipact_2.inmsg,
                   FIX_RECIPACT =
                      DECODE (a.recipact,
                              NULL, v_recipact_2.RECIPACT,
                              a.recipact),
                   --FIX_RSAVNAME  =   decode(a.RESAVNAME,null,v_recipact_2.RSAVNAME ,a.RESAVNAME),
                   recip_seq = v_recipact_2.recip_seq
             WHERE a.dtl_seq = v_dtl_2.dtl_seq;

            --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细

            UPDATE SADTL_RECIPACT_TMP b
               SET b.dtl_seq = v_dtl_2.dtl_seq
             WHERE b.recip_seq = v_recipact_2.recip_seq;


            COMMIT;
            EXIT WHEN 1 = 1;
         END LOOP;

         CLOSE c_recipact_2;

         COMMIT;
      END LOOP;

      CLOSE c_dtl_2;

      COMMIT;



      --打开明细游标2


      OPEN c_dtl;

      LOOP
         FETCH c_dtl INTO v_dtl;

         EXIT WHEN c_dtl%NOTFOUND;

         --打开对手游标
         OPEN c_recipact (v_dtl.sn,
                          v_dtl.amount,
                          v_dtl.busidate,
                          v_dtl.drcrf,
                          v_dtl.recipact);

         LOOP
            FETCH c_recipact INTO v_recipact;

            EXIT WHEN c_recipact%NOTFOUND;

            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET                      --FIX_RBNKNO    =   v_recipact.RBNKNO,
                    --FIX_RBNKNAME  =   decode(a.REbkNAME,null,v_recipact.RBNKNAME ,a.REbkNAME),
                                       --FIX_BAKMSG     =   v_recipact.BAKMSG,
                                         --FIX_INMSG     =   v_recipact.inmsg,
                   FIX_RECIPACT =
                      DECODE (a.recipact,
                              NULL, v_recipact.RECIPACT,
                              a.recipact),
                   --FIX_RSAVNAME  =   decode(a.RESAVNAME,null,v_recipact.RSAVNAME ,a.RESAVNAME),
                   recip_seq = v_recipact.recip_seq
             WHERE a.dtl_seq = v_dtl.dtl_seq;

            --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细

            UPDATE SADTL_RECIPACT_TMP b
               SET b.dtl_seq = v_dtl.dtl_seq
             WHERE b.recip_seq = v_recipact.recip_seq;
            COMMIT;
            EXIT WHEN 1 = 1;
         END LOOP;
         CLOSE c_recipact;
         COMMIT;
      END LOOP;
      CLOSE c_dtl;
      o_code := 0;
      COMMIT;
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'match_2DWY',
                                          SUBSTR (SQLERRM, 1, 100));
         RAISE;
   END;

   --2.3 3网银转入对手匹配
   PROCEDURE match_wy_in (i_workdate IN VARCHAR2)
   IS
      v_dtl        SADTL_DTL_tmp%ROWTYPE;
      v_recipact   SADTL_RECIPACT_TMP%ROWTYPE;

      --明细游标
      CURSOR c_dtl
      IS
           SELECT *
             FROM SADTL_DTL_tmp
            WHERE     recipact IS NULL            --对手不为空,此明细中不含对手名称,所以只补对手为空的
                  AND recip_seq IS NULL                               --未匹配对手的
                  AND tellerno = 12                              --明细中，网银转出的条件
                  AND servface = 10
                  AND drcrf = 2
                  AND trxcode IN (34007, 34009, 34024, 2713)
         ORDER BY accno, timestmp;

      --对手游标
      CURSOR c_recipact (
         i_sn          NUMBER,                                    --同查询批次中相同序号
         i_amount      NUMBER,                                          --金额相同
         i_currtype    NUMBER,                                          --币种相同
         i_busidate    VARCHAR2                                          --同一天
                               )
      IS
           SELECT *
             FROM SADTL_RECIPACT_TMP
            WHERE     dtl_seq IS NULL                              --未有明细与之匹配的
                  AND datasource = 'SDA_S_EFRMLGIN'              --明细中，网银转出的条件
                  AND sn = i_sn
                  AND amount = i_amount
                  AND currtype = i_currtype
                  AND busidate = i_busidate
         ORDER BY busitime;
   BEGIN
      --打开明细游标
      OPEN c_dtl;
      LOOP
         FETCH c_dtl INTO v_dtl;
         EXIT WHEN c_dtl%NOTFOUND;
         --打开对手游标
         OPEN c_recipact (v_dtl.sn,
                          v_dtl.amount,
                          v_dtl.currtype,
                          v_dtl.busidate);

         LOOP
            FETCH c_recipact INTO v_recipact;
            EXIT WHEN c_recipact%NOTFOUND;
            --更新匹配到的信息
            UPDATE SADTL_DTL_tmp a
               SET --FIX_RBNKNO    =   v_recipact.RBNKNO,
                   --FIX_RBNKNAME  =   decode(a.REbkNAME,null,v_recipact.RBNKNAME ,a.REbkNAME),
                   --FIX_BAKMSG     =   v_recipact.BAKMSG,
                   --FIX_INMSG     =   v_recipact.inmsg,
                   FIX_RECIPACT =
                      DECODE (a.recipact,
                              NULL, v_recipact.RECIPACT,
                              a.recipact),
                   --FIX_RSAVNAME  =   decode(a.RESAVNAME,null,v_recipact.RSAVNAME ,a.RESAVNAME),
                   recip_seq = v_recipact.recip_seq
             WHERE a.dtl_seq = v_dtl.dtl_seq;

            --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细
            UPDATE SADTL_RECIPACT_TMP b
               SET b.dtl_seq = v_dtl.dtl_seq
             WHERE b.recip_seq = v_recipact.recip_seq;

            COMMIT;
            EXIT WHEN 1 = 1;
         END LOOP;

         CLOSE c_recipact;

         COMMIT;
      END LOOP;

      CLOSE c_dtl;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         PCK_CREATE_SADTL_PUB.public_log (i_workdate,
                                          'match_wy_in',
                                          SUBSTR (SQLERRM, 1, 100));
         COMMIT;
         RAISE;
   END;

   --2.4 4b2c
       procedure match_b2c (i_workdate in  varchar2)
       is

       v_dtl SADTL_DTL_tmp%ROWTYPE;
       v_recipact SADTL_RECIPACT_TMP%ROWTYPE;


       --明细游标对手为空的
       cursor c_dtl is
         select * from SADTL_DTL_tmp
         where
         --and recipact is null --对手不为空,对方不为空,需要补订单号等信息
           recip_seq is null --未匹配对手的
         AND CURRTYPE=1 --对手只有人民币的数据
         and tellerno =12 --明细中，b2c的条件
         and drcrf=1
         and trxcode in (39833)
         and CASHNOTE ='b2c'
         order by accno,timestmp;


       --对手游标 对手不为空的
       cursor c_recipact (
              i_sn number,--同查询批次中相同序号
              i_amount number,--金额相同
              i_currtype number,--币种相同
              i_busidate varchar2 , --同一天,
              i_recipacc  varchar2 --对手
              ) is
         select * from SADTL_RECIPACT_TMP
       where   dtl_seq is null --未有明细与之匹配的
       and datasource='SDA_S_EFMERROT' --明细中，B2C的条件
       and sn=i_sn
       and amount=i_amount
       and currtype=i_currtype
       and busidate=i_busidate

       and decode(i_recipacc,null,'1',RECIPACT) =decode(i_recipacc,null,'1',i_recipacc)

       order by busitime;
       begin


      --打开明细游标
       open c_dtl;
       loop
       fetch c_dtl into v_dtl;
       exit when c_dtl%NOTFOUND;
         --打开对手游标
         open c_recipact(v_dtl.sn,v_dtl.amount,v_dtl.currtype,v_dtl.busidate,v_dtl.recipact);
         loop
         fetch c_recipact into v_recipact;
         exit when c_recipact%NOTFOUND;
           --更新匹配到的信息
           update SADTL_DTL_tmp a
               set
                   --FIX_RBNKNO    =   v_recipact.RBNKNO,
                   --FIX_RBNKNAME  =   decode(a.REbkNAME,null,v_recipact.RBNKNAME ,a.REbkNAME),
                   --FIX_BAKMSG     =   v_recipact.BAKMSG,
                   --FIX_INMSG     =   v_recipact.inmsg,
                   FIX_RECIPACT  =   decode(a.recipact,null,v_recipact.RECIPACT ,a.recipact),
                   --FIX_RSAVNAME  =   decode(a.RESAVNAME,null,v_recipact.RSAVNAME ,a.RESAVNAME),
                    recip_seq = v_recipact.recip_seq
           where a.dtl_seq=v_dtl.dtl_seq;

           --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细
           update SADTL_RECIPACT_TMP b
               set b.dtl_seq=v_dtl.dtl_seq
           where b.recip_seq=v_recipact.recip_seq;

           commit;
           exit when 1=1 ;

         end loop;
         close c_recipact;

         commit;
       end loop;
       close c_dtl;
       commit;
         EXCEPTION
           WHEN OTHERS THEN

           PCK_CREATE_SADTL_PUB.public_log(i_workdate ,'match_b2c',substr( sqlerrm,1,100));
           commit;
           raise;
       end;

       --2.5 pos
       procedure match_pos (i_workdate in  varchar2)
       is

       v_dtl SADTL_DTL_tmp%ROWTYPE;
       v_recipact SADTL_RECIPACT_TMP%ROWTYPE;

       --明细游标
       cursor c_dtl is
         select * from SADTL_DTL_tmp
         where
         --and recipact is null --对手不为空, 对手不为空,可以补充终端号
          recip_seq is null --未匹配对手的
         AND CURRTYPE=1 --对手只有人民币的数据
         and tellerno =3 --明细中，b2c的条件
         and drcrf=1
         and trxcode in (2860)
         order by  accno,timestmp;

       --对手游标
       cursor c_recipact(
              i_sn number,--同查询批次中相同序号
              i_amount number,--金额相同
              i_currtype number,--币种相同
              i_busidate varchar2,--同一天
              i_recipacc  varchar2 --对手
              ) is
         select * from SADTL_RECIPACT_TMP
       where
         dtl_seq is null --未有明细与之匹配的
       and datasource='BCA_S_BFHINPOS' --明细中，pos 的条件
       and sn=i_sn

       and decode(i_recipacc,null,'1',RECIPACT) =decode(i_recipacc,null,'1',get_full_accno( i_recipacc) )

       and amount=i_amount
       and currtype=i_currtype
       and busidate=i_busidate
       order by busitime;

       begin


      --打开明细游标
       open c_dtl;
       loop
       fetch c_dtl into v_dtl;
       exit when c_dtl%NOTFOUND;
         --打开对手游标
         open c_recipact(v_dtl.sn,v_dtl.amount,v_dtl.currtype,v_dtl.busidate,v_dtl.recipact);
         loop
         fetch c_recipact into v_recipact;
         exit when c_recipact%NOTFOUND;
           --更新匹配到的信息
           update SADTL_DTL_tmp a
               set
                   --FIX_RBNKNO    =   v_recipact.RBNKNO,
                   --FIX_RBNKNAME  =   decode(a.REbkNAME,null,v_recipact.RBNKNAME ,a.REbkNAME),
                   --FIX_BAKMSG     =   v_recipact.BAKMSG,
                   --FIX_INMSG     =   v_recipact.inmsg,
                   FIX_RECIPACT  =   decode(a.recipact,null,v_recipact.RECIPACT ,a.recipact),
                   --FIX_RSAVNAME  =   decode(a.RESAVNAME,null,v_recipact.RSAVNAME ,a.RESAVNAME),
                   recip_seq = v_recipact.recip_seq
           where a.dtl_seq=v_dtl.dtl_seq;

           --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细
           update SADTL_RECIPACT_TMP b
               set b.dtl_seq=v_dtl.dtl_seq
           where b.recip_seq=v_recipact.recip_seq;

           commit;
           exit when 1=1 ;

         end loop;
         close c_recipact;

         commit;
       end loop;
       close c_dtl;
       commit;
         EXCEPTION
           WHEN OTHERS THEN

           PCK_CREATE_SADTL_PUB.public_log(i_workdate ,'match_pos',substr( sqlerrm,1,100));
           commit;
           raise;
       end;


        --2.6 salary
       procedure match_salary (i_workdate in  varchar2)
       is

       v_dtl SADTL_DTL_tmp%ROWTYPE;
       v_recipact SADTL_RECIPACT_TMP%ROWTYPE;

       --明细游标
       cursor c_dtl is
         select * from SADTL_DTL_tmp
         where
           recipact is null --对手不为空, 这里只用判断对手不为空，因为对手表中，也没有帐户名称，不存在对手为空，但是需要从这里补对手名称的情况
         and recip_seq is null --未匹配对手的

         AND CURRTYPE=1 --对手只有人民币的数据

         and drcrf=2 --明细中，代发工资的条件
         and ( trxcode in (71050)  and cashnote like '工资') --还需要增加
         order by accno,timestmp;

       --对手游标
       cursor c_recipact(
              i_sn number,--同查询批次中相同序号
              i_amount number,--金额相同
              i_currtype number,--币种相同
              i_busidate varchar2 --同一天
              ) is
         select * from SADTL_RECIPACT_TMP
       where
         dtl_seq is null --未有明细与之匹配的
       and datasource='cmmid.Bdm_Ev_Agtsal' --对手中，代发工资 的条件
       and sn=i_sn
       and amount=i_amount
       and currtype=i_currtype
       and busidate=i_busidate
       order by busitime;

       begin


      --打开明细游标
       open c_dtl;
       loop
       fetch c_dtl into v_dtl;
       exit when c_dtl%NOTFOUND;
         --打开对手游标
         open c_recipact(v_dtl.sn,v_dtl.amount,v_dtl.currtype,v_dtl.busidate);
         loop
         fetch c_recipact into v_recipact;
         exit when c_recipact%NOTFOUND;
           --更新匹配到的信息
           update SADTL_DTL_tmp a
               set
                   --FIX_RBNKNO    =   v_recipact.RBNKNO,
                   --FIX_RBNKNAME  =   decode(a.REbkNAME,null,v_recipact.RBNKNAME ,a.REbkNAME),
                   --FIX_BAKMSG     =   v_recipact.BAKMSG,
                   --FIX_INMSG     =   v_recipact.inmsg,
                   FIX_RECIPACT  =   decode(a.recipact,null,v_recipact.RECIPACT ,a.recipact),
                   --FIX_RSAVNAME  =   decode(a.RESAVNAME,null,v_recipact.RSAVNAME ,a.RESAVNAME),
                   recip_seq = v_recipact.recip_seq
           where a.dtl_seq=v_dtl.dtl_seq;

           --将对手表中记录设置为已经使用，这条对手记录不会再匹配给第2条明细
           update SADTL_RECIPACT_TMP b
               set b.dtl_seq=v_dtl.dtl_seq
           where b.recip_seq=v_recipact.recip_seq;
           commit;
           exit when 1=1 ;

         end loop;
         close c_recipact;

         commit;
       end loop;
       close c_dtl;
       commit;
         EXCEPTION
           WHEN OTHERS THEN

           PCK_CREATE_SADTL_PUB.public_log(i_workdate ,'match_salary',substr( sqlerrm,1,100));
           commit;
           raise;
       end;


END PCK_MATCH_SADTL;


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

CREATE OR REPLACE PACKAGE BODY pckg_hdm_public_util
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

CREATE OR REPLACE PACKAGE BODY PCKG_TOEBANK_QUERY
IS

    /******************************************************************************
     --函数名称： PROC_BATCH_INIT
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_BATCH_INIT (
            p_i_date            IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2)                          --0：导出完成 1：导出未完成
      IS
           v_batch_date       VARCHAR2(10);
           v_batch_no         VARCHAR2(10);
           v_proc_name        VARCHAR2(100);

           v_step             VARCHAR2(10);
           v_proc_err_code    VARCHAR2 (500) := null;
           v_proc_err_txt     VARCHAR2 (500) := null;
           v_max_data_date    VARCHAR2 (10)  := NULL;
        BEGIN
           p_o_succeed := '0';
           v_batch_date := SUBSTR(p_i_date, 1, 8);
           v_batch_no := SUBSTR(p_i_date, 10);
           v_proc_name  := 'PROC_BATCH_INIT_'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS');
           --任务日志表中记录开始运行状态
           hdm.pckg_hdm_public_util.proc_writelog (v_batch_date,
                                                   v_proc_name,
                                                   '0',
                                                   '0',
                                                   0,
                                                   '任务已开始',
                                                   1);
           v_step := '1';

           -- 最新批量加载日期
           select max(data_date)
            into v_max_data_date
            from hdm_datasource_config_dat
            where business_type = '2';

            v_step := '2';
            -- 更新待查询状态
            UPDATE HDM_QUERY_LOG_DAT_3
            SET STATUS = STATUS_READY
            WHERE STATUS = STATUS_WAITING
              AND QUERY_DATE_TO <= v_max_data_date;

           v_step := '3';
           -- 更新临时表查询状态，再插入正式请求表
           -- 如果查询结束日期大于已加载数据批量日期，查询请求先不处理
           UPDATE HDM_CMP_GROUP_DAT
           SET STATUS = STATUS_WAITING,REQUEST_TYPE = REQUEST_TYPE_CMP
           WHERE QUERY_DATE > v_max_data_date;
           --  否则初始化查询状态为已就绪，值为"0"
           UPDATE HDM_CMP_GROUP_DAT
           SET STATUS = STATUS_READY,REQUEST_TYPE = REQUEST_TYPE_CMP
           WHERE QUERY_DATE <= v_max_data_date;

           v_step := '4';
           -- 插入查询处理表
           INSERT INTO HDM_QUERY_LOG_DAT_3(ESERIALNO,
                                          AGACCNO,
                                          SEQNO,
                                          QUERY_DATE_TO,
                                          ACCTSEQNAME,
                                          CORPNMC,
                                          BANKNAME,
                                          GROUP_ID,
                                          REQUEST_DATE,
                                          REQUEST_TYPE,
                                          STATUS)
          SELECT TRADE_ID,
                  ACCNO,
                  SEQNO,
                  QUERY_DATE,
                  ACCTSEQNAME,
                  CORPNMC,
                  BANKNAME,
                  GROUP_ID,
                  REQUEST_DATE,
                  REQUEST_TYPE,
                  STATUS
           FROM HDM_CMP_GROUP_DAT;

           COMMIT;

           v_step := '5';
           -- 备份和清理表HDM_QUERY_LOG_DAT_3
           IF SUBSTR(v_batch_date,7,2) = '01' AND v_batch_no = '1' THEN

                    INSERT INTO HDM_QUERY_LOG_DAT_3_HIS
                    SELECT * FROM HDM_QUERY_LOG_DAT_3
                    WHERE REQUEST_DATE < TO_CHAR(TO_DATE(v_batch_date,'YYYYMMDD')-30, 'YYYYMMDD');

                    DELETE /*+ INDEX(a INX_QUERY_LOG_DAT_3_1) */ FROM HDM_QUERY_LOG_DAT_3 a
                    WHERE REQUEST_DATE < TO_CHAR(TO_DATE(v_batch_date,'YYYYMMDD')-30, 'YYYYMMDD');
                    COMMIT;
           END IF;

           v_step := '6';
           --  清理表HDM_QUERY_LOG_DAT_3_HIS
           IF SUBSTR(v_batch_date,7,2) = '01' AND SUBSTR(v_batch_date,4,5) IN ('06','12') AND v_batch_no = '1' THEN
                    DELETE /*+ INDEX(a INX_QUERY_LOG_DAT_3_HIS_1) */FROM HDM_QUERY_LOG_DAT_3_HIS a
                    WHERE REQUEST_DATE < TO_CHAR(TO_DATE(v_batch_date,'YYYYMMDD')-360, 'YYYYMMDD');
                    COMMIT;
           END IF;

        EXCEPTION
           WHEN OTHERS
           THEN
              v_proc_err_code := SQLCODE;
              v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
              --任务日志表中记录出错状态
              pckg_hdm_public_util.proc_writelog (
                 v_batch_date,
                 v_proc_name,
                 '0',
                 '0',
                 99,
                    '任务出错! '
                 || '出错步骤号为: '
                 || v_step
                 || ' || 错误码为: '
                 || v_proc_err_code
                 || ' || 错误信息为: '
                 || v_proc_err_txt,
                 1);
              p_o_succeed := '1';
              RAISE;
      END PROC_BATCH_INIT;

      /******************************************************************************
     --函数名称： PROC_EXPORT_EBANK
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_EXPORT_EBANK (
            p_i_date            IN VARCHAR2,
            p_serial_num          IN VARCHAR2,
            p_request_date          IN VARCHAR2,
            P_file_name        IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2)                          --0：导出完成 1：导出未完成
      IS
           v_batch_date       VARCHAR2(10);
           v_batch_no         VARCHAR2(10);
           v_proc_name        VARCHAR2(100);
           v_file             UTL_FILE.file_type;
           v_tbl_num          NUMBER (20);                                  ---查询条件记录数
           v_status           NUMBER (20);
           nase_result        SYS_REFCURSOR;
           v_str              VARCHAR2(32767);
           v_sql_text         VARCHAR2(32767);
           v_query_date       VARCHAR2(20);
           v_count            NUMBER;
           v_step             VARCHAR2(10);
           v_proc_err_code    VARCHAR2 (500) := null;
           v_proc_err_txt     VARCHAR2 (500) := null;
           v_accno            VARCHAR2(30);
           v_seqno            VARCHAR2(30);
           v_qry_date_from    VARCHAR2(30);
           v_qry_date_to      VARCHAR2(30);
           v_where_str        VARCHAR2(32767);
           v_order_str        VARCHAR2(32767);
           TYPE typ_rec_nase IS RECORD (
                       ACCNO    VARCHAR2(17),
                       ZONENO  VARCHAR2(4),
                       TPHYBRNO VARCHAR2(4),
                       TRXCODE  VARCHAR2(5),
                       BUSIDATE VARCHAR2(10),
                       BUSITIME VARCHAR2(8),
                       TIMESTMP VARCHAR2(26),
                       DRCRF    VARCHAR2(1),
                       CASHF    VARCHAR2(1),
                       UPDTRANF VARCHAR2(1),
                       DEBITAMOUNT  NUMBER(18),
                       CREDITAMOUNT NUMBER(18),
                       BALANCE      NUMBER(18),
                       USEABLEBALANCE NUMBER(18),
                       CURRTYPE     VARCHAR2(3),
                       OPERTYPE     VARCHAR2(3),
                       VOUHTYPE     VARCHAR2(9),
                       VOUHNO       VARCHAR2(17),
                       SUMMARY      VARCHAR2(40),
                       PURPOSE      VARCHAR2(20),
                       NOTES        VARCHAR2(140),
                       SUBACCNO     VARCHAR2(1),
                       RAWRECIPACC  VARCHAR2(34),
                       RAWRECIPNAM  VARCHAR2(60),
                       NEAGNFLG     VARCHAR2(9),
                       RECIPACC     VARCHAR2(34),
                       RECIPNAM     VARCHAR2(60),
                       RECIPBKN     VARCHAR2(12),
                       RECIPBNA     VARCHAR2(60),
                       TELLERNO     VARCHAR2(5),
                       EVENTSEQ     VARCHAR2(9),
                       PTRXSEQ      VARCHAR2(7),
                       REF          VARCHAR2(20),
                       OREF         VARCHAR2(20),
                       SERIALNO     VARCHAR2(11),
                       RECIPAC2     VARCHAR2(34),
                       RECIPNA2     VARCHAR2(60),
                       CARDNO       VARCHAR2(19),
                       FEFLAG       VARCHAR2(1),
                       DETAILF      VARCHAR2(1),
                       VALUEDAY     VARCHAR2(10),
                       ACSERNO      VARCHAR2(9),
                       FINFO        VARCHAR2(600),
                       BUSTYPNO     VARCHAR2(3),
                       BAKCHAR3     VARCHAR2(60)
           );
           rec_nase typ_rec_nase;
        BEGIN
           p_o_succeed := '0';
           v_batch_date := SUBSTR(p_i_date, 1, 8);
           v_batch_no := SUBSTR(p_i_date, 10);
           v_proc_name  := 'PROC_EXPORT_EBANK_'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||DBMS_RANDOM.RANDOM();
           --任务日志表中记录开始运行状态
           hdm.pckg_hdm_public_util.proc_writelog (v_batch_date,
                                                   v_proc_name,
                                                   '0',
                                                   '0',
                                                   0,
                                                   '任务已开始',
                                                   1);
           v_step := '1';

            -- 打开文件，如果没有查询结果，则空文件
            v_file := UTL_FILE.fopen ('EXPORT_EBANK_WLH',  P_file_name, 'w', 32767);
            v_step := '2';
            -- 获取查询条件，前台拼接串转换为SQL条件
            FOR i IN (
            SELECT AGACCNO,SEQNO,QUERY_DATE_FROM,QUERY_DATE_TO,QRY_WHERE,QRY_ORDER
            FROM HDM_QUERY_LOG_DAT_3
            WHERE ESERIALNO = p_serial_num
                AND REQUEST_DATE = p_request_date
                AND REQUEST_TYPE = REQUEST_TYPE_EBANK)
            LOOP
                v_accno := i.AGACCNO;
                v_seqno := i.SEQNO;
                v_qry_date_from := i.QUERY_DATE_FROM;
                v_qry_date_to := i.QUERY_DATE_TO;
                v_where_str := i.QRY_WHERE;
                v_order_str := i.QRY_ORDER;
                v_step := '3';
               -- 转换查询条件
               v_where_str := GET_QUERY_WHERE_STRING(v_where_str, 't', v_qry_date_from, v_qry_date_to);
               v_step := '4';
               -- 转换排序字段
               v_order_str := GET_QUERY_ORDER_STRING(v_order_str, 't');
               v_step := '5';
               v_sql_text := 'SELECT /*+index(t) index(t1) use_nl(t t1)*/
                           LPAD (t.AGACCNO, 17, ''0'') as ACCNO,
                           SUBSTR(t.ZONENO, -4) as ZONENO,
                           SUBSTR(t.TORGANNO, -4) as TPHYBRNO,
                           t.TRXCODE,
                           t.BUSIDATE,
                           t.BUSITIME,
                           t.TIMESTMP,
                           t.DRCRF,
                           t.CASHF,
                           t.UPDTRANF,
                           DECODE(t.DRCRF, ''1'', t.AMOUNT, NULL) as DEBITAMOUNT,
                           DECODE(t.DRCRF, ''2'', NULL, t.AMOUNT) as CREDITAMOUNT,
                           t.BALANCE,
                           t.BAKDEC as USEABLEBALANCE,
                           t.CURRTYPE,
                           t.OPERTYPE,
                           t.VOUHTYPE,
                           t.VOUHNO,
                           t.SUMMARY,
                           t.PURPOSE,
                           t.NOTES,
                           t.SUBACCNO,
                           t.RECIPACC as RAWRECIPACC,
                           t.RECIPNAM as RAWRECIPNAM,
                           t.NEAGNFLG,
                           CASE WHEN t.DRCRF = ''2'' AND SUBSTR(t.NEAGNFLG,1,7) = ''AGENTEX'' THEN t.OTACTNO ELSE t.RECIPACC END as RECIPACC,
                           CASE WHEN t.DRCRF = ''2'' AND SUBSTR(t.NEAGNFLG,1,7) = ''AGENTEX'' THEN t.OTACTNAM ELSE t.RECIPNAM END as RECIPNAM,
                           t.RECIPBKN,
                           t.RECIPBNA,
                           t.TELLERNO,
                           t.EVENTSEQ,
                           t.PTRXSEQ,
                           t.REF,
                           t.OREF,
                           t.SERIALNO,
                           t.RECIPAC2,
                           t.RECIPNA2,
                           t.CARDNO,
                           CASE WHEN t.DRCRF = ''2'' AND SUBSTR(t.AGACCNO,9,2) IN (''09'',''19'',''29'',''39'') AND t.CURRTYPE <> ''001'' THEN ''1'' ELSE ''0'' END as FEFLAG,
                           t.DETAILF,
                           t.VALUEDAY,
                           :seqno as ACSERNO,
                           t1.FINFO,
                           t1.BUSTYPNO,
                           t1.BAKCHAR3
                      FROM HDM_S_NFQCHDTL_QUERY_DAT t, HDM_S_NFQCLINF_QUERY_DAT t1
                     WHERE     t.AGACCNO = :p_accno
                           AND t.ZONEACCNO = SUBSTR (:p_accno, 1, 4)
                           AND t.BUSIDATE BETWEEN :p_start_date AND :p_end_date
                           AND t.agaccno = t1.agaccno(+)
                           AND t1.ZONEACCNO(+) = SUBSTR (:p_accno, 1, 4)
                           AND t.busidate = t1.busidate(+)
                           AND t.SERIALNO = t1.SERIALNO(+)
                           AND t1.busidate(+) BETWEEN :p_start_date AND :p_end_date ';
               -- 拼上附加条件
               IF v_where_str IS NOT NULL THEN
                    v_sql_text := v_sql_text||CHR(13)||CHR(10)||v_where_str;
               END IF;
               -- 拼上排序列
               IF v_order_str IS NOT NULL THEN
                    v_sql_text := v_sql_text||CHR(13)||CHR(10)||v_order_str;
               END IF;
                v_step := '6';
               -- 导出明细
               OPEN nase_result FOR v_sql_text USING v_seqno, v_accno,v_accno,v_qry_date_from,v_qry_date_to,v_accno,v_qry_date_from,v_qry_date_to;
               LOOP
                  v_step := '7';
                  FETCH nase_result INTO rec_nase;
                  EXIT WHEN nase_result%NOTFOUND;

                  v_step := '8';
                    v_str :=
                           rec_nase.ACCNO
                        || CHR (27)
                        || rec_nase.ZONENO
                        || CHR (27)
                        || rec_nase.TPHYBRNO
                        || CHR (27)
                        || rec_nase.TRXCODE
                        || CHR (27)
                        || rec_nase.BUSIDATE
                        || CHR (27)
                        || rec_nase.BUSITIME
                        || CHR (27)
                        || rec_nase.TIMESTMP
                        || CHR (27)
                        || rec_nase.DRCRF
                        || CHR (27)
                        || rec_nase.CASHF
                        || CHR (27)
                        || rec_nase.UPDTRANF
                        || CHR (27)
                        || rec_nase.DEBITAMOUNT
                        || CHR (27)
                        || rec_nase.CREDITAMOUNT
                        || CHR (27)
                        || rec_nase.BALANCE
                        || CHR (27)
                        || rec_nase.USEABLEBALANCE
                        || CHR (27)
                        || rec_nase.CURRTYPE
                        || CHR (27)
                        || rec_nase.OPERTYPE
                        || CHR (27)
                        || rec_nase.VOUHTYPE
                        || CHR (27)
                        || rec_nase.VOUHNO
                        || CHR (27)
                        || rec_nase.SUMMARY
                        || CHR (27)
                        || rec_nase.PURPOSE
                        || CHR (27)
                        || rec_nase.NOTES
                        || CHR (27)
                        || rec_nase.SUBACCNO
                        || CHR (27)
                        || rec_nase.RAWRECIPACC
                        || CHR (27)
                        || rec_nase.RAWRECIPNAM
                        || CHR (27)
                        || rec_nase.NEAGNFLG
                        || CHR (27)
                        || rec_nase.RECIPACC
                        || CHR (27)
                        || rec_nase.RECIPNAM
                        || CHR (27)
                        || rec_nase.RECIPBKN
                        || CHR (27)
                        || rec_nase.RECIPBNA
                        || CHR (27)
                        || rec_nase.TELLERNO
                        || CHR (27)
                        || rec_nase.EVENTSEQ
                        || CHR (27)
                        || rec_nase.PTRXSEQ
                        || CHR (27)
                        || rec_nase.REF
                        || CHR (27)
                        || rec_nase.OREF
                        || CHR (27)
                        || rec_nase.SERIALNO
                        || CHR (27)
                        || rec_nase.RECIPAC2
                        || CHR (27)
                        || rec_nase.RECIPNA2
                        || CHR (27)
                        || rec_nase.CARDNO
                        || CHR (27)
                        || rec_nase.FEFLAG
                        || CHR (27)
                        || rec_nase.DETAILF
                        || CHR (27)
                        || rec_nase.VALUEDAY
                        || CHR (27)
                        || rec_nase.ACSERNO
                        || CHR (27)
                        || rec_nase.FINFO
                        || CHR (27)
                        || rec_nase.BUSTYPNO
                        || CHR (27)
                        || rec_nase.BAKCHAR3
                        ;
                     UTL_FILE.put_line (v_file, v_str, FALSE);
                     UTL_FILE.fflush (v_file);
               END LOOP;
          END LOOP;
          v_step := '9';
          UTL_FILE.fclose (v_file);
           p_o_succeed := '0';
        EXCEPTION
           WHEN OTHERS
           THEN
              v_proc_err_code := SQLCODE;
              v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
              --任务日志表中记录出错状态
              pckg_hdm_public_util.proc_writelog (
                 v_batch_date,
                 v_proc_name,
                 '0',
                 '0',
                 99,
                    '任务出错! '
                 || '出错步骤号为: '
                 || v_step
                 || ' || 错误码为: '
                 || v_proc_err_code
                 || ' || 错误信息为: '
                 || v_proc_err_txt,
                 1);
              p_o_succeed := '1';
              RAISE;
      END PROC_EXPORT_EBANK;

       /******************************************************************************
     --函数名称： PROC_EXPORT_CMP
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_EXPORT_CMP (
            p_i_date            IN VARCHAR2,
            p_serial_num          IN VARCHAR2,
            p_request_date          IN VARCHAR2,
            P_file_name        IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2)                          --0：导出完成 1：导出未完成
      IS
           v_file_name        VARCHAR2(60);
           v_to_file        VARCHAR2(60);
           v_batch_date       VARCHAR2(10);
           v_batch_no         VARCHAR2(10);
           v_proc_name        VARCHAR2(100);
           v_file             UTL_FILE.file_type;
           v_tbl_num          NUMBER (20);                                  ---查询条件记录数
           v_status           NUMBER (20);
           v_acctseqname      VARCHAR2(100);
           v_corpnmc      VARCHAR2(100);
           v_bankname      VARCHAR2(100);
           v_group_id      VARCHAR2(100);
           nase_result        SYS_REFCURSOR;
           v_str              VARCHAR2(32767);
           v_sql_text         VARCHAR2(32767);
           v_where_str        VARCHAR2(32767);
           v_query_date       VARCHAR2(20);
           v_count            NUMBER;
           v_total            NUMBER;
           v_step             VARCHAR2(10);
           v_proc_err_code    VARCHAR2 (500) := null;
           v_proc_err_txt     VARCHAR2 (500) := null;
           v_accno            VARCHAR2(30);
           v_seqno    VARCHAR2(30);
           v_qry_date_to      VARCHAR2(30);
           v_row_id           VARCHAR2(100);
           v_ensummry         VARCHAR2(60);
        BEGIN
           p_o_succeed := '0';
           v_batch_date := SUBSTR(p_i_date, 1, 8);
           v_batch_no := SUBSTR(p_i_date, 10);
           v_proc_name  := 'PROC_EXPORT_CMP_'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||DBMS_RANDOM.RANDOM();
           --任务日志表中记录开始运行状态
           hdm.pckg_hdm_public_util.proc_writelog (v_batch_date,
                                                   v_proc_name,
                                                   '0',
                                                   '0',
                                                   0,
                                                   '任务已开始',
                                                   1);
           v_step := '1';
           v_total := 0;
           -- 获取集团ID，命名文件
            SELECT GROUP_ID
                INTO v_group_id
            FROM HDM_QUERY_LOG_DAT_3
            WHERE ESERIALNO = p_serial_num
                AND REQUEST_DATE = p_request_date
                AND REQUEST_TYPE = REQUEST_TYPE_CMP
                AND ROWNUM = 1;
           dbms_output.put_line(v_group_id);
            -- 集团ID命名文件
            v_file_name := REGEXP_REPLACE(p_file_name, '\*', v_group_id, 1, 1);
            v_file := UTL_FILE.fopen ('EXPORT_EBANK_WLH',  v_file_name, 'w', 32767);

            -- 获取查询条件，前台拼接串转换为SQL条件
            FOR i IN (
            SELECT AGACCNO,SEQNO,QUERY_DATE_TO,ACCTSEQNAME,CORPNMC,BANKNAME,GROUP_ID
            FROM HDM_QUERY_LOG_DAT_3
            WHERE ESERIALNO = p_serial_num
                AND REQUEST_DATE = p_request_date
                AND REQUEST_TYPE = REQUEST_TYPE_CMP)
            LOOP

                v_accno := i.AGACCNO;
                v_seqno := i.SEQNO;
                v_qry_date_to := i.QUERY_DATE_TO;
                v_acctseqname := i.ACCTSEQNAME;
                v_corpnmc := i.CORPNMC;
                v_bankname := i.BANKNAME;
                v_group_id := i.GROUP_ID;
                v_where_str := '';
                -- 非账户管家账号
                IF v_seqno IS NULL THEN
                    v_where_str := '';
                END IF;

                -- 账户管家账号，获取实体账号AGACCNO
                IF v_seqno IS NOT NULL THEN
                    v_where_str := 'BAKCHAR IS NOT NULL';
                    BEGIN
                        SELECT TAGACCNO
                            INTO v_accno
                         FROM HDM_S_NFDGPSUB_QUERY_DAT t
                         WHERE GPAGACC = v_accno
                          AND SEQNO = v_seqno
                          AND ROWNUM = 1;
                    EXCEPTION
                        WHEN OTHERS THEN
                        v_accno := '';
                    END;
                END IF;

                v_row_id := NULL;
                v_ensummry := NULL;
                v_count := 0;
                FOR rec_nase IN (
                    SELECT /*+ index(t1) use_nl(tt t1)*/
                    tt.ACCNO,
                    tt.SEQNO,
                    tt.QUERY_DATE,
                    tt.ACCTSEQNAME,
                    tt.PAYACCOUNT,
                    tt.PAYACCTNAME,
                    tt.PAYBANKNAME,
                    tt.RECACCOUNT,
                    tt.RECACCTNAME,
                    tt.RECBANKNAME,
                    tt.AMOUNT,
                    tt.CURRTYPE,
                    tt.SUMMARY,
                    tt.BUSTYPE,
                    tt.USECN,
                    tt.TRANSERIALNO,
                    tt.TIMESTMP,
                    tt.REMARK,
                    tt.TRANSNETCODE,
                    tt.TELLERNO,
                    tt.BUSIDATE,
                    tt.NEAGNFLG,
                    tt.OTACTNO,
                    tt.OTACTNAM,
                    tt.UPDTRANF,
                    tt.VALUEDAY,
                    tt.ACCTSEQ,
                    tt.DRCRF,
                    tt.SERIALNO,
                    tt.VOUCHNO,
                    tt.VOUHTYPE,
                    tt.BUSITIME,
                    tt.ACCTNUM,
                    tt.DEBITAMOUNT,
                    tt.CREDITAMOUNT,
                    tt.TRXCODE,
                    tt.RECIPAC2,
                    tt.RECIPNA2,
                    tt.REF,
                    tt.OREF,
                    tt.CARDNO,
                    tt.ROW_ID,
                    t1.FINFO,
                    t1.BUSTYPNO
                    FROM (SELECT /*+index(t)*/
                            v_accno as ACCNO,
                            v_seqno as SEQNO,
                            v_qry_date_to as QUERY_DATE,
                            v_acctseqname as ACCTSEQNAME,
                            DECODE(t.DRCRF, '1', v_accno||v_seqno, '2', t.RECIPACC) as PAYACCOUNT,
                            DECODE(t.DRCRF, '1', v_corpnmc, '2', t.RECIPNAM) as PAYACCTNAME,
                            DECODE(t.DRCRF, '1', v_bankname, '2', t.RECIPBNA) as PAYBANKNAME,
                            DECODE(t.DRCRF, '1', t.RECIPACC, '2', v_accno||v_seqno) as RECACCOUNT,
                            DECODE(t.DRCRF, '1', t.RECIPNAM, '2', v_corpnmc) as RECACCTNAME,
                            DECODE(t.DRCRF, '1', t.RECIPBNA, '2', v_bankname) as RECBANKNAME,
                            t.AMOUNT,
                            t.CURRTYPE,
                            t.SUMMARY,
                            t.OPERTYPE as BUSTYPE,
                            t.PURPOSE as USECN,
                            SUBSTR(t.EVENTSEQ, -5)||SUBSTR(t.PTRXSEQ, -3) as TRANSERIALNO,
                            t.TIMESTMP,
                            t.NOTES as REMARK,
                            SUBSTR(t.TORGANNO, -4) as TRANSNETCODE,
                            t.TELLERNO,
                            REPLACE(t.BUSIDATE,'-') as BUSIDATE,
                            t.NEAGNFLG,
                            t.OTACTNO,
                            t.OTACTNAM,
                            t.UPDTRANF,
                            t.VALUEDAY,
                            SUBSTR(TRIM(t.BAKCHAR), 1, 26) as ACCTSEQ,
                            t.DRCRF,
                            t.SERIALNO,
                            LTRIM(t.VOUHNO, '0') as VOUCHNO,
                            SUBSTR(t.VOUHTYPE, -3) as VOUHTYPE,
                            t.BUSITIME,
                            FUNC_ACCNOTRANSFER(t.ACCNO) as ACCTNUM,
                            DECODE(t.DRCRF, '1', t.AMOUNT, '2', NULL) as DEBITAMOUNT,
                            DECODE(t.DRCRF, '1', NULL, '2', t.AMOUNT) as CREDITAMOUNT,
                            t.TRXCODE,
                            t.RECIPAC2,
                            t.RECIPNA2,
                            t.REF,
                            t.OREF,
                            t.CARDNO,
                            t.BAKCHAR,
                            ROWNUM as ROW_ID
                          FROM HDM_S_NFQCHDTL_QUERY t
                         WHERE  t.AGACCNO = v_accno
                           AND t.ZONEACCNO = SUBSTR (v_accno, 1, 4)
                           AND t.BUSIDATE = v_qry_date_to) tt, HDM_S_NFQCLINF_QUERY t1
                        WHERE v_accno = t1.AGACCNO(+)
                           AND SUBSTR(v_accno, 1, 4) = t1.ZONEACCNO(+)
                           AND v_qry_date_to = t1.BUSIDATE(+)
                           AND tt.SERIALNO = t1.SERIALNO(+)
                           AND tt.TIMESTMP = t1.TIMESTMP(+))
                LOOP

                    -- 非账户管家，要求BAKCHAR的前26位为空
                    IF v_where_str IS NULL AND rec_nase.ACCTSEQ IS NOT NULL THEN
                        CONTINUE;
                    END IF;
                    -- 账户管家，要求BAKCHAR的前26位非空
                    IF v_where_str IS NOT NULL AND rec_nase.ACCTSEQ IS NULL THEN
                        CONTINUE;
                    END IF;

                    -- 如果补充明细匹配出多条，获取第一条记录
                    IF v_row_id IS NOT NULL AND v_row_id = rec_nase.ROW_ID THEN
                        CONTINUE;
                    END IF;
                    v_row_id := rec_nase.ROW_ID;
                    v_count := 1;
                    -- 获取英文备注，如果匹配出多条记录，优先获取CMPCHGF='1'的记录
                    BEGIN
                        SELECT /*+ index(a) */ENSUMMRY
                          INTO v_ensummry
                          FROM (
                              SELECT ENSUMMRY, DECODE(CMPCHGF, '1', '99', '0') CMPCHGF
                              FROM HDM_S_NBWKFAR_QUERY_DAT a
                              WHERE ZONEACCNO = SUBSTR(v_accno,1,4)
                                AND ACCNO = v_accno
                                AND BUSIDATE = v_qry_date_to
                                AND BUSITIME = rec_nase.BUSITIME
                                AND DRCRF = rec_nase.DRCRF
                                AND AMOUNT = rec_nase.AMOUNT
                                AND VOUHTYPE = rec_nase.VOUHTYPE
                                AND VOUHNO = rec_nase.VOUCHNO
                                AND TRXSQNB||TRXSQNS = rec_nase.TRANSERIALNO
                                AND BRNO = rec_nase.TRANSNETCODE
                                AND TELLERNO = rec_nase.TELLERNO
                                AND TRXCODE = rec_nase.TRXCODE
                                ORDER BY DECODE(CMPCHGF, '1', '99', '0') DESC)
                          WHERE ROWNUM = 1;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_ensummry := '';
                    END;

                    v_str :=
                       rec_nase.ACCNO
                    || CHR (27)
                    || rec_nase.SEQNO
                    || CHR (27)
                    || rec_nase.QUERY_DATE
                    || CHR (27)
                    || rec_nase.ACCTSEQNAME
                    || CHR (27)
                    || rec_nase.PAYACCOUNT
                    || CHR (27)
                    || rec_nase.PAYACCTNAME
                    || CHR (27)
                    || rec_nase.PAYBANKNAME
                    || CHR (27)
                    || rec_nase.RECACCOUNT
                    || CHR (27)
                    || rec_nase.RECACCTNAME
                    || CHR (27)
                    || rec_nase.RECBANKNAME
                    || CHR (27)
                    || rec_nase.AMOUNT
                    || CHR (27)
                    || rec_nase.CURRTYPE
                    || CHR (27)
                    || rec_nase.SUMMARY
                    || CHR (27)
                    || rec_nase.BUSTYPE
                    || CHR (27)
                    || rec_nase.USECN
                    || CHR (27)
                    || rec_nase.TRANSERIALNO
                    || CHR (27)
                    || rec_nase.TIMESTMP
                    || CHR (27)
                    || rec_nase.REMARK
                    || CHR (27)
                    || rec_nase.TRANSNETCODE
                    || CHR (27)
                    || rec_nase.TELLERNO
                    || CHR (27)
                    || rec_nase.BUSIDATE
                    || CHR (27)
                    || rec_nase.NEAGNFLG
                    || CHR (27)
                    || rec_nase.OTACTNO
                    || CHR (27)
                    || rec_nase.OTACTNAM
                    || CHR (27)
                    || rec_nase.UPDTRANF
                    || CHR (27)
                    || rec_nase.VALUEDAY
                    || CHR (27)
                    || rec_nase.FINFO
                    || CHR (27)
                    || rec_nase.ACCTSEQ
                    || CHR (27)
                    || rec_nase.BUSTYPNO
                    || CHR (27)
                    || rec_nase.DRCRF
                    || CHR (27)
                    || rec_nase.SERIALNO
                    || CHR (27)
                    || rec_nase.VOUCHNO
                    || CHR (27)
                    || rec_nase.VOUHTYPE
                    || CHR (27)
                    || rec_nase.BUSITIME
                    || CHR (27)
                    || rec_nase.ACCTNUM
                    || CHR (27)
                    || rec_nase.DEBITAMOUNT
                    || CHR (27)
                    || rec_nase.CREDITAMOUNT
                    || CHR (27)
                    || rec_nase.TRXCODE
                    || CHR (27)
                    || rec_nase.RECIPAC2
                    || CHR (27)
                    || rec_nase.RECIPNA2
                    || CHR (27)
                    || rec_nase.REF
                    || CHR (27)
                    || rec_nase.OREF
                    || CHR (27)
                    || rec_nase.CARDNO
                    || CHR (27)
                    || v_ensummry
                    ;
                     UTL_FILE.put_line (v_file, v_str);
                     UTL_FILE.fflush (v_file);
                     v_total := v_total + 1;
               END LOOP;

               -- 如果没有查明细，则返回原查询请求字段，而明细部分为分隔符拼接空值
               IF v_count = 0 THEN
                    v_str :=
                       v_accno
                    || CHR (27)
                    || v_seqno
                    || CHR (27)
                    || v_qry_date_to
                    || CHR (27)
                    || v_acctseqname
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    || CHR (27)
                    ;
                     UTL_FILE.put_line (v_file, v_str);
                     UTL_FILE.fflush (v_file);
                     v_total := v_total + 1;
               END IF;
            END LOOP;
          UTL_FILE.fclose (v_file);

          -- 修改文件名，带上记录条数

          v_to_file := REGEXP_REPLACE(v_file_name, 'GROUPID', v_group_id, 1, 1);
          v_to_file := REGEXP_REPLACE(v_file_name, 'TOTAL', v_total, 1, 1);
          dbms_output.put_line(v_file_name);
          dbms_output.put_line(v_to_file);
          UTL_FILE.FRENAME('EXPORT_EBANK_WLH',v_file_name,'EXPORT_EBANK_WLH',v_to_file);

          p_o_succeed := '0';
        EXCEPTION
           WHEN OTHERS
           THEN
              v_proc_err_code := SQLCODE;
              v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
              --任务日志表中记录出错状态
              pckg_hdm_public_util.proc_writelog (
                 v_batch_date,
                 v_proc_name,
                 '0',
                 '0',
                 99,
                    '任务出错! '
                 || '出错步骤号为: '
                 || v_step
                 || ' || 错误码为: '
                 || v_proc_err_code
                 || ' || 错误信息为: '
                 || v_proc_err_txt,
                 1);
              p_o_succeed := '1';
              RAISE;
      END PROC_EXPORT_CMP;

      /******************************************************************************
     --函数名称： PROC_UPDATE_RUNNING_STATUS
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_UPDATE_RUNNING_STATUS (
            p_i_date            IN VARCHAR2,
            p_serial_num           IN VARCHAR2,
            p_request_date      IN VARCHAR2,
            p_request_type      IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2)
      IS
           v_batch_date       VARCHAR2(10);
           v_batch_no         VARCHAR2(10);
           v_proc_name        VARCHAR2(100);

           v_step             VARCHAR2(10);
           v_proc_err_code    VARCHAR2 (500) := null;
           v_proc_err_txt     VARCHAR2 (500) := null;
           v_max_data_date    VARCHAR2 (10)  := NULL;
        BEGIN
           p_o_succeed := '0';
           v_batch_date := SUBSTR(p_i_date, 1, 8);
           v_batch_no := SUBSTR(p_i_date, 10);
           v_proc_name  := 'PROC_UPDATE_RUNNING_STATUS_'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||DBMS_RANDOM.RANDOM();
           --任务日志表中记录开始运行状态
           hdm.pckg_hdm_public_util.proc_writelog (v_batch_date,
                                                   v_proc_name,
                                                   '0',
                                                   '0',
                                                   0,
                                                   '任务已开始',
                                                   1);

           v_step := '1';
           -- 更新状态为正在处理
            UPDATE HDM_QUERY_LOG_DAT_3
            SET STATUS = STATUS_RUNNING, EXEC_START_TIME = TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
            WHERE ESERIALNO = p_serial_num
                AND REQUEST_DATE = p_request_date
                AND REQUEST_TYPE = p_request_type;

            COMMIT;

        EXCEPTION
           WHEN OTHERS
           THEN
              v_proc_err_code := SQLCODE;
              v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
              --任务日志表中记录出错状态
              pckg_hdm_public_util.proc_writelog (
                 v_batch_date,
                 v_proc_name,
                 '0',
                 '0',
                 99,
                    '任务出错! '
                 || '出错步骤号为: '
                 || v_step
                 || ' || 错误码为: '
                 || v_proc_err_code
                 || ' || 错误信息为: '
                 || v_proc_err_txt,
                 1);
              p_o_succeed := '1';
              RAISE;
      END PROC_UPDATE_RUNNING_STATUS;

      /******************************************************************************
     --函数名称： PROC_UPDATE_END_STATUS
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_UPDATE_END_STATUS (
            p_i_date            IN VARCHAR2,
            p_serial_num           IN VARCHAR2,
            p_request_date      IN VARCHAR2,
            p_request_type      IN VARCHAR2,
            p_flag              IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2)
      IS
           v_batch_date       VARCHAR2(10);
           v_batch_no         VARCHAR2(10);
           v_proc_name        VARCHAR2(100);

           v_step             VARCHAR2(10);
           v_proc_err_code    VARCHAR2 (500) := null;
           v_proc_err_txt     VARCHAR2 (500) := null;
           v_max_data_date    VARCHAR2 (10)  := NULL;
        BEGIN
           p_o_succeed := '0';
           v_batch_date := SUBSTR(p_i_date, 1, 8);
           v_batch_no := SUBSTR(p_i_date, 10);
           v_proc_name  := 'PROC_UPDATE_END_STATUS_'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||DBMS_RANDOM.RANDOM();
           --任务日志表中记录开始运行状态
           hdm.pckg_hdm_public_util.proc_writelog (v_batch_date,
                                                   v_proc_name,
                                                   '0',
                                                   '0',
                                                   0,
                                                   '任务已开始',
                                                   1);

           v_step := '1';
           -- 更新状态,传入参数为0,则成功状态，否则为失败状态
           IF p_flag = '0' THEN
                UPDATE HDM_QUERY_LOG_DAT_3
                SET STATUS = STATUS_SUCCESSED, EXEC_END_TIME = TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
                WHERE ESERIALNO = p_serial_num
                    AND REQUEST_DATE = p_request_date
                    AND REQUEST_TYPE = p_request_type;
            ELSE
                UPDATE HDM_QUERY_LOG_DAT_3
                SET STATUS = STATUS_FAILED, EXEC_END_TIME = TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')
                WHERE ESERIALNO = p_serial_num
                    AND REQUEST_DATE = p_request_date
                    AND REQUEST_TYPE = p_request_type;
            END IF;

            COMMIT;

        EXCEPTION
           WHEN OTHERS
           THEN
              v_proc_err_code := SQLCODE;
              v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
              --任务日志表中记录出错状态
              pckg_hdm_public_util.proc_writelog (
                 v_batch_date,
                 v_proc_name,
                 '0',
                 '0',
                 99,
                    '任务出错! '
                 || '出错步骤号为: '
                 || v_step
                 || ' || 错误码为: '
                 || v_proc_err_code
                 || ' || 错误信息为: '
                 || v_proc_err_txt,
                 1);
              p_o_succeed := '1';
              RAISE;
      END PROC_UPDATE_END_STATUS;

      /******************************************************************************
     --函数名称： PROC_BACKUP_LOG_DAT
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_i_date (传入参数,批量日期)
     --p_o_succeed (传出参数，成功标志)
     --功能： 模版程序1
     ******************************************************************************/
      PROCEDURE PROC_BACKUP_LOG_DAT (
            p_i_date            IN VARCHAR2,
            p_o_succeed         OUT VARCHAR2)                          --0：导出完成 1：导出未完成
      IS
           v_batch_date       VARCHAR2(10);
           v_batch_no         VARCHAR2(10);
           v_proc_name        VARCHAR2(100);

           v_step             VARCHAR2(10);
           v_proc_err_code    VARCHAR2 (500) := null;
           v_proc_err_txt     VARCHAR2 (500) := null;
           v_max_data_date    VARCHAR2 (10)  := NULL;
        BEGIN
           p_o_succeed := '0';
           v_batch_date := SUBSTR(p_i_date, 1, 8);
           v_batch_no := SUBSTR(p_i_date, 10);
           v_proc_name  := 'PROC_BACKUP_LOG_DAT_'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS')||DBMS_RANDOM.RANDOM();
           --任务日志表中记录开始运行状态
           hdm.pckg_hdm_public_util.proc_writelog (v_batch_date,
                                                   v_proc_name,
                                                   '0',
                                                   '0',
                                                   0,
                                                   '任务已开始',
                                                   1);

           v_step := '1';
           -- 备份和清理表HDM_QUERY_LOG_DAT_3
           IF SUBSTR(v_batch_date,7,2) = '01' AND v_batch_no = '1' THEN

                    INSERT INTO HDM_QUERY_LOG_DAT_3_HIS
                    SELECT * FROM HDM_QUERY_LOG_DAT_3
                    WHERE REQUEST_DATE < TO_CHAR(TO_DATE(v_batch_date,'YYYYMMDD')-90, 'YYYYMMDD');

                    DELETE /*+ INDEX(a INX_QUERY_LOG_DAT_3_1) */ FROM HDM_QUERY_LOG_DAT_3 a
                    WHERE REQUEST_DATE < TO_CHAR(TO_DATE(v_batch_date,'YYYYMMDD')-90, 'YYYYMMDD');
                    COMMIT;
           END IF;

           v_step := '2';
           --  清理表HDM_QUERY_LOG_DAT_3_HIS
           IF SUBSTR(v_batch_date,7,2) = '01' AND SUBSTR(v_batch_date,4,5) IN ('06','12') AND v_batch_no = '1' THEN
                    DELETE /*+ INDEX(a INX_QUERY_LOG_DAT_3_HIS_1) */FROM HDM_QUERY_LOG_DAT_3_HIS a
                    WHERE REQUEST_DATE < TO_CHAR(TO_DATE(v_batch_date,'YYYYMMDD')-360, 'YYYYMMDD');
                    COMMIT;
           END IF;

        EXCEPTION
           WHEN OTHERS
           THEN
              v_proc_err_code := SQLCODE;
              v_proc_err_txt := SUBSTR (SQLERRM, 1, 200);
              --任务日志表中记录出错状态
              pckg_hdm_public_util.proc_writelog (
                 v_batch_date,
                 v_proc_name,
                 '0',
                 '0',
                 99,
                    '任务出错! '
                 || '出错步骤号为: '
                 || v_step
                 || ' || 错误码为: '
                 || v_proc_err_code
                 || ' || 错误信息为: '
                 || v_proc_err_txt,
                 1);
              p_o_succeed := '1';
              RAISE;
      END PROC_BACKUP_LOG_DAT;

     /******************************************************************************
     --函数名称： GET_QUERY_WHERE_STRING
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --p_in_str (传入参数，前台拼接串)
     --功能： 模版程序1
     ******************************************************************************/
    FUNCTION GET_QUERY_WHERE_STRING(
        p_in_str IN VARCHAR2, --(传入参数,前台拼接串)
        p_table_name IN VARCHAR2,
        p_date_from IN VARCHAR2,
        p_date_to IN VARCHAR2
    )
    RETURN VARCHAR2
    IS
         TYPE typ_map IS TABLE OF VARCHAR2(300) INDEX BY VARCHAR2(300);
         where_map typ_map;
         v_where_string VARCHAR2(32767);
         v_key  VARCHAR2(300);
         v_val  VARCHAR2(300);
         v_tmp  VARCHAR2(300);
         v_next_key VARCHAR2(200);
         v_pair_count INT;
         v_pair     VARCHAR2(500);
         v_count INT;
    BEGIN
        -- 初始化
        v_where_string := '';

        -- 检查参数
        IF TRIM(p_in_str) IS NULL THEN
            RETURN v_where_string;
        END IF;
        v_pair_count := REGEXP_COUNT(p_in_str, '\|');
        --IF v_pair_count+1 <> REGEXP_COUNT(p_in_str, ':') THEN
        --    RAISE_APPLICATION_ERROR(-20003, '|与:不匹配');
        --END IF;
        -- 解析参数，'|'符号分隔字段，':'符号分隔键值
        FOR i IN 1..(v_pair_count+1)
        LOOP
            v_pair := REGEXP_SUBSTR(TRIM(p_in_str), '[^\|]+', 1, i);
            v_key  := REGEXP_SUBSTR(v_pair, '[^:]+', 1, 1);
            v_val  := REGEXP_SUBSTR(v_pair, '[^:]+', 1, 2);
            IF v_key IS NOT NULL AND v_val IS NOT NULL THEN
                where_map(upper(v_key)) := v_val;
            END IF;
        END LOOP;
dbms_output.put_line('where_map.COUNT='||where_map.COUNT);
        -- 拼接条件
        FOR i IN 1..where_map.COUNT
        LOOP
            IF i = 1 THEN
                v_key := where_map.FIRST;
            ELSE
                v_key := where_map.NEXT(v_key);
            END IF;
            v_val := where_map(v_key);
            v_tmp := '';
            CASE TRUE
                WHEN v_key IN ('FPDETAILTYPE') THEN -- 是否联动上级,1-是
                    IF v_val = '1' THEN
                        v_tmp := 'SUBSTR(NVL($$$.DTLTYPE,'' ''),1,1) <> ''2''';
                    END IF;
                WHEN v_key IN ('RECIPACC') THEN -- 对方账号,如果是代理汇兑业务则匹配代理汇兑对方账号，否则匹配网外账号
                    v_tmp := '(($$$.RECIPACC = #RECIPACC#) OR ($$$.DRCRF = ''2'' AND SUBSTR($$$.NEAGNFLG,1,7) = ''AGENTEX'' AND $$$.OTACTNO = #RECIPACC#))';
                    v_tmp := REPLACE(v_tmp, '#RECIPACC#', ''''||v_val||'''');
                WHEN v_key IN ('RECORDTYPE') THEN -- 记账类型,NECHDTLD.BAKCHAR1第36位
                    IF v_val = '1' THEN
                        v_tmp := 'SUBSTR($$$.BAKCHAR1,36,1) = ''1''';
                    ELSIF v_val = '0' THEN
                        v_tmp := '(TRIM(SUBSTR($$$.BAKCHAR1,36,1)) IS NULL OR SUBSTR($$$.BAKCHAR1,36,1) = ''0'' OR SUBSTR($$$.BAKCHAR1,36,1) = ''2'')';
                    END IF;
                WHEN v_key IN ('ISDEFSUBACC') THEN -- 是否默认子账号,0-默认子账户 1-非默认子账户
                    IF v_val = '1' OR (v_val = '0' AND where_map('HISFLAG') = '0') THEN
                        v_tmp := '(TRIM(SUBSTR($$$.BAKCHAR,1,26)) IS NOT NULL AND SUBSTR($$$.BAKCHAR,1,26)<>''00000000000000000000000000'')';
                    END IF;
                WHEN v_key = 'MINAMOUNT' THEN -- 最小金额
                    v_tmp :=  '$$$.AMOUNT >= '||v_val;
                WHEN v_key = 'MAXAMOUNT' THEN -- 最大金额
                    v_tmp :=  '$$$.AMOUNT <= '||v_val;
                WHEN v_key = 'BEGTIME' THEN
                    v_tmp :=  '$$$.BUSIDATE||$$$.BUSITIME >= '''||p_date_from||v_val||'''';
                WHEN v_key = 'ENDTIME' THEN
                    v_tmp :=  '$$$.BUSIDATE||$$$.BUSITIME <= '''||p_date_to||v_val||'''';
                ELSE --默认处理
                    IF v_key IS NOT NULL THEN
                        SELECT COUNT(*)
                            INTO v_count
                        FROM USER_TAB_COLUMNS
                        WHERE TABLE_NAME in ('HDM_S_NFQCHDTL_QUERY_DAT')
                          AND COLUMN_NAME=v_key;
                        IF v_count > 0 THEN
                            v_tmp := '$$$.'||v_key||' = '''||v_val||'''';
                        END IF;
                    END IF;
            END CASE;

            IF v_tmp IS NOT NULL THEN
                v_where_string := v_where_string||CHR(13)||CHR(10)||' AND ';
                v_where_string := v_where_string||v_tmp;
            END IF;
        END LOOP;
        v_where_string := REPLACE(v_where_string, '$$$', p_table_name);
        RETURN v_where_string;
    EXCEPTION
        WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20002, 'where解析错误,'||SQLERRM||','||p_in_str);
    END GET_QUERY_WHERE_STRING;

    /******************************************************************************
     --函数名称： GET_QUERY_ORDER_STRING
     --作者： kwg
     --时间： 2017年05月23日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：
     --p_in_str (传入参数，前台拼接串)
     --功能： 模版程序1
     ******************************************************************************/
    FUNCTION GET_QUERY_ORDER_STRING(
        p_in_str IN VARCHAR2,    --(传入参数,前台拼接串)
        p_table_name IN VARCHAR2
    )
    RETURN VARCHAR2

    IS
      TYPE typ_map IS TABLE OF VARCHAR2(100) INDEX BY BINARY_INTEGER;
     order_map typ_map;
     v_key  VARCHAR2(30);
     v_val  VARCHAR2(100);
     v_next_key VARCHAR2(30);
     v_pair_count INT;
     v_pair     VARCHAR2(500);
    v_order_string VARCHAR2(32767);
    BEGIN
        -- 初始化
        v_order_string := '';

        -- 检查参数
        IF p_in_str IS NULL THEN
            RETURN v_order_string;
        END IF;
        v_pair_count := REGEXP_COUNT(p_in_str, '\|');
        --IF v_pair_count+1 <> REGEXP_COUNT(p_in_str, ':') THEN
        --    RAISE_APPLICATION_ERROR(-20003, '|与:不匹配');
        --END IF;

        -- 解析参数，'|'符号分隔字段，':'符号分隔键值
        FOR i IN 1..(v_pair_count+1)
        LOOP
            v_pair := REGEXP_SUBSTR(p_in_str, '[^\|]+', 1, i);
            v_key  := REGEXP_SUBSTR(v_pair, '[^:]+', 1, 1);
            v_val  := REGEXP_SUBSTR(v_pair, '[^:]+', 1, 2);
            --IF v_key IS NULL OR v_val IS NULL THEN
            --    RAISE_APPLICATION_ERROR(-20005, '<'||v_pair||'>键值错误');
            --END IF;

            CASE v_val
                WHEN '0' THEN
                    order_map(i) :=  p_table_name||'.'||v_key||' ASC';
                WHEN '1' THEN
                    order_map(i) :=  p_table_name||'.'||v_key||' DESC';
                ELSE --默认处理
                    NULL;
                   -- RAISE_APPLICATION_ERROR(-20005, '<'||v_pair||'>排序标识错误');
            END CASE;
        END LOOP;

        -- 拼接条件
        FOR i IN 1..order_map.COUNT
        LOOP
            v_key := i;
             IF i = 1 THEN
                v_order_string := 'ORDER BY ';
                v_order_string := v_order_string||order_map(v_key);
            ELSE
                v_order_string := v_order_string||',';
                v_order_string := v_order_string||order_map(v_key);
            END IF;
        END LOOP;
    RETURN v_order_string;
    EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20004, 'order解析错误,'||SQLERRM||','||p_in_str);
    END GET_QUERY_ORDER_STRING;
END;
//
delimiter ;//

