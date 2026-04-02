create table sales_part_test
(
  prod_id       NUMBER not null,
  cust_id       NUMBER not null,
  time_id       DATE not null,
  channel_id    NUMBER not null,
  promo_id      NUMBER not null,
  quantity_sold NUMBER(10,2) not null,
  amount_sold   NUMBER(10,2) not null
)
partition by range(time_id) subpartition  by range  (time_id)
(
 partition sales_part_1998 values less than (TO_DATE('1999-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))

 (
   subpartition sales_part_1998_01 values less than ( TO_DATE('1998-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))  ,
   subpartition sales_part_1998_02 values less than ( TO_DATE('1998-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
 )
 ,
 partition sales_part_1999 values less than (TO_DATE('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))

 (
   subpartition sales_part_1999_01 values less than ( TO_DATE('1999-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))  ,
   subpartition sales_part_1999_02 values less than ( TO_DATE('1999-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
 )
 );
 CREATE INDEX sales_part_test ON sales_part_test (prod_id ASC);


create index global_part_index on sales_part_test(cust_id,prod_id) global


partition by range (cust_id)

(

  partition index_part1 values less than (60),

  partition index_part2 values less than (80),

  partition index_partmax values less than (maxvalue)

);
create table CODE_PRM
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
create table HDM_VIEW_LIST (view_name varchar2(20), table_name varchar2(20), file_name varchar2(20), data_year varchar2(20));
create table DATADATE_DETAIL
(
  table_name      VARCHAR2(30) not null,
  last_date_month VARCHAR2(6)
);

create table LOG_PERFORMANCE
(
  req_id       VARCHAR2(20),
  phase_id     VARCHAR2(2),
  phase_begin  TIMESTAMP(9),
  phase_end    TIMESTAMP(9),
  phase_result VARCHAR2(1),
  trace_no     VARCHAR2(10),
  field1       VARCHAR2(20)
);

CREATE TABLE "HDM_QUERY_LOG_DAT" (
	"QRY_ID" VARCHAR2(20 BYTE) NOT NULL,
	"QRY_DATE_FROM" VARCHAR2(10 BYTE) NOT NULL,
	"QRY_DATE_TO" VARCHAR2(10 BYTE) NOT NULL,
	"QRY_CHANNEL" VARCHAR2(2 BYTE),
	"QRY_DETAIL_KIND" VARCHAR2(3 BYTE),
	"QRY_FEE_FLAG" VARCHAR2(1 BYTE),
	"QRY_REQ_KIND" VARCHAR2(1 BYTE) NOT NULL,
	"QRY_TABLE_NAME" VARCHAR2(30 BYTE) NOT NULL,
	"QRY_AGAIN_FLAG" VARCHAR2(1 BYTE),
	"QRY_TIME" VARCHAR2(8 BYTE),
	"QRY_STATUS" VARCHAR2(1 BYTE),
	"QRY_TIMESTMP_FROM" TIMESTAMP(6),
	"QRY_TIMESTMP_TO" TIMESTAMP(6),
	"QRY_COUNT" VARCHAR2(8 BYTE),
	"QRY_OFFSET" VARCHAR2(8 BYTE),
	"CARDACC" VARCHAR2(1 BYTE),
	"NO" VARCHAR2(20 BYTE),
	"ACSERNO" VARCHAR2(5 BYTE),
	"CURRTYPE" VARCHAR2(3 BYTE),
	"QUERYKIND" VARCHAR2(2 BYTE),
	"CINO" VARCHAR2(15 BYTE),
	"CUSTAREACODE" VARCHAR2(5 BYTE),
	"CCID" VARCHAR2(20 BYTE),
	"INCARDNO" VARCHAR2(20 BYTE),
	"TRANSCHANNEL" VARCHAR2(1 BYTE),
	"FEEACCNO" VARCHAR2(28 BYTE),
	"FEENO" VARCHAR2(80 BYTE),
	"ZONENO" VARCHAR2(5 BYTE),
	"BRNO" VARCHAR2(5 BYTE),
	"EMAIL" VARCHAR2(200 BYTE),
	"MOBNO" VARCHAR2(65 BYTE),
	"ATTACHMENT_COUNT" NUMBER(30,0),
	"SERIALNO" VARCHAR2(20 BYTE),
	"INQFLAG" VARCHAR2(1 BYTE),
	"FUNDID" VARCHAR2(12 BYTE),
	"APPLNUM" VARCHAR2(24 BYTE),
	"TRXFLAG" VARCHAR2(1 BYTE),
	"TRXKND" VARCHAR2(5 BYTE),
	"BOOKSQNO" VARCHAR2(24 BYTE),
	"REVTRANF" VARCHAR2(1 BYTE),
	"TRADETYP" VARCHAR2(1 BYTE),
	"PRODID" VARCHAR2(20 BYTE),
	"PRTOID" VARCHAR2(34 BYTE),
	"OPZONENO" VARCHAR2(5 BYTE),
	"CURRTYPE1" VARCHAR2(3 BYTE),
	"FUNDKIND" VARCHAR2(1 BYTE),
	"CASHEXF" VARCHAR2(3 BYTE)

);

CREATE TABLE "HDM"."HDM_QUERY_DATE_SPLIT" (
	"QUERY_ID" VARCHAR2(20 BYTE) NOT NULL,
	"UP_TIME_START" VARCHAR2(10 BYTE),
	"UP_TIME_END" VARCHAR2(10 BYTE),
	"OUR_TIME_START" VARCHAR2(10 BYTE),
	"OUR_TIME_END" VARCHAR2(10 BYTE),
	"UPDATE_DATE" VARCHAR2(10 BYTE),
	"C_FLAG" VARCHAR2(1 BYTE),
	"V_P_I_NO" VARCHAR2(2 BYTE)
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

create table NTHCABRP_S_DAT
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

create table NTHPAZON_S_DAT
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
create table QUERY_DATE_SPLIT
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

create table QUERY_LOG_DAT
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

create table S_BFHCRDBF_QUERY_DAT
(
  zoneno     VARCHAR2(5),
  accno      NUMBER(17),
  currtype   VARCHAR2(3),
  mdcardno   VARCHAR2(19),
  busidate   VARCHAR2(10),
  busitime   VARCHAR2(8),
  trxserno   VARCHAR2(23),
  trxcode    VARCHAR2(5),
  dbcrf      VARCHAR2(1),
  listflag   VARCHAR2(1),
  mdkmode    VARCHAR2(3),
  balance    NUMBER(18),
  lstbal     NUMBER(18),
  trxcurr    VARCHAR2(3),
  trxamt     NUMBER(18),
  trxdate    VARCHAR2(10),
  brno       VARCHAR2(5),
  tellerno   VARCHAR2(5),
  authtlno   VARCHAR2(5),
  servface   VARCHAR2(3),
  merno      NUMBER(13),
  terno      NUMBER(11),
  trxsite    VARCHAR2(40),
  tipamt     NUMBER(14),
  cfeeamt    NUMBER(14),
  merrate    VARCHAR2(6),
  prirecf    VARCHAR2(1),
  recsqno    VARCHAR2(1),
  asintegr   NUMBER(12),
  spintegr   NUMBER(12),
  allyinte   NUMBER(12),
  refamt     NUMBER(18),
  timestmp   VARCHAR2(26),
  trxdiscp   VARCHAR2(20),
  mcccode    VARCHAR2(5),
  trxcountry VARCHAR2(3),
  trxcurdicp VARCHAR2(3),
  authno     VARCHAR2(6),
  otrxserno  VARCHAR2(23),
  bknotes    VARCHAR2(60),
  bkamt1     NUMBER(18),
  bkamt2     NUMBER(18),
  zoneaccno  VARCHAR2(4),
  accfix     VARCHAR2(3),
  bkamt3     VARCHAR2(18),
  bkamt4     VARCHAR2(18),
  bkamt5     VARCHAR2(18),
  bknotes2   VARCHAR2(60),
  prodid     VARCHAR2(13),
  eserialno  VARCHAR2(27),
  esubseqno  VARCHAR2(3),
  mserialn   VARCHAR2(27),
  oserialn   VARCHAR2(27),
  chantype   VARCHAR2(3),
  channo     VARCHAR2(5),
  recipact   VARCHAR2(34),
  recipcty   VARCHAR2(3),
  rsavname   VARCHAR2(60),
  rprodid    VARCHAR2(13),
  rbrno      VARCHAR2(5),
  orderno    VARCHAR2(32),
  atexcamt   VARCHAR2(19),
  progmsg    VARCHAR2(10),
  emvwrmod   VARCHAR2(1),
  merchno    VARCHAR2(20),
  oflflag    VARCHAR2(1),
  openzone   VARCHAR2(5)
);

create table S_BFHSCDBF_QUERY_DAT
(
  zoneno    VARCHAR2(5),
  accno     NUMBER(17),
  currtype  VARCHAR2(3),
  mdcardno  VARCHAR2(19),
  busidate  VARCHAR2(10),
  busitime  VARCHAR2(8),
  trxserno  VARCHAR2(23),
  trxcode   VARCHAR2(5),
  dbcrf     VARCHAR2(1),
  listflag  VARCHAR2(1),
  mdkmode   VARCHAR2(3),
  balance   NUMBER(18),
  lstbal    NUMBER(18),
  trxcurr   VARCHAR2(3),
  trxamt    NUMBER(18),
  trxdate   VARCHAR2(10),
  brno      VARCHAR2(5),
  tellerno  VARCHAR2(5),
  authtlno  VARCHAR2(5),
  servface  VARCHAR2(3),
  merno     NUMBER(13),
  terno     NUMBER(11),
  trxsite   VARCHAR2(40),
  tipamt    NUMBER(14),
  cfeerat   NUMBER(14),
  merrate   VARCHAR2(6),
  prirecf   VARCHAR2(1),
  recsqno   VARCHAR2(1),
  asintegr  NUMBER(12),
  spintegr  NUMBER(12),
  allyinte  NUMBER(12),
  refamt    NUMBER(18),
  timestmp  VARCHAR2(26),
  trxdiscp  VARCHAR2(20),
  mcccode   VARCHAR2(5),
  bknotes   VARCHAR2(60),
  bkamt1    NUMBER(18),
  bkamt2    NUMBER(18),
  zoneaccno VARCHAR2(4)
);

create table S_PFEPABDL_QUERY_DAT
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

create table S_PFEPOTDL_QUERY_DAT
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

create table S_PFEPSADL_QUERY_DAT
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

create table S_PFEPSADL_QUERY_TEMP
(
  seq      VARCHAR2(20),
  busidate VARCHAR2(20),
  summflag VARCHAR2(20),
  currtype VARCHAR2(20),
  cashexf  VARCHAR2(20),
  drcrf    VARCHAR2(20),
  amount   VARCHAR2(20),
  balance  VARCHAR2(20),
  servface VARCHAR2(20),
  tellerno VARCHAR2(20),
  brno     VARCHAR2(20),
  zoneno   VARCHAR2(20),
  cashnote VARCHAR2(20),
  trxcode  VARCHAR2(20),
  workdate VARCHAR2(20),
  authtlno VARCHAR2(20),
  recipact VARCHAR2(20),
  phybrno  VARCHAR2(20),
  termid   VARCHAR2(20),
  busitime VARCHAR2(20),
  trxcite  VARCHAR2(20),
  accno    VARCHAR2(20),
  serialno VARCHAR2(20)
);

create table S_PFEPVMDL_QUERY_DAT
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

create table VIEW_LIST
(
  view_name  VARCHAR2(20),
  table_name VARCHAR2(20),
  file_name  VARCHAR2(20),
  data_year  VARCHAR2(20)
);
create table HDS_PRAS_YEAR_TABS
(
  table_name VARCHAR2(50),
  del_year   VARCHAR2(10),
  add_year   VARCHAR2(10),
  view_name  VARCHAR2(50),
  dmp_date   VARCHAR2(10)
);

delimiter //;
CREATE OR REPLACE PACKAGE date_splite
AS
   PROCEDURE date_splite (
      p_i_date      IN       VARCHAR2,
      p_i_no        IN       VARCHAR2,
      p_o_succeed   OUT      VARCHAR2
   );
END date_splite;
//


CREATE OR REPLACE PACKAGE list_export
AS

   PROCEDURE list_export (
      p_i_date      IN       VARCHAR2,
      p_i_no        IN       VARCHAR2,
      p_o_succeed   OUT      VARCHAR2
   );
END list_export;
//


CREATE OR REPLACE PACKAGE hdm_query_file_export
AS

   PROCEDURE hdm_query_file_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );


   PROCEDURE hdm_query_file_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );


   PROCEDURE hdm_query_file_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_query_file_export;
//



create or replace package long_help
is

procedure bind_variable( p_name in varchar2, p_value in varchar2);
function substr_of
( p_query in varchar2,
  p_from  in number,
  p_for   in number,
  p_name1 in varchar2  default null,
  p_bind1 in varchar2  default null,
  p_name2 in varchar2  default null,
  p_bind2 in varchar2  default null,
  p_name3 in varchar2  default null,
  p_bind3 in varchar2  default null,
  p_name4 in varchar2  default null,
  p_bind4 in varchar2  default null )
return varchar2;
end ;
//



CREATE OR REPLACE PACKAGE PCKG_DQ001
IS
   TYPE c_dq001_list1 IS REF CURSOR;

   PROCEDURE proc_dq001_1 (
                           i_taskid        IN     VARCHAR2,--查询流水号
                           i_accno         IN     VARCHAR2,--账号
                           i_qry_date_from IN     VARCHAR2,--查询起始日期
                           i_qry_date_to   IN     VARCHAR2,--查询结束日期
                           i_qry_channel   IN     VARCHAR2,--渠道标志 0-1-网银 2-电话银行 3-柜面 4-内管
                           i_currtype      IN     VARCHAR2,--币种
                           i_reqtype       IN     VARCHAR2,--请求类型
                           i_qryoffset     IN     VARCHAR2,--查询起始条数
                           i_acserno       IN     VARCHAR2,--查询明细下挂账户序号
                           i_querykind     IN     VARCHAR2,--帐务明细种类
                           i_cashexf       IN     VARCHAR2,--钞汇标志
                           i_enableLogQueryTime     IN     VARCHAR2,--查询性能日志开关，0：off，1：on
                           i_enableLogStatTime      IN     VARCHAR2,--统计性能日志开关，0：off，1：on
                           i_enableLogAsyncQueTime  IN     VARCHAR2,--异步性能日志开关，0：off，1：on
                           i_drcrf         IN     VARCHAR2,--201505 借贷标识 0-全部 1-借 2-贷
                           o_successflag   OUT    VARCHAR2,--成功失败标识
                           o_errordesc     OUT    VARCHAR2,--错误描述
                           o_resultset     OUT    c_dq001_list1,--结果集
                           o_qry_count     OUT    INTEGER,      --查询记录数
                           o_stat_count    OUT    INTEGER      --统计记录数
  );

  --DQ001第二个接口
  TYPE c_dq001_list2 IS REF CURSOR;

   /************************************************
  --存储过程名称：proc_dq001_2
  --作者：贾雪松
  --时间：2011年01月28
  --版本号:1.0
  --使用源表名称：
  --使用目标表名称：
  --参数说明：
  --功能：查询DQ001接口中帐务查询类型为2,3,4部分
  ************************************************/
   PROCEDURE proc_dq001_2 (
                           i_taskid        IN     VARCHAR2,--查询流水号
                           i_accno         IN     VARCHAR2,--账号
                           i_qry_date_from IN     VARCHAR2,--查询起始日期
                           i_qry_date_to   IN     VARCHAR2,--查询结束日期
                           i_qry_channel   IN     VARCHAR2,--渠道标志 0-1-网银 2-电话银行 3-柜面 4-内管
                           i_currtype      IN     VARCHAR2,--查询开始时间
                           i_reqtype       IN     VARCHAR2,--请求类型
                           i_qryoffset     IN     VARCHAR2,--查询起始条数
                           i_querykind     IN     VARCHAR2,--帐务明细种类
                           i_enableLogQueryTime     IN     VARCHAR2,--查询性能日志开关
                           i_enableLogStatTime      IN     VARCHAR2,--统计性能日志开关
                           i_enableLogAsyncQueTime  IN     VARCHAR2,--异步性能日志开关
                           i_drcrf         IN     VARCHAR2,--201505 借贷标识 0-全部 1-借 2-贷
                           o_successflag   OUT    VARCHAR2,--成功失败标识
                           o_errordesc     OUT    VARCHAR2,--错误描述
                           o_resultset     OUT    c_dq001_list2,--结果集
                           o_qry_count     OUT    INTEGER,      --查询记录数
               o_stat_count    OUT    INTEGER      --统计记录数
  );
END PCKG_DQ001;
//



CREATE OR REPLACE PACKAGE PCKG_GEN_DUMP_LIST
IS
    /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能： 从表HDS_PRAS_YEAR_TABS取得表名和dump包导出的日期，dump包导出清单。
   --      当前批量导出上一年同月的数据，子分区由日期的天进行推算。数据年表64个一级地区分区，12个二级月分区。
   --      64个地区一个月内导出，一天导出3个地区。
   --      每月1日需要重建索引，1日不导出，20日是计息日文件加载，也不导出。
     ******************************************************************************/
    PROCEDURE PROC_GEN_DUMP_LIST(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

END PCKG_GEN_DUMP_LIST;
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
CREATE OR REPLACE PACKAGE PCKG_GEN_STARTING_SCRIPT
IS
    /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成改表名脚本。
   --
     ******************************************************************************/
    PROCEDURE PROC_GEN_RENAME_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表VIEW_LIST取得视图名和包含的表名，生成建视图脚本。
   --
     ******************************************************************************/
  PROCEDURE PROC_GEN_VIEW_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，建表脚本。
   --
     ******************************************************************************/
  PROCEDURE PROC_GEN_TABLE_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


END PCKG_GEN_STARTING_SCRIPT;
//



CREATE OR REPLACE PACKAGE PCKG_GEN_YEAR_SUBPARTITIONS
IS
    /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能： 从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成修改分区脚本。
   --       对表进行增加新的年份的二级子分区，并建新增的子分区的索引
     ******************************************************************************/
    PROCEDURE PROC_SUBPARTITIONS_ADD(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能： 从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成修改分区脚本。
   --       对表进行删除旧年份的所有二级子分区，增加新的年份的二级子分区，并建新增的子分区的索引
     ******************************************************************************/
    PROCEDURE PROC_SUBPARTITIONS_DROP(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年07月16日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成视图脚本程序
   --     从表VIEW_LIST取得视图名和包含的表名，生成建视图脚本。
   --
     ******************************************************************************/
PROCEDURE PROC_GEN_VIEW_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数)
    con_dat_max_date IN VARCHAR2, --(传入参数)
    con_month_min_date IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 );
END PCKG_GEN_YEAR_SUBPARTITIONS;
//


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


CREATE OR REPLACE PACKAGE BODY date_splite
IS
   PROCEDURE date_splite (
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
                        := 'date_splite.date_splite' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10);
      v_max_zoneno      VARCHAR2 (10);
      v_status          log_task_detail.n_status%TYPE;
  --上主机查的情况
  CURSOR cur_pfepsadl_date_split IS
  SELECT a.qry_id,a.qry_table_name,b.up_time_start,b.up_time_end,b.our_time_start,b.our_time_end
  FROM query_log_dat a,query_date_split b
  WHERE a.qry_req_kind = '3'            /*请求类型是异步查询*/
  AND a.qry_status = '7'        /*7-本次批量需要查询的数据*/
  AND a.qry_id=b.query_id
  AND b.update_date=p_i_date
  AND b.up_time_start IS NOT NULL
  AND b.up_time_end IS NOT NULL
  AND UPPER (a.qry_table_name) IN ('S_PFEPOTDL_QUERY'
                   ,'S_PFEPVMDL_QUERY'
                   ,'S_PFEPSADL_QUERY'
                   ,'S_PFQABDTL_QUERY');
   BEGIN
      --任务日志表中记录开始运行状态
      v_status :=
         pckg_public_util.func_get_log_status (p_i_date,
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
      pckg_public_util.proc_writelog (p_i_date,
                                              v_proc_name,
                                              v_min_zoneno,
                                              v_max_zoneno,
                                              0,
                                              '任务已开始',
                                              1
                                             );
      v_step := '1';

      DELETE FROM query_date_split
            WHERE query_id IN (SELECT qry_id
                                 FROM query_log_dat
                                WHERE qry_status = '7');

      /*0：待完成,只删除本批次的数据，因为C_FLAG数据只有到晚批结束才会被更新*/
      v_step := '2';

      INSERT INTO query_date_split
                  (query_id, up_time_start, up_time_end, our_time_start,
                   our_time_end, update_date, c_flag, v_p_i_no)
         SELECT qry_id,
                CASE
           WHEN qry_date_from_month > last_date_month_yz THEN qry_date_from
                   WHEN qry_date_from_month <= last_date_month_yz AND qry_date_to_month>last_date_month_yz
                        THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(last_date_month,'yyyymm'),1),'yyyymm'),0,4)
                             ||'-'||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(last_date_month,'yyyymm'),1),'yyyymm'),5,2)
                             ||'-'||'01'
                   /*如果起始日期小于等于表当前最大日期，且结束日期和起始日期不在一个月份，那么就是下月1号为起始日期*/
                   WHEN qry_date_to_month <= last_date_month_yz THEN NULL
                END up_time_start,
                CASE
                   WHEN  qry_date_to_month > last_date_month_yz THEN TO_CHAR(qry_date_to_day,'yyyy-mm-dd')
                   WHEN  qry_date_to_month <= last_date_month_yz THEN NULL
                END up_time_end,
                CASE
                   WHEN  qry_date_from_month>last_date_month_yz THEN NULL
                   WHEN  qry_date_from_month<=last_date_month_yz THEN TO_CHAR(qry_date_from_day,'yyyy-mm-dd')
                END our_time_start,
                CASE
                   WHEN  qry_date_from_month>last_date_month_yz THEN NULL
                   WHEN  qry_date_from_month<=last_date_month_yz AND qry_date_to_month>last_date_month_yz
                      THEN SUBSTR(TO_CHAR(TO_DATE(TO_CHAR(ADD_MONTHS(TO_DATE(last_date_month,'yyyymm'),1),'yyyymm')||'01','yyyymmdd')-1,'yyyymmdd'),0,4)
                           || '-'
                           ||SUBSTR(TO_CHAR(TO_DATE(TO_CHAR(ADD_MONTHS(TO_DATE(last_date_month,'yyyymm'),1),'yyyymm')||'01','yyyymmdd')-1,'yyyymmdd'),5,2)
                           || '-'
                           ||SUBSTR(TO_CHAR(TO_DATE(TO_CHAR(ADD_MONTHS(TO_DATE(last_date_month,'yyyymm'),1),'yyyymm')||'01','yyyymmdd')-1,'yyyymmdd'),7,2)
                   /*如果起始日期小于等于表当前最大日期，且结束日期和起始日期不在一个月份，那么就是当前最新月份的最后一天*/
                   WHEN  qry_date_to_month <= last_date_month_yz THEN qry_date_to
                END our_time_end,
                p_i_date update_date, '0' c_flag, p_i_no
           FROM (SELECT qry_id, qry_date_from,               /*10位start日期*/
                        TO_DATE
                           (REPLACE (SUBSTR (qry_date_from, 0, 10), '-', NULL),
                            'yyyymmdd'
                           ) qry_date_from_day,               /*8位start日期*/
                        TO_DATE
                           (REPLACE (SUBSTR (qry_date_from, 0, 7), '-', NULL),
                            'yyyymm'
                           ) qry_date_from_month,             /*6位start年月*/
                        qry_date_to,                           /*10位end日期*/
                        TO_DATE
                             (REPLACE (SUBSTR (qry_date_to, 0, 10), '-', NULL),
                              'yyyymmdd'
                             ) qry_date_to_day,                 /*8位end日期*/
                        TO_DATE
                           (REPLACE (SUBSTR (qry_date_to, 0, 7), '-', NULL),
                            'yyyymm'
                           ) qry_date_to_month,                 /*6位end年月*/
                        last_date_month,                       /*6位最新年月*/
                        TO_DATE (last_date_month,
                                 'yyyymm') last_date_month_yz,
                        a.qry_table_name
                   FROM query_log_dat a, datadate_detail b
                  WHERE a.qry_req_kind = '3'            /*请求类型是异步查询*/
                    AND a.qry_status = '7'        /*7-本次批量需要查询的数据*/
                    AND UPPER (a.qry_table_name) = UPPER (b.table_name)
                    AND (substr(qry_date_from,1,4)>='1900' and substr(qry_date_from,1,4)<='2099')
                    and (substr(qry_date_to,1,4)>='1900' and substr(qry_date_to,1,4)<='2099')
                    and (substr(qry_date_from,5,1)='-' and substr(qry_date_from,8,1)='-')
                    and (substr(qry_date_to,5,1)='-' and substr(qry_date_to,8,1)='-')
                    and (substr(qry_date_from,6,2)>='01' and substr(qry_date_from,6,2)<='12')
                    and (substr(qry_date_to,6,2)>='01' and substr(qry_date_to,6,2)<='12')
                    and (substr(qry_date_from,9,2)>='01' and substr(qry_date_from,9,2)<='31')
                    and (substr(qry_date_to,9,2)>='01' and substr(qry_date_to,9,2)<='31')
               );

         --4+7个文件不再上主机查，因加载T-1日数据
          -- ,'S_PFEPCBDL_QUERY'
        -- ,'S_PFEPFODL_QUERY'
        -- ,'S_PFEPFXDL_QUERY'
        -- ,'S_PFEPGKDL_QUERY'
        -- ,'S_PFEPNFDL_QUERY'
        -- ,'S_PFEPSPDL_QUERY'
        -- ,'S_PFEPTZDL_QUERY'
        FOR i IN cur_pfepsadl_date_split
        LOOP
             --全部上主机查询，我库查询起始时间为空的情况
             IF i.our_time_start IS NULL AND i.our_time_end IS NULL
             THEN
               UPDATE query_date_split
               SET our_time_start=up_time_start,our_time_end=up_time_end
               WHERE query_id=i.qry_id;
             END IF;

             --一部分上主机查询一部分我库查询，起始时间不为空且结束时间为空
             IF i.our_time_start IS NOT NULL AND i.our_time_end IS NOT NULL
             THEN
               UPDATE query_date_split
               SET our_time_end=up_time_end
               WHERE query_id=i.qry_id;
             END IF;

             UPDATE query_date_split
             SET up_time_end=NULL,up_time_start=NULL
             WHERE query_id=i.qry_id;

        END LOOP;


      p_o_succeed := '0';
      COMMIT;
      --任务日志表中记录成功完成状态
      pckg_public_util.proc_writelog (p_i_date,
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
         pckg_public_util.proc_writelog (p_i_date,
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
   END date_splite;
END date_splite;
//
create or replace package body long_help
as
        g_cursor number := dbms_sql.open_cursor;
        g_query  varchar2(32765);
procedure bind_variable( p_name in varchar2, p_value in varchar2)
is
begin
    if ( p_name is not null )
    then
       dbms_sql.bind_variable( g_cursor, p_name, p_value );
    end if;
end;

function substr_of
( p_query in varchar2,
  p_from  in number,
  p_for   in number,
  p_name1 in varchar2  default null,
  p_bind1 in varchar2  default null,
  p_name2 in varchar2  default null,
  p_bind2 in varchar2  default null,
  p_name3 in varchar2  default null,
  p_bind3 in varchar2  default null,
  p_name4 in varchar2  default null,
  p_bind4 in varchar2  default null )
return varchar2
as
  l_buffer   varchar2(4000);
  l_buffer_len number;
begin
  if ( p_query <> g_query or g_query is null )
  then
    dbms_sql.parse( g_cursor, p_query, dbms_sql.native );
    g_query := p_query;
  end if;

  bind_variable( p_name1, p_bind1 );
  bind_variable( p_name2, p_bind2 );
  bind_variable( p_name3, p_bind3 );
  bind_variable( p_name4, p_bind4 );
 /*
  dbms_sql.define_column_long(g_cursor, 1);

  if (DBMS_SQL.EXECUTE_AND_FETCH(g_cursor)>0)
  then
      dbms_sql.column_value_long
      (g_cursor, 1, p_for, p_from-1,
       l_buffer, l_buffer_len );
  end if;
  */
  return l_buffer;
end substr_of;
end;
//


CREATE OR REPLACE PACKAGE BODY PCKG_DQ001
IS
   /************************************************
  --存储过程名称：proc_dq001_1
  --作者：贾雪松
  --时间：2011年01月28.
  --版本号:1.0
  --使用源表名称：
  --使用目标表名称：
  --参数说明：
  --功能：查询DQ001接口中帐务查询类型为0，1部分
  ************************************************/
   PROCEDURE proc_dq001_1 (
                           i_taskid        IN     VARCHAR2,--查询流水号
                           i_accno         IN     VARCHAR2,--账号
                           i_qry_date_from IN     VARCHAR2,--查询起始日期
                           i_qry_date_to   IN     VARCHAR2,--查询结束日期
                           i_qry_channel   IN     VARCHAR2,--渠道标志 1-网银；2-WAP；3-电银；4-柜面
                           i_currtype      IN     VARCHAR2,--查询开始时间
                           i_reqtype       IN     VARCHAR2,--请求类型
                           i_qryoffset     IN     VARCHAR2,--查询起始条数
                           i_acserno       IN     VARCHAR2,--查询明细下挂账户序号
                           i_querykind     IN     VARCHAR2,--帐务明细种类
                           i_cashexf       IN     VARCHAR2,--钞汇标志
                           i_enableLogQueryTime     IN     VARCHAR2,--查询性能日志开关
                           i_enableLogStatTime      IN     VARCHAR2,--统计性能日志开关
                           i_enableLogAsyncQueTime  IN     VARCHAR2,--异步性能日志开关
                           i_drcrf         IN     VARCHAR2,--201505 借贷标识 0-全部 1-借 2-贷
                           o_successflag   OUT    VARCHAR2,--成功失败标识
                           o_errordesc     OUT    VARCHAR2,--错误描述
                           o_resultset     OUT    c_dq001_list1,--结果集
                           o_qry_count     OUT    INTEGER,      --查询记录数
                           o_stat_count    OUT    INTEGER      --统计记录数
   ) IS
    v_subcode VARCHAR2(4);
    v_qryoffset_end integer;
    v_shownum integer;
    v_perf_log_begin TIMESTAMP(9);
    v_year VARCHAR2(4) := null;
   BEGIN
      o_successflag := '0';
      o_qry_count := 0;
      o_stat_count := 0;
      v_subcode := substr(i_accno,1,4);
      if(i_reqtype = '0') then
        v_shownum := 20;
      END IF;
      if(i_reqtype = '1') then
        v_shownum := 299;
      END IF;
      v_qryoffset_end := i_qryoffset+v_shownum;
      v_perf_log_begin := SYStimestamp;
      v_year := substr(i_qry_date_from,1,4);

      BEGIN
         ---------------------执行查询-----------------
         --如果是普通查询或下载
         IF(i_reqtype = '0' or i_reqtype = '1') THEN
             IF(i_querykind = '0') THEN
                ------QUERYKIND = 0 and CARDACC = 0  用卡号:理财金E时代卡
                ------MDCARDNO, ACSERNO, CURRTYPE, BUSIDATE, ZONEACCNO
                if(i_qry_channel = '1') then
                   --如果是网银渠道，不考虑币种，查询全部
                   OPEN o_resultset FOR
                    SELECT *
                     FROM (SELECT ROWNUM SEQ,A.*
                    FROM (
                      SELECT BUSIDATE,
                             nvl(SUMMFLAG_DICT.PRMNAME,DETAIL_DATA.SUMMFLAG) SUMMFLAG,
                             CURRTYPE,
                             nvl(CASHEXF_DICT.PRMNAME,DETAIL_DATA.CASHEXF) CASHEXF,
                             nvl(DRCRF_DICT.PRMNAME,DETAIL_DATA.DRCRF) DRCRF,
                             AMOUNT,
                             BALANCE,
                             nvl(SERVFACE_DICT.PRMNAME,DETAIL_DATA.SERVFACE) SERVFACE,
                             TELLERNO,
                             nvl(BRNO_DICT.Notes,DETAIL_DATA.BRNO) BRNO,
                             nvl(ZONENO_DICT.Notes,DETAIL_DATA.ZONENO) ZONENO,
                             CASHNOTE,
                             nvl(TRXCODE_DICT.PRMNAME,DETAIL_DATA.TRXCODE) TRXCODE,
                             WORKDATE,
                             AUTHTLNO,
                             RECIPACT,
                             PHYBRNO,
                             TERMID,
                             BUSITIME,
                             TRXCITE,
                               ' ' TRANCURR,
                               ' ' TRANAMOUNT,
                               ' ' NATIONID
                          FROM (SELECT  /*+index(  T1 INX_PFEPOTDL_QUERY_DAT_1 T1 INX_PFEPOTDL_QUERY_DAT_2)*/
                                       BUSIDATE,SUMMFLAG,CURRTYPE,CASHEXF,DRCRF,AMOUNT,BALANCE,SERVFACE,
                                       TELLERNO,BRNO,ZONENO,CASHNOTE,TRXCODE,WORKDATE,AUTHTLNO,RECIPACT,
                                       PHYBRNO,TERMID,BUSITIME,'' TRXCITE
                              FROM S_PFEPOTDL_QUERY_DAT T1
                             WHERE (BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                               AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                               AND MDCARDNO = I_ACCNO
                               AND ACSERNO = I_ACSERNO
                               --AND CURRTYPE = I_CURRTYPE
                             ORDER BY TIMESTMP
                             ) DETAIL_DATA,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'SUMMFLAG'
                            ) SUMMFLAG_DICT,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'CASHEXF'
                             ) CASHEXF_DICT,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'DRCRF'
                             ) DRCRF_DICT,
                             (
                              SELECT PRMNAME,PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'TRXCODE'
                             ) TRXCODE_DICT,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'SERVFACE'
                             ) SERVFACE_DICT,
                             NTHCABRP_S_DAT BRNO_DICT,
                             NTHPAZON_S_DAT ZONENO_DICT
                         WHERE DETAIL_DATA.SUMMFLAG = SUMMFLAG_DICT.PRMCODE(+)
                           AND DETAIL_DATA.CASHEXF = CASHEXF_DICT.PRMCODE(+)
                           AND DETAIL_DATA.DRCRF = DRCRF_DICT.PRMCODE(+)
                           AND DETAIL_DATA.TRXCODE = TRXCODE_DICT.PRMCODE(+)
                           AND DETAIL_DATA.SERVFACE = SERVFACE_DICT.PRMCODE(+)
                           AND DETAIL_DATA.BRNO = BRNO_DICT.BRNO(+)
                           AND DETAIL_DATA.ZONENO = BRNO_DICT.ZONENO(+)
                           AND DETAIL_DATA.ZONENO = ZONENO_DICT.ZONENO(+)
                    ) A
                     WHERE ROWNUM <= v_qryoffset_end
                   )
                   WHERE SEQ >= i_qryoffset;

                    SELECT COUNT(1) INTO o_qry_count
                    FROM (SELECT 1,ROWNUM SEQ
                    FROM (SELECT  /*+index(  T1 INX_PFEPOTDL_QUERY_DAT_1 T1 INX_PFEPOTDL_QUERY_DAT_2)*/ 1
                      FROM s_pfepotdl_query_dat T1
                       WHERE (t1.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND t1.MDCARDNO = i_accno
                       AND T1.ACSERNO = i_acserno
                       --AND t1.CURRTYPE = i_currtype
                       --AND t1.zoneaccno = v_subcode
                       ORDER BY t1.timestmp
                    ) A
                     WHERE ROWNUM <= v_qryoffset_end)
                     WHERE SEQ >= I_QRYOFFSET;
                 else
                   --否则考虑币种
                   OPEN o_resultset FOR
                    SELECT *
                     FROM (SELECT ROWNUM SEQ,A.*
                    FROM (
                      SELECT BUSIDATE,
                             nvl(SUMMFLAG_DICT.PRMNAME,DETAIL_DATA.SUMMFLAG) SUMMFLAG,
                             CURRTYPE,
                             nvl(CASHEXF_DICT.PRMNAME,DETAIL_DATA.CASHEXF) CASHEXF,
                             nvl(DRCRF_DICT.PRMNAME,DETAIL_DATA.DRCRF) DRCRF,
                             AMOUNT,
                             BALANCE,
                             nvl(SERVFACE_DICT.PRMNAME,DETAIL_DATA.SERVFACE) SERVFACE,
                             TELLERNO,
                             nvl(BRNO_DICT.Notes,DETAIL_DATA.BRNO) BRNO,
                             nvl(ZONENO_DICT.Notes,DETAIL_DATA.ZONENO) ZONENO,
                             CASHNOTE,
                             nvl(TRXCODE_DICT.PRMNAME,DETAIL_DATA.TRXCODE) TRXCODE,
                             WORKDATE,
                             AUTHTLNO,
                             RECIPACT,
                             PHYBRNO,
                             TERMID,
                             BUSITIME,
                             TRXCITE,
                               ' ' TRANCURR,
                               ' ' TRANAMOUNT,
                               ' ' NATIONID
                          FROM (SELECT  /*+index(  T1 INX_PFEPOTDL_QUERY_DAT_1 T1 INX_PFEPOTDL_QUERY_DAT_2)*/
                                       BUSIDATE,SUMMFLAG,CURRTYPE,CASHEXF,DRCRF,AMOUNT,BALANCE,SERVFACE,
                                       TELLERNO,BRNO,ZONENO,CASHNOTE,TRXCODE,WORKDATE,AUTHTLNO,RECIPACT,
                                       PHYBRNO,TERMID,BUSITIME,'' TRXCITE
                              FROM S_PFEPOTDL_QUERY_DAT T1
                             WHERE (BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                               AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                               AND MDCARDNO = I_ACCNO
                               AND ACSERNO = I_ACSERNO
                               AND CURRTYPE = I_CURRTYPE
                             ORDER BY TIMESTMP
                             ) DETAIL_DATA,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'SUMMFLAG'
                            ) SUMMFLAG_DICT,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'CASHEXF'
                             ) CASHEXF_DICT,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'DRCRF'
                             ) DRCRF_DICT,
                             (
                              SELECT PRMNAME,PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'TRXCODE'
                             ) TRXCODE_DICT,
                             (
                              SELECT PRMNAME, PRMCODE
                                FROM CODE_PRM
                               WHERE APP_NAME = 'PRAS'
                                 AND PRMFIELDNAME = 'SERVFACE'
                             ) SERVFACE_DICT,
                             NTHCABRP_S_DAT BRNO_DICT,
                             NTHPAZON_S_DAT ZONENO_DICT
                         WHERE DETAIL_DATA.SUMMFLAG = SUMMFLAG_DICT.PRMCODE(+)
                           AND DETAIL_DATA.CASHEXF = CASHEXF_DICT.PRMCODE(+)
                           AND DETAIL_DATA.DRCRF = DRCRF_DICT.PRMCODE(+)
                           AND DETAIL_DATA.TRXCODE = TRXCODE_DICT.PRMCODE(+)
                           AND DETAIL_DATA.SERVFACE = SERVFACE_DICT.PRMCODE(+)
                           AND DETAIL_DATA.BRNO = BRNO_DICT.BRNO(+)
                           AND DETAIL_DATA.ZONENO = BRNO_DICT.ZONENO(+)
                           AND DETAIL_DATA.ZONENO = ZONENO_DICT.ZONENO(+)
                    ) A
                     WHERE ROWNUM <= v_qryoffset_end
                   )
                   WHERE SEQ >= i_qryoffset;

                    SELECT COUNT(1) INTO o_qry_count
                    FROM (SELECT 1,ROWNUM SEQ
                    FROM (SELECT  /*+index(  T1 INX_PFEPOTDL_QUERY_DAT_1 T1 INX_PFEPOTDL_QUERY_DAT_2)*/ 1
                      FROM s_pfepotdl_query_dat T1
                       WHERE (t1.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND t1.MDCARDNO = i_accno
                       AND T1.ACSERNO = i_acserno
                       AND t1.CURRTYPE = i_currtype
                       --AND t1.zoneaccno = v_subcode
                       ORDER BY t1.timestmp
                    ) A
                     WHERE ROWNUM <= v_qryoffset_end)
                     WHERE SEQ >= I_QRYOFFSET;
                 end if;
             END IF;

             IF(i_querykind = '1') THEN
                --活期，用帐号ACCNO, CURRTYPE, BUSIDATE, ZONEACCNO
                IF(i_qry_channel = '3') THEN
                  IF(I_CURRTYPE IS NULL) THEN
                    IF(i_cashexf IS NULL) THEN
                     ---修改点1  如果柜面渠道 币种 和 钞汇标志均为空。
                      insert into S_PFEPSADL_QUERY_TEMP
                        SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount,
                               balance, servface, tellerno, brno, zoneno, cashnote, trxcode,
                               workdate, authtlno, recipact, phybrno, termid, busitime,
                               trxcite,accno,SERIALNO FROM (SELECT ROWNUM SEQ,A.*
                          FROM (
                                SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                         busidate, summflag, currtype, cashexf, drcrf, amount,
                                         balance, servface, tellerno, brno, zoneno, cashnote, trxcode,
                                         workdate, authtlno, recipact, phybrno, termid, busitime,
                                         trxsite trxcite,accno,SERIALNO
                                    FROM s_pfepsadl_query_dat T1
                                   WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                     AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                     AND accno = i_accno
                                     AND zoneaccno = v_subcode
                                ORDER BY timestmp
                             ) A
                         WHERE ROWNUM <= v_qryoffset_end
                       )
                      WHERE SEQ >= i_qryoffset;

                     OPEN o_resultset FOR
                        SELECT detail_data.seq, detail_data.busidate,
                               NVL (summflag_dict.prmname, detail_data.summflag) summflag,
                               detail_data.currtype,
                               NVL (cashexf_dict.prmname, detail_data.cashexf) cashexf,
                               NVL (drcrf_dict.prmname, detail_data.drcrf) drcrf, detail_data.amount,
                               detail_data.balance,
                               NVL (servface_dict.prmname, detail_data.servface) servface,
                               detail_data.tellerno, detail_data.brno, detail_data.zoneno,
                               detail_data.cashnote, detail_data.trxcode, detail_data.workdate,
                               detail_data.authtlno, detail_data.recipact, detail_data.phybrno,
                               detail_data.termid, detail_data.busitime, detail_data.trxcite,
                               detail_data.trancurr, detail_data.tranamount, detail_data.nationid
                          FROM (SELECT t1.seq, t1.busidate, t1.summflag, t1.currtype, t1.cashexf,
                                       t1.drcrf, t1.amount, t1.balance, t1.servface, t1.tellerno,
                                       t1.brno, t1.zoneno, t1.cashnote, t1.trxcode, t1.workdate,
                                       t1.authtlno, t1.recipact, t1.phybrno, t1.termid, t1.busitime,
                                       t1.trxcite,
                                       (CASE
                                           WHEN t2.trxcurr IS NULL
                                              --THEN t1.currtype
                                        THEN '000'
                                           ELSE t2.trxcurr
                                        END
                                       ) trancurr,
                                       (CASE
                                           WHEN t2.trxamt IS NULL
                                              --THEN t1.amount
                                        THEN 0
                                           ELSE t2.trxamt
                                        END) tranamount,
                                       (CASE
                                           WHEN t2.crycode IS NULL
                                              THEN 'CHN'
                                           ELSE t2.crycode
                                        END
                                       ) nationid
                                  FROM s_pfepsadl_query_temp t1, s_pfepabdl_query_dat t2
                                 WHERE t1.accno = t2.accno(+)
                                   AND t1.SERIALNO = t2.SERIALNO(+)
                                   AND t1.currtype = t2.currtype(+)
                                   AND t1.cashexf = t2.cashexf(+)
                                   AND t1.busidate = t2.busidate(+)
                                   order by to_number(t1.seq)
                                   ) detail_data,
                               (SELECT prmname, prmcode
                                  FROM code_prm
                                 WHERE app_name = 'PRAS' AND prmfieldname = 'SUMMFLAG') summflag_dict,
                               (SELECT prmname, prmcode
                                  FROM code_prm
                                 WHERE app_name = 'PRAS' AND prmfieldname = 'CASHEXF') cashexf_dict,
                               (SELECT prmname, prmcode
                                  FROM code_prm
                                 WHERE app_name = 'PRAS' AND prmfieldname = 'DRCRF') drcrf_dict,
                               (SELECT prmname, prmcode
                                  FROM code_prm
                                 WHERE app_name = 'PRAS' AND prmfieldname = 'SERVFACE') servface_dict
                         WHERE detail_data.summflag = summflag_dict.prmcode(+)
                           AND detail_data.cashexf = cashexf_dict.prmcode(+)
                           AND detail_data.drcrf = drcrf_dict.prmcode(+)
                           AND detail_data.servface = servface_dict.prmcode(+);

                    ---修改点1结束

                      SELECT COUNT(1) INTO o_qry_count
                        FROM (SELECT 1,ROWNUM SEQ
                          FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                            1 FROM S_PFEPSADL_QUERY_DAT T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                           --AND CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                             AND ZONEACCNO = V_SUBCODE
                           --AND CASHEXF LIKE '%'||i_cashexf||'%'
                             ORDER BY t1.timestmp
                          ) A
                           WHERE ROWNUM <= v_qryoffset_end)
                         WHERE SEQ >= I_QRYOFFSET;
                    ELSE

                ---修改点2  如果柜面渠道 币种为空 但 钞汇标志不为空
                      insert into S_PFEPSADL_QUERY_TEMP
                      SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                             servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                             authtlno, recipact, phybrno, termid, busitime, trxcite,
                             accno,SERIALNO
                          FROM (SELECT ROWNUM SEQ,A.*
                            FROM (
                              SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                       busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                                       servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                                       authtlno, recipact, phybrno, termid, busitime, trxsite trxcite,
                                       accno,SERIALNO
                                  FROM s_pfepsadl_query_dat T1
                                 WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                   AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                   AND accno = i_accno
                                   --AND CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                                   AND zoneaccno = v_subcode
                                   AND cashexf = i_cashexf
                              ORDER BY timestmp
                            ) A
                           WHERE ROWNUM <= v_qryoffset_end
                         )
                        WHERE SEQ >= i_qryoffset;

                        OPEN o_resultset FOR
                            SELECT detail_data.seq, detail_data.busidate,
                                   NVL (summflag_dict.prmname, detail_data.summflag) summflag,
                                   detail_data.currtype,
                                   NVL (cashexf_dict.prmname, detail_data.cashexf) cashexf,
                                   NVL (drcrf_dict.prmname, detail_data.drcrf) drcrf, detail_data.amount,
                                   detail_data.balance,
                                   NVL (servface_dict.prmname, detail_data.servface) servface,
                                   detail_data.tellerno, detail_data.brno, detail_data.zoneno,
                                   detail_data.cashnote, detail_data.trxcode, detail_data.workdate,
                                   detail_data.authtlno, detail_data.recipact, detail_data.phybrno,
                                   detail_data.termid, detail_data.busitime, detail_data.trxcite,
                                   detail_data.trancurr, detail_data.tranamount, detail_data.nationid
                              FROM (SELECT t1.seq, t1.busidate, t1.summflag, t1.currtype, t1.cashexf,
                                           t1.drcrf, t1.amount, t1.balance, t1.servface, t1.tellerno,
                                           t1.brno, t1.zoneno, t1.cashnote, t1.trxcode, t1.workdate,
                                           t1.authtlno, t1.recipact, t1.phybrno, t1.termid, t1.busitime,
                                           t1.trxcite,
                                           (CASE
                                               WHEN t2.trxcurr IS NULL
                                                  --THEN t1.currtype
                                            THEN '000'
                                               ELSE t2.trxcurr
                                            END
                                           ) trancurr,
                                           (CASE
                                               WHEN t2.trxamt IS NULL
                                                  --THEN t1.amount
                                            THEN 0
                                               ELSE t2.trxamt
                                            END) tranamount,
                                           (CASE
                                               WHEN t2.crycode IS NULL
                                                  THEN 'CHN'
                                               ELSE t2.crycode
                                            END
                                           ) nationid
                                      FROM s_pfepsadl_query_temp t1, s_pfepabdl_query_dat t2
                                     WHERE t1.accno = t2.accno(+)
                                       AND t1.SERIALNO = t2.SERIALNO(+)
                                       AND t1.currtype = t2.currtype(+)
                                       AND t1.cashexf = t2.cashexf(+)
                                       AND t1.busidate = t2.busidate(+)
                                       order by to_number(t1.seq)
                                       ) detail_data,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'SUMMFLAG') summflag_dict,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'CASHEXF') cashexf_dict,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'DRCRF') drcrf_dict,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'SERVFACE') servface_dict
                             WHERE detail_data.summflag = summflag_dict.prmcode(+)
                               AND detail_data.cashexf = cashexf_dict.prmcode(+)
                               AND detail_data.drcrf = drcrf_dict.prmcode(+)
                               AND detail_data.servface = servface_dict.prmcode(+);
                  ---修改点2 结束
                        SELECT COUNT(1) INTO o_qry_count
                        FROM (SELECT 1,ROWNUM SEQ
                          FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                          1 FROM S_PFEPSADL_QUERY_DAT T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                           --AND CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                             AND ZONEACCNO = V_SUBCODE
                             AND CASHEXF = i_cashexf
                             ORDER BY t1.timestmp
                          ) A
                           WHERE ROWNUM <= v_qryoffset_end)
                         WHERE SEQ >= I_QRYOFFSET;
                    END IF;
                  ELSE
                    IF(i_cashexf IS NULL) THEN

                  ----修改点3 如果柜面渠道 币种不为空 但钞汇标志为空
                      insert into S_PFEPSADL_QUERY_TEMP
                        SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                               servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                               authtlno, recipact, phybrno, termid, busitime, trxcite,
                               accno,SERIALNO
                          FROM (SELECT ROWNUM SEQ,A.*
                            FROM (
                              SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                       busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                                       servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                                       authtlno, recipact, phybrno, termid, busitime, trxsite trxcite,
                                       accno,SERIALNO
                                  FROM s_pfepsadl_query_dat T1
                                 WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                   AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                   AND accno = i_accno
                                   AND currtype = i_currtype
                                   AND zoneaccno = v_subcode
                                 --AND CASHEXF LIKE '%'||i_cashexf||'%'
                                 ORDER BY timestmp
                            ) A
                           WHERE ROWNUM <= v_qryoffset_end
                         )
                        WHERE SEQ >= i_qryoffset;


                        OPEN o_resultset FOR
                            SELECT detail_data.seq, detail_data.busidate,
                                   NVL (summflag_dict.prmname, detail_data.summflag) summflag,
                                   detail_data.currtype,
                                   NVL (cashexf_dict.prmname, detail_data.cashexf) cashexf,
                                   NVL (drcrf_dict.prmname, detail_data.drcrf) drcrf, detail_data.amount,
                                   detail_data.balance,
                                   NVL (servface_dict.prmname, detail_data.servface) servface,
                                   detail_data.tellerno, detail_data.brno, detail_data.zoneno,
                                   detail_data.cashnote, detail_data.trxcode, detail_data.workdate,
                                   detail_data.authtlno, detail_data.recipact, detail_data.phybrno,
                                   detail_data.termid, detail_data.busitime, detail_data.trxcite,
                                   detail_data.trancurr, detail_data.tranamount, detail_data.nationid
                              FROM (SELECT t1.seq, t1.busidate, t1.summflag, t1.currtype, t1.cashexf,
                                           t1.drcrf, t1.amount, t1.balance, t1.servface, t1.tellerno,
                                           t1.brno, t1.zoneno, t1.cashnote, t1.trxcode, t1.workdate,
                                           t1.authtlno, t1.recipact, t1.phybrno, t1.termid, t1.busitime,
                                           t1.trxcite,
                                           (CASE
                                               WHEN t2.trxcurr IS NULL
                                                  --THEN t1.currtype
                                            THEN '000'
                                               ELSE t2.trxcurr
                                            END
                                           ) trancurr,
                                           (CASE
                                               WHEN t2.trxamt IS NULL
                                                  --THEN t1.amount
                                            THEN 0
                                               ELSE t2.trxamt
                                            END) tranamount,
                                           (CASE
                                               WHEN t2.crycode IS NULL
                                                  THEN 'CHN'
                                               ELSE t2.crycode
                                            END
                                           ) nationid
                                      FROM s_pfepsadl_query_temp t1, s_pfepabdl_query_dat t2
                                     WHERE t1.accno = t2.accno(+)
                                       AND t1.SERIALNO = t2.SERIALNO(+)
                                       AND t1.currtype = t2.currtype(+)
                                       AND t1.cashexf = t2.cashexf(+)
                                       AND t1.busidate = t2.busidate(+)
                                       order by to_number(t1.seq)
                                       ) detail_data,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'SUMMFLAG') summflag_dict,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'CASHEXF') cashexf_dict,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'DRCRF') drcrf_dict,
                                   (SELECT prmname, prmcode
                                      FROM code_prm
                                     WHERE app_name = 'PRAS' AND prmfieldname = 'SERVFACE') servface_dict
                             WHERE detail_data.summflag = summflag_dict.prmcode(+)
                               AND detail_data.cashexf = cashexf_dict.prmcode(+)
                               AND detail_data.drcrf = drcrf_dict.prmcode(+)
                               AND detail_data.servface = servface_dict.prmcode(+);
                   ---修改点3结束
                        SELECT COUNT(1) INTO o_qry_count
                        FROM (SELECT 1,ROWNUM SEQ
                          FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                          1 FROM S_PFEPSADL_QUERY_DAT T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             AND CURRTYPE = I_CURRTYPE
                             AND ZONEACCNO = V_SUBCODE
                           --AND CASHEXF LIKE '%'||i_cashexf||'%'
                             ORDER BY t1.timestmp
                          ) A
                           WHERE ROWNUM <= v_qryoffset_end)
                         WHERE SEQ >= I_QRYOFFSET;
                    ELSE

               ---修改点4开始 如果柜面渠道 币种不为空 钞汇标志也不为空
                      insert into S_PFEPSADL_QUERY_TEMP
                        SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                               servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                               authtlno, recipact, phybrno, termid, busitime, trxcite,
                               accno,SERIALNO
                          FROM (SELECT ROWNUM SEQ,A.*
                            FROM (
                              SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                       busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                                       servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                                       authtlno, recipact, phybrno, termid, busitime, trxsite trxcite,
                                       accno,SERIALNO
                                  FROM s_pfepsadl_query_dat T1
                                 WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                   AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                   AND accno = i_accno
                                   AND currtype = i_currtype
                                   AND zoneaccno = v_subcode
                                   AND cashexf = i_cashexf
                                 ORDER BY timestmp
                            ) A
                           WHERE ROWNUM <= v_qryoffset_end
                         )
                        WHERE SEQ >= i_qryoffset;
                      OPEN o_resultset FOR
                          SELECT detail_data.seq, detail_data.busidate,
                                 NVL (summflag_dict.prmname, detail_data.summflag) summflag,
                                 detail_data.currtype,
                                 NVL (cashexf_dict.prmname, detail_data.cashexf) cashexf,
                                 NVL (drcrf_dict.prmname, detail_data.drcrf) drcrf, detail_data.amount,
                                 detail_data.balance,
                                 NVL (servface_dict.prmname, detail_data.servface) servface,
                                 detail_data.tellerno, detail_data.brno, detail_data.zoneno,
                                 detail_data.cashnote, detail_data.trxcode, detail_data.workdate,
                                 detail_data.authtlno, detail_data.recipact, detail_data.phybrno,
                                 detail_data.termid, detail_data.busitime, detail_data.trxcite,
                                 detail_data.trancurr, detail_data.tranamount, detail_data.nationid
                            FROM (SELECT t1.seq, t1.busidate, t1.summflag, t1.currtype, t1.cashexf,
                                         t1.drcrf, t1.amount, t1.balance, t1.servface, t1.tellerno,
                                         t1.brno, t1.zoneno, t1.cashnote, t1.trxcode, t1.workdate,
                                         t1.authtlno, t1.recipact, t1.phybrno, t1.termid, t1.busitime,
                                         t1.trxcite,
                                         (CASE
                                             WHEN t2.trxcurr IS NULL
                                                --THEN t1.currtype
                                          THEN '000'
                                             ELSE t2.trxcurr
                                          END
                                         ) trancurr,
                                         (CASE
                                             WHEN t2.trxamt IS NULL
                                                --THEN t1.amount
                                          THEN 0
                                             ELSE t2.trxamt
                                          END) tranamount,
                                         (CASE
                                             WHEN t2.crycode IS NULL
                                                THEN 'CHN'
                                             ELSE t2.crycode
                                          END
                                         ) nationid
                                    FROM s_pfepsadl_query_temp t1, s_pfepabdl_query_dat t2
                                   WHERE t1.accno = t2.accno(+)
                                     AND t1.SERIALNO = t2.SERIALNO(+)
                                     AND t1.currtype = t2.currtype(+)
                                     AND t1.cashexf = t2.cashexf(+)
                                     AND t1.busidate = t2.busidate(+)
                                     order by to_number(t1.seq)
                                     ) detail_data,
                                 (SELECT prmname, prmcode
                                    FROM code_prm
                                   WHERE app_name = 'PRAS' AND prmfieldname = 'SUMMFLAG') summflag_dict,
                                 (SELECT prmname, prmcode
                                    FROM code_prm
                                   WHERE app_name = 'PRAS' AND prmfieldname = 'CASHEXF') cashexf_dict,
                                 (SELECT prmname, prmcode
                                    FROM code_prm
                                   WHERE app_name = 'PRAS' AND prmfieldname = 'DRCRF') drcrf_dict,
                                 (SELECT prmname, prmcode
                                    FROM code_prm
                                   WHERE app_name = 'PRAS' AND prmfieldname = 'SERVFACE') servface_dict
                           WHERE detail_data.summflag = summflag_dict.prmcode(+)
                             AND detail_data.cashexf = cashexf_dict.prmcode(+)
                             AND detail_data.drcrf = drcrf_dict.prmcode(+)
                             AND detail_data.servface = servface_dict.prmcode(+);
                ----修改点4结束

                      SELECT COUNT(1) INTO o_qry_count
                        FROM (SELECT 1,ROWNUM SEQ
                          FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                          1 FROM S_PFEPSADL_QUERY_DAT T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             AND CURRTYPE = I_CURRTYPE
                             AND ZONEACCNO = V_SUBCODE
                             AND CASHEXF = i_cashexf
                             ORDER BY t1.timestmp
                          ) A
                           WHERE ROWNUM <= v_qryoffset_end)
                         WHERE SEQ >= I_QRYOFFSET;
                    END IF;
                  END IF;
                ELSE

             ----修改点5     如果非柜面渠道  币种是必输的， 不需要考虑钞汇标志
                   if(i_qry_channel = '1') then
                     --如果是网银渠道，不考虑币种，查询全部
                     insert into S_PFEPSADL_QUERY_TEMP
                      SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                             servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                             authtlno, recipact, phybrno, termid, busitime, trxcite,
                             accno,SERIALNO
                        FROM (SELECT ROWNUM SEQ,A.*
                          FROM (
                            SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                     busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                                     servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                                     authtlno, recipact, phybrno, termid, busitime, trxsite trxcite,
                                     accno,SERIALNO
                                FROM s_pfepsadl_query_dat T1
                               WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                 AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                 AND accno = i_accno
                                 --AND currtype = i_currtype
                                 AND zoneaccno = v_subcode
                            ORDER BY timestmp
                          ) A
                         WHERE ROWNUM <= v_qryoffset_end
                       )
                      WHERE SEQ >= i_qryoffset;
                   else
                     --如果是电银，则考虑币种: 如果是000则查询全部； 否则按输入的币种查询
                     if(lpad(i_currtype, 3, '0') = '000') then
                       insert into S_PFEPSADL_QUERY_TEMP
                        SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                               servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                               authtlno, recipact, phybrno, termid, busitime, trxcite,
                               accno,SERIALNO
                          FROM (SELECT ROWNUM SEQ,A.*
                            FROM (
                              SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                       busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                                       servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                                       authtlno, recipact, phybrno, termid, busitime, trxsite trxcite,
                                       accno,SERIALNO
                                  FROM s_pfepsadl_query_dat T1
                                 WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                   AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                   AND accno = i_accno
                                   --AND currtype = i_currtype
                                   AND zoneaccno = v_subcode
                              ORDER BY timestmp
                            ) A
                           WHERE ROWNUM <= v_qryoffset_end
                         )
                        WHERE SEQ >= i_qryoffset;
                     else
                       insert into S_PFEPSADL_QUERY_TEMP
                        SELECT SEQ,busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                               servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                               authtlno, recipact, phybrno, termid, busitime, trxcite,
                               accno,SERIALNO
                          FROM (SELECT ROWNUM SEQ,A.*
                            FROM (
                              SELECT   /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                                       busidate, summflag, currtype, cashexf, drcrf, amount, balance,
                                       servface, tellerno, brno, zoneno, cashnote, trxcode, workdate,
                                       authtlno, recipact, phybrno, termid, busitime, trxsite trxcite,
                                       accno,SERIALNO
                                  FROM s_pfepsadl_query_dat T1
                                 WHERE (busidate BETWEEN i_qry_date_from AND i_qry_date_to)
                                   AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                   AND accno = i_accno
                                   AND currtype = i_currtype
                                   AND zoneaccno = v_subcode
                              ORDER BY timestmp
                            ) A
                           WHERE ROWNUM <= v_qryoffset_end
                         )
                        WHERE SEQ >= i_qryoffset;
                     end if;
                   end if;

                   OPEN o_resultset FOR
                      SELECT detail_data.seq, detail_data.busidate,
                             NVL (summflag_dict.prmname, detail_data.summflag) summflag,
                             detail_data.currtype,
                             NVL (cashexf_dict.prmname, detail_data.cashexf) cashexf,
                             NVL (drcrf_dict.prmname, detail_data.drcrf) drcrf, detail_data.amount,
                             detail_data.balance,
                             NVL (servface_dict.prmname, detail_data.servface) servface,
                             detail_data.tellerno, NVL (brno_dict.notes, detail_data.brno) brno,
                             NVL (zoneno_dict.notes, detail_data.zoneno) zoneno,
                             detail_data.cashnote,
                             NVL (trxcode_dict.prmname, detail_data.trxcode) trxcode,
                             detail_data.workdate, detail_data.authtlno, detail_data.recipact,
                             detail_data.phybrno, detail_data.termid, detail_data.busitime,
                             detail_data.trxcite, detail_data.trancurr, detail_data.tranamount,
                             detail_data.nationid
                        FROM (SELECT t1.seq, t1.busidate, t1.summflag, t1.currtype, t1.cashexf,
                                     t1.drcrf, t1.amount, t1.balance, t1.servface, t1.tellerno,
                                     t1.brno, t1.zoneno, t1.cashnote, t1.trxcode, t1.workdate,
                                     t1.authtlno, t1.recipact, t1.phybrno, t1.termid, t1.busitime,
                                     t1.trxcite,
                                     (CASE
                                         WHEN t2.trxcurr IS NULL
                                            --THEN t1.currtype
                                      THEN '000'
                                         ELSE t2.trxcurr
                                      END
                                     ) trancurr,
                                     (CASE
                                         WHEN t2.trxamt IS NULL
                                            --THEN t1.amount
                                      THEN 0
                                         ELSE t2.trxamt
                                      END) tranamount,
                                     (CASE
                                         WHEN t2.crycode IS NULL
                                            THEN 'CHN'
                                         ELSE t2.crycode
                                      END
                                     ) nationid
                                FROM s_pfepsadl_query_temp t1, s_pfepabdl_query_dat t2
                               WHERE t1.accno = t2.accno(+)
                                 AND t1.SERIALNO = t2.SERIALNO(+)
                                 AND t1.currtype = t2.currtype(+)
                                 AND t1.cashexf = t2.cashexf(+)
                                 AND t1.busidate = t2.busidate(+)
                                 order by to_number(t1.seq)
                                 ) detail_data,
                             (SELECT prmname, prmcode
                                FROM code_prm
                               WHERE app_name = 'PRAS' AND prmfieldname = 'SUMMFLAG') summflag_dict,
                             (SELECT prmname, prmcode
                                FROM code_prm
                               WHERE app_name = 'PRAS' AND prmfieldname = 'CASHEXF') cashexf_dict,
                             (SELECT prmname, prmcode
                                FROM code_prm
                               WHERE app_name = 'PRAS' AND prmfieldname = 'DRCRF') drcrf_dict,
                             (SELECT prmname, prmcode
                                FROM code_prm
                               WHERE app_name = 'PRAS' AND prmfieldname = 'TRXCODE') trxcode_dict,
                             (SELECT prmname, prmcode
                                FROM code_prm
                               WHERE app_name = 'PRAS' AND prmfieldname = 'SERVFACE') servface_dict,
                             nthcabrp_s_dat brno_dict,
                             nthpazon_s_dat zoneno_dict
                       WHERE detail_data.summflag = summflag_dict.prmcode(+)
                         AND detail_data.cashexf = cashexf_dict.prmcode(+)
                         AND detail_data.drcrf = drcrf_dict.prmcode(+)
                         AND detail_data.trxcode = trxcode_dict.prmcode(+)
                         AND detail_data.servface = servface_dict.prmcode(+)
                         AND detail_data.brno = brno_dict.brno(+)
                         AND detail_data.zoneno = brno_dict.zoneno(+)
                         AND detail_data.zoneno = zoneno_dict.zoneno(+);
              ----修改点5结束
                  if(i_qry_channel = '1') then
                     --如果是网银渠道，不考虑币种，查询全部
                     SELECT COUNT(1) INTO o_qry_count
                      FROM (SELECT 1,ROWNUM SEQ
                        FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                        1 FROM S_PFEPSADL_QUERY_DAT T1
                           WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                           AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                           AND T1.ACCNO = I_ACCNO
                           --AND T1.CURRTYPE = I_CURRTYPE
                           AND T1.ZONEACCNO = V_SUBCODE
                           ORDER BY t1.timestmp
                        ) A
                         WHERE ROWNUM <= v_qryoffset_end)
                       WHERE SEQ >= I_QRYOFFSET;
                   else
                     --如果是电银，则考虑币种: 如果是000则查询全部； 否则按输入的币种查询
                     if(lpad(i_currtype, 3, '0') = '000') then
                       SELECT COUNT(1) INTO o_qry_count
                        FROM (SELECT 1,ROWNUM SEQ
                          FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                          1 FROM S_PFEPSADL_QUERY_DAT T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             --AND T1.CURRTYPE = I_CURRTYPE
                             AND T1.ZONEACCNO = V_SUBCODE
                             ORDER BY t1.timestmp
                          ) A
                           WHERE ROWNUM <= v_qryoffset_end)
                         WHERE SEQ >= I_QRYOFFSET;
                     else
                       SELECT COUNT(1) INTO o_qry_count
                        FROM (SELECT 1,ROWNUM SEQ
                          FROM (SELECT /*+index(  T1 INX_PFEPSADL_QUERY_DAT_1)*/
                          1 FROM S_PFEPSADL_QUERY_DAT T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             AND T1.CURRTYPE = I_CURRTYPE
                             AND T1.ZONEACCNO = V_SUBCODE
                             ORDER BY t1.timestmp
                          ) A
                           WHERE ROWNUM <= v_qryoffset_end)
                         WHERE SEQ >= I_QRYOFFSET;
                     end if;
                   end if;
                END IF;
             END IF;

             IF(i_querykind = '5') THEN
                --QUERYKIND = 5 and CARDACC = 1  用帐号  零存整取
                --ACCNO, CURRTYPE, BUSIDATE, ZONEACCNO
                if(i_qry_channel = '1') then
                  --如果是网银渠道， 不区分币种。
                  OPEN o_resultset FOR
                  SELECT *
                    FROM (SELECT ROWNUM SEQ,A.*
                      FROM (
                          SELECT BUSIDATE,
                                 nvl(SUMMFLAG_DICT.PRMNAME,DETAIL_DATA.SUMMFLAG) SUMMFLAG,
                                 CURRTYPE,
                                 nvl(CASHEXF_DICT.PRMNAME,DETAIL_DATA.CASHEXF) CASHEXF,
                                 nvl(DRCRF_DICT.PRMNAME,DETAIL_DATA.DRCRF) DRCRF,
                                 AMOUNT,
                                 BALANCE,
                                 nvl(SERVFACE_DICT.PRMNAME,DETAIL_DATA.SERVFACE) SERVFACE,
                                 TELLERNO,
                                 nvl(BRNO_DICT.Notes,DETAIL_DATA.BRNO) BRNO,
                                 nvl(ZONENO_DICT.Notes,DETAIL_DATA.ZONENO) ZONENO,
                                 CASHNOTE,
                                 nvl(TRXCODE_DICT.PRMNAME,DETAIL_DATA.TRXCODE) TRXCODE,
                                 WORKDATE,
                                 AUTHTLNO,
                                 RECIPACT,
                                 PHYBRNO,
                                 TERMID,
                                 BUSITIME,
                                 TRXCITE,
                                 '' TRANCURR,
                                 '' TRANAMOUNT,
                                 '' NATIONID
                              FROM (SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                                           BUSIDATE,SUMMFLAG,CURRTYPE,CASHEXF,DRCRF,AMOUNT,BALANCE,SERVFACE,
                                           TELLERNO,BRNO,ZONENO,CASHNOTE,TRXCODE,WORKDATE,AUTHTLNO,RECIPACT,
                                           PHYBRNO,TERMID,BUSITIME,'' TRXCITE
                                  FROM S_PFEPVMDL_QUERY_DAT T1
                                 WHERE (BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                                   AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                   AND ACCNO = I_ACCNO
                                   --AND CURRTYPE = I_CURRTYPE
                                   AND ZONEACCNO = V_SUBCODE
                                 ORDER BY TIMESTMP
                                 ) DETAIL_DATA,
                                 (
                                  SELECT PRMNAME, PRMCODE
                                    FROM CODE_PRM
                                   WHERE APP_NAME = 'PRAS'
                                     AND PRMFIELDNAME = 'SUMMFLAG'
                                ) SUMMFLAG_DICT,
                                 (
                                  SELECT PRMNAME, PRMCODE
                                    FROM CODE_PRM
                                   WHERE APP_NAME = 'PRAS'
                                     AND PRMFIELDNAME = 'CASHEXF'
                                 ) CASHEXF_DICT,
                                 (
                                  SELECT PRMNAME, PRMCODE
                                    FROM CODE_PRM
                                   WHERE APP_NAME = 'PRAS'
                                     AND PRMFIELDNAME = 'DRCRF'
                                 ) DRCRF_DICT,
                                 (
                                  SELECT PRMNAME,PRMCODE
                                    FROM CODE_PRM
                                   WHERE APP_NAME = 'PRAS'
                                     AND PRMFIELDNAME = 'TRXCODE'
                                 ) TRXCODE_DICT,
                                 (
                                  SELECT PRMNAME, PRMCODE
                                    FROM CODE_PRM
                                   WHERE APP_NAME = 'PRAS'
                                     AND PRMFIELDNAME = 'SERVFACE'
                                 ) SERVFACE_DICT,
                                 NTHCABRP_S_DAT BRNO_DICT,
                                 NTHPAZON_S_DAT ZONENO_DICT
                             WHERE DETAIL_DATA.SUMMFLAG = SUMMFLAG_DICT.PRMCODE(+)
                               AND DETAIL_DATA.CASHEXF = CASHEXF_DICT.PRMCODE(+)
                               AND DETAIL_DATA.DRCRF = DRCRF_DICT.PRMCODE(+)
                               AND DETAIL_DATA.TRXCODE = TRXCODE_DICT.PRMCODE(+)
                               AND DETAIL_DATA.SERVFACE = SERVFACE_DICT.PRMCODE(+)
                               AND DETAIL_DATA.BRNO = BRNO_DICT.BRNO(+)
                               AND DETAIL_DATA.ZONENO = BRNO_DICT.ZONENO(+)
                               AND DETAIL_DATA.ZONENO = ZONENO_DICT.ZONENO(+)
                      ) A
                       WHERE ROWNUM <= v_qryoffset_end
                     )
                     WHERE SEQ >= i_qryoffset;

                   SELECT COUNT(1)
                     INTO O_QRY_COUNT
                     FROM (SELECT 1, ROWNUM SEQ
                              FROM (SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                                   1 FROM S_PFEPVMDL_QUERY_DAT T1
                                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                       AND T1.ACCNO = I_ACCNO
                                       --AND T1.CURRTYPE = I_CURRTYPE
                                       AND T1.ZONEACCNO = V_SUBCODE
                                     ORDER BY T1.TIMESTMP) A
                             WHERE ROWNUM <= V_QRYOFFSET_END)
                     WHERE SEQ >= I_QRYOFFSET;
                else
                  --如果是电银，则考虑币种: 如果是000则查询全部； 否则按输入的币种查询
                  --如果是柜面，则考虑币种
                   if(i_qry_channel = '2' and lpad(i_currtype, 3, '0') = '000') then
                     OPEN o_resultset FOR
                    SELECT *
                      FROM (SELECT ROWNUM SEQ,A.*
                        FROM (
                            SELECT BUSIDATE,
                                   nvl(SUMMFLAG_DICT.PRMNAME,DETAIL_DATA.SUMMFLAG) SUMMFLAG,
                                   CURRTYPE,
                                   nvl(CASHEXF_DICT.PRMNAME,DETAIL_DATA.CASHEXF) CASHEXF,
                                   nvl(DRCRF_DICT.PRMNAME,DETAIL_DATA.DRCRF) DRCRF,
                                   AMOUNT,
                                   BALANCE,
                                   nvl(SERVFACE_DICT.PRMNAME,DETAIL_DATA.SERVFACE) SERVFACE,
                                   TELLERNO,
                                   nvl(BRNO_DICT.Notes,DETAIL_DATA.BRNO) BRNO,
                                   nvl(ZONENO_DICT.Notes,DETAIL_DATA.ZONENO) ZONENO,
                                   CASHNOTE,
                                   nvl(TRXCODE_DICT.PRMNAME,DETAIL_DATA.TRXCODE) TRXCODE,
                                   WORKDATE,
                                   AUTHTLNO,
                                   RECIPACT,
                                   PHYBRNO,
                                   TERMID,
                                   BUSITIME,
                                   TRXCITE,
                                   '' TRANCURR,
                                   '' TRANAMOUNT,
                                   '' NATIONID
                                FROM (SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                                             BUSIDATE,SUMMFLAG,CURRTYPE,CASHEXF,DRCRF,AMOUNT,BALANCE,SERVFACE,
                                             TELLERNO,BRNO,ZONENO,CASHNOTE,TRXCODE,WORKDATE,AUTHTLNO,RECIPACT,
                                             PHYBRNO,TERMID,BUSITIME,'' TRXCITE
                                    FROM S_PFEPVMDL_QUERY_DAT T1
                                   WHERE (BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                                     AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                     AND ACCNO = I_ACCNO
                                     --AND CURRTYPE = I_CURRTYPE
                                     AND ZONEACCNO = V_SUBCODE
                                   ORDER BY TIMESTMP
                                   ) DETAIL_DATA,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'SUMMFLAG'
                                  ) SUMMFLAG_DICT,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'CASHEXF'
                                   ) CASHEXF_DICT,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'DRCRF'
                                   ) DRCRF_DICT,
                                   (
                                    SELECT PRMNAME,PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'TRXCODE'
                                   ) TRXCODE_DICT,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'SERVFACE'
                                   ) SERVFACE_DICT,
                                   NTHCABRP_S_DAT BRNO_DICT,
                                   NTHPAZON_S_DAT ZONENO_DICT
                               WHERE DETAIL_DATA.SUMMFLAG = SUMMFLAG_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.CASHEXF = CASHEXF_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.DRCRF = DRCRF_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.TRXCODE = TRXCODE_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.SERVFACE = SERVFACE_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.BRNO = BRNO_DICT.BRNO(+)
                                 AND DETAIL_DATA.ZONENO = BRNO_DICT.ZONENO(+)
                                 AND DETAIL_DATA.ZONENO = ZONENO_DICT.ZONENO(+)
                        ) A
                         WHERE ROWNUM <= v_qryoffset_end
                       )
                       WHERE SEQ >= i_qryoffset;

                     SELECT COUNT(1)
                       INTO O_QRY_COUNT
                       FROM (SELECT 1, ROWNUM SEQ
                                FROM (SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                                          1
                                        FROM S_PFEPVMDL_QUERY_DAT T1
                                       WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                                         AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                         AND T1.ACCNO = I_ACCNO
                                         --AND T1.CURRTYPE = I_CURRTYPE
                                         AND T1.ZONEACCNO = V_SUBCODE
                                       ORDER BY T1.TIMESTMP) A
                               WHERE ROWNUM <= V_QRYOFFSET_END)
                       WHERE SEQ >= I_QRYOFFSET;
                   else
                     OPEN o_resultset FOR
                    SELECT *
                      FROM (SELECT ROWNUM SEQ,A.*
                        FROM (
                            SELECT BUSIDATE,
                                   nvl(SUMMFLAG_DICT.PRMNAME,DETAIL_DATA.SUMMFLAG) SUMMFLAG,
                                   CURRTYPE,
                                   nvl(CASHEXF_DICT.PRMNAME,DETAIL_DATA.CASHEXF) CASHEXF,
                                   nvl(DRCRF_DICT.PRMNAME,DETAIL_DATA.DRCRF) DRCRF,
                                   AMOUNT,
                                   BALANCE,
                                   nvl(SERVFACE_DICT.PRMNAME,DETAIL_DATA.SERVFACE) SERVFACE,
                                   TELLERNO,
                                   nvl(BRNO_DICT.Notes,DETAIL_DATA.BRNO) BRNO,
                                   nvl(ZONENO_DICT.Notes,DETAIL_DATA.ZONENO) ZONENO,
                                   CASHNOTE,
                                   nvl(TRXCODE_DICT.PRMNAME,DETAIL_DATA.TRXCODE) TRXCODE,
                                   WORKDATE,
                                   AUTHTLNO,
                                   RECIPACT,
                                   PHYBRNO,
                                   TERMID,
                                   BUSITIME,
                                   TRXCITE,
                                   '' TRANCURR,
                                   '' TRANAMOUNT,
                                   '' NATIONID
                                FROM (SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                                             BUSIDATE,SUMMFLAG,CURRTYPE,CASHEXF,DRCRF,AMOUNT,BALANCE,SERVFACE,
                                             TELLERNO,BRNO,ZONENO,CASHNOTE,TRXCODE,WORKDATE,AUTHTLNO,RECIPACT,
                                             PHYBRNO,TERMID,BUSITIME,'' TRXCITE
                                    FROM S_PFEPVMDL_QUERY_DAT T1
                                   WHERE (BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                                     AND (DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                     AND ACCNO = I_ACCNO
                                     AND CURRTYPE = I_CURRTYPE
                                     AND ZONEACCNO = V_SUBCODE
                                   ORDER BY TIMESTMP
                                   ) DETAIL_DATA,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'SUMMFLAG'
                                  ) SUMMFLAG_DICT,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'CASHEXF'
                                   ) CASHEXF_DICT,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'DRCRF'
                                   ) DRCRF_DICT,
                                   (
                                    SELECT PRMNAME,PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'TRXCODE'
                                   ) TRXCODE_DICT,
                                   (
                                    SELECT PRMNAME, PRMCODE
                                      FROM CODE_PRM
                                     WHERE APP_NAME = 'PRAS'
                                       AND PRMFIELDNAME = 'SERVFACE'
                                   ) SERVFACE_DICT,
                                   NTHCABRP_S_DAT BRNO_DICT,
                                   NTHPAZON_S_DAT ZONENO_DICT
                               WHERE DETAIL_DATA.SUMMFLAG = SUMMFLAG_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.CASHEXF = CASHEXF_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.DRCRF = DRCRF_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.TRXCODE = TRXCODE_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.SERVFACE = SERVFACE_DICT.PRMCODE(+)
                                 AND DETAIL_DATA.BRNO = BRNO_DICT.BRNO(+)
                                 AND DETAIL_DATA.ZONENO = BRNO_DICT.ZONENO(+)
                                 AND DETAIL_DATA.ZONENO = ZONENO_DICT.ZONENO(+)
                        ) A
                         WHERE ROWNUM <= v_qryoffset_end
                       )
                       WHERE SEQ >= i_qryoffset;

                     SELECT COUNT(1)
                       INTO O_QRY_COUNT
                       FROM (SELECT 1, ROWNUM SEQ
                                FROM (SELECT /*+index(T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                                        1
                                        FROM S_PFEPVMDL_QUERY_DAT T1
                                       WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                                         AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                                         AND T1.ACCNO = I_ACCNO
                                         AND T1.CURRTYPE = I_CURRTYPE
                                         AND T1.ZONEACCNO = V_SUBCODE
                                       ORDER BY T1.TIMESTMP) A
                               WHERE ROWNUM <= V_QRYOFFSET_END)
                       WHERE SEQ >= I_QRYOFFSET;
                   end if;
                end if;
             END IF;
         END IF;

         ----如果是统计
         IF(i_reqtype = '2') THEN
            IF(i_querykind = '0') THEN
               --QUERYKIND = 0 and CARDACC = 0  :理财金E时代卡
               OPEN o_resultset FOR SELECT 1 SEQ, '20100523' BUSIDATE, '1' SUMMFLAG, '001' CURRTYPE, '1' CASHEXF, '1' DRCRF, 1 AMOUNT, 1 BALANCE, '1' SERVFACE, '1' TELLERNO, '1' BRNO, '1' ZONENO, '1' CASHNOTE, '1' TRXCODE, '20100523' WORKDATE, '1' AUTHTLNO, '1' RECIPACT, '1' PHYBRNO, '1' TERMID, '1' BUSITIME, '' TRXCITE FROM DUAL;
               if(i_qry_channel = '1') then
                   --如果是网银渠道，不考虑币种，查询全部
                   SELECT  /*+index( T1 INX_PFEPOTDL_QUERY_DAT_1 T1 INX_PFEPOTDL_QUERY_DAT_2)*/ COUNT(1)
                  INTO O_STAT_COUNT
                  FROM S_PFEPOTDL_QUERY_DAT T1
                 WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                   AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND T1.MDCARDNO = I_ACCNO
                   AND T1.ACSERNO = I_ACSERNO
                   --AND T1.CURRTYPE = I_CURRTYPE
                   AND ROWNUM <= 301;
               else
                   --电银和柜面 考虑币种
                   SELECT  /*+index( T1 INX_PFEPOTDL_QUERY_DAT_1 T1 INX_PFEPOTDL_QUERY_DAT_2)*/ COUNT(1)
                  INTO O_STAT_COUNT
                  FROM S_PFEPOTDL_QUERY_DAT T1
                 WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                   AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND T1.MDCARDNO = I_ACCNO
                   AND T1.ACSERNO = I_ACSERNO
                   AND T1.CURRTYPE = I_CURRTYPE
                   AND ROWNUM <= 301;
               end if;
            END IF;

            IF(i_querykind = '1') THEN
              --活期，用帐号ACCNO, CURRTYPE, BUSIDATE, ZONEACCNO
              OPEN o_resultset FOR SELECT 1 SEQ, '20100523' BUSIDATE, '1' SUMMFLAG, '001' CURRTYPE, '1' CASHEXF, '1' DRCRF, 1 AMOUNT, 1 BALANCE, '1' SERVFACE, '1' TELLERNO, '1' BRNO, '1' ZONENO, '1' CASHNOTE, '1' TRXCODE, '20100523' WORKDATE, '1' AUTHTLNO, '1' RECIPACT, '1' PHYBRNO, '1' TERMID, '' BUSITIME, '' TRXCITE FROM DUAL;
              IF(i_qry_channel = '3') THEN
                --如果是柜面查询
                IF(I_CURRTYPE IS NULL) THEN
                  --如果币种为空
                  IF(i_cashexf IS NULL) THEN
                    SELECT /*+index( T1 INX_PFEPSADL_QUERY_DAT_1)*/
                    COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPSADL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                     --AND CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                       AND ZONEACCNO = V_SUBCODE
                     --AND CASHEXF LIKE '%'||i_cashexf||'%'
                       AND ROWNUM <= 301;
                  ELSE
                    SELECT /*+index( T1 INX_PFEPSADL_QUERY_DAT_1)*/
                    COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPSADL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                     --AND CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                       AND ZONEACCNO = V_SUBCODE
                       AND CASHEXF = i_cashexf
                       AND ROWNUM <= 301;
                  END IF;
                ELSE
                  --如果币种不为空
                  IF(i_cashexf IS NULL) THEN
                    SELECT /*+index( T1 INX_PFEPSADL_QUERY_DAT_1)*/
                    COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPSADL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                       AND CURRTYPE = I_CURRTYPE
                       AND ZONEACCNO = V_SUBCODE
                     --AND CASHEXF LIKE '%'||i_cashexf||'%'
                       AND ROWNUM <= 301;
                  ELSE
                    SELECT /*+index(T1 INX_PFEPSADL_QUERY_DAT_1)*/
                    COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPSADL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                       AND CURRTYPE = I_CURRTYPE
                       AND ZONEACCNO = V_SUBCODE
                       AND CASHEXF = i_cashexf
                       AND ROWNUM <= 301;
                  END IF;
                END IF;
              ELSE
                --如果不是柜面查询
                if(i_qry_channel = '1') then
                   --如果是网银渠道，不考虑币种，查询全部
                   SELECT /*+index(T1 INX_PFEPSADL_QUERY_DAT_1)*/
                   COUNT(1)
                  INTO O_STAT_COUNT
                  FROM S_PFEPSADL_QUERY_DAT T1
                 WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                   AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND T1.ACCNO = I_ACCNO
                   --AND T1.CURRTYPE = I_CURRTYPE
                   AND T1.ZONEACCNO = V_SUBCODE
                   AND ROWNUM <= 301;
                 else
                   --如果是电银，则考虑币种: 如果是000则查询全部； 否则按输入的币种查询
                   if(lpad(i_currtype, 3, '0') = '000') then
                     SELECT /*+index(T1 INX_PFEPSADL_QUERY_DAT_1)*/
                     COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPSADL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                       --AND T1.CURRTYPE = I_CURRTYPE
                       AND T1.ZONEACCNO = V_SUBCODE
                       AND ROWNUM <= 301;
                   else
                     SELECT /*+index( T1 INX_PFEPSADL_QUERY_DAT_1)*/
                     COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPSADL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                       AND T1.CURRTYPE = I_CURRTYPE
                       AND T1.ZONEACCNO = V_SUBCODE
                       AND ROWNUM <= 301;
                   end if;
                 end if;
              END IF;
            END IF;

            IF(i_querykind = '5') THEN
               --QUERYKIND = 0 and CARDACC = 1  用帐号  零存整取
               OPEN o_resultset FOR SELECT 1 SEQ, '20100523' BUSIDATE, '1' SUMMFLAG, '001' CURRTYPE, '1' CASHEXF, '1' DRCRF, 1 AMOUNT, 1 BALANCE, '1' SERVFACE, '1' TELLERNO, '1' BRNO, '1' ZONENO, '1' CASHNOTE, '1' TRXCODE, '20100523' WORKDATE, '1' AUTHTLNO, '1' RECIPACT, '1' PHYBRNO, '1' TERMID, '' BUSITIME, '' TRXCITE,'' TRANCURR, '' TRANAMOUNT, '' NATIONID FROM DUAL;
                if(i_qry_channel = '1') then
                   --如果是网银渠道，不考虑币种，查询全部
                   SELECT /*+index(T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                   COUNT(1)
                  INTO O_STAT_COUNT
                  FROM S_PFEPVMDL_QUERY_DAT T1
                 WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                   AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND T1.ACCNO = I_ACCNO
                   --AND T1.CURRTYPE = I_CURRTYPE
                   AND T1.ZONEACCNO = V_SUBCODE
                   AND ROWNUM <= 301;
                 else
                   --如果是电银，如果是000则查询全部； 否则按输入的币种查询
                  --如果是柜面，则考虑币种
                   if(i_qry_channel = '2' and lpad(i_currtype, 3, '0') = '000') then
                     SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                       COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPVMDL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                       --AND T1.CURRTYPE = I_CURRTYPE
                       AND T1.ZONEACCNO = V_SUBCODE
                       AND ROWNUM <= 301;
                   else
                     SELECT /*+index( T1 INX_PFEPVMDL_QUERY_DAT_1 )*/
                       COUNT(1)
                      INTO O_STAT_COUNT
                      FROM S_PFEPVMDL_QUERY_DAT T1
                     WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                       AND (T1.DRCRF = I_DRCRF OR I_DRCRF = '0') --201505
                       AND T1.ACCNO = I_ACCNO
                       AND T1.CURRTYPE = I_CURRTYPE
                       AND T1.ZONEACCNO = V_SUBCODE
                       AND ROWNUM <= 301;
                   end if;
                 end if;
            END IF;
         END IF;

         --如果是异步
         IF(i_reqtype = '3') THEN
           OPEN o_resultset FOR SELECT 1 SEQ, '20100523' BUSIDATE, '1' SUMMFLAG, '001' CURRTYPE, '1' CASHEXF, '1' DRCRF, 1 AMOUNT, 1 BALANCE, '1' SERVFACE, '1' TELLERNO, '1' BRNO, '1' ZONENO, '1' CASHNOTE, '1' TRXCODE, '20100523' WORKDATE, '1' AUTHTLNO, '1' RECIPACT, '1' PHYBRNO, '1' TERMID, '' BUSITIME,'' TRXCITE,'' TRANCURR, '' TRANAMOUNT, '' NATIONID FROM DUAL;
         END IF;

         --记录查询性能日志
         IF((i_reqtype = '0' OR i_reqtype = '1') AND i_enableLogQueryTime = '0') THEN
            INSERT INTO LOG_PERFORMANCE(
                   REQ_ID,PHASE_ID,PHASE_BEGIN,PHASE_END,PHASE_RESULT
            )VALUES(
                   i_taskid,'01',v_perf_log_begin,systimestamp,'0'
            );
         END IF;
         ----记录统计性能日志
         IF(i_reqtype = '2' AND i_enableLogStatTime = '0') THEN
            INSERT INTO LOG_PERFORMANCE(
                   REQ_ID,PHASE_ID,PHASE_BEGIN,PHASE_END,PHASE_RESULT
            )VALUES(
                   i_taskid,'00',v_perf_log_begin,systimestamp,'0'
            );
         END IF;
         --记录插入异步队列性能日志
         IF(i_reqtype = '3' AND i_enableLogAsyncQueTime = '0') THEN
            INSERT INTO LOG_PERFORMANCE(
                   REQ_ID,PHASE_ID,PHASE_BEGIN,PHASE_END,PHASE_RESULT
            )VALUES(
                   i_taskid,'03',v_perf_log_begin,systimestamp,'0'
            );
         END IF;
         COMMIT;

         --清空临时表
         delete from S_PFEPSADL_QUERY_TEMP;
         COMMIT;

      EXCEPTION
         WHEN OTHERS THEN
              ROLLBACK;
              COMMIT;
              o_successflag := '1';
              o_errordesc := SQLERRM (SQLCODE);
              RAISE;
      END;
      RETURN;
   EXCEPTION
      WHEN OTHERS THEN
         o_successflag := 1;
         o_errordesc := SQLERRM (SQLCODE);
   END proc_dq001_1;


    /************************************************
  --存储过程名称：proc_dq001_2
  --作者：贾雪松
  --时间：2011年01月28
  --版本号:1.0
  --使用源表名称：
  --使用目标表名称：
  --参数说明：
  --功能：查询DQ001接口中帐务查询类型为2,3,4部分
  ************************************************/
   PROCEDURE proc_dq001_2 (
                           i_taskid        IN     VARCHAR2,--查询流水号
                           i_accno         IN     VARCHAR2,--账号
                           i_qry_date_from IN     VARCHAR2,--查询起始日期
                           i_qry_date_to   IN     VARCHAR2,--查询结束日期
                           i_qry_channel   IN     VARCHAR2,--渠道标志 1-网银；2-WAP；3-电银；4-柜面
                           i_currtype      IN     VARCHAR2,--查询开始时间
                           i_reqtype       IN     VARCHAR2,--请求类型
                           i_qryoffset     IN     VARCHAR2,--查询起始条数
                           i_querykind     IN     VARCHAR2,--帐务明细种类
                           i_enableLogQueryTime     IN     VARCHAR2,--查询性能日志开关
                           i_enableLogStatTime      IN     VARCHAR2,--统计性能日志开关
                           i_enableLogAsyncQueTime  IN     VARCHAR2,--异步性能日志开关
                           i_drcrf         IN     VARCHAR2,--201505 借贷标识 0-全部 1-借 2-贷
                           o_successflag   OUT    VARCHAR2,--成功失败标识
                           o_errordesc     OUT    VARCHAR2,--错误描述
                           o_resultset     OUT    c_dq001_list2,--结果集
                           o_qry_count     OUT    INTEGER,      --查询记录数
                           o_stat_count    OUT    INTEGER      --统计记录数
   ) IS
    v_subcode VARCHAR2(4);
    v_qryoffset_end integer;
    v_shownum integer;
    v_perf_log_begin DATE;
     BEGIN
        o_successflag := '0';
        o_qry_count := 0;
        o_stat_count := 0;
        v_subcode := substr(i_accno,1,4);
      if(i_reqtype = '0') then
        v_shownum := 20;
      END if;
      if(i_reqtype = '1') then
        v_shownum := 299;
      END IF;
      v_qryoffset_end := i_qryoffset+v_shownum;
      v_perf_log_begin := SYSDATE;
      BEGIN
      ---------------------执行查询-----------------
        IF(i_reqtype = '0' or i_reqtype = '1') THEN
           IF(i_querykind = '2' or i_querykind = '3') THEN
              ---------QUERYKIND = 2:  贷记帐户
              ---------QUERYKIND = 3:  国际卡
              ---------ACCNO, CURRTYPE, BUSIDATE, ZONEACCNO
              IF(i_qry_channel = '3') THEN
                IF(I_CURRTYPE IS NULL) THEN
                  OPEN o_resultset FOR
                   SELECT *
                    FROM (SELECT ROWNUM SEQ,A.*
                      FROM (
                          select
                             MDCARDNO,
                             TRXDATE,
                             BUSIDATE,

                             /*
                               信用卡部分：
                               柜面不查询超期信用卡数据。
                               网银和电银都用MDKMODE作为摘要，我们可以把TRXDISCP字段的值取出来，以MDKMODE的名称来反馈。
                               这样网银和电银就不用修改了信用卡部份了。
                             */
                             TRXDISCP MDKMODE,
                             NVL(SERVFACE_DICT.PRMNAME,detail_data.SERVFACE) SERVFACE,
                             NVL(dbcrf_dict.PRMNAME,detail_data.DBCRF) DBCRF,
                             TRXAMT,
                             TRXCURR,
                             BALANCE,
                             CURRTYPE,
                             LSTBAL,
                             BUSITIME,
                             TRXSITE,
                             detail_data.ZONENO ZONENO,
                             TIPAMT,
                             CFEERAT,
                             detail_data.ZONENOREAL,
                             detail_data.TRXTIME,
                             detail_data.TRXCODE,
                             detail_data.TELLERNO,
                             TRXCOUNTRY
                          from
                        (
                          SELECT /*+  index(T1 INX_BFHCRDBF_QUERY_DAT_1 ) */
                               MDCARDNO,
                               TRXDATE,
                               BUSIDATE,
                               TRXDISCP,
                               SERVFACE,
                               DBCRF,
                               TRXAMT,
                               TRXCURR,
                               BALANCE,
                               CURRTYPE,
                               LSTBAL,
                               BUSITIME,
                               TRXSITE,
                               ZONENO,
                               TIPAMT,
                               CFEEAMT CFEERAT,
                               ZONENO ZONENOREAL,
                               BUSITIME TRXTIME,
                               TRXCODE,
                               TELLERNO,
                               TRXCOUNTRY
                            FROM S_BFHCRDBF_QUERY_DAT T1
                           WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                           --AND T1.CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                             AND T1.ZONEACCNO = V_SUBCODE
                             AND T1.LISTFLAG IN ('1', '9')
                           ORDER BY T1.TIMESTMP
                        ) detail_data,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'DBCRF'
                        ) dbcrf_dict,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'SERVFACE'
                        ) servface_dict
                        WHERE detail_data.DBCRF = dbcrf_dict.PRMCODE(+)
                          AND detail_data.SERVFACE = servface_dict.PRMCODE(+)
                      ) A
                     WHERE ROWNUM <= v_qryoffset_end
                   )
                   WHERE SEQ >= i_qryoffset;

                   SELECT COUNT(1) INTO o_qry_count
                    FROM (SELECT 1,ROWNUM SEQ
                      FROM (
                        SELECT /*+  index(T INX_BFHCRDBF_QUERY_DAT_1 ) */
                        1
                          FROM s_bfhcrdbf_query_dat T
                         WHERE (T.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                           AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                           AND T.ACCNO = I_ACCNO
                         --AND T.CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                           AND T.ZONEACCNO = V_SUBCODE
                           AND T.LISTFLAG IN ('1','9')
                         ORDER BY t.timestmp
                      ) A
                       WHERE ROWNUM <= v_qryoffset_end)
                     WHERE SEQ >= I_QRYOFFSET;
                ELSE
                  OPEN o_resultset FOR
                   SELECT *
                    FROM (SELECT ROWNUM SEQ,A.*
                      FROM (
                          select
                             MDCARDNO,
                             TRXDATE,
                             BUSIDATE,

                             /*
                               信用卡部分：
                               柜面不查询超期信用卡数据。
                               网银和电银都用MDKMODE作为摘要，我们可以把TRXDISCP字段的值取出来，以MDKMODE的名称来反馈。
                               这样网银和电银就不用修改了信用卡部份了。
                             */
                             TRXDISCP MDKMODE,
                             NVL(SERVFACE_DICT.PRMNAME,detail_data.SERVFACE) SERVFACE,
                             NVL(dbcrf_dict.PRMNAME,detail_data.DBCRF) DBCRF,
                             TRXAMT,
                             TRXCURR,
                             BALANCE,
                             CURRTYPE,
                             LSTBAL,
                             BUSITIME,
                             TRXSITE,
                             detail_data.ZONENO ZONENO,
                             TIPAMT,
                             CFEERAT,
                             detail_data.ZONENOREAL,
                             detail_data.TRXTIME,
                             detail_data.TRXCODE,
                             detail_data.TELLERNO,
                             TRXCOUNTRY
                          from
                        (
                          SELECT /*+  index(T1 INX_BFHCRDBF_QUERY_DAT_1 ) */
                               MDCARDNO,
                               TRXDATE,
                               BUSIDATE,
                               TRXDISCP,
                               SERVFACE,
                               DBCRF,
                               TRXAMT,
                               TRXCURR,
                               BALANCE,
                               CURRTYPE,
                               LSTBAL,
                               BUSITIME,
                               TRXSITE,
                               ZONENO,
                               TIPAMT,
                               CFEEAMT CFEERAT,
                               ZONENO ZONENOREAL,
                               BUSITIME TRXTIME,
                               TRXCODE,
                               TELLERNO,
                               TRXCOUNTRY
                            FROM S_BFHCRDBF_QUERY_DAT T1
                           WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             AND T1.CURRTYPE = I_CURRTYPE
                             AND T1.ZONEACCNO = V_SUBCODE
                             AND T1.LISTFLAG IN ('1', '9')
                           ORDER BY T1.TIMESTMP
                        ) detail_data,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'DBCRF'
                        ) dbcrf_dict,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'SERVFACE'
                        ) servface_dict
                        WHERE detail_data.DBCRF = dbcrf_dict.PRMCODE(+)
                          AND detail_data.SERVFACE = servface_dict.PRMCODE(+)
                      ) A
                     WHERE ROWNUM <= v_qryoffset_end
                   )
                   WHERE SEQ >= i_qryoffset;

                   SELECT COUNT(1) INTO o_qry_count
                    FROM (SELECT 1,ROWNUM SEQ
                      FROM (
                        SELECT /*+  index(T INX_BFHCRDBF_QUERY_DAT_1 ) */
                          1
                          FROM s_bfhcrdbf_query_dat T
                         WHERE (T.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                           AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                           AND T.ACCNO = I_ACCNO
                           AND T.CURRTYPE = I_CURRTYPE
                           AND T.ZONEACCNO = V_SUBCODE
                           AND T.LISTFLAG IN ('1','9')
                         ORDER BY t.timestmp
                      ) A
                       WHERE ROWNUM <= v_qryoffset_end)
                     WHERE SEQ >= I_QRYOFFSET;
                END IF;
              ELSE
                OPEN o_resultset FOR
                   SELECT *
                    FROM (SELECT ROWNUM SEQ,A.*
                      FROM (
                          select
                             MDCARDNO,
                             TRXDATE,
                             BUSIDATE,

                             /*
                               信用卡部分：
                               柜面不查询超期信用卡数据。
                               网银和电银都用MDKMODE作为摘要，我们可以把TRXDISCP字段的值取出来，以MDKMODE的名称来反馈。
                               这样网银和电银就不用修改了信用卡部份了。
                             */
                             TRXDISCP MDKMODE,
                             NVL(SERVFACE_DICT.PRMNAME,detail_data.SERVFACE) SERVFACE,
                             NVL(dbcrf_dict.PRMNAME,detail_data.DBCRF) DBCRF,
                             TRXAMT,
                             TRXCURR,
                             BALANCE,
                             CURRTYPE,
                             LSTBAL,
                             BUSITIME,
                             TRXSITE,
                             NVL(ZONENO_DICT.Notes,detail_data.ZONENO) ZONENO,
                             TIPAMT,
                             CFEERAT,
                             detail_data.ZONENOREAL,
                             detail_data.TRXTIME,
                             detail_data.TRXCODE,
                             detail_data.TELLERNO,
                             TRXCOUNTRY
                          from
                        (
                          SELECT /*+  index(T1 INX_BFHCRDBF_QUERY_DAT_1 ) */
                               MDCARDNO,
                               TRXDATE,
                               BUSIDATE,
                               TRXDISCP,
                               SERVFACE,
                               DBCRF,
                               TRXAMT,
                               TRXCURR,
                               BALANCE,
                               CURRTYPE,
                               LSTBAL,
                               BUSITIME,
                               TRXSITE,
                               ZONENO,
                               TIPAMT,
                               CFEEAMT CFEERAT,
                               ZONENO ZONENOREAL,
                               BUSITIME TRXTIME,
                               TRXCODE,
                               TELLERNO,
                               TRXCOUNTRY
                            FROM S_BFHCRDBF_QUERY_DAT T1
                           WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             AND T1.CURRTYPE = I_CURRTYPE
                             AND T1.ZONEACCNO = V_SUBCODE
                             AND T1.LISTFLAG IN ('1', '9')
                           ORDER BY T1.TIMESTMP
                        ) detail_data,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'DBCRF'
                        ) dbcrf_dict,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'SERVFACE'
                        ) servface_dict,
                        NTHPAZON_S_DAT zoneno_dict
                        WHERE detail_data.DBCRF = dbcrf_dict.PRMCODE(+)
                          AND detail_data.SERVFACE = servface_dict.PRMCODE(+)
                          AND detail_data.ZONENO = zoneno_dict.ZONENO(+)
                      ) A
                     WHERE ROWNUM <= v_qryoffset_end
                   )
                   WHERE SEQ >= i_qryoffset;

                   SELECT COUNT(1) INTO o_qry_count
                    FROM (SELECT 1,ROWNUM SEQ
                      FROM (
                        SELECT /*+  index(T INX_BFHCRDBF_QUERY_DAT_1 ) */
                          1
                          FROM s_bfhcrdbf_query_dat T
                         WHERE (T.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                           AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                           AND T.ACCNO = I_ACCNO
                           AND T.CURRTYPE = I_CURRTYPE
                           AND T.ZONEACCNO = V_SUBCODE
                           AND T.LISTFLAG IN ('1','9')
                         ORDER BY t.timestmp
                      ) A
                       WHERE ROWNUM <= v_qryoffset_end)
                     WHERE SEQ >= I_QRYOFFSET;
              END IF;
            END IF;

            IF(i_querykind = '4') THEN
               -----QUERYKIND = 4:  准贷记帐户
               -----ACCNO, CURRTYPE, BUSIDATE, ZONEACCNO
               IF(i_qry_channel = '3') THEN
                 IF(I_CURRTYPE IS NULL) THEN
                   OPEN o_resultset FOR
                     SELECT *
                      FROM (SELECT ROWNUM SEQ,A.*
                        FROM (
                          select
                               MDCARDNO,
                               TRXDATE,
                               BUSIDATE,

                               /*
                                 信用卡部分：
                                 柜面不查询超期信用卡数据。
                                 网银和电银都用MDKMODE作为摘要，我们可以把TRXDISCP字段的值取出来，以MDKMODE的名称来反馈。
                                 这样网银和电银就不用修改了信用卡部份了。
                               */
                               TRXDISCP MDKMODE,
                               NVL(SERVFACE_DICT.PRMNAME,detail_data.SERVFACE) SERVFACE,
                               NVL(dbcrf_dict.PRMNAME,detail_data.DBCRF) DBCRF,
                               TRXAMT,
                               TRXCURR,
                               BALANCE,
                               CURRTYPE,
                               LSTBAL,
                               BUSITIME,
                               TRXSITE,
                               detail_data.ZONENO ZONENO,
                               TIPAMT,
                               CFEERAT,
                               detail_data.ZONENOREAL,
                               detail_data.TRXTIME,
                               detail_data.TRXCODE,
                               detail_data.TELLERNO,
                               TRXCOUNTRY
                            from
                          (
                            SELECT /*+  index(T1 INX_BFHSCDBF_QUERY_DAT_1 ) */
                                 MDCARDNO,
                                 TRXDATE,
                                 BUSIDATE,
                                 TRXDISCP,
                                 SERVFACE,
                                 DBCRF,
                                 TRXAMT,
                                 TRXCURR,
                                 BALANCE,
                                 CURRTYPE,
                                 LSTBAL,
                                 BUSITIME,
                                 TRXSITE,
                                 ZONENO,
                                 TIPAMT,
                                 CFEERAT,
                                 ZONENO ZONENOREAL,
                                 BUSITIME TRXTIME,
                                 TRXCODE,
                                 TELLERNO,
                                 substr(BKNOTES, 24, 3) TRXCOUNTRY
                              FROM s_bfhscdbf_query_dat T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                               AND (T1.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                               AND T1.ACCNO = I_ACCNO
                             --AND T1.CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                               AND T1.ZONEACCNO = V_SUBCODE
                               AND T1.LISTFLAG IN ('1', '9')
                             ORDER BY T1.TIMESTMP
                          ) detail_data,
                          (
                            SELECT PRMNAME, PRMCODE
                              FROM CODE_PRM
                             WHERE APP_NAME = 'BCAS'
                               AND PRMFIELDNAME = 'DBCRF'
                          ) dbcrf_dict,
                          (
                            SELECT PRMNAME, PRMCODE
                              FROM CODE_PRM
                             WHERE APP_NAME = 'BCAS'
                               AND PRMFIELDNAME = 'SERVFACE'
                          ) servface_dict
                          WHERE detail_data.DBCRF = dbcrf_dict.PRMCODE(+)
                            AND detail_data.SERVFACE = servface_dict.PRMCODE(+)
                        ) A
                       WHERE ROWNUM <= v_qryoffset_end
                     )
                     WHERE SEQ >= i_qryoffset;

                     SELECT COUNT(1) INTO o_qry_count
                      FROM (SELECT 1,ROWNUM SEQ
                        FROM (SELECT /*+  index(T INX_BFHSCDBF_QUERY_DAT_1 ) */
                          1
                          FROM s_bfhscdbf_query_dat T
                           WHERE (T.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T.ACCNO = I_ACCNO
                           --AND T.CURRTYPE LIKE '%'||I_CURRTYPE||'%'
                             AND T.ZONEACCNO = V_SUBCODE
                             AND T.LISTFLAG IN ('1','9')
                           ORDER BY t.timestmp
                        ) A
                         WHERE ROWNUM <= v_qryoffset_end)
                       WHERE SEQ >= I_QRYOFFSET;
                 ELSE
                   OPEN o_resultset FOR
                     SELECT *
                      FROM (SELECT ROWNUM SEQ,A.*
                        FROM (
                          select
                               MDCARDNO,
                               TRXDATE,
                               BUSIDATE,

                               /*
                                 信用卡部分：
                                 柜面不查询超期信用卡数据。
                                 网银和电银都用MDKMODE作为摘要，我们可以把TRXDISCP字段的值取出来，以MDKMODE的名称来反馈。
                                 这样网银和电银就不用修改了信用卡部份了。
                               */
                               TRXDISCP MDKMODE,
                               NVL(SERVFACE_DICT.PRMNAME,detail_data.SERVFACE) SERVFACE,
                               NVL(dbcrf_dict.PRMNAME,detail_data.DBCRF) DBCRF,
                               TRXAMT,
                               TRXCURR,
                               BALANCE,
                               CURRTYPE,
                               LSTBAL,
                               BUSITIME,
                               TRXSITE,
                               detail_data.ZONENO ZONENO,
                               TIPAMT,
                               CFEERAT,
                               detail_data.ZONENOREAL,
                               detail_data.TRXTIME,
                               detail_data.TRXCODE,
                               detail_data.TELLERNO,
                               TRXCOUNTRY
                            from
                          (
                            SELECT /*+  index(T1 INX_BFHSCDBF_QUERY_DAT_1 ) */
                                 MDCARDNO,
                                 TRXDATE,
                                 BUSIDATE,
                                 TRXDISCP,
                                 SERVFACE,
                                 DBCRF,
                                 TRXAMT,
                                 TRXCURR,
                                 BALANCE,
                                 CURRTYPE,
                                 LSTBAL,
                                 BUSITIME,
                                 TRXSITE,
                                 ZONENO,
                                 TIPAMT,
                                 CFEERAT,
                                 ZONENO ZONENOREAL,
                                 BUSITIME TRXTIME,
                                 TRXCODE,
                                 TELLERNO,
                                 substr(BKNOTES, 24, 3) TRXCOUNTRY
                              FROM s_bfhscdbf_query_dat T1
                             WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                               AND (T1.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                               AND T1.ACCNO = I_ACCNO
                               AND T1.CURRTYPE = I_CURRTYPE
                               AND T1.ZONEACCNO = V_SUBCODE
                               AND T1.LISTFLAG IN ('1', '9')
                             ORDER BY T1.TIMESTMP
                          ) detail_data,
                          (
                            SELECT PRMNAME, PRMCODE
                              FROM CODE_PRM
                             WHERE APP_NAME = 'BCAS'
                               AND PRMFIELDNAME = 'DBCRF'
                          ) dbcrf_dict,
                          (
                            SELECT PRMNAME, PRMCODE
                              FROM CODE_PRM
                             WHERE APP_NAME = 'BCAS'
                               AND PRMFIELDNAME = 'SERVFACE'
                          ) servface_dict
                          WHERE detail_data.DBCRF = dbcrf_dict.PRMCODE(+)
                            AND detail_data.SERVFACE = servface_dict.PRMCODE(+)
                        ) A
                       WHERE ROWNUM <= v_qryoffset_end
                     )
                     WHERE SEQ >= i_qryoffset;

                     SELECT COUNT(1) INTO o_qry_count
                      FROM (SELECT 1,ROWNUM SEQ
                        FROM (SELECT /*+  index(T INX_BFHSCDBF_QUERY_DAT_1 ) */
                          1
                          FROM s_bfhscdbf_query_dat T
                           WHERE (T.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T.ACCNO = I_ACCNO
                             AND T.CURRTYPE = I_CURRTYPE
                             AND T.ZONEACCNO = V_SUBCODE
                             AND T.LISTFLAG IN ('1','9')
                           ORDER BY t.timestmp
                        ) A
                         WHERE ROWNUM <= v_qryoffset_end)
                       WHERE SEQ >= I_QRYOFFSET;
                 END IF;
               ELSE
                 OPEN o_resultset FOR
                   SELECT *
                    FROM (SELECT ROWNUM SEQ,A.*
                      FROM (
                        select
                             MDCARDNO,
                             TRXDATE,
                             BUSIDATE,

                             /*
                               信用卡部分：
                               柜面不查询超期信用卡数据。
                               网银和电银都用MDKMODE作为摘要，我们可以把TRXDISCP字段的值取出来，以MDKMODE的名称来反馈。
                               这样网银和电银就不用修改了信用卡部份了。
                             */
                             TRXDISCP MDKMODE,
                             NVL(SERVFACE_DICT.PRMNAME,detail_data.SERVFACE) SERVFACE,
                             NVL(dbcrf_dict.PRMNAME,detail_data.DBCRF) DBCRF,
                             TRXAMT,
                             TRXCURR,
                             BALANCE,
                             CURRTYPE,
                             LSTBAL,
                             BUSITIME,
                             TRXSITE,
                             NVL(ZONENO_DICT.Notes,detail_data.ZONENO) ZONENO,
                             TIPAMT,
                             CFEERAT,
                             detail_data.ZONENOREAL,
                             detail_data.TRXTIME,
                             detail_data.TRXCODE,
                             detail_data.TELLERNO,
                             TRXCOUNTRY
                          from
                        (
                          SELECT /*+  index(T1 INX_BFHSCDBF_QUERY_DAT_1 ) */
                               MDCARDNO,
                               TRXDATE,
                               BUSIDATE,
                               TRXDISCP,
                               SERVFACE,
                               DBCRF,
                               TRXAMT,
                               TRXCURR,
                               BALANCE,
                               CURRTYPE,
                               LSTBAL,
                               BUSITIME,
                               TRXSITE,
                               ZONENO,
                               TIPAMT,
                               CFEERAT,
                               ZONENO ZONENOREAL,
                               BUSITIME TRXTIME,
                               TRXCODE,
                               TELLERNO,
                               substr(BKNOTES, 24, 3) TRXCOUNTRY
                            FROM s_bfhscdbf_query_dat T1
                           WHERE (T1.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                             AND (T1.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                             AND T1.ACCNO = I_ACCNO
                             AND T1.CURRTYPE = I_CURRTYPE
                             AND T1.ZONEACCNO = V_SUBCODE
                             AND T1.LISTFLAG IN ('1', '9')
                           ORDER BY T1.TIMESTMP
                        ) detail_data,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'DBCRF'
                        ) dbcrf_dict,
                        (
                          SELECT PRMNAME, PRMCODE
                            FROM CODE_PRM
                           WHERE APP_NAME = 'BCAS'
                             AND PRMFIELDNAME = 'SERVFACE'
                        ) servface_dict,
                        NTHPAZON_S_DAT zoneno_dict
                        WHERE detail_data.DBCRF = dbcrf_dict.PRMCODE(+)
                          AND detail_data.SERVFACE = servface_dict.PRMCODE(+)
                          AND detail_data.ZONENO = zoneno_dict.ZONENO(+)
                      ) A
                     WHERE ROWNUM <= v_qryoffset_end
                   )
                   WHERE SEQ >= i_qryoffset;

                    SELECT COUNT(1) INTO o_qry_count
                    FROM (SELECT 1,ROWNUM SEQ
                      FROM (SELECT /*+  index(T INX_BFHSCDBF_QUERY_DAT_1 ) */
                      1
                        FROM s_bfhscdbf_query_dat T
                         WHERE (T.BUSIDATE BETWEEN I_QRY_DATE_FROM AND I_QRY_DATE_TO)
                           AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                           AND T.ACCNO = I_ACCNO
                           AND T.CURRTYPE = I_CURRTYPE
                           AND T.ZONEACCNO = V_SUBCODE
                           AND T.LISTFLAG IN ('1','9')
                         ORDER BY t.timestmp
                      ) A
                       WHERE ROWNUM <= v_qryoffset_end)
                     WHERE SEQ >= I_QRYOFFSET;
               END IF;
             END IF;
        END IF;

        --如果是统计
        IF(i_reqtype = '2') THEN
            IF(i_querykind = '2' or i_querykind = '3') THEN
              OPEN o_resultset FOR SELECT 1 SEQ,'1001' MDCARDNO ,'2005' TRXDATE ,'2005' BUSIDATE ,'MDK' MDKMODE ,'AS' SERVFACE ,'1' DBCRF ,100 TRXAMT ,'001' TRXCURR ,100 BALANCE ,'001' CURRTYPE ,100 LSTBAL ,'100' BUSITIME ,'ss' TRXSITE ,'11' ZONENO ,100 TIPAMT ,100 CFEERAT, '1' ZONENOREAL, '1' TRXTIME, '1' TRXCODE, '1' TELLERNO   FROM DUAL;
              IF(i_qry_channel = '3') THEN
                IF(i_currtype IS NULL) THEN
                  SELECT /*+  index(t INX_BFHCRDBF_QUERY_DAT_1 ) */
                  count(1) into o_stat_count
                    FROM  s_bfhcrdbf_query_dat t
                   WHERE (t.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                     AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                     AND t.ACCNO = i_accno
                   --AND t.CURRTYPE LIKE '%'||i_currtype||'%'
                     AND t.zoneaccno = v_subcode
                     AND t.LISTFLAG IN ('1','9')
                     AND rownum <= 301;
                ELSE
                  SELECT /*+  index(t INX_BFHCRDBF_QUERY_DAT_1 ) */
                  count(1) into o_stat_count
                    FROM  s_bfhcrdbf_query_dat t
                   WHERE (t.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                     AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                     AND t.ACCNO = i_accno
                     AND t.CURRTYPE = i_currtype
                     AND t.zoneaccno = v_subcode
                     AND t.LISTFLAG IN ('1','9')
                     AND rownum <= 301;
                END IF;
              ELSE
                SELECT /*+  index(t INX_BFHCRDBF_QUERY_DAT_1 ) */
                count(1) into o_stat_count
                  FROM  s_bfhcrdbf_query_dat t
                 WHERE (t.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                   AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND t.ACCNO = i_accno
                   AND t.CURRTYPE = i_currtype
                   AND t.zoneaccno = v_subcode
                   AND t.LISTFLAG IN ('1','9')
                   AND rownum <= 301;
              END IF;
          END IF;

          IF(i_querykind = '4') THEN
            OPEN o_resultset FOR SELECT 1 SEQ,'1001' MDCARDNO ,'2005' TRXDATE ,'2005' BUSIDATE ,'MDK' MDKMODE ,'AS' SERVFACE ,'1' DBCRF ,100 TRXAMT ,'001' TRXCURR ,100 BALANCE ,'001' CURRTYPE ,100 LSTBAL ,'100' BUSITIME ,'ss' TRXSITE ,'11' ZONENO ,100 TIPAMT ,100 CFEERAT, '1' ZONENOREAL, '1' TRXTIME, '1' TRXCODE, '1' TELLERNO   FROM DUAL;
            IF(i_qry_channel = '3') THEN
              IF(i_currtype IS NULL) THEN
                SELECT /*+  index(t INX_BFHSCDBF_QUERY_DAT_1 ) */
                count(1) into o_stat_count
                  FROM  s_bfhscdbf_query_dat t
                 WHERE (t.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                   AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND t.ACCNO = i_accno
                 --AND t.CURRTYPE LIKE '%'||i_currtype||'%'
                   AND t.zoneaccno = v_subcode
                   AND t.LISTFLAG IN ('1','9')
                   AND rownum <= 301;
              ELSE
                SELECT /*+  index(t INX_BFHSCDBF_QUERY_DAT_1 ) */
                count(1) into o_stat_count
                  FROM  s_bfhscdbf_query_dat t
                 WHERE (t.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                   AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                   AND t.ACCNO = i_accno
                   AND t.CURRTYPE = i_currtype
                   AND t.zoneaccno = v_subcode
                   AND t.LISTFLAG IN ('1','9')
                   AND rownum <= 301;
              END IF;
            ELSE
              SELECT /*+  index(t INX_BFHSCDBF_QUERY_DAT_1 ) */
              count(1) into o_stat_count
                FROM  s_bfhscdbf_query_dat t
               WHERE (t.BUSIDATE BETWEEN i_qry_date_from AND i_qry_date_to)
                 AND (T.DBCRF = I_DRCRF OR I_DRCRF = '0') --201505
                 AND t.ACCNO = i_accno
                 AND t.CURRTYPE = i_currtype
                 AND t.zoneaccno = v_subcode
                 AND t.LISTFLAG IN ('1','9')
                 AND rownum <= 301;
            END IF;
          END IF;
        END IF;

        --如果是异步
        IF(i_reqtype = '3') THEN
          OPEN o_resultset FOR SELECT 1 SEQ,'1001' MDCARDNO ,'2005' TRXDATE ,'2005' BUSIDATE ,'MDK' MDKMODE ,'AS' SERVFACE ,'1' DBCRF ,100 TRXAMT ,'001' TRXCURR ,100 BALANCE ,'001' CURRTYPE ,100 LSTBAL ,'100' BUSITIME ,'ss' TRXSITE ,'11' ZONENO ,100 TIPAMT ,100 CFEERAT, '1' ZONENOREAL, '1' TRXTIME, '1' TRXCODE, '1' TELLERNO   FROM DUAL;
        END IF;

        --记录查询性能日志
        IF((i_reqtype = '0' OR i_reqtype = '1') AND i_enableLogQueryTime = '0') THEN
            INSERT INTO LOG_PERFORMANCE(
                   REQ_ID,PHASE_ID,PHASE_BEGIN,PHASE_END,PHASE_RESULT
            )VALUES(
                   i_taskid,'01',v_perf_log_begin,SYSDATE,'0'
            );
        END IF;
        --记录统计性能日志
        IF(i_reqtype = '2' AND i_enableLogStatTime = '0') THEN
            INSERT INTO LOG_PERFORMANCE(
                   REQ_ID,PHASE_ID,PHASE_BEGIN,PHASE_END,PHASE_RESULT
            )VALUES(
                   i_taskid,'00',v_perf_log_begin,SYSDATE,'0'
            );
        END IF;
        --记录插入异步队列性能日志
        IF(i_reqtype = '3' AND i_enableLogAsyncQueTime = '0') THEN
            INSERT INTO LOG_PERFORMANCE(
                   REQ_ID,PHASE_ID,PHASE_BEGIN,PHASE_END,PHASE_RESULT
            )VALUES(
                   i_taskid,'03',v_perf_log_begin,SYSDATE,'0'
            );
        END IF;
        COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            COMMIT;
            o_successflag := '1';
            o_errordesc := SQLERRM (SQLCODE);
            RAISE;
      END;
      RETURN;
   EXCEPTION
      WHEN OTHERS THEN
         o_successflag := 1;
         o_errordesc := SQLERRM (SQLCODE);
   END proc_dq001_2;
END PCKG_DQ001;
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

CREATE OR REPLACE PACKAGE hdm_query_file_export
AS

   PROCEDURE hdm_query_file_pfepsadl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );


   PROCEDURE hdm_query_file_pfepvmdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );


   PROCEDURE hdm_query_file_pfepotdl (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_query_file_export;

//

CREATE OR REPLACE PACKAGE BODY hdm_query_file_export
IS
/******************************************************************************
   --存储过程名称：  hdm_query_file_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_query_file_pfepsadl (
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
          := 'hdm_query_file_export.hdm_query_file_PFEPSADL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_list
      IS
         SELECT   qry_id, NO, up_time_start, up_time_end, currtype,
                  CASE
                     WHEN cardacc = '0'
                        THEN '1'
                     WHEN cardacc = '1'
                        THEN '0'
                     ELSE '0'
                  END AS cardacc,
                  acserno
             FROM hdm_query_log_dat a, hdm_query_date_split b
            WHERE a.qry_req_kind = '3'
              AND a.qry_status = '8'
              AND a.qry_id = b.query_id
              AND UPPER (a.qry_table_name) = 'HDM_S_PFEPSADL_QUERY'
              AND b.up_time_start IS NOT NULL
              AND b.up_time_end IS NOT NULL
              AND b.update_date = p_i_date
         ORDER BY qry_id;

      rec_cur           export_list%ROWTYPE                 := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
      v_instance        VARCHAR2 (100)                      := NULL;
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
      p_i_date_v        VARCHAR2 (8)                        := NULL;
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
      p_i_date_v := TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') + 1, 'YYYYMMDD');
      in_file := 'HM2-SADTLREQUEST-' || p_i_date_v || '-0000000000.bin';
      v_step := '3';

      IF p_i_no = 'M2'
      THEN
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_1', in_file, 'w', 32767);
      ELSE
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_2', in_file, 'w', 32767);
      END IF;

      v_step := '4';
      OPEN export_list;
      LOOP
         v_step := '5';

         FETCH export_list
          INTO rec_cur;

         v_step := '6';
         EXIT WHEN export_list%NOTFOUND;

         IF rec_cur.cardacc = '0'
         THEN
            v_step := '7';
            dbms_output.put_line(1111111111111111);
            UTL_FILE.put_line (v_outputfile,
                                  RPAD (rec_cur.qry_id, 20, ' ')
                               || LPAD (rec_cur.NO, 17, 0)
                               || '                   '
                               || LPAD (NVL (rec_cur.acserno, '11111'), 5, 0)
                               || rec_cur.cardacc
                               || LPAD (rec_cur.up_time_start, 10, 0)
                               || LPAD (rec_cur.up_time_end, 10, 0)
                               || LPAD (rec_cur.currtype, 3, 0)
                               || CHR (13),
                               FALSE
                              );
         END IF;
         UTL_FILE.fflush (v_outputfile);
      END LOOP;
      CLOSE export_list;
      UTL_FILE.fclose (v_outputfile);

      v_step := '1A';
      rec_cur := NULL;
      v_outputfile := NULL;
      v_step := '2A';
      p_i_date_v := TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') + 1, 'YYYYMMDD');
      in_file := 'HM2-PTHABDTLREQUEST-' || p_i_date_v || '-0000000000.bin';
      v_step := '3A';

      IF p_i_no = 'M2'
      THEN
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_1', in_file, 'w', 32767);
      ELSE
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_2', in_file, 'w', 32767);
      END IF;

      v_step := '4A';
      OPEN export_list;
      LOOP
         v_step := '5A';

         FETCH export_list
          INTO rec_cur;

         v_step := '6A';
         EXIT WHEN export_list%NOTFOUND;

         IF rec_cur.cardacc = '0'
         THEN
            v_step := '7A';
            UTL_FILE.put_line (v_outputfile,
                                  RPAD (rec_cur.qry_id, 20, ' ')
                               || LPAD (rec_cur.NO, 17, 0)
                               || LPAD (rec_cur.CURRTYPE, 3, 0)
                               || LPAD (rec_cur.up_time_start, 10, 0)
                               || LPAD (rec_cur.up_time_end, 10, 0)
                               || CHR (13),
                               FALSE
                              );
         END IF;
         UTL_FILE.fflush (v_outputfile);
      END LOOP;
      CLOSE export_list;
      UTL_FILE.fclose (v_outputfile);

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
   END hdm_query_file_pfepsadl;

/******************************************************************************
   --存储过程名称：  hdm_query_file_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_query_file_pfepvmdl (
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
          := 'hdm_query_file_export.hdm_query_file_PFEPVMDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_list
      IS
         SELECT   qry_id, NO, up_time_start, up_time_end, currtype,
                  CASE
                     WHEN cardacc = '0'
                        THEN '1'
                     WHEN cardacc = '1'
                        THEN '0'
                     ELSE '0'
                  END AS cardacc,
                  acserno
             FROM hdm_query_log_dat a, hdm_query_date_split b
            WHERE a.qry_req_kind = '3'
              AND a.qry_status = '8'
              AND a.qry_id = b.query_id
              AND UPPER (a.qry_table_name) = 'HDM_S_PFEPVMDL_QUERY'
              AND b.up_time_start IS NOT NULL
              AND b.up_time_end IS NOT NULL
              AND b.update_date = p_i_date
         ORDER BY qry_id;

      rec_cur           export_list%ROWTYPE                 := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
      v_instance        VARCHAR2 (100)                      := NULL;
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
      p_i_date_v        VARCHAR2 (8)                        := NULL;
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
      p_i_date_v := TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') + 1, 'YYYYMMDD');
      in_file := 'HM2-QVMDLREQUEST-' || p_i_date_v || '-0000000000.bin';
      v_step := '2';

      IF p_i_no = 'M2'
      THEN
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_1', in_file, 'w', 32767);
      ELSE
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_2', in_file, 'w', 32767);
      END IF;

      v_step := '3';

      OPEN export_list;

      LOOP
         v_step := '4';

         FETCH export_list
          INTO rec_cur;

         v_step := '5';
         EXIT WHEN export_list%NOTFOUND;
         v_step := '6';
         UTL_FILE.put_line (v_outputfile,
                               LPAD (rec_cur.qry_id, 20, 0)
                            || LPAD (rec_cur.NO, 17, 0)
                            || LPAD (rec_cur.up_time_start, 10, 0)
                            || LPAD (rec_cur.up_time_end, 10, 0)
                            || LPAD (rec_cur.currtype, 3, 0)
                            || CHR (13),
                            FALSE
                           );
         v_step := '7';
         UTL_FILE.fflush (v_outputfile);
      END LOOP;

      CLOSE export_list;

      UTL_FILE.fclose (v_outputfile);
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
   END hdm_query_file_pfepvmdl;

/******************************************************************************
   --存储过程名称：  hdm_query_file_export
   --作者：          孙艳雯
   --时间：          2011年6月28日
   --版本号:
   --使用源表名称：
   --使用目标表名称：
   --参数说明：       p_i_date                (传入参数 日期)
   --                p_o_succeed             (传出参数 成功返回0，失败返回1)
   --功能：          模版程序1
   ******************************************************************************/
   PROCEDURE hdm_query_file_pfepotdl (
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
          := 'hdm_query_file_export.hdm_query_file_PFEPOTDL' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := '0';
      v_max_zoneno      VARCHAR2 (10)                       := '0';
      v_status          hdm_log_task_detail.n_status%TYPE;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_list
      IS
         SELECT   qry_id, NO, up_time_start, up_time_end, currtype,
                  CASE
                     WHEN cardacc = '0'
                        THEN '1'
                     WHEN cardacc = '1'
                        THEN '0'
                     ELSE '0'
                  END AS cardacc,
                  acserno
             FROM hdm_query_log_dat a, hdm_query_date_split b
            WHERE a.qry_req_kind = '3'
              AND a.qry_status = '8'
              AND a.qry_id = b.query_id
              AND UPPER (a.qry_table_name) = 'HDM_S_PFEPOTDL_QUERY'
              AND b.up_time_start IS NOT NULL
              AND b.up_time_end IS NOT NULL
              AND b.update_date = p_i_date
         ORDER BY qry_id;

      rec_cur           export_list%ROWTYPE                 := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
      v_instance        VARCHAR2 (100)                      := NULL;
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
      p_i_date_v        VARCHAR2 (8)                        := NULL;
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
      p_i_date_v := TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') + 1, 'YYYYMMDD');
      in_file := 'HM2-QOTDLREQUEST-' || p_i_date_v || '-0000000000.bin';
      v_step := '3';

      IF p_i_no = 'M2'
      THEN
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_1', in_file, 'w', 32767);
      ELSE
         v_outputfile :=
                    UTL_FILE.fopen ('EXPORT_PRAS_2', in_file, 'w', 32767);
      END IF;

      v_step := '4';

      OPEN export_list;

      LOOP
         v_step := '5';

         FETCH export_list
          INTO rec_cur;

         v_step := '6';
         EXIT WHEN export_list%NOTFOUND;
         v_step := '7';
         UTL_FILE.put_line (v_outputfile,
                               LPAD (rec_cur.qry_id, 20, 0)
                            || LPAD (rec_cur.NO, 19, 0)
                            || LPAD (NVL (rec_cur.acserno, '11111'), 5, 0)
                            || LPAD (rec_cur.up_time_start, 10, 0)
                            || LPAD (rec_cur.up_time_end, 10, 0)
                            || LPAD (rec_cur.currtype, 3, 0)
                            || CHR (13),
                            FALSE
                           );
         v_step := '8';
         UTL_FILE.fflush (v_outputfile);
      END LOOP;

      CLOSE export_list;

      UTL_FILE.fclose (v_outputfile);
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
   END hdm_query_file_pfepotdl;
END hdm_query_file_export;
//

CREATE OR REPLACE PACKAGE hdm_list_export
AS
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
   PROCEDURE hdm_list_export (
      p_i_date      IN       VARCHAR2,                      --(传入参数 日期)
      p_i_no        IN       VARCHAR2,         --(传入参数 M早批 A午批 N晚批)
      p_o_succeed   OUT      VARCHAR2       --(传出参数 成功返回0，失败返回1)
   );
END hdm_list_export;
//
CREATE OR REPLACE PACKAGE BODY hdm_list_export
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
   PROCEDURE hdm_list_export (
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
                        := 'hdm_list_export.hdm_list_export' || '_' || p_i_no;
      --程序名全称
      v_min_zoneno      VARCHAR2 (10)                       := NULL;
      v_max_zoneno      VARCHAR2 (10)                       := NULL;
      v_status          hdm_log_task_detail.n_status%TYPE   := NULL;
      v_max_cnt         PLS_INTEGER                         := 0;
      v_cnt             PLS_INTEGER                         := 0;
      --utl_file缓冲区中已写入的 字节数/定长最大记录数 向上取整的数值
      v_fix_length      PLS_INTEGER                         := 108;

      CURSOR export_list
      IS
         SELECT qry_id, attachment_count, LPAD (a.zoneno, 5, '0') zoneno,
                LPAD (a.brno, 5, '0') brno, email, qry_channel,
                CASE
                   WHEN qry_detail_kind = '0'
                      THEN '账务明细'
                   WHEN qry_detail_kind = '1'
                      THEN '账务明细'
                   WHEN qry_detail_kind = '2'
                      THEN '工资单明细'
                   WHEN qry_detail_kind = '3'
                      THEN '同城转账日志'
                   WHEN qry_detail_kind = '4'
                      THEN '异地汇款日志'
                   WHEN qry_detail_kind = '5'
                      THEN '预约业务日志'
                   WHEN qry_detail_kind = '6'
                      THEN '代理缴费业务日志'
                   WHEN qry_detail_kind = '7' and FUNDKIND='1' THEN '基金交易历史明细日志'
                   WHEN qry_detail_kind = '7' and FUNDKIND='2' THEN '基金定投明细历史明细日志'
                   WHEN qry_detail_kind = '7' and FUNDKIND='3' THEN '新型理财产品历史明细日志'
                   WHEN qry_detail_kind = '8' and TRXKND='2'
                      THEN '账户贵金属交易日志'
                   WHEN qry_detail_kind = '8' and TRXKND<>'2'
                      THEN '小额结售汇交易日志'
                   WHEN qry_detail_kind = '9'
                      THEN '理财产品交易明细日志'
                   ELSE NULL
                END qry_detail_kind,
                mobno, qry_time,qry_table_name
           FROM hdm_query_log_dat a
          WHERE a.qry_status = '5' AND qry_channel <> '5';

      rec_cur           export_list%ROWTYPE                 := NULL;
      v_outputfile      UTL_FILE.file_type              := NULL;
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

      v_step := '3';
      in_file := 'list_' || v_instance || '.txt';
      v_step := '4';
      v_outputfile := UTL_FILE.fopen ('EXPORT_EMAIL', in_file, 'w', 32767);
      /*UTL_FILE.put_line
         (v_outputfile,
          '查询id,场次， 地区号， 网点号， 实际回复的邮箱地址（可以为空）， EMAIL发件人地址， Email收件人地址， Email抄送地址， 服务界面， 业务种类,手机号,日期(格式yyyy年mm月dd日)',
          FALSE
         );                                          -- 将报文头写入文件缓存区
      UTL_FILE.fflush (v_outputfile);*/
      v_step := '5';

      OPEN export_list;

      LOOP
         v_step := '6';

         FETCH export_list
          INTO rec_cur;

         v_step := '7';
         EXIT WHEN export_list%NOTFOUND;
         v_step := '8';
         UTL_FILE.put_line (v_outputfile,
                               rec_cur.qry_id
                            || ','
                            || rec_cur.attachment_count
                            || ','
                            || rec_cur.zoneno
                            || ','
                            || rec_cur.brno
                            || ','
                            || NULL
                            || ','
                            || '中国工商银行'
                            || ','
                            || rec_cur.email
                            || ','
                            || NULL
                            || ','
                            || rec_cur.qry_channel
                            || ','
                            || rec_cur.qry_detail_kind
                            || ','
                            || rec_cur.mobno
                            || ','
                            || SUBSTR (rec_cur.qry_time, 0, 4)
                            || '年'
                            || SUBSTR (rec_cur.qry_time, 5, 2)
                            || '月'
                            || SUBSTR (rec_cur.qry_time, 7, 2)
                            || '日'
                            || ','
                            || rec_cur.qry_table_name ,
                            FALSE
                           );
         UTL_FILE.fflush (v_outputfile);
      END LOOP;

      CLOSE export_list;

      UTL_FILE.fclose (v_outputfile);
      v_step := '9';

      UPDATE hdm_query_log_dat                             /*6-清单导出成功 */
         SET qry_status = '6'
       WHERE qry_status = '5';

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
   END hdm_list_export;
END hdm_list_export;
//

CREATE OR REPLACE PACKAGE PCKG_GEN_YEAR_SUBPARTITIONS
IS
    /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能： 从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成修改分区脚本。
   --       对表进行增加新的年份的二级子分区，并建新增的子分区的索引
     ******************************************************************************/
    PROCEDURE PROC_SUBPARTITIONS_ADD(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能： 从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成修改分区脚本。
   --       对表进行删除旧年份的所有二级子分区，增加新的年份的二级子分区，并建新增的子分区的索引
     ******************************************************************************/
    PROCEDURE PROC_SUBPARTITIONS_DROP(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年07月16日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成视图脚本程序
   --     从表VIEW_LIST取得视图名和包含的表名，生成建视图脚本。
   --
     ******************************************************************************/
PROCEDURE PROC_GEN_VIEW_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数)
    con_dat_max_date IN VARCHAR2, --(传入参数)
    con_month_min_date IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 );
END PCKG_GEN_YEAR_SUBPARTITIONS;

//

CREATE OR REPLACE PACKAGE BODY PCKG_GEN_YEAR_SUBPARTITIONS
IS
 /******************************************************************************
 --存储过程名称： PCKG_GEN_YEAR_SUBPARTITIONS
 --作者： kwg
 --时间： 2015年06月30日
 --版本号:
 --使用源表名称：
 --使用目标表名称：
 --参数说明：
 --功能： 模版程序1
 ******************************************************************************/
 PROCEDURE PROC_SUBPARTITIONS_ADD(
        p_script_name IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(30) := null;
  v_tmp_table_name varchar2(30) := null;
  v_subpartition_name varchar2(30) := null;
  v_old_subpartition_name varchar2(30) := null;
  v_check_name        varchar2(50) := null;
  v_partition_name    varchar2(30) := null;
  v_tablespace_name   varchar2(30) := null;
  v_index_name        varchar2(50) := null;
  v_del_year          varchar2(10) := null;
  v_add_year          varchar2(10) := null;
  v_last_year         varchar2(10) := null;
  v_low_value          varchar2(50) := null;
  v_high_value          varchar2(50) := null;
  v_sql_statement     varchar2(32767) := null;

  TYPE chars_tab is table of varchar2(32767);
  add_sql_lines      chars_tab         := chars_tab();
  i_a            int               := 1;

  --所有需要增删分区的表，需要删除的年份，需要增加的年份
  cursor cur_tabs
  is
   select table_name, del_year, add_year,view_name
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;
  type typ_tabs is table of cur_tabs%rowtype;
  tab_pras_tabs typ_tabs;

  type record_subpartition_type is record (
      partition_name varchar2(50),
      subpartition_name varchar2(50),
      tablespace_name varchar2(50),
      high_value varchar2(50)
  );
  rec_subpartition   record_subpartition_type  := null;

  type tab_subpartition_type is table of record_subpartition_type;
  tab_subpartition tab_subpartition_type;

    --分区所有的索引
  cursor cur_index_partition ( p_table_name varchar2 )
  is
   select distinct i.index_name
     from user_indexes i, user_ind_subpartitions p
     where i.table_name= p_table_name
     and i.index_name = p.index_name;
  rec_index_partition cur_index_partition%rowtype := null;
  type typ_indexes is table of cur_index_partition%rowtype;
  tab_indexes typ_indexes;

  v_output_file    utl_file.file_type;

  type collection_names is table of user_tab_subpartitions.subpartition_name%type;
  col_check_name  collection_names := null;
BEGIN
  p_o_succeed := '0';
  v_step := '1';


  --生成表的增删分区操作脚本
  v_step := '2';

  select table_name, del_year, add_year,view_name
  into rec_tab
  from hds_pras_year_tabs
  where rownum = 1;
  v_step := '3';

  --上一年的子分区范围
  v_del_year := rec_tab.del_year;
  v_add_year := rec_tab.add_year;
  --v_last_year := to_number(v_add_year)-1;
  --v_low_value  := ''''||v_last_year||'-01-32''';
  --v_high_value := ''''||v_last_year||'-12-32''';
  v_table_name := upper(rec_tab.table_name);

  --找出表中需要增加的子分区
  /*
  select partition_name,subpartition_name,tablespace_name, high_value
  bulk collect into tab_subpartition
      from ( select partition_name,subpartition_name,tablespace_name, long_help.substr_of('select high_value
                                            from user_tab_subpartitions
                                           where table_name  = :n
                                            and  subpartition_name = :p',
                                          1, 4000,
                                          'n', table_name,
                                          'p', subpartition_name ) high_value
               from user_tab_subpartitions
               where table_name = v_table_name )
   where high_value between v_low_value and v_high_value
   order by length(subpartition_name), subpartition_name;
  */


  open cur_tabs;
  fetch cur_tabs bulk collect into tab_pras_tabs;
  close cur_tabs;
  for m in 1..tab_pras_tabs.count
  loop

    rec_tab := tab_pras_tabs(m);
    -- 创建新文件
    v_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name||rec_tab.table_name||'_'||v_add_year||'.sql', 'w', 32765);

    utl_file.put_line(v_output_file, '');
    v_table_name := upper(rec_tab.table_name);

    --获取索引名
    open cur_index_partition(v_table_name);
    fetch cur_index_partition bulk collect into  tab_indexes;
    close cur_index_partition;

    --一级月份分区
    for i in 1..12
    loop

     --二级地区分区
      for z in 1..64
      loop
        v_partition_name := 'T'||z;
        v_tablespace_name := 'DAT_'||z;
        if v_table_name = 'HDM_S_PFMTMDTL_QUERY_DAT' or v_table_name = 'HDM_S_PFMTMDT2_QUERY_DAT' then
          v_tablespace_name := 'TM_DAT_'||z;
        end if;

        v_subpartition_name := v_partition_name||'_'||'P'||v_add_year||LPAD(i,2,'0')||'32';
        v_high_value := ''''||v_add_year||'-'||LPAD(i,2,'0')||'-'||'32'||'''';

        v_step := '5';
        v_sql_statement := 'alter table '||v_table_name||' modify partition '||v_partition_name||' add subpartition '||v_subpartition_name||' values less than ('||v_high_value||') tablespace '|| v_tablespace_name ||';';
        utl_file.put_line(v_output_file, v_sql_statement);
        utl_file.fflush(v_output_file);

        for k in 1..tab_indexes.count
        loop
          rec_index_partition := tab_indexes(k);
          v_step := '6';
          v_index_name := rec_index_partition.index_name;
          v_tablespace_name :=  'IND_'||z;
          if v_table_name = 'HDM_S_PFMTMDTL_QUERY_DAT' or v_table_name = 'HDM_S_PFMTMDT2_QUERY_DAT' then
            v_tablespace_name := 'TM_IND_'||z;
          end if;

          v_sql_statement := 'alter index '|| v_index_name || ' rebuild subpartition '||v_subpartition_name||' tablespace '||v_tablespace_name||';';
          utl_file.put_line(v_output_file, v_sql_statement);
          utl_file.fflush(v_output_file);
        end loop;
      end loop;
    end loop;
    --增加分区语句，输出到文件
    utl_file.fclose(v_output_file);
  end loop;


  p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_SUBPARTITIONS_ADD;

/******************************************************************************
 --存储过程名称： PCKG_GEN_YEAR_SUBPARTITIONS
 --作者： kwg
 --时间： 2015年06月30日
 --版本号:
 --使用源表名称：
 --使用目标表名称：
 --参数说明：
 --功能： 模版程序1
 ******************************************************************************/
 PROCEDURE PROC_SUBPARTITIONS_DROP(
        p_script_name IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(30) := null;
  v_tmp_table_name varchar2(30) := null;
  v_subpartition_name varchar2(30) := null;
  v_check_name        varchar2(50) := null;
  v_partition_name    varchar2(30) := null;
  v_tablespace_name   varchar2(30) := null;
  v_index_name        varchar2(50) := null;
  v_del_year          varchar2(10) := null;
  v_add_year          varchar2(10) := null;
  v_low_value          varchar2(50) := null;
  v_high_value          varchar2(50) := null;
  v_sql_statement     varchar2(32767) := null;

  TYPE chars_tab is table of varchar2(32767);
  drop_sql_lines      chars_tab         := chars_tab();
  i_d            int               := 0;

  --所有需要增删分区的表，需要删除的年份，需要增加的年份
  cursor cur_tabs
  is
   select table_name, del_year, add_year
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;
  type typ_tabs is table of cur_tabs%rowtype;
  tab_pras_tabs typ_tabs;

  --删除年份的所有二级子分区，子分区范围的最值
  cursor cur_subpartition(p_table_name varchar2, p_low_value varchar2, p_high_value varchar2)
  is
    select partition_name,subpartition_name,tablespace_name, high_value
        from ( select partition_name,subpartition_name,tablespace_name, long_help.substr_of('select high_value
                                              from user_tab_subpartitions
                                             where table_name  = :n
                                              and  subpartition_name = :p',
                                            1, 4000,
                                            'n', table_name,
                                            'p', subpartition_name ) high_value
                 from user_tab_subpartitions
                 where table_name = p_table_name )
     where high_value between p_low_value and p_high_value
     order by length(subpartition_name), subpartition_name;
  rec_subpartition   cur_subpartition%rowtype := null;

  v_output_file    utl_file.file_type;

    type collection_names is table of user_tab_subpartitions.subpartition_name%type;
    col_check_name  collection_names := null;
BEGIN
   p_o_succeed := '0';
   v_step := '1';

   --生成表的增删分区操作脚本
   v_step := '2';
   open cur_tabs;
   fetch cur_tabs bulk collect into tab_pras_tabs;
   close cur_tabs;
   for j in 1..tab_pras_tabs.count
   loop

    rec_tab := tab_pras_tabs(j);

    v_step := '3';
    --删除的子分区范围
    v_del_year := rec_tab.del_year;
    v_add_year := rec_tab.add_year;
    v_low_value  := ''''||rec_tab.del_year||'-01-32''';
    v_high_value := ''''||rec_tab.del_year||'-12-32''';
    v_table_name := upper(rec_tab.table_name);
    --打开文件
    v_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name||v_table_name||'_'||v_del_year||'.sql', 'w', 32765);
    utl_file.put_line(v_output_file, '');

    for m in 1..64
    loop
      for n in 1..12
      loop
        v_subpartition_name := 'T'||m||'_P'||v_del_year||LPAD(n,2,'0')||'32';
        --找出表中需要删除的子分区
        v_tmp_table_name := substr(v_table_name,1,length(v_table_name)-4)||'$'|| v_del_year;


        --检查需要增加的分区是否存在，当子分区不存在时，增加分区。
        /*
        select subpartition_name
          bulk collect into col_check_name
        from user_tab_subpartitions
        where subpartition_name = v_subpartition_name
        and table_name = v_table_name
          and rownum = 1;
        --如果删除分区在中途失败后，可以支持重跑。故现检查分区是否存在
        if col_check_name.count = 0 then
          continue;
        end if;
        */

        v_step := '5';

        --删除的分区
        v_sql_statement := 'create table '||v_tmp_table_name||' as select * from '||v_table_name||' where 1=2;';
        utl_file.put_line(v_output_file, v_sql_statement);
        v_sql_statement := 'alter table '||v_table_name||' exchange subpartition '||v_subpartition_name||' with table '||v_tmp_table_name||';';
        utl_file.put_line(v_output_file, v_sql_statement);
        --v_sql_statement := 'drop table '||v_tmp_table_name||' purge;';
        v_sql_statement := 'alter table '||v_tmp_table_name||' rename to '|| substr(v_table_name,1,length(v_table_name)-4) || m || '_' || n||';';
        utl_file.put_line(v_output_file, v_sql_statement);
        v_sql_statement := 'alter table '||v_table_name||' drop subpartition '||v_subpartition_name||';';
        utl_file.put_line(v_output_file, v_sql_statement);
        utl_file.fflush(v_output_file);

      end loop;
     end loop;
   end loop;


   --删除分区语句，输出到文件
   utl_file.fclose(v_output_file);
   p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_SUBPARTITIONS_DROP;

/******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年07月16日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成视图脚本程序
   --     从表HDM_VIEW_LIST取得视图名和包含的表名，生成建视图脚本。
   --
     ******************************************************************************/
PROCEDURE PROC_GEN_VIEW_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数)
    con_dat_max_date IN VARCHAR2, --(传入参数)
    con_month_min_date IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(50) := null;
  v_view_name    varchar2(50) := null;
  v_dat_table_name varchar2(50) := null;
  v_month_table_name varchar2(50) := null;
  v_del_year          varchar2(10) := null;
  i_curr_year         int          := 0;
  i_last_year         int          := 0;
  v_add_year          varchar2(10) := null;
  v_sql_statement     varchar2(32767) := null;
  v_tmp_sql           varchar2(32767) := null;
  i_num               int             := 1;
  con_dat_min_date      varchar2(5)    := '01-01';
  --con_dat_max_date      varchar2(5)    := '11-30';
  --con_month_min_date      varchar2(5)    := '12-01';
  con_month_max_date      varchar2(5)    := '12-31';
  v_dat_min_date      varchar2(20)    := null;
  v_dat_max_date      varchar2(20)    := null;
  v_month_min_date      varchar2(20)    := null;
  v_month_max_date      varchar2(20)    := null;
  v_subpart_column      varchar2(50)    := null;
  v_exist               int;
  --所有需要修改视图的表，
  cursor cur_tabs
  is
   select table_name, del_year, add_year, view_name
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;

  v_output_file    utl_file.file_type;

BEGIN
   p_o_succeed := '0';
   v_step := '1';
   --打开文件
   v_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name, 'w', 32765);

   --生成视图的基准表清单


   v_step := '2';
   open cur_tabs;
   loop
    fetch cur_tabs into rec_tab;
    exit when cur_tabs%notfound;

    v_step := '3';
    --
    v_del_year := rec_tab.del_year;
    i_last_year := to_number(v_del_year) + 1;
    i_curr_year := to_number(v_del_year) + 2;
    v_add_year := rec_tab.add_year;
    v_dat_table_name := upper(rec_tab.table_name); --视图的基准表
    v_view_name := upper(rec_tab.view_name);
    v_month_table_name := v_view_name || '_MONTH'; --月表名

    select listagg(column_name,',') within group (order by column_id)
       into v_tmp_sql
         from user_tab_columns
      where table_name = v_dat_table_name;

    select column_name
      into v_subpart_column
    from user_subpart_key_columns
      where name = v_dat_table_name;

    --建视图
  --年表时间范围
    v_dat_min_date   := i_last_year || '-' || con_dat_min_date;
    v_dat_max_date   := i_curr_year || '-' || con_dat_max_date;
    --月表时间范围
    v_month_min_date := i_curr_year || '-' || con_month_min_date;
    v_month_max_date := i_curr_year || '-' || con_month_max_date;
    utl_file.put_line(v_output_file, '');


    v_sql_statement := 'create or replace view ' ||
                       v_view_name               ||
             CHR(10)                   ||
             ' ( '                     ||
             CHR(10)                   ||
             v_tmp_sql                 ||
             CHR(10)                   ||
             ' ) as select '           ||
             CHR(10)                   ||
             v_tmp_sql                 ||
             CHR(10)                   ||
             ' from '                  ||
             v_dat_table_name          ||
             CHR(10)                   ||
             ' where '                 ||
             v_subpart_column          ||
             ' >= '''                  ||
             v_dat_min_date            ||
                         ''' and '           ||
             v_subpart_column          ||
             ' <= ';
    utl_file.put(v_output_file, v_sql_statement);


    v_step := '4';
    -- 如果月表存在的话
    select count(*)
    INTO v_exist
    from user_objects
    where object_name = v_month_table_name
    and rownum = 1;

    if v_exist > 0 then
      v_sql_statement := ''''|| v_dat_max_date||'''';
            utl_file.put(v_output_file, v_sql_statement);
       v_sql_statement := ' union all ';
      utl_file.put_line(v_output_file, v_sql_statement);

      --月表的列与视图的列对比,并拼接成视图列
      select listagg(column_name, ',') within group (order by column_id)
      into v_tmp_sql
      from (
       select a.column_id, case when b.column_name is null then 'null as ' || a.column_name
                 else  a.column_name
                 end column_name
       from ( select column_name,column_id from user_tab_columns where table_name = v_dat_table_name order by column_id) a,
         ( select column_name  from user_tab_columns where table_name = v_month_table_name order by column_id) b
       where a.column_name = b.column_name(+));
      --拼接月表的视图语句
      v_sql_statement :=   ' select '                ||
                 CHR(10)                   ||
                 v_tmp_sql                 ||
                 CHR(10)                   ||
                 ' from '                  ||
                 v_month_table_name        ||
                 CHR(10)                   ||
                 ' where '                 ||
                 v_subpart_column          ||
                 ' >= '''                  ||
                 v_month_min_date          ||
                 ''' and '           ||
                 v_subpart_column          ||
                 ' <= '''                  ||
                 v_month_max_date          ||
                 '''';
      utl_file.put_line(v_output_file, v_sql_statement);
    else
      v_sql_statement := ''''|| v_month_max_date||'''';
            utl_file.put_line(v_output_file, v_sql_statement);
    end if;

    v_sql_statement :=   ';';
    utl_file.put_line(v_output_file, v_sql_statement);

      --输出到文件
    utl_file.fflush(v_output_file);
   end loop;
   close cur_tabs;

   utl_file.fclose(v_output_file);
   p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_GEN_VIEW_SCRIPT;

END PCKG_GEN_YEAR_SUBPARTITIONS;
//

CREATE OR REPLACE PACKAGE PCKG_GEN_STARTING_SCRIPT
IS
    /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成改表名脚本。
   --
     ******************************************************************************/
    PROCEDURE PROC_GEN_RENAME_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表VIEW_LIST取得视图名和包含的表名，生成建视图脚本。
   --
     ******************************************************************************/
  PROCEDURE PROC_GEN_VIEW_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

  /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，建表脚本。
   --
     ******************************************************************************/
  PROCEDURE PROC_GEN_TABLE_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );


END PCKG_GEN_STARTING_SCRIPT;

//

CREATE OR REPLACE PACKAGE BODY PCKG_GEN_STARTING_SCRIPT
IS
 /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，生成改表名脚本。
   --
     ******************************************************************************/
 PROCEDURE PROC_GEN_RENAME_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(30) := null;
  v_tmp_table_name varchar2(30) := null;
  v_index_name        varchar2(50) := null;
  v_tmp_index_name        varchar2(50) := null;
  v_del_year          varchar2(10) := null;
  v_add_year          varchar2(10) := null;
  v_sql_statement     varchar2(32767) := null;
  i_occurrence        int;
  v_table_suffix      varchar2(20);
  v_view_name         varchar2(50);
  --所有需要增删分区的表，需要删除的年份，需要增加的年份
  cursor cur_tabs
  is
   select table_name, del_year, add_year, view_name
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;


    --一级分区所有的索引，索引所在的表空间
  cursor cur_indexs ( p_table_name varchar2 )
  is
  select i.index_name
       from user_indexes i
    where i.table_name =  p_table_name;

  v_output_file    utl_file.file_type;

BEGIN
   p_o_succeed := '0';
   v_step := '1';
   --打开文件
   v_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name, 'w', 32765);

   --生成表改名脚本
   v_step := '2';
   open cur_tabs;
   loop
    fetch cur_tabs into rec_tab;
    exit when cur_tabs%notfound;
    utl_file.put_line(v_output_file, '');

    v_step := '3';
    --老化年份
    v_del_year := rec_tab.del_year;
    v_add_year := rec_tab.add_year;
    v_table_name := rec_tab.table_name;
    v_view_name := rec_tab.view_name;
    --找出表名后缀
    i_occurrence := regexp_count(v_table_name, '_');
    v_table_suffix := regexp_substr(v_table_name, '[[:alnum:]]+', 1, i_occurrence+1, 'i');
    --替换表名后缀,替换为老化的年份
    v_tmp_table_name := regexp_replace(v_table_name, '_'||v_table_suffix, '_'||v_del_year, 1, 1);

    --重命名索引
    open cur_indexs(v_table_name);
    loop
       v_step := '4';
     fetch cur_indexs into v_index_name;
     exit when cur_indexs%notfound;
      --替换索引名,替换为老化的年份
      v_tmp_index_name := regexp_replace(v_index_name, '_'||v_table_suffix, '_'||v_del_year, 1, 1);
      v_sql_statement := 'alter index  '||v_index_name ||' rename to '||v_tmp_index_name||';';
      utl_file.put_line(v_output_file, v_sql_statement);
    end loop;
    close cur_indexs;

    --重命名表名
    v_sql_statement := 'alter table '||v_table_name ||' rename to '||v_tmp_table_name||';';
    utl_file.put_line(v_output_file, v_sql_statement);

    --建同义词
    v_sql_statement := 'CREATE OR REPLACE PUBLIC SYNONYM '||v_tmp_table_name ||' FOR HDM.'||v_tmp_table_name||';';
    utl_file.put_line(v_output_file, v_sql_statement);

    --新表名加入视图清单

    --insert into HDM_VIEW_LIST (view_name, table_name, file_name, data_year)
      --      values ('HDM_S_PFEPSADL_QUERY', 'HDM_S_PFEPSADL_QUERY_1', 'PFEPSADL', '2011');
    --个金镜外，hds上名字为PFEPABDL，而超期上为PFQABDTL。超期上的视图名保持为HDM_S_PFQABDTL_QUERY
    if instr(v_view_name, 'PFEPABDL') > 0 then
      v_view_name := replace(v_view_name, 'PFEPABDL', 'PFQABDTL');
    end if;

    v_sql_statement := 'insert into HDM_VIEW_LIST (view_name, table_name, file_name, data_year) ' ||
                       'values(''' ||
             v_view_name ||
             ''','''     ||
             v_tmp_table_name||
             ''','''     ||
             regexp_substr(v_view_name, '[[:alnum:]]+', 1, 3, 'i')||
             ''','''     ||
             v_del_year  ||
             ''');';
    utl_file.put_line(v_output_file, v_sql_statement);
    v_sql_statement := 'commit;';
    utl_file.put_line(v_output_file, v_sql_statement);
    --输出到文件
    utl_file.fflush(v_output_file);
   end loop;
   close cur_tabs;

   utl_file.fclose(v_output_file);
   p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_GEN_RENAME_SCRIPT;

/******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDM_VIEW_LIST取得视图名和包含的表名，生成建视图脚本。
   --
     ******************************************************************************/
PROCEDURE PROC_GEN_VIEW_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(50) := null;
  v_dat_table_name varchar2(50) := null;
  v_view_name varchar2(50) := null;
  v_del_year          varchar2(10) := null;
  v_add_year          varchar2(10) := null;
  v_sql_statement     varchar2(32767) := null;
  v_tmp_sql           varchar2(32767) := null;
  i_num               int             := 1;

  --所有需要修改视图的表，
  cursor cur_tabs
  is
   select table_name, del_year, add_year, view_name
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;

  --视图包含的表清单
  cursor cur_views (p_view_name varchar2)
  is
    select distinct table_name, view_name
    from hdm_view_list
  where view_name = p_view_name;
  rec_view  cur_views%rowtype   := null;

  v_output_file    utl_file.file_type;

BEGIN
   p_o_succeed := '0';
   v_step := '1';
   --打开文件
   v_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name, 'w', 32765);

   --生成视图的基准表清单
   v_step := '2';
   open cur_tabs;
   loop
    fetch cur_tabs into rec_tab;
    exit when cur_tabs%notfound;

    v_step := '3';
    --
    v_del_year := rec_tab.del_year;
    v_add_year := rec_tab.add_year;
    /* 活期境外明细PFEPABDL在hdmdata7上的视图名为PFQABDTL */
    v_view_name := replace(rec_tab.view_name, 'PFEPABDL', 'PFQABDTL');

    --视图的基准表，最新的表
     select table_name into v_dat_table_name
    from hdm_view_list
    where data_year = ( select max(data_year) from hdm_view_list where view_name = v_view_name)
    and view_name = v_view_name;



    select listagg(column_name,',') within group (order by column_id)
       into v_tmp_sql
         from user_tab_columns
      where table_name = v_dat_table_name;

    --建视图
    utl_file.put_line(v_output_file, '');
    v_sql_statement := 'create or replace view '||v_view_name ||' ( '||v_tmp_sql||' ) as ';
    utl_file.put_line(v_output_file, v_sql_statement);

    i_num := 1;
    --视图包含的所有表
    open cur_views(v_view_name);
    loop
        v_step := '4';
      fetch cur_views into rec_view;
        exit when cur_views%notfound;
      v_table_name   := rec_view.table_name;

       --表的列与视图的列对比,并拼接成视图列
       v_step := '5';
       select listagg(column_name, ',') within group (order by column_id)
           into v_tmp_sql
      from (
           select a.column_id, case when b.column_name is null then 'null as ' || a.column_name
                     when a.data_type <> b.data_type and a.data_type = 'NUMBER' then 'to_number(' || a.column_name || ')'
                     when  a.data_type <> b.data_type and a.data_type = 'VARCHAR2' then 'to_char(' || a.column_name || ')'
                     else a.column_name
                     end column_name
           from ( select column_name,column_id,data_type from user_tab_columns where table_name = v_dat_table_name order by column_id) a,
             ( select column_name,data_type  from user_tab_columns where table_name = v_table_name order by column_id) b
           where a.column_name = b.column_name(+));
      --拼接表的视图语句
      if i_num <> 1 then
        v_sql_statement := ' union all ';
        utl_file.put_line(v_output_file, v_sql_statement);
      end if;
      v_sql_statement := ' select '|| v_tmp_sql ||' from ' || v_table_name;
      utl_file.put_line(v_output_file, v_sql_statement);
      --输出到文件
      utl_file.fflush(v_output_file);
      i_num := i_num + 1;
    end loop;
     utl_file.put_line(v_output_file, ';');
    --视图没有对应的表
    if cur_views%rowcount = 0 then
      raise_application_error(-20999, 'hdm_view_list表中没有视图'||v_view_name||'对应的表');
    end if;

    close cur_views;

   end loop;
   close cur_tabs;

   utl_file.fclose(v_output_file);
   p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_GEN_VIEW_SCRIPT;

/******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能：年初处理,生成脚本程序
   --     从表HDS_PRAS_YEAR_TABS取得表名和对应的年份，建表脚本。
   --
     ******************************************************************************/

PROCEDURE PROC_GEN_TABLE_SCRIPT(
        p_script_name IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(30) := null;
  v_del_year          varchar2(10) := null;
  v_add_year          varchar2(10) := null;
  v_sql_statement     varchar2(32767) := null;
  c_table_text clob;
  v_sql varchar2(32765);
  i_start_position int := 1;
  i_offset int := 1;
  i_len    int;
  i_reads int := 32765;
  v_pattern1  varchar2(50);
  v_pattern2  varchar2(50);
  v_replace1  varchar2(50);
  v_replace2  varchar2(50);

  --所有需要新建的表，需要删除的年份
  cursor cur_tabs
  is
   select table_name, del_year, add_year
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;
  v_output_file    utl_file.file_type;
  f_output_file    utl_file.file_type;

BEGIN
   p_o_succeed := '0';
   v_step := '1';

   --打开文件
   f_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name, 'w', 32765);

   --生成表脚本
   v_step := '2';
   open cur_tabs;
   loop
    fetch cur_tabs into rec_tab;
    exit when cur_tabs%notfound;
    v_step := '3';
    --
    i_offset := 1;
     utl_file.put_line(f_output_file, '');

    v_del_year := rec_tab.del_year;
    --子分区名与范围都与年份有关
    v_pattern1 := '_P'||v_del_year;
    v_pattern2 := ''''||v_del_year||'-';
    v_replace1 := '_P'|| (TO_NUMBER(v_del_year)+1);
    v_replace2 := ''''|| (TO_NUMBER(v_del_year)+1)||'-';
    v_table_name := rec_tab.table_name;

    --单个表的建表脚本
    v_output_file := utl_file.fopen('GEN_SCRIPT', 'tmp_table_'||v_table_name||'.sql', 'w', 32765);
    --获取脚本语句
    select dbms_metadata.get_ddl('TABLE',v_table_name) into c_table_text from dual;
    i_len := DBMS_LOB.GETLENGTH(c_table_text);

    --建表语句写入临时文件
    while i_offset <= i_len
    loop
       DBMS_LOB.READ(c_table_text, i_reads, i_offset, v_sql);
       v_sql := replace(v_sql, '"');
       utl_file.put_line(v_output_file, v_sql);
         utl_file.fflush(v_output_file);
        i_offset := i_offset + i_reads;
    end loop;
    utl_file.fclose(v_output_file);

    --建表脚本进行子分区名和分区范围替换,最终写入建表脚本中
    v_sql := null;
    v_output_file := utl_file.fopen('GEN_SCRIPT', 'tmp_table_'||v_table_name||'.sql', 'r');
    utl_file.get_line(v_output_file, v_sql);

    --读单表语句,并进行子分区替换
    loop
       begin
      v_sql := replace(v_sql, v_pattern1, v_replace1);
      v_sql := replace(v_sql, v_pattern2, v_replace2);
      --写入
      utl_file.put_line(f_output_file, v_sql);
      utl_file.fflush(f_output_file);
      --读取
      utl_file.get_line(v_output_file, v_sql);
     exception
          --读到文件尾部
        when no_data_found then
         exit;
         end;
    end loop;
    utl_file.fclose(v_output_file);
    utl_file.put_line(f_output_file, ';');

   end loop;
   close cur_tabs;

   utl_file.fclose(f_output_file);
   p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_GEN_TABLE_SCRIPT;

END PCKG_GEN_STARTING_SCRIPT;
//

CREATE OR REPLACE PACKAGE PCKG_GEN_DUMP_LIST
IS
    /******************************************************************************
     --存储过程名称：
     --作者： kwg
     --时间： 2015年06月30日
     --版本号:
     --使用源表名称：
     --使用目标表名称：
     --参数说明：)
     --功能： 从表HDS_PRAS_YEAR_TABS取得表名和dump包导出的日期，dump包导出清单。
   --      当前批量导出上一年同月的数据，子分区由日期的天进行推算。数据年表64个一级地区分区，12个二级月分区。
   --      64个地区一个月内导出，一天导出3个地区。
   --      每月1日需要重建索引，1日不导出，20日是计息日文件加载，也不导出。
     ******************************************************************************/
    PROCEDURE PROC_GEN_DUMP_LIST(
        p_script_name IN VARCHAR2, --(传入参数,查询批次)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
    );

END PCKG_GEN_DUMP_LIST;

//

CREATE OR REPLACE PACKAGE BODY PCKG_GEN_DUMP_LIST
IS
 /******************************************************************************
 --存储过程名称： PROC_GEN_DUMP_LIST
 --作者： kwg
 --时间： 2015年06月30日
 --版本号:
 --使用源表名称：
 --使用目标表名称：
 --参数说明：
 --功能： 模版程序1
 ******************************************************************************/
 PROCEDURE PROC_GEN_DUMP_LIST(
        p_script_name IN VARCHAR2, --(传入参数)
        p_o_succeed OUT VARCHAR2 --(传出参数,成功返回0,失败返回1)
 )
 IS
  v_step  varchar2(5) := null;
  v_table_name   varchar2(30) := null;
  v_tmp_table_name varchar2(30) := null;
  v_subpartition_name varchar2(30) := null;
  v_check_name        varchar2(50) := null;
  v_partition_name    varchar2(30) := null;
  v_index_name        varchar2(50) := null;
  v_del_year          varchar2(10) := null;
  v_dmp_date          varchar2(10) := null;
  v_add_year          varchar2(10) := null;
  i_nodump_day1 int  := 1; --建索引日
  i_nodump_day2 int  := 20; --计息日
  i_max_zone_num  int := 64;
  i_day           int := 0;
  i_mutilple      int := 3;
  i_tmp           int := 0;
  v_subpartition_part varchar2(50) := null;
  v_partition_part varchar2(50) := null;
  v_output_file utl_file.file_type := null;
  v_sql_statement     varchar2(32767) := null;

  --所有需要导出dump包的表
  cursor cur_tabs
  is
   select table_name, del_year, add_year,dmp_date
     from hds_pras_year_tabs;
  rec_tab  cur_tabs%rowtype       := null;


BEGIN
   p_o_succeed := '0';
   v_step := '1';
   --打开文件
   v_output_file := utl_file.fopen('GEN_SCRIPT', p_script_name, 'w', 32765);

   --生成表的增删分区操作脚本
   v_step := '2';
   open cur_tabs;
   loop
    fetch cur_tabs into rec_tab;
    exit when cur_tabs%notfound;

    v_step := '3';
    --删除的子分区范围
    v_table_name := rec_tab.table_name;
    v_dmp_date := rec_tab.dmp_date; --20140630
    v_del_year := rec_tab.del_year; --2014
    v_add_year := rec_tab.add_year; --2015
    v_subpartition_part := 'P'||substr(v_dmp_date, 1, 6)||'32';
    i_day := to_number(substr(v_dmp_date, 7, 2)); --当日天数

    --根据日计算导出的子分区
    v_sql_statement := null;
    dbms_output.put_line('i_day='||i_day);
    dbms_output.put_line('i_nodump_day1='||i_nodump_day1);
    dbms_output.put_line('i_nodump_day2='||i_nodump_day2);
    if i_day > i_nodump_day1 and i_day < i_nodump_day2 then --20日前子分区获取

      for i in 1 .. i_mutilple
      loop
         i_tmp := (i_day - 2) * i_mutilple + i; --从2日开始，每天导出3个子分区
         if i_tmp <= i_max_zone_num then
                if v_sql_statement is not null then
               v_sql_statement := v_sql_statement||','; --分区间用','分开
            end if;

             v_partition_part := 'T'||i_tmp;
             v_sql_statement := v_sql_statement||v_table_name||':'||v_partition_part||'_'||v_subpartition_part; --HDM_S_PFEPSADL_QUERY_DAT:T1_P20140132
         end if;

      end loop;
    elsif i_day > i_nodump_day2 then --20日后子分区获取
      for i in 1 .. i_mutilple
      loop
         i_tmp := (i_day - 3) * i_mutilple + i; --20日没导出，往前推一天，每天导出3个子分区

         if i_tmp <= i_max_zone_num then
            if v_sql_statement is not null then
                v_sql_statement := v_sql_statement||','; --分区间用','分开
              end if;

             v_partition_part := 'T'||i_tmp;
             v_sql_statement := v_sql_statement||v_table_name||':'||v_partition_part||'_'||v_subpartition_part; --HDM_S_PFEPSADL_QUERY_DAT:T1_P20140132
         end if;
      end loop;
    elsif i_day is null then -- 一次导出一个月的
      for i in 1 .. i_max_zone_num
      loop
        v_partition_part := 'T'||i; -- 一个月的所有子分区
        if v_sql_statement is not null then
           v_sql_statement := v_sql_statement||','; --分区间用','分开
        end if;
         v_sql_statement := v_sql_statement||v_table_name||':'||v_partition_part||'_'||v_subpartition_part; --HDM_S_PFEPSADL_QUERY_DAT:T1_P20140132
      end loop;
    end if;
    --输出表需要导出的所有分区
    if v_sql_statement is not null then
      utl_file.put_line(v_output_file,v_sql_statement);
      utl_file.fflush(v_output_file);
    end if;
   end loop;
   close cur_tabs;

   utl_file.fclose(v_output_file);
   p_o_succeed := '1';

EXCEPTION
   WHEN OTHERS
   THEN
    dbms_output.put_line('出错步骤：' || v_step);
      RAISE;
END PROC_GEN_DUMP_LIST;
END PCKG_GEN_DUMP_LIST;
//
delimiter ;//