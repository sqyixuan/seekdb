
create table CPMG.CAP_RAROC_LIMMIT
(
  type_id     VARCHAR2(1 CHAR) not null,
  raroc_ceil  NUMBER(7,4),
  raroc_floor NUMBER(7,4),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
);

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
);

create table CPMG.DIC_ALL_KIND
(
  operation_kind VARCHAR2(30 CHAR) not null,
  kind_id        VARCHAR2(30 CHAR) not null,
  kind_name      VARCHAR2(200 CHAR),
  remark         VARCHAR2(200 CHAR)
);

create table CPMG.DM_CS_CZFX
(
  zoneno   VARCHAR2(5 CHAR) not null,
  brno     VARCHAR2(5 CHAR) not null,
  bankflag CHAR(1) not null,
  currtype VARCHAR2(3 CHAR) not null,
  prodcode VARCHAR2(10 CHAR) not null,
  ec       NUMBER(26,2)
);

create table CPMG.DM_CS_PRODUCT
(
  subject_code VARCHAR2(8 CHAR) not null,
  product_id   VARCHAR2(10 CHAR) not null,
  product_name VARCHAR2(100 CHAR) not null,
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
);

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
);

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
);

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
);

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
);

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
);

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
);

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
);

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
);

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
);

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
);

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
);

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
);

create table CPMG.DW_CAP_GRKH_SUM
(
  data_date VARCHAR2(8 CHAR),
  cisno     VARCHAR2(30 CHAR),
  cap_now   NUMBER(22,2),
  cap_12mon NUMBER(22,2)
);

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
);

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
);
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
);

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
);

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
);

create table CPMG.DW_MOD_PVMSDATA_11
(
  zoneno    VARCHAR2(5 CHAR),
  brno      VARCHAR2(5 CHAR),
  bankflag  VARCHAR2(1 CHAR),
  currtype  VARCHAR2(3 CHAR),
  ecost     NUMBER(28),
  data_date VARCHAR2(8),
  diccode   VARCHAR2(3 CHAR)
);

create table CPMG.L_GETDATA_CALL
(
  proc_name  VARCHAR2(100 CHAR),
  log_level  VARCHAR2(5 CHAR),
  begin_time VARCHAR2(30 CHAR),
  end_time   VARCHAR2(30 CHAR),
  deal_flag  VARCHAR2(10 CHAR)
);

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
);

create table CPMG.PRM_CS032
(
  dbfs_id    VARCHAR2(5 CHAR) not null,
  dbfs_lb    VARCHAR2(50 CHAR),
  load_type  VARCHAR2(5 CHAR) not null,
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

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
);

create table CPMG.PRM_CS05A
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_type VARCHAR2(1 CHAR),
  subject_app  NUMBER(2),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
);

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
);

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
);

create table CPMG.PRM_CS081
(
  product_id   VARCHAR2(10 CHAR) not null,
  subject_code VARCHAR2(8 CHAR) not null,
  subject_name VARCHAR2(50 CHAR),
  subject_app  NUMBER(2),
  subject_type VARCHAR2(1 CHAR),
  start_date   VARCHAR2(8) not null,
  end_date     VARCHAR2(8 CHAR)
);

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
);

create table CPMG.PRM_CSFR5
(
  seg_seq    VARCHAR2(10 CHAR) not null,
  lower      NUMBER(9,6),
  is_default VARCHAR2(1 CHAR),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  seg_name   VARCHAR2(8 CHAR)
);

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
);

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
);

create table CPMG.PRM_GDQLTYF
(
  product_id VARCHAR2(10 CHAR) not null,
  value1     NUMBER(9,6),
  value2     NUMBER(9,6),
  value3     NUMBER(9,6),
  value4     NUMBER(9,6),
  value5     NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_GDTERMF
(
  product_id VARCHAR2(10 CHAR) not null,
  value1     NUMBER(9,6),
  value2     NUMBER(9,6),
  value3     NUMBER(9,6),
  value4     NUMBER(9,6),
  value5     NUMBER(9,6),
  value6     NUMBER(9,6),
  value7     NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_GDTERMP
(
  term_level_no VARCHAR2(2 CHAR) not null,
  left_kb       VARCHAR2(1 CHAR),
  left_val      NUMBER(9),
  right_val     NUMBER(9),
  right_kb      VARCHAR2(1 CHAR),
  start_date    VARCHAR2(8) not null,
  end_date      VARCHAR2(8 CHAR)
);

create table CPMG.PRM_HYDM
(
  hydm       VARCHAR2(7 CHAR) not null,
  hymc       VARCHAR2(100 CHAR) not null,
  hydl       VARCHAR2(50 CHAR) not null,
  hyflag     VARCHAR2(10 CHAR) not null,
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR) not null
);

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
);

create table CPMG.PRM_NPLOANF
(
  product_id VARCHAR2(10 CHAR) not null,
  coef_ceil  NUMBER(9,6),
  coef_floor NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_PRODUCT
(
  product_id    VARCHAR2(10 CHAR) not null,
  product_level VARCHAR2(1 CHAR),
  product_name  VARCHAR2(100 CHAR),
  parent_id     VARCHAR2(10 CHAR),
  start_date    VARCHAR2(8) not null,
  end_date      VARCHAR2(8 CHAR)
);

create table CPMG.PRM_SUBCODE_STATCODE
(
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  subcode    VARCHAR2(4 CHAR) not null,
  statcode   VARCHAR2(5 CHAR)
);

create table CPMG.PRM_XS032
(
  fl12_id    VARCHAR2(5 CHAR) not null,
  xs_value   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XS034
(
  lineno     NUMBER(8),
  field_code VARCHAR2(8 CHAR) not null,
  field_name VARCHAR2(100 CHAR),
  area_code  VARCHAR2(5 CHAR) not null,
  xs_value   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XS044
(
  product_id VARCHAR2(10 CHAR) not null,
  zone_no    VARCHAR2(5 CHAR) not null,
  pzxs       NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XS051
(
  product_name3 VARCHAR2(10 CHAR) not null,
  xs_value      NUMBER(9,6),
  start_date    VARCHAR2(8) not null,
  end_date      VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XS061
(
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR),
  product_id VARCHAR2(10 CHAR) not null,
  pzxs       NUMBER(9,6)
);

CREATE TABLE "CPMG"."EXP_CAP_ASA_A0001" (
	"ZONENO" VARCHAR2(5 CHAR),
	"BRNO" VARCHAR2(5 CHAR),
	"MODULE_ID" VARCHAR2(20 CHAR),
	"CNT" NUMBER(18,0),
	"DATADATE" VARCHAR2(6 CHAR)
);

create table CPMG.PRM_XS071
(
  product_id VARCHAR2(10 CHAR) not null,
  qxdc_id    VARCHAR2(10 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XS081
(
  thproduct  VARCHAR2(10 CHAR) not null,
  quotiety   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XS10
(
  thproduct  VARCHAR2(10 CHAR) not null,
  quotiety   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSFR1
(
  product_id  VARCHAR2(10 CHAR) not null,
  credit_code VARCHAR2(5 CHAR) not null,
  lgd_level   VARCHAR2(5 CHAR) not null,
  par_coef    NUMBER(9,6),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSFR2_TMP
(
  product_id   VARCHAR2(10 CHAR),
  subject_code VARCHAR2(200 CHAR) not null,
  ywpz_code    VARCHAR2(200 CHAR) not null,
  app_type     VARCHAR2(1 CHAR),
  par_coef     NUMBER(9,6)
);

create table CPMG.PRM_XSFR3
(
  qxdc_id     VARCHAR2(10 CHAR) not null,
  credit_code VARCHAR2(5 CHAR) not null,
  par_coef    NUMBER(9,6),
  start_date  VARCHAR2(8) not null,
  end_date    VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSFR6
(
  is_se      VARCHAR2(1 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSFRA
(
  product_id VARCHAR2(10 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSFRDX
(
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSXJ1
(
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

create table CPMG.PRM_XSZJ1
(
  product_id VARCHAR2(10 CHAR) not null,
  par_coef   NUMBER(9,6),
  start_date VARCHAR2(8) not null,
  end_date   VARCHAR2(8 CHAR)
);

CREATE TABLE "CPMG"."EXP_CAP_ASA_A0002" (
	"ZONENO" VARCHAR2(5 CHAR),
	"BRNO" VARCHAR2(5 CHAR),
	"USER_LEVEL" VARCHAR2(1 CHAR),
	"CNT" NUMBER(18,0),
	"CNT_TOTAL" NUMBER(18,0),
	"DATADATE" VARCHAR2(6 CHAR)
);
	
CREATE TABLE "CPMG"."EXP_CAP_ASA_A0003" (
	"MODULE_ID" VARCHAR2(20 CHAR),
	"MODULE_NAME" VARCHAR2(100 CHAR),
	"CNT" NUMBER(18,0),
	"DATADATE" VARCHAR2(6 CHAR)
);

delimiter //;
CREATE OR REPLACE PACKAGE Pack_Getdata_Log

IS

PROCEDURE PR_LOG_WRITE ( p_i_proc_name  IN  VARCHAR2 ,   --存储过程名称
                         p_i_log_level  IN  VARCHAR2 ,   --日志级别
                         p_i_begin_time IN  VARCHAR2 ,   --开始时间
                         p_i_end_time IN  VARCHAR2 ,   --结束时间
                         p_i_deal_flag  IN  VARCHAR2 ,   --存储过程处理结果
             p_i_step_no  IN  VARCHAR2 DEFAULT NULL ,  --步骤号
                         p_i_sql_txt  IN  VARCHAR2 DEFAULT NULL ,  --当前执行的SQL语句
             p_i_err_code IN  VARCHAR2 DEFAULT NULL ,  --错误代码
             p_i_err_txt  IN  VARCHAR2 DEFAULT NULL );
end Pack_Getdata_Log;
//

CREATE OR REPLACE PACKAGE PACK_WRITE_ALLBIN IS
  --分隔符
  PROCEDURE PROC_GENERAL_WRITE(IS_SQLSTR         IN VARCHAR2, --需要输出为文件的sql
                               IS_FILENAME       IN VARCHAR2, --需要导出的文件名
                               IS_DIR            IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                               IS_FIELDDELIMITER IN VARCHAR2, --分隔符
                               OS_FLAG           OUT VARCHAR2, --1:成功 0:失败
                               OS_MSG            OUT VARCHAR2);
   --分隔符 oa
  PROCEDURE PROC_GENERAL_WRITE_OA(IS_SQLSTR         IN VARCHAR2, --需要输出为文件的sql
                               IS_FILENAME       IN VARCHAR2, --需要导出的文件名
                               IS_DIR            IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                               IS_FIELDDELIMITER IN VARCHAR2, --分隔符
                               OS_FLAG           OUT VARCHAR2, --1:成功 0:失败
                               OS_MSG            OUT VARCHAR2);

  --定长 地区网点为第一二个字段
  PROCEDURE PROC_GENERAL_WRITE_FIXLENGTH(IS_SQLSTR   IN VARCHAR2, --需要输出为文件的sql
                                         IS_FILENAME IN VARCHAR2, --需要导出的文件名
                                         IS_DIR      IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                                         IS_OWNER_TBNAME IN VARCHAR2, --用户及表名用.分隔如 CPMG.ODS_ABC
                                         OS_FLAG OUT VARCHAR2, --0成功 1失败
                                         OS_MSG  OUT VARCHAR2);
  --定长 地区网点不为第一二个字段
  PROCEDURE PROC_GENERAL_WRITE_FL_NOR(IS_SQLSTR   IN VARCHAR2, --需要输出为文件的sql
                                         IS_FILENAME IN VARCHAR2, --需要导出的文件名
                                         IS_DIR      IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                                         IS_OWNER_TBNAME IN VARCHAR2, --用户及表名用.分隔如 CPMG.ODS_ABC
                                         OS_FLAG OUT VARCHAR2, --0成功 1失败
                                         OS_MSG  OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FRDK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_GRDK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_PJTX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_BWZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_ZQTZ(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_YHK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_ZJYW(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_WXZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_QTFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_CZFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_ORGAN(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_PRODUCT(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_DBFS(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_XYDJ(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_HYDM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_FRDKQX(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_GRDKQX(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_PJTXQX(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_DATACTL_CHAIFEN(P_I_DATE    IN VARCHAR2,
                                       P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_DATACTL_FENFA(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_ASA_A0001(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_ASA_A0002(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_ASA_A0003(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2);
END PACK_WRITE_ALLBIN;
//

create or replace package PACK_WRITE_BDP is

  PROCEDURE PROC_WRITE_FRDK_IN(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
end PACK_WRITE_BDP;
//

CREATE OR REPLACE package PACK_WRITE_EDI is

  PROCEDURE PROC_WRITE_EDI(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
end PACK_WRITE_EDI;
//

CREATE OR REPLACE PACKAGE PACK_WRITE_EDW IS

  PROCEDURE PROC_WRITE_GSIS_RPT_DATE(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2);
                                 
END PACK_WRITE_EDW;
//

CREATE OR REPLACE PACKAGE PACK_WRITE_GCMS IS

  PROCEDURE PROC_WRITE_PRO_SUB_REL(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_PRO_ZONE_XS(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_TERM_LEVEL(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_PRO_TERM_XS(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_ZLTJ_XS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_LIMMIT_XS(P_I_DATE    IN VARCHAR2,
                                 P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_BASE_XS(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_PRO_XS(P_I_DATE    IN VARCHAR2,
                                 P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_HY_ZONE_XS(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_TERM_LEVEL(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_TERM_XS(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_SMALLCOP_XS(P_I_DATE    IN VARCHAR2,
                                      P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FD_ZLXS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_SPEC_PRO_XS(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_DX_XS(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_LOANTYPE_DEF(P_I_DATE    IN VARCHAR2,
                                    P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_ZBCB_XS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_BW_XS(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FRDK_IN(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_FRDK_OUT(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_PERSON_CAPITAL(P_I_DATE    IN VARCHAR2,
                                      P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_DEPOSIT_XS(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2);
                                  
  PROCEDURE PROC_WRITE_RAROC_LIMMIT(P_I_DATE    IN VARCHAR2,
                                 P_O_SUCCEED OUT VARCHAR2); 
                                 
END PACK_WRITE_GCMS;
//

CREATE OR REPLACE package PACK_WRITE_PVDATA is

  procedure proc_write_pvmsdata(p_i_date    in varchar2,
                                p_o_succeed out varchar2);
  procedure proc_write_iraorgan(p_i_date    in varchar2,
                                p_o_succeed out varchar2);
  procedure proc_write_datactl(p_i_date    in varchar2,
                               p_o_succeed out varchar2);
  procedure proc_write_statcode(p_i_date    in varchar2,
                                p_o_succeed out varchar2);
end PACK_WRITE_PVDATA;
//

CREATE OR REPLACE PACKAGE PACK_WRITE_PVDATA_11 IS


  PROCEDURE PROC_WRITE_PVMSDATA(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_PRODUCTREF(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_DATACTL(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_PJZB(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WRITE_CKXS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_PRMPRODUCT(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_PRMCSPJ2(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2);

  PROCEDURE PROC_WRITE_PRMXS071(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2);
END PACK_WRITE_PVDATA_11;
//

CREATE OR REPLACE PACKAGE BODY Pack_Getdata_Log AS

PROCEDURE PR_LOG_WRITE ( p_i_proc_name  IN  VARCHAR2 ,   --存储过程名称
                         p_i_log_level  IN  VARCHAR2 ,   --日志级别
                         p_i_begin_time IN  VARCHAR2 ,   --开始时间
                         p_i_end_time IN  VARCHAR2 ,   --结束时间
                         p_i_deal_flag  IN  VARCHAR2 ,   --存储过程处理结果
             p_i_step_no  IN  VARCHAR2 DEFAULT NULL ,  --步骤号
                         p_i_sql_txt  IN  VARCHAR2 DEFAULT NULL ,  --当前执行的SQL语句
             p_i_err_code IN  VARCHAR2 DEFAULT NULL ,  --错误代码
             p_i_err_txt  IN  VARCHAR2 DEFAULT NULL )  --错误描述


IS
      --日志变量
        v_proc_name           L_GETDATA_CALL.PROC_NAME%TYPE  ;     --调用日志变量，存储过程名称
        v_log_level           L_GETDATA_CALL.LOG_LEVEL%TYPE  ;     --调用日志变量，程序日志级别
        v_begin_time          L_GETDATA_CALL.BEGIN_TIME%TYPE ;     --调用日志变量，程序开始时间
        v_end_time            L_GETDATA_CALL.END_TIME%TYPE   ;     --调用日志变量，程序结束时间
        v_deal_flag           L_GETDATA_CALL.DEAL_FLAG%TYPE  ;     --调用日志变量，程序处理结果

    v_step_no           L_GETDATA_ERR.STEP_NO%TYPE     ;     --错误日志变量，程序进行步骤
        v_sql_txt           L_GETDATA_ERR.SQL_TXT%TYPE     ;     --错误日志变量，程序执行SQL语句
        v_err_code            L_GETDATA_ERR.ERR_CODE%TYPE    ;     --错误日志变量，错误代码
        v_err_txt             L_GETDATA_ERR.ERR_TXT%TYPE     ;     --错误日志变量，错误描述



     v_sql         VARCHAR2(4000)  ;          --执行的SQL语句变量



BEGIN
    --判断日志级别
    IF SUBSTR(p_i_log_level,1,1) < '3' THEN
      --存储过程为数据处理，则每层存储过程均记录调度日志
      v_sql := ' INSERT INTO L_GETDATA_CALL VALUES  '
              || ' ( :1 , :2 , :3 , :4 , :5 ) ' ;
      EXECUTE IMMEDIATE v_sql
        USING p_i_proc_name , p_i_log_level ,
              p_i_begin_time , p_i_end_time , p_i_deal_flag ;
    END IF;

    IF UPPER(p_i_deal_flag) IN ('FAILED','ERROR') THEN
        --如果失败，则需要记录错误日志，对于调用日志和错误日志不需要区分级别
      v_sql := ' INSERT INTO L_GETDATA_ERR VALUES  '
              || ' ( :1 , :2 , :3 , :4 , :5 , :6 , :7 , :8 ,:9) ' ;
      EXECUTE IMMEDIATE v_sql
        USING p_i_proc_name , p_i_log_level ,
              p_i_step_no ,  SUBSTR(p_i_sql_txt,1,4000) , p_i_err_code ,
        p_i_err_txt ,p_i_begin_time , p_i_end_time,p_i_deal_flag ;
      END IF ;

    COMMIT ;


    EXCEPTION WHEN OTHERS THEN

       ROLLBACK ;
         v_err_code := SQLCODE ;
           v_err_txt  := SUBSTR ( SQLERRM,1,500) ;
       DBMS_OUTPUT.PUT_LINE ( v_sql || ' : ' ||v_err_txt ) ;

  END PR_LOG_WRITE ;

END Pack_Getdata_Log;
//


CREATE OR REPLACE PACKAGE BODY PACK_WRITE_ALLBIN IS

  PROCEDURE PROC_GENERAL_WRITE(IS_SQLSTR         IN VARCHAR2,
                               IS_FILENAME       IN VARCHAR2,
                               IS_DIR            IN VARCHAR2,
                               IS_FIELDDELIMITER IN VARCHAR2,
                               OS_FLAG           OUT VARCHAR2,
                               OS_MSG            OUT VARCHAR2) IS
    V_UTLFILE_FP UTL_FILE.FILE_TYPE;

    VI_DBMSCUR     PLS_INTEGER;
    VI_COLUMN_CNT  PLS_INTEGER;
    VI_FETCHNUMBER PLS_INTEGER;
    V_DESC_TAB     DBMS_SQL.DESC_TAB;
    V_VARCHAR_TAB  DBMS_SQL.VARCHAR2_TABLE;

    VI_ARRAY_BUFFER PLS_INTEGER := 5000;
    VS_RECSTR       VARCHAR2(10000);
    VS_FIELDSTR     VARCHAR2(2000);

  BEGIN
    OS_FLAG      := '1';
    OS_MSG       := '文件导出开始！';
    V_UTLFILE_FP := UTL_FILE.FOPEN(IS_DIR, IS_FILENAME, 'w', 8096);
    VI_DBMSCUR   := DBMS_SQL.OPEN_CURSOR; --游标打开
    DBMS_SQL.PARSE(VI_DBMSCUR, IS_SQLSTR, 0); --游标解析
    DBMS_SQL.DESCRIBE_COLUMNS(VI_DBMSCUR, VI_COLUMN_CNT, V_DESC_TAB); --获取游标对应所有字段信息
    VI_FETCHNUMBER := DBMS_SQL.EXECUTE(VI_DBMSCUR); --游标执行
    <<LOOP_EXPORT_FILES>>
    LOOP
      FOR I IN 1 .. VI_COLUMN_CNT LOOP

        DBMS_SQL.DEFINE_ARRAY(VI_DBMSCUR,
                              I,
                              V_VARCHAR_TAB,
                              VI_ARRAY_BUFFER,
                              VI_ARRAY_BUFFER * (I - 1) + 1);
      END LOOP;

      BEGIN
        VI_FETCHNUMBER := DBMS_SQL.FETCH_ROWS(VI_DBMSCUR);
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -1002 THEN
            VI_FETCHNUMBER := 0;
          ELSE
            RAISE;
          END IF;
      END;
      EXIT WHEN VI_FETCHNUMBER = 0;

      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        DBMS_SQL.COLUMN_VALUE(VI_DBMSCUR, I, V_VARCHAR_TAB);
      END LOOP;
      --循环缓存的数据行，读1行数写1行数到文件中
      <<WRITE_BUFFER_ROWS_LOOP>>
      FOR I_ROWINDEX IN 1 .. VI_FETCHNUMBER LOOP
        VS_RECSTR := '';
        FOR J_COLINDEX IN 1 .. VI_COLUMN_CNT LOOP
          IF (J_COLINDEX > 1) THEN
            VS_RECSTR := VS_RECSTR || IS_FIELDDELIMITER;
          END IF;
          VS_FIELDSTR := V_VARCHAR_TAB(VI_ARRAY_BUFFER * (J_COLINDEX - 1) +
                                       I_ROWINDEX);
          IF (VS_FIELDSTR IS NOT NULL) THEN

            VS_RECSTR := VS_RECSTR || VS_FIELDSTR;
          END IF;
        END LOOP;
        --写一行文件
        UTL_FILE.PUT(V_UTLFILE_FP, convert(VS_RECSTR, 'ZHS16GBK'));
        UTL_FILE.FFLUSH(V_UTLFILE_FP);
        --utl_file.new_line (fp);
        UTL_FILE.PUT(V_UTLFILE_FP, convert(chr(13)||CHR(10),'ZHS16GBK'));
      END LOOP WRITE_BUFFER_ROWS_LOOP;

    END LOOP LOOP_EXPORT_FILES;

    --关闭文件
    UTL_FILE.FCLOSE(V_UTLFILE_FP);
    --关闭游标
    IF DBMS_SQL.IS_OPEN(VI_DBMSCUR) THEN
      DBMS_SQL.CLOSE_CURSOR(VI_DBMSCUR);
    END IF;
    OS_MSG := '文件导出成功！';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OS_FLAG := '0';
      OS_MSG  := SUBSTR('文件导出失败，错误信息：' || SQLERRM, 1, 100);
  END PROC_GENERAL_WRITE;

   PROCEDURE PROC_GENERAL_WRITE_OA(IS_SQLSTR         IN VARCHAR2, --需要输出为文件的sql
                               IS_FILENAME       IN VARCHAR2, --需要导出的文件名
                               IS_DIR            IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                               IS_FIELDDELIMITER IN VARCHAR2, --分隔符
                               OS_FLAG           OUT VARCHAR2, --1:成功 0:失败
                               OS_MSG            OUT VARCHAR2) IS
    V_UTLFILE_FP UTL_FILE.FILE_TYPE;
    --动态游标变量begin
    VI_DBMSCUR     PLS_INTEGER; --DBMS包对应的游标
    VI_COLUMN_CNT  PLS_INTEGER; --记录查询有多少列
    VI_FETCHNUMBER PLS_INTEGER;
    V_DESC_TAB     DBMS_SQL.DESC_TAB; --数组，用来记录SELECT每个“字段”的数据类型、精度、范围、字段名称等信息
    V_VARCHAR_TAB  DBMS_SQL.VARCHAR2_TABLE; --数组用于存储游标读出的数据，类型为 TABLE OF VARCHAR2(2000) INDEX BY BINARY_INTEGER
    --动态游标变量end
    VI_ARRAY_BUFFER PLS_INTEGER := 5000; --每次缓存vi_array_buffer行数据
    VS_RECSTR       VARCHAR2(10000); --记录拼接出的每行的字符串
    VS_FIELDSTR     VARCHAR2(2000); --记录每个字段的字符串

  BEGIN
    OS_FLAG      := '1';
    OS_MSG       := '文件导出开始！';
    V_UTLFILE_FP := UTL_FILE.FOPEN(IS_DIR, IS_FILENAME, 'w', 8096);
    VI_DBMSCUR   := DBMS_SQL.OPEN_CURSOR; --游标打开
    DBMS_SQL.PARSE(VI_DBMSCUR, IS_SQLSTR, 0); --游标解析
    DBMS_SQL.DESCRIBE_COLUMNS(VI_DBMSCUR, VI_COLUMN_CNT, V_DESC_TAB); --获取游标对应所有字段信息
    VI_FETCHNUMBER := DBMS_SQL.EXECUTE(VI_DBMSCUR); --游标执行
    --开始循环读取数据并写文件
    <<LOOP_EXPORT_FILES>>
    LOOP
      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        /*每次缓存vi_array_buffer行数据，在这些数据当中，第i行,j列数据值在varray_col_varvalue
          数组中对应的下标是：vi_array_buffer*(j-1)+i
        */
        DBMS_SQL.DEFINE_ARRAY(VI_DBMSCUR,
                              I,
                              V_VARCHAR_TAB,
                              VI_ARRAY_BUFFER,
                              VI_ARRAY_BUFFER * (I - 1) + 1);
      END LOOP;
      --定义完之后进行批量fetch
      BEGIN
        VI_FETCHNUMBER := DBMS_SQL.FETCH_ROWS(VI_DBMSCUR);
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -1002 THEN
            VI_FETCHNUMBER := 0;
          ELSE
            RAISE;
          END IF;
      END;
      EXIT WHEN VI_FETCHNUMBER = 0;

      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        DBMS_SQL.COLUMN_VALUE(VI_DBMSCUR, I, V_VARCHAR_TAB);
      END LOOP;
      --循环缓存的数据行，读1行数写1行数到文件中
      <<WRITE_BUFFER_ROWS_LOOP>>
      FOR I_ROWINDEX IN 1 .. VI_FETCHNUMBER LOOP
        VS_RECSTR := '';
        FOR J_COLINDEX IN 1 .. VI_COLUMN_CNT LOOP
          IF (J_COLINDEX > 1) THEN
            VS_RECSTR := VS_RECSTR || IS_FIELDDELIMITER;
          END IF;
          VS_FIELDSTR := V_VARCHAR_TAB(VI_ARRAY_BUFFER * (J_COLINDEX - 1) +
                                       I_ROWINDEX);
          IF (VS_FIELDSTR IS NOT NULL) THEN
            --vs_fieldstr := REPLACE(vs_fieldstr,chr(00));
            vs_fieldstr := REPLACE(vs_fieldstr,chr(10)); --去0A
            vs_fieldstr := REPLACE(vs_fieldstr,chr(13)); --去0D
            VS_RECSTR := VS_RECSTR || VS_FIELDSTR;
          END IF;
        END LOOP;
        --写一行文件
        UTL_FILE.PUT(V_UTLFILE_FP, convert(VS_RECSTR, 'ZHS16GBK'));
        UTL_FILE.FFLUSH(V_UTLFILE_FP);
        --utl_file.new_line (fp);
        UTL_FILE.PUT(V_UTLFILE_FP, convert(CHR(10), 'ZHS16GBK')); --换行符为0A


      END LOOP WRITE_BUFFER_ROWS_LOOP;

    END LOOP LOOP_EXPORT_FILES;

    --关闭文件
    UTL_FILE.FCLOSE(V_UTLFILE_FP);
    --关闭游标
    IF DBMS_SQL.IS_OPEN(VI_DBMSCUR) THEN
      DBMS_SQL.CLOSE_CURSOR(VI_DBMSCUR);
    END IF;
    OS_MSG := '文件导出成功！';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OS_FLAG := '0';
      OS_MSG  := SUBSTR('文件导出失败，错误信息：' || SQLERRM, 1, 100);
  END PROC_GENERAL_WRITE_OA;

  PROCEDURE PROC_GENERAL_WRITE_FIXLENGTH(IS_SQLSTR   IN VARCHAR2, --需要输出为文件的sql
                                         IS_FILENAME IN VARCHAR2, --需要导出的文件名
                                         IS_DIR      IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                                         IS_OWNER_TBNAME IN VARCHAR2, --用户及表名用.分隔如 CPMG.ODS_ABC
                                         OS_FLAG OUT VARCHAR2, --0成功 1失败
                                         OS_MSG  OUT VARCHAR2) IS
    V_UTLFILE_FP UTL_FILE.FILE_TYPE;
    --动态游标变量begin
    VI_DBMSCUR     PLS_INTEGER; --DBMS包对应的游标
    VI_COLUMN_CNT  PLS_INTEGER; --记录查询有多少列
    VI_FETCHNUMBER PLS_INTEGER;
    V_DESC_TAB     DBMS_SQL.DESC_TAB; --数组，用来记录SELECT每个“字段”的数据类型、精度、范围、字段名称等信息
    V_VARCHAR_TAB  DBMS_SQL.VARCHAR2_TABLE; --数组用于存储游标读出的数据，类型为 TABLE OF VARCHAR2(2000) INDEX BY BINARY_INTEGER
    --动态游标变量end
    VI_ARRAY_BUFFER PLS_INTEGER := 5000; --每次缓存vi_array_buffer行数据
    VS_RECSTR       VARCHAR2(10000); --记录拼接出的每行的字符串
    VS_FIELDSTR     VARCHAR2(2000); --记录每个字段的字符串
    V_TMPNUM        NUMBER;

  BEGIN

    V_UTLFILE_FP := UTL_FILE.FOPEN(IS_DIR, IS_FILENAME, 'w', 32767);
    VI_DBMSCUR   := DBMS_SQL.OPEN_CURSOR; --游标打开
    DBMS_SQL.PARSE(VI_DBMSCUR, IS_SQLSTR, 0); --游标解析
    DBMS_SQL.DESCRIBE_COLUMNS(VI_DBMSCUR, VI_COLUMN_CNT, V_DESC_TAB); --获取游标对应所有字段信息
    VI_FETCHNUMBER := DBMS_SQL.EXECUTE(VI_DBMSCUR); --游标执行

    FOR I IN 1 .. VI_COLUMN_CNT LOOP
       IF V_DESC_TAB(I).COL_TYPE = 1  THEN--字段类型为字符时处理一下
         --如果字符长度与数据不一致，则列长度以 字符长度 为准
         BEGIN
            select DATA_LENGTH/CHAR_LENGTH into V_TMPNUM from ALL_TAB_COLS
             where owner||'.'||table_name=upper(IS_OWNER_TBNAME) and column_name=V_DESC_TAB(I).col_name;
         EXCEPTION WHEN OTHERS THEN
              V_TMPNUM:= 4;
         END ;
         IF V_TMPNUM > 1 THEN
           V_DESC_TAB(I).COL_MAX_LEN := ROUND(V_DESC_TAB(I).COL_MAX_LEN/V_TMPNUM);
         END IF;
       END IF;
    END LOOP;
    --开始循环读取数据并写文件
    <<LOOP_EXPORT_FILES>>
    LOOP
      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        /*每次缓存vi_array_buffer行数据，在这些数据当中，第i行,j列数据值在varray_col_varvalue
          数组中对应的下标是：vi_array_buffer*(j-1)+i
        */
        DBMS_SQL.DEFINE_ARRAY(VI_DBMSCUR,
                              I,
                              V_VARCHAR_TAB,
                              VI_ARRAY_BUFFER,
                              VI_ARRAY_BUFFER * (I - 1) + 1);
      END LOOP;
      --定义完之后进行批量fetch
      BEGIN
        VI_FETCHNUMBER := DBMS_SQL.FETCH_ROWS(VI_DBMSCUR);
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -1002 THEN
            VI_FETCHNUMBER := 0;
          ELSE
            RAISE;
          END IF;
      END;
      EXIT WHEN VI_FETCHNUMBER = 0;

      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        DBMS_SQL.COLUMN_VALUE(VI_DBMSCUR, I, V_VARCHAR_TAB);
      END LOOP;
      --循环缓存的数据行，读1行数写1行数到文件中
      <<WRITE_BUFFER_ROWS_LOOP>>
      FOR I_ROWINDEX IN 1 .. VI_FETCHNUMBER LOOP
        VS_RECSTR := '';
        FOR J_COLINDEX IN 1 .. VI_COLUMN_CNT LOOP

          IF  J_COLINDEX = 1 OR J_COLINDEX =2
           THEN


          --字符型右补空格

              IF V_DESC_TAB(J_COLINDEX).COL_TYPE = 1 THEN
                VS_FIELDSTR := SUBSTR(RPAD(NVL(V_VARCHAR_TAB(VI_ARRAY_BUFFER *
                                                      (J_COLINDEX - 1) +
                                                      I_ROWINDEX),
                                        ' '),
                                    V_DESC_TAB(J_COLINDEX).COL_MAX_LEN,
                                    ' '),1,5);

                --数字左补空格
              ELSIF V_DESC_TAB(J_COLINDEX).COL_TYPE = 2 THEN

                VS_FIELDSTR := LPAD(NVL(V_VARCHAR_TAB(VI_ARRAY_BUFFER *
                                                      (J_COLINDEX - 1) +
                                                      I_ROWINDEX),
                                        ' '),
                                    V_DESC_TAB(J_COLINDEX).COL_PRECISION,
                                    ' ');

              END IF;

          ELSE
             IF V_DESC_TAB(J_COLINDEX).COL_TYPE = 1 THEN
                VS_FIELDSTR := RPAD(NVL(V_VARCHAR_TAB(VI_ARRAY_BUFFER *
                                                      (J_COLINDEX - 1) +
                                                      I_ROWINDEX),
                                        ' '),
                                    V_DESC_TAB(J_COLINDEX).COL_MAX_LEN,
                                    ' ');

                --数字左补空格
              ELSIF V_DESC_TAB(J_COLINDEX).COL_TYPE = 2 THEN

                VS_FIELDSTR := LPAD(NVL(V_VARCHAR_TAB(VI_ARRAY_BUFFER *
                                                      (J_COLINDEX - 1) +
                                                      I_ROWINDEX),
                                        ' '),
                                    V_DESC_TAB(J_COLINDEX).COL_PRECISION,
                                    ' ');
                END IF;
              END IF;

          IF (VS_FIELDSTR IS NOT NULL) THEN
            --vs_fieldstr := REPLACE(vs_fieldstr,chr(00));
            VS_FIELDSTR := REPLACE(VS_FIELDSTR, CHR(10)); --去0A
            VS_FIELDSTR := REPLACE(VS_FIELDSTR, CHR(13)); --去0D
            VS_RECSTR   := VS_RECSTR || VS_FIELDSTR;
          END IF;
        END LOOP;
        --写一行文件
        UTL_FILE.PUT(V_UTLFILE_FP, convert(VS_RECSTR, 'ZHS16GBK'));
        UTL_FILE.FFLUSH(V_UTLFILE_FP);
        --utl_file.new_line (fp);
        UTL_FILE.PUT(V_UTLFILE_FP, convert(CHR(13) || CHR(10), 'ZHS16GBK')); --换行符为0A
      END LOOP WRITE_BUFFER_ROWS_LOOP;

    END LOOP LOOP_EXPORT_FILES;

    --关闭文件
    UTL_FILE.FCLOSE(V_UTLFILE_FP);
    OS_FLAG := '0';
    OS_MSG  := '文件生成成功';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OS_FLAG := '1';
      OS_MSG  := SUBSTR('文件导出失败，错误信息：' || SQLERRM, 1, 100);
  END PROC_GENERAL_WRITE_FIXLENGTH;





  PROCEDURE PROC_GENERAL_WRITE_FL_NOR(IS_SQLSTR   IN VARCHAR2, --需要输出为文件的sql
                                         IS_FILENAME IN VARCHAR2, --需要导出的文件名
                                         IS_DIR      IN VARCHAR2, --导出文件目录对应的DIRECTORY的名称
                                         IS_OWNER_TBNAME IN VARCHAR2, --用户及表名用.分隔如 CPMG.ODS_ABC
                                         OS_FLAG OUT VARCHAR2, --0成功 1失败
                                         OS_MSG  OUT VARCHAR2) IS
    V_UTLFILE_FP UTL_FILE.FILE_TYPE;
    --动态游标变量begin
    VI_DBMSCUR     PLS_INTEGER; --DBMS包对应的游标
    VI_COLUMN_CNT  PLS_INTEGER; --记录查询有多少列
    VI_FETCHNUMBER PLS_INTEGER;
    V_DESC_TAB     DBMS_SQL.DESC_TAB; --数组，用来记录SELECT每个“字段”的数据类型、精度、范围、字段名称等信息
    V_VARCHAR_TAB  DBMS_SQL.VARCHAR2_TABLE; --数组用于存储游标读出的数据，类型为 TABLE OF VARCHAR2(2000) INDEX BY BINARY_INTEGER
    --动态游标变量end
    VI_ARRAY_BUFFER PLS_INTEGER := 5000; --每次缓存vi_array_buffer行数据
    VS_RECSTR       VARCHAR2(10000); --记录拼接出的每行的字符串
    VS_FIELDSTR     VARCHAR2(2000); --记录每个字段的字符串
    V_TMPNUM        NUMBER;

  BEGIN

    V_UTLFILE_FP := UTL_FILE.FOPEN(IS_DIR, IS_FILENAME, 'w', 32767);
    VI_DBMSCUR   := DBMS_SQL.OPEN_CURSOR; --游标打开
    DBMS_SQL.PARSE(VI_DBMSCUR, IS_SQLSTR, 0); --游标解析
    DBMS_SQL.DESCRIBE_COLUMNS(VI_DBMSCUR, VI_COLUMN_CNT, V_DESC_TAB); --获取游标对应所有字段信息
    VI_FETCHNUMBER := DBMS_SQL.EXECUTE(VI_DBMSCUR); --游标执行
    FOR I IN 1 .. VI_COLUMN_CNT LOOP
       IF V_DESC_TAB(I).COL_TYPE = 1  THEN--字段类型为字符时处理一下
         --如果字符长度与数据不一致，则列长度以 字符长度 为准
         BEGIN
            select DATA_LENGTH/CHAR_LENGTH into V_TMPNUM from ALL_TAB_COLS
             where owner||'.'||table_name=upper(IS_OWNER_TBNAME) and column_name=V_DESC_TAB(I).col_name;
         EXCEPTION WHEN OTHERS THEN
              V_TMPNUM:= 4;
         END ;
         IF V_TMPNUM > 1 THEN
           V_DESC_TAB(I).COL_MAX_LEN := ROUND(V_DESC_TAB(I).COL_MAX_LEN/V_TMPNUM);
         END IF;
       END IF;
    END LOOP;
    --开始循环读取数据并写文件
    <<LOOP_EXPORT_FILES>>
    LOOP
      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        /*每次缓存vi_array_buffer行数据，在这些数据当中，第i行,j列数据值在varray_col_varvalue
          数组中对应的下标是：vi_array_buffer*(j-1)+i
        */
        DBMS_SQL.DEFINE_ARRAY(VI_DBMSCUR,
                              I,
                              V_VARCHAR_TAB,
                              VI_ARRAY_BUFFER,
                              VI_ARRAY_BUFFER * (I - 1) + 1);
      END LOOP;
      --定义完之后进行批量fetch
      BEGIN
        VI_FETCHNUMBER := DBMS_SQL.FETCH_ROWS(VI_DBMSCUR);
      EXCEPTION
        WHEN OTHERS THEN
          IF SQLCODE = -1002 THEN
            VI_FETCHNUMBER := 0;
          ELSE
            RAISE;
          END IF;
      END;
      EXIT WHEN VI_FETCHNUMBER = 0;

      FOR I IN 1 .. VI_COLUMN_CNT LOOP
        DBMS_SQL.COLUMN_VALUE(VI_DBMSCUR, I, V_VARCHAR_TAB);
      END LOOP;
      --循环缓存的数据行，读1行数写1行数到文件中
      <<WRITE_BUFFER_ROWS_LOOP>>
      FOR I_ROWINDEX IN 1 .. VI_FETCHNUMBER LOOP
        VS_RECSTR := '';
        FOR J_COLINDEX IN 1 .. VI_COLUMN_CNT LOOP

          --字符型右补空格

          IF V_DESC_TAB(J_COLINDEX).COL_TYPE = 1 THEN
            VS_FIELDSTR := RPAD(NVL(V_VARCHAR_TAB(VI_ARRAY_BUFFER *
                                                  (J_COLINDEX - 1) +
                                                  I_ROWINDEX),
                                    ' '),
                                V_DESC_TAB(J_COLINDEX).COL_MAX_LEN,
                                ' ');
            --数字左补空格
          ELSIF V_DESC_TAB(J_COLINDEX).COL_TYPE = 2 THEN

            VS_FIELDSTR := LPAD(NVL(V_VARCHAR_TAB(VI_ARRAY_BUFFER *
                                                  (J_COLINDEX - 1) +
                                                  I_ROWINDEX),
                                    ' '),
                                V_DESC_TAB(J_COLINDEX).COL_PRECISION,
                                ' ');

          END IF;

          IF (VS_FIELDSTR IS NOT NULL) THEN
            --vs_fieldstr := REPLACE(vs_fieldstr,chr(00));
            VS_FIELDSTR := REPLACE(VS_FIELDSTR, CHR(10)); --去0A
            VS_FIELDSTR := REPLACE(VS_FIELDSTR, CHR(13)); --去0D
            VS_RECSTR   := VS_RECSTR || VS_FIELDSTR;
          END IF;
        END LOOP;
        --写一行文件
        UTL_FILE.PUT(V_UTLFILE_FP, convert(VS_RECSTR, 'ZHS16GBK'));
        UTL_FILE.FFLUSH(V_UTLFILE_FP);
        --utl_file.new_line (fp);
        UTL_FILE.PUT(V_UTLFILE_FP, convert(CHR(13) || CHR(10), 'ZHS16GBK')); --换行符为0A
      END LOOP WRITE_BUFFER_ROWS_LOOP;

    END LOOP LOOP_EXPORT_FILES;

    --关闭文件
    UTL_FILE.FCLOSE(V_UTLFILE_FP);
    OS_FLAG := '0';
    OS_MSG  := '文件生成成功';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OS_FLAG := '1';
      OS_MSG  := SUBSTR('文件导出失败，错误信息：' || SQLERRM, 1, 100);
  END PROC_GENERAL_WRITE_FL_NOR;











  ----------------------------
  PROCEDURE PROC_WRITE_FRDK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_FRDK';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'select ''0''||zoneno, ''0''||brno, bankflag, qx, currtype, cresort, prodid, loantype, hydm, corpgrade, value01, value02, value03, value04, value05, value06, value07, value08, value09, value010,smallcorp,LGD_LEVEL from dm_cs_zfrdk where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    V_FILE := 'CAP_FRDK.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR,'cpmg.dm_cs_zfrdk', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FRDK;
  --------
  PROCEDURE PROC_WRITE_GRDK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    --v_open_day   VARCHAR2(8);
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_GRDK';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    --徐敏10月版本修改 13年启用新接口表数据
    /*BEGIN
    SELECT t.KIND_ID INTO v_open_day  from dic_all_kind t WHERE t.OPERATION_KIND='ADJUST_DATE_UG';
    EXCEPTION
    WHEN OTHERS THEN
    v_open_day:='20130101';
    END ;*/

    V_FILE := 'CAP_GRDK.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);

    /*IF P_I_DATE>=v_open_day
    THEN*/
    V_SQL  := 'select ''0''||zoneno, ''0''||brno, bankflag, qx, prodid, currtype, loanshape, creditrank, value01, value02, value03, value04, value05, value06, value07, value08, value09, value010, value011, value012 from dm_cs_zgrdk_ug where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR, 'cpmg.dm_cs_zgrdk_ug',V_FLAG, V_MSG);
    /*ELSE
    V_SQL  := 'select ''0''||zoneno, ''0''||brno, bankflag, qx, prodid, currtype, loanshape, creditrank, value01, value02, value03, value04, value05, value06, value07, value08, value09, value010, value011, value012 from dm_cs_zgrdk where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR, V_FLAG, V_MSG);

    END IF;*/

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_GRDK;
  PROCEDURE PROC_WRITE_PJTX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_PJTX';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,qx,currtype,prodcode,loanshape,value01,value02,value03,value04 FROM dm_cs_zpjtx where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    V_FILE := 'CAP_PJTX.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR, 'cpmg.dm_cs_zpjtx',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PJTX;
  ---------------------
  PROCEDURE PROC_WRITE_BWZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_bwzc';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodid,value01,value02,value03,value04 FROM dm_cs_zbwzcdb ORDER BY zoneno';
    V_FILE := 'CAP_BWZC.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR,'cpmg.dm_cs_zbwzcdb', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_BWZC;
  ---------------------
  PROCEDURE PROC_WRITE_ZQTZ(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_ZQTZ';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,currtype,prodid,value01,value02,value03,value04 FROM dm_cs_zzqtz ORDER BY zoneno';
    V_FILE := 'CAP_ZQTZ.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR,'cpmg.dm_cs_zzqtz', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ZQTZ;
  ---------------------
  PROCEDURE PROC_WRITE_YHK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    --v_open_day   VARCHAR2(8);
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_YHK';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    --徐敏10月版本修改 13年启用新接口表数据
    /*BEGIN
    SELECT t.KIND_ID INTO v_open_day  from dic_all_kind t WHERE t.OPERATION_KIND='ADJUST_DATE_UG';
    EXCEPTION
    WHEN OTHERS THEN
    v_open_day:='20130101';
    END ;

    IF P_I_DATE>=v_open_day
    THEN*/
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodid,value01,value02,value03,value04 FROM dm_cs_yhk_ug where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    /*ELSE
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodid,value01,value02,value03,value04 FROM dm_cs_yhk ORDER BY zoneno';
    END IF;*/

    V_FILE := 'CAP_YHK.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR,'cpmg.dm_cs_yhk_ug', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_YHK;
  ---------------------
  ---------------------
  PROCEDURE PROC_WRITE_ZJYW(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_ZJYW';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodid,value01,value02,value03,value04 FROM DM_CS_ZZJYE  ORDER BY zoneno';
    V_FILE := 'CAP_ZJYW.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR,'cpmg.DM_CS_ZZJYE', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ZJYW;
  ---------------------
  ---------------------
  PROCEDURE PROC_WRITE_WXZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_WXZC';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodid,value01,value02,value03,value04 FROM DM_CS_ZWXZC where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    V_FILE := 'CAP_WXZC.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR, 'cpmg.DM_CS_ZWXZC',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_WXZC;
  ---------------------
  ---------------------
  PROCEDURE PROC_WRITE_QTFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_QTFX';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodid,value01,value02,value03,value04,value05,value06 FROM DM_CS_QTFX where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    V_FILE := 'CAP_QTFX.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR, 'cpmg.DM_CS_QTFX',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_QTFX;
  ---------------------
  PROCEDURE PROC_WRITE_CZFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_CZFX';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT ''0''||zoneno, ''0''||brno,bankflag,currtype,prodcode,ec FROM DM_CS_CZFX where currtype in (''001'',''992'',''994'') ORDER BY zoneno';
    V_FILE := 'CAP_CZFX.BIN';
    V_DIR  := 'CHAIFEN_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FIXLENGTH(V_SQL, V_FILE, V_DIR,'cpmg.DM_CS_CZFX', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_CZFX;
  -----------------------------
  PROCEDURE PROC_WRITE_ORGAN(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_ORGAN';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT * FROM PRM_IRA_ORGAN where end_date=''99991231''';
    V_FILE := 'CPMGPRMIRAORGAN0000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR,'cpmg.PRM_IRA_ORGAN', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ORGAN;
  ---------------------
  PROCEDURE PROC_WRITE_PRODUCT(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_PRODUCT';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT * FROM PRM_PRODUCT';
    V_FILE := 'CPMGPRMPRODUCT0000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR, 'cpmg.PRM_PRODUCT',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PRODUCT;
  ---------------------
  PROCEDURE PROC_WRITE_DBFS(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_DBFS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT DISTINCT DBFS_ID, DBFS_LB FROM PRM_CS032  WHERE END_DATE = ''99991231''';
    V_FILE := 'CPMGDMSCCS0320000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR, 'cpmg.PRM_CS032',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_DBFS;
  ---------------------
  PROCEDURE PROC_WRITE_XYDJ(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_XYDJ';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT DICT_CODE,DICT_NAME,DICT_ORDER FROM PRM_DA200011040 WHERE END_DATE = ''99991231''';
    V_FILE := 'CPMGDMSCDA2000110400000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR,'cpmg.PRM_DA200011040', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_XYDJ;
  -------
  PROCEDURE PROC_WRITE_HYDM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_HYDM';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT * FROM PRM_HYDM ';
    V_FILE := 'CPMGPRMHYDL0000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR, 'cpmg.PRM_HYDM',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_HYDM;
  ---------------------
  -------
  PROCEDURE PROC_WRITE_FRDKQX(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_FRDKQX';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT qxdc_id,qxdc_name FROM prm_cs036 where end_date=''99991231''';
    V_FILE := 'PRMFRDKQX0000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR, 'cpmg.prm_cs036',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FRDKQX;
  -------
  PROCEDURE PROC_WRITE_GRDKQX(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_GRDKQX';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'select cast(kind_id as varchar2(10)) kid, cast(kind_name as varchar2(40)) kname from dic_all_kind t where t.operation_kind = ''GDTERM''';
    V_FILE := 'PRMGRDKQX0000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR, 'cpmg.dic_all_kind',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_GRDKQX;
  ----
  PROCEDURE PROC_WRITE_PJTXQX(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_PJTX';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT qxdc_id,qxdc_name FROM PRM_CSPJ2 where end_date=''99991231''';
    V_FILE := 'PRMPJTXQX0000000000.BIN';
    V_DIR  := 'FENFA_CS';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR, 'cpmg.PRM_CSPJ2',V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PJTXQX;
  --------------
  PROCEDURE PROC_WRITE_DATACTL_CHAIFEN(P_I_DATE    IN VARCHAR2,
                                       P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;

  BEGIN
    IN_FILE      := 'datactl.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CHAIFEN_CS', IN_FILE, 'w', 32767);
    V_STRING     := 'IRA风险综合分析                                                ' ||
                    P_I_DATE || '1  10   ';
    UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(V_STRING, 'ZHS16GBK'));
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_DATACTL_CHAIFEN;
  ------------
  PROCEDURE PROC_WRITE_DATACTL_FENFA(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;

  BEGIN
    IN_FILE      := 'datactl.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('FENFA_CS', IN_FILE, 'w', 32767);
    V_STRING     := 'IRA风险综合分析                                                ' ||
                    P_I_DATE || '1  8   ';
    UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(V_STRING, 'ZHS16GBK'));
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';



    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_DATACTL_FENFA;
----------------以下为应用产品统计分析
PROCEDURE PROC_WRITE_ASA_A0001(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_ASA_A0001';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT * FROM EXP_CAP_ASA_A0001';
    V_FILE := 'CAP_ASA_A00010000000000.BIN';
    V_DIR  := 'CPMG_PV';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR,'cpmg.EXP_CAP_ASA_A0001', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ASA_A0001;
  --
  PROCEDURE PROC_WRITE_ASA_A0002(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_ASA_A0002';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT * FROM EXP_CAP_ASA_A0002';
    V_FILE := 'CAP_ASA_A00020000000000.BIN';
    V_DIR  := 'CPMG_PV';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR,'cpmg.EXP_CAP_ASA_A0002', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ASA_A0002;
  --
  PROCEDURE PROC_WRITE_ASA_A0003(P_I_DATE    IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --sql语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_ALLBIN.PROC_WRITE_ASA_A0003';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    V_SQL  := 'SELECT * FROM EXP_CAP_ASA_A0003';
    V_FILE := 'CAP_ASA_A00030000000000.BIN';
    V_DIR  := 'CPMG_PV';
    V_DELI := CHR(27);
    PROC_GENERAL_WRITE_FL_NOR(V_SQL, V_FILE, V_DIR,'cpmg.EXP_CAP_ASA_A0003', V_FLAG, V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ASA_A0003;
END PACK_WRITE_ALLBIN;
//

CREATE OR REPLACE PACKAGE BODY PACK_WRITE_BDP IS

  PROCEDURE PROC_WRITE_FRDK_IN(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_BDP.PROC_WRITE_FRDK_IN';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

          V_SQL := 'SELECT t.LOANNO,t.LOANSQNO,t.ACCNO,t.CURRTYPE,t.BALANCE,t.ECOST FROM dw_mod_frdk t WHERE t.START_DATE='''|| P_I_DATE||'''';

    V_FILE := 'CAP_FRDK_IN_'||P_I_DATE||'.BIN';
    V_DIR  := 'CAP_BDP';
    V_DELI := CHR(27);

    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FRDK_IN;


END PACK_WRITE_BDP;
//

CREATE OR REPLACE PACKAGE BODY PACK_WRITE_EDI IS

  PROCEDURE PROC_WRITE_EDI(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_EDI.PROC_WRITE_EDI';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    IF P_I_DATE =
       TO_CHAR(LAST_DAY(TO_DATE(P_I_DATE, 'YYYYMMDD')), 'YYYYMMDD')  THEN
      V_SQL := 'SELECT ZONENO,PHYBRNO,PCACCNO,PRODCODE,MEMNO,LNNO,TA500251054,RELATEDNO,CURRTYPE,SUBCODE,LOANTYPE,LEVELFIVE,LNAMOUNT,LNBALANCE,RATE,LOANTERM,AVCHTYPE,SURVEYOR,LOANDATE,DUEDATE,CUSTID,CUSTCIS,CUSTNAME,REGTYPE,REGNO,CREDITLEVEL,ECUSFLAG,AVCHAMT,MGAMT,MPAMT,ASSUREAMT,OTHERAMT,INVALIDAMT,CREDITAMT,TOTALAMT,STATUS,FBFLAG,PD_VALUE,LGD_VALUE,R_VALUE,DEFLT_FLAG,OVDUE_CNT,PRODUCT_ID3,PRODUCT_NAME3,MATURITY_DAYS,TERM_LEVEL_NO,TERM_LEVEL_NAME,K_VALUE,EC_COEF,ZCAMOUNT,ECOST,DATA_DATE FROM DW_MOD_GRDK_UG DW WHERE DW.DATA_DATE = '''|| P_I_DATE||'''';
    ELSE
      IF  P_I_DATE = '20160221' THEN
          V_SQL := 'SELECT ZONENO,PHYBRNO,PCACCNO,PRODCODE,MEMNO,LNNO,TA500251054,RELATEDNO,CURRTYPE,SUBCODE,LOANTYPE,LEVELFIVE,LNAMOUNT,LNBALANCE,RATE,LOANTERM,AVCHTYPE,SURVEYOR,LOANDATE,DUEDATE,CUSTID,CUSTCIS,CUSTNAME,REGTYPE,REGNO,CREDITLEVEL,ECUSFLAG,AVCHAMT,MGAMT,MPAMT,ASSUREAMT,OTHERAMT,INVALIDAMT,CREDITAMT,TOTALAMT,STATUS,FBFLAG,PD_VALUE,LGD_VALUE,R_VALUE,DEFLT_FLAG,OVDUE_CNT,PRODUCT_ID3,PRODUCT_NAME3,MATURITY_DAYS,TERM_LEVEL_NO,TERM_LEVEL_NAME,K_VALUE,EC_COEF,ZCAMOUNT,ECOST,DATA_DATE FROM DW_MOD_GRDK_UG DW WHERE DW.DATA_DATE = ''20160131''';
      ELSE
          V_SQL := NULL;
      END IF;
    END IF;
    V_FILE := 'CAP_CAPGRDKEDW0000000000.BIN';
    V_DIR  := 'GRDK_EDI';
    V_DELI := CHR(27);

    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_EDI;


END PACK_WRITE_EDI;
//

CREATE OR REPLACE PACKAGE BODY PACK_WRITE_EDW IS

  PROCEDURE PROC_WRITE_GSIS_RPT_DATE(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(200);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_EDW.PROC_WRITE_GSIS_RPT_DATE';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    --发现导出最后一个字段以中文字符结尾，会显示不出，经过试验，加两个空格就可以正常显示，暂时先这样试试
    V_SQL := 'SELECT REPORT_DATE, REPORT_TYPE FROM GSIFISS.PRM_GSIS_RPT_DATE';

    V_FILE := 'CAP_GSIS_RPT_DATE_' || P_I_DATE || '.BIN';
    V_DIR  := 'CAP_EDW';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_GSIS_RPT_DATE;

END PACK_WRITE_EDW;
//

CREATE OR REPLACE PACKAGE BODY PACK_WRITE_GCMS IS

  PROCEDURE PROC_WRITE_PRO_SUB_REL(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(200);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_PRO_SUB_REL';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------
    --发现导出最后一个字段以中文字符结尾，会显示不出，经过试验，加两个空格就可以正常显示，暂时先这样试试
    V_SQL := 'SELECT PRODUCT_ID, SUBJECT_CODE, PRODUCT_NAME, PRO_NAME||'' ''||'' '' END_FLAG FROM PRM_PRODUCT_SUBJECT';

    V_FILE := 'CAP_PRO_SUB_REL0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PRO_SUB_REL;

  PROCEDURE PROC_WRITE_PRO_ZONE_XS(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_PRO_ZONE_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT DISTINCT T.PRODUCT_ID, T2.ZONE_NO, T.PZXS ' ||
             '  FROM PRM_XS044 T, PRM_IRA_ORGAN T2 ' ||
             ' WHERE T.END_DATE = ''99991231'' ' ||
             '   AND T2.END_DATE = ''99991231'' ' ||
             '   AND ((T2.BELONG_ZONENO = T.ZONE_NO AND T2.BANK_FLAG = ''3'') OR ' ||
             '       (T.ZONE_NO = T2.ZONE_NO AND T2.BANK_FLAG = ''4'')) ';

    V_FILE := 'CAP_PRO_ZONE_XS0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PRO_ZONE_XS;

  PROCEDURE PROC_WRITE_TERM_LEVEL(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_TERM_LEVEL';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT TERM_LEVEL_NO, ' ||
             '       B.KIND_NAME AS TERM_LEVEL_NAME, ' ||
             '       LEFT_KB, ' || '       LEFT_VAL, ' ||
             '       RIGHT_VAL, ' || '       RIGHT_KB ' ||
             '  FROM PRM_GDTERMP A, DIC_ALL_KIND B ' ||
             ' WHERE A.TERM_LEVEL_NO = B.KIND_ID ' ||
             '   AND B.OPERATION_KIND = ''GDTERM'' ' ||
             '   AND A.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_TERM_LEVEL' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_TERM_LEVEL;

  PROCEDURE PROC_WRITE_PRO_TERM_XS(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_PRO_TERM_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PRODUCT_ID, ' || '       T.VALUE1, ' ||
             '       T.VALUE2, ' || '       T.VALUE3, ' ||
             '       T.VALUE4, ' || '       T.VALUE5, ' ||
             '       T.VALUE6, ' || '       T.VALUE7 ' ||
             '  FROM PRM_GDTERMF T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_PRO_TERM_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PRO_TERM_XS;

  PROCEDURE PROC_WRITE_ZLTJ_XS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_ZLTJ_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PRODUCT_ID, T.VALUE1, T.VALUE2, T.VALUE3, T.VALUE4, T.VALUE5 ' ||
             '  FROM PRM_GDQLTYF T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_ZLTJ_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ZLTJ_XS;

  PROCEDURE PROC_WRITE_LIMMIT_XS(P_I_DATE    IN VARCHAR2,
                                 P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_LIMMIT_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PRODUCT_ID, T.COEF_CEIL, T.COEF_FLOOR ' ||
             '  FROM PRM_NPLOANF T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_LIMMIT_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_LIMMIT_XS;

  PROCEDURE PROC_WRITE_FD_BASE_XS(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(1000); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_BASE_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT A.LGD_LEVEL, ' || '       LOWER_LIMIT, ' ||
             '       UPPER_LIMIT, ' || '       B.PRODUCT_ID, ' ||
             '       B.CREDIT_CODE, ' || '       B.PAR_COEF ' ||
             '  FROM (SELECT T.SEG_SEQ LGD_LEVEL, ' ||
             '               T.LOWER LOWER_LIMIT, ' ||
             '               NVL(LEAD(LOWER) OVER(ORDER BY TO_NUMBER(T.SEG_SEQ)), 999) UPPER_LIMIT ' ||
             '          FROM CPMG.PRM_CSFR5 T ' ||
             '         WHERE T.END_DATE = ''99991231'') A, ' ||
             '       (SELECT PRODUCT_ID, ' ||
             '               CREDIT_CODE, ' || '               LGD_LEVEL, ' ||
             '               PAR_COEF, ' || '               START_DATE, ' ||
             '               END_DATE ' || '          FROM CPMG.PRM_XSFR1 ' ||
             '         WHERE END_DATE = ''99991231'') B ' ||
             ' WHERE A.LGD_LEVEL = B.LGD_LEVEL ';

    V_FILE := 'CAP_FD_BASE_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_BASE_XS;

  PROCEDURE PROC_WRITE_FD_PRO_XS(P_I_DATE    IN VARCHAR2,
                                 P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_PRO_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PRODUCT_ID, T.SUBJECT_CODE, T.YWPZ_CODE, T.PAR_COEF ' ||
             '  FROM PRM_XSFR2_TMP T ';

    V_FILE := 'CAP_FD_PRO_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_PRO_XS;

  PROCEDURE PROC_WRITE_FD_HY_ZONE_XS(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_HY_ZONE_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.FIELD_CODE, T.FIELD_NAME, T.AREA_CODE, T.XS_VALUE ' ||
             '  FROM PRM_XS034 T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_FD_HY_ZONE_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_HY_ZONE_XS;

  PROCEDURE PROC_WRITE_FD_TERM_LEVEL(P_I_DATE    IN VARCHAR2,
                                     P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_TERM_LEVEL';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.QXDC_ID, ' || '       T.QXDC_NAME, ' ||
             '       T.LEFT_VALUE, ' || '       T.RIGHT_VALUE, ' ||
             '       T.LEFT_IN, ' || '       T.RIGHT_IN ' ||
             '  FROM PRM_CS036 T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_FD_TERM_LEVEL' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_TERM_LEVEL;

  PROCEDURE PROC_WRITE_FD_TERM_XS(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_TERM_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.QXDC_ID, T.CREDIT_CODE, T.PAR_COEF ' ||
             '  FROM PRM_XSFR3 T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_FD_TERM_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_TERM_XS;

  PROCEDURE PROC_WRITE_FD_SMALLCOP_XS(P_I_DATE    IN VARCHAR2,
                                      P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_SMALLCOP_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PAR_COEF ' || '  FROM PRM_XSFR6 T ' ||
             ' WHERE T.END_DATE = ''99991231'' ' ||
             '   AND T.IS_SE = ''1'' ';

    V_FILE := 'CAP_FD_SMALLCOP_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_SMALLCOP_XS;

  PROCEDURE PROC_WRITE_FD_ZLXS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FD_ZLXS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.FL12_ID, T.XS_VALUE FROM PRM_XS032 T WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_FD_ZLXS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FD_ZLXS;

  PROCEDURE PROC_WRITE_SPEC_PRO_XS(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_SPEC_PRO_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT PRODUCT_ID, PAR_COEF FROM PRM_XSFRA T WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_SPEC_PRO_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_SPEC_PRO_XS;

  PROCEDURE PROC_WRITE_DX_XS(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_DX_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PAR_COEF FROM PRM_XSFRDX T WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_DX_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_DX_XS;

  PROCEDURE PROC_WRITE_LOANTYPE_DEF(P_I_DATE    IN VARCHAR2,
                                    P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_LOANTYPE_DEF';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.DBFS_ID, T.DBFS_LB, T.LOAD_TYPE ' ||
             '  FROM PRM_CS032 T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_LOANTYPE_DEF' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_LOANTYPE_DEF;

  PROCEDURE PROC_WRITE_ZBCB_XS(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_ZBCB_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PAR_COEF FROM PRM_XSXJ1 T WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_ZBCB_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_ZBCB_XS;

  PROCEDURE PROC_WRITE_BW_XS(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_BW_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.PRODUCT_NAME3, T2.PRODUCT_NAME, T.XS_VALUE ' ||
             '  FROM PRM_XS051 T, PRM_PRODUCT T2 ' ||
             ' WHERE T.END_DATE = ''99991231'' ' ||
             '   AND T2.END_DATE = ''99991231'' ' ||
             '   AND T.PRODUCT_NAME3 = T2.PRODUCT_ID ';

    V_FILE := 'CAP_BW_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_BW_XS;

  PROCEDURE PROC_WRITE_FRDK_IN(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FRDK_IN';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT LOANNO, ' ||
             '       LOANSQNO, ' || '       ACCNO, ' || '       ZONENO, ' ||
             '       BRNO, ' || '       CURRTYPE, ' || '       SUBCODE, ' ||
             '       BALANCE, ' || '       TA200261001, ' ||
             '       TA200261002, ' || '       ECOST ' ||
             '  FROM CPMG.DW_MOD_FRDK T ' || ' WHERE T.START_DATE = ''' ||
             P_I_DATE || ''' ';

    V_FILE := 'CAP_FRDK_IN' || '0000000000' || '.BIN';
    V_DIR  := 'CAP_CCRM';
    V_DELI := CHR(27);
    --EDW要求换行符0D0A
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FRDK_IN;

  PROCEDURE PROC_WRITE_FRDK_OUT(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_FRDK_OUT';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT TA280362001, ' || '       TA280362002, ' ||
             '       TA280362003, ' || '       BOM_ID, ' ||
             '       CURR_TYPE, ' || '       BALANCE, ' ||
             '       DB_BAL, ' || '       ECOST ' ||
             '  FROM CPMG.DW_BW_CAP_DETAIL ' || ' WHERE DATA_DATE = ''' ||
             P_I_DATE || ''' ';

    V_FILE := 'CAP_FRDK_OUT' || '0000000000' || '.BIN';
    V_DIR  := 'CAP_CCRM';
    V_DELI := CHR(27);
    --EDW要求换行符0D0A
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_FRDK_OUT;

  PROCEDURE PROC_WRITE_PERSON_CAPITAL(P_I_DATE    IN VARCHAR2,
                                      P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_PERSON_CAPITAL';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT DATA_DATE, CISNO, CAP_12MON ' ||
             '  FROM CPMG.DW_CAP_GRKH_SUM ' || ' WHERE DATA_DATE = ''' ||
             P_I_DATE || ''' ';

    V_FILE := 'CAP_PERSON_CAPITAL_' || P_I_DATE || '.BIN';
    V_DIR  := 'CAP_ALM';
    V_DELI := CHR(27);
    --ALM要求换行符0D0A
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_PERSON_CAPITAL;

  PROCEDURE PROC_WRITE_DEPOSIT_XS(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_DEPOSIT_XS';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.THPRODUCT, T2.PRODUCT_NAME, T.QUOTIETY, T.START_DATE, T.END_DATE ' ||
             '  FROM PRM_XS10 T, PRM_PRODUCT T2 ' ||
             ' WHERE T2.END_DATE = ''99991231'' ' ||
             '   AND T.THPRODUCT = T2.PRODUCT_ID ';

    V_FILE := 'CAP_DEPOSIT_XS' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    --ALM要求换行符0D0A
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_DEPOSIT_XS;

  PROCEDURE PROC_WRITE_RAROC_LIMMIT(P_I_DATE    IN VARCHAR2,
                                 P_O_SUCCEED OUT VARCHAR2) IS
    V_SQL  VARCHAR2(600); --SQL语句
    V_FILE VARCHAR2(100); --文件名
    V_DIR  VARCHAR2(50); --目录
    V_DELI VARCHAR2(2); --分隔符
    V_FLAG VARCHAR2(100);
    V_MSG  VARCHAR2(100);
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN

    --以下是记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_WRITE_GCMS.PROC_WRITE_RAROC_LIMMIT';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    --------

    V_SQL := 'SELECT T.RAROC_CEIL, T.RAROC_FLOOR, T.TYPE_ID ' ||
             '  FROM CAP_RAROC_LIMMIT T ' || ' WHERE T.END_DATE = ''99991231'' ';

    V_FILE := 'CAP_RAROC_LIMMIT' || '0000000000.BIN';
    V_DIR  := 'CAP_GCMS';
    V_DELI := CHR(27);
    PACK_WRITE_ALLBIN.PROC_GENERAL_WRITE_OA(V_SQL,
                                            V_FILE,
                                            V_DIR,
                                            V_DELI,
                                            V_FLAG,
                                            V_MSG);

    ----------------------------
    P_O_SUCCEED := '0';

    --------------------------------------------------------------------------------------------
    --结束，填写日志

    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                  V_LOG_LEVEL,
                                  V_BEGIN_TIME,
                                  V_END_TIME,
                                  'SUCCESS',
                                  V_STEP_NO,
                                  V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,
                                    V_LOG_LEVEL,
                                    V_BEGIN_TIME,
                                    V_END_TIME,
                                    'FAILED',
                                    V_STEP_NO,
                                    V_SQL_TXT,
                                    V_ERR_CODE,
                                    V_ERR_TXT);
      RAISE;
  END PROC_WRITE_RAROC_LIMMIT;

END PACK_WRITE_GCMS;
//

CREATE OR REPLACE package body PACK_WRITE_PVDATA is
  procedure proc_write_pvmsdata(p_i_date    in varchar2,
                                p_o_succeed out varchar2) is
    v_outputfile UTL_FILE.file_type;
    in_file      VARCHAR(50); --文件名
    v_string     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    tmp_string   VARCHAR2(4000); --存储文件中单行数据
    cursor c is
      select * from dw_mod_pvmsdata;
    --v_a c.rowtype;
  begin

    in_file      := 'CAP_JJZB0000000000.BIN';
    v_outputfile := utl_file.fopen('CPMG_PV', in_file, 'w', 32767);
    for v_c in c loop
      v_string   := nvl(v_c.zoneno, ' '); --地区号
      tmp_string := LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.brno, ' '); --网点号
      tmp_string := tmp_string || LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.bankflag, ' '); --行级别
      tmp_string := tmp_string || LPAD(v_string, 1, ' ');
      v_string   := nvl(v_c.subcode, ' '); --科目
      tmp_string := tmp_string || LPAD(v_string, 8, ' ');
      v_string   := nvl(v_c.currtype, ' '); --币种
      tmp_string := tmp_string || LPAD(v_string, 3, ' ');
      v_string   := nvl(to_char(v_c.ecost), ' '); --经济资本
      tmp_string := tmp_string || LPAD(v_string, 28, ' ');
      v_string   := nvl(v_c.data_date, ' '); --日期
      tmp_string := tmp_string || LPAD(v_string, 8, ' ');
      v_string   := nvl(v_c.diccode, ' '); --字典值
      tmp_string := tmp_string || LPAD(v_string, 3, ' ');
      utl_file.put_line(v_outputfile, convert(tmp_string, 'ZHS16GBK'));
    end loop;
    UTL_FILE.fflush(v_outputfile);
    UTL_FILE.fclose(v_outputfile);
    p_o_succeed := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_o_succeed := '生成' || in_file || '文件出错:' || ' 错误号:' ||
                     SQLCODE || SQLERRM;
      RETURN;
  end proc_write_pvmsdata;

  procedure proc_write_iraorgan(p_i_date    in varchar2,
                                p_o_succeed out varchar2) is
    v_outputfile UTL_FILE.file_type;
    in_file      VARCHAR(50); --文件名
    v_string     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    tmp_string   VARCHAR2(4000); --存储文件中单行数据
    v_date       varchar2(8);
    cursor c is
      select * from prm_ira_organ ;
  begin
    select to_char(sysdate, 'YYYYMMDD') into v_date from dual;
    in_file      := 'IRAORGAN0000000000.bin';
    v_outputfile := utl_file.fopen('CPMG_PV', in_file, 'w', 32767);
    for v_c in c loop
      v_string   := nvl(v_c.zone_no, ' '); --地区号
      tmp_string := LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.br_no, ' '); --网点号
      tmp_string := tmp_string || LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.act_br_no, ' '); --核算网点号
      tmp_string := tmp_string || LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.bank_flag, ' '); --行级别
      tmp_string := tmp_string || LPAD(v_string, 1, ' ');
      v_string   := nvl(v_c.organ_name, ' '); --机构名称
      tmp_string := tmp_string || LPAD(v_string, 60, ' ');
      v_string   := nvl(v_c.belong_zoneno, ' '); --上级地区号
      tmp_string := tmp_string || LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.belong_brno, ' '); --上级网点号
      tmp_string := tmp_string || LPAD(v_string, 5, ' ');
      v_string   := nvl(v_c.belong_bank_flag, ' '); --日期
      tmp_string := tmp_string || LPAD(v_string, 1, ' ');
      v_string   := nvl(v_c.br_type, ' '); --网点类型
      tmp_string := tmp_string || LPAD(v_string, 1, ' ');
      v_string   := nvl(v_c.start_date, ' '); --有效起始日期
      tmp_string := tmp_string || LPAD(v_string, 8, ' ');
      v_string   := nvl(v_c.end_date, ' '); --有效截止日期
      tmp_string := tmp_string || LPAD(v_string, 8, ' ');
      utl_file.put_line(v_outputfile, convert(tmp_string, 'ZHS16GBK'));
    end loop;
    UTL_FILE.fflush(v_outputfile);
    UTL_FILE.fclose(v_outputfile);
    p_o_succeed := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_o_succeed := '生成' || in_file || '文件出错:' || ' 错误号:' ||
                     SQLCODE || SQLERRM;
      RETURN;
  end proc_write_iraorgan;
  procedure proc_write_datactl(p_i_date    in varchar2,
                               p_o_succeed out varchar2) is
    v_outputfile UTL_FILE.file_type;
    in_file      VARCHAR(50); --文件名
    v_string     VARCHAR2(4000);

  begin
   
    p_o_succeed := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_o_succeed := '生成' || in_file || '文件出错:' || ' 错误号:' ||
                     SQLCODE || SQLERRM;
      RETURN;
  end proc_write_datactl;
  ---------------------------
  procedure proc_write_statcode(p_i_date    in varchar2,
                                p_o_succeed out varchar2) is
    v_outputfile UTL_FILE.file_type;
    in_file      VARCHAR(50); --文件名
    v_string     VARCHAR2(4000);
    tmp_string   VARCHAR2(4000); --存储文件中单行数据
    cursor c is
      select * from prm_subcode_statcode;
  begin
    in_file      := 'STATCODE0000000000.bin';
    v_outputfile := utl_file.fopen('CPMG_PV', in_file, 'w', 32767);
    for v_c in c loop
      v_string   := nvl(v_c.start_date, ' '); --起始日期
      tmp_string := LPAD(v_string, 8, ' ');
      v_string   := nvl(v_c.end_date, ' '); --结束日期
      tmp_string := tmp_string || LPAD(v_string, 8, ' ');
      v_string   := nvl(v_c.subcode, ' '); --科目
      tmp_string := tmp_string || LPAD(v_string, 4, ' ');
      v_string   := nvl(v_c.statcode, ' '); --统计代码
      tmp_string := tmp_string || LPAD(v_string, 5, ' ');
      utl_file.put_line(v_outputfile, convert(tmp_string, 'ZHS16GBK'));
    end loop;
    UTL_FILE.fflush(v_outputfile);
    UTL_FILE.fclose(v_outputfile);
    p_o_succeed := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_o_succeed := '生成' || in_file || '文件出错:' || ' 错误号:' ||
                     SQLCODE || SQLERRM;
      RETURN;
  end proc_write_statcode;
end PACK_WRITE_PVDATA;
//


CREATE OR REPLACE PACKAGE BODY PACK_WRITE_PVDATA_11 IS
  PROCEDURE PROC_WRITE_PVMSDATA(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT * FROM DW_MOD_PVMSDATA_11;
   BEGIN
     IN_FILE      := 'JJZBHZ0000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);
    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.ZONENO, ' '); --地区号
      TMP_STRING := LPAD(V_STRING, 5, ' ');
      V_STRING   := NVL(V_C.BRNO, ' '); --网点号
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 5, ' ');
      V_STRING   := NVL(V_C.BANKFLAG, ' '); --行级别
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 1, ' ');
      V_STRING   := NVL(V_C.CURRTYPE, ' '); --币种
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 3, ' ');
      V_STRING   := NVL(TO_CHAR(V_C.ECOST), ' '); --经济资本
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 28, ' ');
      V_STRING   := NVL(V_C.DATA_DATE, ' '); --日期
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 8, ' ');
      V_STRING   := NVL(V_C.DICCODE, ' '); --字典值
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 3, ' ');
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE ||
                     SQLERRM;
      RETURN;
  END PROC_WRITE_PVMSDATA;
  ---------------------------------------------------------------生成科目和产品号的报表
  PROCEDURE PROC_WRITE_PRODUCTREF(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT * FROM DM_CS_PRODUCT;
   BEGIN
     IN_FILE      := 'KMCPDY0000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);
    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.SUBJECT_CODE, ' '); --科目号
      TMP_STRING := LPAD(V_STRING, 6, ' ');
      V_STRING   := NVL(V_C.PRODUCT_ID, ' '); --产品号
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(V_C.PRODUCT_NAME, ' '); --科目名称
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 100, ' ');
      V_STRING   := NVL(V_C.START_DATE, ' '); --开始时间
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 8, ' ');
      V_STRING   := NVL(V_C.END_DATE, ' '); --结束时间
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 8, ' ');
       UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE ||
                     SQLERRM;
      RETURN;
  END PROC_WRITE_PRODUCTREF;
   ---------------------------------------------------------------
  PROCEDURE PROC_WRITE_DATACTL(P_I_DATE    IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000);
   BEGIN
    IN_FILE      := 'datactl.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);
    V_STRING     := 'IRA风险综合分析                                                ' ||
                    P_I_DATE || '3  14   ';
    UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(V_STRING, 'ZHS16GBK'));
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE ||
                     SQLERRM;
      RETURN;
  END PROC_WRITE_DATACTL;
   PROCEDURE PROC_WRITE_PJZB(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT innerno,'001' currtype,ec  FROM dw_mod_pjtx  WHERE start_date=p_i_date /*AND end_date>p_i_date*/;
   BEGIN
     IN_FILE      := 'CAP_PJZB0000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);
    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.Innerno, ' '); --内部票号
      TMP_STRING := RPAD(V_STRING, 60, ' ');
      V_STRING   := NVL(V_C.Currtype, ' '); -- 币种
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 3, ' ');
      V_STRING   := NVL(to_char(V_C.ec), ' '); --经济资本
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 26, ' ') || CHR(13);
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE ||
                     SQLERRM;
      RETURN;
  END PROC_WRITE_PJZB;
  PROCEDURE PROC_WRITE_CKXS(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT A.SUBJECT_CODE, B.QUOTIETY
        FROM PRM_CS10 A, PRM_XS10 B
       WHERE A.PRODUCT_ID = B.THPRODUCT
         AND A.START_DATE <= P_I_DATE
         AND A.END_DATE > P_I_DATE
         AND B.START_DATE <= P_I_DATE
         AND B.END_DATE > P_I_DATE
         AND A.SUBJECT_APP = 1;
     CURSOR C_SUBXS IS
      SELECT A.SUBJECT_CODE, A.SUBJECT_TYPE, B.QUOTIETY     --银行卡透支
        FROM PRM_CS081 A, PRM_XS081 B
       WHERE A.PRODUCT_ID = B.THPRODUCT
         AND A.START_DATE <= P_I_DATE
         AND A.END_DATE > P_I_DATE
         AND B.START_DATE <= P_I_DATE
         AND B.END_DATE > P_I_DATE
         AND A.SUBJECT_APP = 1
   union all
      SELECT A.SUBJECT_CODE, A.SUBJECT_TYPE, B.PZXS         --债券投资
        FROM PRM_CS061 A, PRM_XS061 B
       WHERE A.PRODUCT_ID = B.PRODUCT_ID
         AND A.START_DATE <= P_I_DATE
         AND A.END_DATE > P_I_DATE
         AND B.START_DATE <= P_I_DATE
         AND B.END_DATE > P_I_DATE
         AND A.SUBJECT_APP = 1
   union all
      SELECT A.SUBJECT_CODE, A.SUBJECT_TYPE, B.PAR_COEF     --资金业务
        FROM PRM_CS05A A, PRM_XSZJ1 B
       WHERE A.PRODUCT_ID = B.PRODUCT_ID
         AND A.START_DATE <= P_I_DATE
         AND A.END_DATE > P_I_DATE
         AND B.START_DATE <= P_I_DATE
         AND B.END_DATE > P_I_DATE
         AND A.SUBJECT_APP = 1
   union all
      SELECT A.SUBJECT_CODE, A.SUBJECT_TYPE, B.XS_VALUE     --表外资产
        FROM PRM_CS05C A, PRM_XS051 B
       WHERE A.PRODUCT_ID = B.PRODUCT_NAME3
         AND A.START_DATE <= P_I_DATE
         AND A.END_DATE > P_I_DATE
         AND B.START_DATE <= P_I_DATE
         AND B.END_DATE > P_I_DATE
         AND A.SUBJECT_APP = 1;
   BEGIN
     IN_FILE      := 'CAP_CKXS0000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);
    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.SUBJECT_CODE, ' '); --三级产品
      TMP_STRING := RPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(to_char(V_C.Quotiety), ' '); --系数
      TMP_STRING := TMP_STRING || LPAD(V_STRING,9, ' ')|| CHR(13);
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';
     IN_FILE      := 'SUB_XS0000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);
    FOR V_C IN C_SUBXS LOOP
      V_STRING   := NVL(V_C.SUBJECT_CODE, ' ');         --科目号
      TMP_STRING := RPAD(V_STRING, 8, ' ');
      V_STRING   := NVL(V_C.SUBJECT_TYPE, ' ');         --科目级别
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(TO_CHAR(V_C.QUOTIETY), ' ');    --科目系数
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 9, ' ') || CHR(13);
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;
    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);
    P_O_SUCCEED := '0';
   EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE ||
                     SQLERRM;
      RETURN;
  END PROC_WRITE_CKXS;

  PROCEDURE PROC_WRITE_PRMPRODUCT(P_I_DATE    IN VARCHAR2,
                                  P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT PRODUCT_ID, PRODUCT_LEVEL, PRODUCT_NAME, PARENT_ID
        FROM PRM_PRODUCT
       WHERE START_DATE <= P_I_DATE
         AND END_DATE > P_I_DATE;
  BEGIN
    IN_FILE      := 'CAP_PRM_PRODUCT0000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);

    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.PRODUCT_ID, ' ');
      TMP_STRING := RPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(V_C.PRODUCT_LEVEL, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 1, ' ');
      V_STRING   := NVL(V_C.PRODUCT_NAME, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 100, ' ');
      V_STRING   := NVL(V_C.PARENT_ID, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 10, ' ') || CHR(13);
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;

    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);

    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE || SQLERRM;
      RETURN;
  END PROC_WRITE_PRMPRODUCT;

  PROCEDURE PROC_WRITE_PRMCSPJ2(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT QXDC_ID, QXDC_NAME, LEFT_VALUE, RIGHT_VALUE, LEFT_IN, RIGHT_IN
        FROM PRM_CSPJ2
       WHERE START_DATE <= P_I_DATE
         AND END_DATE > P_I_DATE;
  BEGIN
    IN_FILE      := 'CAP_PRM_CSPJ20000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);

    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.QXDC_ID, ' ');
      TMP_STRING := RPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(V_C.QXDC_NAME, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 40, ' ');
      V_STRING   := NVL(TO_CHAR(V_C.LEFT_VALUE), ' ');
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 6, ' ');
      V_STRING   := NVL(TO_CHAR(V_C.RIGHT_VALUE), ' ');
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 6, ' ');
      V_STRING   := NVL(V_C.LEFT_IN, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 1, ' ');
      V_STRING   := NVL(V_C.RIGHT_IN, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 1, ' ') || CHR(13);
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;

    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);

    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE || SQLERRM;
      RETURN;
  END PROC_WRITE_PRMCSPJ2;

  PROCEDURE PROC_WRITE_PRMXS071(P_I_DATE    IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2) IS
    V_OUTPUTFILE UTL_FILE.FILE_TYPE;
    IN_FILE      VARCHAR(50); --文件名
    V_STRING     VARCHAR2(4000); --处理每条游标存储的数据中单个需要判断的字段
    TMP_STRING   VARCHAR2(4000); --存储文件中单行数据
    CURSOR C IS
      SELECT PRODUCT_ID, QXDC_ID, PAR_COEF
        FROM PRM_XS071
       WHERE START_DATE <= P_I_DATE
         AND END_DATE > P_I_DATE;
  BEGIN
    IN_FILE      := 'CAP_PRM_XS0710000000000.bin';
    V_OUTPUTFILE := UTL_FILE.FOPEN('CPMG_PV', IN_FILE, 'w', 32767);

    FOR V_C IN C LOOP
      V_STRING   := NVL(V_C.PRODUCT_ID, ' ');
      TMP_STRING := RPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(V_C.QXDC_ID, ' ');
      TMP_STRING := TMP_STRING || RPAD(V_STRING, 10, ' ');
      V_STRING   := NVL(TO_CHAR(V_C.PAR_COEF), ' ');
      TMP_STRING := TMP_STRING || LPAD(V_STRING, 10, ' ') || CHR(13);
      UTL_FILE.PUT_LINE(V_OUTPUTFILE, convert(TMP_STRING, 'ZHS16GBK'));
    END LOOP;

    UTL_FILE.FFLUSH(V_OUTPUTFILE);
    UTL_FILE.FCLOSE(V_OUTPUTFILE);

    P_O_SUCCEED := '0';
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      P_O_SUCCEED := '生成' || IN_FILE || '文件出错:' || ' 错误号:' || SQLCODE || SQLERRM;
      RETURN;
  END PROC_WRITE_PRMXS071;
END PACK_WRITE_PVDATA_11;
//

delimiter ;//