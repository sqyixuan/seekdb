create table MIC.AB_EXERCISE_APP
(
  business_id          VARCHAR2(20) not null,
  valid_flag           VARCHAR2(1),
  applicant            VARCHAR2(9) not null,
  app_stru             VARCHAR2(10) not null,
  create_time          DATE not null,
  update_time          DATE not null,
  close_time           DATE,
  stru_id              VARCHAR2(10) not null,
  exe_time             DATE,
  exe_place            VARCHAR2(200),
  people_or_num        VARCHAR2(4000),
  content              VARCHAR2(4000),
  purpose              VARCHAR2(2000),
  procedure            VARCHAR2(4000),
  summery              VARCHAR2(4000),
  problem_and_solution VARCHAR2(4000),
  attach_file_ref_id   VARCHAR2(20),
  remark               VARCHAR2(4000),
  attend_stru          VARCHAR2(4000)
)
;
create table MIC.AB_EXERCISE_DRF
(
  business_id          VARCHAR2(20) not null,
  valid_flag           VARCHAR2(1),
  applicant            VARCHAR2(9) not null,
  app_stru             VARCHAR2(10) not null,
  save_time            DATE not null,
  stru_id              VARCHAR2(10) not null,
  exe_time             DATE,
  exe_place            VARCHAR2(200),
  people_or_num        VARCHAR2(4000),
  content              VARCHAR2(4000),
  purpose              VARCHAR2(2000),
  procedure            VARCHAR2(4000),
  summery              VARCHAR2(4000),
  problem_and_solution VARCHAR2(4000),
  attach_file_ref_id   VARCHAR2(20),
  remark               VARCHAR2(4000),
  attend_stru          VARCHAR2(4000)
)
;
create table MIC.AB_EXERCISE_STRU
(
  business_id VARCHAR2(20) not null,
  stru_id     VARCHAR2(10) not null,
  status      VARCHAR2(1) not null,
  update_time DATE not null
)
;
create table MIC.AB_RPT_RSK_APP
(
  rpt_rsk_app_id          VARCHAR2(20) not null,
  rpt_rsk_app_type        VARCHAR2(1) not null,
  status                  VARCHAR2(1) not null,
  emergency_degree        VARCHAR2(1),
  serious_degree          VARCHAR2(1),
  risk_type               VARCHAR2(5),
  biz_line                VARCHAR2(5),
  handle_requirement      VARCHAR2(4000),
  written_date            VARCHAR2(8),
  create_time             DATE not null,
  update_time             DATE not null,
  close_time              DATE,
  event_title             VARCHAR2(1000),
  event_time              DATE,
  event_content           CLOB,
  event_handle_advice     NUMBER(8),
  news_media              VARCHAR2(1000),
  news_republish_and_link CLOB,
  post_carrier            VARCHAR2(1000),
  post_reply_number       VARCHAR2(50),
  post_is_multiple        VARCHAR2(1),
  post_publisher_id       VARCHAR2(1000),
  post_ip                 VARCHAR2(100),
  post_link               CLOB,
  problem_source          VARCHAR2(4000),
  attach_file_ref_id      VARCHAR2(20),
  post_republish_media    VARCHAR2(4000),
  post_republish_number   VARCHAR2(50),
  is_foreign_related      VARCHAR2(5)
)
;
create table MIC.AB_RPT_RSK_APP_WF
(
  business_id                 VARCHAR2(20) not null,
  source_business_id          VARCHAR2(20),
  rpt_rsk_app_id              VARCHAR2(20) not null,
  stru_id                     VARCHAR2(10) not null,
  status                      VARCHAR2(1) not null,
  applicant                   VARCHAR2(9),
  app_stru                    VARCHAR2(10),
  create_time                 DATE not null,
  update_time                 DATE not null,
  urge_time                   DATE,
  close_time                  DATE,
  feedback_is_truth           VARCHAR2(1),
  feedback_treatment          VARCHAR2(4000),
  feedback_response_method    VARCHAR2(4000),
  feedback_status             VARCHAR2(1),
  handle_requirement          VARCHAR2(4000),
  feedback_attach_file_ref_id VARCHAR2(20),
  feedback_situation          CLOB,
  feedback_result             CLOB,
  appraise_content            CLOB,
  appraise_event_strus        VARCHAR2(4000),
  appraise_is_solved          VARCHAR2(1),
  appraise_has_improvement    VARCHAR2(1),
  appraise_solution           VARCHAR2(1),
  appraise_remind_situation   VARCHAR2(1),
  appraise_is_report_cbrc     VARCHAR2(1),
  appraise_spread_situation   CLOB,
  appraise_remarks            CLOB,
  appraise_feedback_situation VARCHAR2(1),
  appraise_update_time        DATE
)
;
create table MIC.COM_BLOB_CONTENTS
(
  blob_uid     VARCHAR2(50) not null,
  content_id   VARCHAR2(50) not null,
  content_type VARCHAR2(2) not null,
  content_blob BLOB,
  file_name    VARCHAR2(1500),
  content_desc VARCHAR2(4000),
  content_date DATE not null,
  reserved1    VARCHAR2(1000),
  reserved2    VARCHAR2(1000)
)
;

CREATE TABLE "MIC"."CTP_BRANCH" (
	"ID" VARCHAR2(20 BYTE) NOT NULL,
	"STATUS" VARCHAR2(2 BYTE) DEFAULT 1  NOT NULL,
	"REGION_ID" VARCHAR2(20 BYTE),
	"NET_TERMINAL" VARCHAR2(20 BYTE),
	"PARENT_ID" VARCHAR2(20 BYTE),
	"BRANCH_LEVEL" VARCHAR2(3 BYTE) DEFAULT 1  NOT NULL,
	"MODI_USER" VARCHAR2(20 BYTE),
	"MODI_TIME" VARCHAR2(8 BYTE),
	"BRANCH_CATEGORY" VARCHAR2(1 BYTE),
	"FINACE_ID" VARCHAR2(40 BYTE),
	"ZIPCODE" VARCHAR2(6 BYTE),
	"PHONE" VARCHAR2(40 BYTE),
	"SIGN" VARCHAR2(12 BYTE),
	"ADMIN_LEVEL" VARCHAR2(4 BYTE),
	"OPEN_TIME" VARCHAR2(6 BYTE),
	"LAST_ALT_TYPE" VARCHAR2(16 BYTE),
	"BALANCE_ID" VARCHAR2(36 BYTE),
	"CODECERT_ID" VARCHAR2(36 BYTE),
	"REVOKE_TIME" VARCHAR2(6 BYTE),
	CONSTRAINT "PKCTP_BRANCH" PRIMARY KEY ("ID"));


create table MIC.CTP_ITEM
(
  id             VARCHAR2(8) not null,
  status         VARCHAR2(2) not null,
  url            VARCHAR2(1024) not null,
  item_type      VARCHAR2(30),
  item_level     VARCHAR2(100),
  item_branch_id VARCHAR2(20)
)
;
create table MIC.CTP_ITEM_NLS
(
  id            VARCHAR2(8) not null,
  name          VARCHAR2(50),
  description   VARCHAR2(500),
  default_label VARCHAR2(40),
  locale        VARCHAR2(20) not null
)
;
create table MIC.CTP_MX_APP_INFO
(
  appname_en VARCHAR2(50) not null,
  appname_zh VARCHAR2(50),
  version    VARCHAR2(100),
  type       VARCHAR2(2)
)
;
create table MIC.CTP_MX_MONITOR_FLAG
(
  hostname    VARCHAR2(50) not null,
  switchtype  VARCHAR2(2) not null,
  channeltype VARCHAR2(2) not null,
  action      VARCHAR2(2),
  modifyuser  VARCHAR2(50),
  midifydate  DATE
)
;
create table MIC.CTP_MX_RESOURCE_INFO
(
  resourcecode VARCHAR2(50) not null,
  resourcename VARCHAR2(100),
  resourcerate NUMBER,
  type         VARCHAR2(2),
  monitor      VARCHAR2(200),
  ismonitor    CHAR(1)
)
;
create table MIC.CTP_MX_RESOURCE_LEVEL_DEFINE
(
  resourcecode VARCHAR2(50) not null,
  eventlevel   VARCHAR2(2) not null,
  occurtime    NUMBER
)
;
create table MIC.CTP_MX_SERVER_IP
(
  hostname VARCHAR2(50) not null,
  ip       VARCHAR2(20)
)
;
create table MIC.CTP_MX_SERVER_PORT
(
  hostname VARCHAR2(50) not null,
  port     NUMBER not null
)
;
create table MIC.CTP_MX_STAT_OPAVGTIME_INFO
(
  hostname  VARCHAR2(50),
  indextype VARCHAR2(4),
  opname    VARCHAR2(100),
  statdate  DATE,
  optime    NUMBER,
  opcount   NUMBER
)
;
create table MIC.CTP_MX_STAT_PEAK_INFO
(
  hostname  VARCHAR2(50),
  indextype VARCHAR2(4),
  statdate  DATE,
  peakvalue NUMBER
)
;
create table MIC.CTP_MX_TRANS_ERROR_INFO
(
  eventcode VARCHAR2(50) not null,
  eventname VARCHAR2(100),
  ismonitor CHAR(1)
)
;
create table MIC.CTP_MX_TRANS_ERROR_LEVEL
(
  eventcode  VARCHAR2(50) not null,
  trancode   VARCHAR2(50) not null,
  eventlevel VARCHAR2(2) not null,
  occurtime  NUMBER
)
;
create table MIC.CTP_MX_USABILITY_INFO
(
  name          VARCHAR2(50) not null,
  modulecode    VARCHAR2(50),
  submodulecode VARCHAR2(50),
  checker       VARCHAR2(200),
  status        VARCHAR2(2),
  msg           VARCHAR2(200),
  ismonitor     CHAR(1)
)
;
create table MIC.CTP_PROC_LOG
(
  porc_name VARCHAR2(100),
  log_time  VARCHAR2(100),
  log_info  CLOB
)
;
create table MIC.CTP_ROLE
(
  id               VARCHAR2(20) not null,
  lst_modi_time    VARCHAR2(8),
  role_level       VARCHAR2(100),
  branch_id        VARCHAR2(20),
  role_level_admin VARCHAR2(100),
  branch_id_admin  VARCHAR2(20),
  role_category    VARCHAR2(1) not null,
  lst_modi_user_id VARCHAR2(20),
  privilege_all    VARCHAR2(1),
  privilege_self   VARCHAR2(1),
  privilege_other  VARCHAR2(2),
  privilege        VARCHAR2(1)
)
;
CREATE TABLE "MIC"."CTP_ROLE_USER_REL" (
	"ROLE_ID" VARCHAR2(20 BYTE) NOT NULL,
	"USER_ID" VARCHAR2(20 BYTE) NOT NULL,
	"MENUCHG_FLAG" CHAR(1 BYTE) DEFAULT 1,
	"MENUSTR" CLOB,
	"LOCALE" VARCHAR2(20 BYTE) NOT NULL,
	CONSTRAINT "PKCTP_ROLE_USER_REL" PRIMARY KEY ("ROLE_ID", "USER_ID", "LOCALE")
);
create table MIC.CTP_USER
(
  id               VARCHAR2(20) not null,
  branch_id        VARCHAR2(20) not null,
  status           VARCHAR2(2) not null,
  password         VARCHAR2(30),
  user_level       VARCHAR2(2),
  phone_no         VARCHAR2(20),
  email            VARCHAR2(50),
  postcode         VARCHAR2(6),
  cert_type        VARCHAR2(30),
  cert_no          VARCHAR2(30),
  freeze_date      VARCHAR2(8),
  fail_num         VARCHAR2(2),
  mobile           VARCHAR2(13),
  lst_modi_time    VARCHAR2(8),
  lst_modi_user_id VARCHAR2(20),
  privilege_all    VARCHAR2(1),
  privilege_self   VARCHAR2(1),
  privilege_other  VARCHAR2(2),
  user_category    VARCHAR2(1)
)
;
create table MIC.CTP_USER_NLS
(
  id          VARCHAR2(20) not null,
  name        VARCHAR2(90),
  description VARCHAR2(500),
  address     VARCHAR2(500),
  locale      VARCHAR2(20) not null
)
;
create table MIC.CTP_USER_SHORTCUT_MENU
(
  user_id VARCHAR2(20) not null,
  role_id VARCHAR2(20),
  item_id VARCHAR2(8) not null
)
;
create table MIC.CTP_USER_ZOOM
(
  id          VARCHAR2(20) not null,
  zoom        BLOB,
  zoomcut     BLOB,
  defaultrole VARCHAR2(20),
  locale      VARCHAR2(20) not null
)
;
create table MIC.DAMS_ACCREDIT_MAIN
(
  p_key           NUMBER(19) not null,
  sys_code        VARCHAR2(10) not null,
  source_user_id  VARCHAR2(10) not null,
  current_user_id VARCHAR2(10) not null,
  role_id         VARCHAR2(20) not null,
  bnch_id         VARCHAR2(10) not null,
  beg_date        VARCHAR2(14),
  end_date        VARCHAR2(14),
  state           VARCHAR2(1) not null,
  s_key           NUMBER(19),
  mail_flag       VARCHAR2(1),
  obj_id          VARCHAR2(20),
  obj_name        VARCHAR2(500)
)
;
create table MIC.DAMS_ACL
(
  resource_id    VARCHAR2(10),
  principal_id   VARCHAR2(10),
  principal_type VARCHAR2(20),
  permission     VARCHAR2(20),
  resource_type  VARCHAR2(20)
)
;
create table MIC.DAMS_ASSIST_USER
(
  task_id             NUMBER(19) not null,
  assist_user_id      VARCHAR2(9) not null,
  business_id         VARCHAR2(20),
  assist_user_comment VARCHAR2(4000),
  status              VARCHAR2(1),
  create_user         VARCHAR2(9),
  create_time         DATE,
  update_time         DATE,
  major_user_comment  VARCHAR2(4000),
  stru_id             VARCHAR2(10)
)
;
create table MIC.DAMS_AUTHAPPLY_LOG
(
  key_id      VARCHAR2(20) not null,
  business_id VARCHAR2(20) not null,
  point_name  VARCHAR2(120),
  copy_auth   VARCHAR2(20),
  print_auth  VARCHAR2(20),
  print_time  VARCHAR2(20),
  start_date  VARCHAR2(8),
  end_date    VARCHAR2(8),
  upd_person  VARCHAR2(20) not null,
  cret_date   VARCHAR2(14) not null
)
;
create table MIC.DAMS_AUTHORITYAPPLY_ARCHVE
(
  key_id              VARCHAR2(20) not null,
  apply_id            VARCHAR2(300),
  file_type           VARCHAR2(20),
  apply_person_id     VARCHAR2(20) not null,
  branch_id           VARCHAR2(20) not null,
  doc_type            VARCHAR2(20) not null,
  doc_id              VARCHAR2(20) not null,
  copy_auth           VARCHAR2(20),
  print_auth          VARCHAR2(20),
  apply_start_date    VARCHAR2(8),
  apply_end_date      VARCHAR2(8),
  print_time          VARCHAR2(20),
  apply_reason        VARCHAR2(4000),
  cret_date           VARCHAR2(14) not null,
  upd_date            VARCHAR2(14),
  upd_user            VARCHAR2(10),
  secure_id           VARCHAR2(20),
  doc_title           VARCHAR2(4000),
  doc_num             VARCHAR2(200),
  file_header_his     VARCHAR2(4000),
  max_order_c         VARCHAR2(10),
  max_order_p         VARCHAR2(10),
  authorized_person_c VARCHAR2(20),
  authorized_person_p VARCHAR2(20),
  read_auth           VARCHAR2(20),
  max_order_r         VARCHAR2(10),
  authorized_person_r VARCHAR2(20),
  sys_code            VARCHAR2(20),
  own_auth            VARCHAR2(20),
  has_doc             VARCHAR2(1),
  ic2_print_times     NUMBER(3),
  encrypt_type        VARCHAR2(1),
  file_header         CLOB,
  file_header2        CLOB
)
;
create table MIC.DAMS_AUTHORITYAPPLY_COMMENT
(
  key_id     VARCHAR2(20) not null,
  user_id    VARCHAR2(10) not null,
  doc_id     VARCHAR2(20),
  point_name VARCHAR2(120),
  role_id    VARCHAR2(20),
  bnch_id    VARCHAR2(10),
  coment     VARCHAR2(4000),
  type_code  VARCHAR2(20),
  cret_date  VARCHAR2(14) not null
)
;
create table MIC.DAMS_AUTHORITYAPPLY_INFO
(
  key_id              VARCHAR2(20) not null,
  apply_id            VARCHAR2(300),
  file_type           VARCHAR2(20),
  apply_person_id     VARCHAR2(20) not null,
  branch_id           VARCHAR2(20) not null,
  doc_type            VARCHAR2(20) not null,
  doc_id              VARCHAR2(20) not null,
  copy_auth           VARCHAR2(20),
  print_auth          VARCHAR2(20),
  apply_start_date    VARCHAR2(8),
  apply_end_date      VARCHAR2(8),
  print_time          VARCHAR2(20),
  apply_reason        VARCHAR2(4000),
  cret_date           VARCHAR2(14) not null,
  upd_date            VARCHAR2(14),
  upd_user            VARCHAR2(10),
  secure_id           VARCHAR2(20),
  doc_title           VARCHAR2(4000),
  doc_num             VARCHAR2(200),
  file_header_his     VARCHAR2(4000),
  max_order_c         VARCHAR2(10),
  max_order_p         VARCHAR2(10),
  authorized_person_c VARCHAR2(20),
  authorized_person_p VARCHAR2(20),
  read_auth           VARCHAR2(20),
  max_order_r         VARCHAR2(10),
  authorized_person_r VARCHAR2(20),
  auth_stat           VARCHAR2(10),
  sys_code            VARCHAR2(20),
  own_auth            VARCHAR2(20),
  has_doc             VARCHAR2(1),
  ic2_print_times     NUMBER(3),
  encrypt_type        VARCHAR2(1),
  file_header         CLOB,
  file_header2        CLOB
)
;
create table MIC.DAMS_AUTHORITYAPPLY_SET
(
  key_id        VARCHAR2(20) not null,
  system_type   VARCHAR2(20),
  duty_lv_code  VARCHAR2(20) not null,
  prm_secret_id VARCHAR2(20) not null,
  apply_type    VARCHAR2(100) not null,
  cret_date     VARCHAR2(14) not null,
  upd_time      VARCHAR2(14) not null,
  cret_person   VARCHAR2(14),
  upd_person    VARCHAR2(14),
  colume1       VARCHAR2(100),
  standard_id   VARCHAR2(100),
  branch_lv     VARCHAR2(1)
)
;
create table MIC.DAMS_BRANCH
(
  stru_id          VARCHAR2(20) not null,
  stru_fname       VARCHAR2(120) not null,
  stru_sname       VARCHAR2(120),
  oldsys_struid    VARCHAR2(20),
  flicence_id      VARCHAR2(60),
  stru_addr        VARCHAR2(180),
  zipcode          VARCHAR2(30),
  phone            VARCHAR2(60),
  stru_sign        VARCHAR2(5) not null,
  stru_lv          VARCHAR2(5),
  admin_lv         VARCHAR2(5),
  sup_stru         VARCHAR2(20),
  setup_time       VARCHAR2(10),
  lst_alt_type     VARCHAR2(5),
  lst_alt_time     VARCHAR2(10),
  stru_state       VARCHAR2(5),
  blicence_id      VARCHAR2(54),
  revoke_time      VARCHAR2(10),
  dist_sign        VARCHAR2(5),
  stru_grade       VARCHAR2(5),
  codecert_id      VARCHAR2(54),
  town_flag        VARCHAR2(5),
  busi_area        VARCHAR2(20),
  busi_site_use    VARCHAR2(5),
  fexchange_flag   VARCHAR2(5),
  man_grade        VARCHAR2(5),
  charge_prop      VARCHAR2(5),
  profession_level VARCHAR2(5),
  node_type        VARCHAR2(5),
  econ_area        VARCHAR2(5),
  is_hun_city      VARCHAR2(5),
  is_hun_county    VARCHAR2(5),
  country          VARCHAR2(150),
  village          VARCHAR2(60),
  np_oper_type     VARCHAR2(5),
  manage_stru_id   VARCHAR2(20),
  specialty_prop   VARCHAR2(120),
  sort             NUMBER(17),
  stru_chname      VARCHAR2(150),
  bussdept_type    VARCHAR2(150),
  manage_type      VARCHAR2(1),
  gw_manage_type   VARCHAR2(1),
  stru_sort        NUMBER(10,2),
  short_name       VARCHAR2(120)
)
;

create table MIC.DAMS_BRANCH_CATALOG
(
  stru_id       VARCHAR2(10) not null,
  major_stru_id VARCHAR2(10),
  dept_stru_id  VARCHAR2(10),
  lead_stru_id  VARCHAR2(10),
  stru_level    VARCHAR2(1),
  is_leaf       VARCHAR2(1)
)
;
create table MIC.DAMS_BRANCH_CFG
(
  stru_id      VARCHAR2(10) not null,
  dept_stru_id VARCHAR2(10),
  reserve1     VARCHAR2(300),
  reserve2     VARCHAR2(300),
  reserve3     VARCHAR2(300)
)
;
create table MIC.DAMS_BRANCH_REL
(
  stru_id          VARCHAR2(20) not null,
  stru_fname       VARCHAR2(120),
  stru_sname       VARCHAR2(120),
  sys_code         VARCHAR2(20) not null,
  oldsys_struid    VARCHAR2(20),
  flicence_id      VARCHAR2(60),
  stru_addr        VARCHAR2(180),
  zipcode          VARCHAR2(30),
  phone            VARCHAR2(60),
  stru_sign        VARCHAR2(5),
  stru_lv          VARCHAR2(5),
  admin_lv         VARCHAR2(5),
  sup_stru         VARCHAR2(20),
  setup_time       VARCHAR2(10),
  lst_alt_type     VARCHAR2(5),
  lst_alt_time     VARCHAR2(10),
  stru_state       VARCHAR2(5),
  blicence_id      VARCHAR2(54),
  revoke_time      VARCHAR2(10),
  dist_sign        VARCHAR2(5),
  stru_grade       VARCHAR2(5),
  codecert_id      VARCHAR2(54),
  town_flag        VARCHAR2(5),
  busi_area        VARCHAR2(20),
  busi_site_use    VARCHAR2(5),
  fexchange_flag   VARCHAR2(5),
  man_grade        VARCHAR2(5),
  charge_prop      VARCHAR2(5),
  profession_level VARCHAR2(5),
  node_type        VARCHAR2(5),
  econ_area        VARCHAR2(5),
  is_hun_city      VARCHAR2(5),
  is_hun_county    VARCHAR2(5),
  country          VARCHAR2(150),
  village          VARCHAR2(60),
  np_oper_type     VARCHAR2(5),
  manage_stru_id   VARCHAR2(20),
  specialty_prop   VARCHAR2(120),
  modtime          VARCHAR2(8),
  descr            VARCHAR2(2000)
)
;
create table MIC.DAMS_BRANCH_RELATION
(
  bnch_id           VARCHAR2(10) not null,
  bnch_lv           VARCHAR2(3),
  bnch_sign         VARCHAR2(3),
  bnch_grade        VARCHAR2(5),
  bnch_man_grade    VARCHAR2(5),
  up_bnch_id        VARCHAR2(10) not null,
  up_bnch_lv        VARCHAR2(3),
  up_bnch_sign      VARCHAR2(3),
  up_bnch_grade     VARCHAR2(5),
  up_bnch_man_grade VARCHAR2(5),
  up_level          NUMBER(2),
  gw_manage_type    VARCHAR2(1),
  up_gw_manage_type VARCHAR2(1)
)
;

create table MIC.DAMS_BRANCH_SPEC_PROP
(
  stru_id   VARCHAR2(10) not null,
  spec_prop VARCHAR2(5) not null,
  upd_user  VARCHAR2(9),
  upd_time  DATE
)
;
create table MIC.DAMS_BRANCH_SYS_REL
(
  stru_id    VARCHAR2(10) not null,
  sys_code   VARCHAR2(10) not null,
  sys_name   VARCHAR2(200),
  stru_sname VARCHAR2(120),
  start_use  VARCHAR2(1)
)
;
create table MIC.DAMS_CHARGE_INFO
(
  model_id    VARCHAR2(5) not null,
  stru_id     VARCHAR2(10) not null,
  ssic_id     VARCHAR2(9) not null,
  sys_code    VARCHAR2(20) not null,
  status      VARCHAR2(1),
  create_time DATE,
  create_user VARCHAR2(9),
  update_time DATE,
  update_user VARCHAR2(9)
)
;
create table MIC.DAMS_CHARGE_MODEL_REL
(
  model_id    VARCHAR2(5) not null,
  sys_code    VARCHAR2(20) not null,
  role_id     VARCHAR2(10) not null,
  create_time DATE
)
;
CREATE TABLE "MIC"."DAMS_FILE" (
	"FILE_ID" VARCHAR2(20 BYTE) NOT NULL,
	"FILE_NAME" VARCHAR2(1000 BYTE) NOT NULL,
	"FILE_CONTENT" BLOB NOT NULL,
	"CREATE_TIME" DATE DEFAULT sysdate  NOT NULL,
	"CREATE_USER" VARCHAR2(9 BYTE),
	CONSTRAINT "PK_DAMS_FILE" PRIMARY KEY ("FILE_ID")
);

create table MIC.DAMS_FILE_REF
(
  file_ref_id VARCHAR2(20) not null,
  file_id     VARCHAR2(20) not null,
  create_time DATE not null,
  create_user VARCHAR2(9),
  source_type VARCHAR2(10),
  status      VARCHAR2(1)
)
;
create table MIC.DAMS_FORM
(
  form_id   VARCHAR2(255) not null,
  form_name VARCHAR2(255),
  form_url  VARCHAR2(255),
  sys_code  VARCHAR2(20),
  form_desc VARCHAR2(255),
  read_url  VARCHAR2(255),
  app_url   VARCHAR2(255),
  res_url   VARCHAR2(255)
)
;
create table MIC.DAMS_GEN_DICT
(
  dicttype       VARCHAR2(50),
  dictcode       VARCHAR2(20),
  dictvalue      VARCHAR2(100),
  defaultvalue   VARCHAR2(100),
  canmodify_code VARCHAR2(1),
  dictdescript   VARCHAR2(200),
  dictlang       VARCHAR2(4000),
  parent_code    VARCHAR2(20),
  sys_code       VARCHAR2(10),
  valid_flag     VARCHAR2(1),
  order_no       NUMBER(3)
)
;
create table MIC.DAMS_HOLIDAY_SET
(
  key_id       VARCHAR2(20) not null,
  native_code  VARCHAR2(4),
  holiday_time VARCHAR2(8) not null,
  holiday_name VARCHAR2(100),
  remark       VARCHAR2(4000),
  cret_time    VARCHAR2(8),
  holiday_type VARCHAR2(2),
  stru_id      VARCHAR2(10)
)
;
create table MIC.DAMS_INNER_TODO_TASK
(
  task_id        VARCHAR2(20) not null,
  ssic_id        VARCHAR2(9) not null,
  prev_task_name VARCHAR2(150),
  prev_ssic_id   VARCHAR2(9),
  create_time    DATE,
  take_time      DATE,
  end_time       DATE,
  task_state     VARCHAR2(20),
  business_id    VARCHAR2(20) not null,
  business_type  VARCHAR2(50),
  title          VARCHAR2(400),
  creator_id     VARCHAR2(9),
  sys_code       VARCHAR2(10) not null,
  url            VARCHAR2(500),
  in_sys_seq     VARCHAR2(300),
  creator_stru   VARCHAR2(10)
)
;
CREATE TABLE "MIC"."DAMS_MAIL" (
	"MAIL_ID" VARCHAR2(12 BYTE) NOT NULL,
	"MAIL_TO" VARCHAR2(300 BYTE),
	"MAIL_CC" VARCHAR2(300 BYTE),
	"MAIL_BCC" VARCHAR2(300 BYTE),
	"SUBJECT" VARCHAR2(1000 BYTE),
	"CONTENT" CLOB,
	"ATTACH_ADDR" VARCHAR2(300 BYTE),
	"ATTACH_TYPE" VARCHAR2(1 BYTE),
	"ATTACH_BODY" BLOB,
	"SEND_FLAG" VARCHAR2(1 BYTE) DEFAULT '0',
	"HOST_IP" VARCHAR2(15 BYTE),
	"LOAD_TIME" TIMESTAMP(6),
	"SEND_TIME" TIMESTAMP(6),
	"MAIL_TYPE" VARCHAR2(3 BYTE),
	"TEMPLATE_ID" NUMBER,
	"TEMPLATE_DATA" CLOB,
	CONSTRAINT "PK_DAMS_MAIL" PRIMARY KEY ("MAIL_ID")
);


create table MIC.DAMS_MAIL_TEMPLATE
(
  tpl_id      NUMBER not null,
  tpl_subject VARCHAR2(1000),
  tpl_content VARCHAR2(4000),
  descr       VARCHAR2(1000),
  language    VARCHAR2(10),
  is_html     VARCHAR2(1)
)
;
create table MIC.DAMS_MAIL_TOKEN
(
  token_id    VARCHAR2(40) not null,
  state       VARCHAR2(10),
  ssic_id     VARCHAR2(20) not null,
  method      VARCHAR2(20),
  url_param   VARCHAR2(500),
  create_time VARCHAR2(20),
  dealed_time VARCHAR2(20),
  expire_time VARCHAR2(20)
)
;
create table MIC.DAMS_MENU
(
  menu_id     VARCHAR2(10) not null,
  parent_id   VARCHAR2(10),
  isleaf      VARCHAR2(1),
  serialno    VARCHAR2(10),
  menuinvoke  VARCHAR2(500),
  sortno      NUMBER(4),
  sys_code    VARCHAR2(10),
  status      VARCHAR2(1),
  creator     VARCHAR2(10),
  create_time VARCHAR2(8),
  lastupdate  VARCHAR2(8),
  updator     VARCHAR2(10),
  item_type   VARCHAR2(30)
)
;
create table MIC.DAMS_MENU_NLS
(
  menu_id       VARCHAR2(10) not null,
  menu_name     VARCHAR2(60),
  locale        VARCHAR2(20) not null,
  default_label VARCHAR2(60),
  menu_desc     VARCHAR2(200)
)
;
create table MIC.DAMS_NOTICE_REPORT_APP
(
  business_id        VARCHAR2(20) not null,
  valid_flag         VARCHAR2(1),
  applicant          VARCHAR2(9) not null,
  app_stru           VARCHAR2(10) not null,
  tel                VARCHAR2(50),
  create_time        DATE not null,
  update_time        DATE not null,
  close_time         DATE,
  title              VARCHAR2(1000),
  content            CLOB,
  attach_file_ref_id VARCHAR2(20),
  need_feed_back     VARCHAR2(1),
  sys_code           VARCHAR2(10),
  biz_type           VARCHAR2(50),
  urge_time          DATE
)
;
create table MIC.DAMS_NOTICE_REPORT_DRF
(
  business_id        VARCHAR2(20) not null,
  valid_flag         VARCHAR2(1),
  applicant          VARCHAR2(9) not null,
  app_stru           VARCHAR2(10) not null,
  tel                VARCHAR2(50),
  save_time          DATE not null,
  title              VARCHAR2(1000),
  content            CLOB,
  attach_file_ref_id VARCHAR2(20),
  need_feed_back     VARCHAR2(1),
  sys_code           VARCHAR2(10),
  biz_type           VARCHAR2(50)
)
;
create table MIC.DAMS_NOTI_APP_FEEDBACK
(
  business_id        VARCHAR2(20) not null,
  stru_id            VARCHAR2(10) not null,
  update_time        DATE not null,
  sub_app_id         VARCHAR2(20),
  sub_applicant      VARCHAR2(9),
  content            VARCHAR2(4000),
  attach_file_ref_id VARCHAR2(20),
  status             VARCHAR2(5),
  urge_time          DATE
)
;
create table MIC.DAMS_OPERATION_LOGINFO
(
  keyid       VARCHAR2(20) not null,
  logobj      VARCHAR2(20),
  logtype     VARCHAR2(3),
  logdate     VARCHAR2(14),
  loghandler  VARCHAR2(10),
  loginfo     VARCHAR2(4000),
  lmodifytime TIMESTAMP(6),
  logdetail   CLOB
)
;
create table MIC.DAMS_OUTER_TODO_TASK
(
  task_id        NUMBER(20) not null,
  task_name      VARCHAR2(150),
  ssic_id        VARCHAR2(9) not null,
  role_id        VARCHAR2(10),
  stru_id        VARCHAR2(10),
  prev_task_name VARCHAR2(150),
  prev_ssic_id   VARCHAR2(9),
  prev_stru_id   VARCHAR2(10),
  prev_role_id   VARCHAR2(10),
  form_id        VARCHAR2(255),
  create_time    VARCHAR2(14),
  take_time      VARCHAR2(14),
  end_time       VARCHAR2(14),
  task_state     VARCHAR2(20),
  outgoing_name  VARCHAR2(255),
  opinion        VARCHAR2(2000),
  business_id    VARCHAR2(20) not null,
  procinst_id    VARCHAR2(20),
  process_id     VARCHAR2(255),
  process_name   VARCHAR2(255),
  business_type  VARCHAR2(50),
  title          VARCHAR2(4000),
  creator_id     VARCHAR2(20),
  start_time     VARCHAR2(10),
  creator_stru   VARCHAR2(10),
  sys_code       VARCHAR2(10) not null,
  prev_task_id   NUMBER(19),
  cfgid          NUMBER(10),
  url            VARCHAR2(500),
  url_view       VARCHAR2(500),
  out_sys_seq    VARCHAR2(300),
  send_flag      VARCHAR2(1)
)
;
create table MIC.DAMS_PAYMENT_POSITION
(
  city_id   VARCHAR2(5) not null,
  area_id   VARCHAR2(6) not null,
  city_name VARCHAR2(100),
  city_py   VARCHAR2(100),
  city_sort NUMBER(7,2)
)
;
create table MIC.DAMS_PRINT_TIMES_USER_REL
(
  doc_id      VARCHAR2(20),
  doc_type    VARCHAR2(20),
  ssic_id     VARCHAR2(20),
  dept_id     VARCHAR2(20),
  print_times VARCHAR2(20),
  update_time DATE,
  print_time  DATE,
  update_user VARCHAR2(20)
)
;
create table MIC.DAMS_QXSQ_ROLE
(
  role_id   VARCHAR2(10) not null,
  role_name VARCHAR2(60),
  role_desc VARCHAR2(200)
)
;
create table MIC.DAMS_READ_EMPOWER
(
  key_id       VARCHAR2(20) not null,
  bussiness_id VARCHAR2(20) not null,
  user_id      VARCHAR2(9) not null,
  create_time  DATE not null,
  create_user  VARCHAR2(9) not null,
  state        VARCHAR2(1) not null,
  task_id      NUMBER(19)
)
;
create table MIC.DAMS_ROLE
(
  role_id     VARCHAR2(10) not null,
  role_type   VARCHAR2(20),
  sys_code    VARCHAR2(20) not null,
  stru_grade  VARCHAR2(3),
  stru_id     VARCHAR2(10),
  creator     VARCHAR2(10),
  create_time VARCHAR2(8),
  lastupdate  VARCHAR2(8),
  updator     VARCHAR2(10),
  status      VARCHAR2(1),
  sp_set      VARCHAR2(1),
  sort        NUMBER(5),
  out_role_id VARCHAR2(50)
)
;
create table MIC.DAMS_ROLE_NLS
(
  role_id   VARCHAR2(10) not null,
  role_name VARCHAR2(60),
  locale    VARCHAR2(20) not null,
  role_desc VARCHAR2(200)
)
;
create table MIC.DAMS_SAFETY_CONTROL_TEMPLATE
(
  key_id        VARCHAR2(20) not null,
  template_name VARCHAR2(400),
  cret_date     VARCHAR2(14),
  cret_person   VARCHAR2(20),
  modify_time   VARCHAR2(14),
  modify_person VARCHAR2(20)
)
;
create table MIC.DAMS_SAVE_INFO
(
  app_id     VARCHAR2(20) not null,
  applyer    VARCHAR2(9),
  app_stru   VARCHAR2(10),
  app_time   DATE,
  biz_type   VARCHAR2(50),
  info       VARCHAR2(600),
  pdid       VARCHAR2(255),
  valid_flag VARCHAR2(1),
  form_id    VARCHAR2(255)
)
;
create table MIC.DAMS_SECURE_DEGREE_BASE
(
  secure_id     VARCHAR2(20) not null,
  secure_degree VARCHAR2(20)
)
;
create table MIC.DAMS_STANDARD_PROCESS_MODEL
(
  key_id        VARCHAR2(20) not null,
  biz_type      VARCHAR2(20) not null,
  activity_id   VARCHAR2(255) not null,
  order_code    VARCHAR2(10),
  create_person VARCHAR2(20),
  cret_date     VARCHAR2(14) not null,
  open_state    VARCHAR2(10),
  other_state   VARCHAR2(10)
)
;
create table MIC.DAMS_STRU_SELECT_RANGE
(
  rule_id      VARCHAR2(20) not null,
  func         VARCHAR2(6) not null,
  stru_id      VARCHAR2(10) not null,
  rule_type    VARCHAR2(10),
  rule_content VARCHAR2(4000),
  remark       VARCHAR2(4000),
  update_time  DATE not null,
  max_level    NUMBER(2)
)
;
create table MIC.DAMS_SUB_SYS_MENU_SORT
(
  sys_code  VARCHAR2(10) not null,
  menu_id   VARCHAR2(10) not null,
  menu_name VARCHAR2(60)
)
;
create table MIC.DAMS_SYS_CONF
(
  sys_code           VARCHAR2(10) not null,
  sys_name           VARCHAR2(200),
  sys_sname          VARCHAR2(50),
  sys_type           VARCHAR2(20),
  sys_desc           VARCHAR2(200),
  sys_server_ip      VARCHAR2(20),
  sys_server_ctxpath VARCHAR2(20),
  sys_mainpage_url   VARCHAR2(200),
  sys_group          VARCHAR2(20),
  ssic_sys_code      VARCHAR2(20),
  sys_order          NUMBER(4),
  start_use          VARCHAR2(1)
)
;
create table MIC.DAMS_SYS_URL_CFG
(
  id       VARCHAR2(10) not null,
  sys_code VARCHAR2(10) not null,
  sysurl   VARCHAR2(300),
  context  VARCHAR2(20),
  disable  VARCHAR2(1)
)
;
create table MIC.DAMS_TEMPLATE_SYS_REL
(
  key_id      VARCHAR2(20) not null,
  template_id VARCHAR2(20),
  syscode     VARCHAR2(20)
)
;
create table MIC.DAMS_USER
(
  ssic_id            VARCHAR2(9) not null,
  person_id          VARCHAR2(10),
  name               VARCHAR2(90),
  sex                VARCHAR2(20),
  birthday           VARCHAR2(20),
  person_type        VARCHAR2(10),
  polity             VARCHAR2(10),
  folk               VARCHAR2(10),
  working_date       VARCHAR2(20),
  interrupted_month  VARCHAR2(20),
  economy_date       VARCHAR2(20),
  finance_date       VARCHAR2(20),
  icbc_date          VARCHAR2(20),
  stru_date          VARCHAR2(20),
  update_flag        VARCHAR2(2) not null,
  update_time        VARCHAR2(20) not null,
  office_phone1      VARCHAR2(50),
  office_phone2      VARCHAR2(50),
  fax                VARCHAR2(50),
  office_address     VARCHAR2(120),
  office_roomno      VARCHAR2(25),
  office_zipcode     VARCHAR2(30),
  vedio_phone        VARCHAR2(50),
  mobile_phone1      VARCHAR2(50),
  mobile_phone2      VARCHAR2(50),
  home_phone1        VARCHAR2(50),
  home_phone2        VARCHAR2(50),
  address            VARCHAR2(120),
  zipcode            VARCHAR2(30),
  email              VARCHAR2(80),
  msn                VARCHAR2(80),
  person_url         VARCHAR2(120),
  specialty_prop     VARCHAR2(10),
  admin_duty         VARCHAR2(10),
  headship           VARCHAR2(10),
  otherpost          VARCHAR2(10),
  duty_level         VARCHAR2(30),
  station_desc       VARCHAR2(300),
  station_type       VARCHAR2(10),
  station_prop       VARCHAR2(10),
  station_no         VARCHAR2(30),
  duty_lv            VARCHAR2(30),
  duty_lvnameid      VARCHAR2(150),
  if_preside_work    VARCHAR2(10),
  hold_time          VARCHAR2(20),
  work_state_code    VARCHAR2(10),
  work_state         VARCHAR2(150),
  stru_id            VARCHAR2(20),
  stru_name          VARCHAR2(120),
  lv1_stru_id        VARCHAR2(20),
  lv2_stru_id        VARCHAR2(20),
  sup_br14_id        VARCHAR2(20),
  notes_id           VARCHAR2(300),
  status             VARCHAR2(20),
  duty_level_back    VARCHAR2(30),
  source_type        VARCHAR2(20),
  ad                 VARCHAR2(200),
  hold_year          VARCHAR2(5),
  if_lead            VARCHAR2(10),
  is_part_time       VARCHAR2(10),
  sort               NUMBER,
  duty_sort          NUMBER,
  duty_type_id       VARCHAR2(6),
  duty_type          VARCHAR2(150),
  data_type          VARCHAR2(2),
  service_start_date VARCHAR2(8)
)
;
create table MIC.DAMS_USER_BRANCH_REL
(
  ssic_id         VARCHAR2(9) not null,
  name            VARCHAR2(90),
  stru_id         VARCHAR2(20) not null,
  stru_name       VARCHAR2(120),
  lv1_stru_id     VARCHAR2(20),
  lv1_stru_name   VARCHAR2(120),
  lv2_stru_id     VARCHAR2(20),
  lv2_stru_name   VARCHAR2(120),
  sup_br14_id     VARCHAR2(20),
  sup_br14_name   VARCHAR2(120),
  sys_code        VARCHAR2(30) not null,
  creator         VARCHAR2(20),
  create_time     VARCHAR2(20),
  admin_duty      VARCHAR2(10),
  hold_time       VARCHAR2(20),
  hold_year       VARCHAR2(5),
  if_lead         VARCHAR2(10),
  if_preside_work VARCHAR2(10),
  is_part_time    VARCHAR2(10),
  duty_type_id    VARCHAR2(6),
  data_type       VARCHAR2(2),
  duty_type       VARCHAR2(150),
  sort            NUMBER,
  dutp_type       VARCHAR2(150)
)
;
create table MIC.DAMS_USER_DAILY_CHANGE
(
  ssic_id     VARCHAR2(9) not null,
  update_time VARCHAR2(8) not null,
  tab_src     NUMBER(7)
)
;
create table MIC.DAMS_USER_ROLE_REL
(
  ssic_id          VARCHAR2(9) not null,
  role_id          VARCHAR2(10) not null,
  person_role_flag VARCHAR2(10),
  stru_id          VARCHAR2(10) not null,
  is_default       VARCHAR2(1),
  sys_code         VARCHAR2(20),
  auth_state       VARCHAR2(2)
)
;
create table MIC.DAMS_WEBSERVICE_DEL_TASK
(
  keyid         VARCHAR2(32) not null,
  exception_num NUMBER(2),
  create_time   TIMESTAMP(6),
  send_flag     VARCHAR2(1)
)
;
create table MIC.DAMS_WF_ACTIVITY_ALIAS
(
  cfgid               NUMBER(10) not null,
  activity_id         VARCHAR2(255) not null,
  incoming_transition VARCHAR2(255) not null,
  activity_name_alias VARCHAR2(255) not null
)
;
create table MIC.DAMS_WF_ACTIVITY_CONF
(
  cfgid          NUMBER(10) not null,
  id             VARCHAR2(255) not null,
  pdid           VARCHAR2(255),
  activity_name  VARCHAR2(255),
  cacu_type      VARCHAR2(2),
  select_range   VARCHAR2(2),
  ext_interface  VARCHAR2(255),
  batch_flag     VARCHAR2(1),
  denial_type    VARCHAR2(1),
  mail_type      VARCHAR2(1),
  parent_lev_num NUMBER(2),
  activity_type  VARCHAR2(1)
)
;
create table MIC.DAMS_WF_ACTIVITY_DISPLAY
(
  id           VARCHAR2(255) not null,
  pdid         VARCHAR2(255),
  show_type    VARCHAR2(1),
  show_action  VARCHAR2(1),
  show_task    VARCHAR2(1),
  show_dealer  VARCHAR2(1),
  show_opinion VARCHAR2(1),
  disp_seq     NUMBER not null,
  action_btn   VARCHAR2(2000)
)
;
create table MIC.DAMS_WF_ACTIVITY_FORM
(
  id          VARCHAR2(255) not null,
  activity_id VARCHAR2(255),
  form_id     VARCHAR2(255),
  authrole_id VARCHAR2(20),
  form_url    VARCHAR2(255),
  read_url    VARCHAR2(255)
)
;
create table MIC.DAMS_WF_ACTIVITY_USER
(
  cfgid           NUMBER(10) not null,
  id              VARCHAR2(255) not null,
  activity_id     VARCHAR2(255),
  user_type       VARCHAR2(20),
  entity_id       VARCHAR2(20),
  entity_name     VARCHAR2(255),
  transition_name VARCHAR2(255)
)
;
create table MIC.DAMS_WF_DICT
(
  dicttype VARCHAR2(20) not null,
  dictname VARCHAR2(50) not null,
  dictval  VARCHAR2(500),
  lang     VARCHAR2(10) not null
)
;
create table MIC.DAMS_WF_HIST_PROCESS
(
  business_id   VARCHAR2(20) not null,
  cfg_id        NUMBER(10),
  business_type VARCHAR2(50),
  process_id    VARCHAR2(255),
  procinst_id   VARCHAR2(20) not null,
  sys_code      VARCHAR2(10),
  title         VARCHAR2(4000),
  process_name  VARCHAR2(255),
  creator_id    VARCHAR2(20),
  start_time    TIMESTAMP(6),
  complete_time TIMESTAMP(6),
  creator_stru  VARCHAR2(10),
  valid_flag    VARCHAR2(1)
)
;
create table MIC.DAMS_WF_HIST_TASK
(
  task_id       NUMBER(19) not null,
  business_id   VARCHAR2(20) not null,
  procinst_id   VARCHAR2(20) not null,
  task_name     VARCHAR2(150),
  ssic_id       VARCHAR2(9),
  role_id       VARCHAR2(10),
  stru_id       VARCHAR2(10),
  prev_task_id  NUMBER(19),
  prev_ssic_id  VARCHAR2(9),
  prev_role_id  VARCHAR2(10),
  prev_stru_id  VARCHAR2(10),
  form_id       VARCHAR2(255),
  sys_code      VARCHAR2(10),
  auth_state    VARCHAR2(2),
  ssic_id_tran  VARCHAR2(9),
  create_time   TIMESTAMP(6),
  take_time     TIMESTAMP(6),
  end_time      TIMESTAMP(6),
  task_state    VARCHAR2(20),
  outgoing_name VARCHAR2(255),
  action_flag   VARCHAR2(2),
  opinion       VARCHAR2(4000),
  is_first      VARCHAR2(2),
  activity_id   VARCHAR2(255),
  sign_channel  VARCHAR2(5)
)
;
create table MIC.DAMS_WF_MAIL_CFG
(
  cfgid       NUMBER(10) not null,
  activity_id VARCHAR2(255) not null,
  subject_tpl VARCHAR2(1000),
  content_tpl VARCHAR2(4000),
  language    VARCHAR2(10) not null
)
;
create table MIC.DAMS_WF_PROCESS
(
  business_id   VARCHAR2(20) not null,
  cfg_id        NUMBER(10),
  business_type VARCHAR2(50),
  process_id    VARCHAR2(255),
  procinst_id   VARCHAR2(20) not null,
  sys_code      VARCHAR2(10),
  title         VARCHAR2(4000),
  process_name  VARCHAR2(255),
  creator_id    VARCHAR2(20),
  start_time    TIMESTAMP(6),
  complete_time TIMESTAMP(6),
  creator_stru  VARCHAR2(10),
  valid_flag    VARCHAR2(1)
)
;
create table MIC.DAMS_WF_PROCESS_CONF
(
  pdid            VARCHAR2(255) not null,
  process_name    VARCHAR2(255),
  process_key     VARCHAR2(255),
  process_version VARCHAR2(255),
  sys_code        VARCHAR2(20),
  status          VARCHAR2(1),
  execution_type  VARCHAR2(10),
  process_desc    VARCHAR2(255)
)
;
create table MIC.DAMS_WF_PROCESS_FORM
(
  id      VARCHAR2(255) not null,
  pdid    VARCHAR2(255),
  form_id VARCHAR2(255)
)
;
create table MIC.DAMS_WF_STRU_CONFIG
(
  stru_id      VARCHAR2(10) not null,
  pdkey        VARCHAR2(255) not null,
  cfgid        NUMBER(10),
  pdkey_mirror VARCHAR2(255),
  stru_range   VARCHAR2(255)
)
;
create table MIC.DAMS_WF_STRU_RANGE_CONFIG
(
  stru_range   VARCHAR2(10) not null,
  pdkey_mirror VARCHAR2(255) not null,
  cfgid        NUMBER(10),
  descr        VARCHAR2(255),
  pdkey        VARCHAR2(255)
)
;
create table MIC.DAMS_WF_SYNC_LOG
(
  pdid VARCHAR2(255),
  info VARCHAR2(4000),
  tm   TIMESTAMP(6)
)
;
create table MIC.DAMS_WF_TASK
(
  task_id       NUMBER(19) not null,
  business_id   VARCHAR2(20) not null,
  procinst_id   VARCHAR2(20) not null,
  task_name     VARCHAR2(150),
  ssic_id       VARCHAR2(9),
  role_id       VARCHAR2(10),
  stru_id       VARCHAR2(10),
  prev_task_id  NUMBER(19),
  prev_ssic_id  VARCHAR2(9),
  prev_role_id  VARCHAR2(10),
  prev_stru_id  VARCHAR2(10),
  form_id       VARCHAR2(255),
  sys_code      VARCHAR2(10),
  auth_state    VARCHAR2(2),
  ssic_id_tran  VARCHAR2(9),
  create_time   TIMESTAMP(6),
  take_time     TIMESTAMP(6),
  end_time      TIMESTAMP(6),
  task_state    VARCHAR2(20),
  outgoing_name VARCHAR2(255),
  action_flag   VARCHAR2(2),
  opinion       VARCHAR2(4000),
  is_first      VARCHAR2(2),
  issend        VARCHAR2(1),
  activity_id   VARCHAR2(255),
  sign_channel  VARCHAR2(5)
)
;
create table MIC.DAMS_WF_TASK_MAIL
(
  mail_id      VARCHAR2(12) not null,
  task_id      NUMBER(19),
  business_id  VARCHAR2(20) not null,
  sys_code     VARCHAR2(10),
  mail_to_ssic VARCHAR2(9)
)
;
CREATE TABLE "MIC"."DB_LOG" (
	"ID" VARCHAR2(20 BYTE),
	"PROC_NAME" VARCHAR2(100 BYTE),
	"INFO" VARCHAR2(4000 BYTE),
	"LOG_LEVEL" VARCHAR2(10 BYTE),
	"TIME_STAMP" VARCHAR2(23 BYTE),
	"ERROR_BACKTRACE" VARCHAR2(4000 BYTE),
	"ERR_STACK" VARCHAR2(4000 BYTE),
	"STEP_NO" VARCHAR2(20 BYTE),
	"LOG_DATE" VARCHAR2(8 BYTE)
);

CREATE TABLE "MIC"."JBPM4_EXECUTION" (
	"DBID_" NUMBER(19,0) NOT NULL,
	"CLASS_" VARCHAR2(255 CHAR) NOT NULL,
	"DBVERSION_" NUMBER(10,0) NOT NULL,
	"ACTIVITYNAME_" VARCHAR2(255 CHAR),
	"PROCDEFID_" VARCHAR2(255 CHAR),
	"HASVARS_" NUMBER(1,0),
	"NAME_" VARCHAR2(255 CHAR),
	"KEY_" VARCHAR2(255 CHAR),
	"ID_" VARCHAR2(255 CHAR),
	"STATE_" VARCHAR2(255 CHAR),
	"SUSPHISTSTATE_" VARCHAR2(255 CHAR),
	"PRIORITY_" NUMBER(10,0),
	"HISACTINST_" NUMBER(19,0),
	"PARENT_" NUMBER(19,0),
	"INSTANCE_" NUMBER(19,0),
	"SUPEREXEC_" NUMBER(19,0),
	"SUBPROCINST_" NUMBER(19,0),
	"PARENT_IDX_" NUMBER(10,0),
	PRIMARY KEY ("DBID_"),
	UNIQUE ("ID_"),
	CONSTRAINT "FK_EXEC_PARENT" FOREIGN KEY ("PARENT_") REFERENCES "MIC"."JBPM4_EXECUTION" ("DBID_") DISABLE,
	CONSTRAINT "FK_EXEC_SUBPI" FOREIGN KEY ("SUBPROCINST_") REFERENCES "MIC"."JBPM4_EXECUTION" ("DBID_") DISABLE,
	CONSTRAINT "FK_EXEC_SUPEREXEC" FOREIGN KEY ("SUPEREXEC_") REFERENCES "MIC"."JBPM4_EXECUTION" ("DBID_") DISABLE,
	CONSTRAINT "FK_EXEC_INSTANCE" FOREIGN KEY ("INSTANCE_") REFERENCES "MIC"."JBPM4_EXECUTION" ("DBID_") DISABLE
);


create table MIC.JBPM4_HIST_ACTINST
(
  dbid_          NUMBER(19) not null,
  class_         VARCHAR2(255 CHAR) not null,
  dbversion_     NUMBER(10) not null,
  hproci_        NUMBER(19),
  type_          VARCHAR2(255 CHAR),
  execution_     VARCHAR2(255 CHAR),
  activity_name_ VARCHAR2(255 CHAR),
  start_         TIMESTAMP(6),
  end_           TIMESTAMP(6),
  duration_      NUMBER(19),
  transition_    VARCHAR2(255 CHAR),
  nextidx_       NUMBER(10),
  htask_         NUMBER(19)
)
;
create table MIC.JBPM4_PARTICIPATION
(
  dbid_      NUMBER(19) not null,
  dbversion_ NUMBER(10) not null,
  groupid_   VARCHAR2(255 CHAR),
  userid_    VARCHAR2(255 CHAR),
  type_      VARCHAR2(255 CHAR),
  task_      NUMBER(19),
  swimlane_  NUMBER(19)
)
;
create table MIC.JBPM4_TASK
(
  dbid_          NUMBER(19) not null,
  class_         CHAR(1 CHAR) not null,
  dbversion_     NUMBER(10) not null,
  name_          VARCHAR2(255 CHAR),
  descr_         CLOB,
  state_         VARCHAR2(255 CHAR),
  susphiststate_ VARCHAR2(255 CHAR),
  assignee_      VARCHAR2(255 CHAR),
  form_          VARCHAR2(255 CHAR),
  priority_      NUMBER(10),
  create_        TIMESTAMP(6),
  duedate_       TIMESTAMP(6),
  progress_      NUMBER(10),
  signalling_    NUMBER(1),
  execution_id_  VARCHAR2(255 CHAR),
  activity_name_ VARCHAR2(255 CHAR),
  hasvars_       NUMBER(1),
  supertask_     NUMBER(19),
  execution_     NUMBER(19),
  procinst_      NUMBER(19),
  swimlane_      NUMBER(19),
  taskdefname_   VARCHAR2(255 CHAR)
)
;
create table MIC.OIS_GEN_DICT
(
  dicttype       VARCHAR2(50) not null,
  dictcode       VARCHAR2(20) not null,
  dictvalue      VARCHAR2(100),
  defaultvalue   VARCHAR2(100),
  canmodify_code VARCHAR2(1),
  dictdescript   VARCHAR2(200),
  dictlang       VARCHAR2(10) not null,
  parent_code    VARCHAR2(20),
  sys_code       VARCHAR2(10) not null,
  valid_flag     VARCHAR2(1),
  order_no       NUMBER(3)
)
;
create table MIC.OIS_GROUP
(
  group_id       VARCHAR2(20) not null,
  group_name     VARCHAR2(1000),
  ssic_id        VARCHAR2(9),
  func           VARCHAR2(6),
  last_modi_user VARCHAR2(9),
  last_modi_time DATE
)
;
create table MIC.OIS_GROUP_CONTENT
(
  group_id      VARCHAR2(20) not null,
  group_content VARCHAR2(200),
  sort          NUMBER(7) not null
)
;
create table MIC.SF_CARD_SP_AREA_DEF
(
  sp_area      VARCHAR2(20) not null,
  sp_area_name VARCHAR2(300) not null
)
;
create table MIC.SU_VACATION_INFO
(
  app_id            VARCHAR2(20) not null,
  applyer           VARCHAR2(10),
  operator          VARCHAR2(10),
  operator_tel      VARCHAR2(50),
  operator_stru     VARCHAR2(10),
  apply_time        DATE,
  vacation_type     VARCHAR2(20),
  start_date        VARCHAR2(8),
  end_date          VARCHAR2(8),
  start_time        VARCHAR2(8),
  end_time          VARCHAR2(8),
  start_peroid      VARCHAR2(20),
  end_peroid        VARCHAR2(20),
  day_time          VARCHAR2(20),
  remark            VARCHAR2(3000),
  attchement        VARCHAR2(20),
  valid_flag        VARCHAR2(1),
  dur_time          VARCHAR2(8),
  complete_time     DATE,
  match_flag        VARCHAR2(1),
  orgapp_id         VARCHAR2(20),
  sick_leave        VARCHAR2(1),
  scanfile          VARCHAR2(20),
  sick_leave_date   VARCHAR2(8),
  sick_leave_period VARCHAR2(20),
  applyer_stru      VARCHAR2(10),
  actual_day_time   VARCHAR2(20),
  payment_state     VARCHAR2(1),
  hld_opinion       VARCHAR2(3000),
  org_end_date      VARCHAR2(8),
  org_end_peroid    VARCHAR2(20)
)
;
create table MIC.SU_VACATION_INFO_DET
(
  app_id           VARCHAR2(20) not null,
  busi_type        VARCHAR2(20),
  end_location     VARCHAR2(300),
  aboard_flag      VARCHAR2(1),
  annual_remain    VARCHAR2(20),
  marriage         VARCHAR2(20),
  is_remarriage    VARCHAR2(20),
  is_late_marriage VARCHAR2(20),
  relative_type    VARCHAR2(20),
  parter           VARCHAR2(4000),
  baby_num         VARCHAR2(5),
  nurse_type       VARCHAR2(5),
  daily_hour       VARCHAR2(5),
  nurse_kind       VARCHAR2(5)
)
;
create table MIC.SU_VACATION_INFO_UPD
(
  id             VARCHAR2(20) not null,
  app_id         VARCHAR2(20),
  upd_times      NUMBER,
  upd_start_time DATE,
  upd_end_time   DATE,
  upd_flag       VARCHAR2(1),
  valid_flag     VARCHAR2(1),
  typ            VARCHAR2(1)
)
;
create table MIC.SU_VACATION_INFO_UPD_HIS
(
  id             VARCHAR2(20) not null,
  app_id         VARCHAR2(20),
  upd_times      NUMBER,
  upd_start_time DATE,
  upd_end_time   DATE,
  upd_flag       VARCHAR2(1),
  valid_flag     VARCHAR2(1),
  typ            VARCHAR2(1)
)
;
create table MIC.SU_VACATION_LOCATION
(
  app_id       VARCHAR2(20) not null,
  location_id  VARCHAR2(20) not null,
  end_location VARCHAR2(300)
)
;
create table MIC.TRANLOG
(
  logserialno      VARCHAR2(20) not null,
  trancode         VARCHAR2(8),
  trandate         VARCHAR2(14),
  areacode         VARCHAR2(20),
  netterminal      VARCHAR2(20),
  userid           VARCHAR2(20),
  errorcode        VARCHAR2(50),
  errormessage     VARCHAR2(512),
  errorlocation    VARCHAR2(50),
  errordescription VARCHAR2(512),
  errorstack       CLOB,
  journal          VARCHAR2(2048)
)
;