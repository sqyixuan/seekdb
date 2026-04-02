create sequence UTPLSQL_RUNNUM_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 57185
increment by 1
nocache
order;

create sequence UT_REFCURSOR_RESULTS_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 10
increment by 1
nocache
order;


create sequence UT_PACKAGE_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 10
increment by 1
nocache
order;


create sequence UT_UNITTEST_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
nocache
order;

create sequence UT_UTP_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 346
increment by 1
nocache
order;

create sequence UT_RECEQ_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 57185
increment by 1
nocache
order;

create sequence UT_SUITE_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 10
increment by 1
nocache
order;

CREATE TABLE "CPMG"."CAP_EC_DATA" (
	"DATADATE" VARCHAR2(8 CHAR) NOT NULL,
	"ORGANCODE" VARCHAR2(8 CHAR) NOT NULL,
	"CURRENCY" VARCHAR2(3 CHAR) NOT NULL,
	"ITEMCODE" VARCHAR2(20 CHAR) NOT NULL,
	"KCDQVALUE" NUMBER(17,2),
	"XSVALUE" NUMBER(9,6),
	"ZCYEVALUE" NUMBER(17,2)
	
);

create table CAP_TRANSLIMIT_STAT_TMP
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

create table UTR_ERROR
(
  run_id      NUMBER,
  suite_id    NUMBER,
  utp_id      NUMBER,
  unittest_id NUMBER,
  testcase_id NUMBER,
  outcome_id  NUMBER,
  errlevel    VARCHAR2(100 CHAR),
  occurred_on DATE,
  errcode     NUMBER,
  errtext     VARCHAR2(1000 CHAR),
  description VARCHAR2(2000 CHAR)
)

;

create table UTR_OUTCOME
(
  run_id      NUMBER not null,
  outcome_id  NUMBER not null,
  start_on    DATE,
  end_on      DATE,
  status      VARCHAR2(100 CHAR),
  description VARCHAR2(2000 CHAR),
  tc_run_id   NUMBER
)

;

create table UTR_SUITE
(
  run_id          NUMBER not null,
  run_by          VARCHAR2(30 CHAR),
  profiler_run_id NUMBER,
  suite_id        NUMBER not null,
  start_on        DATE,
  end_on          DATE,
  status          VARCHAR2(100 CHAR)
)

;

create table UTR_UNITTEST
(
  run_id      NUMBER not null,
  unittest_id NUMBER not null,
  start_on    DATE,
  end_on      DATE,
  status      VARCHAR2(100 CHAR)
)

;

create table UTR_UTP
(
  run_id          NUMBER not null,
  run_by          VARCHAR2(30 CHAR),
  profiler_run_id NUMBER,
  utp_id          NUMBER not null,
  start_on        DATE,
  end_on          DATE,
  status          VARCHAR2(100 CHAR)
)

;

create table UT_ASSERTION
(
  id                       NUMBER not null,
  name                     VARCHAR2(100 CHAR),
  description              VARCHAR2(1000 CHAR),
  use_msg                  CHAR(1 CHAR) default 'Y',
  use_null_ok              CHAR(1 CHAR) default 'Y',
  use_raise_exc            CHAR(1 CHAR) default 'Y',
  use_check_this           CHAR(1 CHAR) default 'Y',
  check_this_label         VARCHAR2(100 CHAR) default 'Check this',
  use_check_this_where     CHAR(1 CHAR) default 'N',
  check_this_where_label   VARCHAR2(100 CHAR) default 'Check this WHERE clause',
  use_against_this         CHAR(1 CHAR) default 'Y',
  against_this_label       VARCHAR2(100 CHAR) default 'Against this',
  use_against_this_where   CHAR(1 CHAR) default 'N',
  against_this_where_label VARCHAR2(100 CHAR) default 'Against this WHERE clause',
  use_check_this_dir       CHAR(1 CHAR) default 'N',
  check_this_dir_label     VARCHAR2(100 CHAR) default 'Location of "check this" file',
  use_against_this_dir     CHAR(1 CHAR) default 'N',
  against_this_dir_label   VARCHAR2(100 CHAR) default 'Location of "against this" file'
)

;

create table UT_CONFIG
(
  username           VARCHAR2(100 CHAR) not null,
  autocompile        CHAR(1 CHAR) default 'Y',
  registertest       CHAR(1 CHAR) default 'N',
  directory          VARCHAR2(2000 CHAR),
  naming_mode        CHAR(2 CHAR),
  prefix             VARCHAR2(100 CHAR),
  delimiter          VARCHAR2(10 CHAR) default '##',
  show_failures_only CHAR(1 CHAR) default 'N',
  filedir            VARCHAR2(2000 CHAR),
  fileuserprefix     VARCHAR2(100 CHAR),
  fileincprogname    VARCHAR2(1 CHAR) default 'N',
  filedateformat     VARCHAR2(100 CHAR) default 'yyyyddmmhh24miss',
  fileextension      VARCHAR2(200 CHAR) default '.UTF',
  show_config_info   VARCHAR2(1 CHAR) default 'Y',
  editor             VARCHAR2(1000 CHAR),
  reporter           VARCHAR2(100 CHAR)
)

;

create table UT_OUTCOME
(
  id              NUMBER not null,
  testcase_id     NUMBER,
  seq             NUMBER,
  name            VARCHAR2(200 CHAR),
  assertion_type  VARCHAR2(100 CHAR),
  action_level    VARCHAR2(10 CHAR),
  null_ok         CHAR(1 CHAR) default 'N',
  raise_exception CHAR(1 CHAR) default 'N',
  declarations    VARCHAR2(2000 CHAR),
  setup           VARCHAR2(2000 CHAR),
  teardown        VARCHAR2(2000 CHAR),
  exceptions      VARCHAR2(2000 CHAR),
  control_info    VARCHAR2(2000 CHAR),
  test_info       VARCHAR2(2000 CHAR)
)

;

create table UT_PACKAGE
(
  id          NUMBER not null,
  suite_id    NUMBER,
  owner       VARCHAR2(30 CHAR),
  name        VARCHAR2(200 CHAR),
  description VARCHAR2(2000 CHAR),
  samepackage CHAR(1 CHAR) default 'N',
  prefix      VARCHAR2(100 CHAR),
  dir         VARCHAR2(2000 CHAR),
  seq         NUMBER,
  executions  NUMBER,
  failures    NUMBER,
  last_status VARCHAR2(20 CHAR),
  last_start  DATE,
  last_end    DATE,
  last_run_id NUMBER
)

;

create table UT_RECEQ
(
  id         NUMBER not null,
  name       VARCHAR2(30 CHAR),
  test_name  VARCHAR2(30 CHAR),
  created_by VARCHAR2(30 CHAR),
  rec_owner  VARCHAR2(30 CHAR)
)

;

create table UT_RECEQ_PKG
(
  receq_id   NUMBER not null,
  pkg_id     NUMBER not null,
  created_by VARCHAR2(30 CHAR) not null
)

;

create table UT_SUITE
(
  id               NUMBER not null,
  name             VARCHAR2(200 CHAR),
  description      VARCHAR2(2000 CHAR),
  frequency        VARCHAR2(2000 CHAR),
  created_by       VARCHAR2(30 CHAR),
  created_on       DATE,
  executions       NUMBER,
  failures         NUMBER,
  last_status      VARCHAR2(20 CHAR),
  last_start       DATE,
  last_end         DATE,
  per_method_setup CHAR(1 CHAR)
)

;

create table UT_SUITE_UTP
(
  suite_id NUMBER not null,
  utp_id   NUMBER not null,
  seq      NUMBER,
  enabled  VARCHAR2(1 CHAR)
)

;

create table UT_TEST
(
  id          NUMBER not null,
  package_id  NUMBER,
  name        VARCHAR2(200 CHAR),
  description VARCHAR2(2000 CHAR),
  seq         NUMBER,
  executions  NUMBER,
  failures    NUMBER,
  last_start  DATE,
  last_end    DATE
)

;

create table UT_TESTCASE
(
  id                    NUMBER not null,
  unittest_id           NUMBER,
  name                  VARCHAR2(200 CHAR),
  seq                   NUMBER default 1,
  description           VARCHAR2(2000 CHAR),
  status                VARCHAR2(20 CHAR),
  declarations          VARCHAR2(2000 CHAR),
  setup                 VARCHAR2(2000 CHAR),
  teardown              VARCHAR2(2000 CHAR),
  exceptions            VARCHAR2(2000 CHAR),
  test_id               NUMBER,
  prefix                VARCHAR2(200 CHAR),
  assertion             VARCHAR2(100 CHAR),
  inline_assertion_call CHAR(1 CHAR) default 'N',
  executions            NUMBER,
  failures              NUMBER,
  last_start            DATE,
  last_end              DATE
)

;

create table UT_UNITTEST
(
  id               NUMBER not null,
  utp_id           NUMBER,
  program_name     VARCHAR2(30 CHAR),
  overload         NUMBER,
  seq              NUMBER,
  description      VARCHAR2(2000 CHAR),
  status           VARCHAR2(20 CHAR),
  is_deterministic CHAR(1 CHAR),
  declarations     VARCHAR2(2000 CHAR),
  setup            VARCHAR2(2000 CHAR),
  teardown         VARCHAR2(2000 CHAR),
  exceptions       VARCHAR2(2000 CHAR)
)

;

create table UT_UTP
(
  id                NUMBER not null,
  description       VARCHAR2(2000 CHAR),
  prefix            VARCHAR2(10 CHAR),
  program           VARCHAR2(30 CHAR),
  owner             VARCHAR2(30 CHAR),
  filename          VARCHAR2(2000 CHAR),
  frequency         VARCHAR2(2000 CHAR),
  program_map       VARCHAR2(2000 CHAR),
  declarations      VARCHAR2(2000 CHAR),
  setup             VARCHAR2(2000 CHAR),
  teardown          VARCHAR2(2000 CHAR),
  exceptions        VARCHAR2(2000 CHAR),
  per_method_setup  CHAR(1 CHAR),
  name              VARCHAR2(30 CHAR),
  utp_owner         VARCHAR2(30 CHAR),
  program_filename  VARCHAR2(30 CHAR),
  directory         VARCHAR2(30 CHAR),
  program_directory VARCHAR2(30 CHAR)
)

;


delimiter //;
CREATE OR REPLACE PACKAGE utplsql_util
 AUTHID CURRENT_USER
AS

   TYPE SQLDATA IS RECORD (
      col_name   VARCHAR2 (50),
      col_type   PLS_INTEGER,
      col_len    PLS_INTEGER
   );

   TYPE sqldata_tab IS TABLE OF SQLDATA
      INDEX BY BINARY_INTEGER;

   TYPE params_rec IS RECORD (
      par_name       VARCHAR2 (50),
      par_type       VARCHAR2 (10),
      par_sql_type   VARCHAR2 (50),
      par_inout      PLS_INTEGER,
      par_pos        PLS_INTEGER,
      par_val        VARCHAR2 (32000)
   );

   TYPE utplsql_array IS RECORD (
      array_pos   PLS_INTEGER,
      array_val   VARCHAR2 (32000)
   );

   TYPE utplsql_params IS TABLE OF params_rec
      INDEX BY BINARY_INTEGER;

   TYPE array_table IS TABLE OF utplsql_array
      INDEX BY BINARY_INTEGER;

   TYPE ut_refc IS REF CURSOR;

   TYPE v30_table IS TABLE OF VARCHAR2 (30)
      INDEX BY BINARY_INTEGER;

   TYPE varchar_array IS TABLE OF VARCHAR2 (4000)
      INDEX BY BINARY_INTEGER;

   array_holder   array_table;

   PROCEDURE reg_in_param (
      par_pos            PLS_INTEGER,
      par_val            VARCHAR2,
      params    IN OUT   utplsql_params
   );

   PROCEDURE reg_in_array (
      par_pos      IN       PLS_INTEGER,
      array_name   IN       VARCHAR2,
      array_vals   IN       varchar_array,
      params       IN OUT   utplsql_params
   );

   PROCEDURE reg_in_param (
      par_pos            PLS_INTEGER,
      par_val            NUMBER,
      params    IN OUT   utplsql_params
   );

   PROCEDURE reg_in_param (
      par_pos            PLS_INTEGER,
      par_val            DATE,
      params    IN OUT   utplsql_params
   );

   PROCEDURE reg_inout_param (
      par_pos            PLS_INTEGER,
      par_val            VARCHAR2,
      params    IN OUT   utplsql_params
   );

   PROCEDURE reg_inout_param (
      par_pos            PLS_INTEGER,
      par_val            NUMBER,
      params    IN OUT   utplsql_params
   );

   PROCEDURE reg_inout_param (
      par_pos            PLS_INTEGER,
      par_val            DATE,
      params    IN OUT   utplsql_params
   );

   PROCEDURE reg_out_param (
      par_pos             PLS_INTEGER,
      par_type            VARCHAR2,
      params     IN OUT   utplsql_params
   );

   PROCEDURE get_table_for_str (
      p_arr         OUT   v30_table,
      p_string            VARCHAR2,
      delim               VARCHAR2 := ',',
      enclose_str         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE get_metadata_for_cursor (
      proc_name         VARCHAR2,
      metadata    OUT   sqldata_tab
   );

   PROCEDURE get_metadata_for_query (
      query_txt         VARCHAR2,
      metadata    OUT   sqldata_tab
   );

   PROCEDURE get_metadata_for_table (
      table_name         VARCHAR2,
      metadata     OUT   sqldata_tab
   );

   PROCEDURE get_metadata_for_proc (
      proc_name         VARCHAR2,
      POSITION          INTEGER,
      data_type   OUT   VARCHAR2,
      metadata    OUT   sqldata_tab
   );

   PROCEDURE test_get_metadata_for_cursor (proc_name VARCHAR2);

   PROCEDURE print_metadata (metadata sqldata_tab);

   FUNCTION get_colnamesstr (metadata sqldata_tab)
      RETURN VARCHAR2;

   FUNCTION get_coltypesstr (metadata sqldata_tab)
      RETURN VARCHAR2;

   FUNCTION get_coltype_syntax (col_type PLS_INTEGER, col_len PLS_INTEGER)
      RETURN VARCHAR2;

   PROCEDURE PRINT (str VARCHAR2);

   FUNCTION get_proc_name (p_proc_nm VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_version
      RETURN VARCHAR2;

   FUNCTION get_val_for_table (
      table_name         VARCHAR2,
      col_name           VARCHAR2,
      col_val      OUT   VARCHAR2,
      col_type     OUT   NUMBER
   )
      RETURN NUMBER;

   FUNCTION get_table_name
      RETURN VARCHAR2;

   PROCEDURE execute_ddl (stmt VARCHAR2);

   FUNCTION get_create_ddl (
      metadata     utplsql_util.sqldata_tab,
      table_name   VARCHAR2,
      owner_name   VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2;

   PROCEDURE prepare_cursor_1 (
      stmt             IN OUT   VARCHAR2,
      table_name                VARCHAR2,
      call_proc_name            VARCHAR2,
      metadata                  utplsql_util.sqldata_tab
   );

   FUNCTION prepare_and_fetch_rc (proc_name VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION prepare_and_fetch_rc (
      proc_name            VARCHAR2,
      params               utplsql_params,
      refc_pos_in_proc     PLS_INTEGER,
      refc_metadata_from   PLS_INTEGER DEFAULT 1,
      refc_metadata_str    VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2;
END;
//

CREATE OR REPLACE PACKAGE utassert2
 AUTHID CURRENT_USER
IS

   test_failure              EXCEPTION;

   c_continue       CONSTANT CHAR (1)  := 'c';
   c_stop           CONSTANT CHAR (1)  := 's';

   TYPE value_name_rt IS RECORD (
      value VARCHAR2(32767),
          name VARCHAR2(100));

   TYPE value_name_tt IS TABLE OF value_name_rt INDEX BY BINARY_INTEGER;

   function id (name_in in ut_assertion.name%type) return ut_assertion.id%type;
   function name (id_in in ut_assertion.id%type) return ut_assertion.name%type;

   PROCEDURE this (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE,
      register_in     IN   BOOLEAN := TRUE
   );

   PROCEDURE eval (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
          using_in IN VARCHAR2,
          value_name_in IN value_name_tt,
          null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eval (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
          using_in IN VARCHAR2,
          value_name_in IN value_name_tt,
          null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   DATE,
      against_this_in   IN   DATE,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE,
      truncate_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   BOOLEAN,
      against_this_in   IN   BOOLEAN,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqtable (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqtabcount (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqquery (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   VARCHAR2,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   DATE,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   NUMBER,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqcursor (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqfile (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      check_this_dir_in     IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      against_this_dir_in   IN   VARCHAR2 := NULL,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqpipe (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqcoll (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqcollapi (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_pkg_in     IN   VARCHAR2,
      against_this_pkg_in   IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      countfunc_in          IN   VARCHAR2 := 'COUNT',
      firstrowfunc_in       IN   VARCHAR2 := 'FIRST',
      lastrowfunc_in        IN   VARCHAR2 := 'LAST',
      nextrowfunc_in        IN   VARCHAR2 := 'NEXT',
      getvalfunc_in         IN   VARCHAR2 := 'NTHVAL',
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE throws (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in           IN   VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   );

   PROCEDURE throws (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   );

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in           IN   VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   );

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   );

   PROCEDURE this (
      outcome_in      IN   ut_outcome.name%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE,
      register_in     IN   BOOLEAN := TRUE
   );

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   DATE,
      against_this_in   IN   DATE,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE,
      truncate_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   BOOLEAN,
      against_this_in   IN   BOOLEAN,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqtable (
      outcome_in         IN   ut_outcome.name%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqtabcount (
      outcome_in         IN   ut_outcome.name%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqquery (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   --Check a query against a single VARCHAR2 value
   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.name%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   VARCHAR2,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   -- Check a query against a single DATE value
   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.name%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   DATE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.name%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   NUMBER,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   -- Not currently implemented
   PROCEDURE eqcursor (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqfile (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      check_this_dir_in     IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      against_this_dir_in   IN   VARCHAR2 := NULL,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqpipe (
      outcome_in        IN   ut_outcome.name%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

 
   PROCEDURE eqcoll (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,                    
      against_this_in       IN   VARCHAR2,                   
      eqfunc_in             IN   VARCHAR2 := NULL,
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

 
   PROCEDURE eqcollapi (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_pkg_in     IN   VARCHAR2,
      against_this_pkg_in   IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      countfunc_in          IN   VARCHAR2 := 'COUNT',
      firstrowfunc_in       IN   VARCHAR2 := 'FIRST',
      lastrowfunc_in        IN   VARCHAR2 := 'LAST',
      nextrowfunc_in        IN   VARCHAR2 := 'NEXT',
      getvalfunc_in         IN   VARCHAR2 := 'NTHVAL',
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.name%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.name%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   );


   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.name%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.name%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   );


   PROCEDURE throws (
      outcome_in       IN   ut_outcome.name%TYPE,
      msg_in           IN   VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   );


   PROCEDURE throws (
      outcome_in       IN   ut_outcome.name%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   );


   PROCEDURE raises (
      outcome_in       IN   ut_outcome.name%TYPE,
      msg_in           IN   VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   );

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.name%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   );

   PROCEDURE showresults;

   PROCEDURE noshowresults;

   FUNCTION showing_results
      RETURN BOOLEAN;


      PROCEDURE fileExists(
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      dir_in in varchar2,
      file_in in varchar2,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

  
   PROCEDURE objExists (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE objnotExists (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   FUNCTION previous_passed
       RETURN BOOLEAN;

   FUNCTION previous_failed
       RETURN BOOLEAN;
 
   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   DBMS_OUTPUT.CHARARR,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   DBMS_OUTPUT.CHARARR,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   VARCHAR2,
      line_delimiter_in     IN   CHAR := NULL,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   VARCHAR2,
      line_delimiter_in     IN   CHAR := NULL,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eq_refc_table(
      outcome_in        IN   ut_outcome.id%TYPE,
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      table_name        IN   VARCHAR2 );

   PROCEDURE eq_refc_query(
      outcome_in        IN   ut_outcome.id%TYPE,
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      qry               IN   VARCHAR2 );

END utassert2;
//

CREATE OR REPLACE PACKAGE utassert  AUTHID CURRENT_USER
IS

   type chararr is table of varchar2(32767) index by binary_integer;

   test_failure          EXCEPTION;

   c_continue   CONSTANT CHAR (1)  := 'c';
   c_stop       CONSTANT CHAR (1)  := 's';

   PROCEDURE this (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE,
      register_in     IN   BOOLEAN := TRUE
   );

   PROCEDURE eval (
      msg_in            IN   VARCHAR2,
          using_in IN VARCHAR2,
          value_name_in IN utAssert2.value_name_tt,
          null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eval (
      msg_in            IN   VARCHAR2,
          using_in IN VARCHAR2,
          value1_in IN VARCHAR2,
          value2_in IN VARCHAR2,
          name1_in IN VARCHAR2 := NULL,
          name2_in IN VARCHAR2 := NULL,
          null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   DATE,
      against_this_in   IN   DATE,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE,
      truncate_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eq (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   BOOLEAN,
      against_this_in   IN   BOOLEAN,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqtable (
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqtabcount (
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqquery (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   VARCHAR2,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   DATE,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqqueryvalue (
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   NUMBER,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   );

   PROCEDURE eqcursor (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqfile (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      check_this_dir_in     IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      against_this_dir_in   IN   VARCHAR2 := NULL,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqpipe (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      check_nth_in      IN   VARCHAR2 := NULL,
      against_nth_in    IN   VARCHAR2 := NULL,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   PROCEDURE eqcoll (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqcollapi (
      msg_in                IN   VARCHAR2,
      check_this_pkg_in     IN   VARCHAR2,
      against_this_pkg_in   IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      countfunc_in          IN   VARCHAR2 := 'COUNT',
      firstrowfunc_in       IN   VARCHAR2 := 'FIRST',
      lastrowfunc_in        IN   VARCHAR2 := 'LAST',
      nextrowfunc_in        IN   VARCHAR2 := 'NEXT',
      getvalfunc_in         IN   VARCHAR2 := 'NTHVAL',
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE isnotnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnotnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE isnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE throws (
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   );

   PROCEDURE throws (
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   );

   PROCEDURE showresults;

   PROCEDURE noshowresults;

   FUNCTION showing_results
      RETURN BOOLEAN;

   PROCEDURE objExists (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

PROCEDURE objnotExists (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   );

   FUNCTION previous_passed
       RETURN BOOLEAN;

   FUNCTION previous_failed
       RETURN BOOLEAN;

   PROCEDURE eqoutput (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   CHARARR,
      against_this_in       IN   CHARARR,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eqoutput (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   CHARARR,
      against_this_in       IN   VARCHAR2,
      line_delimiter_in     IN   CHAR := NULL,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   );

   PROCEDURE eq_refc_table(
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      table_name        IN   VARCHAR2 );

   PROCEDURE eq_refc_query(
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      qry               IN   VARCHAR2 );

END utassert;
//

CREATE OR REPLACE PACKAGE utconfig  AUTHID CURRENT_USER
IS
   c_prefix      CONSTANT CHAR (3) := 'ut_';

   c_delimiter   CONSTANT CHAR (2) := '##';

   TYPE rec_fileinfo IS RECORD (
      filedir           UT_CONFIG.filedir%TYPE
     ,fileuserprefix    UT_CONFIG.fileuserprefix%TYPE
     ,fileincprogname   UT_CONFIG.fileincprogname%TYPE
     ,filedateformat    UT_CONFIG.filedateformat%TYPE
     ,fileextension     UT_CONFIG.filedateformat%TYPE
   );

   TYPE refcur_t IS REF CURSOR;

   CURSOR browser_cur
   IS
      SELECT owner, object_name, object_type, created, last_ddl_time, status
        FROM all_objects;

   cursor source_cur
   IS
     SELECT line, text FROM all_source;

   PROCEDURE settester (username_in IN VARCHAR2 := USER);

   FUNCTION tester
      RETURN VARCHAR2;

   PROCEDURE showconfig (username_in IN VARCHAR2 := NULL);

   PROCEDURE setprefix (prefix_in IN VARCHAR2, username_in IN VARCHAR2 := NULL);

   FUNCTION prefix (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setdelimiter (
      delimiter_in   IN   VARCHAR2
     ,username_in    IN   VARCHAR2 := NULL
   );

   FUNCTION delimiter (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setdir (dir_in IN VARCHAR2, username_in IN VARCHAR2 := NULL);

   FUNCTION dir (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE autocompile (onoff_in IN BOOLEAN, username_in IN VARCHAR2 := NULL);

   FUNCTION autocompiling (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN;

   PROCEDURE registertest (onoff_in IN BOOLEAN, username_in IN VARCHAR2
            := NULL);

   FUNCTION registeringtest (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN;

   PROCEDURE showfailuresonly (
      onoff_in      IN   BOOLEAN
     ,username_in   IN   VARCHAR2 := NULL
   );

   FUNCTION showingfailuresonly (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN;

   PROCEDURE setfiledir (
      dir_in        IN   VARCHAR2 := NULL
     ,username_in   IN   VARCHAR2 := NULL
   );

   FUNCTION filedir (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setreporter (
      reporter_in   IN   VARCHAR2
     ,username_in   IN   VARCHAR2 := NULL
   );

   FUNCTION getreporter (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setuserprefix (
      userprefix_in   IN   VARCHAR2 := NULL
     ,username_in     IN   VARCHAR2 := NULL
   );

   FUNCTION userprefix (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setincludeprogname (
      incname_in    IN   BOOLEAN := FALSE
     ,username_in   IN   VARCHAR2 := NULL
   );

   FUNCTION includeprogname (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN;

   PROCEDURE setdateformat (
      dateformat_in   IN   VARCHAR2 := 'yyyyddmmhh24miss'
     ,username_in     IN   VARCHAR2 := NULL
   );

   FUNCTION DATEFORMAT (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setfileextension (
      fileextension_in   IN   VARCHAR2 := '.UTF'
     ,username_in        IN   VARCHAR2 := NULL
   );

   FUNCTION fileextension (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2;

   PROCEDURE setfileinfo (
      dir_in             IN   VARCHAR2 := NULL
     ,userprefix_in      IN   VARCHAR2 := NULL
     ,incname_in         IN   BOOLEAN := FALSE
     ,dateformat_in      IN   VARCHAR2 := 'yyyyddmmhh24miss'
     ,fileextension_in   IN   VARCHAR2 := '.UTF'
     ,username_in        IN   VARCHAR2 := NULL
   );

   FUNCTION fileinfo (username_in IN VARCHAR2 := NULL)
      RETURN rec_fileinfo;

   PROCEDURE upd (
      username_in             IN   ut_config.username%TYPE
     ,autocompile_in          IN   ut_config.autocompile%TYPE
     ,prefix_in               IN   ut_config.prefix%TYPE
     ,show_failures_only_in   IN   ut_config.show_failures_only%TYPE
     ,directory_in            IN   ut_config.DIRECTORY%TYPE
     ,filedir_in              IN   ut_config.filedir%TYPE
     ,show_config_info_in     IN   ut_config.show_config_info%TYPE
     ,editor_in               IN   ut_config.editor%TYPE
    );

   FUNCTION browser_contents (
      schema_in      IN   VARCHAR2
     ,name_like_in   IN   VARCHAR2 := '%'
     ,type_like_in   IN   VARCHAR2 := '%'
   )
      RETURN refcur_t;

   FUNCTION source_for_program (
      schema_in      IN   VARCHAR2
     ,name_in   IN   VARCHAR2
   )
      RETURN refcur_t;

   FUNCTION onerow (schema_in IN VARCHAR2)
      RETURN refcur_t;

   PROCEDURE get_onerow (
      schema_in                IN       VARCHAR2
     ,username_out             OUT      VARCHAR2
     ,autocompile_out          OUT      ut_config.autocompile%TYPE
     ,prefix_out               OUT      ut_config.prefix%TYPE
     ,show_failures_only_out   OUT      ut_config.show_failures_only%TYPE
     ,directory_out            OUT      ut_config.DIRECTORY%TYPE
     ,filedir_out              OUT      ut_config.filedir%TYPE
     ,show_config_info_out     OUT      ut_config.show_config_info%TYPE
     ,editor_out               OUT      ut_config.editor%TYPE
   );
END;
//

CREATE OR REPLACE PACKAGE utoutcome
IS

   c_name     CONSTANT CHAR (7) := 'OUTCOME';
   c_abbrev   CONSTANT CHAR (2) := 'OC';

   FUNCTION name (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_outcome.name%TYPE;

   FUNCTION id (name_in IN ut_outcome.name%TYPE)
      RETURN ut_outcome.id%TYPE;

   FUNCTION onerow (name_in IN ut_outcome.name%TYPE)
      RETURN ut_outcome%ROWTYPE;

   FUNCTION onerow (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_outcome%ROWTYPE;

   FUNCTION utp (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_utp.id%TYPE;

   PRAGMA restrict_references (utp, WNDS, WNPS);

   FUNCTION unittest (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_unittest.id%TYPE;

   PRAGMA restrict_references (unittest, WNDS, WNPS);
END utoutcome;
//

CREATE OR REPLACE PACKAGE utoutputreporter  AUTHID CURRENT_USER
IS

   PROCEDURE open;
   PROCEDURE pl (str IN   VARCHAR2);

   PROCEDURE before_results(run_id IN utr_outcome.run_id%TYPE);
   PROCEDURE show_failure;
   PROCEDURE show_result;
   PROCEDURE after_results(run_id IN utr_outcome.run_id%TYPE);

   PROCEDURE before_errors(run_id IN utr_error.run_id%TYPE);
   PROCEDURE show_error;
   PROCEDURE after_errors(run_id IN utr_error.run_id%TYPE);

   PROCEDURE close;

END;
//

CREATE OR REPLACE PACKAGE utpackage 
IS

   c_success   CONSTANT VARCHAR2 (7) := 'SUCCESS';
   c_failure   CONSTANT VARCHAR2 (7) := 'FAILURE';

   FUNCTION name_from_id (id_in IN ut_package.id%TYPE)
      RETURN ut_package.name%TYPE;

   FUNCTION id_from_name (name_in IN ut_package.name%TYPE,
       owner_in      IN ut_package.owner%TYPE := NULL)
      RETURN ut_package.id%TYPE;

   PROCEDURE ADD (
      suite_in            IN   VARCHAR2,
      package_in          IN   VARCHAR2,
      samepackage_in      IN   BOOLEAN := FALSE,
      prefix_in           IN   VARCHAR2 := NULL,
      dir_in              IN   VARCHAR2 := NULL,
      seq_in              IN   PLS_INTEGER := NULL,
      owner_in            IN   VARCHAR2 := NULL,
      add_tests_in        IN   BOOLEAN := FALSE,
      test_overloads_in   IN   BOOLEAN := FALSE
   );

   PROCEDURE rem (
      suite_in     IN   VARCHAR2,
      package_in   IN   VARCHAR2,
      owner_in     IN   VARCHAR2 := NULL
   );

   PROCEDURE upd (
      suite_in        IN   VARCHAR2,
      package_in      IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
      owner_in        IN   VARCHAR2 := NULL
   );

   PROCEDURE ADD (
      suite_in            IN   INTEGER,
      package_in          IN   VARCHAR2,
      samepackage_in      IN   BOOLEAN := FALSE,
      prefix_in           IN   VARCHAR2 := NULL,
      dir_in              IN   VARCHAR2 := NULL,
      seq_in              IN   PLS_INTEGER := NULL,
      owner_in            IN   VARCHAR2 := NULL,
      add_tests_in        IN   BOOLEAN := FALSE,
      test_overloads_in   IN   BOOLEAN := FALSE
   );

   PROCEDURE rem (
      suite_in     IN   INTEGER,
      package_in   IN   VARCHAR2,
      owner_in     IN   VARCHAR2 := NULL
   );

   PROCEDURE upd (
      suite_id_in        IN   INTEGER,
      package_in      IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
      owner_in        IN   VARCHAR2 := NULL
   );
END utpackage;
//

CREATE OR REPLACE PACKAGE UTPIPE  AUTHID CURRENT_USER 
IS

   TYPE msg_rectype IS RECORD (
      item_type                     INTEGER,
      mvc2                          VARCHAR2 (4093),
      mdt                           DATE,
      mnum                          NUMBER,
      mrid                          ROWID,
      mraw                          RAW (4093));

   TYPE msg_tbltype IS TABLE OF msg_rectype
      INDEX BY BINARY_INTEGER;

   PROCEDURE receive_and_unpack(pipe_in           IN       VARCHAR2,
      msg_tbl_out       OUT      msg_tbltype,
      pipe_status_out   IN OUT   PLS_INTEGER);

END UTPIPE;
//

CREATE OR REPLACE PACKAGE utplsql  AUTHID CURRENT_USER
IS

   c_success    CONSTANT VARCHAR2 (7)     := 'SUCCESS';
   c_failure    CONSTANT VARCHAR2 (7)     := 'FAILURE';
   c_yes        CONSTANT CHAR (1)         := 'Y';
   c_no         CONSTANT CHAR (1)         := 'N';
   c_setup      CONSTANT CHAR (5)         := 'SETUP';
   c_teardown   CONSTANT CHAR (8)         := 'TEARDOWN';
   c_enabled   CONSTANT CHAR (7)         := 'ENABLED';
   c_disabled   CONSTANT CHAR (8)         := 'DISABLED';

   dbmaxvc2              VARCHAR2 (4000);

   SUBTYPE dbmaxvc2_t IS dbmaxvc2%TYPE;

   maxvc2                VARCHAR2 (32767);

   SUBTYPE maxvc2_t IS maxvc2%TYPE;

   namevc2               VARCHAR2 (100);

   SUBTYPE name_t IS namevc2%TYPE;

   TYPE test_rt IS RECORD (
      pkg                           VARCHAR2 (100),
    owner VARCHAR2(100),
      ispkg                         BOOLEAN,
      samepkg                       BOOLEAN,
      prefix                        VARCHAR2 (100),
      iterations                    PLS_INTEGER);

   TYPE testcase_rt IS RECORD (
      pkg                           VARCHAR2 (100),
      prefix                        VARCHAR2 (100),
      name                          VARCHAR2 (100),
      indx                          PLS_INTEGER,
      iterations                    PLS_INTEGER);

   TYPE test_tt IS TABLE OF testcase_rt
      INDEX BY BINARY_INTEGER;

   currcase              testcase_rt;

   FUNCTION vc2bool (vc IN VARCHAR2)
      RETURN BOOLEAN;

   FUNCTION bool2vc (bool IN BOOLEAN)
      RETURN VARCHAR2;

   FUNCTION ispackage (prog_in IN VARCHAR2, sch_in IN VARCHAR2)
      RETURN BOOLEAN;

   PROCEDURE test (
      package_in         IN   VARCHAR2,
      samepackage_in     IN   BOOLEAN := FALSE,
      prefix_in          IN   VARCHAR2 := NULL,
      recompile_in       IN   BOOLEAN := TRUE,
      dir_in             IN   VARCHAR2 := NULL,
      suite_in           IN   VARCHAR2 := NULL,
      owner_in           IN   VARCHAR2 := NULL,
      reset_results_in   IN   BOOLEAN := TRUE,
      from_suite_in      IN   BOOLEAN := FALSE,
      subprogram_in in varchar2 := '%',
      per_method_setup_in in boolean := FALSE,
    override_package_in IN varchar2 := NULL

   );

   PROCEDURE testsuite (
      suite_in           IN   VARCHAR2,
      recompile_in       IN   BOOLEAN := TRUE,
      reset_results_in   IN   BOOLEAN := TRUE,
      per_method_setup_in in boolean := FALSE,
      override_package_in IN BOOLEAN := FALSE

   );

   PROCEDURE run (
      testpackage_in     IN   VARCHAR2,
      prefix_in          IN   VARCHAR2 := NULL,
      suite_in           IN   VARCHAR2 := NULL,
      owner_in           IN   VARCHAR2 := NULL,
      reset_results_in   IN   BOOLEAN := TRUE,
      from_suite_in      IN   BOOLEAN := FALSE,
      subprogram_in in varchar2 := '%',
      per_method_setup_in in boolean := FALSE
   );

   PROCEDURE runsuite (
      suite_in           IN   VARCHAR2,
      reset_results_in   IN   BOOLEAN := TRUE,
      per_method_setup_in in boolean := FALSE
   );

   FUNCTION currpkg
      RETURN VARCHAR2;

   PROCEDURE addtest (
      package_in      IN   VARCHAR2,
      name_in         IN   VARCHAR2,
      prefix_in       IN   VARCHAR2,
      iterations_in   IN   PLS_INTEGER,
      override_in     IN   BOOLEAN
   );

   PROCEDURE addtest (
      name_in         IN   VARCHAR2,
      prefix_in       IN   VARCHAR2 := NULL,
      iterations_in   IN   PLS_INTEGER := 1,
      override_in     IN   BOOLEAN := FALSE
   );

   PROCEDURE setcase (case_in IN VARCHAR2);

   PROCEDURE setdata (
      dir_in     IN   VARCHAR2,
      file_in    IN   VARCHAR2,
      delim_in   IN   VARCHAR2 := ','
   );

   PROCEDURE passdata (data_in IN VARCHAR2, delim_in IN VARCHAR2 := ',');

   PROCEDURE trc;

   PROCEDURE notrc;

   FUNCTION tracing
      RETURN BOOLEAN;

   FUNCTION version
      RETURN VARCHAR2;

   FUNCTION seqval (tab_in IN VARCHAR2)
      RETURN PLS_INTEGER;

   FUNCTION pkgname (
      package_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2,
      ispkg_in         IN   BOOLEAN,
    owner_in IN VARCHAR2 := NULL
   )
      RETURN VARCHAR2;

   FUNCTION progname (
      program_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2,
      ispkg_in         IN   BOOLEAN
   )
      RETURN VARCHAR2;

   FUNCTION pkgname (
      package_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2,
    owner_in IN VARCHAR2 := NULL
   )
      RETURN VARCHAR2;

   FUNCTION progname (
      program_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2
   )
      RETURN VARCHAR2;

   FUNCTION ifelse (bool_in IN BOOLEAN, tval_in IN BOOLEAN, fval_in IN BOOLEAN)
      RETURN BOOLEAN;

   FUNCTION ifelse (bool_in IN BOOLEAN, tval_in IN DATE, fval_in IN DATE)
      RETURN DATE;

   FUNCTION ifelse (bool_in IN BOOLEAN, tval_in IN NUMBER, fval_in IN NUMBER)
      RETURN NUMBER;

   FUNCTION ifelse (
      bool_in   IN   BOOLEAN,
      tval_in   IN   VARCHAR2,
      fval_in   IN   VARCHAR2
   )
      RETURN VARCHAR2;
END;
//

CREATE OR REPLACE PACKAGE utplsql2  AUTHID CURRENT_USER
IS

V2_naming_mode constant char(2) := 'V1';
V1_naming_mode constant char(2) := 'V2';

   TYPE current_test_rt IS RECORD (
      run_id                        utr_outcome.run_id%TYPE,
      tc_run_id PLS_INTEGER,
      suite_id                      ut_suite.id%TYPE,
      utp_id                        ut_utp.id%TYPE,
      unittest_id                   ut_unittest.id%TYPE,
      testcase_id                   ut_testcase.id%TYPE,
      outcome_id                    ut_outcome.id%TYPE);

   FUNCTION runnum
      RETURN utr_outcome.run_id%TYPE;

   PRAGMA RESTRICT_REFERENCES (runnum, WNDS);

   PROCEDURE set_runnum;

   FUNCTION tc_runnum
      RETURN PLS_INTEGER;

   PROCEDURE move_ahead_tc_runnum;

   FUNCTION current_suite
      RETURN ut_suite.id%TYPE;

   PROCEDURE set_current_suite (suite_in IN ut_suite.id%TYPE);

   FUNCTION current_utp
      RETURN ut_utp.id%TYPE;

   PROCEDURE set_current_utp (utp_in IN ut_utp.id%TYPE);

   FUNCTION current_unittest
      RETURN ut_unittest.id%TYPE;

   PROCEDURE set_current_unittest (unittest_in IN ut_unittest.id%TYPE);

   FUNCTION current_testcase
      RETURN ut_testcase.id%TYPE;

   PROCEDURE set_current_testcase (testcase_in IN ut_testcase.id%TYPE);

   FUNCTION current_outcome
      RETURN ut_outcome.id%TYPE;

   PROCEDURE set_current_outcome (outcome_in IN ut_outcome.id%TYPE);

   PROCEDURE test (
      program_in        IN   VARCHAR2,
      owner_in          IN   VARCHAR2 := NULL,
      show_results_in   IN   BOOLEAN := TRUE,
      naming_mode_in IN VARCHAR2 := V2_naming_mode
   );

   PROCEDURE testsuite (
      suite_in          IN   VARCHAR2,
      show_results_in   IN   BOOLEAN := TRUE,
      naming_mode_in IN VARCHAR2 := V2_naming_mode
   );

   PROCEDURE trc;

   PROCEDURE notrc;

   FUNCTION tracing
      RETURN BOOLEAN;
END utplsql2;
//

CREATE OR REPLACE PACKAGE utreceq  AUTHID CURRENT_USER
IS

   PROCEDURE add(

      pkg_name_in IN ut_package.name%TYPE ,

      record_in  IN ut_receq.name%TYPE ,

      rec_owner_in  IN ut_receq.created_by%TYPE := USER

   );

   PROCEDURE compile(

      pkg_name_in     IN ut_package.name%TYPE

   );

   PROCEDURE rem(

      name_in  IN ut_receq.name%TYPE,

      rec_owner_in   IN ut_receq.created_by%TYPE := USER,

      for_package_in IN BOOLEAN := FALSE

   );

END utreceq;
//

CREATE OR REPLACE PACKAGE utreport  AUTHID CURRENT_USER
IS

   outcome utr_outcome%ROWTYPE;
   error utr_error%ROWTYPE;

   PROCEDURE use(reporter IN VARCHAR2);
   FUNCTION using RETURN VARCHAR2;

   PROCEDURE open;

   PROCEDURE pl (str IN VARCHAR2);
   PROCEDURE pl (bool IN BOOLEAN);

   PROCEDURE before_results(run_id IN utr_outcome.run_id%TYPE);
   PROCEDURE show_failure(rec_result utr_outcome%ROWTYPE);
   PROCEDURE show_result(rec_result utr_outcome%ROWTYPE);
   PROCEDURE after_results(run_id IN utr_outcome.run_id%TYPE);

   PROCEDURE before_errors(run_id IN utr_error.run_id%TYPE);
   PROCEDURE show_error(rec_error utr_error%ROWTYPE);
   PROCEDURE after_errors(run_id IN utr_error.run_id%TYPE);

   PROCEDURE close;

END;
//

CREATE OR REPLACE PACKAGE utrerror  AUTHID CURRENT_USER
IS
   c_error_indicator    CONSTANT VARCHAR2 (7) := 'UT-300%';
   general_error        CONSTANT INTEGER      := 300000;
   no_utp_for_program   CONSTANT INTEGER      := 300001;
   cannot_run_program   CONSTANT INTEGER      := 300002;
   undefined_outcome    CONSTANT INTEGER      := 300003;
   undefined_suite      CONSTANT INTEGER      := 300004;
   assertion_failure    CONSTANT INTEGER      := 300005;
   exc_undefined_suite           EXCEPTION;

   FUNCTION uterrcode (errmsg_in IN VARCHAR2 := NULL)
      RETURN INTEGER;

   PROCEDURE assert (
      condition_in   IN   BOOLEAN,
      message_in     IN   VARCHAR2,
      raiseexc       IN   BOOLEAN := TRUE,
      raiseerr       IN   INTEGER := NULL
   );

   PROCEDURE report (
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      errlevel_in      IN   VARCHAR2 := NULL,
      recorderr        IN   BOOLEAN := TRUE,
      raiseexc         IN   BOOLEAN := TRUE
   );

   PROCEDURE report_define_error (
      define_in    IN   VARCHAR2,
      message_in   IN   VARCHAR2 := NULL
   );

   PROCEDURE suite_report (
      run_in           IN   INTEGER,
      suite_in         IN   ut_suite.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   );

   PROCEDURE utp_report (
      run_in           IN   INTEGER,
      utp_in           IN   ut_utp.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   );

   PROCEDURE ut_report (
      run_in           IN   INTEGER,
      unittest_in      IN   ut_unittest.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   );

   PROCEDURE tc_report (
      run_in           IN   INTEGER,
      testcase_in      IN   ut_testcase.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   );

   PROCEDURE oc_report (
      run_in           IN   INTEGER,
      outcome_in       IN   ut_outcome.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   );
END utrerror;
//

CREATE OR REPLACE PACKAGE utresult
IS

   TYPE result_rt IS RECORD (
      name                          VARCHAR2 (100),
      msg                           VARCHAR2 (32767),
      indx                          PLS_INTEGER,
      status                        BOOLEAN);

   TYPE result_tt IS TABLE OF result_rt
      INDEX BY BINARY_INTEGER;

   results   result_tt;

   PROCEDURE report (msg_in IN VARCHAR2);

   PROCEDURE show (
      run_id_in   IN   utr_outcome.run_id%TYPE := NULL,
      reset_in    IN   BOOLEAN := FALSE
   );

   PROCEDURE showone (
      run_id_in   IN   utr_outcome.run_id%TYPE := NULL,
      indx_in     IN   PLS_INTEGER
   );

   PROCEDURE showlast (run_id_in IN utr_outcome.run_id%TYPE := NULL);

   PROCEDURE init (from_suite_in IN BOOLEAN := FALSE);

   FUNCTION success (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN BOOLEAN;

   FUNCTION failure (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN BOOLEAN;

   PROCEDURE firstresult (run_id_in IN utr_outcome.run_id%TYPE := NULL);

   FUNCTION nextresult (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN result_rt;

   PROCEDURE nextresult (
      name_out        OUT      VARCHAR2,
      msg_out         OUT      VARCHAR2,
      case_indx_out   OUT      PLS_INTEGER,
      run_id_in       IN       utr_outcome.run_id%TYPE := NULL
   );

   FUNCTION nthresult (
      indx_in     IN   PLS_INTEGER,
      run_id_in   IN   utr_outcome.run_id%TYPE := NULL
   )
      RETURN result_rt;

   PROCEDURE nthresult (
      indx_in         IN       PLS_INTEGER,
      name_out        OUT      VARCHAR2,
      msg_out         OUT      VARCHAR2,
      case_indx_out   OUT      PLS_INTEGER,
      run_id_in       IN       utr_outcome.run_id%TYPE := NULL
   );

   FUNCTION resultcount (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN PLS_INTEGER;

      procedure include_successes;
      procedure ignore_successes;

END utresult;
//

CREATE OR REPLACE PACKAGE utresult2
IS

   c_success   CONSTANT CHAR (7) := 'SUCCESS';
   c_failure   CONSTANT CHAR (7) := 'FAILURE';

   TYPE result_rt IS RECORD (
      NAME   VARCHAR2 (100)
     ,msg    VARCHAR2 (32767)
     ,indx   PLS_INTEGER
   );

   TYPE result_tt IS TABLE OF result_rt
      INDEX BY BINARY_INTEGER;

   CURSOR results_header_cur (schema_in IN VARCHAR2, program_in IN VARCHAR2)
   IS
      SELECT run_id, start_on, end_on, status
        FROM ut_utp utp, utr_utp utpr
       WHERE utp.ID = utpr.utp_id
         AND utp.program = program_in
         AND utp.owner = schema_in;

   PROCEDURE report (
      outcome_in       IN   ut_outcome.ID%TYPE
     ,description_in   IN   VARCHAR2
   );

   PROCEDURE report (
      outcome_in       IN   ut_outcome.ID%TYPE
     ,test_failed_in   IN   BOOLEAN
     ,description_in   IN   VARCHAR2
     ,register_in      IN   BOOLEAN := TRUE
     ,
      showresults_in   IN   BOOLEAN := FALSE 
   );

   FUNCTION run_succeeded (runnum_in IN utr_outcome.run_id%TYPE)
      RETURN BOOLEAN;

   FUNCTION run_status (runnum_in IN utr_outcome.run_id%TYPE)
      RETURN VARCHAR2;

   FUNCTION utp_succeeded (
      runnum_in   IN   utr_outcome.run_id%TYPE
     ,utp_in      IN   utr_utp.utp_id%TYPE
   )
      RETURN BOOLEAN;

   FUNCTION utp_status (
      runnum_in   IN   utr_outcome.run_id%TYPE
     ,utp_in      IN   utr_utp.utp_id%TYPE
   )
      RETURN VARCHAR2;

   FUNCTION unittest_succeeded (
      runnum_in     IN   utr_outcome.run_id%TYPE
     ,unittest_in   IN   utr_unittest.unittest_id%TYPE
   )
      RETURN BOOLEAN;

   FUNCTION unittest_status (
      runnum_in     IN   utr_outcome.run_id%TYPE
     ,unittest_in   IN   utr_unittest.unittest_id%TYPE
   )
      RETURN VARCHAR2;

   FUNCTION results_headers (schema_in IN VARCHAR2, program_in IN VARCHAR2)
      RETURN utconfig.refcur_t;

   FUNCTION results_details (
      run_id_in               IN   utr_utp.run_id%TYPE
     ,show_failures_only_in   IN   ut_config.show_failures_only%TYPE
   )
      RETURN utconfig.refcur_t;
END utresult2;
//

CREATE OR REPLACE PACKAGE utroutcome
IS

   PROCEDURE RECORD (
      run_id_in IN utr_outcome.run_id%TYPE
    , tc_run_id_in IN PLS_INTEGER
    , outcome_id_in IN utr_outcome.outcome_id%TYPE
    , test_failed_in IN BOOLEAN
    , description_in IN VARCHAR2 := NULL
    , end_on_in IN DATE := SYSDATE
   );

   PROCEDURE initiate (
      run_id_in IN utr_outcome.run_id%TYPE
    , outcome_id_in IN utr_outcome.outcome_id%TYPE
    , start_on_in IN DATE := SYSDATE
   );

   FUNCTION next_v1_id (run_id_in IN utr_outcome.run_id%TYPE)
      RETURN utr_outcome.outcome_id%TYPE;

   PROCEDURE clear_results (run_id_in IN utr_outcome.run_id%TYPE);

   PROCEDURE clear_results (
      owner_in IN VARCHAR2
    , program_in IN VARCHAR2
    , start_from_in IN DATE
   );

   PROCEDURE clear_all_but_last (owner_in IN VARCHAR2, program_in IN VARCHAR2);
END utroutcome;
//

CREATE OR REPLACE PACKAGE utrsuite
IS

   PROCEDURE terminate (
      run_id_in     IN   utr_suite.run_id%TYPE,
      suite_id_in   IN   utr_suite.suite_id%TYPE,
      end_on_in     IN   DATE := SYSDATE
   );

   PROCEDURE initiate (
      run_id_in     IN   utr_suite.run_id%TYPE,
      suite_id_in   IN   utr_suite.suite_id%TYPE,
      start_on_in   IN   DATE := SYSDATE
   );
END utrsuite;
//

CREATE OR REPLACE PACKAGE utrunittest
IS

   PROCEDURE terminate (
      run_id_in        IN   utr_unittest.run_id%TYPE,
      unittest_id_in   IN   utr_unittest.unittest_id%TYPE,
      end_on_in        IN   DATE := SYSDATE
   );

   PROCEDURE initiate (
      run_id_in        IN   utr_unittest.run_id%TYPE,
      unittest_id_in   IN   utr_unittest.unittest_id%TYPE,
      start_on_in      IN   DATE := SYSDATE
   );
END utrunittest;
//

CREATE OR REPLACE PACKAGE utrutp
IS

   PROCEDURE TERMINATE (
      run_id_in IN utr_utp.run_id%TYPE
    , utp_id_in IN utr_utp.utp_id%TYPE
    , end_on_in IN DATE := SYSDATE
   );

   PROCEDURE initiate (
      run_id_in IN utr_utp.run_id%TYPE
    , utp_id_in IN utr_utp.utp_id%TYPE
    , start_on_in IN DATE := SYSDATE
   );

   PROCEDURE clear_results (run_id_in IN utr_utp.run_id%TYPE);

   PROCEDURE clear_results (
      owner_in IN VARCHAR2
    , program_in IN VARCHAR2
    , start_from_in IN DATE
   );

   PROCEDURE clear_all_but_last (owner_in IN VARCHAR2, program_in IN VARCHAR2);

   function last_run_status (
      owner_in IN VARCHAR2
    , program_in IN VARCHAR2
   )
   return utr_utp.status%type;
END utrutp;
//

CREATE OR REPLACE PACKAGE utsuite
IS
   c_name     CONSTANT CHAR (18) := 'TEST SUITE PACKAGE';
   c_abbrev   CONSTANT CHAR (5)  := 'SUITE';

   FUNCTION name_from_id (id_in IN ut_suite.id%TYPE)
      RETURN ut_suite.name%TYPE;

   FUNCTION id_from_name (name_in IN ut_suite.name%TYPE)
      RETURN ut_suite.id%TYPE;

   FUNCTION onerow (name_in IN ut_suite.name%TYPE)
      RETURN ut_suite%ROWTYPE;

   PROCEDURE ADD (
      name_in            IN   ut_suite.name%TYPE,
      desc_in            IN   VARCHAR2 := NULL,
      rem_if_exists_in   IN   BOOLEAN := TRUE,
    per_method_setup_in in ut_suite.per_method_setup%type := null

   );

   PROCEDURE rem (name_in IN ut_suite.name%TYPE);

   PROCEDURE rem (id_in IN ut_suite.id%TYPE);

   PROCEDURE upd (
      name_in         IN   ut_suite.name%TYPE,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
    per_method_setup_in in ut_suite.per_method_setup%type := null
   );

   PROCEDURE upd (
      id_in           IN   ut_suite.id%TYPE,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
    per_method_setup_in in ut_suite.per_method_setup%type := null
   );

   FUNCTION suites (
      name_like_in   IN   VARCHAR2 := '%'
   )
      RETURN utconfig.refcur_t;

   PROCEDURE show_suites (name_like_in IN VARCHAR2 := '%');

END utsuite;
//

CREATE OR REPLACE PACKAGE uttest
IS

   FUNCTION name_from_id (id_in IN ut_test.id%TYPE)
      RETURN ut_test.name%TYPE;

   FUNCTION id_from_name (name_in IN ut_test.name%TYPE)
      RETURN ut_test.id%TYPE;

   PROCEDURE ADD (
      package_in   IN   INTEGER,
      test_in      IN   VARCHAR2,
      desc_in      IN   VARCHAR2 := NULL,
      seq_in       IN   PLS_INTEGER := NULL
   );

   PROCEDURE ADD (
      package_in   IN   VARCHAR2,
      test_in      IN   VARCHAR2,
      desc_in      IN   VARCHAR2 := NULL,
      seq_in       IN   PLS_INTEGER := NULL
   );

   PROCEDURE rem (package_in IN INTEGER, test_in IN VARCHAR2);

   PROCEDURE rem (package_in IN VARCHAR2, test_in IN VARCHAR2);

   PROCEDURE upd (
      package_in      IN   INTEGER,
      test_in         IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN
   );

   PROCEDURE upd (
      package_in      IN   VARCHAR2,
      test_in         IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN
   );
END uttest;
//

CREATE OR REPLACE PACKAGE uttestcase
IS

   c_name     CONSTANT CHAR (9) := 'TEST CASE';
   c_abbrev   CONSTANT CHAR (3) := 'TC';

   FUNCTION name_from_id (id_in IN ut_testcase.id%TYPE)
      RETURN ut_testcase.name%TYPE;

   FUNCTION id_from_name (name_in IN ut_testcase.name%TYPE)
      RETURN ut_testcase.id%TYPE;

   PROCEDURE ADD (
      test_in       IN   INTEGER,
      testcase_in   IN   VARCHAR2,
      desc_in       IN   VARCHAR2 := NULL,
      seq_in        IN   PLS_INTEGER := NULL
   );

   PROCEDURE ADD (
      test_in       IN   VARCHAR2,
      testcase_in   IN   VARCHAR2,
      desc_in       IN   VARCHAR2 := NULL,
      seq_in        IN   PLS_INTEGER := NULL
   );

   PROCEDURE rem (test_in IN INTEGER, testcase_in IN VARCHAR2);

   PROCEDURE rem (test_in IN VARCHAR2, testcase_in IN VARCHAR2);

   PROCEDURE upd (
      test_in         IN   INTEGER,
      testcase_in     IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN
   );

   PROCEDURE upd (
      test_in         IN   VARCHAR2,
      testcase_in     IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN
   );
END uttestcase;
//

CREATE OR REPLACE PACKAGE utunittest
IS

   c_name     CONSTANT CHAR (9) := 'UNIT TEST';
   c_abbrev   CONSTANT CHAR (2) := 'UT';

   FUNCTION name (
      ut_in    IN   ut_unittest%ROWTYPE
   )
      RETURN VARCHAR2;

   FUNCTION full_name (
      utp_in   IN   ut_utp%ROWTYPE,
      ut_in    IN   ut_unittest%ROWTYPE
   )
      RETURN VARCHAR2;

  FUNCTION name (
      id_in IN ut_unittest.id%TYPE
   )
      RETURN VARCHAR2;

   FUNCTION onerow (id_in IN ut_unittest.id%TYPE)
      RETURN ut_unittest%ROWTYPE;

   FUNCTION program_name (id_in IN ut_unittest.id%TYPE)
      RETURN ut_unittest.program_name%TYPE;

   FUNCTION id (name_in IN VARCHAR2)
      RETURN ut_unittest.id%TYPE;

   PROCEDURE ADD (
      utp_id_in        IN   ut_unittest.utp_id%TYPE,
      program_name_in          IN   ut_unittest.program_name%TYPE,
      seq_in           IN   ut_unittest.seq%TYPE := NULL,
      description_in   IN   ut_unittest.description%TYPE
            := NULL
   );

   PROCEDURE rem (
      name_in     IN   VARCHAR2
   );

   PROCEDURE rem (id_in IN ut_unittest.id%TYPE);
END;
//

CREATE OR REPLACE PACKAGE ututp
IS
   c_name     CONSTANT CHAR (18) := 'UNIT TEST PACKAGE';
   c_abbrev   CONSTANT CHAR (3)  := 'UTP';

   FUNCTION NAME (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2;

   FUNCTION NAME (id_in IN ut_utp.ID%TYPE)
      RETURN VARCHAR2;

   FUNCTION qualified_name (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2;

   FUNCTION qualified_name (id_in IN ut_utp.ID%TYPE)
      RETURN VARCHAR2;

   FUNCTION setup_procedure (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2;

   FUNCTION teardown_procedure (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2;

   FUNCTION prefix (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2;

   FUNCTION onerow (
      owner_in     IN   ut_utp.owner%TYPE
     ,program_in   IN   ut_utp.program%TYPE
   )
      RETURN ut_utp%ROWTYPE;

   FUNCTION onerow (utp_id_in IN ut_utp.ID%TYPE)
      RETURN ut_utp%ROWTYPE;

   PROCEDURE get_onerow (
      owner_in                IN       ut_utp.owner%TYPE
     ,program_in              IN       ut_utp.program%TYPE
     ,id_out                  OUT      ut_utp.ID%TYPE
     ,description_out         OUT      ut_utp.description%TYPE
     ,filename_out            OUT      ut_utp.filename%TYPE
     ,program_directory_out   OUT      ut_utp.program_directory%TYPE
     ,directory_out           OUT      ut_utp.DIRECTORY%TYPE
     ,name_out                OUT      ut_utp.NAME%TYPE
     ,utp_owner_out           OUT      ut_utp.utp_owner%TYPE
     ,prefix_out              OUT      ut_utp.prefix%TYPE
   );

   FUNCTION EXISTSS (
      owner_in     IN   ut_utp.owner%TYPE
     ,program_in   IN   ut_utp.program%TYPE
   )
      RETURN BOOLEAN;

   FUNCTION ID (NAME_IN IN VARCHAR2)
      RETURN ut_utp.ID%TYPE;

   PROCEDURE ADD (

      program_in             IN   ut_utp.program%TYPE := NULL
     ,owner_in               IN   ut_utp.owner%TYPE := NULL
     ,description_in         IN   ut_utp.description%TYPE := NULL
     ,filename_in            IN   ut_utp.filename%TYPE := NULL
     ,frequency_in           IN   ut_utp.frequency%TYPE := NULL
     ,program_map_in         IN   ut_utp.program_map%TYPE := NULL
     ,declarations_in        IN   ut_utp.declarations%TYPE := NULL
     ,setup_in               IN   ut_utp.setup%TYPE := NULL
     ,teardown_in            IN   ut_utp.teardown%TYPE := NULL
     ,exceptions_in          IN   ut_utp.EXCEPTIONS%TYPE := NULL
     ,program_directory_in   IN   ut_utp.program_directory%TYPE := NULL
     ,directory_in           IN   ut_utp.DIRECTORY%TYPE := NULL
     ,NAME_IN                IN   ut_utp.NAME%TYPE := NULL
     ,utp_owner_in           IN   ut_utp.utp_owner%TYPE := NULL
     ,prefix_in              IN   ut_utp.prefix%TYPE := NULL

   );

   PROCEDURE ADD (
      program_in             IN       ut_utp.program%TYPE := NULL
     ,owner_in               IN       ut_utp.owner%TYPE := NULL
     ,description_in         IN       ut_utp.description%TYPE := NULL
     ,filename_in            IN       ut_utp.filename%TYPE := NULL
     ,frequency_in           IN       ut_utp.frequency%TYPE := NULL
     ,program_map_in         IN       ut_utp.program_map%TYPE := NULL
     ,declarations_in        IN       ut_utp.declarations%TYPE := NULL
     ,setup_in               IN       ut_utp.setup%TYPE := NULL
     ,teardown_in            IN       ut_utp.teardown%TYPE := NULL
     ,exceptions_in          IN       ut_utp.EXCEPTIONS%TYPE := NULL
     ,program_directory_in   IN       ut_utp.program_directory%TYPE := NULL
     ,directory_in           IN       ut_utp.DIRECTORY%TYPE := NULL
     ,NAME_IN                IN       ut_utp.NAME%TYPE := NULL
     ,utp_owner_in           IN       ut_utp.utp_owner%TYPE := NULL
     ,prefix_in              IN       ut_utp.prefix%TYPE := NULL
     ,id_out                 OUT      ut_utp.ID%TYPE
   );

   PROCEDURE REM (NAME_IN IN VARCHAR2);

   PROCEDURE REM (id_in IN ut_utp.ID%TYPE);

   FUNCTION utps (
      program_like_in   IN   VARCHAR2 := '%'
   )
      RETURN utconfig.refcur_t;

END ututp;
//

create or replace type cai_type1 force is table of varchar(200)
//

CREATE OR REPLACE PACKAGE BODY utassert
IS
   g_showresults   BOOLEAN := FALSE;

   PROCEDURE this (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE,
      register_in     IN   BOOLEAN := TRUE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.this (
         l_outcome,
         msg_in,
         check_this_in,
         null_ok_in,
         raise_exc_in,
         register_in
      );
   END;

   PROCEDURE eval (
      msg_in            IN   VARCHAR2,
          using_in IN VARCHAR2,
          value_name_in IN utAssert2.value_name_tt,
          null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eval (
         l_outcome,
         msg_in,
         using_in,
                 value_name_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eval (
      msg_in            IN   VARCHAR2,
          using_in IN VARCHAR2,
          value1_in IN VARCHAR2,
          value2_in IN VARCHAR2,
          name1_in IN VARCHAR2 := NULL,
          name2_in IN VARCHAR2 := NULL,
          null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
          value_name utAssert2.value_name_tt;
   BEGIN
      value_name(1).value := value1_in;
      value_name(1).name := name1_in;
      value_name(2).value := value2_in;
      value_name(2).name := name2_in;

      utassert2.eval (
         l_outcome,
         msg_in,
         using_in,
                 value_name,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eq (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   BOOLEAN,
      against_this_in   IN   BOOLEAN,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eq (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   DATE,
      against_this_in   IN   DATE,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE,
      truncate_in       IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eq (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         null_ok_in,
         raise_exc_in,
         truncate_in
      );
   END;

   PROCEDURE eqtable (
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqtable (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         check_where_in,
         against_where_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqtabcount (
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqtabcount (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         check_where_in,
         against_where_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqquery (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqquery (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   VARCHAR2,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqqueryvalue (
         l_outcome,
         msg_in,
         check_query_in,
         against_value_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   DATE,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqqueryvalue (
         l_outcome,
         msg_in,
         check_query_in,
         against_value_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   NUMBER,
      null_ok_in         IN   BOOLEAN := FALSE,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqqueryvalue (
         l_outcome,
         msg_in,
         check_query_in,
         against_value_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqcursor (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )

   IS
   BEGIN
      utreport.pl ('utAssert.eqCursor is not yet implemented!');
   END;

   PROCEDURE eqfile (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      check_this_dir_in     IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      against_this_dir_in   IN   VARCHAR2 := NULL,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqfile (
         l_outcome,
         msg_in,
         check_this_in,
         check_this_dir_in,
         against_this_in,
         against_this_dir_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqpipe (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      check_nth_in      IN   VARCHAR2 := NULL,
      against_nth_in    IN   VARCHAR2 := NULL,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqpipe (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqcoll (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqcoll (
         l_outcome,
         msg_in,
         check_this_in,
         against_this_in,
         eqfunc_in,
         check_startrow_in,
         check_endrow_in,
         against_startrow_in,
         against_endrow_in,
         match_rownum_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqcollapi (
      msg_in                IN   VARCHAR2,
      check_this_pkg_in     IN   VARCHAR2,
      against_this_pkg_in   IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      countfunc_in          IN   VARCHAR2 := 'COUNT',
      firstrowfunc_in       IN   VARCHAR2 := 'FIRST',
      lastrowfunc_in        IN   VARCHAR2 := 'LAST',
      nextrowfunc_in        IN   VARCHAR2 := 'NEXT',
      getvalfunc_in         IN   VARCHAR2 := 'NTHVAL',
      check_startrow_in     IN   PLS_INTEGER := NULL,
      check_endrow_in       IN   PLS_INTEGER := NULL,
      against_startrow_in   IN   PLS_INTEGER := NULL,
      against_endrow_in     IN   PLS_INTEGER := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.eqcollapi (
         l_outcome,
         msg_in,
         check_this_pkg_in,
         against_this_pkg_in,
         eqfunc_in,
         countfunc_in,
         firstrowfunc_in,
         lastrowfunc_in,
         nextrowfunc_in,
         getvalfunc_in,
         check_startrow_in,
         check_endrow_in,
         against_startrow_in,
         against_endrow_in,
         match_rownum_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE isnotnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.isnotnull (l_outcome, msg_in, check_this_in, raise_exc_in);
   END;

   PROCEDURE isnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.isnull (l_outcome, msg_in, check_this_in, raise_exc_in);
   END;

   PROCEDURE isnotnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.isnotnull (l_outcome, msg_in, check_this_in, raise_exc_in);
   END;

   PROCEDURE isnull (
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.isnull (l_outcome, msg_in, check_this_in, raise_exc_in);
   END;

   PROCEDURE throws (
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.throws (l_outcome, msg_in, check_call_in, against_exc_in);
   END;

   PROCEDURE throws (
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.throws (l_outcome, msg_in, check_call_in, against_exc_in);
   END;

   PROCEDURE showresults
   IS
   BEGIN
      g_showresults := TRUE;
   END;

   PROCEDURE noshowresults
   IS
   BEGIN
      g_showresults := FALSE;
   END;

   FUNCTION showing_results
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN g_showresults;
   END;

   PROCEDURE objExists (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   )IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.objExists (
         l_outcome,
         msg_in,
         check_this_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE objnotExists (
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE,
      raise_exc_in      IN   BOOLEAN := FALSE
   )IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      utassert2.objnotExists (
         l_outcome,
         msg_in,
         check_this_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   FUNCTION previous_passed
       RETURN BOOLEAN
   IS
   BEGIN
       RETURN utAssert2.previous_passed;
   END;

   FUNCTION previous_failed
       RETURN BOOLEAN
   IS
   BEGIN
       RETURN utAssert2.previous_failed;
   END;

   PROCEDURE eqoutput (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   CHARARR,
      against_this_in       IN   CHARARR,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
      null;
   END;

   PROCEDURE eqoutput (
      msg_in                IN   VARCHAR2,
      check_this_in         IN   CHARARR,
      against_this_in       IN   VARCHAR2,
      line_delimiter_in     IN   CHAR := NULL,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
     l_outcome   ut_outcome.id%TYPE;
   BEGIN
     null;
   END;

   PROCEDURE eq_refc_table(
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      table_name        IN   VARCHAR2 )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
     utassert2.eq_refc_table(l_outcome,p_msg_nm,proc_name,params,cursor_position,table_name);
   END;

   PROCEDURE eq_refc_query(
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      qry               IN   VARCHAR2 )
   IS
      l_outcome   ut_outcome.id%TYPE;
   BEGIN
     utassert2.eq_refc_query(l_outcome,p_msg_nm,proc_name,params,cursor_position,qry);
   END;

END utassert;
//

CREATE OR REPLACE PACKAGE BODY utassert2
IS

   g_previous_pass              BOOLEAN;

   g_showresults                BOOLEAN     := FALSE ;
   c_not_placeholder   CONSTANT VARCHAR2 (10)
            := '#$NOT$#';

   FUNCTION id (name_in IN ut_assertion.NAME%TYPE)
      RETURN ut_assertion.id%TYPE
   IS
      retval   ut_assertion.id%TYPE;
   BEGIN
      SELECT id
        INTO retval
        FROM ut_assertion
       WHERE NAME = UPPER (name_in);

      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION NAME (id_in IN ut_assertion.id%TYPE)
      RETURN ut_assertion.NAME%TYPE
   IS
      retval   ut_assertion.NAME%TYPE;
   BEGIN
      SELECT NAME
        INTO retval
        FROM ut_assertion
       WHERE id = id_in;

      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION replace_not_placeholder (
      stg_in       IN   VARCHAR2,
      success_in   IN   BOOLEAN
   )
      RETURN VARCHAR2
   IS
   BEGIN
      IF success_in
      THEN
         RETURN REPLACE (
                   stg_in,
                   c_not_placeholder,
                   NULL
                );
      ELSE
         RETURN REPLACE (
                   stg_in,
                   c_not_placeholder,
                   ' not '
                );
      END IF;
   END;

   PROCEDURE this (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE ,
      raise_exc_in    IN   BOOLEAN := FALSE ,
      register_in     IN   BOOLEAN := TRUE
   )
   IS
      l_failure   BOOLEAN
   :=    NOT check_this_in
      OR (    check_this_in IS NULL
          AND NOT null_ok_in
         );
   BEGIN

      g_previous_pass := NOT l_failure;

      IF utplsql2.tracing
      THEN
         utreport.pl (
               'utPLSQL TRACE on Assert: '
            || msg_in
         );
         utreport.pl ('Results:');
         utreport.pl (l_failure);
      END IF;

      utresult2.report (
         outcome_in,
         l_failure,
         utplsql.currcase.pkg || '.' || utplsql.currcase.name || ': ' || msg_in,

         register_in,
         showing_results
      );

      IF      raise_exc_in
          AND l_failure
      THEN
         RAISE test_failure;
      END IF;
   END;

   PROCEDURE this (
      outcome_in       IN   ut_outcome.id%TYPE,
      success_msg_in   IN   VARCHAR2,
      failure_msg_in   IN   VARCHAR2,
      check_this_in    IN   BOOLEAN,
      null_ok_in       IN   BOOLEAN := FALSE ,
      raise_exc_in     IN   BOOLEAN := FALSE ,
      register_in      IN   BOOLEAN := TRUE
   )
   IS
      l_failure   BOOLEAN
   :=    NOT check_this_in
      OR (    check_this_in IS NULL
          AND NOT null_ok_in
         );
   BEGIN
      IF l_failure
      THEN
         this (
            outcome_in,
            failure_msg_in,
            check_this_in,
            null_ok_in,
            raise_exc_in,
            register_in
         );
      ELSE
         this (
            outcome_in,
            success_msg_in,
            check_this_in,
            null_ok_in,
            raise_exc_in,
            register_in
         );
      END IF;
   END;

   FUNCTION expected (
      type_in    IN   VARCHAR2,
      msg_in     IN   VARCHAR2,
      value_in   IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN (   type_in
              || ' "'
              || msg_in
              || '" Result: "'
              || value_in
              || '"'
             );
   END;

   FUNCTION file_descrip (
      file_in   IN   VARCHAR2,
      dir_in    IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    file_in
             || '" located in "'
             || dir_in;
   END;

   FUNCTION message_expected (
      type_in           IN   VARCHAR2,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN (   type_in
              || ' "'
              || msg_in
              || '" Expected "'
              || against_this_in
              || '" and got "'
              || check_this_in
              || '"'
             );
   END;

   FUNCTION message (
      type_in    IN   VARCHAR2,
      msg_in     IN   VARCHAR2,
      value_in   IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN (   type_in
              || ' "'
              || msg_in
              || '" Result: '
              || value_in
             );
   END;

   PROCEDURE get_id (
      outcome_in    IN       ut_outcome.NAME%TYPE,
      outcome_out   OUT      ut_outcome.id%TYPE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      l_id := utoutcome.id (outcome_in);

      IF l_id IS NULL
      THEN
         IF utplsql2.tracing
         THEN
            utreport.pl (
                  'Outcome '
               || outcome_in
               || ' is not defined.'
            );
         END IF;

         utrerror.oc_report (
            run_in=> utplsql2.runnum,
            outcome_in=> NULL,
            errcode_in=> utrerror.undefined_outcome,
            errtext_in=>    'Outcome "'
                         || outcome_in
                         || '" is not defined.'
         );
      ELSE
         outcome_out := l_id;
      END IF;
   END;

   PROCEDURE eval (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      using_in        IN   VARCHAR2,
      value_name_in   IN   value_name_tt,
      null_ok_in      IN   BOOLEAN := FALSE ,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      fdbk               PLS_INTEGER;
      cur                PLS_INTEGER
                          := DBMS_SQL.open_cursor;
      eval_result        CHAR (1);
      eval_block         utplsql.maxvc2_t;
      value_name_str     utplsql.maxvc2_t;
      eval_description   utplsql.maxvc2_t;
      parse_error        EXCEPTION;
   BEGIN
      IF utplsql2.tracing
      THEN

         NULL;
      END IF;

      FOR indx IN
          value_name_in.FIRST .. value_name_in.LAST
      LOOP
         value_name_str :=
               value_name_str
            || ' '
            || NVL (
                  value_name_in (indx).NAME,
                     'P'
                  || indx
               )
            || ' = '
            || value_name_in (indx).VALUE;
      END LOOP;

      eval_description :=    'Evaluation of "'
                          || using_in
                          || '" with'
                          || value_name_str;
      eval_block :=
            'DECLARE
           b_result BOOLEAN;
        BEGIN
           b_result := '
         || using_in
         || ';
           IF b_result THEN :result := '''
         || utplsql.c_yes
         || '''; '
         || 'ELSIF NOT b_result THEN :result := '''
         || utplsql.c_no
         || '''; '
         || 'ELSE :result := NULL;
           END IF;
        END;';

      BEGIN
         DBMS_SQL.parse (
            cur,
            eval_block,
            DBMS_SQL.native
         );
      EXCEPTION
         WHEN OTHERS
         THEN

            IF DBMS_SQL.is_open (cur)
            THEN
               DBMS_SQL.close_cursor (cur);
            END IF;

            this (
               outcome_in=> outcome_in,
               success_msg_in=> NULL,
               failure_msg_in=>    'Error '
                                || SQLCODE
                                || ' parsing '
                                || eval_block,
               check_this_in=> FALSE ,
               null_ok_in=> null_ok_in,
               raise_exc_in=> raise_exc_in
            );
            RAISE parse_error;
      END;

      FOR indx IN
          value_name_in.FIRST .. value_name_in.LAST
      LOOP
         DBMS_SQL.bind_variable (
            cur,
            NVL (
               value_name_in (indx).NAME,
                  'P'
               || indx
            ),
            value_name_in (indx).VALUE
         );
      END LOOP;

      DBMS_SQL.bind_variable (cur, 'result', 'a');
      fdbk := DBMS_SQL.EXECUTE (cur);
   
      DBMS_SQL.close_cursor (cur);
      this (
         outcome_in=> outcome_in,
         success_msg_in=>    eval_description
                          || ' evaluated to TRUE',
         failure_msg_in=>    eval_description
                          || ' evaluated to FALSE',
         check_this_in=> utplsql.vc2bool (
                     eval_result
                  ),
         null_ok_in=> null_ok_in,
         raise_exc_in=> raise_exc_in
      );
   EXCEPTION
      WHEN parse_error
      THEN
         IF raise_exc_in
         THEN
            RAISE;
         ELSE
            NULL;
         END IF;
      WHEN OTHERS
      THEN
         IF DBMS_SQL.is_open (cur)
         THEN
            DBMS_SQL.close_cursor (cur);
         END IF;

         this (
            outcome_in=> outcome_in,
            success_msg_in=> NULL,
            failure_msg_in=>    'Error in '
                             || eval_description
                             || ' SQLERRM: '
                             || SQLERRM,
            check_this_in=> FALSE ,
            null_ok_in=> null_ok_in,
            raise_exc_in=> raise_exc_in
         );
   END;

   PROCEDURE eval (
      outcome_in      IN   ut_outcome.NAME%TYPE,
      msg_in          IN   VARCHAR2,
      using_in        IN   VARCHAR2,
      value_name_in   IN   value_name_tt,
      null_ok_in      IN   BOOLEAN := FALSE ,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eval (
         l_id,
         msg_in,
         using_in,
         value_name_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE ,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      IF utplsql2.tracing
      THEN
         utreport.pl (
               'EQ Compare "'
            || check_this_in
            || '" to "'
            || against_this_in
            || '"'
         );
      END IF;

      this (
         outcome_in=> outcome_in,
         msg_in=> message_expected (
                     'EQ',
                     msg_in,
                     check_this_in,
                     against_this_in
                  ),
         check_this_in=> TRUE
                         OR (    check_this_in IS NULL
                             AND against_this_in IS NULL
                             AND null_ok_in
                            ),
         null_ok_in=> FALSE ,
         raise_exc_in=> raise_exc_in
      );
   END;

   FUNCTION b2v (bool_in IN BOOLEAN)
      RETURN VARCHAR2
   IS
   BEGIN
      IF bool_in
      THEN
         RETURN 'TRUE';
      ELSIF NOT bool_in
      THEN
         RETURN 'FALSE';
      ELSE
         RETURN 'NULL';
      END IF;
   END;

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   BOOLEAN,
      against_this_in   IN   BOOLEAN,
      null_ok_in        IN   BOOLEAN := FALSE ,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      IF utplsql2.tracing
      THEN
         utreport.pl (
               'Compare "'
            || b2v (check_this_in)
            || '" to "'
            || b2v (against_this_in)
            || '"'
         );
      END IF;

      this (
         outcome_in,
         message_expected (
            'EQ',
            msg_in,
            utplsql.bool2vc (check_this_in),
            utplsql.bool2vc (against_this_in)
         ),
            (check_this_in = against_this_in)
         OR (    check_this_in IS NULL
             AND against_this_in IS NULL
             AND null_ok_in
            ),
         FALSE ,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   DATE,
      against_this_in   IN   DATE,
      null_ok_in        IN   BOOLEAN := FALSE ,
      raise_exc_in      IN   BOOLEAN := FALSE ,
      truncate_in       IN   BOOLEAN := FALSE
   )
   IS
      c_format   CONSTANT VARCHAR2 (30)
               := 'MONTH DD, YYYY HH24MISS';
      v_check             VARCHAR2 (100);
      v_against           VARCHAR2 (100);
   BEGIN
      IF truncate_in
      THEN
         v_check := TO_CHAR (
                       TRUNC (check_this_in),
                       c_format
                    );
         v_against :=
            TO_CHAR (
               TRUNC (against_this_in),
               c_format
            );
      ELSE
         v_check :=
                TO_CHAR (check_this_in, c_format);
         v_against :=
              TO_CHAR (against_this_in, c_format);
      END IF;

      IF utplsql2.tracing
      THEN
         utreport.pl (
               'Compare "'
            || v_check
            || '" to "'
            || v_against
            || '"'
         );
      END IF;

      this (
         outcome_in,
         message_expected (
            'EQ',
            msg_in,
            TO_CHAR (
               check_this_in,
               'MON-DD-YYYY HH:MI:SS'
            ),
            TO_CHAR (
               against_this_in,
               'MON-DD-YYYY HH:MI:SS'
            )
         ),
            (check_this_in = against_this_in)
         OR (    check_this_in IS NULL
             AND against_this_in IS NULL
             AND null_ok_in
            ),
         FALSE ,
         raise_exc_in
      );
   END;

   PROCEDURE ieqminus (
     outcome_in     IN ut_outcome.id%TYPE,
     msg_in         IN VARCHAR2,
     check_fields   IN VARCHAR2,
     against_fields IN VARCHAR2,
     query1_in      IN VARCHAR2,
     query2_in      IN VARCHAR2,
     check_this_in  IN VARCHAR2,
     against_this_in IN VARCHAR2
   )
   IS
   ival      PLS_INTEGER;
   TYPE TabCurType IS REF CURSOR;
   cur1      TabCurType;
   cur2      TabCurType;
   curid1    NUMBER;
   curid2    NUMBER;
   colcnt1   NUMBER;
   colcnt2   NUMBER;
   desctab1  DBMS_SQL.desc_tab;
   desctab2  DBMS_SQL.desc_tab;

   field_str   VARCHAR2(32767);
   record_str  VARCHAR2(32767);

   check_sql VARCHAR2(32767);
   against_sql VARCHAR2(32767);

   v_block   VARCHAR2 (32767) := 'DECLARE
      CURSOR cur IS
            SELECT *
            FROM DUAL
            WHERE EXISTS (('
      || query1_in
      || ' MINUS '
      || query2_in
      || ')
        UNION
        ('
      || query2_in
      || ' MINUS '
      || query1_in
      || '));
      rec cur%ROWTYPE;
      BEGIN
          OPEN cur;
          FETCH cur INTO rec;
          IF cur%FOUND
          THEN
         :retval := 1;
          ELSE
         :retval := 0;
          END IF;
          CLOSE cur;
      END;';

   filehandle utl_file.file_type;

   BEGIN
      EXECUTE IMMEDIATE v_block USING OUT ival;

      if ival=1 then

        filehandle := utl_file.fopen('LINK', check_this_in || '.txt', 'w');

        utl_file.put_line(filehandle, check_fields);

        check_sql := 'select ' || check_fields
        || ' from '
        || check_this_in;

      end if;
      this (
         outcome_in,
         replace_not_placeholder (
            msg_in,
            ival = 0
         ),
         ival = 0,
         FALSE ,

         FALSE
      );
   EXCEPTION
      WHEN OTHERS
      THEN
         this (
            outcome_in,
            replace_not_placeholder (
                  msg_in
               || ' SQL Failure: '
               || SQLERRM,
               SQLCODE = 0
            ),
            SQLCODE = 0,
            FALSE ,

            FALSE
         );
   END;

   PROCEDURE ieqminus (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      query1_in       IN   VARCHAR2,
      query2_in       IN   VARCHAR2,
      minus_desc_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS

      ival      PLS_INTEGER;

      v_block   VARCHAR2 (32767) :=    'DECLARE
         CURSOR cur IS
               SELECT 1
               FROM DUAL
               WHERE EXISTS (('
      || query1_in
      || ' MINUS '
      || query2_in
      || ')
        UNION
        ('
      || query2_in
      || ' MINUS '
      || query1_in
      || '));
          rec cur%ROWTYPE;
      BEGIN
          OPEN cur;
          FETCH cur INTO rec;
          IF cur%FOUND
          THEN
         :retval := 1;
          ELSE
         :retval := 0;
          END IF;
          CLOSE cur;
      END;';
   BEGIN

      EXECUTE IMMEDIATE v_block USING  OUT ival;

      this (
         outcome_in,
         replace_not_placeholder (
            msg_in,
            ival = 0
         ),
         ival = 0,
         FALSE ,
         raise_exc_in
      );
   EXCEPTION
      WHEN OTHERS
      THEN

         this (
            outcome_in,
            replace_not_placeholder (
                  msg_in
               || ' SQL Failure: '
               || SQLERRM,
               SQLCODE = 0
            ),
            SQLCODE = 0,
            FALSE ,
            raise_exc_in
         );
   END;

   PROCEDURE eqtable (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      CURSOR info_cur (
         sch_in   IN   VARCHAR2,
         tab_in   IN   VARCHAR2
      )
      IS
         SELECT   t.column_name
             FROM all_tab_columns t
            WHERE t.owner = sch_in
              AND t.table_name = tab_in
         ORDER BY column_id;

      FUNCTION collist (tab IN VARCHAR2)
         RETURN VARCHAR2
      IS
         l_schema   VARCHAR2 (100);
         l_table    VARCHAR2 (100);
         l_dot      PLS_INTEGER
                               := INSTR (tab, '.');
         retval     VARCHAR2 (32767);
      BEGIN
         IF l_dot = 0
         THEN
            l_schema := USER;
            l_table := UPPER (tab);
         ELSE
            l_schema := UPPER (
                           SUBSTR (
                              tab,
                              1,
                                l_dot
                              - 1
                           )
                        );
            l_table :=
                UPPER (SUBSTR (tab,   l_dot
                                    + 1));
         END IF;

         FOR rec IN info_cur (l_schema, l_table)
         LOOP
            retval :=
                   retval
                || ','
                || rec.column_name;
         END LOOP;

         RETURN LTRIM (retval, ',');
      END;
   BEGIN
      ieqminus (
         outcome_in,
         message (
            'EQTABLE',
            msg_in,
               'Contents of "'
            || check_this_in
            || utplsql.ifelse (
                  check_where_in IS NULL,
                  '"',
                     '" WHERE '
                  || check_where_in
               )
            || ' does '
            || c_not_placeholder
            || 'match "'
            || against_this_in
            || utplsql.ifelse (
                  against_where_in IS NULL,
                  '"',
                     '" WHERE '
                  || against_where_in
               )
         ),
         collist (check_this_in),
         collist (against_this_in),
            'SELECT T1.*, COUNT(*) FROM '
         || check_this_in
         || ' T1  WHERE '
         || NVL (check_where_in, '1=1')
         || ' GROUP BY '
         || collist (check_this_in),
            'SELECT T2.*, COUNT(*) FROM '
         || against_this_in
         || ' T2  WHERE '
         || NVL (against_where_in, '1=1')
         || ' GROUP BY '
         || collist (against_this_in),
         check_this_in,
         against_this_in
      );
   END;

   PROCEDURE eqtabcount (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      ival   PLS_INTEGER;
   BEGIN
      ieqminus (
         outcome_in,
         message (
            'EQTABCOUNT',
            msg_in,
               'Row count of "'
            || check_this_in
            || utplsql.ifelse (
                  check_where_in IS NULL,
                  '"',
                     '" WHERE '
                  || check_where_in
               )
            || ' does '
            || c_not_placeholder
            || 'match that of "'
            || against_this_in
            || utplsql.ifelse (
                  against_where_in IS NULL,
                  '"',
                     '" WHERE '
                  || against_where_in
               )
         ),
            'SELECT COUNT(*) FROM '
         || check_this_in
         || '  WHERE '
         || NVL (check_where_in, '1=1'),
            'SELECT COUNT(*) FROM '
         || against_this_in
         || '  WHERE '
         || NVL (against_where_in, '1=1'),
         'Table Count Equality',
         raise_exc_in
      );
   END;

   PROCEDURE eqquery (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS

      ival   PLS_INTEGER;
   BEGIN
      ieqminus (
         outcome_in,
         message (
            'EQQUERY',
            msg_in,
               'Result set for "'
            || check_this_in
            || ' does '
            || c_not_placeholder
            || 'match that of "'
            || against_this_in
            || '"'
         ),
         check_this_in,
         against_this_in,
         'Query Equality',
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   VARCHAR2,
      null_ok_in         IN   BOOLEAN := FALSE ,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_value     VARCHAR2 (2000);
      l_success   BOOLEAN;

      TYPE cv_t IS REF CURSOR;

      cv          cv_t;

   BEGIN
      IF utplsql2.tracing
      THEN
         utreport.pl (
               'V EQQueryValue Compare "'
            || check_query_in
            || '" to "'
            || against_value_in
            || '"'
         );
      END IF;

      OPEN cv FOR check_query_in;
      FETCH cv INTO l_value;
      CLOSE cv;

      l_success :=
            (l_value = against_value_in)
         OR (    l_value IS NULL
             AND against_value_in IS NULL
             AND null_ok_in
            );
      this (
         outcome_in,
         message (
            'EQQUERYVALUE',
            msg_in,
               'Query "'
            || check_query_in
            || '" returned value "'
            || l_value
            || '" that does '
            || utplsql.ifelse (
                  l_success,
                  NULL,
                  ' not '
               )
            || 'match "'
            || against_value_in
            || '"'
         ),
         l_success,
         FALSE ,
         raise_exc_in
      );

   END;

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   DATE,
      null_ok_in         IN   BOOLEAN := FALSE ,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_value     DATE;
      l_success   BOOLEAN;

      TYPE cv_t IS REF CURSOR;

      cv          cv_t;

   BEGIN
      IF utplsql2.tracing
      THEN
         utreport.pl (
               'D EQQueryValue Compare "'
            || check_query_in
            || '" to "'
            || against_value_in
            || '"'
         );
      END IF;

      OPEN cv FOR check_query_in;
      FETCH cv INTO l_value;
      CLOSE cv;

      l_success :=
            (l_value = against_value_in)
         OR (    l_value IS NULL
             AND against_value_in IS NULL
             AND null_ok_in
            );
      this (
         outcome_in,
         message (
            'EQQUERYVALUE',
            msg_in,
               'Query "'
            || check_query_in
            || '" returned value "'
            || TO_CHAR (
                  l_value,
                  'DD-MON-YYYY HH24:MI:SS'
               )
            || '" that does '
            || utplsql.ifelse (
                  l_success,
                  NULL,
                  ' not '
               )
            || 'match "'
            || TO_CHAR (
                  against_value_in,
                  'DD-MON-YYYY HH24:MI:SS'
               )
            || '"'
         ),
         l_success,
         FALSE ,
         raise_exc_in,
         TRUE
      );

   END;

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.id%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   NUMBER,
      null_ok_in         IN   BOOLEAN := FALSE ,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_value     NUMBER;
      l_success   BOOLEAN;

      TYPE cv_t IS REF CURSOR;

      cv          cv_t;

   BEGIN
      IF utplsql2.tracing
      THEN
         utreport.pl (
               'N EQQueryValue Compare "'
            || check_query_in
            || '" to "'
            || against_value_in
            || '"'
         );
      END IF;

      OPEN cv FOR check_query_in;
      FETCH cv INTO l_value;
      CLOSE cv;

      l_success :=
            (l_value = against_value_in)
         OR (    l_value IS NULL
             AND against_value_in IS NULL
             AND null_ok_in
            );
      this (
         outcome_in,
         message (
            'EQQUERYVALUE',
            msg_in,
               'Query "'
            || check_query_in
            || '" returned value "'
            || l_value
            || '" that does '
            || utplsql.ifelse (
                  l_success,
                  NULL,
                  ' not '
               )
            || 'match "'
            || against_value_in
            || '"'
         ),
         l_success,
         FALSE ,
         raise_exc_in,
         TRUE
      );

   END;

   PROCEDURE eqcursor (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )

   IS
   BEGIN
      utreport.pl (
         'utAssert.eqCursor is not yet implemented!'
      );
   END;

   PROCEDURE eqfile (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      check_this_dir_in     IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      against_this_dir_in   IN   VARCHAR2 := NULL,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      checkid                  UTL_FILE.file_type;
      againstid                UTL_FILE.file_type;
      samefiles                BOOLEAN          := TRUE ;
      checkline                VARCHAR2 (32767);
      diffline                 VARCHAR2 (32767);
      againstline              VARCHAR2 (32767);
      check_eof                BOOLEAN;
      against_eof              BOOLEAN;
      diffline_set             BOOLEAN;
      nth_line                 PLS_INTEGER        := 1;
      cant_open_check_file     EXCEPTION;
      cant_open_against_file   EXCEPTION;

      PROCEDURE cleanup (
         val           IN   BOOLEAN,
         line_in       IN   VARCHAR2 := NULL,
         line_set_in   IN   BOOLEAN := FALSE ,
         linenum_in    IN   PLS_INTEGER := NULL,
         msg_in        IN   VARCHAR2
      )
      IS
      BEGIN
         UTL_FILE.fclose (checkid);
         UTL_FILE.fclose (againstid);
         this (
            outcome_in,
            message (
               'EQFILE',
               msg_in,
                  utplsql.ifelse (
                     line_set_in,
                        ' Line '
                     || linenum_in
                     || ' of ',
                     NULL
                  )
               || 'File "'
               || file_descrip (
                     check_this_in,
                     check_this_dir_in
                  )
               || '" does '
               || utplsql.ifelse (
                     val,
                     NULL,
                     ' not '
                  )
               || 'match "'
               || file_descrip (
                     against_this_in,
                     against_this_dir_in
                  )
               || '".'
            ),
            val,
            FALSE ,
            raise_exc_in,
            TRUE
         );
      END;
   BEGIN

      BEGIN
         checkid :=
            UTL_FILE.fopen (
               check_this_dir_in,
               check_this_in,
               'R' , max_linesize => 32767
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            RAISE cant_open_check_file;
      END;

      BEGIN
         againstid :=
            UTL_FILE.fopen (
               NVL (
                  against_this_dir_in,
                  check_this_dir_in
               ),
               against_this_in,
               'R'
           , max_linesize => 32767
            );
      EXCEPTION
         WHEN OTHERS
         THEN
            RAISE cant_open_against_file;
      END;

      LOOP
         BEGIN
            UTL_FILE.get_line (
               checkid,
               checkline
            );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               check_eof := TRUE ;
         END;

         BEGIN
            UTL_FILE.get_line (
               againstid,
               againstline
            );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               against_eof := TRUE ;
         END;

         IF (    check_eof
             AND against_eof
            )
         THEN
            samefiles := TRUE ;
            EXIT;
         ELSIF (checkline != againstline)
         THEN
            diffline := checkline;
            diffline_set := TRUE ;
            samefiles := FALSE ;
            EXIT;
         ELSIF (   check_eof
                OR against_eof
               )
         THEN
            samefiles := FALSE ;
            EXIT;
         END IF;

         IF samefiles
         THEN
            nth_line :=   nth_line
                        + 1;
         END IF;
      END LOOP;

      cleanup (
         samefiles,
         diffline,
         diffline_set,
         nth_line,
         msg_in
      );
   EXCEPTION
      WHEN cant_open_check_file
      THEN
         cleanup (
            FALSE ,
            msg_in=>    'Unable to open '
                     || file_descrip (
                           check_this_in,
                           check_this_dir_in
                        )
         );
      WHEN cant_open_against_file
      THEN
         cleanup (
            FALSE ,
            msg_in=>    'Unable to open '
                     || file_descrip (
                           against_this_in,
                           NVL (
                              against_this_dir_in,
                              check_this_dir_in
                           )
                        )
         );
      WHEN OTHERS
      THEN
         cleanup (FALSE , msg_in => msg_in);
   END;

   PROCEDURE compare_pipe_tabs (
      tab1                utpipe.msg_tbltype,
      tab2                utpipe.msg_tbltype,
      same_out   IN OUT   BOOLEAN
   )
   IS
      indx   PLS_INTEGER := tab1.FIRST;
   BEGIN
      LOOP
         EXIT WHEN indx IS NULL;

         BEGIN
            IF tab1 (indx).item_type = 9
            THEN
               same_out := tab1 (indx).mvc2 =
                                 tab2 (indx).mvc2;
            ELSIF tab1 (indx).item_type = 6
            THEN
               same_out := tab1 (indx).mnum =
                                 tab2 (indx).mnum;
            ELSIF tab1 (indx).item_type = 12
            THEN
               same_out := tab1 (indx).mdt =
                                  tab2 (indx).mdt;
            ELSIF tab1 (indx).item_type = 11
            THEN
               same_out := tab1 (indx).mrid =
                                 tab2 (indx).mrid;
            ELSIF tab1 (indx).item_type = 23
            THEN
               same_out := tab1 (indx).mraw =
                                 tab2 (indx).mraw;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               same_out := FALSE ;
         END;

         EXIT WHEN NOT same_out;
         indx := tab1.NEXT (indx);
      END LOOP;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         same_out := FALSE ;
   END;

   PROCEDURE eqpipe (
      outcome_in        IN   ut_outcome.id%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      check_tab        utpipe.msg_tbltype;
      against_tab      utpipe.msg_tbltype;
      check_status     PLS_INTEGER;
      against_status   PLS_INTEGER;
      same_message     BOOLEAN     := FALSE ;
      msgset           BOOLEAN;
      msgnum           PLS_INTEGER;
      nthmsg           PLS_INTEGER := 1;
   BEGIN

      LOOP
         utpipe.receive_and_unpack (
            check_this_in,
            check_tab,
            check_status
         );
         utpipe.receive_and_unpack (
            against_this_in,
            against_tab,
            against_status
         );

         IF (    check_status = 0
             AND against_status = 0
            )
         THEN
            compare_pipe_tabs (
               check_tab,
               against_tab,
               same_message
            );

            IF NOT same_message
            THEN
               msgset := TRUE ;
               msgnum := nthmsg;
               EXIT;
            END IF;

            EXIT WHEN NOT same_message;
         ELSIF (    check_status = 1
                AND against_status = 1
               )
         THEN
            same_message := TRUE ;
            EXIT;
         ELSE
            same_message := FALSE ;
            EXIT;
         END IF;

         nthmsg :=   nthmsg
                   + 1;
      END LOOP;

      this (
         outcome_in,
         message (
            'EQPIPE',
            msg_in,
               utplsql.ifelse (
                  msgset,
                     ' Message '
                  || msgnum
                  || ' of ',
                  NULL
               )
            || 'Pipe "'
            || check_this_in
            || '" does '
            || utplsql.ifelse (
                  same_message,
                  NULL,
                  ' not '
               )
            || 'match "'
            || against_this_in
            || '".'
         ),
         same_message,
         FALSE ,
         raise_exc_in,
         TRUE
      );
   END;

   FUNCTION numfromstr (str IN VARCHAR2)
      RETURN NUMBER
   IS
      sqlstr   VARCHAR2 (1000)
            :=    'begin :val := '
               || str
               || '; end;';

      retval   NUMBER;
   BEGIN

      EXECUTE IMMEDIATE sqlstr USING  OUT retval;

      RETURN retval;
   EXCEPTION
      WHEN OTHERS
      THEN

         RAISE;
   END;

   PROCEDURE validatecoll (
      msg_in                IN       VARCHAR2,
      check_this_in         IN       VARCHAR2,
      against_this_in       IN       VARCHAR2,
      valid_out             IN OUT   BOOLEAN,
      msg_out               OUT      VARCHAR2,
      countproc_in          IN       VARCHAR2
            := 'COUNT',
      firstrowproc_in       IN       VARCHAR2
            := 'FIRST',
      lastrowproc_in        IN       VARCHAR2
            := 'LAST',
      check_startrow_in     IN       PLS_INTEGER
            := NULL,
      check_endrow_in       IN       PLS_INTEGER
            := NULL,
      against_startrow_in   IN       PLS_INTEGER
            := NULL,
      against_endrow_in     IN       PLS_INTEGER
            := NULL,
      match_rownum_in       IN       BOOLEAN
            := FALSE ,
      null_ok_in            IN       BOOLEAN
            := TRUE ,
      raise_exc_in          IN       BOOLEAN
            := FALSE ,
      null_and_valid        IN OUT   BOOLEAN
   )
   IS
      dynblock     VARCHAR2 (32767);
      v_matchrow   CHAR (1)         := 'N';
      badc         PLS_INTEGER;
      bada         PLS_INTEGER;
      badtext      VARCHAR2 (32767);
      eqcheck      VARCHAR2 (32767);
   BEGIN
      valid_out := TRUE ;
      null_and_valid := FALSE ;

      IF      numfromstr (
                    check_this_in
                 || '.'
                 || countproc_in
              ) = 0
          AND numfromstr (
                    against_this_in
                 || '.'
                 || countproc_in
              ) = 0
      THEN
         IF NOT null_ok_in
         THEN
            valid_out := FALSE ;
            msg_out := 'Invalid NULL collections';
         ELSE

            null_and_valid := TRUE ;
         END IF;
      END IF;

      IF      valid_out
          AND NOT null_and_valid
      THEN
         IF match_rownum_in
         THEN
            valid_out :=FALSE;

            IF NOT valid_out
            THEN
               msg_out :=
                     'Different starting rows in '
                  || check_this_in
                  || ' and '
                  || against_this_in;
            ELSE
               valid_out :=TRUE;

               IF NOT valid_out
               THEN
                  msg_out :=
                        'Different ending rows in '
                     || check_this_in
                     || ' and '
                     || against_this_in;
               END IF;
            END IF;
         END IF;

         IF valid_out
         THEN
            valid_out :=FALSE;

            IF NOT valid_out
            THEN
               msg_out :=
                     'Different number of rows in '
                  || check_this_in
                  || ' and '
                  || against_this_in;
            END IF;
         END IF;
      END IF;
   END;

   FUNCTION dyncollstr (
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      eqfunc_in         IN   VARCHAR2,
      countproc_in      IN   VARCHAR2,
      firstrowproc_in   IN   VARCHAR2,
      lastrowproc_in    IN   VARCHAR2,
      nextrowproc_in    IN   VARCHAR2,
      getvalfunc_in     IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
      eqcheck     VARCHAR2 (32767);
      v_check     VARCHAR2 (100)   := check_this_in;
      v_against   VARCHAR2 (100)
                               := against_this_in;
   BEGIN
      IF getvalfunc_in IS NOT NULL
      THEN
         v_check :=
                    v_check
                 || '.'
                 || getvalfunc_in;
         v_against :=
                  v_against
               || '.'
               || getvalfunc_in;
      END IF;

      IF eqfunc_in IS NULL
      THEN
         eqcheck :=    '('
                    || v_check
                    || '(cindx) = '
                    || v_against
                    || ' (aindx)) OR '
                    || '('
                    || v_check
                    || '(cindx) IS NULL AND '
                    || v_against
                    || ' (aindx) IS NULL)';
      ELSE
         eqcheck :=    eqfunc_in
                    || '('
                    || v_check
                    || '(cindx), '
                    || v_against
                    || '(aindx))';
      END IF;

      RETURN (   'DECLARE
             cindx PLS_INTEGER;
             aindx PLS_INTEGER;
             cend PLS_INTEGER := NVL (:cendit, '
              || check_this_in
              || '.'
              || lastrowproc_in
              || ');
             aend PLS_INTEGER := NVL (:aendit, '
              || against_this_in
              || '.'
              || lastrowproc_in
              || ');
             different_collections exception;

             PROCEDURE setfailure (
                str IN VARCHAR2,
                badc IN PLS_INTEGER,
                bada IN PLS_INTEGER,
                raiseexc IN BOOLEAN := TRUE)
             IS
             BEGIN
                :badcindx := badc;
                :badaindx := bada;
                :badreason := str;
                IF raiseexc THEN RAISE different_collections; END IF;
             END;
          BEGIN
             cindx := NVL (:cstartit, '
              || check_this_in
              || '.'
              || firstrowproc_in
              || ');
             aindx := NVL (:astartit, '
              || against_this_in
              || '.'
              || firstrowproc_in
              || ');

             LOOP
                IF cindx IS NULL AND aindx IS NULL
                THEN
                   EXIT;

                ELSIF cindx IS NULL and aindx IS NOT NULL
                THEN
                   setfailure (
                      ''Check index NULL, Against index NOT NULL'', cindx, aindx);

                ELSIF aindx IS NULL
                THEN
                   setfailure (
                      ''Check index NOT NULL, Against index NULL'', cindx, aindx);
                END IF;

                IF :matchit = ''Y''
                   AND cindx != aindx
                THEN
                   setfailure (''Mismatched row numbers'', cindx, aindx);
                END IF;

                BEGIN
                   IF '
              || eqcheck
              || '
                   THEN
                      NULL;
                   ELSE
                      setfailure (''Mismatched row values'', cindx, aindx);
                   END IF;
                EXCEPTION
                   WHEN OTHERS
                   THEN
                      setfailure (''On EQ check: '
              || eqcheck
              || ''' || '' '' || SQLERRM, cindx, aindx);
                END;

                cindx := '
              || check_this_in
              || '.'
              || nextrowproc_in
              || '(cindx);
                aindx := '
              || against_this_in
              || '.'
              || nextrowproc_in
              || '(aindx);
             END LOOP;
          EXCEPTION
             WHEN OTHERS THEN
                IF :badcindx IS NULL and :badaindx IS NULL
                THEN setfailure (SQLERRM, cindx, aindx, FALSE);
                END IF;
          END;'
             );
   END;

   FUNCTION collection_message (
      collapi_in   IN   BOOLEAN,
      msg_in       IN   VARCHAR2,
      chkcoll_in   IN   VARCHAR2,
      chkrow_in    IN   INTEGER,
      agcoll_in    IN   VARCHAR2,
      agrow_in     IN   INTEGER,
      success_in   IN   BOOLEAN
   )
      RETURN VARCHAR2
   IS
      assert_name   VARCHAR2 (100) := 'EQCOLL';
   BEGIN
      IF collapi_in
      THEN
         assert_name := 'EQCOLLAPI';
      END IF;

      RETURN message (
                assert_name,
                msg_in,
                   utplsql.ifelse (
                      success_in,
                      NULL,
                         ' Row '
                      || NVL (
                            TO_CHAR (agrow_in),
                            '*UNDEFINED*'
                         )
                      || ' of '
                   )
                || 'Collection "'
                || agcoll_in
                || '" does '
                || utplsql.ifelse (
                      success_in,
                      NULL,
                      ' not '
                   )
                || 'match '
                || utplsql.ifelse (
                      success_in,
                      NULL,
                         ' Row '
                      || NVL (
                            TO_CHAR (chkrow_in),
                            '*UNDEFINED*'
                         )
                      || ' of '
                   )
                || chkcoll_in
                || '".'
             );
   END;

   PROCEDURE eqcoll (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      check_startrow_in     IN   PLS_INTEGER
            := NULL,
      check_endrow_in       IN   PLS_INTEGER
            := NULL,
      against_startrow_in   IN   PLS_INTEGER
            := NULL,
      against_endrow_in     IN   PLS_INTEGER
            := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE ,
      null_ok_in            IN   BOOLEAN := TRUE ,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      dynblock              VARCHAR2 (32767);
      v_matchrow            CHAR (1)         := 'N';
      valid_interim         BOOLEAN;
      invalid_interim_msg   VARCHAR2 (4000);
      badc                  PLS_INTEGER;
      bada                  PLS_INTEGER;
      badtext               VARCHAR2 (32767);
      null_and_valid        BOOLEAN          := FALSE ;

   BEGIN
      validatecoll (
         msg_in,
         check_this_in,
         against_this_in,
         valid_interim,
         invalid_interim_msg,
         'COUNT',
         'FIRST',
         'LAST',
         check_startrow_in,
         check_endrow_in,
         against_startrow_in,
         against_endrow_in,
         match_rownum_in,
         null_ok_in,
         raise_exc_in,
         null_and_valid
      );

      IF NOT valid_interim
      THEN

         this (
            outcome_in,
            collection_message (
               FALSE ,
                  msg_in
               || ' - '
               || invalid_interim_msg,
               check_this_in,
               NULL,
               against_this_in,
               NULL,
               FALSE
            ),
            FALSE ,
            raise_exc_in,
            TRUE
         );
      ELSE

         IF NOT null_and_valid
         THEN
            IF match_rownum_in
            THEN
               v_matchrow := 'Y';
            END IF;

            dynblock :=
               dyncollstr (
                  check_this_in,
                  against_this_in,
                  eqfunc_in,
                  'COUNT',
                  'FIRST',
                  'LAST',
                  'NEXT',
                  NULL
               );

            EXECUTE IMMEDIATE dynblock
               USING  IN check_endrow_in,
                IN    against_endrow_in,
                IN  OUT badc,
                IN  OUT bada,
                IN  OUT badtext,
                IN    check_startrow_in,
                IN    against_startrow_in,
                IN    v_matchrow;

         END IF;

         this (
            outcome_in,
            collection_message (
               FALSE ,
               msg_in,
               check_this_in,
               badc,
               against_this_in,
               bada,
                   badc IS NULL
               AND bada IS NULL
            ),
                badc IS NULL
            AND bada IS NULL,
            FALSE ,
            raise_exc_in,
            TRUE
         );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN

         this (
            outcome_in,
            collection_message (
               FALSE ,
                  msg_in
               || ' SQLERROR: '
               || SQLERRM,
               check_this_in,
               badc,
               against_this_in,
               bada,
               SQLCODE = 0
            ),
            SQLCODE = 0,
            FALSE ,
            raise_exc_in,
            TRUE
         );
   END;

   PROCEDURE eqcollapi (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_pkg_in     IN   VARCHAR2,
      against_this_pkg_in   IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      countfunc_in          IN   VARCHAR2 := 'COUNT',
      firstrowfunc_in       IN   VARCHAR2 := 'FIRST',
      lastrowfunc_in        IN   VARCHAR2 := 'LAST',
      nextrowfunc_in        IN   VARCHAR2 := 'NEXT',
      getvalfunc_in         IN   VARCHAR2
            := 'NTHVAL',
      check_startrow_in     IN   PLS_INTEGER
            := NULL,
      check_endrow_in       IN   PLS_INTEGER
            := NULL,
      against_startrow_in   IN   PLS_INTEGER
            := NULL,
      against_endrow_in     IN   PLS_INTEGER
            := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE ,
      null_ok_in            IN   BOOLEAN := TRUE ,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      dynblock              VARCHAR2 (32767);
      v_matchrow            CHAR (1)         := 'N';
      badc                  PLS_INTEGER;
      bada                  PLS_INTEGER;
      badtext               VARCHAR2 (32767);
      valid_interim         BOOLEAN;
      invalid_interim_msg   VARCHAR2 (4000);
      null_and_valid        BOOLEAN          := FALSE ;

   BEGIN
      validatecoll (
         msg_in,
         check_this_pkg_in,
         against_this_pkg_in,
         valid_interim,
         invalid_interim_msg,
         countfunc_in,
         firstrowfunc_in,
         lastrowfunc_in,
         check_startrow_in,
         check_endrow_in,
         against_startrow_in,
         against_endrow_in,
         match_rownum_in,
         null_ok_in,
         raise_exc_in,
         null_and_valid
      );

      IF null_and_valid
      THEN
         GOTO normal_termination;
      END IF;

      IF match_rownum_in
      THEN
         v_matchrow := 'Y';
      END IF;

      dynblock := dyncollstr (
                     check_this_pkg_in,
                     against_this_pkg_in,
                     eqfunc_in,
                     countfunc_in,
                     firstrowfunc_in,
                     lastrowfunc_in,
                     nextrowfunc_in,
                     getvalfunc_in
                  );

      EXECUTE IMMEDIATE dynblock
         USING  IN check_endrow_in,
          IN    against_endrow_in,
          IN  OUT badc,
          IN  OUT bada,
          IN  OUT badtext,
          IN    check_startrow_in,
          IN    against_startrow_in,
          IN    v_matchrow;

      <<normal_termination>>
      this (
         outcome_in,
         collection_message (
            TRUE ,
            msg_in,
            check_this_pkg_in,
            badc,
            against_this_pkg_in,
            bada,
                badc IS NULL
            AND bada IS NULL
         ),
             bada IS NULL
         AND badc IS NULL,
         FALSE ,
         raise_exc_in,
         TRUE
      );
   EXCEPTION
      WHEN OTHERS
      THEN

         this (
            outcome_in,
            collection_message (
               TRUE ,
                  msg_in
               || ' SQLERROR: '
               || SQLERRM,
               check_this_pkg_in,
               badc,
               against_this_pkg_in,
               bada,
                   badc IS NULL
               AND bada IS NULL
            ),
            SQLCODE = 0,
            FALSE ,
            raise_exc_in,
            TRUE
         );
   END;

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      this (
         outcome_in,
         message_expected (
            'ISNOTNULL',
            msg_in,
            check_this_in,
            'NOT NULL'
         ),
         check_this_in IS NOT NULL,
         FALSE ,
         raise_exc_in
      );
   END;

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      this (
         outcome_in,
         message_expected (
            'ISNULL',
            msg_in,
            check_this_in,
            ''
         ),
         check_this_in IS NULL,
         TRUE ,
         raise_exc_in
      );
   END;

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      this (
         outcome_in,
         message_expected (
            'ISNOTNULL',
            msg_in,
            utplsql.bool2vc (check_this_in),
            'NOT NULL'
         ),
         check_this_in IS NOT NULL,
         FALSE ,
         raise_exc_in
      );
   END;

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      this (
         outcome_in,
         message_expected (
            'ISNULL',
            msg_in,
            utplsql.bool2vc (check_this_in),
            ''
         ),
         check_this_in IS NULL,
         TRUE ,
         raise_exc_in
      );
   END;

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   )
   IS
      expected_indicator   PLS_INTEGER      := 1000;
      l_indicator          PLS_INTEGER;
      v_block              VARCHAR2 (32767)
   :=    'BEGIN '
      || RTRIM (RTRIM (check_call_in), ';')
      || ';
               :indicator := 0;
             EXCEPTION
                WHEN '
      || against_exc_in
      || ' THEN
                   :indicator := '
      || expected_indicator
      || ';
                WHEN OTHERS THEN :indicator := SQLCODE;
             END;';

   BEGIN

      EXECUTE IMMEDIATE v_block USING  OUT l_indicator;

      this (
         outcome_in,
         message (
            'RAISES',
            msg_in,
               'Block "'
            || check_call_in
            || '"'
            || ' Exception "'
            || against_exc_in
            || utplsql.ifelse (
                  l_indicator =
                               expected_indicator,
                  NULL,
                     '. Instead it raises SQLCODE = '
                  || l_indicator
                  || '.'
               )
         ),
         l_indicator = expected_indicator
      );
   END;

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   )
   IS
      expected_indicator   PLS_INTEGER      := 1000;
      l_indicator          PLS_INTEGER;
      v_block              VARCHAR2 (32767)
   :=    'BEGIN '
      || RTRIM (RTRIM (check_call_in), ';')
      || ';
               :indicator := 0;
             EXCEPTION
                WHEN OTHERS
                   THEN IF SQLCODE = '
      || against_exc_in
      || ' THEN :indicator := '
      || expected_indicator
      || ';'
      || ' ELSE :indicator := SQLCODE; END IF;
             END;';

   BEGIN

      EXECUTE IMMEDIATE v_block USING  OUT l_indicator;

      this (
         outcome_in,
         message (
            'THROWS',
            msg_in,
               'Block "'
            || check_call_in
            || '"'
            || ' Exception "'
            || against_exc_in
            || utplsql.ifelse (
                  l_indicator =
                               expected_indicator,
                  NULL,
                     '. Instead it raises SQLCODE = '
                  || l_indicator
                  || '.'
               )
         ),
         l_indicator = expected_indicator
      );
   END;

   PROCEDURE throws (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   )
   IS
   BEGIN
      raises (
         outcome_in,
         msg_in,
         check_call_in,
         against_exc_in
      );
   END;

   PROCEDURE throws (
      outcome_in       IN   ut_outcome.id%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   )
   IS
   BEGIN
      raises (
         outcome_in,
         msg_in,
         check_call_in,
         against_exc_in
      );
   END;

   PROCEDURE this (
      outcome_in      IN   ut_outcome.NAME%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      null_ok_in      IN   BOOLEAN := FALSE ,
      raise_exc_in    IN   BOOLEAN := FALSE ,
      register_in     IN   BOOLEAN := TRUE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      this (
         l_id,
         msg_in,
         check_this_in,
         null_ok_in,
         raise_exc_in,
         register_in
      );
   END;

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.NAME%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      null_ok_in        IN   BOOLEAN := FALSE ,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eq (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.NAME%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   BOOLEAN,
      against_this_in   IN   BOOLEAN,
      null_ok_in        IN   BOOLEAN := FALSE ,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eq (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq (
      outcome_in        IN   ut_outcome.NAME%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   DATE,
      against_this_in   IN   DATE,
      null_ok_in        IN   BOOLEAN := FALSE ,
      raise_exc_in      IN   BOOLEAN := FALSE ,
      truncate_in       IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eq (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         null_ok_in,
         raise_exc_in,
         truncate_in
      );
   END;

   PROCEDURE eqtable (
      outcome_in         IN   ut_outcome.NAME%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqtable (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         check_where_in,
         against_where_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqtabcount (
      outcome_in         IN   ut_outcome.NAME%TYPE,
      msg_in             IN   VARCHAR2,
      check_this_in      IN   VARCHAR2,
      against_this_in    IN   VARCHAR2,
      check_where_in     IN   VARCHAR2 := NULL,
      against_where_in   IN   VARCHAR2 := NULL,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqtabcount (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         check_where_in,
         against_where_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqquery (
      outcome_in        IN   ut_outcome.NAME%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqquery (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.NAME%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   VARCHAR2,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqqueryvalue (
         l_id,
         msg_in,
         check_query_in,
         against_value_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.NAME%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   DATE,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqqueryvalue (
         l_id,
         msg_in,
         check_query_in,
         against_value_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqqueryvalue (
      outcome_in         IN   ut_outcome.NAME%TYPE,
      msg_in             IN   VARCHAR2,
      check_query_in     IN   VARCHAR2,
      against_value_in   IN   NUMBER,
      raise_exc_in       IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqqueryvalue (
         l_id,
         msg_in,
         check_query_in,
         against_value_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqcursor (
      outcome_in        IN   ut_outcome.NAME%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )

   IS
   BEGIN
      utreport.pl (
         'utAssert.eqCursor is not yet implemented!'
      );
   END;

   PROCEDURE eqfile (
      outcome_in            IN   ut_outcome.NAME%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      check_this_dir_in     IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      against_this_dir_in   IN   VARCHAR2 := NULL,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqfile (
         l_id,
         msg_in,
         check_this_in,
         check_this_dir_in,
         against_this_in,
         against_this_dir_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqpipe (
      outcome_in        IN   ut_outcome.NAME%TYPE,
      msg_in            IN   VARCHAR2,
      check_this_in     IN   VARCHAR2,
      against_this_in   IN   VARCHAR2,
      raise_exc_in      IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqpipe (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqcoll (
      outcome_in            IN   ut_outcome.NAME%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   VARCHAR2,
      against_this_in       IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      check_startrow_in     IN   PLS_INTEGER
            := NULL,
      check_endrow_in       IN   PLS_INTEGER
            := NULL,
      against_startrow_in   IN   PLS_INTEGER
            := NULL,
      against_endrow_in     IN   PLS_INTEGER
            := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE ,
      null_ok_in            IN   BOOLEAN := TRUE ,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqcoll (
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         eqfunc_in,
         check_startrow_in,
         check_endrow_in,
         against_startrow_in,
         against_endrow_in,
         match_rownum_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqcollapi (
      outcome_in            IN   ut_outcome.NAME%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_pkg_in     IN   VARCHAR2,
      against_this_pkg_in   IN   VARCHAR2,
      eqfunc_in             IN   VARCHAR2 := NULL,
      countfunc_in          IN   VARCHAR2 := 'COUNT',
      firstrowfunc_in       IN   VARCHAR2 := 'FIRST',
      lastrowfunc_in        IN   VARCHAR2 := 'LAST',
      nextrowfunc_in        IN   VARCHAR2 := 'NEXT',
      getvalfunc_in         IN   VARCHAR2
            := 'NTHVAL',
      check_startrow_in     IN   PLS_INTEGER
            := NULL,
      check_endrow_in       IN   PLS_INTEGER
            := NULL,
      against_startrow_in   IN   PLS_INTEGER
            := NULL,
      against_endrow_in     IN   PLS_INTEGER
            := NULL,
      match_rownum_in       IN   BOOLEAN := FALSE ,
      null_ok_in            IN   BOOLEAN := TRUE ,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqcollapi (
         l_id,
         msg_in,
         check_this_pkg_in,
         against_this_pkg_in,
         eqfunc_in,
         countfunc_in,
         firstrowfunc_in,
         lastrowfunc_in,
         nextrowfunc_in,
         getvalfunc_in,
         check_startrow_in,
         check_endrow_in,
         against_startrow_in,
         against_endrow_in,
         match_rownum_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.NAME%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      isnotnull (
         l_id,
         msg_in,
         check_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.NAME%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      isnull (
         l_id,
         msg_in,
         check_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE isnotnull (
      outcome_in      IN   ut_outcome.NAME%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      isnotnull (
         l_id,
         msg_in,
         check_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE isnull (
      outcome_in      IN   ut_outcome.NAME%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   BOOLEAN,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      isnull (
         l_id,
         msg_in,
         check_this_in,
         raise_exc_in
      );
   END;

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.NAME%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      raises (
         l_id,
         msg_in,
         check_call_in,
         against_exc_in
      );
   END;

   PROCEDURE raises (
      outcome_in       IN   ut_outcome.NAME%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      raises (
         l_id,
         msg_in,
         check_call_in,
         against_exc_in
      );
   END;

   PROCEDURE throws (
      outcome_in       IN   ut_outcome.NAME%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   VARCHAR2
   )
   IS
   BEGIN
      raises (
         outcome_in,
         msg_in,
         check_call_in,
         against_exc_in
      );
   END;

   PROCEDURE throws (
      outcome_in       IN   ut_outcome.NAME%TYPE,
      msg_in                VARCHAR2,
      check_call_in    IN   VARCHAR2,
      against_exc_in   IN   NUMBER
   )
   IS
   BEGIN
      raises (
         outcome_in,
         msg_in,
         check_call_in,
         against_exc_in
      );
   END;

   PROCEDURE fileexists (
      outcome_in     IN   ut_outcome.id%TYPE,
      msg_in         IN   VARCHAR2,
      dir_in         IN   VARCHAR2,
      file_in        IN   VARCHAR2,
      null_ok_in     IN   BOOLEAN := FALSE ,
      raise_exc_in   IN   BOOLEAN := FALSE
   )
   IS
      checkid   UTL_FILE.file_type;

      PROCEDURE cleanup (
         val      IN   BOOLEAN,
         msg_in   IN   VARCHAR2
      )
      IS
      BEGIN
         UTL_FILE.fclose (checkid);
         this (
            outcome_in,
            message (
               'FILEEXISTS',
               msg_in,
                  'File "'
               || file_descrip (file_in, dir_in)
               || '" could '
               || utplsql.ifelse (
                     val,
                     NULL,
                     ' not '
                  )
               || 'be opened for reading."'
            ),
            val,
            FALSE ,
            raise_exc_in,
            TRUE
         );
      END;
   BEGIN
      checkid :=
         UTL_FILE.fopen (
            dir_in,
            file_in,
            'R' , max_linesize => 32767
         );
      cleanup (TRUE , msg_in);
   EXCEPTION
      WHEN OTHERS
      THEN
         cleanup (FALSE , msg_in);
   END;

   PROCEDURE showresults
   IS
   BEGIN
      g_showresults := TRUE ;
   END;

   PROCEDURE noshowresults
   IS
   BEGIN
      g_showresults := FALSE ;
   END;

   FUNCTION showing_results
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN g_showresults;
   END;

   FUNCTION find_obj (check_this_in IN VARCHAR2)
      RETURN BOOLEAN
   IS
      v_st         VARCHAR2 (20);
      v_err        VARCHAR2 (100);
      v_schema     VARCHAR2 (100);
      v_obj_name   VARCHAR2 (100);
      v_point      NUMBER
                     := INSTR (check_this_in, '.');
      v_state      BOOLEAN        := FALSE ;
      v_val        VARCHAR2 (30);

      CURSOR c_obj
      IS
         SELECT object_name
           FROM all_objects
          WHERE object_name = UPPER (v_obj_name)
            AND owner = UPPER (v_schema);
   BEGIN
      IF v_point = 0
      THEN
         v_schema := USER;
         v_obj_name := check_this_in;
      ELSE
         v_schema := SUBSTR (
                        check_this_in,
                        0,
                        (  v_point
                         - 1
                        )
                     );
         v_obj_name := SUBSTR (
                          check_this_in,
                          (  v_point
                           + 1
                          )
                       );
      END IF;

      OPEN c_obj;
      FETCH c_obj INTO v_val;

      IF c_obj%FOUND
      THEN
         v_state := TRUE ;
      ELSE
         v_state := FALSE ;
      END IF;

      CLOSE c_obj;
      RETURN v_state;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN FALSE ;
   END;

   PROCEDURE objexists (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      null_ok_in      IN   BOOLEAN := FALSE ,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      IF utplsql2.tracing
      THEN

         utreport.pl (
               'verfying that the object "'
            || check_this_in
            || '"exists'
         );
      END IF;

      this (
         outcome_in,
         message (
            msg_in,
            check_this_in,
            'This object Exists'
         ),
         message (
            msg_in,
            check_this_in,
            'This object does not Exist'
         ),
         find_obj (check_this_in),
         null_ok_in,
         raise_exc_in,
         TRUE
      );
   END;

   PROCEDURE objnotexists (
      outcome_in      IN   ut_outcome.id%TYPE,
      msg_in          IN   VARCHAR2,
      check_this_in   IN   VARCHAR2,
      null_ok_in      IN   BOOLEAN := FALSE ,
      raise_exc_in    IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      IF utplsql2.tracing
      THEN

         utreport.pl (
               'verifying that the object "'
            || check_this_in
            || '"does not exist'
         );
      END IF;

      this (
         outcome_in,
         message (
            msg_in,
            check_this_in,
            'This object does not Exist'
         ),
         message (
            msg_in,
            check_this_in,
            'This object Exists'
         ),
         NOT (find_obj (check_this_in)),
         null_ok_in,
         raise_exc_in,
         TRUE
      );
   END;

   FUNCTION previous_passed
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN g_previous_pass;
   END;

   FUNCTION previous_failed
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN NOT g_previous_pass;
   END;

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   DBMS_OUTPUT.CHARARR,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      v_check_index BINARY_INTEGER;
      v_against_index BINARY_INTEGER;
      v_message VARCHAR2(1000);
      v_line1 VARCHAR2(1000);
      v_line2 VARCHAR2(1000);
      WHITESPACE CONSTANT CHAR(5) := '!' || CHR(9) || CHR(10) || CHR(13) || CHR(32) ;
      NOWHITESPACE CONSTANT CHAR(1) := '!';

      FUNCTION Preview_Line(line_in VARCHAR2)
         RETURN VARCHAR2
      IS
      BEGIN
        IF LENGTH(line_in) <= 100 THEN
          RETURN line_in;
        ELSE
          RETURN SUBSTR(line_in, 1, 97) || '...';
        END IF;
      END;

   BEGIN
      v_check_index := check_this_in.FIRST;
      v_against_index := against_this_in.FIRST;

      WHILE v_check_index IS NOT NULL
         AND v_against_index IS NOT NULL
         AND v_message IS NULL
      LOOP

         v_line1 := check_this_in(v_check_index);
         v_line2 := against_this_in(v_against_index);

         IF ignore_case_in THEN
           v_line1 := UPPER(v_line1);
           v_line2 := UPPER(v_line2);
         END IF;

         IF ignore_whitespace_in THEN
           v_line1 := TRANSLATE(v_line1, WHITESPACE, NOWHITESPACE);
           v_line2 := TRANSLATE(v_line2, WHITESPACE, NOWHITESPACE);
         END IF;

         IF (TRUE) THEN
           v_message := message_expected (
              'EQOUTPUT',
              msg_in,
              Preview_Line(check_this_in(v_check_index)),
              Preview_Line(against_this_in(v_against_index))) ||
                 ' (Comparing line ' || v_check_index ||
                 ' of tested collection against line ' || v_against_index ||
                 ' of reference collection)';
         END IF;

         v_check_index := check_this_in.NEXT(v_check_index);
         v_against_index := against_this_in.NEXT(v_against_index);
      END LOOP;

      IF v_message IS NULL THEN
         IF v_check_index IS NULL AND v_against_index IS NOT NULL THEN
            v_message := message (
               'EQOUTPUT',
                msg_in ,
                'Extra line found at end of reference collection: ' ||
                   Preview_Line(against_this_in(v_against_index)));
         ELSIF v_check_index IS NOT NULL AND v_against_index IS NULL THEN
            v_message := message (
               'EQOUTPUT',
                msg_in ,
                'Extra line found at end of tested collection: ' ||
                   Preview_Line(check_this_in(v_check_index)));
         END IF;
      END IF;

      this(outcome_in,
           NVL(v_message, message('EQOUTPUT', msg_in, 'Collections Match')),
           v_message IS NULL,
           FALSE,
           raise_exc_in,
           TRUE);

   END;

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   DBMS_OUTPUT.CHARARR,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqoutput(
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         ignore_case_in,
         ignore_whitespace_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.id%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   VARCHAR2,
      line_delimiter_in     IN   CHAR := NULL,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_buffer DBMS_OUTPUT.CHARARR;
      l_against_this VARCHAR2(2000) := against_this_in;
      l_delimiter_pos BINARY_INTEGER;
   BEGIN

      IF line_delimiter_in IS NULL THEN
        l_against_this := REPLACE(l_against_this, CHR(13) || CHR(10), CHR(10));
      END IF;

      WHILE l_against_this IS NOT NULL LOOP
        l_delimiter_pos := INSTR(l_against_this, NVL(line_delimiter_in, CHR(10)));
        IF l_delimiter_pos = 0 THEN
          l_buffer(l_buffer.COUNT) := l_against_this;
          l_against_this := NULL;
         ELSE
          l_buffer(l_buffer.COUNT) := SUBSTR(l_against_this, 1, l_delimiter_pos - 1);
          l_against_this := SUBSTR(l_against_this, l_delimiter_pos + 1);
          IF l_against_this IS NULL THEN
             l_buffer(l_buffer.COUNT) := NULL;
          END IF;
        END IF;
      END LOOP;

      eqoutput(
         outcome_in,
         msg_in,
         check_this_in,
         l_buffer,
         ignore_case_in,
         ignore_whitespace_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eqoutput (
      outcome_in            IN   ut_outcome.name%TYPE,
      msg_in                IN   VARCHAR2,
      check_this_in         IN   DBMS_OUTPUT.CHARARR,
      against_this_in       IN   VARCHAR2,
      line_delimiter_in     IN   CHAR := NULL,
      ignore_case_in        IN   BOOLEAN := FALSE,
      ignore_whitespace_in  IN   BOOLEAN := FALSE,
      null_ok_in            IN   BOOLEAN := TRUE,
      raise_exc_in          IN   BOOLEAN := FALSE
   )
   IS
      l_id   ut_outcome.id%TYPE;
   BEGIN
      get_id (outcome_in, l_id);
      eqoutput(
         l_id,
         msg_in,
         check_this_in,
         against_this_in,
         line_delimiter_in,
         ignore_case_in,
         ignore_whitespace_in,
         null_ok_in,
         raise_exc_in
      );
   END;

   PROCEDURE eq_refc_table(
      outcome_in        IN   ut_outcome.id%TYPE,
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      table_name        IN   VARCHAR2 )
   IS
      refc_table_name VARCHAR2(50);
   BEGIN
      refc_table_name := utplsql_util.prepare_and_fetch_rc(proc_name,params,cursor_position,1,table_name);
      IF (refc_table_name IS NOT NULL) THEN
         IF (utplsql.tracing) THEN
             dbms_output.put_line('Doing eqtable ');
         END IF;
         utassert2.eqtable(outcome_in,p_msg_nm,refc_table_name,table_name);
      END IF;
      IF (utplsql.tracing) THEN
         dbms_output.put_line('Table dropped '||refc_table_name);
      END IF;
      utplsql_util.execute_ddl('DROP TABLE '||refc_table_name);
   END;

   PROCEDURE eq_refc_query(
      outcome_in        IN   ut_outcome.id%TYPE,
      p_msg_nm          IN   VARCHAR2,
      proc_name         IN   VARCHAR2,
      params            IN   utplsql_util.utplsql_params,
      cursor_position   IN   PLS_INTEGER,
      qry               IN   VARCHAR2 )
   IS
      refc_table_name VARCHAR2(50);
   BEGIN
      refc_table_name := utplsql_util.prepare_and_fetch_rc(proc_name,params,cursor_position,2,qry);
      IF (refc_table_name IS NOT NULL) THEN
         utassert2.eqquery(outcome_in,p_msg_nm,'select * from '||refc_table_name,qry);
      END IF;
      IF (utplsql.tracing) THEN
         dbms_output.put_line('Table dropped '||refc_table_name);
      END IF;
      utplsql_util.execute_ddl('DROP TABLE '||refc_table_name);
   END;
END utassert2;
//

CREATE OR REPLACE PACKAGE BODY utconfig
IS

   g_config   ut_config%ROWTYPE   := NULL;

   FUNCTION config (username_in IN VARCHAR2 := USER)
      RETURN ut_config%ROWTYPE
   IS
      rec   ut_config%ROWTYPE;
   BEGIN

      IF username_in = tester
      THEN
         RETURN g_config;
      END IF;

      BEGIN

         SELECT *
           INTO rec
           FROM ut_config
          WHERE username = UPPER (username_in);
      EXCEPTION

         WHEN OTHERS
         THEN

            rec := NULL;
      END;

      RETURN rec;
   END;

   PROCEDURE setconfig (
      field_in      IN   VARCHAR2
     ,value_in           VARCHAR2
     ,username_in   IN   VARCHAR2 := NULL
   )
   IS
    PRAGMA AUTONOMOUS_TRANSACTION;

      PROCEDURE do_dml (statement_in IN VARCHAR2)
      IS

      BEGIN

        EXECUTE IMMEDIATE statement_in; COMMIT;

      END;
   BEGIN
      BEGIN

         do_dml (   'INSERT INTO ut_config(username, '
                 || field_in
                 || ')'
                 || 'VALUES ('''
                 || username_in
                 || ''', '''
                 || value_in
                 || ''')'
                );
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN

            do_dml (   'UPDATE ut_config '
                    || 'SET '
                    || field_in
                    || ' = '''
                    || value_in
                    || ''' '
                    || 'WHERE username = '''
                    || username_in
                    || ''' '
                   );
         WHEN OTHERS
         THEN

            UtOutputreporter.pl (SQLERRM);

            RETURN;
      END;

     COMMIT;

      IF username_in = tester
      THEN
         g_config := NULL;
         settester (username_in);
      END IF;
   END;

   PROCEDURE settester (username_in IN VARCHAR2 := USER)
   IS
   BEGIN

      g_config := config (username_in);

      g_config.username := NVL (g_config.username, UPPER (username_in));
   END;

   FUNCTION tester
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN g_config.username;
   END;

   PROCEDURE showconfig (username_in IN VARCHAR2 := NULL)
   IS

      v_user   VARCHAR2 (32)       := NVL (username_in, tester);

      rec      ut_config%ROWTYPE;
   BEGIN
      rec := config (v_user);
      utOutputReporter.pl ('=============================================================');
      utOutputReporter.pl ('utPLSQL Configuration for ' || v_user);
      utOutputReporter.pl ('   Directory: ' || rec.DIRECTORY);
      utOutputReporter.pl ('   Autcompile? ' || rec.autocompile);
      utOutputReporter.pl ('   Manual test registration? ' || rec.registertest);
      utOutputReporter.pl ('   Prefix = ' || rec.prefix);
      utOutputReporter.pl ('   Default reporter     = ' || rec.reporter);
      utOutputReporter.pl ('   ----- File Output settings:');
      utOutputReporter.pl ('   Output directory: ' || rec.filedir);
      utOutputReporter.pl ('   User prefix     = ' || rec.fileuserprefix);
      utOutputReporter.pl ('   Include progname? ' || rec.fileincprogname);
      utOutputReporter.pl ('   Date format     = ' || rec.filedateformat);
      utOutputReporter.pl ('   File extension  = ' || rec.fileextension);
      utOutputReporter.pl ('   ----- End File Output settings');
      utOutputReporter.pl ('=============================================================');
   END;

   PROCEDURE setprefix (prefix_in IN VARCHAR2, username_in IN VARCHAR2 := NULL)
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('prefix', prefix_in, v_user);
   END;

   FUNCTION prefix (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN NVL (rec.prefix, c_prefix);
   END;

   PROCEDURE setdelimiter (
      delimiter_in   IN   VARCHAR2
     ,username_in    IN   VARCHAR2 := NULL
   )
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('delimiter', delimiter_in, v_user);
   END;

   FUNCTION delimiter (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN NVL (rec.delimiter, c_delimiter);
   END;

   PROCEDURE setdir (dir_in IN VARCHAR2, username_in IN VARCHAR2 := NULL)
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('directory', dir_in, v_user);
   END;

   FUNCTION dir (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN rec.DIRECTORY;
   END;

   PROCEDURE autocompile (onoff_in IN BOOLEAN, username_in IN VARCHAR2 := NULL)
   IS

      v_autocompile   CHAR (1)       := utplsql.bool2vc (onoff_in);

      v_user          VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('autocompile', v_autocompile, v_user);
   END;

   FUNCTION autocompiling (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN NVL (utplsql.vc2bool (rec.autocompile), TRUE);

   END;

   PROCEDURE registertest (onoff_in IN BOOLEAN, username_in IN VARCHAR2
            := NULL)
   IS

      v_registertest   CHAR (1)       := utplsql.bool2vc (onoff_in);

      v_user           VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('registertest', v_registertest, v_user);
   END;

   FUNCTION registeringtest (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN NVL (utplsql.vc2bool (rec.registertest), FALSE);

   END;

   PROCEDURE showfailuresonly (
      onoff_in      IN   BOOLEAN
     ,username_in   IN   VARCHAR2 := NULL
   )
   IS

      v_showfailuresonly   CHAR (1)       := utplsql.bool2vc (onoff_in);

      v_user               VARCHAR2 (100)
                                         := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('show_failures_only', v_showfailuresonly, v_user);
   END;

   FUNCTION showingfailuresonly (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN NVL (utplsql.vc2bool (rec.show_failures_only), FALSE);

   END;

   PROCEDURE setfiledir (
      dir_in        IN   VARCHAR2 := NULL
     ,username_in   IN   VARCHAR2 := NULL
   )
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('filedir', dir_in, v_user);
   END;

   FUNCTION filedir (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN rec.filedir;
   END;

   PROCEDURE setreporter (
      reporter_in   IN   VARCHAR2
     ,username_in   IN   VARCHAR2 := NULL
   )
   IS

      v_user      VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('reporter', reporter_in, v_user);
    Utreport.USE(reporter_in);
   END;

   FUNCTION getreporter (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN rec.reporter;
   END;

   PROCEDURE setuserprefix (
      userprefix_in   IN   VARCHAR2 := NULL
     ,username_in     IN   VARCHAR2 := NULL
   )
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('fileuserprefix', userprefix_in, v_user);
   END;

   FUNCTION userprefix (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN rec.fileuserprefix;
   END;

   PROCEDURE setincludeprogname (
      incname_in    IN   BOOLEAN := FALSE
     ,username_in   IN   VARCHAR2 := NULL
   )
   IS

      v_incname   CHAR (1)       := utplsql.bool2vc (incname_in);

      v_user      VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('fileincprogname', v_incname, v_user);
   END;

   FUNCTION includeprogname (username_in IN VARCHAR2 := NULL)
      RETURN BOOLEAN
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN NVL (utplsql.vc2bool (rec.fileincprogname), FALSE);

   END;

   PROCEDURE setdateformat (
      dateformat_in   IN   VARCHAR2 := 'yyyyddmmhh24miss'
     ,username_in     IN   VARCHAR2 := NULL
   )
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('filedateformat', dateformat_in, v_user);
   END;

   FUNCTION DATEFORMAT (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN rec.filedateformat;
   END;

   PROCEDURE setfileextension (
      fileextension_in   IN   VARCHAR2 := '.UTF'
     ,username_in        IN   VARCHAR2 := NULL
   )
   IS

      v_user   VARCHAR2 (100) := NVL (UPPER (username_in), tester);
   BEGIN

      setconfig ('fileextension', fileextension_in, v_user);
   END;

   FUNCTION fileextension (username_in IN VARCHAR2 := NULL)
      RETURN VARCHAR2
   IS

      rec      ut_config%ROWTYPE;

      v_user   VARCHAR2 (100)      := NVL (UPPER (username_in), tester);
   BEGIN

      rec := config (v_user);

      RETURN rec.fileextension;
   END;

   PROCEDURE setfileinfo (
      dir_in             IN   VARCHAR2 := NULL
     ,userprefix_in      IN   VARCHAR2 := NULL
     ,incname_in         IN   BOOLEAN := FALSE
     ,dateformat_in      IN   VARCHAR2 := 'yyyyddmmhh24miss'
     ,fileextension_in   IN   VARCHAR2 := '.UTF'
     ,username_in        IN   VARCHAR2 := NULL
   )
   IS
   BEGIN
      setfiledir (dir_in, username_in);
      setuserprefix (userprefix_in, username_in);
      setincludeprogname (incname_in, username_in);
      setdateformat (dateformat_in, username_in);
      setfileextension (fileextension_in, username_in);
   END setfileinfo;

   FUNCTION fileinfo (username_in IN VARCHAR2 := NULL)
      RETURN rec_fileinfo
   IS

      rec            ut_config%ROWTYPE;

      v_user         VARCHAR2 (100)      := NVL (UPPER (username_in), tester);

      fileinfo_rec   rec_fileinfo;
   BEGIN

      rec := config (v_user);

      fileinfo_rec.filedir := rec.filedir;
      fileinfo_rec.fileuserprefix := rec.fileuserprefix;
      fileinfo_rec.fileincprogname := rec.fileincprogname;
      fileinfo_rec.filedateformat := rec.filedateformat;
      fileinfo_rec.fileextension := rec.fileextension;

      RETURN fileinfo_rec;
   END fileinfo;

   PROCEDURE upd (
      username_in             IN   ut_config.username%TYPE
     ,autocompile_in          IN   ut_config.autocompile%TYPE
     ,prefix_in               IN   ut_config.prefix%TYPE
     ,show_failures_only_in   IN   ut_config.show_failures_only%TYPE
     ,directory_in            IN   ut_config.DIRECTORY%TYPE
     ,filedir_in              IN   ut_config.filedir%TYPE
     ,show_config_info_in     IN   ut_config.show_config_info%TYPE
     ,editor_in               IN   ut_config.editor%TYPE
   )
   IS
   PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO ut_config
                  (username, autocompile, prefix
                  ,show_failures_only, DIRECTORY, filedir
                  ,show_config_info, editor
                  )
           VALUES (username_in, autocompile_in, prefix_in
                  ,show_failures_only_in, directory_in, filedir_in
                  ,show_config_info_in, editor_in
                  );
  COMMIT;
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN

         UPDATE ut_config
            SET autocompile = autocompile_in
               ,prefix = prefix_in
               ,show_failures_only = show_failures_only_in
               ,DIRECTORY = directory_in
               ,filedir = filedir_in
               ,show_config_info = show_config_info_in
          WHERE username = username_in;
      WHEN OTHERS
      THEN
         ROLLBACK;
         NULL;
   END;

   FUNCTION browser_contents (
      schema_in      IN   VARCHAR2
     ,name_like_in   IN   VARCHAR2 := '%'
     ,type_like_in   IN   VARCHAR2 := '%'
   )
      RETURN refcur_t
   IS
      retval   refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT ao.owner, ao.object_name, ao.object_type, ao.created
               ,ao.last_ddl_time, Utrutp.last_run_status (
         ao.owner, ao.object_name) status
           FROM all_objects ao
          WHERE ao.owner = UPPER (schema_in)
        AND ao.owner NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
            AND object_name LIKE UPPER (name_like_in)
            AND object_type LIKE UPPER (type_like_in)
      AND object_type IN ('PACKAGE', 'FUNCTION', 'PROCEDURE', 'OBJECT TYPE');
      RETURN retval;
   END browser_contents;

   FUNCTION source_for_program (schema_in IN VARCHAR2, NAME_IN IN VARCHAR2)
      RETURN refcur_t
   IS
      retval   refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT line, text
           FROM all_source
          WHERE owner = UPPER (schema_in) AND NAME = UPPER (NAME_IN);
      RETURN retval;
   END source_for_program;

   FUNCTION onerow (schema_in IN VARCHAR2)
      RETURN refcur_t
   IS
      retval   refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT autocompile, DIRECTORY, filedir, prefix, show_failures_only
               ,show_config_info, editor
           FROM ut_config
          WHERE username = UPPER (schema_in);
      return retval;
   END onerow;

   PROCEDURE get_onerow (
      schema_in                IN       VARCHAR2
     ,username_out             OUT      VARCHAR2
     ,autocompile_out          OUT      ut_config.autocompile%TYPE
     ,prefix_out               OUT      ut_config.prefix%TYPE
     ,show_failures_only_out   OUT       ut_config.show_failures_only%TYPE
     ,directory_out            OUT      ut_config.DIRECTORY%TYPE
     ,filedir_out              OUT      ut_config.filedir%TYPE
     ,show_config_info_out     OUT      ut_config.show_config_info%TYPE
     ,editor_out               OUT      ut_config.editor%TYPE
   )
   IS
   BEGIN
      SELECT username, autocompile, DIRECTORY, filedir
            ,prefix, show_failures_only, show_config_info
            ,editor
        INTO username_out, autocompile_out, directory_out, filedir_out
            ,prefix_out, show_failures_only_out, show_config_info_out
            ,editor_out
        FROM ut_config
       WHERE username = UPPER (schema_in);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         username_out := NULL;
   END get_onerow;
BEGIN

   settester;
END;
//

CREATE OR REPLACE PACKAGE BODY utoutcome

IS
   FUNCTION name (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_outcome.name%TYPE
   IS
      retval   ut_outcome.name%TYPE;
   BEGIN
      SELECT name
        INTO retval
        FROM ut_outcome
       WHERE id = outcome_id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION id (name_in IN ut_outcome.name%TYPE)
      RETURN ut_outcome.id%TYPE
   IS
      retval   ut_outcome.id%TYPE;
   BEGIN
      SELECT id
        INTO retval
        FROM ut_outcome
       WHERE name = name_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION onerow (name_in IN ut_outcome.name%TYPE)
      RETURN ut_outcome%ROWTYPE
   IS
      retval      ut_outcome%ROWTYPE;
      empty_rec   ut_outcome%ROWTYPE;
   BEGIN
      SELECT *
        INTO retval
        FROM ut_outcome
       WHERE name = name_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN empty_rec;
   END;

   FUNCTION onerow (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_outcome%ROWTYPE
   IS
      retval      ut_outcome%ROWTYPE;
      empty_rec   ut_outcome%ROWTYPE;
   BEGIN
      SELECT *
        INTO retval
        FROM ut_outcome
       WHERE id = outcome_id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN empty_rec;
   END;

   FUNCTION utp (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_utp.id%TYPE
   IS
      CURSOR utp_cur
      IS
         SELECT ut.utp_id
           FROM ut_outcome oc, ut_testcase tc, ut_unittest ut
          WHERE tc.id = oc.testcase_id
            AND tc.unittest_id = ut.id
            AND oc.id = outcome_id_in;

      utp_rec   utp_cur%ROWTYPE;
   BEGIN
      OPEN utp_cur;
      FETCH utp_cur INTO utp_rec;
      CLOSE utp_cur;
      RETURN utp_rec.utp_id;
   END;

   FUNCTION unittest (outcome_id_in IN ut_outcome.id%TYPE)
      RETURN ut_unittest.id%TYPE
   IS
      CURSOR unittest_cur
      IS
         SELECT tc.unittest_id
           FROM ut_outcome oc, ut_testcase tc
          WHERE tc.id = oc.testcase_id
            AND oc.id = outcome_id_in;

      unittest_rec   unittest_cur%ROWTYPE;
   BEGIN
      OPEN unittest_cur;
      FETCH unittest_cur INTO unittest_rec;
      CLOSE unittest_cur;
      RETURN unittest_rec.unittest_id;
   END;
END utoutcome;
//

CREATE OR REPLACE PACKAGE BODY utoutputreporter
IS

   norows BOOLEAN;

   PROCEDURE open
   IS
   BEGIN
     NULL;
   END;

   PROCEDURE close
   IS
   BEGIN
     NULL;
   END;

   PROCEDURE show (
      s                VARCHAR2,
      maxlinelenparm   NUMBER := 255,
      expand           BOOLEAN := TRUE
   )
   IS
      output_buffer_overflow   EXCEPTION;
      PRAGMA EXCEPTION_INIT (output_buffer_overflow, -20000);
      i                        NUMBER;
      maxlinelen               NUMBER
                                    := GREATEST (1, LEAST (255, maxlinelenparm));

      FUNCTION locatenewline (str VARCHAR2)
         RETURN NUMBER
      IS
         i10   NUMBER;
         i13   NUMBER;
      BEGIN
         i13 := NVL (INSTR (SUBSTR (str, 1, maxlinelen), CHR (13)), 0);
         i10 := NVL (INSTR (SUBSTR (str, 1, maxlinelen), CHR (10)), 0);

         IF i13 = 0
         THEN
            RETURN i10;
         ELSIF i10 = 0
         THEN
            RETURN i13;
         ELSE
            RETURN LEAST (i13, i10);
         END IF;
      END;
   BEGIN

      IF s IS NULL
      THEN
         DBMS_OUTPUT.put_line (s);

         RETURN;

      ELSIF LENGTH (s) <= maxlinelen
      THEN
         DBMS_OUTPUT.put_line (s);
         RETURN;
      END IF;

      i := locatenewline (s);

      IF i > 0
      THEN
         DBMS_OUTPUT.put_line (SUBSTR (s, 1, i - 1));
         show (SUBSTR (s, i + 1), maxlinelen, expand);
      ELSE

         i := NVL (INSTR (SUBSTR (s, 1, maxlinelen), ' ', -1), 0);

         IF i > 0
         THEN
            DBMS_OUTPUT.put_line (SUBSTR (s, 1, i - 1));
            show (SUBSTR (s, i + 1), maxlinelen, expand);
         ELSE

            i := maxlinelen;
            DBMS_OUTPUT.put_line (SUBSTR (s, 1, i));
            show (SUBSTR (s, i + 1), maxlinelen, expand);
         END IF;
      END IF;
   EXCEPTION
      WHEN output_buffer_overflow
      THEN
         IF NOT expand
         THEN
            RAISE;
         ELSE
            DBMS_OUTPUT.ENABLE (1000000);

            show (s, maxlinelen, FALSE);
         END IF;
   END;

   PROCEDURE showbanner (
      success_in   IN   BOOLEAN,
      program_in   IN   VARCHAR2,
      run_id_in    IN   utr_outcome.run_id%TYPE := NULL
   )
   IS
   BEGIN
      IF success_in
      THEN
         utreport.pl ('. ');
         utreport.pl (
            '>    SSSS   U     U   CCC     CCC   EEEEEEE   SSSS     SSSS   '
         );
         utreport.pl (
            '>   S    S  U     U  C   C   C   C  E        S    S   S    S  '
         );
         utreport.pl (
            '>  S        U     U C     C C     C E       S        S        '
         );
         utreport.pl (
            '>   S       U     U C       C       E        S        S       '
         );
         utreport.pl (
            '>    SSSS   U     U C       C       EEEE      SSSS     SSSS   '
         );
         utreport.pl (
            '>        S  U     U C       C       E             S        S  '
         );
         utreport.pl (
            '>         S U     U C     C C     C E              S        S '
         );
         utreport.pl (
            '>   S    S   U   U   C   C   C   C  E        S    S   S    S  '
         );
         utreport.pl (
            '>    SSSS     UUU     CCC     CCC   EEEEEEE   SSSS     SSSS   '
         );
      ELSE
         utreport.pl ('. ');
         utreport.pl (
            '>  FFFFFFF   AA     III  L      U     U RRRRR   EEEEEEE '
         );
         utreport.pl (
            '>  F        A  A     I   L      U     U R    R  E       '
         );
         utreport.pl (
            '>  F       A    A    I   L      U     U R     R E       '
         );
         utreport.pl (
            '>  F      A      A   I   L      U     U R     R E       '
         );
         utreport.pl (
            '>  FFFF   A      A   I   L      U     U RRRRRR  EEEE    '
         );
         utreport.pl (
            '>  F      AAAAAAAA   I   L      U     U R   R   E       '
         );
         utreport.pl (
            '>  F      A      A   I   L      U     U R    R  E       '
         );
         utreport.pl (
            '>  F      A      A   I   L       U   U  R     R E       '
         );
         utreport.pl (
            '>  F      A      A  III  LLLLLLL  UUU   R     R EEEEEEE '
         );
      END IF;

      utreport.pl ('. ');

      IF run_id_in IS NOT NULL
      THEN
         utreport.pl ('. Run ID: ' || run_id_in);
      ELSE
         IF success_in
         THEN
            utreport.pl (' SUCCESS: "' || NVL (program_in, 'Unnamed Test') || '"');
         ELSE
            utreport.pl (' FAILURE: "' || NVL (program_in, 'Unnamed Test') || '"');
         END IF;
      END IF;

      utreport.pl ('. ');
   END;

   PROCEDURE pl (str VARCHAR2)
   IS
   BEGIN
     show(str);
   END;

   PROCEDURE before_results(run_id IN utr_outcome.run_id%TYPE)
   IS
   BEGIN
      showbanner (utresult.success (run_id), utplsql.currpkg, run_id);
      utreport.pl ('> Individual Test Case Results:');
      utreport.pl ('>');
      norows := TRUE;
   END;

   PROCEDURE show_failure
   IS
   BEGIN
     utreport.pl (utreport.outcome.description);
     utreport.pl ('>');
     norows := FALSE;
   END;

   PROCEDURE show_result
   IS
   BEGIN
     utreport.pl (utreport.outcome.status || ' - ' || utreport.outcome.description);
     utreport.pl ('>');
     norows := FALSE;
   END;

   PROCEDURE after_results(run_id IN utr_outcome.run_id%TYPE)
   IS
   BEGIN
     IF norows AND utconfig.showingfailuresonly
     THEN
        utreport.pl ('> NO FAILURES FOUND');
     ELSIF norows
     THEN
        utreport.pl ('> NONE FOUND');
     END IF;
   END;

   PROCEDURE before_errors(run_id IN utr_error.run_id%TYPE)
   IS
   BEGIN
      utreport.pl ('>');
      utreport.pl ('> Errors recorded in utPLSQL Error Log:');
      utreport.pl ('>');
      norows := TRUE ;
   END;

   PROCEDURE show_error
   IS
   BEGIN
     norows := FALSE ;
     utreport.pl (utreport.error.errlevel || ' - ' || utreport.error.errcode || ': ' || utreport.error.errtext);
   END;

   PROCEDURE after_errors(run_id IN utr_error.run_id%TYPE)
   IS
   BEGIN
     IF norows
     THEN
        utreport.pl ('> NONE FOUND');
     END IF;
   END;

END;
//

CREATE OR REPLACE PACKAGE BODY utpackage
IS

   FUNCTION name_from_id (id_in IN ut_package.id%TYPE)
      RETURN ut_package.name%TYPE
   IS
      retval   ut_package.name%TYPE;
   BEGIN
      SELECT name
        INTO retval
        FROM ut_package
       WHERE id = id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   -- Need to add owner to param list and query
   FUNCTION id_from_name (name_in IN ut_package.name%TYPE,
       owner_in      IN ut_package.owner%TYPE := NULL)
      RETURN ut_package.id%TYPE
   IS
      retval   ut_package.id%TYPE;
      v_owner ut_package.owner%type := nvl(owner_in,USER);
   BEGIN
      SELECT id
        INTO retval
        FROM ut_package
       WHERE name = UPPER (name_in)
         AND owner=v_owner
   AND SUITE_ID is null;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   PROCEDURE ADD (
      suite_in            IN   INTEGER,
      package_in          IN   VARCHAR2,
      samepackage_in      IN   BOOLEAN := FALSE,
      prefix_in           IN   VARCHAR2 := NULL,
      dir_in              IN   VARCHAR2 := NULL,
      seq_in              IN   PLS_INTEGER := NULL,
      owner_in            IN   VARCHAR2 := NULL,
      add_tests_in        IN   BOOLEAN := FALSE,
      test_overloads_in   IN   BOOLEAN := FALSE
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      v_owner    VARCHAR2 (30)                 := NVL (owner_in, USER);
      v_id       ut_package.id%TYPE;
      v_same     ut_package.samepackage%TYPE   := utplsql.c_yes;
      v_prefix   ut_config.prefix%TYPE
                               := NVL (prefix_in, utconfig.prefix (owner_in));
   BEGIN
      IF NOT (NVL (samepackage_in, FALSE))
      THEN
         v_same := utplsql.c_no;
      END IF;

     v_id := utplsql.seqval ('ut_package');

      INSERT INTO ut_package
                  (id, suite_id, name,
                   owner, samepackage, prefix, dir, seq,
                   executions, failures)
           VALUES (v_id, suite_in, UPPER (package_in),
                   UPPER (v_owner), v_same, v_prefix, dir_in, NVL (seq_in, 1),
                   0, 0);
      IF id_from_name( UPPER (package_in),owner_in) IS NULL
      THEN
          v_id := utplsql.seqval ('ut_package');
         INSERT INTO ut_package
                  (id, suite_id, name,
                   owner, samepackage, prefix, dir, seq,
                   executions, failures)
           VALUES (v_id, NULL, UPPER (package_in),
                   UPPER (v_owner), v_same, v_prefix, dir_in, NVL (seq_in, 1),
                   0, 0);

          COMMIT;
      END IF;
      IF add_tests_in
      THEN

         DECLARE
            TYPE cv_t IS REF CURSOR;

            cv         cv_t;
            v_name     VARCHAR2 (100);
            v_query    VARCHAR2 (32767);
            v_suffix   VARCHAR2 (100)   := NULL;
         BEGIN
            IF test_overloads_in
            THEN
               v_suffix := ' || TO_CHAR(overload)';
            END IF;

            v_query :=
                     'SELECT DISTINCT object_name '
                  || v_suffix
                  || ' name '
                  || ' from all_arguments
                 where owner = :owner and package_name = :package';
            OPEN cv FOR v_query
               USING NVL (UPPER (owner_in), USER), UPPER (package_in);

            LOOP
               FETCH cv INTO v_name;
               EXIT WHEN cv%NOTFOUND;

               IF utplsql.tracing
               THEN
                  utreport.pl (   'Adding test '
                              || package_in
                              || '.'
                              || v_name);
               END IF;

               uttest.ADD (v_id, v_name,    'Test '
                                         || v_name);
            END LOOP;

            CLOSE cv;
         END;

      END IF;
 COMMIT;
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN

         UPDATE ut_package
            SET samepackage = v_same,
                prefix = v_prefix,
                dir = dir_in
          WHERE owner = UPPER (v_owner)
            AND name = UPPER (package_in)
            AND suite_id = suite_in;
      COMMIT;

      WHEN OTHERS
      THEN
         utreport.pl (   'Add package error: '
                     || SQLERRM);
         ROLLBACK;
         RAISE;
   END;

   PROCEDURE ADD (
      suite_in            IN   VARCHAR2,
      package_in          IN   VARCHAR2,
      samepackage_in      IN   BOOLEAN := FALSE,
      prefix_in           IN   VARCHAR2 := NULL,
      dir_in              IN   VARCHAR2 := NULL,
      seq_in              IN   PLS_INTEGER := NULL,
      owner_in            IN   VARCHAR2 := NULL,
      add_tests_in        IN   BOOLEAN := FALSE,
      test_overloads_in   IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      ADD (
         utsuite.id_from_name (suite_in),
         package_in,
         samepackage_in,
         prefix_in,
         dir_in,
         seq_in,
         owner_in,
         add_tests_in,
         test_overloads_in
      );
   END;

   PROCEDURE rem (
      suite_in     IN   INTEGER,
      package_in   IN   VARCHAR2,
      owner_in     IN   VARCHAR2 := NULL
   )
   IS
   PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      DELETE FROM ut_package
            WHERE (   suite_id = UPPER (suite_in)
                   OR (    suite_id IS NULL
                       AND suite_in IS NULL
                      )
                  )
              AND name = UPPER (package_in)
              AND owner = NVL (UPPER (owner_in), USER);
 COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Remove package error: '
                     || SQLERRM);
         ROLLBACK;
         RAISE;
   END;

   PROCEDURE rem (
      suite_in     IN   VARCHAR2,
      package_in   IN   VARCHAR2,
      owner_in     IN   VARCHAR2 := NULL
   )
   IS
   BEGIN
      rem (utsuite.id_from_name (suite_in), package_in, owner_in);
   END;

   PROCEDURE upd (
      suite_id_in        IN   INTEGER,
      package_in      IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
      owner_in        IN   VARCHAR2 := NULL
   )
   IS
      l_status    VARCHAR2 (100) := utplsql.c_success;
      PRAGMA AUTONOMOUS_TRANSACTION;
      v_failure   PLS_INTEGER    := 0;

      PROCEDURE do_upd
      IS
      BEGIN
         UPDATE ut_package
            SET last_status = l_status,
                last_start = start_in,
                last_end = end_in,
                executions =   NVL (executions, 0)
                             + 1,
                failures =   NVL (failures, 0)
                           + v_failure
                ,last_run_id = utplsql2.runnum
          WHERE  nvl(suite_id,0) = nvl(suite_id_in,0)
            AND name = UPPER (package_in)
            AND owner = NVL (UPPER (owner_in), USER);
      END;
   BEGIN
      IF NOT successful_in
      THEN
         v_failure := 1;
         l_status := utplsql.c_failure;
      END IF;

      do_upd;

      IF SQL%ROWCOUNT = 0
      THEN
         ADD (
            suite_id_in,
            package_in,
            samepackage_in=> FALSE,
            prefix_in=> utconfig.prefix (owner_in),
            dir_in=> NULL,
            seq_in=> NULL,
            owner_in=> owner_in,
            add_tests_in=> FALSE,
            test_overloads_in=> FALSE
         );
         do_upd;
      END IF;
  COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Update package error: '
                     || SQLERRM);
         ROLLBACK;
         RAISE;
   END;

   PROCEDURE upd (
      suite_in        IN   VARCHAR2,
      package_in      IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
      owner_in        IN   VARCHAR2 := NULL
   )
   IS
   BEGIN
      upd (
         utsuite.id_from_name (suite_in),
         package_in,
         start_in,
         end_in,
         successful_in,
         owner_in
      );
   END;
END utpackage;
//

CREATE OR REPLACE PACKAGE BODY UTPIPE
IS

   PROCEDURE receive_and_unpack (
      pipe_in           IN       VARCHAR2,
      msg_tbl_out       OUT      msg_tbltype,
      pipe_status_out   IN OUT   PLS_INTEGER
   )
   IS
      invalid_item_type   EXCEPTION;
      null_msg_tbl        msg_tbltype;
      next_item           INTEGER;
      item_count          INTEGER     := 0;
   BEGIN

      IF pipe_status_out != 0
      THEN
         RAISE invalid_item_type;
      END IF;

      LOOP

         EXIT WHEN next_item = 0;
         item_count :=   item_count
                       + 1;
         msg_tbl_out (item_count).item_type :=
                                        next_item;

         IF next_item = 9
         THEN
           null;
         ELSIF next_item = 6
         THEN
          null;
         ELSIF next_item = 11
         THEN
            null;
         ELSIF next_item = 12
         THEN
           null;
         ELSIF next_item = 23
         THEN
           null;
         ELSE
            RAISE invalid_item_type;
         END IF;

      END LOOP;
   EXCEPTION
      WHEN invalid_item_type
      THEN
         msg_tbl_out := null_msg_tbl;
   END receive_and_unpack;

END UTPIPE;
//

CREATE OR REPLACE PACKAGE BODY utplsql
IS

   g_trc       BOOLEAN        := FALSE;
   g_version   VARCHAR2 (100) := '2.2';

   tests       test_tt;
   testpkg     test_rt;

   FUNCTION vc2bool (vc IN VARCHAR2)
      RETURN BOOLEAN
   IS
   BEGIN
      IF vc = c_yes
      THEN
         RETURN TRUE;
      ELSIF vc = c_no
      THEN
         RETURN FALSE;
      ELSE
         RETURN NULL;
      END IF;
   END;

   FUNCTION bool2vc (bool IN BOOLEAN)
      RETURN VARCHAR2
   IS
   BEGIN
      IF bool
      THEN
         RETURN c_yes;
      ELSIF NOT bool
      THEN
         RETURN c_no;
      ELSE
         RETURN 'NULL';
      END IF;
   END;

   FUNCTION progexists (
      prog_in   IN   VARCHAR2,
      sch_in    IN   VARCHAR2
   )
      RETURN BOOLEAN
   IS
      v_prog          VARCHAR2 (1000) := prog_in;
      
      sch             VARCHAR2 (100);
      part1           VARCHAR2 (100);
      part2           VARCHAR2 (100);
      dblink          VARCHAR2 (100);
      part1_type      NUMBER;
      object_number   NUMBER;
   BEGIN
      IF sch_in IS NOT NULL
      THEN
         v_prog :=    sch_in
                   || '.'
                   || prog_in;
      END IF;

     
      DBMS_UTILITY.name_resolve (
         v_prog,
         1,
         sch,
         part1,
         part2,
         dblink,
         part1_type,
         object_number
      );
      RETURN TRUE;
   EXCEPTION
     
      WHEN OTHERS
      THEN
         IF sch_in IS NOT NULL
         THEN
            RETURN progexists (prog_in, NULL);
         ELSE
          
            DECLARE
               block   VARCHAR2(100) :=
               'DECLARE obj ' || v_prog || '; BEGIN NULL; END;';
               
            BEGIN
              
               EXECUTE IMMEDIATE block;
             

               RETURN TRUE;
            EXCEPTION
               WHEN OTHERS
               THEN
                 
                  RETURN FALSE;
            END;
           
         END IF;
   END;

   FUNCTION ispackage (
      prog_in   IN   VARCHAR2,
      sch_in    IN   VARCHAR2
   )
      RETURN BOOLEAN
   IS
      v_prog          VARCHAR2 (1000) := prog_in;
     
      sch             VARCHAR2 (100);
      part1           VARCHAR2 (100);
      part2           VARCHAR2 (100);
      dblink          VARCHAR2 (100);
      part1_type      NUMBER;
      object_number   NUMBER;
   BEGIN
      IF sch_in IS NOT NULL
      THEN
         v_prog :=    sch_in
                   || '.'
                   || prog_in;
      END IF;

     
      DBMS_UTILITY.name_resolve (
         v_prog,
         1,
         sch,
         part1,
         part2,
         dblink,
         part1_type,
         object_number
      );
      RETURN part1_type = 9;
   EXCEPTION
      WHEN OTHERS
      THEN
         IF sch_in IS NOT NULL
         THEN
            RETURN ispackage (prog_in, NULL);
         ELSE
            RETURN FALSE;
         END IF;
   END;

  

   PROCEDURE setcurrcase (indx_in IN PLS_INTEGER)
   IS
   BEGIN
      currcase.pkg := testpkg.pkg;
      currcase.prefix := testpkg.prefix;
      currcase.NAME := tests (indx_in).NAME;
      currcase.indx := indx_in;
   END;

   FUNCTION pkgname (
      package_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2,
      ispkg_in         IN   BOOLEAN,
      owner_in         IN   VARCHAR2 := NULL
   )
      RETURN VARCHAR2
   IS
      retval   VARCHAR2 (1000);
   BEGIN
     
      IF      NOT ispkg_in
          AND NOT samepackage_in 
      THEN
         retval :=    prefix_in
                   || package_in;
      ELSIF samepackage_in
      THEN
         retval := package_in;
      ELSE
         retval :=    prefix_in
                   || package_in;
      END IF;

      IF owner_in IS NOT NULL
      THEN
       
         retval := '"' || owner_in || '"'
        
                   || '.'
                   || retval;
      END IF;

      RETURN retval;
   END;

   
   FUNCTION progname (
      program_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2,
      ispkg_in         IN   BOOLEAN
   )
      RETURN VARCHAR2
   IS
     
      retval   VARCHAR2 (1000)
                    :=    prefix_in
                       || program_in;
   BEGIN
     
      IF NOT utconfig.registeringtest
      THEN
         IF UPPER (program_in) NOT IN
                           (c_setup, c_teardown)
         THEN
            retval := program_in;
         END IF;
      ELSIF NOT ispkg_in
      THEN
         retval :=    prefix_in
                   || program_in;
      ELSIF samepackage_in
      THEN
         retval :=    prefix_in
                   || program_in;
      ELSE
         
         retval := program_in;
      END IF;

      RETURN retval;
   END;

   FUNCTION pkgname (
      package_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2,
      owner_in         IN   VARCHAR2 := NULL
   )
      RETURN VARCHAR2
   IS
   BEGIN
      utassert.this (
         'Pkgname: the package record has not been set!',
         testpkg.pkg IS NOT NULL,
         register_in=> FALSE
      );
      RETURN pkgname (
                package_in,
                samepackage_in,
                prefix_in,
                testpkg.ispkg,
                owner_in
             );
   END;

   FUNCTION progname (
      program_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN,
      prefix_in        IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      utassert.this (
         'Progname: the package record has not been set!',
         testpkg.pkg IS NOT NULL,
         register_in=> FALSE
      );
      RETURN progname (
                program_in,
                samepackage_in,
                prefix_in,
                testpkg.ispkg
             );
   END;

   PROCEDURE runprog (
      name_in        IN   VARCHAR2,
      propagate_in   IN   BOOLEAN := FALSE
   )
   IS
      v_pkg    VARCHAR2 (100)
   := pkgname (
         testpkg.pkg,
         testpkg.samepkg,
         testpkg.prefix,
         testpkg.owner
      );
      v_name   VARCHAR2 (100)   := name_in;
     
      v_str    VARCHAR2 (32767);
     
   BEGIN
      IF tracing
      THEN
         utreport.pl (   'Runprog of '
             || name_in);
         utreport.pl (
               '   Package and program = '
            || v_pkg
            || '.'
            || v_name
         );
         utreport.pl (
               '   Same package? '
            || bool2vc (testpkg.samepkg)
         );
         utreport.pl (
               '   Is package? '
            || bool2vc (testpkg.ispkg)
         );
         utreport.pl (   '   Prefix = '
             || testpkg.prefix);
      END IF;

      v_str :=    'BEGIN '
               || v_pkg
               || '.'
               || v_name
               || ';  END;';
    
      EXECUTE IMMEDIATE v_str;
     
   EXCEPTION
      WHEN OTHERS
      THEN
        

         IF tracing
         THEN
            utreport.pl (
                  'Compile Error "'
               || SQLERRM
               || '" on: '
            );
            utreport.pl (v_str);
         END IF;

         utassert.this (
               'Unable to run '
            || v_pkg
            || '.'
            || v_name
            || ': '
            || SQLERRM,
            FALSE,
            null_ok_in=> NULL,
            raise_exc_in=> propagate_in,
            register_in=> TRUE
         );
   END;

   PROCEDURE runit (
      indx_in               IN   PLS_INTEGER,
      per_method_setup_in   IN   BOOLEAN,
      prefix_in             IN   VARCHAR2
   )
   IS
   BEGIN
      setcurrcase (indx_in);

      IF per_method_setup_in
      THEN
         runprog (   prefix_in
                  || c_setup, TRUE);
      END IF;

      runprog (tests (indx_in).NAME, FALSE);

      IF per_method_setup_in
      THEN
         runprog (
               prefix_in
            || c_teardown,
            TRUE
         );
      END IF;
   END;

   PROCEDURE init_tests
   IS
      nulltest   test_rt;
      nullcase   testcase_rt;
   BEGIN
      tests.DELETE;
      currcase := nullcase;
      testpkg := nulltest;
   END;

   PROCEDURE init (
      prefix_in       IN   VARCHAR2 := NULL,
      dir_in          IN   VARCHAR2 := NULL,
      from_suite_in   IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      init_tests;

     
      IF      prefix_in IS NOT NULL
          AND prefix_in != utconfig.prefix
      THEN
         utconfig.setprefix (prefix_in);
      END IF;

      IF      dir_in IS NOT NULL
          AND (   dir_in != utconfig.dir
               OR utconfig.dir IS NULL
              )
    
      THEN
         utconfig.setdir (dir_in);
      END IF;

      utresult.init (from_suite_in);

     
      utplsql2.set_runnum;

      IF tracing
      THEN
         utreport.pl ('Initialized utPLSQL session...');
      END IF;
   END;

   PROCEDURE COMPILE (
      file_in   IN   VARCHAR2,
      dir_in    IN   VARCHAR2
   )
   IS
      fid     UTL_FILE.file_type;
      v_dir   VARCHAR2 (2000)
                    := NVL (dir_in, utconfig.dir);
      lines   DBMS_SQL.varchar2s;
      cur     PLS_INTEGER       := DBMS_SQL.open_cursor;

      PROCEDURE recngo (str IN VARCHAR2)
      IS
      BEGIN
         UTL_FILE.fclose (fid);
         utreport.pl (
               'Error compiling '
            || file_in
            || ' located in "'
            || v_dir
            || '": '
            || str
         );
         utreport.pl (
               '   Please make sure the directory for utPLSQL is set by calling '
            || 'utConfig.setdir.'
         );
         utreport.pl (
            '   Your test package must reside in this directory.'
         );

         IF DBMS_SQL.is_open (cur)
         THEN
            DBMS_SQL.close_cursor (cur);
         END IF;
      END;
   BEGIN
      utrerror.assert (
         v_dir IS NOT NULL,
         'Compile error: you must specify a directory with utConfig.setdir!'
      );
      fid :=
         UTL_FILE.fopen (
            v_dir,
            file_in,
            'R'
               ,
            max_linesize=> 32767 
         );

      LOOP
         BEGIN
            UTL_FILE.get_line (
               fid,
               lines (  NVL (lines.LAST, 0)
                      + 1)
            );
            
            lines (lines.LAST) :=
               RTRIM (
                  lines (lines.LAST),
                     ' '
                  || CHR (13)
               );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               EXIT;
         END;
      END LOOP;

    
      LOOP
         IF    lines (lines.LAST) = '//'
            OR RTRIM (lines (lines.LAST)) IS NULL
         THEN
            lines.DELETE (lines.LAST);
         ELSE
            EXIT;
         END IF;
      END LOOP;

      UTL_FILE.fclose (fid);

    if tracing then
    utreport.pl ('Compiling ' || lines (lines.first));
    end if;

      DBMS_SQL.parse (
         cur,
         lines,
         lines.FIRST,
         lines.LAST,
         TRUE,
         DBMS_SQL.native
      );
      DBMS_SQL.close_cursor (cur);
   EXCEPTION
      WHEN UTL_FILE.invalid_path
      THEN
         recngo ('invalid_path');
      WHEN UTL_FILE.invalid_mode
      THEN
         recngo ('invalid_mode');
      WHEN UTL_FILE.invalid_filehandle
      THEN
         recngo ('invalid_filehandle');
      WHEN UTL_FILE.invalid_operation
      THEN
         recngo ('invalid_operation');
      WHEN UTL_FILE.read_error
      THEN
         recngo ('read_error');
      WHEN UTL_FILE.write_error
      THEN
         recngo ('write_error');
      WHEN UTL_FILE.internal_error
      THEN
         recngo ('internal_error');
      WHEN OTHERS
      THEN
         recngo (SQLERRM);
   END;



   PROCEDURE setpkg (
      package_in            IN   VARCHAR2,
      samepackage_in        IN   BOOLEAN := FALSE,
      prefix_in             IN   VARCHAR2 := NULL,
      owner_in              IN   VARCHAR2 := NULL,
      subprogram_in         IN   VARCHAR2 := '%',
      override_package_in   IN   VARCHAR2
            := NULL -- 2.0.9.2
   )
   IS
      v_pkg   VARCHAR2 (1000);
   BEGIN

      testpkg.pkg :=
            NVL (override_package_in, package_in);
      testpkg.owner := owner_in; 
      testpkg.ispkg :=
                 ispackage (package_in, owner_in);



      IF override_package_in IS NOT NULL
      THEN
         testpkg.samepkg := TRUE;
      ELSE
         testpkg.samepkg := samepackage_in;
      END IF;

      testpkg.prefix :=
         NVL (
            prefix_in,
            utconfig.prefix (owner_in)
         );
      v_pkg := pkgname (
                  testpkg.pkg,
                  testpkg.samepkg,
                  testpkg.prefix,
                  testpkg.owner
               );

        DECLARE
           rec   ut_utp%ROWTYPE;
        BEGIN
           IF ututp.EXISTSS (testpkg.owner, testpkg.pkg)
           THEN
              rec := ututp.onerow (testpkg.owner, testpkg.pkg);
           ELSE
              ututp.ADD (testpkg.pkg, testpkg.owner, id_out => rec.ID);
           END IF;

           utplsql2.set_current_utp (rec.ID);
           utrutp.initiate(utplsql2.runnum,rec.id);
        END;

     IF tracing
      THEN
         utreport.pl (   'Setpkg to '
             || testpkg.pkg);
         utreport.pl (
               '   Package and program = '
            || v_pkg
         );
         utreport.pl (
               '   Same package? '
            || bool2vc (testpkg.samepkg)
         );
         utreport.pl (
               '   Is package? '
            || bool2vc (testpkg.ispkg)
         );
         utreport.pl (   '   Prefix = '
             || testpkg.prefix);
      END IF;
   END;


   PROCEDURE populate_test_array (
      testpkg_in       IN   test_rt,
      package_in       IN   VARCHAR2,
      samepackage_in   IN   BOOLEAN := FALSE,
      prefix_in        IN   VARCHAR2 := NULL,
      owner_in         IN   VARCHAR2 := NULL,
      subprogram_in    IN   VARCHAR2 := '%'
   )
   IS
      v_pkg   VARCHAR2 (1000)
   := 
      pkgname (
         testpkg_in.pkg,
         testpkg_in.samepkg,
         testpkg_in.prefix
      );
   BEGIN
     
      IF NOT utconfig.registeringtest (
                NVL (UPPER (owner_in), USER)
             )
      THEN
         
         FOR rec IN
            
             (SELECT procedure_name
                FROM all_procedures
               WHERE owner = NVL (UPPER (owner_in), USER)
                 AND object_name = UPPER (v_pkg)
                 AND procedure_name LIKE    UPPER (prefix_in)
                                    || '%'
                 AND procedure_name LIKE
                                UPPER (   prefix_in
                                       || subprogram_in)
                 AND procedure_name NOT IN (UPPER (
                                                  prefix_in
                                               || c_setup
                                            ),
                                            UPPER (
                                                  prefix_in
                                               || c_teardown
                                            )
                                           ))
           
         LOOP
            addtest (
               testpkg_in.pkg,
               rec.procedure_name,
               prefix_in,
               iterations_in=> 1,
               override_in=> TRUE
            );
         END LOOP;
      END IF;
   END;

   FUNCTION currpkg
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN testpkg.pkg;
   END;

   PROCEDURE addtest (
      package_in      IN   VARCHAR2,
      name_in         IN   VARCHAR2,
      prefix_in       IN   VARCHAR2,
      iterations_in   IN   PLS_INTEGER,
      override_in     IN   BOOLEAN
   )
   IS
      indx   PLS_INTEGER
                      :=   NVL (tests.LAST, 0)
                         + 1;
   BEGIN
      IF    utconfig.registeringtest
         OR (    NOT utconfig.registeringtest
             AND override_in
            )
      THEN
         IF tracing
         THEN
            utreport.pl ('Addtest');
            utreport.pl (
                  '   Package and program = '
               || package_in
               || '.'
               || name_in
            );
            utreport.pl (
                  '   Same package? '
               || bool2vc (testpkg.samepkg)
            );
            utreport.pl (
                  '   Override? '
               || bool2vc (override_in)
            );
            utreport.pl (   '   Prefix = '
                || prefix_in);
         END IF;

         tests (indx).pkg := package_in;
         tests (indx).prefix := prefix_in;
         tests (indx).NAME := name_in;
         tests (indx).iterations := iterations_in;
      END IF;
   END;

   PROCEDURE addtest (
      name_in         IN   VARCHAR2,
      prefix_in       IN   VARCHAR2 := NULL,
      iterations_in   IN   PLS_INTEGER := 1,
      override_in     IN   BOOLEAN := FALSE
   )
   IS
      indx   PLS_INTEGER
                      :=   NVL (tests.LAST, 0)
                         + 1;
   BEGIN
      addtest (
         testpkg.pkg,
         name_in,
         prefix_in,
         iterations_in,
         override_in
      );
   END;

  
   PROCEDURE test (
      package_in            IN   VARCHAR2,
      samepackage_in        IN   BOOLEAN := FALSE,
      prefix_in             IN   VARCHAR2 := NULL,
      recompile_in          IN   BOOLEAN := TRUE,
      dir_in                IN   VARCHAR2 := NULL,
      suite_in              IN   VARCHAR2 := NULL,
      owner_in              IN   VARCHAR2 := NULL,
      reset_results_in      IN   BOOLEAN := TRUE,
      from_suite_in         IN   BOOLEAN := FALSE,
      subprogram_in         IN   VARCHAR2 := '%',
      per_method_setup_in   IN   BOOLEAN := FALSE, -- 2.0.8
      override_package_in   IN   VARCHAR2
            := NULL -- 2.0.9.2
   )
   IS
      indx                 PLS_INTEGER;
      v_pkg                VARCHAR2 (100);
     v_dir maxvc2_t := NVL (dir_in, utconfig.dir);
      v_start              DATE              := SYSDATE;
      v_prefix             ut_config.prefix%TYPE
   := NVL (prefix_in, utconfig.prefix (owner_in));
      v_per_method_setup   BOOLEAN
              := NVL (per_method_setup_in, FALSE);

      PROCEDURE cleanup (
         per_method_setup_in   IN   BOOLEAN
      )
      IS
      BEGIN
         utresult.show;

         IF NOT per_method_setup_in
         THEN
            runprog (
                  v_prefix
               || c_teardown,
               TRUE
            );
         END IF;

         utpackage.upd (
            suite_in,
            package_in,
            v_start,
            SYSDATE,
            utresult.success,
            owner_in
         );

         -- 2.1.1 add recording of test in tables.
         utrutp.terminate(utplsql2.runnum, utplsql2.current_utp);

         IF reset_results_in
         THEN
            init_tests;
         END IF;

         IF suite_in IS NULL THEN
            utreport.close;
         END IF;

      END;
   BEGIN
      init (v_prefix, v_dir, from_suite_in);

      IF NOT progexists (package_in, owner_in)
      THEN
         utreport.pl (
               'Program named "'
            || package_in
            || '" does not exist.'
         );
      ELSE
         setpkg (
            package_in,
            samepackage_in,
            v_prefix,
            owner_in,
            subprogram_in,
            override_package_in
         );
         --v_pkg := pkgname (package_in, samepackage_in, v_prefix, owner_in);
         -- 2.0.9.2 Make sure record is used.
         v_pkg := pkgname (
                     testpkg.pkg,
                     testpkg.samepkg,
                     testpkg.prefix,
                     testpkg.owner
                  );

         IF      recompile_in
             AND utconfig.autocompiling (
                    owner_in
                 )
         THEN
            IF tracing
            THEN
               utreport.pl (
                     'Recompiling '
                  || v_pkg
                  || ' in '
                  || v_dir
               );
            END IF;

            utreceq.COMPILE (package_in);

            COMPILE (
                  -- 2.0.9.1 Package name without OWNER
                  -- 2.0.9.2 Switch to use of record based info.
                  --pkgname (package_in, samepackage_in, v_prefix)
                     --      || '.pks', dir_in);
                  pkgname (
                     testpkg.pkg,
                     testpkg.samepkg,
                     testpkg.prefix
                  )
               || '.pks',
               v_dir
            );
            COMPILE (
                  -- 2.0.9.1 Package name without OWNER
                  -- 2.0.9.2 Switch to use of record based info.
                  --pkgname (package_in, samepackage_in, v_prefix)
                     --      || '.pks', dir_in);
                  pkgname (
                     testpkg.pkg,
                     testpkg.samepkg,
                     testpkg.prefix
                  )
               || '.pkb',
               v_dir
            );
         END IF;

         IF NOT v_per_method_setup
         THEN
            runprog (
                  v_prefix
               || c_setup,
               TRUE
            );
         END IF;

         populate_test_array (
            testpkg,
            package_in,
            samepackage_in,
            v_prefix,
            owner_in,
            subprogram_in
         );
         indx := tests.FIRST;

         IF indx IS NULL
         THEN
            utreport.pl ('Warning!');
            utreport.pl (
               'Warning...no tests were identified for execution!'
            );
            utreport.pl ('Warning!');
         ELSE
            LOOP
               EXIT WHEN indx IS NULL;
               runit (
                  indx,
                  v_per_method_setup,
                  v_prefix
               );
               indx := tests.NEXT (indx);
            END LOOP;
         END IF;

         cleanup (v_per_method_setup);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         utassert.this (
               'utPLSQL.test failure: '
            || SQLERRM,
            FALSE
         );
         cleanup (FALSE);
   END;

   PROCEDURE testsuite (
      suite_in              IN   VARCHAR2,
      recompile_in          IN   BOOLEAN := TRUE,
      reset_results_in      IN   BOOLEAN := TRUE,
      per_method_setup_in   IN   BOOLEAN := FALSE, -- 2.0.8
      override_package_in   IN   BOOLEAN := FALSE
   )
   IS
      v_suite         ut_suite.id%TYPE
               := utsuite.id_from_name (suite_in);
      v_success       BOOLEAN            := TRUE;
      v_suite_start   DATE               := SYSDATE;
      v_pkg_start     DATE;
      v_override      VARCHAR2 (1000);
   BEGIN
      IF v_suite IS NULL
      THEN
         utassert.this (
               'Test suite with name "'
            || suite_in
            || '" does not exist.',
            FALSE
         );
         utresult.show;
      ELSE
         FOR rec IN  (SELECT   *
                          FROM ut_package
                         WHERE suite_id = v_suite
                      ORDER BY seq)
         LOOP
            v_pkg_start := SYSDATE;


            IF override_package_in
            THEN
               v_override := rec.NAME;
            ELSE
               v_override := NULL;
            END IF;

begin
            test (
               rec.NAME,
               vc2bool (rec.samepackage),
               rec.prefix,
               recompile_in,
               rec.dir,


               suite_in,
               rec.owner,
               reset_results_in=> FALSE,
               from_suite_in=> TRUE,
               per_method_setup_in=> per_method_setup_in,
               override_package_in=> v_override
            );

             EXCEPTION
                 WHEN OTHERS THEN
                   v_success := FALSE;
             END;
            IF utresult.failure
            THEN
               v_success := FALSE;
            END IF;
         END LOOP;

         utsuite.upd (
            v_suite,
            v_suite_start,
            SYSDATE,
            v_success
         );
      END IF;

      IF reset_results_in
      THEN
         init;
      END IF;

   END;



   PROCEDURE setcase (case_in IN VARCHAR2)
   IS
   BEGIN
      NULL;
   END;

   PROCEDURE setdata (
      dir_in     IN   VARCHAR2,
      file_in    IN   VARCHAR2,
      delim_in   IN   VARCHAR2 := ','
   )
   IS
   BEGIN
      NULL;
   END;

   PROCEDURE passdata (
      data_in    IN   VARCHAR2,
      delim_in   IN   VARCHAR2 := ','
   )
   IS
   BEGIN
      NULL;
   END;

   PROCEDURE trc
   IS
   BEGIN
      g_trc := TRUE;
   END;

   PROCEDURE notrc
   IS
   BEGIN
      g_trc := FALSE;
   END;

   FUNCTION tracing
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN g_trc;
   END;

   FUNCTION version
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN g_version;
   END;

   FUNCTION seqval (tab_in IN VARCHAR2)
      RETURN PLS_INTEGER
   IS
      sqlstr   VARCHAR2 (200)
   :=    'SELECT '
      || tab_in
      || '_seq.NEXTVAL FROM dual';
      fdbk     PLS_INTEGER;
      retval   PLS_INTEGER;
   BEGIN

      EXECUTE IMMEDIATE sqlstr
         INTO retval;

     
      RETURN retval;
   END;

   FUNCTION ifelse (
      bool_in   IN   BOOLEAN,
      tval_in   IN   BOOLEAN,
      fval_in   IN   BOOLEAN
   )
      RETURN BOOLEAN
   IS
   BEGIN
      IF bool_in
      THEN
         RETURN tval_in;
      ELSE
         RETURN fval_in;
      END IF;
   END;

   FUNCTION ifelse (
      bool_in   IN   BOOLEAN,
      tval_in   IN   DATE,
      fval_in   IN   DATE
   )
      RETURN DATE
   IS
   BEGIN
      IF bool_in
      THEN
         RETURN tval_in;
      ELSE
         RETURN fval_in;
      END IF;
   END;

   FUNCTION ifelse (
      bool_in   IN   BOOLEAN,
      tval_in   IN   NUMBER,
      fval_in   IN   NUMBER
   )
      RETURN NUMBER
   IS
   BEGIN
      IF bool_in
      THEN
         RETURN tval_in;
      ELSE
         RETURN fval_in;
      END IF;
   END;

   FUNCTION ifelse (
      bool_in   IN   BOOLEAN,
      tval_in   IN   VARCHAR2,
      fval_in   IN   VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      IF bool_in
      THEN
         RETURN tval_in;
      ELSE
         RETURN fval_in;
      END IF;
   END;


   PROCEDURE run (
      testpackage_in        IN   VARCHAR2,
      prefix_in             IN   VARCHAR2 := NULL,
      suite_in              IN   VARCHAR2 := NULL,
      owner_in              IN   VARCHAR2 := NULL,
      reset_results_in      IN   BOOLEAN := TRUE,
      from_suite_in         IN   BOOLEAN := FALSE,
      subprogram_in         IN   VARCHAR2 := '%',
      per_method_setup_in   IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      test (
         package_in=> testpackage_in,
         samepackage_in=> FALSE,
         prefix_in=> prefix_in,
         recompile_in=> FALSE,
         dir_in=> NULL,
         suite_in=> suite_in,
         owner_in=> owner_in,
         reset_results_in=> reset_results_in,
         from_suite_in=> from_suite_in,
         subprogram_in=> subprogram_in,
         per_method_setup_in=> per_method_setup_in, -- 2.0.8
         override_package_in=> testpackage_in
      );
   END;

   PROCEDURE runsuite (
      suite_in              IN   VARCHAR2,
      reset_results_in      IN   BOOLEAN := TRUE,
      per_method_setup_in   IN   BOOLEAN := FALSE
   )
   IS
   BEGIN
      testsuite (
         suite_in=> suite_in,
         recompile_in=> FALSE,
         reset_results_in=> reset_results_in,
         per_method_setup_in=> per_method_setup_in, -- 2.0.8
         override_package_in=> TRUE
      );
   END;
END;
//

CREATE OR REPLACE PACKAGE BODY utplsql2
IS

   g_trc       BOOLEAN         := FALSE;
   g_current   current_test_rt;

   FUNCTION runnum
      RETURN utr_outcome.run_id%TYPE
   IS
   BEGIN
      RETURN g_current.run_id;
   END;

   PROCEDURE set_runnum
   IS
   BEGIN
      SELECT utplsql_runnum_seq.NEXTVAL
        INTO g_current.run_id
        FROM DUAL;
      g_current.tc_run_id := 1;
   END;

   FUNCTION tc_runnum
      RETURN PLS_INTEGER
   IS
   BEGIN
      RETURN g_current.tc_run_id;
   END;

   PROCEDURE move_ahead_tc_runnum
   IS
   BEGIN
      g_current.tc_run_id := g_current.tc_run_id + 1;
   END;

   FUNCTION current_suite
      RETURN ut_suite.id%TYPE
   IS
   BEGIN
      RETURN g_current.suite_id;
   END;

   PROCEDURE set_current_suite (suite_in IN ut_suite.id%TYPE)
   IS
   BEGIN
      g_current.suite_id := suite_in;
   END;

   FUNCTION current_utp
      RETURN ut_utp.id%TYPE
   IS
   BEGIN
      RETURN g_current.utp_id;
   END;

   PROCEDURE set_current_utp (utp_in IN ut_utp.id%TYPE)
   IS
   BEGIN
      g_current.utp_id := utp_in;
   END;

   FUNCTION current_unittest
      RETURN ut_unittest.id%TYPE
   IS
   BEGIN
      RETURN g_current.unittest_id;
   END;

   PROCEDURE set_current_unittest (
      unittest_in   IN   ut_unittest.id%TYPE
   )
   IS
   BEGIN
      g_current.unittest_id := unittest_in;
   END;

   FUNCTION current_testcase
      RETURN ut_testcase.id%TYPE
   IS
   BEGIN
      RETURN g_current.testcase_id;
   END;

   PROCEDURE set_current_testcase (
      testcase_in   IN   ut_testcase.id%TYPE
   )
   IS
   BEGIN
      g_current.testcase_id := testcase_in;
   END;

   FUNCTION current_outcome
      RETURN ut_outcome.id%TYPE
   IS
   BEGIN
      RETURN g_current.outcome_id;
   END;

   PROCEDURE set_current_outcome (
      outcome_in   IN   ut_outcome.id%TYPE
   )
   IS
   BEGIN
      g_current.outcome_id := outcome_in;
   END;

   PROCEDURE runprog (
      procedure_in     IN   VARCHAR2,
      utp_id_in        IN   ut_utp.id%TYPE := NULL,
      unittest_id_in   IN   ut_unittest.id%TYPE := NULL,
      propagate_in     IN   BOOLEAN := FALSE,
      exceptions_in    IN   VARCHAR2 := NULL
   )
   IS
      v_name   VARCHAR2 (100)   := procedure_in;
      v_str    VARCHAR2 (32767);

   BEGIN
      IF tracing
      THEN
         utreport.pl (   'Runprog of '
                     || procedure_in);
      END IF;

      v_str :=    'BEGIN '
               || procedure_in
               || ';'
               || utplsql.ifelse (
                     true,
                     NULL,
                        RTRIM (
                              'EXCEPTION '
                           || exceptions_in,
                           ';'
                        )
                     || ';'
                  )
               || ' END;';

      EXECUTE IMMEDIATE v_str;

   EXCEPTION
      WHEN OTHERS
      THEN

         IF tracing
         THEN
            utreport.pl (
                  'Procedure execution Error "'
               || SQLERRM
               || '" on: '
            );
            utreport.pl (v_str);
         END IF;

         IF unittest_id_in IS NOT NULL
         THEN
            utrerror.ut_report (
               utplsql2.runnum,
               unittest_id_in,
               utrerror.cannot_run_program,
                  'Procedure named "'
               || procedure_in
               || '" could not be executed.',
               SQLERRM
            );
         ELSE
            utrerror.utp_report (
               utplsql2.runnum,
               utp_id_in,
               utrerror.cannot_run_program,
                  'Procedure named "'
               || procedure_in
               || '" could not be executed.',
               SQLERRM
            );
         END IF;

   END;

   PROCEDURE run_utp_setup (utp_in IN ut_utp%ROWTYPE,
      package_level_in in boolean := TRUE)
   IS
      l_program   VARCHAR2 (100)
                          := ututp.setup_procedure (utp_in);

   BEGIN
      IF l_program IS NOT NULL and
      (package_level_in OR utp_in.per_method_setup = utplsql.c_yes)
      THEN
         runprog (
            l_program,
            utp_in.id,
            exceptions_in=> utp_in.EXCEPTIONS
         );
      END IF;
   END;

   PROCEDURE run_utp_teardown (utp_in IN ut_utp%ROWTYPE,
      package_level_in in boolean := TRUE)
   IS
      l_program   VARCHAR2 (100)
                       := ututp.teardown_procedure (utp_in);

   BEGIN
      IF l_program IS NOT NULL and
      (package_level_in OR utp_in.per_method_setup = utplsql.c_yes)
      THEN
         runprog (
            l_program,
            utp_in.id,
            exceptions_in=> utp_in.EXCEPTIONS
         );
      END IF;
   END;

   PROCEDURE run_unittest (
      utp_in   IN   ut_utp%ROWTYPE,
      ut_in    IN   ut_unittest%ROWTYPE
   )
   IS
   BEGIN
      utrunittest.initiate (utplsql2.runnum, ut_in.id);

      run_utp_setup (utp_in, package_level_in => FALSE);

      runprog (
         utunittest.full_name (utp_in, ut_in),
         utp_in.id,
         ut_in.id,
         exceptions_in=> utp_in.EXCEPTIONS
      );

      run_utp_teardown (utp_in, package_level_in => FALSE);

      utrunittest.terminate (utplsql2.runnum, ut_in.id);
   END;

   PROCEDURE test (
      utp_rec           IN   ut_utp%ROWTYPE,
      show_results_in   IN   BOOLEAN := TRUE,
      program_in        IN   VARCHAR2 := NULL,
      naming_mode_in    IN   VARCHAR2 := V2_naming_mode
   )
   IS
      CURSOR unit_tests_cur (id_in IN ut_utp.id%TYPE)
      IS
         SELECT   *
             FROM ut_unittest
            WHERE utp_id = id_in
              AND status = utplsql.c_enabled
         ORDER BY seq;

      PROCEDURE cleanup (utp_in IN ut_utp%ROWTYPE)
      IS
      BEGIN
         IF utp_in.id IS NOT NULL
         THEN
            run_utp_teardown (utp_in);
            utrutp.terminate (utplsql2.runnum, utp_in.id);
         END IF;

         IF show_results_in
         THEN
            utresult.show (utplsql2.runnum);
         END IF;

         COMMIT;
      END;
   BEGIN
      IF utp_rec.id IS NULL
      THEN
         utrerror.utp_report (
            utplsql2.runnum,
            NULL,
            utrerror.no_utp_for_program,
               'Program named "'
            || program_in
            || '" does not have a UTP defined for it.'
         );
      ELSE
         utrutp.initiate (utplsql2.runnum, utp_rec.id);
         run_utp_setup (utp_rec);

         FOR ut_rec IN unit_tests_cur (utp_rec.id)
         LOOP
            IF tracing
            THEN
               utreport.pl (
                     'Unit testing: '
                  || ut_rec.program_name
               );
            END IF;

            run_unittest (utp_rec, ut_rec);

         END LOOP;
      END IF;

      cleanup (utp_rec);
   EXCEPTION
      WHEN OTHERS
      THEN
         utrerror.utp_report (
            utplsql2.runnum,
            utp_rec.id,
            SQLCODE,
            SQLERRM,
            raiseexc=> FALSE
         );
         cleanup (utp_rec);
   END;

   PROCEDURE test (
      program_in        IN   VARCHAR2,
      owner_in          IN   VARCHAR2 := NULL,
      show_results_in   IN   BOOLEAN := TRUE,
      naming_mode_in    IN   VARCHAR2 := V2_naming_mode
   )
   IS
      utp_rec   ut_utp%ROWTYPE;
   BEGIN
      set_runnum;
      utp_rec := ututp.onerow (owner_in, program_in);

   END;

   PROCEDURE testsuite (
      suite_in          IN   VARCHAR2,
      show_results_in   IN   BOOLEAN := TRUE,
      naming_mode_in    IN   VARCHAR2 := V2_naming_mode
   )
   IS
      v_suite         ut_suite.id%TYPE;
      v_success       BOOLEAN            := TRUE;
      v_suite_start   DATE               := SYSDATE;
      v_pkg_start     DATE;
      suite_rec       ut_suite%ROWTYPE;

      CURSOR utps_cur (suite_in IN ut_suite.id%TYPE)
      IS
         SELECT ut_utp.*
           FROM ut_utp, ut_suite_utp
          WHERE suite_id = suite_in
            AND ut_utp.id = ut_suite_utp.utp_id;
   BEGIN
      set_runnum;
      suite_rec := utsuite.onerow (suite_in);

      IF suite_rec.id IS NULL
      THEN
         utrerror.suite_report (
            utplsql2.runnum,
            NULL,
            utrerror.undefined_suite,
               'Suite named "'
            || suite_in
            || '" is not defined.'
         );
      ELSE
         utrsuite.initiate (utplsql2.runnum, suite_rec.id);

         utrsuite.terminate (utplsql2.runnum, suite_rec.id);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         utrerror.suite_report (
            utplsql2.runnum,
            suite_rec.id,
            SQLCODE,
            SQLERRM,
            raiseexc=> FALSE
         );
   END;

   PROCEDURE trc
   IS
   BEGIN
      g_trc := TRUE;
   END;

   PROCEDURE notrc
   IS
   BEGIN
      g_trc := FALSE;
   END;

   FUNCTION tracing
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN g_trc;
   END;
END utplsql2;
//

CREATE OR REPLACE PACKAGE BODY utplsql_util

AS
   ver_no         VARCHAR2 (10) := '1.0.0';

   TYPE param_rec IS RECORD (
      col_name   VARCHAR2 (50),
      col_type   PLS_INTEGER,
      col_len    PLS_INTEGER,
      col_mode   PLS_INTEGER
   );

   TYPE params_tab IS TABLE OF param_rec
      INDEX BY BINARY_INTEGER;

   par_in         PLS_INTEGER   := 1;
   par_inout      PLS_INTEGER   := 2;
   par_out        PLS_INTEGER   := 3;
   param_prefix   VARCHAR2 (10) := 'ut_';

   FUNCTION get_version
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN (ver_no);
   END;

   FUNCTION get_par_alias (par_type VARCHAR)
      RETURN VARCHAR2
   IS
   BEGIN
      IF (par_type = 'VARCHAR')
      THEN
         RETURN ('vchar');
      ELSIF (par_type = 'NUMBER')
      THEN
         RETURN ('num');
      ELSIF (par_type = 'DATE')
      THEN
         RETURN ('date');
      ELSIF (par_type = 'REF CURSOR')
      THEN
         RETURN ('refc');
      ELSIF (par_type = 'CHAR')
      THEN
         RETURN ('chr');
      ELSE
         RETURN ('oth');
      END IF;
   END;

   PROCEDURE add_params (
      params         IN OUT   utplsql_params,
      par_pos                 PLS_INTEGER,
      par_type                VARCHAR2,
      par_sql_type            VARCHAR2,
      par_inout               PLS_INTEGER,
      par_val                 VARCHAR2
   )
   IS
      idx   PLS_INTEGER;
   BEGIN
      idx := params.COUNT + 1;
      params (idx).par_name :=    param_prefix
                               || get_par_alias (par_type)
                               || '_'
                               || TO_CHAR (par_pos);
      params (idx).par_pos := par_pos;
      params (idx).par_type := par_type;
      params (idx).par_sql_type := par_sql_type;
      params (idx).par_inout := par_inout;
      params (idx).par_val := par_val;
   END;

   PROCEDURE reg_in_param (
      par_pos            PLS_INTEGER,
      par_val            VARCHAR2,
      params    IN OUT   utplsql_params
   )
   IS
   BEGIN
      add_params (params, par_pos, 'VARCHAR2', 'VARCHAR2', par_in, par_val);
   END;

   PROCEDURE reg_in_param (
      par_pos            PLS_INTEGER,
      par_val            NUMBER,
      params    IN OUT   utplsql_params
   )
   IS
   BEGIN
      add_params (
         params,
         par_pos,
         'NUMBER',
         'NUMBER',
         par_in,
         TO_CHAR (par_val)
      );
   END;

   PROCEDURE reg_in_array (
      par_pos      IN       PLS_INTEGER,
      array_name   IN       VARCHAR2,
      array_vals   IN       varchar_array,
      params       IN OUT   utplsql_params
   )
   IS
      idx   PLS_INTEGER;
   BEGIN
      add_params (params, par_pos, 'ARRAY', array_name, par_in, NULL);
      idx := array_holder.COUNT + 1;

      FOR i IN 1 .. array_vals.COUNT
      LOOP
         array_holder (idx).array_pos := par_pos;
         array_holder (idx).array_val := array_vals (i);
         idx := idx + 1;
      END LOOP;
   END;

   PROCEDURE reg_in_param (
      par_pos            PLS_INTEGER,
      par_val            DATE,
      params    IN OUT   utplsql_params
   )
   IS
   BEGIN
      add_params (
         params,
         par_pos,
         'DATE',
         'DATE',
         par_in,
         TO_CHAR (par_val, 'DD-MON-YYYY:HH24:MI:SS')
      );
   END;

   PROCEDURE reg_inout_param (
      par_pos            PLS_INTEGER,
      par_val            VARCHAR2,
      params    IN OUT   utplsql_params
   )
   IS
   BEGIN
      add_params (params, par_pos, 'VARCHAR2', 'VARCHAR2', par_inout, par_val);
   END;

   PROCEDURE reg_inout_param (
      par_pos            PLS_INTEGER,
      par_val            NUMBER,
      params    IN OUT   utplsql_params
   )
   IS
   BEGIN
      add_params (
         params,
         par_pos,
         'NUMBER',
         'NUMBER',
         par_inout,
         TO_CHAR (par_val)
      );
   END;

   PROCEDURE reg_inout_param (
      par_pos            PLS_INTEGER,
      par_val            DATE,
      params    IN OUT   utplsql_params
   )
   IS
   BEGIN
      add_params (
         params,
         par_pos,
         'DATE',
         'DATE',
         par_inout,
         TO_CHAR (par_val, 'DD-MON-YYYY:HH24:MI:SS')
      );
   END;

   PROCEDURE reg_out_param (
      par_pos             PLS_INTEGER,
      par_type            VARCHAR2,
      params     IN OUT   utplsql_params
   )
   IS
      int_par_type   VARCHAR2 (10);
   BEGIN
      IF (par_type = 'NUMBER')
      THEN
         int_par_type := 'NUMBER';
      ELSIF (par_type = 'VARCHAR')
      THEN
         int_par_type := 'VARCHAR2';
      ELSIF (par_type = 'CHAR')
      THEN
         int_par_type := 'VARCHAR2';
      ELSIF (par_type = 'REFCURSOR')
      THEN
         int_par_type := 'REFC';
      ELSE
         int_par_type := par_type;
      END IF;

      add_params (params, par_pos, int_par_type, int_par_type, par_out, NULL);
   END;

   FUNCTION strtoken (s_str IN OUT VARCHAR2, token VARCHAR2)
      RETURN VARCHAR2
   IS
      pos      PLS_INTEGER;
      tmpstr   VARCHAR2 (4000);
   BEGIN
      pos := NVL (INSTR (s_str, token), 0);

      IF (pos = 0)
      THEN
         pos := LENGTH (s_str);
      END IF;

      tmpstr := SUBSTR (s_str, 1, pos);
      s_str := SUBSTR (s_str, pos);
      RETURN (tmpstr);
   END;

   PROCEDURE get_table_for_str (
      p_arr         OUT   v30_table,
      p_string            VARCHAR2,
      delim               VARCHAR2 := ',',
      enclose_str         VARCHAR2 DEFAULT NULL
   )
   IS
      pos       INTEGER         := 1;
      v_idx     INTEGER         := 1;
      tmp_str   VARCHAR2 (4000);
   BEGIN
      IF p_string IS NULL
      THEN
         RETURN;
      END IF;

      tmp_str := p_string;

      LOOP
         EXIT WHEN (pos = 0);
         pos := INSTR (tmp_str, delim);

         IF (pos = 0)
         THEN
            p_arr (v_idx) := enclose_str || tmp_str || enclose_str;
         ELSE
            p_arr (v_idx) :=    enclose_str
                             || SUBSTR (tmp_str, 1, pos - 1)
                             || enclose_str;
            v_idx := v_idx + 1;
            tmp_str := SUBSTR (tmp_str, pos + 1);
         END IF;
      END LOOP;
   END;

   FUNCTION describe_proc (vproc_name VARCHAR2, params OUT params_tab)
      RETURN VARCHAR2
   IS
      outstr           VARCHAR2 (3000)              := NULL;
      seperator        VARCHAR2 (10)                := NULL;
      v_overload       DBMS_DESCRIBE.number_table;
      v_position       DBMS_DESCRIBE.number_table;
      v_level          DBMS_DESCRIBE.number_table;
      v_argumentname   DBMS_DESCRIBE.varchar2_table;
      v_datatype       DBMS_DESCRIBE.number_table;
      v_defaultvalue   DBMS_DESCRIBE.number_table;
      v_inout          DBMS_DESCRIBE.number_table;
      v_length         DBMS_DESCRIBE.number_table;
      v_precision      DBMS_DESCRIBE.number_table;
      v_scale          DBMS_DESCRIBE.number_table;
      v_radix          DBMS_DESCRIBE.number_table;
      v_spare          DBMS_DESCRIBE.number_table;
      v_argcounter     PLS_INTEGER                  := 1;
      curr_level       PLS_INTEGER                  := 1;
      prev_level       PLS_INTEGER                  := 1;
      tab_open         BOOLEAN                      := FALSE ;
      rec_open         BOOLEAN                      := FALSE ;
      pos              PLS_INTEGER;

      PROCEDURE add_param (str VARCHAR2)
      IS
         vtable   v30_table;
      BEGIN
         get_table_for_str (vtable, str, ':');

         IF (utplsql.tracing)
         THEN
            DBMS_OUTPUT.put_line (
                  'Parsed='
               || vtable (1)
               || ','
               || vtable (2)
               || ','
               || vtable (3)
               || ','
               || vtable (4)
            );
         END IF;

         params (pos).col_name := vtable (1);
         params (pos).col_type := vtable (2);
         params (pos).col_len := vtable (3);
         params (pos).col_mode := vtable (4);
         pos := pos + 1;
      END;
   BEGIN
      DBMS_DESCRIBE.describe_procedure (
         vproc_name,
         NULL,
         NULL,
         v_overload,
         v_position,
         v_level,
         v_argumentname,
         v_datatype,
         v_defaultvalue,
         v_inout,
         v_length,
         v_precision,
         v_scale,
         v_radix,
         v_spare
      );
      v_argcounter := 1;

      IF (v_position (1) = 0)
      THEN
         outstr := 'FUNCTION';
      ELSE
         outstr := 'PROCEDURE';
      END IF;

      pos := 1;

      LOOP
         IF (utplsql.tracing)
         THEN
            DBMS_OUTPUT.put_line (
                  'Desc()='
               || v_argumentname (v_argcounter)
               || ','
               || TO_CHAR (v_datatype (v_argcounter))
            );
         END IF;

         curr_level := v_level (v_argcounter);

         IF (utplsql.tracing)
         THEN
            DBMS_OUTPUT.put_line (
                  'Currlevel='
               || TO_CHAR (curr_level)
               || ',Prevlevel='
               || TO_CHAR (prev_level)
            );
         END IF;

         IF (curr_level <= prev_level)
         THEN
            IF (rec_open)
            THEN
               add_param ('RECORDEND:0:0:0');
               rec_open := FALSE ;
            ELSIF (tab_open)
            THEN
               add_param ('TABLEEND:0:0:0');
               tab_open := FALSE ;
            END IF;
         END IF;

         IF (v_datatype (v_argcounter) = 250)
         THEN                                                      
            rec_open := TRUE ;
            add_param (
                  'RECORDBEGIN:'
               || TO_CHAR (v_datatype (v_argcounter))
               || ':'
               || TO_CHAR (v_length (v_argcounter))
               || ':'
               || TO_CHAR (v_inout (v_argcounter))
            );
            seperator := ',';
         ELSIF (v_datatype (v_argcounter) = 251)
         THEN                                               
            tab_open := TRUE ;
            add_param (
                  'TABLEOPEN:'
               || TO_CHAR (v_datatype (v_argcounter))
               || ':'
               || TO_CHAR (v_length (v_argcounter))
               || ':'
               || TO_CHAR (v_inout (v_argcounter))
            );
         ELSIF (v_datatype (v_argcounter) = 102)
         THEN                                                  
            add_param (
                  'CURSOR:'
               || TO_CHAR (v_datatype (v_argcounter))
               || ':'
               || TO_CHAR (v_length (v_argcounter))
               || ':'
               || TO_CHAR (v_inout (v_argcounter))
            );
         ELSE
            add_param (
                  NVL (v_argumentname (v_argcounter), 'UNKNOWN')
               || ':'
               || TO_CHAR (v_datatype (v_argcounter))
               || ':'
               || TO_CHAR (v_length (v_argcounter))
               || ':'
               || TO_CHAR (v_inout (v_argcounter))
            );
         END IF;

         v_argcounter := v_argcounter + 1;
         seperator := ',';
      END LOOP;

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done DBMS_DESCRIBE');
      END IF;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         IF (rec_open)
         THEN
            add_param ('RECORDEND:0:0:0');
            rec_open := FALSE ;
         ELSIF (tab_open)
         THEN
            add_param ('TABLEEND:0:0:0');
            tab_open := FALSE ;
         END IF;

         RETURN (outstr);
      WHEN OTHERS
      THEN
         RAISE;
   END;

   PROCEDURE print_metadata (metadata sqldata_tab)
   IS
   BEGIN
      FOR i IN 1 .. NVL (metadata.COUNT, 0)
      LOOP
         DBMS_OUTPUT.put_line (
               'Name='
            || metadata (i).col_name
            || ',Type='
            || TO_CHAR (metadata (i).col_type)
            || ',Len='
            || TO_CHAR (metadata (i).col_len)
         );
      END LOOP;
   END;

   PROCEDURE get_metadata_for_cursor (
      proc_name         VARCHAR2,
      metadata    OUT   sqldata_tab
   )
   IS
      params      params_tab;
      proc_type   VARCHAR2 (10);
      idx         PLS_INTEGER   := 1;
      pos         PLS_INTEGER   := 1;
   BEGIN
      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Start Describe Proc');
      END IF;

      proc_type := describe_proc (proc_name, params);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('End Describe Proc');
      END IF;


      LOOP
         EXIT WHEN (params (pos).col_name = 'CURSOR');
         pos := pos + 1;
      END LOOP;

      pos := pos + 2;

      LOOP
         EXIT WHEN (params (pos).col_name = 'RECORDEND');
         metadata (idx).col_name := params (pos).col_name;
         metadata (idx).col_type := params (pos).col_type;
         metadata (idx).col_len := params (pos).col_len;
         idx := idx + 1;
         pos := pos + 1;
      END LOOP;
   END;

   PROCEDURE get_metadata_for_query (
      query_txt         VARCHAR2,
      metadata    OUT   sqldata_tab
   )
   IS
      cols    DBMS_SQL.desc_tab;
      ncols   PLS_INTEGER;
      cur     INTEGER           := DBMS_SQL.open_cursor;
   BEGIN
      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Query=' || query_txt);
      END IF;

      DBMS_SQL.parse (cur, query_txt, DBMS_SQL.native);


      FOR i IN 1 .. cols.COUNT
      LOOP
         metadata (i).col_name := cols (i).col_name;
         metadata (i).col_type := cols (i).col_type;
         metadata (i).col_len := cols (i).col_max_len;
      END LOOP;

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done get_metadata_for_query');
      END IF;
   END;

   PROCEDURE get_metadata_for_table (
      table_name         VARCHAR2,
      metadata     OUT   sqldata_tab
   )
   IS
   BEGIN
      get_metadata_for_query ('select * from ' || table_name, metadata);
   END;

   PROCEDURE get_metadata_for_proc (
      proc_name         VARCHAR2,
      POSITION          INTEGER,
      data_type   OUT   VARCHAR2,
      metadata    OUT   sqldata_tab
   )
   IS
      outstr           VARCHAR2 (3000)              := NULL;
      seperator        VARCHAR2 (10)                := NULL;
      v_overload       DBMS_DESCRIBE.number_table;
      v_position       DBMS_DESCRIBE.number_table;
      v_level          DBMS_DESCRIBE.number_table;
      v_argumentname   DBMS_DESCRIBE.varchar2_table;
      v_datatype       DBMS_DESCRIBE.number_table;
      v_defaultvalue   DBMS_DESCRIBE.number_table;
      v_inout          DBMS_DESCRIBE.number_table;
      v_length         DBMS_DESCRIBE.number_table;
      v_precision      DBMS_DESCRIBE.number_table;
      v_scale          DBMS_DESCRIBE.number_table;
      v_radix          DBMS_DESCRIBE.number_table;
      v_spare          DBMS_DESCRIBE.number_table;
      v_argcounter     PLS_INTEGER                  := 1;
      curr_level       PLS_INTEGER                  := 1;
      prev_level       PLS_INTEGER                  := 1;
      tab_open         BOOLEAN                      := FALSE ;
      rec_open         BOOLEAN                      := FALSE ;
      pos              PLS_INTEGER;
      idx              PLS_INTEGER                  := 1;
      recs_copied      PLS_INTEGER;

      FUNCTION get_datatype (l_type INTEGER)
         RETURN VARCHAR2
      IS
         r_type   VARCHAR2 (20);
      BEGIN

         SELECT DECODE (
                   l_type,
                   1, 'VARCHAR2',
                   2, 'NUMBER',
                   12, 'DATE',
                   96, 'CHAR',
                   102, 'REF CURSOR',
                   250, 'RECORD',
                   251, 'PLSQL TABLE',
                   'UNKNOWN'
                )
           INTO r_type
           FROM DUAL;

         RETURN (r_type);
      END;

      PROCEDURE copy_metadata (l_idx INTEGER)
      IS
         m_idx   INTEGER;
      BEGIN
         m_idx := metadata.COUNT + 1;
         metadata (m_idx).col_name := v_argumentname (l_idx);
         metadata (m_idx).col_type := v_datatype (l_idx);
         metadata (m_idx).col_len := v_length (l_idx);
      END;

      FUNCTION copy_level (l_idx INTEGER)
         RETURN INTEGER
      IS
         l_counter   PLS_INTEGER := 1;
         l_level     PLS_INTEGER;
      BEGIN
         l_level := v_level (l_idx);
         l_counter := l_idx;

         LOOP
            EXIT WHEN (   (l_counter > v_argumentname.COUNT)
                       OR l_level <> v_level (l_counter)
                      );
            copy_metadata (l_counter);
            l_counter := l_counter + 1;
         END LOOP;

         RETURN (l_counter - l_idx);
      END;
   BEGIN
      DBMS_DESCRIBE.describe_procedure (
         proc_name,
         NULL,
         NULL,
         v_overload,
         v_position,
         v_level,
         v_argumentname,
         v_datatype,
         v_defaultvalue,
         v_inout,
         v_length,
         v_precision,
         v_scale,
         v_radix,
         v_spare
      );

      IF (v_position.COUNT = 0)
      THEN
         data_type := 'NOTFOUND';
         RETURN;
      END IF;

      idx := 1;

      LOOP
         EXIT WHEN (   ((v_level (idx) = 0) AND (POSITION = v_position (idx)))
                    OR (idx = v_position.COUNT)
                   );
         idx := idx + 1;
      END LOOP;

      IF (idx = v_position.COUNT)
      THEN
         IF (POSITION = v_position (idx))
         THEN
            data_type := get_datatype (v_datatype (idx));

            IF (data_type <> 'REF CURSOR')
            THEN 
            copy_metadata (idx);
            END IF;
         ELSE
            data_type := 'NOTFOUND';
         END IF;

         RETURN;
      END IF;

      data_type := get_datatype (v_datatype (idx));

      

      IF (data_type = 'REF CURSOR')
      THEN
         IF (v_level (idx + 1) > v_level (idx))
         THEN 
         idx := idx + 2;
         recs_copied := copy_level (idx);
         idx := idx + recs_copied;
         END IF;
      ELSIF (data_type = 'PLSQL TABLE')
      THEN
         IF (v_level (idx + 1) > v_level (idx))
         THEN
            IF (v_datatype (idx + 1) = 250)
            THEN 
            idx := idx + 2; 
            ELSE
               idx := idx + 1; 
            END IF;

            recs_copied := copy_level (idx);
            idx := idx + recs_copied;
         ELSE
            copy_metadata (idx);
         END IF;
      ELSIF (data_type = 'RECORD')
      THEN
         IF (v_level (idx + 1) > v_level (idx))
         THEN 
         idx := idx + 1;
         recs_copied := copy_level (idx);
         idx := idx + recs_copied;
         ELSE 
         copy_metadata (idx);
         END IF;
      ELSE
         data_type := get_datatype (v_datatype (idx));
         copy_metadata (idx);
      END IF;
   END;

   PROCEDURE test_get_metadata_for_cursor (proc_name VARCHAR2)
   IS
      metadata   sqldata_tab;
   BEGIN
      get_metadata_for_cursor (proc_name, metadata);

      IF (utplsql.tracing)
      THEN
         print_metadata (metadata);
      END IF;
   END;

   FUNCTION get_colnamesstr (metadata sqldata_tab)
      RETURN VARCHAR2
   IS
      cnt   PLS_INTEGER;
      str   VARCHAR2 (32000);
   BEGIN
      cnt := metadata.COUNT;

      FOR i IN 1 .. cnt
      LOOP
         str := str || metadata (i).col_name || ',';
      END LOOP;

      RETURN (SUBSTR (str, 1, LENGTH (str) - 1));
   END;

   FUNCTION get_coltypesstr (metadata sqldata_tab)
      RETURN VARCHAR2
   IS
      cnt   PLS_INTEGER;
      str   VARCHAR2 (2000);
   BEGIN
      cnt := metadata.COUNT;

      FOR i IN 1 .. cnt
      LOOP
         str := str || TO_CHAR (metadata (i).col_type) || ',';
      END LOOP;

      RETURN (SUBSTR (str, 1, LENGTH (str) - 1));
   END;

   FUNCTION get_coltype_syntax (col_type PLS_INTEGER, col_len PLS_INTEGER)
      RETURN VARCHAR2
   IS
   BEGIN
      IF (col_type = 22)
      THEN
         RETURN ('VARCHAR2(' || TO_CHAR (col_len) || ')');
      ELSIF (col_type = 15)
      THEN
         RETURN ('NUMBER');
      ELSIF (col_type = 19)
      THEN
         RETURN ('DATE');
      ELSIF (col_type = 23)
      THEN
         RETURN ('CHAR(' || TO_CHAR (col_len) || ')');
      ELSE
         DBMS_OUTPUT.PUT_LINE('col_type=' || col_type || ' col_len=' || col_len);
         RETURN ('VARCHAR2(' || TO_CHAR (1000) || ')');
      END IF;    
   END;

   FUNCTION get_coltype_syntax (col_type VARCHAR2, col_len PLS_INTEGER)
      RETURN VARCHAR2
   IS
   BEGIN
      IF (col_type = 'VARCHAR2')
      THEN
         RETURN ('VARCHAR2(' || TO_CHAR (col_len) || ')');
      ELSIF (col_type = 'NUMBER')
      THEN
         RETURN ('NUMBER');
      ELSIF (col_type = 'DATE')
      THEN
         RETURN ('DATE');
      ELSIF (col_type = 'CHAR')
      THEN
         RETURN ('CHAR(' || TO_CHAR (col_len) || ')');
      ELSIF (col_type = 'REFC')
      THEN
         RETURN ('REFC');
      ELSE
         RETURN (col_type);
      END IF;
   END;

   PROCEDURE PRINT (str VARCHAR2)
   IS
      len   PLS_INTEGER;
   BEGIN
      len := LENGTH (str);

      FOR i IN 1 .. len
      LOOP
         DBMS_OUTPUT.put_line (SUBSTR (str, (i - 1) * 255, 255));
      END LOOP;

      IF ((len * 255) > LENGTH (str))
      THEN
         DBMS_OUTPUT.put_line (SUBSTR (str, len * 255));
      END IF;
   END;

   FUNCTION get_proc_name (p_proc_nm VARCHAR2)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN (SUBSTR (p_proc_nm, 1, INSTR (p_proc_nm, '(') - 1));
   END;

   FUNCTION get_val_for_table (
      table_name         VARCHAR2,
      col_name           VARCHAR2,
      col_val      OUT   VARCHAR2,
      col_type     OUT   NUMBER
   )
      RETURN NUMBER
   IS
      
   BEGIN
      null;
   END;

   FUNCTION get_table_name
      RETURN VARCHAR2
   IS
      tmp_seq     NUMBER;
      curr_user   VARCHAR2 (20);
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'CURRENT_USER')
        INTO curr_user
        FROM DUAL;

      SELECT ut_refcursor_results_seq.NEXTVAL
        INTO tmp_seq
        FROM DUAL;


      RETURN (curr_user || '.' || 'UTPLSQL_TEMP_' || tmp_seq);
   END;

   PROCEDURE execute_ddl (stmt VARCHAR2)
   IS

   BEGIN

      EXECUTE IMMEDIATE stmt;
      
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END;

   FUNCTION get_create_ddl (
      metadata     utplsql_util.sqldata_tab,
      table_name   VARCHAR2,
      owner_name   VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      ddl_stmt   VARCHAR2 (5000);
      cnt        PLS_INTEGER;
   BEGIN
    
      IF (NVL (metadata.COUNT, 0) = 0)
      THEN
         RETURN (NULL);
      END IF;

      ddl_stmt := 'create table ' || table_name || ' ( ';
      FOR i IN 1 .. metadata.COUNT
      LOOP
         ddl_stmt :=    ddl_stmt
                     || metadata (i).col_name
                     || ' '
                     || utplsql_util.get_coltype_syntax (
                           metadata (i).col_type,
                           metadata (i).col_len
                        )
                     || ',';
      END LOOP;
      ddl_stmt := SUBSTR (ddl_stmt, 1, LENGTH (ddl_stmt) - 1) || ')';

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Current user = ' || USER);
         utplsql_util.PRINT (ddl_stmt);
      END IF;

      RETURN (ddl_stmt);
   END;

   PROCEDURE prepare_cursor_1 (
      stmt             IN OUT   VARCHAR2,
      table_name                VARCHAR2,
      call_proc_name            VARCHAR2,
      metadata                  utplsql_util.sqldata_tab
   )
   IS
      col_str   VARCHAR2 (200);
   BEGIN
      IF (NVL (metadata.COUNT, 0) = 0)
      THEN
         RETURN;
      END IF;

      stmt :=
         'declare
             p_result_id   PLS_INTEGER;
             p_rec_nm      PLS_INTEGER := 0;
             TYPE refc is ref cursor;
             rc refc;';

      FOR i IN 1 .. metadata.COUNT
      LOOP
         stmt :=    stmt
                 || metadata (i).col_name
                 || ' '
                 || utplsql_util.get_coltype_syntax (
                       metadata (i).col_type,
                       metadata (i).col_len
                    )
                 || ';'
                 || CHR (10);
      END LOOP;

      col_str := utplsql_util.get_colnamesstr (metadata);
      stmt :=
            stmt
         || 'BEGIN
          rc := '
         || call_proc_name
         || ';
          LOOP
             FETCH rc INTO '
         || col_str
         || ';
             EXIT WHEN rc%NOTFOUND;
             p_rec_nm := p_rec_nm + 1;
             INSERT INTO '
         || table_name
         || ' ('
         || col_str
         || ')'
         || ' values ('
         || col_str
         || ');'
         || '
             END LOOP;
             CLOSE rc;
         END;';

      IF (utplsql.tracing)
      THEN
         utplsql_util.PRINT (stmt);
      END IF;
   END;

   FUNCTION get_par_for_decl (params utplsql_params)
      RETURN VARCHAR2
   IS
      decl   VARCHAR2 (1000);
   BEGIN
      FOR i IN 1 .. params.COUNT
      LOOP
         decl :=    decl
                 || params (i).par_name
                 || ' '
                 || utplsql_util.get_coltype_syntax (
                       params (i).par_sql_type,
                       1000
                    )
                 || ';'
                 || CHR (10);
      END LOOP;

      RETURN (decl);
   END;

   FUNCTION get_param_valstr (params utplsql_params, POSITION PLS_INTEGER)
      RETURN VARCHAR2
   IS
   BEGIN
      IF (params (POSITION).par_type = 'DATE')
      THEN
         RETURN (   'TO_DATE('''
                 || params (POSITION).par_val
                 || ''',''DD-MON-YYYY:HH24:MI:SS'')'
                );
      ELSE
         RETURN ('''' || params (POSITION).par_val || '''');
      END IF;
   END;

   FUNCTION get_param_valstr_from_array (
      params      utplsql_params,
      POSITION    PLS_INTEGER,
      array_pos   PLS_INTEGER
   )
      RETURN VARCHAR2
   IS
   BEGIN
      IF (params (POSITION).par_type = 'DATE')
      THEN
         RETURN (   'TO_DATE('''
                 || array_holder (array_pos).array_val
                 || ''',''DD-MON-YYYY:HH24:MI:SS'')'
                );
      ELSE
         RETURN ('''' || array_holder (array_pos).array_val || '''');
      END IF;
   END;

   PROCEDURE print_utplsql_params (params utplsql_params)
   IS
   BEGIN
      FOR i IN 1 .. params.COUNT
      LOOP
         DBMS_OUTPUT.put_line (
               'Name='
            || params (i).par_name
            || ',type='
            || params (i).par_type
            || ',mode='
            || TO_CHAR (params (i).par_inout)
            || ',pos='
            || TO_CHAR (params (i).par_pos)
            || ',val='
            || params (i).par_val
         );
      END LOOP;
   END;

   PROCEDURE init_array_holder
   IS
   BEGIN
      array_holder.DELETE;
   END;

   PROCEDURE print_array_holder
   IS
   BEGIN
      DBMS_OUTPUT.put_line ('Printing array holder');

      FOR i IN 1 .. array_holder.COUNT
      LOOP
         DBMS_OUTPUT.put_line ('pos=' || TO_CHAR (array_holder (i).array_pos));
         PRINT ('Val=' || array_holder (i).array_val);
      END LOOP;
   END;

   FUNCTION get_index_for_array (pos INTEGER)
      RETURN INTEGER
   IS
      idx   PLS_INTEGER := 1;
   BEGIN
      LOOP
         EXIT WHEN (array_holder (idx).array_pos = pos);
         idx := idx + 1;
      END LOOP;

      RETURN (idx);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN (-1);
   END;

   FUNCTION get_par_for_assign (params utplsql_params)
      RETURN VARCHAR2
   IS
      assign      VARCHAR2 (1000);
      array_idx   PLS_INTEGER;
      idx         PLS_INTEGER     := 1;
   BEGIN
      IF (utplsql.tracing)
      THEN
         print_array_holder;
      END IF;

      FOR i IN 1 .. params.COUNT
      LOOP
         IF (params (i).par_inout != par_out)
         THEN
            IF (utplsql.tracing)
            THEN
               DBMS_OUTPUT.put_line (
                  'In get_par_for_assign<' || params (i).par_type || '>'
               );
            END IF;

            IF (params (i).par_type != 'ARRAY')
            THEN
               assign :=    assign
                         || params (i).par_name
                         || ' := '
                         || get_param_valstr (params, i)
                         || ';'
                         || CHR (10);
            ELSE
               array_idx := get_index_for_array (params (i).par_pos);

               IF (utplsql.tracing)
               THEN
                  DBMS_OUTPUT.put_line ('start index =' || TO_CHAR (
                                                              array_idx
                                                           ));
               END IF;

               LOOP
                  BEGIN
                     EXIT WHEN array_holder (array_idx).array_pos !=
                                                           params (i).par_pos;
                     assign :=    assign
                               || params (i).par_name
                               || '('
                               || TO_CHAR (idx)
                               || ') := '
                               || get_param_valstr_from_array (
                                     params,
                                     i,
                                     array_idx
                                  )
                               || ';'
                               || CHR (10);
                     array_idx := array_idx + 1;
                     idx := idx + 1;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        EXIT;
                  END;
               END LOOP;
            END IF;
         END IF;
      END LOOP;

      RETURN (assign);
   END;

   FUNCTION get_par_for_call (params utplsql_params, proc_name VARCHAR2)
      RETURN VARCHAR2
   IS
      call_str    VARCHAR2 (1000);
      start_idx   PLS_INTEGER;
   BEGIN
      IF (params (1).par_pos = 0)
      THEN
         call_str := params (1).par_name || ' := ' || proc_name || '(';
         start_idx := 2;
      ELSE
         call_str := proc_name || '(';
         start_idx := 1;
      END IF;

      FOR i IN start_idx .. params.COUNT
      LOOP
         IF (i != start_idx)
         THEN
            call_str := call_str || ',' || params (i).par_name;
         ELSE
            call_str := call_str || params (i).par_name;
         END IF;
      END LOOP;

      call_str := call_str || ')';
      RETURN (call_str);
   END;

   PROCEDURE get_single_param (
      params                  utplsql_params,
      POSITION                PLS_INTEGER,
      single_param   IN OUT   params_rec
   )
   IS
      idx   PLS_INTEGER;
   BEGIN
      IF (POSITION != 0)
      THEN
         IF (params (1).par_pos = 0)
         THEN
            idx := POSITION + 1;
         ELSE
            idx := POSITION;
         END IF;
      ELSE
         idx := POSITION + 1;
      END IF;

      single_param.par_name := params (idx).par_name;
      single_param.par_type := params (idx).par_type;
      single_param.par_val := params (idx).par_val;
      single_param.par_inout := params (idx).par_inout;
   END;

   PROCEDURE prepare_cursor_100 (
      stmt             IN OUT   VARCHAR2,
      table_name                VARCHAR2,
      call_proc_name            VARCHAR2,
      POSITION                  PLS_INTEGER,
      metadata                  utplsql_util.sqldata_tab,
      params                    utplsql_params
   )
   IS
      col_str        VARCHAR2 (32000);
      single_param   params_rec;
   BEGIN
      IF (NVL (metadata.COUNT, 0) = 0)
      THEN
         RETURN;
      END IF;

      stmt :=
         'declare
             p_result_id   PLS_INTEGER;
             p_rec_nm      PLS_INTEGER := 0;
             TYPE refc is ref cursor;
             rc refc;';

      FOR i IN 1 .. metadata.COUNT
      LOOP
         stmt :=    stmt
                 || metadata (i).col_name
                 || ' '
                 || utplsql_util.get_coltype_syntax (
                       metadata (i).col_type,
                       metadata (i).col_len
                    )
                 || ';'
                 || CHR (10);
      END LOOP;

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done ref cursor column declarations');
      END IF;

      stmt := stmt || get_par_for_decl (params);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done params declarations');
      END IF;

      get_single_param (params, POSITION, single_param);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done get_single_param');
      END IF;

      col_str := utplsql_util.get_colnamesstr (metadata);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done get_colnamesstr');
      END IF;

      stmt :=
            stmt
         || 'BEGIN
          '
         || get_par_for_assign (params)
         || '
          '
         || get_par_for_call (params, call_proc_name)
         || ';
          LOOP
             FETCH '
         || single_param.par_name
         || ' INTO '
         || col_str
         || ';
             EXIT WHEN '
         || single_param.par_name
         || '%NOTFOUND;
             p_rec_nm := p_rec_nm + 1;
             INSERT INTO '
         || table_name
         || ' ('
         || col_str
         || ')'
         || ' values ('
         || col_str
         || ');'
         || '
             END LOOP;
             CLOSE '
         || single_param.par_name
         || ';
         END;';

      IF (utplsql.tracing)
      THEN
         utplsql_util.PRINT (stmt);
      END IF;

      init_array_holder;
   END;

   FUNCTION prepare_and_fetch_rc (proc_name VARCHAR2)
      RETURN VARCHAR2
   IS
      vproc_nm     VARCHAR2 (50);
      metadata     utplsql_util.sqldata_tab;
      stmt         VARCHAR2 (32000);
      table_name   VARCHAR2 (20);
   BEGIN
      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Call=' || proc_name);
      END IF;

      vproc_nm := utplsql_util.get_proc_name (proc_name);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Proc Name=' || vproc_nm);
      END IF;

      utplsql_util.get_metadata_for_cursor (vproc_nm, metadata);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Metadata Done');
      END IF;

      table_name := get_table_name ();

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Refcursor Table Name=' || table_name);
      END IF;

      stmt := get_create_ddl (metadata, table_name);
dbms_output.put_line('Create ddl=' || stmt);
      IF (utplsql.tracing)
      THEN
         utplsql_util.PRINT ('Create ddl=' || stmt);
      END IF;

      execute_ddl (stmt);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Table created');
      END IF;

      IF (NVL (metadata.COUNT, 0) = 0)
      THEN
         RETURN (NULL);
      END IF;

      prepare_cursor_1 (stmt, table_name, proc_name, metadata);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done  prepare_cursor_1');
      END IF;

      execute_ddl (stmt);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done execute_ddl');
      END IF;

      RETURN (table_name);
   END;

   FUNCTION get_par_for_decl_200 (params utplsql_params)
      RETURN VARCHAR2
   IS
      decl   VARCHAR2 (1000);
   BEGIN
      FOR i IN 1 .. params.COUNT
      LOOP
         IF params(i).par_sql_type != 'REFC'
         THEN
         decl :=    decl
                 || params (i).par_name
                 || ' '
                 || utplsql_util.get_coltype_syntax (
                       params (i).par_sql_type,
                       1000
                    )
                 || ';'
                 || CHR (10);
         END IF;
      END LOOP;

      RETURN (decl);
   END;

   PROCEDURE prepare_cursor_200(stmt           IN OUT VARCHAR2,
                                table_name     VARCHAR2,
                                call_proc_name VARCHAR2,
                                POSITION       PLS_INTEGER,
                                metadata       utplsql_util.sqldata_tab,
                                params         utplsql_params) IS
     col_str            VARCHAR2(32000);
     single_param       params_rec;
     internal_cursor    SYS_REFCURSOR;
     internal_cursor_id NUMBER;
     call_str           VARCHAR2(32000);
     desc_tab           DBMS_SQL.desc_tab2; 
     col_count          NUMBER;
   BEGIN
     IF (NVL(metadata.COUNT, 0) = 0) THEN
       RETURN;
     END IF;

    

    
   END;

 FUNCTION prepare_and_fetch_rc (
      proc_name            VARCHAR2,
      params               utplsql_params,
      refc_pos_in_proc     PLS_INTEGER,
      refc_metadata_from   PLS_INTEGER DEFAULT 1,
      refc_metadata_str    VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      metadata     utplsql_util.sqldata_tab;
      stmt         VARCHAR2 (32000);
      table_name   VARCHAR2 (50);
      datatype     VARCHAR2 (20);
   BEGIN
      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('In prepare_and_fetch_rc ');
         DBMS_OUTPUT.put_line ('proc name        =' || proc_name);
         print_utplsql_params (params);
         DBMS_OUTPUT.put_line ('Method=' || TO_CHAR (refc_metadata_from));
         PRINT ('refc_metadata_str=' || refc_metadata_str);
        
         DBMS_OUTPUT.put_line ('Position=' || TO_CHAR (refc_pos_in_proc));
      END IF;

      utplsql_util.get_metadata_for_proc (
         proc_name,
         refc_pos_in_proc,
         datatype,
         metadata
      );

      IF (metadata.COUNT = 0)
      THEN
         IF (utplsql.tracing)
         THEN
            DBMS_OUTPUT.put_line ('Weak ref cursor');
         END IF;

         IF (refc_metadata_from = 1)
         THEN
            utplsql_util.get_metadata_for_table (refc_metadata_str, metadata);
         ELSIF (refc_metadata_from = 2)
         THEN
            utplsql_util.get_metadata_for_query (refc_metadata_str, metadata);
         END IF;
      END IF;

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done metadata');
      END IF;

      IF (metadata.COUNT = 0)
      THEN
         DBMS_OUTPUT.put_line ('ERROR: metadata  is null');
         RETURN (NULL);
      END IF;

      table_name := get_table_name ();

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Refcursor Table Name=' || table_name);
      END IF;

      stmt := get_create_ddl (metadata, table_name);

      IF (utplsql.tracing)
      THEN
         utplsql_util.PRINT ('Create ddl=' || stmt);
      END IF;

      execute_ddl (stmt);

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Table created');
      END IF;

      IF (NVL (metadata.COUNT, 0) = 0)
      THEN
         RETURN (NULL);
      END IF;

     
      prepare_cursor_200 (
         stmt,
         table_name,
         proc_name,
         refc_pos_in_proc,
         metadata,
         params
      );

      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done  prepare_cursor_1');
      END IF;



      IF (utplsql.tracing)
      THEN
         DBMS_OUTPUT.put_line ('Done  execute_ddl');
      END IF;

      RETURN (table_name);
   END;

   

END;
//

CREATE OR REPLACE PACKAGE BODY utreceq
IS

   PROCEDURE delete_receq (id_in ut_receq.id%TYPE)
   IS

    v_cur     number     := DBMS_SQL.open_cursor;
      sql_str   VARCHAR2 (2000);
      v_cnt     PLS_INTEGER;
   BEGIN
      SELECT COUNT (*)
        INTO v_cnt
        FROM ut_receq_pkg
       WHERE receq_id = id_in AND created_by = USER;

      IF v_cnt < 1
      THEN
         SELECT 'DROP FUNCTION ' || test_name
           INTO sql_str
           FROM ut_receq
          WHERE id = id_in AND created_by = USER;

         DBMS_SQL.parse (v_cur, sql_str, DBMS_SQL.native);
         DBMS_SQL.close_cursor (v_cur);
      END IF;

      DELETE FROM ut_receq
            WHERE id = id_in AND created_by = USER;
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (SQLERRM);
         DBMS_SQL.close_cursor (v_cur);
         utreport.pl (
            'Delete failed - function probably used by another package'
         );
   END;

   FUNCTION id_from_name (
      NAME_IN    IN   ut_receq.NAME%TYPE,
      owner_in   IN   ut_receq.rec_owner%TYPE := USER
   )
      RETURN INTEGER
   IS
      retval   INTEGER;
   BEGIN
      SELECT id
        INTO retval
        FROM ut_receq
       WHERE NAME = UPPER (NAME_IN)
         AND rec_owner = UPPER (owner_in)
         AND created_by = USER;

      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END id_from_name;

   FUNCTION name_from_id (id_in IN ut_receq.id%TYPE)
      RETURN VARCHAR2
   IS
      retval   VARCHAR2 (30);
   BEGIN
      SELECT NAME
        INTO retval
        FROM ut_receq
       WHERE id = id_in;

      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END name_from_id;

   FUNCTION receq_name (
      NAME_IN        IN   VARCHAR2,
      owner_in       IN   VARCHAR2 DEFAULT USER,
      test_name_in   IN   VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      retval     VARCHAR2 (30);
      func_len   INTEGER;
   BEGIN
      IF test_name_in IS NOT NULL
      THEN
         retval := UPPER (test_name_in);
      ELSE
         retval := 'eq_';

         IF owner_in <> USER
         THEN
            retval := retval || owner_in || '_';
         END IF;

         retval := retval || NAME_IN;
      END IF;

      RETURN UPPER (retval);
   EXCEPTION
      WHEN VALUE_ERROR
      THEN
         utreport.pl ('Generated test name is too long');
         utreport.pl (
            'Resubmit with a defined test_name_in=>''EQ_your_name'''
         );
         RETURN NULL;
   END receq_name;

   PROCEDURE COMPILE (receq_id_in IN ut_receq.id%TYPE)
   IS
      lines      DBMS_SQL.varchar2s;

    cur        number               := DBMS_SQL.open_cursor;
      v_rec      ut_receq%ROWTYPE;
      v_schema   ut_receq.rec_owner%TYPE;
   BEGIN
      lines.DELETE;

      SELECT *
        INTO v_rec
        FROM ut_receq
       WHERE id = receq_id_in AND created_by = USER;

      IF v_rec.rec_owner <> USER
      THEN
         v_schema := v_rec.rec_owner || '.';
      END IF;

      lines (1) := 'CREATE OR REPLACE FUNCTION ' || v_rec.test_name || '(';
      lines (lines.LAST + 1) := '   a ' || v_schema || v_rec.NAME || '%ROWTYPE , ';
      lines (lines.LAST + 1) := '   b ' || v_schema || v_rec.NAME || '%ROWTYPE ) ';
      lines (lines.LAST + 1) := 'RETURN BOOLEAN ';
      lines (lines.LAST + 1) := 'IS  BEGIN ';
      lines (lines.LAST + 1) := '    RETURN (';

      FOR utc_rec IN (SELECT   *
                          FROM all_tab_columns
                         WHERE table_name = v_rec.NAME
                           AND owner = v_rec.rec_owner
                      ORDER BY column_id)
      LOOP
         IF utc_rec.column_id > 1
         THEN
            lines (lines.LAST + 1) := ' AND ';
         END IF;

         lines (lines.LAST + 1) :=    '( ( a.'
                                   || utc_rec.column_name
                                   || ' IS NULL AND  b.'
                                   || utc_rec.column_name
                                   || ' IS NULL ) OR ';

         IF utc_rec.data_type = 'CLOB'
         THEN
            lines (lines.LAST) :=    lines (lines.LAST)
                                  || 'DBMS_LOB.COMPARE( a.'
                                  || utc_rec.column_name
                                  || ' , b.'
                                  || utc_rec.column_name
                                  || ') = 0 )';
         ELSE
            lines (lines.LAST) :=    lines (lines.LAST)
                                  || 'a.'
                                  || utc_rec.column_name
                                  || ' = b.'
                                  || utc_rec.column_name
                                  || ')';
         END IF;
      END LOOP;

      lines (lines.LAST + 1) := '); END ' || v_rec.test_name || ';';
   
      DBMS_SQL.close_cursor (cur);

   END COMPILE;

   PROCEDURE ADD (
      pkg_name_in    IN   ut_package.NAME%TYPE,
      record_in      IN   ut_receq.NAME%TYPE,
      rec_owner_in   IN   ut_receq.created_by%TYPE := USER
   )
   IS
      v_pkg_id     NUMBER;
      v_receq_id   NUMBER;
      v_obj_type   user_objects.object_type%TYPE;
      v_recname    VARCHAR2 (30);
   BEGIN
      v_pkg_id := utpackage.id_from_name (pkg_name_in);
      v_receq_id := id_from_name (record_in);

      IF v_pkg_id IS NULL
      THEN
         utreport.pl (pkg_name_in || ' does not exist');
      ELSIF v_receq_id IS NULL
      THEN
         SELECT object_type
           INTO v_obj_type
           FROM all_objects
          WHERE object_name = UPPER (record_in)
            AND owner = UPPER (rec_owner_in)
            AND object_type IN ('TABLE', 'VIEW');

         v_receq_id := utplsql.seqval ('ut_receq');
         v_recname := receq_name (record_in, rec_owner_in);

         INSERT INTO ut_receq
              VALUES (v_receq_id, UPPER (record_in), v_recname, USER, UPPER (
                                                                         rec_owner_in
                                                                      ));
      END IF;

      utreceq.COMPILE (v_receq_id);
      utreport.pl (v_recname || ' compiled for ' || v_obj_type || ' ' || record_in);

      BEGIN
         INSERT INTO ut_receq_pkg
              VALUES (v_receq_id, v_pkg_id, USER);
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN
            utreport.pl (
               v_recname || ' already registered for package ' || pkg_name_in
            );
      END;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         utreport.pl (record_in || ' does not exist in schema ' || rec_owner_in);
   END ADD;

   PROCEDURE COMPILE (pkg_name_in IN ut_package.NAME%TYPE)
   IS
      v_pkg_id   NUMBER;
   BEGIN
      v_pkg_id := utpackage.id_from_name (pkg_name_in);

      FOR j IN (SELECT receq_id
                  FROM ut_receq_pkg
                 WHERE pkg_id = v_pkg_id AND created_by = USER)
      LOOP
         COMPILE (j.receq_id);
      END LOOP;
   END COMPILE;

   PROCEDURE REM (
      NAME_IN          IN   ut_receq.NAME%TYPE,
      rec_owner_in     IN   ut_receq.created_by%TYPE,
      for_package_in   IN   BOOLEAN := FALSE
   )
   IS
      v_pkg_id     INTEGER;
      v_receq_id   INTEGER;
   BEGIN
      IF for_package_in
      THEN
         v_pkg_id := utpackage.id_from_name (NAME_IN);

         FOR j IN (SELECT receq_id
                     FROM ut_receq_pkg
                    WHERE pkg_id = v_pkg_id)
         LOOP
            DELETE FROM ut_receq_pkg
                  WHERE pkg_id = v_pkg_id
                    AND created_by = USER
                    AND receq_id = j.receq_id;

            delete_receq (v_receq_id);
         END LOOP;
      ELSE
         v_receq_id := utreceq.id_from_name (NAME_IN, rec_owner_in);

         DELETE FROM ut_receq_pkg
               WHERE receq_id = v_receq_id;

         delete_receq (v_receq_id);
      END IF;
   END REM;
END utreceq;
//

CREATE OR REPLACE PACKAGE BODY Utreport
IS

   DEFAULT_REPORTER VARCHAR2(100) := 'Output';

   DYNAMIC_PLSQL_FAILURE NUMBER(10) := -6550;

   g_reporter VARCHAR2(100);

   g_actual VARCHAR2(100);

   FUNCTION parse_it(proc IN VARCHAR2, params IN NUMBER, force_reporter IN VARCHAR2)
      RETURN INTEGER
   IS
      dyn_handle INTEGER := NULL;
      query VARCHAR2(1000);
   BEGIN
      dyn_handle := DBMS_SQL.OPEN_CURSOR;
      QUERY := 'BEGIN ut' || NVL(force_reporter, g_actual) || 'Reporter.' || proc ;
      IF params = 1 THEN
         QUERY := QUERY || '(:p)';
      END IF;
      QUERY := QUERY || '; END;';
      DBMS_SQL.PARSE(dyn_handle, QUERY, DBMS_SQL.NATIVE);
      RETURN dyn_handle;
   EXCEPTION
      WHEN OTHERS THEN
        DBMS_SQL.CLOSE_CURSOR (dyn_handle);
        RAISE;
   END;

   PROCEDURE execute_it(dyn_handle IN OUT INTEGER)
   IS
      dyn_result INTEGER;
   BEGIN
      dyn_result := DBMS_SQL.EXECUTE (dyn_handle);
      DBMS_SQL.CLOSE_CURSOR (dyn_handle);
   END;

   PROCEDURE call(proc IN VARCHAR2,
                  param IN VARCHAR2,
                  params IN NUMBER := 1,
                  force_reporter IN VARCHAR2 := NULL,
                  failover IN BOOLEAN := TRUE)
   IS
      dyn_handle INTEGER := NULL;
   BEGIN
      dyn_handle := parse_it(proc, params, force_reporter);
      IF params = 1 THEN
         DBMS_SQL.BIND_VARIABLE (dyn_handle, 'p', param);
      END IF;
      execute_it(dyn_handle);
    EXCEPTION
      WHEN OTHERS THEN

        IF dyn_handle IS NOT NULL THEN
          DBMS_SQL.CLOSE_CURSOR (dyn_handle);
        END IF;

        IF g_actual <> DEFAULT_REPORTER THEN

          IF NOT failover OR SQLCODE <> DYNAMIC_PLSQL_FAILURE THEN
            g_actual := DEFAULT_REPORTER;
            pl(SQLERRM);
            pl('** REVERTING TO DEFAULT REPORTER **');
          END IF;

        ELSE
          RAISE;
        END IF;

        call(proc, param, params, force_reporter => DEFAULT_REPORTER);
   END;

   PROCEDURE call(proc IN VARCHAR2,
                  failover IN BOOLEAN := TRUE)
   IS
   BEGIN
     call(proc => proc,
          param => '',
          params => 0,
          failover => failover);
   END;

   PROCEDURE use(reporter IN VARCHAR2)
   IS
   BEGIN
     g_reporter := NVL(reporter, DEFAULT_REPORTER);
     g_actual := g_reporter;
   END;

   FUNCTION using RETURN VARCHAR2
   IS
   BEGIN
     RETURN g_reporter;
   END;

   PROCEDURE open
   IS
   BEGIN
      g_actual := g_reporter;
      call('open', failover => FALSE);
   END;

   PROCEDURE pl (str IN VARCHAR2)
   IS
   BEGIN
      call('pl', str);
   END;

   PROCEDURE pl (bool IN BOOLEAN)
   IS
   BEGIN
      pl (Utplsql.bool2vc (bool));
   END;

   PROCEDURE before_results(run_id IN utr_outcome.run_id%TYPE)
   IS
   BEGIN
      call('before_results', run_id);
   END;

   PROCEDURE show_failure(rec_result IN utr_outcome%ROWTYPE)
   IS
   BEGIN
      outcome := rec_result;
      call('show_failure');
   END;

   PROCEDURE show_result(rec_result IN utr_outcome%ROWTYPE)
   IS
   BEGIN
      outcome := rec_result;
      call('show_result');
   END;

   PROCEDURE after_results(run_id IN utr_outcome.run_id%TYPE)
   IS
   BEGIN
      call('after_results', run_id);
   END;

   PROCEDURE before_errors(run_id IN utr_error.run_id%TYPE)
   IS
   BEGIN
      call('before_errors', run_id);
   END;

   PROCEDURE show_error(rec_error IN utr_error%ROWTYPE)
   IS
   BEGIN
      error := rec_error;
      call('show_error');
   END;

   PROCEDURE after_errors(run_id IN utr_error.run_id%TYPE)
   IS
   BEGIN
      call('after_errors', run_id);
   END;

   PROCEDURE close
   IS
   BEGIN
      call('close');
   END;

BEGIN

   g_reporter := NVL(utconfig.getreporter, DEFAULT_REPORTER);
   g_actual := g_reporter;

END;
//

CREATE OR REPLACE PACKAGE BODY utrerror
IS

   FUNCTION uterrcode (errmsg_in IN VARCHAR2 := NULL)
      RETURN INTEGER
   IS
   BEGIN

      IF NVL (errmsg_in, SQLERRM) LIKE 'OBE-_____: ' || c_error_indicator
      THEN
         RETURN SUBSTR (NVL (errmsg_in, SQLERRM), 15, 6);
      ELSE
         RETURN NULL;
      END IF;
   END;

   PROCEDURE raise_error (
      errcode_in   IN   utr_error.errcode%TYPE,
      errtext_in   IN   utr_error.errtext%TYPE
   )
   IS
   BEGIN

      IF errcode_in BETWEEN 300000 AND 399999
      THEN
         raise_application_error (
            -20000,
            SUBSTR (   'UT-'
                    || errcode_in
                    || ': '
                    || errtext_in, 1, 255)
         );
      ELSE
         raise_application_error (
            -20000,
            SUBSTR (   errcode_in
                    || ': '
                    || errtext_in
                    || '"', 1, 255)
         );
      END IF;
   END;

   PROCEDURE ins (
      run_id_in        IN   utr_error.run_id%TYPE := NULL,
      suite_id_in      IN   utr_error.suite_id%TYPE := NULL,
      utp_id_in        IN   utr_error.utp_id%TYPE := NULL,
      unittest_id_in   IN   utr_error.unittest_id%TYPE := NULL,
      testcase_id_in   IN   utr_error.testcase_id%TYPE := NULL,
      outcome_id_in    IN   utr_error.outcome_id%TYPE := NULL,
      errlevel_in      IN   utr_error.errlevel%TYPE := NULL,
      errcode_in       IN   utr_error.errcode%TYPE := NULL,
      errtext_in       IN   utr_error.errtext%TYPE := NULL,
      description_in   IN   utr_error.description%TYPE := NULL,
      recorderr        IN   BOOLEAN := TRUE,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
      l_message   VARCHAR2 (2000);

      PRAGMA autonomous_transaction;

   BEGIN

      IF errtext_in LIKE c_error_indicator
      THEN

         NULL;
      ELSE
         IF recorderr
         THEN
            INSERT INTO utr_error
                        (run_id, suite_id, utp_id, unittest_id,
                         testcase_id, outcome_id, errlevel, occurred_on,
                         errcode, errtext, description)
                 VALUES (run_id_in, suite_id_in, utp_id_in, unittest_id_in,
                         testcase_id_in, outcome_id_in, errlevel_in, SYSDATE,
                         errcode_in, errtext_in, description_in);
         ELSE
            l_message :=    'Error UT-'
                         || NVL (errcode_in, general_error)
                         || ': '
                         || errtext_in;

            IF errlevel_in IS NOT NULL
            THEN
               l_message :=    l_message
                            || ' '
                            || errlevel_in;
            END IF;

            IF description_in IS NOT NULL
            THEN
               l_message :=    l_message
                            || ' '
                            || description_in;
            END IF;

            utreport.pl (l_message);
         END IF;
      END IF;

      COMMIT;

      IF raiseexc
      THEN
         raise_error (errcode_in, errtext_in);
      END IF;

   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         RAISE;

   END;

   PROCEDURE report (
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      errlevel_in      IN   VARCHAR2 := NULL,
      recorderr        IN   BOOLEAN := TRUE,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
   BEGIN
      ins (
         run_id_in=> NULL,
         suite_id_in=> NULL,
         utp_id_in=> NULL,
         unittest_id_in=> NULL,
         testcase_id_in=> NULL,
         outcome_id_in=> NULL,
         errlevel_in=> errlevel_in,
         errcode_in=> errcode_in,
         errtext_in=> errtext_in,
         description_in=> description_in,
         recorderr=> recorderr,
         raiseexc=> raiseexc
      );
   END;

   PROCEDURE report_define_error (
      define_in    IN   VARCHAR2,
      message_in   IN   VARCHAR2 := NULL
   )
   IS
   BEGIN
      report (
         errcode_in=> SQLCODE,
         errtext_in=> SQLERRM,
         description_in=> message_in,
         errlevel_in=> define_in,
         recorderr=> FALSE,
         raiseexc=> TRUE
      );
   END;

   PROCEDURE assert (
      condition_in   IN   BOOLEAN,
      message_in     IN   VARCHAR2,
      raiseexc       IN   BOOLEAN := TRUE,
      raiseerr       IN   INTEGER := NULL
   )
   IS
   BEGIN
      IF    condition_in IS NULL
         OR NOT condition_in
      THEN
         report (
            errcode_in=> NVL (raiseerr, assertion_failure),
            errtext_in=> message_in,
            description_in=> NULL,
            errlevel_in=> NULL,
            recorderr=> FALSE,
            raiseexc=> raiseexc
         );
      END IF;
   END;

   PROCEDURE suite_report (
      run_in           IN   INTEGER,
      suite_in         IN   ut_suite.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
   BEGIN
      ins (
         run_id_in=> run_in,
         suite_id_in=> suite_in,
         utp_id_in=> NULL,
         unittest_id_in=> NULL,
         testcase_id_in=> NULL,
         outcome_id_in=> NULL,
         errlevel_in=> ututp.c_abbrev,
         errcode_in=> errcode_in,
         errtext_in=> errtext_in,
         description_in=> description_in,
         raiseexc=> raiseexc
      );
   END;

   PROCEDURE utp_report (
      run_in           IN   INTEGER,
      utp_in           IN   ut_utp.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
   BEGIN
      ins (
         run_id_in=> run_in,
         utp_id_in=> utp_in,
         unittest_id_in=> NULL,
         testcase_id_in=> NULL,
         outcome_id_in=> NULL,
         errlevel_in=> ututp.c_abbrev,
         errcode_in=> errcode_in,
         errtext_in=> errtext_in,
         description_in=> description_in,
         raiseexc=> raiseexc
      );
   END;

   PROCEDURE ut_report (
      run_in           IN   INTEGER,
      unittest_in      IN   ut_unittest.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
   BEGIN
      ins (
         run_id_in=> run_in,
         utp_id_in=> NULL,
         unittest_id_in=> unittest_in,
         testcase_id_in=> NULL,
         outcome_id_in=> NULL,
         errlevel_in=> utunittest.c_abbrev,
         errcode_in=> errcode_in,
         errtext_in=> errtext_in,
         description_in=> description_in,
         raiseexc=> raiseexc
      );
   END;

   PROCEDURE tc_report (
      run_in           IN   INTEGER,
      testcase_in      IN   ut_testcase.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
   BEGIN
      ins (
         run_id_in=> run_in,
         utp_id_in=> NULL,
         unittest_id_in=> NULL,
         testcase_id_in=> testcase_in,
         outcome_id_in=> NULL,
         errlevel_in=> uttestcase.c_abbrev,
         errcode_in=> errcode_in,
         errtext_in=> errtext_in,
         description_in=> description_in,
         raiseexc=> raiseexc
      );
   END;

   PROCEDURE oc_report (
      run_in           IN   INTEGER,
      outcome_in       IN   ut_outcome.id%TYPE,
      errcode_in       IN   INTEGER,
      errtext_in       IN   VARCHAR2 := NULL,
      description_in   IN   VARCHAR2 := NULL,
      raiseexc         IN   BOOLEAN := TRUE
   )
   IS
   BEGIN
      ins (
         run_id_in=> run_in,
         utp_id_in=> NULL,
         unittest_id_in=> NULL,
         testcase_id_in=> NULL,
         outcome_id_in=> outcome_in,
         errlevel_in=> utoutcome.c_abbrev,
         errcode_in=> errcode_in,
         errtext_in=> errtext_in,
         description_in=> description_in,
         raiseexc=> raiseexc
      );
   END;
END utrerror;
//

CREATE OR REPLACE PACKAGE BODY utresult
IS

   resultindx            PLS_INTEGER;
   g_header_shown        BOOLEAN     := FALSE ;
   g_include_successes   BOOLEAN     := TRUE ;

   PROCEDURE include_successes
   IS
   BEGIN
      g_include_successes := TRUE ;
   END;

   PROCEDURE ignore_successes
   IS
   BEGIN
      g_include_successes := FALSE ;
   END;

   PROCEDURE showone (
      run_id_in   IN   utr_outcome.run_id%TYPE := NULL,
      indx_in     IN   PLS_INTEGER
   )
   IS
   BEGIN
      utreport.pl (results (indx_in).NAME || ': ' || results (indx_in).msg);
   END;

   PROCEDURE show (
      run_id_in   IN   utr_outcome.run_id%TYPE := NULL,
      reset_in    IN   BOOLEAN := FALSE
   )
   IS
      indx     PLS_INTEGER               := results.FIRST;
      l_id     utr_outcome.run_id%TYPE   := NVL (run_id_in, utplsql2.runnum);
      norows   BOOLEAN;
   BEGIN

      utreport.before_results(run_id_in);

      FOR rec IN (SELECT   *
                      FROM utr_outcome
                     WHERE run_id = l_id
                  ORDER BY tc_run_id 
      )
      LOOP
         IF rec.status = utplsql.c_success
        AND
      (NOT NVL (g_include_successes, FALSE )

       OR
       utconfig.showingfailuresonly)
         THEN

            NULL;
         ELSIF utconfig.showingfailuresonly
     THEN
            null;

         END IF;
      END LOOP;

      utreport.after_results(run_id_in);

      utreport.before_errors(run_id_in);

      FOR rec IN (SELECT *
                    FROM utr_error
                   WHERE run_id = l_id)
      LOOP
         null;
      END LOOP;

      utreport.after_errors(run_id_in);

      IF reset_in
      THEN
         init;
      END IF;
   END;

   PROCEDURE showlast (run_id_in IN utr_outcome.run_id%TYPE := NULL)
   IS
      indx   PLS_INTEGER := results.LAST;
   BEGIN

      IF failure
      THEN
         showone (run_id_in, indx);
      END IF;
   END;

   PROCEDURE report (msg_in IN VARCHAR2)
   IS
      indx   PLS_INTEGER := NVL (results.LAST, 0) + 1;
   BEGIN
      results (indx).NAME := utplsql.currcase.NAME;
      results (indx).indx := utplsql.currcase.indx;
      results (indx).msg := msg_in;
   END;

   PROCEDURE init (from_suite_in IN BOOLEAN := FALSE )
   IS
   BEGIN
      results.DELETE;

   END;

   FUNCTION success (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN BOOLEAN
   IS
      l_count   PLS_INTEGER;
   BEGIN
      RETURN utresult2.run_succeeded (NVL (run_id_in, utplsql2.runnum));
   END;

   FUNCTION failure (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN BOOLEAN
   IS
   BEGIN
      RETURN (NOT success (run_id_in));
   END;

   PROCEDURE firstresult (run_id_in IN utr_outcome.run_id%TYPE := NULL)
   IS
   BEGIN
      resultindx := results.FIRST;
   END;

   FUNCTION nextresult (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN result_rt
   IS
   BEGIN

      IF resultindx IS NULL
      THEN
         firstresult;
      ELSE
         resultindx := results.NEXT (resultindx);
      END IF;

      RETURN results (resultindx);
   END;

   FUNCTION nthresult (
      indx_in     IN   PLS_INTEGER,
      run_id_in   IN   utr_outcome.run_id%TYPE := NULL
   )
      RETURN result_rt
   IS
      nullval   result_rt;
   BEGIN
      IF indx_in > resultcount OR NOT results.EXISTS (indx_in)
      THEN
         RETURN nullval;
      ELSE
         RETURN results (indx_in);
      END IF;
   END;

   PROCEDURE nextresult (
      name_out        OUT      VARCHAR2,
      msg_out         OUT      VARCHAR2,
      case_indx_out   OUT      PLS_INTEGER,
      run_id_in       IN       utr_outcome.run_id%TYPE := NULL
   )
   IS
      rec   result_rt;
   BEGIN
      rec := nextresult;
      name_out := rec.NAME;
      msg_out := rec.msg;
      case_indx_out := rec.indx;
   END;

   PROCEDURE nthresult (
      indx_in         IN       PLS_INTEGER,
      name_out        OUT      VARCHAR2,
      msg_out         OUT      VARCHAR2,
      case_indx_out   OUT      PLS_INTEGER,
      run_id_in       IN       utr_outcome.run_id%TYPE := NULL
   )
   IS
      rec   result_rt;
   BEGIN
      rec := nthresult (indx_in);
      name_out := rec.NAME;
      msg_out := rec.msg;
      case_indx_out := rec.indx;
   END;

   FUNCTION resultcount (run_id_in IN utr_outcome.run_id%TYPE := NULL)
      RETURN PLS_INTEGER
   IS
   BEGIN
      RETURN results.COUNT;
   END;
END utresult;
//

CREATE OR REPLACE PACKAGE BODY utresult2
IS

   PROCEDURE report (
      outcome_in       IN   ut_outcome.ID%TYPE
     ,description_in   IN   VARCHAR2
   )
   IS
   BEGIN
      NULL;
   END;

   PROCEDURE report (
      outcome_in       IN   ut_outcome.ID%TYPE
     ,test_failed_in   IN   BOOLEAN
     ,description_in   IN   VARCHAR2
     ,register_in      IN   BOOLEAN := TRUE
     , -- v1 compatibility
      showresults_in   IN   BOOLEAN := FALSE -- v1 compatibility
   )
   IS
      l_id            utr_outcome.outcome_id%TYPE    := outcome_in;
      l_description   utr_outcome.description%TYPE   := description_in;
   BEGIN
      IF utplsql2.tracing
      THEN
         utreport.pl ('Record outcome result:');
         utreport.pl (utplsql2.runnum);
         utreport.pl (utplsql2.tc_runnum);
         utreport.pl (outcome_in);
         utreport.pl (test_failed_in);
         utreport.pl (description_in);
      END IF;

      IF register_in
      THEN
         IF l_id IS NULL
         THEN

            l_id := utroutcome.next_v1_id (utplsql2.runnum);
            l_description := utplsql.currcase.NAME || ': ' || l_description;
         END IF;

         utroutcome.RECORD (utplsql2.runnum
                           ,utplsql2.tc_runnum
                           , -- 2.0.9.1
                            l_id
                           ,test_failed_in
                           ,description_in
                           );
      END IF;

      IF test_failed_in
      THEN
         IF register_in
         THEN
            utresult.report (description_in);
         ELSE
            utreport.pl (description_in);
         END IF;

         IF showresults_in AND register_in
         THEN
            utresult.showlast;
         END IF;
      END IF;
   END;

   FUNCTION run_succeeded (runnum_in IN utr_outcome.run_id%TYPE)
      RETURN BOOLEAN

   IS
      l_val           CHAR (1);
      success_found   BOOLEAN;
      failure_found   BOOLEAN;

      CURSOR err_cur
      IS
         SELECT 'x'
           FROM utr_error
          WHERE run_id = runnum_in;

      CURSOR stat_cur (status_in IN VARCHAR2)
      IS
         SELECT 'x'
           FROM utr_outcome
          WHERE run_id = runnum_in AND status LIKE status_in;
   BEGIN

      OPEN err_cur;
      FETCH err_cur INTO l_val;
      failure_found := err_cur%FOUND;

      IF NOT failure_found
      THEN
         OPEN stat_cur (c_success);
         FETCH stat_cur INTO l_val;
         success_found := stat_cur%FOUND;
         CLOSE stat_cur;
         OPEN stat_cur (c_failure);
         FETCH stat_cur INTO l_val;
         failure_found := stat_cur%FOUND;
         CLOSE stat_cur;
      END IF;

      IF NOT failure_found AND NOT success_found
      THEN
         RETURN NULL;
      ELSE
         RETURN NOT failure_found;
      END IF;

   END run_succeeded;

   FUNCTION run_status (runnum_in IN utr_outcome.run_id%TYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      IF run_succeeded (runnum_in)
      THEN
         RETURN c_success;
      ELSE
         RETURN c_failure;
      END IF;
   END;

   FUNCTION utp_succeeded (
      runnum_in   IN   utr_outcome.run_id%TYPE
     ,utp_in      IN   utr_utp.utp_id%TYPE
   )
      RETURN BOOLEAN
   IS
      l_val           CHAR (1);
      success_found   BOOLEAN;
      failure_found   BOOLEAN;

      CURSOR err_cur
      IS
         SELECT 'x'
           FROM utr_error
          WHERE run_id = runnum_in
            AND utp_id = utp_in
            AND errlevel = ututp.c_abbrev;

      CURSOR stat_cur (status_in IN VARCHAR2)
      IS
         SELECT 'x'
           FROM utr_outcome
          WHERE run_id = runnum_in
            AND utoutcome.utp (outcome_id) = utp_in
            AND status LIKE status_in;
   BEGIN
      OPEN err_cur;
      FETCH err_cur INTO l_val;
      failure_found := err_cur%FOUND;

      IF NOT failure_found
      THEN
         OPEN stat_cur (c_success);
         FETCH stat_cur INTO l_val;
         success_found := stat_cur%FOUND;
         CLOSE stat_cur;
         OPEN stat_cur (c_failure);
         FETCH stat_cur INTO l_val;
         failure_found := stat_cur%FOUND;
         CLOSE stat_cur;
      END IF;

      IF NOT failure_found AND NOT success_found
      THEN
         RETURN NULL;
      ELSE
         RETURN NOT failure_found;
      END IF;
   END utp_succeeded;

   FUNCTION utp_status (
      runnum_in   IN   utr_outcome.run_id%TYPE
     ,utp_in      IN   utr_utp.utp_id%TYPE
   )
      RETURN VARCHAR2
   IS
   BEGIN
      IF utp_succeeded (runnum_in, utp_in)
      THEN
         RETURN c_success;
      ELSE
         RETURN c_failure;
      END IF;
   END;

   FUNCTION unittest_succeeded (
      runnum_in     IN   utr_outcome.run_id%TYPE
     ,unittest_in   IN   utr_unittest.unittest_id%TYPE
   )
      RETURN BOOLEAN
   IS
      l_val           CHAR (1);
      success_found   BOOLEAN;
      failure_found   BOOLEAN;

      CURSOR err_cur
      IS
         SELECT 'x'
           FROM utr_error
          WHERE run_id = runnum_in
            AND unittest_id = unittest_in
            AND errlevel = utunittest.c_abbrev;

      CURSOR stat_cur (status_in IN VARCHAR2)
      IS
         SELECT 'x'
           FROM utr_outcome
          WHERE run_id = runnum_in
            AND utoutcome.unittest (outcome_id) = unittest_in
            AND status LIKE status_in;
   BEGIN
      OPEN err_cur;
      FETCH err_cur INTO l_val;
      failure_found := err_cur%FOUND;

      IF NOT failure_found
      THEN
         OPEN stat_cur (c_success);
         FETCH stat_cur INTO l_val;
         success_found := stat_cur%FOUND;
         CLOSE stat_cur;
         OPEN stat_cur (c_failure);
         FETCH stat_cur INTO l_val;
         failure_found := stat_cur%FOUND;
         CLOSE stat_cur;
      END IF;

      IF NOT failure_found AND NOT success_found
      THEN
         RETURN NULL;
      ELSE
         RETURN NOT failure_found;
      END IF;
   END unittest_succeeded;

   FUNCTION unittest_status (
      runnum_in     IN   utr_outcome.run_id%TYPE
     ,unittest_in   IN   utr_unittest.unittest_id%TYPE
   )
      RETURN VARCHAR2
   IS
   BEGIN
      IF unittest_succeeded (runnum_in, unittest_in)
      THEN
         RETURN c_success;
      ELSE
         RETURN c_failure;
      END IF;
   END;

   FUNCTION results_headers (schema_in IN VARCHAR2, program_in IN VARCHAR2)
      RETURN utconfig.refcur_t
   IS
      retval   utconfig.refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT   run_id, start_on, end_on, status
             FROM ut_utp utp, utr_utp utpr
            WHERE utp.ID = utpr.utp_id
              AND utp.program = UPPER (program_in)
              AND utp.owner = UPPER (schema_in)
         ORDER BY end_on;
      RETURN retval;
   END results_headers;

   FUNCTION results_details (
      run_id_in               IN   utr_utp.run_id%TYPE
     ,show_failures_only_in   IN   ut_config.show_failures_only%TYPE
   )
      RETURN utconfig.refcur_t
   IS
      retval   utconfig.refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT   start_on, end_on, status, description
             FROM utr_outcome
            WHERE run_id = run_id_in
              AND (   show_failures_only_in = 'N'
                   OR (show_failures_only_in = 'Y' AND status = 'FAILURE'
                      )
                  )
         ORDER BY start_on;
      RETURN retval;
   END results_details;
END utresult2;
//

CREATE OR REPLACE PACKAGE BODY utroutcome
IS

   PROCEDURE initiate (
      run_id_in IN utr_outcome.run_id%TYPE
    , outcome_id_in IN utr_outcome.outcome_id%TYPE
    , start_on_in IN DATE := SYSDATE
   )
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      utplsql2.set_current_outcome (outcome_id_in);

      INSERT INTO utr_outcome
                  (run_id, outcome_id, start_on
                  )
           VALUES (run_id_in, outcome_id_in, start_on_in
                  );

      COMMIT;

   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN

         NULL;

         ROLLBACK;

      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.oc_report (run_id_in
                           , outcome_id_in
                           , SQLCODE
                           , SQLERRM
                           ,    'Unable to initiate outcome for run '
                             || run_id_in
                             || ' outcome ID '
                             || outcome_id_in
                            );
   END initiate;

   PROCEDURE RECORD (
      run_id_in IN utr_outcome.run_id%TYPE
    , tc_run_id_in IN PLS_INTEGER
    , outcome_id_in IN utr_outcome.outcome_id%TYPE
    , test_failed_in IN BOOLEAN
    , description_in IN VARCHAR2 := NULL
    , end_on_in IN DATE := SYSDATE
   )
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

      CURSOR start_cur (id_in IN utr_outcome.outcome_id%TYPE)
      IS
         SELECT start_on, end_on
           FROM utr_outcome
          WHERE run_id = run_id_in AND outcome_id = id_in;

      rec        start_cur%ROWTYPE;
      l_status   utr_outcome.status%TYPE;
   BEGIN

      IF test_failed_in
      THEN
         l_status := utresult2.c_failure;
      ELSE
         l_status := utresult2.c_success;
      END IF;

      OPEN start_cur (outcome_id_in);
      FETCH start_cur INTO rec;

      IF start_cur%FOUND AND rec.end_on IS NULL
      THEN
         UPDATE utr_outcome
            SET end_on = end_on_in
              , status = l_status
              , description = description_in
          WHERE run_id = run_id_in AND outcome_id = outcome_id_in;
      ELSIF start_cur%FOUND AND rec.end_on IS NOT NULL
      THEN

         NULL;
      ELSE
         INSERT INTO utr_outcome
                     (run_id, tc_run_id, outcome_id, status
                    , end_on, description
                     )
              VALUES (run_id_in, tc_run_id_in, outcome_id_in, l_status
                    , end_on_in, description_in
                     );

         utplsql2.move_ahead_tc_runnum; -- 2.0.9.1
      END IF;

      CLOSE start_cur;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.oc_report (run_id_in
                           , outcome_id_in
                           , SQLCODE
                           , SQLERRM
                           ,    'Unable to insert or update the utr_outcome table for run '
                             || run_id_in
                             || ' outcome ID '
                             || outcome_id_in
                            );
   END RECORD;

   FUNCTION next_v1_id (run_id_in IN utr_outcome.run_id%TYPE)
      RETURN utr_outcome.outcome_id%TYPE
   IS
      retval   utr_outcome.outcome_id%TYPE;
   BEGIN
      SELECT MIN (outcome_id)
        INTO retval
        FROM utr_outcome
       WHERE run_id = run_id_in;

      retval := LEAST (NVL (retval, 0), 0) - 1;
      RETURN retval;
   END;

   PROCEDURE clear_results (run_id_in IN utr_outcome.run_id%TYPE)
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      DELETE FROM utr_outcome
            WHERE run_id = run_id_in;

      COMMIT;

   END;

   PROCEDURE clear_results (
      owner_in IN VARCHAR2
    , program_in IN VARCHAR2
    , start_from_in IN DATE
   )
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      DELETE FROM utr_outcome
            WHERE start_on >= start_from_in
              AND run_id IN (
                     SELECT r.run_id
                       FROM utr_utp r, ut_utp u
                      WHERE r.utp_id = u.ID
                        AND u.owner = owner_in
                        AND u.program = program_in);

      COMMIT;

   END;

   PROCEDURE clear_all_but_last (owner_in IN VARCHAR2, program_in IN VARCHAR2)
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      DELETE FROM utr_outcome
            WHERE start_on <
                     (SELECT MAX (o.start_on)
                        FROM utr_outcome o, utr_utp r, ut_utp u
                       WHERE r.utp_id = u.ID
                         AND u.owner = owner_in
                         AND u.program = program_in
                         AND o.run_id = r.run_id)
              AND run_id IN (
                     SELECT r.run_id
                       FROM utr_utp r, ut_utp u
                      WHERE r.utp_id = u.ID
                        AND u.owner = owner_in
                        AND u.program = program_in);

      COMMIT;

   END;
END utroutcome;
//

CREATE OR REPLACE PACKAGE BODY utrsuite
IS

   PROCEDURE initiate (
      run_id_in     IN   utr_suite.run_id%TYPE,
      suite_id_in   IN   utr_suite.suite_id%TYPE,
      start_on_in   IN   DATE := SYSDATE
   )
   IS

      PRAGMA autonomous_transaction;

   BEGIN
      utplsql2.set_current_suite (suite_id_in);

      INSERT INTO utr_suite
                  (run_id, suite_id, start_on)
           VALUES (run_id_in, suite_id_in, start_on_in);

      COMMIT;

   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN

         NULL;

         ROLLBACK;

      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.suite_report (
            run_id_in,
            suite_id_in,
            SQLCODE,
            SQLERRM,
               'Unable to initiate suite for run '
            || run_id_in
            || ' SUITE ID '
            || suite_id_in
         );
   END initiate;

   PROCEDURE terminate (
      run_id_in     IN   utr_suite.run_id%TYPE,
      suite_id_in   IN   utr_suite.suite_id%TYPE,
      end_on_in     IN   DATE := SYSDATE
   )
   IS

      PRAGMA autonomous_transaction;

      CURSOR start_cur
      IS
         SELECT start_on, end_on
           FROM utr_suite
          WHERE run_id = run_id_in
            AND suite_id_in = suite_id;

      rec        start_cur%ROWTYPE;
      l_status   utr_suite.status%TYPE;
   BEGIN
      l_status := utresult2.run_status (run_id_in);
      OPEN start_cur;
      FETCH start_cur INTO rec;

      IF      start_cur%FOUND
          AND rec.end_on IS NULL
      THEN
         UPDATE utr_suite
            SET end_on = end_on_in,
                status = l_status
          WHERE run_id = run_id_in
            AND suite_id_in = suite_id;
      ELSIF      start_cur%FOUND
             AND rec.end_on IS NOT NULL
      THEN

         NULL;
      ELSE
         INSERT INTO utr_suite
                     (run_id, suite_id, status, end_on)
              VALUES (run_id_in, suite_id_in, l_status, end_on_in);
      END IF;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.suite_report (
            run_id_in,
            suite_id_in,
            SQLCODE,
            SQLERRM,
               'Unable to insert or update the utr_suite table for run '
            || run_id_in
            || ' SUITE ID '
            || suite_id_in
         );
   END terminate;
END utrsuite;
//

CREATE OR REPLACE PACKAGE BODY utrunittest
IS

   PROCEDURE initiate (
      run_id_in        IN   utr_unittest.run_id%TYPE,
      unittest_id_in   IN   utr_unittest.unittest_id%TYPE,
      start_on_in      IN   DATE := SYSDATE
   )
   IS

      PRAGMA autonomous_transaction;

   BEGIN
      utplsql2.set_current_unittest (unittest_id_in);

      INSERT INTO utr_unittest
                  (run_id, unittest_id, start_on)
           VALUES (run_id_in, unittest_id_in, start_on_in);

      COMMIT;

   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN

         NULL;

         ROLLBACK;

      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.ut_report (
            run_id_in,
            unittest_id_in,
            SQLCODE,
            SQLERRM,
               'Unable to initiate unit test for run '
            || run_id_in
            || ' unit test ID '
            || unittest_id_in
         );
   END initiate;

   PROCEDURE terminate (
      run_id_in        IN   utr_unittest.run_id%TYPE,
      unittest_id_in   IN   utr_unittest.unittest_id%TYPE,
      end_on_in        IN   DATE := SYSDATE
   )
   IS

      PRAGMA autonomous_transaction;

      CURSOR start_cur
      IS
         SELECT start_on, end_on
           FROM utr_unittest
          WHERE run_id = run_id_in
            AND unittest_id_in = unittest_id;

      rec        start_cur%ROWTYPE;
      l_status   utr_unittest.status%TYPE
                     := utresult2.unittest_status (run_id_in, unittest_id_in);
   BEGIN
      OPEN start_cur;
      FETCH start_cur INTO rec;

      IF      start_cur%FOUND
          AND rec.end_on IS NULL
      THEN
         UPDATE utr_unittest
            SET end_on = end_on_in,
                status = l_status
          WHERE run_id = run_id_in
            AND unittest_id_in = unittest_id;
      ELSIF      start_cur%FOUND
             AND rec.end_on IS NOT NULL
      THEN

         NULL;
      ELSE
         INSERT INTO utr_unittest
                     (run_id, unittest_id, status, start_on,
                      end_on)
              VALUES (run_id_in, unittest_id_in, l_status, SYSDATE,
                      end_on_in);
      END IF;

      CLOSE start_cur;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.ut_report (
            run_id_in,
            unittest_id_in,
            SQLCODE,
            SQLERRM,
               'Unable to insert or update the utr_unittest table for run '
            || run_id_in
            || ' outcome ID '
            || unittest_id_in
         );
   END terminate;
END utrunittest;
//

CREATE OR REPLACE PACKAGE BODY utrutp
IS

   PROCEDURE initiate (
      run_id_in     IN   utr_utp.run_id%TYPE,
      utp_id_in     IN   utr_utp.utp_id%TYPE,
      start_on_in   IN   DATE := SYSDATE
   )
   IS

      PRAGMA autonomous_transaction;

   BEGIN
      utplsql2.set_current_utp (utp_id_in);

      INSERT INTO utr_utp
                  (run_id, utp_id, start_on)
           VALUES (run_id_in, utp_id_in, start_on_in);

      COMMIT;

   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN

         NULL;

         ROLLBACK;

      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.utp_report (
            run_id_in,
            utp_id_in,
            SQLCODE,
            SQLERRM,
               'Unable to initiate UTP for run '
            || run_id_in
            || ' UTP ID '
            || utp_id_in
         );
   END initiate;

   PROCEDURE terminate (
      run_id_in   IN   utr_utp.run_id%TYPE,
      utp_id_in   IN   utr_utp.utp_id%TYPE,
      end_on_in   IN   DATE := SYSDATE
   )
   IS

      PRAGMA autonomous_transaction;

      CURSOR start_cur
      IS
         SELECT start_on, end_on
           FROM utr_utp
          WHERE run_id = run_id_in
            AND utp_id_in = utp_id;

      rec        start_cur%ROWTYPE;
      l_status   utr_utp.status%TYPE;
   BEGIN
      l_status := utresult2.run_status (run_id_in);
      OPEN start_cur;
      FETCH start_cur INTO rec;

      IF      start_cur%FOUND
          AND rec.end_on IS NULL
      THEN
         UPDATE utr_utp
            SET end_on = end_on_in,
                status = l_status
          WHERE run_id = run_id_in
            AND utp_id_in = utp_id;
      ELSIF      start_cur%FOUND
             AND rec.end_on IS NOT NULL
      THEN

         NULL;
      ELSE
         INSERT INTO utr_utp
                     (run_id, utp_id, status, start_on, end_on)
              VALUES (run_id_in, utp_id_in, l_status, end_on_in, end_on_in);
      END IF;

      CLOSE start_cur;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN

         ROLLBACK;

         utrerror.utp_report (
            run_id_in,
            utp_id_in,
            SQLCODE,
            SQLERRM,
               'Unable to insert or update the utr_utp table for run '
            || run_id_in
            || ' outcome ID '
            || utp_id_in
         );
   END terminate;

   PROCEDURE clear_results (run_id_in IN utr_utp.run_id%TYPE)
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      DELETE FROM utr_utp
            WHERE run_id = run_id_in;

      COMMIT;

   END;

   PROCEDURE clear_results (
      owner_in IN VARCHAR2
    , program_in IN VARCHAR2
    , start_from_in IN DATE
   )
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      DELETE FROM utr_utp
            WHERE start_on >= start_from_in
              AND run_id IN (
                     SELECT r.run_id
                       FROM utr_utp r, ut_utp u
                      WHERE r.utp_id = u.ID
                        AND u.owner = owner_in
                        AND u.program = program_in);

      COMMIT;

   END;

   PROCEDURE clear_all_but_last (owner_in IN VARCHAR2, program_in IN VARCHAR2)
   IS

      PRAGMA AUTONOMOUS_TRANSACTION;

   BEGIN
      DELETE FROM utr_utp
            WHERE start_on <
                     (SELECT MAX (r.start_on)
                        FROM utr_utp r, ut_utp u
                       WHERE r.utp_id = u.ID
                         AND u.owner = owner_in
                         AND u.program = program_in)
              AND run_id IN (
                     SELECT r.run_id
                       FROM utr_utp r, ut_utp u
                      WHERE r.utp_id = u.ID
                        AND u.owner = owner_in
                        AND u.program = program_in);

      COMMIT;

   END;

   FUNCTION last_run_status (owner_in IN VARCHAR2, program_in IN VARCHAR2)
     RETURN utr_utp.status%TYPE
   IS
     retval   utr_utp.status%TYPE;
   BEGIN
     SELECT status
       INTO retval
       FROM utr_utp
      WHERE (utp_id, start_on) =
               (SELECT   r.utp_id, MAX (r.start_on)
                    FROM utr_utp r, ut_utp u
                   WHERE r.utp_id = u.ID
                     AND u.owner = owner_in
                     AND u.program = program_in
                GROUP BY r.utp_id)
        AND ROWNUM < 2;

     RETURN retval;
   EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        RETURN NULL;
   END;

END utrutp;
//

CREATE OR REPLACE PACKAGE BODY utsuite
IS

   FUNCTION name_from_id (id_in IN ut_suite.id%TYPE)
      RETURN ut_suite.name%TYPE
   IS
      retval   ut_suite.name%TYPE;
   BEGIN
      SELECT name
        INTO retval
        FROM ut_suite
       WHERE id = id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION id_from_name (name_in IN ut_suite.name%TYPE)
      RETURN ut_suite.id%TYPE
   IS
      retval   ut_suite.id%TYPE;
   BEGIN
      SELECT id
        INTO retval
        FROM ut_suite
       WHERE name = UPPER (name_in);
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION onerow (name_in IN ut_suite.name%TYPE)
      RETURN ut_suite%ROWTYPE
   IS
      retval   ut_suite%ROWTYPE;
   BEGIN
      SELECT *
        INTO retval
        FROM ut_suite
       WHERE name = UPPER (name_in);
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN retval;
   END;

   PROCEDURE ADD (
      name_in            IN   ut_suite.name%TYPE,
      desc_in            IN   VARCHAR2 := NULL,
      rem_if_exists_in   IN   BOOLEAN := TRUE,
    per_method_setup_in in ut_suite.per_method_setup%type := null
   )
   IS
       PRAGMA AUTONOMOUS_TRANSACTION;
      v_id   ut_suite.id%TYPE;
   BEGIN
      utrerror.assert (name_in IS NOT NULL, 'Suite names cannot be null.');

       v_id := utplsql.seqval ('ut_suite');

      INSERT INTO ut_suite
                  (id, name, description, executions, failures,per_method_setup)
           VALUES (v_id, UPPER (name_in), desc_in, 0, 0,per_method_setup_in);
    COMMIT;
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN
         IF rem_if_exists_in
         THEN
            rem (name_in);
            ADD (name_in, desc_in, per_method_setup_in => per_method_setup_in);
         ELSE
            ROLLBACK;
            RAISE;
         END IF;
      WHEN OTHERS
      THEN

        IF utrerror.uterrcode = utrerror.assertion_failure
         THEN
                     ROLLBACK;
                     raise;
         ELSE
                  ROLLBACK;
         utrerror.report_define_error (
            c_abbrev,
               'Suite '
            || name_in
         );
         END IF;

   END;

   PROCEDURE rem (id_in IN ut_suite.id%TYPE)
   IS
    PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN

      DELETE FROM ut_package
            WHERE suite_id = id_in;

      DELETE FROM ut_suite_utp
            WHERE suite_id = id_in;

      DELETE FROM ut_suite
            WHERE id = id_in;
   COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Remove suite error: '
                     || SQLERRM);
         ROLLBACK;
         RAISE;
   END;

   PROCEDURE rem (name_in IN ut_suite.name%TYPE)
   IS
       PRAGMA AUTONOMOUS_TRANSACTION;
      v_id   ut_suite.id%TYPE;
   BEGIN
      rem (id_from_name (name_in));
   END;

   PROCEDURE upd (
      id_in           IN   ut_suite.id%TYPE,
      start_in             DATE,
      end_in               DATE,
      successful_in        in BOOLEAN,
    per_method_setup_in in ut_suite.per_method_setup%type := null
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_status    VARCHAR2 (100) := utplsql.c_success;
      v_failure   PLS_INTEGER    := 0;

      PROCEDURE do_upd
      IS
      BEGIN
         UPDATE ut_suite
            SET last_status = l_status,
                last_start = start_in,
                last_end = end_in,
        per_method_setup = per_method_setup_in,
                executions =   NVL (executions, 0)
                             + 1,
                failures =   NVL (failures, 0)
                           + v_failure
          WHERE id = id_in;
      END;
   BEGIN
      IF NOT successful_in
      THEN
         v_failure := 1;
         l_status := utplsql.c_failure;
      END IF;

      do_upd;

      IF SQL%ROWCOUNT = 0
      THEN
         ADD (
            name_in=> name_from_id (id_in),
            desc_in=>    'No description for "'
                      || name_from_id (id_in)
                      || '"',
            rem_if_exists_in=> FALSE,
      per_method_setup_in=>per_method_setup_in
         );
         do_upd;
      END IF;
    COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Update suite error: '
                     || SQLERRM);
          ROLLBACK;
         RAISE;
   END;

   PROCEDURE upd (
      name_in         IN   ut_suite.name%TYPE,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN,
    per_method_setup_in in ut_suite.per_method_setup%type := null
   )
   IS
   BEGIN
      upd (id_from_name (name_in), start_in, end_in, successful_in,
    per_method_setup_in);
   END;

   FUNCTION suites (name_like_in IN VARCHAR2 := '%')
      RETURN utconfig.refcur_t
   IS
      retval   utconfig.refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT *
           FROM ut_suite
          WHERE NAME LIKE UPPER (name_like_in);
      RETURN retval;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN retval;
   END;

   PROCEDURE show_suites (name_like_in IN VARCHAR2 := '%')
   IS
     indent VARCHAR2(20) := ' ';
     suites_cur utconfig.refcur_t;
     suite ut_suite%ROWTYPE;

     CURSOR packages_cur (pi_id ut_suite.id%TYPE) IS
        SELECT * FROM ut_package
        WHERE suite_id = pi_id;

   BEGIN
      suites_cur := suites(name_like_in);

      LOOP
         FETCH suites_cur INTO suite;
         EXIT WHEN suites_cur%NOTFOUND;
         dbms_output.put_line(suite.name);
         FOR pack IN packages_cur(suite.id) LOOP
            dbms_output.put_line(indent || pack.name);
         END LOOP;
      END LOOP;
   END show_suites;

END utsuite;
//

CREATE OR REPLACE PACKAGE BODY uttest
IS

   FUNCTION name_from_id (id_in IN ut_test.id%TYPE)
      RETURN ut_test.name%TYPE
   IS
      retval   ut_test.name%TYPE;
   BEGIN
      SELECT name
        INTO retval
        FROM ut_test
       WHERE id = id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION id_from_name (name_in IN ut_test.name%TYPE)
      RETURN ut_test.id%TYPE
   IS
      retval   ut_test.id%TYPE;
   BEGIN
      SELECT name
        INTO retval
        FROM ut_test
       WHERE name = UPPER (name_in);
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   PROCEDURE ADD (
      package_in   IN   INTEGER,
      test_in      IN   VARCHAR2,
      desc_in      IN   VARCHAR2 := NULL,
      seq_in       IN   PLS_INTEGER := NULL
   )
   IS
       PRAGMA AUTONOMOUS_TRANSACTION; 
      v_id   ut_test.id%TYPE;
   BEGIN
     

      INSERT INTO ut_test
                  (id, package_id, name, description,
                   seq)
           VALUES (v_id, package_in, UPPER (test_in), desc_in,
                   NVL (seq_in, 1));
  COMMIT; 
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Add test error: '
                     || SQLERRM);
         ROLLBACK; 
         RAISE;
   END;

   PROCEDURE ADD (
      package_in   IN   VARCHAR2,
      test_in      IN   VARCHAR2,
      desc_in      IN   VARCHAR2 := NULL,
      seq_in       IN   PLS_INTEGER := NULL
   )
   IS
   BEGIN
      ADD (utpackage.id_from_name (package_in), test_in, desc_in, seq_in);
   END;

   PROCEDURE rem (package_in IN INTEGER, test_in IN VARCHAR2)
   IS
    PRAGMA AUTONOMOUS_TRANSACTION; 
   BEGIN
      DELETE FROM ut_test
            WHERE package_id = package_in
              AND name LIKE UPPER (test_in);
   COMMIT; 
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Remove test error: '
                     || SQLERRM);
         ROLLBACK; 
         RAISE;
   END;

   PROCEDURE rem (package_in IN VARCHAR2, test_in IN VARCHAR2)
   IS
   BEGIN
      rem (utpackage.id_from_name (package_in), test_in);
   END;

   PROCEDURE upd (
      package_in      IN   INTEGER,
      test_in         IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN
   )
   IS
     PRAGMA AUTONOMOUS_TRANSACTION; 
      v_failure   PLS_INTEGER := 0;
   BEGIN
      IF NOT successful_in
      THEN
         v_failure := 1;
      END IF;

      UPDATE ut_test
         SET last_start = start_in,
             last_end = end_in,
             executions =   executions
                          + 1,
             failures =   failures
                        + v_failure
       WHERE package_id = package_in
         AND name = UPPER (test_in);
   COMMIT; 
   EXCEPTION
      WHEN OTHERS
      THEN
         utreport.pl (   'Update test error: '
                     || SQLERRM);
        ROLLBACK; 
         RAISE;
   END;

   PROCEDURE upd (
      package_in      IN   VARCHAR2,
      test_in         IN   VARCHAR2,
      start_in             DATE,
      end_in               DATE,
      successful_in        BOOLEAN
   )
   IS
   BEGIN
      upd (
         utpackage.id_from_name (package_in),
         test_in,
         start_in,
         end_in,
         successful_in
      );
   END;
END uttest;
//

CREATE OR REPLACE PACKAGE BODY utunittest
IS

   FUNCTION name (
      ut_in    IN   ut_unittest%ROWTYPE
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN  c_abbrev || utconfig.delimiter || ut_in.id;
   END name;

  FUNCTION name (
      id_in IN ut_unittest.id%TYPE
   )
      RETURN VARCHAR2 is rec ut_unittest%rowtype;
      begin
      rec := onerow (id_in);
      return name (rec); end;

FUNCTION full_name (
      utp_in   IN   ut_utp%ROWTYPE,
      ut_in    IN   ut_unittest%ROWTYPE
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    ututp.qualified_name (utp_in) || '.'
             || name (ut_in.id);
   END full_name;

   FUNCTION onerow (id_in IN ut_unittest.id%TYPE)
      RETURN ut_unittest%ROWTYPE
   IS
      retval   ut_unittest%ROWTYPE;
      empty_rec   ut_unittest%ROWTYPE;
   BEGIN
      SELECT *
        INTO retval
        FROM ut_unittest
       WHERE id = id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN empty_rec;
   END;

   FUNCTION program_name (id_in IN ut_unittest.id%TYPE)
      RETURN ut_unittest.program_name%TYPE
   IS
      retval   ut_unittest.program_name%TYPE;
   BEGIN
      SELECT program_name
        INTO retval
        FROM ut_unittest
       WHERE id = id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
   END;

   FUNCTION id (name_in IN VARCHAR2)
      RETURN ut_unittest.id%TYPE
   IS
      l_delimiter   ut_config.delimiter%TYPE   := utconfig.delimiter;
      l_loc         PLS_INTEGER;
      retval        ut_unittest.id%TYPE;
   BEGIN
      l_loc := INSTR (name_in, l_delimiter);

      IF l_loc = 0
      THEN
         RETURN NULL;
      ELSE
         RETURN to_number (SUBSTR (name_in,   l_loc
                                  + LENGTH (l_delimiter))
                );
      END IF;
      end;

   PROCEDURE ADD (
      utp_id_in        IN   ut_unittest.utp_id%TYPE,
      program_name_in          IN   ut_unittest.program_name%TYPE,
      seq_in           IN   ut_unittest.seq%TYPE := NULL,
      description_in   IN   ut_unittest.description%TYPE
            := NULL
   )
   IS

      PRAGMA autonomous_transaction;

      l_id   ut_unittest.id%TYPE;
   BEGIN
      SELECT ut_unittest_seq.NEXTVAL
        INTO l_id
        FROM DUAL;

      INSERT INTO ut_unittest
                  (id, program_name, seq,
                   description)
           VALUES (l_id, UPPER (program_name_in), seq_in,  description_in);

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN
         IF utrerror.uterrcode = utrerror.assertion_failure
         THEN
         ROLLBACK;
            RAISE;
         ELSE
            ROLLBACK;
            utrerror.report_define_error (
               c_abbrev,
                  'Unittest for '
               || program_name_in
               || ' UTP ID '
               || utp_id_in
            );
         END IF;
   END;

   PROCEDURE rem (
      name_in       IN   varchar2
   )
   IS begin rem (id (name_in));
   END;

   PROCEDURE rem (id_in IN ut_unittest.id%TYPE)
   IS

      PRAGMA autonomous_transaction;

   BEGIN
      DELETE FROM ut_unittest
            WHERE id = id_in;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN
         IF utrerror.uterrcode = utrerror.assertion_failure
         THEN
            ROLLBACK;
            RAISE;
         ELSE
             ROLLBACK;
            utrerror.report_define_error (
               c_abbrev,
                  'Unittest ID '
               || id_in
            );
         END IF;
   END;
END utunittest;
//

CREATE OR REPLACE PACKAGE BODY ututp
IS

   FUNCTION name (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    c_abbrev
             || utconfig.delimiter
             || utp_in.id;
   END;

   FUNCTION name (id_in IN ut_utp.id%TYPE)
      RETURN VARCHAR2
   IS
      rec   ut_utp%ROWTYPE;
   BEGIN
      rec := onerow (id_in);
      RETURN name (rec);
   END;

   FUNCTION qualified_name (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    utp_in.owner
             || '.'
             || name (utp_in);
   END;

   FUNCTION qualified_name (id_in IN ut_utp.id%TYPE)
      RETURN VARCHAR2
   IS
      rec   ut_utp%ROWTYPE;
   BEGIN
      rec := onerow (id_in);
      RETURN qualified_name (rec);
   END;

   FUNCTION setup_procedure (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    utp_in.owner
             || '.'
             || name (utp_in)
             || '.'
             || utplsql.c_setup;
   END;

   FUNCTION teardown_procedure (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    utp_in.owner
             || '.'
             || name (utp_in)
             || '.'
             || utplsql.c_teardown;
   END;

   FUNCTION prefix (utp_in IN ut_utp%ROWTYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN utp_in.prefix;
   END;

   FUNCTION onerow (
      owner_in     IN   ut_utp.owner%TYPE,
      program_in   IN   ut_utp.program%TYPE
   )
      RETURN ut_utp%ROWTYPE
   IS
      retval      ut_utp%ROWTYPE;
      empty_rec   ut_utp%ROWTYPE;
   BEGIN
      SELECT *
        INTO retval
        FROM ut_utp
       WHERE owner = NVL (UPPER (owner_in), USER)
         AND program = UPPER (program_in);
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN empty_rec;
   END;

   PROCEDURE get_onerow (
      owner_in                IN       ut_utp.owner%TYPE
     ,program_in              IN       ut_utp.program%TYPE
     ,id_out                  OUT      ut_utp.ID%TYPE
     ,description_out         OUT      ut_utp.description%TYPE
     ,filename_out            OUT      ut_utp.filename%TYPE
     ,program_directory_out   OUT      ut_utp.program_directory%TYPE
     ,directory_out           OUT      ut_utp.DIRECTORY%TYPE
     ,name_out                OUT      ut_utp.NAME%TYPE
     ,utp_owner_out           OUT      ut_utp.utp_owner%TYPE
     ,prefix_out              OUT      ut_utp.prefix%TYPE
   )
   is
   rec ut_utp%rowtype;
   begin
   rec := onerow (owner_in, program_in);
   id_out := rec.id;
     if rec.id is not null then
description_out   := rec.description;
filename_out    := rec.filename;
program_directory_out  := rec.program_directory;
directory_out    := rec.directory;
name_out     := rec.name;
utp_owner_out   := rec.utp_owner;
prefix_out  := rec.prefix;

   end if;
   end get_onerow;

 FUNCTION EXISTSS (
      owner_in     IN   ut_utp.owner%TYPE,
      program_in   IN   ut_utp.program%TYPE
   )
      RETURN Boolean
   IS
      retval      char(1);

   BEGIN
      SELECT 'x'
        INTO retval
        FROM ut_utp
       WHERE owner = NVL (UPPER (owner_in), USER)
         AND program = UPPER (program_in);
      RETURN true;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN false;
   END;

   FUNCTION onerow (utp_id_in IN ut_utp.id%TYPE)
      RETURN ut_utp%ROWTYPE
   IS
      retval      ut_utp%ROWTYPE;
      empty_rec   ut_utp%ROWTYPE;
   BEGIN
      SELECT *
        INTO retval
        FROM ut_utp
       WHERE id = utp_id_in;
      RETURN retval;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN empty_rec;
   END;

   FUNCTION id (name_in IN VARCHAR2)
      RETURN ut_utp.id%TYPE
   IS
      l_delimiter   ut_config.delimiter%TYPE   := utconfig.delimiter;
      l_loc         PLS_INTEGER;
      retval        ut_utp.id%TYPE;
   BEGIN
      l_loc := INSTR (name_in, l_delimiter);

      IF l_loc = 0
      THEN
         RETURN NULL;
      ELSE
         RETURN to_number (SUBSTR (name_in,   l_loc
                                  + LENGTH (l_delimiter))
                );
      END IF;

   END;

   PROCEDURE ADD (
      program_in        IN   ut_utp.program%TYPE := NULL,
      owner_in          IN   ut_utp.owner%TYPE := NULL,
      description_in    IN   ut_utp.description%TYPE := NULL,
      filename_in       IN   ut_utp.filename%TYPE := NULL,
      frequency_in      IN   ut_utp.frequency%TYPE := NULL,
      program_map_in    IN   ut_utp.program_map%TYPE := NULL,
      declarations_in   IN   ut_utp.declarations%TYPE
            := NULL,
      setup_in          IN   ut_utp.setup%TYPE := NULL,
      teardown_in       IN   ut_utp.teardown%TYPE := NULL,
      exceptions_in       IN   ut_utp.exceptions%TYPE := NULL,
      program_directory_in       IN   ut_utp.program_directory%TYPE := NULL,
      directory_in       IN   ut_utp.directory%TYPE := NULL,
      name_in       IN   ut_utp.name%TYPE := NULL,
      utp_owner_in       IN   ut_utp.utp_owner%TYPE := NULL,
      prefix_in       IN   ut_utp.prefix%TYPE := NULL,
      id_out OUT ut_utp.id%TYPE
   )
   IS

      PRAGMA autonomous_transaction;

      l_id   ut_utp.id%TYPE;
    l_program ut_utp.name%type := program_in;
   BEGIN
   if l_program not like '"%' then l_program := upper (l_program); end if;
      SELECT ut_utp_seq.NEXTVAL
        INTO l_id
        FROM DUAL;

      INSERT INTO ut_utp
                  (id, program,
                   owner, description, filename,
                   frequency, program_map, declarations,
                   setup, teardown, program_directory, directory,
                   name, utp_owner, prefix
                                        )
           VALUES (l_id, l_program,
                   NVL (owner_in, USER), description_in, filename_in,
                   frequency_in, program_map_in, declarations_in,
                   setup_in, teardown_in, program_directory_in, directory_in,
                   name_in, utp_owner_in, prefix_in
                                        );

      COMMIT;

      id_out := l_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         IF utrerror.uterrcode = utrerror.assertion_failure
         THEN
           ROLLBACK;
            RAISE;
         ELSE
             ROLLBACK;
            utrerror.report_define_error (
               c_abbrev,
                  'UTP for '
               || program_in
               || ' Owner '
               || owner_in
            );
         END IF;
   END;

   PROCEDURE ADD (
      program_in        IN   ut_utp.program%TYPE := NULL,
      owner_in          IN   ut_utp.owner%TYPE := NULL,
      description_in    IN   ut_utp.description%TYPE := NULL,
      filename_in       IN   ut_utp.filename%TYPE := NULL,
      frequency_in      IN   ut_utp.frequency%TYPE := NULL,
      program_map_in    IN   ut_utp.program_map%TYPE := NULL,
      declarations_in   IN   ut_utp.declarations%TYPE
            := NULL,
      setup_in          IN   ut_utp.setup%TYPE := NULL,
      teardown_in       IN   ut_utp.teardown%TYPE := NULL,
      exceptions_in       IN   ut_utp.exceptions%TYPE := NULL,
      program_directory_in       IN   ut_utp.program_directory%TYPE := NULL,
      directory_in       IN   ut_utp.directory%TYPE := NULL,
      name_in       IN   ut_utp.name%TYPE := NULL,
      utp_owner_in       IN   ut_utp.utp_owner%TYPE := NULL,
      prefix_in       IN   ut_utp.prefix%TYPE := NULL
   ) is l_id ut_utp.id%TYPE; begin
   add ( program_in,
     owner_in,
      description_in,
       filename_in,
      frequency_in,
     program_map_in,
     declarations_in,
      setup_in,
      teardown_in,
      exceptions_in,
       program_directory_in,
      directory_in,
      name_in,
      utp_owner_in,
      prefix_in,
     l_id);
   end;

   PROCEDURE rem (name_in IN VARCHAR2)
   IS
   BEGIN
      rem (id (name_in));
   END;

   PROCEDURE rem (id_in IN ut_utp.id%TYPE)
   IS

      PRAGMA autonomous_transaction;

   BEGIN
      DELETE FROM ut_utp
            WHERE id = id_in;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN
         IF utrerror.uterrcode = utrerror.assertion_failure
         THEN
             ROLLBACK;
            RAISE;
         ELSE
            ROLLBACK;
            utrerror.report_define_error (c_abbrev,    'UTP '
                                                    || id_in);
         END IF;
   END;

   PROCEDURE upd (
      id_in         IN   ut_utp.id%TYPE,
      program_directory_in       IN   ut_utp.program_directory%TYPE := NULL,
      directory_in       IN   ut_utp.directory%TYPE := NULL,
      name_in       IN   ut_utp.name%TYPE := NULL,
      utp_owner_in       IN   ut_utp.utp_owner%TYPE := NULL,
      filename_in       IN   ut_utp.filename%TYPE := NULL,
      prefix_in       IN   ut_utp.prefix%TYPE := NULL
   )
   IS

      PRAGMA autonomous_transaction;

    l_name ut_utp.name%type := name_in;
   BEGIN
   if l_name not like '"%' then l_name := upper (l_name); end if;
      update ut_utp
        set filename = filename_in,
        program_directory = program_directory_in,
        directory = directory_in,
        name = l_name,
        utp_owner = utp_owner_in,
        prefix = prefix_in
       where id = id_in;

      COMMIT;

   EXCEPTION
      WHEN OTHERS
      THEN
             ROLLBACK;
            utrerror.report_define_error (
               c_abbrev,
                  'UTP update for UTP '
               || id_in
            );
   END;

   FUNCTION utps (program_like_in IN VARCHAR2 := '%')
      RETURN utconfig.refcur_t
   IS
      retval   utconfig.refcur_t;
   BEGIN
      OPEN retval FOR
         SELECT *
           FROM ut_utp
          WHERE program LIKE UPPER (program_like_in);
      RETURN retval;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN retval;
   END;
END ututp;
//

delimiter ;//
