#package_name:dbms_standard
#author: linlin.xll

CREATE OR REPLACE PACKAGE STANDARD IS

  subtype "DOUBLE PRECISION" is FLOAT;
  subtype DEC is DECIMAL;
  subtype STRING is VARCHAR2(32760);
  subtype LONG is VARCHAR2(32760);
  subtype "CHARACTER VARYING" is VARCHAR(32767);
  subtype "CHAR VARYING" is VARCHAR(32767);
  subtype "CHARACTER LARGE OBJECT" is CLOB;
  subtype "BINARY LARGE OBJECT" is BLOB;
  subtype "CHAR LARGE OBJECT" is CLOB;
  SUBTYPE TIMESTAMP_UNCONSTRAINED IS TIMESTAMP(9);
  SUBTYPE TIMESTAMP_TZ_UNCONSTRAINED IS TIMESTAMP(9) WITH TIME ZONE;
  SUBTYPE YMINTERVAL_UNCONSTRAINED IS INTERVAL YEAR(9) TO MONTH;
  SUBTYPE DSINTERVAL_UNCONSTRAINED IS INTERVAL DAY(9) TO SECOND(9);
  subtype timestamp_ltz_unconstrained is timestamp(9) with local time zone;
  type "<ADT_1>" is record(dummy char(1));
  type "<RECORD_1>" is record(dummy char(1));
  type "<TUPLE_1>" is record(dummy char(1));
  type "<VARRAY_1>" is varray(1) of char(1);
  type "<V2_TABLE_1>" is table of char(1) index by binary_integer;
  type "<TABLE_1>" is table of char(1);
  type "<COLLECTION_1>" is table of char(1);
  type "<REF_CURSOR_1>" is ref cursor;
  type "<TYPED_TABLE>" is table of  "<ADT_1>";
  subtype "<ADT_WITH_OID>" is "<TYPED_TABLE>";
  type " SYS$INT_V2TABLE" is table of integer index by binary_integer;
  type " SYS$BULK_ERROR_RECORD" is
          record(error_index pls_integer, error_code pls_integer);
  type " SYS$REC_V2TABLE" is table of " SYS$BULK_ERROR_RECORD"
                               index by binary_integer;
  type "<ASSOC_ARRAY_1>" is table of char(1) index by varchar2(1);

  CURSOR_ALREADY_OPEN exception;
    pragma EXCEPTION_INIT(CURSOR_ALREADY_OPEN, '-5589');

  DUP_VAL_ON_INDEX exception;
    pragma EXCEPTION_INIT(DUP_VAL_ON_INDEX, '-5024');

  TIMEOUT_ON_RESOURCE exception;
    pragma EXCEPTION_INIT(TIMEOUT_ON_RESOURCE, '-5848');

  INVALID_CURSOR exception;
    pragma EXCEPTION_INIT(INVALID_CURSOR, '-5844');

  NOT_LOGGED_ON exception;
    pragma EXCEPTION_INIT(NOT_LOGGED_ON, '-5846');

  LOGIN_DENIED exception;
    pragma EXCEPTION_INIT(LOGIN_DENIED, '-5845');

  NO_DATA_FOUND exception;
    pragma EXCEPTION_INIT(NO_DATA_FOUND, '-4026');

  ZERO_DIVIDE exception;
    pragma EXCEPTION_INIT(ZERO_DIVIDE, '-4333');

  INVALID_NUMBER exception;
    pragma EXCEPTION_INIT(INVALID_NUMBER, '-5114');

  TOO_MANY_ROWS exception;
    pragma EXCEPTION_INIT(TOO_MANY_ROWS, '-5294');

  STORAGE_ERROR exception;
    pragma EXCEPTION_INIT(STORAGE_ERROR, '-5842');

  PROGRAM_ERROR exception;
    pragma EXCEPTION_INIT(PROGRAM_ERROR, '-5840');

  VALUE_ERROR exception;
    pragma EXCEPTION_INIT(VALUE_ERROR, '-5677');

  ACCESS_INTO_NULL exception;
    pragma EXCEPTION_INIT(ACCESS_INTO_NULL, '-5837');

  COLLECTION_IS_NULL exception;
    pragma EXCEPTION_INIT(COLLECTION_IS_NULL , '-5838');

  SUBSCRIPT_OUTSIDE_LIMIT exception;
    pragma EXCEPTION_INIT(SUBSCRIPT_OUTSIDE_LIMIT,'-5843');

  SUBSCRIPT_BEYOND_COUNT exception;
    pragma EXCEPTION_INIT(SUBSCRIPT_BEYOND_COUNT ,'-5828');

  ROWTYPE_MISMATCH exception;
    pragma EXCEPTION_INIT(ROWTYPE_MISMATCH, '-5841');

  SYS_INVALID_ROWID  EXCEPTION;
    PRAGMA EXCEPTION_INIT(SYS_INVALID_ROWID, '-5802');

  SELF_IS_NULL exception;
    pragma EXCEPTION_INIT(SELF_IS_NULL, '-5847');

  CASE_NOT_FOUND exception;
    pragma EXCEPTION_INIT(CASE_NOT_FOUND, '-5571');

  NO_DATA_NEEDED exception;
    pragma EXCEPTION_INIT(NO_DATA_NEEDED, '-5839');

END;
//
