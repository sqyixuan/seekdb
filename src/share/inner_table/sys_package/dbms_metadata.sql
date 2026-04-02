#package_name:dbms_metadata
#author: guangang.gg
#normalizer: linlin.xll

CREATE OR REPLACE PACKAGE dbms_metadata AUTHID CURRENT_USER AS

  object_not_found EXCEPTION;
    PRAGMA EXCEPTION_INIT(object_not_found, -9503);
    object_not_found_num CONSTANT NUMBER := -9503;

  invalid_argval EXCEPTION;
    PRAGMA EXCEPTION_INIT(invalid_argval, -9504);
    invalid_argval_num CONSTANT NUMBER := -9504;

  FUNCTION get_ddl (
    object_type     VARCHAR,
    name            VARCHAR,
    ob_schema       VARCHAR DEFAULT NULL,
    version         VARCHAR DEFAULT 'COMPATIBLE',
    model           VARCHAR DEFAULT 'ORACLE',
    transform       VARCHAR DEFAULT 'DDL')
  RETURN CLOB;
END DBMS_METADATA;
//
