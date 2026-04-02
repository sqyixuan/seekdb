#package_name:dbms_errlog
#author:zimiao.dkz

CREATE OR REPLACE PACKAGE dbms_errlog AUTHID CURRENT_USER AS

-- for the SESSION or SYSTEM
--  /*
--   * For the following functions, meanings of parameters are:
--   *
--   * 1. base_table_name:
--   *    will create error_log_table depend it
--   *
--   * 2. error_log_table_name:
--   *    name of error_log_table which will be created
--   *    which can be omitted, if error_log_table_name is null
--   *    table name is err$_ + base_table_name
--   */

  --
  -- This API suspends the session for a specified period of time
  --
  PROCEDURE CREATE_ERROR_LOG(DML_TABLE_NAME IN VARCHAR2, 
                            ERR_LOG_TABLE_NAME IN VARCHAR2 DEFAULT NULL,
                            ERR_LOG_TABLE_OWNER IN VARCHAR2 DEFAULT NULL,
                            ERR_LOG_TABLE_SPACE IN VARCHAR2 DEFAULT NULL,
                            SKIP_UNSUPPORTED IN BOOLEAN DEFAULT FALSE);
 
END dbms_errlog;
//
