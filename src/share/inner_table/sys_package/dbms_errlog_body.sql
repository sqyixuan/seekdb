#package_name:dbms_errlog
#author:zimiao.dkz

CREATE OR replace PACKAGE BODY dbms_errlog AS
  PROCEDURE CREATE_ERROR_LOG(DML_TABLE_NAME IN VARCHAR2, 
                            ERR_LOG_TABLE_NAME IN VARCHAR2 DEFAULT NULL,
                            ERR_LOG_TABLE_OWNER IN VARCHAR2 DEFAULT NULL,
                            ERR_LOG_TABLE_SPACE IN VARCHAR2 DEFAULT NULL,
                            SKIP_UNSUPPORTED IN BOOLEAN DEFAULT FALSE);
  pragma interface (C, CREATE_ERROR_LOG);
END dbms_errlog;
//
