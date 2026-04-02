#package_name:dbms_monitor
#author: xiaoyi.xy
CREATE OR REPLACE PACKAGE BODY "DBMS_MONITOR" IS
  PROCEDURE OB_SESSION_TRACE_ENABLE(SESSION_ID   IN  INT,
                                    LEVEL        IN  INT,
                                    SAMPLE_PCT   IN  NUMBER,
                                    RECORD_POLICY IN VARCHAR2);
  PRAGMA INTERFACE(c, OB_SESSION_TRACE_ENABLE);
  PROCEDURE OB_SESSION_TRACE_DISABLE(session_id   IN  INT);
  PRAGMA INTERFACE(c, OB_SESSION_TRACE_DISABLE);

  PROCEDURE OB_CLIENT_ID_TRACE_ENABLE(CLIENT_ID    IN  VARCHAR2,
                                      LEVEL        IN  INT,
                                      SAMPLE_PCT   IN  NUMBER,
                                      RECORD_POLICY IN VARCHAR2);
  PRAGMA INTERFACE(c, OB_CLIENT_ID_TRACE_ENABLE);
  PROCEDURE OB_CLIENT_ID_TRACE_DISABLE(CLIENT_ID IN  VARCHAR2);
  PRAGMA INTERFACE(c, OB_CLIENT_ID_TRACE_DISABLE);

  PROCEDURE OB_MOD_ACT_TRACE_ENABLE(MODULE_NAME     IN VARCHAR2,
                                    ACTION_NAME     IN VARCHAR2,
                                    LEVEL        IN  INT,
                                    SAMPLE_PCT   IN  NUMBER,
                                    RECORD_POLICY IN VARCHAR2);
  PRAGMA INTERFACE(c, OB_MOD_ACT_TRACE_ENABLE);
  PROCEDURE OB_MOD_ACT_TRACE_DISABLE(MODULE_NAME     IN  VARCHAR2,
                                     ACTION_NAME     IN  VARCHAR2);
  PRAGMA INTERFACE(c, OB_MOD_ACT_TRACE_DISABLE);

  PROCEDURE OB_TENANT_TRACE_ENABLE(LEVEL        IN  INT,
                                   SAMPLE_PCT   IN  NUMBER,
                                   RECORD_POLICY IN VARCHAR2);
  PRAGMA INTERFACE(c, OB_TENANT_TRACE_ENABLE);
  PROCEDURE OB_TENANT_TRACE_DISABLE(tenant_name  IN VARCHAR2 DEFAULT NULL);
  PRAGMA INTERFACE(c, OB_TENANT_TRACE_DISABLE);
END;


//

