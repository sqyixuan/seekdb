CREATE OR REPLACE PACKAGE dbms_audit_mgmt AS
  AUDIT_TRAIL_AUD_STD    CONSTANT NUMBER := 1;
  AUDIT_TRAIL_FGA_STD    CONSTANT NUMBER := 2;
  AUDIT_TRAIL_DB_STD     CONSTANT NUMBER := 3;
  AUDIT_TRAIL_OS         CONSTANT NUMBER := 4;
  AUDIT_TRAIL_XML        CONSTANT NUMBER := 8;
  AUDIT_TRAIL_FILES      CONSTANT NUMBER := 12;
  AUDIT_TRAIL_ALL        CONSTANT NUMBER := 15;

  OS_FILE_MAX_SIZE       CONSTANT NUMBER := 16;
  OS_FILE_MAX_AGE        CONSTANT NUMBER := 17;

  CLEAN_UP_INTERVAL      CONSTANT NUMBER := 21;
  DB_AUDIT_TABLEPSACE    CONSTANT NUMBER := 22;
  DB_DELETE_BATCH_SIZE   CONSTANT NUMBER := 23;
  TRACE_LEVEL            CONSTANT NUMBER := 24;

  AUD_TAB_MOVEMENT_FLAG  CONSTANT NUMBER := 25;
  FILE_DELETE_BATCH_SIZE CONSTANT NUMBER := 26;

  PURGE_JOB_ENABLE       CONSTANT NUMBER := 31;
  PURGE_JOB_DISABLE      CONSTANT NUMBER := 32;

  TRACE_LEVEL_DEBUG      CONSTANT PLS_INTEGER := 1;
  TRACE_LEVEL_ERROR      CONSTANT PLS_INTEGER := 2;

  PROCEDURE set_last_archive_timestamp(audit_trail_type  IN PLS_INTEGER,
                                       last_archive_time IN TIMESTAMP);

  PROCEDURE clear_last_archive_timestamp(audit_trail_type IN PLS_INTEGER);

  PROCEDURE clean_audit_trail(audit_trail_type        IN PLS_INTEGER,
                              use_last_arch_timestamp IN BOOLEAN := TRUE);

  PROCEDURE create_purge_job(audit_trail_type           IN PLS_INTEGER,
                             audit_trail_purge_interval IN PLS_INTEGER,
                             audit_trail_purge_name     IN VARCHAR2,
                             use_last_arch_timestamp    IN BOOLEAN := TRUE);

  PROCEDURE set_purge_job_status(audit_trail_purge_name   IN VARCHAR2,
                                 audit_trail_status_value IN PLS_INTEGER);

  PROCEDURE set_purge_job_interval(audit_trail_purge_name     IN VARCHAR2,
                                   audit_trail_interval_value IN PLS_INTEGER);

  PROCEDURE drop_purge_job(audit_trail_purge_name IN VARCHAR2);

END dbms_audit_mgmt;
//
