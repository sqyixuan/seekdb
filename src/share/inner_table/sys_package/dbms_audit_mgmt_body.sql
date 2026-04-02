CREATE OR REPLACE PACKAGE BODY dbms_audit_mgmt AS

PROCEDURE drop_purge_job_impl(audit_trail_purge_id IN BINARY_INTEGER,
                              audit_job_name       IN VARCHAR2);
PRAGMA INTERFACE(C, DROP_PURGE_JOB);

PROCEDURE set_last_archive_timestamp_impl(audit_trail_type  IN PLS_INTEGER,
                                          last_archive_time IN TIMESTAMP);
PRAGMA INTERFACE(C, SET_LAST_ARCH_TS);

PROCEDURE clear_last_archive_timestamp_impl(audit_trail_type IN PLS_INTEGER);
PRAGMA INTERFACE(C, DEL_LAST_ARCH_TS);

PROCEDURE clear_last_archive_timestamp(audit_trail_type IN PLS_INTEGER) IS
BEGIN
  IF AUDIT_TRAIL_TYPE != AUDIT_TRAIL_AUD_STD AND
     AUDIT_TRAIL_TYPE != AUDIT_TRAIL_OS
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_TYPE');
  END IF;
  clear_last_archive_timestamp_impl(audit_trail_type);
  COMMIT;
END;

PROCEDURE set_last_archive_timestamp(audit_trail_type  IN PLS_INTEGER,
                                     last_archive_time IN TIMESTAMP) IS
  UTC_TS              TIMESTAMP;
BEGIN
  IF AUDIT_TRAIL_TYPE != AUDIT_TRAIL_AUD_STD AND
     AUDIT_TRAIL_TYPE != AUDIT_TRAIL_OS
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_TYPE');
  END IF;

  IF LAST_ARCHIVE_TIME IS NULL THEN
    RAISE_APPLICATION_ERROR(46250, 'LAST_ARCHIVE_TIME');
  END IF;

  SELECT TO_TIMESTAMP(SYS_EXTRACT_UTC(last_archive_time)) INTO UTC_TS FROM DUAL;
  set_last_archive_timestamp_impl(audit_trail_type, UTC_TS);
  COMMIT;
END;

PROCEDURE clean_audit_trail_impl(audit_trail_type        IN PLS_INTEGER,
                                 use_last_arch_timestamp IN BOOLEAN := TRUE);
PRAGMA INTERFACE(C, CLEAN_AUDIT_TRAIL);

PROCEDURE clean_audit_trail(audit_trail_type        IN PLS_INTEGER,
                            use_last_arch_timestamp IN BOOLEAN := TRUE) IS
  LAST_TS_CNT int         := 0;
  ATRAIL_TYPE PLS_INTEGER := audit_trail_type;
BEGIN
  IF AUDIT_TRAIL_TYPE < AUDIT_TRAIL_AUD_STD OR
     AUDIT_TRAIL_TYPE > AUDIT_TRAIL_ALL THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_TYPE:' || audit_trail_type);
  END IF;

  IF use_last_arch_timestamp = TRUE THEN
    select count(*) into LAST_TS_CNT
    from sys.all_virtual_dam_last_arch_ts_real_agent t where t.audit_trail_type = ATRAIL_TYPE;

    IF LAST_TS_CNT > 1 THEN
      RAISE_APPLICATION_ERROR(46250, 'last_ts_cnt: ' || LAST_TS_CNT || 'trail_type: ' || audit_trail_type);
    ELSIF LAST_TS_CNT = 0 THEN
      NULL;
    ELSE
      clean_audit_trail_impl(audit_trail_type, use_last_arch_timestamp);
    END IF;
  ELSE
    clean_audit_trail_impl(audit_trail_type, use_last_arch_timestamp);
  END IF;
  COMMIT;
END;

PROCEDURE update_clean_job_info_impl(job_name         IN VARCHAR2,
                                     job_id           IN BINARY_INTEGER,
                                     job_status       IN BINARY_INTEGER,
                                     audit_trail_type IN PLS_INTEGER,
                                     job_interval     IN PLS_INTEGER,
                                     job_frequency    IN VARCHAR2,
                                     job_flags        IN BINARY_INTEGER default 0);
PRAGMA INTERFACE(C, UPDATE_JOB_INFO);

PROCEDURE create_purge_job(audit_trail_type           IN PLS_INTEGER,
                           audit_trail_purge_interval IN PLS_INTEGER,
                           audit_trail_purge_name     IN VARCHAR2,
                           use_last_arch_timestamp    IN BOOLEAN := TRUE) IS
  INTERVAL         VARCHAR2(200);
  SQL_STMT         VARCHAR2(1000);
  JOB_CNT          NUMBER := 0;
  TRAIL_COUNT      NUMBER := 0;
  ATRAIL_TYPE      NUMBER := AUDIT_TRAIL_TYPE;
  NEW_JOB_NAME     VARCHAR2(100);
  ZONE_INFO        VARCHAR2(100);
  JOB_ID           BINARY_INTEGER;
  INTERVAL_MIN     PLS_INTEGER;
BEGIN
  IF AUDIT_TRAIL_TYPE < AUDIT_TRAIL_AUD_STD OR
     AUDIT_TRAIL_TYPE > AUDIT_TRAIL_ALL
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_TYPE');
  END IF;

  IF AUDIT_TRAIL_PURGE_NAME IS NULL OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) = 0 OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) > 100
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_PURGE_NAME');
  END IF;

  IF AUDIT_TRAIL_PURGE_INTERVAL <= 0 OR
     AUDIT_TRAIL_PURGE_INTERVAL >= 1000
  THEN
    RAISE_APPLICATION_ERROR(46251, 'AUDIT_TRAIL_PURGE_INTERVAL');
  END IF;

  SELECT COUNT(JOB_NAME) INTO JOB_CNT
  FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
  WHERE NLS_UPPER(JOB_NAME) = NLS_UPPER(NEW_JOB_NAME);

  IF JOB_CNT >= 1 THEN
    RAISE_APPLICATION_ERROR(46254, NEW_JOB_NAME);
  END IF;

  SELECT COUNT(JOB_NAME) INTO TRAIL_COUNT
  FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
  WHERE AUDIT_TRAIL_TYPE = AUDIT_TRAIL_ALL;

  IF TRAIL_COUNT >= 1 THEN
    RAISE_APPLICATION_ERROR(46252, NULL);
  END IF;

  TRAIL_COUNT := 0;
  IF AUDIT_TRAIL_TYPE = AUDIT_TRAIL_AUD_STD OR
     AUDIT_TRAIL_TYPE = AUDIT_TRAIL_FGA_STD
  THEN
    SELECT COUNT(JOB_NAME) INTO TRAIL_COUNT
    FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
    WHERE AUDIT_TRAIL_TYPE = AUDIT_TRAIL_DB_STD;
  ELSIF AUDIT_TRAIL_TYPE = AUDIT_TRAIL_OS OR
        AUDIT_TRAIL_TYPE = AUDIT_TRAIL_XML
  THEN
    SELECT COUNT(JOB_NAME) INTO TRAIL_COUNT
    FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
    WHERE AUDIT_TRAIL_TYPE = AUDIT_TRAIL_FILES;
  ELSIF AUDIT_TRAIL_TYPE = AUDIT_TRAIL_DB_STD
  THEN
    SELECT COUNT(JOB_NAME) INTO TRAIL_COUNT
    FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
    WHERE AUDIT_TRAIL_TYPE = AUDIT_TRAIL_AUD_STD OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_FGA_STD;
  ELSIF AUDIT_TRAIL_TYPE = AUDIT_TRAIL_FILES
  THEN
    SELECT COUNT(JOB_NAME) INTO TRAIL_COUNT
    FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
    WHERE AUDIT_TRAIL_TYPE = AUDIT_TRAIL_OS OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_XML;
  ELSIF AUDIT_TRAIL_TYPE = AUDIT_TRAIL_ALL
  THEN
    SELECT COUNT(JOB_NAME) INTO TRAIL_COUNT
    FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
    WHERE AUDIT_TRAIL_TYPE = AUDIT_TRAIL_OS OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_XML OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_AUD_STD OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_FGA_STD OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_DB_STD OR
          AUDIT_TRAIL_TYPE = AUDIT_TRAIL_FILES;
  END IF;

  IF TRAIL_COUNT >= 1 THEN
    RAISE_APPLICATION_ERROR(46252, NULL);
  END IF;

  IF USE_LAST_ARCH_TIMESTAMP = TRUE THEN
    SQL_STMT := 'BEGIN DBMS_AUDIT_MGMT.CLEAN_AUDIT_TRAIL(' || ATRAIL_TYPE || ', TRUE); END;';
  ELSE
    SQL_STMT := 'BEGIN DBMS_AUDIT_MGMT.CLEAN_AUDIT_TRAIL(' || ATRAIL_TYPE || ', FALSE); END;';
  END IF;

  INTERVAL_MIN := audit_trail_purge_interval * 60;
  INTERVAL := 'SYSDATE + ' || INTERVAL_MIN || '/24/60';
  NEW_JOB_NAME := audit_trail_purge_name;
  IF AUDIT_TRAIL_FILES = audit_trail_type OR AUDIT_TRAIL_ALL = audit_trail_type THEN
    ZONE_INFO := '__ALL_SERVER_BC';
  ELSE
    ZONE_INFO := NULL;
  END IF;
  BEGIN
    DBMS_JOB.SUBMIT(
      JOB_ID,
      SQL_STMT,
      sysdate + INTERVAL_MIN/24/60,
      INTERVAL,
      FALSE,
      ZONE_INFO
    );
  END;
  update_clean_job_info_impl(audit_trail_purge_name,
                             PURGE_JOB_ENABLE,
                             JOB_ID,
                             ATRAIL_TYPE,
                             audit_trail_purge_interval,
                             INTERVAL,
                             0);
  COMMIT;
END;

PROCEDURE set_purge_job_status_impl(audit_trail_purge_name   IN VARCHAR2,
                                    audit_trail_status_value IN PLS_INTEGER);
PRAGMA INTERFACE(C, UPDATE_JOB_STATUS);

PROCEDURE set_purge_job_status(audit_trail_purge_name   IN VARCHAR2,
                               audit_trail_status_value IN PLS_INTEGER) IS
  JOB_CNT NUMBER;
BEGIN
  IF AUDIT_TRAIL_PURGE_NAME IS NULL OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) = 0 OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) > 100
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_PURGE_NAME');
  END IF;
  IF AUDIT_TRAIL_STATUS_VALUE < PURGE_JOB_ENABLE OR
     AUDIT_TRAIL_STATUS_VALUE > PURGE_JOB_DISABLE
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_STATUS_VALUE');
  END IF;

  SELECT COUNT(JOB_NAME) INTO JOB_CNT
  FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
  WHERE NLS_UPPER(JOB_NAME) = NLS_UPPER(audit_trail_purge_name);

  IF JOB_CNT <= 0 THEN
    RAISE_APPLICATION_ERROR(46255, NULL);
  END IF;

  set_purge_job_status_impl(audit_trail_purge_name, audit_trail_status_value);
  COMMIT;
END;

PROCEDURE set_purge_job_interval_impl(audit_trail_purge_name     IN VARCHAR2,
                                      audit_trail_interval_value IN PLS_INTEGER);
PRAGMA INTERFACE(C, UPDATE_JOB_INTERVAL);

PROCEDURE set_purge_job_interval(audit_trail_purge_name     IN VARCHAR2,
                                 audit_trail_interval_value IN PLS_INTEGER) IS
  JOB_CNT NUMBER;
BEGIN
  IF AUDIT_TRAIL_PURGE_NAME IS NULL OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) = 0 OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) > 100
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_PURGE_NAME');
  END IF;
  IF AUDIT_TRAIL_INTERVAL_VALUE <= 0 OR
     AUDIT_TRAIL_INTERVAL_VALUE >= 1000
  THEN
    RAISE_APPLICATION_ERROR(46251, 'AUDIT_TRAIL_INTERVAL_VALUE');
  END IF;
  SELECT COUNT(JOB_NAME) INTO JOB_CNT
  FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
  WHERE NLS_UPPER(JOB_NAME) = NLS_UPPER(audit_trail_purge_name);
  IF JOB_CNT <= 0 THEN
    RAISE_APPLICATION_ERROR(46255, NULL);
  END IF;

  set_purge_job_interval_impl(audit_trail_purge_name, audit_trail_interval_value);
  COMMIT;
END;

PROCEDURE drop_purge_job(audit_trail_purge_name IN VARCHAR2) IS
  JOB_ID  BINARY_INTEGER;
  JOB_CNT NUMBER;
BEGIN
  IF AUDIT_TRAIL_PURGE_NAME IS NULL OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) = 0 OR
     LENGTH(AUDIT_TRAIL_PURGE_NAME) > 100
  THEN
    RAISE_APPLICATION_ERROR(46250, 'AUDIT_TRAIL_PURGE_NAME');
  END IF;
  SELECT COUNT(JOB_NAME) INTO JOB_CNT
  FROM SYS.all_virtual_dam_cleanup_jobs_real_agent
  WHERE NLS_UPPER(JOB_NAME) = NLS_UPPER(audit_trail_purge_name);
  IF JOB_CNT <= 0 THEN
    RAISE_APPLICATION_ERROR(46255, NULL);
  END IF;

  select a.job_id into JOB_ID
      from sys.all_virtual_dam_cleanup_jobs_real_agent a
      where job_name = audit_trail_purge_name;
  drop_purge_job_impl(JOB_ID, audit_trail_purge_name);
  COMMIT;
END;

END;
//
