# package_name : dbms_stat
# author : link.zt

CREATE OR REPLACE PACKAGE BODY dbms_stats AS
  PROCEDURE gather_table_stats (
    ownname            VARCHAR2,
    tabname            VARCHAR2,
    partname           VARCHAR2 DEFAULT NULL,
    estimate_percent   NUMBER DEFAULT AUTO_SAMPLE_SIZE,
    block_sample       BOOLEAN DEFAULT NULL,
    method_opt         VARCHAR2 DEFAULT DEFAULT_METHOD_OPT,
    degree             NUMBER DEFAULT NULL,
    granularity        VARCHAR2 DEFAULT DEFAULT_GRANULARITY,
    cascade            BOOLEAN DEFAULT NULL,
    stattab            VARCHAR2 DEFAULT NULL,
    statid             VARCHAR2 DEFAULT NULL,
    statown            VARCHAR2 DEFAULT NULL,
    no_invalidate      BOOLEAN DEFAULT FALSE,
    stattype           VARCHAR2 DEFAULT 'DATA',
    force              BOOLEAN DEFAULT FALSE,
    hist_est_percent   NUMBER DEFAULT AUTO_SAMPLE_SIZE,
    hist_block_sample  BOOLEAN DEFAULT NULL
  );
  PRAGMA INTERFACE(C, GATHER_TABLE_STATS);

  PROCEDURE gather_schema_stats (
    ownname            VARCHAR2,
    estimate_percent   NUMBER DEFAULT AUTO_SAMPLE_SIZE,
    block_sample       BOOLEAN DEFAULT NULL,
    method_opt         VARCHAR2 DEFAULT DEFAULT_METHOD_OPT,
    degree             NUMBER DEFAULT NULL,
    granularity        VARCHAR2 DEFAULT DEFAULT_GRANULARITY,
    cascade            BOOLEAN DEFAULT NULL,
    stattab            VARCHAR2 DEFAULT NULL,
    statid             VARCHAR2 DEFAULT NULL,
    statown            VARCHAR2 DEFAULT NULL,
    no_invalidate      BOOLEAN DEFAULT FALSE,
    stattype           VARCHAR2 DEFAULT 'DATA',
    force              BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, GATHER_SCHEMA_STATS);

  PROCEDURE gather_index_stats (
    ownname            VARCHAR2,
    indname            VARCHAR2,
    partname           VARCHAR2 DEFAULT NULL,
    estimate_percent   NUMBER DEFAULT AUTO_SAMPLE_SIZE,
    stattab            VARCHAR2 DEFAULT NULL,
    statid             VARCHAR2 DEFAULT NULL,
    statown            VARCHAR2 DEFAULT NULL,
    degree             NUMBER DEFAULT NULL,
    granularity        VARCHAR2 DEFAULT DEFAULT_GRANULARITY,
    no_invalidate      BOOLEAN DEFAULT FALSE,
    force              BOOLEAN DEFAULT FALSE,
    tabname            VARCHAR2  DEFAULT NULL
  );
  PRAGMA INTERFACE(C, GATHER_INDEX_STATS);

  PROCEDURE set_table_stats (
    ownname            VARCHAR2,
    tabname            VARCHAR2,
    partname           VARCHAR2 DEFAULT NULL,
    stattab            VARCHAR2 DEFAULT NULL,
    statid             VARCHAR2 DEFAULT NULL,
    numrows            NUMBER DEFAULT NULL,
    numblks            NUMBER DEFAULT NULL,
    avgrlen            NUMBER DEFAULT NULL,
    flags              NUMBER DEFAULT NULL,
    statown            VARCHAR2 DEFAULT NULL,
    no_invalidate      BOOLEAN DEFAULT FALSE,
    cachedblk          NUMBER DEFAULT NULL,
    cachehit           NUMBER DEFAULT NULL,
    force              BOOLEAN DEFAULT FALSE,
    nummacroblks       NUMBER DEFAULT NULL,
    nummicroblks       NUMBER DEFAULT NULL
  );
  PRAGMA INTERFACE(C, SET_TABLE_STATS);

  PROCEDURE set_column_stats (
    ownname            VARCHAR2,
    tabname            VARCHAR2,
    colname            VARCHAR2,
    partname           VARCHAR2 DEFAULT NULL,
    stattab            VARCHAR2 DEFAULT NULL,
    statid             VARCHAR2 DEFAULT NULL,
    distcnt            NUMBER   DEFAULT NULL,
    density            NUMBER   DEFAULT NULL,
    nullcnt            NUMBER   DEFAULT NULL,
    epc                NUMBER   DEFAULT NULL,
    minval             VARCHAR2 DEFAULT NULL,
    maxval             VARCHAR2 DEFAULT NULL,
    bkvals             NUMARRAY  DEFAULT NULL,
    novals             NUMARRAY  DEFAULT NULL,
    chvals             CHARARRAY DEFAULT NULL,
    eavals             RAWARRAY  DEFAULT NULL,
    rpcnts             NUMARRAY  DEFAULT NULL,
    eavs               NUMBER   DEFAULT NULL,
    avgclen            NUMBER   DEFAULT NULL,
    flags              NUMBER   DEFAULT NULL,
    statown            VARCHAR2 DEFAULT NULL,
    no_invalidate      BOOLEAN DEFAULT FALSE,
    force              BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, SET_COLUMN_STATS);

  PROCEDURE set_index_stats (
    ownname            VARCHAR2,
    indname            VARCHAR2,
    partname           VARCHAR2  DEFAULT NULL,
    stattab            VARCHAR2  DEFAULT NULL,
    statid             VARCHAR2  DEFAULT NULL,
    numrows            NUMBER    DEFAULT NULL,
    numlblks           NUMBER    DEFAULT NULL,
    numdist            NUMBER    DEFAULT NULL,
    avglblk            NUMBER    DEFAULT NULL,
    avgdblk            NUMBER    DEFAULT NULL,
    clstfct            NUMBER    DEFAULT NULL,
    indlevel           NUMBER    DEFAULT NULL,
    flags              NUMBER    DEFAULT NULL,
    statown            VARCHAR2  DEFAULT NULL,
    no_invalidate      BOOLEAN   DEFAULT FALSE,
    guessq             NUMBER    DEFAULT NULL,
    cachedblk          NUMBER    DEFAULT NULL,
    cachehit           NUMBER    DEFAULT NULL,
    force              BOOLEAN   DEFAULT FALSE,
    avgrlen            NUMBER    DEFAULT NULL,
    nummacroblks       NUMBER    DEFAULT NULL,
    nummicroblks       NUMBER    DEFAULT NULL,
    tabname            VARCHAR2  DEFAULT NULL
  );
  PRAGMA INTERFACE(C, SET_INDEX_STATS);

  PROCEDURE delete_table_stats (
    ownname           VARCHAR2,
    tabname           VARCHAR2,
    partname          VARCHAR2 DEFAULT NULL,
    stattab           VARCHAR2 DEFAULT NULL,
    statid            VARCHAR2 DEFAULT NULL,
    cascade_parts     BOOLEAN DEFAULT TRUE,
    cascade_columns   BOOLEAN DEFAULT TRUE,
    cascade_indexes   BOOLEAN DEFAULT TRUE,
    statown           VARCHAR2 DEFAULT NULL,
    no_invalidate     BOOLEAN DEFAULT FALSE,
    force             BOOLEAN DEFAULT FALSE,
    degree            DECIMAL DEFAULT 1
  );
  PRAGMA INTERFACE(C, DELETE_TABLE_STATS);

  PROCEDURE delete_column_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    colname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2 DEFAULT NULL,
    statid           VARCHAR2 DEFAULT NULL,
    cascade_parts    BOOLEAN DEFAULT TRUE,
    statown          VARCHAR2 DEFAULT NULL,
    no_invalidate    BOOLEAN DEFAULT FALSE,
    force            BOOLEAN DEFAULT FALSE,
    col_stat_type    VARCHAR2 DEFAULT 'ALL',
    degree           DECIMAL DEFAULT 1
  );
  PRAGMA INTERFACE(C, DELETE_COLUMN_STATS);

  PROCEDURE delete_schema_stats (
    ownname           VARCHAR2,
    stattab           VARCHAR2 DEFAULT NULL,
    statid            VARCHAR2 DEFAULT NULL,
    statown           VARCHAR2 DEFAULT NULL,
    no_invalidate     BOOLEAN DEFAULT FALSE,
    force             BOOLEAN DEFAULT FALSE,
    degree            DECIMAL DEFAULT 1
  );
  PRAGMA INTERFACE(C, DELETE_SCHEMA_STATS);

  procedure delete_index_stats(
    ownname          VARCHAR2,
    indname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2 DEFAULT NULL,
    statid           VARCHAR2 DEFAULT NULL,
    cascade_parts    BOOLEAN  DEFAULT TRUE,
    statown          VARCHAR2 DEFAULT NULL,
    no_invalidate    BOOLEAN  DEFAULT FALSE,
    stattype         VARCHAR2 DEFAULT 'ALL',
    force            BOOLEAN  DEFAULT FALSE,
    tabname          VARCHAR2 DEFAULT NULL,
    degree           DECIMAL DEFAULT 1
  );
  PRAGMA INTERFACE(C, DELETE_INDEX_STATS);

  PROCEDURE FLUSH_DATABASE_MONITORING_INFO;
  PRAGMA INTERFACE(C, FLUSH_DATABASE_MONITORING_INFO);

  PROCEDURE GATHER_DATABASE_STATS_JOB_PROC(duration NUMBER DEFAULT NULL);
  PRAGMA INTERFACE(C, GATHER_DATABASE_STATS_JOB_PROC);

  PROCEDURE create_stat_table(
    ownname          VARCHAR2,
    stattab          VARCHAR2,
    tblspace         VARCHAR2 DEFAULT NULL,
    global_temporary BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, CREATE_STAT_TABLE);

  PROCEDURE drop_stat_table(
    ownname          VARCHAR2,
    stattab          VARCHAR2
  );
  PRAGMA INTERFACE(C, DROP_STAT_TABLE);

  PROCEDURE export_table_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    cascade          BOOLEAN DEFAULT TRUE,
    statown          VARCHAR2 DEFAULT NULL,
    stat_category    VARCHAR2 DEFAULT DEFAULT_STAT_CATEGORY
  );
  PRAGMA INTERFACE(C, EXPORT_TABLE_STATS);

  PROCEDURE export_column_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    colname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    statown          VARCHAR2 DEFAULT NULL
  );
  PRAGMA INTERFACE(C, EXPORT_COLUMN_STATS);

  PROCEDURE export_schema_stats (
    ownname          VARCHAR2,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    statown          VARCHAR2 DEFAULT NULL
  );
  PRAGMA INTERFACE(C, EXPORT_SCHEMA_STATS);

  PROCEDURE export_index_stats (
    ownname           VARCHAR2,
    indname           VARCHAR2,
    partname          VARCHAR2 DEFAULT NULL,
    stattab           VARCHAR2,
    statid            VARCHAR2 DEFAULT NULL,
    statown           VARCHAR2 DEFAULT NULL,
    tabname           VARCHAR2 DEFAULT NULL
  );
  PRAGMA INTERFACE(C, EXPORT_INDEX_STATS);

  PROCEDURE import_table_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    cascade          BOOLEAN DEFAULT TRUE,
    statown          VARCHAR2 DEFAULT NULL,
    no_invalidate    BOOLEAN DEFAULT FALSE,
    force            BOOLEAN DEFAULT FALSE,
    stat_category    VARCHAR2 DEFAULT DEFAULT_STAT_CATEGORY
  );
  PRAGMA INTERFACE(C, IMPORT_TABLE_STATS);

  PROCEDURE import_column_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    colname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    statown          VARCHAR2 DEFAULT NULL,
    no_invalidate    BOOLEAN DEFAULT FALSE,
    force            BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, IMPORT_COLUMN_STATS);

  PROCEDURE import_schema_stats (
    ownname          VARCHAR2,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    statown          VARCHAR2 DEFAULT NULL,
    no_invalidate    BOOLEAN DEFAULT FALSE,
    force            BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, IMPORT_SCHEMA_STATS);

  PROCEDURE import_index_stats (
    ownname          VARCHAR2,
    indname          VARCHAR2,
    partname         VARCHAR2 DEFAULT NULL,
    stattab          VARCHAR2,
    statid           VARCHAR2 DEFAULT NULL,
    statown          VARCHAR2 DEFAULT NULL,
    no_invalidate    BOOLEAN DEFAULT FALSE,
    force            BOOLEAN DEFAULT FALSE,
    tabname          VARCHAR DEFAULT NULL
  );
  PRAGMA INTERFACE(C, IMPORT_INDEX_STATS);

  PROCEDURE lock_table_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    stattype         VARCHAR2 DEFAULT 'ALL'
  );
  PRAGMA INTERFACE(C, LOCK_TABLE_STATS);

  PROCEDURE lock_partition_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    partname         VARCHAR2
  );
  PRAGMA INTERFACE(C, LOCK_PARTITION_STATS);

  PROCEDURE lock_schema_stats(
    ownname          VARCHAR2,
    STATTYPE         VARCHAR2 DEFAULT 'ALL'
  );
  PRAGMA INTERFACE(C, LOCK_SCHEMA_STATS);

  PROCEDURE unlock_table_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    stattype         VARCHAR2 DEFAULT 'ALL'
  );
  PRAGMA INTERFACE(C, UNLOCK_TABLE_STATS);

  PROCEDURE unlock_partition_stats (
    ownname          VARCHAR2,
    tabname          VARCHAR2,
    partname         VARCHAR2
  );
  PRAGMA INTERFACE(C, UNLOCK_PARTITION_STATS);

  PROCEDURE unlock_schema_stats(
    ownname          VARCHAR2,
    STATTYPE         VARCHAR2 DEFAULT 'ALL'
  );
  PRAGMA INTERFACE(C, UNLOCK_SCHEMA_STATS);

  PROCEDURE restore_table_stats (
    ownname               VARCHAR2,
    tabname               VARCHAR2,
    as_of_timestamp       TIMESTAMP WITH TIME ZONE,
    restore_cluster_index BOOLEAN DEFAULT FALSE,
    force                 BOOLEAN DEFAULT FALSE,
    no_invalidate         BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, RESTORE_TABLE_STATS);

  PROCEDURE restore_schema_stats (
    ownname               VARCHAR2,
    as_of_timestamp       TIMESTAMP WITH TIME ZONE,
    force                 BOOLEAN DEFAULT FALSE,
    no_invalidate         BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, RESTORE_SCHEMA_STATS);

  PROCEDURE purge_stats(
    before_timestamp      TIMESTAMP WITH TIME ZONE
  );
  PRAGMA INTERFACE(C, PURGE_STATS);

  PROCEDURE alter_stats_history_retention(
    retention             NUMBER
  );
  PRAGMA INTERFACE(C, ALTER_STATS_HISTORY_RETENTION);

  FUNCTION get_stats_history_availability RETURN TIMESTAMP WITH TIME ZONE;
  PRAGMA INTERFACE(C, GET_STATS_HISTORY_AVAILABILITY);

  FUNCTION get_stats_history_retention RETURN NUMBER;
  PRAGMA INTERFACE(C, GET_STATS_HISTORY_RETENTION);

  PROCEDURE reset_global_pref_defaults;
  PRAGMA INTERFACE(C, RESET_GLOBAL_PREF_DEFAULTS);

  PROCEDURE reset_param_defaults IS
  BEGIN
    reset_global_pref_defaults();
  END;

  PROCEDURE set_global_prefs(
    pname           VARCHAR2,
    pvalue          VARCHAR2
  );
  PRAGMA INTERFACE(C, SET_GLOBAL_PREFS);

  PROCEDURE set_param(pname VARCHAR2, pval VARCHAR2) IS
  BEGIN
    set_global_prefs(pname, pval);
  END;

  PROCEDURE set_schema_prefs(
    ownname         VARCHAR2,
    pname           VARCHAR2,
    pvalue          VARCHAR2
  );
  PRAGMA INTERFACE(C, SET_SCHEMA_PREFS);

  PROCEDURE set_table_prefs(
    ownname         VARCHAR2,
    tabname         VARCHAR2,
    pname           VARCHAR2,
    pvalue          VARCHAR2
  );
  PRAGMA INTERFACE(C, SET_TABLE_PREFS);

  FUNCTION get_prefs (
    pname           VARCHAR2,
    ownname         VARCHAR2 DEFAULT NULL,
    tabname         VARCHAR2 DEFAULT NULL
  ) RETURN VARCHAR2;
  PRAGMA INTERFACE(C, GET_PREFS);

  FUNCTION get_param(pname VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    RETURN get_prefs(pname);
  END;

  PROCEDURE delete_schema_prefs(
    ownname        VARCHAR2,
    pname          VARCHAR2
  );
  PRAGMA INTERFACE(C, DELETE_SCHEMA_PREFS);

  PROCEDURE delete_table_prefs (
    ownname        VARCHAR2,
    tabname        VARCHAR2,
    pname          VARCHAR2
  );
  PRAGMA INTERFACE(C, DELETE_TABLE_PREFS);

  PROCEDURE copy_table_stats (
    ownname        VARCHAR2,
    tabname        VARCHAR2,
    srcpartname    VARCHAR2,
    dstpartname		 VARCHAR2,
    scale_factor	 NUMBER DEFAULT 1,
    flags					 NUMBER DEFAULT NULL,
    force          BOOLEAN DEFAULT FALSE
  );
  PRAGMA INTERFACE(C, COPY_TABLE_STATS);

  PROCEDURE cancel_gather_stats (
    taskid         VARCHAR2
  );
  PRAGMA INTERFACE(C, CANCEL_GATHER_STATS);

  PROCEDURE GATHER_SYSTEM_STATS;
  PRAGMA INTERFACE(C, GATHER_SYSTEM_STATS);

  PROCEDURE DELETE_SYSTEM_STATS;
  PRAGMA INTERFACE(C, DELETE_SYSTEM_STATS);

  PROCEDURE SET_SYSTEM_STATS (
    pname          VARCHAR2,
    pvalue         NUMBER
  );
  PRAGMA INTERFACE(C, SET_SYSTEM_STATS);

  PROCEDURE async_gather_stats_job_proc (duration NUMBER DEFAULT NULL);
  PRAGMA INTERFACE(C, ASYNC_GATHER_STATS_JOB_PROC);
END dbms_stats;
