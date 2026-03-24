create or replace PACKAGE dbms_stats AUTHID CURRENT_USER AS

    TYPE numarray IS VARRAY(2050) OF NUMBER;
    TYPE datearray IS VARRAY(2050) OF DATE;
    TYPE chararray IS VARRAY(2050) OF VARCHAR2(4000);
    TYPE rawarray IS VARRAY(2050) OF RAW(2000);
    TYPE fltarray IS VARRAY(2050) OF BINARY_FLOAT;
    TYPE dblarray IS VARRAY(2050) OF BINARY_DOUBLE;

    DEFAULT_METHOD_OPT    constant varchar2(1) := 'Z';
    DEFAULT_GRANULARITY   constant varchar2(1) := 'Z';
    AUTO_SAMPLE_SIZE      constant NUMBER := 0;
    DEFAULT_STAT_CATEGORY constant VARCHAR2(20) := 'OBJECT_STATS';

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
      tabname            VARCHAR2 DEFAULT NULL
    );

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
      degree            NUMBER DEFAULT 1
    );

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
      degree           NUMBER DEFAULT 1
    );

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
      degree           NUMBER DEFAULT 1
    );

    PROCEDURE delete_schema_stats (
      ownname           VARCHAR2,
      stattab           VARCHAR2 DEFAULT NULL,
      statid            VARCHAR2 DEFAULT NULL,
      statown           VARCHAR2 DEFAULT NULL,
      no_invalidate     BOOLEAN DEFAULT FALSE,
      force             BOOLEAN DEFAULT FALSE,
      degree            NUMBER DEFAULT 1
    );

    PROCEDURE FLUSH_DATABASE_MONITORING_INFO;
    PROCEDURE GATHER_DATABASE_STATS_JOB_PROC(duration NUMBER DEFAULT NULL);

    PROCEDURE create_stat_table(
      ownname          VARCHAR2,
      stattab          VARCHAR2,
      tblspace         VARCHAR2 DEFAULT NULL,
      global_temporary BOOLEAN DEFAULT FALSE
    );

    PROCEDURE drop_stat_table(
      ownname VARCHAR2,
      stattab VARCHAR2
    );

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

    PROCEDURE export_column_stats (
      ownname          VARCHAR2,
      tabname          VARCHAR2,
      colname          VARCHAR2,
      partname         VARCHAR2 DEFAULT NULL,
      stattab          VARCHAR2,
      statid           VARCHAR2 DEFAULT NULL,
      statown          VARCHAR2 DEFAULT NULL
    );

    PROCEDURE export_schema_stats (
      ownname          VARCHAR2,
      stattab          VARCHAR2,
      statid           VARCHAR2 DEFAULT NULL,
      statown          VARCHAR2 DEFAULT NULL
    );

    PROCEDURE export_index_stats (
      ownname           VARCHAR2,
      indname           VARCHAR2,
      partname          VARCHAR2 DEFAULT NULL,
      stattab           VARCHAR2,
      statid            VARCHAR2 DEFAULT NULL,
      statown           VARCHAR2 DEFAULT NULL,
      tabname           VARCHAR2  DEFAULT NULL
    );

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

   PROCEDURE import_schema_stats (
      ownname          VARCHAR2,
      stattab          VARCHAR2,
      statid           VARCHAR2 DEFAULT NULL,
      statown          VARCHAR2 DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE
    );

    PROCEDURE import_index_stats (
      ownname          VARCHAR2,
      indname          VARCHAR2,
      partname         VARCHAR2 DEFAULT NULL,
      stattab          VARCHAR2,
      statid           VARCHAR2 DEFAULT NULL,
      statown          VARCHAR2 DEFAULT NULL,
      no_invalidate    BOOLEAN DEFAULT FALSE,
      force            BOOLEAN DEFAULT FALSE,
      tabname          VARCHAR2  DEFAULT NULL
    );

    PROCEDURE lock_table_stats (
      ownname          VARCHAR2,
      tabname          VARCHAR2,
      stattype         VARCHAR2 DEFAULT 'ALL'
    );

    PROCEDURE lock_partition_stats (
      ownname          VARCHAR2,
      tabname          VARCHAR2,
      partname         VARCHAR2
    );

    PROCEDURE lock_schema_stats(
      ownname          VARCHAR2,
      STATTYPE         VARCHAR2 DEFAULT 'ALL'
    );

    PROCEDURE unlock_table_stats (
      ownname          VARCHAR2,
      tabname          VARCHAR2,
      stattype         VARCHAR2 DEFAULT 'ALL'
    );

    PROCEDURE unlock_partition_stats (
      ownname          VARCHAR2,
      tabname          VARCHAR2,
      partname         VARCHAR2
    );

    PROCEDURE unlock_schema_stats(
      ownname          VARCHAR2,
      STATTYPE         VARCHAR2 DEFAULT 'ALL'
    );

    PROCEDURE restore_table_stats (
      ownname               VARCHAR2,
      tabname               VARCHAR2,
      as_of_timestamp       TIMESTAMP WITH TIME ZONE,
      restore_cluster_index BOOLEAN DEFAULT FALSE,
      force                 BOOLEAN DEFAULT FALSE,
      no_invalidate         BOOLEAN DEFAULT FALSE
    );

    PROCEDURE restore_schema_stats (
      ownname               VARCHAR2,
      as_of_timestamp       TIMESTAMP WITH TIME ZONE,
      force                 BOOLEAN DEFAULT FALSE,
      no_invalidate         BOOLEAN DEFAULT FALSE
    );

    PROCEDURE purge_stats(
      before_timestamp      TIMESTAMP WITH TIME ZONE
    );

    PROCEDURE alter_stats_history_retention(
      retention             NUMBER
    );

    FUNCTION get_stats_history_availability RETURN TIMESTAMP WITH TIME ZONE;

    FUNCTION get_stats_history_retention RETURN NUMBER;

    PROCEDURE reset_global_pref_defaults;

    PROCEDURE reset_param_defaults;

    PROCEDURE set_global_prefs(
      pname         VARCHAR2,
      pvalue        VARCHAR2
    );

    PROCEDURE set_param(
      pname          VARCHAR2,
      pval           VARCHAR2
    );

    PROCEDURE set_schema_prefs(
      ownname        VARCHAR2,
      pname          VARCHAR2,
      pvalue         VARCHAR2
    );

    PROCEDURE set_table_prefs(
      ownname        VARCHAR2,
      tabname        VARCHAR2,
      pname          VARCHAR2,
      pvalue         VARCHAR2
    );

    FUNCTION get_prefs (
      pname           VARCHAR2,
      ownname         VARCHAR2 DEFAULT NULL,
      tabname         VARCHAR2 DEFAULT NULL
    ) RETURN VARCHAR2;

    FUNCTION get_param (
      pname           VARCHAR2
    )RETURN VARCHAR2;

    PROCEDURE delete_schema_prefs(
      ownname        VARCHAR2,
      pname          VARCHAR2
    );

    PROCEDURE delete_table_prefs (
      ownname        VARCHAR2,
      tabname        VARCHAR2,
      pname          VARCHAR2
    );

    PROCEDURE copy_table_stats (
      ownname        VARCHAR2,
      tabname        VARCHAR2,
      srcpartname    VARCHAR2,
      dstpartname		 VARCHAR2,
      scale_factor	 NUMBER DEFAULT 1,
      flags					 NUMBER DEFAULT NULL,
      force          BOOLEAN DEFAULT FALSE
    );

    PROCEDURE cancel_gather_stats (
      taskid         VARCHAR2
    );

    PROCEDURE GATHER_SYSTEM_STATS;

    PROCEDURE DELETE_SYSTEM_STATS;

    PROCEDURE SET_SYSTEM_STATS (
      pname          VARCHAR2,
      pvalue         NUMBER
    );

    PROCEDURE async_gather_stats_job_proc (duration NUMBER DEFAULT NULL);
END dbms_stats;
