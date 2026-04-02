CREATE OR REPLACE PACKAGE dbms_ijob IS

  FUNCTION  PUSER RETURN VARCHAR2;
  FUNCTION  CLUSER RETURN VARCHAR2;
  FUNCTION  NEXTVALS RETURN BINARY_INTEGER;

  PROCEDURE ZONE_CHECK(ZONE     IN VARCHAR2,
                       FORCE    IN BOOLEAN);

  PROCEDURE SUBMIT(JOB       IN  BINARY_INTEGER,
                   LUSER     IN  VARCHAR2,
                   PUSER     IN  VARCHAR2,
                   CUSER     IN  VARCHAR2,
                   NEXT_DATE IN  DATE,
                   INTERVAL  IN  VARCHAR2,
                   BROKEN    IN  BOOLEAN,
                   WHAT      IN  VARCHAR2,
                   NLSENV    IN  VARCHAR2,
                   ENV       IN  RAW );

  PROCEDURE SET_JOB_AFFINITY(MYNUM IN BINARY_INTEGER,
                             ZONE  IN VARCHAR2);

  PROCEDURE CHECK_PRIVS(JOB IN BINARY_INTEGER);

  PROCEDURE REMOVE(JOB IN BINARY_INTEGER);

  PROCEDURE what(job  IN BINARY_INTEGER,
                 what IN VARCHAR2);

  PROCEDURE next_date(job       IN BINARY_INTEGER,
                      next_date IN DATE);

  PROCEDURE zone(job   IN BINARY_INTEGER,
                 zone  IN VARCHAR2,
                 force IN BOOLEAN DEFAULT FALSE);

  PROCEDURE interval(job      IN BINARY_INTEGER,
                     interval IN VARCHAR2);

  PROCEDURE broken(job       IN BINARY_INTEGER,
                   broken    IN BOOLEAN,
                   next_date IN DATE DEFAULT SYSDATE);

  PROCEDURE run(job   IN BINARY_INTEGER DEFAULT 0,
                force IN BOOLEAN DEFAULT FALSE);

  PROCEDURE user_export(job    IN     BINARY_INTEGER,
                        mycall IN OUT VARCHAR2);

END;
//
