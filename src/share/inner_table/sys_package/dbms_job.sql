CREATE OR REPLACE PACKAGE dbms_job AUTHID CURRENT_USER IS

  FUNCTION FIND_DATE(INTERVAL IN VARCHAR2) RETURN DATE;
  PROCEDURE PARSE_JOB(WHAT IN VARCHAR2);

  PROCEDURE submit(job       OUT BINARY_INTEGER,
                   what      IN  VARCHAR2,
                   next_date IN  DATE DEFAULT sysdate,
                   interval  IN  VARCHAR2 DEFAULT 'null',
                   no_parse  IN  BOOLEAN DEFAULT FALSE,
                   zone      IN  VARCHAR2 DEFAULT NULL,
                   force     IN  BOOLEAN DEFAULT FALSE );

  PROCEDURE remove (job IN BINARY_INTEGER);

  PROCEDURE change(job       IN BINARY_INTEGER,
                   what      IN VARCHAR2,
                   next_date IN DATE,
                   interval  IN VARCHAR2,
                   zone      IN VARCHAR2 DEFAULT NULL,
                   force     IN BOOLEAN DEFAULT FALSE);

  PROCEDURE what(job  IN  BINARY_INTEGER,
                 what IN  VARCHAR2 );

  PROCEDURE next_date(job       IN BINARY_INTEGER,
                      next_date IN DATE     );

  PROCEDURE zone(job   IN BINARY_INTEGER,
                 zone  IN VARCHAR2,
                 force IN BOOLEAN DEFAULT FALSE);

  PROCEDURE interval(job      IN BINARY_INTEGER,
                     interval IN VARCHAR2 );

  PROCEDURE broken(job       IN  BINARY_INTEGER,
                   broken    IN  BOOLEAN,
                   next_date IN  DATE DEFAULT SYSDATE );

  PROCEDURE run(job   IN  BINARY_INTEGER,
                force IN  BOOLEAN DEFAULT FALSE);

  PROCEDURE user_export(job    IN     BINARY_INTEGER,
                        mycall IN OUT VARCHAR2);
END;
//
