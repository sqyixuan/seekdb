#package_name: dbms_sys_error
#author: linlin.xll

CREATE OR REPLACE PACKAGE dbms_sys_error IS
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               ARG6           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               ARG6           VARCHAR2,
                               ARG7           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               ARG6           VARCHAR2,
                               ARG7           VARCHAR2,
                               ARG8           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(RAISE_SYSTEM_ERROR,WNDS,RNDS,WNPS,RNPS);
END;
//