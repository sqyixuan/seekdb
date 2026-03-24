#package_name: dbms_sys_error
#author: linlin.xll

CREATE OR REPLACE PACKAGE BODY dbms_sys_error IS

  PROCEDURE SYS_ERR1_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR1_IMPL);
  PROCEDURE SYS_ERR0_IMPL(NUM            BINARY_INTEGER,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR0_IMPL);
  PROCEDURE SYS_ERR2_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR2_IMPL);
  PROCEDURE SYS_ERR3_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          ARG3           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR3_IMPL);
  PROCEDURE SYS_ERR4_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          ARG3           VARCHAR2,
                          ARG4           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR4_IMPL);
  PROCEDURE SYS_ERR5_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          ARG3           VARCHAR2,
                          ARG4           VARCHAR2,
                          ARG5           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR5_IMPL);
  PROCEDURE SYS_ERR6_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          ARG3           VARCHAR2,
                          ARG4           VARCHAR2,
                          ARG5           VARCHAR2,
                          ARG6           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR6_IMPL);
  PROCEDURE SYS_ERR7_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          ARG3           VARCHAR2,
                          ARG4           VARCHAR2,
                          ARG5           VARCHAR2,
                          ARG6           VARCHAR2,
                          ARG7           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR7_IMPL);
  PROCEDURE SYS_ERR8_IMPL(NUM            BINARY_INTEGER,
                          ARG1           VARCHAR2,
                          ARG2           VARCHAR2,
                          ARG3           VARCHAR2,
                          ARG4           VARCHAR2,
                          ARG5           VARCHAR2,
                          ARG6           VARCHAR2,
                          ARG7           VARCHAR2,
                          ARG8           VARCHAR2,
                          KEEPERRORSTACK BOOLEAN DEFAULT FALSE);
    PRAGMA INTERFACE(C, SYS_ERR8_IMPL);

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR0_IMPL(NUM, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR1_IMPL(NUM, ARG1, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR2_IMPL(NUM, ARG1, ARG2, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR3_IMPL(NUM, ARG1, ARG2, ARG3, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR4_IMPL(NUM, ARG1, ARG2, ARG3, ARG4, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR5_IMPL(NUM, ARG1, ARG2, ARG3, ARG4, ARG5, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               ARG6           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR6_IMPL(NUM, ARG1, ARG2, ARG3, ARG4, ARG5, ARG6, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               ARG6           VARCHAR2,
                               ARG7           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR7_IMPL(NUM, ARG1, ARG2, ARG3, ARG4, ARG5, ARG6, ARG7, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

  PROCEDURE RAISE_SYSTEM_ERROR(NUM            BINARY_INTEGER,
                               ARG1           VARCHAR2,
                               ARG2           VARCHAR2,
                               ARG3           VARCHAR2,
                               ARG4           VARCHAR2,
                               ARG5           VARCHAR2,
                               ARG6           VARCHAR2,
                               ARG7           VARCHAR2,
                               ARG8           VARCHAR2,
                               KEEPERRORSTACK BOOLEAN DEFAULT FALSE) IS
  BEGIN
    SYS_ERR8_IMPL(NUM, ARG1, ARG2, ARG3, ARG4, ARG5, ARG6, ARG7, ARG8, KEEPERRORSTACK);
  END RAISE_SYSTEM_ERROR;

END;
//