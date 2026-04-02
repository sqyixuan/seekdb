#package_name:dbms_describe
#author: adou.ly

CREATE OR REPLACE PACKAGE BODY DBMS_DESCRIBE IS
  NLS_NCHAR_CS VARCHAR2(20) := NULL;

  PROCEDURE DESCRIBE_PROCEDURE(OBJECT_NAME    IN   VARCHAR2,
                               RESERVED1      IN   VARCHAR2,
                               RESERVED2      IN   VARCHAR2,
                               OVERLOAD       OUT  NUMBER_TABLE,
                               POSITION       OUT  NUMBER_TABLE,
                               DATALEVEL      OUT  NUMBER_TABLE,
                               ARGUMENT_NAME  OUT  VARCHAR2_TABLE,
                               DATATYPE       OUT  NUMBER_TABLE,
                               DEFAULT_VALUE  OUT  NUMBER_TABLE,
                               IN_OUT         OUT  NUMBER_TABLE,
                               DATALENGTH     OUT  NUMBER_TABLE,
                               DATAPRECISION  OUT  NUMBER_TABLE,
                               SCALE          OUT  NUMBER_TABLE,
                               RADIX          OUT  NUMBER_TABLE,
                               SPARE          OUT  NUMBER_TABLE,
                               INCLUDE_STRING_CONSTRAINTS  BOOLEAN := FALSE)
  IS
    SCHEMA_NAME VARCHAR2(32);
    PART1       VARCHAR2(32);
    PART2       VARCHAR2(32);
    DBLINK      VARCHAR2(128);
    TYPECODE    NUMBER;
    OBJNO       NUMBER;
    I           BINARY_INTEGER := 0;
    FOUND       BOOLEAN := FALSE;
    STATUS      VARCHAR2(10);
    NAME        VARCHAR2(200);
    LINE_CNT    NUMBER;

    OBJECT_NOT_EXIST EXCEPTION;
    PRAGMA EXCEPTION_INIT(OBJECT_NOT_EXIST, -6564);
    PKG_PROC_NOT_EXIST EXCEPTION;
    PRAGMA EXCEPTION_INIT(PKG_PROC_NOT_EXIST, -6563);
  BEGIN
    BEGIN
      DBMS_UTILITY.NAME_RESOLVE(OBJECT_NAME, 1, SCHEMA_NAME, PART1, PART2, DBLINK, TYPECODE, OBJNO);
    EXCEPTION
      WHEN OBJECT_NOT_EXIST OR PKG_PROC_NOT_EXIST THEN RAISE;
      WHEN OTHERS THEN RAISE_APPLICATION_ERROR(-20004, 'syntax error attempting to parse "' || OBJECT_NAME || '"');
    END;

    IF (OBJNO = -1 AND PART2 IS NOT NULL) THEN
      RAISE_APPLICATION_ERROR(-20002, 'ORU-10033: object ' || OBJECT_NAME || ' is remote, cannot describe a procedure in a remote package');
    END IF;

    IF (NLS_NCHAR_CS IS NULL) THEN
      SELECT VALUE INTO NLS_NCHAR_CS FROM SYS.TENANT_VIRTUAL_SESSION_VARIABLE WHERE VARIABLE_NAME = 'nls_nchar_characterset';
    END IF;

    IF (DBLINK IS NOT NULL) THEN
      NAME := SCHEMA_NAME;
      IF (PART1 IS NOT NULL) THEN NAME := NAME || '.' || PART1; END IF;
      IF (PART2 IS NOT NULL) THEN NAME := NAME || '.' || PART2; END IF;
      IF (DBLINK IS NOT NULL) THEN NAME := NAME || '@' || DBLINK; END IF;
      RAISE_APPLICATION_ERROR(-20002, 'ORU-10033: object '
                                      || OBJECT_NAME
                                      || ' is remote, cannot describe; expanded name: '
                                      || NAME);
    ELSE
      IF (PART1 IS NOT NULL AND PART2 IS NULL) THEN
        RAISE_APPLICATION_ERROR(-20000, 'ORU-10035: cannot describe a package ('
                                        || OBJECT_NAME
                                        || '); only a procedure within a package');
      END IF;

      BEGIN
        SELECT STATUS INTO STATUS FROM SYS.ALL_OBJECTS
          WHERE OBJECT_ID = OBJNO AND DECODE(OBJECT_TYPE, 'PROCEDURE', 7, 'FUNCTION', 8, 'PACKAGE', 9, -1) = TYPECODE;
        IF STATUS != 'VALID' THEN
          RAISE_APPLICATION_ERROR(-20003, 'ORU-10036: object '
                                          || OBJECT_NAME
                                          || ' is invalid and cannot be described');
        END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          RAISE_APPLICATION_ERROR(-20003, 'ORU-10036: object '
                                          || OBJECT_NAME
                                          || ' is invalid and cannot be described');
      END;

      IF (TYPECODE = 7 OR TYPECODE = 8) THEN
        FOR REC IN (SELECT ARGUMENT_NAME,
                           NVL(a.OVERLOAD, 0) OVERLOAD,
                           POSITION,
                           t.PARAM_TYPE DATA_TYPE,
                           NVL(CHARACTER_SET_NAME, 0) CHARACTER_SET_NAME,
                           DECODE(CHARACTER_SET_NAME, 'BINARY', 1, 'UTF8MB4', 2, 0) CHARSETFORM,
                           DECODE(DEFAULTED, 'Y', 1, 'N', 0, 0) DEFAULT_VALUE,
                           DECODE(IN_OUT, 'IN', 0, 'OUT', 1, 'INOUT', 2) IN_OUT,
                           NVL(DATA_LEVEL, 0) DATA_LEVEL,
                           NVL(DATA_LENGTH, 0) DATA_LENGTH,
                           NVL(DATA_PRECISION, 0) DATA_PRECISION,
                           NVL(DATA_SCALE, 0) SCALE,
                           0 RADIX,
                           DECODE(t.PARAM_TYPE, 1, DATA_SCALE, 96, DATA_SCALE, 0) CHARLENGTH
                    FROM SYS.ALL_ARGUMENTS a, SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT t
                    WHERE OBJECT_ID = OBJNO AND t.ROUTINE_ID = OBJECT_ID AND t.PARAM_POSITION = a.POSITION
                    ORDER BY a.OBJECT_ID, a.OBJECT_NAME, a.OVERLOAD, a.SEQUENCE)
        LOOP
          I := I + 1;
          ARGUMENT_NAME(I) := REC.ARGUMENT_NAME;
          OVERLOAD(I) := REC.OVERLOAD;
          POSITION(I) := REC.POSITION;
          DATATYPE(I) := REC.DATA_TYPE;
          IF (REC.CHARSETFORM = 2 AND REC.DATA_LENGTH > 0) THEN
            IF (NLS_NCHAR_CS = 'UTF8') THEN
               DATALENGTH(I) := REC.DATA_LENGTH / 3;
            ELSIF (NLS_NCHAR_CS = 'AL16UTF16') THEN
               DATALENGTH(I) := REC.DATA_LENGTH / 2;
            ELSE
               DATALENGTH(I) := REC.DATA_LENGTH;
            END IF;
          ELSE
            DATALENGTH(I) := REC.DATA_LENGTH;
          END IF;
          DEFAULT_VALUE(I) := REC.DEFAULT_VALUE;
          DATALEVEL(I) := REC.DATA_LEVEL;
          IN_OUT(I) := REC.IN_OUT;
          DATAPRECISION(I) := REC.DATA_PRECISION;
          SCALE(I) := REC.SCALE;
          RADIX(I) := REC.RADIX;
          SPARE(I) := REC.CHARLENGTH;
          IF ((NOT INCLUDE_STRING_CONSTRAINTS)
              AND (REC.DATA_TYPE IN (1, 8, 23, 24, 96))
              AND (REC.DATA_LEVEL = 0)) THEN
            DATALENGTH(I) := 0;
            SPARE(I) := 0;
          END IF;
        END LOOP;
      ELSE
        FOR REC IN (SELECT a.ARGUMENT_NAME,
                           NVL(a.OVERLOAD, 0) OVERLOAD,
                           a.POSITION,
                           t.PARAM_TYPE DATA_TYPE,
                           NVL(a.CHARACTER_SET_NAME,0) CHARACTER_SET_NAME,
                           DECODE(a.CHARACTER_SET_NAME, 'BINARY', 1, 'UTF8MB4', 2, 0) CHARSETFORM,
                           DECODE(a.DEFAULTED, 'Y', 1, 'N', 0, 0) DEFAULT_VALUE,
                           DECODE(a.IN_OUT, 'IN', 0, 'OUT', 1, 'INOUT', 2) IN_OUT,
                           NVL(a.DATA_LEVEL, 0) DATA_LEVEL,
                           NVL(a.DATA_LENGTH, 0) DATA_LENGTH,
                           NVL(a.DATA_PRECISION, 0) DATA_PRECISION,
                           NVL(a.DATA_SCALE, 0) SCALE,
                           0 RADIX,
                           DECODE(t.PARAM_TYPE, 1, DATA_SCALE, 96, DATA_SCALE, 0) CHARLENGTH
                    FROM SYS.ALL_ARGUMENTS a, SYS.ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT t,
                         SYS.ALL_VIRTUAL_ROUTINE_REAL_AGENT vr
                    WHERE a.OBJECT_ID = OBJNO AND a.OBJECT_NAME = PART2 AND a.SUBPROGRAM_ID = t.SUBPROGRAM_ID
                          AND a.SEQUENCE = t.SEQUENCE AND a.OBJECT_ID = vr.PACKAGE_ID AND vr.ROUTINE_ID = t.ROUTINE_ID
                    ORDER BY a.OBJECT_ID,a.OBJECT_NAME,a.OVERLOAD,a.SEQUENCE)
        LOOP
          I := I + 1;
          ARGUMENT_NAME(I) := REC.ARGUMENT_NAME;
          OVERLOAD(I) := REC.OVERLOAD;
          POSITION(I) := REC.POSITION;
          DATATYPE(I) := REC.DATA_TYPE;
          IF (REC.CHARSETFORM = 2 AND REC.DATA_LENGTH > 0) THEN
            IF (NLS_NCHAR_CS = 'UTF8') THEN
               DATALENGTH(I) := REC.DATA_LENGTH / 3;
            ELSIF (NLS_NCHAR_CS = 'AL16UTF16') THEN
               DATALENGTH(I) := REC.DATA_LENGTH / 2;
            ELSE
               DATALENGTH(I) := REC.DATA_LENGTH;
            END IF;
          ELSE
            DATALENGTH(I) := REC.DATA_LENGTH;
          END IF;
          DEFAULT_VALUE(I) := REC.DEFAULT_VALUE;
          IN_OUT(I) := REC.IN_OUT;
          DATALEVEL(I) := REC.DATA_LEVEL;
          DATAPRECISION(I) := REC.DATA_PRECISION;
          SCALE(I) := REC.SCALE;
          RADIX(I) := REC.RADIX;
          SPARE(I) := REC.CHARLENGTH;
          IF ((NOT INCLUDE_STRING_CONSTRAINTS)
              AND (REC.DATA_TYPE IN (1, 8, 23, 24, 96))
              AND (REC.DATA_LEVEL = 0)) THEN
            DATALENGTH(I) := 0;
            SPARE(I) := 0;
          END IF;
        END LOOP;

        IF I = 0 THEN
          SELECT COUNT(*) INTO LINE_CNT FROM SYS.ALL_PROCEDURES
            WHERE OBJECT_ID = OBJNO AND OBJECT_NAME = PART1 AND PROCEDURE_NAME = PART2;
          IF LINE_CNT != 1 THEN
            RAISE_APPLICATION_ERROR(-20001, 'ORU-10032: procedure '
                                            || PART2
                                            || ' within package '
                                            || PART1
                                            || ' does not exist');
          ELSE
            ARGUMENT_NAME(1) := NULL;
            OVERLOAD(1)      := 0;
            POSITION(1)      := 1;
            DATATYPE(1)      := 0;
            DATALENGTH(1)    := 0;
            DEFAULT_VALUE(1) := 0;
            IN_OUT(1)        := 0;
            DATALEVEL(1)     := 0;
            DATAPRECISION(1) := 0;
            SCALE(1)         := 0;
            RADIX(1)         := 0;
            SPARE(1)         := 0;
          END IF;
        END IF;
      END IF;
    END IF;
  END;

END DBMS_DESCRIBE;
//
