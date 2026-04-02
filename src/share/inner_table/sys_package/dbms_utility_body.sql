-- package_name:dbms_random_body
-- author:jingxing

CREATE or REPLACE package body dbms_utility AS
  TYPE_UNCL CONSTANT BINARY_INTEGER := 1;
  TYPE_LNAME CONSTANT BINARY_INTEGER := 2;
  MAX_LNAME_LENGTH CONSTANT BINARY_INTEGER := 4000;

  function format_call_stack return varchar2;
    pragma interface (C, format_call_stack);

  function format_error_stack return varchar2;
    pragma interface (C, format_error_stack);

  function format_error_backtrace return varchar2;
    pragma interface (C, format_error_backtrace);

  procedure name_tokenize( NAME IN VARCHAR2, A OUT VARCHAR2, B OUT VARCHAR2, C OUT VARCHAR2,
                           DBLINK OUT VARCHAR2,
                           NEXTPOS OUT BINARY_INTEGER);
    pragma interface (C, name_tokenize);


  PROCEDURE ICD_NAME_RES(NAME IN VARCHAR2, CONTEXT IN NUMBER,
    SCHEMA1 OUT VARCHAR2, PART1 OUT VARCHAR2, PART2 OUT VARCHAR2,
    DBLINK OUT VARCHAR2, PART1_TYPE OUT NUMBER, OBJECT_NUMBER OUT NUMBER);
    pragma interface (C, name_resolve);
    
  PROCEDURE NAME_RESOLVE(NAME IN VARCHAR2, CONTEXT IN NUMBER,
    SCHEMA1 OUT VARCHAR2, PART1 OUT VARCHAR2, PART2 OUT VARCHAR2,
    DBLINK OUT VARCHAR2, PART1_TYPE OUT NUMBER, OBJECT_NUMBER OUT NUMBER) IS
    err_msg VARCHAR(128) := 'ORU-10034: context argument must be integral, 0 to 10';
  BEGIN
    IF (CONTEXT NOT IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) THEN
      RAISE_APPLICATION_ERROR(-20005, err_msg);
    END IF;
    ICD_NAME_RES(NAME, CONTEXT, SCHEMA1, PART1, PART2, DBLINK, PART1_TYPE, OBJECT_NUMBER);
  END;

  FUNCTION ICD_GET_ENDIANNESS RETURN NUMBER;
  PRAGMA INTERFACE (C, GET_ENDIAN);

  FUNCTION GET_ENDIANNESS RETURN NUMBER IS
  BEGIN
    RETURN ICD_GET_ENDIANNESS;
  END;

  PROCEDURE PSDANAM( NAME              IN  VARCHAR2,
                     MAX_LNAME_LENGTH  IN  BINARY_INTEGER,
                     NEXTPOS           OUT BINARY_INTEGER);
    PRAGMA INTERFACE (C, PSDANAM);

  PROCEDURE LNAME_PARSE( NAME              IN  VARCHAR2,
                         MAX_LNAME_LENGTH  IN BINARY_INTEGER,
                         NEXTPOS           OUT BINARY_INTEGER) IS
  BEGIN
    PSDANAM( NAME, MAX_LNAME_LENGTH, NEXTPOS);
  END;

  PROCEDURE ICD_GET_PARAM_VALUE(PARNAM  IN      VARCHAR2,
                      PARTYP  IN OUT  BINARY_INTEGER,
                      INTVAL  IN OUT  BINARY_INTEGER,
                      STRVAL  IN OUT  VARCHAR2,
                      LISTNO  IN      BINARY_INTEGER);
  PRAGMA INTERFACE (C, GET_PARAM_VALUE);

  FUNCTION GET_PARAMETER_VALUE(PARNAM IN     VARCHAR2,
                               INTVAL IN OUT BINARY_INTEGER,
                               STRVAL IN OUT VARCHAR2,
                               LISTNO IN     BINARY_INTEGER DEFAULT 1)
  RETURN BINARY_INTEGER IS
    RET_PARTYP BINARY_INTEGER;
    listno_err_msg VARCHAR2(128) := 'get_parameter_value: listno value must be a positive value';
    null_err_msg VARCHAR2(128) := 'get_parameter_value: input parameter must not be null';
    invalid_err_msg VARCHAR2(128) := 'get_parameter_value: invalid or unsupported parameter';
  BEGIN
    IF (LISTNO IS NULL) OR (LISTNO <= 0) THEN
      RAISE_APPLICATION_ERROR(-20000, listno_err_msg);
    END IF;
    IF (PARNAM IS NULL) THEN
      RAISE_APPLICATION_ERROR(-20000, null_err_msg);
    END IF;
    ICD_GET_PARAM_VALUE(PARNAM, RET_PARTYP, INTVAL, STRVAL, LISTNO);
    IF (INTVAL IS NULL) AND (STRVAL IS NULL) THEN
      RAISE_APPLICATION_ERROR(-20000, invalid_err_msg || ' "' || SUBSTR(PARNAM, 1, 1000) || '"');
    END IF;
    
    RETURN RET_PARTYP;
    EXCEPTION WHEN OTHERS THEN
        RAISE;
  END GET_PARAMETER_VALUE;

  FUNCTION GET_SQL_HASH_I(NAME IN VARCHAR2, HASH OUT RAW,
                          PRE10IHASH OUT NUMBER) RETURN NUMBER;
    PRAGMA INTERFACE (C, GETSQLHASH);

  FUNCTION GET_SQL_HASH(NAME IN VARCHAR2, HASH OUT RAW, 
                        PRE10IHASH OUT NUMBER)
    RETURN NUMBER IS
    RET NUMBER;
  BEGIN
    RET := GET_SQL_HASH_I(NAME, HASH, PRE10IHASH);
    RETURN RET;
  END GET_SQL_HASH;

  FUNCTION RAWS(BIT_OFFSET IN NUMBER) RETURN RAW IS
  BEGIN
    CASE BIT_OFFSET
    WHEN 1 THEN 
      RETURN HEXTORAW('01');
    WHEN 2 THEN
      RETURN HEXTORAW('02');
    WHEN 3 THEN
      RETURN HEXTORAW('04');
    WHEN 4 THEN
      RETURN HEXTORAW('08');
    WHEN 5 THEN
      RETURN HEXTORAW('10');
    WHEN 6 THEN
      RETURN HEXTORAW('20');
    WHEN 7 THEN
      RETURN HEXTORAW('40');
    ELSE
      RETURN HEXTORAW('80');
    END CASE;
  END RAWS;

  FUNCTION BIT_XX(FLAG       IN RAW,
               BYTE       IN NUMBER,
               BIT_OFFSET IN NUMBER) RETURN BOOLEAN IS
  BIT_FLAG RAW(1) := RAWS(BIT_OFFSET);
  BEGIN
    RETURN UTL_RAW.BIT_AND(UTL_RAW.SUBSTR(FLAG, BYTE, 1), BIT_FLAG) = BIT_FLAG;
  END BIT_XX;

  FUNCTION IS_BIT_SET(R IN RAW, N IN NUMBER) 
  RETURN NUMBER IS
  RET NUMBER;
  BEGIN
    IF (BIT_XX(R, 4 - (TRUNC((N - 1) / 8, 0)), MOD((N - 1), 8) + 1)) THEN 
      RET := 1;
    ELSE 
      RET := 0;
    END IF;
    RETURN RET;
  END IS_BIT_SET;

  PROCEDURE ICD_CANAM(NAME              IN  VARCHAR2,
                     CANON_LEN         IN  BINARY_INTEGER,
                     CANON_NAME        OUT VARCHAR2);
    PRAGMA INTERFACE (C, CANONICALIZE);

  PROCEDURE CANONICALIZE(NAME           IN   VARCHAR2,
                         CANON_NAME     OUT  VARCHAR2,
                         CANON_LEN      IN   BINARY_INTEGER) IS
  BEGIN
    IF NAME IS NOT NULL THEN
      ICD_CANAM(NAME, CANON_LEN, CANON_NAME);
    ELSE
      CANON_NAME := NULL;
    END IF;
  END CANONICALIZE;

  PROCEDURE TABLE_TO_COMMA( TAB    IN  UNCL_ARRAY, 
                            TABLEN OUT BINARY_INTEGER,
                            LIST   OUT VARCHAR2) IS
    BUF  VARCHAR2(32512) := '';
    IDX  BINARY_INTEGER  :=  1;
  BEGIN
    IF TAB(IDX) IS NOT NULL THEN
      BUF := TAB(IDX);
      IDX := IDX + 1;
      LOOP
        EXIT WHEN (TAB(IDX) IS NULL);
        BUF := BUF || ',' || TAB(IDX);
        IDX := IDX + 1;
      END LOOP;
    END IF;
    LIST   := BUF;
    TABLEN := IDX - 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      LIST   := BUF;
      TABLEN := IDX - 1;
  END TABLE_TO_COMMA;

  PROCEDURE TABLE_TO_COMMA( TAB    IN  LNAME_ARRAY, 
                            TABLEN OUT BINARY_INTEGER,
                            LIST   OUT VARCHAR2) IS
    BUF  VARCHAR2(32512) := '';
    IDX  BINARY_INTEGER  :=  1;
  BEGIN
    IF TAB(IDX) IS NOT NULL THEN
      BUF := TAB(IDX);
      IDX := IDX + 1;
      LOOP
        EXIT WHEN (TAB(IDX) IS NULL);
        BUF := BUF || ',' || TAB(IDX);
        IDX := IDX + 1;
      END LOOP;
    END IF;
    LIST   := BUF;
    TABLEN := IDX - 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      LIST   := BUF; 
      TABLEN := IDX - 1;
  END TABLE_TO_COMMA;


  PROCEDURE COMMA_TO_TABLE(LIST        IN  VARCHAR2,
                          ARRAY_TYPE  IN  BINARY_INTEGER,
                          TABLEN      OUT BINARY_INTEGER,
                          TAB_U       OUT UNCL_ARRAY,
                          TAB_A       OUT LNAME_ARRAY ) IS
  NEXT_POS    BINARY_INTEGER := 1;
  PRIV_POS    BINARY_INTEGER;
  IDX         BINARY_INTEGER := 1;
  LEN         BINARY_INTEGER;
  temp       VARCHAR2(2000);
  err_msg     VARCHAR2(128) := 'comma-separated list invalid near ';
  BEGIN
    LEN := NVL(LENGTHB(LIST),0);

    LOOP
      PRIV_POS := NEXT_POS;
      IF ARRAY_TYPE = TYPE_UNCL THEN
        DBMS_UTILITY.NAME_TOKENIZE(SUBSTRB(LIST,PRIV_POS), temp, temp, temp, temp, NEXT_POS);
        TAB_U(IDX) := SUBSTRB(LIST, PRIV_POS, NEXT_POS );
      ELSE
        DBMS_UTILITY.LNAME_PARSE( SUBSTRB(LIST,PRIV_POS), MAX_LNAME_LENGTH, NEXT_POS );
        TAB_A(IDX) := SUBSTRB( LIST, PRIV_POS, NEXT_POS );
      END IF;

      NEXT_POS := PRIV_POS + NEXT_POS;
      IF NEXT_POS > LEN THEN
        IDX := IDX + 1;
        EXIT;
      ELSIF SUBSTRB(LIST, NEXT_POS, 1) = ',' THEN
        NEXT_POS := NEXT_POS + 1;
      ELSE
        RAISE_APPLICATION_ERROR( -20001, err_msg || SUBSTRB(LIST,NEXT_POS-2, 5));
      END IF;
      IDX := IDX + 1;
    END LOOP;
 
    IF ARRAY_TYPE = TYPE_UNCL THEN
      TAB_U(IDX) := NULL;
    ELSE
      TAB_A(IDX) := NULL;
    END IF;
    TABLEN := IDX - 1;
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -5963 or SQLCODE = -9714 THEN
        RAISE_APPLICATION_ERROR( -20001, err_msg || SUBSTRB(LIST, PRIV_POS-2, 5));
      ELSE
        RAISE;
      END IF;
  END COMMA_TO_TABLE;

  PROCEDURE COMMA_TO_TABLE( LIST   IN  VARCHAR2,
                            TABLEN OUT BINARY_INTEGER,
                            TAB    OUT UNCL_ARRAY ) IS
    TAB_DUMMY  LNAME_ARRAY;
  BEGIN
    COMMA_TO_TABLE(LIST, TYPE_UNCL, TABLEN, TAB, TAB_DUMMY);
  END COMMA_TO_TABLE;

  PROCEDURE COMMA_TO_TABLE( LIST   IN  VARCHAR2,
                            TABLEN OUT BINARY_INTEGER,
                            TAB    OUT LNAME_ARRAY ) IS
    TAB_DUMMY  UNCL_ARRAY;
  BEGIN
    COMMA_TO_TABLE(LIST, TYPE_LNAME, TABLEN, TAB_DUMMY, TAB);
  END COMMA_TO_TABLE;

  FUNCTION ICD_GET_DBV RETURN VARCHAR2;
  PRAGMA INTERFACE (C,GET_DB_VERSION);

  PROCEDURE DB_VERSION(VERSION       OUT VARCHAR2,
                       COMPATIBILITY OUT VARCHAR2) IS
  BEGIN
    VERSION := ICD_GET_DBV;
    COMPATIBILITY := NULL;
  END DB_VERSION;

  FUNCTION PORT_STRING RETURN VARCHAR2;
  PRAGMA INTERFACE(C, GET_PORT_STRING);

  FUNCTION GET_HASH_VALUE(NAME VARCHAR2, BASE NUMBER, HASH_SIZE NUMBER) RETURN NUMBER;
  PRAGMA INTERFACE(C, GET_HASH_VALUE);

  FUNCTION ICD_GET_TIME RETURN BINARY_INTEGER;
  PRAGMA INTERFACE (C, GET_TIME);

  FUNCTION GET_TIME RETURN NUMBER IS
  BEGIN
    RETURN ICD_GET_TIME;
  END GET_TIME;

  FUNCTION IS_CLUSTER_DATABASE RETURN BOOLEAN IS
  BEGIN
    RETURN TRUE;
  END IS_CLUSTER_DATABASE;

  FUNCTION CURRENT_INSTANCE RETURN NUMBER IS
  CURR_INSTANCE NUMBER;
  BEGIN
    BEGIN
      SELECT INSTANCE_NUMBER INTO CURR_INSTANCE FROM SYS.V$INSTANCE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      RETURN NULL;
    END;
  RETURN CURR_INSTANCE;
  END CURRENT_INSTANCE;

  PROCEDURE ACTIVE_INSTANCES (INSTANCE_TABLE OUT INSTANCE_TABLE,
                            INSTANCE_COUNT OUT NUMBER) AS
  INST_REC INSTANCE_RECORD;
  BEGIN 
    INSTANCE_COUNT := 0;
    FOR REC IN (SELECT INSTANCE_NUMBER, INSTANCE_NAME FROM SYS.GV$INSTANCE ORDER BY INSTANCE_NUMBER, INSTANCE_NAME) LOOP
      INST_REC.INST_NUMBER := REC.INSTANCE_NUMBER;
      INST_REC.INST_NAME := REC.INSTANCE_NAME;
      INSTANCE_COUNT :=INSTANCE_COUNT + 1;
      INSTANCE_TABLE(INSTANCE_COUNT) := INST_REC;
    END LOOP;
  END ACTIVE_INSTANCES;

  FUNCTION OLD_CURRENT_SCHEMA RETURN VARCHAR2 IS
    ocs VARCHAR2(4000);
  BEGIN 
    SELECT sys_context('userenv', 'current_schema') into ocs from dual;
    RETURN ocs;
  END OLD_CURRENT_SCHEMA;
  
  FUNCTION OLD_CURRENT_USER RETURN VARCHAR2 IS
    ocu VARCHAR2(4000);
  BEGIN 
    SELECT sys_context('userenv', 'session_user') into ocu from dual;
    RETURN ocu;
  END OLD_CURRENT_USER;

  PROCEDURE EXEC_DDL_STATEMENT(parse_string IN VARCHAR2) IS
  C BINARY_INTEGER;
  BEGIN
    C := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(C, PARSE_STRING, DBMS_SQL.NATIVE);
    DBMS_SQL.CLOSE_CURSOR(C);
  EXCEPTION WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(C) THEN
        DBMS_SQL.CLOSE_CURSOR(C);
      END IF;
      RAISE;
  END EXEC_DDL_STATEMENT;

  PROCEDURE INNER_VALIDATE(OWNER VARCHAR2, OBJNAME VARCHAR2, OBJTYPE VARCHAR2, IS_RECOMPILE BOOLEAN);
    PRAGMA INTERFACE (C, VALIDATE);

  PROCEDURE VALIDATE(OBJECT_ID NUMBER, IS_RECOMPILE BOOLEAN DEFAULT FALSE) AS
    objowner varchar(100);
    objname varchar(128);
    objtype varchar(50);
    objstatus varchar(100);
    id number;
  begin
    id := object_id;
    select owner, object_name, object_type, status
      into objowner, objname, objtype, objstatus
      from sys.all_objects o where o.object_id = id
        and rownum = 1 order by o.object_id;
    INNER_VALIDATE(objowner, objname, objtype, IS_RECOMPILE);
    exception
      when NO_DATA_FOUND then
        null;
  end; 
  PROCEDURE PL_RECOMPILE;
    PRAGMA INTERFACE (C, PL_RECOMPILE);

  PROCEDURE POLLING_ASK_JOB AS 
    exist_job_num int;
  BEGIN
    select count(*) into exist_job_num from sys.all_virtual_tenant_scheduler_job_real_agent 
      where JOB_NAME = 'RECOMP_PL_OBJ' and job > 0;
    IF exist_job_num =  0 THEN 
    BEGIN 
        dbms_scheduler.create_job(JOB_NAME => 'RECOMP_PL_OBJ',
                                    JOB_TYPE => 'STORED_PROCEDURE',
                                    JOB_ACTION => ' dbms_utility.PL_RECOMPILE',
                                    START_DATE => systimestamp,
                                    END_DATE => systimestamp + 1,
                                    ENABLED => true,
                                    AUTO_DROP => true,
                                    job_class => 'DEFAULT_JOB_CLASS',
                                    MAX_RUN_DURATION => 7200);
    END;                            
    END IF;
  END;

END
//