#package_name:dbms_describe
#author: adou.ly

CREATE OR REPLACE PACKAGE dbms_describe IS

  TYPE VARCHAR2_TABLE IS TABLE OF VARCHAR2(32) INDEX BY BINARY_INTEGER;
  TYPE NUMBER_TABLE   IS TABLE OF NUMBER       INDEX BY BINARY_INTEGER;

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
                               INCLUDE_STRING_CONSTRAINTS  BOOLEAN := FALSE);
END;
//
