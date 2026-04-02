#describe:oracle sys type
#author:webber.wb

CREATE OR REPLACE TYPE ODCIColInfo OID '300006' AS object
(
  TableSchema          VARCHAR2(128),
  TableName            VARCHAR2(128),
  ColName              VARCHAR2(4000),
  ColTypeName          VARCHAR2(128),
  ColTypeSchema        VARCHAR2(128),
  TablePartition       VARCHAR2(128),
  ColInfoFlags         NUMBER,
  OrderByPosition      NUMBER,
  TablePartitionIden   NUMBER,
  TablePartitionTotal  NUMBER
);
//

CREATE OR REPLACE TYPE ODCIPartInfo OID '300007' AS object
(
  TablePartition      VARCHAR2(128),
  IndexPartition      VARCHAR2(128),
  IndexPartitionIden  NUMBER,
  PartOp              NUMBER
);
//

CREATE OR REPLACE TYPE ODCIPredInfo OID '300008' AS object
(
  ObjectSchema    VARCHAR2(128),
  ObjectName      VARCHAR2(128),
  MethodName      VARCHAR2(128),
  Flags           NUMBER
);
//

CREATE OR REPLACE TYPE ODCIRidList OID '300009'
 AS VARRAY(32767) OF VARCHAR2(5072);
//

CREATE OR REPLACE TYPE ODCINumberList OID '300010'
 AS VARRAY(32767) OF NUMBER;
//

CREATE OR REPLACE TYPE ODCIVarchar2List OID '300011'
 AS VARRAY(32767) OF VARCHAR2(4000);
//

CREATE OR REPLACE TYPE ODCIDateList OID '300012'
 AS VARRAY(32767) OF Date;
//

CREATE OR REPLACE TYPE ODCIObject OID '300013' AS object
(
  ObjectSchema VARCHAR2(128),
  ObjectName   VARCHAR2(128)
);
//

CREATE OR REPLACE TYPE ODCIOrderByInfo OID '300014' AS OBJECT
(
  ExprType          NUMBER,
  ObjectSchema      VARCHAR2(128),
  TableName         VARCHAR2(128),
  ExprName          VARCHAR2(128),
  SortOrder         NUMBER
);
//



CREATE OR REPLACE TYPE ODCIFuncInfo OID '300015' AS object
(
  ObjectSchema    VARCHAR2(128),
  ObjectName      VARCHAR2(128),
  MethodName      VARCHAR2(128),
  Flags           NUMBER
);
//

CREATE OR REPLACE TYPE ODCICost OID '300016' AS object
(
  CPUcost         NUMBER,
  IOcost          NUMBER,
  NetworkCost     NUMBER,
  IndexCostInfo   VARCHAR2(255)
);
//

CREATE OR REPLACE TYPE ODCIArgDesc OID '300017' AS object
(
  ArgType              NUMBER,
  TableName            VARCHAR2(128),
  TableSchema          VARCHAR2(128),
  ColName              VARCHAR2(4000),
  TablePartitionLower  VARCHAR2(128),
  TablePartitionUpper  VARCHAR2(128),
  Cardinality          NUMBER
);
//

CREATE OR REPLACE TYPE ODCIStatsOptions OID '300018' AS object
(
  Sample          NUMBER,
  Options         NUMBER,
  Flags           NUMBER
);
//

CREATE OR REPLACE TYPE ODCIEnv OID '300019' AS object
(
  EnvFlags     NUMBER,
  CallProperty NUMBER,
  DebugLevel   NUMBER,
  CursorNum    NUMBER
);
//

CREATE OR REPLACE TYPE ODCITabFuncStats OID '300020' AS OBJECT
(
  num_rows NUMBER
);
//

CREATE OR REPLACE TYPE ODCIGranuleList OID '300021'
  AS VARRAY(65535) of NUMBER;
//

CREATE OR REPLACE TYPE ODCISecObj OID '300022' AS OBJECT
(
  pobjschema    varchar2(30),
  pobjname      varchar2(30),
  objschema     varchar2(30),
  objname       varchar2(30)
);
//
