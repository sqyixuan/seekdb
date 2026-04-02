#package_name:dbms_types, anydata, anytype
#author: linlin.xll

CREATE OR REPLACE PACKAGE dbms_types AS

  TYPECODE_DATE            PLS_INTEGER :=  12;
  TYPECODE_NUMBER          PLS_INTEGER :=   2;
  TYPECODE_RAW             PLS_INTEGER :=  95;
  TYPECODE_CHAR            PLS_INTEGER :=  96;
  TYPECODE_VARCHAR2        PLS_INTEGER :=   9;
  TYPECODE_VARCHAR         PLS_INTEGER :=   1;
  TYPECODE_MLSLABEL        PLS_INTEGER := 105;
  TYPECODE_BLOB            PLS_INTEGER := 113;
  TYPECODE_BFILE           PLS_INTEGER := 114;
  TYPECODE_CLOB            PLS_INTEGER := 112;
  TYPECODE_CFILE           PLS_INTEGER := 115;
  TYPECODE_TIMESTAMP       PLS_INTEGER := 187;
  TYPECODE_TIMESTAMP_TZ    PLS_INTEGER := 188;
  TYPECODE_TIMESTAMP_LTZ   PLS_INTEGER := 232;
  TYPECODE_INTERVAL_YM     PLS_INTEGER := 189;
  TYPECODE_INTERVAL_DS     PLS_INTEGER := 190;

  TYPECODE_REF             PLS_INTEGER := 110;
  TYPECODE_OBJECT          PLS_INTEGER := 108;
  TYPECODE_VARRAY          PLS_INTEGER := 247;
  TYPECODE_TABLE           PLS_INTEGER := 248;
  TYPECODE_NAMEDCOLLECTION PLS_INTEGER := 122;
  TYPECODE_OPAQUE          PLS_INTEGER := 58;

  TYPECODE_NCHAR           PLS_INTEGER := 286;
  TYPECODE_NVARCHAR2       PLS_INTEGER := 287;
  TYPECODE_NCLOB           PLS_INTEGER := 288;

  TYPECODE_BFLOAT          PLS_INTEGER := 100;
  TYPECODE_BDOUBLE         PLS_INTEGER := 101;
  TYPECODE_UROWID          PLS_INTEGER := 104;

  SUCCESS                  PLS_INTEGER := 0;
  NO_DATA                  PLS_INTEGER := 100;

  invalid_parameters EXCEPTION;
  PRAGMA EXCEPTION_INIT(invalid_parameters, -5955);

  incorrect_usage EXCEPTION;
  PRAGMA EXCEPTION_INIT(incorrect_usage, -5956);

  type_mismatch EXCEPTION;
  PRAGMA EXCEPTION_INIT(type_mismatch, -5957);

END dbms_types;
//

CREATE OR REPLACE TYPE ANYTYPE FORCE OID '300004' AS OPAQUE
(

  STATIC PROCEDURE BeginCreate(typecode IN PLS_INTEGER,
                               atype OUT NOCOPY AnyType),

  MEMBER PROCEDURE SetInfo(self IN OUT NOCOPY AnyType,
           prec IN PLS_INTEGER, scale IN PLS_INTEGER,
           len IN PLS_INTEGER,
           csid IN PLS_INTEGER, csfrm IN PLS_INTEGER,
           atype IN ANYTYPE DEFAULT NULL,
           elem_tc IN PLS_INTEGER DEFAULT NULL,
           elem_count IN PLS_INTEGER DEFAULT 0),

  MEMBER PROCEDURE AddAttr(self IN OUT NOCOPY AnyType,
           aname IN VARCHAR2,
           typecode IN PLS_INTEGER,
           prec IN PLS_INTEGER, scale IN PLS_INTEGER,
           len IN PLS_INTEGER,
           csid IN PLS_INTEGER, csfrm IN PLS_INTEGER,
           attr_type IN ANYTYPE DEFAULT NULL),

  MEMBER PROCEDURE EndCreate(self IN OUT NOCOPY AnyType),

  STATIC FUNCTION GetPersistent(schema_name IN VARCHAR2,
                      type_name IN VARCHAR2,
                      version IN varchar2 DEFAULT NULL) return AnyType,

  MEMBER FUNCTION GetInfo (self IN AnyType,
       prec OUT PLS_INTEGER, scale OUT PLS_INTEGER,
       len OUT PLS_INTEGER, csid OUT PLS_INTEGER,
       csfrm OUT PLS_INTEGER,
       schema_name OUT VARCHAR2, type_name OUT VARCHAR2, version OUT varchar2,
       numelems OUT PLS_INTEGER)
                 return PLS_INTEGER,

  MEMBER FUNCTION GetAttrElemInfo (self IN AnyType, pos IN PLS_INTEGER,
       prec OUT PLS_INTEGER, scale OUT PLS_INTEGER,
       len OUT PLS_INTEGER, csid OUT PLS_INTEGER, csfrm OUT PLS_INTEGER,
       attr_elt_type OUT ANYTYPE, aname OUT VARCHAR2) return PLS_INTEGER

);
//

CREATE OR REPLACE TYPE AnyData OID '300005' as OPAQUE
(

  STATIC FUNCTION ConvertNumber(num IN NUMBER) return AnyData,
  STATIC FUNCTION ConvertDate(dat IN DATE) return AnyData,
  STATIC FUNCTION ConvertChar(c IN CHAR) return AnyData,
  STATIC FUNCTION ConvertVarchar(c IN VARCHAR) return AnyData,
  STATIC FUNCTION ConvertVarchar2(c IN VARCHAR2) return AnyData,
  STATIC FUNCTION ConvertRaw(r IN RAW) return AnyData,
  STATIC FUNCTION ConvertBlob(b IN BLOB) return AnyData,
  STATIC FUNCTION ConvertClob(c IN CLOB) return AnyData,

  STATIC FUNCTION ConvertObject(obj IN "<ADT_1>") return AnyData,

  STATIC FUNCTION ConvertCollection(col IN "<COLLECTION_1>") return AnyData,

  STATIC FUNCTION ConvertTimestamp(ts IN TIMESTAMP_UNCONSTRAINED) return AnyData,
  STATIC FUNCTION ConvertTimestampTZ(ts IN TIMESTAMP_TZ_UNCONSTRAINED) return AnyData,
  STATIC FUNCTION ConvertTimestampLTZ(ts IN TIMESTAMP_LTZ_UNCONSTRAINED) return AnyData,
  STATIC FUNCTION ConvertIntervalYM(inv IN YMINTERVAL_UNCONSTRAINED) return AnyData,
  STATIC FUNCTION ConvertIntervalDS(inv IN DSINTERVAL_UNCONSTRAINED) return AnyData,
  STATIC FUNCTION ConvertNchar(nc IN NCHAR) return AnyData,
  STATIC FUNCTION ConvertNVarchar2(nc IN NVARCHAR2) return AnyData,

  STATIC FUNCTION ConvertBFloat(fl IN BINARY_FLOAT) return AnyData,
  STATIC FUNCTION ConvertBDouble(dbl IN BINARY_DOUBLE) return AnyData,
  STATIC FUNCTION ConvertURowid(rid IN UROWID) return AnyData,

  STATIC PROCEDURE BeginCreate(dtype IN OUT NOCOPY AnyType,
                               adata OUT NOCOPY AnyData),

  MEMBER PROCEDURE PieceWise(self IN OUT NOCOPY AnyData),

  MEMBER PROCEDURE SetNumber(self IN OUT NOCOPY AnyData, num IN NUMBER,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetDate(self IN OUT NOCOPY AnyData, dat IN DATE,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetChar(self IN OUT NOCOPY AnyData, c IN CHAR,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetVarchar(self IN OUT NOCOPY AnyData, c IN VARCHAR,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetVarchar2(self IN OUT NOCOPY AnyData,
                    c IN VARCHAR2, last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetRaw(self IN OUT NOCOPY AnyData, r IN RAW,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetBlob(self IN OUT NOCOPY AnyData, b IN BLOB,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetClob(self IN OUT NOCOPY AnyData, c IN CLOB,
                    last_elem IN boolean DEFAULT FALSE),

  MEMBER PROCEDURE SetObject(self IN OUT NOCOPY AnyData, obj IN "<ADT_1>",
                    last_elem IN boolean DEFAULT FALSE),

  MEMBER PROCEDURE SetCollection(self IN OUT NOCOPY AnyData, col IN "<COLLECTION_1>",
                    last_elem IN boolean DEFAULT FALSE),

  MEMBER PROCEDURE SetTimestamp(self IN OUT NOCOPY AnyData, ts IN TIMESTAMP_UNCONSTRAINED,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetTimestampTZ(self IN OUT NOCOPY AnyData,
                    ts IN TIMESTAMP_TZ_UNCONSTRAINED,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetTimestampLTZ(self IN OUT NOCOPY AnyData,
                    ts IN TIMESTAMP_LTZ_UNCONSTRAINED,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetIntervalYM(self IN OUT NOCOPY AnyData,
                    inv IN YMINTERVAL_UNCONSTRAINED,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetIntervalDS(self IN OUT NOCOPY AnyData,
                    inv IN DSINTERVAL_UNCONSTRAINED,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetNchar(self IN OUT NOCOPY AnyData,
                    nc IN NCHAR, last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetNVarchar2(self IN OUT NOCOPY AnyData,
                    nc IN NVarchar2, last_elem IN boolean DEFAULT FALSE),

  MEMBER PROCEDURE SetBFloat(self IN OUT NOCOPY AnyData, fl IN BINARY_FLOAT,
                    last_elem IN boolean DEFAULT FALSE),
  MEMBER PROCEDURE SetBDouble(self IN OUT NOCOPY AnyData, dbl IN BINARY_DOUBLE,
                    last_elem IN boolean DEFAULT FALSE),

  MEMBER PROCEDURE EndCreate(self IN OUT NOCOPY AnyData),

  MEMBER FUNCTION GetTypeName(self IN AnyData) return VARCHAR2
    DETERMINISTIC,

  MEMBER FUNCTION GetType(self IN AnyData, typ OUT NOCOPY AnyType)
      return PLS_INTEGER,

  MEMBER FUNCTION GetNumber(self IN AnyData, num OUT NOCOPY NUMBER)
              return PLS_INTEGER,
  MEMBER FUNCTION GetDate(self IN AnyData, dat OUT NOCOPY DATE)
              return PLS_INTEGER,
  MEMBER FUNCTION GetChar(self IN AnyData, c OUT NOCOPY CHAR)
              return PLS_INTEGER,
  MEMBER FUNCTION GetVarchar(self IN AnyData, c OUT NOCOPY VARCHAR)
              return PLS_INTEGER,
  MEMBER FUNCTION GetVarchar2(self IN AnyData, c OUT NOCOPY VARCHAR2)
              return PLS_INTEGER,
  MEMBER FUNCTION GetRaw(self IN AnyData, r OUT NOCOPY RAW)
              return PLS_INTEGER,
  MEMBER FUNCTION GetBlob(self IN AnyData, b OUT NOCOPY BLOB)
              return PLS_INTEGER,
  MEMBER FUNCTION GetClob(self IN AnyData, c OUT NOCOPY CLOB)
              return PLS_INTEGER,

  MEMBER FUNCTION GetObject(self IN AnyData, obj OUT NOCOPY "<ADT_1>")
              return PLS_INTEGER,

  MEMBER FUNCTION GetCollection(self IN AnyData, col OUT NOCOPY "<COLLECTION_1>")
              return PLS_INTEGER,

  MEMBER FUNCTION GetTimestamp(self IN AnyData, ts OUT NOCOPY TIMESTAMP_UNCONSTRAINED)
              return PLS_INTEGER,
  MEMBER FUNCTION GetTimestampTZ(self IN AnyData, ts OUT NOCOPY TIMESTAMP_TZ_UNCONSTRAINED)
              return PLS_INTEGER,
  MEMBER FUNCTION GetTimestampLTZ(self IN AnyData, ts OUT NOCOPY TIMESTAMP_LTZ_UNCONSTRAINED)
              return PLS_INTEGER,
  MEMBER FUNCTION GetIntervalYM(self IN AnyData, inv IN OUT NOCOPY YMINTERVAL_UNCONSTRAINED)
              return PLS_INTEGER,
  MEMBER FUNCTION GetIntervalDS(self IN AnyData, inv IN OUT NOCOPY DSINTERVAL_UNCONSTRAINED)
              return PLS_INTEGER,
  MEMBER FUNCTION GetNchar(self IN AnyData, nc OUT NOCOPY NCHAR)
              return PLS_INTEGER,
  MEMBER FUNCTION GetNVarchar2(self IN AnyData, nc OUT NOCOPY NVARCHAR2)
              return PLS_INTEGER,

  MEMBER FUNCTION GetBFloat(self IN AnyData, fl OUT NOCOPY BINARY_FLOAT)
              return PLS_INTEGER,
  MEMBER FUNCTION GetBDouble(self IN AnyData, dbl OUT NOCOPY BINARY_DOUBLE)
              return PLS_INTEGER,

  MEMBER FUNCTION AccessNumber(self IN AnyData) return NUMBER DETERMINISTIC,
  MEMBER FUNCTION AccessDate(self IN AnyData) return DATE DETERMINISTIC,
  MEMBER FUNCTION AccessChar(self IN AnyData) return CHAR DETERMINISTIC,
  MEMBER FUNCTION AccessVarchar(self IN AnyData) return VARCHAR DETERMINISTIC,
  MEMBER FUNCTION AccessVarchar2(self IN AnyData) return VARCHAR2 DETERMINISTIC,
  MEMBER FUNCTION AccessRaw(self IN AnyData) return RAW DETERMINISTIC,
  MEMBER FUNCTION AccessBlob(self IN AnyData) return BLOB DETERMINISTIC,
  MEMBER FUNCTION AccessClob(self IN AnyData) return CLOB DETERMINISTIC,

  MEMBER FUNCTION AccessTimestamp(self IN AnyData) return TIMESTAMP_UNCONSTRAINED DETERMINISTIC,
  MEMBER FUNCTION AccessTimestampTZ(self IN AnyData) return TIMESTAMP_TZ_UNCONSTRAINED DETERMINISTIC,
  MEMBER FUNCTION AccessTimestampLTZ(self IN AnyData) return TIMESTAMP_LTZ_UNCONSTRAINED DETERMINISTIC,
  MEMBER FUNCTION AccessIntervalYM(self IN AnyData) return YMINTERVAL_UNCONSTRAINED DETERMINISTIC,
  MEMBER FUNCTION AccessIntervalDS(self IN AnyData) return DSINTERVAL_UNCONSTRAINED DETERMINISTIC,
  MEMBER FUNCTION AccessNchar(self IN AnyData) return NCHAR DETERMINISTIC,
  MEMBER FUNCTION AccessNVarchar2(self IN AnyData) return NVARCHAR2 DETERMINISTIC,

  MEMBER FUNCTION AccessBFloat(self IN AnyData) return BINARY_FLOAT DETERMINISTIC,
  MEMBER FUNCTION AccessBDouble(self IN AnyData) return BINARY_DOUBLE DETERMINISTIC,
  MEMBER FUNCTION AccessURowid(self IN AnyData) return UROWID DETERMINISTIC
);
//
