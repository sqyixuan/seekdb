#package_name:utl_raw
#author: jianhua.sjh

CREATE OR REPLACE PACKAGE UTL_RAW AS
  FUNCTION CAST_TO_RAW (
      v              IN VARCHAR2 CHARACTER SET ANY_CS
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION CAST_TO_VARCHAR2 (
      r              IN RAW
  )
  RETURN VARCHAR DETERMINISTIC;

  FUNCTION LENGTH (
      r              IN RAW
  )
  RETURN NUMBER DETERMINISTIC;

  FUNCTION BIT_COMPLEMENT (
      r              IN RAW
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION BIT_AND (
      r1            IN RAW,
      r2            IN RAW
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION BIT_OR (
      r1            IN RAW,
      r2            IN RAW
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION BIT_XOR (
      r1            IN RAW,
      r2            IN RAW
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION COPIES (
      r            IN RAW,
      n            IN NUMBER
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION COMPARE (
      r1            IN RAW,
      r2            IN RAW,
      pad           IN RAW := NULL
  )
  RETURN NUMBER DETERMINISTIC;

  FUNCTION CONCAT (
      r1            IN RAW := NULL,
      r2            IN RAW := NULL,
      r3            IN RAW := NULL,
      r4            IN RAW := NULL,
      r5            IN RAW := NULL,
      r6            IN RAW := NULL,
      r7            IN RAW := NULL,
      r8            IN RAW := NULL,
      r9            IN RAW := NULL,
      r10           IN RAW := NULL,
      r11           IN RAW := NULL,
      r12           IN RAW := NULL
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION SUBSTR (
      r             IN RAW,
      pos           IN NUMBER,
      len           IN NUMBER := NULL
  )
  RETURN RAW DETERMINISTIC;

FUNCTION REVERSE (
      r              IN RAW
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION CAST_FROM_BINARY_INTEGER(
      n           IN BINARY_INTEGER,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION CAST_TO_BINARY_INTEGER(
      r           IN RAW,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN BINARY_INTEGER DETERMINISTIC;

  FUNCTION CAST_FROM_BINARY_FLOAT(
      n           IN BINARY_FLOAT,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION CAST_TO_BINARY_FLOAT(
      r           IN RAW,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN BINARY_FLOAT DETERMINISTIC;

  FUNCTION CAST_FROM_BINARY_DOUBLE(
      n           IN BINARY_DOUBLE,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION CAST_TO_BINARY_DOUBLE(
      r           IN RAW,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN BINARY_DOUBLE DETERMINISTIC;

  FUNCTION CAST_FROM_NUMBER(
      n           IN NUMBER
  )
  RETURN RAW DETERMINISTIC;

  FUNCTION CAST_TO_NUMBER(
      r           IN RAW
  )
  RETURN NUMBER DETERMINISTIC;

  FUNCTION CAST_TO_NVARCHAR2(
      r           IN RAW
  )
  RETURN NVARCHAR2 DETERMINISTIC;

END UTL_RAW;
//
