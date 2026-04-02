#package_name:utl_raw
#author: jianhua.sjh

CREATE OR REPLACE PACKAGE BODY UTL_RAW AS

  FUNCTION CAST_TO_RAW (
      v              IN VARCHAR2 CHARACTER SET ANY_CS)
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_cast_to_raw);

  FUNCTION CAST_TO_VARCHAR2(
      r              IN RAW
  )
  RETURN VARCHAR;
  PRAGMA INTERFACE(c, utl_raw_cast_to_varchar2);

  FUNCTION LENGTH(
      r              IN RAW
  )
  RETURN NUMBER;
  PRAGMA INTERFACE(c, utl_raw_length);

  FUNCTION BIT_COMPLEMENT(
      r              IN RAW
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_bit_complement);

  FUNCTION BIT_AND(
      r1              IN RAW,
      r2              IN RAW
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_bit_and);

  FUNCTION BIT_OR(
      r1              IN RAW,
      r2              IN RAW
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_bit_or);

  FUNCTION BIT_XOR(
      r1              IN RAW,
      r2              IN RAW
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_bit_xor);

  FUNCTION COPIES(
      r            IN RAW,
      n            IN NUMBER
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_copies);

  FUNCTION COMPARE(
      r1            IN RAW,
      r2            IN RAW,
      pad           IN RAW  := NULL
  )
  RETURN NUMBER;
  PRAGMA INTERFACE(c, utl_raw_compare);

  FUNCTION CONCAT(
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
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_concat);

  FUNCTION SUBSTR(
      r             IN RAW,
      pos           IN NUMBER,
      len           IN NUMBER := NULL
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_substr);

  FUNCTION REVERSE(
      r              IN RAW
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_reverse);

  FUNCTION CAST_FROM_BINARY_INTEGER(
      n           IN BINARY_INTEGER,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_cast_from_binary_integer);

  FUNCTION CAST_TO_BINARY_INTEGER(
      r           IN RAW,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN BINARY_INTEGER;
  PRAGMA INTERFACE(c, utl_raw_cast_to_binary_integer);

  FUNCTION CAST_FROM_BINARY_FLOAT(
      n           IN BINARY_FLOAT,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_cast_from_binary_float);

  FUNCTION CAST_TO_BINARY_FLOAT(
      r           IN RAW,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN BINARY_FLOAT;
  PRAGMA INTERFACE(c, utl_raw_cast_to_binary_float);

  FUNCTION CAST_FROM_BINARY_DOUBLE(
      n           IN BINARY_DOUBLE,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_cast_from_binary_double);

  FUNCTION CAST_TO_BINARY_DOUBLE(
      r           IN RAW,
      endianess   IN PLS_INTEGER := 1
  )
  RETURN BINARY_DOUBLE;
  PRAGMA INTERFACE(c, utl_raw_cast_to_binary_double);

  FUNCTION CAST_FROM_NUMBER(
      n           IN NUMBER
  )
  RETURN RAW;
  PRAGMA INTERFACE(c, utl_raw_cast_from_number);

  FUNCTION CAST_TO_NUMBER(
      r           IN RAW
  )
  RETURN NUMBER;
  PRAGMA INTERFACE(c, utl_raw_cast_to_number);

  FUNCTION CAST_TO_NVARCHAR2(
      r              IN RAW)
  RETURN NVARCHAR2;
  PRAGMA INTERFACE(c, utl_raw_cast_to_nvarchar2);

END UTL_RAW;
//

