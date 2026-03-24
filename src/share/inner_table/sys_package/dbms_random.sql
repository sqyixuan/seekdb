#package_name:dbms_random
#author:jingxing.lj

CREATE OR REPLACE PACKAGE dbms_random AS

  PROCEDURE seed(val IN VARCHAR2);

  FUNCTION value RETURN NUMBER;

  FUNCTION  value(low IN NUMBER, high IN NUMBER) RETURN NUMBER;

  FUNCTION  string(opt IN CHAR, len IN NUMBER) RETURN VARCHAR2;

  TYPE num_arr IS TABLE OF NUMBER;

  FUNCTION normal RETURN NUMBER;

  PROCEDURE initialize(val IN BINARY_INTEGER);

  FUNCTION random RETURN BINARY_INTEGER;

END dbms_random;
//
