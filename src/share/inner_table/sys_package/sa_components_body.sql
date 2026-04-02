#package_name:sa_components
#author: jim.wjh

CREATE OR REPLACE PACKAGE BODY SA_COMPONENTS AS

  PROCEDURE CREATE_LEVEL (
    policy_name       IN VARCHAR,
    level_num         IN NUMBER,
    short_name        IN VARCHAR,
    long_name         IN VARCHAR
  );
  PRAGMA INTERFACE(C, SA_COMPONENTS_CREATE_LEVEL);

  PROCEDURE ALTER_LEVEL (
    policy_name     IN VARCHAR,
    level_num       IN NUMBER,
    new_short_name  IN VARCHAR := NULL,
    new_long_name   IN VARCHAR := NULL
  );
  PRAGMA INTERFACE(C, SA_COMPONENTS_ALTER_LEVEL);

  PROCEDURE DROP_LEVEL (
    policy_name IN VARCHAR,
    level_num   IN NUMBER
  );
  PRAGMA INTERFACE(C, SA_COMPONENTS_DROP_LEVEL);

END SA_COMPONENTS;
//

