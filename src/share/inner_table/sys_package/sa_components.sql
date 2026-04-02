#package_name:sa_components
#author: jim.wjh

CREATE OR REPLACE PACKAGE SA_COMPONENTS AS

  PROCEDURE CREATE_LEVEL (
    policy_name       IN VARCHAR,
    level_num         IN NUMBER,
    short_name        IN VARCHAR,
    long_name         IN VARCHAR
  );

  PROCEDURE DROP_LEVEL (
    policy_name IN VARCHAR,
    level_num   IN NUMBER
  );

  PROCEDURE ALTER_LEVEL (
    policy_name     IN VARCHAR,
    level_num       IN NUMBER,
    new_short_name  IN VARCHAR := NULL,
    new_long_name   IN VARCHAR := NULL
  );

END SA_COMPONENTS;
//

