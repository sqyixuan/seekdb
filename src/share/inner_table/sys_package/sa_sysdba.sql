#package_name:sa_sysdba
#author: jim.wjh

CREATE OR REPLACE PACKAGE SA_SYSDBA AS

  PROCEDURE ALTER_POLICY (
    policy_name       IN  VARCHAR,
    default_options   IN  VARCHAR := NULL
  );

  PROCEDURE CREATE_POLICY (
    policy_name       IN VARCHAR,
    column_name       IN VARCHAR := NULL,
    default_options   IN VARCHAR := NULL
  );

  PROCEDURE DISABLE_POLICY (
    policy_name IN VARCHAR
  );

  PROCEDURE ENABLE_POLICY (
    policy_name IN VARCHAR
  );

  PROCEDURE DROP_POLICY ( 
    policy_name IN VARCHAR,
    drop_column IN BOOLEAN := FALSE
  );

END SA_SYSDBA;
//
