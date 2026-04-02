#package_name:sa_policy_admin
#author: jim.wjh

CREATE OR REPLACE PACKAGE BODY SA_POLICY_ADMIN AS
  
  PROCEDURE APPLY_TABLE_POLICY (
    policy_name       IN VARCHAR,
    schema_name       IN VARCHAR,
    table_name        IN VARCHAR,
    table_options     IN VARCHAR := NULL,
    label_function    IN VARCHAR := NULL,
    predicate         IN VARCHAR := NULL
  );
  PRAGMA INTERFACE (C, SA_POLICY_ADMIN_APPLY_TABLE_POLICY);

  PROCEDURE DISABLE_TABLE_POLICY (
    policy_name      IN VARCHAR,
    schema_name      IN VARCHAR,
    table_name       IN VARCHAR
  );
  PRAGMA INTERFACE (C, SA_POLICY_ADMIN_DISABLE_TABLE_POLICY);

  PROCEDURE ENABLE_TABLE_POLICY (
    policy_name     IN VARCHAR,
    schema_name     IN VARCHAR,
    table_name      IN VARCHAR
  );
  PRAGMA INTERFACE (C, SA_POLICY_ADMIN_ENABLE_TABLE_POLICY);

  PROCEDURE REMOVE_TABLE_POLICY (
    policy_name        IN VARCHAR,
    schema_name        IN VARCHAR,
    table_name         IN VARCHAR,
    drop_column        IN BOOLEAN := FALSE
  );
  PRAGMA INTERFACE (C, SA_POLICY_ADMIN_REMOVE_TABLE_POLICY);

END SA_POLICY_ADMIN;
//
