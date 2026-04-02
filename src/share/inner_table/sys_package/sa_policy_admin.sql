#package_name:sa_policy_admin
#author: jim.wjh

CREATE OR REPLACE PACKAGE SA_POLICY_ADMIN AS
  
  PROCEDURE APPLY_TABLE_POLICY (
    policy_name       IN VARCHAR,
    schema_name       IN VARCHAR,
    table_name        IN VARCHAR,
    table_options     IN VARCHAR := NULL,
    label_function    IN VARCHAR := NULL,
    predicate         IN VARCHAR := NULL
  );

  PROCEDURE DISABLE_TABLE_POLICY (
    policy_name      IN VARCHAR,
    schema_name      IN VARCHAR,
    table_name       IN VARCHAR
  );

  PROCEDURE ENABLE_TABLE_POLICY (
    policy_name     IN VARCHAR,
    schema_name     IN VARCHAR,
    table_name      IN VARCHAR
  );

  PROCEDURE REMOVE_TABLE_POLICY (
    policy_name        IN VARCHAR,
    schema_name        IN VARCHAR,
    table_name         IN VARCHAR,
    drop_column        IN BOOLEAN := FALSE
  );

END SA_POLICY_ADMIN;
//
