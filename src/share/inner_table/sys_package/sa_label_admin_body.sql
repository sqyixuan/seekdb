#package_name:sa_label_admin
#author: jim.wjh

CREATE OR REPLACE PACKAGE BODY SA_LABEL_ADMIN AS
  PROCEDURE CREATE_LABEL (
    policy_name IN VARCHAR,
    label_tag   IN BINARY_INTEGER,
    label_value IN VARCHAR,
    data_label  IN BOOLEAN := TRUE
  );
  PRAGMA INTERFACE(C, SA_LABEL_ADMIN_CRETAE_LABEL);

  PROCEDURE ALTER_LABEL (
    policy_name       IN VARCHAR,
    label_tag         IN BINARY_INTEGER,
    new_label_value   IN VARCHAR := NULL,
    new_data_label    IN BOOLEAN := NULL
  );
  PRAGMA INTERFACE(C, SA_LABEL_ADMIN_ALTER_LABEL);

  PROCEDURE DROP_LABEL (
    policy_name       IN VARCHAR,
    label_tag         IN BINARY_INTEGER
  );
  PRAGMA INTERFACE(C, SA_LABEL_ADMIN_DROP_LABEL);

END SA_LABEL_ADMIN;
//
