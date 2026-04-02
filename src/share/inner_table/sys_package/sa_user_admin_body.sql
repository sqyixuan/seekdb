#package_name:sa_user_admin
#author: jim.wjh

CREATE OR REPLACE PACKAGE BODY SA_USER_ADMIN AS

  PROCEDURE SET_LEVELS (
    policy_name      IN VARCHAR,
    user_name        IN VARCHAR,
    max_level        IN VARCHAR,
    min_level        IN VARCHAR := NULL,
    def_level        IN VARCHAR := NULL,
    row_level        IN VARCHAR := NULL
  );
  PRAGMA INTERFACE(C, SA_USER_ADMIN_SET_LEVELS);

END SA_USER_ADMIN;
//