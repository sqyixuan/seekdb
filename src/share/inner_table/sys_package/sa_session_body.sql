#package_name:sa_session
#author: jim.wjh

CREATE OR REPLACE PACKAGE BODY SA_SESSION AS

  FUNCTION LABEL ( 
    policy_name IN VARCHAR
  ) 
  RETURN VARCHAR AS
    ret VARCHAR (4000);
  BEGIN
    ret := label_to_char(ols_session_label(policy_name));
    RETURN ret;
  END;

  PROCEDURE RESTORE_DEFAULT_LABELS (
    policy_name in VARCHAR
  );
  PRAGMA INTERFACE (C, SA_SESSION_RESTORE_DEFAULT_LABELS);

  FUNCTION ROW_LABEL ( 
    policy_name IN VARCHAR
  )
  RETURN VARCHAR AS
    ret VARCHAR (4000);
  BEGIN
    ret := label_to_char(ols_session_row_label(policy_name));
    RETURN ret;
  END;

  PROCEDURE SET_LABEL (
    policy_name IN VARCHAR,
    label       IN VARCHAR
  );
  PRAGMA INTERFACE (C, SA_SESSION_SET_LABEL);

  PROCEDURE SET_ROW_LABEL (
    policy_name   IN VARCHAR,
    row_label     IN VARCHAR
  );
  PRAGMA INTERFACE (C, SA_SESSION_SET_ROW_LABEL);

END SA_SESSION;
//
