#package_name:sa_session
#author: jim.wjh

CREATE OR REPLACE PACKAGE SA_SESSION AS

  FUNCTION LABEL ( 
    policy_name IN VARCHAR
  )
  RETURN VARCHAR;

  PROCEDURE RESTORE_DEFAULT_LABELS (
    policy_name in VARCHAR
  );

  FUNCTION ROW_LABEL ( 
    policy_name IN VARCHAR
  )
  RETURN VARCHAR; 

  PROCEDURE SET_LABEL (
    policy_name IN VARCHAR,
    label       IN VARCHAR
  );

  PROCEDURE SET_ROW_LABEL (
    policy_name   IN VARCHAR,
    row_label     IN VARCHAR
  );

END SA_SESSION;
//
