#package_name:dbms_lock
#author:zimiao.dkz, yangyifei.yyf

CREATE OR replace PACKAGE BODY dbms_lock IS
  PROCEDURE SLEEP(SECONDS IN NUMBER);
  pragma interface (C, SLEEP_LOCK);

  PROCEDURE allocate_unique(lockname IN VARCHAR2,
                            lockhandle OUT VARCHAR2,
                            expiration_secs IN INTEGER DEFAULT 864000);
  pragma interface (C, ALLOCATE_UNIQUE_LOCK);

  PROCEDURE allocate_unique_autonomous(lockname IN VARCHAR2,
                                       lockhandle OUT VARCHAR2,
                                       expiration_secs IN INTEGER DEFAULT 864000)
  IS PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DBMS_LOCK.allocate_unique(lockname, lockhandle, expiration_secs);
  END;

  FUNCTION release(id IN INTEGER) RETURN INTEGER;
  pragma interface (C, RELEASE_LOCK);

  FUNCTION release(lockhandle IN VARCHAR2) RETURN INTEGER;
  pragma interface (C, RELEASE_LOCK);

  FUNCTION request(id IN INTEGER,
                   lockmode IN INTEGER DEFAULT X_MODE,
                   timeout IN INTEGER DEFAULT MAXWAIT,
                   release_on_commit IN BOOLEAN DEFAULT FALSE) RETURN INTEGER;
  pragma interface (C, REQUEST_LOCK);

  FUNCTION request(lockhandle IN VARCHAR2,
                   lockmode IN INTEGER DEFAULT X_MODE,
                   timeout IN INTEGER DEFAULT MAXWAIT,
                   release_on_commit IN BOOLEAN DEFAULT FALSE) RETURN INTEGER;
  pragma interface (C, REQUEST_LOCK);

END;
//
