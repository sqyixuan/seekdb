#package_name:dbms_lock
#author:zimiao.dkz, yangyifei.yyf

CREATE OR REPLACE PACKAGE dbms_lock IS

-- for the SESSION or SYSTEM
--  /*
--   * For the following functions, meanings of parameters are:
--   *
--   * 1. seconds:
--   *    Amount of time, in seconds, to suspend the session.
--   *    The smallest increment can be entered in hundredths of a second; for example, 1.95 is a legal time value.
--   */

  --
  -- This API suspends the session for a specified period of time
  --

  NL_MODE CONSTANT INTEGER := 1;
  SS_MODE CONSTANT INTEGER := 2;
  SX_MODE CONSTANT INTEGER := 3;
  S_MODE CONSTANT INTEGER := 4;
  SSX_MODE CONSTANT INTEGER := 5;
  X_MODE CONSTANT INTEGER := 6;
  MAXWAIT CONSTANT INTEGER := 32767;  -- The constant maxwait waits forever.

  PROCEDURE sleep(seconds IN NUMBER);
  PROCEDURE allocate_unique(lockname IN VARCHAR2,
                            lockhandle OUT VARCHAR2,
                            expiration_secs IN INTEGER DEFAULT 864000);
  PROCEDURE allocate_unique_autonomous(lockname IN VARCHAR2,
                                       lockhandle OUT VARCHAR2,
                                       expiration_secs IN INTEGER DEFAULT 864000);
  -- FUNCTION convert(id IN INTEGER,
  --                  lockmode IN INTEGER,
  --                  timeout IN NUMBER DEFAULT MAXWAIT)
  -- RETURN INTEGER;
  -- FUNCTION convert(lockhandle IN VARCHAR2,
  --                  lockmode IN INTEGER,
  --                  timeout IN NUMBER DEFAULT MAXWAIT)
  -- RETURN INTEGER;
  FUNCTION release(id IN INTEGER) RETURN INTEGER;
  FUNCTION release(lockhandle IN VARCHAR2) RETURN INTEGER;
  FUNCTION request(id IN INTEGER,
                   lockmode IN INTEGER DEFAULT X_MODE,
                   timeout IN INTEGER DEFAULT MAXWAIT,
                   release_on_commit IN BOOLEAN DEFAULT FALSE)
  RETURN INTEGER;
  FUNCTION request(lockhandle IN VARCHAR2,
                   lockmode IN INTEGER DEFAULT X_MODE,
                   timeout IN INTEGER DEFAULT MAXWAIT,
                   release_on_commit IN BOOLEAN DEFAULT FALSE)
  RETURN INTEGER;
END;
//
