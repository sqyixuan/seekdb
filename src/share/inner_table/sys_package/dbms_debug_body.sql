#package_name:dbms_debug
#author: linlin.xll

CREATE OR REPLACE PACKAGE BODY DBMS_DEBUG IS

  VERSION_MAJOR CONSTANT BINARY_INTEGER := 1;
  VERSION_MINOR CONSTANT BINARY_INTEGER := 0;

  TIMEOUT_BEHAVIOUR      BINARY_INTEGER := CONTINUE_ON_TIMEOUT;

  UNIMPLEMENTED_CODE     PLS_INTEGER    := -20001;

  DUMMY                  PLS_INTEGER    := 0;

  DEFAULT_INFO           BINARY_INTEGER :=
    info_getLineinfo + info_getStackDepth + info_getBreakpoint;


  PROCEDURE probe_version(major out BINARY_INTEGER,
                          minor out BINARY_INTEGER)
  IS
  BEGIN
    major := VERSION_MAJOR;
    minor := VERSION_MINOR;
  END;

  FUNCTION TARGET_DEBUGID RETURN BINARY_INTEGER;
    PRAGMA INTERFACE(c, GET_TARGET_DEBUG_ID);

  FUNCTION inner_set_timeout(timeout BINARY_INTEGER) RETURN BINARY_INTEGER;
    PRAGMA INTERFACE(c, SET_TIMEOUT);

  FUNCTION inner_get_runtime_info  RETURN runtime_info;
    PRAGMA INTERFACE(c, GET_RUNTIME_INFO);

  FUNCTION inner_show_breakpoints RETURN VARCHAR2;
    PRAGMA INTERFACE(c, SHOW_BREAKPOINTS);

  PROCEDURE inner_enable_breakpoint(breakpoint IN BINARY_INTEGER);
    PRAGMA INTERFACE(c, ENABLE_BREAKPOINT);

  PROCEDURE inner_disable_breakpoint(breakpoint IN BINARY_INTEGER);
    PRAGMA INTERFACE(c, DISABLE_BREAKPOINT);

  PROCEDURE inner_delete_breakpoint(breakpoint IN BINARY_INTEGER);
    PRAGMA INTERFACE(c, DEL_BREAKPOINT);

  FUNCTION inner_set_breakpoint(name        IN  VARCHAR2,
                                line#       IN  BINARY_INTEGER)
    RETURN BINARY_INTEGER;
    PRAGMA INTERFACE(c, SET_BREAKPOINT);

  PROCEDURE inner_continue(breakflags IN BINARY_INTEGER);
    PRAGMA INTERFACE(c, DEBUG_CONTINUE);

  PROCEDURE inner_attach_session(debug_session_id  IN VARCHAR2);
    PRAGMA INTERFACE(c, ATTACH_SESSION);

  PROCEDURE inner_set_timeout_behaviour(behaviour IN PLS_INTEGER);
    PRAGMA INTERFACE(c, SET_TIMEOUT_BEHAVIOUR);

  PROCEDURE inner_debug_off;
    PRAGMA INTERFACE(c, DEBUG_OFF);

  PROCEDURE inner_debug_on;
    PRAGMA INTERFACE(c, DEBUG_ON);

  FUNCTION inner_initialize RETURN VARCHAR2;
    PRAGMA INTERFACE(c, INITIALIZE);

  FUNCTION inner_print_backtrace RETURN VARCHAR;
    PRAGMA INTERFACE(c, PRINT_VARCHAR_BACKTRACE);

  FUNCTION inner_get_value(variable_name  IN  VARCHAR2,
                           frame#         IN  BINARY_INTEGER)
    RETURN VARCHAR2;
    PRAGMA INTERFACE(c, GET_VALUE);

  FUNCTION set_timeout(timeout BINARY_INTEGER)
    RETURN BINARY_INTEGER
  IS
  BEGIN
    IF (timeout = 0) THEN
      default_timeout := 3600;
    ELSE
      default_timeout := timeout;
    END IF;

    IF TARGET_DEBUGID IS NOT NULL THEN
      default_timeout := inner_set_timeout(default_timeout);
    END IF;

    RETURN default_timeout;
  END;


  FUNCTION initialize(debug_session_id  IN VARCHAR2       := NULL, 
                      diagnostics       IN BINARY_INTEGER := 0,
                      debug_role        IN VARCHAR2       := NULL,
                      debug_role_pwd    IN VARCHAR2       := NULL)
    RETURN VARCHAR2
  IS
  TARGET_DEBUGID VARCHAR2(16);
  BEGIN
    -- all parameters is no meaning for now! check it!
    IF debug_session_id IS NOT NULL THEN
      RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'unimplemented DEBUG_SESSION_ID must be NULL!');
    ELSIF diagnostics != 0 THEN
      RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'unimplemented DIAGNOSTICS must be 0!');
    ELSIF debug_role IS NOT NULL THEN
      RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'unimplemented DEBUG_ROLE must be NULL!');
    ELSIF debug_role_pwd IS NOT NULL THEN
      RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'unimplemented DEBUG_ROLE_PWD must be NULL!');
    END IF;

    TARGET_DEBUGID := inner_initialize();

    RETURN TARGET_DEBUGID;
  END;


  PROCEDURE debug_on(no_client_side_plsql_engine BOOLEAN := TRUE,
                     immediate                   BOOLEAN := FALSE)
  IS
  DUMMY_DEBUGID VARCHAR2(16);
  BEGIN
    IF no_client_side_plsql_engine != 1 THEN
      RAISE_APPLICATION_ERROR(
        UNIMPLEMENTED_CODE,
        'unimplemented no_client_side_plsql_engine need be ture and immediate must be false!');
    END IF;

    IF TARGET_DEBUGID IS NULL THEN
      DUMMY_DEBUGID := inner_initialize();
    END IF;

    inner_debug_on();
    DUMMY := inner_set_timeout(default_timeout);
    inner_set_timeout_behaviour(TIMEOUT_BEHAVIOUR);
  END;


  PROCEDURE debug_off IS
  BEGIN
    IF TARGET_DEBUGID IS NOT NULL THEN
      inner_debug_off();
    END IF;
  END;


  PROCEDURE set_timeout_behaviour(behaviour IN PLS_INTEGER) IS
  BEGIN
    IF (behaviour < RETRY_ON_TIMEOUT) OR (behaviour > ABORT_ON_TIMEOUT) THEN
      RAISE_APPLICATION_ERROR(
        UNIMPLEMENTED_CODE, 'timeout behaviour' || behaviour || 'not recognize!');
    END IF;

    TIMEOUT_BEHAVIOUR := behaviour;
    
    IF TARGET_DEBUGID IS NOT NULL THEN
      inner_set_timeout_behaviour(TIMEOUT_BEHAVIOUR);
    END IF;
  END;


  FUNCTION get_timeout_behaviour RETURN BINARY_INTEGER IS
  BEGIN
    RETURN TIMEOUT_BEHAVIOUR;
  END;


  PROCEDURE attach_session(debug_session_id  IN VARCHAR2,
                           diagnostics       IN BINARY_INTEGER := 0)
  IS
  BEGIN
    IF diagnostics != 0 THEN
      RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'unimplemented diagnostics must be 0!');
    END IF;
  
    inner_attach_session(debug_session_id);
  END;


  PROCEDURE synchronize_inner;
    PRAGMA INTERFACE(C, SYNCHRONIZE);

  FUNCTION synchronize(run_info       OUT  runtime_info,
                       info_requested IN   BINARY_INTEGER := NULL)
    RETURN BINARY_INTEGER
  IS
    DUMMY BINARY_INTEGER;
  BEGIN
    synchronize_inner;
    DUMMY := get_runtime_info(info_requested, run_info);
    RETURN DUMMY;
    EXCEPTION WHEN OTHERS THEN
      RETURN error_exception;
  END;


  PROCEDURE print_backtrace(listing IN OUT VARCHAR)
  IS
  BEGIN
    listing := inner_print_backtrace();
  END;

  PROCEDURE print_backtrace(backtrace OUT backtrace_table);
    PRAGMA INTERFACE(c, PRINT_BACKTRACE);

  FUNCTION continue(run_info       IN OUT runtime_info,
                    breakflags     IN     BINARY_INTEGER,
                    info_requested IN     BINARY_INTEGER := null)
    RETURN BINARY_INTEGER
  IS
  BEGIN
    inner_continue(breakflags);
    DUMMY := get_runtime_info(info_requested, run_info);
    RETURN DUMMY;
    EXCEPTION WHEN OTHERS THEN
      RETURN error_exception;
  END;


  FUNCTION set_breakpoint(program     IN  program_info,
                          line#       IN  BINARY_INTEGER,
                          breakpoint# OUT BINARY_INTEGER,
                          fuzzy       IN  BINARY_INTEGER := 0,
                          iterations  IN  BINARY_INTEGER := 0)
    RETURN BINARY_INTEGER
  IS
    name varchar2(2000);
  BEGIN
    name := program.owner || '.' || program.name;
    breakpoint# := inner_set_breakpoint(name, line#);
    IF breakpoint# = -1 THEN
      RETURN error_illegal_line;
    ELSE
      RETURN success;
    END IF;
    EXCEPTION WHEN OTHERS THEN
      RETURN error_exception;
  END;


  FUNCTION delete_breakpoint(breakpoint IN BINARY_INTEGER)
    RETURN BINARY_INTEGER
  IS
    nosuch_breakpt EXCEPTION;
    PRAGMA EXCEPTION_INIT(nosuch_breakpt, -4018);
  BEGIN
    inner_delete_breakpoint(breakpoint);
    RETURN success;
    EXCEPTION
      WHEN nosuch_breakpt THEN
        RETURN error_no_such_breakpt;
      WHEN OTHERS THEN
        RETURN error_exception;
  END;


  FUNCTION disable_breakpoint(breakpoint IN BINARY_INTEGER)
    RETURN BINARY_INTEGER
  IS
    nosuch_breakpt EXCEPTION;
    PRAGMA EXCEPTION_INIT(nosuch_breakpt, -4018);
  BEGIN
    inner_disable_breakpoint(breakpoint);
    RETURN success;
    EXCEPTION
      WHEN nosuch_breakpt THEN
        RETURN error_no_such_breakpt;
      WHEN OTHERS THEN
        RETURN error_exception;
  END;


  FUNCTION enable_breakpoint(breakpoint IN BINARY_INTEGER)
    RETURN BINARY_INTEGER
  IS
    nosuch_breakpt EXCEPTION;
    PRAGMA EXCEPTION_INIT(nosuch_breakpt, -4018);
  BEGIN
    inner_enable_breakpoint(breakpoint);
    RETURN success;
    EXCEPTION
      WHEN nosuch_breakpt THEN
        RETURN error_no_such_breakpt;
      WHEN OTHERS THEN
        RETURN error_exception;
  END;


  PROCEDURE show_breakpoints(listing IN OUT VARCHAR2)
  IS
  BEGIN
    listing := inner_show_breakpoints();
    EXCEPTION
      WHEN OTHERS THEN
        listing := NULL;
  END;


  PROCEDURE show_breakpoints(listing  OUT breakpoint_table)
  IS
  BEGIN
    RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'not supported yet!');
  END;


  FUNCTION get_value(variable_name  IN  VARCHAR2,
                     frame#         IN  BINARY_INTEGER,
                     scalar_value   OUT VARCHAR2,
                     format         IN  VARCHAR2 := NULL)
    RETURN BINARY_INTEGER
  IS
    nosuch_frame EXCEPTION;
    PRAGMA EXCEPTION_INIT(nosuch_frame, -4003);
    nosuch_object EXCEPTION;
    PRAGMA EXCEPTION_INIT(nosuch_object, -4018);
    null_value EXCEPTION;
    PRAGMA EXCEPTION_INIT(null_value, -4152);
  BEGIN
    scalar_value := inner_get_value(variable_name, frame#);
    RETURN success;
    EXCEPTION
      WHEN nosuch_frame THEN
        RETURN error_bogus_frame;
      WHEN nosuch_object THEN
        RETURN error_no_such_object;
      WHEN null_value THEN
        RETURN error_nullvalue;
      WHEN OTHERS THEN
        RETURN error_exception;
  END;


  FUNCTION get_value(variable_name  IN  VARCHAR2,
                     handle         IN  program_info,
                     scalar_value   OUT VARCHAR2,
                     format         IN  VARCHAR2 := NULL)
     RETURN BINARY_INTEGER
  IS
  BEGIN
    RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'not supported yet!');
    RETURN NULL;
  END;

  PROCEDURE detach_session;
    PRAGMA INTERFACE(c, DETACH_SESSION);

  FUNCTION get_runtime_info(info_requested IN  BINARY_INTEGER,
                            run_info       OUT runtime_info)
    RETURN BINARY_INTEGER
  IS
    REAL_INFO_REQUESTED BINARY_INTEGER := info_requested;
  BEGIN
    IF (REAL_INFO_REQUESTED IS NULL) THEN
      REAL_INFO_REQUESTED := DEFAULT_INFO;
    END IF;
    -- TODO:
    run_info := inner_get_runtime_info();
    RETURN success;
    EXCEPTION WHEN OTHERS THEN
      RETURN error_exception;
  END;


  FUNCTION target_program_running RETURN BOOLEAN;
    PRAGMA INTERFACE(c, TARGET_PROGRAM_RUNNING);


  PROCEDURE ping;
    PRAGMA INTERFACE(c, PING);


  FUNCTION get_line_map(program IN program_info,
                        maxline OUT BINARY_INTEGER,
                        number_of_entry_points OUT BINARY_INTEGER,
                        linemap OUT raw)
    RETURN binary_integer
  IS
  BEGIN
    RAISE_APPLICATION_ERROR(UNIMPLEMENTED_CODE, 'not supported yet!');
  END;

  PROCEDURE get_values_c(scalar_values OUT VARCHAR2, frame IN BINARY_INTEGER := 0);
    PRAGMA INTERFACE(c, GET_VALUES);

  FUNCTION get_values(scalar_values OUT VARCHAR2,
                      frame IN BINARY_INTEGER := 0)
    RETURN BINARY_INTEGER
  IS
    result BINARY_INTEGER;
  BEGIN
    get_values_c(scalar_values, frame);
    RETURN success;
    EXCEPTION
      WHEN OTHERS THEN
        RETURN error_exception;
  END;

END DBMS_DEBUG;
//
