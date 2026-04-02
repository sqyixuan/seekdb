#package_name:dbms_debug
#author: linlin.xll

CREATE OR REPLACE PACKAGE DBMS_DEBUG AS

  TYPE program_info IS RECORD
  (

    Namespace        BINARY_INTEGER,
    Name             VARCHAR2(128),
    Owner            VARCHAR2(128)
  );

  TYPE runtime_info IS RECORD
  (
    Line#            BINARY_INTEGER,
    Terminated       BINARY_INTEGER,
    Breakpoint       BINARY_INTEGER,
    StackDepth       BINARY_INTEGER,
    Reason           BINARY_INTEGER,
    Program          program_info
   );

  TYPE breakpoint_info IS RECORD
  (

     Name        VARCHAR2(128),
     Owner       VARCHAR2(128),
     Line#       BINARY_INTEGER,

     Status      BINARY_INTEGER
  );

  breakpoint_status_unused    CONSTANT BINARY_INTEGER := 0;
  breakpoint_status_active    CONSTANT BINARY_INTEGER := 1;
  breakpoint_status_disabled  CONSTANT BINARY_INTEGER := 4;

  TYPE index_table IS table of BINARY_INTEGER index by BINARY_INTEGER;

  TYPE backtrace_table IS TABLE OF program_info INDEX BY BINARY_INTEGER;

  TYPE breakpoint_table IS TABLE OF breakpoint_info INDEX BY BINARY_INTEGER;

  namespace_cursor               CONSTANT BINARY_INTEGER := 0;
  namespace_pkgspec_or_toplevel  CONSTANT BINARY_INTEGER := 1;
  namespace_pkg_body             CONSTANT BINARY_INTEGER := 2;
  namespace_trigger              CONSTANT BINARY_INTEGER := 3;
  namespace_none                 CONSTANT BINARY_INTEGER := 255;

  break_exception      CONSTANT PLS_INTEGER :=    2;
  break_any_call       CONSTANT PLS_INTEGER :=   12;
  break_return         CONSTANT PLS_INTEGER :=   16;
  break_next_line      CONSTANT PLS_INTEGER :=   32;
  break_any_return     CONSTANT PLS_INTEGER :=  512;
  break_handler        CONSTANT PLS_INTEGER := 2048;
  abort_execution      CONSTANT PLS_INTEGER := 8192;

  info_getStackDepth    CONSTANT PLS_INTEGER := 2;
  info_getBreakpoint    CONSTANT PLS_INTEGER := 4;
  info_getLineinfo      CONSTANT PLS_INTEGER := 8;
  info_getOerInfo       CONSTANT PLS_INTEGER := 32;

  reason_none        CONSTANT BINARY_INTEGER :=  0;
  reason_interpreter_starting CONSTANT BINARY_INTEGER :=  2;
  reason_breakpoint  CONSTANT BINARY_INTEGER :=  3;
  reason_enter       CONSTANT BINARY_INTEGER :=  6;
  reason_return      CONSTANT BINARY_INTEGER :=  7;
  reason_finish      CONSTANT BINARY_INTEGER :=  8;
  reason_line        CONSTANT BINARY_INTEGER :=  9;
  reason_interrupt   CONSTANT BINARY_INTEGER := 10;
  reason_exception   CONSTANT BINARY_INTEGER := 11;
  reason_exit        CONSTANT BINARY_INTEGER := 15;
  reason_handler     CONSTANT BINARY_INTEGER := 16;
  reason_timeout     CONSTANT BINARY_INTEGER := 17;
  reason_instantiate CONSTANT BINARY_INTEGER := 20;
  reason_abort       CONSTANT BINARY_INTEGER := 21;
  reason_knl_exit    CONSTANT BINARY_INTEGER := 25;

  success               CONSTANT binary_integer :=  0;

  error_bogus_frame     CONSTANT binary_integer :=  1;
  error_no_debug_info   CONSTANT binary_integer :=  2;
  error_no_such_object  CONSTANT binary_integer :=  3;
  error_unknown_type    CONSTANT binary_integer :=  4;
  error_indexed_table   CONSTANT binary_integer := 18;

  error_illegal_index   CONSTANT binary_integer := 19;

  error_nullcollection  CONSTANT binary_integer := 40;

  error_nullvalue     CONSTANT binary_integer := 32;

  error_illegal_value   CONSTANT binary_integer :=  5;
  error_illegal_null    CONSTANT binary_integer :=  6;
  error_value_malformed CONSTANT binary_integer :=  7;
  error_other           CONSTANT binary_integer :=  8;
  error_name_incomplete CONSTANT binary_integer := 11;

  error_illegal_line    CONSTANT binary_integer := 12;
  error_no_such_breakpt CONSTANT binary_integer := 13;
  error_idle_breakpt    CONSTANT binary_integer := 14;
  error_stale_breakpt   CONSTANT binary_integer := 15;

  error_bad_handle      CONSTANT binary_integer := 16;

  error_unimplemented   CONSTANT binary_integer := 17;
  error_deferred        CONSTANT binary_integer := 27;

  error_exception     CONSTANT binary_integer := 28;
  error_communication CONSTANT binary_integer := 29;
  error_timeout       CONSTANT binary_integer := 31;

  error_pbrun_mismatch  CONSTANT binary_integer :=  9;
  error_no_rph          CONSTANT binary_integer := 10;
  error_probe_invalid   CONSTANT binary_integer := 20;
  error_upierr          CONSTANT binary_integer := 21;
  error_noasync         CONSTANT binary_integer := 22;
  error_nologon         CONSTANT binary_integer := 23;
  error_reinit          CONSTANT binary_integer := 24;
  error_unrecognized    CONSTANT binary_integer := 25;
  error_synch           CONSTANT binary_integer := 26;
  error_incompatible    CONSTANT binary_integer := 30;

  retry_on_timeout      CONSTANT BINARY_INTEGER := 0;
  continue_on_timeout   CONSTANT BINARY_INTEGER := 1;
  nodebug_on_timeout    CONSTANT BINARY_INTEGER := 2;
  abort_on_timeout      CONSTANT BINARY_INTEGER := 3;

  illegal_init         EXCEPTION;

  desupported             EXCEPTION;

  unimplemented           EXCEPTION;
  target_error            EXCEPTION;

  no_target_program       EXCEPTION;

  default_timeout  BINARY_INTEGER := 3600;

  PROCEDURE probe_version(major out BINARY_INTEGER,
                          minor out BINARY_INTEGER);

  FUNCTION set_timeout(timeout BINARY_INTEGER) RETURN BINARY_INTEGER;

  FUNCTION initialize(debug_session_id  IN VARCHAR2       := NULL,
                      diagnostics       IN BINARY_INTEGER := 0,
                      debug_role        IN VARCHAR2       := NULL,
                      debug_role_pwd    IN VARCHAR2       := NULL)
    RETURN VARCHAR2;

  PROCEDURE debug_on(no_client_side_plsql_engine BOOLEAN := TRUE,
                     immediate                   BOOLEAN := FALSE);

  PROCEDURE debug_off;

  PROCEDURE set_timeout_behaviour(behaviour IN PLS_INTEGER);

  FUNCTION get_timeout_behaviour RETURN BINARY_INTEGER;

  PROCEDURE attach_session(debug_session_id  IN VARCHAR2,
                           diagnostics       IN BINARY_INTEGER := 0);

  FUNCTION synchronize(run_info       OUT  runtime_info,
                       info_requested IN   BINARY_INTEGER := NULL)
    RETURN BINARY_INTEGER;

  PROCEDURE print_backtrace(listing IN OUT VARCHAR);

  PROCEDURE print_backtrace(backtrace OUT backtrace_table);

  FUNCTION continue(run_info       IN OUT runtime_info,
                    breakflags     IN     BINARY_INTEGER,
                    info_requested IN     BINARY_INTEGER := null)
    RETURN BINARY_INTEGER;

  FUNCTION set_breakpoint(program     IN  program_info,
                          line#       IN  BINARY_INTEGER,
                          breakpoint# OUT BINARY_INTEGER,
                          fuzzy       IN  BINARY_INTEGER := 0,
                          iterations  IN  BINARY_INTEGER := 0)
    RETURN BINARY_INTEGER;

  FUNCTION delete_breakpoint(breakpoint IN BINARY_INTEGER)
    RETURN BINARY_INTEGER;

  FUNCTION disable_breakpoint(breakpoint IN BINARY_INTEGER)
    RETURN BINARY_INTEGER;

  FUNCTION enable_breakpoint(breakpoint IN BINARY_INTEGER)
    RETURN BINARY_INTEGER;

  PROCEDURE show_breakpoints(listing    IN OUT VARCHAR2);

  PROCEDURE show_breakpoints(listing  OUT breakpoint_table);

  FUNCTION get_value(variable_name  IN  VARCHAR2,
                     frame#         IN  BINARY_INTEGER,
                     scalar_value   OUT VARCHAR2,
                     format         IN  VARCHAR2 := NULL)
    RETURN BINARY_INTEGER;

  FUNCTION get_value(variable_name  IN  VARCHAR2,
                     handle         IN  program_info,
                     scalar_value   OUT VARCHAR2,
                     format         IN  VARCHAR2 := NULL)
     RETURN BINARY_INTEGER;

  PROCEDURE detach_session;

  FUNCTION get_runtime_info(info_requested  IN  BINARY_INTEGER,
                            run_info        OUT runtime_info)
    RETURN BINARY_INTEGER;

  FUNCTION target_program_running RETURN BOOLEAN;

  PROCEDURE ping;

    FUNCTION get_line_map(program IN program_info,
                          maxline OUT BINARY_INTEGER,
                          number_of_entry_points OUT BINARY_INTEGER,
                          linemap OUT raw)
      RETURN binary_integer;

    FUNCTION get_values(scalar_values OUT VARCHAR2,
                        frame IN BINARY_INTEGER := 0)
      RETURN binary_integer;

END DBMS_DEBUG;
//
