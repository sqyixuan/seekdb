#package_name:dbms_session
#author: peihan.dph

CREATE OR REPLACE PACKAGE DBMS_SESSION IS
-- procedure set_role(role_cmd varchar2);
-- procedure set_sql_trace(sql_trace boolean);
-- procedure set_nls(param varchar2, value varchar2);
-- procedure close_database_link(dblink varchar2);
procedure reset_package;
-- FREE_ALL_RESOURCES   constant PLS_INTEGER := 1;
-- REINITIALIZE         constant PLS_INTEGER := 2;
-- procedure modify_package_state(action_flags IN PLS_INTEGER);
-- function unique_session_id return varchar2;
--   pragma restrict_references(unique_session_id,WNDS,RNDS,WNPS);
-- function is_role_enabled(rolename varchar2) return boolean;
-- pragma deprecate(IS_ROLE_ENABLED, 'Use DBMS_SESSION.CURRENT_IS_ROLE_ENABLED 
--                   or DBMS_SESSION.SESSION_IS_ROLE_ENABLED instead.');
-- function current_is_role_enabled(rolename varchar2) return boolean;
-- function session_is_role_enabled(rolename varchar2) return boolean;
-- function is_session_alive(uniqueid varchar2) return boolean;
-- procedure set_close_cached_open_cursors(close_cursors boolean);
-- procedure free_unused_user_memory;
PROCEDURE SET_CONTEXT(NAMESPACE VARCHAR2, ATTRIBUTE VARCHAR2, VALUE VARCHAR2,
                      USERNAME VARCHAR2 DEFAULT NULL,
                      CLIENT_ID VARCHAR2 DEFAULT NULL);
  --  Input arguments:
  --    namespace
  --      Name of the namespace to use for the application context
  --    attribute
  --      Name of the attribute to be set
  --    value
  --      Value to be set
  --    username
  --      username attribute for application context . default value is null. 
  --    client_id
  --      client identifier that identifies a user session for which we need
  --      to set this context.
PROCEDURE SET_IDENTIFIER(CLIENT_ID VARCHAR2);
  --    Input parameters: 
  --    client_id
  --      client identifier being set for this session .
PROCEDURE CLEAR_CONTEXT(NAMESPACE VARCHAR2, CLIENT_ID VARCHAR2 DEFAULT NULL,
                          ATTRIBUTE VARCHAR2 DEFAULT NULL);
  -- Input parameters:
  --   namespace
  --     namespace where the application context is to be cleared 
  --   client_id 
  --      all ns contexts associated with this client id are cleared.
  --   attribute
  --     attribute to clear . 
PROCEDURE CLEAR_ALL_CONTEXT(NAMESPACE VARCHAR2);
  -- Input parameters:
  --    namespace
  --      namespace where the application context is to be cleared
PROCEDURE CLEAR_IDENTIFIER;
  -- Input parameters:
  --   none


-- TYPE AppCtxRecTyp IS RECORD ( namespace varchar2(30), attribute varchar2(30),
--       value varchar2(4000));
-- TYPE AppCtxTabTyp IS TABLE OF AppCtxRecTyp INDEX BY BINARY_INTEGER;
-- procedure list_context(list OUT AppCtxTabTyp, lsize OUT number);
-- procedure switch_current_consumer_group(new_consumer_group IN VARCHAR2,
--                                           old_consumer_group OUT VARCHAR2,
--                                           initial_group_on_error IN BOOLEAN);
-- procedure session_trace_enable(waits IN BOOLEAN DEFAULT TRUE,
--                                  binds IN BOOLEAN DEFAULT FALSE,
--                                  plan_stat IN VARCHAR2 DEFAULT NULL);
-- procedure session_trace_disable;
-- procedure set_edition_deferred(edition varchar2);
-- type lname_array   IS table of VARCHAR2(4000) index by BINARY_INTEGER;
-- type integer_array IS table of BINARY_INTEGER index by BINARY_INTEGER;
-- procedure get_package_memory_utilization(
--             owner_names   OUT NOCOPY lname_array,
--             unit_names    OUT NOCOPY lname_array,
--             unit_types    OUT NOCOPY integer_array,
--             used_amounts  OUT NOCOPY integer_array,
--             free_amounts  OUT NOCOPY integer_array);
-- used_memory CONSTANT BINARY_INTEGER := 1;
-- free_memory CONSTANT BINARY_INTEGER := 2;
-- type big_integer_array IS table of INTEGER index by BINARY_INTEGER;
-- type big_integer_matrix IS table of big_integer_array
--                             index by BINARY_INTEGER;
-- procedure get_package_memory_utilization(
--             desired_info  IN         integer_array,
--             owner_names   OUT NOCOPY lname_array,
--             unit_names    OUT NOCOPY lname_array,
--             unit_types    OUT NOCOPY integer_array,
--             amounts       OUT NOCOPY big_integer_matrix);
-- procedure use_default_edition_deferred;
-- procedure use_default_edition_always(mode_on IN BOOLEAN DEFAULT TRUE);
-- procedure set_current_schema_deferred(schema_name varchar2);
-- procedure sleep(seconds in number);
END;
//
