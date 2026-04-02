CREATE OR REPLACE PACKAGE BODY dbms_scheduler IS

  FUNCTION GENERATE_JOB_NAME(in_type_name VARCHAR2 DEFAULT 'JOB$_') RETURN VARCHAR2 IS
    JID VARCHAR2(128);
    JNAME VARCHAR2(128);
  BEGIN
    JID := DBMS_ISCHEDULER.GET_AND_INCREASE_JOB_ID;
    JNAME := in_type_name || TO_CHAR(JID);
    RETURN JNAME;
  END;

  FUNCTION IS_JOB_CLASS_ATTR(attribute VARCHAR2) RETURN BOOLEAN IS
    ANAME VARCHAR2(30) := UPPER(attribute);
  BEGIN
    IF ANAME = 'LOGGING_LEVEL' THEN
      RETURN (TRUE);
    ELSIF ANAME = 'LOG_HISTORY' THEN
      RETURN (TRUE);
    ELSE
      RETURN (FALSE);
    END IF;
  END;
  

  PROCEDURE create_job    ( job_name            IN  VARCHAR2,
                            job_type            IN  VARCHAR2,
                            job_action          IN  VARCHAR2,
                            number_of_argument  IN  BINARY_INTEGER DEFAULT 0,
                            start_date          IN  TIMESTAMP_UNCONSTRAINED DEFAULT NULL,
                            repeat_interval     IN  VARCHAR2 DEFAULT NULL,
                            end_date            IN  TIMESTAMP_UNCONSTRAINED DEFAULT NULL,
                            job_class           IN  VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS',
                            enabled             IN  BOOLEAN DEFAULT FALSE,
                            auto_drop           IN  BOOLEAN DEFAULT TRUE,
                            comments            IN  VARCHAR2 DEFAULT NULL,
                            credential_name     IN  VARCHAR2 DEFAULT NULL,
                            destination_name    IN  VARCHAR2 DEFAULT NULL,
                            max_run_duration    IN  BINARY_INTEGER DEFAULT 0) 
  IS
    SYS_PRIVS      BINARY_INTEGER := 0;
    SCHEDULE_TYPE  VARCHAR2(30);
    final_start_date TIMESTAMP_UNCONSTRAINED;
    final_end_date TIMESTAMP_UNCONSTRAINED;
  BEGIN

    IF start_date IS NULL AND repeat_interval IS NULL THEN
      SCHEDULE_TYPE := 'IMMEDIATE';
    ELSIF repeat_interval IS NULL THEN
      SCHEDULE_TYPE := 'ONCE';
    ELSE
      SCHEDULE_TYPE := NULL;
    END IF;

    final_start_date := NVL(SYS_EXTRACT_UTC(create_job.start_date), SYS_EXTRACT_UTC(SYSTIMESTAMP));
    final_end_date := NVL(SYS_EXTRACT_UTC(create_job.end_date), SYS_EXTRACT_UTC(TIMESTAMP '4000-01-01 00:00:00'));

    DBMS_ISCHEDULER.create_job(job_name, 'REGULAR', UPPER(job_type), job_action,
                              number_of_argument, SCHEDULE_TYPE, UPPER(repeat_interval), null, final_start_date, 
                              final_end_date, job_class, comments, enabled, auto_drop,
                              SYS_CONTEXT('USERENV','CURRENT_USER'), SYS_PRIVS, FALSE,
                              destination_name, credential_name, max_run_duration);
  END;

  PROCEDURE create_job    ( job_name            IN  VARCHAR2,
                            program_name        IN  VARCHAR2,
                            start_date          IN  TIMESTAMP_UNCONSTRAINED DEFAULT NULL,
                            repeat_interval     IN  VARCHAR2 DEFAULT NULL,
                            end_date            IN  TIMESTAMP_UNCONSTRAINED DEFAULT NULL,
                            job_class           IN  VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS',
                            enabled             IN  BOOLEAN DEFAULT FALSE,
                            auto_drop           IN  BOOLEAN DEFAULT TRUE,
                            comments            IN  VARCHAR2 DEFAULT NULL,
                            job_style           IN  VARCHAR2 DEFAULT 'REGULAR',
                            credential_name     IN  VARCHAR2 DEFAULT NULL,
                            destination_name    IN  VARCHAR2 DEFAULT NULL,
                            max_run_duration    IN  BINARY_INTEGER DEFAULT 0) 
  IS
    SYS_PRIVS      BINARY_INTEGER := 0;
    SCHEDULE_TYPE  VARCHAR2(30);
    final_start_date TIMESTAMP_UNCONSTRAINED;
    final_end_date TIMESTAMP_UNCONSTRAINED;
  BEGIN

    IF start_date IS NULL AND repeat_interval IS NULL THEN
      SCHEDULE_TYPE := 'IMMEDIATE';
    ELSIF repeat_interval IS NULL THEN
      SCHEDULE_TYPE := 'ONCE';
    ELSE
      SCHEDULE_TYPE := NULL;
    END IF;

    final_start_date := NVL(SYS_EXTRACT_UTC(create_job.start_date), SYS_EXTRACT_UTC(SYSTIMESTAMP));
    final_end_date := NVL(SYS_EXTRACT_UTC(create_job.end_date), SYS_EXTRACT_UTC(TIMESTAMP '4000-01-01 00:00:00'));

    DBMS_ISCHEDULER.create_job(job_name, UPPER(job_style), 'NAMED', program_name,
                              0, SCHEDULE_TYPE, UPPER(repeat_interval), null, final_start_date, 
                              final_end_date, job_class, comments, enabled, auto_drop,
                              SYS_CONTEXT('USERENV','CURRENT_USER'), SYS_PRIVS, FALSE,
                              destination_name, credential_name, max_run_duration);
  END;

  PROCEDURE drop_job      ( job_name            IN  VARCHAR2,
                            force               IN  BOOLEAN DEFAULT FALSE,
                            defer               IN  BOOLEAN DEFAULT FALSE,
                            commit_semantics    IN  VARCHAR2 DEFAULT 'STOP_ON_FIRST_ERROR') IS
  BEGIN
    DBMS_ISCHEDULER.drop_job(job_name, force, defer, commit_semantics);
  END;


  PROCEDURE create_program    ( program_name        IN  VARCHAR2,
                                program_type        IN  VARCHAR2,
                                program_action      IN  VARCHAR2,
                                number_of_arguments IN  PLS_INTEGER DEFAULT 0,
                                enabled             IN  BOOLEAN DEFAULT FALSE,
                                comments            IN  VARCHAR2 DEFAULT NULL) IS
  BEGIN
    DBMS_ISCHEDULER.create_program(program_name, program_type, program_action, number_of_arguments, enabled, comments);
  END;

  PROCEDURE define_program_argument    ( program_name       IN  VARCHAR2,
                                         argument_position  IN  PLS_INTEGER,
                                         argument_name      IN  VARCHAR2 DEFAULT NULL,
                                         argument_type      IN  VARCHAR2,
                                         default_value      IN  VARCHAR2,
                                         out_argument       IN  BOOLEAN DEFAULT FALSE) IS
    job_name VARCHAR2(128) := 'default';
    is_for_default BOOLEAN := TRUE;
  BEGIN
    DBMS_ISCHEDULER.define_program_argument(program_name, argument_position, argument_name, argument_type, default_value, out_argument, job_name, is_for_default);
  END;

  PROCEDURE drop_program (  program_name  IN  VARCHAR2,
                            force         IN  BOOLEAN DEFAULT FALSE) IS
  BEGIN
    DBMS_ISCHEDULER.drop_program(program_name, force);
  END;

  PROCEDURE set_attribute ( name      IN  VARCHAR2,
                            attribute IN  VARCHAR2,
                            value     IN  VARCHAR2) IS
  BEGIN
    IF IS_JOB_CLASS_ATTR(attribute) THEN
      DBMS_ISCHEDULER.set_job_class_attribute(name, attribute, value);
    ELSE -- default set_job_attribute
      DBMS_ISCHEDULER.set_job_attribute( name, attribute, value);
    END IF;
  END;

  PROCEDURE run_job ( job_name  IN  VARCHAR2,
                      force     IN  BOOLEAN) IS
  BEGIN
    DBMS_ISCHEDULER.run_job( job_name, force );
  END;

  PROCEDURE stop_job ( job_name  IN  VARCHAR2,
                      force     IN  BOOLEAN DEFAULT FALSE) IS
  BEGIN
    DBMS_ISCHEDULER.stop_job( job_name, force );
  END;

  PROCEDURE set_job_argument_value (  job_name          IN  VARCHAR2,
                                      argument_position IN  PLS_INTEGER,
                                      argument_value    IN  VARCHAR2) IS
    program_name VARCHAR2(128) := NULL;
    argument_name VARCHAR2(128) := NULL;
    argument_type VARCHAR2(128) := NULL;
    out_argument BOOLEAN := FALSE;
    is_for_default BOOLEAN := FALSE;
  BEGIN
    DBMS_ISCHEDULER.define_program_argument(program_name, argument_position, argument_name, argument_type, argument_value, out_argument, job_name, is_for_default);
  END;

  PROCEDURE set_job_argument_value (  job_name          IN  VARCHAR2,
                                      argument_name     IN  VARCHAR2,
                                      argument_value    IN  VARCHAR2) IS
    program_name VARCHAR2(128) := NULL;
    argument_position PLS_INTEGER := 0;
    argument_type VARCHAR2(128) := NULL;
    out_argument BOOLEAN := FALSE;
    is_for_default BOOLEAN := FALSE;
  BEGIN
    DBMS_ISCHEDULER.define_program_argument(program_name, argument_position, argument_name, argument_type, argument_value, out_argument, job_name, is_for_default);
  END;

  PROCEDURE enable ( job_name IN  VARCHAR2) IS
  BEGIN
    DBMS_ISCHEDULER.enable( job_name);
  END;

  PROCEDURE disable ( job_name          IN VARCHAR2,
                      force             IN BOOLEAN DEFAULT FALSE,
                      commit_semantics  IN  VARCHAR2 DEFAULT  'STOP_ON_FIRST_ERROR') IS
  BEGIN
    DBMS_ISCHEDULER.disable( job_name, force, commit_semantics);
  END;
  
  PROCEDURE create_job_class ( job_class_name            IN VARCHAR2,
                               resource_consumer_group   IN VARCHAR2 DEFAULT NULL,
                               service                   IN VARCHAR2 DEFAULT NULL,
                               logging_level             IN VARCHAR2 DEFAULT 'RUNS', 
                               log_history               IN PLS_INTEGER DEFAULT 30,
                               comments                  IN VARCHAR2 DEFAULT NULL) IS
  BEGIN
    DBMS_ISCHEDULER.create_job_class(job_class_name, resource_consumer_group, service, logging_level, log_history, comments);
  END;

  PROCEDURE drop_job_class ( job_class_name          IN VARCHAR2,
                             force                   IN BOOLEAN DEFAULT FALSE) IS
  BEGIN
    IF force = TRUE THEN
      RAISE_APPLICATION_ERROR(-20000, 'OBE-23422: not support set force = true');
    END IF;
    DBMS_ISCHEDULER.drop_job_class(job_class_name, force);
  END;

  PROCEDURE purge_log( log_history  IN PLS_INTEGER DEFAULT 0,
                       which_log    IN VARCHAR2 DEFAULT 'JOB_AND_WINDOW_LOG',
                       job_name     IN VARCHAR2 DEFAULT NULL) IS 
  BEGIN
    DBMS_ISCHEDULER.purge_log(log_history, which_log, job_name);
  END;
END;
//
