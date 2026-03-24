CREATE OR REPLACE PACKAGE dbms_scheduler AUTHID CURRENT_USER IS

  LOGGING_RUNS         CONSTANT   VARCHAR2(12) := 'RUNS';
  LOGGING_OFF          CONSTANT   VARCHAR2(12) := 'OFF';
  LOGGING_FAILED_RUNS  CONSTANT   VARCHAR2(12) := 'FAILED RUNS';

  FUNCTION GENERATE_JOB_NAME(in_type_name VARCHAR2 DEFAULT 'JOB$_') RETURN VARCHAR2;

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
                            max_run_duration    IN  BINARY_INTEGER DEFAULT 0);

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
                            max_run_duration    IN  BINARY_INTEGER DEFAULT 0);

  PROCEDURE drop_job      ( job_name            IN  VARCHAR2,
                            force               IN  BOOLEAN DEFAULT FALSE,
                            defer               IN  BOOLEAN DEFAULT FALSE,
                            commit_semantics    IN  VARCHAR2 DEFAULT 'STOP_ON_FIRST_ERROR');


  PROCEDURE create_program    ( program_name        IN  VARCHAR2,
                                program_type        IN  VARCHAR2,
                                program_action      IN  VARCHAR2,
                                number_of_arguments IN  PLS_INTEGER DEFAULT 0,
                                enabled             IN  BOOLEAN DEFAULT FALSE,
                                comments            IN  VARCHAR2 DEFAULT NULL);

  PROCEDURE define_program_argument    ( program_name       IN  VARCHAR2,
                                         argument_position  IN  PLS_INTEGER,
                                         argument_name      IN  VARCHAR2 DEFAULT NULL,
                                         argument_type      IN  VARCHAR2,
                                         default_value      IN  VARCHAR2,
                                         out_argument       IN  BOOLEAN DEFAULT FALSE);

  PROCEDURE drop_program (  program_name  IN  VARCHAR2,
                            force         IN  BOOLEAN DEFAULT FALSE);

  PROCEDURE set_attribute ( name      IN  VARCHAR2,
                            attribute IN  VARCHAR2,
                            value     IN  VARCHAR2);

  PROCEDURE run_job ( job_name  IN  VARCHAR2,
                      force     IN  BOOLEAN);

  PROCEDURE stop_job ( job_name IN VARCHAR2,
                       force    IN BOOLEAN DEFAULT FALSE);

  PROCEDURE set_job_argument_value (  job_name          IN  VARCHAR2,
                                      argument_position IN  PLS_INTEGER,
                                      argument_value    IN  VARCHAR2);

  PROCEDURE set_job_argument_value (  job_name          IN  VARCHAR2,
                                      argument_name     IN  VARCHAR2,
                                      argument_value    IN  VARCHAR2);

  PROCEDURE enable ( job_name IN  VARCHAR2);

  PROCEDURE disable ( job_name          IN VARCHAR2,
                      force             IN BOOLEAN DEFAULT FALSE,
                      commit_semantics  IN VARCHAR2 DEFAULT  'STOP_ON_FIRST_ERROR');

  PROCEDURE create_job_class ( job_class_name            IN VARCHAR2,
                               resource_consumer_group   IN VARCHAR2 DEFAULT NULL,
                               service                   IN VARCHAR2 DEFAULT NULL,
                               logging_level             IN VARCHAR2 DEFAULT 'RUNS',
                               log_history               IN PLS_INTEGER DEFAULT 30,
                               comments                  IN VARCHAR2 DEFAULT NULL);
  
  PROCEDURE drop_job_class ( job_class_name          IN VARCHAR2,
                             force                   IN BOOLEAN DEFAULT FALSE);

  PROCEDURE purge_log( log_history  IN PLS_INTEGER DEFAULT 0,
                       which_log    IN VARCHAR2 DEFAULT 'JOB_AND_WINDOW_LOG',
                       job_name     IN VARCHAR2 DEFAULT NULL);
  
END;
//
