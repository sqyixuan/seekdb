CREATE OR REPLACE PACKAGE dbms_ischeduler IS

  FUNCTION  GET_AND_INCREASE_JOB_ID RETURN VARCHAR2;

  PROCEDURE create_job( job_name           IN  VARCHAR2,
                        job_style          IN  VARCHAR2,
                        program_type       IN  VARCHAR2,
                        program_action     IN  VARCHAR2,
                        number_of_argument IN  BINARY_INTEGER,
                        schedule_type      IN  VARCHAR2,
                        schedule_expr      IN  VARCHAR2,
                        queue_spec         IN  VARCHAR2,
                        start_date         IN  TIMESTAMP_UNCONSTRAINED,
                        end_date           IN  TIMESTAMP_UNCONSTRAINED,
                        job_class          IN  VARCHAR2,
                        comments           IN  VARCHAR2,
                        enabled            IN  BOOLEAN,
                        auto_drop          IN  BOOLEAN,
                        invoker            IN  VARCHAR2,
                        sys_privs          IN  PLS_INTEGER,
                        aq_job             IN  BOOLEAN,
                        destination_name   IN  VARCHAR2,
                        credential         IN  VARCHAR2,
                        max_run_duration   IN  BINARY_INTEGER);

  PROCEDURE drop_job      ( job_name          IN  VARCHAR2,
                            force             IN  BOOLEAN DEFAULT FALSE,
                            defer             IN  BOOLEAN DEFAULT FALSE,
                            commit_semantics  IN  VARCHAR2 DEFAULT 'STOP_ON_FIRST_ERROR');


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
                                         out_argument       IN  BOOLEAN DEFAULT FALSE,
                                         job_name           IN  VARCHAR2,
                                         is_for_default     IN  BOOLEAN);

  PROCEDURE drop_program (  program_name  IN  VARCHAR2,
                            force         IN  BOOLEAN DEFAULT FALSE);

  PROCEDURE set_job_attribute ( name       IN  VARCHAR2,
                                attribute  IN  VARCHAR2,
                                value      IN  VARCHAR2);

  PROCEDURE run_job ( job_name  IN  VARCHAR2,
                      force     IN  BOOLEAN);

  PROCEDURE stop_job ( job_name  IN  VARCHAR2,
                      force     IN  BOOLEAN);

  PROCEDURE enable (job_name IN VARCHAR2);

  PROCEDURE disable ( job_name          IN VARCHAR2,
                      force             IN BOOLEAN,
                      commit_semantics  IN  VARCHAR2);

  PROCEDURE create_job_class ( job_class_name            IN VARCHAR2,
                               resource_consumer_group   IN VARCHAR2 DEFAULT NULL,
                               service                   IN VARCHAR2 DEFAULT NULL,
                               logging_level             IN VARCHAR2 DEFAULT 'RUNS',
                               log_history               IN PLS_INTEGER DEFAULT NULL,
                               comments                  IN VARCHAR2 DEFAULT NULL);
  
  PROCEDURE drop_job_class ( job_class_name          IN VARCHAR2,
                             force                   IN BOOLEAN DEFAULT FALSE);

  PROCEDURE set_job_class_attribute ( job_class_name  IN  VARCHAR2,
                                      name            IN  VARCHAR2,
                                      value           IN  VARCHAR2);
                                      
  PROCEDURE purge_log( log_history  IN PLS_INTEGER,
                        which_log   IN VARCHAR2,
                        job_name    IN VARCHAR2);
END;
//
