#package_name: dbms_preprocessor
#author: linlin.xll

create or replace package dbms_preprocessor authid current_user as

  type source_lines_t is
    table of varchar2(32767) index by binary_integer;

  empty_input exception;
  pragma exception_init(empty_input, -5960);

  procedure print_post_processed_source(object_type varchar2,
                                        schema_name varchar2,
                                        object_name varchar2);

  procedure print_post_processed_source(source varchar2);

  procedure print_post_processed_source(source source_lines_t);

  function get_post_processed_source(object_type varchar2,
                                     schema_name varchar2,
                                     object_name varchar2)
    return source_lines_t;

  function get_post_processed_source(source varchar2)
    return source_lines_t;

  function get_post_processed_source(source source_lines_t)
    return source_lines_t;

end;
//
