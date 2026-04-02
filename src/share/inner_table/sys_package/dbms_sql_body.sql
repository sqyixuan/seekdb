#package_name:dbms_sql
#author: ethan.xy

CREATE OR REPLACE PACKAGE BODY DBMS_SQL IS

  FUNCTION OPEN_CURSOR
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_sql_open_cursor);

  PROCEDURE PARSE(cursor_id       IN INTEGER,
                  sql_stmt        IN VARCHAR2,
                  language_flag   IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_parse);

  PROCEDURE PARSE(c IN INTEGER,
                  statement IN VARCHAR2s,
                  lb IN INTEGER,
                  ub IN INTEGER,
                  lfflg IN BOOLEAN,
                  language_flag IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_parse);

  PROCEDURE PARSE(c in integer, statement in varchar2a,
                lb in integer, ub in integer,
                lfflg in boolean, language_flag in integer);
  PRAGMA INTERFACE(c, dbms_sql_parse);
  
  PROCEDURE BIND_VARIABLE(cursor_id   IN INTEGER,
                          name        IN VARCHAR2,
                          value       IN NUMBER);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE(cursor_id   IN INTEGER,
                          name        IN VARCHAR2,
                          value       IN VARCHAR2);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN VARCHAR2, OUT_VALUE_SIZE IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN DATE);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN TIMESTAMP_UNCONSTRAINED);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN BLOB);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE_CHAR(C IN INTEGER, NAME IN VARCHAR2, VALUE IN CHAR);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE_CHAR(C IN INTEGER, NAME IN VARCHAR2, VALUE IN CHAR, OUT_VALUE_SIZE IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE_RAW(C IN INTEGER, NAME IN VARCHAR2, VALUE IN RAW);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE_RAW(C IN INTEGER, NAME IN VARCHAR2, VALUE IN RAW, OUT_VALUE_SIZE IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_bind_variable);

  FUNCTION  EXECUTE(cursor_id   IN INTEGER)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_sql_execute);

  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN NUMBER);
  PRAGMA INTERFACE(c, dbms_sql_define_column);

  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN VARCHAR2);
  PRAGMA INTERFACE(c, dbms_sql_define_column);  

  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN VARCHAR2,
                          column_size IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_define_column);   

  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN DATE);
  PRAGMA INTERFACE(c, dbms_sql_define_column);   

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in binary_float);  
  PRAGMA INTERFACE(c, dbms_sql_define_column);                               

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in binary_double);    
  PRAGMA INTERFACE(c, dbms_sql_define_column);                           

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in blob);  
  PRAGMA INTERFACE(c, dbms_sql_define_column);                           

  procedure DEFINE_COLUMN_RAW(c in integer, 
                              position in integer,
                              column in raw,
                              column_size IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_define_column);      
  
  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             n_tab in Number_Table,
			             cnt in integer,
			             lower_bound in integer);
  PRAGMA INTERFACE(c, dbms_sql_define_array);    
			             
  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             c_tab in Varchar2_Table,
			             cnt in integer,
			             lower_bound in integer);
  PRAGMA INTERFACE(c, dbms_sql_define_array);   
			             
  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             d_tab in Date_Table,
			             cnt in integer,
			             lower_bound in integer);
  PRAGMA INTERFACE(c, dbms_sql_define_array);   
			             
  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             bl_tab in Blob_Table,
			             cnt in integer,
			             lower_bound in integer);
  PRAGMA INTERFACE(c, dbms_sql_define_array);   		         
			             
  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             cl_tab in Clob_Table,
			             cnt in integer,
			             lower_bound in integer);
  PRAGMA INTERFACE(c, dbms_sql_define_array);   		             
			             
--  procedure DEFINE_ARRAY(c in integer,
--                         position in integer,
--			             bf_tab in Bfile_Table,
--			             cnt in integer,
--			             lower_bound in integer);
--  PRAGMA INTERFACE(c, dbms_sql_define_array);                                        
  
  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT NUMBER);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE(C IN INTEGER,
                         POSITION IN INTEGER,
                         VALUE OUT NUMBER,
                         COLUMN_ERROR OUT NUMBER,
                         ACTUAL_LENGTH OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT VARCHAR2);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE(C IN INTEGER,
                         POSITION IN INTEGER,
                         VALUE OUT VARCHAR2,
                         COLUMN_ERROR OUT NUMBER,
                         ACTUAL_LENGTH OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT DATE);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE(C IN INTEGER,
                         POSITION IN INTEGER,
                         VALUE OUT DATE,
                         COLUMN_ERROR OUT NUMBER,
                         ACTUAL_LENGTH OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT binary_float);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT binary_double);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT blob);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT clob);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE_RAW (c IN INTEGER,
                              position IN INTEGER,
                              value OUT raw);
  PRAGMA INTERFACE(c, dbms_sql_column_value);
  
  PROCEDURE COLUMN_VALUE_RAW(c IN INTEGER, position IN INTEGER, value OUT RAW,
                             column_error OUT NUMBER,
                             actual_length OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  n_tab IN OUT nocopy Number_table);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  c_tab IN OUT nocopy Varchar2_table);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  d_tab IN OUT nocopy Date_table);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  bl_tab IN OUT nocopy Blob_table);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  cl_tab IN OUT nocopy Clob_table);
  PRAGMA INTERFACE(c, dbms_sql_column_value);
  
  PROCEDURE DESCRIBE_COLUMNS(c IN INTEGER,
  							 col_cnt OUT INTEGER,
                             desc_t OUT DESC_TAB);
  PRAGMA INTERFACE(c, dbms_sql_describe_columns);     
  
  PROCEDURE DESCRIBE_COLUMNS2(c IN INTEGER,
  							 col_cnt OUT INTEGER,
                             desc_t OUT DESC_TAB2);
  PRAGMA INTERFACE(c, dbms_sql_describe_columns2); 
                             
  PROCEDURE DESCRIBE_COLUMNS3(c IN INTEGER,
  							 col_cnt OUT INTEGER,
                             desc_t OUT DESC_TAB3);  
  PRAGMA INTERFACE(c, dbms_sql_describe_columns3);                  

  FUNCTION FETCH_ROWS (c IN INTEGER)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_sql_fetch_rows);

  PROCEDURE CLOSE_CURSOR(cursor_id    IN OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_close_cursor);

  FUNCTION IS_OPEN(C IN INTEGER) RETURN BOOLEAN;
  PRAGMA INTERFACE(c, dbms_sql_is_open);

  FUNCTION EXECUTE_AND_FETCH(C IN INTEGER, EXACT IN BOOLEAN DEFAULT FALSE)
  RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_sql_execute_and_fetch);

  PROCEDURE DEFINE_COLUMN_LONG(C IN INTEGER, POSITION IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_define_column_long);

  PROCEDURE COLUMN_VALUE_LONG(C IN INTEGER, POSITION IN INTEGER,
                              LENGTH IN INTEGER, OFFSET IN INTEGER,
                              VALUE OUT VARCHAR2, VALUE_LENGTH OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_column_value_long);

  PROCEDURE DEFINE_COLUMN_CHAR(C IN INTEGER, POSITION IN INTEGER,
                               COLUMN IN CHAR,
                               COLUMN_SIZE IN INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_define_column);

  PROCEDURE COLUMN_VALUE_CHAR(C IN INTEGER, POSITION IN INTEGER,
                              VALUE OUT CHAR);
  PRAGMA INTERFACE(c, dbms_sql_column_value);
  
  PROCEDURE COLUMN_VALUE_CHAR(C IN INTEGER,
                              POSITION IN INTEGER,
                              VALUE OUT CHAR,
                              COLUMN_ERROR OUT NUMBER,
                              ACTUAL_LENGTH OUT INTEGER);
  PRAGMA INTERFACE(c, dbms_sql_column_value);

  PROCEDURE VARIABLE_VALUE_CHAR(C IN INTEGER, NAME IN VARCHAR2,
                                VALUE OUT CHAR);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE_RAW(C IN INTEGER, NAME IN VARCHAR2,
                               VALUE OUT RAW);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT NUMBER);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT VARCHAR2);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT DATE);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, value OUT BLOB);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT CLOB);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, value OUT BFILE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY NUMBER_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY VARCHAR2_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY DATE_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY BLOB_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY CLOB_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY BFILE_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT UROWID);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY  UROWID_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIME_UNCONSTRAINED);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIME_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT TIMESTAMP_UNCONSTRAINED);
  PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIME_TZ_UNCONSTRAINED );
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIME_WITH_TIME_ZONE_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIMESTAMP_TZ_UNCONSTRAINED);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIMESTAMP_WITH_TIME_ZONE_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIMESTAMP_LTZ_UNCONSTRAINED);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIMESTAMP_WITH_LTZ_TABLE);
  -- PRAGMA INTERFACE(c, dbms_sql_variable_value);
  
  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT YMINTERVAL_UNCONSTRAINED);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY INTERVAL_YEAR_TO_MONTH_TABLE);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT DSINTERVAL_UNCONSTRAINED);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY INTERVAL_DAY_TO_SECOND_TABLE);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT BINARY_FLOAT);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY BINARY_FLOAT_TABLE);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT BINARY_DOUBLE);
  
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY BINARY_DOUBLE_TABLE);

  FUNCTION TO_CURSOR_NUMBER(rc IN OUT SYS_REFCURSOR) RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_sql_to_cursor_number);

  FUNCTION TO_REFCURSOR(cursor_number in out INTEGER) RETURN SYS_REFCURSOR;
  PRAGMA INTERFACE(c, dbms_sql_to_refcursor);

  FUNCTION LAST_ERROR_POSITION RETURN INTEGER;
  PRAGMA INTERFACE(c, dbms_sql_last_error_position);

  PROCEDURE BIND_VARIABLE(c IN INTEGER, NAME IN VARCHAR2, VALUE IN "<COLLECTION_1>");
  pragma interface(c, dbms_sql_bind_variable);

  PROCEDURE BIND_VARIABLE(c IN INTEGER, NAME IN VARCHAR2, VALUE IN "<ADT_1>");
  pragma interface(c, dbms_sql_bind_variable);
  
END DBMS_SQL;
//

