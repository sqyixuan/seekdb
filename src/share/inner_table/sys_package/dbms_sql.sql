#package_name:dbms_sql
#author: ethan.xy

CREATE OR REPLACE PACKAGE DBMS_SQL AUTHID CURRENT_USER IS

--  CONSTANTS
  v6 constant integer := 0;
  native constant integer := 1;
  v7 constant integer := 2;
  foreign_syntax constant integer := 4294967295;

--  TYPES
  TYPE varchar2a is table of varchar2(32767) index by binary_integer;
  TYPE varchar2s is table of varchar2(256) index by binary_integer;
  TYPE desc_rec is record (
	                    col_type	    binary_integer := 0,
	                    col_max_len	    binary_integer := 0,
                      col_name	    varchar2(32)   := '',
                      col_name_len	    binary_integer := 0,
                      col_schema_name     varchar2(32)   := '',
                      col_schema_name_len binary_integer := 0,
                      col_precision	    binary_integer := 0,
                      col_scale	    binary_integer := 0,
                      col_charsetid	    binary_integer := 0,
                      col_charsetform     binary_integer := 0,
                      col_null_ok	    boolean	   := TRUE);
  TYPE desc_tab is table of desc_rec index by binary_integer;
  TYPE desc_rec2 is record (
                      col_type	    binary_integer := 0,
                      col_max_len	    binary_integer := 0,
                      col_name	    varchar2(32767) := '',
                      col_name_len	    binary_integer := 0,
                      col_schema_name     varchar2(32)   := '',
                      col_schema_name_len binary_integer := 0,
                      col_precision	    binary_integer := 0,
                      col_scale	    binary_integer := 0,
                      col_charsetid	    binary_integer := 0,
                      col_charsetform     binary_integer := 0,
                      col_null_ok	    boolean	   := TRUE);
  TYPE desc_tab2 is table of desc_rec2 index by binary_integer;

  TYPE desc_rec3 is record (
                      col_type	    binary_integer := 0,
                      col_max_len	    binary_integer := 0,
                      col_name	    varchar2(32767) := '',
                      col_name_len	    binary_integer := 0,
                      col_schema_name     varchar2(32)   := '',
                      col_schema_name_len binary_integer := 0,
                      col_precision	    binary_integer := 0,
                      col_scale	    binary_integer := 0,
                      col_charsetid	    binary_integer := 0,
                      col_charsetform     binary_integer := 0,
                      col_null_ok	    boolean	   := TRUE,
                      col_type_name	    varchar2(32)   := '',
                      col_type_name_len   binary_integer := 0);
  TYPE desc_tab3 is table of desc_rec3 index by binary_integer;

  --TYPE desc_rec4 is record (
  --                    col_type	    binary_integer := 0,
  --                    col_max_len	    binary_integer := 0,
  --                    col_name	    varchar2(32767) := '',
  --                    col_name_len	    binary_integer := 0,
  --                    col_schema_name     dbms_id	   := '',
  --                    col_schema_name_len binary_integer := 0,
  --                    col_precision	    binary_integer := 0,
  --                    col_scale	    binary_integer := 0,
  --                    col_charsetid	    binary_integer := 0,
  --                    col_charsetform     binary_integer := 0,
  --                    col_null_ok	    boolean	   := TRUE,
  --                    col_type_name	    dbms_id	   := '',
  --                    col_type_name_len   binary_integer := 0);
  --TYPE desc_tab4 is table of desc_rec4 index by binary_integer;

  TYPE Number_Table   is table of number	 index by binary_integer;
  TYPE Varchar2_Table is table of varchar2(4000) index by binary_integer;
  TYPE Date_Table     is table of date		 index by binary_integer;
  TYPE Blob_Table     is table of Blob		 index by binary_integer;
  TYPE Clob_Table     is table of Clob		 index by binary_integer;
  --TYPE Bfile_Table    is table of Bfile 	 index by binary_integer;
  TYPE Urowid_Table   IS TABLE OF urowid	 INDEX BY binary_integer;
  --TYPE time_Table     IS TABLE OF time_unconstrained	       INDEX BY binary_integer;
  --TYPE timestamp_Table	 IS TABLE OF timeenamp_unconstrained	     INDEX BY binary_integer;
  --TYPE time_with_time_zone_Table IS TABLE OF TIME_TZ_UNCONSTRAINED INDEX BY binary_integer;
  --TYPE timestamp_with_time_zone_Table IS TABLE OF TIMESTAMP_TZ_UNCONSTRAINED INDEX BY binary_integer;
  --TYPE timestamp_with_ltz_Table IS TABLE OF TIMESTAMP_LTZ_UNCONSTRAINED INDEX BY binary_integer;
  --TYPE interval_year_to_MONTH_Table IS TABLE OF yminterval_unconstrained INDEX BY binary_integer;
  --TYPE interval_day_to_second_Table IS TABLE OF dsinterval_unconstrained INDEX BY binary_integer;
  TYPE Binary_Float_Table is table of binary_float index by binary_integer;
  TYPE Binary_Double_Table is table of binary_double index by binary_integer;

-- EXCEPTIONS
  inconsistent_type exception; 
  pragma exception_init(inconsistent_type, -6562);

-- FUNCTIONS and PROCEDURES
  FUNCTION  OPEN_CURSOR
  RETURN INTEGER;

  FUNCTION IS_OPEN(C IN INTEGER) RETURN BOOLEAN;

  FUNCTION LAST_ERROR_POSITION RETURN INTEGER;
  PRAGMA RESTRICT_REFERENCES(LAST_ERROR_POSITION, RNDS, WNDS);

  FUNCTION EXECUTE_AND_FETCH(C IN INTEGER, EXACT IN BOOLEAN DEFAULT FALSE)
  RETURN INTEGER;  

  FUNCTION TO_CURSOR_NUMBER(rc IN OUT SYS_REFCURSOR) RETURN INTEGER;

  FUNCTION TO_REFCURSOR(cursor_number IN OUT INTEGER) RETURN SYS_REFCURSOR;

  PROCEDURE DEFINE_COLUMN_LONG(C IN INTEGER, POSITION IN INTEGER);

  PROCEDURE COLUMN_VALUE_LONG(C IN INTEGER, POSITION IN INTEGER,
                              LENGTH IN INTEGER, OFFSET IN INTEGER,
                              VALUE OUT VARCHAR2, VALUE_LENGTH OUT INTEGER);

  
  PROCEDURE DEFINE_COLUMN_CHAR(C IN INTEGER, POSITION IN INTEGER,
                               COLUMN IN CHAR,
                               COLUMN_SIZE IN INTEGER);
  PROCEDURE COLUMN_VALUE_CHAR(C IN INTEGER, POSITION IN INTEGER,
                              VALUE OUT CHAR);
  PROCEDURE COLUMN_VALUE_CHAR(C IN INTEGER,
                              POSITION IN INTEGER,
                              VALUE OUT CHAR,
                              COLUMN_ERROR OUT NUMBER,
                              ACTUAL_LENGTH OUT INTEGER);

  PROCEDURE PARSE(cursor_id       IN INTEGER,
                  sql_stmt        IN VARCHAR2,
                  language_flag   IN INTEGER);

  PROCEDURE PARSE(c IN INTEGER,
                  statement IN VARCHAR2s,
                  lb IN INTEGER,
                  ub IN INTEGER,
                  lfflg IN BOOLEAN,
                  language_flag IN INTEGER);
  PROCEDURE PARSE(c in integer, statement in varchar2a,
                lb in integer, ub in integer,
                lfflg in boolean, language_flag in integer);

  PROCEDURE BIND_VARIABLE(cursor_id   IN INTEGER,
                          name        IN VARCHAR2,
                          value       IN NUMBER);

  PROCEDURE BIND_VARIABLE(cursor_id   IN INTEGER,
                          name        IN VARCHAR2,
                          value       IN VARCHAR2);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN VARCHAR2, OUT_VALUE_SIZE IN INTEGER);
  
  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN DATE);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN TIMESTAMP_UNCONSTRAINED);

  PROCEDURE BIND_VARIABLE(C IN INTEGER, NAME IN VARCHAR2, VALUE IN BLOB);

  PROCEDURE BIND_VARIABLE(c IN INTEGER, NAME IN VARCHAR2, VALUE IN "<COLLECTION_1>");

  PROCEDURE BIND_VARIABLE(c IN INTEGER, NAME IN VARCHAR2, VALUE IN "<ADT_1>");

  PROCEDURE BIND_VARIABLE_CHAR(C IN INTEGER, NAME IN VARCHAR2, VALUE IN CHAR);

  PROCEDURE BIND_VARIABLE_CHAR(C IN INTEGER, NAME IN VARCHAR2, VALUE IN CHAR, OUT_VALUE_SIZE IN INTEGER);

  PROCEDURE BIND_VARIABLE_RAW(C IN INTEGER, NAME IN VARCHAR2, VALUE IN RAW);

  PROCEDURE BIND_VARIABLE_RAW(C IN INTEGER, NAME IN VARCHAR2, VALUE IN RAW, OUT_VALUE_SIZE IN INTEGER);

  FUNCTION  EXECUTE(cursor_id   IN INTEGER)
  RETURN INTEGER;


  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN NUMBER);                    

  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN VARCHAR2);      

  PROCEDURE DEFINE_COLUMN (c IN INTEGER,
                          position IN INTEGER,
                          column IN VARCHAR2,
                          column_size IN INTEGER);      

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in date); 

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in binary_float);      

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in binary_double);    

  procedure DEFINE_COLUMN(c in integer, 
                          position in integer, 
                          column in blob);  

  procedure DEFINE_COLUMN_RAW(c in integer, 
                              position in integer,
                              column in raw,
                              column_size IN INTEGER);

  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             n_tab in Number_Table,
			             cnt in integer,
			             lower_bound in integer);

  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             c_tab in Varchar2_Table,
			             cnt in integer,
			             lower_bound in integer);

  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             d_tab in Date_Table,
			             cnt in integer,
			             lower_bound in integer);

  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             bl_tab in Blob_Table,
			             cnt in integer,
			             lower_bound in integer);

  procedure DEFINE_ARRAY(c in integer,
                         position in integer,
			             cl_tab in Clob_Table,
			             cnt in integer,
			             lower_bound in integer);

--  procedure DEFINE_ARRAY(c in integer,
--                         position in integer,
--			             bf_tab in Bfile_Table,
--			             cnt in integer,
--			             lower_bound in integer);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT NUMBER);
                          
  PROCEDURE COLUMN_VALUE(C IN INTEGER,
                         POSITION IN INTEGER,
                         VALUE OUT NUMBER,
                         COLUMN_ERROR OUT NUMBER,
                         ACTUAL_LENGTH OUT INTEGER);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT VARCHAR2);
  
  PROCEDURE COLUMN_VALUE(C IN INTEGER,
                         POSITION IN INTEGER,
                         VALUE OUT VARCHAR2,
                         COLUMN_ERROR OUT NUMBER,
                         ACTUAL_LENGTH OUT INTEGER);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT date);
  
  PROCEDURE COLUMN_VALUE(C IN INTEGER,
                         POSITION IN INTEGER,
                         VALUE OUT DATE,
                         COLUMN_ERROR OUT NUMBER,
                         ACTUAL_LENGTH OUT INTEGER);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT binary_float);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT binary_double);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT blob);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
                          value OUT clob);

  PROCEDURE COLUMN_VALUE_RAW (c IN INTEGER,
                              position IN INTEGER,
                              value OUT raw);
  PROCEDURE COLUMN_VALUE_RAW (c IN INTEGER, position IN INTEGER, value OUT RAW,
                              column_error OUT NUMBER,
                              actual_length OUT INTEGER);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  n_tab IN OUT nocopy Number_table);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  c_tab IN OUT nocopy Varchar2_table);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  d_tab IN OUT nocopy Date_table);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  bl_tab IN OUT nocopy Blob_table);

  PROCEDURE COLUMN_VALUE (c IN INTEGER,
                          position IN INTEGER,
			 			  cl_tab IN OUT nocopy Clob_table);

--  PROCEDURE COLUMN_VALUE (c IN INTEGER,
--                          position IN INTEGER,
--			 			  bf_tab IN OUT nocopy Bfile_table);

  PROCEDURE DESCRIBE_COLUMNS(c IN INTEGER,
  							 col_cnt OUT INTEGER,
                             desc_t OUT DESC_TAB);
                             
  PROCEDURE DESCRIBE_COLUMNS2(c IN INTEGER,
  							 col_cnt OUT INTEGER,
                             desc_t OUT DESC_TAB2);
                             
  PROCEDURE DESCRIBE_COLUMNS3(c IN INTEGER,
  							 col_cnt OUT INTEGER,
                             desc_t OUT DESC_TAB3);

  FUNCTION FETCH_ROWS (c IN INTEGER)
  RETURN INTEGER;

  PROCEDURE CLOSE_CURSOR(cursor_id    IN OUT INTEGER);

  PROCEDURE VARIABLE_VALUE_CHAR(C IN INTEGER, NAME IN VARCHAR2,
                                VALUE OUT CHAR);

  PROCEDURE VARIABLE_VALUE_RAW(C IN INTEGER, NAME IN VARCHAR2,
                                VALUE OUT RAW);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT NUMBER);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT VARCHAR2);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT DATE);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, value OUT BLOB);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT CLOB);
  -- ob not support BFILE type
  --PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, value OUT BFILE);

  -- not support BIND_ARRAY, so not support VARIABLE_VALUE with ARRAY type parameters
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY NUMBER_TABLE);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY VARCHAR2_TABLE);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY DATE_TABLE);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY BLOB_TABLE);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY CLOB_TABLE);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY BFILE_TABLE);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT UROWID);
  
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY  UROWID_TABLE);
  
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIME_UNCONSTRAINED);
  
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIME_TABLE);

  PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
                           value OUT TIMESTAMP_UNCONSTRAINED);
  
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIME_TZ_UNCONSTRAINED );

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIME_WITH_TIME_ZONE_TABLE);
  
  -- conflict with TIMESTAMP_UNCONSTRAINED
  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIMESTAMP_TZ_UNCONSTRAINED);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIMESTAMP_WITH_TIME_ZONE_TABLE);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2,
  --                          value OUT TIMESTAMP_LTZ_UNCONSTRAINED);

  -- PROCEDURE VARIABLE_VALUE(c IN INTEGER, name IN VARCHAR2, 
  --                          value OUT NOCOPY TIMESTAMP_WITH_LTZ_TABLE);

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

  
END DBMS_SQL;
//

