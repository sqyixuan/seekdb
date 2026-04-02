-- package_name: utl_file
-- author: jiahua.cjh

CREATE OR REPLACE PACKAGE utl_file AUTHID CURRENT_USER AS

  TYPE NCHAR_LINETAB IS TABLE OF NVARCHAR2(32767) INDEX BY BINARY_INTEGER;
  NCHAR_LINE NCHAR_LINETAB;

  TYPE LINETAB IS TABLE OF VARCHAR2(32767) INDEX BY BINARY_INTEGER;
  LINE LINETAB;

  TYPE file_type IS RECORD (id BINARY_INTEGER,
                            datatype BINARY_INTEGER,
                            byte_mode BOOLEAN);

  file_open            EXCEPTION;
  charsetmismatch      EXCEPTION;
  invalid_path         EXCEPTION;
  invalid_mode         EXCEPTION;
  invalid_filehandle   EXCEPTION;
  invalid_operation    EXCEPTION;
  read_error           EXCEPTION;
  write_error          EXCEPTION;
  internal_error       EXCEPTION;
  invalid_maxlinesize  EXCEPTION;
  invalid_filename     EXCEPTION;
  access_denied        EXCEPTION;
  invalid_offset       EXCEPTION;
  delete_failed        EXCEPTION;
  rename_failed        EXCEPTION;

  charsetmismatch_errcode     CONSTANT PLS_INTEGER:= -29298;
  invalid_path_errcode        CONSTANT PLS_INTEGER:= -29280;
  invalid_mode_errcode        CONSTANT PLS_INTEGER:= -29281;
  invalid_filehandle_errcode  CONSTANT PLS_INTEGER:= -29282;
  invalid_operation_errcode   CONSTANT PLS_INTEGER:= -29283;
  read_error_errcode          CONSTANT PLS_INTEGER:= -29284;
  write_error_errcode         CONSTANT PLS_INTEGER:= -29285;
  internal_error_errcode      CONSTANT PLS_INTEGER:= -29286;
  invalid_maxlinesize_errcode CONSTANT PLS_INTEGER:= -29287;
  invalid_filename_errcode    CONSTANT PLS_INTEGER:= -29288;
  access_denied_errcode       CONSTANT PLS_INTEGER:= -29289;
  invalid_offset_errcode      CONSTANT PLS_INTEGER:= -29290;
  delete_failed_errcode       CONSTANT PLS_INTEGER:= -29291;
  rename_failed_errcode       CONSTANT PLS_INTEGER:= -29292;

  PRAGMA EXCEPTION_INIT(charsetmismatch,     -9561);
  PRAGMA EXCEPTION_INIT(invalid_path,        -9554);
  PRAGMA EXCEPTION_INIT(invalid_mode,        -9555);
  PRAGMA EXCEPTION_INIT(invalid_filehandle,  -9556);
  PRAGMA EXCEPTION_INIT(invalid_operation,   -9557);
  PRAGMA EXCEPTION_INIT(read_error,          -9558);
  PRAGMA EXCEPTION_INIT(write_error,         -9559);
  PRAGMA EXCEPTION_INIT(internal_error,      -9560);
  PRAGMA EXCEPTION_INIT(invalid_maxlinesize, -9562);
  PRAGMA EXCEPTION_INIT(invalid_filename,    -9563);
  PRAGMA EXCEPTION_INIT(access_denied,       -9564);
  PRAGMA EXCEPTION_INIT(invalid_offset,      -9565);
  PRAGMA EXCEPTION_INIT(delete_failed,       -9566);
  PRAGMA EXCEPTION_INIT(rename_failed,       -9567);

  FUNCTION fopen(location     IN VARCHAR2,
                 filename     IN VARCHAR2,
                 open_mode    IN VARCHAR2,
                 max_linesize IN BINARY_INTEGER DEFAULT NULL)
           RETURN file_type;
  PRAGMA RESTRICT_REFERENCES(fopen, WNDS, RNDS, TRUST);

  FUNCTION fopen_nchar(location     IN VARCHAR2,
                       filename     IN VARCHAR2,
                       open_mode    IN VARCHAR2,
                       max_linesize IN BINARY_INTEGER DEFAULT NULL)
           RETURN file_type;
  PRAGMA RESTRICT_REFERENCES(fopen_nchar, WNDS, RNDS, TRUST);

  FUNCTION is_open(file IN file_type) RETURN BOOLEAN;
  PRAGMA RESTRICT_REFERENCES(is_open, WNDS, RNDS, WNPS, RNPS, TRUST);

  PROCEDURE fclose(file IN OUT file_type);
  PRAGMA RESTRICT_REFERENCES(fclose, WNDS, RNDS, TRUST);

  PROCEDURE fclose_all;
  PRAGMA RESTRICT_REFERENCES(fclose_all, WNDS, RNDS, TRUST);

  PROCEDURE get_line(file   IN file_type,
                     buffer OUT VARCHAR2,
                     len    IN BINARY_INTEGER DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(get_line, WNDS, RNDS, WNPS, RNPS, TRUST);

  PROCEDURE get_line_nchar(file   IN  file_type,
                           buffer OUT NVARCHAR2,
                           len    IN  BINARY_INTEGER DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(get_line_nchar, WNDS, RNDS, WNPS, TRUST);


  PROCEDURE put(file   IN file_type,
                buffer IN VARCHAR2);
  PRAGMA RESTRICT_REFERENCES(put, WNDS, RNDS, TRUST);

  PROCEDURE put_nchar(file   IN file_type,
                buffer IN NVARCHAR2);
  PRAGMA RESTRICT_REFERENCES(put_nchar, WNDS, RNDS, TRUST);

  PROCEDURE new_line(file  IN file_type,
                     lines IN NATURAL := 1);
  PRAGMA RESTRICT_REFERENCES(new_line, WNDS, RNDS, TRUST);

  PROCEDURE new_line_nchar(file  IN file_type,
                           lines IN NATURAL := 1);
  PRAGMA RESTRICT_REFERENCES(new_line, WNDS, RNDS, TRUST);

  PROCEDURE put_line(file   IN file_type,
                     buffer IN VARCHAR2,
                     autoflush IN BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(put_line, WNDS, RNDS, TRUST);

  PROCEDURE put_line_nchar(file   IN file_type,
                     buffer IN NVARCHAR2);
  PRAGMA RESTRICT_REFERENCES(put_line_nchar, WNDS, RNDS, TRUST);

  procedure putf(file   IN file_type,
                 format IN VARCHAR2,
                 arg1   IN VARCHAR2 DEFAULT NULL,
                 arg2   IN VARCHAR2 DEFAULT NULL,
                 arg3   IN VARCHAR2 DEFAULT NULL,
                 arg4   IN VARCHAR2 DEFAULT NULL,
                 arg5   IN VARCHAR2 DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(putf, WNDS, RNDS, TRUST);

  procedure putf_nchar(file   IN file_type,
                 format IN NVARCHAR2,
                 arg1   IN NVARCHAR2 DEFAULT NULL,
                 arg2   IN NVARCHAR2 DEFAULT NULL,
                 arg3   IN NVARCHAR2 DEFAULT NULL,
                 arg4   IN NVARCHAR2 DEFAULT NULL,
                 arg5   IN NVARCHAR2 DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(putf_nchar, WNDS, RNDS, TRUST);

  PROCEDURE fflush(file IN file_type);
  PRAGMA RESTRICT_REFERENCES(fflush, WNDS, RNDS, TRUST);

  PROCEDURE put_raw(file      IN file_type,
                    buffer    IN RAW,
                    autoflush IN BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(put_raw, WNDS, RNDS, TRUST);

  PROCEDURE get_raw(file   IN  file_type,
                    buffer OUT NOCOPY RAW,
                    len    IN  BINARY_INTEGER DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(get_raw, WNDS, RNDS, TRUST);

  PROCEDURE fseek(file            IN OUT file_type,
                  absolute_offset IN     BINARY_INTEGER DEFAULT NULL,
                  relative_offset IN     BINARY_INTEGER DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(fseek, WNDS, RNDS, TRUST);

  PROCEDURE fremove(location IN VARCHAR2,
                    filename IN VARCHAR2);
  PRAGMA RESTRICT_REFERENCES(fremove, WNDS, RNDS, TRUST);

  PROCEDURE fcopy(src_location  IN VARCHAR2,
                  src_filename  IN VARCHAR2,
                  dest_location IN VARCHAR2,
                  dest_filename IN VARCHAR2,
                  start_line    IN BINARY_INTEGER DEFAULT 1,
                  end_line      IN BINARY_INTEGER DEFAULT NULL);
  PRAGMA RESTRICT_REFERENCES(fcopy, WNDS, RNDS, TRUST);

  PROCEDURE fgetattr(location    IN VARCHAR2,
                     filename    IN VARCHAR2,
                     fexists     OUT BOOLEAN,
                     file_length OUT NUMBER,
                     block_size  OUT BINARY_INTEGER);
  PRAGMA RESTRICT_REFERENCES(fgetattr, WNDS, RNDS, TRUST);


  FUNCTION fgetpos(file IN file_type) RETURN BINARY_INTEGER;
  PRAGMA RESTRICT_REFERENCES(fgetpos, WNDS, RNDS, TRUST);

  PROCEDURE frename(src_location   IN VARCHAR2,
                    src_filename   IN VARCHAR2,
                    dest_location  IN VARCHAR2,
                    dest_filename  IN VARCHAR2,
                    overwrite      IN BOOLEAN DEFAULT FALSE);
  PRAGMA RESTRICT_REFERENCES(frename, WNDS, RNDS, TRUST);

END utl_file;
//
