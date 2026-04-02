#package_name:dbms_output_body
#author: guangang.gg

CREATE OR REPLACE PACKAGE BODY dbms_output AS

  TYPE char_arr IS TABLE OF VARCHAR(32767);

  enabled             BOOLEAN DEFAULT FALSE;

  buf                 char_arr := char_arr();
  buf_capacity        INT;
  buf_free_size       INT     DEFAULT -1;

  linebuflen          INT     DEFAULT 0;
  write_line_cursor   INT     DEFAULT 1;
  read_line_cursor    INT     DEFAULT 1;
  buf_read_flag       BOOLEAN DEFAULT TRUE;

  PROCEDURE enable(buffer_size IN INT DEFAULT 20000) IS
  BEGIN
    enabled := true;
    IF buffer_size < 2000 THEN
      buf_capacity := 2000;
    ELSIF buffer_size > 1000000 THEN
      buf_capacity := 1000000;
    ELSIF buffer_size IS NULL THEN
      buf_capacity := -1;
    ELSE
      buf_capacity := buffer_size;
    END IF;
    buf_free_size := buf_capacity;
  END;

  PROCEDURE reset_buf IS
  BEGIN
    buf.delete;
    buf.extend;
    write_line_cursor := 1;
    buf(write_line_cursor) := '';
  END;

  PROCEDURE disable IS
  BEGIN
    enabled := false;
    reset_buf;
    buf_read_flag := true;
  END;

  PROCEDURE put_init IS
  BEGIN
    reset_buf;
    linebuflen := 0;
    buf_free_size := buf_capacity;
    buf_read_flag := false;
  END;

  PROCEDURE put(a VARCHAR2) IS
    strlen BINARY_INTEGER;
    line_overflow EXCEPTION;
    PRAGMA EXCEPTION_INIT(line_overflow, -4019);
    buff_overflow EXCEPTION;
    PRAGMA EXCEPTION_INIT(buff_overflow, -4024);
  BEGIN
    IF enabled THEN
      IF buf_read_flag THEN -- clear buffer
        put_init;
      END IF;

      strlen := NVL(LENGTH(a), 0);
      IF ((strlen + linebuflen) > 32767) THEN
        linebuflen := 0;
        buf(write_line_cursor) := '';
        RAISE line_overflow;
      END IF;

      IF (buf_capacity <> -1) THEN
        IF (strlen > buf_free_size) THEN  
          RAISE buff_overflow;
        END IF;
        buf_free_size := buf_free_size - strlen;
      END IF;

      buf(write_line_cursor) := buf(write_line_cursor) || a;
      linebuflen := linebuflen + strlen;
    END IF;
  END;

  PROCEDURE put_line(a VARCHAR2) IS
  BEGIN
    IF enabled THEN
      put(a);
      new_line;
    END IF;
  END;

  PROCEDURE new_line IS
  BEGIN
    IF enabled THEN
      IF buf_read_flag THEN
        put_init;
      END IF;
      linebuflen := 0;
      buf.EXTEND;
      write_line_cursor := write_line_cursor + 1;
      buf(write_line_cursor) := '';
    END IF;
  END;

  -- get one line once, if we call put after get_line, data will be cleared
  PROCEDURE get_line(line OUT VARCHAR2, status OUT INT) IS
  BEGIN
    status := 1;
    IF enabled THEN
      IF NOT buf_read_flag THEN
        buf_read_flag := true;
        IF (linebuflen > 0) and (write_line_cursor = 1) THEN -- first line not finish yet
          RETURN;
        END IF;
        read_line_cursor := 1; -- we get line from first line
      END IF;

      IF read_line_cursor < write_line_cursor THEN
        line := buf(read_line_cursor); -- get current line
        read_line_cursor := read_line_cursor + 1; -- ready to get next line
        status := 0;
      END IF;
    END IF;
    RETURN;
  END;

  PROCEDURE GET_LINES(LINES OUT CHARARR, NUMLINES IN OUT INT) IS
    I      INT := 1;
    STATUS INT;
  BEGIN
    IF NOT ENABLED THEN
      NUMLINES := 0;
    ELSE
      WHILE I <= NUMLINES LOOP
        GET_LINE(LINES(I), STATUS);
        EXIT WHEN STATUS = 1;
        I := I + 1;
      END LOOP;
      NUMLINES := I - 1;
    END IF;
    RETURN;
  END;

  PROCEDURE GET_LINES(LINES OUT DBMSOUTPUT_LINESARRAY, NUMLINES IN OUT INT) IS
    I       INT := 1;
    STATUS  INT;
  BEGIN
    IF NOT ENABLED THEN
      NUMLINES := 0;
    ELSE
      LINES := DBMSOUTPUT_LINESARRAY();
      LINES.DELETE;

      IF NUMLINES < BUF.COUNT THEN
        NUMLINES := BUF.COUNT;
      END IF;

      LINES.EXTEND(NUMLINES);
      WHILE I <= NUMLINES LOOP
        GET_LINE(LINES(I), STATUS);
        EXIT WHEN STATUS = 1;
        I := I + 1;
      END LOOP;
      NUMLINES := I - 1;
    END IF;
    RETURN;
  END;

END dbms_output;
//
