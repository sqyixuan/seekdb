DELIMITER //;
CREATE OR REPLACE function func_split(str in VARCHAR2,i_separator IN VARCHAR2)
  --分隔符默认为'$|$'，可自己设定,法则:str1$|$str2$|$str3$|$
return TP_ARRAY_TABLE is
  resultarr TP_ARRAY_TABLE;
  i      INTEGER;
  b_pos  INTEGER;
  e_pos  INTEGER;
  len    INTEGER;

  separator VARCHAR2(10);
  sep_len   NUMBER;
BEGIN
  separator := i_separator;
  IF(separator IS NULL)THEN
     separator := '$|$';
  END IF;
  sep_len := length(separator);
  resultarr := TP_ARRAY_TABLE();
  i := 1;
  b_pos  := 1;
  e_pos  := instr(str,separator,1,i);

  WHILE(e_pos!= 0)LOOP
     resultarr.extend();
     IF(e_pos-b_pos > 4000)THEN
        len := 4000;
     ELSE
        len := e_pos-b_pos;
     END IF;
     resultarr(i) := substr(str,b_pos,len);
     i := i+1;
     b_pos := e_pos+sep_len;
     e_pos := instr(str,separator,1,i);
	 dbms_output.put_line('1......');
  END LOOP;
  e_pos := length(str)+1;
  IF(b_pos <> e_pos)THEN
     resultarr.extend();
     resultarr(i) := substr(str,b_pos,length(str));
	 dbms_output.put_line('2......');
  END IF;
  RETURN(resultarr);

  EXCEPTION
     WHEN OTHERS THEN
          RETURN NULL;
end func_split;
//


CREATE OR REPLACE FUNCTION func_split2(list IN VARCHAR2,
									  delimiter IN VARCHAR2 DEFAULT ',') RETURN dams_arrytype AS
  splitted dams_arrytype := dams_arrytype();
  i        PLS_INTEGER := 0;
  list_    VARCHAR2(32767) := list;
BEGIN
  IF list IS NULL THEN
    dbms_output.put_line('1......');
    RETURN splitted;
  ELSE
    LOOP
      i := instr(list_, delimiter);
      IF i > 0 THEN
        splitted.extend(1);
        splitted(splitted.last) := substr(list_, 1, i - 1);
        list_ := substr(list_, i + length(delimiter));
		dbms_output.put_line('2......');
      ELSE
        splitted.extend(1);
        splitted(splitted.last) := list_;
		dbms_output.put_line('3......');
        RETURN splitted;
      END IF;
    END LOOP;
  END IF;
END func_split2;
//


CREATE OR REPLACE FUNCTION func_dams_getarrayfromstr(i_str IN VARCHAR2,
split IN VARCHAR2 := ',') RETURN dbms_sql.varchar2_table IS
  --格式调整为1,2,3,4  末尾为,则表示最后一个字符串为空 mod by xiesy
  v_pos    BINARY_INTEGER;
  v_oldpos BINARY_INTEGER;
  i        BINARY_INTEGER;
  v_temp   VARCHAR2(2000);
  t_res    dbms_sql.varchar2_table;
BEGIN
  v_oldpos := 1;
  i        := 1;
  v_pos    := instr(i_str, split);
  IF v_pos = 0 THEN
    t_res(1) := i_str;
	dbms_output.put_line('1......');
  END IF;
  WHILE v_pos > 0
  LOOP
    v_temp := substr(i_str, v_oldpos, v_pos - v_oldpos);
    t_res(i) := v_temp;
    i := i + 1;
    v_oldpos := v_pos + length(split);
    v_pos := instr(i_str, split, v_oldpos);
	dbms_output.put_line('2......');
  END LOOP;
  --IF v_oldpos <= length(i_str) THEN
  t_res(i) := substr(i_str, v_oldpos);
  --END IF;
  RETURN t_res;
END func_dams_getarrayfromstr;
//

DELIMITER ;//

