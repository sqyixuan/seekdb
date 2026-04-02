create or replace function f00(a int,b int) return number is
c int;
begin
select count(*) into c from t00 where c0=a and c1=b;
return c;
end;
/
create or replace function oceanbase_rand(rand_range  IN INTEGER) return number
as
  random_value number(24,0);
  random_seed int;
  seed1 int;
  seed2 int;
  BEGIN
    random_seed := to_number(to_char(systimestamp, 'FF6'));
    seed1 := mod(random_seed * 65537 + 55555555, rand_range);
    seed2 := mod(random_seed * 268435457, rand_range);
    random_value := mod(seed1 * 3 + seed2, rand_range);
    return random_value;
END;
/

CREATE OR REPLACE function uf_sec_NumberFormat
(
  i_Number IN NUMBER
)
return char as
begin
  if i_Number is null then
    return '0';
  end if;

  if abs(i_Number) < 1 THEN
    return replace(to_char(i_Number),'.','0.');
  else
    return to_char(i_Number);
  end if;
end uf_sec_NumberFormat;
/


CREATE OR REPLACE FUNCTION uf_GetVersion RETURN VARCHAR
AS
BEGIN
    RETURN 'CTPII V3.0.0_P1.6315-609d0d17';
END;
/

CREATE OR REPLACE FUNCTION FUNC_GETWORKDAY(i_area_code IN VARCHAR2)
  RETURN VARCHAR2 IS
  WorkDay      VARCHAR2(8);
  v_sysdate    VARCHAR2(8) := TO_CHAR(SYSDATE, 'yyyymmdd');
  v_group_code icbc.mag_area.group_code%TYPE;
BEGIN
  BEGIN
    SELECT group_code
      INTO v_group_code
      FROM icbc.mag_area
     WHERE area_code = i_area_code;
  EXCEPTION
    WHEN OTHERS THEN
      --默认境内机构
      v_group_code := '1';
  END;
  SELECT nvl(mag_workdate.WORKDATE, v_sysdate)
    INTO WorkDay
    FROM icbc.mag_workdate
   WHERE GROUP_CODE = v_group_code
     AND VALID = '1'
     AND rownum = 1;
  RETURN(WorkDay);
EXCEPTION
  WHEN OTHERS THEN
    RETURN v_sysdate;
END FUNC_GETWORKDAY;
/

CREATE OR REPLACE FUNCTION FUNC_GETAREADATE(i_area_code in varchar2)
  return date is
  Result date;
  v_diff MAG_TIMEZONE.TIME_DIFF%type;
begin
  --获取时差
  begin
    select TIME_DIFF
      into v_diff
      from MAG_TIMEZONE
     where TIME_ZONE =
           (select TIME_ZONE from MAG_AREA where AREA_CODE = i_area_code);
  exception
    when others then
      return(sysdate);
  end;

  --获取B地区的时间
  begin
    Result := sysdate + (v_diff) / 24;
  exception
    when others then
      return(sysdate);
  end;
  return(Result);
end FUNC_GETAREADATE;
/

CREATE OR REPLACE FUNCTION func_cm2002_2_gram(i_currency IN VARCHAR2)
  RETURN VARCHAR2 IS
  RESULT VARCHAR2(10);
BEGIN
  BEGIN
    SELECT para_code
      INTO RESULT
      FROM MAG_PARAMETER_SYS
     WHERE table_name = 'CURRENCY_MAPPING'
       AND para_vaue_1 = i_currency;
    RETURN(RESULT);
  EXCEPTION
    WHEN OTHERS THEN
      RESULT := i_currency;
      RETURN(RESULT);
  END;
END func_cm2002_2_gram;
/


CREATE OR REPLACE FUNCTION func_gram_2_cm2002(i_currency IN VARCHAR2)
  RETURN VARCHAR2 IS
  RESULT VARCHAR2(10);
BEGIN
  BEGIN
    SELECT para_vaue_1
      INTO RESULT
      FROM MAG_PARAMETER_SYS
     WHERE table_name = 'CURRENCY_MAPPING'
       AND para_code = i_currency;
    RETURN(RESULT);
  EXCEPTION
    WHEN OTHERS THEN
      RESULT := i_currency;
      RETURN(RESULT);
  END;
END func_gram_2_cm2002;
/


CREATE OR REPLACE FUNCTION      func_getcontent(content VARCHAR2,field varchar2, contentid NUMBER)
/*
  created:20100519
  creator:kfzx-jialf
  content:return contentid's substrb in content,every substrb is delimited by field.
*/
  RETURN VARCHAR2 IS
  v_tmpcontent    VARCHAR2(500);
  v_resultcontent VARCHAR2(500);
  v_countflag     NUMBER := 0;
  v_currcount     NUMBER := 0;
BEGIN
  IF substrb(content, 1, 1) = field THEN
    v_tmpcontent := ' ' || content;
  ELSE
    v_tmpcontent := content;
  END IF;

  FOR i IN 1 .. contentid LOOP
    v_countflag := INSTR(v_tmpcontent, field, 1);
    v_currcount := i;

    IF (v_countflag = 0 OR v_countflag IS NULL) THEN
      v_resultcontent := v_tmpcontent;
      GOTO out_loop;
    ELSE
      v_resultcontent := substr(v_tmpcontent, 1, v_countflag - 1);
      v_tmpcontent    := substr(v_tmpcontent,
                              v_countflag + 1,
                              LENGTH(v_tmpcontent) - v_countflag);
    END IF;
  END LOOP;

  <<out_loop>>
  IF (v_currcount = contentid) THEN
    RETURN v_resultcontent;
  ELSE
    RETURN '';
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    RETURN '';
END func_getcontent;
/


CREATE OR REPLACE FUNCTION func_getCHNKIND RETURN NUMBER IS
/*
  created:20100519
  creator:kfzx-jialf
  content:get channel kind

  channel type:
  web:       'w'|chn_no|time_zone|area_code|lang_code|emp_code|op
  batch:    'b'|chn_no|group_code|work_batcmode|work_batchid|parallel_id
  job:      'j'|chn_no|time_zone|area_code|lang_code|emp_code|job_no
  scripts:  's'|chn_no|time_zone|area_code|lang_code|emp_code|project_no
*/

  V_CHNKIND       NUMBER := 0;
  v_clientinfo  varchar2(64);
BEGIN
  --v_clientinfo := sys_context('userenv', 'client_info');
  v_clientinfo := '';

  IF v_clientinfo IS NULL OR v_clientinfo = '' THEN
    return V_CHNKIND;
  else
      V_CHNKIND := func_getcontent(v_clientinfo,'|', 2);
  END IF;

  return  V_CHNKIND;
exception when others then
   return  0;
END func_getCHNKIND;
/


CREATE OR REPLACE FUNCTION FUNC_GET_SYSTIMESTAMP
     RETURN VARCHAR2
   IS
     v_systimestamp varchar2(17) := ''; --17位时间戳
   BEGIN
     select substr(to_char(systimestamp,'yyyymmddhh24missff'),1,17)
   into v_systimestamp
   from dual;
     RETURN v_systimestamp;

   EXCEPTION
     WHEN OTHERS THEN
       RETURN '1970-01-01';
   END FUNC_GET_SYSTIMESTAMP;
/


CREATE OR REPLACE FUNCTION func_getgroupcode RETURN VARCHAR2
/*
    created:20101103
    creator:kfzx-jialf
    content:get group_code
  */
 IS
  v_groupcode  VARCHAR2(8);
  v_clientinfo VARCHAR2(64);
  v_type       VARCHAR2(1);
BEGIN
  --v_clientinfo := SYS_CONTEXT('userenv', 'client_info');
  v_clientinfo := '';

  IF v_clientinfo IS NULL OR v_clientinfo = '' THEN
    RETURN('1');
  ELSE
    v_type := func_getcontent(v_clientinfo, '|', 1);

    IF v_type = 'b' THEN
      --20130115,func_getcontent(,,3)-->func_getcontent(,,4)
      v_groupcode := func_getcontent(v_clientinfo, '|', 4);
    ELSE
      v_groupcode := '1';
    END IF;
  END IF;

  RETURN v_groupcode;
EXCEPTION
  WHEN OTHERS THEN
    RETURN '1';
END func_getgroupcode;
/


CREATE OR REPLACE function FUNC_CHECK_TYPE_INSTANCE(in_job_code in varchar2)
  return number is
  v_instance_id number := 1;--默认在批量节点跑
begin
  begin
    select nvl(instance_id, 1)
      into v_instance_id
      from cidp_job_type
     where job_code = in_job_code;
  exception
    when others then
      v_instance_id := 1;
  end;
  return(v_instance_id);
exception
  when others then
    v_instance_id := 1;
    return(v_instance_id);
end FUNC_CHECK_TYPE_INSTANCE;
/


CREATE OR REPLACE function FUNC_CHECKPALLALL(in_instance_id in number)
  return number is
  parallel_num  CIDP_JOB_PARALLEL_DEF.parallel_num%type;
  total_num     NUMBER := 0;
  flag          NUMBER := 0;
  BEGIN
        --取当前时段作业时间间隔以及系统运行运行作业数
  BEGIN
    SELECT  min(PARALLEL_NUM)
      INTO parallel_num
      FROM CIDP_JOB_PARALLEL_DEF
     WHERE PERIOD_END >= TO_CHAR(SYSDATE, 'HH24Mi')
       AND PERIOD_BEGIN <= TO_CHAR(SYSDATE, 'HH24Mi')
       AND GROUP_CODE = '*'
       AND INSTANCE_ID = in_instance_id; ----20150210 增加节点的过滤
  EXCEPTION
    WHEN OTHERS THEN
      -----如果取不到则取系统默认的
      ------------并发个数
      parallel_num := 999;
  END;
  
  SELECT COUNT(1)
    INTO total_num
    from (select distinct RUNED_JOBNAME
            from cidp_job_list
           where QUEUE_STATUS in ('0', '1')
             AND FUNC_CHECK_TYPE_INSTANCE(job_code) = in_instance_id --20150210 增加判断作业所属节点
             and RUNED_JOBNAME is not null);

            ------总数不大于全局的并发数
      IF parallel_num <=  total_num THEN
        flag := 0;
      else
        flag := parallel_num - total_num; 
      END IF;
  --返回
  return(flag);
END FUNC_CHECKPALLALL;
/


CREATE OR REPLACE function FUNC_CHECKGROUPKPALLALL(IN_GROUP_CODE in varchar2)
  return number is

  /**
   主要逻辑为：获取作业组剩余的可执行的并发数；
  **/
  
  v_groupflag number := -1;
  v_parallnum cidp_job_parallel_def.parallel_num%type;
  v_num       number := 0;
  v_tempnum   number := 0;
  v_job_name  varchar2(100); --作业名称
BEGIN
  --获取组并发数
  BEGIN
    select a.parallel_num
      into v_parallnum
      from cidp_job_parallel_def a
     where a.group_code = IN_GROUP_CODE
       AND rownum = 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      return v_groupflag;
  END;
  
  --获取组内所有作业使用的并发数
  FOR V_CUR IN (select job_code
                  from cidp_job_type
                 where group_code = IN_GROUP_CODE) LOOP
    v_job_name := 'CIDPJOB' || '_' || '%';
    SELECT count(1)
      INTO v_tempnum
      from (select distinct RUNED_JOBNAME
              from cidp_job_list
             where job_code = V_CUR.JOB_CODE
               and QUEUE_STATUS in ('0', '1')
               and RUNED_JOBNAME is not null);
  
    v_num := v_num + v_tempnum;
  END LOOP;
  
  IF v_num < v_parallnum THEN
    v_groupflag := v_parallnum - v_num;
  ELSE
    v_groupflag := 0;
  END IF;
  --返回
  return v_groupflag;
END FUNC_CHECKGROUPKPALLALL;
/


CREATE OR REPLACE function FUNC_CHECKJOBTYPEPALL(IN_JOB_CODE     in varchar2,
                                                 IN_PARALLEL_NUM in number)
  return number is
  
  /**
   主要逻辑为：获取作业type剩余的可执行的并发数；
  **/
  
  v_typeNum number := 0; --作业剩余并发数
  v_tempnum number := 0; --作业使用的并发数
BEGIN
  select count(1)
    into v_tempnum
    from (select distinct RUNED_JOBNAME
            from cidp_job_list
           where job_code = IN_JOB_CODE
             and QUEUE_STATUS in ('0', '1')
             and RUNED_JOBNAME is not null);

  IF v_tempnum < IN_PARALLEL_NUM THEN
    v_typeNum := IN_PARALLEL_NUM - v_tempnum;
  ELSE
    v_typeNum := 0;
  END IF;
  --返回
  return v_typeNum;
END FUNC_CHECKJOBTYPEPALL;
/


CREATE OR REPLACE function getInfoMsg(err_code in VARCHAR2, langcode in VARCHAR2) return varchar2 is
  Result varchar2(2000);
begin
  Result:=pckg_errormsg_manage.getinfomsg(err_code,langcode);
  return(Result);
end getInfoMsg;
/


CREATE OR REPLACE FUNCTION      FUNC_GETEMPLOYEE return varchar2 is
  Result       MAG_EMPLOYEE.Employee_Code%type;
  v_clientinfo varchar2(64);
  v_beginpos   number(3);
  v_endpos     number(3);
begin
  Result       := '000000000';
  --v_clientinfo := SYS_CONTEXT('userenv', 'client_info');
  v_clientinfo := '';
  if (v_clientinfo is not null) then
    v_beginpos := instr(v_clientinfo, '|', 1, 4);
    v_endpos   := instr(v_clientinfo, '|', 1, 5);
    if (v_beginpos <> 0 and v_endpos <> 0) then
      Result := substr(v_clientinfo,
                       v_beginpos + 1,
                       v_endpos - v_beginpos - 1);
    end if;
  end if;
  return(Result);
end FUNC_GETEMPLOYEE;
/


CREATE OR REPLACE function FUNC_GETAREANAME(areacode in VARCHAR2)
  return varchar2 is
  Result varchar2(100);
begin
  BEGIN
    SELECT ma.area_name
      into Result
      FROM mag_area ma
     WHERE ma.area_code = areacode;
    return(Result);
  EXCEPTION
    WHEN OTHERS THEN
      Result := '';
      return(Result);
  END;
end FUNC_GETAREANAME;
/


CREATE OR REPLACE FUNCTION Func_Get_Filename(In_Filename VARCHAR2)
  RETURN VARCHAR2 IS
  v_Len     PLS_INTEGER;
  v_Spe_Pos PLS_INTEGER;
  v_Spe_Pos2 PLS_INTEGER;
  v_Spe_Pos3 PLS_INTEGER;

  v_Pos     PLS_INTEGER;
  v_Char    VARCHAR2(1);
  Out_Char  VARCHAR2(200);
BEGIN
  v_Spe_Pos := 0;
  v_Pos     := 1;
  v_Len     := Length(In_Filename);
  v_Spe_Pos := Instr(In_Filename, 'DATADATE');
  v_Spe_Pos2 := Instr(In_Filename, 'VEXP_AREACODE');
  v_Spe_Pos3 := Instr(In_Filename, 'VEXP_TIME');


  BEGIN
    LOOP
      v_Char := Substr(In_Filename, v_Pos, 1);
      --琛ㄧず杩涘叆鍒扮壒娈婂瓧娈电洿鎺ユ坊鍔?
      IF (v_Pos = v_Spe_Pos) THEN
        Out_Char := Out_Char || 'DATADATE';
        v_Pos    := v_Pos + 8;
      elsif(v_Pos = v_Spe_Pos2) THEN
        Out_Char := Out_Char || 'VEXP_AREACODE';
        v_Pos    := v_Pos + 13;
      elsif(v_Pos = v_Spe_Pos3) THEN
        Out_Char := Out_Char || 'VEXP_TIME';
        v_Pos    := v_Pos + 9;


      ELSE
        --濡傛灉鏁版嵁鍦╝-z or A-Z鍒欒繘琛屾浛鎹?
        IF ((Ascii(v_Char) >= 65 AND Ascii(v_Char) <= 90) OR
           (Ascii(v_Char) >= 97 AND Ascii(v_Char) <= 122)) THEN
          Out_Char := Out_Char || '[' || Upper(v_Char) || Lower(v_Char) || ']';
          v_Pos    := v_Pos + 1;
        ELSE
          Out_Char := Out_Char || v_Char;
          v_Pos    := v_Pos + 1;
        END IF;

      END IF;
      EXIT WHEN v_Pos > v_Len;
    END LOOP;
    RETURN Out_Char;
  END;
END Func_Get_Filename;
/


CREATE OR REPLACE FUNCTION dsc_fn_nullifemptystr(istr in varchar2) RETURN varchar2 IS
  vnum_str  NUMERIC;
  vnum_flag BOOLEAN;
BEGIN
  BEGIN
    vnum_str  := CAST(istr AS NUMERIC);
    vnum_flag := TRUE;
  EXCEPTION
    WHEN OTHERS THEN
      vnum_flag := FALSE;
  END;

  IF vnum_flag = TRUE THEN
    RETURN istr;
  ELSE
    RETURN NULLIF(istr, '');
  END IF;
END;
/


CREATE OR REPLACE FUNCTION GET_DEBTLIC_LEDGERTYPE_NAME(i_dealtype   VARCHAR2, --DEAL_TYPE, 抵债电子许可证业务种类
                                                       i_ledgertype VARCHAR2, --LEDGER_TYPE, 抵债电子许可证入账类型
                                                       i_language   VARCHAR2 --语言
                                                       ) RETURN VARCHAR2 IS
  v_dictname VARCHAR2(1000) := '';
  v_lang     VARCHAR2(50);
  v_beancode VARCHAR2(200) := 'LICENCE_ACCOUNT#LEDGER_TYPE';
BEGIN
  v_lang := i_language;

  IF i_language IS NULL THEN
    v_lang := 'zh_CN';
  END IF;

  v_beancode := v_beancode || i_dealtype;

  SELECT DICT_NAME
    INTO v_dictname
    FROM SYS_DICT_DATA
   WHERE BEAN_CODE = v_beancode
     AND DICT_CODE = i_ledgertype
     AND LANG_CODE = v_lang;

  RETURN v_dictname;

EXCEPTION
  WHEN OTHERS THEN
    RETURN i_ledgertype;

END GET_DEBTLIC_LEDGERTYPE_NAME;
/


CREATE OR REPLACE FUNCTION        FUNC_GETCURRENTSID RETURN NUMBER IS
  v_sid NUMBER;

BEGIN
  --获取当前的session id.
  --SELECT SID INTO v_sid FROM v$mystat WHERE ROWNUM = 1;
  v_sid:=0;

  RETURN v_sid;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    -- Consider logging the error and then re-raise
    --RAISE;
    RETURN 0;
END func_getcurrentsid;
/


CREATE OR REPLACE function cidp_func_job_manage_splitkey(in_str in varchar2, in_split in varchar2)
    return ARRYTYPE as
    v_count1  integer;
    v_count2  integer;
    v_strlist ARRYTYPE;
    v_node    varchar2(2000);
    v_str     varchar2(2000);
  begin
    --out_mess:='';
    v_str     := in_split || in_str || in_split;
    v_count2  := 0;
    v_strlist := ARRYTYPE();
    if (in_str is null) or (length(in_str) <= 0) then
      return null;
    end if;
    for v_i in 1 .. length(v_str) loop
      v_count1 := INSTRB(v_str, in_split, 1, v_i);
      v_count2 := INSTRB(v_str, in_split, 1, v_i + 1);
      v_node   := SUBSTRB(v_str, v_count1 + 1, v_count2 - v_count1 - 1);
      if v_node is null then
        v_node := '';
      end if;
      if (v_count2 = 0) or (v_count2 is null) then
        exit;
      else
        v_strlist.extend();
        v_strlist(v_i) := v_node;
        v_node := '';
      end if;
    end loop;
    return v_strlist;
  end cidp_func_job_manage_splitkey;
/


CREATE OR REPLACE FUNCTION      GET_DEBT_ASSET_COL_NAME (
   i_DEBT_TYPE   IN VARCHAR2,                                       /*抵债资产种类*/
   i_COL_CODE    IN VARCHAR2,                    /*该列在数据库存储的英文名,如TYPE_OTHERM*/
   i_COL_DICT    IN VARCHAR2,                          /*该列对应的dict_code值,如01*/
   i_language    IN VARCHAR2)
   RETURN VARCHAR2
IS
   v_dict_name   VARCHAR2 (100);
BEGIN
   v_dict_name := '';

   BEGIN
      SELECT dict_name
        INTO v_dict_name
        FROM sys_dict_data
       WHERE bean_code IN
                   (SELECT DISTINCT c.ELEMENT_FORMAT_VALUE
                      FROM ASSETPL_ELEM_PRES_STORAGE a,
                           ASSETPL_MODELELEM_RELATIVE b,
                           pom_element c,
                           sys_dict_data d
                     WHERE     a.relative_kind_code = '006'
                           AND a.BUSI_TABLE = 'DEBT_ASSET'
                           AND a.RELATIVE_CODE = b.RELATIVE_CODE
                           AND c.ELEMENT_CODE = a.element_code
                           AND D.BEAN_CODE = 'DEBT_ASSET#DEBT_TYPE'
                           AND d.dict_code = SUBSTR (b.COMPLEX_PK, 6, 3)
                           AND a.TABLE_COLUMN = i_COL_CODE            --表字段英文名
                           AND d.dict_code = i_DEBT_TYPE)
             AND DICT_CODE = i_COL_DICT
             AND LANG_CODE = i_language;
   EXCEPTION
      WHEN OTHERS
      THEN
         v_dict_name := i_COL_DICT;
   END;

   RETURN v_dict_name;
END;
/


CREATE OR REPLACE function FUNC_GETJOBNAME(in_instance_id in number)
  return VARCHAR2 is
  v_job_name SYS_DICT_DATA.DICT_NAME%TYPE;
  resultname varchar2(100);
BEGIN
  v_job_name := 'CIDPJOB'  || '_';
  
  --获取一个空闲的job
  begin
  SELECT JOB_NAME
    INTO resultname
    FROM DBA_SCHEDULER_JOBS
   WHERE JOB_NAME LIKE v_job_name || '%'
     AND owner = cidp_pack_scheduler_manage.get_job_owner
     and decode(instance_id,'1','1','2','2','','1') = in_instance_id --20150210 进行节点的过滤
     AND JOB_NAME NOT IN (SELECT nvl(RUNED_JOBNAME,'0') FROM CIDP_JOB_LIST WHERE QUEUE_STATUS in ('1','0'))
     and rownum = 1;  
  exception
    when others then
      resultname := '';
  end;

  --返回
  return(resultname);
END FUNC_GETJOBNAME;
/


CREATE OR REPLACE FUNCTION FUNC_GETBASEDATE return date is
  Result date;
begin
  Result := sysdate;
  return(Result);
end FUNC_GETBASEDATE;
/


CREATE OR REPLACE function func_getjobtitle(in_dict_code in varchar2,
                                           in_langcode  in varchar2)
  return varchar2 is
  Result sys_dict_data.dict_name%type;
begin

  select dict_name
    into result
    from sys_dict_data
   where area_code = '0'
     and bean_code = 'MAG_JOB_FILE#TITLE'
     and dict_code = in_dict_code
     and lang_code = in_langcode;

  return(Result);
exception
  when others then
    return in_dict_code;
end func_getjobtitle;
/


CREATE OR REPLACE function func_getsplitstring(in_str   in varchar2,
                                               in_split in varchar2)
  return arrytype as
  v_count1  integer;
  v_count2  integer;
  v_strlist arrytype;
  v_node    varchar2(2000);
begin
  --out_mess:='';
  v_count2  := 0;
  v_strlist := arryType();
  if (in_str is null) or (length(in_str) <= 0) then
    return null;
  end if;
  for v_i in 1 .. length(in_str) loop
    v_count1 := INSTRB(in_str, in_split, 1, v_i);
    v_count2 := INSTRB(in_str, in_split, 1, v_i + 1);
    v_node   := SUBSTRB(in_str, v_count1 + 1, v_count2 - v_count1 - 1);
    if v_node is null then
      v_node := '';
    end if;
    if (v_count2 = 0) or (v_count2 is null) then
      exit;
    else
      v_strlist.extend();
      v_strlist(v_i) := v_node;
      v_node := '';
    end if;
  end loop;
  return v_strlist;
end func_getsplitstring;
/


CREATE OR REPLACE FUNCTION func_fgram_get_sequence(in_area_code VARCHAR2, --机构代码 10位
                                             in_seq_code  VARCHAR2, --序列号代码,目前对应协议种类,5位
                                             out_msg      OUT VARCHAR2)
  RETURN VARCHAR2 IS
  temp_number  gcmsequence.seq_number%TYPE; ---number(9)
  ret_sequence VARCHAR2(60);
  v_seq_code   VARCHAR2(20);
  v_seq_length INTEGER;
  v_array      arrytype;
  --未来我行协议编号编码方式建议：机构编号（10 位）＋协议类型（5 位）＋协议序号（9 位）
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  v_seq_code   := in_seq_code;
  v_seq_length := 9;
  v_array      := func_getsplitstring(in_seq_code, '|');
  IF v_array.COUNT >= 2 THEN
    v_seq_code   := v_array(1);
    v_seq_length := to_number(v_array(2));
  END IF;

  BEGIN
    INSERT INTO gcmsequence
      (seq_area, seq_code, seq_name, seq_number)
    VALUES
      (in_area_code, v_seq_code, '', 1);
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
      --并发的情况,多个进程同时新增的时候,发生Exception应该继续往下执行  2010-02-20
      NULL;
    WHEN OTHERS THEN
      --raise;
      out_msg := '写入gcmsequence表失败,可能原因:' || SQLERRM;
      ROLLBACK;
      RETURN 'ERROR';
  END;

  BEGIN
    SELECT seq_number
      INTO temp_number
      FROM gcmsequence
     WHERE seq_code = v_seq_code
       AND seq_area = in_area_code
       FOR UPDATE NOWAIT;
  EXCEPTION
    WHEN OTHERS THEN
      out_msg := '查询流水号管理表地区' || in_area_code || '、流水号' || v_seq_code ||
                 '资源正忙，请稍后再试';
      COMMIT;
      RETURN 'ERROR';
  END;

  BEGIN
    UPDATE gcmsequence
       SET seq_number = seq_number + 1
     WHERE seq_code = v_seq_code
       AND seq_area = in_area_code;
  EXCEPTION
    WHEN OTHERS THEN
      out_msg := '修改机构' || in_area_code || '、流水号' || v_seq_code ||
                 '的计数器出错：' || SQLCODE || ':' || SQLERRM;
      COMMIT;
      RETURN 'ERROR';
  END;

  ret_sequence := in_area_code || '-' || v_seq_code || '-' ||
                  LPAD(TO_CHAR(temp_number), v_seq_length, '0');
  COMMIT;
  out_msg := 'OK';
  RETURN ret_sequence;
EXCEPTION
  WHEN OTHERS THEN
    out_msg := SQLCODE || ' ' || SQLERRM;
    ROLLBACK;
    RETURN 'ERROR';
END func_fgram_get_sequence;
/


CREATE OR REPLACE FUNCTION func_getdictname(i_AREA_CODE varchar2,
                                            i_BEAN_CODE VARCHAR2,
                                            i_LANG_CODE VARCHAR2,
                                            i_DICT_CODE varchar2)
  RETURN VARCHAR2 is
  v_dict_name sys_dict_data.dict_name%type;
  v_area_code mag_area.area_code%type;
  v_lang_code sys_dict_data.lang_code%type;
begin
  if i_AREA_CODE is null then
    v_AREA_CODE := '0';
  else
    v_area_code := i_AREA_CODE;
  end if;
  if i_LANG_CODE is null then
    v_LANG_CODE := 'zh_CN';
  else
    v_lang_code := i_LANG_CODE;
  end if;
  select DICT_NAME
    into v_dict_name
    from sys_dict_data
   where area_code = v_AREA_CODE
     and bean_code = i_BEAN_CODE
     and lang_code = v_LANG_CODE
     and dict_code = i_DICT_CODE
     and rownum = 1;
  return v_dict_name;
EXCEPTION
  WHEN OTHERS THEN
    RETURN '';

end func_getdictname;
/


CREATE OR REPLACE FUNCTION FUNC_GETAREABYLEVEL
(
    in_area_code  IN VARCHAR2,
    in_bank_level IN VARCHAR2
) RETURN VARCHAR2 IS
    RESULT mag_area.area_code%TYPE;
BEGIN
    SELECT area_code
      INTO RESULT
      FROM mag_area
     WHERE bank_level = in_bank_level
     START WITH area_code = in_area_code
    CONNECT BY PRIOR belong_area_code = area_code;
    RETURN(RESULT);
EXCEPTION
    WHEN OTHERS THEN
        RETURN in_area_code;
END FUNC_GETAREABYLEVEL;
/


CREATE OR REPLACE FUNCTION func_get_sequence (
   in_area_code         VARCHAR2,                              --机构代码 10位
   in_seq_code          VARCHAR2,            --序列号代码,目前对应协议种类,5位
   out_msg        OUT   VARCHAR2
)
   RETURN VARCHAR2
IS
   temp_number    gcmsequence.seq_number%TYPE;                   ---number(9)
   ret_sequence   VARCHAR2 (60);
--未来我行协议编号编码方式建议：机构编号（10 位）＋协议类型（5 位）＋协议序号（9 位）
   PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
   BEGIN
      INSERT INTO gcmsequence
                  (seq_area, seq_code, seq_name, seq_number
                  )
           VALUES (in_area_code, in_seq_code, '', 1
                  );
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN
 --并发的情况,多个进程同时新增的时候,发生Exception应该继续往下执行  2010-02-20
         NULL;
      WHEN OTHERS
      THEN
         --raise;
         out_msg := '写入gcmsequence表失败,可能原因:' || SQLERRM;
         ROLLBACK;
         RETURN 'ERROR';
   END;

   BEGIN
      SELECT     seq_number
            INTO temp_number
            FROM gcmsequence
           WHERE seq_code = in_seq_code AND seq_area = in_area_code
      FOR UPDATE NOWAIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         out_msg :=
               '查询流水号管理表地区'
            || in_area_code
            || '、流水号'
            || in_seq_code
            || '资源正忙，请稍后再试';
         COMMIT;
         RETURN 'ERROR';
   END;

   BEGIN
      UPDATE gcmsequence
         SET seq_number = seq_number + 1
       WHERE seq_code = in_seq_code AND seq_area = in_area_code;
   EXCEPTION
      WHEN OTHERS
      THEN
         out_msg :=
               '修改机构'
            || in_area_code
            || '、流水号'
            || in_seq_code
            || '的计数器出错：'
            || SQLCODE
            || ':'
            || SQLERRM;
         COMMIT;
         RETURN 'ERROR';
   END;

   ret_sequence :=
         in_area_code
      || '-'
      || in_seq_code
      || '-'
      || LPAD (TO_CHAR (temp_number), 9, '0');
   COMMIT;
   out_msg := 'OK';
   RETURN ret_sequence;
EXCEPTION
   WHEN OTHERS
   THEN
      out_msg := SQLCODE || ' ' || SQLERRM;
      ROLLBACK;
      RETURN 'ERROR';
END func_get_sequence;
/


CREATE OR REPLACE FUNCTION FUNC_ISDIRECT(IN_FLOW_NO IN VARCHAR2)
    RETURN VARCHAR2 IS
    RESULT VARCHAR2(5);
BEGIN
    SELECT B.FLOW_TYPE
    INTO   RESULT
    FROM   AFW_FLOW_INFO_BAK A,
           AFW_FLOW_DEF  B
    WHERE  A.FLOW_CODE = B.FLOW_CODE
    AND    A.EDIT_NO = B.EDIT_NO
    AND    A.FLOW_NO = IN_FLOW_NO
    AND    ROWNUM = 1;
    IF TO_NUMBER(RESULT) = 2 THEN
        RETURN '1';
    ELSE
        RETURN '0';
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN '0';
END FUNC_ISDIRECT;
/


CREATE OR REPLACE FUNCTION func_errmsg_filter(in_errmsg IN VARCHAR2)
  RETURN VARCHAR2 IS

  v_errmsg VARCHAR2(4000);

BEGIN
  v_errmsg := '';

  SELECT substrb(regexp_replace(in_errmsg,
                                '(ORA-[[:digit:]]{5})', --正则表达式
                                '', --替换的字符
                                1, --起始位置
                                0, --替换的次数(0代表无限次)
                                'i' --i代表不区分大小写
                                ),
                 1,
                 4000)
    INTO v_errmsg
    FROM dual;

  RETURN v_errmsg;
EXCEPTION
  WHEN OTHERS THEN
    RETURN v_errmsg;
END func_errmsg_filter;
/


CREATE OR REPLACE FUNCTION FUNC_A2( istr IN numeric )
RETURN numeric
is
BEGIN
  dbms_output.put_line(1);
RETURN istr;
END;
/


CREATE OR REPLACE FUNCTION FUNC_CIDP_GETPARTITION(in_TABLE IN VARCHAR2,
                                                  in_date  in varchar2)
/**********************************************
    版本号：NOVA+ V1.3.5
    修改日期：20110825
    创建人员：kfzx-jialf
    功能:get current partition by date and table
  **********************************************/

 RETURN NUMBER IS
  v_partition    NUMBER(2); --current partition
  v_count        NUMBER;
  v_MAG_LOG_CTRL MAG_LOG_CTRL%rowtype;
BEGIN
  begin
    SELECT LOG_CURR_PART, LOG_TOTAL_PART, LOG_INTERVAL, LOG_CURR_DATE
      INTO v_MAG_LOG_CTRL.LOG_CURR_PART,
           v_MAG_LOG_CTRL.LOG_TOTAL_PART,
           v_MAG_LOG_CTRL.LOG_INTERVAL,
           v_MAG_LOG_CTRL.LOG_CURR_DATE
      FROM mag_log_ctrl
     WHERE log_id = UPPER(in_TABLE);
  exception
    when others then
      RETURN - 1;
  end;

  IF v_MAG_LOG_CTRL.LOG_INTERVAL = '1' THEN
    v_count := TO_DATE(v_MAG_LOG_CTRL.LOG_CURR_DATE, 'yyyymmdd') -
               TO_DATE(in_date, 'yyyymmdd');
  ELSIF v_MAG_LOG_CTRL.LOG_INTERVAL = '2' THEN
    v_count := MONTHS_BETWEEN(TO_DATE(SUBSTR(v_MAG_LOG_CTRL.LOG_CURR_DATE,
                                             1,
                                             6),
                                      'yyyymm'),
                              TO_DATE(SUBSTR(in_date, 1, 6), 'yyyymm'));
  ELSIF v_MAG_LOG_CTRL.LOG_INTERVAL = '3' THEN
    v_count := (TO_NUMBER(SUBSTR(v_MAG_LOG_CTRL.LOG_CURR_DATE, 1, 4)) -
               TO_NUMBER(SUBSTR(in_date, 1, 4)));
  END IF;

  v_count := MOD(v_count, v_MAG_LOG_CTRL.LOG_TOTAL_PART);

  IF v_count < 0 THEN
    v_count := v_count + v_MAG_LOG_CTRL.LOG_TOTAL_PART;
  END IF;

  IF v_MAG_LOG_CTRL.LOG_CURR_PART > v_count THEN
    v_partition := v_MAG_LOG_CTRL.LOG_CURR_PART - v_count;
  ELSE
    v_partition := v_MAG_LOG_CTRL.LOG_CURR_PART +
                   v_MAG_LOG_CTRL.LOG_TOTAL_PART - v_count;
  END IF;

  RETURN v_partition;
EXCEPTION
  WHEN OTHERS THEN
    RETURN - 1;
END FUNC_CIDP_GETPARTITION;
/


CREATE OR REPLACE FUNCTION func_getpartition (i_TABLE IN VARCHAR2,
i_date  in varchar2
)
/*
  修改时间：20100519
  修 改 人：贾琳飞
  修改内容：根据传入的表名和日期，获取对应的分区号
*/
RETURN NUMBER IS
   v_partition       NUMBER (2);--当前分区
   v_count           NUMBER;
   v_MAG_LOG_CTRL MAG_LOG_CTRL%rowtype;--日志分区控制表对象
BEGIN
   begin
   SELECT LOG_CURR_PART, LOG_TOTAL_PART, LOG_INTERVAL, LOG_CURR_DATE
     INTO v_MAG_LOG_CTRL.LOG_CURR_PART, v_MAG_LOG_CTRL.LOG_TOTAL_PART, v_MAG_LOG_CTRL.LOG_INTERVAL, v_MAG_LOG_CTRL.LOG_CURR_DATE
     FROM MAG_LOG_CTRL
    WHERE log_id = UPPER (i_TABLE);
    exception
    when others then
      RETURN -1;
    end ;

   --根据时间输入时间对应的分区
   IF v_MAG_LOG_CTRL.LOG_INTERVAL = '1'
   THEN                                                         --处理频度为日
      v_count :=
              TO_DATE (v_MAG_LOG_CTRL.LOG_CURR_DATE, 'yyyymmdd')
              - TO_DATE (i_date, 'yyyymmdd');
                                         --得到当前日期和上次清日志的时间间隔
   ELSIF v_MAG_LOG_CTRL.LOG_INTERVAL = '2'
   THEN                                                       --处理频度为每月
      v_count :=
         MONTHS_BETWEEN (TO_DATE (SUBSTR (v_MAG_LOG_CTRL.LOG_CURR_DATE, 1, 6), 'yyyymm'),
                         TO_DATE (SUBSTR (i_date, 1, 6), 'yyyymm')
                        );                --得到当前月份和上次清日志的时间间隔
   ELSIF v_MAG_LOG_CTRL.LOG_INTERVAL = '3'
   THEN                                                     --处理频度为年存放
      v_count :=
         (  TO_NUMBER (SUBSTR (v_MAG_LOG_CTRL.LOG_CURR_DATE, 1, 4))
          - TO_NUMBER (SUBSTR (i_date, 1, 4))
         );                               --得到当前月份和上次清日志的时间间隔
   END IF;

   v_count := MOD (v_count, v_MAG_LOG_CTRL.LOG_TOTAL_PART);

   IF v_count < 0
   THEN
      v_count := v_count + v_MAG_LOG_CTRL.LOG_TOTAL_PART;
   END IF;

   IF v_MAG_LOG_CTRL.LOG_CURR_PART > v_count
   THEN
      v_partition := v_MAG_LOG_CTRL.LOG_CURR_PART - v_count;
   ELSE
      v_partition := v_MAG_LOG_CTRL.LOG_CURR_PART + v_MAG_LOG_CTRL.LOG_TOTAL_PART - v_count;
   END IF;

   RETURN v_partition;
EXCEPTION
   WHEN OTHERS
   THEN
      RETURN -1;
END func_getpartition;
/


CREATE OR REPLACE function where_at_i return varchar2
 is
     l_owner     varchar2(30);
     l_name      varchar2(30);
     l_lineno    number;
     l_type      varchar2(30);
 begin
    who_called_me( l_owner, l_name, l_lineno, l_type );
    return l_lineno;
 end;
/


CREATE OR REPLACE function who_am_i return varchar2
 is
     l_owner     varchar2(30);
     l_name      varchar2(30);
     l_lineno    number;
     l_type      varchar2(30);
 begin
    who_called_me( l_owner, l_name, l_lineno, l_type );
    return l_owner || '.' || l_name||'.'||l_lineno||'.'||l_type;
 end;
/


CREATE OR REPLACE FUNCTION func_getLOADPROTOCOLCODE(i_table varchar2) RETURN varchar2
/*
  created:20100519
  creator:kfzx-jialf
  content:return protocal code
*/

 IS
  v_date     VARCHAR2(8);
  v_clientinfo  varchar2(64);
  v_type    varchar2(1);
BEGIN

  RETURN '';
EXCEPTION
  WHEN OTHERS THEN
    return('');
END func_getLOADPROTOCOLCODE;
/


CREATE OR REPLACE FUNCTION func_getCHNINFO RETURN VARCHAR2 IS
/*
  created:20100519
  creator:kfzx-jialf
  content:get channel info,web:opname,batch:work_batchid,job:job_no,scripts:project_no

  channel type:
  web:       'w'|chn_no|time_zone|area_code|lang_code|emp_code|op
  batch:    'b'|chn_no|group_code|work_batcmode|work_batchid|parallel_id
  job:      'j'|chn_no|time_zone|area_code|lang_code|emp_code|job_no
  scripts:  's'|chn_no|time_zone|area_code|lang_code|emp_code|project_no
*/
  v_type    varchar2(1);
  v_str       VARCHAR2(64):='';
  v_clientinfo  varchar2(64);
BEGIN
  --v_clientinfo := sys_context('userenv', 'client_info');
  v_clientinfo := '';

  IF v_clientinfo IS NULL OR v_clientinfo = '' THEN
    return v_str;
  else
    v_type := func_getcontent(v_clientinfo,'|', 1);
    if v_type = 'w' or v_type = 's' or v_type = 'j' then
      v_str := func_getcontent(v_clientinfo,'|', 7);
    else
      v_str := func_getcontent(v_clientinfo,'|', 5);
    end if;
  END IF;

  return  v_str;
exception when others then
   return  v_str;
END func_getCHNINFO;
/


CREATE OR REPLACE FUNCTION        "FUNC_GETCHNNO" RETURN NUMBER IS
/*
  created:20100519
  creator:kfzx-jialf
  content:get channel no

  channel type:
  web:       'w'|chn_no|time_zone|area_code|lang_code|emp_code|op
  batch:    'b'|chn_no|group_code|work_batcmode|work_batchid|parallel_id
  job:      'j'|chn_no|time_zone|area_code|lang_code|emp_code|job_no
  scripts:  's'|chn_no|time_zone|area_code|lang_code|emp_code|project_no
*/

  V_CHNNO       NUMBER := 0;
  v_clientinfo  varchar2(64);
BEGIN
  --v_clientinfo := sys_context('userenv', 'client_info');
  v_clientinfo := '';

  IF v_clientinfo IS NULL OR v_clientinfo = '' THEN
    return V_CHNNO;
  else
      V_CHNNO := func_getcontent(v_clientinfo,'|', 2);
  END IF;

  return  V_CHNNO;
exception when others then
   return  0;
END func_getCHNNO;
/


CREATE OR REPLACE function      FUNC_GETBATCHDAY(i_groupcode in varchar2)
  return varchar2 is
  WorkDay varchar2(8);
begin
  select nvl(mag_workdate.WORKDATE, to_char(sysdate,'YYYYMMDD'))
    into WorkDay
    from mag_workdate
   where
     GROUP_CODE = i_groupcode
     and mag_workdate.VALID = '1'
     and rownum = 1;

  return(WorkDay);
exception
  when others then
    return to_char(sysdate,'YYYYMMDD');
end FUNC_GETBATCHDAY;
/


CREATE OR REPLACE FUNCTION func_DCT_clob_analyze(str         IN CLOB,
                                             i_separator IN VARCHAR2)
--分隔符默认为'$|$'，可自己设定,法则:str1$|$str2$|$str3$|$
  --注意确保最后一个元素后必须要有一个分隔符　或　确保最后一个元素非空
 RETURN DCT_ARRYTYPE IS
  RESULT DCT_ARRYTYPE;
  i      INTEGER;
  b_pos  INTEGER;
  e_pos  INTEGER;
  len    INTEGER;

  separator VARCHAR2(10);
  sep_len   NUMBER;
BEGIN
  separator := i_separator;
  IF (separator IS NULL)
  THEN
    separator := '$|$';
  END IF;
  sep_len := length(separator);
  RESULT  := DCT_ARRYTYPE();
  i       := 1;
  b_pos   := 1;
  e_pos   := dbms_lob.instr(str,
                            separator,
                            1,
                            i);

  WHILE (e_pos != 0)
  LOOP
    RESULT.EXTEND();
    len := e_pos - b_pos;
    RESULT(i) := dbms_lob.substr(str,
                                 len,
                                 b_pos);
    i := i + 1;
    b_pos := e_pos + sep_len;
    e_pos := dbms_lob.instr(str,
                            separator,
                            1,
                            i);
  END LOOP;
  e_pos := dbms_lob.getlength(str) + 1;
  IF (b_pos <> e_pos)
  THEN
    RESULT.EXTEND();
    len := dbms_lob.getlength(str) + 1 - b_pos;
    RESULT(i) := dbms_lob.substr(str,
                                 len,
                                 b_pos);
  END IF;
  RETURN(RESULT);

EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END func_DCT_clob_analyze;
/


CREATE OR REPLACE FUNCTION func_DCT_str_analyze(str         IN VARCHAR2,
                                            i_separator IN VARCHAR2)
--分隔符默认为'$|$'，可自己设定,法则:str1$|$str2$|$str3$|$

 RETURN DCT_ARRYTYPE IS
  RESULT DCT_ARRYTYPE;
  i      INTEGER;
  b_pos  INTEGER;
  e_pos  INTEGER;
  len    INTEGER;

  separator VARCHAR2(10);
  sep_len   NUMBER;
BEGIN
  separator := i_separator;
  IF (separator IS NULL)
  THEN
    separator := '$|$';
  END IF;
  sep_len := length(separator);
  RESULT  := DCT_ARRYTYPE();
  i       := 1;
  b_pos   := 1;
  e_pos   := instr(str,
                   separator,
                   1,
                   i);

  WHILE (e_pos != 0)
  LOOP
    RESULT.EXTEND();
    IF (e_pos - b_pos > 4000)
    THEN
      len := 4000;
    ELSE
      len := e_pos - b_pos;
    END IF;
    RESULT(i) := substr(str,
                        b_pos,
                        len);
    i := i + 1;
    b_pos := e_pos + sep_len;
    e_pos := instr(str,
                   separator,
                   1,
                   i);
  END LOOP;
  e_pos := length(str) + 1;
  IF (b_pos <> e_pos)
  THEN
    RESULT.EXTEND();
    RESULT(i) := substr(str,
                        b_pos,
                        length(str));
  END IF;
  RETURN(RESULT);

EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END func_DCT_str_analyze;
/


CREATE OR REPLACE FUNCTION func_getdsrtransno_fova RETURN VARCHAR2 IS
  v_trans_num VARCHAR2(8);
BEGIN

  SELECT lpad(to_char(SEQ_DSRTRANSNO_FOVA.nextval), 8, '0')
    INTO v_trans_num
    FROM dual;

  RETURN v_trans_num;

EXCEPTION
  WHEN OTHERS THEN
    RETURN 'xxxxxxxx';

END func_getdsrtransno_fova;
/


CREATE OR REPLACE FUNCTION func_getarrayfromstr (srcstr IN VARCHAR2, split IN CHAR)
      RETURN arrytype
   IS
      idx          INTEGER;
      splitcount   INTEGER;
      objdata      arrytype;

      FUNCTION GETCONTENT (Content      VARCHAR2,
                           ContentId    NUMBER,
                           Split        VARCHAR2)
         RETURN VARCHAR2
      IS
         TmpContent      VARCHAR2 (32000);
         ResultContent   VARCHAR2 (5000);
         CountFlag       NUMBER := 0;
         CurrCount       NUMBER := 0;
      BEGIN
         TmpContent := Content;

         FOR i IN 1 .. ContentId
         LOOP
            CountFlag := INSTR (TmpContent, Split, 1);
            CurrCount := i;

            IF (CountFlag = 0)
            THEN
               ResultContent := TmpContent;
               GOTO out_loop;
            ELSE
               ResultContent := SUBSTR (TmpContent, 1, CountFlag - 1);
               TmpContent :=
                  SUBSTR (TmpContent,
                          CountFlag + 1,
                          LENGTH (TmpContent) - CountFlag);
            END IF;
         END LOOP;

        <<out_loop>>
         IF (CurrCount = ContentId)
         THEN
            RETURN ResultContent;
         ELSE
            RETURN NULL;
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            RETURN '';
      END Getcontent;
   /******************************************************************************
      NAME:       getarrayfromstr
      PURPOSE:    分离以|为间隔的参数,并返回集合

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        2007-9-7          1. Created this function.

      NOTES:

      Automatically available Auto Replace Keywords:
         Object Name:     getarrayfromstr
         Sysdate:         2007-9-7
         Date and Time:   2007-9-7, 14:07:15, and 2007-9-7 14:07:15
         Username:         (set in TOAD Options, Procedure Editor)
         Table Name:       (set in the "New PL/SQL Object" dialog)

   ******************************************************************************/
   BEGIN
      IF srcstr IS NULL
      THEN
         RETURN NULL;
      END IF;

      objdata := arrytype ();

      --get count of split
      splitcount :=
           LENGTH (REPLACE (srcstr, split, split || split))
         - LENGTH (srcstr)
         + 1;

      IF splitcount = 1
      THEN
         --not contain split
         objdata.EXTEND ();
         objdata (1) := srcstr;
         RETURN objdata;
      END IF;

      FOR idx IN 1 .. splitcount
      LOOP
         objdata.EXTEND ();
         objdata (idx) := getcontent (srcstr, idx, split);
      END LOOP;

      RETURN objdata;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END func_getarrayfromstr;
/


CREATE OR REPLACE FUNCTION FUNC_GETNEXTSEQ (in_table_name   IN VARCHAR2,
                                                 in_col_name     IN VARCHAR2,
                                                 in_number       IN VARCHAR2)
   RETURN VARCHAR2
IS
   ret     VARCHAR2 (5000);                                      /*任何异常均返回-1*/
   v_sql   VARCHAR2 (5000);
BEGIN
   /*in_number_type:0表示都是数字，1表示前一位不是数字。。。n表示前n位不是数字*/
   IF in_number = '0'
   THEN
      /*列值均为数字作为查询条件*/
      v_sql :=
            'SELECT rownum seq FROM all_objects'
         || ' WHERE rownum <= (SELECT MAX(to_number('
         || in_col_name
         || '))'
         || ' FROM '
         || in_table_name
         || ' WHERE regexp_like('
         || in_col_name
         || ' , ''^([0-9]+)$'')) MINUS SELECT to_number('
         || in_col_name
         || ' ) seq FROM '
         || in_table_name
         || ' WHERE regexp_like('
         || in_col_name
         || ', ''^([0-9]+)$'')';
   ELSE
      /*列值前in_number位为非数字，in_number+1位以后均为数字作为查询条件*/
      v_sql :=
            'SELECT rownum seq FROM all_objects'
         || ' WHERE rownum <= (SELECT MAX(to_number(substrb('
         || in_col_name
         || ',to_number('
         || in_number
         || ')+1)))'
         || ' FROM '
         || in_table_name
         || ' WHERE regexp_like(substrb('
         || in_col_name
         || ',1,'
         || in_number
         || ') , ''^([^0-9]+)$'') AND regexp_like(substrb('
         || in_col_name
         || ',to_number('
         || in_number
         || ')+1), ''^([0-9]+)$'')) MINUS SELECT to_number(substrb('
         || in_col_name
         || ',to_number('
         || in_number
         || ')+1)) seq FROM '
         || in_table_name
         || ' WHERE regexp_like(substrb('
         || in_col_name
         || ',1,'
         || in_number
         || ') , ''^([^0-9]+)$'') AND regexp_like(substrb('
         || in_col_name
         || ',to_number('
         || in_number
         || ')+1), ''^([0-9]+)$'')';
   END IF;

   v_sql := 'begin select min(seq) into :ret from (' || v_sql || '); end;';

   EXECUTE IMMEDIATE v_sql USING OUT ret;

   /*ret为空表示当前列值无遗漏，取当前列值的最大值+1*/
   IF ret IS NULL
   THEN
      IF in_number = '0'
      THEN
         /*列值均为数字作为查询条件*/
         v_sql :=
               'SELECT nvl(MAX(to_number('
            || in_col_name
            || ')),0)+1 seq FROM '
            || in_table_name
            || ' WHERE regexp_like('
            || in_col_name
            || ' , ''^([0-9]+)$'')';
      ELSE
         /*列值前in_number位为非数字，in_number+1位以后均为数字作为查询条件*/
         v_sql :=
               'SELECT nvl(MAX(to_number(substrb('
            || in_col_name
            || ',to_number('
            || in_number
            || ')+1))),0)+1 seq FROM '
            || in_table_name
            || ' WHERE regexp_like(substrb('
            || in_col_name
            || ',1,'
            || in_number
            || ') , ''^([^0-9]+)$'') AND regexp_like(substrb('
            || in_col_name
            || ',to_number('
            || in_number
            || ')+1), ''^([0-9]+)$'')';
      END IF;

      EXECUTE IMMEDIATE 'SELECT (' || v_sql || ') FROM dual' INTO ret;
   END IF;

   RETURN (ret);
EXCEPTION
   WHEN OTHERS
   THEN
      dbms_output.put_line(sqlerrm);
      RETURN '-1';
END FUNC_GETNEXTSEQ;
/


CREATE OR REPLACE FUNCTION func_get_wherestr(i_table_enname IN VARCHAR2,
                                               i_table_owner  IN VARCHAR2)
  RETURN VARCHAR2 IS
  v_table_name     dct_dataclr_policy.table_enname%TYPE;
  v_remain_time    VARCHAR2(20);
  v_time_cole_type VARCHAR2(200);
  v_where_str      VARCHAR2(200);
  v_type_col_cmp   VARCHAR2(200);
  v_clr_type       VARCHAR2(200);
  v_where_sql      VARCHAR2(200);
BEGIN
  v_where_str := '';

  SELECT t.clr_way
    INTO v_clr_type
    FROM dct_dataclr_policy t
   WHERE t.table_owner = i_table_owner
     AND t.table_enname = i_table_enname;

  IF v_clr_type <> '1'
  THEN
    RETURN '';
  END IF;

  SELECT t.where_sql
    INTO v_where_sql
    FROM dct_dataclr_condition t
   WHERE t.table_owner = i_table_owner
     AND t.table_enname = i_table_enname;

  IF v_where_sql <> '' OR v_where_sql IS NOT NULL
  THEN
    RETURN v_where_sql;
  END IF;

  --查询清理条件表
  FOR v1 IN (SELECT t.time_col,
                    t.on_remain_time,
                    decode(t.on_remain_time_unit,
                           '1',
                           '天',
                           '2',
                           '个月',
                           '3',
                           '年',
                           t.on_remain_time_unit) on_remain_time_unit,
                    t.type_col,
                    t.type_col_cmp,
                    t.type_col_value,
                    t.table_enname,
                    t.table_owner
               FROM dct_dataclr_condition t
              WHERE t.table_enname = i_table_enname
                AND t.table_owner = i_table_owner)
  LOOP
    IF v_where_str IS NOT NULL
    THEN
      v_where_str := v_where_str || ' and ';
    END IF;
    --同时组合按时间清理和按状态字段清理的条件
    IF v1.time_col IS NOT NULL
    THEN

      v_where_str := v1.time_col || '<' || 'sysdate-' || v1.on_remain_time ||
                     v1.on_remain_time_unit;

    ELSE
      v_where_str := '';
    END IF;

    IF v1.type_col IS NOT NULL
    THEN
      IF v_where_str IS NOT NULL
      THEN
        v_where_str := v_where_str || ' and ';
      END IF;
      SELECT decode(v1.type_col_cmp,
                    '1',
                    '=',
                    '2',
                    '<',
                    '3',
                    '<=',
                    '4',
                    '>',
                    '5',
                    '>=',
                    '=')
        INTO v_type_col_cmp
        FROM dual;
      v_where_str := v_where_str || v1.type_col || v_type_col_cmp || '''' ||
                     v1.type_col_value || '''';
    ELSE
      v_where_str := v_where_str;
    END IF;
    --由于主键限制,该查询必然只能查出一条,故循环一次后就直接返回
    RETURN v_where_str;
  END LOOP;
END func_get_wherestr;
/


CREATE OR REPLACE function get_seq_REP_DBMOCK(seq_name in varchar2)
        return number
 is
        seq_val number;
begin
        execute immediate 'select ' || seq_name || '.nextval from dual' into seq_val;
        return seq_val;
end get_seq_REP_DBMOCK;
/


CREATE OR REPLACE FUNCTION FUNC_A9( istr IN numeric )
RETURN numeric
is
BEGIN
RETURN istr;
END;
/


CREATE OR REPLACE FUNCTION wm_concat_clob(input VARCHAR2) RETURN CLOB
  PARALLEL_ENABLE
  AGGREGATE USING string_agg_type_clob;
/


CREATE OR REPLACE FUNCTION func_getday RETURN varchar2
/*
  修改时间：20110104
  修 改 人：
  修改内容：
*/

 IS
  v_date    VARCHAR2(8);
  v_clientinfo  varchar2(64);
  v_type    varchar2(1);
BEGIN
    return( to_char(sysdate, 'yyyymmdd'));
EXCEPTION
  WHEN OTHERS THEN
    return( to_char(sysdate, 'yyyymmdd'));
END func_getday;
/


CREATE OR REPLACE FUNCTION wm_concat(input VARCHAR2) RETURN VARCHAR2
  PARALLEL_ENABLE
  AGGREGATE USING string_agg_type;
/


CREATE OR REPLACE FUNCTION FUNC_A8( istr IN  VARCHAR2 )
 RETURN VARCHAR2
 is
BEGIN
     RETURN istr;
END;
/


CREATE OR REPLACE FUNCTION func_getdsrtransno_nova RETURN VARCHAR2 IS
  v_trans_num VARCHAR2(8);
BEGIN

  SELECT lpad(to_char(SEQ_DSRTRANSNO_NOVA.nextval), 8, '0')
    INTO v_trans_num
    FROM dual;

  RETURN v_trans_num;

EXCEPTION
  WHEN OTHERS THEN
    RETURN 'xxxxxxxx';

END func_getdsrtransno_nova;
/


CREATE OR REPLACE function FUNC_GETCUSTOMERNAME(customercode in VARCHAR2)
  return varchar2 is
  Result varchar2(180);
begin
  BEGIN
    SELECT ma.customer_name
      INTO RESULT
      FROM NP_CUSTOMER MA
     WHERE ma.customer_code = customercode;
    return(Result);
  EXCEPTION
    WHEN OTHERS THEN
      Result := '';
      return(Result);
  END;
end FUNC_GETCUSTOMERNAME;
/


CREATE OR REPLACE FUNCTION func_getareaformatdate(i_area_code IN VARCHAR2,
                                                  i_format    IN VARCHAR2)
  RETURN VARCHAR2 IS
  v_date   DATE;
  v_diff   MAG_TIMEZONE.TIME_DIFF%TYPE;
  v_result VARCHAR2(100);
BEGIN
  --获取时差
  BEGIN
    SELECT TIME_DIFF
      INTO v_diff
      FROM MAG_TIMEZONE
     WHERE TIME_ZONE =
           (SELECT TIME_ZONE FROM MAG_AREA WHERE AREA_CODE = i_area_code);
  EXCEPTION
    WHEN OTHERS THEN
      v_diff := 0;
  END;

  --获取B地区的时间
  v_date   := sysdate + (v_diff) / 24;
  v_result := TO_CHAR(v_date, i_format);

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    RETURN '1970-01-01';

END func_getareaformatdate;
/


CREATE OR REPLACE FUNCTION func_get_areaparameter(i_table_name IN VARCHAR2, --参数表名
                                                  i_area_code  IN VARCHAR2, -- 机构代码
                                                  i_para_code  IN VARCHAR2 --参数名
                                                  )
  RETURN mag_parameter_area%ROWTYPE IS
  RESULT mag_parameter_area%ROWTYPE;
  v_flag VARCHAR2(1);
BEGIN
  v_flag := '';
  FOR rec IN (SELECT area_code
                FROM mag_area
               START WITH area_code = i_area_code
              CONNECT BY area_code = PRIOR belong_area_code) LOOP
    BEGIN
      SELECT *
        INTO RESULT
        FROM mag_parameter_area
       WHERE table_name = i_table_name
         AND area_code = rec.area_code
         AND para_code = i_para_code;

      v_flag := '1';
    EXCEPTION
      WHEN no_data_found THEN
        v_flag := '0';
      WHEN OTHERS THEN
        RETURN NULL;
    END;
    IF v_flag = '1' THEN
      RETURN RESULT;
    END IF;
  END LOOP;

  RETURN NULL;
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END func_get_areaparameter;
/


CREATE OR REPLACE FUNCTION FUNC_CHECKBANKFLAG
(
    is_bank_flag      IN VARCHAR2,
    is_valid_bankflag IN VARCHAR2
) RETURN VARCHAR2 IS
    RESULT           VARCHAR2(10);
    max_bankflag     NUMBER(10);
    min_bankflag     NUMBER(10);
    v_valid_bankflag VARCHAR2(10);
    v_bank_flag      mag_area.BANK_LEVEL%TYPE;
BEGIN
    v_bank_flag      := is_bank_flag;
    v_valid_bankflag := is_valid_bankflag;
    min_bankflag     := SUBSTR(v_valid_bankflag,
                               1,
                               INSTR(v_valid_bankflag, ',') - 1);
    max_bankflag     := SUBSTR(v_valid_bankflag,
                               INSTR(v_valid_bankflag, ',') + 1);
    IF (v_bank_flag <= TO_NUMBER(max_bankflag) AND
       v_bank_flag >= TO_NUMBER(min_bankflag)) THEN
        RESULT := 'true';
    ELSE
        RESULT := 'false';
    END IF;
    RETURN(RESULT);
END FUNC_CHECKBANKFLAG;
/


CREATE OR REPLACE function FUNC_GETFORMATWORKDAY(i_area_code in varchar2,
                                                  i_format    IN VARCHAR2)
  return varchar2 is
  WorkDay varchar2(10);
begin
/*  select to_char(to_date(nvl(mag_workdate.WORKDATE, '19700101'),'yyyyMMdd'),i_format)
    into WorkDay
    from mag_area, mag_workdate
   where mag_area.AREA_CODE = i_area_code
     and mag_area.GROUP_CODE = mag_workdate.GROUP_CODE
     and mag_workdate.VALID = '1'
     and rownum = 1;*/

     select to_char(to_date(FUNC_GETWORKDAY(i_area_code),'yyyyMMdd'),i_format)
      into WorkDay
      from dual;

  return(WorkDay);
exception
  when others then
    return '19700101';
end FUNC_GETFORMATWORKDAY;
/


CREATE OR REPLACE FUNCTION FUNC_GETSERVERZONE return varchar2 is
  Result       varchar2(64);
  v_clientinfo varchar2(64);
  v_beginpos   number(3);
  v_endpos     number(3);
begin
  Result       := '1';
  /*已经废弃
  v_clientinfo := SYS_CONTEXT('userenv', 'client_info');
  if (v_clientinfo is not null) then
    v_beginpos := instr(v_clientinfo, '|', 1, 1);
    v_endpos   := instr(v_clientinfo, '|', 1, 2);
    if (v_beginpos <> 0 and v_endpos <> 0) then
      Result := substr(v_clientinfo,
                       v_beginpos + 1,
                       v_endpos - v_beginpos - 1);
    end if;
  end if;
  */
  return(Result);
end FUNC_GETSERVERZONE;
/


CREATE OR REPLACE FUNCTION      FUNC_GETTIMEZONE return varchar2 is
  Result       MAG_AREA.Time_Zone%type;
  v_clientinfo varchar2(64);
  v_beginpos   number(3);
  v_endpos     number(3);
begin
  Result       := 'GMT+08:00';
  --v_clientinfo := SYS_CONTEXT('userenv', 'client_info');
  v_clientinfo :='';
  if (v_clientinfo is not null) then
    v_beginpos := instr(v_clientinfo, '|', 1, 2);
    v_endpos   := instr(v_clientinfo, '|', 1, 3);
    if (v_beginpos <> 0 and v_endpos <> 0) then
      Result := substr(v_clientinfo,
                       v_beginpos + 1,
                       v_endpos - v_beginpos - 1);
    end if;
  end if;
  return(Result);
end FUNC_GETTIMEZONE;
/


CREATE OR REPLACE FUNCTION "GETINFOMSGWITHPARAM" (
      err_code   IN   VARCHAR2,
      langcode   IN   VARCHAR2,
      param      IN   VARCHAR2
   )
      RETURN VARCHAR2 is
  Result varchar2(2000);
begin
  Result:=pckg_errormsg_manage.getinfomsgwithparam(err_code,langcode,param);
  return(Result);
end getinfomsgwithparam;
/


CREATE OR REPLACE FUNCTION FUNC_GETAREA return varchar2 is
  Result       MAG_AREA.AREA_CODE%type;
  v_clientinfo varchar2(64);
  v_beginpos   number(3);
  v_endpos     number(3);
begin
  Result       := '0';
  --v_clientinfo := SYS_CONTEXT('userenv', 'client_info');
  v_clientinfo := '';
  if (v_clientinfo is not null) then
    v_beginpos := instr(v_clientinfo, '|', 1, 3);
    v_endpos   := instr(v_clientinfo, '|', 1, 4);
    if (v_beginpos <> 0 and v_endpos <> 0) then
      Result := substr(v_clientinfo,
                       v_beginpos + 1,
                       v_endpos - v_beginpos - 1);
    end if;
  end if;
  return(Result);
end FUNC_GETAREA;
/


CREATE OR REPLACE FUNCTION GETINFOMSGWITHNOFORMAT (
      err_code   IN   VARCHAR2,
      langcode   IN   VARCHAR2,
      param      IN   VARCHAR2
   )
      RETURN VARCHAR2 is
  Result varchar2(2000);
begin
  Result:=pckg_errormsg_manage.getinfomsgwithnoformat(err_code,langcode,param);
  return(Result);
end getinfomsgwithnoformat;
/


CREATE OR REPLACE FUNCTION FUNC_GETCURRDATE return date is
  Result date;
begin
  Result := FUNC_GETAREADATE(FUNC_GETAREA);
  return(Result);
end FUNC_GETCURRDATE;
/



CREATE OR REPLACE FUNCTION func_getPRODCODE (i_table varchar2) RETURN varchar2
/*
  created:20100519
  creator:kfzx-jialf
  content:get product code
*/
 IS
  v_date     VARCHAR2(8);
  v_clientinfo  varchar2(64);
  v_type    varchar2(1);
BEGIN

  RETURN '';
EXCEPTION
  WHEN OTHERS THEN
    return('');
END func_getPRODCODE;
/


CREATE OR REPLACE FUNCTION get_amt_for_curr_mon(in_area_code       IN VARCHAR2, --机构
                            in_real_mon      IN VARCHAR2, --日期
                            in_currency        IN VARCHAR2, --币种
                            in_amount          IN NUMBER, --金额
                            in_unit            IN PLS_INTEGER, --1:元 10000:万元
                            in_target_currency IN VARCHAR2 default '001' --目标币种001人民币
                            ) RETURN VARCHAR2 IS

    out_flag              varchar2(2);
    out_msg               varchar2(2000);
    out_convert_rate      NUMBER(17, 9);
    out_convert_rate_unit varchar2(10);
    out_target_amount     NUMBER(17, 2);
    v_amount              varchar2(18);
    v_real_mon           varchar2(8);
  begin

    if trim(in_amount) is null or in_amount = 0 then
      return '0.00';
    end if;

    if in_real_mon is null then
      select substr(icbc.func_getworkday(in_area_code),1,6) into v_real_mon from dual;
      v_real_mon := v_real_mon || '31';
    else
      v_real_mon := in_real_mon || '31';
    end if;

    if v_real_mon is null or length(v_real_mon) <> 8 then
      return '';
    end if;

    pckg_exchange_rate.rate_exchange_new(in_area_code, --机构
                                         v_real_mon, --日期
                                         in_currency, --币种
                                         in_amount, --金额
                                         in_target_currency, --目标币种
                                         null, --汇率类型(1:实时汇率，2:月末汇率，3:年初汇率)
                                         out_flag, --程序执行状态
                                         out_msg, --程序执行说明
                                         out_convert_rate, --折算汇率
                                         out_convert_rate_unit, --折算汇率单位
                                         out_target_amount --目标金额
                                         );

    v_amount := trim(to_char(out_target_amount / in_unit,
                             '999999999999990.99'));
    return v_amount;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '';
  end get_amt_for_curr_mon;
/



CREATE OR REPLACE FUNCTION FUNC_GETLANG return varchar2 is
  Result       MAG_EMPLOYEE.Default_Language%type;
  v_clientinfo varchar2(64);
  v_beginpos   number(3);
  v_endpos     number(3);
begin
  Result       := 'zh_CN';
  --v_clientinfo := SYS_CONTEXT('userenv', 'client_info');
  v_clientinfo := '';
  if (v_clientinfo is not null) then
    v_beginpos := instr(v_clientinfo, '|', 1, 5);
    v_endpos   := instr(v_clientinfo, '|', 1, 6);
    if (v_beginpos <> 0 and v_endpos <> 0) then
      Result := substr(v_clientinfo,
                       v_beginpos + 1,
                       v_endpos - v_beginpos - 1);
    end if;
  end if;
  return(Result);
end FUNC_GETLANG;
/


CREATE OR REPLACE function FUNC_GETEMPNAME(empcode in VARCHAR2) return varchar2 is
  Result varchar2(100);
begin
 BEGIN
      select me.EMPLOYEE_Name
into Result
from mag_employee me
 where me.EMPLOYEE_CODE=empcode;
 --Result := '测试人员名称';--addfortest
  return(Result);
   EXCEPTION
      WHEN OTHERS
      THEN
         Result:='';
  --Result := '测试人员名称';--addfortest
        return(Result);
   END;
end FUNC_GETEMPNAME;
/


CREATE OR REPLACE function func_gram_getdictcode( in_app_code varchar2, in_bean_code varchar2,in_dict_code varchar2) return varchar2
is
/*
  修改内容：字典对照功能
  修改时间：20111121
  修改人员：kfzx-jialf

  修改时间：20111130
  修改人员：kfzx-jialf
  修改内容：对于无设置的对照关系，返回原值。
*/
  v_dict_code icbc.sys_dict_mapping.dist_dict%type;
begin
    select dist_dict  into v_dict_code from icbc.sys_dict_mapping where app_code = in_app_code and bean_code = in_bean_code and  sour_dict = in_dict_code;
    return v_dict_code;
exception when others then
    return in_dict_code;
end;
/


CREATE OR REPLACE function FUNC_CALCDATE(i_target_area_code   in varchar2,
                                         i_resource_area_code in varchar2,
                                         i_resource_date      in date)
  return date is
  Result   date;
  v_diff_a MAG_TIMEZONE.TIME_DIFF%type;
  v_diff_b MAG_TIMEZONE.TIME_DIFF%type;
begin
  --获取时差
  begin
    select TIME_DIFF
      into v_diff_a
      from MAG_TIMEZONE
     where TIME_ZONE =
           (select TIME_ZONE
              from MAG_AREA
             where AREA_CODE = i_resource_area_code);

    select TIME_DIFF
      into v_diff_b
      from MAG_TIMEZONE
     where TIME_ZONE = (select TIME_ZONE
                          from MAG_AREA
                         where AREA_CODE = i_target_area_code);
  exception
    when others then
      return(i_resource_date);
  end;

  --获取B地区的时间
  begin
    Result := i_resource_date + (v_diff_b - v_diff_a) / 24;
  exception
    when others then
      return(i_resource_date);
  end;

  --返回
  return(Result);
end FUNC_CALCDATE;
/

CREATE OR REPLACE FUNCTION CHAN_LIMIT_QUERY(V_EBUSER_NO             IN VARCHAR2,
                                            V_CHAN_TYP              IN VARCHAR2,
                                            V_SEC_WAY               IN VARCHAR2,
                                            V_SEC_WAY_NAME          IN VARCHAR2,
                                            V_CCY                   IN VARCHAR2,
                                            V_TRANS_DATE            IN VARCHAR2,
                                            V_IS_HAS_SOTP           IN VARCHAR2,
                                            V_SMART_LIMIT_LIST_DATA IN OUT LIMIT_LIST_TYPE_NEW)
  RETURN VARCHAR2 AS
  V_HAS_SEC_WAY            VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
  V_IS_CUST_AUTH_LIMIT     VARCHAR2(1); --是否有客户自定义限额 0-没有 1-有
  V_DAY_SUM_LIMIT_USE      NUMBER(18, 4); --客户单日已使用限额
  V_ONCE_LIMIT             NUMBER(18, 4); --单笔限额
  V_DAY_LIMIT              NUMBER(18, 4); --单日累计限额
  V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18, 4); -- 客户单日剩余额度
  --add--by-feixiaobo--20161109--增加单日笔数及年累计限额
  V_DAY_TOTAL              INTEGER;        --日累计笔数
  V_DAY_TOTAL_RESIDUAL     INTEGER;        --单日剩余累计笔数
  V_DAY_TOTAL_USE          INTEGER;        --单日已转账次数
  V_YEAR_LIMIT              NUMBER(18, 4); --年累计限额
  V_YEAR_SUM_LIMIT_RESIDUAL NUMBER(18, 4); -- 客户单年剩余额度
  V_YEAR_SUM_LIMIT_USE      NUMBER(18, 4); -- 客户单年已使用额度
  V_YEAR                   VARCHAR2(4);--当前年
  --add--by-feixiaobo--20161109--增加单日笔数及年累计限额
  --add--by--rc--20180630--增加客户版本号判断
  V_OPEN_WAY               VARCHAR2(4);  -- 用户版本类型   01-大众版  02-专业版
  --add--by--rc--20180630--增加非@盾认证方式默认限额表
BEGIN
  V_HAS_SEC_WAY        := '1'; --默认有客户认证方式
  V_IS_CUST_AUTH_LIMIT := '1'; --默认有客户自设限额
  V_YEAR := SUBSTR(V_TRANS_DATE,0,4); --当前年
  V_OPEN_WAY := '02';                 --默认专业版
  --add by rc 判断用户是大众版还是专业版
 DBMS_OUTPUT.PUT_LINE('1 V_OPEN_WAY===>' || V_OPEN_WAY);
  IF V_CHAN_TYP = '059' THEN
     SELECT G.OPEN_WAY
      INTO V_OPEN_WAY
      FROM QDZH.USER_CHANNEL G
     WHERE G.EBUSER_NO = V_EBUSER_NO
       AND G.CHAN_TYP = V_CHAN_TYP 
       AND G.CHANNEL_ID='0000000036';
  END IF;

  DBMS_OUTPUT.PUT_LINE('2 V_OPEN_WAY===>' || V_OPEN_WAY);
  --add by rc 判断用户是大众版还是专业版

  --查询客户今天当前认证方式已使用累计限额
  BEGIN
    --查询单日已使用限额与单日已转账次数
    BEGIN
      SELECT CCALU.DAY_SUM_LIMIT, CCALU.DAY_TOTAL
        INTO V_DAY_SUM_LIMIT_USE, V_DAY_TOTAL_USE
        FROM QDZH.CUST_CHAN_AUTH_LIM_USE CCALU
       WHERE CCALU.EBUSER_NO = V_EBUSER_NO
         AND CCALU.CHAN_TYP = V_CHAN_TYP
         AND CCALU.SEC_WAY = V_SEC_WAY
         AND CCALU.CCY = V_CCY
         AND CCALU.LAST_TRAN_DATE = V_TRANS_DATE;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_DAY_SUM_LIMIT_USE := 0;
        V_DAY_TOTAL_USE     := 0;
    END;
    --查询单年已使用限额
    BEGIN
      SELECT CCALU.YEAR_SUM_LIMIT
        INTO V_YEAR_SUM_LIMIT_USE
        FROM QDZH.CUST_CHAN_AUTH_LIM_USE CCALU
       WHERE CCALU.EBUSER_NO = V_EBUSER_NO
         AND CCALU.CHAN_TYP = V_CHAN_TYP
         AND CCALU.SEC_WAY = V_SEC_WAY
         AND CCALU.CCY = V_CCY
         AND CCALU.LAST_YEAR = V_YEAR;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_YEAR_SUM_LIMIT_USE := 0;
    END;
  END;
  --查询客户当前认证方式的单笔限额、单日累计限额、单日转账笔数、年累计限额
  BEGIN
    SELECT CCATT.ONCE_LIMIT, CCATT.DAY_LIMIT,CCATT.DAY_TOTAL,CCATT.YEAR_LIMIT
      INTO V_ONCE_LIMIT, V_DAY_LIMIT,V_DAY_TOTAL,V_YEAR_LIMIT
      FROM QDZH.CUST_CH_AU_SELF_LIM CCATT
     WHERE CCATT.EBUSER_NO = V_EBUSER_NO
       AND CCATT.CHAN_TYP = V_CHAN_TYP
       AND CCATT.SEC_WAY = V_SEC_WAY
       AND CCATT.CCY = V_CCY;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_IS_CUST_AUTH_LIMIT := '0';
  END;
  DBMS_OUTPUT.PUT_LINE('V_IS_CUST_AUTH_LIMIT===>' || V_IS_CUST_AUTH_LIMIT);
  --如果客户没有自设限额，则查询渠道系统默认限额
  IF V_IS_CUST_AUTH_LIMIT = '0' THEN
  DBMS_OUTPUT.PUT_LINE('V_SEC_WAY===>' || V_SEC_WAY);
  DBMS_OUTPUT.PUT_LINE('V_CHAN_TYP===>' || V_CHAN_TYP);
  DBMS_OUTPUT.PUT_LINE('V_CCY===>' || V_CCY);
  DBMS_OUTPUT.PUT_LINE('V_OPEN_WAY===>' || V_OPEN_WAY);
    --0-取默认表 1-取sotp默认表  060-网银取默认表
    IF V_IS_HAS_SOTP = '0' OR V_CHAN_TYP = '060' THEN
     DBMS_OUTPUT.PUT_LINE('CHAN_AUTH_LIM_DEF');
      SELECT CALD.ONCE_LIMIT, CALD.DAY_LIMIT,CALD.DAY_TOTAL,CALD.YEAR_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT,V_DAY_TOTAL,V_YEAR_LIMIT
        FROM QDZH.CHAN_AUTH_LIM_DEF CALD
       WHERE CALD.CHAN_TYP = V_CHAN_TYP
         AND CALD.SEC_WAY = V_SEC_WAY
         AND CALD.CCY = V_CCY
         -- alter by rc 增加版本号查询
         AND CALD.OPEN_WAY = V_OPEN_WAY;
         -- alter by rc 增加版本号查询
      DBMS_OUTPUT.PUT_LINE('V_ONCE_LIMIT' || V_ONCE_LIMIT || 'V_DAY_LIMIT：' || V_DAY_LIMIT ||
                       '，V_DAY_TOTAL：' || V_DAY_TOTAL || '，客户剩余额度：' ||
                       V_DAY_SUM_LIMIT_RESIDUAL ||'，V_YEAR_LIMIT'|| V_YEAR_LIMIT);
    ELSE
    DBMS_OUTPUT.PUT_LINE('SOTP_TRAN_AUTH_DEFL_LIMIT');
      SELECT SL.ONCE_LIMIT, SL.DAY_LIMIT,SL.DAY_TOTAL,SL.YEAR_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT,V_DAY_TOTAL,V_YEAR_LIMIT
        FROM QDZH.SOTP_TRAN_AUTH_DEFL_LIMIT SL
       WHERE SL.CHAN_TYP = V_CHAN_TYP
         AND SL.SEC_WAY = V_SEC_WAY
         AND SL.CCY = V_CCY
         AND SL.TRAN_TYPE = '1'; --转账
    END IF;
  END IF;
  --如果客户已使用限额大于 单日累计额度，则剩余额度为0
  --否则剩余= 单日累计限额-客户单日已使用限额
  IF V_DAY_SUM_LIMIT_USE >= V_DAY_LIMIT THEN
    V_DAY_SUM_LIMIT_RESIDUAL := 0;
  ELSE
    V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT - V_DAY_SUM_LIMIT_USE;
  END IF;
  --20161109-add-feixiaobo-增加单日剩余笔数，单年剩余限额
  IF V_YEAR_SUM_LIMIT_USE >= V_YEAR_LIMIT THEN
    V_YEAR_SUM_LIMIT_RESIDUAL := 0;
  ELSE
    V_YEAR_SUM_LIMIT_RESIDUAL := V_YEAR_LIMIT - V_YEAR_SUM_LIMIT_USE;
  END IF;

  IF V_DAY_TOTAL_USE >= V_DAY_TOTAL THEN
    V_DAY_TOTAL_RESIDUAL := 0;
  ELSE
    V_DAY_TOTAL_RESIDUAL := V_DAY_TOTAL - V_DAY_TOTAL_USE;
  END IF;
  --20161109-add-feixiaobo-增加单日剩余笔数，单年剩余限额
  DBMS_OUTPUT.PUT_LINE('客户号' || V_EBUSER_NO || '客户认证方式：' || V_SEC_WAY ||
                       '，客户单笔限额：' || V_ONCE_LIMIT || '，客户剩余额度：' ||
                       V_DAY_SUM_LIMIT_RESIDUAL ||'，单日笔数'|| V_DAY_TOTAL||'，单日剩余笔数'||V_DAY_TOTAL_RESIDUAL||'，单年累计限额'||V_YEAR_LIMIT
                       ||'，单年剩余累计限额'||V_YEAR_SUM_LIMIT_RESIDUAL);
  --为返回数据设置值
  V_SMART_LIMIT_LIST_DATA.EXTEND;
  V_SMART_LIMIT_LIST_DATA(V_SMART_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT_NEW(V_SEC_WAY,
                                                                             V_SEC_WAY_NAME,
                                                                             V_ONCE_LIMIT,
                                                                             V_DAY_SUM_LIMIT_RESIDUAL,
                                                                             V_DAY_LIMIT,
                                                                             V_DAY_TOTAL,
                                                                             V_DAY_TOTAL_RESIDUAL,
                                                                             V_YEAR_LIMIT,
                                                                             V_YEAR_SUM_LIMIT_RESIDUAL);

  RETURN V_HAS_SEC_WAY;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('func sqlcode : ' || SQLCODE);
    DBMS_OUTPUT.PUT_LINE('func sqlerrm : ' || SQLERRM);
  RETURN V_HAS_SEC_WAY;
END;
/

CREATE OR REPLACE FUNCTION CHAN_PAY_QUERY(V_EBUSER_NO           IN VARCHAR2,
                                          V_CHAN_TYP            IN VARCHAR2,
                                          V_SEC_WAY             IN VARCHAR2,
                                          V_SEC_WAY_NAME        IN VARCHAR2,
                                          V_CCY                 IN VARCHAR2,
                                          V_TRANS_DATE          IN VARCHAR2,
                                          V_IS_HAS_SOTP         IN VARCHAR2,
                                          V_PAY_LIMIT_LIST_DATA IN OUT LIMIT_LIST_TYPE)
  RETURN VARCHAR2 AS
  V_HAS_SEC_WAY            VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
  V_IS_CUST_AUTH_LIMIT     VARCHAR2(1); --是否有客户自定义限额 0-没有 1-有
  V_DAY_SUM_LIMIT_USE      NUMBER(18, 4); --客户单日已使用限额
  V_ONCE_LIMIT             NUMBER(18, 4); --单笔限额
  V_DAY_LIMIT              NUMBER(18, 4); --单日累计限额
  V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18, 4); -- 客户单日剩余额度
BEGIN
  V_HAS_SEC_WAY := '1'; --默认有客户认证方式
  --查询客户今天当前认证方式已使用累计限额
dbms_output.put_line(V_EBUSER_NO); 
 BEGIN
    SELECT CPALU.DAY_SUM_LIMIT
      INTO V_DAY_SUM_LIMIT_USE
      FROM QDZH.CUST_PAY_AUTH_LIM_USE CPALU
     WHERE CPALU.EBUSER_NO = V_EBUSER_NO
       AND CPALU.SEC_WAY = V_SEC_WAY
       AND CPALU.CCY = V_CCY
       AND CPALU.LAST_TRAN_DATE = V_TRANS_DATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_DAY_SUM_LIMIT_USE := 0;
  END;
dbms_output.put_line(V_DAY_SUM_LIMIT_USE); 
  --查询客户当前认证方式的单笔限额和单日累计限额
  BEGIN
    SELECT CPASL.ONCE_LIMIT, CPASL.DAY_LIMIT
      INTO V_ONCE_LIMIT, V_DAY_LIMIT
      FROM QDZH.CUST_PAY_AUTH_SET_LIMT CPASL
     WHERE CPASL.EBUSER_NO = V_EBUSER_NO
       AND CPASL.SEC_WAY = V_SEC_WAY
       AND CPASL.CCY = V_CCY;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_IS_CUST_AUTH_LIMIT := '0';
  END;
dbms_output.put_line(V_IS_CUST_AUTH_LIMIT); 
  --如果客户没有自设限额，则查询渠道系统默认限额
  IF V_IS_CUST_AUTH_LIMIT = '0' THEN

    --0-取默认表 1-取sotp默认表  060-网银取默认表
    IF V_IS_HAS_SOTP = '0' OR V_CHAN_TYP = '060' THEN
dbms_output.put_line(V_IS_HAS_SOTP); 
dbms_output.put_line(V_CHAN_TYP); 
dbms_output.put_line(V_SEC_WAY); 
dbms_output.put_line(V_CCY); 

      SELECT PALD.ONCE_LIMIT, PALD.DAY_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT
        FROM QDZH.PAY_AUTH_LIMT_DEF PALD
       WHERE PALD.SEC_WAY = V_SEC_WAY
         AND PALD.CCY = V_CCY;
    ELSE
dbms_output.put_line(V_SEC_WAY); 
dbms_output.put_line(V_CCY); 
      SELECT SL.ONCE_LIMIT, SL.DAY_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT
        FROM QDZH.SOTP_TRAN_AUTH_DEFL_LIMIT SL
       WHERE SL.CHAN_TYP = 'ALL'
         AND SL.SEC_WAY = V_SEC_WAY
         AND SL.CCY = V_CCY
         AND SL.TRAN_TYPE = '2'; --支付
    END IF;

  END IF;
  --如果客户已使用限额大于 单日累计额度，则剩余额度为0
  --否则剩余= 单日累计限额-客户单日已使用限额
  IF V_DAY_SUM_LIMIT_USE > V_DAY_LIMIT THEN
    V_DAY_SUM_LIMIT_RESIDUAL := 0;
  ELSE
    V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT - V_DAY_SUM_LIMIT_USE;
  END IF;
  DBMS_OUTPUT.PUT_LINE('客户认证方式：' || V_SEC_WAY || '，客户单笔限额：' ||
                       V_ONCE_LIMIT || '，客户剩余额度：' ||
                       V_DAY_SUM_LIMIT_RESIDUAL);
  --为返回数据设置值
  V_PAY_LIMIT_LIST_DATA.EXTEND;
  V_PAY_LIMIT_LIST_DATA(V_PAY_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT(V_SEC_WAY,
                                                                         V_SEC_WAY_NAME,
                                                                         V_ONCE_LIMIT,
                                                                         V_DAY_SUM_LIMIT_RESIDUAL,
                                                                         V_DAY_LIMIT);
  RETURN V_HAS_SEC_WAY;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
    DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
RETURN V_HAS_SEC_WAY;
END;
/

CREATE OR REPLACE FUNCTION F_PINYIN(P_NAME IN VARCHAR2) RETURN VARCHAR2 AS
V_COMPARE VARCHAR2(100);
V_RETURN VARCHAR2(4000);

FUNCTION F_NLSSORT(P_WORD IN VARCHAR2) RETURN VARCHAR2 AS
BEGIN
  RETURN NLSSORT(P_WORD, 'NLS_SORT=SCHINESE_PINYIN_M');
  END;
  BEGIN
FOR I IN 1..LENGTH(P_NAME) LOOP
V_COMPARE := F_NLSSORT(SUBSTR(P_NAME, I, 1));
IF V_COMPARE >= F_NLSSORT('吖 ') AND V_COMPARE <= F_NLSSORT('驁') THEN
V_RETURN := V_RETURN || 'A';
ELSIF V_COMPARE >= F_NLSSORT('八 ') AND V_COMPARE <= F_NLSSORT('簿') THEN
V_RETURN := V_RETURN || 'B';
ELSIF V_COMPARE >= F_NLSSORT('嚓 ') AND V_COMPARE <= F_NLSSORT('錯') THEN
V_RETURN := V_RETURN || 'C';
ELSIF V_COMPARE >= F_NLSSORT('咑 ') AND V_COMPARE <= F_NLSSORT('鵽') THEN
V_RETURN := V_RETURN || 'D';
ELSIF V_COMPARE >= F_NLSSORT('妸 ') AND V_COMPARE <= F_NLSSORT('樲') THEN
V_RETURN := V_RETURN || 'E';
ELSIF V_COMPARE >= F_NLSSORT('发 ') AND V_COMPARE <= F_NLSSORT('猤') THEN
V_RETURN := V_RETURN || 'F';
ELSIF V_COMPARE >= F_NLSSORT('旮 ') AND V_COMPARE <= F_NLSSORT('腂') THEN
V_RETURN := V_RETURN || 'G';
ELSIF V_COMPARE >= F_NLSSORT('妎 ') AND V_COMPARE <= F_NLSSORT('夻') THEN
V_RETURN := V_RETURN || 'H';
ELSIF V_COMPARE >= F_NLSSORT('丌 ') AND V_COMPARE <= F_NLSSORT('攈') THEN
V_RETURN := V_RETURN || 'J';
ELSIF V_COMPARE >= F_NLSSORT('咔 ') AND V_COMPARE <= F_NLSSORT('穒') THEN
V_RETURN := V_RETURN || 'K';
ELSIF V_COMPARE >= F_NLSSORT('垃 ') AND V_COMPARE <= F_NLSSORT('擽') THEN
V_RETURN := V_RETURN || 'L';
ELSIF V_COMPARE >= F_NLSSORT('嘸 ') AND V_COMPARE <= F_NLSSORT('椧') THEN
V_RETURN := V_RETURN || 'M';
ELSIF V_COMPARE >= F_NLSSORT('拏 ') AND V_COMPARE <= F_NLSSORT('瘧') THEN
V_RETURN := V_RETURN || 'N';
ELSIF V_COMPARE >= F_NLSSORT('筽 ') AND V_COMPARE <= F_NLSSORT('漚') THEN
V_RETURN := V_RETURN || 'O';
ELSIF V_COMPARE >= F_NLSSORT('妑 ') AND V_COMPARE <= F_NLSSORT('曝') THEN
V_RETURN := V_RETURN || 'P';
ELSIF V_COMPARE >= F_NLSSORT('七 ') AND V_COMPARE <= F_NLSSORT('裠') THEN
V_RETURN := V_RETURN || 'Q';
ELSIF V_COMPARE >= F_NLSSORT('亽 ') AND V_COMPARE <= F_NLSSORT('鶸') THEN
V_RETURN := V_RETURN || 'R';
ELSIF V_COMPARE >= F_NLSSORT('仨 ') AND V_COMPARE <= F_NLSSORT('蜶') THEN
V_RETURN := V_RETURN || 'S';
ELSIF V_COMPARE >= F_NLSSORT('侤 ') AND V_COMPARE <= F_NLSSORT('籜') THEN
V_RETURN := V_RETURN || 'T';
ELSIF V_COMPARE >= F_NLSSORT('屲 ') AND V_COMPARE <= F_NLSSORT('鶩') THEN
V_RETURN := V_RETURN || 'W';
ELSIF V_COMPARE >= F_NLSSORT('夕 ') AND V_COMPARE <= F_NLSSORT('鑂') THEN
V_RETURN := V_RETURN || 'X';
ELSIF V_COMPARE >= F_NLSSORT('丫 ') AND V_COMPARE <= F_NLSSORT('韻') THEN
V_RETURN := V_RETURN || 'Y';
ELSIF V_COMPARE >= F_NLSSORT('帀 ') AND V_COMPARE <= F_NLSSORT('咗') THEN
V_RETURN := V_RETURN || 'Z';
END IF;
END LOOP;
RETURN V_RETURN;
END;
/
CREATE OR REPLACE FUNCTION CHAN_LIMIT_QUERY(V_EBUSER_NO             IN VARCHAR2,
                                            V_CHAN_TYP              IN VARCHAR2,
                                            V_SEC_WAY               IN VARCHAR2,
                                            V_SEC_WAY_NAME          IN VARCHAR2,
                                            V_CCY                   IN VARCHAR2,
                                            V_TRANS_DATE            IN VARCHAR2,
                                            V_IS_HAS_SOTP           IN VARCHAR2,
                                            V_SMART_LIMIT_LIST_DATA IN OUT LIMIT_LIST_TYPE_NEW)
  RETURN VARCHAR2 AS
  V_HAS_SEC_WAY            VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
  V_IS_CUST_AUTH_LIMIT     VARCHAR2(1); --是否有客户自定义限额 0-没有 1-有
  V_DAY_SUM_LIMIT_USE      NUMBER(18, 4); --客户单日已使用限额
  V_ONCE_LIMIT             NUMBER(18, 4); --单笔限额
  V_DAY_LIMIT              NUMBER(18, 4); --单日累计限额
  V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18, 4); -- 客户单日剩余额度
  --add--by-feixiaobo--20161109--增加单日笔数及年累计限额
  V_DAY_TOTAL              INTEGER;        --日累计笔数
  V_DAY_TOTAL_RESIDUAL     INTEGER;        --单日剩余累计笔数
  V_DAY_TOTAL_USE          INTEGER;        --单日已转账次数
  V_YEAR_LIMIT              NUMBER(18, 4); --年累计限额
  V_YEAR_SUM_LIMIT_RESIDUAL NUMBER(18, 4); -- 客户单年剩余额度
  V_YEAR_SUM_LIMIT_USE      NUMBER(18, 4); -- 客户单年已使用额度
  V_YEAR                   VARCHAR2(4);--当前年
  --add--by-feixiaobo--20161109--增加单日笔数及年累计限额
  --add--by--rc--20180630--增加客户版本号判断
  V_OPEN_WAY               VARCHAR2(4);  -- 用户版本类型   01-大众版  02-专业版
  --add--by--rc--20180630--增加非@盾认证方式默认限额表
BEGIN
  V_HAS_SEC_WAY        := '1'; --默认有客户认证方式
  V_IS_CUST_AUTH_LIMIT := '1'; --默认有客户自设限额
  V_YEAR := SUBSTR(V_TRANS_DATE,0,4); --当前年
  V_OPEN_WAY := '02';                 --默认专业版
  --add by rc 判断用户是大众版还是专业版

  IF V_CHAN_TYP = '059' THEN
     SELECT G.OPEN_WAY
      INTO V_OPEN_WAY
      FROM QDZH.USER_CHANNEL G
     WHERE G.EBUSER_NO = V_EBUSER_NO
       AND G.CHAN_TYP = V_CHAN_TYP;
  END IF;

  DBMS_OUTPUT.PUT_LINE('V_OPEN_WAY===>' || V_OPEN_WAY);
  --add by rc 判断用户是大众版还是专业版

  --查询客户今天当前认证方式已使用累计限额
  BEGIN
    --查询单日已使用限额与单日已转账次数
    BEGIN
      SELECT CCALU.DAY_SUM_LIMIT, CCALU.DAY_TOTAL
        INTO V_DAY_SUM_LIMIT_USE, V_DAY_TOTAL_USE
        FROM QDZH.CUST_CHAN_AUTH_LIM_USE CCALU
       WHERE CCALU.EBUSER_NO = V_EBUSER_NO
         AND CCALU.CHAN_TYP = V_CHAN_TYP
         AND CCALU.SEC_WAY = V_SEC_WAY
         AND CCALU.CCY = V_CCY
         AND CCALU.LAST_TRAN_DATE = V_TRANS_DATE;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_DAY_SUM_LIMIT_USE := 0;
        V_DAY_TOTAL_USE     := 0;
    END;
    --查询单年已使用限额
    BEGIN
      SELECT CCALU.YEAR_SUM_LIMIT
        INTO V_YEAR_SUM_LIMIT_USE
        FROM QDZH.CUST_CHAN_AUTH_LIM_USE CCALU
       WHERE CCALU.EBUSER_NO = V_EBUSER_NO
         AND CCALU.CHAN_TYP = V_CHAN_TYP
         AND CCALU.SEC_WAY = V_SEC_WAY
         AND CCALU.CCY = V_CCY
         AND CCALU.LAST_YEAR = V_YEAR;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_YEAR_SUM_LIMIT_USE := 0;
    END;
  END;
  --查询客户当前认证方式的单笔限额、单日累计限额、单日转账笔数、年累计限额
  BEGIN
    SELECT CCATT.ONCE_LIMIT, CCATT.DAY_LIMIT,CCATT.DAY_TOTAL,CCATT.YEAR_LIMIT
      INTO V_ONCE_LIMIT, V_DAY_LIMIT,V_DAY_TOTAL,V_YEAR_LIMIT
      FROM QDZH.CUST_CH_AU_SELF_LIM CCATT
     WHERE CCATT.EBUSER_NO = V_EBUSER_NO
       AND CCATT.CHAN_TYP = V_CHAN_TYP
       AND CCATT.SEC_WAY = V_SEC_WAY
       AND CCATT.CCY = V_CCY;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_IS_CUST_AUTH_LIMIT := '0';
  END;
  DBMS_OUTPUT.PUT_LINE('V_IS_CUST_AUTH_LIMIT===>' || V_IS_CUST_AUTH_LIMIT);
  --如果客户没有自设限额，则查询渠道系统默认限额
  IF V_IS_CUST_AUTH_LIMIT = '0' THEN
  DBMS_OUTPUT.PUT_LINE('V_SEC_WAY===>' || V_SEC_WAY);
  DBMS_OUTPUT.PUT_LINE('V_CHAN_TYP===>' || V_CHAN_TYP);
  DBMS_OUTPUT.PUT_LINE('V_CCY===>' || V_CCY);
  DBMS_OUTPUT.PUT_LINE('V_OPEN_WAY===>' || V_OPEN_WAY);
    --0-取默认表 1-取sotp默认表  060-网银取默认表
    IF V_IS_HAS_SOTP = '0' OR V_CHAN_TYP = '060' THEN
     DBMS_OUTPUT.PUT_LINE('CHAN_AUTH_LIM_DEF');
      SELECT CALD.ONCE_LIMIT, CALD.DAY_LIMIT,CALD.DAY_TOTAL,CALD.YEAR_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT,V_DAY_TOTAL,V_YEAR_LIMIT
        FROM QDZH.CHAN_AUTH_LIM_DEF CALD
       WHERE CALD.CHAN_TYP = V_CHAN_TYP
         AND CALD.SEC_WAY = V_SEC_WAY
         AND CALD.CCY = V_CCY
         -- alter by rc 增加版本号查询
         AND CALD.OPEN_WAY = V_OPEN_WAY;
         -- alter by rc 增加版本号查询
      DBMS_OUTPUT.PUT_LINE('V_ONCE_LIMIT' || V_ONCE_LIMIT || 'V_DAY_LIMIT：' || V_DAY_LIMIT ||
                       '，V_DAY_TOTAL：' || V_DAY_TOTAL || '，客户剩余额度：' ||
                       V_DAY_SUM_LIMIT_RESIDUAL ||'，V_YEAR_LIMIT'|| V_YEAR_LIMIT);
    ELSE
    DBMS_OUTPUT.PUT_LINE('SOTP_TRAN_AUTH_DEFL_LIMIT');
      SELECT SL.ONCE_LIMIT, SL.DAY_LIMIT,SL.DAY_TOTAL,SL.YEAR_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT,V_DAY_TOTAL,V_YEAR_LIMIT
        FROM QDZH.SOTP_TRAN_AUTH_DEFL_LIMIT SL
       WHERE SL.CHAN_TYP = V_CHAN_TYP
         AND SL.SEC_WAY = V_SEC_WAY
         AND SL.CCY = V_CCY
         AND SL.TRAN_TYPE = '1'; --转账
    END IF;
  END IF;
  --如果客户已使用限额大于 单日累计额度，则剩余额度为0
  --否则剩余= 单日累计限额-客户单日已使用限额
  IF V_DAY_SUM_LIMIT_USE >= V_DAY_LIMIT THEN
    V_DAY_SUM_LIMIT_RESIDUAL := 0;
  ELSE
    V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT - V_DAY_SUM_LIMIT_USE;
  END IF;
  --20161109-add-feixiaobo-增加单日剩余笔数，单年剩余限额
  IF V_YEAR_SUM_LIMIT_USE >= V_YEAR_LIMIT THEN
    V_YEAR_SUM_LIMIT_RESIDUAL := 0;
  ELSE
    V_YEAR_SUM_LIMIT_RESIDUAL := V_YEAR_LIMIT - V_YEAR_SUM_LIMIT_USE;
  END IF;

  IF V_DAY_TOTAL_USE >= V_DAY_TOTAL THEN
    V_DAY_TOTAL_RESIDUAL := 0;
  ELSE
    V_DAY_TOTAL_RESIDUAL := V_DAY_TOTAL - V_DAY_TOTAL_USE;
  END IF;
  --20161109-add-feixiaobo-增加单日剩余笔数，单年剩余限额
  DBMS_OUTPUT.PUT_LINE('客户号' || V_EBUSER_NO || '客户认证方式：' || V_SEC_WAY ||
                       '，客户单笔限额：' || V_ONCE_LIMIT || '，客户剩余额度：' ||
                       V_DAY_SUM_LIMIT_RESIDUAL ||'，单日笔数'|| V_DAY_TOTAL||'，单日剩余笔数'||V_DAY_TOTAL_RESIDUAL||'，单年累计限额'||V_YEAR_LIMIT
                       ||'，单年剩余累计限额'||V_YEAR_SUM_LIMIT_RESIDUAL);
  --为返回数据设置值
  V_SMART_LIMIT_LIST_DATA.EXTEND;
  V_SMART_LIMIT_LIST_DATA(V_SMART_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT_NEW(V_SEC_WAY,
                                                                             V_SEC_WAY_NAME,
                                                                             V_ONCE_LIMIT,
                                                                             V_DAY_SUM_LIMIT_RESIDUAL,
                                                                             V_DAY_LIMIT,
                                                                             V_DAY_TOTAL,
                                                                             V_DAY_TOTAL_RESIDUAL,
                                                                             V_YEAR_LIMIT,
                                                                             V_YEAR_SUM_LIMIT_RESIDUAL);

  RETURN V_HAS_SEC_WAY;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
    DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
END;
/

CREATE OR REPLACE FUNCTION CHAN_PAY_QUERY(V_EBUSER_NO           IN VARCHAR2,
                                          V_CHAN_TYP            IN VARCHAR2,
                                          V_SEC_WAY             IN VARCHAR2,
                                          V_SEC_WAY_NAME        IN VARCHAR2,
                                          V_CCY                 IN VARCHAR2,
                                          V_TRANS_DATE          IN VARCHAR2,
                                          V_IS_HAS_SOTP         IN VARCHAR2,
                                          V_PAY_LIMIT_LIST_DATA IN OUT LIMIT_LIST_TYPE)
  RETURN VARCHAR2 AS
  V_HAS_SEC_WAY            VARCHAR2(1); -- 1：代表有认证方式 0：代表无认证方式
  V_IS_CUST_AUTH_LIMIT     VARCHAR2(1); --是否有客户自定义限额 0-没有 1-有
  V_DAY_SUM_LIMIT_USE      NUMBER(18, 4); --客户单日已使用限额
  V_ONCE_LIMIT             NUMBER(18, 4); --单笔限额
  V_DAY_LIMIT              NUMBER(18, 4); --单日累计限额
  V_DAY_SUM_LIMIT_RESIDUAL NUMBER(18, 4); -- 客户单日剩余额度
BEGIN
  V_HAS_SEC_WAY := '1'; --默认有客户认证方式
  --查询客户今天当前认证方式已使用累计限额
  BEGIN
    SELECT CPALU.DAY_SUM_LIMIT
      INTO V_DAY_SUM_LIMIT_USE
      FROM QDZH.CUST_PAY_AUTH_LIM_USE CPALU
     WHERE CPALU.EBUSER_NO = V_EBUSER_NO
       AND CPALU.SEC_WAY = V_SEC_WAY
       AND CPALU.CCY = V_CCY
       AND CPALU.LAST_TRAN_DATE = V_TRANS_DATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_DAY_SUM_LIMIT_USE := 0;
  END;
  --查询客户当前认证方式的单笔限额和单日累计限额
  BEGIN
    SELECT CPASL.ONCE_LIMIT, CPASL.DAY_LIMIT
      INTO V_ONCE_LIMIT, V_DAY_LIMIT
      FROM QDZH.CUST_PAY_AUTH_SET_LIMT CPASL
     WHERE CPASL.EBUSER_NO = V_EBUSER_NO
       AND CPASL.SEC_WAY = V_SEC_WAY
       AND CPASL.CCY = V_CCY;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_IS_CUST_AUTH_LIMIT := '0';
  END;
  --如果客户没有自设限额，则查询渠道系统默认限额
  IF V_IS_CUST_AUTH_LIMIT = '0' THEN

    --0-取默认表 1-取sotp默认表  060-网银取默认表
    IF V_IS_HAS_SOTP = '0' OR V_CHAN_TYP = '060' THEN

      SELECT PALD.ONCE_LIMIT, PALD.DAY_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT
        FROM QDZH.PAY_AUTH_LIMT_DEF PALD
       WHERE PALD.SEC_WAY = V_SEC_WAY
         AND PALD.CCY = V_CCY;
    ELSE
      SELECT SL.ONCE_LIMIT, SL.DAY_LIMIT
        INTO V_ONCE_LIMIT, V_DAY_LIMIT
        FROM QDZH.SOTP_TRAN_AUTH_DEFL_LIMIT SL
       WHERE SL.CHAN_TYP = 'ALL'
         AND SL.SEC_WAY = V_SEC_WAY
         AND SL.CCY = V_CCY
         AND SL.TRAN_TYPE = '2'; --支付
    END IF;

  END IF;
  --如果客户已使用限额大于 单日累计额度，则剩余额度为0
  --否则剩余= 单日累计限额-客户单日已使用限额
  IF V_DAY_SUM_LIMIT_USE > V_DAY_LIMIT THEN
    V_DAY_SUM_LIMIT_RESIDUAL := 0;
  ELSE
    V_DAY_SUM_LIMIT_RESIDUAL := V_DAY_LIMIT - V_DAY_SUM_LIMIT_USE;
  END IF;
  DBMS_OUTPUT.PUT_LINE('客户认证方式：' || V_SEC_WAY || '，客户单笔限额：' ||
                       V_ONCE_LIMIT || '，客户剩余额度：' ||
                       V_DAY_SUM_LIMIT_RESIDUAL);
  --为返回数据设置值
  V_PAY_LIMIT_LIST_DATA.EXTEND;
  V_PAY_LIMIT_LIST_DATA(V_PAY_LIMIT_LIST_DATA.LAST) := LIMIT_DATA_OBJECT(V_SEC_WAY,
                                                                         V_SEC_WAY_NAME,
                                                                         V_ONCE_LIMIT,
                                                                         V_DAY_SUM_LIMIT_RESIDUAL,
                                                                         V_DAY_LIMIT);
  RETURN V_HAS_SEC_WAY;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('sqlcode : ' || SQLCODE);
    DBMS_OUTPUT.PUT_LINE('sqlerrm : ' || SQLERRM);
END;
/

CREATE OR REPLACE FUNCTION F_PINYIN(P_NAME IN VARCHAR2) RETURN VARCHAR2 AS
V_COMPARE VARCHAR2(100);
V_RETURN VARCHAR2(4000);

FUNCTION F_NLSSORT(P_WORD IN VARCHAR2) RETURN VARCHAR2 AS
BEGIN
  RETURN NLSSORT(P_WORD, 'NLS_SORT=SCHINESE_PINYIN_M');
  END;
  BEGIN
FOR I IN 1..LENGTH(P_NAME) LOOP
V_COMPARE := F_NLSSORT(SUBSTR(P_NAME, I, 1));
IF V_COMPARE >= F_NLSSORT('吖 ') AND V_COMPARE <= F_NLSSORT('驁') THEN
V_RETURN := V_RETURN || 'A';
ELSIF V_COMPARE >= F_NLSSORT('八 ') AND V_COMPARE <= F_NLSSORT('簿') THEN
V_RETURN := V_RETURN || 'B';
ELSIF V_COMPARE >= F_NLSSORT('嚓 ') AND V_COMPARE <= F_NLSSORT('錯') THEN
V_RETURN := V_RETURN || 'C';
ELSIF V_COMPARE >= F_NLSSORT('咑 ') AND V_COMPARE <= F_NLSSORT('鵽') THEN
V_RETURN := V_RETURN || 'D';
ELSIF V_COMPARE >= F_NLSSORT('妸 ') AND V_COMPARE <= F_NLSSORT('樲') THEN
V_RETURN := V_RETURN || 'E';
ELSIF V_COMPARE >= F_NLSSORT('发 ') AND V_COMPARE <= F_NLSSORT('猤') THEN
V_RETURN := V_RETURN || 'F';
ELSIF V_COMPARE >= F_NLSSORT('旮 ') AND V_COMPARE <= F_NLSSORT('腂') THEN
V_RETURN := V_RETURN || 'G';
ELSIF V_COMPARE >= F_NLSSORT('妎 ') AND V_COMPARE <= F_NLSSORT('夻') THEN
V_RETURN := V_RETURN || 'H';
ELSIF V_COMPARE >= F_NLSSORT('丌 ') AND V_COMPARE <= F_NLSSORT('攈') THEN
V_RETURN := V_RETURN || 'J';
ELSIF V_COMPARE >= F_NLSSORT('咔 ') AND V_COMPARE <= F_NLSSORT('穒') THEN
V_RETURN := V_RETURN || 'K';
ELSIF V_COMPARE >= F_NLSSORT('垃 ') AND V_COMPARE <= F_NLSSORT('擽') THEN
V_RETURN := V_RETURN || 'L';
ELSIF V_COMPARE >= F_NLSSORT('嘸 ') AND V_COMPARE <= F_NLSSORT('椧') THEN
V_RETURN := V_RETURN || 'M';
ELSIF V_COMPARE >= F_NLSSORT('拏 ') AND V_COMPARE <= F_NLSSORT('瘧') THEN
V_RETURN := V_RETURN || 'N';
ELSIF V_COMPARE >= F_NLSSORT('筽 ') AND V_COMPARE <= F_NLSSORT('漚') THEN
V_RETURN := V_RETURN || 'O';
ELSIF V_COMPARE >= F_NLSSORT('妑 ') AND V_COMPARE <= F_NLSSORT('曝') THEN
V_RETURN := V_RETURN || 'P';
ELSIF V_COMPARE >= F_NLSSORT('七 ') AND V_COMPARE <= F_NLSSORT('裠') THEN
V_RETURN := V_RETURN || 'Q';
ELSIF V_COMPARE >= F_NLSSORT('亽 ') AND V_COMPARE <= F_NLSSORT('鶸') THEN
V_RETURN := V_RETURN || 'R';
ELSIF V_COMPARE >= F_NLSSORT('仨 ') AND V_COMPARE <= F_NLSSORT('蜶') THEN
V_RETURN := V_RETURN || 'S';
ELSIF V_COMPARE >= F_NLSSORT('侤 ') AND V_COMPARE <= F_NLSSORT('籜') THEN
V_RETURN := V_RETURN || 'T';
ELSIF V_COMPARE >= F_NLSSORT('屲 ') AND V_COMPARE <= F_NLSSORT('鶩') THEN
V_RETURN := V_RETURN || 'W';
ELSIF V_COMPARE >= F_NLSSORT('夕 ') AND V_COMPARE <= F_NLSSORT('鑂') THEN
V_RETURN := V_RETURN || 'X';
ELSIF V_COMPARE >= F_NLSSORT('丫 ') AND V_COMPARE <= F_NLSSORT('韻') THEN
V_RETURN := V_RETURN || 'Y';
ELSIF V_COMPARE >= F_NLSSORT('帀 ') AND V_COMPARE <= F_NLSSORT('咗') THEN
V_RETURN := V_RETURN || 'Z';
END IF;
END LOOP;
RETURN V_RETURN;
END;
/
