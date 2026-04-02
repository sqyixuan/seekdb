DELIMITER //;
create or replace package aaa_coverage_tool is

 procedure start_coverage(pck_name varchar2, id out integer);
 procedure add_line(cvr_id    integer,
                     stm_no    integer,
                     start_    integer,
                     end_      integer,
                     txt_start varchar2,
                     txt_end   varchar2);

  procedure end_coverage(id integer);

  procedure abort_coverage(id integer);
  
  procedure end_coverage(pck_name varchar2);

  procedure clean_up;

  type bool_tab is table of boolean;
  
  function get_statement_tab(id integer) return bool_tab;
  
end ;
//

create or replace package body aaa_coverage_tool is

  procedure fetch_source(object_name varchar2,object_type varchar2,source out clob) is
    tmpsrc clob;
  begin
   --DBMS_LOB.CREATETEMPORARY (tmpsrc,true,DBMS_LOB.CALL);
    for r in (select *
                from user_source s
               where s.name = object_name
                 and s.TYPE = object_type
               order by s.line) loop
       DBMS_LOB.WRITEAPPEND(tmpsrc ,length(r.text), r.text);
    end loop;
    source := tmpsrc;
  end;

  procedure start_coverage(pck_name varchar2, id out integer) is
    body_source   clob;
    spec_source   clob;
    source2  clob;
    t         varchar2(200);
    iscovered integer;
    id_temp integer;
  begin
    select object_type
      into t
      from user_objects u
     where u.object_type = 'PACKAGE BODY'
       and u.object_name = pck_name;
    select count(*)
      into iscovered
      from aaa_coverage c
     where c.package_name = start_coverage.pck_name
       and c.is_covered = 1;
    if iscovered > 0 then
      raise_application_error(-20001, 'package ' || pck_name || ' is still covered');
    end if;

    delete from aaa_coverage_statements s
     where s.cvr_id in
           (select id from aaa_coverage c where c.package_name = pck_name);
    delete from aaa_coverage c where c.package_name = pck_name ;
    select aaa_coverage_seq.nextval into id_temp from dual;
    start_coverage.id := id_temp;
    insert into aaa_coverage
      (id, package_name, start_date, ORIGINAL_body_SOURCE,ORIGINAL_spec_SOURCE,is_covered)
    values
      (id_temp,pck_name, sysdate, EMPTY_cLOB(),empty_clob(),1)
    returning ORIGINAL_body_SOURCE,original_spec_source into body_source,spec_source;
   
    fetch_source(pck_name,'PACKAGE BODY',source2);
    dbms_lob.append(body_source,source2);
    fetch_source(pck_name,'PACKAGE',source2);
    dbms_lob.append(spec_source,source2);
  end;

  procedure end_coverage(pck_name varchar2) is
    size_  integer;
    cov_id integer;
  begin
    select count(*), max(c.id)
      into size_, cov_id
      from aaa_coverage c
     where c.package_name = end_coverage.pck_name
       and c.end_date is null;
    if size_ = 0 then
      raise_application_error(-20001, 'no coverage open for package: '|| pck_name);
    elsif size_ > 1 then
      raise_application_error(-20001, 'more than one coverage open for ' || pck_name);
    else
      end_coverage(cov_id);
    end if;
  end;

  procedure end_coverage(id integer) is
    r          aaa_coverage%rowtype;
    sql_cursor integer;
  begin
    select * into r from aaa_coverage where id = end_coverage.id;
    update aaa_coverage c
       set c.end_date = sysdate,
           c.is_covered = 0
     where c.id = end_coverage.id;
  end;

  procedure abort_coverage(id integer) is
  begin
    update aaa_coverage c
       set c.end_date = null,
           c.start_date = null,
           c.is_covered = 0
     where c.id = abort_coverage.id;
  end;

  procedure add_line(cvr_id    integer,
                     stm_no    integer,
                     start_    integer,
                     end_      integer,
                     txt_start varchar2,
                     txt_end   varchar2) is
  begin
    insert into aaa_coverage_statements
      (cvr_id, stm_no, line_no, start_, END_, txt_start, txt_end, hit)
    values
      (cvr_id,
       add_line.stm_no,
       -1,
       add_line.start_,
       add_line.end_,
       add_line.txt_start,
       add_line.txt_end,
       0);
  end;
 
  procedure clean_up is
  begin
    delete from aaa_coverage_statements;
    delete from aaa_coverage;
  end;

  function get_statement_tab(id integer) return bool_tab is
    res bool_tab := bool_tab();
    max_stm_no integer;
  begin
    select max(stm_no) into max_stm_no from aaa_coverage_statements s where s.cvr_id = get_statement_tab.id;
    if nvl(max_stm_no,0)=0 then
      return res;
    end if;
    res.extend(max_stm_no);
    for r in (select stm_no from aaa_coverage_statements s where s.cvr_id = get_statement_tab.id and hit=1) loop
      res(r.stm_no):=true;
    end loop;
    return res;
  end;
end;
//



CREATE OR REPLACE PACKAGE PACK_DEAL_BWZC_1005 IS
  PROCEDURE PROC_DEAL_BWZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_BWZC_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);

END PACK_DEAL_BWZC_1005;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_BWZC_1005 IS
    "$hits" aaa_coverage_tool.bool_tab; 
    "$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction; --req utl_http.req; resp utl_http.resp;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(40);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(40);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(40,x); "$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(40,x); "$times"(x):=true; 
	  end if;
	end;
	
	PROCEDURE PROC_DEAL_BWZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_XS            PRM_XS051.XS_VALUE%TYPE;
    V_DAY           NUMBER;
    V_PRODUCTID6180 PRM_CS05C.PRODUCT_ID%TYPE;
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_BWZC';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --读取最大系数
    "$log"(8);V_STEP_NO := '2.1';
    "$log"(9);SELECT MAX(XS_VALUE)
      INTO V_XS
      FROM PRM_XS051 PRM
     WHERE P_I_DATE >= PRM.START_DATE
       AND P_I_DATE < PRM.END_DATE;
    --读取该月的天数
    "$log"(10);SELECT EXTRACT(DAY FROM LAST_DAY(TO_DATE(P_I_DATE, 'yyyymmdd')))
      INTO V_DAY
      FROM DUAL;
    --读取6180科目最大的产品号
    "$log"(11);SELECT MAX(P.PRODUCT_ID)
      INTO V_PRODUCTID6180
      FROM PRM_CS05C P
     WHERE P.SUBJECT_CODE LIKE '6180%';
    --回退操作，删除当天数据
    "$log"(12);
	DELETE FROM DW_MOD_BWZC_DB DW WHERE DW.DATA_DATE = P_I_DATE;
    "$log"(13);
	V_STEP_NO := '2.2';
    "$log"(14);
	DELETE FROM DW_GCMS_ZTM280362 WHERE DATA_DATE = P_I_DATE;
    "$log"(15);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_TMP_GCMS_ZTM280362';
    "$log"(16);INSERT INTO DW_GCMS_ZTM280362
      (DATA_DATE,
       BUSINESS_ID,
       ORGANIZATION_ID,
       ZONENO,
       BRANCHNO,
       BANKFLAG,
       CUST_ID,
       CUST_NAME,
       CONTRACT_ID,
       LOAN_KIND,
       BUSINESS_KIND,
       APPROVE_AMT,
       PROMISS_AMT,
       CURRENCY,
       INDUSTRY,
       SUBJECT,
       PROMISS_FLAG)
      SELECT P_I_DATE,
             T1.TM280362003 BUSINESS_ID,
             T1.TM280362017 ORGANIZATION_ID,
             SUBSTR(T1.TM280362085, 1, 4) ZONENO,
             SUBSTR(T1.TM280362085, 5, 4) BRANCHNO,
             '0' BANKFLAG, --初始数据
             T1.TM280362001 CUST_ID,
             T1.TM280362043 CUST_NAME,
             T1.TM280362002 CONTRACT_ID,
             NULL LOAN_KIND,
             T1.TM280362020 BUSINESS_KIND,
             T1.TM280362004 APPROVE_AMT,
             T1.TM280362006 PROMISS_AMT,
             T1.TM280362005 CURRENCY,
             T1.TM280362026 INDUSTRY,
             null SUBJECT,
             DECODE(T1.TM280362080, '1', 1, '2', 2, '3', 3, 4) PROMISS_FLAG
        FROM ODS_CM_ZTM280362 T1;
  
    --外币折美元
    "$log"(17);
	INSERT INTO DW_GCMS_ZTM280362
      (DATA_DATE,
       BUSINESS_ID,
       ORGANIZATION_ID,
       ZONENO,
       BRANCHNO,
       BANKFLAG,
       CUST_ID,
       CUST_NAME,
       CONTRACT_ID,
       LOAN_KIND,
       BUSINESS_KIND,
       APPROVE_AMT,
       PROMISS_AMT,
       CURRENCY,
       INDUSTRY,
       SUBJECT,
       PROMISS_FLAG)
      select C.DATA_DATE,
             C.BUSINESS_ID,
             C.ORGANIZATION_ID,
             C.ZONENO,
             C.BRANCHNO,
             C.BANKFLAG,
             C.CUST_ID,
             C.CUST_NAME,
             C.CONTRACT_ID,
             C.LOAN_KIND,
             C.BUSINESS_KIND,
             C.APPROVE_AMT * R.USCVRATE,
             C.PROMISS_AMT * R.USCVRATE,
             '992' CURRENCY,
             C.INDUSTRY,
             C.SUBJECT,
             C.PROMISS_FLAG
        from DW_GCMS_ZTM280362 c, PRM_CURRENCY_RATE R
       WHERE C.DATA_DATE = P_I_DATE
         AND C.CURRENCY = R.CURRENCY
         AND C.CURRENCY NOT IN ('001', '992', '994');
  
    --本外币合计折人民币
    "$log"(18);INSERT INTO DW_GCMS_ZTM280362
      (DATA_DATE,
       BUSINESS_ID,
       ORGANIZATION_ID,
       ZONENO,
       BRANCHNO,
       BANKFLAG,
       CUST_ID,
       CUST_NAME,
       CONTRACT_ID,
       LOAN_KIND,
       BUSINESS_KIND,
       APPROVE_AMT,
       PROMISS_AMT,
       CURRENCY,
       INDUSTRY,
       SUBJECT,
       PROMISS_FLAG)
      select C.DATA_DATE,
             C.BUSINESS_ID,
             C.ORGANIZATION_ID,
             C.ZONENO,
             C.BRANCHNO,
             C.BANKFLAG,
             C.CUST_ID,
             C.CUST_NAME,
             C.CONTRACT_ID,
             C.LOAN_KIND,
             C.BUSINESS_KIND,
             C.APPROVE_AMT * R.USCVRATE *
             DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE),
             C.PROMISS_AMT * R.USCVRATE *
             DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE),
             '994' CURRENCY,
             C.INDUSTRY,
             C.SUBJECT,
             C.PROMISS_FLAG
        from DW_GCMS_ZTM280362 c, PRM_CURRENCY_RATE R, PRM_CURRENCY_RATE I
       WHERE C.DATA_DATE = P_I_DATE
         AND C.CURRENCY = R.CURRENCY
         AND C.CURRENCY NOT IN ('992', '994')
         AND I.CURRENCY = '001';
    "$log"(19);COMMIT;

    "$log"(20);INSERT INTO STG_TMP_GCMS_ZTM280362 --计算各项值
      (ZONENO, BRNO, DATA_DATE, CURRTYPE, PRODUCT_ID, BALANCE, BANKFLAG)
      select c.zoneno,
             c.branchno,
             c.data_date,
             c.currency,
             decode(c.promiss_flag, 1, '1', '0'), --c.promiss_flag 1-可随时无条件撤销,2-可有条件撤销,3-不可撤销,4-其他
             sum(c.promiss_amt),
             c.bankflag
        from DW_GCMS_ZTM280362 c
       where c.data_date = p_i_date
       group by c.zoneno,
                c.branchno,
                c.data_date,
                c.currency,
                decode(c.promiss_flag, 1, '1', '0'), --c.promiss_flag 1-可随时无条件撤销,2-可有条件撤销,3-不可撤销,4-其他
                c.bankflag;
    "$log"(21);commit;
  
    --汇总其他层级
    "$log"(22);FOR V_BANKFLAG IN 2 .. 5 LOOP
      "$log"(23);MERGE INTO STG_TMP_GCMS_ZTM280362 D --计算各项值
      USING (SELECT /*+use_hash(g,prm)*/
              PRM.BELONG_ZONENO,
              PRM.BELONG_BRNO,
              PRM.BELONG_BANK_FLAG,
              G.CURRTYPE,
              G.PRODUCT_ID,
              SUM(BALANCE) BALANCE,
              G.DATA_DATE
               FROM STG_TMP_GCMS_ZTM280362 G, PRM_IRA_ORGAN PRM
              WHERE PRM.ZONE_NO = G.ZONENO
                AND PRM.BR_NO = G.BRNO
                AND PRM.BANK_FLAG = G.BANKFLAG
                AND PRM.START_DATE <= P_I_DATE
                AND PRM.END_DATE > P_I_DATE
                AND PRM.BELONG_BANK_FLAG = V_BANKFLAG
                AND PRM.BANK_FLAG <> V_BANKFLAG
              GROUP BY PRM.BELONG_ZONENO, PRM.BELONG_BRNO, PRM.BELONG_BANK_FLAG, G.CURRTYPE, G.PRODUCT_ID, G.DATA_DATE) TMP
      ON (D.ZONENO = TMP.BELONG_ZONENO AND D.BRNO = TMP.BELONG_BRNO AND D.BANKFLAG = TMP.BELONG_BANK_FLAG AND D.CURRTYPE = TMP.CURRTYPE AND D.PRODUCT_ID = TMP.PRODUCT_ID AND D.DATA_DATE = TMP.DATA_DATE)
      WHEN MATCHED THEN
        UPDATE SET D.BALANCE = TMP.BALANCE
      WHEN NOT MATCHED THEN
        INSERT (D.ZONENO, D.BRNO, D.DATA_DATE, D.CURRTYPE, D.PRODUCT_ID, D.BALANCE, D.BANKFLAG)
        VALUES (TMP.BELONG_ZONENO, TMP.BELONG_BRNO, TMP.DATA_DATE, TMP.CURRTYPE, TMP.PRODUCT_ID, TMP.BALANCE, TMP.BELONG_BANK_FLAG);
      "$log"(24);
	  COMMIT;
    END LOOP;
    "$log"(25);
	INSERT INTO DW_MOD_BWZC_DB
      (ZONENO,
       BRNO,
       DATA_DATE,
       CURRTYPE,
       PRODUCT_ID,
       SJCPJE,
       FPXS,
       JJZBZYE,
       SUMSJCPJE,
       SUMJJZBZYE,
       BANKFLAG)
      (SELECT A.ZONENO,
              A.BRNO,
              P_I_DATE,
              A.CURRTYPE,
              B.PRODUCT_ID,
              DECODE(SIGN(SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                     '8',
                                     A.TDCRBAL,
                                     '2',
                                     A.TDCRBAL,
                                     '4',
                                     DECODE(SIGN(A.TDDRBAL - A.TDCRBAL),
                                            -1,
                                            0,
                                            A.TDDRBAL - A.TDCRBAL),
                                     A.TDDRBAL) * B.SUBJECT_APP)),
                     -1,
                     0,
                     SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                '8',
                                A.TDCRBAL,
                                '2',
                                A.TDCRBAL,
                                '4',
                                DECODE(SIGN(A.TDDRBAL - A.TDCRBAL),
                                       -1,
                                       0,
                                       A.TDDRBAL - A.TDCRBAL),
                                A.TDDRBAL) * B.SUBJECT_APP)),
              NVL(T11.XS_VALUE, V_XS),
              DECODE(SIGN(SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                     '8',
                                     A.TDCRBAL,
                                     '2',
                                     A.TDCRBAL,
                                     '4',
                                     DECODE(SIGN(A.TDDRBAL - A.TDCRBAL),
                                            -1,
                                            0,
                                            A.TDDRBAL - A.TDCRBAL),
                                     A.TDDRBAL) * B.SUBJECT_APP *
                              NVL(T11.XS_VALUE, V_XS))),
                     -1,
                     0,
                     SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                '8',
                                A.TDCRBAL,
                                '2',
                                A.TDCRBAL,
                                '4',
                                DECODE(SIGN(A.TDDRBAL - A.TDCRBAL),
                                       -1,
                                       0,
                                       A.TDDRBAL - A.TDCRBAL),
                                A.TDDRBAL) * B.SUBJECT_APP *
                         NVL(T11.XS_VALUE, V_XS))),
              DECODE(SIGN(SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                     '8',
                                     A.SUMCRBAL,
                                     '2',
                                     A.SUMCRBAL,
                                     '4',
                                     DECODE(SIGN(A.SUMDRBAL - A.SUMCRBAL),
                                            -1,
                                            0,
                                            A.SUMDRBAL - A.SUMCRBAL),
                                     A.SUMDRBAL) * B.SUBJECT_APP / V_DAY)),
                     -1,
                     0,
                     SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                '8',
                                A.SUMCRBAL,
                                '2',
                                A.SUMCRBAL,
                                '4',
                                DECODE(SIGN(A.SUMDRBAL - A.SUMCRBAL),
                                       -1,
                                       0,
                                       A.SUMDRBAL - A.SUMCRBAL),
                                A.SUMDRBAL) * B.SUBJECT_APP / V_DAY)),
              DECODE(SIGN(SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                     '8',
                                     A.SUMCRBAL,
                                     '2',
                                     A.SUMCRBAL,
                                     '4',
                                     DECODE(SIGN(A.SUMDRBAL - A.SUMCRBAL),
                                            -1,
                                            0,
                                            A.SUMDRBAL - A.SUMCRBAL),
                                     A.SUMDRBAL) * B.SUBJECT_APP / V_DAY *
                              NVL(T11.XS_VALUE, V_XS))),
                     -1,
                     0,
                     SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                                '8',
                                A.SUMCRBAL,
                                '2',
                                A.SUMCRBAL,
                                '4',
                                DECODE(SIGN(A.SUMDRBAL - A.SUMCRBAL),
                                       -1,
                                       0,
                                       A.SUMDRBAL - A.SUMCRBAL),
                                A.SUMDRBAL) * B.SUBJECT_APP / V_DAY *
                         NVL(T11.XS_VALUE, V_XS))),
              A.BANKFLAG
         FROM (SELECT ZONENO,BRNO,BANKFLAG,CURRTYPE,SUBCODE,TDDRBAL,TDCRBAL,SUMDRBAL,SUMCRBAL,DATA_DATE
               FROM STG_DAT_NFGNLED WHERE SUBCODE <> '618199' --2002版本修改,更换此科目数据源从HDWCCRGN取数
               UNION ALL --6181 仅配置一个一级科目, 618199直接使用即可.
               SELECT  ZONENO,BRNO,BANKFLAG,CURRTYPE,SUBCODE,TDDRBAL,TDCRBAL,SUMDRBAL,SUMCRBAL,DATA_DATE
               FROM STG_DAT_HDWCCRGN WHERE SUBCODE = '618199') A, PRM_CS05C B, PRM_XS051 T11
        WHERE P_I_DATE >= B.START_DATE
          AND P_I_DATE < B.END_DATE
          AND DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) =
              B.SUBJECT_CODE
          AND B.PRODUCT_ID = T11.PRODUCT_NAME3
          AND P_I_DATE >= T11.START_DATE(+)
          AND P_I_DATE < T11.END_DATE(+)
        GROUP BY A.ZONENO,
                 A.BRNO,
                 A.CURRTYPE,
                 B.PRODUCT_ID,
                 NVL(T11.XS_VALUE, V_XS),
                 A.BANKFLAG);
    "$log"(26);
	COMMIT;

      "$log"(27);MERGE INTO DW_MOD_BWZC_DB BW
      USING (SELECT ZONENO,
                    BRNO,
                    DATA_DATE,
                    CURRTYPE,
                    PRODUCT_ID,
                    BALANCE,
                    BANKFLAG
               FROM STG_TMP_GCMS_ZTM280362 --计算各项值
              WHERE PRODUCT_ID = '1') STG
      ON (BW.DATA_DATE = STG.DATA_DATE AND BW.ZONENO = STG.ZONENO AND BW.BRNO = STG.BRNO AND BW.CURRTYPE = STG.CURRTYPE AND BW.BANKFLAG = STG.BANKFLAG AND BW.PRODUCT_ID = V_PRODUCTID6180 AND BW.DATA_DATE = STG.DATA_DATE)
      WHEN MATCHED THEN
        UPDATE
           SET BW.JJZBZYE = CASE
                              WHEN NVL(BW.SJCPJE, 0) - NVL(STG.BALANCE, 0) <= 0 THEN
                               0
                              ELSE
                               (NVL(BW.SJCPJE, 0) - NVL(STG.BALANCE, 0)) * BW.FPXS
                            END;
    "$log"(28);COMMIT;
    --历史数据清理 删除 上月之前的非月末明细数据
    "$log"(29);DELETE FROM DW_GCMS_ZTM280362 D
     WHERE D.DATA_DATE < TO_CHAR(ADD_MONTHS(TRUNC(TO_DATE(P_I_DATE, 'YYYYMMDD'), 'MM'), -1), 'YYYYMMDD')
       AND D.DATA_DATE <> TO_CHAR(LAST_DAY(TO_DATE(D.DATA_DATE, 'YYYYMMDD')), 'YYYYMMDD');
    "$log"(30);COMMIT;
  
    "$log"(31);P_O_SUCCEED := '0';
  
  
    "$log"(32);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(33);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
   EXCEPTION
    WHEN OTHERS THEN
      "$log"(34);ROLLBACK;
      "$log"(35);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(36);P_O_SUCCEED := '1';
      "$log"(37);V_ERR_CODE  := SQLCODE;
      "$log"(38);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(39);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(40);RAISE;
  END PROC_DEAL_BWZC;
  
  PROCEDURE PROC_BWZC_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    --记载数据处理日志
    "$log"(41);V_STEP_NO    := '1';
    "$log"(42);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_BWZC_DM';
    "$log"(43);V_LOG_LEVEL  := '3';
    "$log"(44);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(45);V_STEP_NO    := '2.1';
    "$log"(46);SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS')
      INTO V_BEGIN_TIME
      FROM DUAL;
    --开始删除DM表中数据
    "$log"(47);EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_ZBWZCDB';
    --数据写入DM表
    "$log"(48);INSERT INTO DM_CS_ZBWZCDB
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT A.ZONENO,
             A.BRNO,
             A.BANKFLAG,
             A.CURRTYPE,
             A.PRODUCT_ID,
             SUM(A.SJCPJE),
             SUM(A.JJZBZYE),
             SUM(A.SUMSJCPJE),
             SUM(A.SUMJJZBZYE)
        FROM DW_MOD_BWZC_DB A
       WHERE A.CURRTYPE IN ('001', '992', '994')
         AND A.DATA_DATE = P_I_DATE
       GROUP BY A.ZONENO, A.BRNO, A.BANKFLAG, A.PRODUCT_ID, A.CURRTYPE;
    "$log"(49);COMMIT;
    "$log"(50);P_O_SUCCEED := '0';
    "$log"(51);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(52);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(53);ROLLBACK;
      "$log"(54);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(55);P_O_SUCCEED := '1';
      "$log"(56);V_ERR_CODE  := SQLCODE;
      "$log"(57);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(58);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(59);RAISE;
  END PROC_BWZC_DM;
END PACK_DEAL_BWZC_1005;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_GRDK_1210 IS
  PROCEDURE PROC_DEAL_GRDK_MAIN(P_I_DATE IN VARCHAR2,
                                --P_I_ZONENO  IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_GRDK_MON(P_I_DATE IN VARCHAR2,
                               --P_I_ZONENO  IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_GRDK_YZ(P_I_DATE IN VARCHAR2,
                              --P_I_ZONENO  IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_GRDK_DW(P_I_DATE IN VARCHAR2,
                         --P_I_ZONENO  IN VARCHAR2,
                         P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_GRDK_DM(P_I_DATE IN VARCHAR2,
                         --P_I_ZONENO  IN VARCHAR2,
                         P_O_SUCCEED OUT VARCHAR2);
  FUNCTION F_GET_ZLXS(IS_DATEDATE      IN VARCHAR2,
                      IS_PRODUCT_ID    IN VARCHAR2,
                      IS_FIVLEVEL_CODE IN VARCHAR2) RETURN NUMBER;

  FUNCTION F_GET_BLXS(IS_DATEDATE   IN VARCHAR2,
                      IS_PRODUCT_ID IN VARCHAR2,
                      IS_RAWXS      IN NUMBER) RETURN NUMBER;
END PACK_DEAL_GRDK_1210;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_GRDK_1210 IS
	"$hits" aaa_coverage_tool.bool_tab; 
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c set hit = 1 where hit = 0 and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction; --req utl_http.req; resp utl_http.resp;
	  begin
        insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(36);
	  end if;
	  if "$times" is null then 
		 --"$times" := aaa_coverage_tool.get_statement_time_tab(36);
		 dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(36,x); "$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(36,x); "$times"(x):=true; 
	  end if;
	end;
	
	PROCEDURE PROC_DEAL_GRDK_MAIN(P_I_DATE IN VARCHAR2,
								  P_O_SUCCEED OUT VARCHAR2) IS
    BEGIN
    "$log"(2);
	IF P_I_DATE = TO_CHAR(LAST_DAY(TO_DATE(P_I_DATE, 'YYYYMMDD')), 'YYYYMMDD') THEN
      --每月清理月中的明细数据 PS后续考虑TRUNCATE PARTITION方式清理
      "$log"(3);
	  DELETE FROM DW_MOD_GRDK_UG DW WHERE DW.DATA_DATE = P_I_DATE;
      "$log"(4);
	  COMMIT;
      "$log"(5);
	  PROC_DEAL_GRDK_MON(P_I_DATE, P_O_SUCCEED);
      "$log"(6);
	  PROC_GRDK_DW(P_I_DATE, P_O_SUCCEED);
      "$log"(7);
	  PROC_GRDK_DM(P_I_DATE, P_O_SUCCEED);
    ELSE
      "$log"(8);
	  PROC_DEAL_GRDK_YZ(P_I_DATE, P_O_SUCCEED);
      "$log"(9);
	  PROC_GRDK_DW(P_I_DATE, P_O_SUCCEED);
      "$log"(10);
	  PROC_GRDK_DM(P_I_DATE, P_O_SUCCEED);
    END IF;
    --成功记录存储过程运行日志
    "$log"(11);P_O_SUCCEED := '0';
    "$log"(12);COMMIT;
    --出错处理
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(13);ROLLBACK;
      "$log"(14);P_O_SUCCEED := '1';
      "$log"(15);RAISE;
  END PROC_DEAL_GRDK_MAIN;

  PROCEDURE PROC_DEAL_GRDK_MON(P_I_DATE IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_X042       PRM_XS042.QUOTIETY%TYPE;
  BEGIN
    --第一步记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_DEAL_GRDK_1210.PROC_DEAL_GRDK_MON';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    SELECT MAX(X.QUOTIETY) INTO V_X042 FROM PRM_XS042 X WHERE X.START_DATE <= P_I_DATE AND X.END_DATE > P_I_DATE;
    IF V_X042 IS NULL THEN
      V_X042 := 1;
    END IF;
    V_STEP_NO    := '2.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    INSERT INTO DW_MOD_GRDK_UG
      (ZONENO,
       PHYBRNO,
       PCACCNO,
       PRODCODE,
       MEMNO,
       LNNO,
       TA500251054,
       RELATEDNO,
       CURRTYPE,
       SUBCODE,
       LOANTYPE,
       LEVELFIVE,
       LNAMOUNT,
       LNBALANCE,
       RATE,
       LOANTERM,
       AVCHTYPE,
       SURVEYOR,
       LOANDATE,
       DUEDATE,
       CUSTID,
       CUSTCIS,
       CUSTNAME,
       REGTYPE,
       REGNO,
       CREDITLEVEL,
       ECUSFLAG,
       AVCHAMT,
       MGAMT,
       MPAMT,
       ASSUREAMT,
       OTHERAMT,
       INVALIDAMT,
       CREDITAMT,
       TOTALAMT,
       STATUS,
       FBFLAG,
       PD_VALUE,
       LGD_VALUE,
       R_VALUE,
       DEFLT_FLAG,
       OVDUE_CNT,
       PRODUCT_ID3,
       PRODUCT_NAME3,
       MATURITY_DAYS,
       TERM_LEVEL_NO,
       TERM_LEVEL_NAME,
       K_VALUE,
       EC_COEF,
       ZCAMOUNT,
       ECOST,
       DATA_DATE,
       NLOAN_TJXS)
      SELECT 
       RES.ZONENO, --主机地区号
       RES.PHYBRNO, --物理网点号
       RES.PCACCNO, --贷款账号
       RES.PRODCODE, --产品代码
       RES.MEMNO, --主机合同号
       RES.LNNO, --主机借据序号
       RES.TA500251054, --主机合同号
       RES.RELATEDNO, --主机合同关联号
       RES.CURRTYPE, --币种
       RES.SUBCODE, --科目
       RES.LOANTYPE, --贷款种类
       RES.LEVELFIVE, --贷款形态（五级分类）
       RES.LNAMOUNT, --贷款金额（合同）
       RES.LNBALANCE, --借据当前余额
       RES.RATE, --利率
       RES.LOANTERM, --贷款期限（月）
       RES.AVCHTYPE, --担保方式
       RES.SURVEYOR, --调查人
       RES.LOANDATE, --借据起息日
       RES.DUEDATE, --实际约定到期日
       RES.CUSTID, --客户统一编码
       RES.CUSTCIS, --客户主机CIS号
       RES.CUSTNAME, --客户名称
       RES.REGTYPE, --证件类型
       RES.REGNO, --证件号码
       RES.CREDITLEVEL, --客户信用等级
       RES.ECUSFLAG, --优质客户认定标志
       RES.AVCHAMT, --担保额度（保证）
       RES.MGAMT, --抵押额度
       RES.MPAMT, --质押额度
       RES.ASSUREAMT, --保险金额
       RES.OTHERAMT, --其他担保方式金额
       RES.INVALIDAMT, --无效担保额
       RES.CREDITAMT, --信用额度
       RES.TOTALAMT, --担保及信用总额度
       RES.STATUS, --账户状态
       RES.FBFLAG, --零售风险暴露分类
       RES.PD_VALUE, --违约概率PD(百分比)
       RES.LGD_VALUE, --违约损失率LGD(百分比)
       RES.R_VALUE, --相关系数R(百分比)
       RES.DEFLT_FLAG, --违约标识
       RES.OVDUE_CNT, --逾期天数
       RES.PRODUCT_ID3, --三级产品代码
       RES.PRODUCT_NAME3, --三级产品名称
       RES.MATURITY_DAYS, --贷款余期天数 到日
       RES.TERM_LEVEL_NO, -- 贷款期限档次
       RES.TERM_LEVEL_NAME, --贷款期限名称
       RES.K_VALUE, --K值百分比
       CASE WHEN RES.EC_COEF >= 0.125 THEN 
               RES.EC_COEF 
            ELSE 
               RES.EC_COEF * NVL(PNX.TJXS,1) 
       END EC_COEF,
       RES.ZCAMOUNT, --资产余额
       RES.ECOST * CASE WHEN RES.EC_COEF >= 0.125 THEN 
               RES.EC_COEF 
            ELSE 
               RES.EC_COEF * NVL(PNX.TJXS,1) 
       END ECOST,--202001版本新增经济资本增减计调节系数,
       RES.DATA_DATE,
       NVL(PNX.TJXS,1) NLOAN_TJXS --经济资本增减及系数
      FROM (
      SELECT /*+LEADING(ODS PPS1 PGP) USE_HASH(ODS PPS1 PGP)*/
       ZONENO, --主机地区号
       PHYBRNO, --物理网点号
       PCACCNO, --贷款账号
       PRODCODE, --产品代码
       MEMNO, --主机合同号
       LNNO, --主机借据序号
       TA500251054, --主机合同号
       RELATEDNO, --主机合同关联号
       CURRTYPE, --币种
       SUBCODE, --科目
       LOANTYPE, --贷款种类
       LEVELFIVE, --贷款形态（五级分类）
       LNAMOUNT, --贷款金额（合同）
       LNBALANCE, --借据当前余额
       RATE, --利率
       LOANTERM, --贷款期限（月）
       AVCHTYPE, --担保方式
       SURVEYOR, --调查人
       LOANDATE, --借据起息日
       DUEDATE, --实际约定到期日
       CUSTID, --客户统一编码
       CUSTCIS, --客户主机CIS号
       CUSTNAME, --客户名称
       REGTYPE, --证件类型
       REGNO, --证件号码
       CREDITLEVEL, --客户信用等级
       ECUSFLAG, --优质客户认定标志
       AVCHAMT, --担保额度（保证）
       MGAMT, --抵押额度
       MPAMT, --质押额度
       ASSUREAMT, --保险金额
       OTHERAMT, --其他担保方式金额
       INVALIDAMT, --无效担保额
       CREDITAMT, --信用额度
       TOTALAMT, --担保及信用总额度
       STATUS, --账户状态
       FBFLAG, --零售风险暴露分类
       PD_VALUE, --违约概率PD(百分比)
       LGD_VALUE, --违约损失率LGD(百分比)
       R_VALUE, --相关系数R(百分比)
       DEFLT_FLAG, --违约标识
       OVDUE_CNT, --逾期天数
       PPS1.PRODUCT_ID PRODUCT_ID3, --三级产品代码
       PPS1.PRO_NAME PRODUCT_NAME3, --三级产品名称
       TO_DATE(ODS.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') MATURITY_DAYS, --贷款余期天数 到日
       pgp.TERM_LEVEL_NO TERM_LEVEL_NO, -- 贷款期限档次
       pgp.TERM_LEVEL_NAME TERM_LEVEL_NAME, --贷款期限名称
       ODS.K_VALUE K_VALUE, --K值百分比
       CASE
         WHEN ODS.DEFLT_FLAG <> '1' or ODS.DEFLT_FLAG is null THEN
          ODS.K_VALUE / 100 * DECODE(PGP.TERM_LEVEL_NO,
                                     '1',
                                     PPS1.VALUE1,
                                     '2',
                                     PPS1.VALUE2,
                                     '3',
                                     PPS1.VALUE3,
                                     '4',
                                     PPS1.VALUE4,
                                     '5',
                                     PPS1.VALUE5,
                                     '6',
                                     PPS1.VALUE6,
                                     '7',
                                     PPS1.VALUE7) * PPS1.PZXS
         WHEN ODS.DEFLT_FLAG = '1' THEN
          PACK_DEAL_GRDK_1210.F_GET_BLXS(P_I_DATE,
                                         PPS1.PRODUCT_ID,
                                         ODS.K_VALUE * 5 / 100) *
          PACK_DEAL_GRDK_1210.F_GET_ZLXS(P_I_DATE,
                                         PPS1.PRODUCT_ID,
                                         ODS.LEVELFIVE)
       END EC_COEF,
       DECODE(ODS.STATUS, --帐户状态6-已核销,8-作废,9-结清不计算经济资本
              '6',
              0,
              '8',
              0,
              '9',
              0,
              NVL(ABS(ODS.LNBALANCE), 0)) ZCAMOUNT, --资产余额
       DECODE(ODS.STATUS,
              '6',
              0,
              '8',
              0,
              '9',
              0,
              NVL(ABS(ODS.LNBALANCE), 0))  ECOST,--20191216 此处不乘经济资本系数
       P_I_DATE DATA_DATE

        FROM ODS_TMP_GRDK2 ODS,
             (select p.PRODUCT_ID,
                     SUBJECT_CODE,
                     PRODUCT_NAME,
                     PRO_NAME,
                     VALUE1,
                     VALUE2,
                     VALUE3,
                     VALUE4,
                     VALUE5,
                     VALUE6,
                     VALUE7,
                     zone_no,
                     pzxs
                from (select pps.PRODUCT_ID,
                             SUBJECT_CODE,
                             PRODUCT_NAME,
                             PRO_NAME,
                             VALUE1,
                             VALUE2,
                             VALUE3,
                             VALUE4,
                             VALUE5,
                             VALUE6,
                             VALUE7
                        from PRM_PRODUCT_SUBJECT pps, PRM_GDTERMF pgf --个人贷款信用风险余期调节系数表
                       where pps.product_id = pgf.product_id(+)
                         and P_I_DATE >= pgf.start_date
                         and P_I_DATE < pgf.end_date) p,
                     (SELECT B.product_id, A.zone_no, B.pzxs
                        FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                                FROM PRM_IRA_ORGAN
                               WHERE BELONG_BANK_FLAG = '4'
                                 AND START_DATE <= P_I_DATE
                                 AND END_DATE > P_I_DATE) A,
                             PRM_XS044 B --个人贷款信用风险地区产品调节系数表
                       WHERE A.BELONG_ZONENO = B.zone_no
                         AND B.START_DATE <= P_I_DATE
                         AND B.END_DATE > P_I_DATE) PX44
               where p.product_id = px44.product_id(+)
                 and p.PRODUCT_NAME = '2') pps1,
             (select term_level_no,
                     b.kind_name as term_level_name,
                     left_kb,
                     left_val,
                     right_val,
                     right_kb,
                     start_date,
                     end_date
                from prm_gdtermp a, dic_all_kind b --个人贷款期限档次定义表
               where a.term_level_no = b.kind_id
                 and b.operation_kind = 'GDTERM') PGP
       WHERE ODS.SUBCODE = PPS1.SUBJECT_CODE(+)
         AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') -
             TO_DATE(P_I_DATE, 'YYYYMMDD') >
             DECODE(PGP.LEFT_KB,
                    '1',
                    PGP.LEFT_VAL,
                    '2',
                    PGP.LEFT_VAL - 0.000000001)
         AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') -
             TO_DATE(P_I_DATE, 'YYYYMMDD') <
             DECODE(PGP.RIGHT_KB,
                    '1',
                    PGP.RIGHT_VAL,
                    '2',
                    PGP.RIGHT_VAL + 0.000000001)
         AND ODS.ZONENO = pps1.ZONE_NO(+)
         AND P_I_DATE >= PGP.START_DATE
         AND P_I_DATE < PGP.END_DATE
         AND ODS.K_VALUE IS NOT NULL
         and ODS.K_VALUE <> 0
      UNION ALL
      SELECT /*+ordered*/
       ZONENO, --主机地区号
       PHYBRNO, --物理网点号
       PCACCNO, --贷款账号
       PRODCODE, --产品代码
       MEMNO, --主机合同号
       LNNO, --主机借据序号
       TA500251054, --主机合同号
       RELATEDNO, --主机合同关联号
       CURRTYPE, --币种
       SUBCODE, --科目
       LOANTYPE, --贷款种类
       LEVELFIVE, --贷款形态（五级分类）
       LNAMOUNT, --贷款金额（合同）
       LNBALANCE, --借据当前余额
       RATE, --利率
       LOANTERM, --贷款期限（月）
       AVCHTYPE, --担保方式
       SURVEYOR, --调查人
       LOANDATE, --借据起息日
       DUEDATE, --实际约定到期日
       CUSTID, --客户统一编码
       CUSTCIS, --客户主机CIS号
       CUSTNAME, --客户名称
       REGTYPE, --证件类型
       REGNO, --证件号码
       CREDITLEVEL, --客户信用等级
       ECUSFLAG, --优质客户认定标志
       AVCHAMT, --担保额度（保证）
       MGAMT, --抵押额度
       MPAMT, --质押额度
       ASSUREAMT, --保险金额
       OTHERAMT, --其他担保方式金额
       INVALIDAMT, --无效担保额
       CREDITAMT, --信用额度
       TOTALAMT, --担保及信用总额度
       STATUS, --账户状态
       FBFLAG, --零售风险暴露分类
       PD_VALUE, --违约概率PD(百分比)
       LGD_VALUE, --违约损失率LGD(百分比)
       R_VALUE, --相关系数R(百分比)
       DEFLT_FLAG, --违约标识
       OVDUE_CNT, --逾期天数
       P.PRODUCT_ID PRODUCT_ID3, --三级产品代码
       P.PRO_NAME PRODUCT_NAME3, --三级产品名称
       TO_DATE(ODS.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') MATURITY_DAYS, --贷款余期天数 到日
       X43.QXDC_ID TERM_LEVEL_NO, -- 贷款期限档次
       X43.DBDC_NAME TERM_LEVEL_NAME, --贷款期限名称
       ODS.K_VALUE K_VALUE, --K值百分比
       --经济资本系数
       DECODE(NVL(ODS.TOTALAMT, 0),
              0,
              0,
              (NVL(ODS.AVCHAMT, 0) * X41.BZXS_VALUE +
              NVL(ODS.MGAMT, 0) * X41.DYXS_VALUE +
              NVL(ODS.MPAMT, 0) * X41.ZYXS_VALUE +
              NVL(ODS.ASSUREAMT, 0) * X41.BDXS_VALUE +
              NVL(ODS.OTHERAMT, 0) * X41.QTXS_VALUE +
              NVL(ODS.INVALIDAMT, 0) * X41.WXXS_VALUE +
              NVL(ODS.CREDITAMT, 0) * X41.XYXS_VALUE) / NVL(ODS.TOTALAMT, 0) *
              NVL(X42.QUOTIETY,
                  V_X042 ----如空 则取这个产品最大的调整系数
                  ) * X43.XS_VALUE) EC_COEF,
       --资产余额
       DECODE(ODS.STATUS, --帐户状态6-已核销,8-作废,9-结清不计算经济资本
              '6',
              0,
              '8',
              0,
              '9',
              0,
              NVL(ABS(ODS.LNBALANCE), 0)) ZCAMOUNT,
       --经济资本
       DECODE(ODS.STATUS,
              '6',
              0,
              '8',
              0,
              '9',
              0,
              NVL(ABS(ODS.LNBALANCE), 0))  ECOST,--20191216 此处不乘经济资本系数
       P_I_DATE DATA_DATE
        FROM ODS_TMP_GRDK2 ODS,
             PRM_PRODUCT_SUBJECT P,
             PRM_XS041 X41,
             (SELECT A.FL5_ID    FL5_ID,
                     A.BBPRECENT BBPRECENT,
                     B.QUOTIETY  QUOTIETY
                FROM PRM_CS042 A, PRM_XS042 B
               WHERE A.START_DATE <= P_I_DATE
                 AND A.END_DATE > P_I_DATE
                 AND B.START_DATE <= P_I_DATE
                 AND B.END_DATE > P_I_DATE
                 AND A.FL5_ID = B.QCLASS) X42, --个人贷款信用风险贷款质量调节系数表 关联条件不用产品
             PRM_XS043 X43 --个人贷款信用风险余期调节系数表
       WHERE P.PRODUCT_ID = X41.PRODUCT_ID(+) --信用风险取值
         AND X41.START_DATE(+) <= P_I_DATE
         AND X41.END_DATE(+) > P_I_DATE
         AND ODS.LEVELFIVE = X42.FL5_ID(+)
         AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') -
             TO_DATE(P_I_DATE, 'YYYYMMDD') >
             DECODE(X43.LEFT_IN,
                    '1',
                    X43.LEFT_VALUE,
                    '2',
                    X43.LEFT_VALUE - 0.000000001)
         AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') -
             TO_DATE(P_I_DATE, 'YYYYMMDD') <
             DECODE(X43.RIGHT_IN,
                    '1',
                    X43.RIGHT_VALUE,
                    '2',
                    X43.RIGHT_VALUE + 0.000000001)
         AND X43.START_DATE(+) <= P_I_DATE
         AND X43.END_DATE(+) > P_I_DATE
         AND ODS.SUBCODE = P.SUBJECT_CODE(+)
         AND P.PRODUCT_NAME(+) = '2'
         AND (ODS.K_VALUE IS NULL OR ODS.K_VALUE = 0)) res, --K系数为空的逻辑(内评未覆盖情况，K为空或者0)
         (SELECT PRODUCT_ID, NLOAN_STARTDAY, NLOAN_ENDDAY, TJXS
                FROM (SELECT P.PRODUCT_ID,
                             P.NLOAN_STARTDAY,
                             P.NLOAN_ENDDAY,
                             P.TJXS,
                             ROW_NUMBER() OVER(PARTITION BY P.PRODUCT_ID, P.NLOAN_STARTDAY ORDER BY P.IMPORT_TIME DESC) RN
                        FROM PRM_NLOAN_XS P
                       WHERE P.VALID_STATUS = '1')
               WHERE RN = 1) PNX --202001版本新增经济资本增减计调节系数
       WHERE RES.PRODUCT_ID3 = PNX.PRODUCT_ID(+)
         AND RES.LOANDATE >= PNX.NLOAN_STARTDAY(+)
         AND RES.LOANDATE <= PNX.NLOAN_ENDDAY(+);
    COMMIT;

    P_O_SUCCEED := '0';
    --记载数据处理日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END PROC_DEAL_GRDK_MON;

  PROCEDURE PROC_DEAL_GRDK_YZ(P_I_DATE IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_FIRSTDAY   DW_MOD_YHK_UG.DATA_DATE%TYPE;
    V_LASTMONTH  DW_MOD_YHK_UG.DATA_DATE%TYPE;
    V_X042       PRM_XS042.QUOTIETY%TYPE;
  BEGIN
    --第一步记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_DEAL_GRDK_1210.PROC_DEAL_GRDK_YZ';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_FIRSTDAY   := SUBSTR(P_I_DATE, 1, 6) || '01';
    V_LASTMONTH  := TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(P_I_DATE, 'YYYYMMDD'), -1)), 'YYYYMMDD');
    --第二步月初特殊处理
    V_STEP_NO    := '2.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    SELECT MAX(X.QUOTIETY) INTO V_X042 FROM PRM_XS042 X WHERE X.START_DATE <= P_I_DATE AND X.END_DATE > P_I_DATE;
    IF V_X042 IS NULL THEN
      V_X042 := 1;
    END IF;
    --如果等于月初，则进行复制上月末明细的操作
    IF P_I_DATE = V_FIRSTDAY THEN
      --预先清理数据
      DELETE FROM DW_MOD_GRDK_UG WHERE DATA_DATE = P_I_DATE;
      INSERT INTO DW_MOD_GRDK_UG
        (ZONENO,
         PHYBRNO,
         PCACCNO,
         PRODCODE,
         MEMNO,
         LNNO,
         TA500251054,
         RELATEDNO,
         CURRTYPE,
         SUBCODE,
         LOANTYPE,
         LEVELFIVE,
         LNAMOUNT,
         LNBALANCE,
         RATE,
         LOANTERM,
         AVCHTYPE,
         SURVEYOR,
         LOANDATE,
         DUEDATE,
         CUSTID,
         CUSTCIS,
         CUSTNAME,
         REGTYPE,
         REGNO,
         CREDITLEVEL,
         ECUSFLAG,
         AVCHAMT,
         MGAMT,
         MPAMT,
         ASSUREAMT,
         OTHERAMT,
         INVALIDAMT,
         CREDITAMT,
         TOTALAMT,
         STATUS,
         FBFLAG,
         PD_VALUE,
         LGD_VALUE,
         R_VALUE,
         DEFLT_FLAG,
         OVDUE_CNT,
         PRODUCT_ID3,
         PRODUCT_NAME3,
         MATURITY_DAYS,
         TERM_LEVEL_NO,
         TERM_LEVEL_NAME,
         K_VALUE,
         EC_COEF,
         ZCAMOUNT,
         ECOST,
         DATA_DATE)
        SELECT /*+PARALLEL(DW,8) */
         ZONENO,
         PHYBRNO,
         PCACCNO,
         PRODCODE,
         MEMNO,
         LNNO,
         TA500251054,
         RELATEDNO,
         CURRTYPE,
         SUBCODE,
         LOANTYPE,
         LEVELFIVE,
         LNAMOUNT,
         LNBALANCE,
         RATE,
         LOANTERM,
         AVCHTYPE,
         SURVEYOR,
         LOANDATE,
         DUEDATE,
         CUSTID,
         CUSTCIS,
         CUSTNAME,
         REGTYPE,
         REGNO,
         CREDITLEVEL,
         ECUSFLAG,
         AVCHAMT,
         MGAMT,
         MPAMT,
         ASSUREAMT,
         OTHERAMT,
         INVALIDAMT,
         CREDITAMT,
         TOTALAMT,
         STATUS,
         FBFLAG,
         PD_VALUE,
         LGD_VALUE,
         R_VALUE,
         DEFLT_FLAG,
         OVDUE_CNT,
         PRODUCT_ID3,
         PRODUCT_NAME3,
         MATURITY_DAYS,
         TERM_LEVEL_NO,
         TERM_LEVEL_NAME,
         K_VALUE,
         EC_COEF,
         ZCAMOUNT,
         ECOST,
         P_I_DATE
          FROM DW_MOD_GRDK_UG DW
         WHERE DW.DATA_DATE = V_LASTMONTH;
    END IF;
    --第三步ODS表进DW表
    V_STEP_NO    := '3.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    -----------对DW表中月初至今的明细进行增量更新当月末经济资本
    MERGE INTO DW_MOD_GRDK_UG DW
    USING (SELECT RES.ZONENO, --主机地区号
            RES.PHYBRNO, --物理网点号
            RES.PCACCNO, --贷款账号
            RES.PRODCODE, --产品代码
            RES.MEMNO, --主机合同号
            RES.LNNO, --主机借据序号
            RES.TA500251054, --主机合同号
            RES.RELATEDNO, --主机合同关联号
            RES.CURRTYPE, --币种
            RES.SUBCODE, --科目
            RES.LOANTYPE, --贷款种类
            RES.LEVELFIVE, --贷款形态（五级分类）
            RES.LNAMOUNT, --贷款金额（合同）
            RES.LNBALANCE, --借据当前余额
            RES.RATE, --利率
            RES.LOANTERM, --贷款期限（月）
            RES.AVCHTYPE, --担保方式
            RES.SURVEYOR, --调查人
            RES.LOANDATE, --借据起息日
            RES.DUEDATE, --实际约定到期日
            RES.CUSTID, --客户统一编码
            RES.CUSTCIS, --客户主机CIS号
            RES.CUSTNAME, --客户名称
            RES.REGTYPE, --证件类型
            RES.REGNO, --证件号码
            RES.CREDITLEVEL, --客户信用等级
            RES.ECUSFLAG, --优质客户认定标志
            RES.AVCHAMT, --担保额度（保证）
            RES.MGAMT, --抵押额度
            RES.MPAMT, --质押额度
            RES.ASSUREAMT, --保险金额
            RES.OTHERAMT, --其他担保方式金额
            RES.INVALIDAMT, --无效担保额
            RES.CREDITAMT, --信用额度
            RES.TOTALAMT, --担保及信用总额度
            RES.STATUS, --账户状态
            RES.FBFLAG, --零售风险暴露分类
            RES.PD_VALUE, --违约概率PD(百分比)
            RES.LGD_VALUE, --违约损失率LGD(百分比)
            RES.R_VALUE, --相关系数R(百分比)
            RES.DEFLT_FLAG, --违约标识
            RES.OVDUE_CNT, --逾期天数
            RES.PRODUCT_ID3, --三级产品代码
            RES.PRODUCT_NAME3, --三级产品名称
            RES.MATURITY_DAYS, --贷款余期天数 到日
            RES.TERM_LEVEL_NO, -- 贷款期限档次
            RES.TERM_LEVEL_NAME, --贷款期限名称
            RES.K_VALUE, --K值百分比
            CASE WHEN RES.EC_COEF >= 0.125 THEN 
               RES.EC_COEF 
            ELSE 
               RES.EC_COEF * NVL(PNX.TJXS,1) 
            END EC_COEF,
            RES.ZCAMOUNT, --资产余额
            RES.ECOST * CASE WHEN RES.EC_COEF >= 0.125 THEN 
               RES.EC_COEF 
            ELSE 
               RES.EC_COEF * NVL(PNX.TJXS,1) 
            END ECOST,--202001版本新增经济资本增减计调节系数
            RES.DATA_DATE,
            NVL(PNX.TJXS,1) NLOAN_TJXS  
      FROM (
    SELECT /*+LEADING(ODS PPS1 PGP) USE_HASH(ODS PPS1 PGP)*/
            ZONENO, --主机地区号
            PHYBRNO, --物理网点号
            PCACCNO, --贷款账号
            PRODCODE, --产品代码
            MEMNO, --主机合同号
            LNNO, --主机借据序号
            TA500251054, --主机合同号
            RELATEDNO, --主机合同关联号
            CURRTYPE, --币种
            SUBCODE, --科目
            LOANTYPE, --贷款种类
            LEVELFIVE, --贷款形态（五级分类）
            LNAMOUNT, --贷款金额（合同）
            LNBALANCE, --借据当前余额
            RATE, --利率
            LOANTERM, --贷款期限（月）
            AVCHTYPE, --担保方式
            SURVEYOR, --调查人
            LOANDATE, --借据起息日
            DUEDATE, --实际约定到期日
            CUSTID, --客户统一编码
            CUSTCIS, --客户主机CIS号
            CUSTNAME, --客户名称
            REGTYPE, --证件类型
            REGNO, --证件号码
            CREDITLEVEL, --客户信用等级
            ECUSFLAG, --优质客户认定标志
            AVCHAMT, --担保额度（保证）
            MGAMT, --抵押额度
            MPAMT, --质押额度
            ASSUREAMT, --保险金额
            OTHERAMT, --其他担保方式金额
            INVALIDAMT, --无效担保额
            CREDITAMT, --信用额度
            TOTALAMT, --担保及信用总额度
            STATUS, --账户状态
            FBFLAG, --零售风险暴露分类
            PD_VALUE, --违约概率PD(百分比)
            LGD_VALUE, --违约损失率LGD(百分比)
            R_VALUE, --相关系数R(百分比)
            DEFLT_FLAG, --违约标识
            OVDUE_CNT, --逾期天数
            PPS1.PRODUCT_ID PRODUCT_ID3, --三级产品代码
            PPS1.PRO_NAME PRODUCT_NAME3, --三级产品名称
            TO_DATE(ODS.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') MATURITY_DAYS, --贷款余期天数 到日
            pgp.TERM_LEVEL_NO TERM_LEVEL_NO, -- 贷款期限档次
            pgp.TERM_LEVEL_NAME TERM_LEVEL_NAME, --贷款期限名称
            ODS.K_VALUE K_VALUE, --K值百分比
            CASE
              WHEN ODS.DEFLT_FLAG <> '1' or ODS.DEFLT_FLAG is null THEN
               ODS.K_VALUE / 100 * DECODE(PGP.TERM_LEVEL_NO,
                                          '1',
                                          PPS1.VALUE1,
                                          '2',
                                          PPS1.VALUE2,
                                          '3',
                                          PPS1.VALUE3,
                                          '4',
                                          PPS1.VALUE4,
                                          '5',
                                          PPS1.VALUE5,
                                          '6',
                                          PPS1.VALUE6,
                                          '7',
                                          PPS1.VALUE7) * PPS1.PZXS
              WHEN ODS.DEFLT_FLAG = '1' THEN
               PACK_DEAL_GRDK_1210.F_GET_BLXS(P_I_DATE, PPS1.PRODUCT_ID, ODS.K_VALUE * 5 / 100) *
               PACK_DEAL_GRDK_1210.F_GET_ZLXS(P_I_DATE, PPS1.PRODUCT_ID, ODS.LEVELFIVE)
            END EC_COEF,
            DECODE(ODS.STATUS, --帐户状态6-已核销,8-作废,9-结清不计算经济资本
                   '6',
                   0,
                   '8',
                   0,
                   '9',
                   0,
                   NVL(ABS(ODS.LNBALANCE), 0)) ZCAMOUNT, --资产余额
            DECODE(ODS.STATUS,
                   '6',
                   0,
                   '8',
                   0,
                   '9',
                   0,
                   NVL(ABS(ODS.LNBALANCE), 0)) ECOST,--20191216 此处不乘经济资本系数
            P_I_DATE DATA_DATE

             FROM ODS_TMP_GRDK2 ODS,
                  (select p.PRODUCT_ID,
                          SUBJECT_CODE,
                          PRODUCT_NAME,
                          PRO_NAME,
                          VALUE1,
                          VALUE2,
                          VALUE3,
                          VALUE4,
                          VALUE5,
                          VALUE6,
                          VALUE7,
                          zone_no,
                          pzxs
                     from (select pps.PRODUCT_ID,
                                  SUBJECT_CODE,
                                  PRODUCT_NAME,
                                  PRO_NAME,
                                  VALUE1,
                                  VALUE2,
                                  VALUE3,
                                  VALUE4,
                                  VALUE5,
                                  VALUE6,
                                  VALUE7
                             from PRM_PRODUCT_SUBJECT pps, PRM_GDTERMF pgf
                            where pps.product_id = pgf.product_id(+)
                              and P_I_DATE >= pgf.start_date
                              and P_I_DATE < pgf.end_date) p,
                          (SELECT B.product_id, A.zone_no, B.pzxs
                             FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                                     FROM PRM_IRA_ORGAN
                                    WHERE BELONG_BANK_FLAG = '4'
                                      AND START_DATE <= P_I_DATE
                                      AND END_DATE > P_I_DATE) A,
                                  PRM_XS044 B
                            WHERE A.BELONG_ZONENO = B.zone_no
                              AND B.START_DATE <= P_I_DATE
                              AND B.END_DATE > P_I_DATE) PX44
                    where p.product_id = px44.product_id(+)
                      and p.PRODUCT_NAME = '2') pps1,
                  (select term_level_no,
                          b.kind_name as term_level_name,
                          left_kb,
                          left_val,
                          right_val,
                          right_kb,
                          start_date,
                          end_date
                     from prm_gdtermp a, dic_all_kind b
                    where a.term_level_no = b.kind_id
                      and b.operation_kind = 'GDTERM') PGP
            WHERE ODS.SUBCODE = PPS1.SUBJECT_CODE(+)
              AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') -
                  TO_DATE(P_I_DATE, 'YYYYMMDD') >
                  DECODE(PGP.LEFT_KB,
                         '1',
                         PGP.LEFT_VAL,
                         '2',
                         PGP.LEFT_VAL - 0.000000001)
              AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') -
                  TO_DATE(P_I_DATE, 'YYYYMMDD') <
                  DECODE(PGP.RIGHT_KB,
                         '1',
                         PGP.RIGHT_VAL,
                         '2',
                         PGP.RIGHT_VAL + 0.000000001)
              AND ODS.ZONENO = pps1.ZONE_NO(+)
              AND P_I_DATE >= PGP.START_DATE
              AND P_I_DATE < PGP.END_DATE
              AND ODS.K_VALUE IS NOT NULL
              and ODS.K_VALUE <> 0
           --K系数不空不为0的逻辑（20121220内评覆盖定义：不空不0为覆盖）
           UNION ALL
           SELECT /*+ordered*/
            ZONENO, --主机地区号
            PHYBRNO, --物理网点号
            PCACCNO, --贷款账号
            PRODCODE, --产品代码
            MEMNO, --主机合同号
            LNNO, --主机借据序号
            TA500251054, --主机合同号
            RELATEDNO, --主机合同关联号
            CURRTYPE, --币种
            SUBCODE, --科目
            LOANTYPE, --贷款种类
            LEVELFIVE, --贷款形态（五级分类）
            LNAMOUNT, --贷款金额（合同）
            LNBALANCE, --借据当前余额
            RATE, --利率
            LOANTERM, --贷款期限（月）
            AVCHTYPE, --担保方式
            SURVEYOR, --调查人
            LOANDATE, --借据起息日
            DUEDATE, --实际约定到期日
            CUSTID, --客户统一编码
            CUSTCIS, --客户主机CIS号
            CUSTNAME, --客户名称
            REGTYPE, --证件类型
            REGNO, --证件号码
            CREDITLEVEL, --客户信用等级
            ECUSFLAG, --优质客户认定标志
            AVCHAMT, --担保额度（保证）
            MGAMT, --抵押额度
            MPAMT, --质押额度
            ASSUREAMT, --保险金额
            OTHERAMT, --其他担保方式金额
            INVALIDAMT, --无效担保额
            CREDITAMT, --信用额度
            TOTALAMT, --担保及信用总额度
            STATUS, --账户状态
            FBFLAG, --零售风险暴露分类
            PD_VALUE, --违约概率PD(百分比)
            LGD_VALUE, --违约损失率LGD(百分比)
            R_VALUE, --相关系数R(百分比)
            DEFLT_FLAG, --违约标识
            OVDUE_CNT, --逾期天数
            P.PRODUCT_ID PRODUCT_ID3, --三级产品代码
            P.PRO_NAME PRODUCT_NAME3, --三级产品名称
            TO_DATE(ODS.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') MATURITY_DAYS, --贷款余期天数 到日
            X43.QXDC_ID TERM_LEVEL_NO, -- 贷款期限档次
            X43.DBDC_NAME TERM_LEVEL_NAME, --贷款期限名称
            ODS.K_VALUE K_VALUE, --K值百分比
            --经济资本系数
            DECODE(NVL(ODS.TOTALAMT, 0),
                   0,
                   0,
                   (NVL(ODS.AVCHAMT, 0) * X41.BZXS_VALUE +
                   NVL(ODS.MGAMT, 0) * X41.DYXS_VALUE +
                   NVL(ODS.MPAMT, 0) * X41.ZYXS_VALUE +
                   NVL(ODS.ASSUREAMT, 0) * X41.BDXS_VALUE +
                   NVL(ODS.OTHERAMT, 0) * X41.QTXS_VALUE +
                   NVL(ODS.INVALIDAMT, 0) * X41.WXXS_VALUE +
                   NVL(ODS.CREDITAMT, 0) * X41.XYXS_VALUE) /
                   NVL(ODS.TOTALAMT, 0) * NVL(X42.QUOTIETY, V_X042 ) * X43.XS_VALUE) EC_COEF,
            --资产余额
            DECODE(ODS.STATUS, --帐户状态6-已核销,8-作废,9-结清不计算经济资本
                   '6',
                   0,
                   '8',
                   0,
                   '9',
                   0,
                   NVL(ABS(ODS.LNBALANCE), 0)) ZCAMOUNT,
            --经济资本
            DECODE(ODS.STATUS,
                   '6',
                   0,
                   '8',
                   0,
                   '9',
                   0,
                   NVL(ABS(ODS.LNBALANCE), 0))  ECOST,--20191216 此处不乘经济资本系数
            P_I_DATE DATA_DATE
             FROM ODS_TMP_GRDK2 ODS,
                  PRM_PRODUCT_SUBJECT P,
                  PRM_XS041 X41,
                  (SELECT A.FL5_ID    FL5_ID,
                          A.BBPRECENT BBPRECENT,
                          B.QUOTIETY  QUOTIETY
                     FROM PRM_CS042 A, PRM_XS042 B
                    WHERE A.START_DATE <= P_I_DATE
                      AND A.END_DATE > P_I_DATE
                      AND B.START_DATE <= P_I_DATE
                      AND B.END_DATE > P_I_DATE
                      AND A.FL5_ID = B.QCLASS) X42, --个人贷款信用风险贷款质量调节系数表 关联条件不用产品
                  PRM_XS043 X43 --个人贷款信用风险余期调节系数表
            WHERE P.PRODUCT_ID = X41.PRODUCT_ID(+) --信用风险取值
              AND X41.START_DATE(+) <= P_I_DATE
              AND X41.END_DATE(+) > P_I_DATE
              AND ODS.LEVELFIVE = X42.FL5_ID(+)
              AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') > DECODE(X43.LEFT_IN, '1', X43.LEFT_VALUE, '2', X43.LEFT_VALUE - 0.000000001)
              AND TO_DATE(ODS.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') < DECODE(X43.RIGHT_IN, '1', X43.RIGHT_VALUE, '2', X43.RIGHT_VALUE + 0.000000001)
              AND X43.START_DATE(+) <= P_I_DATE
              AND X43.END_DATE(+) > P_I_DATE
              AND ODS.SUBCODE = P.SUBJECT_CODE(+)
              AND P.PRODUCT_NAME(+) = '2'
              AND (ODS.K_VALUE IS NULL OR ODS.K_VALUE = 0)) RES,
              (SELECT PRODUCT_ID, NLOAN_STARTDAY, NLOAN_ENDDAY, TJXS
                FROM (SELECT P.PRODUCT_ID,
                             P.NLOAN_STARTDAY,
                             P.NLOAN_ENDDAY,
                             P.TJXS,
                             ROW_NUMBER() OVER(PARTITION BY P.PRODUCT_ID, P.NLOAN_STARTDAY ORDER BY P.IMPORT_TIME DESC) RN
                        FROM PRM_NLOAN_XS P
                       WHERE P.VALID_STATUS = '1')
               WHERE RN = 1) PNX --202001版本新增经济资本增减计调节系数
       WHERE RES.PRODUCT_ID3 = PNX.PRODUCT_ID(+)
         AND RES.LOANDATE >= PNX.NLOAN_STARTDAY(+)
         AND RES.LOANDATE <= PNX.NLOAN_ENDDAY(+)   
              ) ODS
    ON (ODS.PCACCNO = DW.PCACCNO AND ODS.PRODCODE = DW.PRODCODE AND ODS.MEMNO = DW.MEMNO AND
    ODS.LNNO = DW.LNNO AND ODS.CURRTYPE = DW.CURRTYPE AND DW.DATA_DATE >= V_FIRSTDAY AND DW.DATA_DATE <= P_I_DATE)
    WHEN MATCHED THEN
      UPDATE
         SET DW.ZONENO          = ODS.ZONENO,
             DW.PHYBRNO         = ODS.PHYBRNO,
             DW.TA500251054     = ODS.TA500251054,
             DW.RELATEDNO       = ODS.RELATEDNO,
             DW.SUBCODE         = ODS.SUBCODE,
             DW.LOANTYPE        = ODS.LOANTYPE,
             DW.LEVELFIVE       = ODS.LEVELFIVE,
             DW.LNAMOUNT        = ODS.LNAMOUNT,
             DW.LNBALANCE       = ODS.LNBALANCE,
             DW.RATE            = ODS.RATE,
             DW.LOANTERM        = ODS.LOANTERM,
             DW.AVCHTYPE        = ODS.AVCHTYPE,
             DW.SURVEYOR        = ODS.SURVEYOR,
             DW.LOANDATE        = ODS.LOANDATE,
             DW.DUEDATE         = ODS.DUEDATE,
             DW.CUSTID          = ODS.CUSTID,
             DW.CUSTCIS         = ODS.CUSTCIS,
             DW.CUSTNAME        = ODS.CUSTNAME,
             DW.REGTYPE         = ODS.REGTYPE,
             DW.REGNO           = ODS.REGNO,
             DW.CREDITLEVEL     = ODS.CREDITLEVEL,
             DW.ECUSFLAG        = ODS.ECUSFLAG,
             DW.AVCHAMT         = ODS.AVCHAMT,
             DW.MGAMT           = ODS.MGAMT,
             DW.MPAMT           = ODS.MPAMT,
             DW.ASSUREAMT       = ODS.ASSUREAMT,
             DW.OTHERAMT        = ODS.OTHERAMT,
             DW.INVALIDAMT      = ODS.INVALIDAMT,
             DW.CREDITAMT       = ODS.CREDITAMT,
             DW.TOTALAMT        = ODS.TOTALAMT,
             DW.STATUS          = ODS.STATUS,
             DW.FBFLAG          = ODS.FBFLAG,
             DW.PD_VALUE        = ODS.PD_VALUE,
             DW.LGD_VALUE       = ODS.LGD_VALUE,
             DW.R_VALUE         = ODS.R_VALUE,
             DW.DEFLT_FLAG      = ODS.DEFLT_FLAG,
             DW.OVDUE_CNT       = ODS.OVDUE_CNT,
             DW.PRODUCT_ID3     = ODS.PRODUCT_ID3,
             DW.PRODUCT_NAME3   = ODS.PRODUCT_NAME3,
             DW.MATURITY_DAYS   = ODS.MATURITY_DAYS,
             DW.TERM_LEVEL_NO   = ODS.TERM_LEVEL_NO,
             DW.TERM_LEVEL_NAME = ODS.TERM_LEVEL_NAME,
             DW.K_VALUE         = ODS.K_VALUE,
             DW.EC_COEF         = ODS.EC_COEF,
             DW.ZCAMOUNT        = ODS.ZCAMOUNT,
             DW.ECOST           = ODS.ECOST,
             DW.NLOAN_TJXS      = ODS.NLOAN_TJXS
    WHEN NOT MATCHED THEN
      INSERT
        (DW.ZONENO,
         DW.PHYBRNO,
         DW.PCACCNO,
         DW.PRODCODE,
         DW.MEMNO,
         DW.LNNO,
         DW.TA500251054,
         DW.RELATEDNO,
         DW.CURRTYPE,
         DW.SUBCODE,
         DW.LOANTYPE,
         DW.LEVELFIVE,
         DW.LNAMOUNT,
         DW.LNBALANCE,
         DW.RATE,
         DW.LOANTERM,
         DW.AVCHTYPE,
         DW.SURVEYOR,
         DW.LOANDATE,
         DW.DUEDATE,
         DW.CUSTID,
         DW.CUSTCIS,
         DW.CUSTNAME,
         DW.REGTYPE,
         DW.REGNO,
         DW.CREDITLEVEL,
         DW.ECUSFLAG,
         DW.AVCHAMT,
         DW.MGAMT,
         DW.MPAMT,
         DW.ASSUREAMT,
         DW.OTHERAMT,
         DW.INVALIDAMT,
         DW.CREDITAMT,
         DW.TOTALAMT,
         DW.STATUS,
         DW.FBFLAG,
         DW.PD_VALUE,
         DW.LGD_VALUE,
         DW.R_VALUE,
         DW.DEFLT_FLAG,
         DW.OVDUE_CNT,
         DW.PRODUCT_ID3,
         DW.PRODUCT_NAME3,
         DW.MATURITY_DAYS,
         DW.TERM_LEVEL_NO,
         DW.TERM_LEVEL_NAME,
         DW.K_VALUE,
         DW.EC_COEF,
         DW.ZCAMOUNT,
         DW.ECOST,
         DW.DATA_DATE,
         DW.NLOAN_TJXS)
      VALUES
        (ODS.ZONENO,
         ODS.PHYBRNO,
         ODS.PCACCNO,
         ODS.PRODCODE,
         ODS.MEMNO,
         ODS.LNNO,
         ODS.TA500251054,
         ODS.RELATEDNO,
         ODS.CURRTYPE,
         ODS.SUBCODE,
         ODS.LOANTYPE,
         ODS.LEVELFIVE,
         ODS.LNAMOUNT,
         ODS.LNBALANCE,
         ODS.RATE,
         ODS.LOANTERM,
         ODS.AVCHTYPE,
         ODS.SURVEYOR,
         ODS.LOANDATE,
         ODS.DUEDATE,
         ODS.CUSTID,
         ODS.CUSTCIS,
         ODS.CUSTNAME,
         ODS.REGTYPE,
         ODS.REGNO,
         ODS.CREDITLEVEL,
         ODS.ECUSFLAG,
         ODS.AVCHAMT,
         ODS.MGAMT,
         ODS.MPAMT,
         ODS.ASSUREAMT,
         ODS.OTHERAMT,
         ODS.INVALIDAMT,
         ODS.CREDITAMT,
         ODS.TOTALAMT,
         ODS.STATUS,
         ODS.FBFLAG,
         ODS.PD_VALUE,
         ODS.LGD_VALUE,
         ODS.R_VALUE,
         ODS.DEFLT_FLAG,
         ODS.OVDUE_CNT,
         ODS.PRODUCT_ID3,
         ODS.PRODUCT_NAME3,
         ODS.MATURITY_DAYS,
         ODS.TERM_LEVEL_NO,
         ODS.TERM_LEVEL_NAME,
         ODS.K_VALUE,
         ODS.EC_COEF,
         ODS.ZCAMOUNT,
         ODS.ECOST,
         P_I_DATE,
         ODS.NLOAN_TJXS);
    COMMIT;
    P_O_SUCCEED := '0';
    --记载数据处理日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL,V_BEGIN_TIME,V_END_TIME,'FAILED',V_STEP_NO,V_SQL_TXT,V_ERR_CODE,V_ERR_TXT);
      RAISE;
  END PROC_DEAL_GRDK_YZ;

  PROCEDURE PROC_GRDK_DW(P_I_DATE    IN VARCHAR2, 
                         P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_FIRSTDAY   DW_MOD_YHK_UG.DATA_DATE%TYPE;
    V_LASTMONTH  DW_MOD_YHK_UG.DATA_DATE%TYPE;
  BEGIN

    --第一步记载数据处理日志
    "$log"(65);V_STEP_NO    := '1';
    "$log"(66);V_PROC_NAME  := 'PACK_DEAL_GRDK_1210.PROC_GRDK_DW';
    "$log"(67);V_LOG_LEVEL  := '3';
    "$log"(68);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(69);V_FIRSTDAY   := SUBSTR(P_I_DATE, 1, 6) || '01';
    "$log"(70);V_LASTMONTH  := TO_CHAR(LAST_DAY(TO_DATE(P_I_DATE, 'YYYYMMDD')), 'YYYYMMDD');--应为本月末 by caohui 20130201
    --第二步月初特殊处理
    "$log"(71);V_STEP_NO    := '2.1';
    "$log"(72);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --如果月初，则对DW_SUM_GRDK_UG表进行truncate处理
    "$log"(73);
	IF P_I_DATE = V_FIRSTDAY THEN
      "$log"(74);
	  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_SUM_GRDK_UG';
    END IF;
    --预先清理历史数据
    "$log"(75);
	DELETE FROM DW_SUM_GRDK_UG UG WHERE UG.DATA_DATE = P_I_DATE;
    --第三步每日对DW进行载入处理特殊处理
    "$log"(76);V_STEP_NO    := '3.1';
    "$log"(77);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(78);
	IF P_I_DATE = V_LASTMONTH THEN
      --每日存量统计汇总值
      "$log"(79);
	  INSERT INTO DW_SUM_GRDK_UG
        (ZONENO,
         ACTBRNO,
         SUBCODE,
         CURRTYPE,
         BALANCE,
         BALANCE_CM,
         QX,
         PRODUCT_ID3,
         TA500261018,
         TA500251012,
         TA500251012_CM,
         TA290008004,
         YYBB,
         YYBB_CM,
         SYBB,
         SYBB_CM,
         ECOST,
         ECOST_CM,
         DATA_DATE)
        SELECT DW.ZONENO,
               DW.PHYBRNO,
               DW.SUBCODE,
               nvl(DW.CURRTYPE, 'null'),
               SUM(DW.ZCAMOUNT),
               0,
               nvl(DW.TERM_LEVEL_NO, 'null'),
               nvl(DW.PRODUCT_ID3, 'null'),
               nvl(DW.LEVELFIVE, '1'),
               SUM(DW.LNAMOUNT),
               0,
               nvl(DW.CREDITLEVEL, 'null'),
               SUM(DECODE(DW.LEVELFIVE,
                          1,
                          0,
                          2,
                          0.02,
                          3,
                          0.25,
                          4,
                          0.50,
                          5,
                          1.00) * DW.ZCAMOUNT),
               0,
               0,
               0,
               SUM(DW.ECOST),
               0,
               P_I_DATE
          FROM DW_MOD_GRDK_UG DW
         WHERE DW.DATA_DATE = P_I_DATE
         GROUP BY DW.ZONENO,
                  DW.PHYBRNO,
                  DW.SUBCODE,
                  nvl(DW.CURRTYPE, 'null'),
                  nvl(DW.TERM_LEVEL_NO, 'null'),
                  nvl(DW.PRODUCT_ID3, 'null'),
                  nvl(DW.LEVELFIVE, '1'),
                  nvl(DW.CREDITLEVEL, 'null');

    ELSE
      "$log"(80);INSERT INTO DW_SUM_GRDK_UG
        (ZONENO,
         ACTBRNO,
         SUBCODE,
         CURRTYPE,
         BALANCE,
         BALANCE_CM,
         QX,
         PRODUCT_ID3,
         TA500261018,
         TA500251012,
         TA500251012_CM,
         TA290008004,
         YYBB,
         YYBB_CM,
         SYBB,
         SYBB_CM,
         ECOST,
         ECOST_CM,
         DATA_DATE)
        SELECT DW.ZONENO,
               DW.PHYBRNO,
               DW.SUBCODE,
               nvl(DW.CURRTYPE, 'null'),
               SUM(DW.ZCAMOUNT),
               0,
               nvl(DW.TERM_LEVEL_NO, 'null'),
               nvl(DW.PRODUCT_ID3, 'null'),
               nvl(DW.LEVELFIVE, '1'),
               SUM(DW.LNAMOUNT),
               0,
               nvl(DW.CREDITLEVEL, 'null'),
               SUM(DECODE(DW.LEVELFIVE,
                          1,
                          0,
                          2,
                          0.02,
                          3,
                          0.25,
                          4,
                          0.50,
                          5,
                          1.00) * DW.ZCAMOUNT),
               0,
               0,
               0,
               SUM(DW.ECOST),
               0,
               P_I_DATE
          FROM DW_MOD_GRDK_UG DW
         WHERE DW.DATA_DATE >= V_FIRSTDAY
           AND DW.DATA_DATE <= P_I_DATE
         GROUP BY DW.ZONENO,
                  DW.PHYBRNO,
                  DW.SUBCODE,
                  DW.CURRTYPE,
                  DW.TERM_LEVEL_NO,
                  DW.PRODUCT_ID3,
                  DW.LEVELFIVE,
                  DW.CREDITLEVEL;
    END IF;

    --提交结果
    "$log"(81);COMMIT;
    "$log"(82);P_O_SUCCEED := '0';
    --结束，填写日志
    "$log"(83);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(84);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(85);ROLLBACK;
      "$log"(86);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(87);P_O_SUCCEED := '1';
      "$log"(88);V_ERR_CODE  := SQLCODE;
      "$log"(89);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(90);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(91);RAISE;
  END PROC_GRDK_DW;

  PROCEDURE PROC_GRDK_DM(P_I_DATE    IN VARCHAR2,
                         P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_MONTH_DAY  NUMBER(3); --当月天数
    V_FIRSTDAY   DW_MOD_YHK_UG.DATA_DATE%TYPE;
  BEGIN
    --第一步记载数据处理日志
    "$log"(92);V_STEP_NO    := '1';
    "$log"(93);V_PROC_NAME  := 'PACK_DEAL_GRDK_1210.PROC_GRDK_DM';
    "$log"(94);V_LOG_LEVEL  := '3';
    "$log"(95);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(96);V_FIRSTDAY   := SUBSTR(P_I_DATE, 1, 6) || '01';
    "$log"(97);
	SELECT TO_DATE(P_I_DATE, 'YYYYMMDD') - TO_DATE(V_FIRSTDAY, 'YYYYMMDD') + 1 INTO V_MONTH_DAY FROM DUAL;
    --第二步月初特殊处理
    "$log"(98);V_STEP_NO    := '2.1';
    "$log"(99);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --每日清理DM临时表数据
    "$log"(100);EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_ZGRDK_UG';
    --第三步每日对DW进行载入汇总全月数据并按实际天数计算日均
    "$log"(101);V_STEP_NO    := '3.1';
    "$log"(102);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(103);
	INSERT INTO DM_CS_ZGRDK_UG
      (ZONENO,
       BRNO,
       BANKFLAG,
       QX,
       PRODID,
       CURRTYPE,
       LOANSHAPE,
       CREDITRANK,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04,
       VALUE05,
       VALUE06,
       VALUE07,
       VALUE08,
       VALUE09,
       VALUE010,
       VALUE011,
       VALUE012,
       ZONENO_FOR_PART)

      SELECT /*+FULL(DW)*/
       ZONENO,
       ACTBRNO,
       '0',
       QX,
       PRODUCT_ID3,
       CURRTYPE,
       TA500261018,
       TA290008004,
       --贷款金额（合同）
       SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.TA500251012
             ELSE
              0
           END),
       --借据当前余额
       SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.BALANCE
             ELSE
              0
           END),
       --应保有拨备额
       SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.YYBB
             ELSE
              0
           END),
       -- 实际保有拨备额
       0,
       --资产净额
       SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.BALANCE - DW.YYBB
             ELSE
              0
           END),
       --经济资本占用额
       SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.ECOST
             ELSE
              0
           END),
       --贷款金额（合同）日均
       SUM(DW.TA500251012) / V_MONTH_DAY,
       --借据当前余额日均
       SUM(DW.BALANCE) / V_MONTH_DAY,
       --应保有拨备额日均
       SUM(DW.YYBB) / V_MONTH_DAY,
       --实际保有拨备额日均
       0,
       --资产净额日均
       SUM(DW.BALANCE - DW.YYBB) / V_MONTH_DAY,
       SUM(DW.ECOST) / V_MONTH_DAY,
       SUBSTR(DW.ZONENO, 1, 2)
        FROM DW_SUM_GRDK_UG DW
       GROUP BY ZONENO,
                ACTBRNO,
                QX,
                PRODUCT_ID3,
                CURRTYPE,
                TA500261018,
                TA290008004;

    --第四步折币处理 外币折美元 本外币折美元
    "$log"(104);V_STEP_NO    := '4.1';
    "$log"(105);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');

    ---------------------开始外币折美元-----------------
    "$log"(106);INSERT INTO DM_CS_ZGRDK_UG
      (ZONENO,
       BRNO,
       BANKFLAG,
       QX,
       PRODID,
       CURRTYPE,
       LOANSHAPE,
       CREDITRANK,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04,
       VALUE05,
       VALUE06,
       VALUE07,
       VALUE08,
       VALUE09,
       VALUE010,
       VALUE011,
       VALUE012,
       ZONENO_FOR_PART)
      SELECT A.ZONENO,
             A.BRNO,
             '0',
             '992',
             A.PRODID,
             A.CURRTYPE,
             A.LOANSHAPE,
             A.CREDITRANK,
             SUM(A.VALUE01 * B.USCVRATE),
             SUM(A.VALUE02 * B.USCVRATE),
             SUM(A.VALUE03 * B.USCVRATE),
             SUM(A.VALUE04 * B.USCVRATE),
             SUM(A.VALUE05 * B.USCVRATE),
             SUM(A.VALUE06 * B.USCVRATE),
             SUM(A.VALUE07 * B.USCVRATE),
             SUM(A.VALUE08 * B.USCVRATE),
             SUM(A.VALUE09 * B.USCVRATE),
             SUM(A.VALUE010 * B.USCVRATE),
             SUM(A.VALUE011 * B.USCVRATE),
             SUM(A.VALUE012 * B.USCVRATE),
             SUBSTR(A.ZONENO, 1, 2)
        FROM DM_CS_ZGRDK_UG A, PRM_CURRENCY_RATE B
       WHERE A.CURRTYPE <> '001'
         AND A.CURRTYPE = B.CURRENCY
       GROUP BY A.ZONENO,
                A.BRNO,
                A.PRODID,
                A.CURRTYPE,
                A.LOANSHAPE,
                A.CREDITRANK;

    "$log"(107);V_STEP_NO    := '4.2';
    "$log"(108);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');

    ----------------开始本外币折人民币----------------
    "$log"(109);
	INSERT INTO DM_CS_ZGRDK_UG
      (ZONENO,
       BRNO,
       BANKFLAG,
       QX,
       PRODID,
       CURRTYPE,
       LOANSHAPE,
       CREDITRANK,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04,
       VALUE05,
       VALUE06,
       VALUE07,
       VALUE08,
       VALUE09,
       VALUE010,
       VALUE011,
       VALUE012,
       ZONENO_FOR_PART)
      SELECT A.ZONENO,
             A.BRNO,
             '0',
             A.QX,
             A.PRODID,
             '994',
             A.LOANSHAPE,
             A.CREDITRANK,
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE01,
                        A.VALUE01 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE02,
                        A.VALUE02 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE03,
                        A.VALUE03 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE04,
                        A.VALUE04 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE05,
                        A.VALUE05 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE06,
                        A.VALUE06 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE07,
                        A.VALUE07 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE08,
                        A.VALUE08 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE09,
                        A.VALUE09 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE010,
                        A.VALUE010 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE011,
                        A.VALUE011 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE012,
                        A.VALUE012 / B.USCVRATE)),
             SUBSTR(A.ZONENO, 1, 2)
        FROM DM_CS_ZGRDK_UG A, PRM_CURRENCY_RATE B
       WHERE A.CURRTYPE IN ('001', '992')
         AND B.CURRENCY = '001'
       GROUP BY A.ZONENO,
                A.BRNO,
                A.QX,
                A.PRODID,
                A.CURRTYPE,
                A.LOANSHAPE,
                A.CREDITRANK;

    --第五步机构汇总 该明细最低粒度为核算网点，且一个地区只有一个
    "$log"(110);V_STEP_NO    := '4.1';
    "$log"(111);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');

    --开始汇总，核算网点起汇总，从3开始
    "$log"(112);
	FOR V_FLAG IN 2 .. 5 LOOP
      "$log"(113);
	  MERGE INTO DM_CS_ZGRDK_UG DM
      USING (SELECT PRM.BELONG_ZONENO ZONENO,
                    PRM.BELONG_BRNO BRNO,
                    PRM.BELONG_BANK_FLAG BANKFLAG,
                    DM.QX,
                    DM.PRODID,
                    DM.CURRTYPE,
                    DM.LOANSHAPE,
                    DM.CREDITRANK,
                    SUM(DM.VALUE01) VALUE01,
                    SUM(DM.VALUE02) VALUE02,
                    SUM(DM.VALUE03) VALUE03,
                    SUM(DM.VALUE04) VALUE04,
                    SUM(DM.VALUE05) VALUE05,
                    SUM(DM.VALUE06) VALUE06,
                    SUM(DM.VALUE07) VALUE07,
                    SUM(DM.VALUE08) VALUE08,
                    SUM(DM.VALUE09) VALUE09,
                    SUM(DM.VALUE010) VALUE010,
                    SUM(DM.VALUE011) VALUE011,
                    SUM(DM.VALUE012) VALUE012,
                    SUBSTR(PRM.BELONG_ZONENO, 1, 2) ZONENO_FOR_PART
               FROM DM_CS_ZGRDK_UG DM, PRM_IRA_ORGAN PRM
              WHERE DM.ZONENO = PRM.ZONE_NO
                AND DM.BRNO = PRM.BR_NO
                AND DM.BANKFLAG = PRM.BANK_FLAG
                AND PRM.BELONG_BANK_FLAG = V_FLAG
                AND PRM.BANK_FLAG <> V_FLAG
                AND P_I_DATE >= PRM.START_DATE
                AND P_I_DATE < PRM.END_DATE
              GROUP BY PRM.BELONG_ZONENO,
                       PRM.BELONG_BRNO,
                       PRM.BELONG_BANK_FLAG,
                       DM.QX,
                       DM.PRODID,
                       DM.CURRTYPE,
                       DM.LOANSHAPE,
                       DM.CREDITRANK) TMP
      ON (DM.ZONENO = TMP.ZONENO AND DM.BRNO = TMP.BRNO AND DM.BANKFLAG = TMP.BANKFLAG AND DM.QX = TMP.QX AND DM.PRODID = TMP.PRODID AND DM.CURRTYPE = TMP.CURRTYPE AND DM.LOANSHAPE = TMP.LOANSHAPE AND DM.CREDITRANK = TMP.CREDITRANK)
      WHEN MATCHED THEN
        UPDATE
           SET DM.VALUE01  = DM.VALUE01 + TMP.VALUE01,
               DM.VALUE02  = DM.VALUE02 + TMP.VALUE02,
               DM.VALUE03  = DM.VALUE03 + TMP.VALUE03,
               DM.VALUE04  = DM.VALUE04 + TMP.VALUE04,
               DM.VALUE05  = DM.VALUE05 + TMP.VALUE05,
               DM.VALUE06  = DM.VALUE06 + TMP.VALUE06,
               DM.VALUE07  = DM.VALUE07 + TMP.VALUE07,
               DM.VALUE08  = DM.VALUE08 + TMP.VALUE08,
               DM.VALUE09  = DM.VALUE09 + TMP.VALUE09,
               DM.VALUE010 = DM.VALUE010 + TMP.VALUE010,
               DM.VALUE011 = DM.VALUE011 + TMP.VALUE011,
               DM.VALUE012 = DM.VALUE012 + TMP.VALUE012
      WHEN NOT MATCHED THEN
        INSERT
          (ZONENO,
           BRNO,
           BANKFLAG,
           QX,
           PRODID,
           CURRTYPE,
           LOANSHAPE,
           CREDITRANK,
           VALUE01,
           VALUE02,
           VALUE03,
           VALUE04,
           VALUE05,
           VALUE06,
           VALUE07,
           VALUE08,
           VALUE09,
           VALUE010,
           VALUE011,
           VALUE012,
           ZONENO_FOR_PART)
        VALUES
          (TMP.ZONENO,
           TMP.BRNO,
           TMP.BANKFLAG,
           TMP.QX,
           TMP.PRODID,
           TMP.CURRTYPE,
           TMP.LOANSHAPE,
           TMP.CREDITRANK,
           TMP.VALUE01,
           TMP.VALUE02,
           TMP.VALUE03,
           TMP.VALUE04,
           TMP.VALUE05,
           TMP.VALUE06,
           TMP.VALUE07,
           TMP.VALUE08,
           TMP.VALUE09,
           TMP.VALUE010,
           TMP.VALUE011,
           TMP.VALUE012,
           SUBSTR(TMP.ZONENO, 1, 2));
    END LOOP;

    --提交结果
    "$log"(114);COMMIT;
    "$log"(115);P_O_SUCCEED := '0';

    --结束，填写日志
    "$log"(116);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(117);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(118);ROLLBACK;
      "$log"(119);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(120);P_O_SUCCEED := '1';
      "$log"(121);V_ERR_CODE  := SQLCODE;
      "$log"(122);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(123);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(124);RAISE;
  END PROC_GRDK_DM;

  FUNCTION F_GET_ZLXS(IS_DATEDATE      IN VARCHAR2,
                      IS_PRODUCT_ID    IN VARCHAR2,
                      IS_FIVLEVEL_CODE IN VARCHAR2) RETURN NUMBER IS
    VN_ZLXS PRM_GDQLTYF.VALUE1%TYPE;
  BEGIN
    VN_ZLXS := 1;
    SELECT DECODE(IS_FIVLEVEL_CODE,
                  '1',
                  VALUE1,
                  '2',
                  VALUE2,
                  '3',
                  VALUE3,
                  '4',
                  VALUE4,
                  '5',
                  VALUE5,
                  1)
      INTO VN_ZLXS
      FROM PRM_GDQLTYF PF
     WHERE PF.PRODUCT_ID = IS_PRODUCT_ID
       AND PF.START_DATE <= IS_DATEDATE
       AND PF.END_DATE > IS_DATEDATE;
    RETURN VN_ZLXS;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN VN_ZLXS;
  END F_GET_ZLXS;

  FUNCTION F_GET_BLXS(IS_DATEDATE   IN VARCHAR2,
                      IS_PRODUCT_ID IN VARCHAR2,
                      IS_RAWXS      IN NUMBER) RETURN NUMBER IS
    VN_EC_COEF DW_MOD_GRDK_UG.EC_COEF%TYPE;
  BEGIN
    VN_EC_COEF := IS_RAWXS;
    --经济资本配置系数：Max（不良个人贷款经济资本配置系数下限，Min（基准配置系数×个人贷款质量调节系数，不良个人贷款经济资本配置系数上限））
    SELECT GREATEST(PN.COEF_FLOOR, LEAST(IS_RAWXS, PN.COEF_CEIL))
      INTO VN_EC_COEF
      FROM PRM_NPLOANF PN
     WHERE PN.PRODUCT_ID = IS_PRODUCT_ID
       AND PN.START_DATE <= IS_DATEDATE
       AND PN.END_DATE > IS_DATEDATE;
    RETURN VN_EC_COEF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN VN_EC_COEF;
  END F_GET_BLXS;
END PACK_DEAL_GRDK_1210;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_NFGNLED IS

  PROCEDURE PROC_DEAL_NFGNLED(P_I_DATE    IN VARCHAR2, --输入日期
                              P_O_SUCCEED OUT VARCHAR2 --成功标志
                              );
  PROCEDURE PROC_RE_STG_NFGNLED(P_I_DATE    IN VARCHAR2, --输入日期
                                P_O_SUCCEED OUT VARCHAR2 --成功标志
                                );

  PROCEDURE PROC_DEAL_HDWCCRGN(P_I_DATE    IN VARCHAR2, --输入日期
                              P_O_SUCCEED OUT VARCHAR2 --成功标志
                              );
  PROCEDURE PROC_RE_STG_HDWCCRGN(P_I_DATE    IN VARCHAR2, --输入日期
                                P_O_SUCCEED OUT VARCHAR2 --成功标志
                                );
END PACK_DEAL_NFGNLED;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_NFGNLED IS
	"$hits" aaa_coverage_tool.bool_tab; 
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(46);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(46);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(46,x); "$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(46,x); "$times"(x):=true; 
	  end if;
	end;
   
   PROCEDURE PROC_DEAL_NFGNLED(P_I_DATE    IN VARCHAR2, --输入日期
                              P_O_SUCCEED OUT VARCHAR2 --成功标志
                              ) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT  L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO  L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT  L_GETDATA_ERR.SQL_TXT%TYPE;
    V_BANKFLAG NUMBER;
  BEGIN
    --记载数据处理日志
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PROC_DEAL_NFGNLED';
    "$log"(4);V_LOG_LEVEL  := '3'; --需要商定
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_BANKFLAG   := 2;
    --删除中间表中当日数据及以后的数据
    "$log"(7);
	DELETE FROM STG_DAT_NFGNLED_OR WHERE DATA_DATE >= P_I_DATE;
    --DELETE FROM STG_BAK_NFGNLED;
    "$log"(8);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_BAK_NFGNLED';
    "$log"(9);COMMIT;
    "$log"(10);V_STEP_NO := '2';
    --select max(DATA_DATE) into max_date from STG_DAT_NFGNLED;
    --备份将要更新的中间表
    "$log"(11);
	INSERT INTO STG_BAK_NFGNLED
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE,
       BAK_DATE)
      SELECT D.ZONENO,
             D.BRNO,
             D.BANKFLAG,
             D.CURRTYPE,
             D.SUBCODE,
             D.TDDRBAL,
             D.TDCRBAL,
             D.SUMDRBAL,
             D.SUMCRBAL,
             D.DATA_DATE,
             P_I_DATE
        FROM STG_DAT_NFGNLED_OR D, ODS_TMP_NFGNLED O
       WHERE D.ZONENO = O.ZONENO
         AND D.BRNO = O.BRNO
         AND D.CURRTYPE = O.CURRTYPE
         AND D.SUBCODE = O.SUBCODE;
    -- WHERE D.DATA_DATE = max_date;
    "$log"(12);COMMIT;

    --判断当日是否为月初日，如是则将中间表的月累计金额字段清0
    "$log"(13);
	IF P_I_DATE = SUBSTR(P_I_DATE, 1, 6) || '01' THEN
      "$log"(14);
	  UPDATE STG_DAT_NFGNLED_OR SET SUMDRBAL = 0, SUMCRBAL = 0 WHERE 1 = 1;
    END IF;
    "$log"(15);V_STEP_NO := '3.1';
    --更新中间表,
    "$log"(16);
	MERGE INTO STG_DAT_NFGNLED_OR D
    USING ODS_TMP_NFGNLED O
    ON (D.ZONENO = O.ZONENO AND D.BRNO = O.BRNO AND D.CURRTYPE = O.CURRTYPE AND D.SUBCODE = O.SUBCODE)
    WHEN MATCHED THEN
      UPDATE
         SET D.TDDRBAL   = O.TDDRBAL / 100,
             D.TDCRBAL   = O.TDCRBAL / 100,
             D.DATA_DATE = P_I_DATE
    WHEN NOT MATCHED THEN
      INSERT
        (ZONENO,
         BRNO,
         BANKFLAG,
         CURRTYPE,
         SUBCODE,
         TDDRBAL,
         TDCRBAL,
         SUMDRBAL,
         SUMCRBAL,
         DATA_DATE)
      VALUES
        (O.ZONENO,
         O.BRNO,
         '2',
         O.CURRTYPE,
         O.SUBCODE,
         O.TDDRBAL / 100,
         O.TDCRBAL / 100,
         0,
         0,
         P_I_DATE);

    "$log"(17);V_STEP_NO := '3.2';
    --计算月积数
    "$log"(18);
	UPDATE STG_DAT_NFGNLED_OR D
       SET D.SUMDRBAL = D.SUMDRBAL + D.TDDRBAL,
           D.SUMCRBAL = D.SUMCRBAL + D.TDCRBAL
     WHERE 1 = 1 ;

    "$log"(19);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_DAT_NFGNLED';

    "$log"(20);
	INSERT INTO STG_DAT_NFGNLED
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT  ZONENO,
             BRNO,
             BANKFLAG,
             CURRTYPE,
             SUBCODE,
             TDDRBAL,
             TDCRBAL,
             SUMDRBAL,
             SUMCRBAL,
             DATA_DATE
        FROM STG_DAT_NFGNLED_OR;
       --外币折美元
    "$log"(21);
	INSERT INTO STG_DAT_NFGNLED
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT T.ZONENO,
             T.BRNO,
             T.BANKFLAG,
             '992',
             T.SUBCODE,
             SUM(T.TDDRBAL * R.USCVRATE),
             SUM(T.TDCRBAL * R.USCVRATE),
             SUM(T.SUMDRBAL * R.USCVRATE),
             SUM(T.SUMCRBAL * R.USCVRATE),
             T.DATA_DATE
        FROM STG_DAT_NFGNLED T,PRM_CURRENCY_RATE R
       WHERE  T.CURRTYPE = R.CURRENCY
         AND T.CURRTYPE NOT IN ('001', '992', '994')
       GROUP BY T.ZONENO, T.BRNO, T.BANKFLAG, T.SUBCODE, T.DATA_DATE;

    --本外币合计折人民币
    "$log"(22);
	INSERT INTO STG_DAT_NFGNLED
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT T.ZONENO,
             T.BRNO,
             T.BANKFLAG,
             '994',
             T.SUBCODE,
             SUM(T.TDDRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.TDCRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.SUMDRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.SUMCRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             T.DATA_DATE
        FROM STG_DAT_NFGNLED T, PRM_CURRENCY_RATE R, PRM_CURRENCY_RATE I
       WHERE  T.CURRTYPE = R.CURRENCY
         AND T.CURRTYPE NOT IN ('992', '994')
         AND I.CURRENCY = '001'
       GROUP BY T.ZONENO, T.BRNO, T.BANKFLAG, T.SUBCODE, T.DATA_DATE;

    --机构汇总 到一级行
    "$log"(23);
	FOR V_BANKFLAG IN 2 .. 5 LOOP
      "$log"(24);
	  MERGE INTO STG_DAT_NFGNLED S
      USING (SELECT/*+use_hash(stg,prm)*/ PRM.BELONG_ZONENO,
                    PRM.BELONG_BRNO,
                    PRM.BELONG_BANK_FLAG,
                    STG.CURRTYPE,
                    SUM(TDDRBAL) TDDRBAL,
                    SUM(TDCRBAL) TDCRBAL,
                    SUM(SUMDRBAL) SUMDRBAL,
                    SUM(SUMCRBAL) SUMCRBAL,
                    STG.SUBCODE
               FROM STG_DAT_NFGNLED STG, PRM_IRA_ORGAN PRM
              WHERE PRM.ZONE_NO = STG.ZONENO
                AND PRM.BR_NO = STG.BRNO
                AND PRM.BANK_FLAG = STG.BANKFLAG
                AND PRM.START_DATE <= P_I_DATE
                AND PRM.END_DATE > P_I_DATE
                AND PRM.BELONG_BANK_FLAG = V_BANKFLAG
                AND PRM.BANK_FLAG <> V_BANKFLAG
              GROUP BY PRM.BELONG_ZONENO,
                       PRM.BELONG_BRNO,
                       PRM.BELONG_BANK_FLAG,
                       STG.CURRTYPE,
                       STG.SUBCODE) TMP
      ON (S.ZONENO = TMP.BELONG_ZONENO AND S.BRNO = TMP.BELONG_BRNO AND S.BANKFLAG = TMP.BELONG_BANK_FLAG AND S.CURRTYPE = TMP.CURRTYPE AND S.SUBCODE = TMP.SUBCODE)
      WHEN MATCHED THEN
        UPDATE
           SET S.TDDRBAL   = NVL(S.TDDRBAL, 0) + NVL(TMP.TDDRBAL, 0),
               S.TDCRBAL   = NVL(S.TDCRBAL, 0) + NVL(TMP.TDCRBAL, 0),
               S.SUMDRBAL  = NVL(S.SUMDRBAL, 0) + NVL(TMP.SUMDRBAL, 0),
               S.SUMCRBAL  = NVL(S.SUMCRBAL, 0) + NVL(TMP.SUMCRBAL, 0),
               S.DATA_DATE = P_I_DATE
      WHEN NOT MATCHED THEN
        INSERT
          (S.ZONENO,
           S.BRNO,
           S.BANKFLAG,
           S.CURRTYPE,
           S.TDDRBAL,
           S.TDCRBAL,
           S.SUMDRBAL,
           S.SUMCRBAL,
           S.SUBCODE,
           S.DATA_DATE)
        VALUES
          (TMP.BELONG_ZONENO,
           TMP.BELONG_BRNO,
           TMP.BELONG_BANK_FLAG,
           TMP.CURRTYPE,
           TMP.TDDRBAL,
           TMP.TDCRBAL,
           TMP.SUMDRBAL,
           TMP.SUMCRBAL,
           TMP.SUBCODE,
           P_I_DATE);
    END LOOP;
    --2002版本增加
    "$log"(25);
	--PROC_DEAL_HDWCCRGN(P_I_DATE => P_I_DATE, P_O_SUCCEED => P_O_SUCCEED);
    --数据处理成功
    "$log"(26);P_O_SUCCEED := '0';
    "$log"(27);COMMIT;
    --记载数据处理日志
    "$log"(28);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(29);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(30);BEGIN
        "$log"(31);ROLLBACK;
        "$log"(32);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        "$log"(33);P_O_SUCCEED := '1';
        "$log"(34);V_ERR_CODE  := SQLCODE;
        "$log"(35);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
        "$log"(36);
		--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
        "$log"(37);RAISE;
      END;
  END PROC_DEAL_NFGNLED;

  PROCEDURE PROC_RE_STG_NFGNLED(P_I_DATE    IN VARCHAR2, --输入日期
                                P_O_SUCCEED OUT VARCHAR2 --成功标志
                                ) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT  L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO  L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT  L_GETDATA_ERR.SQL_TXT%TYPE;

  BEGIN
    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PROC_RE_STG_NFGNLED';
    V_LOG_LEVEL  := '3'; --需要商定
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    DELETE FROM STG_DAT_NFGNLED_OR D WHERE D.DATA_DATE = P_I_DATE;
    DELETE FROM STG_DAT_NFGNLED_OR D
     WHERE EXISTS (SELECT 1
              FROM STG_BAK_NFGNLED B
             WHERE D.ZONENO = B.ZONENO
               AND D.BRNO = B.BRNO
               AND D.CURRTYPE = B.CURRTYPE
               AND D.SUBCODE = B.SUBCODE
               AND D.DATA_DATE = B.DATA_DATE);
    COMMIT;
    --从备份表恢复中间表当日备份的数据
    INSERT INTO STG_DAT_NFGNLED_OR
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT ZONENO,
             BRNO,
             BANKFLAG,
             CURRTYPE,
             SUBCODE,
             TDDRBAL,
             TDCRBAL,
             SUMDRBAL,
             SUMCRBAL,
             DATA_DATE
        FROM STG_BAK_NFGNLED;
    COMMIT;
    --2002版本增加
    --PROC_RE_STG_HDWCCRGN(P_I_DATE => P_I_DATE, P_O_SUCCEED => P_O_SUCCEED);
    --数据处理成功
    P_O_SUCCEED := '0';
    COMMIT;
    --记载数据处理日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);

  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        V_ERR_CODE  := SQLCODE;
        V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
        --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
        RAISE;
      END;
  END PROC_RE_STG_NFGNLED;

  PROCEDURE PROC_DEAL_HDWCCRGN(P_I_DATE    IN VARCHAR2, --输入日期
                              P_O_SUCCEED OUT VARCHAR2 --成功标志
                              ) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT  L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO  L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT  L_GETDATA_ERR.SQL_TXT%TYPE;
    V_BANKFLAG NUMBER;
  BEGIN
    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PROC_DEAL_HDWCCRGN';
    V_LOG_LEVEL  := '3'; --需要商定
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_BANKFLAG   := 2;
    --删除中间表(此表只有当日数据)中数据及以后的数据
    --删除备份表的当日数据, 需重新计算,重新备份.
    DELETE FROM STG_BAK_HDWCCRGN WHERE DATA_DATE >= P_I_DATE;
    EXECUTE IMMEDIATE 'TRUNCATE  TABLE STG_DAT_HDWCCRGN';
    COMMIT;
    V_STEP_NO := '2';
    --将昨日全量备份数据导入stg_dat
    INSERT INTO STG_DAT_HDWCCRGN
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       LOGNO,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT D.ZONENO,
             D.BRNO,
             D.BANKFLAG,
             D.CURRTYPE,
             D.SUBCODE,
             D.LOGNO,
             D.TDDRBAL,
             D.TDCRBAL,
             D.SUMDRBAL,
             D.SUMCRBAL,
             P_I_DATE
        FROM STG_BAK_HDWCCRGN D
       WHERE DATA_DATE=TO_CHAR(TO_DATE(P_I_DATE,'YYYYMMDD')-1,'YYYYMMDD');
    COMMIT;
    --判断当日是否为月初日，如是则将中间表的月累计金额字段清0
    IF P_I_DATE = SUBSTR(P_I_DATE, 1, 6) || '01' THEN
      UPDATE STG_DAT_HDWCCRGN SET SUMDRBAL = 0, SUMCRBAL = 0 WHERE 1 = 1;
    END IF;

    V_STEP_NO := '3.1';
    --用今日增量数据,恢复今日全量,更新中间表,
    --减小表的大小, 只处理618199 cap2002版本 k20191952
    MERGE INTO STG_DAT_HDWCCRGN D
    USING (SELECT O.ZONENO,
                  O.BRNO,
                  PRM.BANK_FLAG BANKFLAG,
                  O.CURRTYPE,
                  O.SUBCODE,
                  O.LOGNO,
                  O.TDDRBAL,
                  O.TDCRBAL
             FROM ODS_TMP_HDWCCRGN O
            INNER JOIN PRM_IRA_ORGAN PRM
               ON O.ZONENO = PRM.ZONE_NO
              AND O.BRNO = PRM.BR_NO
              AND PRM.START_DATE <= P_I_DATE
              AND PRM.END_DATE > P_I_DATE
            WHERE SUBCODE = '618199') O
    ON (D.ZONENO = O.ZONENO AND D.BRNO = O.BRNO AND D.BANKFLAG = O.BANKFLAG AND D.CURRTYPE = O.CURRTYPE AND D.SUBCODE = O.SUBCODE AND D.LOGNO = O.LOGNO)
    WHEN MATCHED THEN
      UPDATE SET D.TDDRBAL = O.TDDRBAL / 100, D.TDCRBAL = O.TDCRBAL / 100
    WHEN NOT MATCHED THEN
      INSERT
        (ZONENO,
         BRNO,
         BANKFLAG,
         CURRTYPE,
         SUBCODE,
         LOGNO,
         TDDRBAL,
         TDCRBAL,
         SUMDRBAL,
         SUMCRBAL,
         DATA_DATE)
      VALUES
        (O.ZONENO,
         O.BRNO,
         O.BANKFLAG,
         O.CURRTYPE,
         O.SUBCODE,
         O.LOGNO,
         O.TDDRBAL / 100,
         O.TDCRBAL / 100,
         0,
         0,
         P_I_DATE);

    V_STEP_NO := '3.2';
    --计算月积数
    UPDATE STG_DAT_HDWCCRGN D
       SET D.SUMDRBAL = D.SUMDRBAL + D.TDDRBAL,
           D.SUMCRBAL = D.SUMCRBAL + D.TDCRBAL
     WHERE 1 = 1 ;

    --此时,将stg表备份,备份表中只保留原始数据的全量还原和月累计的重算.
    INSERT INTO STG_BAK_HDWCCRGN(ZONENO, BRNO, BANKFLAG, CURRTYPE, SUBCODE, LOGNO, TDDRBAL, TDCRBAL, SUMDRBAL, SUMCRBAL, DATA_DATE)
    SELECT ZONENO, BRNO, BANKFLAG, CURRTYPE, SUBCODE, LOGNO, TDDRBAL, TDCRBAL, SUMDRBAL, SUMCRBAL, DATA_DATE FROM STG_DAT_HDWCCRGN;
    COMMIT;
    
    --清理备份表,保留一年的数据
    DELETE FROM STG_BAK_HDWCCRGN 
    WHERE DATA_DATE < TO_CHAR(TO_DATE(P_I_DATE,'YYYYMMDD')-380,'YYYYMMDD');
    COMMIT;

       --外币折美元
    INSERT INTO STG_DAT_HDWCCRGN
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       LOGNO,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT T.ZONENO,
             T.BRNO,
             T.BANKFLAG,
             '992',
             T.SUBCODE,
             T.LOGNO,
             SUM(T.TDDRBAL * R.USCVRATE),
             SUM(T.TDCRBAL * R.USCVRATE),
             SUM(T.SUMDRBAL * R.USCVRATE),
             SUM(T.SUMCRBAL * R.USCVRATE),
             T.DATA_DATE
        FROM STG_DAT_HDWCCRGN T,PRM_CURRENCY_RATE R
       WHERE  T.CURRTYPE = R.CURRENCY
         AND T.CURRTYPE NOT IN ('001', '992', '994')
       GROUP BY T.ZONENO, T.BRNO, T.BANKFLAG, T.SUBCODE, T.LOGNO, T.DATA_DATE;

    --本外币合计折人民币
    INSERT INTO STG_DAT_HDWCCRGN
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       LOGNO,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT T.ZONENO,
             T.BRNO,
             T.BANKFLAG,
             '994',
             T.SUBCODE,
             T.LOGNO,
             SUM(T.TDDRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.TDCRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.SUMDRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.SUMCRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             T.DATA_DATE
        FROM STG_DAT_HDWCCRGN T, PRM_CURRENCY_RATE R, PRM_CURRENCY_RATE I
       WHERE  T.CURRTYPE = R.CURRENCY
         AND T.CURRTYPE NOT IN ('992', '994')
         AND I.CURRENCY = '001'
       GROUP BY T.ZONENO, T.BRNO, T.BANKFLAG, T.SUBCODE, T.LOGNO, T.DATA_DATE;

    --机构汇总 到一级行
    FOR V_BANKFLAG IN 4 .. 5 LOOP
      MERGE INTO STG_DAT_HDWCCRGN S
      USING (SELECT/*+use_hash(stg,prm)*/ PRM.BELONG_ZONENO,
                    PRM.BELONG_BRNO,
                    PRM.BELONG_BANK_FLAG,
                    STG.CURRTYPE,
                    SUM(TDDRBAL) TDDRBAL,
                    SUM(TDCRBAL) TDCRBAL,
                    SUM(SUMDRBAL) SUMDRBAL,
                    SUM(SUMCRBAL) SUMCRBAL,
                    STG.SUBCODE,
                    STG.LOGNO
               FROM STG_DAT_HDWCCRGN STG, PRM_IRA_ORGAN PRM
              WHERE PRM.ZONE_NO = STG.ZONENO
                AND PRM.BR_NO = STG.BRNO
                AND PRM.BANK_FLAG = STG.BANKFLAG
                AND PRM.START_DATE <= P_I_DATE
                AND PRM.END_DATE > P_I_DATE
                AND PRM.BELONG_BANK_FLAG = V_BANKFLAG
                AND PRM.BANK_FLAG <> V_BANKFLAG
              GROUP BY PRM.BELONG_ZONENO,
                       PRM.BELONG_BRNO,
                       PRM.BELONG_BANK_FLAG,
                       STG.CURRTYPE,
                       STG.SUBCODE,
                       STG.LOGNO) TMP
      ON (S.ZONENO = TMP.BELONG_ZONENO AND S.BRNO = TMP.BELONG_BRNO AND S.BANKFLAG = TMP.BELONG_BANK_FLAG AND S.CURRTYPE = TMP.CURRTYPE AND S.SUBCODE = TMP.SUBCODE AND S.LOGNO = TMP.LOGNO)
      WHEN MATCHED THEN
        UPDATE
           SET S.TDDRBAL   = NVL(S.TDDRBAL, 0) + NVL(TMP.TDDRBAL, 0),
               S.TDCRBAL   = NVL(S.TDCRBAL, 0) + NVL(TMP.TDCRBAL, 0),
               S.SUMDRBAL  = NVL(S.SUMDRBAL, 0) + NVL(TMP.SUMDRBAL, 0),
               S.SUMCRBAL  = NVL(S.SUMCRBAL, 0) + NVL(TMP.SUMCRBAL, 0),
               S.DATA_DATE = P_I_DATE
      WHEN NOT MATCHED THEN
        INSERT
          (S.ZONENO,
           S.BRNO,
           S.BANKFLAG,
           S.CURRTYPE,
           S.TDDRBAL,
           S.TDCRBAL,
           S.SUMDRBAL,
           S.SUMCRBAL,
           S.SUBCODE,
           S.LOGNO,
           S.DATA_DATE)
        VALUES
          (TMP.BELONG_ZONENO,
           TMP.BELONG_BRNO,
           TMP.BELONG_BANK_FLAG,
           TMP.CURRTYPE,
           TMP.TDDRBAL,
           TMP.TDCRBAL,
           TMP.SUMDRBAL,
           TMP.SUMCRBAL,
           TMP.SUBCODE,
           TMP.LOGNO,
           P_I_DATE);
       COMMIT;
    END LOOP;
    --数据处理成功
    P_O_SUCCEED := '0';
    COMMIT;
    --记载数据处理日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        V_ERR_CODE  := SQLCODE;
        V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
        --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
        RAISE;
      END;
  END PROC_DEAL_HDWCCRGN;

  PROCEDURE PROC_RE_STG_HDWCCRGN(P_I_DATE    IN VARCHAR2, --输入日期
                                P_O_SUCCEED OUT VARCHAR2 --成功标志
                                ) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT  L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO  L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT  L_GETDATA_ERR.SQL_TXT%TYPE;

  BEGIN
    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PROC_RE_STG_HDWCCRGN';
    V_LOG_LEVEL  := '3'; --需要商定
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');

    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_DAT_HDWCCRGN' ;

    COMMIT;
    --从备份表恢复中间表当日备份的数据
    INSERT INTO STG_DAT_HDWCCRGN
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       SUBCODE,
       LOGNO,
       TDDRBAL,
       TDCRBAL,
       SUMDRBAL,
       SUMCRBAL,
       DATA_DATE)
      SELECT ZONENO,
             BRNO,
             BANKFLAG,
             CURRTYPE,
             SUBCODE,
             LOGNO,
             TDDRBAL,
             TDCRBAL,
             SUMDRBAL,
             SUMCRBAL,
             DATA_DATE
        FROM STG_BAK_HDWCCRGN
       WHERE DATA_DATE=P_I_DATE;
    COMMIT;
    --数据处理成功
    P_O_SUCCEED := '0';
    COMMIT;
    --记载数据处理日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);

  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        V_ERR_CODE  := SQLCODE;
        V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
        --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
        RAISE;
      END;
  END PROC_RE_STG_HDWCCRGN;
END PACK_DEAL_NFGNLED;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_QTFX_1005 IS
  PROCEDURE PROC_DEAL_QTFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_QTFX_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  FUNCTION FUNC_PRM_PRODUCT(I_DATEDATE  IN VARCHAR2,
                            IN_FUNDTYPE IN VARCHAR2) RETURN VARCHAR2;
END PACK_DEAL_QTFX_1005;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_QTFX_1005 IS
	"$hits" aaa_coverage_tool.bool_tab;
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id
		   and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate);
		commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(1064);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(1064);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(1064,x); 
		"$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(1064,x); 
		"$times"(x):=true; 
	  end if;
	end;
	
   PROCEDURE PROC_DEAL_QTFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_VNUM       NUMBER(3);
  BEGIN
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_QTFX';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    "$log"(8);
	SELECT ceil(to_date(P_I_DATE, 'YYYYMMDD') - trunc(to_date(P_I_DATE, 'YYYYMMDD'), 'yyyy'))+1 INTO V_VNUM FROM DUAL;
    "$log"(9);V_STEP_NO := '2.1';
    "$log"(10);
	EXECUTE IMMEDIATE 'truncate table STG_DAT_QTFX';
    "$log"(11);
	INSERT INTO STG_DAT_QTFX
      (ZONENO,
       BRNO,
       CURRTYPE,
       SUBCODE,
       BALANCE,
       BALANCE_CM,
       BAL_AVEACM,
       FUNDKIND,
       FUNDTYPE,
       PRODUCT,
       DATA_DATE,
       baltype
      )
      SELECT  ods.zoneno,
             ODS.ACTBRNO,
             ODS.CURRTYPE,
             ODS.SUBCODE,
             ODS.BALANCE/100,
             ODS.ACM/100,
             ODS.AVEACM/100,
             ODS.FUNDKIND,
             ODS.FUNDTYPE,
             FUNC_PRM_PRODUCT(P_I_DATE, ODS.FUNDTYPE),
             P_I_DATE,
             ods.baltype
        FROM ODS_TMP_KFFNBAL ODS
       WHERE ODS.FUNDKIND < '070'
         AND ODS.FUNDTYPE NOT IN ( '0000000','0054001','0054002')
         AND ODS.SUBCODE <> '0'
         AND ods.baltype='2';
    "$log"(12);
	INSERT INTO STG_DAT_QTFX
      (ZONENO,
       BRNO,
       CURRTYPE,
       SUBCODE,
       BALANCE,
       BALANCE_CM,
       BAL_AVEACM,
       FUNDKIND,
       FUNDTYPE,
       PRODUCT,
       DATA_DATE,
       baltype)
      SELECT  ods.zoneno,
             ODS.ACTBRNO,
             ODS.CURRTYPE,
             ODS.SUBCODE,
             ODS.BALANCE/100,
             ODS.ACM/100,
             ODS.AVEACM/100,
             ODS.FUNDKIND,
             ODS.FUNDTYPE,
             FUNC_PRM_PRODUCT(P_I_DATE, ODS.FUNDTYPE),
             P_I_DATE,
             ods.baltype
        FROM ODS_TMP_KFFNBAL ODS
       WHERE ODS.FUNDKIND < '070'
         AND ODS.FUNDTYPE  IN ('0054001','0054002')
         AND ODS.SUBCODE <> '0';
    "$log"(13);
	COMMIT;
    "$log"(14);V_STEP_NO := '2.2';
    "$log"(15);
	DELETE FROM DW_MOD_QTFX DW WHERE DW.DATA_DATE >= P_I_DATE;
    "$log"(16);
	INSERT INTO DW_MOD_QTFX
      (ZONENO,
       BRNO,
       CURRTYPE,
       SUBCODE,
       BALANCE,
       BALANCE_CM,
       BAL_AVEACM,
       DATA_DATE,
       PRODUCT,
       XSQT1,
       EC,
       EC_CM,
       EC_AVEACM)
      SELECT STG.ZONENO,
             STG.BRNO,
             STG.CURRTYPE,
             STG.SUBCODE,
             SUM(STG.BALANCE * PRC.SUBJECT_APP) VALUE01,
             SUM(STG.BALANCE_CM * PRC.SUBJECT_APP) VALUE02,
             SUM(STG.BAL_AVEACM * PRC.SUBJECT_APP/V_VNUM) VALUE05,
             P_I_DATE,
             STG.PRODUCT,
             PRX.QUOTIETY,
             SUM(STG.BALANCE * PRX.QUOTIETY * PRC.SUBJECT_APP) VALUE03,
             SUM(STG.BALANCE_CM * PRX.QUOTIETY * PRC.SUBJECT_APP) VALUE04,
             SUM(STG.BAL_AVEACM * PRX.QUOTIETY * PRC.SUBJECT_APP/V_VNUM) VALUE06
        FROM STG_DAT_QTFX STG, PRM_CS10 PRC, PRM_XS10 PRX
       WHERE decode(prc.subject_type,'1',STG.Fundkind,stg.fundtype) = PRC.Subject_Code
         AND PRC.START_DATE <= P_I_DATE
         AND PRC.END_DATE > P_I_DATE
         AND PRX.START_DATE(+) <= P_I_DATE
         AND PRX.END_DATE(+) > P_I_DATE
         AND PRC.PRODUCT_ID = PRX.THPRODUCT(+)
       GROUP BY STG.ZONENO,
                STG.BRNO,
                STG.CURRTYPE,
                PRX.QUOTIETY,
                STG.SUBCODE,
                STG.PRODUCT;
    "$log"(17);COMMIT;
    "$log"(18);P_O_SUCCEED := '0';
    "$log"(19);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(20);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME,V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(21);ROLLBACK;
      "$log"(22);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(23);P_O_SUCCEED := '1';
      "$log"(24);V_ERR_CODE  := SQLCODE;
      "$log"(25);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(26);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(27);RAISE;
  END PROC_DEAL_QTFX;
  
  PROCEDURE PROC_QTFX_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_FLAG NUMBER(1) := 2; --行级别
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(28);V_STEP_NO    := '1';
    "$log"(29);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_QTFX_DM';
    "$log"(30);V_LOG_LEVEL  := '3';
    "$log"(31);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(32);V_STEP_NO    := '2.1';
    "$log"(33);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --开始删除DM表中数据
    "$log"(34);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_QTFX';
    --把其他风险数据插入dm表，并在此基础上进行汇总和折币
    "$log"(35);
	INSERT INTO DM_CS_QTFX
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04,
       VALUE05,
       VALUE06)
      SELECT DW.ZONENO,
             DW.BRNO,
             '0',
             DW.CURRTYPE,
             DW.PRODUCT,
             SUM(DW.BALANCE),
             SUM(DW.EC),
             SUM(DW.Balance_Cm),
             SUM(DW.EC_CM),
             SUM(DW.BAL_AVEACM),
             SUM(DW.EC_AVEACM)
        FROM DW_MOD_QTFX DW
       WHERE DW.DATA_DATE = P_I_DATE
       GROUP BY DW.ZONENO, DW.BRNO, DW.CURRTYPE, DW.PRODUCT;
    ---------------------开始外币折美元-----------------
    "$log"(36);
	INSERT INTO DM_CS_QTFX
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04,
       VALUE05,
       VALUE06)
      SELECT A.ZONENO,
             A.BRNO,
             A.BANKFLAG,
             '992',
             A.PRODID,
             SUM(A.VALUE01 * B.USCVRATE),
             SUM(A.VALUE02 * B.USCVRATE),
             SUM(A.VALUE03 * B.USCVRATE),
             SUM(A.VALUE04 * B.USCVRATE),
             SUM(A.VALUE05 * B.USCVRATE),
             SUM(A.VALUE06 * B.USCVRATE)
        FROM DM_CS_QTFX A, PRM_EXCHANGE_RATE B
       WHERE A.CURRTYPE <> '001'
         AND A.ZONENO = B.ZONE_NO
         AND A.CURRTYPE = B.CURRENCY
         AND P_I_DATE >= B.START_DATE
         AND P_I_DATE < B.END_DATE
       GROUP BY ZONENO, BRNO, BANKFLAG, PRODID;
    ----------------开始本外币折人民币----------------
    "$log"(37);
	INSERT INTO DM_CS_QTFX
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04,
       VALUE05,
       VALUE06)
      SELECT A.ZONENO,
             A.BRNO,
             A.BANKFLAG,
             '994',
             A.PRODID,
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE01,
                        A.VALUE01 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE02,
                        A.VALUE02 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE03,
                        A.VALUE03 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE04,
                        A.VALUE04 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE05,
                        A.VALUE05 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE06,
                        A.VALUE06 / B.USCVRATE))
        FROM DM_CS_QTFX A, PRM_EXCHANGE_RATE B
       WHERE A.CURRTYPE IN ('001', '992')
         AND A.ZONENO = B.ZONE_NO
         AND B.CURRENCY = '001'
         AND P_I_DATE >= B.START_DATE
         AND P_I_DATE < B.END_DATE
       GROUP BY ZONENO, BRNO, BANKFLAG, PRODID;
    --开始汇总
    "$log"(38);
	FOR V_FLAG IN 2 .. 5 LOOP
      "$log"(39);
	  MERGE INTO DM_CS_QTFX DM
      USING (SELECT PRM.BELONG_ZONENO ZONENO,
                    PRM.BELONG_BRNO BRNO,
                    PRM.BELONG_BANK_FLAG BANKFLAG,
                    DM.PRODID,
                    DM.CURRTYPE,
                    SUM(DM.VALUE01) VALUE01,
                    SUM(DM.VALUE02) VALUE02,
                    SUM(DM.VALUE03) VALUE03,
                    SUM(DM.VALUE04) VALUE04,
                    SUM(DM.VALUE05) VALUE05,
                    SUM(DM.VALUE06) VALUE06
               FROM DM_CS_QTFX DM, PRM_IRA_ORGAN PRM
              WHERE DM.ZONENO = PRM.ZONE_NO
                AND DM.BRNO = PRM.BR_NO
                AND DM.BANKFLAG = PRM.BANK_FLAG
                AND PRM.BELONG_BANK_FLAG = V_FLAG
                AND PRM.BANK_FLAG <> V_FLAG
                AND P_I_DATE >= PRM.START_DATE
                AND P_I_DATE < PRM.END_DATE
              GROUP BY PRM.BELONG_ZONENO,
                       PRM.BELONG_BRNO,
                       PRM.BELONG_BANK_FLAG,
                       DM.PRODID,
                       DM.CURRTYPE) TMP
      ON (DM.ZONENO = TMP.ZONENO AND DM.BRNO = TMP.BRNO AND DM.BANKFLAG = TMP.BANKFLAG AND DM.PRODID = TMP.PRODID AND DM.CURRTYPE = TMP.CURRTYPE)
      WHEN MATCHED THEN
        UPDATE
           SET DM.VALUE01 = DM.VALUE01 + TMP.VALUE01,
               DM.VALUE02 = DM.VALUE02 + TMP.VALUE02,
               DM.VALUE03 = DM.VALUE03 + TMP.VALUE03,
               DM.VALUE04 = DM.VALUE04 + TMP.VALUE04,
               DM.VALUE05 = DM.VALUE05 + TMP.VALUE05,
               DM.VALUE06 = DM.VALUE06 + TMP.VALUE06
      WHEN NOT MATCHED THEN
        INSERT
          (DM.ZONENO,
           DM.BRNO,
           DM.BANKFLAG,
           DM.PRODID,
           DM.CURRTYPE,
           DM.VALUE01,
           DM.VALUE02,
           DM.VALUE03,
           DM.VALUE04,
           DM.VALUE05,
           DM.VALUE06)
        VALUES
          (TMP.ZONENO,
           TMP.BRNO,
           TMP.BANKFLAG,
           TMP.PRODID,
           TMP.CURRTYPE,
           TMP.VALUE01,
           TMP.VALUE02,
           TMP.VALUE03,
           TMP.VALUE04,
           TMP.VALUE05,
           TMP.VALUE06);
    END LOOP;
    --EC为负置0
    "$log"(40);
	UPDATE DM_CS_QTFX SET VALUE02 = greatest(VALUE02,0), VALUE04 = greatest(VALUE04,0), VALUE06 = greatest(VALUE06,0);
    "$log"(41);
	COMMIT;
    "$log"(42);
	P_O_SUCCEED := '0';
    "$log"(43);
	V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(44);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(45);ROLLBACK;
      "$log"(46);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(47);P_O_SUCCEED := '1';
      "$log"(48);V_ERR_CODE  := SQLCODE;
      "$log"(49);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(50);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(51);RAISE;
  END PROC_QTFX_DM;

  FUNCTION FUNC_PRM_PRODUCT(I_DATEDATE  IN VARCHAR2,
                            IN_FUNDTYPE IN VARCHAR2) RETURN VARCHAR2 IS
    N_PRODUCT PRM_CS10.PRODUCT_ID%TYPE; --特殊产品的资本系数
  BEGIN
    BEGIN
      SELECT T.PRODUCT_ID
        INTO N_PRODUCT
        FROM PRM_CS10 T
       WHERE T.SUBJECT_CODE = IN_FUNDTYPE
         AND T.START_DATE <= I_DATEDATE
         AND T.END_DATE > I_DATEDATE
         AND T.SUBJECT_APP = 1;
      RETURN N_PRODUCT;
    EXCEPTION
      WHEN OTHERS THEN
        SELECT T.PRODUCT_ID
          INTO N_PRODUCT
          FROM PRM_CS10 T
         WHERE T.SUBJECT_CODE = SUBSTR(IN_FUNDTYPE, 2, 3)
           AND T.START_DATE <= I_DATEDATE
           AND T.END_DATE > I_DATEDATE
           AND T.SUBJECT_APP = 1;
        RETURN N_PRODUCT;
    END;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
END PACK_DEAL_QTFX_1005;
//


CREATE OR REPLACE PACKAGE      PACK_DEAL_WXZC_1005 IS
  PROCEDURE PROC_DEAL_WXZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_WXZC_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_WXZC_1005;
//

CREATE OR REPLACE PACKAGE BODY      PACK_DEAL_WXZC_1005 IS
	"$hits" aaa_coverage_tool.bool_tab;
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id
		   and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate);
		commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(1068);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(1068);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(1068,x); 
		"$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(1068,x); 
		"$times"(x):=true; 
	  end if;
	end;
	
   PROCEDURE PROC_DEAL_WXZC(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_XS  PRM_XS081.QUOTIETY%TYPE;
    V_DAY NUMBER;
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    --记载数据处理日志
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_WXZC';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --读取最大系数
    "$log"(8);V_STEP_NO := '2.1';
    "$log"(9);
	SELECT MAX(FXXS) INTO V_XS FROM PRM_XS053 T WHERE P_I_DATE >= T.START_DATE AND P_I_DATE < T.END_DATE;
    --读取该月的天数
    "$log"(10);
	SELECT EXTRACT(DAY FROM LAST_DAY(TO_DATE(P_I_DATE, 'yyyymmdd'))) INTO V_DAY FROM DUAL;
    --回退操作，删除当天数据
    "$log"(11);
	DELETE FROM DW_MOD_WXZCZJB WHERE DATA_DATE = P_I_DATE;
    --开始计算银行卡类经济资本
    "$log"(12);V_STEP_NO := '2.2';
    "$log"(13);
	INSERT INTO DW_MOD_WXZCZJB
      (ZONENO,
       BRNO,
       DATA_DATE,
       CURRTYPE,
       SJCP,
       SJCPJE,
       FPXS,
       JJZBZYE,
       SUMSJCPJE,
       SUMJJZBZYE,
       BANKFLAG)
      SELECT ZONENO,
             BRNO,
             P_I_DATE,
             CURRTYPE,
             PRODUCT_ID,
             SUM(VALUE01),
             VALUE_FXX,
             SUM(VALUE02),
             SUM(VALUE03),
             SUM(VALUE04),
             BANKFLAG
        FROM (SELECT A.ZONENO,
                     A.BRNO,
                     A.CURRTYPE,
                     B.PRODUCT_ID,
                     DECODE(SUBSTR(SUBSTR(A.SUBCODE, 1, 4), 1, 1),
                            '8',
                            SUM(A.TDCRBAL * B.SUBJECT_APP),
                            '2',
                            SUM(A.TDCRBAL * B.SUBJECT_APP),
                            '4',
                            DECODE(SIGN(SUM(A.TDDRBAL * B.SUBJECT_APP) -
                                        SUM(A.TDCRBAL * B.SUBJECT_APP)),
                                   -1,
                                   0,
                                   (SUM(A.TDDRBAL * B.SUBJECT_APP) -
                                   SUM(A.TDCRBAL * B.SUBJECT_APP))),
                            SUM(A.TDDRBAL * B.SUBJECT_APP)) VALUE01,
                     NVL(C.FXXS, V_XS) VALUE_FXX,
                     DECODE(SUBSTR(SUBSTR(A.SUBCODE, 1, 4), 1, 1),
                            '8',
                            SUM(A.TDCRBAL * NVL(C.FXXS, V_XS) * B.SUBJECT_APP),
                            '2',
                            SUM(A.TDCRBAL * NVL(C.FXXS, V_XS) * B.SUBJECT_APP),
                            '4',
                            DECODE(SIGN(SUM(A.TDDRBAL * NVL(C.FXXS, V_XS) *
                                            B.SUBJECT_APP) -
                                        SUM(A.TDCRBAL * NVL(C.FXXS, V_XS) *
                                            B.SUBJECT_APP)),
                                   -1,
                                   0,
                                   SUM(A.TDDRBAL * NVL(C.FXXS, V_XS) *
                                       B.SUBJECT_APP) -
                                   SUM(A.TDCRBAL * NVL(C.FXXS, V_XS) *
                                       B.SUBJECT_APP)),
                            SUM(A.TDDRBAL * NVL(C.FXXS, V_XS) * B.SUBJECT_APP)) VALUE02,
                     DECODE(SUBSTR(SUBSTR(A.SUBCODE, 1, 4), 1, 1),
                            '8',
                            SUM(A.SUMCRBAL * B.SUBJECT_APP / V_DAY),
                            '2',
                            SUM(A.SUMCRBAL * B.SUBJECT_APP / V_DAY),
                            '4',
                            DECODE(SIGN(SUM(A.SUMDRBAL * B.SUBJECT_APP /
                                            V_DAY) -
                                        SUM(A.SUMCRBAL * B.SUBJECT_APP /
                                            V_DAY)),
                                   -1,
                                   0,
                                   SUM(A.SUMDRBAL * B.SUBJECT_APP / V_DAY) -
                                   SUM(A.SUMCRBAL * B.SUBJECT_APP / V_DAY)),
                            SUM(A.SUMDRBAL * B.SUBJECT_APP / V_DAY)) VALUE03,
                     DECODE(SUBSTR(SUBSTR(A.SUBCODE, 1, 4), 1, 1),
                            '8',
                            SUM(A.SUMCRBAL * B.SUBJECT_APP / V_DAY *
                                NVL(C.FXXS, V_XS)),
                            '2',
                            SUM(A.SUMCRBAL * B.SUBJECT_APP / V_DAY *
                                NVL(C.FXXS, V_XS)),
                            '4',
                            DECODE(SIGN(SUM(A.SUMDRBAL * B.SUBJECT_APP /
                                            V_DAY * NVL(C.FXXS, V_XS)) -
                                        SUM(A.SUMCRBAL * B.SUBJECT_APP /
                                            V_DAY * NVL(C.FXXS, V_XS))),
                                   -1,
                                   0,
                                   SUM(A.SUMDRBAL * B.SUBJECT_APP / V_DAY *
                                       NVL(C.FXXS, V_XS)) -
                                   SUM(A.SUMCRBAL * B.SUBJECT_APP / V_DAY *
                                       NVL(C.FXXS, V_XS))),
                            SUM(A.SUMDRBAL * B.SUBJECT_APP / V_DAY *
                                NVL(C.FXXS, V_XS))) VALUE04,
                     A.BANKFLAG
                FROM STG_DAT_NFGNLED A, PRM_CS05B B, PRM_XS053 C
               WHERE DECODE(B.SUBJECT_TYPE,
                            1,
                            SUBSTR(A.SUBCODE, 1, 4),
                            A.SUBCODE) = B.SUBJECT_CODE
                 AND B.PRODUCT_ID = C.PRODUCT_ID(+)
                 AND P_I_DATE >= B.START_DATE
                 AND P_I_DATE < B.END_DATE
                 AND P_I_DATE >= C.START_DATE(+)
                 AND P_I_DATE < C.END_DATE(+)
               GROUP BY A.ZONENO,
                        A.BRNO,
                        A.CURRTYPE,
                        B.PRODUCT_ID,
                        NVL(C.FXXS, V_XS),
                        SUBSTR(A.SUBCODE, 1, 4),
                        A.BANKFLAG)
       GROUP BY ZONENO, BRNO, CURRTYPE, PRODUCT_ID, BANKFLAG, VALUE_FXX;
    "$log"(14);COMMIT;
    "$log"(15);
	P_O_SUCCEED := '0';
    "$log"(16);
	V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(17);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(18);ROLLBACK;
      "$log"(19);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(20);P_O_SUCCEED := '1';
      "$log"(21);V_ERR_CODE  := SQLCODE;
      "$log"(22);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(23);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(24);RAISE;
  END PROC_DEAL_WXZC;
  
  PROCEDURE PROC_WXZC_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    --记载数据处理日志
    "$log"(25);V_STEP_NO    := '1';
    "$log"(26);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_WXZC_DM';
    "$log"(27);V_LOG_LEVEL  := '3';
    "$log"(28);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(29);V_STEP_NO    := '2.1';
    "$log"(30);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --开始删除DM表中数据
    "$log"(31);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_ZWXZC';
    --数据写入DM表
    "$log"(32);
	INSERT INTO DM_CS_ZWXZC
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT DW.ZONENO,
             DW.BRNO,
             DW.BANKFLAG,
             DW.CURRTYPE,
             DW.SJCP,
             SUM(DW.SJCPJE),
             SUM(DW.JJZBZYE),
             SUM(DW.SUMSJCPJE),
             SUM(DW.SUMJJZBZYE)
        FROM DW_MOD_WXZCZJB DW
       WHERE DW.DATA_DATE = P_I_DATE
         AND DW.CURRTYPE IN ('001', '992', '994')
       GROUP BY DW.ZONENO, DW.BRNO, DW.BANKFLAG, DW.CURRTYPE, DW.SJCP;
    "$log"(33);COMMIT;
    "$log"(34);P_O_SUCCEED := '0';
    "$log"(35);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(36);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(37);ROLLBACK;
      "$log"(38);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(39);P_O_SUCCEED := '1';
      "$log"(40);V_ERR_CODE  := SQLCODE;
      "$log"(41);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(42);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(43);RAISE;
  END PROC_WXZC_DM;
END PACK_DEAL_WXZC_1005;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_YHK_1005 IS
  PROCEDURE PROC_DEAL_YHK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_YHK_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_YHK_1005;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_YHK_1005 IS
	"$hits" aaa_coverage_tool.bool_tab;
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id
		   and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate);
		commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(1067);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(1067);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(1067,x); 
		"$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(1067,x); 
		"$times"(x):=true; 
	  end if;
	end;
   
   PROCEDURE PROC_DEAL_YHK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_XS  PRM_XS081.QUOTIETY%TYPE;
    V_DAY NUMBER;
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_OPEN_DAY  DIC_ALL_KIND.KIND_ID%TYPE;
  BEGIN
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_YHK';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	BEGIN
      "$log"(8);
	  SELECT T.KIND_ID INTO V_OPEN_DAY FROM DIC_ALL_KIND T WHERE T.OPERATION_KIND = 'ADJUST_DATE_UG';
    EXCEPTION
      WHEN OTHERS THEN
        "$log"(9);
	    V_OPEN_DAY := '20130101';
    END;
    "$log"(10);
	IF P_I_DATE >= V_OPEN_DAY THEN
      "$log"(11);
	  NULL;
    ELSE
    "$log"(12);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --读取最大系数
    "$log"(13);V_STEP_NO := '2.1';
    "$log"(14);
	SELECT MAX(QUOTIETY) INTO V_XS FROM PRM_XS081 PRM WHERE P_I_DATE >= PRM.START_DATE AND P_I_DATE < PRM.END_DATE;
    --读取该月的天数
    "$log"(15);
	SELECT EXTRACT(DAY FROM LAST_DAY(TO_DATE(P_I_DATE, 'yyyymmdd'))) INTO V_DAY FROM DUAL;
    --回退操作，删除当天数据
    "$log"(16);
	DELETE FROM DW_MOD_YHK DW WHERE DW.DATA_DATE = P_I_DATE;
    --开始计算银行卡类经济资本
    "$log"(17);V_STEP_NO := '2.2';
    "$log"(18);
	INSERT INTO DW_MOD_YHK
      (ZONENO,
       BRNO,
       DATA_DATE,
       CURRTYPE,
       SJCP,
       SJCPJE,
       FPXS,
       JJZBZYE,
       SUMSJCPJE,
       SUMJJZBZYE,
       BANKFLAG)
      SELECT ZONENO,
             BRNO,
             P_I_DATE,
             CURRTYPE,
             PRODUCT_ID,
             SUM(VALUE01),
             VALUE_FXX,
             SUM(VALUE02),
             SUM(VALUE03),
             SUM(VALUE04),
             BANKFLAG
        FROM (SELECT STG.ZONENO,
                     STG.BRNO,
                     STG.CURRTYPE,
                     PRC.PRODUCT_ID,
                     SUM(STG.TDDRBAL * PRC.SUBJECT_APP) VALUE01,
                     NVL(PRX.QUOTIETY, V_XS) VALUE_FXX,
                     SUM(STG.TDDRBAL * NVL(PRX.QUOTIETY, V_XS) * PRC.SUBJECT_APP) VALUE02,
                     SUM(STG.SUMDRBAL * PRC.SUBJECT_APP / V_DAY) VALUE03,
                     SUM(STG.SUMDRBAL * PRC.SUBJECT_APP / V_DAY * NVL(PRX.QUOTIETY, V_XS)) VALUE04,
                     STG.BANKFLAG
                FROM STG_DAT_NFGNLED STG, PRM_CS081 PRC, PRM_XS081 PRX
               WHERE DECODE(PRC.SUBJECT_TYPE,
                            1,
                            SUBSTR(STG.SUBCODE, 1, 4),
                            STG.SUBCODE) = PRC.SUBJECT_CODE
                 AND PRC.PRODUCT_ID = PRX.THPRODUCT(+)
                 AND P_I_DATE >= PRC.START_DATE
                 AND P_I_DATE < PRC.END_DATE
                 AND P_I_DATE >= PRX.START_DATE(+)
                 AND P_I_DATE < PRX.END_DATE(+)
               GROUP BY STG.ZONENO,
                        STG.BRNO,
                        STG.CURRTYPE,
                        PRC.PRODUCT_ID,
                        NVL(PRX.QUOTIETY, V_XS),
                        SUBSTR(STG.SUBCODE, 1, 4),
                        STG.BANKFLAG)
       GROUP BY ZONENO, BRNO, CURRTYPE, PRODUCT_ID, BANKFLAG, VALUE_FXX;
       END IF;
    "$log"(19);
	COMMIT;
    "$log"(20);P_O_SUCCEED := '0';
    "$log"(21);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(22);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(23);ROLLBACK;
      "$log"(24);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(25);P_O_SUCCEED := '1';
      "$log"(26);V_ERR_CODE  := SQLCODE;
      "$log"(27);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(28);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(29);RAISE;
  END PROC_DEAL_YHK;
  
  PROCEDURE PROC_YHK_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_OPEN_DAY  DIC_ALL_KIND.KIND_ID%TYPE;
  BEGIN
    "$log"(30);V_STEP_NO    := '1';
    "$log"(31);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_YHK_DM';
    "$log"(32);V_LOG_LEVEL  := '3';
    "$log"(33);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(34);V_STEP_NO    := '2.1';
    "$log"(35);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    "$log"(36);BEGIN
    "$log"(37);
	SELECT T.KIND_ID INTO V_OPEN_DAY FROM DIC_ALL_KIND T WHERE T.OPERATION_KIND = 'ADJUST_DATE_UG';
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(38);V_OPEN_DAY := '20130101';
  END;

  "$log"(39);
  IF P_I_DATE >= V_OPEN_DAY THEN
    "$log"(40);NULL;
    "$log"(41);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_YHK';
    --数据写入DM表
    "$log"(42);
	INSERT INTO DM_CS_YHK
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT DW.ZONENO,
             DW.BRNO,
             DW.BANKFLAG,
             DW.CURRTYPE,
             DW.SJCP,
             decode(sign(SUM(DW.SJCPJE)),   -1,0,SUM(DW.SJCPJE)),
			 decode(sign(SUM(DW.JJZBZYE)),  -1,0,SUM(DW.JJZBZYE)),
			 decode(sign(SUM(DW.SUMSJCPJE)),-1,0,SUM(DW.SUMSJCPJE)),
			 decode(sign(SUM(DW.SUMJJZBZYE)),-1,0,SUM(DW.SUMJJZBZYE))
        FROM DW_MOD_YHK DW
       WHERE DW.DATA_DATE = P_I_DATE
         AND DW.CURRTYPE IN ('001', '992', '994')
       GROUP BY DW.ZONENO, DW.BRNO, DW.BANKFLAG, DW.CURRTYPE, DW.SJCP;
    END IF;
     "$log"(43);COMMIT;
    "$log"(44);P_O_SUCCEED := '0';
    "$log"(45);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(46);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(47);ROLLBACK;
      "$log"(48);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(49);P_O_SUCCEED := '1';
      "$log"(50);V_ERR_CODE  := SQLCODE;
      "$log"(51);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(52);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(53);RAISE;
  END PROC_YHK_DM;
END PACK_DEAL_YHK_1005;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_YHK_1210 IS
   PROCEDURE PROC_DEAL_YHK_MAIN(P_I_DATE IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_YHK_MON(P_I_DATE IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_YHK_YZ(P_I_DATE IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_YHK_DW(P_I_DATE IN VARCHAR2,
                        P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_YHK_DM(P_I_DATE IN VARCHAR2,
                        P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_YHK_1210;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_YHK_1210 IS
	"$hits" aaa_coverage_tool.bool_tab;
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction; --req utl_http.req; resp utl_http.resp;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(272);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(272);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(272,x); "$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(272,x); "$times"(x):=true; 
	  end if;
	end;
	
   PROCEDURE PROC_DEAL_YHK_MAIN(P_I_DATE IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2) IS
   BEGIN
      "$log"(2);PROC_DEAL_YHK_MON(P_I_DATE, P_O_SUCCEED);
      "$log"(3);PROC_YHK_DW(P_I_DATE, P_O_SUCCEED);
      "$log"(4);PROC_YHK_DM(P_I_DATE, P_O_SUCCEED);
      "$log"(5);P_O_SUCCEED := '0';
      "$log"(6);COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(7);ROLLBACK;
      "$log"(8);P_O_SUCCEED := '1';
      "$log"(9);RAISE;
   END PROC_DEAL_YHK_MAIN;

  PROCEDURE PROC_DEAL_YHK_MON(P_I_DATE IN VARCHAR2,
                              P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    v_string     VARCHAR2(400);
  BEGIN
    --第一步记载数据处理日志
    "$log"(10);V_STEP_NO    := '1';
    "$log"(11);V_PROC_NAME  := 'PACK_DEAL_YHK_1210.PROC_DEAL_YHK_MON';
    "$log"(12);V_LOG_LEVEL  := '3';
    "$log"(13);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     --第二步ODS表进DW表
    "$log"(14);V_STEP_NO    := '2.1';
    "$log"(15);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     -----------DW进行存量更新当月末经济资本
     "$log"(16);
	 DELETE FROM DW_MOD_YHK_UG WHERE DATA_DATE = P_I_DATE;
     "$log"(17);
	-- v_string := 'ALTER TABLE DW_MOD_YHK_UG TRUNCATE PARTITION P'||substr(P_I_DATE,1,6);
     "$log"(18);
	-- EXECUTE IMMEDIATE v_string;
     --给ods表做数据去重,疑似上游可能会给重复数据:
     "$log"(19);
	 DELETE FROM ODS_TMP_YHK_UG A WHERE A.ROWID <> (SELECT MAX(B.ROWID) FROM ODS_TMP_YHK_UG B WHERE A.ACCNO= B.ACCNO AND A.MDCARDNO = B.MDCARDNO  AND A.CURRTYPE = B.CURRTYPE AND A.SUBCODE = B.SUBCODE);
     "$log"(20);COMMIT;
     "$log"(21);
	 INSERT INTO DW_MOD_YHK_UG
      (ACCNO,
       MDCARDNO,
       CARDKIND,
       CORPERF,
       CURRTYPE,
       CINO,
       OPENZONO,
       OPENBRNO,
       OPENDATE,
       SUBCODE,
       CREDIT,
       FIVICODE,
       CUSTNAME,
       CUSTSORT,
       CUSTCODE,
       FBFLAG,
       PD_VALUE,
       LGD_VALUE,
       R_VALUE,
       K_VALUE,
       DEFLT_FLAG,
       PRODUCT_ID3,
       PRODUCT_NAME3,
       ZCAMOUNT,
       ECOST,
       EC_COEF,
       DATA_DATE)
       SELECT /*+LEADING(ODS PRC PCP PCN PCQ PCF) USE_HASH(ODS PRC PCP PCN PCQ PCF)*/
       ACCNO, --银行卡帐号
       MDCARDNO, --卡号
       CARDKIND, --卡种
       CORPERF, --单位个人标志   1-个人；0-单位
       LPAD(CURRTYPE,3,'0') CURRTYPE, --币种
       CINO, --客户主机CIS号
       LPAD(OPENZONO,4,'0') OPENZONO, --开户地区
       LPAD(OPENBRNO,4,'0') OPENBRNO, --开户物理网点
       OPENDATE, --开户日期
       SUBCODE, --科目
       CREDIT, --信用额度
       FIVICODE, --贷款形态（五级分类） 1-正常;2-关注;3-次级;4-可疑;5-损失
       CUSTNAME, --客户名称
       CUSTSORT, --证件类型
       CUSTCODE, --证件号码
       FBFLAG, --零售风险暴露分类
       PD_VALUE, --违约概率PD      百分比%
       LGD_VALUE, --违约损失率LGD   百分比%
       R_VALUE, --相关系数R       百分比%
       K_VALUE, --监管资本系数K   百分比%
       DEFLT_FLAG, --违约标识        1、违约；0、不违约
       PRC.PRODUCT_ID, --三级产品代码
       PRC.SUBJECT_NAME, --三级产品名称
       abs(ODS.LSTBAL), --资产余额  上游为负，需要取正
       --经济资本占用额
       abs(ODS.LSTBAL) * case
         when ODS.SUBCODE IN ('158002', '158004', '158006') then
          CASE
            WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND ODS.FIVICODE NOT IN ('3', '4', '5') THEN
             ODS.K_VALUE / 100 * PCP.COEF
             WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND ODS.FIVICODE IN ('3', '4', '5') THEN
             GREATEST(PCF.COEF_FLOOR, LEAST(ODS.K_VALUE * 5 / 100, PCF.COEF_CEIL)) * 
             DECODE(ODS.FIVICODE,
                    '1',
                    PCQ.VALUE1,
                    '2',
                    PCQ.VALUE2,
                    '3',
                    PCQ.VALUE3,
                    '4',
                    PCQ.VALUE4,
                    '5',
                    PCQ.VALUE5,
                    1)
             WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND ODS.FIVICODE NOT IN ('3', '4', '5') THEN
               PCN.COEF * PCP.COEF
             WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND ODS.FIVICODE IN ('3', '4', '5') THEN
               PCN.COEF * DECODE(ODS.FIVICODE, '3', PCQ.VALUE3, '4', PCQ.VALUE4, '5', PCQ.VALUE5)
          END
         else
          CASE
             WHEN ODS.FIVICODE NOT IN ('3', '4', '5') THEN
               PCN.COEF * PCP.COEF
             WHEN ODS.FIVICODE IN ('3', '4', '5') THEN
               PCN.COEF * DECODE(ODS.FIVICODE, '3', PCQ.VALUE3, '4', PCQ.VALUE4, '5', PCQ.VALUE5)
          END
       end ECOST,
        --经济资本配置系数
       case
         when ODS.SUBCODE IN ('158002', '158004', '158006') then --个人卡透支为内评覆盖，其他卡都为内评未覆盖modified by caohui20130117
          CASE
            WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND ODS.FIVICODE NOT IN ('3', '4', '5')
             THEN
             ODS.K_VALUE / 100 * PCP.COEF
             WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND ODS.FIVICODE IN ('3', '4', '5')
             THEN
             GREATEST(PCF.COEF_FLOOR, LEAST(ODS.K_VALUE * 5 / 100, PCF.COEF_CEIL)) * --K值为百分数 需除100
             DECODE(ODS.FIVICODE,
                    '1',
                    PCQ.VALUE1,
                    '2',
                    PCQ.VALUE2,
                    '3',
                    PCQ.VALUE3,
                    '4',
                    PCQ.VALUE4,
                    '5',
                    PCQ.VALUE5,
                    1)
             WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND ODS.FIVICODE NOT IN ('3', '4', '5') THEN
             PCN.COEF * PCP.COEF
             WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND ODS.FIVICODE IN ('3', '4', '5') THEN
             PCN.COEF * DECODE(ODS.FIVICODE, '3', PCQ.VALUE3, '4', PCQ.VALUE4, '5', PCQ.VALUE5)
          END
         else
          CASE
             WHEN ODS.FIVICODE NOT IN ('3', '4', '5') THEN
               PCN.COEF * PCP.COEF
             WHEN ODS.FIVICODE IN ('3', '4', '5') THEN
               PCN.COEF * DECODE(ODS.FIVICODE, '3', PCQ.VALUE3, '4', PCQ.VALUE4, '5', PCQ.VALUE5)
          END
       end EC_COEF,
       P_I_DATE --DATA_DATE
        FROM ods_tmp_yhk_ug ODS,
             PRM_CS081 PRC, -- 银行卡透支产品定义表
             PRM_CNINCLF PCN, --银行卡内评未覆盖产品信用风险基准配置系数表
             (SELECT /*+no_merge*/
               B.product_id, A.ZONE_NO as ZONENO, B.COEF
                FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                        FROM PRM_IRA_ORGAN
                       WHERE BELONG_BANK_FLAG = '4'
                         AND START_DATE <= P_I_DATE
                         AND END_DATE > P_I_DATE) A,
                     PRM_CPRODZONEF B
               WHERE A.BELONG_ZONENO = B.ZONENO
                 AND B.START_DATE <= P_I_DATE
                 AND B.END_DATE > P_I_DATE) PCP, --银行卡产品地区调节系数表
             PRM_CQLTYF PCQ, --银行卡质量调节系数表
             PRM_CNPCEILF PCF --不良银行卡透支经济资本配置系数上限
       WHERE ODS.SUBCODE = PRC.SUBJECT_CODE(+)
         AND ODS.OPENZONO = PCP.ZONENO(+)
         AND PRC.PRODUCT_ID = PCN.PRODUCT_ID
         AND PRC.PRODUCT_ID = PCP.PRODUCT_ID
         AND PRC.PRODUCT_ID = PCQ.PRODUCT_ID
         AND PRC.PRODUCT_ID = PCF.PRODUCT_ID
         AND P_I_DATE >= PRC.START_DATE(+)
         AND P_I_DATE < PRC.END_DATE(+)
         AND P_I_DATE >= PCN.START_DATE(+)
         AND P_I_DATE < PCN.END_DATE(+)
          AND P_I_DATE >= PCQ.START_DATE(+)
         AND P_I_DATE < PCQ.END_DATE(+)
         AND P_I_DATE >= PCF.START_DATE(+)
         AND P_I_DATE < PCF.END_DATE(+);
     "$log"(22);COMMIT;
     "$log"(23);P_O_SUCCEED := '0';
    --记载数据处理日志
    "$log"(24);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(25);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(26);ROLLBACK;
      "$log"(27);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(28);P_O_SUCCEED := '1';
      "$log"(29);V_ERR_CODE  := SQLCODE;
      "$log"(30);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(31);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(32);RAISE;
  END PROC_DEAL_YHK_MON;
  
  PROCEDURE PROC_DEAL_YHK_YZ(P_I_DATE IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_FIRSTDAY   DW_MOD_YHK_UG.DATA_DATE%TYPE;
    V_LASTMONTH  DW_MOD_YHK_UG.DATA_DATE%TYPE;
  BEGIN
    --第一步记载数据处理日志
    "$log"(33);V_STEP_NO    := '1';
    "$log"(34);V_PROC_NAME  := 'PACK_DEAL_YHK_1210.PROC_DEAL_YHK_YZ';
    "$log"(35);V_LOG_LEVEL  := '3';
    "$log"(36);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(37);V_FIRSTDAY   := SUBSTR(P_I_DATE, 1, 6) || '01';
    "$log"(38);V_LASTMONTH  := TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(P_I_DATE, 'YYYYMMDD'), -1)), 'YYYYMMDD');
     --第二步月初特殊处理
    "$log"(39);V_STEP_NO    := '2.1';
    "$log"(40);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    
     --给ods表做数据去重,疑似上游可能会给重复数据:
     "$log"(41);
	 DELETE FROM ODS_TMP_YHK_UG A
       WHERE A.ROWID <> (SELECT MAX(B.ROWID) FROM ODS_TMP_YHK_UG B WHERE A.ACCNO= B.ACCNO AND A.MDCARDNO = B.MDCARDNO AND A.CURRTYPE = B.CURRTYPE AND A.SUBCODE = B.SUBCODE);
     "$log"(42);COMMIT;
     --如果等于月初，则进行复制上月末明细的操作
    "$log"(43);
	IF P_I_DATE = V_FIRSTDAY THEN
      "$log"(44);
	  INSERT INTO DW_MOD_YHK_UG
        (ACCNO,
         MDCARDNO,
         CARDKIND,
         CORPERF,
         CURRTYPE,
         CINO,
         OPENZONO,
         OPENBRNO,
         OPENDATE,
         SUBCODE,
         CREDIT,
         FIVICODE,
         CUSTNAME,
         CUSTSORT,
         CUSTCODE,
         FBFLAG,
         PD_VALUE,
         LGD_VALUE,
         R_VALUE,
         K_VALUE,
         DEFLT_FLAG,
         PRODUCT_ID3,
         PRODUCT_NAME3,
         ZCAMOUNT,
         ECOST,
         EC_COEF,
         DATA_DATE)
        SELECT /*+PARALLEL(DW,8) */
         ACCNO,
         MDCARDNO,
         CARDKIND,
         CORPERF,
         CURRTYPE,
         CINO,
         OPENZONO,
         OPENBRNO,
         OPENDATE,
         SUBCODE,
         CREDIT,
         FIVICODE,
         CUSTNAME,
         CUSTSORT,
         CUSTCODE,
         FBFLAG,
         PD_VALUE,
         LGD_VALUE,
         R_VALUE,
         K_VALUE,
         DEFLT_FLAG,
         PRODUCT_ID3,
         PRODUCT_NAME3,
         ZCAMOUNT,
         ECOST,
         EC_COEF,
         P_I_DATE
          FROM DW_MOD_YHK_UG DW
         WHERE DW.DATA_DATE = V_LASTMONTH;
    END IF;
     --第三步ODS表进DW表
    "$log"(45);V_STEP_NO    := '3.1';
    "$log"(46);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     -----------对DW表中月初至今的明细进行增量更新当月末经济资本
    "$log"(47);
	MERGE /*+use_hash(dw ods)*/
    INTO DW_MOD_YHK_UG DW
    USING (SELECT /*+LEADING(ODS PRC PCP PCN PCQ PCF) USE_HASH(ODS PRC PCP PCN PCQ PCF)*/
            ACCNO, --银行卡帐号
            MDCARDNO, --卡号
            CARDKIND, --卡种
            CORPERF, --单位个人标志   1-个人；0-单位
            LPAD(CURRTYPE,3,'0') CURRTYPE, --币种
            CINO, --客户主机CIS号
            LPAD(OPENZONO,4,'0') OPENZONO, --开户地区
            LPAD(OPENBRNO,4,'0') OPENBRNO, --开户物理网点
            OPENDATE, --开户日期
            SUBCODE, --科目
            CREDIT, --信用额度
            FIVICODE, --贷款形态（五级分类） 1-正常;2-关注;3-次级;4-可疑;5-损失
            CUSTNAME, --客户名称
            CUSTSORT, --证件类型
            CUSTCODE, --证件号码
            FBFLAG, --零售风险暴露分类
            PD_VALUE, --违约概率PD      百分比%
            LGD_VALUE, --违约损失率LGD   百分比%
            R_VALUE, --相关系数R       百分比%
            K_VALUE, --监管资本系数K   百分比%
            DEFLT_FLAG, --违约标识        1、违约；0、不违约
            PRC.PRODUCT_ID PRODUCT_ID3, --三级产品代码
            PRC.SUBJECT_NAME PRODUCT_NAME3, --三级产品名称
            ABS(ODS.LSTBAL) ZCAMOUNT, --资产余额  上游为负，需要取正
            --经济资本占用额
            ABS(ODS.LSTBAL) * case
              when ODS.SUBCODE IN ('158002', '158004', '158006') then --个人卡透支为内评覆盖，其他卡都为内评未覆盖modified by caohui20130117
               CASE
                 WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND ODS.FIVICODE NOT IN ('3', '4', '5') THEN
                  ODS.K_VALUE / 100 * PCP.COEF
                  WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND ODS.FIVICODE IN ('3', '4', '5') THEN
                  GREATEST(PCF.COEF_FLOOR, LEAST(ODS.K_VALUE * 5 / 100, PCF.COEF_CEIL)) * --K值为百分数 需除100
                  DECODE(ODS.FIVICODE,
                         '1',
                         PCQ.VALUE1,
                         '2',
                         PCQ.VALUE2,
                         '3',
                         PCQ.VALUE3,
                         '4',
                         PCQ.VALUE4,
                         '5',
                         PCQ.VALUE5,
                         1)
                 WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND ODS.FIVICODE NOT IN ('3', '4', '5') --内评未覆盖（K空或者0）且不违约经济资本配置系数×产品地区调节系数
                  THEN
                  PCN.COEF * PCP.COEF
                  WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND ODS.FIVICODE IN ('3', '4', '5') --内评未覆盖且违约
                  THEN
                  PCN.COEF * DECODE(ODS.FIVICODE,
                                    '3',
                                    PCQ.VALUE3,
                                    '4',
                                    PCQ.VALUE4,
                                    '5',
                                    PCQ.VALUE5)
               END
              else --ODS.SUBCODE not IN ('158002', '158004', '158006')
               CASE
                  WHEN ODS.FIVICODE NOT IN ('3', '4', '5') --内评未覆盖（K空或者0）且不违约经济资本配置系数×产品地区调节系数
                  THEN
                  PCN.COEF * PCP.COEF
                  WHEN ODS.FIVICODE IN ('3', '4', '5') --内评未覆盖且违约
                  THEN
                  PCN.COEF * DECODE(ODS.FIVICODE,
                                    '3',
                                    PCQ.VALUE3,
                                    '4',
                                    PCQ.VALUE4,
                                    '5',
                                    PCQ.VALUE5)
               END
            end ECOST,
            case
              when ODS.SUBCODE IN ('158002', '158004', '158006') then --个人卡透支为内评覆盖，其他卡都为内评未覆盖modified by caohui20130117
               CASE
                 WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND
                      ODS.FIVICODE NOT IN ('3', '4', '5')
                  THEN
                  ODS.K_VALUE / 100 * PCP.COEF
                  WHEN ODS.K_VALUE IS NOT NULL and ODS.K_VALUE <> 0 AND
                      ODS.FIVICODE IN ('3', '4', '5')
                  THEN
                  GREATEST(PCF.COEF_FLOOR,
                           LEAST(ODS.K_VALUE * 5 / 100, PCF.COEF_CEIL)) * --K值为百分数 需除100
                  DECODE(ODS.FIVICODE,
                         '1',
                         PCQ.VALUE1,
                         '2',
                         PCQ.VALUE2,
                         '3',
                         PCQ.VALUE3,
                         '4',
                         PCQ.VALUE4,
                         '5',
                         PCQ.VALUE5,
                         1)
                  WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND
                      ODS.FIVICODE NOT IN ('3', '4', '5') --内评未覆盖（K空或者0）且不违约经济资本配置系数×产品地区调节系数
                  THEN
                  PCN.COEF * PCP.COEF
                  WHEN (ODS.K_VALUE IS NULL or ODS.K_VALUE = 0) AND
                      ODS.FIVICODE IN ('3', '4', '5') --内评未覆盖且违约
                  THEN
                  PCN.COEF * DECODE(ODS.FIVICODE,
                                    '3',
                                    PCQ.VALUE3,
                                    '4',
                                    PCQ.VALUE4,
                                    '5',
                                    PCQ.VALUE5)
               END
              else --ODS.SUBCODE IN ('158002', '158004', '158006')
               CASE
                  WHEN ODS.FIVICODE NOT IN ('3', '4', '5') --内评未覆盖（K空或者0）且不违约经济资本配置系数×产品地区调节系数
                  THEN
                  PCN.COEF * PCP.COEF
                  WHEN ODS.FIVICODE IN ('3', '4', '5') --内评未覆盖且违约
                  THEN
                  PCN.COEF * DECODE(ODS.FIVICODE,
                                    '3',
                                    PCQ.VALUE3,
                                    '4',
                                    PCQ.VALUE4,
                                    '5',
                                    PCQ.VALUE5)
               END
            end EC_COEF,
            P_I_DATE DATA_DATE --DATA_DATE
             FROM ODS_TMP_YHK_UG ODS, --ODS信用卡接口表
                  PRM_CS081 PRC, -- 银行卡透支产品定义表
                  PRM_CNINCLF PCN, --银行卡内评未覆盖产品信用风险基准配置系数表
                  (SELECT /*+no_merge*/
                    B.product_id, A.ZONE_NO as ZONENO, B.COEF
                     FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                             FROM PRM_IRA_ORGAN
                            WHERE BELONG_BANK_FLAG = '4'
                              AND START_DATE <= P_I_DATE
                              AND END_DATE > P_I_DATE) A,
                          PRM_CPRODZONEF B
                    WHERE A.BELONG_ZONENO = B.ZONENO
                      AND B.START_DATE <= P_I_DATE
                      AND B.END_DATE > P_I_DATE) PCP, --银行卡产品地区调节系数表
                  PRM_CQLTYF PCQ, --银行卡质量调节系数表
                  PRM_CNPCEILF PCF --不良银行卡透支经济资本配置系数上限
            WHERE ODS.SUBCODE = PRC.SUBJECT_CODE(+)
              AND ODS.OPENZONO = PCP.ZONENO(+)
              AND PRC.PRODUCT_ID = PCN.PRODUCT_ID
              AND PRC.PRODUCT_ID = PCP.PRODUCT_ID
              AND PRC.PRODUCT_ID = PCQ.PRODUCT_ID
              AND PRC.PRODUCT_ID = PCF.PRODUCT_ID
              AND P_I_DATE >= PRC.START_DATE(+)
              AND P_I_DATE < PRC.END_DATE(+)
              AND P_I_DATE >= PCN.START_DATE(+)
              AND P_I_DATE < PCN.END_DATE(+)
               AND P_I_DATE >= PCQ.START_DATE(+)
              AND P_I_DATE < PCQ.END_DATE(+)
              AND P_I_DATE >= PCF.START_DATE(+)
              AND P_I_DATE < PCF.END_DATE(+)) ODS
    ON (ODS.ACCNO = DW.ACCNO AND ODS.CURRTYPE = DW.CURRTYPE AND ODS.SUBCODE = DW.SUBCODE AND DW.DATA_DATE >= V_FIRSTDAY AND DW.DATA_DATE <= P_I_DATE)
    WHEN MATCHED THEN
      UPDATE
         SET DW.MDCARDNO      = ODS.MDCARDNO,
             DW.CARDKIND      = ODS.CARDKIND,
             DW.CORPERF       = ODS.CORPERF,
             DW.CINO          = ODS.CINO,
             DW.OPENZONO      = ODS.OPENZONO,
             DW.OPENBRNO      = ODS.OPENBRNO,
             DW.OPENDATE      = ODS.OPENDATE,
             DW.CREDIT        = ODS.CREDIT,
             DW.FIVICODE      = ODS.FIVICODE,
             DW.CUSTNAME      = ODS.CUSTNAME,
             DW.CUSTSORT      = ODS.CUSTSORT,
             DW.CUSTCODE      = ODS.CUSTCODE,
             DW.FBFLAG        = ODS.FBFLAG,
             DW.PD_VALUE      = ODS.PD_VALUE,
             DW.LGD_VALUE     = ODS.LGD_VALUE,
             DW.R_VALUE       = ODS.R_VALUE,
             DW.K_VALUE       = ODS.K_VALUE,
             DW.DEFLT_FLAG    = ODS.DEFLT_FLAG,
             DW.PRODUCT_ID3   = ODS.PRODUCT_ID3,
             DW.PRODUCT_NAME3 = ODS.PRODUCT_NAME3,
             DW.ZCAMOUNT      = ODS.ZCAMOUNT,
             DW.ECOST         = ODS.ECOST,
             DW.EC_COEF       = ODS.EC_COEF
    WHEN NOT MATCHED THEN
      INSERT
        (DW.ACCNO,
         DW.MDCARDNO,
         DW.CARDKIND,
         DW.CORPERF,
         DW.CURRTYPE,
         DW.CINO,
         DW.OPENZONO,
         DW.OPENBRNO,
         DW.OPENDATE,
         DW.SUBCODE,
         DW.CREDIT,
         DW.FIVICODE,
         DW.CUSTNAME,
         DW.CUSTSORT,
         DW.CUSTCODE,
         DW.FBFLAG,
         DW.PD_VALUE,
         DW.LGD_VALUE,
         DW.R_VALUE,
         DW.K_VALUE,
         DW.DEFLT_FLAG,
         DW.PRODUCT_ID3,
         DW.PRODUCT_NAME3,
         DW.ZCAMOUNT,
         DW.ECOST,
         DW.EC_COEF,
         DW.DATA_DATE)
      VALUES
        (ODS.ACCNO,
         ODS.MDCARDNO,
         ODS.CARDKIND,
         ODS.CORPERF,
         ODS.CURRTYPE,
         ODS.CINO,
         ODS.OPENZONO,
         ODS.OPENBRNO,
         ODS.OPENDATE,
         ODS.SUBCODE,
         ODS.CREDIT,
         ODS.FIVICODE,
         ODS.CUSTNAME,
         ODS.CUSTSORT,
         ODS.CUSTCODE,
         ODS.FBFLAG,
         ODS.PD_VALUE,
         ODS.LGD_VALUE,
         ODS.R_VALUE,
         ODS.K_VALUE,
         ODS.DEFLT_FLAG,
         ODS.PRODUCT_ID3,
         ODS.PRODUCT_NAME3,
         ODS.ZCAMOUNT,
         ODS.ECOST,
         ODS.EC_COEF,
         P_I_DATE);
     "$log"(48);COMMIT;
     "$log"(49);P_O_SUCCEED := '0';
    --记载数据处理日志
    "$log"(50);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(51);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(52);ROLLBACK;
      "$log"(53);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(54);P_O_SUCCEED := '1';
      "$log"(55);V_ERR_CODE  := SQLCODE;
      "$log"(56);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(57);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(58);RAISE;
  END PROC_DEAL_YHK_YZ;

   PROCEDURE PROC_YHK_DW(P_I_DATE    IN VARCHAR2,
                        P_O_SUCCEED OUT VARCHAR2) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_FIRSTDAY   DW_MOD_YHK_UG.DATA_DATE%TYPE;
    V_LASTMONTH  DW_MOD_YHK_UG.DATA_DATE%TYPE;
  BEGIN
     --第一步记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_DEAL_YHK_1210.PROC_YHK_DW';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_FIRSTDAY   := SUBSTR(P_I_DATE, 1, 6) || '01';
    V_LASTMONTH  := TO_CHAR(LAST_DAY(TO_DATE(P_I_DATE, 'YYYYMMDD')), 'YYYYMMDD');--应为本月末 by caohui 20130201
     --第二步月初特殊处理
    V_STEP_NO    := '2.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     --如果月初，则对DW_SUM_YHK_UG表进行truncate处理
    IF P_I_DATE = V_FIRSTDAY THEN
      EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_SUM_YHK_UG';
    END IF;
     --预先清理历史数据
    DELETE FROM DW_SUM_YHK_UG UG WHERE UG.DATA_DATE = P_I_DATE;
     --第三步每日对DW进行载入处理特殊处理
    V_STEP_NO    := '3.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     IF P_I_DATE = V_LASTMONTH THEN
       --每日存量统计汇总值
      INSERT INTO DW_SUM_YHK_UG
        (CURRTYPE,
         OPENZONO,
         OPENBRNO,
         SUBCODE,
         LSTBAL,
         LSTBAL_CM,
         FIVICODE,
         PRODUCT_ID3,
         ECOST,
         ECOST_CM,
         DATA_DATE)
        SELECT DW.CURRTYPE,
               DW.OPENZONO,
               DW.OPENBRNO,
               DW.SUBCODE,
               SUM(DW.ZCAMOUNT),
               NULL,
               DW.FIVICODE,
               DW.PRODUCT_ID3,
               SUM(DW.ECOST),
               NULL,
               P_I_DATE
          FROM DW_MOD_YHK_UG DW
         WHERE DW.DATA_DATE = P_I_DATE
         GROUP BY DW.CURRTYPE,
                  DW.OPENZONO,
                  DW.OPENBRNO,
                  DW.SUBCODE,
                  DW.FIVICODE,
                  DW.PRODUCT_ID3;
     ELSE
      INSERT INTO DW_SUM_YHK_UG
        (CURRTYPE,
         OPENZONO,
         OPENBRNO,
         SUBCODE,
         LSTBAL,
         LSTBAL_CM,
         FIVICODE,
         PRODUCT_ID3,
         ECOST,
         ECOST_CM,
         DATA_DATE)
        SELECT DW.CURRTYPE,
               DW.OPENZONO,
               DW.OPENBRNO,
               DW.SUBCODE,
               SUM(DW.ZCAMOUNT),
               NULL,
               DW.FIVICODE,
               DW.PRODUCT_ID3,
               SUM(DW.ECOST),
               NULL,
               P_I_DATE
          FROM DW_MOD_YHK_UG DW
         WHERE DW.DATA_DATE = P_I_DATE
         GROUP BY DW.CURRTYPE,
                  DW.OPENZONO,
                  DW.OPENBRNO,
                  DW.SUBCODE,
                  DW.FIVICODE,
                  DW.PRODUCT_ID3;
     END IF;
     --提交结果
    COMMIT;
    P_O_SUCCEED := '0';
     --结束，填写日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END PROC_YHK_DW;

   PROCEDURE PROC_YHK_DM(P_I_DATE    IN VARCHAR2,
                        P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_MONTH_DAY  NUMBER(3); --当月天数
    V_FIRSTDAY   DW_MOD_YHK_UG.DATA_DATE%TYPE;
  BEGIN
     --第一步记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_DEAL_YHK_1210.PROC_YHK_DM';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_FIRSTDAY   := SUBSTR(P_I_DATE, 1, 6) || '01';
    SELECT TO_DATE(P_I_DATE, 'YYYYMMDD') - TO_DATE(V_FIRSTDAY, 'YYYYMMDD') + 1 INTO V_MONTH_DAY FROM DUAL;
     --第二步月初特殊处理
    V_STEP_NO    := '2.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --每日清理DM临时表数据
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_YHK_UG';
     --第三步每日对DW进行载入汇总全月数据并按实际天数计算日均
    V_STEP_NO    := '3.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     INSERT INTO DM_CS_YHK_UG
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT /*+FULL(DW)*/
       DW.OPENZONO ,
       DW.OPENBRNO,
       '2',
       LPAD(CURRTYPE, 3, '0'),
       PRODUCT_ID3,
       SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.LSTBAL
             ELSE
              0
           END),
        SUM(CASE
             WHEN DW.DATA_DATE = P_I_DATE THEN
              DW.ECOST
             ELSE
              0
           END),
       SUM(DW.LSTBAL) / V_MONTH_DAY,
       SUM(DW.ECOST) / V_MONTH_DAY
        FROM DW_SUM_YHK_UG DW
       GROUP BY CURRTYPE, PRODUCT_ID3, OPENZONO,OPENBRNO;
     --第四步折币处理 外币折美元 本外币折美元
    V_STEP_NO    := '4.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     ---------------------开始外币折美元-----------------
    INSERT INTO DM_CS_YHK_UG
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT A.ZONENO,
             A.BRNO,
             '2',
             '992',
             A.PRODID,
             SUM(A.VALUE01 * B.USCVRATE),
             SUM(A.VALUE02 * B.USCVRATE),
             SUM(A.VALUE03 * B.USCVRATE),
             SUM(A.VALUE04 * B.USCVRATE)
        FROM DM_CS_YHK_UG A, PRM_CURRENCY_RATE B
       WHERE A.CURRTYPE <> '001'
         AND A.CURRTYPE = B.CURRENCY
       GROUP BY A.ZONENO, A.BRNO, A.PRODID;
     V_STEP_NO    := '4.2';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     ----------------开始本外币折人民币----------------
    INSERT INTO DM_CS_YHK_UG
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT A.ZONENO,
             A.BRNO,
             '2',
             '994',
             A.PRODID,
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE01,
                        A.VALUE01 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE02,
                        A.VALUE02 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE03,
                        A.VALUE03 / B.USCVRATE)),
             SUM(DECODE(A.CURRTYPE,
                        '001',
                        A.VALUE04,
                        A.VALUE04 / B.USCVRATE))
        FROM DM_CS_YHK_UG A, PRM_CURRENCY_RATE B
       WHERE A.CURRTYPE IN ('001', '992')
         AND B.CURRENCY = '001'
       GROUP BY A.ZONENO, A.BRNO, A.PRODID;
     --第五步机构汇总 该明细最低粒度为核算网点，且一个地区只有一个
    V_STEP_NO    := '4.1';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
     --开始汇总，核算网点起汇总，从3开始
    FOR V_FLAG IN 3 .. 5 LOOP
      MERGE INTO DM_CS_YHK_UG DM
      USING (SELECT PRM.BELONG_ZONENO ZONENO,
                    PRM.BELONG_BRNO BRNO,
                    PRM.BELONG_BANK_FLAG BANKFLAG,
                    DM.PRODID,
                    DM.CURRTYPE,
                    SUM(DM.VALUE01) VALUE01,
                    SUM(DM.VALUE02) VALUE02,
                    SUM(DM.VALUE03) VALUE03,
                    SUM(DM.VALUE04) VALUE04
               FROM DM_CS_YHK_UG DM, PRM_IRA_ORGAN PRM
              WHERE DM.ZONENO = PRM.ZONE_NO
                AND DM.BRNO = PRM.BR_NO
                AND DM.BANKFLAG = PRM.BANK_FLAG
                AND PRM.BELONG_BANK_FLAG = V_FLAG
                AND PRM.BANK_FLAG <> V_FLAG
                AND P_I_DATE >= PRM.START_DATE
                AND P_I_DATE < PRM.END_DATE
              GROUP BY PRM.BELONG_ZONENO,
                       PRM.BELONG_BRNO,
                       PRM.BELONG_BANK_FLAG,
                       DM.PRODID,
                       DM.CURRTYPE) TMP
      ON (DM.ZONENO = TMP.ZONENO AND DM.BRNO = TMP.BRNO AND DM.BANKFLAG = TMP.BANKFLAG AND DM.PRODID = TMP.PRODID AND DM.CURRTYPE = TMP.CURRTYPE)
      WHEN MATCHED THEN
        UPDATE
           SET DM.VALUE01 = DM.VALUE01 + TMP.VALUE01,
               DM.VALUE02 = DM.VALUE02 + TMP.VALUE02,
               DM.VALUE03 = DM.VALUE03 + TMP.VALUE03,
               DM.VALUE04 = DM.VALUE04 + TMP.VALUE04
      WHEN NOT MATCHED THEN
        INSERT
          (ZONENO,
           BRNO,
           BANKFLAG,
           CURRTYPE,
           PRODID,
           VALUE01,
           VALUE02,
           VALUE03,
           VALUE04)
        VALUES
          (TMP.ZONENO,
           TMP.BRNO,
           TMP.BANKFLAG,
           TMP.CURRTYPE,
           TMP.PRODID,
           TMP.VALUE01,
           TMP.VALUE02,
           TMP.VALUE03,
           TMP.VALUE04);
    END LOOP;
     --提交结果
    COMMIT;
    P_O_SUCCEED := '0';
     --结束，填写日志
    V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END PROC_YHK_DM;
END PACK_DEAL_YHK_1210;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_ZJYW_1005 IS
  PROCEDURE PROC_DEAL_ZJYW(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_ZJYW_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_ZJYW_1005;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_ZJYW_1005 IS
	"$hits" aaa_coverage_tool.bool_tab;
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id
		   and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate);
		commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(1059);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(1059);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(1059,x); 
		"$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(1059,x); 
		"$times"(x):=true; 
	  end if;
	end;
   
   PROCEDURE PROC_DEAL_ZJYW(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_XS  PRM_XS081.QUOTIETY%TYPE;
    V_DAY NUMBER;
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_ZJYW';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --读取最大系数
    "$log"(8);V_STEP_NO := '2.1';
    "$log"(9);
	SELECT MAX(par_coef) INTO V_XS FROM PRM_XSZJ1 T  WHERE P_I_DATE >= T.START_DATE AND P_I_DATE < T.END_DATE;
    --读取该月的天数
    "$log"(10);
	SELECT EXTRACT(DAY FROM LAST_DAY(TO_DATE(P_I_DATE, 'yyyymmdd'))) INTO V_DAY FROM DUAL;
    --回退操作，删除当天数据
    "$log"(11);
	DELETE FROM DW_MOD_ZJYWZJB WHERE DATA_DATE = P_I_DATE;
    --开始计算银行卡类经济资本
    "$log"(12);V_STEP_NO := '2.2';
    "$log"(13);
	INSERT INTO DW_MOD_ZJYWZJB
      (ZONENO,
       BRNO,
       DATA_DATE,
       CURRTYPE,
       SJCP,
       SJCPJE,
       FPXS,
       JJZBZYE,
       BANK_FLAG,
       SUMSJCPJE,
       SUMEC)
      (SELECT A.ZONENO,
              A.BRNO,
              P_I_DATE,
              A.CURRTYPE,
              B.PRODUCT_ID,
              SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                         '8',
                         A.TDCRBAL,
                         '2',
                         A.TDCRBAL,
                         '4',
                         DECODE(SIGN(A.TDDRBAL - A.TDCRBAL),
                                -1,
                                0,
                                A.TDDRBAL - A.TDCRBAL),
                         A.TDDRBAL) * B.SUBJECT_APP),
              NVL(C.par_coef, V_XS),
              SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                         '8',
                         A.TDCRBAL,
                         '2',
                         A.TDCRBAL,
                         '4',
                         DECODE(SIGN(A.TDDRBAL - A.TDCRBAL),
                                -1,
                                0,
                                A.TDDRBAL - A.TDCRBAL),
                         A.TDDRBAL) * NVL(C.par_coef, V_XS) * B.SUBJECT_APP),
              A.BANKFLAG,
              SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                         '8',
                         A.SUMCRBAL,
                         '2',
                         A.SUMCRBAL,
                         '4',
                         DECODE(SIGN(A.SUMDRBAL - A.SUMCRBAL),
                                -1,
                                0,
                                A.SUMDRBAL - A.SUMCRBAL),
                         A.SUMDRBAL) * B.SUBJECT_APP) / V_DAY,
              SUM(DECODE(SUBSTR(A.SUBCODE, 1, 1),
                         '8',
                         A.SUMCRBAL,
                         '2',
                         A.SUMCRBAL,
                         '4',
                         DECODE(SIGN(A.SUMDRBAL - A.SUMCRBAL),
                                -1,
                                0,
                                A.SUMDRBAL - A.SUMCRBAL),
                         A.SUMDRBAL) * NVL(C.par_coef, V_XS) * B.SUBJECT_APP) /
              V_DAY
         FROM STG_DAT_NFGNLED A, PRM_CS05A B, PRM_XSZJ1 C
        WHERE DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) = B.SUBJECT_CODE
          AND P_I_DATE >= B.START_DATE
          AND P_I_DATE < B.END_DATE
          AND B.PRODUCT_ID = C.PRODUCT_ID(+)
          AND P_I_DATE >= C.START_DATE(+)
          AND P_I_DATE < C.END_DATE(+)
        GROUP BY A.ZONENO,
                 A.BRNO,
                 A.CURRTYPE,
                 B.PRODUCT_ID,
                 NVL(c.par_coef, V_XS),
                 A.BANKFLAG);
    "$log"(14);COMMIT;
    "$log"(15);P_O_SUCCEED := '0';
    "$log"(16);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(17);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(18);ROLLBACK;
      "$log"(19);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(20);P_O_SUCCEED := '1';
      "$log"(21);V_ERR_CODE  := SQLCODE;
      "$log"(22);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(23);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(24);RAISE;
  END PROC_DEAL_ZJYW;
  
  PROCEDURE PROC_ZJYW_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(25);V_STEP_NO    := '1';
    "$log"(26);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_YHK_DM';
    "$log"(27);V_LOG_LEVEL  := '3';
    "$log"(28);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(29);V_STEP_NO    := '2.1';
    "$log"(30);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --开始删除DM表中数据
    "$log"(31);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_ZZJYE';
    --数据写入DM表
    "$log"(32);
	INSERT INTO DM_CS_ZZJYE
      (ZONENO,
       BRNO,
       BANKFLAG,
       CURRTYPE,
       PRODID,
       VALUE01,
       VALUE02,
       VALUE03,
       VALUE04)
      SELECT DW.ZONENO,
             DW.BRNO,
             DW.BANK_FLAG,
             DW.CURRTYPE,
             DW.SJCP,
             SUM(DW.SJCPJE),
             SUM(DW.JJZBZYE),
             SUM(DW.SUMSJCPJE),
             SUM(DW.SUMEC)
        FROM DW_MOD_ZJYWZJB DW
       WHERE DW.DATA_DATE = P_I_DATE
         AND DW.CURRTYPE IN ('001', '992', '994')
       GROUP BY DW.ZONENO, DW.BRNO, DW.BANK_FLAG, DW.CURRTYPE, DW.SJCP;
    "$log"(33);COMMIT;
    "$log"(34);P_O_SUCCEED := '0';
    "$log"(35);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(36);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(37);ROLLBACK;
      "$log"(38);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(39);P_O_SUCCEED := '1';
      "$log"(40);V_ERR_CODE  := SQLCODE;
      "$log"(41);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(42);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(43);RAISE;
  END PROC_ZJYW_DM;
END PACK_DEAL_ZJYW_1005;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_ZQTZ_1005 IS
  PROCEDURE PROC_DEAL_ZQTZ(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_ZQTZ_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_ZQTZ_1005;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_ZQTZ_1005 IS
	"$hits" aaa_coverage_tool.bool_tab;
	"$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id
		   and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate);
		commit;
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(1055);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(1055);
		dbms_output.put_line('在aaa_coverage_tool源码中无 get_statement_time_tab 方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(1055,x); 
		"$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(1055,x); 
		"$times"(x):=true; 
	  end if;
	end;
	
   PROCEDURE PROC_DEAL_ZQTZ(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_XS  PRM_XS081.QUOTIETY%TYPE;
    V_DAY NUMBER;
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_ZQTZ';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --读取最大系数
    "$log"(8);V_STEP_NO := '2.1';
    "$log"(9);
	SELECT MAX(PZXS) INTO V_XS FROM PRM_XS061 PRM WHERE P_I_DATE >= PRM.START_DATE AND P_I_DATE < PRM.END_DATE;
    --读取该月的天数
    "$log"(10);
	SELECT EXTRACT(DAY FROM LAST_DAY(TO_DATE(P_I_DATE, 'yyyymmdd'))) INTO V_DAY FROM DUAL;
    --回退操作，删除当天数据
    "$log"(11);
	DELETE FROM DW_MOD_ZQTZZJB1 DW WHERE DW.DATA_DATE = P_I_DATE;
    --开始计算银行卡类经济资本
    "$log"(12);
	V_STEP_NO := '2.2';
    "$log"(13);
	INSERT INTO DW_MOD_ZQTZZJB1
      (ZONENO,
       BRNO,
       DATA_DATE,
       CURRTYPE,
       EJCP,
       ZQTZJE,
       FPXS,
       ECOST,
       SUMZQTZJE,
       SUMECOST,
       source_flag)
      SELECT ZONENO,
             BRNO,
             P_I_DATE,
             CURRTYPE,
             PRODUCT_ID,
             SUM(VALUE01),
             VALUE_FXX,
             SUM(VALUE02),
             SUM(VALUE03),
             SUM(VALUE04),
            '0'
        FROM (SELECT STG.ZONENO,
                     STG.BRNO,
                     STG.CURRTYPE,
                     PRC.PRODUCT_ID,
                     SUM(STG.TDDRBAL * PRC.SUBJECT_APP) VALUE01,
                     NVL(PRX.PZXS, V_XS) VALUE_FXX,
                     SUM(STG.TDDRBAL * NVL(PRX.PZXS, V_XS) * PRC.SUBJECT_APP) VALUE02,
                     SUM(STG.SUMDRBAL * PRC.SUBJECT_APP / V_DAY) VALUE03,
                     SUM(STG.SUMDRBAL * PRC.SUBJECT_APP / V_DAY *
                         NVL(PRX.PZXS, V_XS)) VALUE04
                FROM STG_DAT_NFGNLED STG, PRM_CS061 PRC, PRM_XS061 PRX
               WHERE DECODE(PRC.SUBJECT_TYPE, 1, SUBSTR(STG.SUBCODE, 1, 4), STG.SUBCODE) = PRC.SUBJECT_CODE
                 AND PRC.PRODUCT_ID = PRX.PRODUCT_ID(+)
                 AND P_I_DATE >= PRC.START_DATE
                 AND P_I_DATE < PRC.END_DATE
                 AND P_I_DATE >= PRX.START_DATE(+)
                 AND P_I_DATE < PRX.END_DATE(+)
                 AND PRC.DMLX = '1'
                 AND STG.BANKFLAG = '4'
               GROUP BY STG.ZONENO,
                        STG.BRNO,
                        STG.CURRTYPE,
                        PRC.PRODUCT_ID,
                        NVL(PRX.PZXS, V_XS)
                        )
       GROUP BY ZONENO, BRNO, CURRTYPE, PRODUCT_ID, VALUE_FXX;
    "$log"(14);COMMIT;
    "$log"(15);P_O_SUCCEED := '0';
    "$log"(16);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(17);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(18);ROLLBACK;
      "$log"(19);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(20);P_O_SUCCEED := '1';
      "$log"(21);V_ERR_CODE  := SQLCODE;
      "$log"(22);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(23);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(24);RAISE;
  END PROC_DEAL_ZQTZ;
  
  PROCEDURE PROC_ZQTZ_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(25);V_STEP_NO    := '1';
    "$log"(26);V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_YHK_DM';
    "$log"(27);V_LOG_LEVEL  := '3';
    "$log"(28);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(29);V_STEP_NO    := '2.1';
    "$log"(30);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
    --开始删除DM表中数据
    "$log"(31);
	EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_ZZQTZ';
    --数据写入DM表
    "$log"(32);
	INSERT INTO DM_CS_ZZQTZ
      (ZONENO, BRNO, CURRTYPE, PRODID, VALUE01, VALUE02,value03,value04)
      SELECT DW.ZONENO,
             DW.BRNO,
             DW.CURRTYPE,
             DW.EJCP,
             SUM(DW.ZQTZJE),
             SUM(DW.ECOST),
             SUM(dw.sumzqtzje),
             SUM(dw.sumecost)
        FROM DW_MOD_ZQTZZJB1 DW
       WHERE DW.DATA_DATE = P_I_DATE
         AND DW.CURRTYPE IN ('001', '992', '994')
       GROUP BY DW.ZONENO, DW.BRNO, DW.CURRTYPE, DW.EJCP;
    "$log"(33);COMMIT;
    "$log"(34);P_O_SUCCEED := '0';
    "$log"(35);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(36);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(37);ROLLBACK;
      "$log"(38);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(39);P_O_SUCCEED := '1';
      "$log"(40);V_ERR_CODE  := SQLCODE;
      "$log"(41);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(42);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(43);RAISE;
  END PROC_ZQTZ_DM;
END PACK_DEAL_ZQTZ_1005;
//

CREATE OR REPLACE PROCEDURE PROC_GEN_PRM_PRODUCT_SUBJECT (p_i_date IN VARCHAR2, p_o_succeed OUT VARCHAR2)
IS
   "$hits" aaa_coverage_tool.bool_tab;
   "$times" aaa_coverage_tool.bool_tab;
   v_proc_name    L_GETDATA_CALL.PROC_NAME%TYPE;
   v_log_level    L_GETDATA_CALL.LOG_LEVEL%TYPE;
   v_begin_time   L_GETDATA_CALL.BEGIN_TIME%TYPE;
   v_end_time     L_GETDATA_CALL.END_TIME%TYPE;
   v_err_code     L_GETDATA_ERR.ERR_CODE%TYPE;
   v_err_txt      L_GETDATA_ERR.ERR_TXT%TYPE;
   v_step_no      L_GETDATA_ERR.STEP_NO%TYPE;
   v_sql_txt      L_GETDATA_ERR.SQL_TXT%TYPE;
   v_fd_cnt       NUMBER;
   v_gd_cnt       NUMBER;

BEGIN
   v_step_no := '1';
   v_proc_name := 'PROC_GEN_PRM_PRODUCT_SUBJECT';
   v_log_level := '3';
   v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
   v_step_no := '2.1';
   SELECT COUNT ( * ) INTO v_fd_cnt FROM PRM_CS031 WHERE START_DATE = p_i_date;
   --生成法贷产品科目对应关系
   v_step_no := '2.2';
	  DELETE FROM PRM_PRODUCT_SUBJECT P WHERE P.PRODUCT_NAME='1';
	  INSERT INTO PRM_PRODUCT_SUBJECT (PRODUCT_ID, SUBJECT_CODE, PRODUCT_NAME, PRO_NAME)
         (SELECT DISTINCT A.PRODUCT_ID, B.SUBCODE, '1', C.PRODUCT_NAME FROM PRM_CS031 A, PRM_SUBJECT B, PRM_PRODUCT C
           WHERE A.SUBJECT_TYPE = 1
                   AND A.SUBJECT_CODE = SUBSTR (B.SUBCODE, 1, 4)
                   AND A.PRODUCT_ID = C.PRODUCT_ID
                   AND A.START_DATE <= p_i_date
                   AND A.END_DATE > p_i_date
                   AND B.START_DATE <= p_i_date
                   AND B.END_DATE > p_i_date
                   AND C.START_DATE <= p_i_date
                   AND C.END_DATE > p_i_date
                   AND NOT EXISTS
                         (SELECT * FROM PRM_CS031 WHERE  PRODUCT_ID = A.PRODUCT_ID
                                   AND SUBJECT_CODE = B.SUBCODE
                                   AND SUBJECT_APP = -1
                                   AND START_DATE <= p_i_date
                                   AND END_DATE > p_i_date)
          UNION ALL
          SELECT DISTINCT A.PRODUCT_ID, A.SUBJECT_CODE, '1', B.PRODUCT_NAME
            FROM PRM_CS031 A, PRM_PRODUCT B
           WHERE A.SUBJECT_TYPE = 2
                   AND A.SUBJECT_APP != -1
                   AND A.PRODUCT_ID = B.PRODUCT_ID
                   AND A.START_DATE <= p_i_date
                   AND A.END_DATE > p_i_date
                   AND B.START_DATE <= p_i_date
                   AND B.END_DATE > p_i_date);
   v_step_no := '2.3';
   SELECT COUNT ( * ) INTO v_gd_cnt FROM PRM_CS044 WHERE START_DATE = p_i_date;
   --生成个贷产品科目对应关系
   v_step_no := '2.4';
	  DELETE FROM PRM_PRODUCT_SUBJECT P  WHERE P.PRODUCT_NAME='2';
	  INSERT INTO PRM_PRODUCT_SUBJECT (PRODUCT_ID, SUBJECT_CODE, PRODUCT_NAME, PRO_NAME)
         (SELECT DISTINCT A.PRODUCT_ID, B.SUBCODE, '2', C.PRODUCT_NAME FROM PRM_CS044 A, PRM_SUBJECT B, PRM_PRODUCT C
           WHERE A.SUBJECT_TYPE = 1
                   AND A.SUBJECT_CODE = SUBSTR (B.SUBCODE, 1, 4)
                   AND A.PRODUCT_ID = C.PRODUCT_ID
                   AND A.START_DATE <= p_i_date
                   AND A.END_DATE > p_i_date
                   AND B.START_DATE <= p_i_date
                   AND B.END_DATE > p_i_date
                   AND C.START_DATE <= p_i_date
                   AND C.END_DATE > p_i_date
                   AND NOT EXISTS
                         (SELECT *
                            FROM PRM_CS044
                           WHERE PRODUCT_ID = A.PRODUCT_ID
                                   AND SUBJECT_CODE = B.SUBCODE
                                   AND SUBJECT_APP = -1
                                   AND START_DATE <= p_i_date
                                   AND END_DATE > p_i_date)
          UNION ALL
          SELECT DISTINCT A.PRODUCT_ID, A.SUBJECT_CODE, '2', B.PRODUCT_NAME
            FROM PRM_CS044 A, PRM_PRODUCT B
           WHERE A.SUBJECT_TYPE = 2
                   AND A.SUBJECT_APP != -1
                   AND A.PRODUCT_ID = B.PRODUCT_ID
                   AND A.START_DATE <= p_i_date
                   AND A.END_DATE > p_i_date
                   AND B.START_DATE <= p_i_date
                   AND B.END_DATE > p_i_date);
   COMMIT;
   p_o_succeed := '0';
   v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
EXCEPTION
   WHEN OTHERS THEN
	  ROLLBACK;
	  v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
	  p_o_succeed := '1';
	  v_err_code := SQLCODE;
	  v_err_txt := SUBSTR (SQLERRM, 1, 4000);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE (v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
	  RAISE;
END PROC_GEN_PRM_PRODUCT_SUBJECT;
//



CREATE OR REPLACE PACKAGE PACK_DEAL_BWCN_201604 IS
  PROCEDURE PROC_DEAL_BW_CAP_DETAIL(P_I_DATE    IN VARCHAR2,
                                    P_O_SUCCEED OUT VARCHAR2);

END PACK_DEAL_BWCN_201604;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_BWCN_201604 IS
    "$hits" aaa_coverage_tool.bool_tab; 
    "$times" aaa_coverage_tool.bool_tab;
	procedure "$log"(x integer) is
	  procedure log(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		update aaa_coverage_statements c
		   set hit = 1
		 where hit = 0
		   and cvr_id = log.id and stm_no = log.stm_no;
		commit;
	  end;
	  procedure logtime(id integer, stm_no integer) is
		pragma autonomous_transaction;
	  begin
		insert into aaa_coverage_statements_time(cvr_id, stm_no, c_time) values (logtime.id, logtime.stm_no, sysdate); 
		commit; 
	  end;
	begin
	  if "$hits" is null then 
		"$hits" := aaa_coverage_tool.get_statement_tab(54);
	  end if;
	  if "$times" is null then 
		--"$times" := aaa_coverage_tool.get_statement_time_tab(54);
		dbms_output.put_line('在aaa_coverage_tool源码中无get_statement_time_tab方法');
	  end if;
	  if x >= 1 and x <= "$hits".last and (not "$hits".exists(x) or "$hits"(x) is null) then 
		log(54,x); "$hits"(x):=true; 
	  end if;
	  if x >= 1 and x <= "$times".last and (not "$times".exists(x) or "$times"(x) is null) then 
		logtime(54,x); "$times"(x):=true; 
	  end if;
	end;
	
  PROCEDURE PROC_DEAL_BW_CAP_DETAIL(P_I_DATE    IN VARCHAR2,
                                    P_O_SUCCEED OUT VARCHAR2) IS
    V_XS PRM_XS051.XS_VALUE%TYPE;
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    "$log"(2);V_STEP_NO    := '1';
    "$log"(3);V_PROC_NAME  := 'PACK_DEAL_BWCN_201604.PROC_DEAL_BW_CAP_DETAIL';
    "$log"(4);V_LOG_LEVEL  := '3';
    "$log"(5);V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(6);V_STEP_NO    := '2.1';
    "$log"(7);
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME  FROM DUAL;
    "$log"(8);
	DELETE FROM ODS_GCMS_BW T
     WHERE T.TA280362007 NOT IN
           ('4060', '4061', '4020', '4050', '4010', '8044', '8089')
       AND T.TA280362007 NOT LIKE '91%'
       AND T.TA280362007 NOT LIKE '92%';
    "$log"(9);V_STEP_NO := '2.1';
    "$log"(10);SELECT MAX(XS_VALUE)
      INTO V_XS
      FROM PRM_XS051 PRM
     WHERE PRM.END_DATE = '99991231';
    "$log"(11);
	DELETE FROM DW_BW_CAP_DETAIL DW WHERE DW.DATA_DATE = P_I_DATE;
    "$log"(12);V_STEP_NO := '2.2';
  
    "$log"(13);
	INSERT INTO DW_BW_CAP_DETAIL
      (DATA_DATE,
       TA280362001,
       TA280362002,
       TA280362003,
       BOM_ID,
       CURR_TYPE,
       BALANCE,
       DB_BAL,
       ECOST)
      SELECT P_I_DATE,
             TT.CUST_CODE,
             TT.CONTR_NO,
             TT.DEPT_NO,
             TT.BOM_ID,
             TT.CURR_CODE,
             TT.BALANCE,
             TT.RMB_DB_BAL *
             CASE CURR_CODE
               WHEN '001' THEN
                1
               ELSE
                NVL(R2.RMBVRATE / DECODE(R1.RMBVRATE, 0, 1, R1.RMBVRATE), 1)
             END DB_BAL, --担保金额是已经折好的人民币，要折成与余额同一币种，不是折回担保的原币种
             GREATEST((TT.BALANCE -
                      TT.RMB_DB_BAL *
                      CASE CURR_CODE
                        WHEN '001' THEN
                         1
                        ELSE
                         NVL(R2.RMBVRATE /
                             DECODE(R1.RMBVRATE, 0, 1, R1.RMBVRATE),
                             1)
                      END),
                      0) * --净额负数处理成0
             NVL(XS_VALUE, V_XS) ECOST
        FROM (SELECT T1.TA280362000 DATA_DATE,
                     T1.TA280362001 CUST_CODE,
                     T1.TA280362002 CONTR_NO,
                     T1.TA280362003 DEPT_NO,
                     T1.TA280362005 CURR_CODE,
                     T1.TA280362006 BALANCE,
                     T1.TA280362007 BUSI_TYPE,
                     CASE
                       WHEN SUBSTR(T1.TA280362007, 1, 2) = '91' THEN
                        '232' --融资性保函
                       WHEN SUBSTR(T1.TA280362007, 1, 2) = '92' THEN
                        '233' --非融资性保函
                       WHEN T1.TA280362007 in ('4060', '4061') THEN --提货担保
                        '233'
                       WHEN T1.TA280362007 = '4020' THEN --承兑
                        '231' --银行承兑汇票
                       WHEN T1.TA280362007 in
                            ('4050', '4010', '8044', '8089') THEN --4050 4010 8044信用证 8089信用证保兑
                        '234' --信用证
                     END CAP_BUSI_TYPE,
                     T1.TA280362015 DB_CURR_CODE,
                     T1.TA280362016 RMB_DB_BAL,
                     T1.TA280362017 BOM_ID
                FROM ODS_GCMS_BW T1
               WHERE T1.TA280362000 = SUBSTR(P_I_DATE, 1, 6) --数据日期字段是月末日期，文件名的日期T+2
              UNION ALL
              SELECT T2.TM280362000 DATA_DATE,
                     T2.TM280362001 CUST_CODE,
                     T2.TM280362002 CONTR_NO,
                     T2.TM280362003 DEPT_NO,
                     T2.TM280362005 CURR_CODE,
                     T2.TM280362006 BALANCE,
                     T2.TM280362007 BUSI_TYPE,
                     '235' CAP_BUSI_TYPE, --贷款承诺
                     T2.TM280362015 DB_CURR_CODE,
                     T2.TM280362016 RMB_DB_BAL,
                     T2.TM280362017 BOM_ID
                FROM ODS_GCMS_CN T2
               WHERE T2.TM280362000 = P_I_DATE) TT
        LEFT JOIN (SELECT T.PRODUCT_NAME3, T.XS_VALUE
                     FROM PRM_XS051 T
                    WHERE T.END_DATE = '99991231') P
          ON TT.CAP_BUSI_TYPE = P.PRODUCT_NAME3
        LEFT JOIN (SELECT CURRENCY, RMBVRATE
                     --FROM RWA.PRM_RMBVRATE
                     FROM PRM_RMBVRATE
                    WHERE DATA_DATE = P_I_DATE) R1
          ON TT.CURR_CODE = R1.CURRENCY
        LEFT JOIN (SELECT MAX(RMBVRATE) RMBVRATE
                     --FROM RWA.PRM_RMBVRATE
                     FROM PRM_RMBVRATE
                    WHERE DATA_DATE = P_I_DATE
                      AND CURRENCY = '001') R2
          ON 1 = 1;
    "$log"(14);COMMIT;
    "$log"(15);P_O_SUCCEED := '0';  
    "$log"(16);V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    "$log"(17);
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      "$log"(18);ROLLBACK;
      "$log"(19);V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      "$log"(20);P_O_SUCCEED := '1';
      "$log"(21);V_ERR_CODE  := SQLCODE;
      "$log"(22);V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      "$log"(23);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      "$log"(24);RAISE;
  END PROC_DEAL_BW_CAP_DETAIL;
END PACK_DEAL_BWCN_201604;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_CZFX_1005 IS
  PROCEDURE PROC_DEAL_CZFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_CZFX_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  FUNCTION F_STRWITHDIVZERO(IS_EXPR IN VARCHAR2) RETURN VARCHAR2;
  PROCEDURE PROC_SQLBYEXPR(IS_EXPR     IN VARCHAR2,
                           IS_ZCFL_ID  IN VARCHAR2,
                           OS_SELECT   OUT LONG,
                           OS_EXPR     OUT LONG,
                           OS_WHERE    OUT LONG);
  FUNCTION F_SQL_MEASURE(IS_DATADATE     IN VARCHAR2) RETURN LONG;
  PROCEDURE PROC_CALC_EXESQL(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_CZFX_1005;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_CZFX_1005 IS
   PROCEDURE PROC_DEAL_CZFX(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_ZL     PRM_XSCZ2.PAR_COEF%TYPE; --总量调节系数
    V_YEAR_1 VARCHAR2(8); --去年
    V_YEAR_2 VARCHAR2(8); --前年
    V_YEAR_3 VARCHAR2(8); --大前年
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
	  IF SUBSTR(P_I_DATE, 5, 4) IN ('0331', '0630', '0930', '1231') THEN
      V_STEP_NO    := '1';
      V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_DEAL_CZFX';
      V_LOG_LEVEL  := '3';
      V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      V_STEP_NO    := '2.1';
      
	  SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
	  --DELETE FROM RWA.ods_sync_stat WHERE DATA_DATE = P_I_DATE AND TABLE_NAME = 'CPMG.ODS_TMP_NFWKPRF';
	  DELETE FROM ods_sync_stat WHERE DATA_DATE = P_I_DATE AND TABLE_NAME = 'CPMG.ODS_TMP_NFWKPRF';

	  --insert into RWA.ods_sync_stat (data_date, table_name, sync_stat, sync_time) values (P_I_DATE, 'CPMG.ODS_TMP_NFWKPRF', '1', SYSDATE);     
	  insert into ods_sync_stat (data_date, table_name, sync_stat, sync_time) values (P_I_DATE, 'CPMG.ODS_TMP_NFWKPRF', '1', to_date('20210318','yyyymmdd'));     
      V_STEP_NO := '2.1';
	  SELECT TO_CHAR(TO_NUMBER(SUBSTR(P_I_DATE, 1, 4)) - 1) || SUBSTR(P_I_DATE, 5, 4) INTO V_YEAR_1 FROM DUAL;
	  SELECT TO_CHAR(TO_NUMBER(SUBSTR(P_I_DATE, 1, 4)) - 2) || SUBSTR(P_I_DATE, 5, 4) INTO V_YEAR_2 FROM DUAL;
	  SELECT TO_CHAR(TO_NUMBER(SUBSTR(P_I_DATE, 1, 4)) - 3) || SUBSTR(P_I_DATE, 5, 4) INTO V_YEAR_3 FROM DUAL;
	  DELETE DW_MOD_CZFX DW WHERE DW.START_DATE >= P_I_DATE;
	  DELETE STG_DAT_CZFX stg WHERE stg.Data_DATE >= P_I_DATE;
	  INSERT INTO STG_BAK_NFWKPRF (ZONENO, BRNO, BANKFLAG, DATADATE, CURRTYPE, SUBCODE, AMOUNT, DATA_DATE)
        SELECT STG.ZONENO, STG.BRNO, '2', STG.DATA_DATE, STG.CURRTYPE, STG.SUBCODE, STG.INCOME, P_I_DATE
          FROM STG_DAT_CZFX STG
         WHERE STG.DATA_DATE < DECODE(SUBSTR(P_I_DATE, 5, 4), '1231', V_YEAR_2, V_YEAR_3);
	  DELETE FROM STG_DAT_CZFX STG WHERE STG.DATA_DATE < DECODE(SUBSTR(P_I_DATE, 5, 4), '1231', V_YEAR_2, V_YEAR_3);
	  INSERT INTO STG_DAT_CZFX (ZONENO, BRNO, CURRTYPE, SUBCODE, INCOME, DATA_DATE)
        SELECT ODS.ZONENO, ODS.BRNO, ODS.CURRTYPE, ODS.SUBNO, SUM(NVL(ODS.AMOUNT,0) / 100), P_I_DATE
          FROM ODS_TMP_NFWKPRF ODS GROUP BY ODS.ZONENO, ODS.BRNO, ODS.CURRTYPE, ODS.SUBNO;
	  SELECT PRM.PAR_COEF INTO V_ZL FROM PRM_XSCZ2 PRM WHERE PRM.END_DATE = '99991231';
	   EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_DAT_CZFX_TMP';
       PROC_CALC_EXESQL(P_I_DATE,P_O_SUCCEED);
      EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_TMP_CZFX';
      --计算操作风险经济资本
	  INSERT INTO STG_TMP_CZFX
        (ZONENO,
         BRNO,
         CURRTYPE,
         INCOME,
         DATA_DATE,
         PRODUCT_CODE,
         XS1,
         XS_ZL,
         XS_FH,
         XS_EC,
         EC,
         START_DATE,
         END_DATE)
        SELECT STG.ZONENO,
               STG.BRNO,
               STG.CURRTYPE,
               STG.INCOME_SUM,
               STG.DATA_DATE,
               STG.PRODUCT_CODE,
               PRX1.PAR_COEF,
               V_ZL,
               PRX3.PAR_COEF,
               PRX1.PAR_COEF * V_ZL * PRX3.PAR_COEF,
               nvl(STG.INCOME_SUM/3,0) * PRX1.PAR_COEF * V_ZL * PRX3.PAR_COEF,
               P_I_DATE,
               '99991231'
          FROM STG_DAT_CZFX_TMP STG,
               PRM_XSCZ1    PRX1,
               (SELECT A.ZONE_NO,
                       BELONG_ZONENO,
                       PAR_COEF,
                       START_DATE,
                       END_DATE
                  FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                          FROM PRM_IRA_ORGAN
                         WHERE BELONG_BANK_FLAG = '4'
                           AND START_DATE <= P_I_DATE
                           AND END_DATE > P_I_DATE) A,
                       PRM_XSCZ3 B
                 WHERE A.BELONG_ZONENO = B.ZONE_NO
                   AND B.START_DATE <= P_I_DATE) PRX3
         WHERE STG.PRODUCT_CODE = PRX1.ZCFL_ID
           AND P_I_DATE >= PRX1.START_DATE
           AND P_I_DATE < PRX1.END_DATE
           AND STG.ZONENO = PRX3.ZONE_NO;
      --插入DW表
	  INSERT INTO DW_MOD_CZFX
        (ZONENO,
         BRNO,
         CURRTYPE,
         INCOME,
         DATA_DATE,
         PRODUCT_CODE,
         XS1,
         XS_ZL,
         XS_FH,
         XS_EC,
         EC,
         START_DATE,
         END_DATE)
        SELECT ZONENO,
               BRNO,
               CURRTYPE,
               INCOME,
               DATA_DATE,
               PRODUCT_CODE,
               XS1,
               XS_ZL,
               XS_FH,
               XS_EC,
               EC,
               START_DATE,
               END_DATE
          FROM STG_TMP_CZFX;
      COMMIT;
      P_O_SUCCEED := '0';
      V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
    ELSE
      P_O_SUCCEED := '0';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
	  RAISE;
  END PROC_DEAL_CZFX;
  
  PROCEDURE PROC_CZFX_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2) IS
    V_FLAG NUMBER(1) := 3; --行级别
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    --每季末计算
	IF SUBSTR(P_I_DATE, 5, 4) IN ('0331', '0630', '0930', '1231') THEN
      V_STEP_NO    := '1';
      V_PROC_NAME  := 'PACK_DEAL_YHK_1005.PROC_CZFX_DM';
      V_LOG_LEVEL  := '3';
      V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      V_STEP_NO    := '2.1';
	  SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
	  EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_CZFX';
	  INSERT INTO DM_CS_CZFX(ZONENO, BRNO, BANKFLAG, CURRTYPE, PRODCODE, EC)
        SELECT DW.ZONENO,
               DW.BRNO,
               '2',
               DW.CURRTYPE,
               DW.PRODUCT_CODE,
               SUM(DECODE(SIGN(DW.EC),-1,0,DW.EC))
          FROM DW_MOD_CZFX DW
         WHERE DW.START_DATE = P_I_DATE
         GROUP BY DW.ZONENO, DW.BRNO, DW.CURRTYPE, DW.PRODUCT_CODE;
	  FOR V_FLAG IN 3 .. 5 LOOP
		MERGE INTO DM_CS_CZFX DM
        USING (SELECT PRM.BELONG_ZONENO ZONENO,
                      PRM.BELONG_BRNO BRNO,
                      PRM.BELONG_BANK_FLAG BANKFLAG,
                      DM.PRODCODE,
                      DM.CURRTYPE,
                      SUM(DM.EC) VALUE01
                 FROM DM_CS_CZFX DM, PRM_IRA_ORGAN PRM
                WHERE DM.ZONENO = PRM.ZONE_NO
                  AND DM.BRNO = PRM.BR_NO
                  AND DM.BANKFLAG = PRM.BANK_FLAG
                  AND PRM.BELONG_BANK_FLAG = V_FLAG
                  AND PRM.BANK_FLAG <> V_FLAG
                  AND P_I_DATE >= PRM.START_DATE
                  AND P_I_DATE < PRM.END_DATE
                GROUP BY PRM.BELONG_ZONENO,
                         PRM.BELONG_BRNO,
                         PRM.BELONG_BANK_FLAG,
                         DM.PRODCODE,
                         DM.CURRTYPE) TMP
        ON (DM.ZONENO = TMP.ZONENO AND DM.BRNO = TMP.BRNO AND DM.BANKFLAG = TMP.BANKFLAG AND DM.PRODCODE = TMP.PRODCODE AND DM.CURRTYPE = TMP.CURRTYPE)
        WHEN MATCHED THEN
          UPDATE SET DM.EC = DM.EC + TMP.VALUE01
        WHEN NOT MATCHED THEN
          INSERT
            (DM.ZONENO,
             DM.BRNO,
             DM.BANKFLAG,
             DM.PRODCODE,
             DM.CURRTYPE,
             DM.EC)
          VALUES
            (TMP.ZONENO,
             TMP.BRNO,
             TMP.BANKFLAG,
             TMP.PRODCODE,
             TMP.CURRTYPE,
             TMP.VALUE01);
      END LOOP;
	  INSERT INTO DM_CS_CZFX(ZONENO, BRNO, BANKFLAG, CURRTYPE, PRODCODE, EC)
        SELECT A.ZONENO,
               A.BRNO,
               A.BANKFLAG,
               '992',
               A.PRODCODE,
               SUM(A.EC * B.USCVRATE)
          FROM DM_CS_CZFX A, PRM_EXCHANGE_RATE B
         WHERE A.CURRTYPE <> '001'
           AND A.ZONENO = B.ZONE_NO
           AND A.CURRTYPE = B.CURRENCY
           AND P_I_DATE >= B.START_DATE
           AND P_I_DATE < B.END_DATE
         GROUP BY ZONENO, BRNO, BANKFLAG, PRODCODE;
	  INSERT INTO DM_CS_CZFX(ZONENO, BRNO, BANKFLAG, CURRTYPE, PRODCODE, EC)
        SELECT A.ZONENO,
               A.BRNO,
               A.BANKFLAG,
               '994',
               A.PRODCODE,
               SUM(DECODE(A.CURRTYPE, '001', A.EC, A.EC / B.USCVRATE))
          FROM DM_CS_CZFX A, PRM_EXCHANGE_RATE B
         WHERE A.CURRTYPE IN ('001', '992')
           AND A.ZONENO = B.ZONE_NO
           AND B.CURRENCY = '001'
           AND P_I_DATE >= B.START_DATE
           AND P_I_DATE < B.END_DATE
         GROUP BY ZONENO, BRNO, BANKFLAG, PRODCODE;
      COMMIT;
      P_O_SUCCEED := '0';
      V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
    ELSE
      P_O_SUCCEED := '0';
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END PROC_CZFX_DM;

  FUNCTION F_STRWITHDIVZERO(IS_EXPR IN VARCHAR2) RETURN VARCHAR2 IS
    FUNCTION RECURDIV(IS_SRC IN VARCHAR2, II_LEV IN INTEGER DEFAULT 0)
      RETURN VARCHAR2 IS
      I      INTEGER;
      IBRACE INTEGER;
      C      CHAR;
    BEGIN
      I := INSTR(IS_SRC, '/');
      IF II_LEV = 0 THEN
        IF I <= 0 THEN
          RETURN IS_SRC;
        ELSE
          RETURN SUBSTR(IS_SRC, 1, I) || RECURDIV(SUBSTR(IS_SRC, I + 1), II_LEV + 1);
        END IF;
      END IF;

      IF I > 0 THEN
        RETURN RECURDIV(SUBSTR(IS_SRC, 1, I - 1), II_LEV + 1) || '/' || RECURDIV(SUBSTR(IS_SRC, I + 1), II_LEV + 1);
      END IF;

      --除以常数，不做替换
      IF REGEXP_LIKE(IS_SRC, '^\.?\d+') THEN
        RETURN IS_SRC;
      END IF;

      IBRACE := 0;
      FOR I IN 1 .. LENGTH(IS_SRC) LOOP
        C := SUBSTR(IS_SRC, I, 1);
        IF C = '(' THEN
          IBRACE := IBRACE + 1;
        ELSIF C = ')' THEN
          IBRACE := IBRACE - 1;
        ELSIF C IN ('+', '-', '*') THEN
          IF IBRACE <= 0 THEN
            RETURN 'NULLIF(' || SUBSTR(IS_SRC, 1, I - 2) || ',0))' || SUBSTR(IS_SRC, I);
          END IF;
        END IF;
      END LOOP;
      RETURN 'NULLIF(' || SUBSTR(IS_SRC,1,length(IS_SRC)-1) || ',0)'||SUBSTR(IS_SRC,length(IS_SRC));
    END RECURDIV;
  BEGIN
    RETURN RECURDIV(IS_EXPR);
  END F_STRWITHDIVZERO;

  PROCEDURE PROC_SQLBYEXPR(IS_EXPR     IN VARCHAR2,
                           IS_ZCFL_ID  IN VARCHAR2,
                           OS_SELECT   OUT LONG,
                           OS_EXPR     OUT LONG,
                           OS_WHERE    OUT LONG) IS
    V_SEXP          LONG;
    V_SS            LONG;
    V_SSQL          LONG;
    V_SUBCODE       LONG;
    V_SPATT         LONG;
    V_SUBCODE_ORDER  NUMBER;
  BEGIN
    V_SEXP   := IS_EXPR;
    V_SEXP   := REGEXP_REPLACE(V_SEXP, '\s|"', '');
    V_SSQL   := '';
    V_SPATT := '\d{4,6}';
    V_SUBCODE_ORDER := 0;
    LOOP
      V_SS := REGEXP_SUBSTR(V_SEXP, V_SPATT);
      IF V_SS IS NULL THEN
        EXIT;
      END IF;
      V_SUBCODE      := REGEXP_SUBSTR(V_SS, '\d+{4,6}');
      V_SUBCODE_ORDER := V_SUBCODE_ORDER+1;
      IF length(V_SUBCODE)=6 THEN
        V_SSQL := V_SSQL || ',SUM(CASE WHEN SUBCODE = ''' || V_SUBCODE || ''' THEN NVL(INCOME,0) ELSE 0 END) CL_'||IS_ZCFL_ID || '_' || TO_CHAR(V_SUBCODE_ORDER);
      ELSE
      V_SSQL := V_SSQL || ',SUM(CASE WHEN SUBSTR(SUBCODE,1,4) = ''' || V_SUBCODE || ''' THEN NVL(INCOME,0) ELSE 0 END) CL_' ||IS_ZCFL_ID|| '_' || TO_CHAR(V_SUBCODE_ORDER);
      END IF;
      V_SEXP := REPLACE(V_SEXP, V_SS, 'CL_'||IS_ZCFL_ID || '_' || TO_CHAR(V_SUBCODE_ORDER));
    END LOOP;
    OS_EXPR   := F_STRWITHDIVZERO(V_SEXP);
    OS_SELECT := V_SSQL;
    OS_WHERE  := SUBSTR(OS_WHERE, 2);

  END PROC_SQLBYEXPR;

  FUNCTION F_SQL_MEASURE(IS_DATADATE IN VARCHAR2)
    RETURN LONG IS
    V_ALL_COLUMN_NAMES VARCHAR2(1000);
    V_SSQL_SG          LONG;
    V_SEXP_SG          LONG;
    V_SWHERE_SG        LONG;
    V_SWHERE           LONG := '';
    V_INSERT_SQL       LONG := '';
    V_SWHERE_SQL       LONG := '';
    V_CASE_SQL         LONG := '';
    V_ALL_INSERT_SQL   LONG := '';
  BEGIN
    V_ALL_COLUMN_NAMES := 'ZONENO,BRNO,CURRTYPE,PRODUCT_CODE,INCOME_SUM,DATA_DATE';
    --指标处理拼接顺序
    FOR V_KPI_GROUP_ROW IN (SELECT  ZCFL_ID,
                                    ZCFL_NAME,
                                    EXPRESSION,
                                    START_DATE,
                                    END_DATE
                              FROM PRM_CSCZ1 PRC
                             WHERE IS_DATADATE >= PRC.START_DATE
           AND IS_DATADATE < PRC.END_DATE)
     LOOP
      V_SEXP_SG :=''''||V_KPI_GROUP_ROW.ZCFL_ID||''''||','||V_SEXP_SG;
      V_INSERT_SQL := V_INSERT_SQL || ' INTO STG_DAT_CZFX_TMP' || '(' || V_ALL_COLUMN_NAMES || ')' || ' VALUES ' || '(' ||'ZONENO,BRNO,CURRTYPE'||','||V_SEXP_SG||','||'DATA_DATE'||')';
      V_SWHERE_SQL := V_SWHERE_SQL || ',' || V_SWHERE_SG;
      V_CASE_SQL   := V_CASE_SQL || TRIM(V_SSQL_SG);
    V_ALL_INSERT_SQL := ' INSERT ALL' || V_INSERT_SQL || ' SELECT ' || 'ZONENO,BRNO,CURRTYPE,DATA_DATE' || V_CASE_SQL || ' FROM STG_DAT_CZFX ' || V_SWHERE || 'GROUP BY ' || 'ZONENO,BRNO,CURRTYPE,DATA_DATE';
    END LOOP;
    RETURN V_ALL_INSERT_SQL;
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;

  PROCEDURE PROC_CALC_EXESQL (P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2)  IS
    VS_SQL       LONG;
    V_DT_COUNTER NUMBER(20);
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
  BEGIN
    VS_SQL := F_SQL_MEASURE(P_I_DATE);
    IF VS_SQL IS NOT NULL THEN
      EXECUTE IMMEDIATE VS_SQL;
      V_DT_COUNTER := SQL%ROWCOUNT;
      COMMIT;
     --INSERT INTO log_czfx_sql(MSG, EXESQL, BEGIN_DATE) VALUES (P_I_DATE||'更新了'||V_DT_COUNTER||'记录', VS_SQL, TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
     INSERT INTO log_czfx_sql(MSG, EXESQL, BEGIN_DATE) VALUES (P_I_DATE||'更新了'||V_DT_COUNTER||'记录', VS_SQL, to_date('20210317','yyyymmdd'));
    ELSE
     --INSERT INTO log_czfx_sql(MSG, EXESQL, BEGIN_DATE) VALUES (P_I_DATE||'更新了'||V_DT_COUNTER||'记录', VS_SQL, TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
     INSERT INTO log_czfx_sql(MSG, EXESQL, BEGIN_DATE) VALUES (P_I_DATE||'更新了'||V_DT_COUNTER||'记录', VS_SQL, to_date('20210318','yyyymmdd'));
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
      --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END;
END PACK_DEAL_CZFX_1005;
//



CREATE OR REPLACE PACKAGE PACK_DEAL_FRDK_1005 IS
  --ods进stg表stg表进dw表
  PROCEDURE PROC_DEAL_FRDK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  --dw表进dm表
  PROCEDURE PROC_FRDK_DM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  --恢复过程
  PROCEDURE PROC_RESUME_FRDK(P_I_DATE    IN VARCHAR2,
                             P_O_SUCCEED OUT VARCHAR2);

  --法贷特别产品处理
  FUNCTION FUNC_PRM_XFRA(I_DATEDATE IN VARCHAR2, IN_PRODUCT_ID IN VARCHAR2)
    RETURN NUMBER;

  --法人贷款信用风险产品调节系数表特殊处理
  FUNCTION FUNC_PRM_XSFR2(I_DATEDATE IN VARCHAR2,
                          IN_PRODUCT IN VARCHAR2,
                          IN_SUBCODE IN VARCHAR2,
                          IN_YWTYPE  IN VARCHAR2) RETURN NUMBER;

  --法人贷款科目转产品特殊处理
  FUNCTION FUNC_PRM_CS031(I_DATEDATE      IN VARCHAR2,
                          IN_SUBJECT_CODE IN VARCHAR2) RETURN VARCHAR2;
END PACK_DEAL_FRDK_1005;
//


CREATE OR REPLACE PACKAGE BODY pack_deal_frdk_1005
IS
   PROCEDURE proc_deal_frdk (p_i_date IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      --以下变量为记录日志时使用的。
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_month_day    NUMBER (3);                                   --当月天数
      --拆分业务种类变量
      v_num1         NUMBER;
      v_num2         NUMBER;
      v_pro_id       prm_xsfr2.product_id%TYPE;
      v_str1         prm_xsfr2.subject_all%TYPE;
      v_str1_old     prm_xsfr2.subject_all%TYPE;
      v_str2         prm_xsfr2.ywpz_all%TYPE;
      v_tmp_str1     prm_xsfr2.subject_all%TYPE;
      v_tmp_str2     prm_xsfr2.subject_all%TYPE;
      v_app_flag     prm_xsfr2.app_type%TYPE;
      v_par          prm_xsfr2.par_coef%TYPE;
      v_c32          prm_cs032.dbfs_id%TYPE;
      v_p32          prm_cs032.dbfs_lb%TYPE;
      v_x31          prm_xs031.xs_value%TYPE;

      CURSOR c
      IS                                 --获取该表的字段以及字段长度，类型等
         SELECT a.product_id, a.subject_all, a.ywpz_all, a.app_type,
                a.par_coef
           FROM prm_xsfr2 a
          WHERE a.start_date <= p_i_date AND a.end_date > p_i_date;
   BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PACK_DEAL_FRDK_1005.PROC_DEAL_FRDK';
      v_log_level := '3';
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';



      --去逐渐重复
       delete from ods_tmp_frdk2 a
  where a.rowid <>
  (select max(b.rowid) from ods_tmp_frdk2 b
    where a.loanno=b.loanno
      and a.loansqno=b.loansqno
      and a.accno=b.accno
      );
      commit;
      --月初月积数清0
      IF p_i_date = SUBSTR (p_i_date, 1, 6) || '01'
      THEN
         UPDATE cpmg.stg_dat_frdk t
            SET t.restamount_cm = 0,
                t.realboamt_cm = 0;

         COMMIT;
      END IF;

      ---BAK表备份当日日更新数据
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2';

      SELECT MAX (dbfs_id)
        INTO v_c32
        FROM cpmg.prm_cs032
       WHERE start_date <= p_i_date AND end_date > p_i_date;

      SELECT DISTINCT dbfs_lb
                 INTO v_p32
                 FROM cpmg.prm_cs032
                WHERE dbfs_id = v_c32
                  AND start_date <= p_i_date
                  AND end_date > p_i_date;

      SELECT MAX(xs_value)
      INTO v_x31
      FROM cpmg.prm_xs031
      WHERE start_date<=p_i_date
      AND end_date>p_i_date;

      EXECUTE IMMEDIATE 'TRUNCATE TABLE CPMG.prm_xsfr2_tmp';

      FOR v_c IN c
      LOOP
         v_pro_id := v_c.product_id;
         v_str1_old := v_c.subject_all;
         v_str2 := v_c.ywpz_all;
         v_app_flag := v_c.app_type;
         v_par := v_c.par_coef;

         --如果业务种类为单项的则只对科目号做截取；
         IF v_str2 = 'N/A' OR INSTR (v_str2, '+') = 0
         THEN
            SELECT   LENGTH ('+' || v_str1_old)
                   - LENGTH (REPLACE (REPLACE ('+' || v_str1_old, '+', ''),
                                      '-',
                                      ''
                                     )
                            )
              INTO v_num1
                        --需要循环的次数，字符串内科目元素的个数（包括负科目）
              FROM DUAL;

            v_str1 :=
                     REPLACE (REPLACE ('+' || v_str1_old, '+', ','), '-', ',');

            --所有-，+号都替换为“，”号，便于循环拆分
            FOR i IN 1 .. v_num1
            LOOP
               IF i = v_num1
               THEN
         --如果为最后一位，则直接从最后一位开始截起，否则则按科目长度截取区间
                  v_tmp_str1 :=
                              SUBSTR (v_str1, INSTR (v_str1, ',', 1, v_num1));

                  IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) =
                                                                          '-'
                  THEN
                     --如果科目为负，则剔除该科目
                     NULL;
                  ELSE
                     INSERT INTO prm_xsfr2_tmp
                                 (product_id, subject_code, ywpz_code,
                                  app_type, par_coef
                                 )
                          VALUES (v_pro_id, LTRIM (v_tmp_str1, ','),
                                                       --删除该字符前面的","号
                                                                    v_str2,
                                  v_app_flag, v_par
                                 );

                     COMMIT;
                  END IF;
               ELSE
                  --如果不为最后个科目号，则从该字符“，”出现i次处开始截取长度为该科目的字符
                  v_tmp_str1 :=
                     SUBSTR (v_str1,
                             INSTR (v_str1, ',', 1, i),
                               INSTR (v_str1, ',', 1, i + 1)
                             - INSTR (v_str1, ',', 1, i)
                            );

                  IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) =
                                                                           '-'
                  THEN
                     NULL;
                  ELSE
                     INSERT INTO prm_xsfr2_tmp
                                 (product_id, subject_code, ywpz_code,
                                  app_type, par_coef
                                 )
                          VALUES (v_pro_id, LTRIM (v_tmp_str1, ','), v_str2,
                                  v_app_flag, v_par
                                 );

                     COMMIT;
                  END IF;
               END IF;
            END LOOP;
         --如果业务品种也为xx+xx之类的类型，则分科目+业务品种进行循环
         ELSIF INSTR (v_str2, '+') <> 0
         THEN
            SELECT   LENGTH ('+' || v_str1_old)
                   - LENGTH (REPLACE (REPLACE ('+' || v_str1_old, '+', ''),
                                      '-',
                                      ''
                                     )
                            )
              INTO v_num1
              FROM DUAL;

            SELECT   LENGTH ('+' || v_str2)
                   - LENGTH (REPLACE (REPLACE ('+' || v_str2, '+', ''),
                                      '-',
                                      ''
                                     )
                            )
              INTO v_num2                             --求该科目需要循环的次数
              FROM DUAL;

            v_str1 :=
                     REPLACE (REPLACE ('+' || v_str1_old, '+', ','), '-', ',');
            v_str2 := REPLACE (REPLACE ('+' || v_str2, '+', ','), '-', ',');

            --
            FOR j IN 1 .. v_num2
            LOOP
               IF j = v_num2
               THEN
                  v_tmp_str2 :=
                     LTRIM (SUBSTR (v_str2, INSTR (v_str2, ',', 1, v_num2)),
                            ','
                           );
               ELSE
                  v_tmp_str2 :=
                     LTRIM (SUBSTR (v_str2,
                                    INSTR (v_str2, ',', 1, j),
                                      INSTR (v_str2, ',', 1, j + 1)
                                    - INSTR (v_str2, ',', 1, j)
                                   ),
                            ','
                           );
               END IF;

               FOR i IN 1 .. v_num1
               LOOP
                  IF i = v_num1
                  THEN
                     v_tmp_str1 :=
                              SUBSTR (v_str1, INSTR (v_str1, ',', 1, v_num1));

                     IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1,
                                1) = '-'
                     THEN
                        NULL;
                     ELSE
                        INSERT INTO prm_xsfr2_tmp
                                    (product_id, subject_code,
                                     ywpz_code, app_type, par_coef
                                    )
                             VALUES (v_pro_id, LTRIM (v_tmp_str1, ','),
                                     v_tmp_str2, v_app_flag, v_par
                                    );

                        COMMIT;
                     END IF;
                  ELSE
                     v_tmp_str1 :=
                        SUBSTR (v_str1,
                                INSTR (v_str1, ',', 1, i),
                                  INSTR (v_str1, ',', 1, i + 1)
                                - INSTR (v_str1, ',', 1, i)
                               );

                     IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) =
                                                                           '-'
                     THEN
                        NULL;
                     ELSE
                        INSERT INTO prm_xsfr2_tmp
                                    (product_id, subject_code,
                                     ywpz_code, app_type, par_coef
                                    )
                             VALUES (v_pro_id, LTRIM (v_tmp_str1, ','),
                                     v_tmp_str2, v_app_flag, v_par
                                    );

                        COMMIT;
                     END IF;
                  END IF;
               END LOOP;
            END LOOP;
         END IF;
      END LOOP;

      ------当日bak表清0
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.3';

      DELETE FROM cpmg.stg_bak_frdk t
            WHERE t.data_date = p_i_date;

      INSERT INTO cpmg.stg_bak_frdk
                  (zoneno, phybrno, loanno, loansqno, accno, lnmemno,
                   restamount, restamount_cm, currtype, subcode, buskind,
                   lnudflag, loanway, loandirt, leveltwl, levelfive, avchcof,
                   realboamt, realboamt_cm, surveyor, loandate, duedate,
                   custcode, custname, entrsize, creditlevel, groupcode,
                   groupname, industrycode, configrate, cust_exe_rate, income,
                   pd, lgd, status, modify_date, data_date, product_id,smallcorp,dubilstdrate,ratechgmode)
         SELECT s.zoneno, s.phybrno, s.loanno, s.loansqno, s.accno, s.lnmemno,
                s.restamount, s.restamount_cm, s.currtype, s.subcode,
                s.buskind, s.lnudflag, s.loanway, s.loandirt, s.leveltwl,
                s.levelfive, s.avchcof, s.realboamt, s.realboamt_cm,
                s.surveyor, s.loandate, s.duedate, s.custcode, s.custname,
                s.entrsize, s.creditlevel, s.groupcode, s.groupname,
                s.industrycode, s.configrate, s.cust_exe_rate, s.income, s.pd,
                s.lgd, s.status, s.modify_date, p_i_date, s.product_id,s.smallcorp,s.dubilstdrate,s.ratechgmode
           FROM cpmg.stg_dat_frdk s, cpmg.ods_tmp_frdk2 o
          WHERE s.loanno = o.loanno
            AND s.loansqno = o.loansqno
            AND s.accno = o.accno;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.4';

      --月初月积数清0
      IF p_i_date = SUBSTR (p_i_date, 1, 6) || '01'
      THEN
         UPDATE cpmg.stg_dat_frdk t
            SET t.restamount_cm = 0,
                t.realboamt_cm = 0;
      END IF;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.5';

      ------------如果不是月末对stg_dat_frdk表中法人贷款本日未有变动的日积数置为上日余额。
      IF p_i_date <>
       TO_CHAR(LAST_DAY(TO_DATE(p_i_date, 'yyyymmdd')), 'YYYYMMDD') THEN
      UPDATE stg_dat_frdk t
         SET t.restamount_cm = t.restamount_cm + t.restamount,
             t.realboamt_cm = t.realboamt_cm + t.realboamt
       WHERE NOT EXISTS (
                SELECT 1
                  FROM ods_tmp_frdk2 ods
                 WHERE t.loanno = ods.loanno
                   AND t.loansqno = ods.loansqno
                   AND t.accno = ods.accno);
     ELSE--如果为月末，则把balance也置为0
     UPDATE stg_dat_frdk t
         SET t.restamount_cm = t.restamount_cm + t.restamount,
             t.realboamt_cm = t.realboamt_cm + t.realboamt,
             t.restamount=0
       WHERE NOT EXISTS (
                SELECT 1
                  FROM ods_tmp_frdk2 ods
                 WHERE t.loanno = ods.loanno
                   AND t.loansqno = ods.loansqno
                   AND t.accno = ods.accno);
     END IF;
     --
      --更新STG表
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.6';
      MERGE INTO cpmg.stg_dat_frdk s
         USING (SELECT *
                  FROM ods_tmp_frdk2 o
                 WHERE SIGN (o.restamount) = -1) ods
         ON (    s.loanno = ods.loanno
             AND s.loansqno = ods.loansqno
             AND s.accno = ods.accno)
         WHEN MATCHED THEN
            UPDATE
               SET                         /*地区网点号转换 参见PRM_ORGANMAP*/
                  --S.ZONENO        = ODS.ZONENO,
                  s.zoneno =
                     CASE ods.zoneno || ods.phybrno
                        WHEN '13110400'
                           THEN '1318'
                        WHEN '13110380'
                           THEN '1318'
                        WHEN '13110430'
                           THEN '1318'
                        WHEN '13113228'
                           THEN '1318'
                        WHEN '23074881'
                           THEN '2321'
                        WHEN '23074921'
                           THEN '2321'
                        WHEN '23074931'
                           THEN '2321'
                        WHEN '23074891'
                           THEN '2321'
                        ELSE ods.zoneno
                     END,
                  s.phybrno = ods.phybrno, s.lnmemno = ods.lnmemno,

                  --帐户状态等于004 销户  008 作废  024 已核销  025 本利结清不计算经济资本，需保留月积数计算月均表 故不能删除
                  --补充说明 EDI接口没有做字典转换 现做如下筛选
                  --5-已核销呆账（坏账）,6-本利结清,8-作废
                  s.restamount =
                     DECODE (ods.status,
                             '5', 0,
                             '6', 0,
                             '8', 0,
                             NVL (ABS (DECODE (ods.lnudflag,
                                               '40001', ods.restamount,
                                               NULL, ods.restamount,
                                               0
                                              )
                                      ),
                                  0
                                 )
                            ),

                  --特殊处理 借据自定义标志LNUDFLAG=‘40001’或者为空的取直接取余额，否则则置零
                  --补充说明 EDI接口没有做字典转换 现做如下筛选
                  --5-已核销呆账（坏账）,6-本利结清,8-作废
                  s.restamount_cm =
                       ABS (s.restamount_cm)
                     + DECODE (ods.status,
                               '5', 0,
                               '6', 0,
                               '8', 0,
                               NVL (ABS (DECODE (ods.lnudflag,
                                                 '40001', ods.restamount,
                                                 NULL, ods.restamount,
                                                 0
                                                )
                                        ),
                                    0
                                   )
                              ),
                  s.currtype = ods.currtype, s.subcode = ods.subcode,
                  s.buskind = ods.buskind, s.lnudflag = ods.lnudflag,
                  s.loanway = NVL (ods.loanway, '1A01'),
                  s.loandirt = ods.loandirt,
                  s.leveltwl = NVL (ods.leveltwl, '10'),
                  s.levelfive = ods.levelfive,
                  s.avchcof = DECODE (ods.loanway, '2A01', 0, ods.avchcof),
                  s.realboamt = ods.realboamt,
                  s.realboamt_cm = s.realboamt_cm + ods.realboamt,
                  s.surveyor = ods.surveyor, s.loandate = ods.loandate,
                  s.duedate = ods.duedate, s.custcode = ods.custcode,
                  s.custname = ods.custname, s.entrsize = ods.entrsize,
                  s.creditlevel = ods.creditlevel,
                  s.groupcode = ods.groupcode, s.groupname = ods.groupname,
                  s.industrycode = NVL (ods.industrycode, 'null'),
                  s.configrate = ods.configrate,
                  s.cust_exe_rate = ods.cust_exe_rate,
                                      --20100421修改客户利率=基准利率+浮动利率
                                                      s.income = ods.income,
                  s.pd = ods.pd, s.lgd = DECODE (ods.lgd, -1, NULL, ods.lgd),
                  s.status = ods.status,
                  s.product_id = func_prm_cs031 (p_i_date, ods.subcode),
                  s.modify_date = p_i_date,
                  s.smallcorp=ods.small_corp,
                  s.dubilstdrate=ods.dubil_std_rate,
                  s.ratechgmode=ods.rate_chg_mode
         WHEN NOT MATCHED THEN
            INSERT (s.zoneno, s.phybrno, s.loanno, s.loansqno, s.accno,
                    s.lnmemno, s.restamount, s.restamount_cm, s.currtype,
                    s.subcode, s.buskind, s.lnudflag, s.loanway, s.loandirt,
                    s.leveltwl, s.levelfive, s.avchcof, s.realboamt,
                    s.realboamt_cm, s.surveyor, s.loandate, s.duedate,
                    s.custcode, s.custname, s.entrsize, s.creditlevel,
                    s.groupcode, s.groupname, s.industrycode, s.configrate,
                    s.cust_exe_rate, s.income, s.pd, s.lgd, s.status,
                    s.modify_date, s.product_id,s.smallcorp,s.dubilstdrate,s.ratechgmode)
            VALUES (                                             --ODS.ZONENO,
                    CASE ods.zoneno || ods.phybrno
                       WHEN '13110400'
                          THEN '1318'
                       WHEN '13110380'
                          THEN '1318'
                       WHEN '13110430'
                          THEN '1318'
                       WHEN '13113228'
                          THEN '1318'
                       WHEN '23074881'
                          THEN '2321'
                       WHEN '23074921'
                          THEN '2321'
                       WHEN '23074931'
                          THEN '2321'
                       WHEN '23074891'
                          THEN '2321'
                       ELSE ods.zoneno
                    END,
                    ods.phybrno, ods.loanno, ods.loansqno, ods.accno,
                    ods.lnmemno,

                    --帐户状态等于004 销户  008 作废  024 已核销  025 本利结清不计算经济资本，需保留月积数计算月均表 故不能删除
                    --补充说明 EDI接口没有做字典转换 现做如下筛选
                    --5-已核销呆账（坏账）,6-本利结清,8-作废
                    DECODE (ods.status,
                            '5', 0,
                            '6', 0,
                            '8', 0,
                            NVL (ABS (DECODE (ods.lnudflag,
                                              '40001', ods.restamount,
                                              NULL, ods.restamount,
                                              0
                                             )
                                     ),
                                 0
                                )
                           ),

                    --特殊处理 借据自定义标志LNUDFLAG=‘40001’或者为空的取直接取余额，否则则置零
                    --补充说明 EDI接口没有做字典转换 现做如下筛选
                    --5-已核销呆账（坏账）,6-本利结清,8-作废
                    DECODE (ods.status,
                            '5', 0,
                            '6', 0,
                            '8', 0,
                            NVL (ABS (DECODE (ods.lnudflag,
                                              '40001', ods.restamount,
                                              NULL, ods.restamount,
                                              0
                                             )
                                     ),
                                 0
                                )
                           ),
                    ods.currtype, ods.subcode, ods.buskind, ods.lnudflag,
                    NVL (ods.loanway, '1A01'), ods.loandirt,
                    NVL (ods.leveltwl, '10'), ods.levelfive,

                    --特殊处理 贷款方式LOANWAY值为=‘2A01’则把担保系数AVCHCOF置为0，否则则不变
                    DECODE (ods.loanway, '2A01', 0, ods.avchcof),
                    ods.realboamt, ods.realboamt, ods.surveyor, ods.loandate,
                    ods.duedate, ods.custcode, ods.custname, ods.entrsize,
                    ods.creditlevel, ods.groupcode, ods.groupname,
                    NVL (ods.industrycode, 'null'), ods.configrate,

                    /*ODS.STADRATE,
                    ODS.FLORATE,*/
                    ods.cust_exe_rate, ods.income, ods.pd,
                    DECODE (ods.lgd, -1, NULL, ods.lgd), ods.status, p_i_date,
                    func_prm_cs031 (p_i_date, ods.subcode),
                    ods.small_corp,
                    ods.dubil_std_rate,
                    ods.rate_chg_mode);
      /*-------全量更新月积表

      UPDATE STG_DAT_FRDK T
         SET T.RESTAMOUNT_CM = T.RESTAMOUNT_CM + T.RESTAMOUNT,
             T.REALBOAMT_CM  = T.REALBOAMT_CM + T.REALBOAMT,
             T.MODIFY_DATE = P_I_DATE;*/
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.7';

      SELECT   LAST_DAY (TO_DATE (p_i_date, 'yyyymmdd'))
             - TO_DATE (SUBSTR (p_i_date, 1, 6) || '01', 'yyyymmdd')
             + 1
        INTO v_month_day
        FROM DUAL;

      --全部删除 个人贷款模型结果表临时表 stg_tmp_grdkls中的数据
      EXECUTE IMMEDIATE 'TRUNCATE TABLE CPMG.STG_TMP_FRDK';

---------------------------------------------------
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.8';

      --个人贷款模型中间表关联参数表向个人贷款模型结果表临时表插入数据
      INSERT INTO cpmg.stg_tmp_frdk
                  (loanno, loansqno, accno, zoneno, brno, currtype, subcode,
                   balance, balance_cm, valueday, exhmatud, ta490461016,
                   ta490461016_cm, ta490461017, ta200261001, ta200261002,
                   ta200261004, ta200261037, ta200251013, ta200251011,
                   ta200251020, ta200011014, ta200011040, ta200011070,
                   ta200011005, ta200011016, ta200011034, ta340362013,
                   ta200268004, rzzd_fl, product_id3, product_name3, xyvalue,
                   xyname, dbdc_id, dbdc_name, dbfs_id, dbfs_lb, qxdc_id1,
                   qxdc_name1, qxdc_id2, qxdc_name2, xs031, xs034, xs033,
                   xs032, xs035, xs036, gjrz_xs, jjzb_xs, ytbbbl, ytbbj,
                   ytbbj_cm, dkamount, ecost, ecost_cm, start_date, end_date,
                   xs_product, ywzl, htsfsr, rate_today, cust_exe_rate, pd,
                   lgd, status,smallcorp,dubilstdrate,ratechgmode)
         SELECT /*+ordered use_hash(SDF PP C32 P36 P40 XF1 X32 C37 X33 X34)*/
                sdf.loanno, sdf.loansqno, sdf.accno, sdf.zoneno, sdf.phybrno,
                sdf.currtype, sdf.subcode, sdf.restamount, sdf.restamount_cm,
                sdf.loandate, sdf.duedate, sdf.realboamt, sdf.realboamt_cm,
                NULL,                                             --实提拨比例
                     sdf.custcode, sdf.lnmemno, sdf.accno, sdf.leveltwl, NULL,
                                                                    --贷款分类
                sdf.loanway, sdf.surveyor, sdf.industrycode, NULL,
                                                                --年初信用等级
                                                                  NULL,
                                                                --比照信用等级
                                                                       NULL,
                                                              --技术监督局代码
                sdf.entrsize, sdf.groupcode, sdf.avchcof, sdf.loandirt, NULL,
                                                    --国际贸易融资转贷款的分类
                sdf.product_id,                                 --三级产品名称
                               pp.product_name, sdf.creditlevel,      --直接取
                p40.dict_name, NULL,                            --担保档次编号
                                    NULL,                       --担保档次名称
                                         NVL (c32.dbfs_id, v_c32),
                                                              --贷款方式类别ID
                NVL (c32.dbfs_lb, v_p32),         --贷款方式类别 去租prm_cs032
                                         NULL,                      --贷款期限
                                              NULL,         --贷款期限档次名称
                                                   p36.qxdc_id,
                                                        --贷款剩余期限档次编号
                                                               p36.qxdc_name,
                                          --贷款剩余期限档次名称 取之prm_cs036
                NVL (xf1.par_coef, 1),                    --基本配置系数 xs031
                                      nvl (x34.xs_value, 1),
                                                            --行业地区调节系数
                                                              x33.par_coef,
                                                            --剩余期限调节系数
                x32.xs_value,                                   --质量调节系数
                             0,                         --贸易融资产品调节系数
                               0,                       --项目贷款产品调节系数
                                 0,               --国际融资转贷款产品调节系数

                --20100707修改，如果贷款方式为低风险质押，则基准系数直接取1.6%,如果总体系数<1.6%,也取1.6%
                DECODE
                   (SIGN
                       (  NVL
                             (pack_deal_frdk_1005.func_prm_xfra
                                                               (p_i_date,
                                                                sdf.product_id
                                                               ),
                                DECODE (c32.dbfs_id,
                                        '00001', 0.016,
                                        NVL (xf1.par_coef, 1)
                                       )
                              * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                              * x32.xs_value
                              * x33.par_coef
                              * nvl (x34.xs_value, 1)
                             )
                        - 0.016
                       ),
                    -1, 0.016,
                    NVL (pack_deal_frdk_1005.func_prm_xfra (p_i_date,
                                                            sdf.product_id
                                                           ),
                           DECODE (c32.dbfs_id,
                                   '00001', 0.016,
                                   NVL (xf1.par_coef, 1)
                                  )
                         * pack_deal_frdk_1005.func_prm_xsfr2 (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                         * x32.xs_value
                         * x33.par_coef
                         * nvl (x34.xs_value, 1)
                        )
                   ),
                c37.bbprecent,                                  --应提拨备比例
                              sdf.restamount * c37.bbprecent,     --应提拨备额
                sdf.restamount_cm * c37.bbprecent,          --应提拨备额月积数
                DECODE (SIGN (sdf.restamount - sdf.realboamt),
                        -1, 0,
                        (sdf.restamount - sdf.realboamt)
                       ),             --贷款净额为贷款余额-拨备额，小于零就取0

                  --20100707修改，如果贷款方式为低风险质押，则基准系数直接取1.6%,如果总体系数<1.6%,也取1.6%
                  sdf.restamount
                * DECODE
                     (SIGN
                         (  NVL
                               (pack_deal_frdk_1005.func_prm_xfra
                                                               (p_i_date,
                                                                sdf.product_id
                                                               ),
                                  DECODE (c32.dbfs_id,
                                          '00001', 0.016,
                                          NVL (xf1.par_coef, 1)
                                         )
                                * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                                * x32.xs_value
                                * x33.par_coef
                                * nvl (x34.xs_value, 1)
                               )
                          - 0.016
                         ),
                      -1, 0.016,
                      NVL
                         (pack_deal_frdk_1005.func_prm_xfra (p_i_date,
                                                             sdf.product_id
                                                            ),
                            DECODE (c32.dbfs_id,
                                    '00001', 0.016,
                                    NVL (xf1.par_coef, 1)
                                   )
                          * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                          * x32.xs_value
                          * x33.par_coef
                          * nvl (x34.xs_value, 1)
                         )
                     ),
                  sdf.restamount_cm
                * DECODE
                     (SIGN
                         (  NVL
                               (pack_deal_frdk_1005.func_prm_xfra
                                                               (p_i_date,
                                                                sdf.product_id
                                                               ),
                                  DECODE (c32.dbfs_id,
                                          '00001', 0.016,
                                          NVL (xf1.par_coef, 1)
                                         )
                                * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                                * x32.xs_value
                                * x33.par_coef
                                * nvl (x34.xs_value, 1)
                               )
                          - 0.016
                         ),
                      -1, 0.016,
                      NVL
                         (pack_deal_frdk_1005.func_prm_xfra (p_i_date,
                                                             sdf.product_id
                                                            ),
                            DECODE (c32.dbfs_id,
                                    '00001', 0.016,
                                    NVL (xf1.par_coef, 1)
                                   )
                          * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                          * x32.xs_value
                          * x33.par_coef
                          * LEAST (x34.xs_value, 1)
                         )
                     ),
                p_i_date, '99991231',
                pack_deal_frdk_1005.func_prm_xsfr2 (p_i_date,
                                                    sdf.product_id,
                                                    sdf.subcode,
                                                    sdf.buskind
                                                   ),           --产品调节系数
                sdf.buskind, sdf.income, sdf.configrate, sdf.cust_exe_rate,
                sdf.pd, sdf.lgd, sdf.status,sdf.smallcorp,sdf.dubilstdrate,sdf.ratechgmode
           FROM cpmg.stg_dat_frdk sdf,                    --法人贷款模型中间表
                --CPMG.PRM_CS031 C31, --法人贷款产品定义表
                cpmg.prm_product pp,                      --一二三级产品对照表
                cpmg.prm_cs032 c32,                   --法人贷款方式类别对照表
                cpmg.prm_cs036 p36,                   --法人贷款期限档次定义表

                cpmg.prm_da200011040 p40,                 --企业信用等级参数表
                (SELECT a.lgd_level, lower_limit, upper_limit, b.product_id,
                        b.credit_code, b.par_coef, b.start_date, b.end_date
                   FROM (SELECT t.seg_seq lgd_level, t.LOWER lower_limit,
                                NVL
                                   (LEAD (LOWER) OVER (ORDER BY TO_NUMBER
                                                                    (t.seg_seq)),
                                    999
                                   ) upper_limit
                           FROM cpmg.prm_csfr5 t
                          WHERE t.start_date <= p_i_date
                            AND t.end_date > p_i_date) a,
                        (SELECT *
                           FROM cpmg.prm_xsfr1
                          WHERE start_date <= p_i_date AND end_date > p_i_date) b
                  WHERE a.lgd_level = b.lgd_level) xf1,
                cpmg.prm_xs032 x32,                 --法人贷款风险质量调节系数
                cpmg.prm_cs037 c37,                   --法人贷款拨备比例参数表
                cpmg.prm_xsfr3 x33,
               --法人贷款信用风险期限调节系数表 修改成FD3 关联条件增加信用等级
                --法人贷款信用风险行业（地区）调节系数表
                 (SELECT zone_no,belong_zoneno,lineno,field_code,field_name,xs_value,start_date,end_date FROM (
SELECT DISTINCT zone_no, belong_zoneno
                            FROM prm_ira_organ
                           WHERE belong_bank_flag = '4'
                             AND start_date <= p_i_date
                             AND end_date > p_i_date)a,prm_xs034 b
                             WHERE a.belong_zoneno=b.area_code
                             AND b.start_date <= p_i_date
                             AND b.end_date > p_i_date) x34
          WHERE
                sdf.product_id = pp.product_id
            AND sdf.product_id = xf1.product_id(+)
            AND sdf.creditlevel = xf1.credit_code(+)
            --AND CF5.LGD_LEVEL = XF1.LGD_LEVEL --lgf档次关联
            AND sdf.lgd >= xf1.lower_limit(+)
            AND sdf.lgd < xf1.upper_limit(+)
            AND sdf.leveltwl = x32.fl12_id                      --12级分类关联
            AND x33.qxdc_id = p36.qxdc_id              --期限档次+信用等级关联
            AND sdf.creditlevel = x33.credit_code       --20100321新增关联条件
            AND   TO_DATE (sdf.duedate, 'YYYYMMDD')
                -                                           --期限档次参系数表
                  TO_DATE (p_i_date, 'YYYYMMDD') >
                   DECODE (p36.left_in,
                           '1', p36.left_value,
                           '2', p36.left_value - 1
                          )
            AND   TO_DATE (sdf.duedate, 'YYYYMMDD')
                - TO_DATE (p_i_date, 'YYYYMMDD') <
                   DECODE (p36.right_in,
                           '1', p36.right_value,
                           '2', p36.right_value + 1
                          )
            -- AND SUBSTR(SDF.ZONENO, 1, 2) || '00' = X34.AREA_CODE --地区和行业代码参数
            -- AND SDF.INDUSTRYCODE = X34.FIELD_CODE
            AND sdf.industrycode = x34.field_code(+)
            AND sdf.zoneno = x34.zone_no(+)
            AND sdf.creditlevel = p40.dict_code                 --企业信用等级
            AND sdf.loanway = c32.load_type(+)                      --贷款方式
            AND x32.fl12_id = c37.fl12_id                         --拨备系数表
            /*AND C31.START_DATE <= P_I_DATE
            AND C31.END_DATE > P_I_DATE*/
            AND pp.start_date <= p_i_date
            AND pp.end_date > p_i_date
            AND c32.start_date(+) <= p_i_date
            AND c32.end_date(+) > p_i_date
            AND c37.start_date <= p_i_date
            AND c37.end_date > p_i_date
            AND p36.start_date <= p_i_date
            AND p36.end_date > p_i_date
            AND p40.start_date <= p_i_date
            AND p40.end_date > p_i_date
            AND xf1.start_date(+) <= p_i_date
            AND xf1.end_date(+) > p_i_date
            AND x32.start_date <= p_i_date
            AND x32.end_date > p_i_date
            AND x33.start_date <= p_i_date
            AND x33.end_date > p_i_date
            AND x34.start_date(+) <= p_i_date
            AND x34.end_date(+) > p_i_date
            AND sdf.modify_date = p_i_date
            AND sdf.lgd IS NOT NULL
         UNION ALL
         SELECT /*+ordered use_hash(SDF PP C32 P36 P40 XF1 X32 C37 X33 X34)*/
                sdf.loanno, sdf.loansqno, sdf.accno, sdf.zoneno, sdf.phybrno,
                sdf.currtype, sdf.subcode, sdf.restamount, sdf.restamount_cm,
                sdf.loandate, sdf.duedate, sdf.realboamt, sdf.realboamt_cm,
                NULL,                                             --实提拨比例
                     sdf.custcode, sdf.lnmemno, sdf.accno, sdf.leveltwl, NULL,
                                                                    --贷款分类
                sdf.loanway, sdf.surveyor, sdf.industrycode, NULL,
                                                                --年初信用等级
                                                                  NULL,
                                                                --比照信用等级
                                                                       NULL,
                                                              --技术监督局代码
                sdf.entrsize, sdf.groupcode, sdf.avchcof, sdf.loandirt, NULL,
                                                    --国际贸易融资转贷款的分类
                sdf.product_id,                                 --三级产品名称
                               pp.product_name, sdf.creditlevel,      --直接取
                p40.dict_name, NULL,                            --担保档次编号
                                    NULL,                       --担保档次名称
                                         NVL (c32.dbfs_id, v_c32),
                                                              --贷款方式类别ID
                NVL (c32.dbfs_lb, v_p32),         --贷款方式类别 去租prm_cs032
                                         NULL,                      --贷款期限
                                              NULL,         --贷款期限档次名称
                                                   p36.qxdc_id,
                                                        --贷款剩余期限档次编号
                                                               p36.qxdc_name,
                                          --贷款剩余期限档次名称 取之prm_cs036
                nvl(xf1.par_coef,v_x31),                             --基本配置系数 xs031
                             nvl (x34.xs_value, 1),       --行业地区调节系数
                                                     NVL (x33.par_coef, 1),
                                                            --剩余期限调节系数
                x32.xs_value,                                   --质量调节系数
                             0,                         --贸易融资产品调节系数
                               0,                       --项目贷款产品调节系数
                                 0,               --国际融资转贷款产品调节系数
                DECODE
                   (SIGN
                       (  NVL
                             (pack_deal_frdk_1005.func_prm_xfra
                                                               (p_i_date,
                                                                sdf.product_id
                                                               ),
                                DECODE (c32.dbfs_id,
                                        '00001', 0.016,
                                        NVL (xf1.par_coef, v_x31)
                                       )
                              * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                              * x32.xs_value
                              * x33.par_coef
                              * nvl (x34.xs_value, 1)
                             )
                        - 0.016
                       ),
                    -1, 0.016,
                    NVL (pack_deal_frdk_1005.func_prm_xfra (p_i_date,
                                                            sdf.product_id
                                                           ),
                           DECODE (c32.dbfs_id,
                                   '00001', 0.016,
                                   NVL (xf1.par_coef, v_x31)
                                  )
                         * pack_deal_frdk_1005.func_prm_xsfr2 (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                         * x32.xs_value
                         * x33.par_coef
                         * nvl (x34.xs_value, 1)
                        )
                   ),
                c37.bbprecent,                                  --应提拨备比例
                              sdf.restamount * c37.bbprecent,     --应提拨备额
                sdf.restamount_cm * c37.bbprecent,          --应提拨备额月积数
                DECODE (SIGN (sdf.restamount - sdf.realboamt),
                        -1, 0,
                        (sdf.restamount - sdf.realboamt)
                       ),             --贷款净额为贷款余额-拨备额，小于零就取0
                  sdf.restamount
                * DECODE
                     (SIGN
                         (  NVL
                               (pack_deal_frdk_1005.func_prm_xfra
                                                               (p_i_date,
                                                                sdf.product_id
                                                               ),
                                  DECODE (c32.dbfs_id,
                                          '00001', 0.016,
                                          NVL (xf1.par_coef, v_x31)
                                         )
                                * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                                * x32.xs_value
                                * x33.par_coef
                                * nvl (x34.xs_value, 1)
                               )
                          - 0.016
                         ),
                      -1, 0.016,
                      NVL
                         (pack_deal_frdk_1005.func_prm_xfra (p_i_date,
                                                             sdf.product_id
                                                            ),
                            DECODE (c32.dbfs_id,
                                    '00001', 0.016,
                                    NVL (xf1.par_coef, v_x31)
                                   )
                          * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                          * x32.xs_value
                          * x33.par_coef
                          * nvl (x34.xs_value, 1)
                         )
                     ),
                  sdf.restamount_cm
                * DECODE
                     (SIGN
                         (  NVL
                               (pack_deal_frdk_1005.func_prm_xfra
                                                               (p_i_date,
                                                                sdf.product_id
                                                               ),
                                  DECODE (c32.dbfs_id,
                                          '00001', 0.016,
                                          NVL (xf1.par_coef, v_x31)
                                         )
                                * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                                * x32.xs_value
                                * x33.par_coef
                                * nvl (x34.xs_value, 1)
                               )
                          - 0.016
                         ),
                      -1, 0.016,
                      NVL
                         (pack_deal_frdk_1005.func_prm_xfra (p_i_date,
                                                             sdf.product_id
                                                            ),
                            DECODE (c32.dbfs_id,
                                    '00001', 0.016,
                                    NVL (xf1.par_coef, v_x31)
                                   )
                          * pack_deal_frdk_1005.func_prm_xsfr2
                                                              (p_i_date,
                                                               sdf.product_id,
                                                               sdf.subcode,
                                                               sdf.buskind
                                                              )
                          * x32.xs_value
                          * x33.par_coef
                          * nvl (x34.xs_value, 1)
                         )
                     ),
                p_i_date, '99991231',
                pack_deal_frdk_1005.func_prm_xsfr2 (p_i_date,
                                                    sdf.product_id,
                                                    sdf.subcode,
                                                    sdf.buskind
                                                   ),           --产品调节系数
                sdf.buskind, sdf.income, sdf.configrate, sdf.cust_exe_rate,
                sdf.pd, sdf.lgd, sdf.status,sdf.smallcorp,sdf.dubilstdrate,sdf.ratechgmode
           FROM cpmg.stg_dat_frdk sdf,                    --法人贷款模型中间表
                --CPMG.PRM_CS031 C31, --法人贷款产品定义表
                cpmg.prm_product pp,                      --一二三级产品对照表
                cpmg.prm_cs032 c32,                   --法人贷款方式类别对照表
                cpmg.prm_cs036 p36,                   --法人贷款期限档次定义表

                cpmg.prm_da200011040 p40,                 --企业信用等级参数表
                (SELECT b.product_id product_id, b.xyvalue credit_code,
                        b.dbdc_id dbdc_id, a.lower_limit, a.upper_limit,
                        b.xs_value par_coef
                   FROM (SELECT dbdc_id, dbdc_name,
                                DECODE (c35.left_in,
                                        '1', c35.left_value - 0.000000001,
                                        c35.left_value
                                       ) lower_limit,
                                DECODE (c35.right_in,
                                        '1', c35.right_value - 0.000000001,
                                        c35.right_value
                                       ) upper_limit
                           FROM prm_cs035 c35
                          WHERE c35.start_date <= p_i_date
                            AND c35.end_date > p_i_date) a,
                        (SELECT product_id, xyvalue, dbdc_id, xs_value
                           FROM prm_xs031
                          WHERE start_date <= p_i_date AND end_date > p_i_date) b
                  WHERE a.dbdc_id = b.dbdc_id) xf1,

                cpmg.prm_xs032 x32,                 --法人贷款风险质量调节系数
                cpmg.prm_cs037 c37,                   --法人贷款拨备比例参数表
                cpmg.prm_xsfr3 x33,
               --法人贷款信用风险期限调节系数表 修改成FD3 关联条件增加信用等级
                (SELECT zone_no,belong_zoneno,lineno,field_code,field_name,xs_value,start_date,end_date FROM (
SELECT DISTINCT zone_no, belong_zoneno
                            FROM prm_ira_organ
                           WHERE belong_bank_flag = '4'
                             AND start_date <= p_i_date
                             AND end_date > p_i_date)a,prm_xs034 b
                             WHERE a.belong_zoneno=b.area_code
                             AND b.start_date <= p_i_date
                             AND b.end_date > p_i_date) x34
          WHERE
                sdf.product_id = pp.product_id
            AND sdf.product_id = xf1.product_id(+)
            AND sdf.creditlevel = xf1.credit_code(+)
            --AND CF5.LGD_LEVEL = XF1.LGD_LEVEL --lgf档次关联
            AND sdf.avchcof >= xf1.lower_limit(+)
            AND sdf.avchcof <= xf1.upper_limit(+)
            AND sdf.leveltwl = x32.fl12_id                      --12级分类关联
            AND x33.qxdc_id = p36.qxdc_id              --期限档次+信用等级关联
            AND sdf.creditlevel = x33.credit_code       --20100321新增关联条件
            AND   TO_DATE (sdf.duedate, 'YYYYMMDD')
                -                                           --期限档次参系数表
                  TO_DATE (p_i_date, 'YYYYMMDD') >
                   DECODE (p36.left_in,
                           '1', p36.left_value,
                           '2', p36.left_value - 1
                          )
            AND   TO_DATE (sdf.duedate, 'YYYYMMDD')
                - TO_DATE (p_i_date, 'YYYYMMDD') <
                   DECODE (p36.right_in,
                           '1', p36.right_value,
                           '2', p36.right_value + 1
                          )
            -- AND SUBSTR(SDF.ZONENO, 1, 2) || '00' = X34.AREA_CODE --地区和行业代码参数
            -- AND SDF.INDUSTRYCODE = X34.FIELD_CODE
            AND sdf.industrycode = x34.field_code(+)
            AND sdf.zoneno = x34.zone_no(+)
            AND sdf.creditlevel = p40.dict_code                 --企业信用等级
            AND sdf.loanway = c32.load_type(+)                      --贷款方式
            AND x32.fl12_id = c37.fl12_id                         --拨备系数表
            /*AND C31.START_DATE <= P_I_DATE
            AND C31.END_DATE > P_I_DATE*/
            AND pp.start_date <= p_i_date
            AND pp.end_date > p_i_date
            AND c32.start_date(+) <= p_i_date
            AND c32.end_date(+) > p_i_date
            AND c37.start_date <= p_i_date
            AND c37.end_date > p_i_date
            AND p36.start_date <= p_i_date
            AND p36.end_date > p_i_date
            AND p40.start_date <= p_i_date
            AND p40.end_date > p_i_date
            AND x32.start_date <= p_i_date
            AND x32.end_date > p_i_date
            AND x33.start_date <= p_i_date
            AND x33.end_date > p_i_date
            AND x34.start_date(+) <= p_i_date
            AND x34.end_date(+) > p_i_date
            AND sdf.modify_date = p_i_date
            AND sdf.lgd IS NULL;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.9';


      --月末特殊处理
       IF p_i_date =
       TO_CHAR(LAST_DAY(TO_DATE(p_i_date, 'yyyymmdd')), 'YYYYMMDD') THEN
      ---删除增量的数据，只保留一份月末存量
      delete from dw_mod_frdk
       where start_date >= substr(p_i_date, 1, 6) || '01'
         and start_date < p_i_date;
      ---把上个月月末的数据的截止日期改为这个月末的前一天
      if substr(p_i_date,5,4)='0131'
      then
      update dw_mod_frdk dmf
         set end_date = TO_CHAR(TO_DATE(p_i_date, 'YYYYMMDD') - 1,
                                'YYYYMMDD')
       where start_date = substr(p_i_date,1,4)-1||'1231';
      else
      --------------------------
      update dw_mod_frdk dmf
         set end_date = TO_CHAR(TO_DATE(p_i_date, 'YYYYMMDD') - 1,
                                'YYYYMMDD')
       where start_date = TO_CHAR(LAST_DAY(TO_DATE(substr(p_i_date, 1, 6) - 1 || '01',
                                                   'yyyymmdd')),
                                  'YYYYMMDD');
                                  end if;
    end if;

      --加载dw_mod表
      UPDATE cpmg.dw_mod_frdk dwf
         SET dwf.end_date =
                       TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') - 1,
                                'YYYYMMDD')
       WHERE EXISTS (
                SELECT '1'
                  FROM cpmg.stg_tmp_frdk st
                 WHERE dwf.loanno = st.loanno
                   AND dwf.loansqno = st.loansqno
                   AND dwf.accno = st.accno)
         --AND DWF.CURRTYPE = ST.CURRTYPE) ??币种关联是否需要
         AND dwf.end_date = '99991231';

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '3.0';

      INSERT INTO cpmg.dw_mod_frdk
                  (loanno, loansqno, accno, zoneno, brno, currtype, subcode,
                   balance, valueday, exhmatud, ta490461016, ta490461017,
                   ta200261001, ta200261002, ta200261004, ta200261037,
                   ta200251013, ta200251011, ta200251020, ta200011014,
                   ta200011040, ta200011070, ta200011005, ta200011016,
                   ta200011034, ta340362013, ta200268004, rzzd_fl,
                   product_id3, product_name3, xyvalue, xyname, dbdc_id,
                   dbdc_name, dbfs_id, dbfs_lb, qxdc_id1, qxdc_name1,
                   qxdc_id2, qxdc_name2, xs031, xs034, xs033, xs032, xs035,
                   xs036, gjrz_xs, jjzb_xs, ytbbbl, ytbbj, dkamount, ecost,
                   start_date, end_date, xs_product, ywzl, htsfsr, rate_today,
                   cust_exe_rate, pd, lgd, balance_cm, ta490461016_cm,
                   ytbbj_cm, ecost_cm,small_corp,dubil_std_rate,rate_chg_mode,corp_name,croup_name)
         SELECT s.loanno, s.loansqno, s.accno, s.zoneno, s.brno, s.currtype,
                s.subcode, s.balance, s.valueday, s.exhmatud, s.ta490461016,
                s.ta490461017, s.ta200261001, s.ta200261002, s.ta200261004,
                s.ta200261037, s.ta200251013, s.ta200251011, s.ta200251020,
                s.ta200011014, s.ta200011040, s.ta200011070, s.ta200011005,
                s.ta200011016, s.ta200011034, s.ta340362013, s.ta200268004,
                s.rzzd_fl, s.product_id3, s.product_name3, s.xyvalue,
                s.xyname, s.dbdc_id, s.dbdc_name, s.dbfs_id, s.dbfs_lb,
                s.qxdc_id1, s.qxdc_name1, s.qxdc_id2, s.qxdc_name2, s.xs031,
                s.xs034, s.xs033, s.xs032, s.xs035, s.xs036, s.gjrz_xs,
                s.jjzb_xs, s.ytbbbl, s.ytbbj, s.dkamount, s.ecost,
                s.start_date, s.end_date, s.xs_product, s.ywzl, s.htsfsr,
                s.rate_today, s.cust_exe_rate, s.pd, s.lgd, s.balance_cm,
                s.ta490461016_cm, s.ytbbj_cm, s.ecost_cm,s.smallcorp,s.dubilstdrate,s.ratechgmode,s.custname,s.groupname
           FROM cpmg.stg_tmp_frdk s;

      COMMIT;
      p_o_succeed := '0';
      --记载数据处理日志
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');

   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);

         RAISE;
   END proc_deal_frdk;

   PROCEDURE proc_frdk_dm (p_i_date IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      --以下变量为记录日志时使用的。
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_flag         NUMBER (1)                       := 3;          --行级别
      v_month_day    NUMBER (3);
   BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PACK_DEAL_FRDK_1005.PROC_FRDK_DM';
      v_log_level := '3';
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';

      --读取系统时间记载开始处理时间
      SELECT TO_CHAR (SYSDATE, 'YYYYMMDDHH24MISS')
        INTO v_begin_time
        FROM DUAL;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2';

      --当月天数
      SELECT   LAST_DAY (TO_DATE (p_i_date, 'yyyymmdd'))
             - TO_DATE (SUBSTR (p_i_date, 1, 6) || '01', 'yyyymmdd')
             + 1
        INTO v_month_day
        FROM DUAL;

      --开始删除DM表中数据
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.3';

      EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_CS_ZFRDK';

      --把法人贷款数据插入dm表
      INSERT INTO dm_cs_zfrdk
                  (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                   loantype, hydm, corpgrade, value01, value02, value03,
                   value04, value05, value06, value07, value08, value09,
                   value010,smallcorp)
         SELECT   dw.zoneno, dw.brno, '0', dw.qxdc_id2,    --逻辑改动 剩余期限
                                                       dw.currtype,
                  NVL (dw.ta200261037, 'null'), dw.product_id3,
                  NVL (dw.dbfs_id, 'null'), NVL (dw.ta200011014, 'null'),
                  NVL (dw.xyvalue, 'null'), SUM (dw.balance),
                  decode(sign(SUM (dw.ta490461016)),-1,0,SUM (dw.ta490461016)),
                  decode(sign(SUM (dw.ytbbj)),-1,0,SUM (dw.ytbbj)),
                  decode(sign(SUM (dw.dkamount)),-1,0,SUM (dw.dkamount)),
                  decode(sign(SUM (dw.ecost)),-1,0,SUM (dw.ecost)),
                  decode(sign(SUM (dw.balance_cm)),-1,0,SUM (dw.balance_cm)) / v_month_day,                                             --日均贷款余额
                  decode(sign(SUM (dw.ta490461016_cm)),-1,0,SUM (dw.ta490461016_cm)) / v_month_day,      --实提拨备额日均
                  decode(sign(SUM (dw.ytbbj_cm)),-1,0,SUM (dw.ytbbj_cm)) / v_month_day,            --应提拨备额日均
                  decode(sign(SUM (dw.balance_cm-dw.ta490461016_cm)),-1,0,SUM (dw.balance_cm-dw.ta490461016_cm)) / v_month_day,
                  decode(sign(SUM (dw.ecost_cm)),-1,0,SUM(dw.ecost_cm)) / v_month_day,           --经济资本占用日均
                  small_corp
             FROM dw_mod_frdk dw
            WHERE dw.start_date <= p_i_date AND dw.end_date > p_i_date
         GROUP BY dw.zoneno,
                  dw.brno,
                  dw.qxdc_id2,
                  dw.currtype,
                  dw.ta200261037,
                  dw.product_id3,
                  dw.dbfs_id,
                  dw.ta200011014,
                  dw.xyvalue,
                  dw.small_corp;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.4';

      ---开始折币
      ---------------------开始外币折美元-----------------
      INSERT INTO dm_cs_zfrdk
                  (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                   loantype, hydm, corpgrade, value01, value02, value03,
                   value04, value05, value06, value07, value08, value09,
                   value010,smallcorp)
         SELECT   a.zoneno, a.brno, a.bankflag, a.qx, '992', a.cresort,
                  a.prodid, a.loantype, a.hydm, a.corpgrade,
                  SUM (a.value01 * b.uscvrate), SUM (a.value02 * b.uscvrate),
                  SUM (a.value03 * b.uscvrate), SUM (a.value04 * b.uscvrate),
                  SUM (a.value05 * b.uscvrate), SUM (a.value06 * b.uscvrate),
                  SUM (a.value07 * b.uscvrate), SUM (a.value08 * b.uscvrate),
                  SUM (a.value09 * b.uscvrate), SUM (a.value010 * b.uscvrate),
                  a.smallcorp
             FROM dm_cs_zfrdk a, prm_exchange_rate b
            WHERE a.currtype <> '001'
              AND a.zoneno = b.zone_no
              AND a.currtype = b.currency
              AND p_i_date >= b.start_date
              AND p_i_date < b.end_date
         GROUP BY a.zoneno,
                  a.brno,
                  a.bankflag,
                  a.qx,
                  a.cresort,
                  a.prodid,
                  a.loantype,
                  a.hydm,
                  a.corpgrade,
                  a.smallcorp;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.5';

      ----------------开始本外币折人民币----------------
      INSERT INTO dm_cs_zfrdk
                  (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                   loantype, hydm, corpgrade, value01, value02, value03,
                   value04, value05, value06, value07, value08, value09,
                   value010,smallcorp)
         SELECT   a.zoneno, a.brno, a.bankflag, a.qx, '994', a.cresort,
                  a.prodid, a.loantype, a.hydm, a.corpgrade,
                  SUM (DECODE (a.currtype,
                               '001', a.value01,
                               a.value01 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value02,
                               a.value02 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value03,
                               a.value03 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value04,
                               a.value04 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value05,
                               a.value05 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value06,
                               a.value06 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value07,
                               a.value07 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value08,
                               a.value08 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value09,
                               a.value09 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value010,
                               a.value010 / b.uscvrate
                              )
                      ),
                      smallcorp
             FROM dm_cs_zfrdk a, prm_exchange_rate b
            WHERE a.currtype IN ('001', '992')
              AND a.zoneno = b.zone_no
              AND b.currency = '001'
              AND p_i_date >= b.start_date
              AND p_i_date < b.end_date
         GROUP BY a.zoneno,
                  a.brno,
                  a.bankflag,
                  a.qx,
                  a.cresort,
                  a.prodid,
                  a.loantype,
                  a.hydm,
                  a.corpgrade,
                  a.smallcorp;

      --开始汇总
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.6';

      FOR v_flag IN 2 .. 5
      LOOP
         MERGE INTO dm_cs_zfrdk dm
            USING (SELECT   prm.belong_zoneno zoneno, prm.belong_brno brno,
                            prm.belong_bank_flag bankflag, dm.qx,
                            dm.currtype, dm.cresort, dm.prodid, dm.loantype,
                            dm.hydm, dm.corpgrade, SUM (dm.value01) value01,
                            SUM (dm.value02) value02,
                            SUM (dm.value03) value03,
                            SUM (dm.value04) value04,
                            SUM (dm.value05) value05,
                            SUM (dm.value06) value06,
                            SUM (dm.value07) value07,
                            SUM (dm.value08) value08,
                            SUM (dm.value09) value09,
                            SUM (dm.value010) value010,
                            dm.smallcorp
                       FROM dm_cs_zfrdk dm, prm_ira_organ prm
                      WHERE dm.zoneno = prm.zone_no
                        AND dm.brno = prm.br_no
                        AND dm.bankflag = prm.bank_flag
                        AND prm.belong_bank_flag = v_flag
                        AND prm.bank_flag <> v_flag
                        AND p_i_date >= prm.start_date
                        AND p_i_date < prm.end_date
                   GROUP BY prm.belong_zoneno,
                            prm.belong_brno,
                            prm.belong_bank_flag,
                            dm.qx,
                            dm.currtype,
                            dm.cresort,
                            dm.prodid,
                            dm.loantype,
                            dm.hydm,
                            dm.corpgrade,
                            dm.smallcorp) tmp
            ON (dm.zoneno = tmp.zoneno
            AND dm.brno = tmp.brno
            AND dm.bankflag = tmp.bankflag
            AND dm.qx = tmp.qx
            AND dm.currtype = tmp.currtype
            AND dm.cresort = tmp.cresort
            AND dm.prodid = tmp.prodid
            AND dm.loantype = tmp.loantype
            AND dm.hydm = tmp.hydm
            AND dm.corpgrade = tmp.corpgrade
            AND dm.smallcorp=tmp.smallcorp)
            WHEN MATCHED THEN
               UPDATE
                  SET dm.value01 = dm.value01 + tmp.value01,
                      dm.value02 = dm.value02 + tmp.value02,
                      dm.value03 = dm.value03 + tmp.value03,
                      dm.value04 = dm.value04 + tmp.value04,
                      dm.value05 = dm.value05 + tmp.value05,
                      dm.value06 = dm.value06 + tmp.value06,
                      dm.value07 = dm.value07 + tmp.value07,
                      dm.value08 = dm.value08 + tmp.value08,
                      dm.value09 = dm.value09 + tmp.value09,
                      dm.value010 = dm.value010 + tmp.value010
            WHEN NOT MATCHED THEN
               INSERT (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                       loantype, hydm, corpgrade, value01, value02, value03,
                       value04, value05, value06, value07, value08, value09,
                       value010,smallcorp)
               VALUES (tmp.zoneno, tmp.brno, tmp.bankflag, tmp.qx,
                       tmp.currtype, tmp.cresort, tmp.prodid, tmp.loantype,
                       tmp.hydm, tmp.corpgrade, tmp.value01, tmp.value02,
                       tmp.value03, tmp.value04, tmp.value05, tmp.value06,
                       tmp.value07, tmp.value08, tmp.value09, tmp.value010,tmp.smallcorp);
      END LOOP;

-----------
      COMMIT;
      p_o_succeed := '0';
--------------------------------------------------------------------------------------------
--结束，填写日志
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
         
         RAISE;
   END proc_frdk_dm;

   PROCEDURE proc_resume_frdk (p_i_date IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_count        VARCHAR2 (10);                                  --记录数
   BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PROC_RESUME_FRDK';
      v_log_level := '3';                                          --需要商定
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';

      --从模型结果表中删除当日日批错误数据
      DELETE FROM cpmg.dw_mod_frdk a
            WHERE a.start_date >= p_i_date;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2
    ';

      --将结束日期为恢复日-1的记录置为99991231
      UPDATE cpmg.dw_mod_frdk a
         SET a.end_date = '99991231'
       WHERE a.end_date =
                       TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') - 1,
                                'YYYYMMDD');

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.3';

      --从中间表中删除当日日批错误数据
      DELETE FROM cpmg.stg_dat_frdk a
            WHERE a.modify_date >= p_i_date;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.4';

      --判断错误恢复日期是否在备份表中存在
      SELECT COUNT (*)
        INTO v_count
        FROM cpmg.stg_bak_frdk a
       WHERE a.data_date = p_i_date;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.5';

      --回退BAK表当日修改的数据进STG_DAT表
      IF v_count <> 0
      THEN
         INSERT INTO cpmg.stg_dat_frdk
                     (zoneno, phybrno, loanno, loansqno, accno, lnmemno,
                      restamount, restamount_cm, currtype, subcode, buskind,
                      lnudflag, loanway, loandirt, leveltwl, levelfive,
                      avchcof, realboamt, realboamt_cm, surveyor, loandate,
                      duedate, custcode, custname, entrsize, creditlevel,
                      groupcode, groupname, industrycode, configrate,
                      cust_exe_rate, income, pd, lgd, status, modify_date,
                      product_id,smallcorp,dubilstdrate,ratechgmode)
            SELECT a.zoneno, a.phybrno, a.loanno, a.loansqno, a.accno,
                   a.lnmemno, a.restamount, a.restamount_cm, a.currtype,
                   a.subcode, a.buskind, a.lnudflag, a.loanway, a.loandirt,
                   a.leveltwl, a.levelfive, a.avchcof, a.realboamt,
                   a.realboamt_cm, a.surveyor, a.loandate, a.duedate,
                   a.custcode, a.custname, a.entrsize, a.creditlevel,
                   a.groupcode, a.groupname, a.industrycode, a.configrate,
                   a.cust_exe_rate, a.income, a.pd, a.lgd, a.status,
                   a.modify_date, a.product_id,a.smallcorp,a.dubilstdrate,a.ratechgmode
              FROM cpmg.stg_bak_frdk a
             WHERE a.data_date = p_i_date;

         COMMIT;
      END IF;

      p_o_succeed := '0';
      COMMIT;
      --记载数据处理日志
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
        
         RAISE;
   END proc_resume_frdk;

   FUNCTION func_prm_xfra (i_datedate IN VARCHAR2, in_product_id IN VARCHAR2)
      RETURN NUMBER
   IS
      n_par_coef   NUMBER;                               --特殊产品的资本系数
   BEGIN
      SELECT MAX (x.par_coef)
        INTO n_par_coef
        FROM cpmg.prm_xsfra x
       WHERE x.product_id = in_product_id
         AND x.start_date <= i_datedate
         AND x.end_date > i_datedate;

      IF n_par_coef IS NULL
      THEN
         RETURN NULL;
      ELSE
         RETURN n_par_coef;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;

   FUNCTION func_prm_xsfr2 (
      i_datedate   IN   VARCHAR2,
      in_product   IN   VARCHAR2,
      in_subcode   IN   VARCHAR2,
      in_ywtype    IN   VARCHAR2
   )
      RETURN NUMBER
   IS
      n_par_coef2   prm_xsfr2_tmp.par_coef%TYPE;
                                     --法人贷款信用风险产品调节系数表特殊处理
   --V_APP_FLAG  NUMBER;
   --V_YWPZ      VARCHAR2(10);
   BEGIN
      ---ods业务种类输入不为空 修改

      /*IF IN_YWTYPE IS NULL THEN
        SELECT MAX(X.PAR_COEF)
          INTO N_PAR_COEF2
          FROM CPMG.PRM_XSFR2 X
         WHERE X.PRODUCT_ID = IN_PRODUCT
           AND SUBSTR(X.SUBJECT_ALL, INSTR(X.SUBJECT_ALL, IN_SUBCODE) - 1, 1) != '-' --剔除负号的科目号
           AND INSTR(X.SUBJECT_ALL, IN_SUBCODE) <> 0 --科目号匹配
           AND X.START_DATE <= I_DATEDATE
           AND X.END_DATE > I_DATEDATE;

        IF N_PAR_COEF2 IS NULL THEN
          RETURN 1; --取不到置为常数
        ELSE
          RETURN N_PAR_COEF2;
        END IF;
      ELSE
        SELECT X.PAR_COEF
          INTO N_PAR_COEF2
          FROM CPMG.PRM_XSFR2 X
         WHERE X.PRODUCT_ID = IN_PRODUCT
           AND SUBSTR(X.SUBJECT_ALL, INSTR(X.SUBJECT_ALL, IN_SUBCODE) - 1, 1) != '-' --剔除负号的科目号
           AND INSTR(X.YWPZ_ALL, IN_YWTYPE) <> 0
           AND INSTR(X.SUBJECT_ALL, IN_SUBCODE) <> 0 --科目号匹配
           AND X.START_DATE <= I_DATEDATE
           AND X.END_DATE > I_DATEDATE;
        IF N_PAR_COEF2 IS NULL THEN
          RETURN 1;
        ELSE
          RETURN N_PAR_COEF2;
        END IF;
      END IF;*/

      /*SELECT MAX(X.APP_TYPE)
        INTO V_APP_FLAG
        FROM CPMG.PRM_XSFR2 X
       WHERE X.PRODUCT_ID = IN_PRODUCT
         AND SUBSTR(X.SUBJECT_ALL, INSTR(X.SUBJECT_ALL, IN_SUBCODE) - 1, 1) != '-'
         AND INSTR(X.SUBJECT_ALL, IN_SUBCODE) <> 0
         AND X.START_DATE <= I_DATEDATE
         AND X.END_DATE > I_DATEDATE;

      IF V_APP_FLAG = 1 THEN
        SELECT MAX(X.PAR_COEF)
          INTO N_PAR_COEF2
          FROM CPMG.PRM_XSFR2 X
         WHERE X.PRODUCT_ID = IN_PRODUCT
           AND SUBSTR(X.SUBJECT_ALL, INSTR(X.SUBJECT_ALL, IN_SUBCODE) - 1, 1) != '-' --剔除负号的科目号
           AND INSTR(X.SUBJECT_ALL, IN_SUBCODE) <> 0 --科目号匹配
           AND X.START_DATE <= I_DATEDATE
           AND X.END_DATE > I_DATEDATE;

        IF N_PAR_COEF2 IS NULL THEN
          RETURN 1; --取不到置为常数
        ELSE
          RETURN N_PAR_COEF2;
        END IF;

      ELSIF V_APP_FLAG = 2 THEN

        SELECT X.PAR_COEF
          INTO N_PAR_COEF2
          FROM CPMG.PRM_XSFR2 X
         WHERE X.PRODUCT_ID = IN_PRODUCT
           AND SUBSTR(X.SUBJECT_ALL, INSTR(X.SUBJECT_ALL, IN_SUBCODE) - 1, 1) != '-' --剔除负号的科目号
           AND INSTR(X.YWPZ_ALL, IN_YWTYPE) <> 0
           AND INSTR(X.SUBJECT_ALL, IN_SUBCODE) <> 0 --科目号匹配
           AND X.START_DATE <= I_DATEDATE
           AND X.END_DATE > I_DATEDATE;
        IF N_PAR_COEF2 IS NULL THEN
          RETURN 1;
        ELSE
          RETURN N_PAR_COEF2;
        END IF;

      ELSE
        RETURN 1;
      END IF;*/

      -----新建tmp表修改

      --按产品+科目+业务种类进行查询，如果没有结果集出来，说明此为NA，

      --全匹配
      SELECT MAX (x.par_coef)
        INTO n_par_coef2
        FROM cpmg.prm_xsfr2_tmp x
       WHERE     /*X.PRODUCT_ID = IN_PRODUCT
             AND*/ x.subject_code = in_subcode AND x.ywpz_code = in_ywtype;

      --科目截4位。
      IF n_par_coef2 IS NULL
      THEN
         SELECT MAX (x.par_coef)
           INTO n_par_coef2
           FROM cpmg.prm_xsfr2_tmp x
          WHERE     /*X.PRODUCT_ID = IN_PRODUCT
                AND*/ x.subject_code = SUBSTR (in_subcode, 1, 4)
            AND x.ywpz_code = in_ywtype;

         IF n_par_coef2 IS NULL
         THEN
            --Na类型，用产品+科目取出系数
            SELECT MAX (x.par_coef)
              INTO n_par_coef2
              FROM cpmg.prm_xsfr2_tmp x
             WHERE     /*X.PRODUCT_ID = IN_PRODUCT
                   AND*/ x.subject_code = in_subcode AND x.ywpz_code = 'N/A';
                                                                    --定死为NA

            IF n_par_coef2 IS NULL
            THEN
               --再没有定死为1
               RETURN 1;
            ELSE
               RETURN n_par_coef2;
            END IF;
         ELSE
            RETURN n_par_coef2;
         END IF;
      ELSE
         RETURN n_par_coef2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END;

   FUNCTION func_prm_cs031 (i_datedate IN VARCHAR2, in_subject_code IN VARCHAR2)
      RETURN VARCHAR2
   IS
      out_product   prm_cs031.product_id%TYPE;                              --
   BEGIN
      SELECT MAX (c31.product_id)
        INTO out_product
        FROM cpmg.prm_cs031 c31
       WHERE c31.subject_code = in_subject_code
         AND c31.subject_type = '2'
         AND c31.subject_app = 1
         AND c31.start_date <= i_datedate
         AND c31.end_date > i_datedate;

      IF out_product IS NULL
      THEN
         SELECT MAX (c31.product_id)
           INTO out_product
           FROM cpmg.prm_cs031 c31
          WHERE c31.subject_code = SUBSTR (in_subject_code, 1, 4)
            AND c31.subject_type = '1'
            AND c31.subject_app = 1
            AND c31.start_date <= i_datedate
            AND c31.end_date > i_datedate;

         RETURN out_product;
      ELSE
         RETURN out_product;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;
END pack_deal_frdk_1005;
//

CREATE OR REPLACE PACKAGE PACK_DEAL_FRDK_1005_P IS
  PROCEDURE PROC_DEAL_FRDK(P_I_DATE IN VARCHAR2,p_i_zoneno IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_FRDK_DM(P_I_DATE IN VARCHAR2,p_i_zoneno IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_RESUME_FRDK(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  FUNCTION FUNC_PRM_XFRA(I_DATEDATE IN VARCHAR2, IN_PRODUCT_ID IN VARCHAR2)
    RETURN NUMBER;
  FUNCTION FUNC_PRM_XSFR2(I_DATEDATE IN VARCHAR2,
                          IN_PRODUCT IN VARCHAR2,
                          IN_SUBCODE IN VARCHAR2,
                          IN_YWTYPE  IN VARCHAR2) RETURN NUMBER;  
  FUNCTION func_prm_dkzbpzb (i_level IN VARCHAR2) RETURN PRM_DKZBPZB.Value%TYPE;
  FUNCTION FUNC_PRM_CS031(I_DATEDATE      IN VARCHAR2,
                          IN_SUBJECT_CODE IN VARCHAR2) RETURN VARCHAR2;
  PROCEDURE PROC_DEAL_FRDK_BAK(P_I_DATE    IN VARCHAR2,
                               P_I_ZONENO  IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_FRDK_MAIN(P_I_DATE    IN VARCHAR2,
                                P_I_ZONENO  IN VARCHAR2,
                                P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_FRDK_MON(P_I_DATE    IN VARCHAR2,
                               P_I_ZONENO  IN VARCHAR2,
                               P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_FRDK_DM_SUM(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
  PROCEDURE PROC_DEAL_FRDK_PRETREAT(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_FRDK_1005_P;
//

CREATE OR REPLACE PACKAGE BODY pack_deal_frdk_1005_P IS
   PROCEDURE proc_deal_frdk (p_i_date IN VARCHAR2,p_i_zoneno IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_month_day    NUMBER (3);
      v_c32          prm_cs032.dbfs_id%TYPE;
      v_p32          prm_cs032.dbfs_lb%TYPE;
      v_x31          prm_xs031.xs_value%TYPE;
      v_par_coef     prm_xsfrdx.par_coef%TYPE;
      v_xqyxs        prm_xsfr6.PAR_COEF%TYPE;
    BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PACK_DEAL_FRDK_1005_P.PROC_DEAL_FRDK';
      v_log_level := '3';
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';
       ---BAK表备份当日日更新数据
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2';
      select nvl(MAX (x.par_coef),0.016) into v_par_coef from prm_xsfrdx x where  x.start_date <= P_I_DATE AND x.end_date > P_I_DATE;
      SELECT MAX (dbfs_id) INTO v_c32 FROM prm_cs032 WHERE start_date <= p_i_date AND end_date > p_i_date;
      SELECT DISTINCT dbfs_lb INTO v_p32 FROM prm_cs032 WHERE dbfs_id = v_c32 AND start_date <= p_i_date AND end_date > p_i_date;
      SELECT MAX(xs_value) INTO v_x31 FROM prm_xs031 WHERE start_date<=p_i_date AND end_date>p_i_date;
      SELECT MAX(PAR_COEF) INTO v_xqyxs FROM prm_xsfr6 WHERE start_date<=p_i_date AND end_date>p_i_date AND IS_SE='1';

     IF p_i_date = SUBSTR (p_i_date, 1, 6) || '01'
      THEN
         UPDATE stg_dat_frdk t SET t.restamount_cm = 0, t.realboamt_cm = 0 WHERE t.zone_par=p_i_zoneno;
      END IF;
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.3';

      UPDATE stg_dat_frdk t SET t.restamount_cm = t.restamount_cm + t.restamount, t.realboamt_cm =  t.realboamt_cm + t.realboamt
       WHERE NOT EXISTS ( SELECT 1 FROM ods_tmp_frdk2 ods WHERE t.loanno = ods.loanno AND t.loansqno = ods.loansqno AND t.accno = ods.accno AND ods.zone_par=t.zone_par) AND t.zone_par=p_i_zoneno;
       v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
       v_step_no := '2.4';
       v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
       v_step_no := '2.5';

      --更新STG表
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.6';
       MERGE/*+use_hash(s ods)*/ INTO stg_dat_frdk s
         USING (SELECT o.ZONENO,o.PHYBRNO,o.LOANNO,o.LOANSQNO,o.ACCNO,o.LNMEMNO,o.RESTAMOUNT,o.CURRTYPE,o.SUBCODE,o.BUSKIND,
                       o.LNUDFLAG,o.LOANWAY,o.LOANDIRT,o.LEVELTWL,o.LEVELFIVE,o.AVCHCOF,o.REALBOAMT,o.SURVEYOR,o.LOANDATE,o.DUEDATE,
                       o.CUSTCODE,o.CUSTNAME,o.ENTRSIZE,o.CREDITLEVEL,o.GROUPCODE,o.GROUPNAME,o.INDUSTRYCODE,o.SMALL_CORP,o.DUBIL_STD_RATE,
                       o.RATE_CHG_MODE,o.CONFIGRATE,o.CUST_EXE_RATE,o.INCOME,o.PD,o.LGD,o.STATUS,o.ZONE_PAR,P.PRODUCT_ID PRD,o.bnkrate
                  FROM ods_tmp_frdk2 o,PRM_PRODUCT_SUBJECT P
                 WHERE  O.SUBCODE=P.SUBJECT_CODE
                 AND P.PRODUCT_NAME='1'  ---1 法贷，2个贷
                AND o.zone_par=P_I_ZONENO) ods
         ON ( s.zone_par=p_i_zoneno
             AND s.loanno = ods.loanno
             AND s.loansqno = ods.loansqno
             AND s.accno = ods.accno)
         WHEN MATCHED THEN UPDATE
               SET s.zoneno = ods.zoneno,
                  s.phybrno = ods.phybrno, s.lnmemno = ods.lnmemno,
                  s.restamount = DECODE (ods.status, '5', 0, '6', 0, '8', 0, NVL (ABS (DECODE (ods.lnudflag, '40001', ods.restamount, NULL, ods.restamount,0)),0)),
                  s.restamount_cm = ABS (s.restamount_cm) + DECODE (ods.status, '5', 0, '6', 0, '8', 0, NVL (ABS (DECODE (ods.lnudflag, '40001', ods.restamount, NULL, ods.restamount, 0)),0)),
                  s.currtype = ods.currtype, 
				  s.subcode = ods.subcode,
                  s.buskind = ods.buskind, 
				  s.lnudflag = ods.lnudflag,
                  s.loanway = NVL (ods.loanway, '1A01'),
                  s.loandirt = ods.loandirt,
                  s.leveltwl = NVL (ods.leveltwl, '10'),
                  s.levelfive = ods.levelfive,
                  s.avchcof = DECODE (ods.loanway, '2A01', 0, ods.avchcof),
                  s.realboamt = ods.realboamt,
                  s.realboamt_cm = s.realboamt_cm + ods.realboamt,
                  s.surveyor = ods.surveyor, s.loandate = ods.loandate,
                  s.duedate = ods.duedate, s.custcode = ods.custcode,
                  s.custname = ods.custname, s.entrsize = ods.entrsize,
                  s.creditlevel = ods.creditlevel,
                  s.groupcode = ods.groupcode, s.groupname = ods.groupname,
                  s.industrycode = NVL (ods.industrycode, 'null'),
                  s.configrate = ods.configrate,
                  s.cust_exe_rate = ods.cust_exe_rate,
                  s.income = ods.income,
                  s.pd = ods.pd, s.lgd = DECODE (ods.lgd, -1, NULL, ods.lgd),
                  s.status = ods.status,
                  s.product_id = ods.prd,
                  s.modify_date = p_i_date,
                  s.smallcorp=ods.small_corp,
                  s.dubilstdrate=ods.dubil_std_rate,
                  s.ratechgmode=ods.rate_chg_mode,
                  s.bnkrate=ods.bnkrate
         WHEN NOT MATCHED THEN
            INSERT (s.zoneno, s.phybrno, s.loanno, s.loansqno, s.accno,
                    s.lnmemno, s.restamount, s.restamount_cm, s.currtype,
                    s.subcode, s.buskind, s.lnudflag, s.loanway, s.loandirt,
                    s.leveltwl, s.levelfive, s.avchcof, s.realboamt,
                    s.realboamt_cm, s.surveyor, s.loandate, s.duedate,
                    s.custcode, s.custname, s.entrsize, s.creditlevel,
                    s.groupcode, s.groupname, s.industrycode, s.configrate,
                    s.cust_exe_rate, s.income, s.pd, s.lgd, s.status,
                    s.modify_date, s.product_id,s.smallcorp,s.dubilstdrate,s.ratechgmode,s.zone_par,s.bnkrate)
            VALUES (ods.zoneno, ods.phybrno, ods.loanno, ods.loansqno, ods.accno, ods.lnmemno,
                    DECODE (ods.status, '5', 0, '6', 0, '8', 0, NVL (ABS (DECODE (ods.lnudflag, '40001', ods.restamount, NULL, ods.restamount, 0)),0)),
                    DECODE (ods.status, '5', 0, '6', 0, '8', 0, NVL (ABS (DECODE (ods.lnudflag, '40001', ods.restamount, NULL, ods.restamount, 0)),0)),
                    ods.currtype, ods.subcode, ods.buskind, ods.lnudflag,
                    NVL (ods.loanway, '1A01'), ods.loandirt,
                    NVL (ods.leveltwl, '10'), ods.levelfive,
                    DECODE (ods.loanway, '2A01', 0, ods.avchcof),
                    ods.realboamt, ods.realboamt, ods.surveyor, ods.loandate,
                    ods.duedate, ods.custcode, ods.custname, ods.entrsize,
                    ods.creditlevel, ods.groupcode, ods.groupname,
                    NVL (ods.industrycode, 'null'), ods.configrate,
                    ods.cust_exe_rate, ods.income, ods.pd,
                    DECODE (ods.lgd, -1, NULL, ods.lgd), ods.status, p_i_date,
                    ods.prd,
                    ods.small_corp,
                    ods.dubil_std_rate,
                    ods.rate_chg_mode,
                    P_I_ZONENO,
                    ods.bnkrate);
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.7';
      SELECT LAST_DAY (TO_DATE (p_i_date, 'yyyymmdd')) - TO_DATE (SUBSTR (p_i_date, 1, 6) || '01', 'yyyymmdd') + 1 INTO v_month_day FROM DUAL;

      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.8';
      UPDATE /*+full(dwf)*/ dw_mod_frdk dwf SET dwf.end_date = TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') - 1, 'YYYYMMDD')
       WHERE EXISTS ( SELECT '1' FROM ods_tmp_frdk2 st WHERE dwf.loanno = st.loanno AND dwf.loansqno = st.loansqno AND dwf.accno = st.accno AND st.zone_par=p_i_zoneno)
         AND dwf.end_date = '99991231'
         AND dwf.start_date >= TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(SUBSTR(p_i_date,1,6),'YYYYMM'),-1)),'YYYYMMDD')
         AND dwf.start_date< p_i_date
         AND dwf.ZONE_PAR=p_i_zoneno;
       --个人贷款模型中间表关联参数表向个人贷款模型结果表临时表插入数据
      INSERT INTO dw_mod_frdk
                  (loanno, loansqno, accno, zoneno, brno, currtype, subcode,
                   balance, balance_cm, valueday, exhmatud, ta490461016,
                   ta490461016_cm, ta490461017, ta200261001, ta200261002,
                   ta200261004, ta200261037, ta200251013, ta200251011,
                   ta200251020, ta200011014, ta200011040, ta200011070,
                   ta200011005, ta200011016, ta200011034, ta340362013,
                   ta200268004, rzzd_fl, product_id3, product_name3, xyvalue,
                   xyname, dbdc_id, dbdc_name, dbfs_id, dbfs_lb, qxdc_id1,
                   qxdc_name1, qxdc_id2, qxdc_name2, xs031, xs034, xs033,
                   xs032, xs035, xs036, gjrz_xs, jjzb_xs, ytbbbl, ytbbj,
                   ytbbj_cm, dkamount, ecost, ecost_cm, start_date, end_date,
                   xs_product, ywzl, htsfsr, rate_today, cust_exe_rate, pd,
                   lgd,lgd_level,small_corp,dubil_std_rate,rate_chg_mode,corp_name,croup_name,zone_par,bnkrate)
         SELECT /*+ordered use_hash(SDF PP C32 P36 C37 P40 XF1 X32 X33 X34)*/
         SDF.LOANNO,
         SDF.LOANSQNO,
         SDF.ACCNO,
         SDF.ZONENO,
         SDF.PHYBRNO,
         SDF.CURRTYPE,
         SDF.SUBCODE,
         SDF.RESTAMOUNT,
         SDF.RESTAMOUNT_CM,
         SDF.LOANDATE,
         SDF.DUEDATE,
         SDF.REALBOAMT,
         SDF.REALBOAMT_CM,
         NULL, --实提拨比例
         SDF.CUSTCODE,
         SDF.LNMEMNO,
         SDF.ACCNO,
         SDF.LEVELTWL,
         NULL,
         --贷款分类
         SDF.LOANWAY,
         SDF.SURVEYOR,
         SDF.INDUSTRYCODE,
         NULL,
         --年初信用等级
         NULL,
         --比照信用等级
         NULL,
         --技术监督局代码
         SDF.ENTRSIZE,
         SDF.GROUPCODE,
         SDF.AVCHCOF,
         SDF.LOANDIRT,
         NULL,
         --国际贸易融资转贷款的分类
         SDF.PRODUCT_ID, --三级产品名称
         PP.PRODUCT_NAME,
         SDF.CREDITLEVEL, --直接取
         P40.DICT_NAME,
         NULL, --担保档次编号
         NULL, --担保档次名称
         NVL(C32.DBFS_ID, V_C32),
         --贷款方式类别ID
         NVL(C32.DBFS_LB, V_P32), --贷款方式类别 去租prm_cs032
         NULL, --贷款期限
         NULL, --贷款期限档次名称
         P36.QXDC_ID,
         --贷款剩余期限档次编号
         P36.QXDC_NAME,
         --贷款剩余期限档次名称 取之prm_cs036
         NVL(XF1.PAR_COEF, 1), --基本配置系数 xs031
         NVL(X34.XS_VALUE, 1),--行业地区调节系数
         X33.PAR_COEF,--剩余期限调节系数
         X32.XS_VALUE, --质量调节系数
         0, --贸易融资产品调节系数
         0, --项目贷款产品调节系数
         0, --国际融资转贷款产品调节系数
         DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                         DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ) -
                     v_par_coef),
                -1,
                v_par_coef,
                NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                    DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                    decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                    )),
         C37.BBPRECENT, --应提拨备比例
         SDF.RESTAMOUNT * C37.BBPRECENT, --应提拨备额
         SDF.RESTAMOUNT_CM * C37.BBPRECENT, --应提拨备额月积数
         DECODE(SIGN(SDF.RESTAMOUNT - SDF.REALBOAMT),
                -1,
                0,
                (SDF.RESTAMOUNT - SDF.REALBOAMT)), --贷款净额为贷款余额-拨备额，小于零就取0
          --20100707修改，如果贷款方式为低风险质押，则基准系数直接取1.6%,如果总体系数<1.6%,也取1.6%
         SDF.RESTAMOUNT *
                  DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                         DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ) -
                     v_par_coef),
                -1,
                v_par_coef,
                NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                    DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                    decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                    )),
          SDF.RESTAMOUNT_CM *
                  DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                         DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ) -
                     v_par_coef),
                -1,
                v_par_coef,
                NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                    DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                    decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                    )),
          P_I_DATE,
         '99991231',
         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND),
         SDF.BUSKIND,
         SDF.INCOME,
         SDF.CONFIGRATE,
         SDF.CUST_EXE_RATE,
         SDF.PD,
         SDF.LGD,
         nvl(XF1.LGD_LEVEL,'999'),
         SDF.SMALLCORP,
         SDF.DUBILSTDRATE,
         SDF.RATECHGMODE,
         SDF.CUSTNAME,
         SDF.GROUPNAME,
         SDF.ZONE_PAR,
         SDF.BNKRATE
          FROM STG_DAT_FRDK SDF, --法人贷款模型中间表
               --PRM_CS031 C31, --法人贷款产品定义表
               PRM_PRODUCT PP, --一二三级产品对照表
               PRM_CS032 C32, --法人贷款方式类别对照表
               PRM_CS036 P36, --法人贷款期限档次定义表
               PRM_DA200011040 P40, --企业信用等级参数表
               (SELECT A.LGD_LEVEL,
                       LOWER_LIMIT,
                       UPPER_LIMIT,
                       B.PRODUCT_ID,
                       B.CREDIT_CODE,
                       B.PAR_COEF,
                       B.START_DATE,
                       B.END_DATE
                  FROM (SELECT T.SEG_SEQ LGD_LEVEL,
                               T.LOWER LOWER_LIMIT,
                               NVL(LEAD(LOWER) OVER(ORDER BY TO_NUMBER(T.SEG_SEQ)),
                                   999) UPPER_LIMIT
                          FROM PRM_CSFR5 T
                         WHERE T.START_DATE <= P_I_DATE
                           AND T.END_DATE > P_I_DATE) A,
                       (SELECT PRODUCT_ID,
                               CREDIT_CODE,
                               LGD_LEVEL,
                               PAR_COEF,
                               START_DATE,
                               END_DATE
                          FROM PRM_XSFR1
                         WHERE START_DATE <= P_I_DATE
                           AND END_DATE > P_I_DATE) B
                 WHERE A.LGD_LEVEL = B.LGD_LEVEL) XF1,
               PRM_XS032 X32, --法人贷款风险质量调节系数
               PRM_CS037 C37, --法人贷款拨备比例参数表
               PRM_XSFR3 X33,
               (SELECT ZONE_NO,
                       BELONG_ZONENO,
                       LINENO,
                       FIELD_CODE,
                       FIELD_NAME,
                       XS_VALUE,
                       START_DATE,
                       END_DATE
                  FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                          FROM PRM_IRA_ORGAN
                         WHERE BELONG_BANK_FLAG = '4'
                           AND START_DATE <= P_I_DATE
                           AND END_DATE > P_I_DATE) A,
                       PRM_XS034 B
                 WHERE A.BELONG_ZONENO = B.AREA_CODE
                   AND B.START_DATE <= P_I_DATE
                   AND B.END_DATE > P_I_DATE) X34
         WHERE SDF.PRODUCT_ID = PP.PRODUCT_ID
           AND SDF.PRODUCT_ID = XF1.PRODUCT_ID(+)
           AND SDF.CREDITLEVEL = XF1.CREDIT_CODE(+)
           AND SDF.LGD >= XF1.LOWER_LIMIT(+)
           AND SDF.LGD < XF1.UPPER_LIMIT(+)
           AND SDF.LEVELTWL = X32.FL12_ID --12级分类关联
           AND X33.QXDC_ID = P36.QXDC_ID --期限档次+信用等级关联
           AND SDF.CREDITLEVEL = X33.CREDIT_CODE --20100321新增关联条件
           AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') > DECODE(P36.LEFT_IN, '1', P36.LEFT_VALUE, '2', P36.LEFT_VALUE - 1)
           AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') < DECODE(P36.RIGHT_IN, '1', P36.RIGHT_VALUE, '2', P36.RIGHT_VALUE + 1)
           AND SDF.INDUSTRYCODE = X34.FIELD_CODE(+)
           AND SDF.ZONENO = X34.ZONE_NO(+)
           AND SDF.CREDITLEVEL = P40.DICT_CODE --企业信用等级
           AND SDF.LOANWAY = C32.LOAD_TYPE(+) --贷款方式
           AND X32.FL12_ID = C37.FL12_ID --拨备系数表
           AND PP.START_DATE <= P_I_DATE
           AND PP.END_DATE > P_I_DATE
           AND C32.START_DATE(+) <= P_I_DATE
           AND C32.END_DATE(+) > P_I_DATE
           AND C37.START_DATE <= P_I_DATE
           AND C37.END_DATE > P_I_DATE
           AND P36.START_DATE <= P_I_DATE
           AND P36.END_DATE > P_I_DATE
           AND P40.START_DATE <= P_I_DATE
           AND P40.END_DATE > P_I_DATE
           AND XF1.START_DATE(+) <= P_I_DATE
           AND XF1.END_DATE(+) > P_I_DATE
           AND X32.START_DATE <= P_I_DATE
           AND X32.END_DATE > P_I_DATE
           AND X33.START_DATE <= P_I_DATE
           AND X33.END_DATE > P_I_DATE
           AND X34.START_DATE(+) <= P_I_DATE
           AND X34.END_DATE(+) > P_I_DATE
           AND SDF.MODIFY_DATE = P_I_DATE
           AND SDF.LGD IS NOT NULL
           AND SDF.ZONE_PAR = P_I_ZONENO
         UNION ALL
        SELECT /*+ordered use_hash(SDF PP C32 P36 C37 P40 XF1 X32 X33 X34)*/
         SDF.LOANNO,
         SDF.LOANSQNO,
         SDF.ACCNO,
         SDF.ZONENO,
         SDF.PHYBRNO,
         SDF.CURRTYPE,
         SDF.SUBCODE,
         SDF.RESTAMOUNT,
         SDF.RESTAMOUNT_CM,
         SDF.LOANDATE,
         SDF.DUEDATE,
         SDF.REALBOAMT,
         SDF.REALBOAMT_CM,
         NULL, --实提拨比例
         SDF.CUSTCODE,
         SDF.LNMEMNO,
         SDF.ACCNO,
         SDF.LEVELTWL,
         NULL,
         --贷款分类
         SDF.LOANWAY,
         SDF.SURVEYOR,
         SDF.INDUSTRYCODE,
         NULL,
         --年初信用等级
         NULL,
         --比照信用等级
         NULL,
         --技术监督局代码
         SDF.ENTRSIZE,
         SDF.GROUPCODE,
         SDF.AVCHCOF,
         SDF.LOANDIRT,
         NULL,
         --国际贸易融资转贷款的分类
         SDF.PRODUCT_ID, --三级产品名称
         PP.PRODUCT_NAME,
         SDF.CREDITLEVEL, --直接取
         P40.DICT_NAME,
         NULL, --担保档次编号
         NULL, --担保档次名称
         NVL(C32.DBFS_ID, V_C32),
         --贷款方式类别ID
         NVL(C32.DBFS_LB, V_P32), --贷款方式类别 去租prm_cs032
         NULL, --贷款期限
         NULL, --贷款期限档次名称
         P36.QXDC_ID,
         --贷款剩余期限档次编号
         P36.QXDC_NAME,
         --贷款剩余期限档次名称 取之prm_cs036
         NVL(XF1.PAR_COEF, V_X31), --基本配置系数 xs031
         NVL(X34.XS_VALUE, 1), --行业地区调节系数
         NVL(X33.PAR_COEF, 1),
         --剩余期限调节系数
         X32.XS_VALUE, --质量调节系数
         0, --贸易融资产品调节系数
         0, --项目贷款产品调节系数
         0, --国际融资转贷款产品调节系数
         DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                         DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ) -
                     v_par_coef),
                -1,
                v_par_coef,
                NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                    DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                    decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                    )),
         C37.BBPRECENT, --应提拨备比例
         SDF.RESTAMOUNT * C37.BBPRECENT, --应提拨备额
         SDF.RESTAMOUNT_CM * C37.BBPRECENT, --应提拨备额月积数
         DECODE(SIGN(SDF.RESTAMOUNT - SDF.REALBOAMT),
                -1,
                0,
                (SDF.RESTAMOUNT - SDF.REALBOAMT)), --贷款净额为贷款余额-拨备额，小于零就取0
         (SDF.RESTAMOUNT ) *
         DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                         DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ) -
                     v_par_coef),
                -1,
                v_par_coef,
                NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                    DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                    decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                    )),
         (SDF.RESTAMOUNT_CM ) *
         DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                         DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ) -
                     v_par_coef),
                -1,
                v_par_coef,
                NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                    DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                    decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                            SDF.PRODUCT_ID,
                                                            SDF.SUBCODE,
                                                            SDF.BUSKIND)
                         *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                    )),
         P_I_DATE,
         '99991231',
         PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND),
         SDF.BUSKIND,
         SDF.INCOME,
         SDF.CONFIGRATE,
         SDF.CUST_EXE_RATE,
         SDF.PD,
         SDF.LGD,
         '999' LGD_LEVEL,
         SDF.SMALLCORP,
         SDF.DUBILSTDRATE,
         SDF.RATECHGMODE,
         SDF.CUSTNAME,
         SDF.GROUPNAME,
         SDF.ZONE_PAR,
         SDF.BNKRATE
          FROM STG_DAT_FRDK SDF, --法人贷款模型中间表
               PRM_PRODUCT PP, --一二三级产品对照表
               PRM_CS032 C32, --法人贷款方式类别对照表
               PRM_CS036 P36, --法人贷款期限档次定义表
               PRM_DA200011040 P40, --企业信用等级参数表
               (SELECT B.PRODUCT_ID PRODUCT_ID,
                       B.XYVALUE CREDIT_CODE,
                       B.DBDC_ID DBDC_ID,
                       A.LOWER_LIMIT,
                       A.UPPER_LIMIT,
                       B.XS_VALUE PAR_COEF
                  FROM (SELECT DBDC_ID,
                               DBDC_NAME,
                               DECODE(C35.LEFT_IN,
                                      '1',
                                      C35.LEFT_VALUE - 0.000000001,
                                      C35.LEFT_VALUE) LOWER_LIMIT,
                               DECODE(C35.RIGHT_IN,
                                      '1',
                                      C35.RIGHT_VALUE - 0.000000001,
                                      C35.RIGHT_VALUE) UPPER_LIMIT
                          FROM PRM_CS035 C35
                         WHERE C35.START_DATE <= P_I_DATE
                           AND C35.END_DATE > P_I_DATE) A,
                       (SELECT PRODUCT_ID, XYVALUE, DBDC_ID, XS_VALUE
                          FROM PRM_XS031
                         WHERE START_DATE <= P_I_DATE
                           AND END_DATE > P_I_DATE) B
                 WHERE A.DBDC_ID = B.DBDC_ID) XF1,
               PRM_XS032 X32, --法人贷款风险质量调节系数
               PRM_CS037 C37, --法人贷款拨备比例参数表
               PRM_XSFR3 X33,
               (SELECT ZONE_NO,
                       BELONG_ZONENO,
                       LINENO,
                       FIELD_CODE,
                       FIELD_NAME,
                       XS_VALUE,
                       START_DATE,
                       END_DATE
                  FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                          FROM PRM_IRA_ORGAN
                         WHERE BELONG_BANK_FLAG = '4'
                           AND START_DATE <= P_I_DATE
                           AND END_DATE > P_I_DATE) A,
                       PRM_XS034 B
                 WHERE A.BELONG_ZONENO = B.AREA_CODE
                   AND B.START_DATE <= P_I_DATE
                   AND B.END_DATE > P_I_DATE) X34
         WHERE SDF.PRODUCT_ID = PP.PRODUCT_ID
           AND SDF.PRODUCT_ID = XF1.PRODUCT_ID(+)
           AND SDF.CREDITLEVEL = XF1.CREDIT_CODE(+)
              --AND CF5.LGD_LEVEL = XF1.LGD_LEVEL --lgf档次关联
           AND SDF.AVCHCOF >= XF1.LOWER_LIMIT(+)
           AND SDF.AVCHCOF <= XF1.UPPER_LIMIT(+)
           AND SDF.LEVELTWL = X32.FL12_ID --12级分类关联
           AND X33.QXDC_ID = P36.QXDC_ID --期限档次+信用等级关联
           AND SDF.CREDITLEVEL = X33.CREDIT_CODE --20100321新增关联条件
           AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - --期限档次参系数表
               TO_DATE(P_I_DATE, 'YYYYMMDD') >
               DECODE(P36.LEFT_IN, '1', P36.LEFT_VALUE, '2', P36.LEFT_VALUE - 1)
           AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') <
               DECODE(P36.RIGHT_IN, '1', P36.RIGHT_VALUE, '2', P36.RIGHT_VALUE + 1)
           AND SDF.INDUSTRYCODE = X34.FIELD_CODE(+)
           AND SDF.ZONENO = X34.ZONE_NO(+)
           AND SDF.CREDITLEVEL = P40.DICT_CODE --企业信用等级
           AND SDF.LOANWAY = C32.LOAD_TYPE(+) --贷款方式
           AND X32.FL12_ID = C37.FL12_ID --拨备系数表
           AND PP.START_DATE <= P_I_DATE
           AND PP.END_DATE > P_I_DATE
           AND C32.START_DATE(+) <= P_I_DATE
           AND C32.END_DATE(+) > P_I_DATE
           AND C37.START_DATE <= P_I_DATE
           AND C37.END_DATE > P_I_DATE
           AND P36.START_DATE <= P_I_DATE
           AND P36.END_DATE > P_I_DATE
           AND P40.START_DATE <= P_I_DATE
           AND P40.END_DATE > P_I_DATE
           AND X32.START_DATE <= P_I_DATE
           AND X32.END_DATE > P_I_DATE
           AND X33.START_DATE <= P_I_DATE
           AND X33.END_DATE > P_I_DATE
           AND X34.START_DATE(+) <= P_I_DATE
           AND X34.END_DATE(+) > P_I_DATE
           AND SDF.MODIFY_DATE = P_I_DATE
           AND SDF.LGD IS NULL
           AND SDF.ZONE_PAR = P_I_ZONENO;
      COMMIT;
      p_o_succeed := '0';
      --记载数据处理日志
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
         --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
         RAISE;
   END proc_deal_frdk;
  
  PROCEDURE proc_frdk_dm (p_i_date IN VARCHAR2,p_i_zoneno IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_month_day    NUMBER (3);
   BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PACK_DEAL_FRDK_1005.PROC_FRDK_DM';
      v_log_level := '3';
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';
       --读取系统时间记载开始处理时间
	  SELECT TO_CHAR (SYSDATE, 'YYYYMMDDHH24MISS') INTO v_begin_time FROM DUAL;
       v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
       v_step_no := '2.2';
       --当月天数
       --2014年8月版本将原来月中增量月末存量改成每日存量，今天是本月第几天 modified by caohui 20140804
	  SELECT TO_DATE (p_i_date, 'yyyymmdd') - TO_DATE (SUBSTR (p_i_date, 1, 6) || '01', 'yyyymmdd') + 1 INTO v_month_day FROM DUAL;
       --开始删除DM表中数据
	  v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
	  v_step_no := '2.3';
	  DELETE FROM DM_CS_ZFRDK T WHERE T.Zone_Par=P_I_ZONENO;
	  INSERT INTO dm_cs_zfrdk (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                   loantype, hydm, corpgrade, value01, value02, value03,
                   value04, value05, value06, value07, value08, value09,
                   value010,smallcorp,lgd_level,zone_par)
         SELECT  /*+full(dw)*/ dw.zoneno, dw.brno, '0', dw.qxdc_id2, dw.currtype,
                  NVL (dw.ta200261037, 'null'), dw.product_id3,
                  NVL (dw.dbfs_id, 'null'), NVL (dw.ta200011014, 'null'),
                  NVL (dw.xyvalue, 'null'), SUM (dw.balance),
                  SUM (dw.ta490461016), SUM (dw.ytbbj), SUM (dw.dkamount),
                  SUM (dw.ecost), SUM (dw.balance_cm) / v_month_day, --日均贷款余额
                  SUM (dw.ta490461016_cm) / v_month_day,      --实提拨备额日均
                  SUM (dw.ytbbj_cm) / v_month_day,            --应提拨备额日均
                  SUM (dw.dkamount) / v_month_day,
                  SUM (dw.ecost_cm) / v_month_day,           --经济资本占用日均
                  dw.small_corp,
                  nvl(dw.lgd_level,'999'),
                 p_i_zoneno
             FROM dw_mod_frdk dw
            WHERE dw.start_date = p_i_date
                  AND dw.zone_par=p_i_zoneno
         GROUP BY dw.zoneno,
                  dw.brno,
                  dw.qxdc_id2,
                  dw.currtype,
                  dw.ta200261037,
                  dw.product_id3,
                  dw.dbfs_id,
                  dw.ta200011014,
                  dw.xyvalue,
                  dw.small_corp,
                  nvl(dw.lgd_level,'999');
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.4';
      INSERT INTO dm_cs_zfrdk
                  (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                   loantype, hydm, corpgrade, value01, value02, value03,
                   value04, value05, value06, value07, value08, value09,
                   value010,smallcorp,lgd_level,zone_par)
         SELECT   a.zoneno, a.brno, a.bankflag, a.qx, '992', a.cresort,
                  a.prodid, a.loantype, a.hydm, a.corpgrade,
                  SUM (a.value01 * b.uscvrate), SUM (a.value02 * b.uscvrate),
                  SUM (a.value03 * b.uscvrate), SUM (a.value04 * b.uscvrate),
                  SUM (a.value05 * b.uscvrate), SUM (a.value06 * b.uscvrate),
                  SUM (a.value07 * b.uscvrate), SUM (a.value08 * b.uscvrate),
                  SUM (a.value09 * b.uscvrate), SUM (a.value010 * b.uscvrate),
                  a.smallcorp,
                  a.lgd_level,
                  p_i_zoneno
             FROM dm_cs_zfrdk a, PRM_CURRENCY_RATE B
            WHERE a.currtype <> '001'
                  AND a.zone_par=p_i_zoneno
              AND a.currtype = b.currency
         GROUP BY a.zoneno,
                  a.brno,
                  a.bankflag,
                  a.qx,
                  a.cresort,
                  a.prodid,
                  a.loantype,
                  a.hydm,
                  a.corpgrade,
                  a.smallcorp,
                  a.lgd_level;
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.5';
       ----------------开始本外币折人民币----------------
      INSERT INTO dm_cs_zfrdk
                  (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                   loantype, hydm, corpgrade, value01, value02, value03,
                   value04, value05, value06, value07, value08, value09,
                   value010,smallcorp,lgd_level,zone_par)
         SELECT   a.zoneno, a.brno, a.bankflag, a.qx, '994', a.cresort,
                  a.prodid, a.loantype, a.hydm, a.corpgrade,
                  SUM (DECODE (a.currtype,
                               '001', a.value01,
                               a.value01 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value02,
                               a.value02 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value03,
                               a.value03 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value04,
                               a.value04 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value05,
                               a.value05 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value06,
                               a.value06 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value07,
                               a.value07 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value08,
                               a.value08 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value09,
                               a.value09 / b.uscvrate
                              )
                      ),
                  SUM (DECODE (a.currtype,
                               '001', a.value010,
                               a.value010 / b.uscvrate
                              )
                      ),
                      a.smallcorp,
                      a.lgd_level,
                      p_i_zoneno
             FROM dm_cs_zfrdk a,  PRM_CURRENCY_RATE B
            WHERE a.currtype IN ('001', '992')
             AND a.zone_par=p_i_zoneno
              AND b.currency = '001'
         GROUP BY a.zoneno,
                  a.brno,
                  a.bankflag,
                  a.qx,
                  a.cresort,
                  a.prodid,
                  a.loantype,
                  a.hydm,
                  a.corpgrade,
                  a.smallcorp,
                  a.lgd_level;
      COMMIT;
      p_o_succeed := '0';
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
	  --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
		-- pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
   END proc_frdk_dm; 
   
   PROCEDURE proc_resume_frdk (p_i_date IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_count        VARCHAR2 (10);
   BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PROC_RESUME_FRDK';
      v_log_level := '3';                                          --需要商定
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';
       --从模型结果表中删除当日日批错误数据
      DELETE FROM dw_mod_frdk a  WHERE a.start_date >= p_i_date;
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2';
       --将结束日期为恢复日-1的记录置为99991231
      UPDATE dw_mod_frdk a SET a.end_date = '99991231' WHERE a.end_date = TO_CHAR (TO_DATE (p_i_date, 'YYYYMMDD') - 1, 'YYYYMMDD');
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.3';
       --从中间表中删除当日日批错误数据
      DELETE FROM stg_dat_frdk a WHERE a.modify_date >= p_i_date;
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.4';
      SELECT COUNT (1) INTO v_count FROM stg_bak_frdk a WHERE a.data_date = p_i_date;
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.5';
       --回退BAK表当日修改的数据进STG_DAT表
      IF v_count <> 0
      THEN
         INSERT INTO stg_dat_frdk
                     (zoneno, phybrno, loanno, loansqno, accno, lnmemno,
                      restamount, restamount_cm, currtype, subcode, buskind,
                      lnudflag, loanway, loandirt, leveltwl, levelfive,
                      avchcof, realboamt, realboamt_cm, surveyor, loandate,
                      duedate, custcode, custname, entrsize, creditlevel,
                      groupcode, groupname, industrycode, configrate,
                      cust_exe_rate, income, pd, lgd, status, modify_date,
                      product_id,smallcorp,dubilstdrate,ratechgmode,zone_par,bnkrate)
            SELECT a.zoneno, a.phybrno, a.loanno, a.loansqno, a.accno,
                   a.lnmemno, a.restamount, a.restamount_cm, a.currtype,
                   a.subcode, a.buskind, a.lnudflag, a.loanway, a.loandirt,
                   a.leveltwl, a.levelfive, a.avchcof, a.realboamt,
                   a.realboamt_cm, a.surveyor, a.loandate, a.duedate,
                   a.custcode, a.custname, a.entrsize, a.creditlevel,
                   a.groupcode, a.groupname, a.industrycode, a.configrate,
                   a.cust_exe_rate, a.income, a.pd, a.lgd, a.status,
                   a.modify_date, a.product_id,a.smallcorp,a.dubilstdrate,
                   a.ratechgmode,a.zone_par,bnkrate
              FROM stg_bak_frdk a
             WHERE a.data_date = p_i_date;
          COMMIT;
      END IF;
       p_o_succeed := '0';
      COMMIT;
      --记载数据处理日志
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
         --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
         RAISE;
   END proc_resume_frdk;

  FUNCTION func_prm_xfra (i_datedate IN VARCHAR2, in_product_id IN VARCHAR2)
      RETURN NUMBER
   IS
      n_par_coef   NUMBER;                               --特殊产品的资本系数
   BEGIN
      SELECT MAX (x.par_coef)
        INTO n_par_coef
        FROM prm_xsfra x
       WHERE x.product_id = in_product_id
         AND x.start_date <= i_datedate
         AND x.end_date > i_datedate;
       IF n_par_coef IS NULL
      THEN
         RETURN NULL;
      ELSE
         RETURN n_par_coef;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;   
   
   FUNCTION func_prm_xsfr2 (
      i_datedate   IN   VARCHAR2,
      in_product   IN   VARCHAR2,
      in_subcode   IN   VARCHAR2,
      in_ywtype    IN   VARCHAR2
   )
      RETURN NUMBER
   IS
      n_par_coef2   prm_xsfr2_tmp.par_coef%TYPE;
   BEGIN
      SELECT MAX (x.par_coef) INTO n_par_coef2 FROM prm_xsfr2_tmp x WHERE x.subject_code = in_subcode AND x.ywpz_code = in_ywtype;
      --科目截4位。
      IF n_par_coef2 IS NULL
      THEN
         SELECT MAX (x.par_coef) INTO n_par_coef2 FROM prm_xsfr2_tmp x WHERE x.subject_code = SUBSTR (in_subcode, 1, 4) AND x.ywpz_code = in_ywtype;
         IF n_par_coef2 IS NULL
         THEN
            SELECT MAX (x.par_coef) INTO n_par_coef2 FROM prm_xsfr2_tmp x WHERE x.subject_code = in_subcode AND x.ywpz_code = 'N/A';
            IF n_par_coef2 IS NULL THEN
               SELECT MAX (x.par_coef) INTO n_par_coef2 FROM prm_xsfr2_tmp x WHERE x.subject_code = SUBSTR (in_subcode, 1, 4) AND x.ywpz_code = 'N/A';
               IF n_par_coef2 IS NULL THEN
                 RETURN 1;
               ELSE
                 RETURN n_par_coef2;
              END IF;
            ELSE
               RETURN n_par_coef2;
            END IF;
         ELSE
            RETURN n_par_coef2;
         END IF;
      ELSE
         RETURN n_par_coef2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END;
     
  FUNCTION func_prm_dkzbpzb (i_level IN VARCHAR2)
      RETURN PRM_DKZBPZB.Value%TYPE
   IS
      v_count   NUMBER;
      v_value   PRM_DKZBPZB.Value%TYPE;
   BEGIN
      select count(1) into v_count from  PRM_DKZBPZB T WHERE T.END_DATE = '99991231' AND T.PRODUCT_ID= i_level;
      if v_count<>0 then
        SELECT T.VALUE INTO v_value FROM PRM_DKZBPZB T WHERE T.END_DATE = '99991231' AND T.PRODUCT_ID= i_level;
      else
        v_value :='0';
      end if;
      RETURN v_value;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN '0';
   END;
  
  FUNCTION func_prm_cs031 (i_datedate IN VARCHAR2, in_subject_code IN VARCHAR2)
      RETURN VARCHAR2
   IS
      out_product   prm_cs031.product_id%TYPE;                              --
   BEGIN
      SELECT MAX (c31.product_id) INTO out_product FROM prm_cs031 c31
       WHERE c31.subject_code = in_subject_code AND c31.subject_type = '2' AND c31.subject_app = 1 AND c31.start_date <= i_datedate AND c31.end_date > i_datedate;
       IF out_product IS NULL THEN
         SELECT MAX (c31.product_id) INTO out_product FROM prm_cs031 c31
          WHERE c31.subject_code = SUBSTR (in_subject_code, 1, 4)
            AND c31.subject_type = '1'
            AND c31.subject_app = 1
            AND c31.start_date <= i_datedate
            AND c31.end_date > i_datedate;
          RETURN out_product;
      ELSE
         RETURN out_product;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;
  
  PROCEDURE PROC_DEAL_FRDK_BAK(P_I_DATE IN VARCHAR2,P_I_ZONENO IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2)  IS
      --以下变量为记录日志时使用的。
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
   BEGIN
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PACK_DEAL_FRDK_1005.PROC_DEAL_FRDK_BAK';
      v_log_level := '3';
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';
       --读取系统时间记载开始处理时间
	  SELECT TO_CHAR (SYSDATE, 'YYYYMMDDHH24MISS') INTO v_begin_time FROM DUAL;
	  v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2';
       --开始按地区进行备份
	   DELETE FROM stg_bak_frdk t WHERE t.data_date = p_i_date AND t.zoneno=P_I_ZONENO;
	   INSERT INTO stg_bak_frdk
                  (zoneno, phybrno, loanno, loansqno, accno, lnmemno,
                   restamount, restamount_cm, currtype, subcode, buskind,
                   lnudflag, loanway, loandirt, leveltwl, levelfive, avchcof,
                   realboamt, realboamt_cm, surveyor, loandate, duedate,
                   custcode, custname, entrsize, creditlevel, groupcode,
                   groupname, industrycode, configrate, cust_exe_rate, income,
                   pd, lgd, status, modify_date, data_date, product_id,smallcorp,dubilstdrate,ratechgmode,zone_par,bnkrate)
         SELECT s.zoneno, s.phybrno, s.loanno, s.loansqno, s.accno, s.lnmemno,
                s.restamount, s.restamount_cm, s.currtype, s.subcode,
                s.buskind, s.lnudflag, s.loanway, s.loandirt, s.leveltwl,
                s.levelfive, s.avchcof, s.realboamt, s.realboamt_cm,
                s.surveyor, s.loandate, s.duedate, s.custcode, s.custname,
                s.entrsize, s.creditlevel, s.groupcode, s.groupname,
                s.industrycode, s.configrate, s.cust_exe_rate, s.income, s.pd,
                s.lgd, s.status, s.modify_date, p_i_date, s.product_id,s.smallcorp,s.dubilstdrate,s.ratechgmode,P_I_ZONENO,bnkrate
            FROM stg_dat_frdk s
          WHERE s.zone_par=p_i_zoneno and s.modify_date = p_i_date;
	 IF p_i_date = TO_CHAR (LAST_DAY (TO_DATE (p_i_date, 'yyyymmdd')), 'YYYYMMDD') THEN
      --added 20110523
     --NSERT INTO  DW_MOD_FRDK_YZMX
     --            (loanno, loansqno, accno, zoneno, brno, currtype, subcode,
     --             balance, balance_cm, valueday, exhmatud, ta490461016,
     --             ta490461016_cm, ta490461017, ta200261001, ta200261002,
     --             ta200261004, ta200261037, ta200251013, ta200251011,
     --             ta200251020, ta200011014, ta200011040, ta200011070,
     --             ta200011005, ta200011016, ta200011034, ta340362013,
     --             ta200268004, rzzd_fl, product_id3, product_name3, xyvalue,
     --             xyname, dbdc_id, dbdc_name, dbfs_id, dbfs_lb, qxdc_id1,
     --             qxdc_name1, qxdc_id2, qxdc_name2, xs031, xs034, xs033,
     --             xs032, xs035, xs036, gjrz_xs, jjzb_xs, ytbbbl, ytbbj,
     --             ytbbj_cm, dkamount, ecost, ecost_cm, start_date, end_date,
     --             xs_product, ywzl, htsfsr, rate_today, cust_exe_rate, pd,
     --             lgd,lgd_level,small_corp,dubil_std_rate,rate_chg_mode,corp_name,croup_name,zone_par,BNKRATE)
     --    SELECT   loanno, loansqno, accno, zoneno, brno, currtype, subcode,
     --             balance, balance_cm, valueday, exhmatud, ta490461016,
     --             ta490461016_cm, ta490461017, ta200261001, ta200261002,
     --             ta200261004, ta200261037, ta200251013, ta200251011,
     --             ta200251020, ta200011014, ta200011040, ta200011070,
     --             ta200011005, ta200011016, ta200011034, ta340362013,
     --             ta200268004, rzzd_fl, product_id3, product_name3, xyvalue,
     --             xyname, dbdc_id, dbdc_name, dbfs_id, dbfs_lb, qxdc_id1,
     --             qxdc_name1, qxdc_id2, qxdc_name2, xs031, xs034, xs033,
     --             xs032, xs035, xs036, gjrz_xs, jjzb_xs, ytbbbl, ytbbj,
     --             ytbbj_cm, dkamount, ecost, ecost_cm, start_date, end_date,
     --             xs_product, ywzl, htsfsr, rate_today, cust_exe_rate, pd,
     --             lgd,lgd_level,small_corp,dubil_std_rate,rate_chg_mode,corp_name,croup_name,zone_par,BNKRATE
     --      from  DW_MOD_FRDK DMF
     --     WHERE  DMF.START_DATE>=SUBSTR(P_I_DATE,1,6)||'01'
     --       AND  DMF.START_DATE<= TO_CHAR(LAST_DAY(TO_DATE(SUBSTR(P_I_DATE,1,6),'YYYYMM'))-1,'YYYYMMDD')
     --       AND  DMF.ZONE_PAR=P_I_ZONENO;
     DELETE FROM DW_MOD_FRDK DMF
     WHERE DMF.START_DATE>=SUBSTR(P_I_DATE,1,6)||'01'
     AND DMF.START_DATE<= TO_CHAR(LAST_DAY(TO_DATE(SUBSTR(P_I_DATE,1,6),'YYYYMM'))-1,'YYYYMMDD')
     AND DMF.ZONE_PAR=P_I_ZONENO;
      END IF;
      COMMIT;
      COMMIT;
      p_o_succeed := '0';
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      
	  --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
         
		 --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
		 RAISE;
   END proc_deal_frdk_bak;
  
  PROCEDURE PROC_DEAL_FRDK_MAIN(P_I_DATE IN VARCHAR2,P_I_ZONENO IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2)IS
  BEGIN
	PROC_DEAL_FRDK_MON(P_I_DATE,P_I_ZONENO,P_O_SUCCEED);
	P_O_SUCCEED:= '0';
	COMMIT;
  EXCEPTION  WHEN OTHERS THEN
	ROLLBACK;
	P_O_SUCCEED := '1';
	RAISE;
   END PROC_DEAL_FRDK_MAIN;
  
  PROCEDURE proc_deal_frdk_mon (p_i_date IN VARCHAR2,p_i_zoneno IN VARCHAR2, p_o_succeed OUT VARCHAR2)
   IS
      --以下变量为记录日志时使用的。
      v_proc_name    l_getdata_call.proc_name%TYPE;
      v_log_level    l_getdata_call.log_level%TYPE;
      v_begin_time   l_getdata_call.begin_time%TYPE;
      v_end_time     l_getdata_call.end_time%TYPE;
      v_err_code     l_getdata_err.err_code%TYPE;
      v_err_txt      l_getdata_err.err_txt%TYPE;
      v_step_no      l_getdata_err.step_no%TYPE;
      v_sql_txt      l_getdata_err.sql_txt%TYPE;
      v_month_day    NUMBER (3);                                   --当月天数
      v_c32          prm_cs032.dbfs_id%TYPE;
      v_p32          prm_cs032.dbfs_lb%TYPE;
      v_x31          prm_xs031.xs_value%TYPE;
      v_xqyxs        prm_xsfr6.PAR_COEF%TYPE;
      v_par_coef     prm_xsfrdx.par_coef%TYPE;
      v_dkzbpzb_level1  PRM_DKZBPZB.Value%TYPE;--贷款五级分类:10 正常
      v_dkzbpzb_level2  PRM_DKZBPZB.Value%TYPE;--贷款五级分类:20 次级
      v_dkzbpzb_level3  PRM_DKZBPZB.Value%TYPE;--贷款五级分类:30 关注
      v_dkzbpzb_level4  PRM_DKZBPZB.Value%TYPE;--贷款五级分类:40 可疑
      v_dkzbpzb_level5  PRM_DKZBPZB.Value%TYPE;--贷款五级分类:50 损失
    BEGIN
     v_dkzbpzb_level1:=PACK_DEAL_FRDK_1005_P.func_prm_dkzbpzb ('10');
     v_dkzbpzb_level2:=PACK_DEAL_FRDK_1005_P.func_prm_dkzbpzb ('20');
     v_dkzbpzb_level3:=PACK_DEAL_FRDK_1005_P.func_prm_dkzbpzb ('30');
     v_dkzbpzb_level4:=PACK_DEAL_FRDK_1005_P.func_prm_dkzbpzb ('40');
     v_dkzbpzb_level5:=PACK_DEAL_FRDK_1005_P.func_prm_dkzbpzb ('50');
      --记载数据处理日志
      v_step_no := '1';
      v_proc_name := 'PACK_DEAL_FRDK_1005.PROC_DEAL_FRDK';
      v_log_level := '3';
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.1';
       ---BAK表备份当日日更新数据
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.2';
       SELECT MAX (dbfs_id)
        INTO v_c32
        FROM prm_cs032
       WHERE start_date <= p_i_date AND end_date > p_i_date;
       select nvl(MAX (x.par_coef),0.016)
      into v_par_coef
      from prm_xsfrdx x
      where  x.start_date <= P_I_DATE AND x.end_date > P_I_DATE;
       SELECT DISTINCT dbfs_lb
                 INTO v_p32
                 FROM prm_cs032
                WHERE dbfs_id = v_c32
                  AND start_date <= p_i_date
                  AND end_date > p_i_date;
       SELECT MAX(xs_value)
      INTO v_x31
      FROM prm_xs031
      WHERE start_date<=p_i_date
      AND end_date>p_i_date;
        ---20100926新增修改 小企业系数
      SELECT MAX(PAR_COEF)
      INTO v_xqyxs
      FROM prm_xsfr6
      WHERE start_date<=p_i_date
      AND end_date>p_i_date
      AND IS_SE='1';
       v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.3';
       UPDATE stg_dat_frdk t
         SET t.restamount_cm = t.restamount_cm + t.restamount,
             t.realboamt_cm = t.realboamt_cm + t.realboamt,
             t.restamount=0
       WHERE NOT EXISTS (
                SELECT 1
                  FROM ods_tmp_frdk2 ods
                 WHERE t.loanno = ods.loanno
                   AND t.loansqno = ods.loansqno
                   AND t.accno = ods.accno
                   AND ods.zone_par=t.zone_par)
                   AND t.zone_par=p_i_zoneno;
       --2014年8月版本将原来月中增量月末存量改成每日存量，增加每月1号，月积数清0 modified by caohui 20140804
       v_step_no := '2.4';
      IF p_i_date = SUBSTR (p_i_date, 1, 6) || '01'
      THEN
         UPDATE stg_dat_frdk t SET t.restamount_cm = 0, t.realboamt_cm = 0  WHERE t.zone_par=p_i_zoneno;
      END IF;
      --更新STG表
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.6';
     MERGE INTO stg_dat_frdk s
         USING (SELECT o.ZONENO,o.PHYBRNO,o.LOANNO,o.LOANSQNO,o.ACCNO,o.LNMEMNO,o.RESTAMOUNT,o.CURRTYPE,o.SUBCODE,o.BUSKIND,
                       o.LNUDFLAG,o.LOANWAY,o.LOANDIRT,o.LEVELTWL,o.LEVELFIVE,o.AVCHCOF,o.REALBOAMT,o.SURVEYOR,o.LOANDATE,o.DUEDATE,
                       o.CUSTCODE,o.CUSTNAME,o.ENTRSIZE,o.CREDITLEVEL,o.GROUPCODE,o.GROUPNAME,o.INDUSTRYCODE,o.SMALL_CORP,o.DUBIL_STD_RATE,
                       o.RATE_CHG_MODE,o.CONFIGRATE,o.CUST_EXE_RATE,o.INCOME,o.PD,o.LGD,o.STATUS,o.ZONE_PAR,P.PRODUCT_ID PRD,o.bnkrate
                  FROM ods_tmp_frdk2 o,PRM_PRODUCT_SUBJECT P
                 WHERE  O.SUBCODE=P.SUBJECT_CODE
                 AND P.PRODUCT_NAME='1'  ---1 法贷，2个贷
                AND o.zone_par=P_I_ZONENO) ods
         ON (  s.zone_par=p_i_zoneno
             AND   s.loanno = ods.loanno
             AND s.loansqno = ods.loansqno
             AND s.accno = ods.accno)
         WHEN MATCHED THEN UPDATE
               SET s.zoneno = ods.zoneno,
                  s.phybrno = ods.phybrno, s.lnmemno = ods.lnmemno,
                  s.restamount =
                     DECODE (ods.status,
                             '5', 0,
                             '6', 0,
                             '8', 0,
                             NVL (ABS (DECODE (ods.lnudflag,
                                               '40001', ods.restamount,
                                               NULL, ods.restamount,
                                               0
                                              )
                                      ),
                                  0
                                 )
                            ),
                  s.restamount_cm =
                       ABS (s.restamount_cm)
                     + DECODE (ods.status,
                               '5', 0,
                               '6', 0,
                               '8', 0,
                               NVL (ABS (DECODE (ods.lnudflag,
                                                 '40001', ods.restamount,
                                                 NULL, ods.restamount,
                                                 0
                                                )
                                        ),
                                    0
                                   )
                              ),
                  s.currtype = ods.currtype, s.subcode = ods.subcode,
                  s.buskind = ods.buskind, s.lnudflag = ods.lnudflag,
                  s.loanway = NVL (ods.loanway, '1A01'),
                  s.loandirt = ods.loandirt,
                  s.leveltwl = NVL (ods.leveltwl, '10'),
                  s.levelfive = ods.levelfive,
                  s.avchcof = DECODE (ods.loanway, '2A01', 0, ods.avchcof),
                  s.realboamt = ods.realboamt,
                  s.realboamt_cm = s.realboamt_cm + ods.realboamt,
                  s.surveyor = ods.surveyor, s.loandate = ods.loandate,
                  s.duedate = ods.duedate, s.custcode = ods.custcode,
                  s.custname = ods.custname, s.entrsize = ods.entrsize,
                  s.creditlevel = ods.creditlevel,
                  s.groupcode = ods.groupcode, s.groupname = ods.groupname,
                  s.industrycode = NVL (ods.industrycode, 'null'),
                  s.configrate = ods.configrate,
                  s.cust_exe_rate = ods.cust_exe_rate,
                  s.income = ods.income,
                  s.pd = ods.pd, s.lgd = DECODE (ods.lgd, -1, NULL, ods.lgd),
                  s.status = ods.status,
                  s.product_id = ods.prd,
                  s.modify_date = p_i_date,
                  s.smallcorp=ods.small_corp,
                  s.dubilstdrate=ods.dubil_std_rate,
                  s.ratechgmode=ods.rate_chg_mode,
                  s.bnkrate = ods.bnkrate
         WHEN NOT MATCHED THEN
            INSERT (s.zoneno, s.phybrno, s.loanno, s.loansqno, s.accno,
                    s.lnmemno, s.restamount, s.restamount_cm, s.currtype,
                    s.subcode, s.buskind, s.lnudflag, s.loanway, s.loandirt,
                    s.leveltwl, s.levelfive, s.avchcof, s.realboamt,
                    s.realboamt_cm, s.surveyor, s.loandate, s.duedate,
                    s.custcode, s.custname, s.entrsize, s.creditlevel,
                    s.groupcode, s.groupname, s.industrycode, s.configrate,
                    s.cust_exe_rate, s.income, s.pd, s.lgd, s.status,
                    s.modify_date, s.product_id,s.smallcorp,s.dubilstdrate,s.ratechgmode,s.zone_par,s.bnkrate)
            VALUES (ods.zoneno,
                    ods.phybrno, ods.loanno, ods.loansqno, ods.accno,
                    ods.lnmemno,
                    DECODE (ods.status,
                            '5', 0,
                            '6', 0,
                            '8', 0,
                            NVL (ABS (DECODE (ods.lnudflag,
                                              '40001', ods.restamount,
                                              NULL, ods.restamount,
                                              0
                                             )
                                     ),
                                 0
                                )
                           ),
                    DECODE (ods.status,
                            '5', 0,
                            '6', 0,
                            '8', 0,
                            NVL (ABS (DECODE (ods.lnudflag,
                                              '40001', ods.restamount,
                                              NULL, ods.restamount,
                                              0
                                             )
                                     ),
                                 0
                                )
                           ),
                    ods.currtype, ods.subcode, ods.buskind, ods.lnudflag,
                    NVL (ods.loanway, '1A01'), ods.loandirt,
                    NVL (ods.leveltwl, '10'), ods.levelfive,
                     --特殊处理 贷款方式LOANWAY值为=‘2A01’则把担保系数AVCHCOF置为0，否则则不变
                    DECODE (ods.loanway, '2A01', 0, ods.avchcof),
                    ods.realboamt, ods.realboamt, ods.surveyor, ods.loandate,
                    ods.duedate, ods.custcode, ods.custname, ods.entrsize,
                    ods.creditlevel, ods.groupcode, ods.groupname,
                    NVL (ods.industrycode, 'null'), ods.configrate,
                    ods.cust_exe_rate, ods.income, ods.pd,
                    DECODE (ods.lgd, -1, NULL, ods.lgd), ods.status, p_i_date,
                    ods.prd,
                    ods.small_corp,
                    ods.dubil_std_rate,
                    ods.rate_chg_mode,
                    P_I_ZONENO,
                    ods.bnkrate);
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.7';
      SELECT LAST_DAY (TO_DATE (p_i_date, 'yyyymmdd')) - TO_DATE (SUBSTR (p_i_date, 1, 6) || '01', 'yyyymmdd') + 1 INTO v_month_day FROM DUAL;
      v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      v_step_no := '2.8';
      INSERT INTO dw_mod_frdk
                  (loanno, loansqno, accno, zoneno, brno, currtype, subcode,
                   balance, balance_cm, valueday, exhmatud, ta490461016,
                   ta490461016_cm, ta490461017, ta200261001, ta200261002,
                   ta200261004, ta200261037, ta200251013, ta200251011,
                   ta200251020, ta200011014, ta200011040, ta200011070,
                   ta200011005, ta200011016, ta200011034, ta340362013,
                   ta200268004, rzzd_fl, product_id3, product_name3, xyvalue,
                   xyname, dbdc_id, dbdc_name, dbfs_id, dbfs_lb, qxdc_id1,
                   qxdc_name1, qxdc_id2, qxdc_name2, xs031, xs034, xs033,
                   xs032, xs035, xs036, gjrz_xs, jjzb_xs, ytbbbl, ytbbj,
                   ytbbj_cm, dkamount, ecost, ecost_cm, start_date, end_date,
                   xs_product, ywzl, htsfsr, rate_today, cust_exe_rate, pd,
                   lgd,lgd_level,small_corp,dubil_std_rate,rate_chg_mode,
                   corp_name,croup_name,zone_par,bnkrate,nloan_tjxs)
          SELECT /*+ordered use_hash(SDF PP C32 P36 C37 P40 XF1 X32 X33 X34)*/
       SDF.LOANNO,
       SDF.LOANSQNO,
       SDF.ACCNO,
       SDF.ZONENO,
       SDF.PHYBRNO,
       SDF.CURRTYPE,
       SDF.SUBCODE,
       SDF.RESTAMOUNT,
       SDF.RESTAMOUNT_CM,
       SDF.LOANDATE,
       SDF.DUEDATE,
       SDF.REALBOAMT,
       SDF.REALBOAMT_CM,
       NULL, --实提拨比例
       SDF.CUSTCODE,
       SDF.LNMEMNO,
       SDF.ACCNO,
       SDF.LEVELTWL,
       SDF.LEVELFIVE,
       --贷款分类
       SDF.LOANWAY,
       SDF.SURVEYOR,
       SDF.INDUSTRYCODE,
       NULL,
       --年初信用等级
       NULL,
       --比照信用等级
       NULL,
       --技术监督局代码
       SDF.ENTRSIZE,
       SDF.GROUPCODE,
       SDF.AVCHCOF,
       SDF.LOANDIRT,
       NULL,
       --国际贸易融资转贷款的分类
       SDF.PRODUCT_ID, --三级产品名称
       PP.PRODUCT_NAME,
       SDF.CREDITLEVEL, --直接取
       P40.DICT_NAME,
       NULL, --担保档次编号
       NULL, --担保档次名称
       NVL(C32.DBFS_ID, V_C32),
       --贷款方式类别ID
       NVL(C32.DBFS_LB, V_P32), --贷款方式类别 去租prm_cs032
       NULL, --贷款期限
       NULL, --贷款期限档次名称
       P36.QXDC_ID,
       --贷款剩余期限档次编号
       P36.QXDC_NAME,
       --贷款剩余期限档次名称 取之prm_cs036
       NVL(XF1.PAR_COEF, 1), --基本配置系数 xs031
       NVL(X34.XS_VALUE, 1),
       --行业地区调节系数
       X33.PAR_COEF,
       --剩余期限调节系数
       X32.XS_VALUE, --质量调节系数
       0, --贸易融资产品调节系数
       0, --项目贷款产品调节系数
       0, --国际融资转贷款产品调节系数
        --20100707修改，如果贷款方式为低风险质押，则基准系数直接取1.6%,如果总体系数<1.6%,也取1.6%
        --201501变更再修改，如果贷款已违约，或者是垫款，系数取0
       CASE WHEN 
         CASE
         WHEN SDF.CREDITLEVEL = '60'  THEN
           DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
         WHEN PP.PRODUCT_ID = '104' THEN
           0
         ELSE
           DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                             DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                              decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                             PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                SDF.PRODUCT_ID,
                                                                SDF.SUBCODE,
                                                                SDF.BUSKIND)
                             *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                              ) -
                         v_par_coef),
                    -1,
                    v_par_coef,
                    NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                        DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                        decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                             PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                SDF.PRODUCT_ID,
                                                                SDF.SUBCODE,
                                                                SDF.BUSKIND)
                             *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                        ))
         END >=0.125 THEN 
           CASE
             --WHEN SDF.CREDITLEVEL = '60' OR PP.PRODUCT_ID = '104' THEN
             --0
             WHEN SDF.CREDITLEVEL = '60'  THEN
               DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
             WHEN PP.PRODUCT_ID = '104' THEN
               0
             ELSE
               DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                                 DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                                  decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                  ) -
                             v_par_coef),
                        -1,
                        v_par_coef,
                        NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                            DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                            decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                            ))
           END 
        ELSE 
           CASE
             --WHEN SDF.CREDITLEVEL = '60' OR PP.PRODUCT_ID = '104' THEN
             --0
             WHEN SDF.CREDITLEVEL = '60'  THEN
               DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
             WHEN PP.PRODUCT_ID = '104' THEN
               0
             ELSE
               DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                                 DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                                  decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                  ) -
                             v_par_coef),
                        -1,
                        v_par_coef,
                        NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                            DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                            decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                            ))
           END * NVL(PNX.TJXS,1)  --202001版本新增经济资本增减计调节系数
       END JJZB_XS,--经济资本配置系数
       C37.BBPRECENT, --应提拨备比例
       SDF.RESTAMOUNT * C37.BBPRECENT, --应提拨备额
       SDF.RESTAMOUNT_CM * C37.BBPRECENT, --应提拨备额月积数
       DECODE(SIGN(SDF.RESTAMOUNT - SDF.REALBOAMT),
              -1,
              0,
              (SDF.RESTAMOUNT - SDF.REALBOAMT)), --贷款净额为贷款余额-拨备额，小于零就取0
       SDF.RESTAMOUNT *
       CASE WHEN 
         CASE
         --WHEN SDF.CREDITLEVEL = '60' OR PP.PRODUCT_ID = '104' THEN
         --0
         WHEN SDF.CREDITLEVEL = '60'  THEN
           DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
         WHEN PP.PRODUCT_ID = '104' THEN
           0
         ELSE
           DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                             DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                              decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                             PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                SDF.PRODUCT_ID,
                                                                SDF.SUBCODE,
                                                                SDF.BUSKIND)
                             *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                              ) -
                         v_par_coef),
                    -1,
                    v_par_coef,
                    NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                        DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                        decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                             PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                SDF.PRODUCT_ID,
                                                                SDF.SUBCODE,
                                                                SDF.BUSKIND)
                             *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                        ))
         END >=0.125 THEN 
           CASE
             --WHEN SDF.CREDITLEVEL = '60' OR PP.PRODUCT_ID = '104' THEN
             --0
             WHEN SDF.CREDITLEVEL = '60'  THEN
               DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
             WHEN PP.PRODUCT_ID = '104' THEN
               0
             ELSE
               DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                                 DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                                  decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                  ) -
                             v_par_coef),
                        -1,
                        v_par_coef,
                        NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                            DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                            decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                            ))
           END 
        ELSE 
           CASE
             WHEN SDF.CREDITLEVEL = '60'  THEN
               DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
             WHEN PP.PRODUCT_ID = '104' THEN
               0
             ELSE
               DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                                 DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                                  decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                  ) -
                             v_par_coef),
                        -1,
                        v_par_coef,
                        NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                            DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                            decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                            ))
           END * NVL(PNX.TJXS,1)  --202001版本新增经济资本增减计调节系数
       END,--202001版本新增经济资本增减计调节系数
       SDF.RESTAMOUNT_CM *
       CASE WHEN 
         CASE
         WHEN SDF.CREDITLEVEL = '60'  THEN
           DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
         WHEN PP.PRODUCT_ID = '104' THEN
           0
         ELSE
           DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                             DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                              decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                             PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                SDF.PRODUCT_ID,
                                                                SDF.SUBCODE,
                                                                SDF.BUSKIND)
                             *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                              ) -
                         v_par_coef),
                    -1,
                    v_par_coef,
                    NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                        DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                        decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                             PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                SDF.PRODUCT_ID,
                                                                SDF.SUBCODE,
                                                                SDF.BUSKIND)
                             *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                        ))
         END >=0.125 THEN 
           CASE
             WHEN SDF.CREDITLEVEL = '60'  THEN
               DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
             WHEN PP.PRODUCT_ID = '104' THEN
               0
             ELSE
               DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                                 DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                                  decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                  ) -
                             v_par_coef),
                        -1,
                        v_par_coef,
                        NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                            DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                            decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                            ))
           END 
        ELSE 
           CASE
             WHEN SDF.CREDITLEVEL = '60'  THEN
               DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
             WHEN PP.PRODUCT_ID = '104' THEN
               0
             ELSE
               DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                                 DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                                  decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                  ) -
                             v_par_coef),
                        -1,
                        v_par_coef,
                        NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                            DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, 1)) * X32.XS_VALUE *
                            decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                                 PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                    SDF.PRODUCT_ID,
                                                                    SDF.SUBCODE,
                                                                    SDF.BUSKIND)
                                 *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                            ))
           END * NVL(PNX.TJXS,1)  --202001版本新增经济资本增减计调节系数
       END,
       P_I_DATE,
       P_I_DATE,
       PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND), --产品调节系数
       SDF.BUSKIND,
       SDF.INCOME,
       SDF.CONFIGRATE,
       SDF.CUST_EXE_RATE,
       SDF.PD,
       SDF.LGD,
       nvl(XF1.LGD_LEVEL,'999'),
       SDF.SMALLCORP,
       SDF.DUBILSTDRATE,
       SDF.RATECHGMODE,
       SDF.CUSTNAME,
       SDF.GROUPNAME,
       SDF.ZONE_PAR,
       SDF.BNKRATE,
       NVL(PNX.TJXS,1)  --202001版本新增经济资本增减计调节系数
        FROM STG_DAT_FRDK SDF, --法人贷款模型中间表
             PRM_PRODUCT PP, --一二三级产品对照表
             PRM_CS032 C32, --法人贷款方式类别对照表
             PRM_CS036 P36, --法人贷款期限档次定义表
             PRM_DA200011040 P40, --企业信用等级参数表
             (SELECT A.LGD_LEVEL,
                     LOWER_LIMIT,
                     UPPER_LIMIT,
                     B.PRODUCT_ID,
                     B.CREDIT_CODE,
                     B.PAR_COEF,
                     B.START_DATE,
                     B.END_DATE
                FROM (SELECT T.SEG_SEQ LGD_LEVEL,
                             T.LOWER LOWER_LIMIT,
                             NVL(LEAD(LOWER) OVER(ORDER BY TO_NUMBER(T.SEG_SEQ)),
                                 999) UPPER_LIMIT
                        FROM PRM_CSFR5 T
                       WHERE T.START_DATE <= P_I_DATE
                         AND T.END_DATE > P_I_DATE) A,
                     (SELECT PRODUCT_ID,
                             CREDIT_CODE,
                             LGD_LEVEL,
                             PAR_COEF,
                             START_DATE,
                             END_DATE
                        FROM PRM_XSFR1
                       WHERE START_DATE <= P_I_DATE
                         AND END_DATE > P_I_DATE) B
               WHERE A.LGD_LEVEL = B.LGD_LEVEL) XF1,
             PRM_XS032 X32, --法人贷款风险质量调节系数
             PRM_CS037 C37, --法人贷款拨备比例参数表
             PRM_XSFR3 X33,
             (SELECT ZONE_NO,
                     BELONG_ZONENO,
                     LINENO,
                     FIELD_CODE,
                     FIELD_NAME,
                     XS_VALUE,
                     START_DATE,
                     END_DATE
                FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                        FROM PRM_IRA_ORGAN
                       WHERE BELONG_BANK_FLAG = '4'
                         AND START_DATE <= P_I_DATE
                         AND END_DATE > P_I_DATE) A,
                     PRM_XS034 B
               WHERE A.BELONG_ZONENO = B.AREA_CODE
                 AND B.START_DATE <= P_I_DATE
                 AND B.END_DATE > P_I_DATE) X34,
             (SELECT PRODUCT_ID, NLOAN_STARTDAY, NLOAN_ENDDAY, TJXS
                FROM (SELECT P.PRODUCT_ID,
                             P.NLOAN_STARTDAY,
                             P.NLOAN_ENDDAY,
                             P.TJXS,
                             ROW_NUMBER() OVER(PARTITION BY P.PRODUCT_ID, P.NLOAN_STARTDAY ORDER BY P.IMPORT_TIME DESC) RN
                        FROM PRM_NLOAN_XS P
                       WHERE P.VALID_STATUS = '1')
               WHERE RN = 1) PNX --202001版本新增经济资本增减计调节系数
       WHERE SDF.PRODUCT_ID = PP.PRODUCT_ID
         AND SDF.PRODUCT_ID = XF1.PRODUCT_ID(+)
         AND SDF.CREDITLEVEL = XF1.CREDIT_CODE(+)
         AND SDF.LGD >= XF1.LOWER_LIMIT(+)
         AND SDF.LGD < XF1.UPPER_LIMIT(+)
         AND SDF.LEVELTWL = X32.FL12_ID --12级分类关联
         AND X33.QXDC_ID = P36.QXDC_ID --期限档次+信用等级关联
         AND SDF.CREDITLEVEL = X33.CREDIT_CODE --20100321新增关联条件
         AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') > DECODE(P36.LEFT_IN, '1', P36.LEFT_VALUE, '2', P36.LEFT_VALUE - 1)
         AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') < DECODE(P36.RIGHT_IN, '1', P36.RIGHT_VALUE, '2', P36.RIGHT_VALUE + 1)
         AND SDF.INDUSTRYCODE = X34.FIELD_CODE(+)
         AND SDF.ZONENO = X34.ZONE_NO(+)
         AND SDF.CREDITLEVEL = P40.DICT_CODE --企业信用等级
         AND SDF.LOANWAY = C32.LOAD_TYPE(+) --贷款方式
         AND X32.FL12_ID = C37.FL12_ID --拨备系数表
         AND PP.START_DATE <= P_I_DATE
         AND PP.END_DATE > P_I_DATE
         AND C32.START_DATE(+) <= P_I_DATE
         AND C32.END_DATE(+) > P_I_DATE
         AND C37.START_DATE <= P_I_DATE
         AND C37.END_DATE > P_I_DATE
         AND P36.START_DATE <= P_I_DATE
         AND P36.END_DATE > P_I_DATE
         AND P40.START_DATE <= P_I_DATE
         AND P40.END_DATE > P_I_DATE
         AND XF1.START_DATE(+) <= P_I_DATE
         AND XF1.END_DATE(+) > P_I_DATE
         AND X32.START_DATE <= P_I_DATE
         AND X32.END_DATE > P_I_DATE
         AND X33.START_DATE <= P_I_DATE
         AND X33.END_DATE > P_I_DATE
         AND X34.START_DATE(+) <= P_I_DATE
         AND X34.END_DATE(+) > P_I_DATE
         AND SDF.MODIFY_DATE = P_I_DATE
         AND SDF.LGD IS NOT NULL
         AND SDF.ZONE_PAR = P_I_ZONENO
         AND SDF.PRODUCT_ID = PNX.PRODUCT_ID(+)
         AND SDF.LOANDATE >= PNX.NLOAN_STARTDAY(+)
         AND SDF.LOANDATE <= PNX.NLOAN_ENDDAY(+)
      UNION ALL
      SELECT /*+ordered use_hash(SDF PP C32 P36 C37 P40 XF1 X32 X33 X34)*/
       SDF.LOANNO,
       SDF.LOANSQNO,
       SDF.ACCNO,
       SDF.ZONENO,
       SDF.PHYBRNO,
       SDF.CURRTYPE,
       SDF.SUBCODE,
       SDF.RESTAMOUNT,
       SDF.RESTAMOUNT_CM,
       SDF.LOANDATE,
       SDF.DUEDATE,
       SDF.REALBOAMT,
       SDF.REALBOAMT_CM,
       NULL, --实提拨比例
       SDF.CUSTCODE,
       SDF.LNMEMNO,
       SDF.ACCNO,
       SDF.LEVELTWL,
       SDF.LEVELFIVE,
       --贷款分类
       SDF.LOANWAY,
       SDF.SURVEYOR,
       SDF.INDUSTRYCODE,
       NULL,
       --年初信用等级
       NULL,
       --比照信用等级
       NULL,
       --技术监督局代码
       SDF.ENTRSIZE,
       SDF.GROUPCODE,
       SDF.AVCHCOF,
       SDF.LOANDIRT,
       NULL,
       --国际贸易融资转贷款的分类
       SDF.PRODUCT_ID, --三级产品名称
       PP.PRODUCT_NAME,
       SDF.CREDITLEVEL, --直接取
       P40.DICT_NAME,
       NULL, --担保档次编号
       NULL, --担保档次名称
       NVL(C32.DBFS_ID, V_C32),
       --贷款方式类别ID
       NVL(C32.DBFS_LB, V_P32), --贷款方式类别 去租prm_cs032
       NULL, --贷款期限
       NULL, --贷款期限档次名称
       P36.QXDC_ID,
       --贷款剩余期限档次编号
       P36.QXDC_NAME,
       --贷款剩余期限档次名称 取之prm_cs036
       NVL(XF1.PAR_COEF, V_X31), --基本配置系数 xs031
       NVL(X34.XS_VALUE, 1), --行业地区调节系数
       NVL(X33.PAR_COEF, 1),
       --剩余期限调节系数
       X32.XS_VALUE, --质量调节系数
       0, --贸易融资产品调节系数
       0, --项目贷款产品调节系数
       0, --国际融资转贷款产品调节系数
       CASE WHEN 
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END >= 0.125 
       THEN
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END
       ELSE 
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END * NVL(PNX.TJXS,1)
       END ,--20191216
       C37.BBPRECENT, --应提拨备比例
       SDF.RESTAMOUNT * C37.BBPRECENT, --应提拨备额
       SDF.RESTAMOUNT_CM * C37.BBPRECENT, --应提拨备额月积数
       DECODE(SIGN(SDF.RESTAMOUNT - SDF.REALBOAMT), -1, 0, (SDF.RESTAMOUNT - SDF.REALBOAMT)), (SDF.RESTAMOUNT) *
       CASE WHEN 
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END >= 0.125 
       THEN
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END
       ELSE 
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END * NVL(PNX.TJXS,1)
       END,--202001版本新增经济资本增减计调节系数
       SDF.RESTAMOUNT_CM  *
       CASE WHEN 
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END >= 0.125 
       THEN
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END
       ELSE 
         CASE
          WHEN SDF.CREDITLEVEL = '60'  THEN
             DECODE(SDF.LEVELFIVE,'10',v_dkzbpzb_level1,'20',v_dkzbpzb_level2,'30',v_dkzbpzb_level3,'40',v_dkzbpzb_level4,'50',v_dkzbpzb_level5,'0')
           WHEN PP.PRODUCT_ID = '104' THEN
             0
           ELSE
             DECODE(SIGN(NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                               DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                                decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                                ) -
                           v_par_coef),
                      -1,
                      v_par_coef,
                      NVL(PACK_DEAL_FRDK_1005.FUNC_PRM_XFRA(P_I_DATE, SDF.PRODUCT_ID),
                          DECODE(C32.DBFS_ID, '007', v_par_coef, NVL(XF1.PAR_COEF, V_X31)) * X32.XS_VALUE *
                          decode(SDF.LEVELFIVE,'30',1,'40',1,'50',1,
                               PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE,
                                                                  SDF.PRODUCT_ID,
                                                                  SDF.SUBCODE,
                                                                  SDF.BUSKIND)
                               *X33.PAR_COEF*DECODE(SDF.SMALLCORP,'1',V_XQYXS,1) * NVL(X34.XS_VALUE, 1))
                          ))
         END * NVL(PNX.TJXS,1)
       END,--202001版本新增经济资本增减计调节系数
       P_I_DATE,
       --'99991231',2014年8月版本将原来月中增量月末存量改成每日存量，时间拉链不需要，modified by caohui20140804
       P_I_DATE,--'99991231',
       PACK_DEAL_FRDK_1005_P.FUNC_PRM_XSFR2(P_I_DATE, SDF.PRODUCT_ID, SDF.SUBCODE, SDF.BUSKIND), --产品调节系数
       SDF.BUSKIND,
       SDF.INCOME,
       SDF.CONFIGRATE,
       SDF.CUST_EXE_RATE,
       SDF.PD,
       SDF.LGD,
       '999' LGD_LEVEL,
       SDF.SMALLCORP,
       SDF.DUBILSTDRATE,
       SDF.RATECHGMODE,
       SDF.CUSTNAME,
       SDF.GROUPNAME,
       SDF.ZONE_PAR,
       SDF.BNKRATE,
       NVL(PNX.TJXS,1) --202001版本新增经济资本增减计调节系数
        FROM STG_DAT_FRDK SDF, --法人贷款模型中间表
             --PRM_CS031 C31, --法人贷款产品定义表
             PRM_PRODUCT PP, --一二三级产品对照表
             PRM_CS032 C32, --法人贷款方式类别对照表
             PRM_CS036 P36, --法人贷款期限档次定义表
             PRM_DA200011040 P40, --企业信用等级参数表
             (SELECT B.PRODUCT_ID PRODUCT_ID,
                     B.XYVALUE CREDIT_CODE,
                     B.DBDC_ID DBDC_ID,
                     A.LOWER_LIMIT,
                     A.UPPER_LIMIT,
                     B.XS_VALUE PAR_COEF
                FROM (SELECT DBDC_ID,
                             DBDC_NAME,
                             DECODE(C35.LEFT_IN,
                                    '1',
                                    C35.LEFT_VALUE - 0.000000001,
                                    C35.LEFT_VALUE) LOWER_LIMIT,
                             DECODE(C35.RIGHT_IN,
                                    '1',
                                    C35.RIGHT_VALUE - 0.000000001,
                                    C35.RIGHT_VALUE) UPPER_LIMIT
                        FROM PRM_CS035 C35
                       WHERE C35.START_DATE <= P_I_DATE
                         AND C35.END_DATE > P_I_DATE) A,
                     (SELECT PRODUCT_ID, XYVALUE, DBDC_ID, XS_VALUE
                        FROM PRM_XS031
                       WHERE START_DATE <= P_I_DATE
                         AND END_DATE > P_I_DATE) B
               WHERE A.DBDC_ID = B.DBDC_ID) XF1,
              PRM_XS032 X32, --法人贷款风险质量调节系数
             PRM_CS037 C37, --法人贷款拨备比例参数表
             PRM_XSFR3 X33,
             --法人贷款信用风险期限调节系数表 修改成FD3 关联条件增加信用等级
             (SELECT ZONE_NO,
                     BELONG_ZONENO,
                     LINENO,
                     FIELD_CODE,
                     FIELD_NAME,
                     XS_VALUE,
                     START_DATE,
                     END_DATE
                FROM (SELECT DISTINCT ZONE_NO, BELONG_ZONENO
                        FROM PRM_IRA_ORGAN
                       WHERE BELONG_BANK_FLAG = '4'
                         AND START_DATE <= P_I_DATE
                         AND END_DATE > P_I_DATE) A,
                     PRM_XS034 B
               WHERE A.BELONG_ZONENO = B.AREA_CODE
                 AND B.START_DATE <= P_I_DATE
                 AND B.END_DATE > P_I_DATE) X34,
             (SELECT PRODUCT_ID, NLOAN_STARTDAY, NLOAN_ENDDAY, TJXS
                FROM (SELECT P.PRODUCT_ID,
                             P.NLOAN_STARTDAY,
                             P.NLOAN_ENDDAY,
                             P.TJXS,
                             ROW_NUMBER() OVER(PARTITION BY P.PRODUCT_ID, P.NLOAN_STARTDAY ORDER BY P.IMPORT_TIME DESC) RN
                        FROM PRM_NLOAN_XS P
                       WHERE P.VALID_STATUS = '1')
               WHERE RN = 1) PNX --202001版本新增经济资本增减计调节系数
       WHERE SDF.PRODUCT_ID = PP.PRODUCT_ID
         AND SDF.PRODUCT_ID = XF1.PRODUCT_ID(+)
         AND SDF.CREDITLEVEL = XF1.CREDIT_CODE(+)
         AND SDF.AVCHCOF >= XF1.LOWER_LIMIT(+)
         AND SDF.AVCHCOF <= XF1.UPPER_LIMIT(+)
         AND SDF.LEVELTWL = X32.FL12_ID --12级分类关联
         AND X33.QXDC_ID = P36.QXDC_ID --期限档次+信用等级关联
         AND SDF.CREDITLEVEL = X33.CREDIT_CODE --20100321新增关联条件
         AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') -  TO_DATE(P_I_DATE, 'YYYYMMDD') > DECODE(P36.LEFT_IN, '1', P36.LEFT_VALUE, '2', P36.LEFT_VALUE - 1)
         AND TO_DATE(SDF.DUEDATE, 'YYYYMMDD') - TO_DATE(P_I_DATE, 'YYYYMMDD') < DECODE(P36.RIGHT_IN, '1', P36.RIGHT_VALUE, '2', P36.RIGHT_VALUE + 1)
         AND SDF.INDUSTRYCODE = X34.FIELD_CODE(+)
         AND SDF.ZONENO = X34.ZONE_NO(+)
         AND SDF.CREDITLEVEL = P40.DICT_CODE --企业信用等级
         AND SDF.LOANWAY = C32.LOAD_TYPE(+) --贷款方式
         AND X32.FL12_ID = C37.FL12_ID --拨备系数表
         AND PP.START_DATE <= P_I_DATE
         AND PP.END_DATE > P_I_DATE
         AND C32.START_DATE(+) <= P_I_DATE
         AND C32.END_DATE(+) > P_I_DATE
         AND C37.START_DATE <= P_I_DATE
         AND C37.END_DATE > P_I_DATE
         AND P36.START_DATE <= P_I_DATE
         AND P36.END_DATE > P_I_DATE
         AND P40.START_DATE <= P_I_DATE
         AND P40.END_DATE > P_I_DATE
         AND X32.START_DATE <= P_I_DATE
         AND X32.END_DATE > P_I_DATE
         AND X33.START_DATE <= P_I_DATE
         AND X33.END_DATE > P_I_DATE
         AND X34.START_DATE(+) <= P_I_DATE
         AND X34.END_DATE(+) > P_I_DATE
         AND SDF.MODIFY_DATE = P_I_DATE
         AND SDF.LGD IS NULL
         AND SDF.ZONE_PAR = P_I_ZONENO
         AND SDF.PRODUCT_ID = PNX.PRODUCT_ID(+)
         AND SDF.LOANDATE >= PNX.NLOAN_STARTDAY(+)
         AND SDF.LOANDATE <= PNX.NLOAN_ENDDAY(+);
       v_begin_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
       v_step_no := '2.9';
       COMMIT;
      p_o_succeed := '0';
      --记载数据处理日志
      v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         v_end_time := TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
         p_o_succeed := '1';
         v_err_code := SQLCODE;
         v_err_txt := SUBSTR (SQLERRM, 1, 4000);
         --pack_getdata_log.pr_log_write (v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
         RAISE;
   END proc_deal_frdk_MON;
   
   PROCEDURE PROC_FRDK_DM_SUM(P_I_DATE IN VARCHAR2,P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_MONTH_DAY  NUMBER(3); --当月天数
  BEGIN
     --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_DEAL_GRDK_1005.PROC_FRDK_DM_SUM';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    
	SELECT LAST_DAY(TO_DATE(P_I_DATE, 'yyyymmdd')) - TO_DATE(SUBSTR(P_I_DATE, 1, 6) || '01', 'yyyymmdd') + 1 INTO V_MONTH_DAY FROM DUAL;
     --读取系统时间记载开始处理时间
    V_STEP_NO    := '2.2';
    
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;
     --开始汇总
	 FOR v_flag IN 2 .. 5 LOOP
		 MERGE INTO dm_cs_zfrdk dm
            USING (SELECT   prm.belong_zoneno zoneno, prm.belong_brno brno,
                            prm.belong_bank_flag bankflag, dm.qx,
                            dm.currtype, dm.cresort, dm.prodid, dm.loantype,
                            dm.hydm, dm.corpgrade, SUM (dm.value01) value01,
                            SUM (dm.value02) value02,
                            SUM (dm.value03) value03,
                            SUM (dm.value04) value04,
                            SUM (dm.value05) value05,
                            SUM (dm.value06) value06,
                            SUM (dm.value07) value07,
                            SUM (dm.value08) value08,
                            SUM (dm.value09) value09,
                            SUM (dm.value010) value010,
                            dm.smallcorp,
                            dm.lgd_level,
                            SUBSTR(PRM.BELONG_ZONENO,1,2) zone_par
                       FROM dm_cs_zfrdk dm, prm_ira_organ prm
                      WHERE dm.zoneno = prm.zone_no
                        AND dm.brno = prm.br_no
                        AND dm.bankflag = prm.bank_flag
                        AND prm.belong_bank_flag = v_flag
                        AND prm.bank_flag <> v_flag
                        AND p_i_date >= prm.start_date
                        AND p_i_date < prm.end_date
                   GROUP BY prm.belong_zoneno,
                            prm.belong_brno,
                            prm.belong_bank_flag,
                            dm.qx,
                            dm.currtype,
                            dm.cresort,
                            dm.prodid,
                            dm.loantype,
                            dm.hydm,
                            dm.corpgrade,
                            dm.smallcorp,
                            dm.lgd_level) tmp
            ON (dm.zoneno = tmp.zoneno
            AND dm.brno = tmp.brno
            AND dm.bankflag = tmp.bankflag
            AND dm.qx = tmp.qx
            AND dm.currtype = tmp.currtype
            AND dm.cresort = tmp.cresort
            AND dm.prodid = tmp.prodid
            AND dm.loantype = tmp.loantype
            AND dm.hydm = tmp.hydm
            AND dm.corpgrade = tmp.corpgrade
            and dm.smallcorp = tmp.smallcorp
            and dm.lgd_level = tmp.lgd_level)
            WHEN MATCHED THEN
               UPDATE
                  SET dm.value01 = dm.value01 + tmp.value01,
                      dm.value02 = dm.value02 + tmp.value02,
                      dm.value03 = dm.value03 + tmp.value03,
                      dm.value04 = dm.value04 + tmp.value04,
                      dm.value05 = dm.value05 + tmp.value05,
                      dm.value06 = dm.value06 + tmp.value06,
                      dm.value07 = dm.value07 + tmp.value07,
                      dm.value08 = dm.value08 + tmp.value08,
                      dm.value09 = dm.value09 + tmp.value09,
                      dm.value010 = dm.value010 + tmp.value010
            WHEN NOT MATCHED THEN
               INSERT (zoneno, brno, bankflag, qx, currtype, cresort, prodid,
                       loantype, hydm, corpgrade, value01, value02, value03,
                       value04, value05, value06, value07, value08, value09,
                       value010,smallcorp,lgd_level,zone_par)
               VALUES (tmp.zoneno, tmp.brno, tmp.bankflag, tmp.qx,
                       tmp.currtype, tmp.cresort, tmp.prodid, tmp.loantype,
                       tmp.hydm, tmp.corpgrade, tmp.value01, tmp.value02,
                       tmp.value03, tmp.value04, tmp.value05, tmp.value06,
                       tmp.value07, tmp.value08, tmp.value09, tmp.value010,
                       tmp.smallcorp,tmp.lgd_level,substr(tmp.zoneno,1,2));
      END LOOP;
      COMMIT;
     
	 P_O_SUCCEED := '0';
	 V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
	 --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END PROC_FRDK_DM_SUM;  
  
  PROCEDURE PROC_DEAL_FRDK_PRETREAT(P_I_DATE IN VARCHAR2, P_O_SUCCEED OUT VARCHAR2)IS
      v_num1         NUMBER;
      v_num2         NUMBER;
      v_pro_id       prm_xsfr2.product_id%TYPE;
      v_str1         prm_xsfr2.subject_all%TYPE;
      v_str1_old     prm_xsfr2.subject_all%TYPE;
      v_str2         prm_xsfr2.ywpz_all%TYPE;
      v_tmp_str1     prm_xsfr2.subject_all%TYPE;
      v_tmp_str2     prm_xsfr2.subject_all%TYPE;
      v_app_flag     prm_xsfr2.app_type%TYPE;
      v_par          prm_xsfr2.par_coef%TYPE;
      v_c32          prm_cs032.dbfs_id%TYPE;
      v_p32          prm_cs032.dbfs_lb%TYPE;
      v_x31          prm_xs031.xs_value%TYPE;
 CURSOR c
      IS
         SELECT a.product_id, a.subject_all, a.ywpz_all, a.app_type, a.par_coef FROM prm_xsfr2 a
          WHERE a.start_date <= p_i_date AND a.end_date > p_i_date;
 BEGIN
	  DELETE FROM ODS_TMP_FRDK2 A
       WHERE A.ROWID <> (SELECT MAX(B.ROWID) FROM ODS_TMP_FRDK2 B  WHERE A.Loanno= B.loanno AND A.Loansqno = B.Loansqno AND A.Accno = B.Accno);
        COMMIT;
		EXECUTE IMMEDIATE 'TRUNCATE TABLE prm_xsfr2_tmp';
      FOR v_c IN c
      LOOP
         v_pro_id := v_c.product_id;
         v_str1_old := v_c.subject_all;
         v_str2 := v_c.ywpz_all;
         v_app_flag := v_c.app_type;
         v_par := v_c.par_coef;
         IF v_str2 = 'N/A' OR INSTR (v_str2, '+') = 0
         THEN
			SELECT LENGTH ('+' || v_str1_old) - LENGTH (REPLACE (REPLACE ('+' || v_str1_old, '+', ''), '-', '')) INTO v_num1 FROM DUAL;
			v_str1 := REPLACE (REPLACE ('+' || v_str1_old, '+', ','), '-', ',');
			FOR i IN 1 .. v_num1
            LOOP
			   IF i = v_num1 THEN
				  v_tmp_str1 := SUBSTR (v_str1, INSTR (v_str1, ',', 1, v_num1));
				  IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) = '-' THEN
                     --如果科目为负，则剔除该科目
                     NULL;
                  ELSE
					 INSERT INTO prm_xsfr2_tmp (product_id, subject_code, ywpz_code, app_type, par_coef)
                          VALUES (v_pro_id, LTRIM (v_tmp_str1, ','), v_str2, v_app_flag, v_par);
					 COMMIT;
                  END IF;
               ELSE
                  --如果不为最后个科目号，则从该字符“，”出现i次处开始截取长度为该科目的字符
				  v_tmp_str1 := SUBSTR (v_str1, INSTR (v_str1, ',', 1, i), INSTR (v_str1, ',', 1, i + 1) - INSTR (v_str1, ',', 1, i));
				  IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) = '-' THEN
                     NULL;
                  ELSE
					 INSERT INTO prm_xsfr2_tmp (product_id, subject_code, ywpz_code, app_type, par_coef)
                          VALUES (v_pro_id, LTRIM (v_tmp_str1, ','), v_str2, v_app_flag, v_par);
                     COMMIT;
                  END IF;
               END IF;
            END LOOP;
         --如果业务品种也为xx+xx之类的类型，则分科目+业务品种进行循环
         ELSIF 
		    INSTR (v_str2, '+') <> 0
         THEN
			SELECT LENGTH ('+' || v_str1_old) - LENGTH (REPLACE (REPLACE ('+' || v_str1_old, '+', ''),'-', ''  )) INTO v_num1 FROM DUAL;
			SELECT LENGTH ('+' || v_str2) - LENGTH (REPLACE (REPLACE ('+' || v_str2, '+', ''), '-', '' )) INTO v_num2 FROM DUAL;
			v_str1 := REPLACE (REPLACE ('+' || v_str1_old, '+', ','), '-', ',');
			v_str2 := REPLACE (REPLACE ('+' || v_str2, '+', ','), '-', ',');
			FOR j IN 1 .. v_num2 LOOP
               IF j = v_num2
               THEN
                  v_tmp_str2 := LTRIM (SUBSTR (v_str2, INSTR (v_str2, ',', 1, v_num2)), ',' );
               ELSE
				  v_tmp_str2 := LTRIM (SUBSTR (v_str2, INSTR (v_str2, ',', 1, j), INSTR (v_str2, ',', 1, j + 1) - INSTR (v_str2, ',', 1, j)), ',' );
               END IF;
			   FOR i IN 1 .. v_num1 LOOP
				  IF i = v_num1 THEN
					 v_tmp_str1 := SUBSTR (v_str1, INSTR (v_str1, ',', 1, v_num1));
					 IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) = '-' THEN
						NULL;
                     ELSE
						INSERT INTO prm_xsfr2_tmp (product_id, subject_code, ywpz_code, app_type, par_coef)
                             VALUES (v_pro_id, LTRIM (v_tmp_str1, ','), v_tmp_str2, v_app_flag, v_par);
						COMMIT;
                     END IF;
                  ELSE
					 v_tmp_str1 := SUBSTR (v_str1, INSTR (v_str1, ',', 1, i), INSTR (v_str1, ',', 1, i + 1) - INSTR (v_str1, ',', 1, i));
					 IF SUBSTR (v_str1_old, INSTR (v_str1, v_tmp_str1) - 1, 1) = '-' THEN
                        NULL;
                     ELSE
						INSERT INTO prm_xsfr2_tmp (product_id, subject_code, ywpz_code, app_type, par_coef )
                             VALUES (v_pro_id, LTRIM (v_tmp_str1, ','), v_tmp_str2, v_app_flag, v_par );
                        COMMIT;
                     END IF;
                  END IF;
               END LOOP;
            END LOOP;
         END IF;
      END LOOP;
      COMMIT;
   P_O_SUCCEED:= '0';
   COMMIT;
   EXCEPTION  WHEN OTHERS THEN
    ROLLBACK;
    P_O_SUCCEED := '1';
    RAISE;
   END PROC_DEAL_FRDK_PRETREAT;
END pack_deal_frdk_1005_P;
//


CREATE OR REPLACE PACKAGE PACK_DEAL_GRCK_201604 IS
  PROCEDURE PROC_DEAL_CAP_GRKH_SUM(P_I_DATE    IN VARCHAR2,
                                   P_O_SUCCEED OUT VARCHAR2);
END PACK_DEAL_GRCK_201604;
//

CREATE OR REPLACE PACKAGE BODY PACK_DEAL_GRCK_201604 IS
   PROCEDURE PROC_DEAL_CAP_GRKH_SUM(P_I_DATE    IN VARCHAR2,
                                    P_O_SUCCEED OUT VARCHAR2) IS
    --以下变量为记录日志时使用的。
    V_PROC_NAME  L_GETDATA_CALL.PROC_NAME%TYPE;
    V_LOG_LEVEL  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    V_BEGIN_TIME L_GETDATA_CALL.BEGIN_TIME%TYPE;
    V_END_TIME   L_GETDATA_CALL.END_TIME%TYPE;
    V_ERR_CODE   L_GETDATA_ERR.ERR_CODE%TYPE;
    V_ERR_TXT    L_GETDATA_ERR.ERR_TXT%TYPE;
    V_STEP_NO    L_GETDATA_ERR.STEP_NO%TYPE;
    V_SQL_TXT    L_GETDATA_ERR.SQL_TXT%TYPE;
    V_MONTH_L1 VARCHAR2(8);
    V_MONTH_L2 VARCHAR2(8);
    V_MONTH_L3 VARCHAR2(8);
    V_MONTH_L4 VARCHAR2(8);
    V_MONTH_L5 VARCHAR2(8);
    V_MONTH_L6 VARCHAR2(8);
    V_MONTH_L7 VARCHAR2(8);
    V_MONTH_L8 VARCHAR2(8);
    V_MONTH_L9 VARCHAR2(8);
    V_MONTH_L10 VARCHAR2(8);
    V_MONTH_L11 VARCHAR2(8);
    V_RMBVRATE NUMBER(17, 6);
  BEGIN

    --记载数据处理日志
    V_STEP_NO    := '1';
    V_PROC_NAME  := 'PACK_DEAL_GRCK_201604.PROC_DEAL_CAP_GRKH_SUM';
    V_LOG_LEVEL  := '3';
    V_BEGIN_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    V_STEP_NO    := '2.1';
    
	SELECT TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') INTO V_BEGIN_TIME FROM DUAL;

    V_MONTH_L1 := TO_CHAR(ADD_MONTHS(TO_DATE(P_I_DATE, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L2 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L1, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L3 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L2, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L4 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L3, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L5 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L4, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L6 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L5, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L7 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L6, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L8 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L7, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L9 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L8, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L10 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L9, 'YYYYMMDD'), -1), 'YYYYMMDD');
    V_MONTH_L11 := TO_CHAR(ADD_MONTHS(TO_DATE(V_MONTH_L10, 'YYYYMMDD'), -1), 'YYYYMMDD');
	SELECT DECODE(MAX(RMBVRATE), 0 ,1, MAX(RMBVRATE)) RMBVRATE INTO V_RMBVRATE FROM PRM_RMBVRATE WHERE DATA_DATE = P_I_DATE AND CURRENCY = '001';
    --开始按客户维度汇总个人存款贷款信用卡12月平均经济资本
    V_STEP_NO := '2.2';
    INSERT INTO DW_CAP_GRKH_SUM
      (DATA_DATE, CISNO, CAP_NOW, CAP_12MON)
      SELECT P_I_DATE, TT.CISNO, SUM(TT.CAP_NOW), SUM(TT.CAP_12MON)
        FROM (SELECT/*+no_index(T2 PK_DW_MOD_GRDK_UG)*/ T2.CUSTCIS CISNO,
                     SUM(CASE T2.DATA_DATE
                           WHEN P_I_DATE THEN
                            T2.ECOST
                           ELSE
                            0
                         END) CAP_NOW,
                     SUM(T2.ECOST) / 12 CAP_12MON
                FROM DW_MOD_GRDK_UG T2
               WHERE T2.DATA_DATE IN (P_I_DATE, V_MONTH_L1, V_MONTH_L2, V_MONTH_L3, V_MONTH_L4, V_MONTH_L5, V_MONTH_L6, V_MONTH_L7, V_MONTH_L8, V_MONTH_L9, V_MONTH_L10, V_MONTH_L11)
               GROUP BY T2.CUSTCIS
              UNION ALL
              --信用卡有外币余额，也需预先做折币处理，从RWA汇率表取最新一期汇率折12个月金额
              SELECT T3.CINO CISNO,
                     SUM(CASE T3.DATA_DATE
                           WHEN P_I_DATE THEN
                            T3.ECOST * CASE WHEN TO_NUMBER(T3.CURRTYPE) = 1 THEN 1 ELSE NVL(R1.RMBVRATE / V_RMBVRATE, 1) END
                           ELSE
                            0
                         END) CAP_NOW,
                     SUM(T3.ECOST * CASE WHEN TO_NUMBER(T3.CURRTYPE) = 1 THEN 1 ELSE NVL(R1.RMBVRATE / V_RMBVRATE, 1) END) / 12 CAP_12MON
                FROM DW_MOD_YHK_UG T3
                LEFT JOIN (SELECT CURRENCY, RMBVRATE
                             FROM PRM_RMBVRATE
                            WHERE DATA_DATE = P_I_DATE) R1
                  ON T3.CURRTYPE = R1.CURRENCY
               WHERE T3.DATA_DATE IN (P_I_DATE, V_MONTH_L1, V_MONTH_L2, V_MONTH_L3, V_MONTH_L4, V_MONTH_L5, V_MONTH_L6, V_MONTH_L7, V_MONTH_L8, V_MONTH_L9, V_MONTH_L10, V_MONTH_L11)
               GROUP BY T3.CINO
              UNION ALL
              SELECT T1.CISNO, GREATEST(T1.CAP_NOW, 0), GREATEST(T1.CAP_12MON, 0) --201607 modify by 40,经济资本不能为负
                FROM ODS_TD_CAP_GRCK T1
               WHERE T1.DATA_DATE = P_I_DATE) TT
       GROUP BY TT.CISNO;
	COMMIT;
	P_O_SUCCEED := '0';
	V_END_TIME := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
	--PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'SUCCESS', V_STEP_NO, V_SQL_TXT);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_END_TIME  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
      P_O_SUCCEED := '1';
      V_ERR_CODE  := SQLCODE;
      V_ERR_TXT   := SUBSTR(SQLERRM, 1, 4000);
	  --PACK_GETDATA_LOG.PR_LOG_WRITE(V_PROC_NAME, V_LOG_LEVEL, V_BEGIN_TIME, V_END_TIME, 'FAILED', V_STEP_NO, V_SQL_TXT, V_ERR_CODE, V_ERR_TXT);
      RAISE;
  END PROC_DEAL_CAP_GRKH_SUM;

END PACK_DEAL_GRCK_201604;
//


CREATE OR REPLACE package PACK_DEAL_PVMSDATA_SR is
  procedure proc_deal_pjtxbb_sr --处理票据贴现本币首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_exchage_rate_sr --处理汇率表首日
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_yhk_sr --处理银行卡首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_frdk_sr --处理法人贷款首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_grdk_sr --处理法人贷款首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_nfgnled_forpv --处理法人贷款首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_pjtxwb_sr --处理票据贴现外币首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_wxzc_sr --处理无息资产首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_zjyw_sr --处理资金业务首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
  procedure proc_deal_bwzc_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              );
  procedure proc_deal_qtfx_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              );
  procedure proc_deal_zqtz_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              );
  procedure proc_deal_total_sr --处理汇总首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   );
end PACK_DEAL_PVMSDATA_SR;
//

CREATE OR REPLACE package body PACK_DEAL_PVMSDATA_SR is
  procedure proc_deal_exchage_rate_sr --处理汇率表首日
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_date_bak     varchar2(8);
  begin
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_EXCHANGE_RATE_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    v_step_no    := '2';
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')),'YYYYMMDD');
    end loop;
    execute immediate 'truncate table prm_exchange_rate_pv';
    v_step_no := '2';
    for v_count in 1 .. v_mounth_count loop
      insert into prm_exchange_rate_pv
        select O.ZONENO,
               O.CURRTYPE,
               O.USCVRATE / 1000000000,
               v_date_set(v_count),
               to_char(to_date(v_date_set(v_count), 'yyyymmdd') + 1, 'yyyymmdd'),
               O.CURRSMBL
          from dw_dat_hfcaexoa o
         where data_date = v_date_set(v_count);

      insert into PRM_EXCHANGE_RATE_pv
        (ZONE_NO, CURRENCY, USCVRATE, START_DATE, END_DATE, CURRSMBL)
        select distinct b.zone_no,
                        a.CURRENCY,
                        a.USCVRATE,
                        a.START_DATE,
                        a.END_DATE,
                        a.CURRSMBL
          from PRM_EXCHANGE_RATE_pv a, prm_ira_organ b
         where a.zone_no = '0200'
           and b.start_date <= v_date_set(v_count)
           and b.end_date > v_date_set(v_count)
           and b.bank_flag = '4'
           and b.zone_no not in (select distinct zone_no from PRM_EXCHANGE_RATE_pv c);
    end loop;

    insert into prm_exchange_rate_pv
    (zone_no,currency,uscvrate,start_date,end_date,currsmbl)
    select '1318',b.currency,b.uscvrate,b.start_date,b.end_date,b.currsmbl
    from prm_exchange_rate_pv b
    where b.zone_no='1311';
    insert into prm_exchange_rate_pv
    (zone_no,currency,uscvrate,start_date,end_date,currsmbl)
    select '2321',b.currency,b.uscvrate,b.start_date,b.end_date,b.currsmbl
    from prm_exchange_rate_pv b
    where b.zone_no='2307';
    commit;
    p_o_succeed := '0';
    v_end_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS',  v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_exchage_rate_sr;

  procedure proc_deal_pjtxbb_sr --处理票据贴现本币经济资本首日
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_flag       number(1) := 3; --行级别
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_date_bak     varchar2(8);
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_PJTXBB_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    -----------开始计算截止到p_i_date之前有多少个月
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    -----------把数组中放入每个月月末日期
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')), 'YYYYMMDD');
    end loop;
    --删除中间表中当日数据
    execute immediate 'truncate table stg_tmp_pjtxbb';
    v_step_no := '2';
      insert into stg_tmp_pjtxbb
        (zoneno,
         brno,
         bankflag,
         subcode,
         currtype,
         ecost,
         data_date,
         diccode)
        select zone, brno, '2', subcode, '001', sum(capital), start_date, '100'
          from dw_mod_bmsbill
         where start_date in ('20080131','20080229','20080331','20080430','20080531','20080630','20080731','20080831','20080930')
         group by zone, brno, subcode,start_date;
    update stg_tmp_pjtxbb a
       set a.ZONENO = (select b.zoneno_m from prm_organmap b where a.ZONENO = b.ZONENO and a.brno = b.brno)
     where a.zoneno in (select zoneno from prm_organmap b)
       and a.brno in (select brno from prm_organmap b);

    for v_flag in 3 .. 4 loop
      merge into stg_tmp_pjtxbb stg
      using (select prm.belong_zoneno zoneno,
                    prm.belong_brno brno,
                    prm.belong_bank_flag bankflag,
                    stg.subcode,
                    stg.currtype,
                    sum(stg.ecost) ecost,
                    stg.data_date,
                    stg.diccode
               from stg_tmp_pjtxbb stg, prm_ira_organ prm
              where stg.zoneno = prm.zone_no
                and stg.brno = prm.br_no
                and stg.bankflag = prm.bank_flag
                and prm.belong_bank_flag = v_flag
                and prm.bank_flag <> v_flag
                and data_date >= prm.start_date
                and data_date < prm.end_date
              group by prm.belong_zoneno,
                       prm.belong_brno,
                       prm.belong_bank_flag,
                       stg.subcode,
                       stg.currtype,
                       stg.data_date,
                       stg.diccode) tmp
      on (stg.zoneno = tmp.zoneno and stg.brno = tmp.brno and stg.bankflag = tmp.bankflag and stg.subcode = tmp.subcode and stg.currtype = tmp.currtype and stg.data_date = tmp.data_date and stg.diccode = tmp.diccode)
      when matched then
        update set stg.ecost = stg.ecost + tmp.ecost
      when not matched then
        insert
          (stg.zoneno,
           stg.brno,
           stg.bankflag,
           stg.subcode,
           stg.currtype,
           stg.ecost,
           stg.data_date,
           stg.diccode)
        values
          (tmp.zoneno,
           tmp.brno,
           tmp.bankflag,
           tmp.subcode,
           tmp.currtype,
           tmp.ecost,
           tmp.data_date,
           tmp.diccode);
    end loop;
    insert into stg_tmp_pjtxbb
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             '994',
             sum(decode(a.currtype, '001', ecost, ecost / uscvrate)),
             data_date,
             diccode
        from stg_tmp_pjtxbb a, PRM_EXCHANGE_RATE_PV b
       where a.zoneno = b.zone_no
         and a.currtype in ('001', '992')
         and a.data_date >= b.start_date
         and a.data_date < b.end_date
         and b.currency = '001'
       group by zoneno, brno, bankflag, subcode, data_date, diccode;
    p_o_succeed := '0';
    commit;
    v_end_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_pjtxbb_sr;

  procedure proc_deal_yhk_sr(p_i_date    in varchar2, --输入日期
                             p_o_succeed out varchar2 --输出标志
                             ) is
    v_flag       number(1) := 3; --行级别
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_date_bak     varchar2(8);
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_YHK_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    -----------开始计算截止到p_i_date之前有多少个月
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    -----------把数组中放入每个月月末日期
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')), 'YYYYMMDD');
    end loop;
    --删除中间表中当日数据
    execute immediate 'truncate table stg_tmp_yhk';
    v_step_no := '2';
      insert into stg_tmp_yhk
        (zoneno,
         brno,
         bankflag,
         subcode,
         currtype,
         ecost,
         data_date,
         diccode)
        select a.openzone,
               a.openbrno,
               mag.BANK_FLAG,
               a.subcode,
               a.currtype,
               sum(capital),
               a.data_date,
               '102'
          from dw_mod_card a,prm_ira_organ mag
         where a.data_date in ('20080131','20080229','20080331','20080430','20080531','20080630','20080731','20080831','20080930')
         and A.openZONE = MAG.ZONE_NO AND A.openBRNO = MAG.BR_NO and  mag.BANK_FLAG in ('2','3','4')
         group by a.openzone, a.openbrno, a.subcode, a.currtype,a.data_date,mag.bank_flag;
    update stg_tmp_yhk a
       set a.ZONENO = (select b.zoneno_m from prm_organmap b where a.ZONENO = b.ZONENO and a.brno = b.brno)
     where a.zoneno in (select zoneno from prm_organmap b)
       and a.brno in (select brno from prm_organmap b);

    for v_flag in 3 .. 4 loop
      merge into stg_tmp_yhk stg
      using (select prm.belong_zoneno zoneno,
                    prm.belong_brno brno,
                    prm.belong_bank_flag bankflag,
                    stg.subcode,
                    stg.currtype,
                    sum(stg.ecost) ecost,
                    stg.data_date,
                    stg.diccode
               from stg_tmp_yhk stg, prm_ira_organ prm
              where stg.zoneno = prm.zone_no
                and stg.brno = prm.br_no
                and stg.bankflag = prm.bank_flag
                and prm.belong_bank_flag = v_flag
                and prm.bank_flag <> v_flag
                and data_date >= prm.start_date
                and data_date < prm.end_date
              group by prm.belong_zoneno,
                       prm.belong_brno,
                       prm.belong_bank_flag,
                       stg.subcode,
                       stg.currtype,
                       stg.data_date,
                       stg.diccode) tmp
      on (stg.zoneno = tmp.zoneno and stg.brno = tmp.brno and stg.bankflag = tmp.bankflag and stg.subcode = tmp.subcode and stg.currtype = tmp.currtype and stg.data_date = tmp.data_date and stg.diccode = tmp.diccode)
      when matched then
        update set stg.ecost = stg.ecost + tmp.ecost
      when not matched then
        insert
          (stg.zoneno,
           stg.brno,
           stg.bankflag,
           stg.subcode,
           stg.currtype,
           stg.ecost,
           stg.data_date,
           stg.diccode)
        values
          (tmp.zoneno,
           tmp.brno,
           tmp.bankflag,
           tmp.subcode,
           tmp.currtype,
           tmp.ecost,
           tmp.data_date,
           tmp.diccode);
    end loop;
    ---------------------开始外币折美元-----------------
    insert into stg_tmp_yhk
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             '992',
             sum(a.ecost * b.uscvrate),
             a.data_date,
             a.diccode
        from stg_tmp_yhk a, prm_exchange_rate_PV b
       where a.currtype <> '001'
         and a.zoneno = b.zone_no
         and a.currtype = b.currency
         and data_date >= b.start_date
         and data_date < b.end_date
       group by zoneno, brno, bankflag, subcode, data_date, diccode;
    ----------------开始本外币折人民币----------------
    insert into stg_tmp_yhk
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             '994',
             sum(decode(a.currtype, '001', a.ecost, a.ecost / b.uscvrate)),
             a.data_date,
             a.diccode
        from stg_tmp_yhk a, prm_exchange_rate_PV b
       where a.currtype in ('001', '992')
         and a.zoneno = b.zone_no
         and b.currency = '001'
         and data_date >= b.start_date
         and data_date < b.end_date
       group by zoneno, brno, bankflag, subcode, data_date, diccode;
    ---------------------结束银行卡汇总存储过程------------
    p_o_succeed := '0';
    commit;
    v_end_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_yhk_sr;

  procedure proc_deal_frdk_sr --处理法人贷款经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_flag       number(1) := 3; --行级别
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_date_bak     varchar2(8);
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_FRDK_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    -----------开始计算截止到p_i_date之前有多少个月
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    -----------把数组中放入每个月月末日期
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')), 'YYYYMMDD');
    end loop;
    --删除中间表中当日数据
    execute immediate 'truncate table stg_tmp_frdk';
    v_step_no := '2';
      commit;
    for v_count in 7 .. v_mounth_count loop
      commit;
    end loop;
    --特殊地区改变地区号
    update stg_tmp_frdk a
       set a.ZONENO = (select b.zoneno_m from prm_organmap b where a.ZONENO = b.ZONENO and a.brno = b.brno)
     where a.zoneno in (select zoneno from prm_organmap b)
       and a.brno in (select brno from prm_organmap b);
    p_o_succeed := '0';
    commit;
    v_end_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_frdk_sr;

  procedure proc_deal_grdk_sr --处理个人贷款经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_flag       number(1) := 3; --行级别
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_date_bak     varchar2(8);
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_GRDK_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    -----------开始计算截止到p_i_date之前有多少个月
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    -----------把数组中放入每个月月末日期
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')), 'YYYYMMDD');
    end loop;
    --删除中间表中当日数据
    execute immediate 'truncate table stg_tmp_grdk';
    v_step_no := '2';
      insert into stg_tmp_grdk
        (zoneno,
         brno,
         bankflag,
         subcode,
         currtype,
         ecost,
         data_date,
         diccode)
        select zoneno,
               actbrno,
               '2',
               subcode,
               currtype,
               sum(ecost),
               start_date,
               '104'
          from dw_mod_grdk_c
         where start_date in ('20080131','20080229','20080331','20080430','20080531','20080630','20080731','20080831','20080930')
         group by zoneno, actbrno, subcode, currtype,start_date;
      commit;
    for v_count in 7 .. v_mounth_count loop
      insert into stg_tmp_grdk
        (zoneno,
         brno,
         bankflag,
         subcode,
         currtype,
         ecost,
         data_date,
         diccode)
        select zoneno,
               actbrno,
               '2',
               subcode,
               currtype,
               sum(ecost),
               v_date_set(v_count),
               '104'
          from dw_mod_grdk
         where start_date <= v_date_set(v_count)
           and v_date_set(v_count) <= end_date
         group by zoneno, actbrno, subcode, currtype;
      commit;
    end loop;
    --特殊地区改变地区号
    update stg_tmp_grdk a
       set a.ZONENO = (select b.zoneno_m from prm_organmap b where a.ZONENO = b.ZONENO and a.brno = b.brno)
     where a.zoneno in (select zoneno from prm_organmap b) and a.brno in (select brno from prm_organmap b);
    -----------------------------
    for v_flag in 3 .. 4 loop
      merge into stg_tmp_grdk stg
      using (select prm.belong_zoneno zoneno,
                    prm.belong_brno brno,
                    prm.belong_bank_flag bankflag,
                    stg.subcode,
                    stg.currtype,
                    sum(stg.ecost) ecost,
                    stg.data_date,
                    stg.diccode
               from stg_tmp_grdk stg, prm_ira_organ prm
              where stg.zoneno = prm.zone_no
                and stg.brno = prm.br_no
                and stg.bankflag = prm.bank_flag
                and prm.belong_bank_flag = v_flag
                and prm.bank_flag <> v_flag
                and data_date >= prm.start_date
                and data_date < prm.end_date
              group by prm.belong_zoneno,
                       prm.belong_brno,
                       prm.belong_bank_flag,
                       stg.subcode,
                       stg.currtype,
                       stg.data_date,
                       stg.diccode) tmp
      on (stg.zoneno = tmp.zoneno and stg.brno = tmp.brno and stg.bankflag = tmp.bankflag and stg.subcode = tmp.subcode and stg.currtype = tmp.currtype and stg.data_date = tmp.data_date and stg.diccode = tmp.diccode)
      when matched then
        update set stg.ecost = stg.ecost + tmp.ecost
      when not matched then
        insert
          (stg.zoneno,
           stg.brno,
           stg.bankflag,
           stg.subcode,
           stg.currtype,
           stg.ecost,
           stg.data_date,
           stg.diccode)
        values
          (tmp.zoneno,
           tmp.brno,
           tmp.bankflag,
           tmp.subcode,
           tmp.currtype,
           tmp.ecost,
           tmp.data_date,
           tmp.diccode);
    end loop;
    ---------------------开始外币折美元-----------------
    insert into stg_tmp_grdk
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             '992',
             sum(a.ecost * b.uscvrate),
             a.data_date,
             a.diccode
        from stg_tmp_grdk a, prm_exchange_rate_PV b
       where a.currtype <> '001'
         and a.zoneno = b.zone_no
         and a.currtype = b.currency
         and a.data_date >= b.start_date
         and a.data_date < b.end_date
       group by zoneno, brno, bankflag, subcode, data_date, diccode;
    ----------------开始本外币折人民币----------------
    insert into stg_tmp_grdk
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             '994',
             sum(decode(a.currtype, '001', a.ecost, a.ecost / b.uscvrate)),
             a.data_date,
             a.diccode
        from stg_tmp_grdk a, prm_exchange_rate_PV b
       where a.currtype in ('001', '992')
         and a.zoneno = b.zone_no
         and b.currency = '001'
         and data_date >= b.start_date
         and data_date < b.end_date
       group by zoneno, brno, bankflag, subcode, data_date, diccode;
    ---------------------结束个人贷款汇总存储过程------------
    p_o_succeed := '0';
    commit;
    v_end_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_grdk_sr;

  procedure proc_deal_nfgnled_forpv --处理法人贷款首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code   L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt    L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no    L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt    L_GETDATA_ERR.SQL_TXT%TYPE;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_bankflag     number(1) := 2;
    v_date_bak     varchar2(8);
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_GRDK_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    -----------开始计算截止到p_i_date之前有多少个月
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    -----------把数组中放入每个月月末日期
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')), 'YYYYMMDD');
    end loop;
    --删除中间表中当日数据
    execute immediate 'truncate table stg_dat_nfgnled_pv';
      insert into stg_dat_nfgnled_pv
        (zoneno,
         brno,
         bankflag,
         currtype,
         subcode,
         tddrbal,
         tdcrbal,
         data_date)
        select a.zoneno,
               a.brno,
               '2',
               a.currtype,
               a.subcode,
               nvl(a.tddrbal / 100, 0),
               nvl(a.tdcrbal / 100, 0),
               a.data_date
          from dw_dat_nfgnled a
         where a.data_date in ('20080131','20080229','20080331','20080430','20080531','20080630','20080731','20080831','20080930');
    --end loop;
    commit;
    --特殊地区改变地区号
    update STG_DAT_NFGNLED_pv a
       set a.ZONENO = (select b.zoneno_m from prm_organmap b where a.ZONENO = b.ZONENO and a.brno = b.brno)
     where a.zoneno in (select zoneno from prm_organmap b) and a.brno in (select brno from prm_organmap b);
    -----------------
    FOR V_BANKFLAG IN 2 .. 4 LOOP
      MERGE INTO STG_DAT_NFGNLED_pv S
      USING (SELECT PRM.BELONG_ZONENO,
                    PRM.BELONG_BRNO,
                    PRM.BELONG_BANK_FLAG,
                    STG.CURRTYPE,
                    SUM(TDDRBAL) TDDRBAL,
                    SUM(TDCRBAL) TDCRBAL,
                    STG.SUBCODE,
                    stg.data_date
               FROM STG_DAT_NFGNLED_pv STG, PRM_IRA_ORGAN PRM
              WHERE PRM.ZONE_NO = STG.ZONENO
                AND PRM.BR_NO = STG.BRNO
                AND PRM.BANK_FLAG = STG.BANKFLAG
                AND PRM.START_DATE <= data_date
                AND PRM.END_DATE > data_date
                AND PRM.BELONG_BANK_FLAG = V_BANKFLAG
                AND PRM.BANK_FLAG <> V_BANKFLAG
              GROUP BY PRM.BELONG_ZONENO,
                       PRM.BELONG_BRNO,
                       PRM.BELONG_BANK_FLAG,
                       STG.CURRTYPE,
                       STG.SUBCODE,
                       stg.data_date) TMP
      ON (S.ZONENO = TMP.BELONG_ZONENO AND S.BRNO = TMP.BELONG_BRNO AND S.BANKFLAG = TMP.BELONG_BANK_FLAG AND S.CURRTYPE = TMP.CURRTYPE AND S.SUBCODE = TMP.SUBCODE and s.data_date = tmp.data_date)
      WHEN MATCHED THEN
        UPDATE
           SET S.TDDRBAL = NVL(S.TDDRBAL, 0) + NVL(TMP.TDDRBAL, 0),
               S.TDCRBAL = NVL(S.TDCRBAL, 0) + NVL(TMP.TDCRBAL, 0)
      WHEN NOT MATCHED THEN
        INSERT
          (S.ZONENO,
           S.BRNO,
           S.BANKFLAG,
           S.CURRTYPE,
           S.TDDRBAL,
           S.TDCRBAL,
           S.SUBCODE,
           S.DATA_DATE)
        VALUES
          (TMP.BELONG_ZONENO,
           TMP.BELONG_BRNO,
           TMP.BELONG_BANK_FLAG,
           TMP.CURRTYPE,
           TMP.TDDRBAL,
           TMP.TDCRBAL,
           TMP.SUBCODE,
           tmp.data_date);
    END LOOP;
    commit;
    INSERT INTO STG_DAT_NFGNLED_pv
      (ZONENO,
       BRNO,
       bankflag,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       DATA_DATE)
      SELECT T.ZONENO,
             T.BRNO,
             t.bankflag,
             '992',
             t.SUBCODE,
             SUM(T.TDDRBAL * R.USCVRATE),
             SUM(T.TDCRBAL * R.USCVRATE),
             t.DATA_DATE
        FROM STG_DAT_NFGNLED_pv T, PRM_EXCHANGE_RATE_PV R
       WHERE T.ZONENO = R.ZONE_NO
         AND T.CURRTYPE = R.CURRENCY
         AND T.CURRTYPE NOT IN ('001', '992', '994')
         AND R.START_DATE <= data_date
         AND R.END_DATE > data_date
       GROUP BY T.ZONENO, T.BRNO, t.bankflag, T.SUBCODE, t.DATA_DATE;

    --本外币合计折人民币
    INSERT INTO STG_DAT_NFGNLED_pv
      (ZONENO,
       BRNO,
       bankflag,
       CURRTYPE,
       SUBCODE,
       TDDRBAL,
       TDCRBAL,
       DATA_DATE)
      SELECT T.ZONENO,
             T.BRNO,
             t.bankflag,
             '994',
             t.SUBCODE,
             SUM(T.TDDRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             SUM(T.TDCRBAL * R.USCVRATE *
                 DECODE(NVL(I.USCVRATE, 0), 0, 0, 1 / I.USCVRATE)),
             t.DATA_DATE
        FROM STG_DAT_NFGNLED_pv   T,
             PRM_EXCHANGE_RATE_PV R,
             PRM_EXCHANGE_RATE_PV I
       WHERE T.ZONENO = R.ZONE_NO
         AND T.CURRTYPE = R.CURRENCY
         AND T.CURRTYPE NOT IN ('992', '994')
         AND T.ZONENO = I.ZONE_NO --按地区取人民币折美元汇率
         AND I.CURRENCY = '001'
         AND R.START_DATE <= data_date
         AND R.END_DATE > data_date
         AND I.START_DATE <= data_date
         AND I.END_DATE > data_date
       GROUP BY T.ZONENO, T.BRNO, t.bankflag, T.SUBCODE, t.DATA_DATE;
    --------------结束------------
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_nfgnled_forpv;

  procedure proc_deal_pjtxwb_sr --处理票据贴现外币经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_PJTXWB_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --删除中间表中当日数据
    execute immediate 'truncate table stg_tmp_pjtxwb';
    v_step_no := '2';
    p_o_succeed := '0';
    commit;
    v_end_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_pjtxwb_sr;

  procedure proc_deal_wxzc_sr --处理无息资产首日经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    V_FXXS     PRM_XS053.FXXS%TYPE;
  begin
    --记载数据处理日志
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_WXZC_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    execute immediate 'truncate table stg_tmp_wxzc';
    select max(fxxs) into v_fxxs from prm_xs053
     where p_i_date >= start_date and p_i_date < end_date;
    v_step_no := '2';
    insert into stg_tmp_wxzc
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             decode(substr(a.subcode, 1, 1),
                    8,
                    a.tdcrbal * nvl(c.fxxs, v_fxxs) * b.subject_app,
                    2,
                    a.tdcrbal * nvl(c.fxxs, v_fxxs) * b.subject_app,
                    4,

                    (a.tddrbal - a.tdcrbal) * nvl(c.fxxs, v_fxxs) *
                    b.subject_app,
                    a.tddrbal * nvl(c.fxxs, v_fxxs) * b.subject_app),
             a.data_date,
             '108'
        from stg_dat_nfgnled_pv a, prm_cs05b b, prm_xs053 c
       where a.currtype in ('001', '992', '994')
         and DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) =
             b.SUBJECT_CODE
         and b.PRODUCT_ID = c.PRODUCT_ID(+)
         and a.data_date >= b.start_date
         and a.data_date <= b.end_date
         and a.data_date >= nvl(c.start_date,'19000101')
         and a.data_date <= nvl(c.end_date,'99991231');
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_wxzc_sr;

  procedure proc_deal_zjyw_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    V_FXXS     PRM_XS053.FXXS%TYPE;
  begin
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_ZJYW_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --删除中间表中当日数据
    execute immediate 'truncate table stg_tmp_zjyw';
    select max(fxxs) into v_fxxs from prm_xs053 where p_i_date >= start_date and p_i_date < end_date;
    v_step_no := '2';
    insert into stg_tmp_zjyw
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             decode(substr(a.subcode, 1, 1),
                    8,
                    a.tdcrbal * nvl(c.fxxs, v_fxxs) * b.subject_app,
                    2,
                    a.tdcrbal * nvl(c.fxxs, v_fxxs) * b.subject_app,
                    4,

                           (a.tddrbal - a.tdcrbal) * nvl(c.fxxs, v_fxxs) *
                           b.subject_app,
                    a.tddrbal * nvl(c.fxxs, v_fxxs) * b.subject_app),
             a.data_date,
             '109'
        from stg_dat_nfgnled_pv a, prm_cs05a b, prm_xs053 c
       where a.currtype in ('001', '992', '994')
         and DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) =
             b.SUBJECT_CODE
         and b.PRODUCT_ID = c.PRODUCT_ID(+)
         and a.data_date >= b.start_date
         and a.data_date <= b.end_date
         and a.data_date >= nvl(c.start_date,'19000101')
         and a.data_date <= nvl(c.end_date,'99991231');
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_zjyw_sr;

  procedure proc_deal_bwzc_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    v_xs       PRM_XS051.Xs_Value%type;
  begin
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_BWZC';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    execute immediate 'truncate table stg_tmp_bwzc';
    select max(xs_value) into v_xs from prm_xs051 where p_i_date >= start_date and p_i_date < end_date;

    insert into stg_tmp_bwzc
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             decode(substr(a.subcode, 1, 1),
                    8,
                    a.tdcrbal * nvl(c.xs_value, v_xs) * b.subject_app,
                    2,
                    a.tdcrbal * nvl(c.xs_value, v_xs) * b.subject_app,
                    4,

                           (a.tddrbal - a.tdcrbal) * nvl(c.xs_value, v_xs) *
                           b.subject_app,
                    a.tddrbal * nvl(c.xs_value, v_xs) * b.subject_app),
             a.data_date,
             '106'
        from stg_dat_nfgnled_pv a, prm_cs05c b, prm_xs051 c
       where a.currtype in ('001', '992', '994')
         AND DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) = b.SUBJECT_CODE
         AND B.PRODUCT_ID = c.PRODUCT_NAME3(+)
         AND B.DATA_TYPE = '1'
         and a.data_date >= b.start_date
         and a.data_date <= b.end_date
         and a.data_date >= nvl(c.start_date,'19000101')
         and a.data_date <= nvl(c.end_date,'99991231');
    commit;
    insert into stg_tmp_bwzc
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             decode(substr(a.subcode, 1, 1),
                    8,
                    a.tdcrbal * nvl(c.xs_value, v_xs) * b.subject_app,
                    2,
                    a.tdcrbal * nvl(c.xs_value, v_xs) * b.subject_app,
                    4,

                           (a.tddrbal - a.tdcrbal) * nvl(c.xs_value, v_xs) *
                           b.subject_app,
                    a.tddrbal * nvl(c.xs_value, v_xs) * b.subject_app),
             a.data_date,
             '107'
        from stg_dat_nfgnled_pv a, prm_cs05c b, prm_xs051 c
       where a.currtype in ('001', '992', '994')
         AND DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) = b.SUBJECT_CODE
         AND B.PRODUCT_ID = c.PRODUCT_NAME3(+)
         AND B.DATA_TYPE = '2'
         and a.data_date >= b.start_date
         and a.data_date <= b.end_date
         and a.data_date >= nvl(c.start_date,'19000101')
         and a.data_date <= nvl(c.end_date,'99991231');
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_bwzc_sr;

  procedure proc_deal_qtfx_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    v_xs       prm_xs10.quotiety%type;
  begin
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_QTFX_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    execute immediate 'truncate table stg_tmp_qtfx';
    select max(quotiety) into v_xs from prm_xs10 where p_i_date >= start_date and p_i_date < end_date;
    insert into stg_tmp_qtfx
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             decode(substr(a.subcode, 1, 1),
                    8,
                    a.tdcrbal * nvl(c.quotiety, v_xs) * b.subject_app,
                    2,
                    a.tdcrbal * nvl(c.quotiety, v_xs) * b.subject_app,
                    4,

                           (a.tddrbal - a.tdcrbal) * nvl(c.quotiety, v_xs) *
                           b.subject_app,
                    a.tddrbal * nvl(c.quotiety, v_xs) * b.subject_app),
             a.data_date,
             '200'
        from stg_dat_nfgnled_pv a, prm_cs10 b, prm_xs10 c
       where a.currtype in ('001', '992', '994')
         AND DECODE(B.SUBJECT_TYPE, 1, SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) = b.SUBJECT_CODE
         AND B.PRODUCT_ID = c.THPRODUCT(+)
         and a.data_date >= b.start_date
         and a.data_date <= b.end_date
         and a.data_date >= nvl(c.start_date,'19000101')
         and a.data_date <= nvl(c.end_date,'99991231');
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_qtfx_sr;

  procedure proc_deal_zqtz_sr(p_i_date    in varchar2, --输入日期
                              p_o_succeed out varchar2 --输出标志
                              ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
    v_xs       prm_xs061.pzxs%type;
    type date_set is table of varchar2(8) index by binary_integer;
    v_date_set     date_set;
    v_mounth_count number(2);
    v_date_bak     varchar2(8);
  begin
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_ZQTZ';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    execute immediate 'truncate table stg_tmp_zqtz';
    execute immediate 'truncate table stg_tmp_zqtz_tmp';
    v_mounth_count := to_number(substr(p_i_date, 5, 2)) - 1;
    v_date_bak     := substr(p_i_date, 1, 6) || '15';
    for v_ct in 1 .. v_mounth_count loop
      v_date_set(v_ct) := to_char(last_day(to_date(substr(v_date_bak, 1, 6) - v_mounth_count - 1 + v_ct || substr(v_date_bak, 7, 2), 'YYYYMMDD')), 'YYYYMMDD');
    end loop;
    select max(pzxs) into v_xs from prm_xs061 where p_i_date >= start_date and p_i_date < end_date;
    for v_count in 1 .. v_mounth_count loop
      insert into STG_TMP_ZQTZ_tmp
        (zoneno,
         brno,
         bankflag,
         subcode,
         currtype,
         ecost,
         data_date,
         diccode)
        select substr(a.organ, 1, 4),
               '0000',
               '4',
               a.statcode,
               '001',
               decode(sign(a.balance), -1, 0, a.balance) *
               nvl(c.pzxs, v_xs),
               v_date_set(v_count),
               '105'
          from prm_sg_bond_rmb a,
               prm_cs061 b,
               prm_xs061 c
         where a.statcode = b.subject_code
           and b.dmlx = '2'
           and a.datadate = v_date_set(v_count)
           and v_date_set(v_count) >= b.start_date
           and v_date_set(v_count) < b.end_date
           and b.product_id = c.product_id(+)
           and v_date_set(v_count) >= c.start_date
           and v_date_set(v_count) < c.end_date;
		   
      insert into STG_TMP_ZQTZ_tmp
        (zoneno,
         brno,
         bankflag,
         subcode,
         currtype,
         ecost,
         data_date,
         diccode)
        select substr(a.organ, 1, 4),
               '0000',
               '4',
               a.statcode,
               '992',
               decode(sign(a.balance), -1, 0, a.balance) *
               nvl(c.pzxs, v_xs),
               v_date_set(v_count),
               '105'
          from prm_sg_bond_d a,
               prm_cs061 b,
               prm_xs061 c
         where a.statcode = b.subject_code
           and b.dmlx = '2'
           and a.datadate = v_date_set(v_count)
           and v_date_set(v_count) >= b.start_date
           and v_date_set(v_count) < b.end_date
           and b.product_id = c.product_id(+)
           and v_date_set(v_count) >= c.start_date
           and v_date_set(v_count) < c.end_date;
    end loop;
    commit;

    insert into stg_tmp_zqtz
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             b.subcode,
             a.currtype,
             sum(a.ecost),
             a.data_date,
             a.diccode
        from STG_TMP_ZQTZ_tmp a, prm_subcode_statcode b
       where substr(a.subcode, 1, 3) || '00' = b.statcode
         and a.data_date >= b.start_date
         and a.data_date < b.end_date
       group by a.zoneno, a.brno, a.bankflag, b.subcode, a.currtype, a.data_date, a.diccode;
    --------------------本外币折人民币------------------
    insert into stg_tmp_zqtz
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             '994',
             sum(decode(a.currtype, '001', a.ecost, a.ecost / b.uscvrate)),
             a.data_date,
             a.diccode
        from stg_tmp_zqtz a, prm_exchange_rate_PV b
       where a.currtype in ('001', '992')
         and a.zoneno = b.zone_no
         and b.currency = '001'
         and a.data_date >= b.start_date
         and a.data_date < b.end_date
       group by zoneno, brno, bankflag, subcode, data_date, diccode;
    commit;
    -------------------插入总帐数据-------
    insert into stg_tmp_zqtz
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             decode(substr(a.subcode, 1, 1),
                    8,
                    a.tdcrbal * nvl(c.pzxs, v_xs) * B.SUBJECT_APP,
                    2,
                    a.tdcrbal * nvl(c.pzxs, v_xs) * B.SUBJECT_APP,
                    4,

                           (a.tddrbal - a.tdcrbal) * nvl(c.pzxs, v_xs) *
                           B.SUBJECT_APP,
                    a.tddrbal * nvl(c.pzxs, v_xs) * B.SUBJECT_APP),
             a.data_date,
             '105'
        from stg_dat_nfgnled_pv a, prm_cs061 b, PRM_XS061 C
       where a.currtype in ('001', '992', '994')
         and a.bankflag = '4'
         and DECODE(nvl(B.SUBJECT_TYPE, '2'), '1', SUBSTR(A.SUBCODE, 1, 4), A.SUBCODE) = B.SUBJECT_CODE
         and a.data_date >= b.start_date
         and a.data_date <= b.end_date
         and b.product_id = c.product_id(+)
         and b.dmlx = '1'
         and a.data_date >= nvl(c.start_date,'19000101')
         and a.data_date <= nvl(c.end_date,'99991231');
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_zqtz_sr;

  procedure proc_deal_total_sr --处理首日汇总经济资本
  (p_i_date    in varchar2, --输入日期
   p_o_succeed out varchar2 --输出标志
   ) is
    v_proc_name  L_GETDATA_CALL.PROC_NAME%TYPE;
    v_log_level  L_GETDATA_CALL.LOG_LEVEL%TYPE;
    v_begin_time L_GETDATA_CALL.BEGIN_TIME%TYPE;
    v_end_time   L_GETDATA_CALL.END_TIME%TYPE;
    v_err_code L_GETDATA_ERR.ERR_CODE%TYPE;
    v_err_txt  L_GETDATA_ERR.ERR_TXT%TYPE;
    v_step_no  L_GETDATA_ERR.STEP_NO%TYPE;
    v_sql_txt  L_GETDATA_ERR.SQL_TXT%TYPE;
  begin
    v_step_no    := '1';
    v_proc_name  := 'PROC_DEAL_TOTAL_SR';
    v_log_level  := '3'; --需要商定
    v_begin_time := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    execute immediate 'truncate table dw_mod_pvmsdata';
    -----插入票据贴现本币数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_pjtxbb a
       where a.currtype in ('001', '992', '994');
    -----插入银行卡数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_yhk a
       where a.currtype in ('001', '992', '994');
    -----插入个人贷款数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_grdk a
       where a.currtype in ('001', '992', '994');
    -----插入票据贴现外币数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_pjtxwb a ;
    -----插入无息资产数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_wxzc a;
    -----插入资金业务数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_zjyw a;
    -----插入表外资产数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_bwzc a;
    -----插入债券投资数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_zqtz a;
    -----插入其它风险数据
    insert into dw_mod_pvmsdata
      (zoneno,
       brno,
       bankflag,
       subcode,
       currtype,
       ecost,
       data_date,
       diccode)
      select a.zoneno,
             a.brno,
             a.bankflag,
             a.subcode,
             a.currtype,
             a.ecost * 10000,
             a.data_date,
             a.diccode
        from stg_tmp_qtfx a;
    -----把外币折美元改成pv需要的014
    update dw_mod_pvmsdata a
       set a.currtype = '014'
     where a.currtype = '992';
    ------把本外币折人民币改成pv需要的999
    update dw_mod_pvmsdata a
       set a.currtype = '999'
     where a.currtype = '994';
    commit;
    p_o_succeed := '0';
    v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
    --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'SUCCESS', v_step_no, v_sql_txt);
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        ROLLBACK;
        v_end_time  := TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS');
        P_O_SUCCEED := '1';
        v_err_code  := SQLCODE;
        v_err_txt   := SUBSTR(SQLERRM, 1, 4000);
        --Pack_Getdata_Log.PR_LOG_WRITE(v_proc_name, v_log_level, v_begin_time, v_end_time, 'FAILED', v_step_no, v_sql_txt, v_err_code, v_err_txt);
        RAISE;
      END;
  end proc_deal_total_sr;
end PACK_DEAL_PVMSDATA_SR;
//

DELIMITER ;//
