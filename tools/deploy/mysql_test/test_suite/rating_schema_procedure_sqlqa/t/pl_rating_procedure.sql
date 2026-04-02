delimiter //;

CREATE OR REPLACE FUNCTION "DEC_TO_HEX" (N in number) RETURN varchar2 IS
  hexval varchar2(64);
  N2     number := N;
  digit  number;
  hexdigit  char;
begin
  while ( N2 > 0 ) loop
    digit := mod(N2, 16);
    if digit > 9 then
       hexdigit := chr(ascii('A') + digit - 10);
    else
       hexdigit := to_char(digit);
    end if;
    hexval := hexdigit || hexval;
    N2 := trunc( N2 / 16 );
 end loop;
 return hexval;
end;
//




CREATE OR REPLACE FUNCTION "HEX_TO_DEC" (hexval in char) RETURN number IS
    i                 number;
    digits            number;
    result            number := 0;
    current_digit     char(1);
    current_digit_dec number;
begin
    digits := length(hexval);
    for i in 1..digits loop
        current_digit := SUBSTR(upper(hexval), i, 1);
        if current_digit in ('A','B','C','D','E','F') then
           current_digit_dec := ascii(current_digit) - ascii('A') + 10;
        else
           current_digit_dec := to_number(current_digit);
        end if;
        result := (result * 16) + current_digit_dec;
    end loop;
    return result;
end;
//





CREATE OR REPLACE PACKAGE "PKG_DYNAMICCURSOR" 
as
type d_cursor is ref cursor;
end;
//




CREATE OR REPLACE FUNCTION "AF_TEL_TO_IMSI" (as_telnum VARCHAR2)
  RETURN VARCHAR2 IS
  vs_imsi VARCHAR2(15);
  vs_head VARCHAR2(15);
BEGIN
  IF substr(as_telnum, 1, 3) NOT IN
     ('134', '135', '136', '137', '138', '139','147', '152', '158', '159', '150', '157', '151', '187', '188','182','183','184','178','170','172','198','148','165')
  THEN
    RETURN as_telnum;
  END IF;

  IF (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348') OR
     (substr(as_telnum, 1, 3) IN ('158', '159', '150', '151', '152','187','182','183','184'))
  THEN
    vs_head := '2';
  ELSIF (substr(as_telnum, 1, 3) in ('157', '147', '188','178','172','198','165') or substr(as_telnum, 1, 4) in ('1705','1703',1706))
  THEN
    vs_head := '7';
  ELSE
    vs_head := '0';
  END IF;

  IF vs_head = '2' AND (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '152')
  THEN
    vs_imsi := '4600' || vs_head || '2' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '187')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '158')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '159')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '150')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '151')
  THEN
    vs_imsi := '4600' || vs_head || '1' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '182')
  THEN
    vs_imsi := '4600' || vs_head || '6' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 4) = '1705')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 4) = '1703')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 4) = '1706')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '188')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '147')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '178')
  THEN
    vs_imsi := '4600' || vs_head || '5' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '172')
  THEN
    vs_imsi := '4600' || vs_head || '2' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '198')
  THEN
    vs_imsi := '4600' || vs_head || '1' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '165')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '183')
  THEN
    vs_imsi := '4600' || vs_head || '5' || substr(as_telnum, 4, 4);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '184')
  THEN
    vs_imsi := '4600' || vs_head || '4' || substr(as_telnum, 4, 4);
  ELSIF substr(as_telnum, 4, 1) = '0'
  THEN
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               substr(as_telnum, 3, 1);
  ELSE
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               (substr(as_telnum, 3, 1) - 5) || substr(as_telnum, 4, 1);
  END IF;

IF (substr(as_telnum, 1, 3) = '148')
  THEN
    vs_head := '3';
    vs_imsi := '4601' || vs_head || '8' || substr(as_telnum, 4, 4);
END IF;

  RETURN vs_imsi;
END af_tel_to_imsi;
//




CREATE OR REPLACE PROCEDURE "PRO_ALTER_TABLE" 
(TABLE_NAME_IN IN VARCHAR2)
IS
    TEMP_NUM NUMBER;
BEGIN
    SELECT COUNT(*) into TEMP_NUM FROM USER_TABLES WHERE TABLE_NAME = TO_CHAR(UPPER(TABLE_NAME_IN));
    IF TEMP_NUM>0 THEN
	dbms_output.put_line('1..........');
	    EXECUTE IMMEDIATE 'alter TABLE '||TABLE_NAME_IN||' modify instance_id number(18)';
    END IF;
END PRO_ALTER_TABLE;
//




CREATE OR REPLACE PROCEDURE "ADD_ACCTITEM" (a_itemcode varchar2, a_itemname varchar2) is
begin
  if length(a_itemcode)<4 then
    RETURN;
  end if;
  begin
    insert into AcctItem_def (ITEMCODE, ITEMNAME, ITEMLEVEL, PARENTITEMCODE, WRTOFF_ORDER, SERV_ID, SUBSERV_ID, IN_USE, ITEMCODEN, FINANCEITEM_CODE)
      values (a_itemcode, a_itemname, ceil(length(a_itemcode)/2), substr(a_itemcode, 1, length(a_itemcode)-2), 4, '1', null, 1, to_number(a_itemcode), null);
	  dbms_output.put_line('1..........');
	  
    insert into feegroup_accitem_def (FEEGROUPID, ITEMCODE, STARTCYCLE, ENDCYCLE, PRECEDENCE) select FEEGROUPID, a_ITEMCODE, STARTCYCLE, ENDCYCLE, PRECEDENCE from feegroup_accitem_def where itemcode='3413';
	dbms_output.put_line('2..........');
	
    insert into CUSTBILL_CELL_DEF (TEMPLATE_ID, CELL_ID, CELL_NAME, SORT_ORDER, CELL_TYPE) select 106, a_itemcode, '****'||a_itemname, max(sort_order) + 1, 0 from CUSTBILL_CELL_DEF where TEMPLATE_ID=106 and cell_id='3413';
	dbms_output.put_line('3..........');
	
    insert into BILLITEM_CELL_DEF (TEMPLATE_ID, CELL_ID, ITEMTYPE, ITEMCODE, ITEMADDCODE, ISWRTOFFED, INCL_SUBJECTID, EXCL_SUBJECTID, EXCL_ITEMCODE) select TEMPLATE_ID, a_itemcode, ITEMTYPE, a_ITEMCODE, ITEMADDCODE, ISWRTOFFED, INCL_SUBJECTID, EXCL_SUBJECTID, EXCL_ITEMCODE from BILLITEM_CELL_DEF where TEMPLATE_ID=106 and cell_id='3413';
	dbms_output.put_line('4..........');
	
    insert into code_dict (catalog, code, name, note) values ('T_PACKPRIV_FEETYPE', a_itemcode, a_itemname, null);
	dbms_output.put_line('5..........');
  end;
end add_acctitem;
//




CREATE OR REPLACE PROCEDURE "AP_BILLING_DEF_DELETERULE" (a_event_id in number, a_Unique_Id in number, a_ParentUniqueId in varchar2, a_info in out varchar2) is
    v_RecordNum number(10);
    v_AllChildUniqueId varchar(1024);
    v_ProcessLevel number(10);
  v_CurRuleId number(10);
begin

    if a_info is not null and length(a_info) > 0 then
      a_info := '';
    v_RecordNum := 0;
    for c1 in (select * from (select ruleid,rulename,nextruleid, unique_id from billing_def
                                  where event_id = a_event_id start with unique_id = a_Unique_Id
                    connect by prior ruleid=nextruleid and event_id = a_event_id
                   ) where instr( ','||a_ParentUniqueId||trim(to_char(a_Unique_Id))||','
                               , ','||to_char(unique_id)||',') <1
                 ) loop
	dbms_output.put_line('1..........');
          a_info := a_info || '[' || c1.ruleid || ']' || c1.rulename || chr(13) || chr(10);
      v_RecordNum := v_RecordNum + 1;
      if v_RecordNum > 9 then
              a_info := a_info || '......';
        return;
            end if;
    end loop;
    return;
    end if;

    select nvl(max(ruleid),-1) into v_RecordNum from billing_def where unique_id = a_Unique_Id;
    if v_RecordNum < 0  then
        a_info := 'the record (unique_id=' || a_Unique_Id || ') no ruleid';
    return;
    end if;

  select nvl(max(ruleid),-1) into v_CurRuleId from billing_def where unique_id = a_Unique_Id;

    v_AllChildUniqueId := ',';
    for c1 in (select ruleid,nextruleid,unique_id,level from billing_def where event_id = a_event_id start with unique_id =a_Unique_Id connect by prior nextruleid=ruleid) loop
        v_AllChildUniqueId := trim(to_char(c1.unique_id)) || ',';
	dbms_output.put_line('2..........');
    end loop;

    v_ProcessLevel := 1;
    for c1 in (select ruleid,nextruleid,unique_id,level from billing_def where event_id = a_event_id start with unique_id = a_Unique_Id connect by prior nextruleid=ruleid) loop
	dbms_output.put_line('3..........');
        if c1."LEVEL" <= v_ProcessLevel then
            if c1."LEVEL" = 1  then
                v_RecordNum := 0;
            else
                select count(distinct unique_id) into v_RecordNum from billing_def
                    where event_id = a_event_id and nextruleid = c1.ruleid and instr( v_AllChildUniqueId, ','||to_char(unique_id)||',') < 1;
					dbms_output.put_line('4..........');
            end if;

            if v_RecordNum > 1  then
                v_ProcessLevel := c1."LEVEL";
				dbms_output.put_line('5..........1');
            else
                delete from billing_def where unique_id = c1.unique_id;
        v_ProcessLevel := v_ProcessLevel + 1;
		dbms_output.put_line('5..........2');
            end if;
        end if;
    end loop;

  select count(*) into v_RecordNum from billing_def where event_id = a_event_id and ruleid = v_CurRuleId;
  if v_RecordNum <= 1 then
  dbms_output.put_line('6..........');
      update billing_def set nextruleid=0  where event_id = a_event_id and nextruleid = v_CurRuleId;
    end if;
  a_info := 'true';
EXCEPTION WHEN OTHERS THEN
    a_info := SQLERRM;
	dbms_output.put_line('异常了..........');
end ap_billing_def_deleterule;
//




CREATE OR REPLACE PROCEDURE "AP_BJMY" 
IS
   d_applytime    DATE    := SYSDATE;
   d_expiretime   DATE    := SYSDATE;
BEGIN

   DELETE FROM set_value
         WHERE set_id = 123;

   FOR cc IN (SELECT 123 set_id, begin_date applytime, end_date expiretime,
                        msc_id
                     || ','
                     || LOWER (lac_id)
                     || ','
                     || LOWER (cell_id)
                     || ','
                     || covered_area VALUE,
                     remark note
                FROM g_tb_bjmy
               WHERE visited_prov <> '531' AND covered_prov = '531')
   LOOP
      BEGIN
         IF cc.applytime > SYSDATE + 10000
         THEN
		 dbms_output.put_line('1..........');
            d_applytime := TO_DATE ('20370101', 'yyyymmdd');
         ELSE
            d_applytime := cc.applytime;
         END IF;

         IF cc.expiretime > SYSDATE + 10000
         THEN
		 dbms_output.put_line('2..........');
            d_expiretime := TO_DATE ('20370101', 'yyyymmdd');
         ELSE
            d_expiretime := cc.expiretime;
         END IF;

         INSERT INTO set_value
              VALUES (cc.set_id, d_applytime, d_expiretime, cc.VALUE, cc.note);
		dbms_output.put_line('3..........');
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;
   END LOOP;

  
   DELETE FROM set_value
         WHERE set_id = 124;

   FOR cc IN (SELECT 124, begin_date, end_date,
                        msc_id
                     || ','
                     || LOWER (lac_id)
                     || ','
                     || LOWER (cell_id)
                     || ','
                     || covered_area VALUE,
                     remark
                FROM g_tb_bjmy
               WHERE visited_prov <> '531'
                 AND covered_prov = '531'
                 AND up_date > SYSDATE)
   LOOP
   dbms_output.put_line('begin_date的值: ' || cc.begin_date);
      BEGIN
         IF cc.begin_date > SYSDATE + 10000
         THEN
		 dbms_output.put_line('4..........');
            d_applytime := TO_DATE ('20370101', 'yyyymmdd');
         ELSE
            d_applytime := cc.begin_date;
         END IF;

         IF cc.end_date > SYSDATE + 10000
         THEN
		 dbms_output.put_line('5..........');
            d_expiretime := TO_DATE ('20370101', 'yyyymmdd');
         ELSE
            d_expiretime := cc.end_date;
         END IF;

         INSERT INTO set_value
              VALUES (124, d_applytime, d_expiretime, cc.VALUE, cc.remark);
			  dbms_output.put_line('6..........');
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;

      END;
   END LOOP;
END;
//




  CREATE OR REPLACE PROCEDURE "AP_CB_GET_ISMG" (v_cycle varchar2) is
v_execsql varchar2(2048);
v_billingcycle varchar2(8);
v_region       varchar2(5);
v_endtime      date ;
begin
     

          v_billingcycle := v_cycle ;
      
      
      for c1 in (select subobject_name from all_objects where object_name = upper('ismgerrorcdr') and  object_type = 'TABLE PARTITION' and subobject_name like '%'||v_billingcycle||'%') loop
        
          v_execsql := '
                    insert into stl_gst_sms_'||v_billingcycle||'
                    select * 
                         from ismgerrorcdr partition ('||c1.subobject_name||')
                    where spcode in (''446729'',''446791'',''877000'',''877001'')
                   
                    ';
          execute immediate v_execsql ;
          commit;

          v_execsql := '
                    insert into stl_gst_sms_'||v_billingcycle||'
                    select * from ismgemptycdr partition ('||c1.subobject_name||')
                    where spcode in (''446729'',''446791'',''877000'',''877001'') 
                    ';
          execute immediate v_execsql ;
          commit; 
		dbms_output.put_line('1..........');
      end loop ;
     
      for c1 in (select subobject_name from all_objects where object_name = upper('mmserrorcdr') and  object_type = 'TABLE PARTITION' and subobject_name like '%'||v_billingcycle||'%') loop

          v_execsql := '
                    insert into stl_gst_mms_'||v_billingcycle||'
                    select *
                         from mmserrorcdr partition ('||c1.subobject_name||')
                    where sp_code in (''446729'',''446791'',''877000'',''877001'')

                    ';
          execute immediate v_execsql ;
          commit;

          v_execsql := '
                    insert into stl_gst_mms_'||v_billingcycle||'
                    select * from mmsemptycdr partition ('||c1.subobject_name||')
                    where sp_code in (''446729'',''446791'',''877000'',''877001'')
                    ';
          execute immediate v_execsql ;
          commit;
		dbms_output.put_line('2..........');
      end loop ;
end ;
//




  CREATE OR REPLACE PROCEDURE "AP_CB_TEST" is

  v_mapping_id NUMBER(10);

  v_date     VARCHAR2(14);
  v_filename VARCHAR2(40);
  ---modify 20080118
  v_filename1    VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

  v_conent_flag NUMBER(2); ---用于判断是否有局数据不用同步,但是内容计费需要同步局数据的情况.
  v_post_flag varchar2(32);
begin
  v_content_id  := 203;
  v_conent_flag := 0;
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     ---20090615----WHERE (sourfilename = v_filename OR sourfilename = v_filename1) --20080118 modify 
     WHERE sourfilename = 'MCBBJ_00_SP_OPER_20170124.txt' ---modify by mayong 20090615 暂不处理01文件
    --WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
            and valid_date <>  nvl(expire_date,to_date('20370101','yyyymmdd'))
            and serv_type ='090526' 
           and bill_flag ='3' 
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     /* 20090615  WHERE (sourfilename = v_lastfilename_0 OR
           sourfilename = v_lastfilename_1) --and serv_type=c1.serv_type*/
     WHERE sourfilename = 'MCBBJ_00_SP_OPER_20170123.txt'   ---modify by mayong 20090615  暂不处理01文件
           and valid_date <>  nvl(expire_date,to_date('20370101','yyyymmdd'))
           and serv_type ='090526' 
           and bill_flag ='3' 
    )
  LOOP
    dbms_output.put_line('1..........');
    v_conent_flag := 0; ---内容计费同步标志清0 
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20081021    
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通 
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20090116
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信  
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20081021   
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        if  c1.sp_code in ('699213','699216','699218','699214','699211','699217','699215','699212') then
            v_content_mapping_sour := '51,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '51,' || c1.fee / 1000;
        else
            v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '14,' || c1.fee / 1000;
        end if ;
        
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信            
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载   
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_conent_flag          := 1;
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图 
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1; ---设置标志,说明当前纪录有局数据不同步,内容计费要同步的数据
        v_content_mapping_sour := '20,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '20,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐不需要进行同步
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
        v_mapping_id   := -1;
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃                      
    /*------------------20081008-----------------------------*/
    
      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM,目前未稽核，不需要同步                      
    
      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)             

    /*    
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 61; --国际问候短语(os) ,目前未稽核，不需要同步                      
            v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
            v_mapping_dest := c1.operator_name || ',' ||
                              lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                              ',1|' || c1.out_prop || '|1';
            v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      */
      WHEN c1.serv_type = '008007' THEN
        --cuibiao 20100322 修改 只是同步局数据到 mapping_list 
        /*v_mapping_id := -1; --手机游戏,目前未稽核，不需要同步 */
        v_mapping_id   := 87; --手机地图   
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||','||
                  lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 ||
                  ',' || c1.out_prop;
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';   
        
         ---内容计费同步局数据使用 cuibiao 20100705添加
        v_conent_flag          := 1;
        v_content_mapping_sour := '28,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '28,' || c1.fee / 1000;
        
                      
    
      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通,目前未稽核，不需要同步                      
    
      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,目前未稽核，不需要同步   
       /* cuibiao  20091223注释                   
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1;  */
               
      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := -1; --Mobile Market 业务局数据只是查询用,直接入查询库    

      
        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '57,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '57,' || c1.fee / 1000;


      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 67; --农信通彩信(CIMM)     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 96; --Widget      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '56,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '56,' || c1.fee / 1000;
                
       WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 98; --Widget      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---add by cuibiao 20091217 
        v_conent_flag          := 1;
        v_content_mapping_sour := '60,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '60,' || c1.fee / 1000;    
       
       --cuibiao 20100115 stm
       WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 103;       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '53,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '53,' || c1.fee / 1000; 
       --wangqi 20111213 手机动漫
       WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 105;       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '36,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '36,' || c1.fee / 1000;        
       --cuibiao 20100621 手机导航 
       WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航暂不计费
        v_conent_flag:= 1;
        v_content_mapping_sour := '65,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '65,' || c1.fee / 1000;  
        
       --cuibiao 20101221
       WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机poc
       
       WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包
        
       WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云     
     
       WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;
        
       WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;
        
       WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;  
        
       WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;  
                
       WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;     

       WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;
        --begin add cuibiao 20170125     
        v_conent_flag:= 1;
        v_content_mapping_sour := '81,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '81,' || c1.fee / 1000;
        --end add cuibiao 20170125   
       WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1; 
        v_conent_flag          := 1;
        v_content_mapping_sour := '80,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '80,' || c1.fee / 1000; 
        /*
        v_mapping_id   := 175; --手机poc      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
        
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '39,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '39,' || c1.fee / 1000; 
        
        
        */
                      
    /*------------------20081008-----------------------------*/
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
    
    v_post_flag :='mapping_list|业务' ;
    IF v_mapping_id <> -1
    THEN
	dbms_output.put_line('AP_CB_TEST -- ?????????');
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);   
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.operator_code);
    
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        --20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;
    
    END IF;
  
   v_post_flag :='mapping_list|内容计费' ;
    ----内容计费局数据同步
    IF v_conent_flag = 1
    THEN
	dbms_output.put_line('2..........');
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
	  dbms_output.put_line('3..........');
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
		dbms_output.put_line('4..........');
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;
      
        --插入新数据  
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
		dbms_output.put_line('5..........');
        EXCEPTION
          --WHEN OTHERS THEN
          --20080118 modify
          WHEN dup_val_on_index THEN
            NULL;
        END;
      
      END IF;
    END IF;
  END LOOP;
end ap_cb_test;
//




  CREATE OR REPLACE PROCEDURE "AP_CDRCALLBACK" (A_CDRTABLE  VARCHAR2,
                                           A_TEMPTABLE VARCHAR2,
                                           A_BACKTABLE VARCHAR2,
                                           A_WHERE     VARCHAR2,
                                           A_FIELD     VARCHAR2) AS

  V_INSERTSQL VARCHAR2(10000);
  V_DELETESQL VARCHAR2(10000);
  V_TRUNCASQL VARCHAR2(10000);
BEGIN
  IF (A_WHERE IS NULL) THEN
    V_INSERTSQL := 'INSERT INTO ' || A_BACKTABLE || ' SELECT * FROM ' ||
                   A_TEMPTABLE || ' WHERE ACTION = ''1''';
    V_DELETESQL := 'DELETE FROM ' || A_CDRTABLE || ' WHERE ' || A_FIELD ||
                   ' IN (SELECT ' || A_FIELD || ' FROM ' || A_TEMPTABLE ||
                   ' WHERE ACTION=''1'')';
	dbms_output.put_line('1..........');
  ELSE
    V_INSERTSQL := 'INSERT INTO ' || A_BACKTABLE || ' SELECT * FROM ' ||
                   A_TEMPTABLE || ' WHERE ACTION = ''1'' AND ' || A_WHERE;

    V_DELETESQL := 'DELETE FROM ' || A_CDRTABLE || ' WHERE ' || A_FIELD ||
                   ' IN (SELECT ' || A_FIELD || ' FROM ' || A_TEMPTABLE ||
                   ' WHERE ACTION=''1'' AND ' || A_WHERE || ')';
	dbms_output.put_line('2..........');
  END IF;
  V_TRUNCASQL := 'TRUNCATE TABLE ' || A_TEMPTABLE;

  execute immediate V_INSERTSQL;
  execute immediate V_DELETESQL;
  execute immediate V_TRUNCASQL;
  commit;
END;
//



CREATE OR REPLACE PROCEDURE "AP_CDRQUERY_LOG" (i_operid   VARCHAR2,
                                         i_query_type  VARCHAR2,
                                         i_query_table VARCHAR2,
                                         i_event_id    VARCHAR2,
                                         i_query_monty VARCHAR2,
                                         i_telnum			 VARCHAR2,
                                         i_query_cond	 VARCHAR2
                                         ) AS

BEGIN

    INSERT INTO CDR_QUERY_LOG
        (OPERID,
         QUERY_TYPE,
         QUERY_TIME,
         QUERY_TABLE,
         EVENT_ID,
         QUERY_MONTH,
         TELNUM,
         QUERY_COND)
    VALUES
        (i_operid,
         i_query_type,
         sysdate,
         i_query_table,
         i_event_id,
         i_query_monty,
         i_telnum,
         i_query_cond);

      COMMIT;
END;
//




CREATE OR REPLACE PROCEDURE "AP_CHECK_MAPPING_LIST35" 
(
 v_mscid 		   		varchar2,
 v_areacode 			varchar2,
 v_name     			varchar2,
 v_function 			number,    		--0 only check 1 delete old insert new one  5 query
 v_time					in  date,       --time
 v_result 				out	number,		--0 exist 1 insert newone 2 old and new diff
 v_old_mscid   			out varchar2,
 v_old_areacode 		out varchar2,
 v_old_provincename		out varchar2,
 v_old_areaname			out varchar2,
 v_old_count 			out number)

as

v_dest varchar2(100);
v_manage varchar2(100);
v_area  varchar2(100);

begin

	  v_result := 0;
	  v_old_count :=1;
	  v_area := v_areacode ;
      if v_area like '0%' then
        v_area := substr(v_areacode,2);
      end if;

	  begin
	      if v_area in ('23','22','21') then
	      	v_area := '23';
		  dbms_output.put_line('1..........');
	      end if;

		  if substr(v_area,1,1) between '1' and '9' then
			  select v_area||','||provincecode into v_dest from domesticareacode b
			  		where b.areacode='0'||v_area;
			dbms_output.put_line('2..........');
		  else
		  dbms_output.put_line('3..........');
			  select substr(b.areacode,2),
			  substr(b.areacode,2)||','||provincecode into v_area,
			  v_dest from domesticareacode b
			  		where  b.areaname like v_area||'%';
		  end if;

	  exception
	  		when no_data_found then
	  			if  v_function = 5 then
	  				goto aa;
	  			end if;
	  			raise_application_error(-20101, v_areacode||'？？ó|μ？？？o？？′？òμ？'||chr(13)||chr(10)||'？ò'||v_mscid||'2？′？？ú');
	  			return ;
	  end ;
<<aa>>
	  begin
	  dbms_output.put_line('4..........');  -- domesticareacode mv_area
		  select mapping_sour,substr(mapping_dest,1,instr(mapping_dest,',') - 1) into
		  v_old_mscid ,v_old_areacode from mapping_list
		  where mapping_id=35 and mapping_sour = v_mscid
		  and nvl(expiretime,sysdate+1000)>sysdate;
	  exception
	  	when no_data_found then
	  		v_result := 1;
	  		if v_function = 1 then
	  			insert into mapping_list
	  				select v_mscid,v_dest ,v_time,null ,35 ,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
			dbms_output.put_line('10..........');
	  		end if;
	  		return ;
	  	when too_many_rows then
	   		select max(substr(mapping_dest,1,instr(mapping_dest,',') - 1))
	   		,min(substr(mapping_dest,1,instr(mapping_dest,',') - 1)),count(*)  into
		  		v_old_mscid ,v_old_areacode ,v_old_count from mapping_list
		  		where mapping_id=35 and mapping_sour = v_mscid
		  		and nvl(expiretime,sysdate+1000)>sysdate;

	  	    if v_old_mscid != v_area then
	  	    	v_old_areacode := v_old_mscid;
	  	    end if;

	  	    v_old_mscid := v_mscid;

	  		if v_function = 1 then
		  		delete from mapping_list where mapping_id=35 and mapping_sour = v_mscid;
		  		insert into mapping_list
		  		select v_mscid,v_dest ,v_time,null ,35 ,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
		  		v_result :=1;
				dbms_output.put_line('11..........');
		  		return ;
		  	else
		  		select  a.areaname,b.provincename
		  		into v_old_areaname,v_old_provincename
		  	    from domesticareacode a,province b
					where a.provincecode=b.provincecode and a.areacode='0'||v_old_areacode;
			  	v_result :=2;
				dbms_output.put_line('12..........');
			  	return ;
		    end if;
	  		return ;
	  end ;
		dbms_output.put_line('v_area:  ' || v_area);
		dbms_output.put_line('v_old_areacode:  ' || v_old_areacode);
	  if v_area != v_old_areacode then  -- 23 <> 4
	  	if v_function = 1 then
		    dbms_output.put_line('5..........');
	  		delete from mapping_list where mapping_id=35 and mapping_sour = v_mscid;
	  		insert into mapping_list
	  		select v_mscid,v_dest ,v_time,null ,35 ,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
	  		v_result :=1;
	  		return ;
	  	else
		dbms_output.put_line('6..........');
	  		select  a.areaname,b.provincename
	  		into v_old_areaname,v_old_provincename
	  	    from domesticareacode a,province b
				where a.provincecode=b.provincecode and a.areacode='0'||v_old_areacode;
		  	v_result :=2;
		  	return ;
	    end if;
	  end if;


end;
//




  CREATE OR REPLACE PROCEDURE "AP_CHECK_MAPPING_LIST5" 
(
 v_mobilecode 			in varchar2,
 v_areacode 			in varchar2,
 v_name     			in varchar2,
 v_function 			in number,    	--0 only check 1 delete old insert new one  5 query
 v_szx					in	number,     --0 check mapping_list 1 check set_value (83)
 v_time					in  date,       --time
 v_result 				out	number,		--0 exist 1 insert newone 2 old and new diff
 v_old_mobileareacode 	out varchar2,
 v_old_areacode 		out varchar2,
 v_old_provincename		out varchar2,
 v_old_areaname			out varchar2,
 v_old_count 			out number )

as

v_dest varchar2(100);
v_manage varchar2(100);
v_area  varchar2(100);

begin
	  if v_szx is null then
	      raise_application_error(-20101,'请重新下载程序');
	      null;
	  end if;
	  v_result := 0;
	  v_old_count :=1;
	  v_area := v_areacode ;
      if v_area like '0%' then
        v_area := substr(v_areacode,2);  -- '07'
		dbms_output.put_line('1..........');
      end if;

	  begin
	      if v_area in ('重庆','万县','涪陵','黔江') then
	      	v_area := '23';
	      end if;

		  if substr(v_area,1,1) between '1' and '9' then
			  select v_area||','||a.name||'.'||provincecode into v_dest from code_dict a,domesticareacode b
			  		where a.catalog='T_OTHERMANAGEHEAD' and v_mobilecode like a.code||'%'
		  			and b.areacode='0'||v_area;
			dbms_output.put_line('2..........');
		  else
			  select substr(b.areacode,2),
			  substr(b.areacode,2)||','||a.name||'.'||provincecode into v_area,  -- '8,3.3'
			  v_dest from code_dict a,domesticareacode b
			  		where a.catalog='T_OTHERMANAGEHEAD' and v_mobilecode like a.code||'%'
		  			and b.areaname like v_area||'%';
			dbms_output.put_line('3..........');
		  end if;

	  exception
	  		when no_data_found then
	  			if  v_function = 5 then
	  				goto aa;
	  			end if;
	  			raise_application_error(-20101, v_areacode||'对应的区号未找到'||chr(13)||chr(10)||'或'||v_mobilecode||'的字冠不存在');
	  			return ;
	  end ;
<<aa>>

    /*对于神州行转全球通用户，要修改set_value 16,116*/ 

    if v_szx = 1 then
        begin
            v_result := 1;

	  		    if v_function = 1 then
                update set_value
		  	        set expiretime=v_time, note=v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')
                where set_id=16 and value=v_mobilecode and nvl(expiretime,sysdate+1000)>sysdate;
                
                update set_value
		  	        set expiretime=v_time, note=v_mobilecode||','||v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')
                where set_id=116 and value=af_tel_to_imsi(v_mobilecode) 
                and nvl(expiretime,sysdate+1000)>sysdate;                
		  	    end if;
            dbms_output.put_line('4..........');
      	    exception
      	    when no_data_found then
			dbms_output.put_line('AP_CHECK_MAPPING_LIST5 - ?????????');
				        v_result := 1;
                raise_application_error(-20102, v_mobilecode||'不在神州行集合中:表set_value, set_id=16'||chr(13)||chr(10));
	  			         
        end ;
        return ;
    end if;
  /* 
      if v_szx = 1 then
      	begin
			select value,v_area into
				v_old_mobileareacode ,v_old_areacode from set_value
		  	where set_id=83
		  		and value = v_mobilecode
			  	and nvl(expiretime,sysdate+1000)>sysdate
			  	and rownum = 1 ;
      	exception
      		when no_data_found then
				v_result := 1;
	  			if v_function = 1 then
	  				insert into set_value
		  				select 83,v_time,null,v_mobilecode,v_dest||','||v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')
		  				from dual;
		  		end if;
      	end ;
      	return ;
      end if;
*/
	  begin
	  dbms_output.put_line('5..........');
		  select mapping_sour,substr(mapping_dest,1,instr(mapping_dest,',') - 1) into
		  v_old_mobileareacode ,v_old_areacode from mapping_list
		  where mapping_id=5 and mapping_sour = v_mobilecode
		  and nvl(expiretime,sysdate+1000)>sysdate;
	  exception
	  	when no_data_found then
	  		v_result := 1;
	  		if v_function = 1 then
		  			insert into mapping_list (MAPPING_SOUR,MAPPING_DEST, APPLYTIME   , EXPIRETIME  , MAPPING_ID ,note)
		  				select v_mobilecode,v_dest ,v_time,null ,5 ,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
			  		if v_name like '%-SCP-%' then
			  			delete from set_value where set_id=16 and value = v_mobilecode;
			  			insert into set_value
			  			select 16,v_time,null,v_mobilecode,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')
		  				from dual;
						dbms_output.put_line('10..........');
		  			end if;
					
	  		end if;
	  		return ;
	  	when too_many_rows then
	   		select max(substr(mapping_dest,1,instr(mapping_dest,',') - 1))
	   		,min(substr(mapping_dest,1,instr(mapping_dest,',') - 1)),count(*)  into
		  		v_old_mobileareacode ,v_old_areacode ,v_old_count from mapping_list
		  		where mapping_id=5 and mapping_sour = v_mobilecode
		  		and nvl(expiretime,sysdate+1000)>sysdate;

	  	    if v_old_mobileareacode != v_area then
	  	    	v_old_areacode := v_old_mobileareacode;
	  	    end if;

	  	    v_old_mobileareacode := v_mobilecode;

	  		if v_function = 1 then
				dbms_output.put_line('11..........');
		  		delete from mapping_list where mapping_id=5 and mapping_sour = v_mobilecode;
		  		insert into mapping_list (MAPPING_SOUR,MAPPING_DEST, APPLYTIME   , EXPIRETIME  , MAPPING_ID ,note)
		  		select v_mobilecode,v_dest ,v_time,null ,5 ,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
		  		v_result :=1;
		  		if v_name like '%-SCP-%' then
		  			delete from set_value where set_id=16 and value = v_mobilecode;
		  			insert into set_value
		  			select 16,v_time,null,v_mobilecode,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')
		  			from dual;
		  		end if;
				dbms_output.put_line('12..........');
		  		return ;
		  	else
		  		select  a.areaname,b.provincename
		  		into v_old_areaname,v_old_provincename
		  	    from domesticareacode a,province b
					where a.provincecode=b.provincecode and a.areacode='0'||v_old_areacode;
			  	v_result :=2;
			  	return ;
		    end if;
	  		return ;
	  end ;
	  if v_area != v_old_areacode then  -- ',3.3' 
	  	if v_function = 1 then
		dbms_output.put_line('6..........');
	  		delete from mapping_list where mapping_id=5 and mapping_sour = v_mobilecode;
	  		insert into mapping_list (MAPPING_SOUR,MAPPING_DEST, APPLYTIME   , EXPIRETIME  , MAPPING_ID ,note)
	  		select v_mobilecode,v_dest ,v_time,null ,5 ,v_name||','||to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual;
	  		v_result :=1;
	  		return ;
	  	else
		dbms_output.put_line('7..........');
	  		select  a.areaname,b.provincename
	  		into v_old_areaname,v_old_provincename
	  	    from domesticareacode a,province b
				where a.provincecode=b.provincecode and a.areacode='0'||v_old_areacode;
		  	v_result :=2;
		  	return ;
	    end if;
	  end if;


end;
//




  CREATE OR REPLACE PROCEDURE "AP_CHECK_SET_VALUE180" 
is
begin
    --删除集合中多余的
    for c1 in (
    select value, applytime, nvl(expiretime,to_date('20370101','yyyymmdd'))   expiretime
        from set_value where set_id=180  minus
    select module_id||','||flow_id, apply_time, nvl(expire_time,to_date('20370101','yyyymmdd')) expire_time
    from event_flow_def where flow_type=2 and store_type<>'M')
    --and sysdate>=apply_time and (expire_time is null or sysdate<expire_time) )
    loop
        --dbms_output.put_line('delete:'||c1.value||';'||to_char(c1.applytime,'yyyymmddhh24miss')||';'||to_char(c1.expiretime,'yyyymmddhh24miss'));
		dbms_output.put_line('1..........');
        delete set_value where SET_ID=180 and VALUE=c1.value
        and applytime=c1.applytime and nvl(expiretime,to_date('20370101','yyyymmdd'))=c1.expiretime;
    end loop;
    --插入集合中不存在的
    for c1 in (
    select module_id||','||flow_id copyflow, apply_time, expire_time
    from event_flow_def where flow_type=2 and store_type<>'M'
    --and sysdate>=apply_time and (expire_time is null or sysdate<expire_time)
    minus select value, applytime, expiretime from set_value where set_id=180)
    loop
        --dbms_output.put_line('add:'||c1.copyflow||';'||to_char(c1.apply_time,'yyyymmddhh24miss')||';'||to_char(c1.expire_time,'yyyymmddhh24miss'));
        insert into set_value (SET_ID,  APPLYTIME,  EXPIRETIME, VALUE, NOTE)
        values(180, c1.apply_time, c1.expire_time,
            c1.copyflow,'insert '||to_char(sysdate,'yyyymmddhh24miss'));
		dbms_output.put_line('2..........');
    end loop;
    commit;
end;
//




  CREATE OR REPLACE PROCEDURE "AP_CONVERT_IMSI" 
is
begin
	--raise_application_error(-20101,'only test for fun!');
	insert into mapping_list
	select mapping_sour,mapping_dest,trunc(sysdate) - 100 ,null,2,'changed at '||to_char(sysdate)
	from (
		select af_tel_to_imsi(mapping_sour) mapping_sour,mapping_dest
			from mapping_list where mapping_dest like '___,1.311' and mapping_id=5
		minus
		select mapping_sour,mapping_dest
			from mapping_list where mapping_dest like '___,1.311' and mapping_id=2
	);
end ap_convert_imsi;
//



  CREATE OR REPLACE PROCEDURE "AP_DO_12590" (
   v_spcode      IN   VARCHAR2,                          /*12590对应的SP代码*/
   v_spname      IN   VARCHAR2,                               /*SP对应的SP名*/
   v_tel         IN   VARCHAR2,                                 /*语音接入号*/
   v_feerate     IN   VARCHAR2,                                /*费用代码0.1*/
   v_provcode    IN   VARCHAR2,                                 /*接入省代码*/
   v_begindate        DATE                                        /*生效时间*/
)
IS
BEGIN
   IF (SUBSTR (v_tel, 1, 5) != '12590')
   THEN
      RETURN;
   END IF;
   --50 音信互动接入号映射接入省
   INSERT INTO mapping_list
               (mapping_id, mapping_sour, mapping_dest, applytime,
                expiretime, note
               )
        VALUES (50, v_tel, v_provcode, v_begindate,
                '20370101000000', v_spname
               );

   --52 移动沙龙-语音杂志接入号映射SP代码
   INSERT INTO mapping_list
               (mapping_id, mapping_sour, mapping_dest, applytime,
                expiretime, note
               )
        VALUES (52, v_tel, v_spcode, v_begindate,
                '20370101000000', v_spname
               );

   --set 42Q文件漫游来访上传合法号码集合
   INSERT INTO set_value
               (set_id, VALUE, applytime, expiretime, note
               )
        VALUES (42, v_tel, v_begindate, '20370101000000', v_spname
               );

   --资费模式800
   INSERT INTO tariff_item
        VALUES (800, v_begindate, '20370101000000', v_tel, 1, 171,
                v_feerate / 0.1, ' ', ' ', 5, NULL, ' ', 0, NULL,null,0,null,null,null,null,0,1);

   COMMIT;
   DBMS_OUTPUT.put_line (   '接入号:'
                         || v_tel
                         || 'SP代码,名称'
                         || v_spcode
                         || v_spname
                         || '信息费用(元)='
                         || v_feerate
                         || '生效时间:'
                         || v_begindate
                        );
END;
//





  CREATE OR REPLACE PROCEDURE "AP_EXPR_MAPPING_LIST" is
     s varchar2(256);
     --v_grant varchar2(256);
     v_rule_id_max number(5);
     v_seqno number(6);
begin
     v_seqno := 0;
     --v_grant := 'grant create table to V3R5BDSP';
     --execute immediate v_grant;
     s := 'create table expr_mapping_list_'||to_char(sysdate,'yyyymmddhh24mi')||' as (select * from expr_mapping_list)';
     execute immediate s;

      select max(rule_id) into v_rule_id_max from expr_mapping_list;
     for c1 in 0..v_rule_id_max
     loop
         v_seqno := 0;
         for c2 in (select t.*,t.rowid from expr_mapping_list t where t.rule_id = c1 order by t.seqno)
         loop
             update expr_mapping_list a set a.seqno = v_seqno where a.rowid = c2.rowid;
             v_seqno := v_seqno + 1;
			 dbms_output.put_line('1..........');
         end loop;
     end loop;
commit;
end ap_expr_mapping_list;
//




  CREATE OR REPLACE PROCEDURE "AP_FIND_EXPR" (a_str in varchar2)
is
    type dyncur is ref cursor;
    v_dyncur    dyncur;
    v_count     number(6);
    v_sql       varchar2(1024);
begin
    if nvl(length(a_str),0) <= 2 then
        dbms_output.put_line('So short ['||a_str||']. To find it? Are you crazy?');
        return;
    end if;

    for cur in (
                 select table_name, column_name from user_tab_columns a
                 where exists (
                     select 1 from user_tab_columns b
                     where b.table_name=a.table_name and b.column_name='G_'||a.column_name)
               )
    loop
	
        v_sql := 'select count(*) from '||cur.table_name||' where '||cur.column_name||' like ''%'||a_str||'%''';
        open v_dyncur for v_sql;
        fetch v_dyncur into v_count;
        close v_dyncur;
		
        if v_count>0 then
            dbms_output.put_line('select * from '||cur.table_name||' where '||cur.column_name||' like ''%'||a_str||'%''');
			dbms_output.put_line('2..........');
        end if;
    end loop;
end ap_find_expr;
//




  CREATE OR REPLACE PROCEDURE "AP_FIX_BILLING_DEF" 
as
begin
for cc in (select * from int_feetype where sourcetable='usercost') loop
	update billing_def
	set itemcode = decode(cc.feetypeid,
	'SMS','SMSFEE',
	'WLAN','WLANFEE',
	'INFOFEE_FEE3','FEE3INFO',cc.feetypeid)
	where itemcode= cc.sourcecolumn ;
	update feegroup_accitem_def
	set  itemcode = decode(cc.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',cc.feetypeid)
	where itemcode= cc.sourcecolumn ;
	update billing_def
	set itemcode='RURALFEE' WHERE ITEMCODE='cost1';
	update billing_def
	set itemcode='FNSRENT' WHERE ITEMCODE='cost10';
	update billing_def
	set itemcode=upper(itemcode) where ITEMCODE IN (
	'dddaddfee'     ,
	'month_discount' ,
	'otherfee'        ,
	'roamlongaddfee'
	);

	update feegroup_accitem_def
	set itemcode='RURALFEE' WHERE ITEMCODE='cost1';
	update feegroup_accitem_def
	set itemcode='FNSRENT' WHERE ITEMCODE='cost10';
	update feegroup_accitem_def
	set itemcode=upper(itemcode) where ITEMCODE IN (
	'dddaddfee'     ,
	'month_discount' ,
	'otherfee'        ,
	'roamlongaddfee'
	);

	dbms_output.put_line('1.........');
end loop;
end;
//




CREATE OR REPLACE PROCEDURE "AP_FIX_EXPR_MAPPING_LIST" 
as
v1 varchar2(1024);
v2 varchar2(1024);
begin
	--process chrreplace
	for cc in (select a.rowid,a.* from expr_mapping_list  a  where a.expr like 'chrreplace%') loop
		v1 := cc.expr;
		v2 := cc.g_expr;
		if v1 like 'chrreplace%,`)' then
			v1 := replace(v1,'`)','''`'')');
			v2 := replace(cc.g_expr,'`)','''`'')');
		end if;
		if v1 like 'chrreplace%U)'  then
			v1 := replace(v1,'chrreplace','upper');
			v1 := replace(v1,','''',U)',')');
			v2 := replace(cc.g_expr,'chrreplace','upper');
			v2 := replace(v2,','''',U)',')');
		end if;
		if  v1 like 'chrreplace%L)' then
			v1 := replace(v1,'chrreplace','lower');
			v1 := replace(v1,','''',L)',')');
			v2 := replace(cc.g_expr,'chrreplace','lower');
			v2 := replace(v2,','''',L)',')');
		end if;
		dbms_output.put_line(v1||'---'||v2);
		if v1 != cc.expr then
			update expr_mapping_list set expr = v1 where rowid = cc.rowid;
			dbms_output.put_line('1........');
		end if;
		if v2 != cc.g_expr then
			update expr_mapping_list set g_expr = v2 where rowid = cc.rowid;
			dbms_output.put_line('2........');
		end if;
	end loop;
	for cc in (select a.rowid,a.* from expr_mapping_list a where rule_id=370) loop
		v1 := cc.expr;
		v2 := cc.g_expr;
		v1 := replace(v1,'+ 5 )',')');
		v2 := replace(v2,'+ 5 )',')');
		if v1 != cc.expr then
			update expr_mapping_list set expr = v1 where rowid = cc.rowid;
			dbms_output.put_line('3........');
		end if;
		if v2 != cc.g_expr then
			update expr_mapping_list set g_expr = v2 where rowid = cc.rowid;
			dbms_output.put_line('4........');
		end if;
	end loop;
	for cc in (select a.rowid,a.* from process_flow	a where a.RULE_COND	 like '%chrreplace%' ) loop
		v1 := cc.rule_cond;
		v2 := cc.g_rule_cond;
		if v1 like '%chrreplace%,`)%' then
			v1 := replace(v1,'`)','''`'')');
			v2 := replace(v2,'`)','''`'')');
		end if;
		if v1 like '%chrreplace%U)%'  then
			v1 := replace(v1,'chrreplace','upper');
			v1 := replace(v1,','''',U)',')');
			v2 := replace(v2,'chrreplace','upper');
			v2 := replace(v2,','''',U)',')');
		end if;
		if  v1 like '%chrreplace%L)%' then
			v1 := replace(v1,'chrreplace','lower');
			v1 := replace(v1,','''',L)',')');
			v2 := replace(v2,'chrreplace','lower');
			v2 := replace(v2,','''',L)',')');
		end if;
		if v1 != cc.rule_cond then
			update process_flow set RULE_COND = v1 where rowid = cc.rowid;
			dbms_output.put_line('5........');
		end if;
		if v2 != cc.g_rule_cond then
			update process_flow set g_rule_cond = v2 where rowid = cc.rowid;
			dbms_output.put_line('6........');
		end if;

	end loop;

	for cc in (select a.rowid,a.* from event_flow_def	 a where flow_COND	 like '%chrreplace%' ) loop
		v1 := cc.flow_cond;
		v2 := cc.g_flow_cond;
		if v1 like '%chrreplace%,`)%' then
			v1 := replace(v1,'`)','''`'')');
			v2 := replace(v2,'`)','''`'')');
		end if;
		if v1 like '%chrreplace%U)%'  then
			v1 := replace(v1,'chrreplace','upper');
			v1 := replace(v1,','''',U)',')');
			v2 := replace(v2,'chrreplace','upper');
			v2 := replace(v2,','''',U)',')');
		end if;
		if  v1 like '%chrreplace%L)%' then
			v1 := replace(v1,'chrreplace','lower');
			v1 := replace(v1,','''',L)',')');
			v2 := replace(v2,'chrreplace','lower');
			v2 := replace(v2,','''',L)',')');
		end if;
		if v1 != cc.flow_cond then
			update event_flow_def set flow_cond = v1 where rowid = cc.rowid;
			dbms_output.put_line('7........');
		end if;
		if v2 != cc.g_flow_cond then
			update event_flow_def set g_flow_cond = v2 where rowid = cc.rowid;
			dbms_output.put_line('8........');
		end if;

	end loop;

	update  process_flow set expiretime=expiretime+20*365 where expiretime is not null and expiretime <sysdate;

	delete from event_flow_def where module_id=38;
	commit ;
end ;
//




  CREATE OR REPLACE PROCEDURE "AP_FIX_PATH" 
as
begin
	--datafile
	update datafile
	set backuppath =  decode(instr(backuppath,'roamingfile'),0,
		'~/'||substr(backuppath,instr(backuppath,'cdrfile')),
		'~/'||substr(backuppath,instr(backuppath,'roamingfile'))
		),
		pathname =  decode(instr(pathname,'roamingfile'),0,
		'~/'||substr(pathname,instr(pathname,'cdrfile')),
		'~/'||substr(pathname,instr(pathname,'roamingfile'))
		),
		errorcdrpath = decode(instr(errorcdrpath,'roamingfile'),0,
		'~/'||substr(errorcdrpath,instr(errorcdrpath,'cdrfile')),
		'~/'||substr(errorcdrpath,instr(errorcdrpath,'roamingfile'))
		)
	where pathname not like '~%';
	--filedescribe
	update filedescribe
	set sourpath=decode(instr(sourpath,'roamingfile'),0,
		'~/'||substr(sourpath,instr(sourpath,'cdrfile')),
		'~/'||substr(sourpath,instr(sourpath,'roamingfile'))
		),
		backpath=decode(instr(backpath,'roamingfile'),0,
		'~/'||substr(backpath,instr(backpath,'cdrfile')),
		'~/'||substr(backpath,instr(backpath,'roamingfile'))
		),
		destpath=decode(instr(destpath,'roamingfile'),0,
		'~/'||substr(destpath,instr(destpath,'cdrfile')),
		'~/'||substr(destpath,instr(destpath,'roamingfile'))
		),
		errorpath=decode(instr(errorpath,'roamingfile'),0,
		'~/'||substr(errorpath,instr(errorpath,'cdrfile')),
		'~/'||substr(errorpath,instr(errorpath,'roamingfile'))
		)
	where sourpath not like '~%';
	dbms_output.put_line('1..........');
end ;
//



  CREATE OR REPLACE PROCEDURE "AP_GEN_REPORTVIEW" 
(ai_viewid number)

is

li_status number(2);
li_cnt number(8);
li_storageid number(8);

Begin

    select report_storage_id,status into li_storageid,li_status from report_view where report_view_id=ai_viewid;

    update report_view set status=1 where report_view_id=ai_viewid;

    if li_status=0 then
    	select count(*) into li_cnt from report_element where report_view_id=ai_viewid;
    	if li_cnt=0 then
		    dbms_output.put_line('1..........');
    		INSERT INTO REPORT_ELEMENT
		         ( REPORT_VIEW_ID,
		           ELEMENT_TYPE,
		           SEQUENCENO,
		           STATISTICS_TYPE,
		           STATUS,
		           FIELD_ID )
		     SELECT ai_viewid,
		            decode(ISUNIQUE,0,2,1),
		            FIELD_ID,
		            decode(ISUNIQUE,0,2,0),
		            0,
		            FIELD_ID
		       FROM REPORT_FIELD_DEF
		        WHERE report_storage_id=li_storageid and status=1;

    	end if;
    else
		update report_element set status=1 where report_view_id=ai_viewid and status=0;
		update report_stat_def set status=1 where status=0;
		dbms_output.put_line('2..........');
	end if;

	commit;
end;
//





CREATE OR REPLACE PROCEDURE "AP_GET_BAL_CHECK_SQL" (v_sql_text out long) is

  sql_head     varchar2(4000);--long; --varchar2(4096);
  sql_diff     varchar2(128);
  v_nrbflg     varchar(4); --non rollback flag
  v_format     varchar(32);
  v_squotes    varchar(4); --single quotes
  v_total_flag varchar2(4); --tatol quotes flag
  v_rbflg_r varchar2(4);
  ----Add by gKF12550 2009-4-30
  --add by gkf12550 2009-12-24 优化查询，在stat_cdr_bal_sourcefile 表中添加class_code 字段

begin
  v_format     := chr(39) || 'yyyy-mm-dd hh24:mi:ss' || chr(39);
  v_nrbflg   := '#';
  v_rbflg_r     := 'R';

  v_squotes    := chr(39);
  sql_diff     := ',(sum(d.total)-sum(d.no_rb_count)) Difference_CDR';
  sql_head     := 'select min(d.first_flow_time) first_flow_time,d.source_hash,d.source_name,sum(total) Total_CDR';
  v_nrbflg     := chr(39) || v_nrbflg || chr(39);
  v_rbflg_r    := chr(39) || v_rbflg_r || chr(39);
  v_total_flag := chr(39) || chr(61) || chr(39);

  for c1 in (select class_code, class_name
               from store_class_def
              WHERE class_code != 'Total_CDR' and need_check <> 'N'
              ORDER BY NVL(ORDER_INDEX, 9998)) loop
    sql_head := sql_head ||',sum(decode(d.class_code,' ||
                  v_squotes || c1.class_code || v_squotes ||
                  ',d.event_count,0)' || ')"' || c1.class_name || '"';
  end loop;

  v_sql_text := sql_head || sql_diff ||
                ' from (select a.source_hash,a.source_name,a.store_type,a.store_name,to_char(a.first_flow_time,' ||
                v_format || ') first_flow_time,' ||
                --tatol cdr
                'decode(a.store_type,' || v_total_flag ||
                ',decode(a.rollback_flag,' || v_nrbflg ||
                ',a.event_count,0),0) total,' ||

                'decode(a.rollback_flag,' || v_rbflg_r ||
                ',0,decode(a.store_type,' || v_total_flag ||
                ',0,a.event_count)) no_rb_count' ||
               --auto create fields
               --sql_decode ||
                ',a.class_code,a.event_count '||
                --fitlter
                ' from stat_cdr_bal_sourcefile a where a.first_flow_time <to_date(:v_end,' ||
                v_format || ')' ||
                ' and a.first_flow_time>= to_date(:v_from,' || v_format || ')
               and NVL((select c.store_class from store_class_map c where a.store_type = c.store_type and a.store_name like c.store_name_prefix||' ||
                v_squotes || '%' || v_squotes || '),' ||v_squotes||'Unknow_CDR'||v_squotes||')'||
                ' not in (select class_code from store_class_def where need_check =' ||
                v_squotes || 'N' || v_squotes || ')) d'

                || ' group by d.source_hash,d.source_name order by 1';
end ap_get_bal_check_sql;
//





  CREATE OR REPLACE PROCEDURE "AP_GET_NOSERVREASON" (a_tablename  in   string,
                                                a_rowid      in   string,
                                                a_errorcode  out  string,
                                                a_reason     out  string,
                                                a_solvemeth  out  string,
                                                a_classify   out  string,
                                                a_retcode    out  string,
                                                a_err_mes    out  string)
IS
BEGIN
declare
  v_EventID        number(4);
  v_FieldName      Varchar2(20);
  v_TelNum         Varchar2(25);
  v_IMSI           Varchar2(15);
  v_SubscriberID   number(15);
  v_AccountID      number(13);
  v_ErrorCode      Varchar2(10);
  v_TotalFree      Varchar2(120);
  v_StartTime      Varchar2(14);
  v_tempnum        number(14);
  v_tempnum2        number(14);
  v_SQL            Varchar2(1024);
  v_subtabname     Varchar2(128);
  v_acctabname     Varchar2(128);
  v_cursor   PKG_DYNAMICCURSOR.d_cursor;
  v_cursor1   PKG_DYNAMICCURSOR.d_cursor;
  v_classifydetail  Varchar2(128);
  begin
  --检验参数合法性--
  a_retcode := '';
  a_err_mes := '';
  IF a_tablename IS NULL then
    a_err_mes := 'table name is NULL';
    RETURN;
  END IF;
  --取客户资料表名--
  begin--取用户资料表名 v_subtabname 是 substu 表
  select attr_tablename into v_subtabname from cust_entity_def where entity_id=1;
  exception
    when no_data_found then
         a_err_mes :='can not get subscriber attr table name from cust_entity_def';
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  begin--取帐户资料表名  v_acctabname 是 accstu 表
  select attr_tablename into v_acctabname from cust_entity_def where entity_id=3;
  exception
    when no_data_found then
         a_err_mes :='can not get account attr table name from cust_entity_def';
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  --根据无主表名获取表中存储的计费事件编号--
  begin
    select event_id into v_EventID from event_store_def  where table_name = a_tablename;
  exception
    when no_data_found then
         a_err_mes :='can not get event_id event_id from event_store_def: table_name = '||a_tablename;
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  dbms_output.put_line('1..........');
  
  v_SQL := 'select count(*) from '||a_tablename||' where rowid = '''||a_rowid||'''';
  begin
    execute immediate v_SQL into v_tempnum;
    exception
    when no_data_found then
         a_err_mes :='can not get event data from'||a_tablename;
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  if v_tempnum = 0 then
     a_err_mes :='can not get event data from'||a_tablename;
     return;
  end if;

  --查询event_attr_def表，根据class定义到无主表中读取错误编码--
  begin
  dbms_output.put_line('2..........');
    v_FieldName :='';
    select field_name into v_FieldName from EVENT_ATTR_DEF where event_id= v_EventID and attr_class=1;--取错误编码
    exception
    when no_data_found then
         a_err_mes :='can not get error_code column name';--获取不到错误编码字段，返回
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  if v_FieldName is not NULL then
  v_SQL := 'select '||v_FieldName||' from '||a_tablename||' where rowid = '''||a_rowid||'''';
  begin
  dbms_output.put_line('3..........');
    execute immediate v_SQL into v_ErrorCode;
    exception
    when no_data_found then
         a_err_mes :='can not get error_code';
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  end if;
  if v_ErrorCode is NULL then
  a_err_mes :='error_code is NULL';
  return;
  end if;

  ------根据errorcode到errorcode_map表中映射出错误信息
  begin
  dbms_output.put_line('4..........');
      select DESCRIPTION,REASON,SOLVEMETHOD into a_errorcode,a_reason,a_solvemeth from errorcode_map where errorcode=v_ErrorCode;  -- v_ErrorCode=1
  exception
    when no_data_found then
         a_errorcode :=v_ErrorCode;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  if a_errorcode != v_ErrorCode then
     a_errorcode := '['||v_ErrorCode||']'||a_errorcode;
  end if;


  ----------客户资料匹配出错分析逻辑----------
    if v_ErrorCode = 'T220' or v_ErrorCode = 'T320'then
      begin
	  dbms_output.put_line('5..........');
        v_FieldName := NULL;
        select field_name into v_FieldName from EVENT_ATTR_DEF where event_id= v_EventID and attr_class=8;--取电话号码
      exception
        when no_data_found then
         a_err_mes :='can not get telnum column name';
        when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
      end;
      if v_FieldName is not NULL then
	  dbms_output.put_line('6..........');
      v_SQL := 'select '||v_FieldName||' from '||a_tablename||' where rowid = '''||a_rowid||'''';
      begin
        execute immediate v_SQL into v_TelNum;  -- v_TelNum = 110
        exception
        when no_data_found then
             a_err_mes :='can not get tel_num';
             return;
        when others then
             a_retcode := sqlcode;
             a_err_mes := sqlerrm;
             return;
      end;
      end if;
      begin
        v_FieldName := NULL;
        select field_name into v_FieldName from EVENT_ATTR_DEF where event_id= v_EventID and attr_class=7;--取IMSI码   v_FieldName = IMSI
        exception
        when no_data_found then
         a_err_mes :='can not get IMSI column name';
        when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
      end;
      if v_FieldName is not NULL then
	  dbms_output.put_line('7..........');
      v_SQL := 'select '||v_FieldName||' from '||a_tablename||' where rowid = '''||a_rowid||'''';
      begin
        execute immediate v_SQL into v_IMSI;  -- v_IMSI = IMSI码
        exception
        when no_data_found then
             a_err_mes :='can not get IMSI';
             return;
        when others then
             a_retcode := sqlcode;
             a_err_mes := sqlerrm;
             return;
      end;
      end if;
      begin
        select field_name into v_FieldName from EVENT_ATTR_DEF where event_id= v_EventID and attr_class=3;--取服务开始时间
        exception
        when no_data_found then
         a_err_mes :='can not get starttime column name';--取不到服务开始时间字段名，返回
         return;
        when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
      end;
      if v_FieldName is not NULL then
	  dbms_output.put_line('8..........');
      v_SQL := 'select to_char('||v_FieldName||',''yyyymmddhh24miss'||''') from '||a_tablename||' where rowid = '''||a_rowid||'''';
      begin
        execute immediate v_SQL into v_StartTime;  -- v_StartTime = 20201001000000
        exception
        when no_data_found then
             a_err_mes :='can not get start_time';
             return;
        when others then
             a_retcode := sqlcode;
             a_err_mes := sqlerrm;
             return;
      end;
      end if;
      if v_StartTime is NULL then
      a_err_mes :='start_time is empty';
      return;
      end if;

      if v_TelNum is NULL and v_IMSI is NULL then
        a_solvemeth := '';
        a_classify := '话单中没有电话号码也没有IMSI号';--话单中无电话号码和IMSI码
        v_classifydetail := '0';--话单中无电话号码和IMSI码
        return;
      end if;

     --根据电话号码查询用户号,按服务开始时间匹配
        if v_TelNum is not NULL then
	    dbms_output.put_line('9..........');
          --查询用户号个数
          v_SQL := 'select count(*) from '||v_subtabname||' where attr_id = 2 and value = '||v_TelNum||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
          begin
          v_tempnum := 0;
          execute immediate v_SQL into v_tempnum;  -- v_tempnum = 1
          exception
          when others then
               a_retcode := sqlcode;
               a_err_mes := sqlerrm;
               return;
          end;
          if v_tempnum = 0 then
               a_classify := '电话号码无法匹配用户';--根据电话号码查询不到用户号
               v_classifydetail := '10';--有电话号码，但查询不到用户号
               return;
          else
            if v_tempnum = 1 then --电话号码查询到一个用户号
			   dbms_output.put_line('10..........');
               --查询用户号
               v_SQL := 'select key_id from '||v_subtabname||' where attr_id = 2 and value = '||v_TelNum||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
               begin
               execute immediate v_SQL into v_SubscriberID;  -- v_SubscriberID = 1000
               exception
               when no_data_found then
                  a_classify := '电话号码无法匹配用户';--根据电话号码查询不到用户号
                  v_classifydetail := '10';--有电话号码，但查询不到用户号
                  return;
               when others then
                  a_retcode := sqlcode;
                  a_err_mes := sqlerrm;
                  return;
                end;
            ----------------根据用户号查询账号信息-------------------------

                v_tempnum := 0;
                --查询账号个数
                v_SQL := 'select count(*) from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                begin
                execute immediate v_SQL into v_tempnum;  -- v_tempnum = 1
			    dbms_output.put_line('11..........');
                exception
                when others then
                     a_retcode := sqlcode;
                     a_err_mes := sqlerrm;
                     return;
                end;
              if  v_tempnum = 0 then
                   a_classify := '用户无帐户信息';--用户无帐户信息
                   v_classifydetail := '110';--有电话号码，查询到一个用户号，该用户号查询不到账号
                   return;
              else 
			    if v_tempnum = 1 then--查询到一个账号
                   --查询账号
                   v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   begin
                   --select value into v_AccountID from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                   execute immediate v_SQL into v_AccountID;  -- v_AccountID = 160
				   dbms_output.put_line('12..........');
                   exception
                   when no_data_found then
                      a_classify := '用户无帐户信息';--用户无帐户信息
                      v_classifydetail := '110';--有电话号码，查询到一个用户号，该用户号查询不到账号
                      return;
                   when others then
                      a_retcode := sqlcode;
                      a_err_mes := sqlerrm;
                      return;
                   end;
                   begin
                   v_tempnum2 := 0;
                   --查询该账号在帐户属性表中有没有数据  v_acctabname 是 accstu 表
                   v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                   execute immediate v_SQL into v_tempnum2;  -- v_tempnum2 = 1
				   dbms_output.put_line('13..........');
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                   end;
                   if v_tempnum2 = 0 then
                        a_classify := '用户的帐户资料无效';--一个用户号，查询到的一个账号，帐户资料无效
                        v_classifydetail := '1110';--有电话号码，查询到一个用户号，该用户号查询到一个账号，但该账号无效
                        return;
                   else
                        v_classifydetail := '1111';--有电话号码，查询到一个用户号，该用户号查询到一个账号，该账号有效
                   end if;
                else--查询到多个账号,检测每个账号的状态
                   v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --declare CURSOR v_cursor1 is select value from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                   begin
                   open v_cursor1 for v_SQL;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                   end;
                   loop--循环读取账号
                   fetch v_cursor1 into v_AccountID;
                   exit when v_cursor1%notfound ;
                   -----------------------------------------
                   begin
                   v_tempnum2 := 0;
                   --根据账号到帐户属性表中搜索数据
                   v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                   execute immediate v_SQL into v_tempnum2;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;

                   end;
                   if v_tempnum2 = 0 then
                        a_classify := '双资料场景（根据电话号码，查询到一个用户号，该用户号查询到多个账号，但有账号无效）';--有一个账号无效，认为是双资料问题
                        v_classifydetail := '1120';--有电话号码，查询到一个用户号，该用户号查询到多个账号，但有账号无效
                        return;
                   end if;
                   -----------------------------------------
                   end loop;
                   close v_cursor1;
                   v_classifydetail := '1121';--有电话号码，查询到一个用户号，该用户号查询到多个账号，每个账号均有效
                end if; --  284 - 362
              end if;

            else--电话号码查询到多个用户号
                   v_SQL := 'select key_id from '||v_subtabname||' where attr_id = 2 and value = '||v_TelNum||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --declare cursor v_cursor is  select key_id from subscriber_attr where attr_id = 2 and value = v_TelNum and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                   begin
                   open v_cursor for v_SQL;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                   end;
                   loop--循环每个用户号
                   fetch v_cursor into v_SubscriberID;
                   exit when v_cursor%notfound ;
                   ----------------根据用户号查询账号信息-------------------------
                    v_tempnum := 0;
                    begin
                    --查询该用户对应的帐号数量
                    v_SQL := 'select count(*) from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                    --select count(*) into v_tempnum from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                    execute immediate v_SQL into v_tempnum;
                    exception
                    when others then
                         a_retcode := sqlcode;
                         a_err_mes := sqlerrm;
                         return;
                    end;
                    if  v_tempnum = 0 then
                         a_classify := '双资料场景（根据电话号码，查询到多个用户号，有一个用户号查询不到账号）';--有一个用户无帐户信息，认为是双资料
                         v_classifydetail := '120';--有电话号码，查询到多个用户号，有一个用户号查询不到账号
                         return;
                    else if v_tempnum = 1 then--查询到一个账号
                         begin
                         --查询账号
                         v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --select value into v_AccountID from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                         execute immediate v_SQL into v_AccountID;
                         exception
                         when no_data_found then
                            a_classify := '双资料场景（根据电话号码，查询到多个用户号，有一个用户号查询不到账号）';--有一个用户无帐户信息，认为是双资料
                            v_classifydetail := '120';--有电话号码，查询到多个用户号，有一个用户号查询不到账号
                            return;
                         when others then
                            a_retcode := sqlcode;
                            a_err_mes := sqlerrm;
                            return;
                         end;
                         begin
                         v_tempnum2 := 0;
                         --查询账号在帐户属性表中的数据
                         v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                         execute immediate v_SQL into v_tempnum2;
                         exception
                         when no_data_found then
                              a_classify := '双资料场景（根据电话号码，查询到多个用户号，有一个用户号查询到一个无效账号）';--其中一个用户号的账号资料无效
                              v_classifydetail := '1210';--有电话号码，查询到多个用户号，有一个用户号查询到一个账号，但该账号无效
                              return;
                         when others then
                              a_retcode := sqlcode;
                              a_err_mes := sqlerrm;
                              return;
                         end;
                         if v_tempnum2 = 0 then
                              a_classify := '双资料场景（根据电话号码，查询到多个用户号，有一个用户号查询到一个无效账号）';--其中一个用户号的账号资料无效
                              v_classifydetail := '1210';--有电话号码，查询到多个用户号，有一个用户号查询到一个账号，但该账号无效
                              return;
                         end if;
                         else--查询到多个账号,检测每个账号的状态
                         v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --declare cursor v_cursor1 is  select value from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                         begin
                         open v_cursor1 for v_SQL;
                         exception
                         when others then
                              a_retcode := sqlcode;
                              a_err_mes := sqlerrm;
                              return;
                         end;
                         loop--循环读取账号
                         fetch v_cursor1 into v_AccountID;
                         exit when v_cursor1%notfound ;
                         -----------------------------------------
                         begin
                         v_tempnum2 := 0;
                         --查询账号在帐户属性表中的数据
                         v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                         execute immediate v_SQL into v_tempnum2;
                         exception
                         when no_data_found then
                              a_classify := '双资料场景（根据电话号码，查询到多个用户号，有一个用户号查询到多个账号其中一个账号无效）';--其中一个用户号有多个账号，有一个账号资料无效
                              v_classifydetail := '1220';--有电话号码，查询到多个用户号，有一个用户号查询到多个账号，有一个账号无效
                              return;
                         when others then
                              a_retcode := sqlcode;
                              a_err_mes := sqlerrm;
                              return;

                         end;
                         if v_tempnum2 = 0 then
                              a_classify := '双资料场景（根据电话号码，查询到多个用户号，有一个用户号查询到多个账号其中一个账号无效）';--其中一个用户号有多个账号，有一个账号资料无效
                              v_classifydetail := '1220';--有电话号码，查询到多个用户号，有一个用户号查询到多个账号，有一个账号无效
                              return;
                         end if;
                         -----------------------------------------
                         end loop;
                         close v_cursor1;

                         end if;
                    end if;
                  ----------------根据用户号查询账号信息-------------------------
                   end loop;
                   close v_cursor;
                   v_classifydetail := '121';--有电话号码，查询到多个用户号，所有用户号对应的所有账号均有效
            end if;
       end if;
       return;


     --电话号码为空，根据IMSI号码查询用户号
     else
       --查询用户号个数
       v_SQL := 'select count(*) from '||v_subtabname||' where attr_id = 3 and value = '||v_IMSI||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
       begin
       v_tempnum := 0;
       execute immediate v_SQL into v_tempnum;
       exception
       when others then
            a_retcode := sqlcode;
            a_err_mes := sqlerrm;
            return;
       end;
       if v_tempnum = 0 then
            a_classify := 'IMSI无法匹配用户';--根据IMSI查询不到用户号
            v_classifydetail := '20';--有IMSI，但查询不到用户号
            return;
       else
            if v_tempnum = 1 then --IMSI查询到一个用户号
            --查询用户号
            v_SQL := 'select key_id from '||v_subtabname||' where attr_id = 3 and value = '||v_IMSI||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
            begin
              execute immediate v_SQL into v_SubscriberID;
              exception
              when no_data_found then
                 a_classify := 'IMSI无法匹配用户';--根据IMSI查询不到用户号
                 v_classifydetail := '20';--有IMSI，但查询不到用户号
                 return;
              when others then
                 a_retcode := sqlcode;
                 a_err_mes := sqlerrm;
                 return;
            end;
            ----------------根据用户号查询账号信息-------------------------

              v_tempnum := 0;
              --查询账号个数
              v_SQL := 'select count(*) from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
              begin
              execute immediate v_SQL into v_tempnum;
              exception
              when others then
                   a_retcode := sqlcode;
                   a_err_mes := sqlerrm;
                   return;
              end;
              if  v_tempnum = 0 then
                   a_classify := '用户无帐户信息';--查询到一个用户号，该用户号无帐户信息
                   v_classifydetail := '210';--有IMSI，查询到一个用户号，该用户号查询不到账号
                   return;
              else if v_tempnum = 1 then--查询到一个账号
                   --查询账号
                   v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   begin
                   --select value into v_AccountID from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                   execute immediate v_SQL into v_AccountID;
                   exception
                   when no_data_found then
                      a_classify := '用户无帐户信息';--用户无帐户信息
                      v_classifydetail := '210';--有IMSI，查询到一个用户号，该用户号查询不到账号
                      return;
                   when others then
                      a_retcode := sqlcode;
                      a_err_mes := sqlerrm;
                      return;
                   end;
                   begin
                   v_tempnum2 := 0;
                   --查询该账号在帐户属性表中有没有数据
                   v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                   execute immediate v_SQL into v_tempnum2;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                    end;
                   if v_tempnum2 = 0 then
                        a_classify := '用户的帐户资料无效';--查询到一个用户号，对应的帐户资料无效
                        v_classifydetail := '2110';--有IMSI，查询到一个用户号，查询到一个账号，账号无效
                        return;
                   else
                        v_classifydetail := '2111';--有IMSI，查询到一个用户号，查询到一个账号，账号有效
                   end if;
                   else--查询到多个账号,检测每个账号的状态
                   v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --declare CURSOR v_cursor1 is select value from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                   begin
                   open v_cursor1 for v_SQL;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                   end;
                   loop--循环读取账号
                   fetch v_cursor1 into v_AccountID;
                   exit when v_cursor1%notfound ;
                   -----------------------------------------
                   begin
                   v_tempnum2 := 0;
                   --根据账号到帐户属性表中搜索数据
                   v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                   execute immediate v_SQL into v_tempnum2;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                   end;
                   if v_tempnum2 = 0 then
                        a_classify := '双资料场景（根据IMSI，查询到一个用户号，该用户号查询到多个账号，但有账号无效）';--有一个账号无效，认为是双资料问题
                        v_classifydetail := '2120';--有IMSI，查询到一个用户号，用户号查询到多个账号，有一个账号无效
                        return;
                   end if;
                   -----------------------------------------
                   end loop;
                   close v_cursor1;
                   v_classifydetail := '2121';--有IMSI，查询到一个用户号，查询到多个账号，账号均有效
                   end if;
              end if;

            else--IMSI查询到多个用户号
                   v_SQL := 'select key_id from '||v_subtabname||' where attr_id = 3 and value = '||v_IMSI||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                   --declare cursor v_cursor is  select key_id from subscriber_attr where attr_id = 2 and value = v_TelNum and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                   begin
                   open v_cursor for v_SQL;
                   exception
                   when others then
                        a_retcode := sqlcode;
                        a_err_mes := sqlerrm;
                        return;
                   end;
                   loop--循环每个用户号
                   fetch v_cursor into v_SubscriberID;
                   exit when v_cursor%notfound ;
                   ----------------根据用户号查询账号信息-------------------------
                    v_tempnum := 0;
                    begin
                    --查询该用户对应的帐号数量
                    v_SQL := 'select count(*) from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                    --select count(*) into v_tempnum from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                    execute immediate v_SQL into v_tempnum;
                    exception
                    when others then
                         a_retcode := sqlcode;
                         a_err_mes := sqlerrm;
                         return;
                    end;
                    if  v_tempnum = 0 then
                         a_classify := '双资料场景（根据IMSI，查询到多个用户号，有一个用户无帐户信息）';--有一个用户无帐户信息，认为是双资料
                         v_classifydetail := '220';--有IMSI，查询到多个用户号，有一个用户无帐户信息
                         return;
                    else if v_tempnum = 1 then--查询到一个账号
                         begin
                         --查询账号
                         v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --select value into v_AccountID from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                         execute immediate v_SQL into v_AccountID;
                         exception
                         when no_data_found then
                            a_classify := '双资料场景（根据IMSI，查询到多个用户号，有一个用户无帐户信息）';--有一个用户无帐户信息，认为是双资料
                            v_classifydetail := '220';--有IMSI，查询到多个用户号，有一个用户无帐户信息
                            return;
                         when others then
                            a_retcode := sqlcode;
                            a_err_mes := sqlerrm;
                            return;
                         end;
                         begin
                         v_tempnum2 := 0;
                         --查询账号在帐户属性表中的数据
                         v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                         execute immediate v_SQL into v_tempnum2;
                         exception
                         when no_data_found then
                              a_classify := '双资料场景（根据IMSI，查询到多个用户号，有一个用户对应的账号无效）';--双资料
                              v_classifydetail := '2210';--有IMSI，查询到多个用户号，有一个用户对应一个账号，账号无效
                              return;
                         when others then
                              a_retcode := sqlcode;
                              a_err_mes := sqlerrm;
                              return;

                         end;
                         if v_tempnum2 = 0 then
                              a_classify := '双资料场景（根据IMSI，查询到多个用户号，有一个用户对应的账号无效）';--双资料
                              v_classifydetail := '2210';--有IMSI，查询到多个用户号，有一个用户对应一个账号，账号无效
                              return;
                         end if;
                         else--查询到多个账号,检测每个账号的状态
                         v_SQL := 'select value from '||v_subtabname||' where attr_id = 9 and key_id = '||v_SubscriberID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --declare cursor v_cursor1 is  select value from subscriber_attr where attr_id = 9 and key_id = v_SubscriberID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) > v_StartTime;
                         begin
                         open v_cursor1 for v_SQL;
                         exception
                         when others then
                              a_retcode := sqlcode;
                              a_err_mes := sqlerrm;
                              return;
                         end;
                         loop--循环读取账号
                         fetch v_cursor1 into v_AccountID;
                         exit when v_cursor1%notfound ;
                         -----------------------------------------
                         begin
                         v_tempnum2 := 0;
                         --查询账号在帐户属性表中的数据
                         v_SQL := 'select count(*) from '||v_acctabname||' where key_id = '||v_AccountID||' and availtime <= to_date('||v_StartTime||',''yyyymmddhh24miss'') and nvl(expiretime,sysdate+1000) > to_date('||v_StartTime||',''yyyymmddhh24miss'')';
                         --select count(*) into v_tempnum2 from account_attr where key_id = v_AccountID and availtime <=v_StartTime and nvl(expiretime,sysdate+1000) >v_StartTime;
                         execute immediate v_SQL into v_tempnum2;
                         exception
                         when no_data_found then
                              a_classify := '双资料场景（根据IMSI，查询到多个用户号，有一个用户号查询到多个账号其中一个账号无效）';--双资料
                              v_classifydetail := '2220';--有IMSI，查询到多个用户号，有一个用户对应多个账号，有一个账号无效
                              return;
                         when others then
                              a_retcode := sqlcode;
                              a_err_mes := sqlerrm;
                              return;
                         end;
                         if v_tempnum2 = 0 then
                              a_classify := '双资料场景（根据IMSI，查询到多个用户号，有一个用户号查询到多个账号其中一个账号无效）';--双资料
                              v_classifydetail := '2220';--有IMSI，查询到多个用户号，有一个用户对应多个账号，有一个账号无效
                              return;
                         end if;
                         -----------------------------------------
                         end loop;
                         close v_cursor1;
                         end if;
                    end if;
                  ----------------根据用户号查询账号信息-------------------------
                   end loop;
                   close v_cursor;
                   v_classifydetail := '221';--有IMSI，查询到多个用户号，每个用户号对应的账号均有效
            end if;
       end if;
       return;
     end if;
     return;
  end if;

  begin
    v_FieldName :=NULL;
    select field_name into v_FieldName from EVENT_ATTR_DEF where event_id= v_EventID and attr_class=15;--取totalfree信息
    exception
        when no_data_found then
         a_err_mes :='can not get totalfree column name';
        when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  if v_FieldName is not NULL then
  v_SQL := 'select '||v_FieldName||' from '||a_tablename||' where rowid = '''||a_rowid||'''';
  begin
    execute immediate v_SQL into v_TotalFree;
    exception
    when no_data_found then
         a_err_mes :='can not get totalfree';
         return;
    when others then
         a_retcode := sqlcode;
         a_err_mes := sqlerrm;
         return;
  end;
  end if;
  ----------批价出错逻辑-----------------
  if v_ErrorCode = 'T130' or v_ErrorCode = 'T230' then
     a_classify := '资费政策'||v_TotalFree||'运行出错';
     return;
  end if;

  ----------预处理出错逻辑-----------------
  if v_ErrorCode = 'T140' or v_ErrorCode = 'T240' then
     if v_TotalFree='1' then
        a_classify := '运行批价前预处理出错';
     else if v_TotalFree='2' then
        a_classify := '运行批价后预处理出错';
          end if;
     end if;
     return;
  end if;

  end;
END Ap_Get_Noservreason;
//





CREATE OR REPLACE PROCEDURE "AP_GSM_12593" IS
	L_SQL        VARCHAR2(4096);
	V_TABLE_NAME VARCHAR2(100);
BEGIN
	V_TABLE_NAME := 'RTCCVPMNCDR';
	FOR CC IN (SELECT * FROM USER_SEGMENTS WHERE SEGMENT_NAME = V_TABLE_NAME) LOOP
		L_SQL := 'insert into rtcc_check_log 
(TASKID, BILLINGCYCLE, REGION, 
CHECK_DAY, STATUS, ON_TOTAL_NUM, OFF_TOTAL_NUM, SAME_NUM, ON_DIFF_NUM, 
OFF_DIFF_NUM, BEGIN_TIME, END_TIME)
select 2, BILLINGCYCLE, hregion, 
max(starttime), 1, count(1), count(1), count(1), 1, 1, max(starttime), 
max(starttime)
from rtccvpmncdr partition (' || CC.PARTITION_NAME || ')
group by 2, BILLINGCYCLE, hregion';
		EXECUTE IMMEDIATE L_SQL;
		COMMIT;
		dbms_output.put_line('1..........');
	END LOOP;
END;
//





  CREATE OR REPLACE PROCEDURE "AP_IMP_SGSN" (a_sgsn_first in varchar2, a_sgsn_last in varchar2, a_region in varchar2, a_starttime in varchar2, a_stoptime in varchar2) is
   v_sgsn_first number(10);
   v_sgsn_last  number(10);
   vc_sgsn_first  varchar2(8);
   vc_sgsn_last  varchar2(8);
   v_region  varchar2(6);
   v_starttime date;
   v_stoptime date;
   i number(10);
begin
   if substr(a_sgsn_first,6,3)='000' and substr(a_sgsn_last,6,3)='FFF' then
      vc_sgsn_first := substr(a_sgsn_first,1,5);
      vc_sgsn_last := substr(a_sgsn_last,1,5);
   elsif substr(a_sgsn_first,7,2)='00' and substr(a_sgsn_last,7,2)='FF' then
      vc_sgsn_first := substr(a_sgsn_first,1,6);
      vc_sgsn_last := substr(a_sgsn_last,1,6);
   elsif substr(a_sgsn_first,8,1)='0' and substr(a_sgsn_last,8,1)='F' then
      vc_sgsn_first := substr(a_sgsn_first,1,7);
      vc_sgsn_last := substr(a_sgsn_last,1,7);
   end if;
   v_sgsn_first := hex_to_dec(vc_sgsn_first);
   v_sgsn_last := hex_to_dec(vc_sgsn_last);
   v_region := a_region;
   if substr(a_region,1,1) in ('1','2') then
      v_region := substr(a_region,1,2);
   end if;
   v_starttime := to_date(a_starttime, 'yyyy-mm-dd');
   v_stoptime := to_date(a_stoptime, 'yyyy-mm-dd');
   if v_starttime is null then
      v_starttime := to_date('20060101','yyyymmdd');
   end if;
   if v_starttime > to_date('20370101','yyyymmdd') then
      v_starttime := to_date('20370101','yyyymmdd');
   end if;
   if v_stoptime is null or v_stoptime > to_date('20370101','yyyymmdd') then
      v_stoptime := to_date('20370101','yyyymmdd');
   end if;

   i := v_sgsn_first;
   dbms_output.put_line('i:  ' || i);
   dbms_output.put_line('v_sgsn_last:  ' || v_sgsn_last);
   while i <= v_sgsn_last loop
      insert into tmp_mapping_list_121 (MAPPING_SOUR, MAPPING_DEST, APPLYTIME, EXPIRETIME, MAPPING_ID, NOTE)
        values (dec_to_hex(i), v_region, v_starttime, v_stoptime, 121, 'Init at '||to_char(sysdate, 'yyyymmdd'));
      i := i + 100000;
	  dbms_output.put_line('1..........');
   end loop;
   commit;
end ap_imp_sgsn;
//




CREATE OR REPLACE PACKAGE "PKG_MASKCHECK" IS
--echo ##type ty_str_split是function maskcheck中找到并置于此处
type ty_str_split IS TABLE OF VARCHAR2 (64);
 ---十进制转化为二进制
 function d2b(n in number) return varchar2;

 ---某个字符串是否为数字字符串
 function isNumber(p in varchar2) return number;

 ---将分隔符分隔的字符串拆分
 function fn_split (p_str IN VARCHAR2, p_delimiter IN VARCHAR2) RETURN ty_str_split PIPELINED;

 ---判断登录IP是否在IP_RANGE_LIST中定义
 function maskcheck(a_ipmask in varchar2, a_checkip in varchar2 := null) return varchar2;

end pkg_MaskCheck;
//


CREATE OR REPLACE PACKAGE BODY "PKG_MASKCHECK" is

  --------------十进制转化为二进制 begin----------------------
 function d2b (n in number) return varchar2 is
    binval varchar2(64);
    n2     number := n;
    begin
      if n = 0 then
	   dbms_output.put_line('1');
         return '0';
      end if;
      while ( n2 > 0 ) loop
        binval := mod(n2, 2) || binval;
        n2 := trunc( n2 / 2 );
		 dbms_output.put_line('2');
      end loop;
      return binval;
  end d2b;
 --------------十进制转化为二进制 end---------------------------


  -----------某个字符串是否为数字字符串 begin-------------------
  function isNumber(p in varchar2)
  return number
  is
  result number;
  begin
    result := to_number(p);
	 dbms_output.put_line('3');
    return 1;
    exception
    when VALUE_ERROR then
	 dbms_output.put_line('4');
	return 0;
  end isNumber;
  -----------某个字符串是否为数字字符串 end-----------------------

 ------------将分隔符分隔的字符串拆分 begin-----------------------
 function fn_split (p_str IN VARCHAR2, p_delimiter IN VARCHAR2)
 RETURN ty_str_split PIPELINED
 IS
  j number:= 0;    ---分隔符的位置
  i number:= 1;    ---从哪个位置开始截取字符串
  len number:= 0;
  len1 number:= 0;
  str VARCHAR2 (64);
  BEGIN
    len := LENGTH (p_str);   ----被分割字符串的长度
    len1 := LENGTH (p_delimiter); ---分隔符的长度
    WHILE j < len
    LOOP
      j:= INSTR(p_str, p_delimiter, i);  ---取得分隔符的位置
      IF j = 0 THEN
        j := len;
        str := SUBSTR (p_str, i);
        PIPE ROW (str);
        IF i >= len THEN
		 dbms_output.put_line('5');
          EXIT;
        END IF;
      ELSE
        str := SUBSTR (p_str, i, j - i);
        i := j + len1;
        PIPE ROW (str);
		 dbms_output.put_line('6');
      END IF;
    END LOOP;
      RETURN;
    END fn_split;


 function maskcheck(a_ipmask in varchar2, a_checkip in varchar2 := null)
 return varchar2 is
  ip   varchar2(64);  ----登录IP的二进制流
  cmp  varchar2(64);  ----IP_rang_list中的IP二进制流
  mask number;        ----mask的位数
  n    number;        ----/的位置,如1.0.0.255/8，则n=10
  numFlag number;     ----是否为数字字符串
  type ty_str_split IS TABLE OF VARCHAR2 (64);
  arr2 ty_str_split := ty_str_split(); ----arr2存放IP地址拆分后的数据

  begin
  if a_ipmask = a_checkip then   --输入IP和IP_RANGE_LIST中IP一致返回1
     dbms_output.put_line('7');
	return '1';
  end if;
  -----------取得网络IP，网络位数 begin--------------------------
  n := instr(a_ipmask, '/');
  if n > 0 then                    ---取得/前面的IP和子网网络位的位数
     mask := to_number(substr(a_ipmask, n + 1));
     if mask<0 or mask >32 then    ---子网网络位数不在0-32之间
        dbms_output.put_line('8');
	   return '0';
     end if;
     if n = 1 then  ---如果只有/,则返回
	  dbms_output.put_line('9');
        return '0';
     end if;
     ip := substr(a_ipmask, 1, n-1);
  else
     mask := 0;
     ip := a_ipmask;
	  dbms_output.put_line('10');
  end if;
  -----------取得网络IP，网络位数 end--------------------------

  -----------将IP根据分隔符.进行拆分，并判断是否符合要求 begin-------
  select column_value bulk collect into arr2 from table(fn_split(ip, '.'));
  if arr2.count <> 4 then ---如果IP不是10.164.130.108这样被.分割成四部分，则返回
   dbms_output.put_line('11');
     return 0;
  end if;
  ip := '';
  for z in 1..arr2.count loop  --ip转化为二进制
     numFlag := isNumber(arr2(z));
     if numFlag = 0 or arr2(z)<0 or arr2(z)>255 then  ----如果分割后的数据，每部分不在0-255之间，或不是数字字符串，则返回
	  dbms_output.put_line('12');
        return 0;
     end if;
     ip := ip || substr(lpad(nvl(d2b(to_number(arr2(z))),'0'),8,'0'),0,8) ;
  end loop;
  if a_checkip is null then
    if mask > 0 then
      ip :=  substr(ip,1,mask);
	   dbms_output.put_line('13');
    end if ;
    return ip;
  else
    cmp := maskcheck(a_checkip);
    if mask > 0 and substr(cmp,1,mask) = substr(ip,1,mask) then
	 dbms_output.put_line('14');
      return '1';
    else
	 dbms_output.put_line('15');
      return '0';
    end if;
  end if;
  -----------将IP根据分隔符.进行拆分，并判断是否符合要求 end-------
end maskcheck;

end pkg_MaskCheck;
//




  CREATE OR REPLACE PROCEDURE "AP_LOGON_APP" (APP_USERNAME    IN VARCHAR2,
                                         APP_PASSWORD    IN VARCHAR2,
                                         APP_SERVERNAME  IN VARCHAR2,
                                         LOGON_RESULT    OUT NUMBER,
                                         DB_DRIVER_NAME  OUT VARCHAR2,
                                         DB_URL          OUT VARCHAR2,
                                         DB_USERNAME     OUT VARCHAR2,
                                         DB_PASSWORD     OUT VARCHAR2,
                                         RESULT_STR      OUT VARCHAR2,
                                         APP_UAFLAG      IN VARCHAR2 := '0',
                                         LOGON_IP        IN VARCHAR2 := '0',
                                         LOGON_MAC       IN VARCHAR2 := '0',
                                         APP_OLDPASSWORD IN VARCHAR2 := '') AS
  /*******************************************************************************
  修订记录:
    2006-07-16 裘学欣39824 增加统一认证参数处理
  *******************************************************************************/

  /*******************************************************************************
  主要功能:本地用户认证验证密码，取数据库连接串。
  调用参数:
  App_Username   IN   用户名称
  App_Password   IN   用户密码
  App_Servername IN   服务名称
  Logon_Result   OUT  返回结果码
  Db_Driver_Name OUT  数据库驱动名
  Db_Url         OUT  数据库连接串
  Db_Username    OUT  数据库登录名
  Db_Password    OUT  数据库登录密码
  Result_Str     OUT  返回结果信息
  App_Uaflag     IN   统一认证标志
  logon_ip        in  用户地址   --V2R2C05B30   39835 20061217 add
  logon_msc       in  用户MSCid  --V2R2C05B30   39835 20061217 add
  *******************************************************************************/
  THE_APP_PASSWORD    VARCHAR2(257); /*用户口令*/
  THE_APP_USER_STATUS CHAR(1); /*用户状态*/
  FAILCNT             NUMBER(4) := 0; /*失败登陆次数*/
  MODIFYDATE          DATE; /*更新日期*/
  MAXCYCLE            NUMBER(4) := 0; /*有效天数*/
  MAXFAILCNT          NUMBER(4) := 0; /*失败次数*/
  --V2R2C05B30   39835 20061217 add
  NMAXLOGONLOCKDUR NUMBER(4) := 0; /*密码锁定时长*/
  LOCKDATE         DATE; /*密码锁定日期*/
  NUSERTYPE        NUMBER(1); /*用户类型*/
  NPWDTYPE         NUMBER(1); /*密码类型*/
  NPWDVLDPROMPT    NUMBER(4) := 0; /*密码提前提示天数*/
  NINTERVALDAY     NUMBER(6, 2) := 0; /*间隔天数*/
  NUPGRADECOUNT    NUMBER(4) := 0; /*升级次数*/
  --V2R2C05B30   39835 20061217 end

  ---added for issue13211 begin---
  FLAG number(1);
  ---added for issue13211 end---

  ---added for issue14360 begin---
  nStatus varchar(1);
  ---added for issue14360 end---

  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-24 add
  DLOGONTIME      DATE;
  NLOGONAFTERDAYS NUMBER(4) := 90;
  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-24 end

  --zkf28147 2010-8-9  add
  V_SYSTEMMODE VARCHAR2(64);
  --zkf28147 2010-8-9  end

  --5024 20130106 add
  WAITFREELOCKTIME NUMBER(24) := 0; /*等待解除锁定时间*/
  --5024 20130106 end

  --added for #19807 V3R5C31安全整改 begin
  INUSESTARTTIME DATE;
  INUSEENDTIME   DATE;
  --added for #19807 V3R5C31安全整改 end

  PROCEDURE FREE_USE IS
    DB_IPADDR VARCHAR2(30) := '10.132.42.21';
    DB_PORT   VARCHAR2(30) := '1521';
    DB_NAME   VARCHAR2(30) := SYS_CONTEXT('USERENV', 'DB_NAME'); --boss21
    A_USERID  VARCHAR2(30) := APP_USERNAME;
  BEGIN
    --Db_Url          := 'jdbc:oracle:thin:@10.132.42.21:1521:boss21';
    DB_URL         := 'jdbc:oracle:thin:@' || DB_IPADDR || ':' || DB_PORT || ':' ||
                      DB_NAME;
    DB_USERNAME    := SYS_CONTEXT('USERENV', 'CURRENT_USER'); --billing
    DB_PASSWORD    := 'Z1GbzJ2RSxGZn1TP';
    DB_PASSWORD    := 'billdev';
    DB_DRIVER_NAME := 'oracle.jdbc.driver.OracleDriver';
    LOGON_RESULT   := 100; /*返回结果码*/
    RESULT_STR     := ''; /*返回结果信息*/
    BEGIN
      INSERT INTO APP_USER
        (USER_CODE,
         USER_NAME,
         PASSWORD,
         STATUS,
         DESCRIPTION,
         PASSWORD_HIS,
         MODIFY_DATE,
         FAIL_COUNT,
         USERTYPE,
         PWDTYPE)
      VALUES
        (A_USERID, A_USERID, '', '1', '', '', SYSDATE, 0, 1, 0);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    BEGIN
      INSERT INTO PRIV_DESIGNATE
        (OPERATOR_NAME, RESOURCE_TYPE, RESOURCE_ID, PRIV_SET)
      VALUES
        (A_USERID, 1, 1, 15);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    BEGIN
      INSERT INTO PRIV_DESIGNATE
        (OPERATOR_NAME, RESOURCE_TYPE, RESOURCE_ID, PRIV_SET)
      VALUES
        (A_USERID, 1, 2, 15);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
    COMMIT;
  END;

BEGIN
  --free_use;
  --return;
  LOGON_RESULT := 0; /*返回结果码*/
  RESULT_STR   := 'test'; /*返回结果信息*/

  IF APP_UAFLAG = '2' THEN
    /*如果统一认证标志等于2，则出错返回*/
    LOGON_RESULT := 0;
    RESULT_STR   := '执行SQL出错，请换新程序版本。';
    RETURN;
  END IF;
  --add by zkf28147 2010-8-9 增加开发模式,在开发模式下登陆系统时不需要密码验证 begin
  BEGIN
    SELECT VALUE
      INTO V_SYSTEMMODE
      FROM FEATURE_DEF
     WHERE NAME = 'system_mode';
    IF V_SYSTEMMODE = 'development' THEN
      /*取数据库连接信息，未找到连接需提示*/
      BEGIN
	  dbms_output.put_line('1..........');
        SELECT DRIVER_NAME, URL, USER_NAME, PASSWORD
          INTO DB_DRIVER_NAME, DB_URL, DB_USERNAME, DB_PASSWORD
          FROM APP_CONNECTION_LIST
         WHERE SERVER_NAME = APP_SERVERNAME
         AND LAN_TYPE = 'JAVA';
      EXCEPTION
        WHEN OTHERS THEN
          LOGON_RESULT := 0;
          RESULT_STR   := '未找到指定的连接名称[' || APP_SERVERNAME || ']。';
          RETURN;
      END;
      LOGON_RESULT := 1;
      RETURN;
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_SYSTEMMODE := '';
  END;
  --add by zkf28147 2010-8-9 增加开发模式，在开发模式下登陆系统时不需要密码验证 end
  /*取用户密码和状态，如不存在需提示*/
  BEGIN
    SELECT PASSWORD, STATUS, NVL(INUSE_STARTTIME, SYSDATE), NVL(INUSE_ENDTIME, SYSDATE+1)
      INTO THE_APP_PASSWORD, THE_APP_USER_STATUS, INUSESTARTTIME, INUSEENDTIME
      FROM APP_USER
     WHERE USER_CODE = APP_USERNAME;
	 dbms_output.put_line('2..........');
  EXCEPTION
    WHEN OTHERS THEN
      LOGON_RESULT := 0;
      RESULT_STR := '用户名或者口令错误，登录失败。'; --V2R2C05B30   39835 20061214 add
      RETURN;
  END;
  --added for #19807 V3R5C31安全整改 begin----------------------
  IF INUSESTARTTIME > INUSEENDTIME THEN
    LOGON_RESULT := 0;
    RESULT_STR   := '该用户有效期设置出错，请检查。';
    RETURN;
  ELSE
    IF SYSDATE < INUSESTARTTIME THEN
      RESULT_STR   := '该用户未生效，请与系统管理员联系。';
      LOGON_RESULT := 0;
      RETURN;
    ELSIF SYSDATE > INUSEENDTIME THEN
      RESULT_STR   := '该用户已失效，请与系统管理员联系。';
      LOGON_RESULT := 0;
      RETURN;
    END IF;
	dbms_output.put_line('3..........');
  END IF;
  --added for #19807 V3R5C31安全整改 end------------------------
  /*
  检查用户状态
  - 锁定
  0 初始
  1 正常
  U 统一认证
  */
  --V2R2C05B30   39835 20061217  本地认证 增加密码登录锁定时长（分钟）  判断逻辑
  --取出数据
  IF APP_UAFLAG = '0' THEN  -- APP_UAFLAG = 0
    IF (THE_APP_USER_STATUS = '-') THEN
	dbms_output.put_line('4..........');
      SELECT NVL(MAX(LOGONLOCKDUR), 0)
        INTO NMAXLOGONLOCKDUR
        FROM SAFETY_STRATEGY
       WHERE ROWNUM = 1;
      --无限期锁定
      IF NMAXLOGONLOCKDUR = 0 THEN
        LOGON_RESULT := 0;
        RESULT_STR   := '用户已被锁定，请联系管理员解锁。';
        RETURN;
      END IF;
      BEGIN
        --密码错误登陆次数第一次超过限制的登陆时间
        SELECT NVL(MAX(LOGONTIME), TO_DATE('20000101', 'yyyymmdd'))
          INTO LOCKDATE
          FROM BILLINGCLIENTLOG
         WHERE OPERID = APP_USERNAME
           AND SUCCESS = 9;
        --当前日期与锁定日期进行比较，如大于则设置状态为正常
        LOCKDATE := LOCKDATE + NMAXLOGONLOCKDUR / 1440;
        IF (SYSDATE >= LOCKDATE) THEN
          THE_APP_USER_STATUS := '1';
          UPDATE APP_USER
             SET STATUS = '1', FAIL_COUNT = 0
           WHERE USER_CODE = APP_USERNAME;
		   dbms_output.put_line('5..........');
        ELSE
          WAITFREELOCKTIME := (TO_NUMBER(LOCKDATE - SYSDATE) * 86400);
          LOGON_RESULT     := 0;
          RESULT_STR       := '用户已被锁定，请等待，' || WAITFREELOCKTIME || '秒后自动解锁';
          RETURN;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
    END IF;
  END IF;
  --V2R2C05B30   39835 20061217  增加密码登录锁定时长（分钟）  判断逻辑     结束
  --V2R2C05B30   39835 20061217  增加限制客户端登陆逻辑
  IF LOGON_IP = '0' OR LOGON_MAC = '0' THEN
    GOTO LABEL1;
  END IF;
  IF APP_UAFLAG = '0' THEN
    BEGIN
      FOR C1 IN (SELECT * FROM LIMITLOGON WHERE INUSE = 1) LOOP
        IF (C1.STARTHOUR <= TO_NUMBER(TO_CHAR(SYSDATE, 'hh24miss')) AND
           C1.ENDHOUR >= TO_NUMBER(TO_CHAR(SYSDATE, 'hh24miss'))) THEN
          BEGIN
            IF (C1.IPADDR = LOGON_IP OR TRIM(C1.IPADDR) IS NULL) AND
               (C1.MACADDR = LOGON_MAC OR TRIM(C1.MACADDR) IS NULL) THEN
              THE_APP_USER_STATUS := '-';
            END IF;
          END;
        END IF;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END IF;

  -------added for issue13211 begin-------------------------------
  IF APP_UAFLAG = '0' THEN
     select nvl(sum(to_number(pkg_maskcheck.maskcheck(IP_PATTERN,LOGON_IP))),1) into flag from IP_RANGE_LIST where 1>=0;
     if flag =0 then
         --UPDATE APP_USER SET STATUS = '-' WHERE USER_CODE = APP_USERNAME;
          LOGON_RESULT := 0;
          RESULT_STR   := '该用户IP已被限制登录，请与系统管理员联系。';
          RETURN;
     end if;
     ------added for issue14360 begin-----------
     select status into nStatus from app_user where user_code = APP_USERNAME;
     if nStatus = 'F' then
          LOGON_RESULT := 0;
          RESULT_STR   := '该用户已被禁用，请与系统管理员联系。';
          RETURN;
     end if;
     ------added for issue14360 end-------------
  END IF;
  -------added for issue13211 end-------------------------------


  <<LABEL1>>
  IF THE_APP_USER_STATUS = '-' THEN
    LOGON_RESULT := 0;
    RESULT_STR   := '用户已被锁定，请与系统管理员联系。';
    RETURN;
  ELSIF THE_APP_USER_STATUS = '0' THEN
    LOGON_RESULT := 100;
  ELSIF THE_APP_USER_STATUS = '1' THEN  -- THE_APP_USER_STATUS = 1
    LOGON_RESULT := 1;
  ELSIF THE_APP_USER_STATUS = 'U' THEN
    IF APP_UAFLAG = '0' THEN
      LOGON_RESULT := 0;
      RESULT_STR   := '需执行统一认证，请检查billingclient.con配置文件。';
      RETURN;
    ELSIF APP_UAFLAG = '1' THEN
      LOGON_RESULT := 1;
    END IF;
  ELSE
    LOGON_RESULT := 0;
    RESULT_STR   := '用户状态异常。';
    RETURN;
  END IF;

  /*如为本地验证，则检查用户密码*/
  IF APP_UAFLAG = '0' THEN
    --验证密码是否为空，如是则返回 V2R2C05B30   39835 20061218 新增
    IF (THE_APP_PASSWORD IS NULL) THEN
      RESULT_STR   := '密码不能为空，请与系统管理员联系';
      LOGON_RESULT := 0;
      RETURN;
    END IF;
    --V2R2C05B30   39835 20061218 结束  THE_APP_PASSWORD = 2  APP_PASSWORD = 'abc'
	dbms_output.put_line('6..........');
    IF 'P' || THE_APP_PASSWORD <> 'P' || APP_PASSWORD THEN
      LOGON_RESULT := 0;
      RESULT_STR   := '用户名或密码错误';
      --V2R2C05B30   39835 20061218 新增
      --增加前台密码与数据库中密码同的判断
      IF 'p' || THE_APP_PASSWORD = 'p' || APP_OLDPASSWORD THEN
        --密码升级自动更新
        SELECT COUNT(*)
          INTO NUPGRADECOUNT
          FROM BILLINGCLIENTLOG
         WHERE OPERID = APP_USERNAME
           AND SUCCESS = 8;
        IF NUPGRADECOUNT = 0 THEN
          LOGON_RESULT := 996;
          GOTO LABEL2;
        END IF;
      END IF;
      --V2R2C05B30   39835 20061218 新增  结束
      --如果用户累计登陆失败5次以上，锁定帐号
      SELECT NVL(MAX(FAILCOUNT_LIMIT), 0)
        INTO MAXFAILCNT   -- MAXFAILCNT = 9
        FROM SAFETY_STRATEGY
       WHERE ROWNUM = 1;
	   dbms_output.put_line('7..........');
      IF (MAXFAILCNT > 0) THEN
        UPDATE APP_USER
           SET FAIL_COUNT = FAIL_COUNT + 1
         WHERE USER_CODE = APP_USERNAME;
        SELECT FAIL_COUNT
          INTO FAILCNT  -- FAILCNT = 8
          FROM APP_USER
         WHERE USER_CODE = APP_USERNAME;
		 dbms_output.put_line('8..........');
        IF (FAILCNT >= MAXFAILCNT) THEN
          UPDATE APP_USER SET STATUS = '-' WHERE USER_CODE = APP_USERNAME;
          RESULT_STR   := RESULT_STR || ',已' || MAXFAILCNT/2 || '次登陆失败,用户被锁定';
          LOGON_RESULT := -999; --V2R2C05B30   39835 20061217  几次以上锁定帐号
        ELSE
          RESULT_STR := RESULT_STR || ',' || MAXFAILCNT/2 || '次登陆失败用户将被锁定';
		  dbms_output.put_line('9..........');
        END IF;
      END IF;
      COMMIT;
      RETURN;
    END IF;
  END IF;
  <<LABEL2>> --V2R2C05B30   39835 20061218  新增 游标标记
  /*如果用户验证通过，则置用户登陆失败次数为0*/
  UPDATE APP_USER SET FAIL_COUNT = 0 WHERE USER_CODE = APP_USERNAME;
  COMMIT;

  /*取数据库连接信息，未找到连接需提示*/
  BEGIN
    SELECT DRIVER_NAME, URL, USER_NAME, PASSWORD
      INTO DB_DRIVER_NAME, DB_URL, DB_USERNAME, DB_PASSWORD
      FROM APP_CONNECTION_LIST
     WHERE SERVER_NAME = APP_SERVERNAME
   AND LAN_TYPE = 'JAVA';
  EXCEPTION
    WHEN OTHERS THEN
      LOGON_RESULT := 0;
      RESULT_STR   := '未找到指定的连接名称[' || APP_SERVERNAME || ']。';
      RETURN;
  END;
  IF (LOGON_RESULT = 996) THEN
    RETURN;
  END IF;
  /*如果用户密码已失效，则返回999，提示用户修改密码*/
  IF APP_UAFLAG = '0' THEN
    --V2R2C05B30   39835 20061218 新增 usertype,pwdtype
    SELECT NVL(MODIFY_DATE, TO_DATE('19000101', 'yyyymmdd')),
           USERTYPE,
           PWDTYPE
      INTO MODIFYDATE, NUSERTYPE, NPWDTYPE
      FROM APP_USER
     WHERE USER_CODE = APP_USERNAME;
    --V2R2C05B30   39835 20061218  增加默认口令或管理员设置口令的逻辑
    --if nUserType = 0 THEN
    IF NPWDTYPE = 1 THEN
      LOGON_RESULT := 998;
      RETURN;
    END IF;
    --END IF;
    --V2R2C05B30   39835 20061218  增加默认口令或管理员设置口令的逻辑 结束
    --新增PWDVLD_PROMPT 密码提前提示天数
    SELECT NVL(MIN(INVALID_CYCLE), 0), NVL(MIN(PWDVLD_PROMPT), 0)
      INTO MAXCYCLE, NPWDVLDPROMPT
      FROM SAFETY_STRATEGY;
    --V2R2C05B30   39835 20061218  增加口令到期提示判断逻辑
    IF ((MAXCYCLE > 0) AND (NPWDVLDPROMPT > 0) AND
       (SYSDATE >= MODIFYDATE + MAXCYCLE - NPWDVLDPROMPT) AND
       (SYSDATE <= MODIFYDATE + MAXCYCLE)) THEN
      LOGON_RESULT := 997;
      NINTERVALDAY := TRUNC(MODIFYDATE + MAXCYCLE - SYSDATE, 2);
      RESULT_STR   := '口令还有' || NINTERVALDAY || '天失效，请尽快修改密码';
      RETURN;
    END IF;
    --V2R2C05B30   39835 20061218  增加口令到期提示判断逻辑 end
    IF ((MAXCYCLE > 0) AND (TRUNC(SYSDATE) > TRUNC(MODIFYDATE) + MAXCYCLE)) THEN
      LOGON_RESULT := 999;
      RETURN;
    END IF;
  END IF;

  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-24 add
  SELECT NVL(T.LOGONTIME, NVL(T.MODIFY_DATE, SYSDATE))
    INTO DLOGONTIME
    FROM APP_USER T
   WHERE T.USER_CODE = APP_USERNAME;

  SELECT NVL(MIN(T.LOGONAFTERDAYS), 0)
    INTO NLOGONAFTERDAYS
    FROM SAFETY_STRATEGY T;

  IF (SYSDATE - DLOGONTIME) > NLOGONAFTERDAYS THEN
    LOGON_RESULT := 995; --禁止登录
  END IF;
  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-24 end

  RETURN;
END;
//




CREATE OR REPLACE PACKAGE "PKG_VALIDUSER" AS
  Logonid       NUMBER(10) := 0; --2006-12-15 V2R2C05B30 39824
  Userid        VARCHAR2(100) := '_SYS_';
  Istriggerneed NUMBER := 1;
  Usertype      NUMBER := 0; --V3R1C01B01 39824 2007-3-19
  ClientInfo    varchar2(1024) := NULL;
  LogonSeq      VARCHAR2(32) := '0';

  PROCEDURE Setuser(v_User          IN VARCHAR2,
                    v_Istriggerneed NUMBER := 1,
                    v_clientinfo in varchar2 := '');
  PROCEDURE Defaultuser(v_Istriggerneed NUMBER := 1);
  PROCEDURE Setusertype(v_Usertype NUMBER := 0); --V3R1C01B01 39824 2007-3-19
  PROCEDURE SetLogonid(v_Logonid NUMBER := 0); --V3R1C12L07n11 sKF42558 2012-07-10
  --V3R1C01B01 39885 2007-03-15
  SUBTYPE Auditlogtype IS Audit_Log%ROWTYPE;
  FUNCTION Beginauditlog(a_Auditlog IN OUT Auditlogtype) RETURN NUMBER;
  PROCEDURE Writeauditlog(a_Auditlog  IN Auditlogtype,
                          a_Fieldname VARCHAR2,
                          a_Oldvalue  VARCHAR2,
                          a_Newvalue  VARCHAR2);
  PROCEDURE Writeauditlog(a_Auditlog  IN Auditlogtype,
                          a_Fieldname VARCHAR2,
                          a_Oldvalue  DATE,
                          a_Newvalue  DATE);
  PROCEDURE Endauditlog(a_Auditlog IN OUT Auditlogtype);
  --V3R1C01B01 39885 2007-03-15
  FUNCTION  GetUserid(a_userid in varchar2) return varchar2;
  PROCEDURE GetLogonSeq(a_LogonSeq OUT VARCHAR2);
END Pkg_Validuser;
//



sleep 5//
  CREATE OR REPLACE PROCEDURE "AP_LOGON_LOG" (I_OPERID   VARCHAR2,
                                         I_MAC_ADDR VARCHAR2,
                                         I_IP_ADDR  VARCHAR2,
                                         I_SUCCESS  NUMBER,
                                         I_HOSTNAME VARCHAR2 DEFAULT NULL) AS
  /*******************************************************************************
  Author:   q39824
  create:   2006-07-06
  update:   2007-12-29 by y46156 add parameter i_HostName
  --------------------------------------------------------------------------------
  description:  insert user logon log into Billingclientlog
  parameter:
  i_Operid    IN   Operator id
  i_Mac_Addr  IN   MAC addr
  i_Ip_Addr   IN   IP addr
  i_Success   IN   Success Flag
  i_HostName  IN   Host name
  *******************************************************************************/
  V_LOGONID NUMBER(10);
  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-23 add
  DLOGONTIME DATE;
  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-23 end
  ---added for issue14360 begin---
  V_SID NUMBER(32);
  ---added for issue14360 end---
BEGIN
  IF PKG_VALIDUSER.LOGONID = 0 THEN
    SELECT SEQ_BILLINGCLIENTLOG.NEXTVAL INTO V_LOGONID FROM DUAL;
	dbms_output.put_line('1..........');
  ELSE
    V_LOGONID := PKG_VALIDUSER.LOGONID;
  END IF;

  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-23 add
  IF I_SUCCESS = 7 THEN
    UPDATE APP_USER SET LOGOUTTIME = SYSDATE WHERE USER_CODE = I_OPERID;
	dbms_output.put_line('2..........');
  END IF;

  SELECT NVL(LOGONTIME, SYSDATE)
    INTO DLOGONTIME
    FROM APP_USER
   WHERE USER_CODE = I_OPERID;
  --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-23 end

  --5024 strat 记录登录锁定时间
  IF I_SUCCESS = 9 THEN
    SELECT SYSDATE INTO DLOGONTIME FROM DUAL;
  END IF;
  --5024 end 记录登录锁定时间

  --added for issue14360 begin --
  SELECT SYS_CONTEXT ('USERENV', 'sessionid') into V_SID FROM DUAL;
  dbms_output.put_line('2..........');
  --added for issue14360 end --

  --added the new field SID for issue14360 --
  INSERT INTO BILLINGCLIENTLOG
    (LOGONID, OPERID, MACADDR, IPADDR, SUCCESS, LOGONTIME, HOSTNAME, SID)
  VALUES
    (V_LOGONID,
     I_OPERID,
     I_MAC_ADDR,
     I_IP_ADDR,
     I_SUCCESS,
     --SYSDATE, --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-25 delete
     DLOGONTIME, --V3R1C01B093 AR.FUNC.011 skf12497 2009-2-25 add
     I_HOSTNAME,
     V_SID);

  PKG_VALIDUSER.LOGONID := V_LOGONID;
  COMMIT;
  dbms_output.put_line('3..........');
END;
//




CREATE OR REPLACE PROCEDURE "AP_MAPPING_LIST_CHECK" (v_mapping_sour in varchar2,v_mapping_dest in varchar2, v_mapping_id in number, result out number) is
v_count1 number;
v_count2 number;
begin
  select count(*) into v_count1
  from mapping_list
  where mapping_id = v_mapping_id and mapping_sour = v_mapping_sour and mapping_dest = v_mapping_dest;

  select count(*) into v_count2
  from mapping_list
  where mapping_id = v_mapping_id and mapping_sour = v_mapping_sour;

  if(v_count2 = 0) then
             result := 0;--does not find the same mapping_sour as v_mapping_sour
  else
    if(v_count1 >= 1) then
               result := 1;
    else
               result := 2;
    end if;
	dbms_output.put_line('1..........');
  end if;

end;
//




CREATE OR REPLACE PROCEDURE "AP_MAPPING_LIST_CLEAN" 
(
   cleanmonth NUMBER,          --清理几个月之前失效的数据
   a_mapping_id NUMBER       --需要清理的值映射编码，为-1时默认清理所有的失效值映射
)
Is
   V_ENDDATE DATE;

Begin
  Begin
     V_ENDDATE := trunc(add_months(sysdate,(-1*cleanmonth)),'mm');
     IF (a_mapping_id = -1) THEN
        insert into mapping_list_his (
                    MAPPING_SOUR ,
                    MAPPING_DEST,
                    APPLYTIME,
                    EXPIRETIME,
                    MAPPING_ID,
                    NOTE)
        select
                    MAPPING_SOUR ,
                    MAPPING_DEST,
                    APPLYTIME,
                    EXPIRETIME,
                    MAPPING_ID,
                    NOTE
        from mapping_list Where EXPIRETIME<=V_ENDDATE;
        Delete From mapping_list Where EXPIRETIME<=V_ENDDATE;
        commit;
		dbms_output.put_line('1..........');
     ELSE
            insert into mapping_list_his (
                        MAPPING_SOUR ,
                        MAPPING_DEST,
                        APPLYTIME,
                        EXPIRETIME,
                        MAPPING_ID,
                        NOTE)
            select
                        MAPPING_SOUR ,
                        MAPPING_DEST,
                        APPLYTIME,
                        EXPIRETIME,
                        MAPPING_ID,
                        NOTE
            from mapping_list Where EXPIRETIME<=V_ENDDATE AND MAPPING_ID = a_mapping_id;
            Delete From mapping_list Where EXPIRETIME<=V_ENDDATE AND MAPPING_ID = a_mapping_id;
            commit;
			dbms_output.put_line('2..........');
      END IF;
    Exception
      when others then
        dbms_output.put_line('call ap_mapping_list_clean() failed!');
        rollback;
     end;
End;
//




CREATE OR REPLACE PROCEDURE "AP_MED_TESTCPRUN" 
  as
   v_month    varchar2(20);
   v_sql      varchar2(8192);

begin
   v_month := to_char(to_date('20201001','yyyymmdd'),'yyyymm');

   for i in (select t.*,rowid from med_test000 t)
   loop
   v_sql :='insert into med_test_'||v_month||'(NODE,BATFILE,id,FILENAME,FILESIZE,BEGINTIME,BEGINUSEC,ENDTIME,ENDUSEC,LOGTIME,DAY)
                    values( :v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10,:v11)';
   EXECUTE IMMEDIATE v_sql using i.node,
                                 i.batfile,
                                 i.id,
                                 i.filename,
                                 i.filesize,
                                 i.begintime,
                                 i.beginusec,
                                 i.endtime,
                                 i.endusec,
                                 to_date('20201001','yyyymmdd'),
                                 i.day ;
                                
   --删除临时表中的数据
   delete from med_test000 q where q.rowid=i.rowid;
   dbms_output.put_line('1..........');
   commit;
   end loop;
commit;
end ap_med_testcprun;
//




CREATE OR REPLACE PROCEDURE "AP_MISCDUP" (v_month IN varchar2, v_tabname IN VARCHAR2)
AS
   l_sql        VARCHAR2 (2024);
   next_month   DATE;
   next_c_mon   VARCHAR2 (6);
   tabname      VARCHAR2 (30);
   v_oper_no    VARCHAR2 (4);
BEGIN
   IF v_tabname = 'ismg'
   THEN
      tabname := 'ismgmisccdr_dup';
      v_oper_no := '001';
	  dbms_output.put_line('1..........');
   ELSIF v_tabname = 'wap'
   THEN
      tabname := 'wapmisccdr_dup';
      v_oper_no := '003';
   ELSIF v_tabname = 'mms'
   THEN
      tabname := 'mmsmisccdr_dup';
      v_oper_no := '002';
   END IF;

   l_sql := 'truncate table ' || tabname;

   EXECUTE IMMEDIATE l_sql;
   

   DELETE FROM stl_misc_check
         WHERE CYCLE = v_month AND oper_no = v_oper_no;

   COMMIT;

   SELECT ADD_MONTHS (TO_DATE (v_month, 'yyyymm'), 1)
     INTO next_month
     FROM DUAL;

   SELECT TO_CHAR (next_month, 'yyyymm')
     INTO next_c_mon  -- '202004'
     FROM DUAL;
	
   IF v_tabname = 'ismg'
   THEN
      tabname := 'ISMGMISCCDR';

      FOR c IN (SELECT partition_name
                  FROM user_segments
                 WHERE segment_name = tabname
                   AND partition_name LIKE '%' || v_month || '%')
      LOOP
	  dbms_output.put_line('2..........');
         l_sql := ' ';
         l_sql :=
               'insert into ismgmisccdr_dup(cdrtype, roamtype, calltype, userclass, sequenceno,
       telnum, thirdtelnum, vregion, hregion, hmanage,
       spcode, othermanage, ismgid, fismgid, smsid,
       starttime, endtime, servicecode, fee, rentfee,
       status, priority, length, billingcycle, sourfilename,
       subscriberid, accountid, destfilename, errorcode,
       devicetype, specialtype, processtime, tariffflag,
       totaldiscount, total_free, rollback_flag, duration,
       imsi, region, usertype, discount_info, index_seq)  select cdrtype, roamtype, calltype, userclass, sequenceno,
       telnum, thirdtelnum, vregion, hregion, hmanage,
       spcode, othermanage, ismgid, fismgid, smsid,
       starttime, endtime, servicecode, fee, rentfee,
       status, priority, length, '||v_month||', sourfilename,
       subscriberid, accountid, destfilename, errorcode,
       devicetype, specialtype, processtime, tariffflag,
       totaldiscount, total_free, rollback_flag, duration,
       imsi, region, usertype, discount_info, telnum||rpad(spcode,6,'' '')||rpad(servicecode,6,'' '')||to_char(starttime,''yyyymm'')||thirdtelnum from ismgmisccdr  partition ('
            || c.partition_name
            || ') where starttime between to_date('''||v_month||''',''yyyymm'') and ADD_MONTHS (TO_DATE ('''||v_month||''', ''yyyymm''), 1)';
        
         EXECUTE IMMEDIATE l_sql;
		dbms_output.put_line('3..........');
         COMMIT;
      END LOOP;

      FOR c IN (SELECT partition_name
                  FROM user_segments
                 WHERE segment_name = tabname
                   AND partition_name LIKE '%' || next_c_mon || '%'
                   AND SUBSTR (partition_name, -2, 1) <= '06')
      LOOP
         l_sql :=
               'insert into ismgmisccdr_dup(cdrtype, roamtype, calltype, userclass, sequenceno,
       telnum, thirdtelnum, vregion, hregion, hmanage,
       spcode, othermanage, ismgid, fismgid, smsid,
       starttime, endtime, servicecode, fee, rentfee,
       status, priority, length, billingcycle, sourfilename,
       subscriberid, accountid, destfilename, errorcode,
       devicetype, specialtype, processtime, tariffflag,
       totaldiscount, total_free, rollback_flag, duration,
       imsi, region, usertype, discount_info, index_seq)  select cdrtype, roamtype, calltype, userclass, sequenceno,
       telnum, thirdtelnum, vregion, hregion, hmanage,
       spcode, othermanage, ismgid, fismgid, smsid,
       starttime, endtime, servicecode, fee, rentfee,
       status, priority, length, '||v_month||', sourfilename,
       subscriberid, accountid, destfilename, errorcode,
       devicetype, specialtype, processtime, tariffflag,
       totaldiscount, total_free, rollback_flag, duration,
       imsi, region, usertype, discount_info, telnum||rpad(spcode,6,'' '')||rpad(servicecode,6,'' '')||to_char(starttime,''yyyymm'')||thirdtelnum from ismgmisccdr  partition ('
            || c.partition_name
            || ') where starttime between to_date('''||v_month||''',''yyyymm'') and ADD_MONTHS (TO_DATE ('''||v_month||''', ''yyyymm''), 1)';

         EXECUTE IMMEDIATE l_sql;

         COMMIT;
		 dbms_output.put_line('4..........');
      END LOOP;

      l_sql := 'alter index i_ismgmisccdr_dup rebuild';

      --EXECUTE IMMEDIATE l_sql;

      DELETE FROM ismgmisccdr_dup a
            WHERE a.ROWID != (SELECT MAX (ROWID)
                                FROM ismgmisccdr_dup b
                               WHERE a.index_seq = b.index_seq);

      COMMIT;

      INSERT INTO stl_misc_check
                  (oper_no, CYCLE, hprov, sp_code, oper_code, chrg_type,
                   adjustflag, times, info_fee)
         SELECT   '001', billingcycle, '531', spcode, servicecode, calltype,
                  '1', COUNT (*), SUM (rentfee) * 100
             FROM ismgmisccdr_dup
            WHERE billingcycle = v_month
         GROUP BY billingcycle, spcode, servicecode, calltype;
       commit;
	   dbms_output.put_line('5..........');
   ELSIF v_tabname = 'wap'
   THEN
      tabname := 'WAPMISCCDR';

      FOR c IN (SELECT partition_name
                  FROM user_segments
                 WHERE segment_name = tabname
                   AND partition_name LIKE '%' || v_month || '%')
      LOOP
         l_sql := ' ';
         l_sql :=
               ' insert into wapmisccdr_dup( cdrtype, servicecode, servicetype, telnum, starttime,
       stoptime, service_attr, othertelnum, duration, hregion,
       vregion, gwid, calltype, netfee, monthfee,
       charge_type, sourfilename, billingcycle, processtime,
       intype, misc_id, errorcode, tariffflag, devicetype,
       total_free, subscriberid, accountid, rollback_flag,
       length, discount, region, usertype, discount_info,
       cdrid, index_seq)
       select  cdrtype, servicecode, servicetype, telnum, starttime,
       stoptime, service_attr, othertelnum, duration, hregion,
       vregion, gwid, calltype, netfee, monthfee,
       charge_type, sourfilename, '||v_month||', processtime,
       intype, misc_id, errorcode, tariffflag, devicetype,
       total_free, subscriberid, accountid, rollback_flag,
       length, discount, region, usertype, discount_info,
       cdrid,decode( cdrtype,''T'',to_char(starttime,''yyyymmhh24miss'')||telnum||cdrtype,
                                   to_char(starttime,''yyyymm'')||SERVICECODE||SERVICETYPE||telnum)  from wapmisccdr  partition ('
            || c.partition_name
            || ') where starttime between to_date('''||v_month||''',''yyyymm'') and ADD_MONTHS (TO_DATE ('''||v_month||''', ''yyyymm''), 1)';

         EXECUTE IMMEDIATE l_sql;
		dbms_output.put_line('10.........');
         COMMIT;
      END LOOP;

      FOR c IN (SELECT partition_name
                  FROM user_segments
                 WHERE segment_name = tabname
                   AND partition_name LIKE '%' || next_c_mon || '%'
                   AND SUBSTR (partition_name, -2, 1) <= '06')
      LOOP
         l_sql :=
               ' insert into wapmisccdr_dup( cdrtype, servicecode, servicetype, telnum, starttime,
       stoptime, service_attr, othertelnum, duration, hregion,
       vregion, gwid, calltype, netfee, monthfee,
       charge_type, sourfilename, billingcycle, processtime,
       intype, misc_id, errorcode, tariffflag, devicetype,
       total_free, subscriberid, accountid, rollback_flag,
       length, discount, region, usertype, discount_info,
       cdrid, index_seq)
       select  cdrtype, servicecode, servicetype, telnum, starttime,
       stoptime, service_attr, othertelnum, duration, hregion,
       vregion, gwid, calltype, netfee, monthfee,
       charge_type, sourfilename, '||v_month||', processtime,
       intype, misc_id, errorcode, tariffflag, devicetype,
       total_free, subscriberid, accountid, rollback_flag,
       length, discount, region, usertype, discount_info,
       cdrid,decode( cdrtype,''T'',to_char(starttime,''yyyymmhh24miss'')||telnum||cdrtype,
                                   to_char(starttime,''yyyymm'')||SERVICECODE||SERVICETYPE||telnum)  from wapmisccdr  partition ('
            || c.partition_name
            || ') where starttime between to_date('''||v_month||''',''yyyymm'') and ADD_MONTHS (TO_DATE ('''||v_month||''', ''yyyymm''), 1)';

         EXECUTE IMMEDIATE l_sql;

         COMMIT;
		 dbms_output.put_line('11.........');
      END LOOP;

      l_sql := 'alter index i_wapmisccdr_dup rebuild';

      --EXECUTE IMMEDIATE l_sql;

      DELETE FROM wapmisccdr_dup a
            WHERE a.ROWID != (SELECT MAX (ROWID)
                                FROM wapmisccdr_dup b
                               WHERE a.index_seq = b.index_seq);

      COMMIT;

      INSERT INTO stl_misc_check
                  (oper_no, CYCLE, hprov, sp_code, oper_code, chrg_type,
                   adjustflag, times, info_fee)
         SELECT   '003', billingcycle, '531', servicecode, servicetype,
                  charge_type, '1', COUNT (*), SUM (decode(cdrtype,'T',netfee,monthfee)) * 100
             FROM wapmisccdr_dup
            WHERE billingcycle = v_month AND sourfilename LIKE 'WAP_%'
         GROUP BY billingcycle, servicecode, servicetype, charge_type;
      commit;
	  dbms_output.put_line('12.........');
   ELSIF v_tabname = 'mms'
   THEN
      tabname := 'MMSMISCCDR';

      FOR c IN (SELECT partition_name
                  FROM user_segments
                 WHERE segment_name = tabname
                   AND partition_name LIKE '%' || v_month || '%')
      LOOP
         l_sql := ' ';
         l_sql :=
               ' insert into mmsmisccdr_dup (telnum, imsi, send_add, recv_add, cdrtype, roamtype,
       userclass, called_code, forward_telnum, infotype,
       apptype, forwardtype, chargetype, basicfee, inforfee,
       hregion, vregion, hmanage, carrytype, starttime,
       earlytime, storetime, mmlength, basicfee_s, inforfee_s,
       sendstatus, sendmmscid, recvmmscid, sp_code, app_code,
       oper_code, contenttype, mmtype, reporttype, add_hide,
       content_sw, mm_seq, errorcode, devicetype,
       sourfilename, processtime, total_free, destfilename,
       billingcycle, subscriberid, accountid, tariffflag,
       rollback_flag, disgprsflow, region, usertype,
       discount_info, recvtime, index_seq)
  select telnum, imsi, send_add, recv_add, cdrtype, roamtype,
       userclass, called_code, forward_telnum, infotype,
       apptype, forwardtype, chargetype, basicfee, inforfee,
       hregion, vregion, hmanage, carrytype, starttime,
       earlytime, storetime, mmlength, basicfee_s, inforfee_s,
       sendstatus, sendmmscid, recvmmscid, sp_code, app_code,
       oper_code, contenttype, mmtype, reporttype, add_hide,
       content_sw, mm_seq, errorcode, devicetype,
       sourfilename, processtime, total_free, destfilename,
       '||v_month||', subscriberid, accountid, tariffflag,
       rollback_flag, disgprsflow, region, usertype,
       discount_info, recvtime,to_char(starttime,''yyyymm'')||cdrtype||telnum||sp_code||oper_code||RECV_ADD   from mmsmisccdr  partition ('
            || c.partition_name
            || ') where starttime between to_date('''||v_month||''',''yyyymm'') and ADD_MONTHS (TO_DATE ('''||v_month||''', ''yyyymm''), 1)';

         EXECUTE IMMEDIATE l_sql;

         COMMIT;
		 dbms_output.put_line('20.........');
      END LOOP;

      FOR c IN (SELECT partition_name
                  FROM user_segments
                 WHERE segment_name = tabname
                   AND partition_name LIKE '%' || next_c_mon || '%'
                   AND SUBSTR (partition_name, -2, 1) <= '06')
      LOOP
         l_sql :=
               ' insert into mmsmisccdr_dup (telnum, imsi, send_add, recv_add, cdrtype, roamtype,
       userclass, called_code, forward_telnum, infotype,
       apptype, forwardtype, chargetype, basicfee, inforfee,
       hregion, vregion, hmanage, carrytype, starttime,
       earlytime, storetime, mmlength, basicfee_s, inforfee_s,
       sendstatus, sendmmscid, recvmmscid, sp_code, app_code,
       oper_code, contenttype, mmtype, reporttype, add_hide,
       content_sw, mm_seq, errorcode, devicetype,
       sourfilename, processtime, total_free, destfilename,
       billingcycle, subscriberid, accountid, tariffflag,
       rollback_flag, disgprsflow, region, usertype,
       discount_info, recvtime, index_seq)
  select telnum, imsi, send_add, recv_add, cdrtype, roamtype,
       userclass, called_code, forward_telnum, infotype,
       apptype, forwardtype, chargetype, basicfee, inforfee,
       hregion, vregion, hmanage, carrytype, starttime,
       earlytime, storetime, mmlength, basicfee_s, inforfee_s,
       sendstatus, sendmmscid, recvmmscid, sp_code, app_code,
       oper_code, contenttype, mmtype, reporttype, add_hide,
       content_sw, mm_seq, errorcode, devicetype,
       sourfilename, processtime, total_free, destfilename,
       '||v_month||', subscriberid, accountid, tariffflag,
       rollback_flag, disgprsflow, region, usertype,
       discount_info, recvtime,to_char(starttime,''yyyymm'')||cdrtype||telnum||sp_code||oper_code||RECV_ADD   from mmsmisccdr  partition ('
            || c.partition_name
            || ') where starttime between to_date('''||v_month||''',''yyyymm'') and ADD_MONTHS (TO_DATE ('''||v_month||''', ''yyyymm''), 1)';

         EXECUTE IMMEDIATE l_sql;

         COMMIT;
		 dbms_output.put_line('21.........');
      END LOOP;

      l_sql := 'alter index i_mmsmisc_dup rebuild';

      --EXECUTE IMMEDIATE l_sql;

      DELETE FROM mmsmisccdr_dup a
            WHERE a.ROWID != (SELECT MAX (ROWID)
                                FROM mmsmisccdr_dup b
                               WHERE a.index_seq = b.index_seq);

      COMMIT;

      INSERT INTO stl_misc_check
                  (oper_no, CYCLE, hprov, sp_code, oper_code, chrg_type,
                   adjustflag, times, info_fee)
         SELECT   '002', billingcycle, '531', sp_code, oper_code, chargetype,
                  '1', COUNT (*), SUM (inforfee) * 100
             FROM mmsmisccdr_dup
            WHERE billingcycle = v_month AND SUBSTR (sp_code, 2, 2) = '15'
         GROUP BY billingcycle, sp_code, oper_code, chargetype;

      COMMIT;
	  dbms_output.put_line('22.........');
   END IF;
END;
//




  CREATE OR REPLACE PROCEDURE "AP_MODIFY_PASSWORD_LOG" (I_DB_AUTH_ID        VARCHAR2,
                                                   I_OLD_USER_PASSWORD VARCHAR2,
                                                   I_NEW_USER_PASSWORD VARCHAR2,
                                                   I_USER_CODE         VARCHAR2) AS
  /*******************************************************************************
  Author:   wx145563
  create:   2013-01-09
  --------------------------------------------------------------------------------
     I_DB_AUTH_ID            系统认证ID
     I_OLD_USER_PASSWORD     用户原口令
     I_NEW_USER_PASSWORD     用户新口令
     I_USER_CODE             用户编码
  *******************************************************************************/

  V_USER_NAME VARCHAR2(30);
  V_LOGIN_IP  VARCHAR2(30);

BEGIN
  SELECT USER_NAME
    INTO V_USER_NAME
    FROM DB_AUTH_DEF
   WHERE DB_AUTH_ID = I_DB_AUTH_ID;
  SELECT SYS_CONTEXT('userenv', 'IP_ADDRESS') IPADDR
    INTO V_LOGIN_IP
    FROM DUAL;

  INSERT INTO BOSS_PASS_CHANGE_LOG
    (DB_AUTH_ID, --系统认证ID
     USER_NAME, --用户名称
     OLD_USER_PASSWORD, --用户原口令
     NEW_USER_PASSWORD, --用户新口令
     PASSCHANGETIME, --用户口令的变更时间
     USER_CODE, --用户编码
     LOGIN_IP) --变更的IP
  VALUES
    (I_DB_AUTH_ID,
     V_USER_NAME,
     I_OLD_USER_PASSWORD,
     I_NEW_USER_PASSWORD,
     SYSDATE,
     I_USER_CODE,
     V_LOGIN_IP);
  COMMIT;
  dbms_output.put_line('1..........');
END;
//




  CREATE OR REPLACE PROCEDURE "AP_REP" 
IS
   rpdate   DATE;
BEGIN
   SELECT SYSDATE
     INTO rpdate
     FROM DUAL;

   DBMS_OUTPUT.put_line ('1..........');
EXCEPTION
   WHEN OTHERS
   THEN
      DBMS_OUTPUT.put_line ('hello ');
END;  
//   





  CREATE OR REPLACE PROCEDURE "AP_RTCC_ROLLBACK" is
v_sql varchar2(2048);
v_count number;
begin
 for c1 in ( select a.*,rowid crow from RTCCVPMNCDR a 
             where starttime > trunc(sysdate) 
             and partialflag in ('0','00','000','1','01','001') and res9 is null 
             ) 
       loop 
     v_sql :='
           select count(*) from 
           gsm'||c1.hregion||'_'||c1.billingcycle||'
           where telnum = '''||c1.telnum||'''
           and othertelnum = '''||c1.othertelnum||'''
           and starttime = '''||c1.starttime||'''
           ';
     execute immediate v_sql into v_count ;

     update RTCCVPMNCDR
      set res9 = v_count 
      where rowid = c1.crow;
      dbms_output.put_line('1..........');
 end loop ;
 commit;
 end ;
 //
 
 


  CREATE OR REPLACE PROCEDURE "AP_SET_HIGHALERTLEVEL" (an_month number,an_sdrRate number)
as
vd_begin date;
vd_end   date;
begin

	vd_begin := to_date(an_month||'01000000','yyyymmddhh24miss');
	vd_end   := to_date(an_month||'01235959','yyyymmddhh24miss');
	vd_end   := last_day(vd_end);

	update code_dict set code=to_char(an_sdrRate) where catalog = 'T_SDR_RATE';
  if sql%rowcount=0 then
	dbms_output.put_line('1.........');
     insert into code_dict (catalog,code,name,note)
        values ('T_SDR_RATE',to_char(an_sdrRate),'SDR汇率','') ;
  end if;

	INSERT INTO highalertlevel
         (NETTYPE,USERTYPE,ALARMLEVELNO,LOWERLIMIT,UPPERLIMIT,
          ALARMLEVELNOTE,APPLY_TIME,EXPIRE_TIME,SDRRATE)
    VALUES (	'G','231',1,(an_sdrRate * 50),(an_sdrRate * 150),'低级告警',vd_begin,vd_end,an_sdrRate);

	INSERT INTO highalertlevel
         (NETTYPE,USERTYPE,ALARMLEVELNO,LOWERLIMIT,UPPERLIMIT,
          ALARMLEVELNOTE,APPLY_TIME,EXPIRE_TIME,SDRRATE)
    VALUES (	'G','231',2,(an_sdrRate * 150) ,(an_sdrRate * 500),'中级告警',vd_begin,vd_end,an_sdrRate);

	INSERT INTO highalertlevel
         (NETTYPE,USERTYPE,ALARMLEVELNO,LOWERLIMIT,UPPERLIMIT,
          ALARMLEVELNOTE,APPLY_TIME,EXPIRE_TIME,SDRRATE)
    VALUES (	'G','231',3,(an_sdrRate * 500),(an_sdrRate * 10000) ,'高级告警',vd_begin,	vd_end,an_sdrRate);
	
	dbms_output.put_line('2..........');
	return ;

end ;
//




  CREATE OR REPLACE PROCEDURE "AP_SOX_CHECK" (App_Username  IN VARCHAR2,
                                         Logon_Result OUT NUMBER)
as
  --登录结果 1:登录成功 995:禁止登录 994:重复登录 993:数据库异常
  vSessionId    VARCHAR2(20);
  vCurSessionId VARCHAR2(20);
  nActiveCnt  NUMBER(1);
  dLogonTime  DATE;
  dLogoutTime DATE;
  dCurLogonTime DATE;
  nLastCallEt NUMBER(5);
  nUNIQUEID   NUMBER(1);
begin
     Logon_Result := 1; --登录成功
end AP_SOX_CHECK;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDAILYDONGMAN" is
  --变量定义
  v_mapping_id NUMBER(10);
  v_date     VARCHAR2(14);
  v_date_1     VARCHAR2(14);
  v_date_2     VARCHAR2(14);
  v_filename VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_lastfilename_2 VARCHAR2(40);
  v_info           VARCHAR2(200);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

BEGIN
  dbms_output.put_line('ap_spdailygame()开始运行...');
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_date FROM dual;
  SELECT to_char(SYSDATE - 2, 'yyyymmdd') INTO v_date_1 FROM dual;
  SELECT to_char(SYSDATE - 3, 'yyyymmdd') INTO v_date_2 FROM dual;

  -------------------以下处理手机游戏内容编码局数据--------------------------
  dbms_output.put_line('ap_spdailygame()装入内容编码局数据...');
  v_mapping_id := 0;
  v_filename   := 'DAILYGAME' || v_date || '.txt';
  v_lastfilename_1   := 'DAILYGAME' || v_date_1 || '.txt';
  v_lastfilename_2   := 'DAILYGAME' || v_date_2 || '.txt';
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 1
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的手机游戏内容编码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载局数据。清先查看分拣日志。');
  END IF;
  --删除之前的数据
  delete from DAILYGAME_DATA
  where sourfilename = v_lastfilename_2;
  
  commit;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT   SERV_TYPE,
         SPCODE,
         OPERATOR_CODE,
         OPERATOR_NAME,
         VALID_DATE,
         EXPIRE_DATE
      FROM DAILYGAME_DATA
     WHERE sourfilename = v_filename
    MINUS
    SELECT SERV_TYPE,
         SPCODE,
         OPERATOR_CODE,
         OPERATOR_NAME,
         VALID_DATE,
         EXPIRE_DATE
      FROM DAILYGAME_DATA
     WHERE sourfilename = v_lastfilename_1)
  LOOP
     CASE
      WHEN c1.serv_type = '008007' THEN
	  dbms_output.put_line('1..........');
        v_mapping_id   := 176; --手机游戏内容编码局数据             
        v_mapping_sour := c1.spcode || ',' || c1.operator_code;  -- '3,4'
        v_mapping_dest := c1.operator_name;   -- 5
        v_sourfilename := 'DAILYGAME' || v_date || '.txt';
      
        
    
          
    /*------------------20110121-----------------------------*/
    
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
    END CASE;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         --AND applytime = to_date(c1.valid_date,'yyyymmdd')
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('2..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           --AND applytime = to_date(c1.valid_date,'yyyymmdd')
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);        
      END IF;
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
        commit;
		dbms_output.put_line('3..........');
      EXCEPTION
        WHEN dup_val_on_index THEN
          NULL;
      END;
    END IF;
    commit;
  END LOOP;    
end ap_spdailydongman;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDAILYGAME" is
  --变量定义
  v_mapping_id NUMBER(10);
  v_date     VARCHAR2(14);
  v_date_1     VARCHAR2(14);
  v_date_2     VARCHAR2(14);
  v_filename VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_lastfilename_2 VARCHAR2(40);
  v_info           VARCHAR2(200);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

BEGIN
  dbms_output.put_line('ap_spdailygame()开始运行...');
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_date FROM dual;
  SELECT to_char(SYSDATE - 2, 'yyyymmdd') INTO v_date_1 FROM dual;
  SELECT to_char(SYSDATE - 3, 'yyyymmdd') INTO v_date_2 FROM dual;

  -------------------以下处理手机游戏内容编码局数据--------------------------
  dbms_output.put_line('ap_spdailygame()装入内容编码局数据...');
  v_mapping_id := 0;
  v_filename   := 'DAILYGAME' || v_date || '.txt';
  v_lastfilename_1   := 'DAILYGAME' || v_date_1 || '.txt';
  v_lastfilename_2   := 'DAILYGAME' || v_date_2 || '.txt';
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 1
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的手机游戏内容编码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载局数据。清先查看分拣日志。');
  END IF;
  --删除之前的数据
  delete from DAILYGAME_DATA
  where sourfilename = v_lastfilename_2;
  
  commit;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT   SERV_TYPE,
         SPCODE,
         OPERATOR_CODE,
         OPERATOR_NAME,
         VALID_DATE,
         EXPIRE_DATE
      FROM DAILYGAME_DATA
     WHERE sourfilename = v_filename
    MINUS
    SELECT SERV_TYPE,
         SPCODE,
         OPERATOR_CODE,
         OPERATOR_NAME,
         VALID_DATE,
         EXPIRE_DATE
      FROM DAILYGAME_DATA
     WHERE sourfilename = v_lastfilename_1)
  LOOP
  dbms_output.put_line('1..........');
     CASE
      WHEN c1.serv_type = '008007' THEN
        v_mapping_id   := 176; --手机游戏内容编码局数据             
        v_mapping_sour := c1.spcode || ',' || c1.operator_code;  -- '3,4'
        v_mapping_dest := c1.operator_name;                      -- 5
        v_sourfilename := 'DAILYGAME' || v_date || '.txt';
      
        
    
          
    /*------------------20110121-----------------------------*/
    
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
    END CASE;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         --AND applytime = to_date(c1.valid_date,'yyyymmdd')
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           --AND applytime = to_date(c1.valid_date,'yyyymmdd')
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);        
		dbms_output.put_line('2..........');
      END IF;
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           --to_date(c1.valid_date,'yyyymmdd'),
           c1.valid_date,
           c1.expire_date,
           --to_date(c1.expire_date,'yyyymmdd'),
           v_mapping_id,
           v_sourfilename);
        commit;
		dbms_output.put_line('3..........');
      EXCEPTION
        WHEN dup_val_on_index THEN
          NULL;
		  dbms_output.put_line('??????????');
      END;
    END IF;
    commit;
  END LOOP;    
end ap_spdailygame;
//




CREATE OR REPLACE FUNCTION "AF_GETSTR" (str VARCHAR2, v_split VARCHAR2, pos NUMBER)
  RETURN VARCHAR2 IS
  v_arstr    VARCHAR2(400);
  v_str      VARCHAR2(300);
  v_last_pos NUMBER;
  v_pos      NUMBER;
BEGIN
  v_str := '';
  IF substr(str, length(str), 1) <> v_split
  THEN
    v_arstr := str || v_split;
  ELSE
    v_arstr := str;
  END IF;
  IF pos != 1
  THEN
    v_last_pos := instr(v_arstr, v_split, 1, pos - 1);
    v_pos      := instr(v_arstr, v_split, 1, pos);
    v_str      := substr(v_arstr, v_last_pos + 1, v_pos - v_last_pos - 1);
  ELSE
    v_pos := instr(v_arstr, v_split, 1, pos);
    v_str := substr(v_arstr, 1, v_pos - 1);
  END IF;

  RETURN v_str;
END;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD" IS
  --变量定义
  v_mapping_id NUMBER(10);

  v_date     VARCHAR2(14);
  v_filename VARCHAR2(40);
  ---modify 20080118
  v_filename1    VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

  v_conent_flag NUMBER(2); ---用于判断是否有局数据不用同步,但是内容计费需要同步局数据的情况.
  v_post_flag varchar2(32);

BEGIN
  v_content_id  := 203;
  v_conent_flag := 0;
  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');
  ---20080218 modify
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_date FROM dual;
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename LIKE v_filename
    MINUS
    SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1)
  LOOP
  dbms_output.put_line('1..........');
    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 60; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 77; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 60; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 79; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 73; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.dev_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.serv_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 81; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 66; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 62; --WAP
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 75; --手机动画
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 68; --通用下载
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 90; --无线音乐
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.dev_code;
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000204' THEN
        v_mapping_id   := 110; --彩铃
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',1,1|85';
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
        /*------------------20081008-----------------------------*/
      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM ,目前没有稽核sp数据

      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)
    /*
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 60; --国际问候短语(os)
            v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
            v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
            v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
          */
      WHEN c1.serv_type = '008007' THEN
        v_mapping_id := -1; --手机游戏 ,目前没有稽核sp数据

      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通 ,目前没有稽核sp数据

      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,MRBT,目前没有稽核sp数据
       /*cuibiao  20091223注释
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1; */


      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := 93; --Mobile Market
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 66; --农信通彩信(CIMM)
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 95; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 97; --手机阅读
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      --cuibiao add 20100115 stm
      WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 102; --手机电视
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 104; --手机动漫
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      --cuibiao add 20100621 手机导航
      WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航 暂时不计费

      WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机POC业务暂时不处理

      WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包业务暂时不处理

      WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云业务暂时不处理

      WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;
        
      WHEN c1.serv_type = '090532' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1;
      --cuibiao add 20170405 不同步sp局数据
      WHEN c1.serv_type = '008013' THEN
        v_mapping_id := -1;
      --shaojiwei add 20170707 不同步sp局数据
      WHEN c1.serv_type = '0001' THEN
        v_mapping_id := -1;
      WHEN c1.serv_type = '0002' THEN
        v_mapping_id := -1;
    /*------------------20081008-----------------------------*/
           WHEN c1.serv_type = '0004' THEN
        v_mapping_id := -1;
           WHEN c1.serv_type = '0005' THEN
        v_mapping_id := -1;
           WHEN c1.serv_type = '0006' THEN
        v_mapping_id := -1;
        ----modify 20180527 hxy

      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  --dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']');

   v_post_flag :='mapping_list' ;
    IF v_mapping_id <> -1
    THEN
	dbms_output.put_line('2.........');
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);
		dbms_output.put_line('3.........');
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.serv_code);
      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        ---20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;
    END IF;

  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;
  dbms_output.put_line('4.........');

  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
  --20080118 modify
  v_filename1 := 'MCBBJ_01_SP_OPER_' || v_date ;

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename || '%' OR filename LIKE v_filename1 || '%';
      ---mayong 20090615---OR filename LIKE v_filename1 || '%'; --20080118 modify
  --WHERE filename LIKE v_filename;
  IF v_count < 1
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     ---20090615----WHERE (sourfilename = v_filename OR sourfilename = v_filename1) --20080118 modify
   ---WHERE sourfilename = v_filename ---modify by mayong 20090615 暂不处理01文件
    --WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
         WHERE (sourfilename = v_filename OR substr(sourfilename,1,25) = v_filename1)  --恢复01文件处理20170518
                and valid_date <>  nvl(expire_date,to_date('20370101','yyyymmdd'))
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     /* 20090615  WHERE (sourfilename = v_lastfilename_0 OR
           sourfilename = v_lastfilename_1) --and serv_type=c1.serv_type*/
    --WHERE sourfilename = v_lastfilename_0   ---modify by mayong 20090615  暂不处理01文件
   WHERE (sourfilename = v_lastfilename_0 OR
           substr(sourfilename,1,25)||'.txt' = v_lastfilename_1)  --恢复01文件处理20170518
    )
  LOOP
  dbms_output.put_line('5.........');
    v_conent_flag := 0; ---内容计费同步标志清0
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20081021
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20090116
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20081021
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        if  c1.sp_code in ('699213','699216','699218','699214','699211','699217','699215','699212') then
            v_content_mapping_sour := '51,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '51,' || c1.fee / 1000;
        else
            v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '14,' || c1.fee / 1000;
        end if ;

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        v_conent_flag          := 1;
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1; ---设置标志,说明当前纪录有局数据不同步,内容计费要同步的数据
        v_content_mapping_sour := '20,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '20,' || c1.fee / 1000;

      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐不需要进行同步
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
        v_mapping_id   := -1;

        v_conent_flag  := 1;---恢复同步203值影射
        v_content_mapping_sour := '25,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '25,' || c1.fee / 1000; ---addby hxy20170518


      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃
    /*------------------20081008-----------------------------*/

      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM,目前未稽核，不需要同步

      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)

    /*
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 61; --国际问候短语(os) ,目前未稽核，不需要同步
            v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
            v_mapping_dest := c1.operator_name || ',' ||
                              lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                              ',1|' || c1.out_prop || '|1';
            v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      */
      WHEN c1.serv_type = '008007' THEN
        --cuibiao 20100322 修改 只是同步局数据到 mapping_list
        /*v_mapping_id := -1; --手机游戏,目前未稽核，不需要同步 */
	      v_mapping_id   := 87; --手机地图
	      v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
	      v_mapping_dest := c1.operator_name || ',' ||','||
                  lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 ||
                  ',' || c1.out_prop;
	      v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

         ---内容计费同步局数据使用 cuibiao 20100705添加
        v_conent_flag          := 1;
        v_content_mapping_sour := '28,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '28,' || c1.fee / 1000;



      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通,目前未稽核，不需要同步

      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,目前未稽核，不需要同步
       /* cuibiao  20091223注释
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1;  */

      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := -1; --Mobile Market 业务局数据只是查询用,直接入查询库


        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '57,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '57,' || c1.fee / 1000;


      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 67; --农信通彩信(CIMM)
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 96; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '56,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '56,' || c1.fee / 1000;

       WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 98; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---add by cuibiao 20091217
        v_conent_flag          := 1;
        v_content_mapping_sour := '60,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '60,' || c1.fee / 1000;

       --cuibiao 20100115 stm
       WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 103;
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '53,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '53,' || c1.fee / 1000;
       --wangqi 20111213 手机动漫
       WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 105;
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '36,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '36,' || c1.fee / 1000;
       --cuibiao 20100621 手机导航
       WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航暂不计费
        v_conent_flag:= 1;
        v_content_mapping_sour := '65,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '65,' || c1.fee / 1000;

       --cuibiao 20101221
       WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机poc

       WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包

       WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云

       WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;
        
       WHEN c1.serv_type = '090532' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;
        --begin add cuibiao 20170125
        v_conent_flag:= 1;
        v_content_mapping_sour := '81,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '81,' || c1.fee / 1000;
        --end add cuibiao 20170125



       WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1;
        v_conent_flag          := 1;
        v_content_mapping_sour := '80,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '80,' || c1.fee / 1000;
        /*
        v_mapping_id   := 175; --手机poc
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '39,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '39,' || c1.fee / 1000;


        */
        --cuibiao add 20170405
        WHEN c1.serv_type = '008013' THEN
        v_mapping_id := -1; --互联网计费 不同步业务编码局数据
        v_conent_flag:= 1;
        v_content_mapping_sour := '82,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '82,' || c1.fee / 1000;
        --shaojiwei add 20170707
        WHEN c1.serv_type = '0001' THEN
        v_mapping_id := -1; --任我看 不同步业务编码局数据
        v_conent_flag:= 1;
        v_content_mapping_sour := '82,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '82,' || c1.fee / 1000;
        WHEN c1.serv_type = '0002' THEN
        v_mapping_id := -1; --任我用 不同步业务编码局数据
        v_conent_flag:= 1;
        v_content_mapping_sour := '82,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '82,' || c1.fee / 1000;


        WHEN c1.serv_type = '0004' THEN
        v_mapping_id := -1;
        v_conent_flag:= 1;
        v_content_mapping_sour := '82,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '82,' || c1.fee / 1000;
        WHEN c1.serv_type = '0005' THEN
        v_mapping_id := -1;
        v_conent_flag:= 1;
        v_content_mapping_sour := '82,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '82,' || c1.fee / 1000;
        WHEN c1.serv_type = '0006' THEN
        v_mapping_id := -1;
        v_conent_flag:= 1;
        v_content_mapping_sour := '82,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '82,' || c1.fee / 1000;
    /*------------------20081008-----------------------------*/
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;

    v_post_flag :='mapping_list|业务' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('6.........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.operator_code);

      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        --20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;

    END IF;

   v_post_flag :='mapping_list|内容计费' ;
    ----内容计费局数据同步
    IF v_conent_flag = 1
    THEN
	dbms_output.put_line('7.........');
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
		dbms_output.put_line('8.........');
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;

        --插入新数据
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
        EXCEPTION
          --WHEN OTHERS THEN
          --20080118 modify
          WHEN dup_val_on_index THEN
            NULL;
        END;

      END IF;
    END IF;
  END LOOP;

  commit;
   v_post_flag :='mapping_list|异常处理0' ;
	--特殊处理营业和计费局数据bizcode不一致的数据
	   --sp      营业bizcode  信用bizcode
     --901312   38            04
		 --901317   97            04
     --801084   97            05
  --先删除主键重复的，再更新
    ---add by x00231576 begin
    --901312
    for cur in (
                select * from mapping_list a where a.mapping_id = '203'  and af_getstr(mapping_sour, ',', 2) = '901312'
                and af_getstr(mapping_sour, ',', 1) = '04' /*and (expiretime > SYSDATE OR expiretime IS NULL)*/
               ) loop
     --如果这一条已经有38的那一条直接删除掉38的这一条
	 dbms_output.put_line('9.........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '38,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '38,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --901317
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                      and af_getstr(mapping_sour, ',', 2) = '901317'
                      and af_getstr(mapping_sour, ',', 3) not in
                       (select af_getstr(mapping_sour, ',', 3)
                          from mapping_list
                         where af_getstr(mapping_sour, ',', 1) = '97'
                           and af_getstr(mapping_sour, ',', 2) = '901317')
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('10.........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --915982
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                      and af_getstr(mapping_sour, ',', 2) = '915982'
                      and af_getstr(mapping_sour, ',', 1)='04'
                      and af_getstr(mapping_sour, ',', 3) not in
                       (select af_getstr(mapping_sour, ',', 3)
                          from mapping_list
                         where af_getstr(mapping_sour, ',', 1) = '97'
                           and af_getstr(mapping_sour, ',', 2) = '915982')
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('11.........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --801084
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                         and af_getstr(mapping_sour, ',', 2) = '801084'
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('12.........');
     v_mapping_sour:=replace(cur.mapping_sour, '05,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '05,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    ---add by x00231576 end

     update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '38,'),
            mapping_dest = replace(mapping_dest, '04,', '38,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901312'
        and (expiretime > SYSDATE OR expiretime IS NULL);
		 commit;
		 v_post_flag :='mapping_list|异常处理1' ;
		  update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901317'
        and af_getstr(mapping_sour, ',', 3) not in
         (select af_getstr(mapping_sour, ',', 3)
            from mapping_list
           where af_getstr(mapping_sour, ',', 1) = '97'
             and af_getstr(mapping_sour, ',', 2) = '901317');
		 commit;
           update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
        where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '915982'
        and af_getstr(mapping_sour, ',', 1)='04'
        and af_getstr(mapping_sour, ',', 3) not in
         (select af_getstr(mapping_sour, ',', 3)
            from mapping_list
           where af_getstr(mapping_sour, ',', 1) = '97'
             and af_getstr(mapping_sour, ',', 2) = '915982');
     commit;
		 v_post_flag :='mapping_list|异常处理2' ;
		  update mapping_list
        set mapping_sour = replace(mapping_sour, '05,', '97,'),
            mapping_dest = replace(mapping_dest, '05,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '801084'
        and (expiretime > SYSDATE OR expiretime IS NULL);
		 commit;

	   v_post_flag :='mapping_list|异常处理3' ;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);

  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(v_post_flag||':'||v_mapping_id||'|'||v_content_mapping_sour||'|'||v_content_mapping_dest||'|'||v_content_id);
    dbms_output.put_line(': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(': ' || ' SP业务代码装载数据失败！');
    END IF;

    ROLLBACK;

END ap_spdataload;
//





  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD2" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。
  同时维护内容计费2期的局数据
  
  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。
  
  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ----------------------------------------------------------*/

  --变量定义

  v_mapping_id NUMBER(10);

  v_date         VARCHAR2(14);
  v_yestoday     VARCHAR2(14);
  v_filename     VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

BEGIN
  v_content_id := 203;
  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');

  SELECT to_char(SYSDATE, 'yyyymmdd') INTO v_date FROM dual;
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_yestoday FROM dual;

  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  -- v_oldfilename := 'MCBBJ____SP_INFO_'||v_yestoday||'.txt';
  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename LIKE v_filename
    MINUS
    SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1)
  LOOP
  
    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
	dbms_output.put_line('1..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 60; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 77; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 60; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 79; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 73; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.dev_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.serv_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 81; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 66; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 62; --WAP
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 75; --手机动画
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 68; --通用下载
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 90; --无线音乐
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.dev_code;
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id   := 110; --彩铃
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',1,1|85';
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
      
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  --dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']');
  
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('2..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);
      END IF;
      dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
                           c1.serv_code);
      --插入新数据
      INSERT INTO mapping_list
        (mapping_sour,
         mapping_dest,
         applytime,
         expiretime,
         mapping_id,
         note)
      VALUES
        (v_mapping_sour,
         v_mapping_dest,
         c1.valid_date,
         c1.expire_date,
         v_mapping_id,
         v_sourfilename);
    END IF;
  
  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;
  dbms_output.put_line('3..........');

  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_OPER_' || v_date || '.txt';
  --v_oldfilename := 'MCBBJ____SP_OPER_'||v_yestoday||'.txt';

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_yestoday ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1 --and serv_type=c1.serv_type
    )
  LOOP
  dbms_output.put_line('4..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_content_mapping_sour := '21,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '21,' || c1.fee / 1000;
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '14,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图
    
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃
    
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
    
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('5..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);
      END IF;
      dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
                           c1.operator_code);
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
		
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;
        --插入新数据
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_mapping_sour,
             v_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_mapping_id,
             v_sourfilename);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
      
      END IF;
    END IF;
  END LOOP;
  --文件名插入处理日志
  dbms_output.put_line('6..........');
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);

  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(SYSDATE || ': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(SYSDATE || ': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(SYSDATE || ': ' || ' SP业务代码装载数据失败！');
    END IF;
  
    ROLLBACK;
  
END ap_spdataload2;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD_ALL" (v_check_change in char) is
/*---------------------------------------------------------
用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表全量导入mapping_list

参数:v_check_change, ='0', 不检查与最昨天数据的变化，所有数据全量导入。
                     ='1', 检查与最昨天数据的变化，只全量导入有变动的业务类别数据

涉及的表：
SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。

业务类别编码	描述
001102	梦网短信               "SMS"       60  61
001103	一点结算梦网短信       "CM"        77  78
001104	农信通                 "CI"        ----
001122	捐款短信               "CHILD"     79  80
001123	流媒体                 "STREAM"    73  74
001322	手机邮箱               "EMAIL"     81  82
000103	梦网彩信               "MMS"       66  67
000104	WAP                    "WAP"       62  63
000105	手机动画               "FLASH"     75  76
000005	通用下载               "KJAVA"     68  69
008002	手机地图               "MAP"       ----
008001	无线音乐               "MUSIC"     90  91
000204	彩铃                   "CRING"     110 --
---------------------------------------------------------*/

v_mapping_id   number (10);

v_date         varchar2(14);
v_yestoday     varchar2(14);
v_filename     varchar2(40);
v_oldfilename     varchar2(40);
v_info         varchar2(200);
v_sql          varchar2(400);
v_changed      BOOLEAN;


EXCEPTION_UNKNOWN_SERVTYPE EXCEPTION;
/*--------------------------------------------------------*/   
-- p_filename = v_filename
function if_changed_spcode (p_serv_type in number, p_filename in varchar2, p_oldfilename in varchar2) RETURN BOOLEAN IS
    v_num      number (10);
begin
    if v_check_change = '0' then
        return TRUE;
    end if;
    select count(*) into v_num from 
    (    
     select sp_code,SERV_CODE,to_char(VALID_DATE,'yyyymmdd'),serv_type from spcodecdr where sourfilename like p_filename and serv_type=p_serv_type
     minus select sp_code,SERV_CODE,to_char(VALID_DATE,'yyyymmdd'),serv_type from spcodecdr where sourfilename like p_oldfilename and serv_type=p_serv_type
     union
     select sp_code,SERV_CODE,to_char(VALID_DATE,'yyyymmdd'),serv_type from spcodecdr where sourfilename like p_oldfilename and serv_type=p_serv_type
     minus select sp_code,SERV_CODE,to_char(VALID_DATE,'yyyymmdd'),serv_type from spcodecdr where sourfilename like p_filename and serv_type=p_serv_type
    );
    if v_num >0  then 
        dbms_output.put_line(p_serv_type||' is ok');
        return TRUE;
    else
        return FALSE;
    end if;
end if_changed_spcode;


function if_changed_spoper (p_serv_type in number, p_filename in varchar2, p_oldfilename in varchar2) RETURN BOOLEAN IS
    v_num      number (10);
begin
    if v_check_change = '0' then
        return TRUE;
    end if;
    select count(*) into v_num from 
    (   
     
     select sp_code,operator_code,to_char(VALID_DATE,'yyyymmdd'),serv_type from spopercdr where sourfilename like p_filename and serv_type=p_serv_type
     minus select sp_code,operator_code,to_char(VALID_DATE,'yyyymmdd'),serv_type from spopercdr where sourfilename like p_oldfilename and serv_type=p_serv_type
     union
     select sp_code,operator_code,to_char(VALID_DATE,'yyyymmdd'),serv_type from spopercdr where sourfilename like p_oldfilename and serv_type=p_serv_type
     minus select sp_code,operator_code,to_char(VALID_DATE,'yyyymmdd'),serv_type from spopercdr where sourfilename like p_filename and serv_type=p_serv_type
    );
    if v_num >0  then 
        dbms_output.put_line(p_serv_type||' is ok');
        return TRUE;
    else
        return FALSE;
    end if;
end if_changed_spoper;

begin
    select to_char(sysdate-1,'yyyymmdd') into v_date from dual;
    select to_char(sysdate-2,'yyyymmdd') into v_yestoday from dual;
    v_mapping_id :=0;
    v_filename := 'MCBBJ____SP_INFO_'||v_date||'.txt';
    v_oldfilename := 'MCBBJ____SP_INFO_'||v_yestoday||'.txt';
    for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
	dbms_output.put_line('1..........');
        case
        when c1.serv_type='001102' then v_mapping_id:=60;  --梦网短信  
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||serv_code, ''1,''||sp_name||'',''||prov_code, valid_date, 
            expire_date,' || v_mapping_id || ', sourfilename from spcodecdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='001103' then v_mapping_id:=77;  --一点结算梦网短信 
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);   
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||serv_code, ''1,''||sp_name||'',''||prov_code||'','', valid_date, 
            expire_date,' || v_mapping_id || ', sourfilename from spcodecdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通                                                              
        when c1.serv_type='001122' then v_mapping_id:=79;  --捐款短信    
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);                                                        
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||serv_code, ''1,''||sp_name||'',''||prov_code, valid_date, 
            expire_date,' || v_mapping_id || ', sourfilename from spcodecdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='001123' then v_mapping_id:=73;  --流媒体      
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);                                                        
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||dev_code, sp_name||'',''||prov_code||'',''||serv_code, valid_date, 
            expire_date,' || v_mapping_id || ', sourfilename from spcodecdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='001322' then v_mapping_id:=81;  --手机邮箱
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);  
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||serv_code, ''1,''||sp_name||'',''||prov_code||'','', valid_date, 
            expire_date,' || v_mapping_id || ', sourfilename from spcodecdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000103' then v_mapping_id:=66;  --梦网彩信
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||serv_code, ''1,''||sp_name||'',''||prov_code, valid_date, 
            expire_date,' || v_mapping_id || ', sourfilename from spcodecdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000104' then v_mapping_id:=62;  --WAP 
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code, ''1,''||sp_name||'','', valid_date, expire_date,' || v_mapping_id || 
            ', sourfilename from spcodecdr where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000105' then v_mapping_id:=75;  --手机动画 
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code, ''1,''||sp_name||'','', valid_date, expire_date,' || v_mapping_id || 
            ', sourfilename from spcodecdr where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';                                                           
        when c1.serv_type='000005' then v_mapping_id:=68;  --通用下载    
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code, ''1,''||sp_name||'','', valid_date, expire_date,' || v_mapping_id || 
            ', sourfilename from spcodecdr where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';                                                      
        when c1.serv_type='008002' then v_mapping_id:=-1;  --手机地图                                                            
        when c1.serv_type='008001' then v_mapping_id:=90;  --无线音乐 
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code, sp_name||'',''||prov_code||'',''||dev_code, valid_date, expire_date,' || v_mapping_id || 
            ', sourfilename from spcodecdr where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000204' then v_mapping_id:=110;  --彩铃      
            v_changed:=if_changed_spcode(c1.serv_type,v_filename,v_oldfilename);       
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code, sp_name||'',1,1|85'', valid_date, expire_date,'||v_mapping_id||
            ', sourfilename from spcodecdr where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';   
                                                          
        else                                                                  
             v_info := '发现未定义的serv_type:' || c1.serv_type ||'，请确认';
             dbms_output.put_line('sql:['||v_info||']'); 
             raise EXCEPTION_UNKNOWN_SERVTYPE ;                                                        
        end case; 
      
        if v_mapping_id<>-1 then
		dbms_output.put_line('2..........');
            if v_changed=TRUE then
			dbms_output.put_line('3..........');
dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']'); 
                delete data_map where mapping_id=v_mapping_id and substr(mapping_sour,2,2)<>'15';  
                execute immediate v_sql;
                
                null;
            end if;
        end if;
          
    end loop;
    commit;    
        
    v_mapping_id :=0;
    v_filename := 'MCBBJ____SP_OPER_'||v_date||'.txt';
    v_oldfilename := 'MCBBJ____SP_OPER_'||v_yestoday||'.txt';
	dbms_output.put_line('4..........');

    for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
	dbms_output.put_line('5..........');
        case                                                                                                   
        when c1.serv_type='001102' then v_mapping_id:=61;  --梦网短信 
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';                                                       
        when c1.serv_type='001103' then v_mapping_id:=78;  --一点结算梦网短信     
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';                                              
        when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通                                                              
        when c1.serv_type='001122' then v_mapping_id:=80;  --捐款短信 
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';                                                          
        when c1.serv_type='001123' then v_mapping_id:=74;  --流媒体     
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='001322' then v_mapping_id:=82;  --手机邮箱 
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000103' then v_mapping_id:=67;  --梦网彩信 
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag-1,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000104' then v_mapping_id:=63;  --WAP 
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000105' then v_mapping_id:=76;  --手机动画
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000005' then v_mapping_id:=69;  --通用下载
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||lpad(bill_flag,2,''0'')||''.''||fee/10||'',1|''||out_prop||''|1'', 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='008002' then v_mapping_id:=-1;  --手机地图                                                            
        when c1.serv_type='008001' then v_mapping_id:=91;  --无线音乐
            v_changed:=if_changed_spoper(c1.serv_type,v_filename,v_oldfilename); 
            v_sql:='insert into data_map (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note) 
            select sp_code||'',''||operator_code, operator_name||'',''||content_code||'',''||lpad(bill_flag,2,''0'')||'',''||fee/10||'',''||out_prop, 
            valid_date, expire_date,' || v_mapping_id || ', sourfilename from spopercdr 
            where serv_type='''||c1.serv_type||''' and sourfilename like '''||v_filename||'''';
        when c1.serv_type='000204' then v_mapping_id:=-1;  --彩铃                      
                                                                              
        else                                                                  
             v_info := '发现未定义的serv_type:' || c1.serv_type ||'，请确认';
             dbms_output.put_line('sql:['||v_info||']'); 
             raise EXCEPTION_UNKNOWN_SERVTYPE ;                                                       
        end case;  
        
        if v_mapping_id<>-1 then
            if v_changed=TRUE then
			dbms_output.put_line('6..........');
dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']'); 
                delete data_map where mapping_id=v_mapping_id and substr(mapping_sour,2,2)<>'15';  
                execute immediate v_sql;              
                null;
            end if;
        end if;
    end loop;
    
    commit;
    --异常
    exception 
    when EXCEPTION_UNKNOWN_SERVTYPE then
        rollback;   
    when others then
        dbms_output.put_line('error: '||substr(v_sql,1,200));  
        dbms_output.put_line(substr(v_sql,201));
        if v_filename like 'MCBBJ____SP_INFO%' then 
            dbms_output.put_line(sysdate||': SP企业代码装载数据失败！');  
        else
            dbms_output.put_line(sysdate||': SP业务代码装载数据失败！');  
        end if;
        rollback;
    
end ap_spdataload_all;
//






  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD_JERRY" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。
  同时维护内容计费2期的局数据

  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。

  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
    ***200810增加下面4个类型********
  060101  PIM                    "PIM"       ----
  000401  国际问候短语(os)       "OS"        60  61
  008007  手机游戏               "GP"      --87
  001126  飞信互通               "FX"        ----

  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ***200810增加下面1个类型********
  008104  多媒体彩铃             "MRBT"    ----
  ***20090805添加3个业务
  008011  Mobile Market          "MMK"       93  94
  090604  农信通彩信             "CIMM"      66  67
  090502  Widget                 "WID"       95  96
  090508  手机阅读（MREAD）      "MER"       97  98
  001124  手机电视               "STM"       ----
  **** 20100621增加1个类型
  008003  手机导航                           ----
  090515  手机POC                 "POC"      --175
  090521  手机钱包
  090520  彩云
  ----------------------------------------------------------*/

  --变量定义

  v_mapping_id NUMBER(10);

  v_date     VARCHAR2(14);
  v_filename VARCHAR2(40);
  ---modify 20080118
  v_filename1    VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

  v_conent_flag NUMBER(2); ---用于判断是否有局数据不用同步,但是内容计费需要同步局数据的情况.
  v_post_flag varchar2(32);

BEGIN
  v_content_id  := 203;
  v_conent_flag := 0;
  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');
  ---20080218 modify
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_date FROM dual;
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

 
  /*
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;
  */
  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
  --20080118 modify
  v_filename1 := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename || '%';
      ---mayong 20090615---OR filename LIKE v_filename1 || '%'; --20080118 modify
  --WHERE filename LIKE v_filename;
  IF v_count < 1
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (
   SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     ---20090615----WHERE (sourfilename = v_filename OR sourfilename = v_filename1) --20080118 modify
     WHERE sourfilename = 'MCBBJ_00_SP_OPER_20170123.txt' 
            and sp_code='698038' and operator_code='80000000000100008147'
    )
  LOOP
  dbms_output.put_line('1.........');
    v_conent_flag := 0; ---内容计费同步标志清0
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20081021
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20081021
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        if  c1.sp_code in ('699213','699216','699218','699214','699211','699217','699215','699212') then
            v_content_mapping_sour := '51,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '51,' || c1.fee / 1000;
        else
            v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '14,' || c1.fee / 1000;
        end if ;

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        v_conent_flag          := 1;
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1; ---设置标志,说明当前纪录有局数据不同步,内容计费要同步的数据
        v_content_mapping_sour := '20,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '20,' || c1.fee / 1000;

      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐不需要进行同步
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
        v_mapping_id   := -1;
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃
    /*------------------20081008-----------------------------*/

      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM,目前未稽核，不需要同步

      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)

    /*
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 61; --国际问候短语(os) ,目前未稽核，不需要同步
            v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
            v_mapping_dest := c1.operator_name || ',' ||
                              lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                              ',1|' || c1.out_prop || '|1';
            v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      */
      WHEN c1.serv_type = '008007' THEN
        --cuibiao 20100322 修改 只是同步局数据到 mapping_list
        /*v_mapping_id := -1; --手机游戏,目前未稽核，不需要同步 */
        v_mapping_id   := 87; --手机地图
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||','||
                  lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 ||
                  ',' || c1.out_prop;
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

         ---内容计费同步局数据使用 cuibiao 20100705添加
        v_conent_flag          := 1;
        v_content_mapping_sour := '28,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '28,' || c1.fee / 1000;



      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通,目前未稽核，不需要同步

      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,目前未稽核，不需要同步
       /* cuibiao  20091223注释
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1;  */

      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := -1; --Mobile Market 业务局数据只是查询用,直接入查询库


        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '57,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '57,' || c1.fee / 1000;


      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 67; --农信通彩信(CIMM)
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 96; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '56,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '56,' || c1.fee / 1000;

       WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 98; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---add by cuibiao 20091217
        v_conent_flag          := 1;
        v_content_mapping_sour := '60,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '60,' || c1.fee / 1000;

       --cuibiao 20100115 stm
       WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 103;
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '53,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '53,' || c1.fee / 1000;
       --wangqi 20111213 手机动漫
       WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 105;
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '36,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '36,' || c1.fee / 1000;
       --cuibiao 20100621 手机导航
       WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航暂不计费
        v_conent_flag:= 1;
        v_content_mapping_sour := '65,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '65,' || c1.fee / 1000;

       --cuibiao 20101221
       WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机poc

       WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包

       WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云

       WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1;
        v_conent_flag          := 1;
        v_content_mapping_sour := '80,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '80,' || c1.fee / 1000;
        /*
        v_mapping_id   := 175; --手机poc
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '39,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '39,' || c1.fee / 1000;


        */

    /*------------------20081008-----------------------------*/
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;

    v_post_flag :='mapping_list|业务' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
	  dbms_output.put_line('2.........');
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('3.........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.operator_code);

      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        --20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;

    END IF;

   v_post_flag :='mapping_list|内容计费' ;
    ----内容计费局数据同步
    IF v_conent_flag = 1
    THEN
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
	  dbms_output.put_line('4.........');
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;

        --插入新数据
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
        EXCEPTION
          --WHEN OTHERS THEN
          --20080118 modify
          WHEN dup_val_on_index THEN
            NULL;
        END;

      END IF;
    END IF;
  END LOOP;

  commit;
   v_post_flag :='mapping_list|异常处理0' ;
  --特殊处理营业和计费局数据bizcode不一致的数据
     --sp      营业bizcode  信用bizcode
     --901312   38            04
     --901317   97            04
     --801084   97            05
  --先删除主键重复的，再更新
    ---add by x00231576 begin
    --901312
    for cur in (
                select * from mapping_list a where a.mapping_id = '203'  and af_getstr(mapping_sour, ',', 2) = '901312'
                and af_getstr(mapping_sour, ',', 1) = '04' /*and (expiretime > SYSDATE OR expiretime IS NULL)*/
               ) loop
	dbms_output.put_line('5.........');
     --如果这一条已经有38的那一条直接删除掉38的这一条
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '38,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '38,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --901317
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                      and af_getstr(mapping_sour, ',', 2) = '901317'
                      and af_getstr(mapping_sour, ',', 3) not in
                       (select af_getstr(mapping_sour, ',', 3)
                          from mapping_list
                         where af_getstr(mapping_sour, ',', 1) = '97'
                           and af_getstr(mapping_sour, ',', 2) = '901317')
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('6.........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --915982
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                      and af_getstr(mapping_sour, ',', 2) = '915982'
                      and af_getstr(mapping_sour, ',', 1)='04'
                      and af_getstr(mapping_sour, ',', 3) not in
                       (select af_getstr(mapping_sour, ',', 3)
                          from mapping_list
                         where af_getstr(mapping_sour, ',', 1) = '97'
                           and af_getstr(mapping_sour, ',', 2) = '915982')
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('7.........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --801084
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                         and af_getstr(mapping_sour, ',', 2) = '801084'
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('8.........');
     v_mapping_sour:=replace(cur.mapping_sour, '05,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '05,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    ---add by x00231576 end

     update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '38,'),
            mapping_dest = replace(mapping_dest, '04,', '38,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901312'
        and (expiretime > SYSDATE OR expiretime IS NULL);
     commit;
     v_post_flag :='mapping_list|异常处理1' ;
      update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901317'
        and af_getstr(mapping_sour, ',', 3) not in
         (select af_getstr(mapping_sour, ',', 3)
            from mapping_list
           where af_getstr(mapping_sour, ',', 1) = '97'
             and af_getstr(mapping_sour, ',', 2) = '901317');
     commit;
           update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
        where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '915982'
        and af_getstr(mapping_sour, ',', 1)='04'
        and af_getstr(mapping_sour, ',', 3) not in
         (select af_getstr(mapping_sour, ',', 3)
            from mapping_list
           where af_getstr(mapping_sour, ',', 1) = '97'
             and af_getstr(mapping_sour, ',', 2) = '915982');
     commit;
     v_post_flag :='mapping_list|异常处理2' ;
      update mapping_list
        set mapping_sour = replace(mapping_sour, '05,', '97,'),
            mapping_dest = replace(mapping_dest, '05,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '801084'
        and (expiretime > SYSDATE OR expiretime IS NULL);
     commit;

     v_post_flag :='mapping_list|异常处理3' ;
  /*
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);
  */
  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(v_post_flag||':'||v_mapping_id||'|'||v_content_mapping_sour||'|'||v_content_mapping_dest||'|'||v_content_id);
    dbms_output.put_line(': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(': ' || ' SP业务代码装载数据失败！');
    END IF;

    ROLLBACK;

END ap_spdataload_jerry;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD_OLD" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。
  
  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。
  
  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ----------------------------------------------------------*/

  --变量定义

  v_mapping_id NUMBER(10);

  v_date         VARCHAR2(14);
  v_yestoday     VARCHAR2(14);
  v_filename     VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);

  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

BEGIN

  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');

  SELECT to_char(SYSDATE, 'yyyymmdd') INTO v_date FROM dual;
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_yestoday FROM dual;
  v_date     := '20080128';
  v_yestoday := '20080127';
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');   

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  -- v_oldfilename := 'MCBBJ____SP_INFO_'||v_yestoday||'.txt';  
  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename LIKE v_filename
    MINUS
    SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1)
  LOOP
  
    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
	dbms_output.put_line('1..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 60; --梦网短信             
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 77; --一点结算梦网短信 
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通 
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 60; --农信通 
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 79; --捐款短信    
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 73; --流媒体      
        v_mapping_sour := c1.sp_code || ',' || c1.dev_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.serv_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 81; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 66; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 62; --WAP 
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 75; --手机动画 
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 68; --通用下载    
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图                                                            
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 90; --无线音乐 
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.dev_code;
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id   := 110; --彩铃      
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',1,1|85';
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
      
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  --dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']');  
  
    IF v_mapping_id <> -1
    THEN
      --删除旧数据  expire_date
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('2..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);   
      END IF;
      dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
                           c1.serv_code);
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
    END IF;
  
  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;
  dbms_output.put_line('3..........');

  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_OPER_' || v_date || '.txt';
  --v_oldfilename := 'MCBBJ____SP_OPER_'||v_yestoday||'.txt';

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_yestoday ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1 --and serv_type=c1.serv_type
    )
  LOOP
  dbms_output.put_line('4..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通 
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信  
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信            
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载   
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图     
    
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃                      
    
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  
    IF v_mapping_id <> -1
    THEN
      --删除旧数据  
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('5..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);   
      END IF;
      dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
                           c1.operator_code);
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
    
    END IF;
  
  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);

  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));  
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(SYSDATE || ': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(SYSDATE || ': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(SYSDATE || ': ' || ' SP业务代码装载数据失败！');
    END IF;
  
    ROLLBACK;
  
END ap_spdataload_old;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD_TEMP" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。
  同时维护内容计费2期的局数据

  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。

  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
    ***200810增加下面4个类型********
  060101  PIM                    "PIM"       ----
  000401  国际问候短语(os)       "OS"        60  61
  008007  手机游戏               "GP"      --87
  001126  飞信互通               "FX"        ----

  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ***200810增加下面1个类型********
  008104  多媒体彩铃             "MRBT"    ----
  ***20090805添加3个业务
  008011  Mobile Market          "MMK"       93  94
  090604  农信通彩信             "CIMM"      66  67
  090502  Widget                 "WID"       95  96
  090508  手机阅读（MREAD）      "MER"       97  98
  001124  手机电视               "STM"       ----
  **** 20100621增加1个类型
  008003  手机导航                           ----
  090515  手机POC                 "POC"      --175
  090521  手机钱包
  090520  彩云
  ----------------------------------------------------------*/

  --变量定义

  v_mapping_id NUMBER(10);

  v_date     VARCHAR2(14);
  v_filename VARCHAR2(40);
  ---modify 20080118
  v_filename1    VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

  v_conent_flag NUMBER(2); ---用于判断是否有局数据不用同步,但是内容计费需要同步局数据的情况.
  v_post_flag varchar2(32);

BEGIN
  v_content_id  := 203;
  v_conent_flag := 0;
  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');
  ---20080218 modify
  SELECT to_char(SYSDATE , 'yyyymmdd') INTO v_date FROM dual;
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename LIKE v_filename
    MINUS
    SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1)
  LOOP
  dbms_output.put_line('1..........');
    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 60; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 77; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 60; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 79; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 73; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.dev_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.serv_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 81; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 66; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 62; --WAP
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 75; --手机动画
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 68; --通用下载
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 90; --无线音乐
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.dev_code;
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000204' THEN
        v_mapping_id   := 110; --彩铃
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',1,1|85';
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
        /*------------------20081008-----------------------------*/
      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM ,目前没有稽核sp数据

      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)
    /*
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 60; --国际问候短语(os)
            v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
            v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
            v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
          */
      WHEN c1.serv_type = '008007' THEN
        v_mapping_id := -1; --手机游戏 ,目前没有稽核sp数据

      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通 ,目前没有稽核sp数据

      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,MRBT,目前没有稽核sp数据
       /*cuibiao  20091223注释
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1; */


      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := 93; --Mobile Market
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 66; --农信通彩信(CIMM)
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 95; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 97; --手机阅读
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      --cuibiao add 20100115 stm
      WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 102; --手机电视
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 104; --手机动漫
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      --cuibiao add 20100621 手机导航
      WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航 暂时不计费

      WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机POC业务暂时不处理

      WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包业务暂时不处理

      WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云业务暂时不处理

      WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;

      WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1;
    /*------------------20081008-----------------------------*/

      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  --dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']');

   v_post_flag :='mapping_list' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('2..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.serv_code);
      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        ---20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;
    END IF;

  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;
  dbms_output.put_line('3..........');

  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
  --20080118 modify
  v_filename1 := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename || '%';
      ---mayong 20090615---OR filename LIKE v_filename1 || '%'; --20080118 modify
  --WHERE filename LIKE v_filename;
  IF v_count < 1
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     ---20090615----WHERE (sourfilename = v_filename OR sourfilename = v_filename1) --20080118 modify
     WHERE sourfilename = v_filename ---modify by mayong 20090615 暂不处理01文件
    --WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     /* 20090615  WHERE (sourfilename = v_lastfilename_0 OR
           sourfilename = v_lastfilename_1) --and serv_type=c1.serv_type*/
     WHERE sourfilename = v_lastfilename_0   ---modify by mayong 20090615  暂不处理01文件
    )
  LOOP
    v_conent_flag := 0; ---内容计费同步标志清0
	dbms_output.put_line('4..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20081021
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20090116
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20081021
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '14,' || c1.fee / 1000;

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        v_conent_flag          := 1;
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1; ---设置标志,说明当前纪录有局数据不同步,内容计费要同步的数据
        v_content_mapping_sour := '20,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '20,' || c1.fee / 1000;

      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐不需要进行同步
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
        v_mapping_id   := -1;
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃
    /*------------------20081008-----------------------------*/

      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM,目前未稽核，不需要同步

      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)

    /*
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 61; --国际问候短语(os) ,目前未稽核，不需要同步
            v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
            v_mapping_dest := c1.operator_name || ',' ||
                              lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                              ',1|' || c1.out_prop || '|1';
            v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      */
      WHEN c1.serv_type = '008007' THEN
        --cuibiao 20100322 修改 只是同步局数据到 mapping_list
        /*v_mapping_id := -1; --手机游戏,目前未稽核，不需要同步 */
        v_mapping_id   := 87; --手机地图
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||','||
                  lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 ||
                  ',' || c1.out_prop;
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

         ---内容计费同步局数据使用 cuibiao 20100705添加
        v_conent_flag          := 1;
        v_content_mapping_sour := '28,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '28,' || c1.fee / 1000;



      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通,目前未稽核，不需要同步

      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,目前未稽核，不需要同步
       /* cuibiao  20091223注释
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1;  */

      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := -1; --Mobile Market 业务局数据只是查询用,直接入查询库


        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '57,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '57,' || c1.fee / 1000;


      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 67; --农信通彩信(CIMM)
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 96; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '56,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '56,' || c1.fee / 1000;

       WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 98; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

        ---add by cuibiao 20091217
        v_conent_flag          := 1;
        v_content_mapping_sour := '60,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '60,' || c1.fee / 1000;

       --cuibiao 20100115 stm
       WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 103;
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '53,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '53,' || c1.fee / 1000;
       --wangqi 20111213 手机动漫
       WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 105;
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '36,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '36,' || c1.fee / 1000;
       --cuibiao 20100621 手机导航
       WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航暂不计费
        v_conent_flag:= 1;
        v_content_mapping_sour := '65,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '65,' || c1.fee / 1000;

       --cuibiao 20101221
       WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机poc

       WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包

       WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云

       WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1;
        /*
        v_mapping_id   := 175; --手机poc
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';


        v_conent_flag          := 1;
        v_content_mapping_sour := '39,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '39,' || c1.fee / 1000;


        */

    /*------------------20081008-----------------------------*/
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;

    v_post_flag :='mapping_list|业务' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('5..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.operator_code);

      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        --20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;

    END IF;

   v_post_flag :='mapping_list|内容计费' ;
   dbms_output.put_line('6..........');
    ----内容计费局数据同步
    IF v_conent_flag = 1
    THEN
	dbms_output.put_line('7..........');
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
		dbms_output.put_line('8..........');
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;

        --插入新数据
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
        EXCEPTION
          --WHEN OTHERS THEN
          --20080118 modify
          WHEN dup_val_on_index THEN
            NULL;
        END;

      END IF;
    END IF;
  END LOOP;

  commit;
   v_post_flag :='mapping_list|异常处理0' ;
  --特殊处理营业和计费局数据bizcode不一致的数据
     --sp      营业bizcode  信用bizcode
     --901312   38            04
     --901317   97            04
     --801084   97            05
  --先删除主键重复的，再更新
    ---add by x00231576 begin
    --901312
    for cur in (
                select * from mapping_list a where a.mapping_id = '203'  and af_getstr(mapping_sour, ',', 2) = '901312'
                and af_getstr(mapping_sour, ',', 1) = '04' /*and (expiretime > SYSDATE OR expiretime IS NULL)*/
               ) loop
     --如果这一条已经有38的那一条直接删除掉38的这一条
	 dbms_output.put_line('9..........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '38,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '38,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --901317
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                      and af_getstr(mapping_sour, ',', 2) = '901317'
                      and af_getstr(mapping_sour, ',', 3) not in
                       (select af_getstr(mapping_sour, ',', 3)
                          from mapping_list
                         where af_getstr(mapping_sour, ',', 1) = '97'
                           and af_getstr(mapping_sour, ',', 2) = '901317')
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('10..........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --915982
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                      and af_getstr(mapping_sour, ',', 2) = '915982'
                      and af_getstr(mapping_sour, ',', 1)='04'
                      and af_getstr(mapping_sour, ',', 3) not in
                       (select af_getstr(mapping_sour, ',', 3)
                          from mapping_list
                         where af_getstr(mapping_sour, ',', 1) = '97'
                           and af_getstr(mapping_sour, ',', 2) = '915982')
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('11..........');
     v_mapping_sour:=replace(cur.mapping_sour, '04,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '04,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    --801084
    for cur in (
                select * from mapping_list a where mapping_id = '203'
                         and af_getstr(mapping_sour, ',', 2) = '801084'
               ) loop
     --如果这一条已经有97的那一条直接删除掉97的这一条
	 dbms_output.put_line('12..........');
     v_mapping_sour:=replace(cur.mapping_sour, '05,', '97,');
     v_mapping_dest:=replace(cur.mapping_dest, '05,', '97,');
     insert into tmp_mappint_list_err
         select * from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     delete from mapping_list a
            where a.mapping_sour=v_mapping_sour and mapping_dest=v_mapping_dest and MAPPING_ID=203 and APPLYTIME=cur.applytime;
     commit;
    end loop ;
    ---add by x00231576 end

     update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '38,'),
            mapping_dest = replace(mapping_dest, '04,', '38,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901312'
        and (expiretime > SYSDATE OR expiretime IS NULL);
     commit;
     v_post_flag :='mapping_list|异常处理1' ;
      update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901317'
        and af_getstr(mapping_sour, ',', 3) not in
         (select af_getstr(mapping_sour, ',', 3)
            from mapping_list
           where af_getstr(mapping_sour, ',', 1) = '97'
             and af_getstr(mapping_sour, ',', 2) = '901317');
     commit;
           update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
        where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '915982'
        and af_getstr(mapping_sour, ',', 1)='04'
        and af_getstr(mapping_sour, ',', 3) not in
         (select af_getstr(mapping_sour, ',', 3)
            from mapping_list
           where af_getstr(mapping_sour, ',', 1) = '97'
             and af_getstr(mapping_sour, ',', 2) = '915982');
     commit;
     v_post_flag :='mapping_list|异常处理2' ;
      update mapping_list
        set mapping_sour = replace(mapping_sour, '05,', '97,'),
            mapping_dest = replace(mapping_dest, '05,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '801084'
        and (expiretime > SYSDATE OR expiretime IS NULL);
     commit;

     v_post_flag :='mapping_list|异常处理3' ;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);

  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(v_post_flag||':'||v_mapping_id||'|'||v_content_mapping_sour||'|'||v_content_mapping_dest||'|'||v_content_id);
    dbms_output.put_line(SYSDATE || ': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(SYSDATE || ': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(SYSDATE || ': ' || ' SP业务代码装载数据失败！');
    END IF;

    ROLLBACK;

END ap_spdataload_temp;
//




  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD_TEMPORARY" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。
  同时维护内容计费2期的局数据
  
  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。
  
  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
    ***200810增加下面4个类型********
  060101  PIM                    "PIM"       ----
  000401  国际问候短语(os)       "OS"        60  61
  008007  手机游戏               "GP"      --87
  001126  飞信互通               "FX"        ----
  
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ***200810增加下面1个类型********
  008104  多媒体彩铃             "MRBT"    ----
  ***20090805添加3个业务
  008011	Mobile Market          "MMK"       93  94 
  090604	农信通彩信             "CIMM"      66  67
  090502	Widget                 "WID"       95  96
  090508	手机阅读（MREAD）      "MER"       97  98
  001124  手机电视               "STM"       ----
  **** 20100621增加1个类型
  008003  手机导航                           ----
  090515  手机POC                 "POC"      --175
  090521  手机钱包
  090520  彩云
  ----------------------------------------------------------*/

  --变量定义

  v_mapping_id NUMBER(10);

  v_date     VARCHAR2(14);
  v_filename VARCHAR2(40);
  ---modify 20080118
  --v_filename1    VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  --v_lastfilename_0 VARCHAR2(40);
  --v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

  v_conent_flag NUMBER(2); ---用于判断是否有局数据不用同步,但是内容计费需要同步局数据的情况.
  v_post_flag varchar2(32);

BEGIN
  v_content_id  := 203;
  v_conent_flag := 0;
  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');
  ---20080218 modify
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_date FROM dual;
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');   

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
 
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (
   select * from spopercdr 
   where sourfilename='MCBBJ_00_SP_OPER_20161108.txt' and sp_code='801234' and operator_code
   in ('110230')
    )
  LOOP
    v_conent_flag := 0; ---内容计费同步标志清0 
	dbms_output.put_line('1..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20081021    
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通 
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20090116
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信  
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20081021   
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        if  c1.sp_code in ('699213','699216','699218','699214','699211','699217','699215','699212') then
            v_content_mapping_sour := '51,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '51,' || c1.fee / 1000;
        else
            v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
            v_content_mapping_dest := '14,' || c1.fee / 1000;
        end if ;
        
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信            
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载   
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_conent_flag          := 1;
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图 
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1; ---设置标志,说明当前纪录有局数据不同步,内容计费要同步的数据
        v_content_mapping_sour := '20,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '20,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐不需要进行同步
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
        v_mapping_id   := -1;
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃                      
    /*------------------20081008-----------------------------*/
    
      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM,目前未稽核，不需要同步                      
    
      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)             

    /*    
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 61; --国际问候短语(os) ,目前未稽核，不需要同步                      
            v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
            v_mapping_dest := c1.operator_name || ',' ||
                              lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                              ',1|' || c1.out_prop || '|1';
            v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      */
      WHEN c1.serv_type = '008007' THEN
        --cuibiao 20100322 修改 只是同步局数据到 mapping_list 
        /*v_mapping_id := -1; --手机游戏,目前未稽核，不需要同步 */
	      v_mapping_id   := 87; --手机地图   
	      v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
	      v_mapping_dest := c1.operator_name || ',' ||','||
                  lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 ||
                  ',' || c1.out_prop;
	      v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';   
        
         ---内容计费同步局数据使用 cuibiao 20100705添加
        v_conent_flag          := 1;
        v_content_mapping_sour := '28,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '28,' || c1.fee / 1000;
        
                      
    
      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通,目前未稽核，不需要同步                      
    
      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,目前未稽核，不需要同步   
       /* cuibiao  20091223注释                   
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1;  */
               
      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := -1; --Mobile Market 业务局数据只是查询用,直接入查询库    

      
        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '57,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '57,' || c1.fee / 1000;


      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 67; --农信通彩信(CIMM)     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 96; --Widget      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '56,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '56,' || c1.fee / 1000;
                
       WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 98; --Widget      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---add by cuibiao 20091217 
        v_conent_flag          := 1;
        v_content_mapping_sour := '60,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '60,' || c1.fee / 1000;    
       
       --cuibiao 20100115 stm
       WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 103;       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '53,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '53,' || c1.fee / 1000; 
       --wangqi 20111213 手机动漫
       WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 105;       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '36,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '36,' || c1.fee / 1000;        
       --cuibiao 20100621 手机导航 
       WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航暂不计费
        v_conent_flag:= 1;
        v_content_mapping_sour := '65,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '65,' || c1.fee / 1000;  
        
       --cuibiao 20101221
       WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机poc
       
       WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包
        
       WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云     
     
       WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '008400' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090413' THEN
        v_mapping_id := -1;
        
       WHEN c1.serv_type = '090509' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090523' THEN
        v_mapping_id := -1;

       WHEN c1.serv_type = '090524' THEN
        v_mapping_id := -1;
        
       WHEN c1.serv_type = '101123' THEN
        v_mapping_id := -1;  
        
       WHEN c1.serv_type = '090522' THEN
        v_mapping_id := -1;  
                
       WHEN c1.serv_type = '090525' THEN
        v_mapping_id := -1;     

       WHEN c1.serv_type = '090526' THEN
        v_mapping_id := -1;     

       WHEN c1.serv_type = '090527' THEN
        v_mapping_id := -1; 
        /*
        v_mapping_id   := 175; --手机poc      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
        
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '39,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '39,' || c1.fee / 1000; 
        
        
        */
                      
    /*------------------20081008-----------------------------*/
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
    
    v_post_flag :='mapping_list|业务' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
	  dbms_output.put_line('2..........');
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('3..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);   
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.operator_code);
    
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        --20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;
    
    END IF;
  dbms_output.put_line('4..........');
   v_post_flag :='mapping_list|内容计费' ;
    ----内容计费局数据同步
    IF v_conent_flag = 1
    THEN
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
	  dbms_output.put_line('5..........');
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;
      
        --插入新数据  
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
        EXCEPTION
          --WHEN OTHERS THEN
          --20080118 modify
          WHEN dup_val_on_index THEN
            NULL;
        END;
      
      END IF;
    END IF;
  END LOOP;
	
  commit;
 
  
END ap_spdataload_temporary;
//



  CREATE OR REPLACE PROCEDURE "AP_SPDATALOAD_ZD" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。
  同时维护内容计费2期的局数据
  
  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。
  
  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
    ***200810增加下面4个类型********
  060101  PIM                    "PIM"       ----
  000401  国际问候短语(os)       "OS"        60  61
  008007  手机游戏               "GP"      --87
  001126  飞信互通               "FX"        ----
  
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ***200810增加下面1个类型********
  008104  多媒体彩铃             "MRBT"    ----
  ***20090805添加3个业务
  008011	Mobile Market          "MMK"       93  94 
  090604	农信通彩信             "CIMM"      66  67
  090502	Widget                 "WID"       95  96
  090508	手机阅读（MREAD）      "MER"       97  98
  001124  手机电视               "STM"       ----
  **** 20100621增加1个类型
  008003  手机导航                           ----
  090515  手机POC                 "POC"      --175
  090521  手机钱包
  090520  彩云
  ----------------------------------------------------------*/

  --变量定义

  v_mapping_id NUMBER(10);

  v_date     VARCHAR2(14);
  v_filename VARCHAR2(40);
  ---modify 20080118
  v_filename1    VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);
  --内容计费二期修改
  v_content_id           NUMBER(10); /*内容计费二期值映射id定义*/
  v_content_mapping_sour VARCHAR2(60); /*业务代码*/
  v_content_mapping_dest VARCHAR2(30); /*费用*/
  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

  v_conent_flag NUMBER(2); ---用于判断是否有局数据不用同步,但是内容计费需要同步局数据的情况.
  v_post_flag varchar2(32);

BEGIN
  v_content_id  := 203;
  v_conent_flag := 0;
  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');
  ---20080218 modify
  SELECT to_char(SYSDATE - 1, 'yyyymmdd') INTO v_date FROM dual;
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');   

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename LIKE v_filename
    MINUS
    SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1)
  LOOP
    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
	dbms_output.put_line('1.........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 60; --梦网短信             
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 77; --一点结算梦网短信 
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通 
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 60; --农信通 
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 79; --捐款短信    
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 73; --流媒体      
        v_mapping_sour := c1.sp_code || ',' || c1.dev_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.serv_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 81; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 66; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 62; --WAP 
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 75; --手机动画 
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 68; --通用下载    
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图                                                            
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 90; --无线音乐 
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.dev_code;
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id   := 110; --彩铃      
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',1,1|85';
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';
        /*------------------20081008-----------------------------*/
      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM ,目前没有稽核sp数据
    
      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)             
    /* 
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 60; --国际问候短语(os)             
            v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
            v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
            v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
          */
      WHEN c1.serv_type = '008007' THEN
        v_mapping_id := -1; --手机游戏 ,目前没有稽核sp数据             
    
      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通 ,目前没有稽核sp数据             
    
      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,MRBT,目前没有稽核sp数据 
       /*cuibiao  20091223注释
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1; */  
      
      
      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := 93; --Mobile Market
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
        
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 66; --农信通彩信(CIMM)
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 95; --Widget
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      
      WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 97; --手机阅读
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
      --cuibiao add 20100115 stm 
      WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 102; --手机电视
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';
        
      WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 104; --手机动漫
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';      
      --cuibiao add 20100621 手机导航
      WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航 暂时不计费
      
      WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机POC业务暂时不处理
      
      WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包业务暂时不处理
        
      WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云业务暂时不处理
              
      WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;
          
    /*------------------20081008-----------------------------*/
    
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  --dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']');  
  
   v_post_flag :='mapping_list' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
	  dbms_output.put_line('2.........');
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('3.........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);   
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.serv_code);
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        ---20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;
    END IF;
  
  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;

  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
  --20080118 modify
  v_filename1 := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename || '%';
      ---mayong 20090615---OR filename LIKE v_filename1 || '%'; --20080118 modify
  --WHERE filename LIKE v_filename;
  IF v_count < 1
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     ---20090615----WHERE (sourfilename = v_filename OR sourfilename = v_filename1) --20080118 modify 
     WHERE sourfilename = v_filename and sp_code='698001' ---modify by mayong 20090615 暂不处理01文件
    --WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     /* 20090615  WHERE (sourfilename = v_lastfilename_0 OR
           sourfilename = v_lastfilename_1) --and serv_type=c1.serv_type*/
     WHERE sourfilename = v_lastfilename_0   ---modify by mayong 20090615  暂不处理01文件
    )
  LOOP
    v_conent_flag := 0; ---内容计费同步标志清0 
	dbms_output.put_line('4.........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20081021    
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通 
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20090116
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信  
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20081021   
        v_conent_flag          := 1;
        v_content_mapping_sour := '04,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '04,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '14,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '14,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信            
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '05,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '05,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP 
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '03,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '03,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '13,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '13,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载   
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        v_conent_flag          := 1;
        v_content_mapping_sour := '17,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '17,' || c1.fee / 1000;
      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图 
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1; ---设置标志,说明当前纪录有局数据不同步,内容计费要同步的数据
        v_content_mapping_sour := '20,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '20,' || c1.fee / 1000;
      
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐不需要进行同步
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';
        v_mapping_id   := -1;
      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃                      
    /*------------------20081008-----------------------------*/
    
      WHEN c1.serv_type = '060101' THEN
        v_mapping_id := -1; --PIM,目前未稽核，不需要同步                      
    
      WHEN c1.serv_type = '000401' THEN
        v_mapping_id := -1; --国际问候短语(os)             

    /*    
          WHEN c1.serv_type = '000401' THEN
            v_mapping_id   := 61; --国际问候短语(os) ,目前未稽核，不需要同步                      
            v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
            v_mapping_dest := c1.operator_name || ',' ||
                              lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                              ',1|' || c1.out_prop || '|1';
            v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      */
      WHEN c1.serv_type = '008007' THEN
        --cuibiao 20100322 修改 只是同步局数据到 mapping_list 
        /*v_mapping_id := -1; --手机游戏,目前未稽核，不需要同步 */
	      v_mapping_id   := 87; --手机地图   
	      v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
	      v_mapping_dest := c1.operator_name || ',' ||','||
                  lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 ||
                  ',' || c1.out_prop;
	      v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';   
        
         ---内容计费同步局数据使用 cuibiao 20100705添加
        v_conent_flag          := 1;
        v_content_mapping_sour := '28,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '28,' || c1.fee / 1000;
        
                      
    
      WHEN c1.serv_type = '001126' THEN
        v_mapping_id := -1; --飞信互通,目前未稽核，不需要同步                      
    
      WHEN c1.serv_type = '008104' THEN
        v_mapping_id := -1; --多媒体彩铃,目前未稽核，不需要同步   
       /* cuibiao  20091223注释                   
      WHEN c1.serv_type = '090604' THEN
        v_mapping_id := -1;  */
               
      WHEN c1.serv_type = '008011' THEN
        v_mapping_id   := -1; --Mobile Market 业务局数据只是查询用,直接入查询库    

      
        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '57,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '57,' || c1.fee / 1000;


      WHEN c1.serv_type = '090604' THEN
        v_mapping_id   := 67; --农信通彩信(CIMM)     
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用
        v_conent_flag          := 1;
        v_content_mapping_sour := '97,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '97,' || c1.fee / 1000;


      WHEN c1.serv_type = '090502' THEN
        v_mapping_id   := 96; --Widget      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---内容计费同步局数据使用 20080219
        v_conent_flag          := 1;
        v_content_mapping_sour := '56,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '56,' || c1.fee / 1000;
                
       WHEN c1.serv_type = '090508' THEN
        v_mapping_id   := 98; --Widget      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        ---add by cuibiao 20091217 
        v_conent_flag          := 1;
        v_content_mapping_sour := '60,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '60,' || c1.fee / 1000;    
       
       --cuibiao 20100115 stm
       WHEN c1.serv_type = '001124' THEN
        v_mapping_id   := 103;       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '53,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '53,' || c1.fee / 1000; 
       --wangqi 20111213 手机动漫
       WHEN c1.serv_type = '090516' THEN
        v_mapping_id   := 105;       
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
      
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '36,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '36,' || c1.fee / 1000;        
       --cuibiao 20100621 手机导航 
       WHEN c1.serv_type = '008003' THEN
        v_mapping_id := -1; --手机导航暂不计费
        
       --cuibiao 20101221
       WHEN c1.serv_type = '090515' THEN
        v_mapping_id := -1; --手机poc
       
       WHEN c1.serv_type = '090521' THEN
        v_mapping_id := -1; --手机钱包
        
       WHEN c1.serv_type = '090520' THEN
        v_mapping_id := -1; --彩云     
     
       WHEN c1.serv_type = '090518' THEN
        v_mapping_id := -1;

        /*
        v_mapping_id   := 175; --手机poc      
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
        
        
        v_conent_flag          := 1;
        v_content_mapping_sour := '39,' || c1.sp_code || ',' ||
                                  c1.operator_code;
        v_content_mapping_dest := '39,' || c1.fee / 1000; 
        
        
        */
                      
    /*------------------20081008-----------------------------*/
      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
    
    v_post_flag :='mapping_list|业务' ;
    IF v_mapping_id <> -1
    THEN
      --删除旧数据
	  dbms_output.put_line('5.........');
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('6.........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);   
      END IF;
      --dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
      --                  c1.operator_code);
    
      --插入新数据  
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        --20080118 modify
        WHEN dup_val_on_index THEN
          NULL;
      END;
    
    END IF;
  
   v_post_flag :='mapping_list|内容计费' ;
    ----内容计费局数据同步
	dbms_output.put_line('7.........');
    IF v_conent_flag = 1
    THEN
      v_count := 0;
      IF c1.bill_flag = '3'
      THEN
        SELECT COUNT(*)
          INTO v_count
          FROM mapping_list
         WHERE mapping_sour = v_content_mapping_sour
           AND mapping_dest = v_content_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_content_id;
        IF (v_count > 0)
        THEN
		dbms_output.put_line('8.........');
          DELETE FROM mapping_list
           WHERE mapping_sour = v_content_mapping_sour
             AND mapping_dest = v_content_mapping_dest
             AND applytime = c1.valid_date
             AND mapping_id = v_content_id
             AND (expiretime > SYSDATE OR expiretime IS NULL);
        END IF;
      
        --插入新数据  
        BEGIN
          INSERT INTO mapping_list
            (mapping_sour,
             mapping_dest,
             applytime,
             expiretime,
             mapping_id,
             note)
          VALUES
            (v_content_mapping_sour,
             v_content_mapping_dest,
             c1.valid_date,
             c1.expire_date,
             v_content_id,
             v_sourfilename);
        EXCEPTION
          --WHEN OTHERS THEN
          --20080118 modify
          WHEN dup_val_on_index THEN
            NULL;
        END;
      
      END IF;
    END IF;
  END LOOP;
	
  
   v_post_flag :='mapping_list|异常处理' ;
	--特殊处理营业和计费局数据bizcode不一致的数据
	   --sp      营业bizcode  信用bizcode
     --901312   38            04 	
		 --901317   97            04
     --801084   97            05
     update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '38,'),
            mapping_dest = replace(mapping_dest, '04,', '38,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901312';
		 commit;
		 
		  update mapping_list
        set mapping_sour = replace(mapping_sour, '04,', '97,'),
            mapping_dest = replace(mapping_dest, '04,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '901317';
		 commit;
		 
		  update mapping_list
        set mapping_sour = replace(mapping_sour, '05,', '97,'),
            mapping_dest = replace(mapping_dest, '05,', '97,')
      where mapping_id = '203'
        and af_getstr(mapping_sour, ',', 2) = '801084';
		 commit;

	
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);

  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));  
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(v_post_flag||':'||v_mapping_id||'|'||v_content_mapping_sour||'|'||v_content_mapping_dest||'|'||v_content_id);
    dbms_output.put_line(SYSDATE || ': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(': ' || ' SP业务代码装载数据失败！');
    END IF;
  
    ROLLBACK;
  
END ap_spdataload_zd;
//




  CREATE OR REPLACE PROCEDURE "AP_STAT_R_JF" 
IS
 v_P   NUMBER(8);
 v_sql VARCHAR2(4000);
BEGIN
  v_P:=20080700;
  FOR c1 IN 1..10 LOOP
      v_P:=v_P+1;
      v_sql:='insert into stat_r_jf
              select destfilename,SUM(LOCALFEE),sum(ROAMFEE),sum(TOLLFEE),SUM(RURALADDFEE),SUM(TOLLADDFEE),
                     SUM(TOLLFEE2),SUM(LOCALFEE_S),SUM(ROAMFEE_S),SUM(TOLLFEE_S),SUM(RURALADDFEE_S),SUM(TOLLADDFEE_S),
                     SUM(TOLLFEE2_S),SUM(TOLLDISCOUNT)
             from gsmguestcdr200807 partition (P_'||v_P||') where destfilename like ''R%''
             GROUP BY destfilename';
  EXECUTE IMMEDIATE v_sql;
  COMMIT;
  END LOOP;
END;
//




CREATE OR REPLACE FUNCTION "AF_GETPARTIAL" (str VARCHAR2)
  RETURN VARCHAR2 IS
  v_resturn      number;
  v_str      number;
  v_last_str varchar2(100) ;
  v_pos      NUMBER;
  v_last_str_t  varchar2(100)  ;
  v_str_10      varchar2(32);
BEGIN
  v_resturn := 0;

  v_pos := length(str) - length(replace(str,',')) + 1 ;

  v_last_str:=replace(af_getstr(str,',',v_pos),'<','');

  --查看第10条话单的分割值，
  --如果是16进制，转换成10进制
  v_str_10:=af_getstr(str,',',11);
  if v_str_10 ='0A' then
    v_last_str_t:=to_number(v_last_str,'XXX');
  elsif v_str_10 ='10' then
   v_last_str_t:=to_number(v_last_str);
  else
   v_last_str_t:=to_number(v_last_str);
  end if ;

  if v_last_str_t>to_number(v_pos)-1 then
     v_resturn := 1 ;
  elsif v_last_str_t<to_number(v_pos)-1 then
     v_resturn := 2 ;
  else
     v_resturn := 3 ;
  end if ;

  RETURN v_resturn;
END;
//



CREATE OR REPLACE FUNCTION "AF_GETPARTIAL_TIME" (v_str VARCHAR2)
  RETURN VARCHAR2 IS
  v_loop_count number;
  v_tmp_str varchar2(2048);
  v_zs_flag varchar2(2048):='0';
  v_ls_flag varchar2(2048):='0';
  v_tmp_time varchar2(14);
  v_tmp_duration number;
  v_mid_time varchar2(14);
  v_mid_duration number;
  v_resturn varchar2(2048);
BEGIN


  v_loop_count := length(v_str)-length(replace(v_str,',',''))+1 ;

  for i in 1..v_loop_count loop

      v_tmp_str:=af_getstr(v_str,',',i);

      if v_tmp_str <> '0' then
           v_tmp_time := af_getstr(v_tmp_str,'|',1);
           v_tmp_duration := to_number(af_getstr(v_tmp_str,'|',2));
      end if ;


      if i  > 1 and i <= 2 then
         if mod(v_tmp_duration,60) <> 0 then
            v_zs_flag :=v_zs_flag||',t_head('||(i-1)||')';
         end if ;

         if v_tmp_duration <= 60 then
            v_zs_flag :=v_zs_flag||',s_head('||(i-1)||')';
         end if ;

      elsif i  > 2 and i < v_loop_count then
         if mod(v_tmp_duration,60) <> 0 then
            v_zs_flag:=v_zs_flag||',t_mid('||(i-1)||')';
         end if ;

         if v_tmp_duration <= 60 then
            v_zs_flag :=v_zs_flag||',s_mid('||(i-1)||')';
         end if ;

         if (to_date(v_mid_time,'yyyymmddhh24miss')+v_mid_duration*1/24/60/60 ) <> to_date(v_tmp_time,'yyyymmddhh24miss') then
            v_ls_flag := v_ls_flag ||',l_mid('||(i-1)||')';
         end if ;

      else

         if (to_date(v_mid_time,'yyyymmddhh24miss')+v_mid_duration*1/24/60/60 ) <> to_date(v_tmp_time,'yyyymmddhh24miss') then
            v_ls_flag := v_ls_flag ||',l_end('||(i-1)||')';
         end if ;

      end if ;


      v_mid_time := af_getstr(v_tmp_str,'|',1);
      v_mid_duration := to_number(af_getstr(v_tmp_str,'|',2));

  end loop ;

  if (v_zs_flag='0' and v_ls_flag ='0') then
     v_resturn:='0#0';
  else
     v_resturn := v_zs_flag||'#'||v_ls_flag ;
  end if ;
  RETURN v_resturn;

 EXCEPTION
 WHEN OTHERS THEN
  RETURN 0;
END;
//




  CREATE OR REPLACE PROCEDURE "AP_STAT_TJ_CHAOCHANG" (v_day varchar2) is
v_sql varchar2(2048);


v_head_count number;
v_mid_count number;
v_end_count number;
v_next_day varchar2(32);
v_last_day varchar2(32);
v_end_day  varchar2(32);
begin


      v_next_day := to_char(to_date(v_day,'yyyymmdd')+1,'yyyymmdd') ;
      v_last_day := to_char(to_date(v_day,'yyyymmdd')-1,'yyyymmdd') ;
      v_end_day  := to_char(to_date(v_day,'yyyymmdd')+2,'yyyymmdd') ;

      

      delete start_gsmguest_tj where start_time = v_day ;
      commit;
      --取当天数据
      
      v_sql :='truncate table start_gsmguest_cdr';
      execute immediate v_sql ;

      v_sql:='
              insert into start_gsmguest_cdr
              select * from gsmsegmentcdr
              where 
                     starttime >= to_date('''||v_day||''',''yyyymmdd'')
              and    starttime <  to_date('''||v_next_day||''',''yyyymmdd'')
              ';
      execute immediate v_sql ;
      commit;

      insert into start_gsmguest_tj (hregion, telnum, othertelnum, callreference_s, start_time)
      select hregion, telnum, othertelnum, OTHERLAC,v_day from start_gsmguest_cdr
      group by hregion, telnum, othertelnum, OTHERLAC;
      commit;
	  dbms_output.put_line('1.........');

      --取后一天的数据
      
      v_sql := '
            insert into start_gsmguest_cdr
            select a.*
              from gsmsegmentcdr a,
                   start_gsmguest_tj     b
             where a.telnum = b.telnum
               and a.hregion = b.hregion
               and a.othertelnum = b.othertelnum
               and a.OTHERLAC = b.callreference_s
               and b.start_time = '||v_day||'
               and    a.starttime >= to_date('''||v_next_day||''',''yyyymmdd'')
               and    a.starttime <  to_date('''||v_end_day||''',''yyyymmdd'')
            ';
      execute immediate v_sql ;
      commit;
      
      
     --取前一天的数据 
     
      v_sql := '
            insert into start_gsmguest_cdr
            select a.*
              from gsmsegmentcdr a,
                   start_gsmguest_tj    b
             where a.telnum = b.telnum
               and a.hregion = b.hregion
               and a.othertelnum = b.othertelnum
               and a.OTHERLAC = b.callreference_s
               and b.start_time = '||v_day||'
               and    a.starttime >= to_date('''||v_last_day||''',''yyyymmdd'')
               and    a.starttime <  to_date('''||v_day||''',''yyyymmdd'')
            ';
      execute immediate v_sql ;
      commit;


      for c1 in (select a.*,rowid crow from start_gsmguest_tj a where start_time = v_day ) loop
	  dbms_output.put_line('2.........');
          for c2 in (select * from start_gsmguest_cdr where
                            telnum = c1.telnum and hregion = c1.hregion and
                            othertelnum = c1.othertelnum and OTHERLAC = c1.callreference_s
                            order by PARTIALFLAG,starttime ) loop
				dbms_output.put_line('3.........');
               update  start_gsmguest_tj
               set partial_flag = partial_flag||','||c2.partialflag,
               p_time=p_time||','||to_char(c2.starttime,'yyyymmddhh24miss')||'|'||c2.duration
               where rowid = c1.crow ;
               commit;
          end loop ;


      end loop ;



update  start_gsmguest_tj a
set flag = 'res3'
where start_time = v_day
and 
(
PARTIAL_FLAG  not like  '0,1%' and 
PARTIAL_FLAG  not like  '0,01%' and
PARTIAL_FLAG  not like  '0,001%' 
)  ;
commit;

update  start_gsmguest_tj a
set flag = 'res5'
where start_time = v_day
and substr(PARTIAL_FLAG,-1)<>'<' ;
commit;
update  start_gsmguest_tj a
set flag = 'res4'
where start_time = v_day
and  af_getPARTIAL(PARTIAL_FLAG) =1
and af_getstr(PARTIAL_FLAG,',',2)  in ('1','01','001')
and  substr(PARTIAL_FLAG,-1)='<';
commit;

     
update  start_gsmguest_tj a
set 
ZHENGSHU_FLAG = af_getstr(af_getpartial_time(a.p_time),'#',1),
LIANXU_FLAG = nvl(af_getstr(af_getpartial_time(a.p_time),'#',2),0)
where start_time = v_day ;
commit;


select count(*)  into v_head_count from start_gsmguest_tj where start_time = v_day  and flag = 'res3' ;
select count(*)  into v_mid_count from start_gsmguest_tj where start_time = v_day  and flag = 'res4' ;
select count(*)  into v_end_count from start_gsmguest_tj where start_time = v_day  and flag = 'res5' ;



/*
13 
select count(*)   from start_gsmguest_tj_bak where  flag = 'res3' ;
1
select count(*)   from start_gsmguest_tj_bak where  flag = 'res4' ;
750
select count(*)   from start_gsmguest_tj_bak where  flag = 'res5' ;



1112817
select sum(length(lianxu_flag) - length(replace(lianxu_flag, 'l', '')))
  from start_gsmguest_tj_bak
 where lianxu_flag <> '0';
11
select sum(length(zhengshu_flag) - length(replace(zhengshu_flag, 's', '')))
  from start_gsmguest_tj_bak
 where  zhengshu_flag like '%s%';

4780850
select count(*)
  from start_gsmguest_tj_bak
 where  zhengshu_flag like '%t_head%';
 
39
select sum(length(zhengshu_flag) - length(replace(zhengshu_flag, 't_mid', '')))/5
  from start_gsmguest_tj_bak
 where  zhengshu_flag like '%t_mid%';

*/
dbms_output.put_line(v_day||'计费本地分割话单统计已经完成。其中首条丢失:'||v_head_count||',中间丢失:'||v_mid_count||',尾条丢失:'||v_end_count||'。');

end ;
//




CREATE OR REPLACE PROCEDURE "AP_STL_GET_ERRORCDR" (v_cdr_type varchar2,v_billingcycle varchar2) is
v_num number ;
v_sql varchar2(4000);
/*
v_cdr_type= errorcdr
*/
begin

v_sql :='truncate table stl_kftx_pay_error';
execute immediate v_sql;


for c1 in (select * from  all_objects where object_name =upper('ismgerrorcdr') and subobject_name like '%'||v_billingcycle||'%') loop
  dbms_output.put_line('1.........');
  v_sql:='insert into stl_kftx_pay_error
       select to_char(sysdate, ''yyyymmdd''),''MMS'',''05'', substr(mm_seq,-10),
       telnum,sp_code,OPER_CODE,chargetype,starttime, sysdate,
       inforfee * 100, hregion,''e'',to_char(starttime, ''yyyymm''),
       null,null,null,telnum as THIR_FLAG,mm_seq as SEQNO,'''' SUB_TYPE
       from mms'||v_cdr_type||' partition ('||c1.subobject_name||')
       where starttime >= to_date('''||v_billingcycle||''',''yyyymm'')
       and substr(sp_code,2,2)<>''15''
       and basicfee <> 0';
     execute immediate v_sql;
    commit;

  v_sql:='insert into stl_kftx_pay_error
       select to_char(sysdate, ''yyyymmdd''), ''CM'', ''05'',
       substr(sequenceno,-10), telnum, spcode, servicecode, calltype,
       starttime, sysdate, fee * 100, hregion, ''e'',
       to_char(starttime, ''yyyymm''), null,null, null,'''' THIR_FLAG,sequenceno as SEQNO,'''' SUB_TYPE
       from ismg'||v_cdr_type||'  partition ('||c1.subobject_name||')
       where starttime >= to_date('''||v_billingcycle||''',''yyyymm'')
       and sourfilename like ''CM%''
       and fee>0';
    execute immediate v_sql;
     commit;
dbms_output.put_line('2.........');
end loop ;

end ;
//




  CREATE OR REPLACE PROCEDURE "AP_TARIFF_PLAN_ATTR_INIT" 
(a_tariff_plan_id in number,
 a_szInfo  out varchar2
)
as
 nStatus number(2);                --资费政策状态
 nMaxTariffSchemaId number;        --最大资费模式编号
 szTariffSchemaName varchar2(30);  --组合后的资费模式名称
 szFieldDef varchar2(256);         --组合后的参数定义
 szGFieldDef varchar2(256);        --组合后的参数定义中文显示
 szAttrName varchar2(32);          --组合后的实体名称
 nSwitchUnit number(8);            --资费切换单位
 nAttrType  number(1);             --使用属性类型
 nCount number(8);                 --临时累计变量
 nBaseAttrId number(4);            --基本属性ID
 nRoundType number(8);             --舍入类型
 nRateCode number(8);              --免费费率
begin
 --add 39835 20061014
 --初始化变量
  nStatus:=0;
  nMaxTariffSchemaId:=0;
  szTariffSchemaName:='';
  szFieldDef:='';
  szGFieldDef:='';
  szAttrName:='';
  nSwitchUnit:=0;
  nAttrType:=0;
  nCount:=0;
  nBaseAttrId:=0;
  nRoundType:=0;
  nRateCode:=0;
 --获取最大的资费模式号
   select max(TARIFF_SCHEMA_ID) into nMaxTariffSchemaId from (select tariff_schema_id  from

tariff_schema  union all
                       select tariff_schema_id*(-1) from tariff_schema_redu);

 --新赠资费模式号
   nMaxTariffSchemaId:=nMaxTariffSchemaId*(-1)-1;

 --判断资费政策状态
 select max(status) into nStatus from tariff_plan_redu where

tariff_plan_id=a_tariff_plan_id;

 --已经提交审核或者审核已经通过 或者更新提交
 if( nStatus =1 or nStatus=2 or nStatus=5) then
     a_szInfo:='资费政策已经提交审核，不需要再次提交';
     return;
 end if;

 --判断资费政策项状态
  begin
  for c1 in(select a.rowid nrowid,a.* from tariff_plan_item_redu a where

a.tariff_plan_id=a_tariff_plan_id )
  loop
  dbms_output.put_line('1.........');
       --获得基本属性ID，基本属性类型
	   dbms_output.put_line('1.........');
       select attr_id ,attr_type into  nBaseAttrId,nAttrType
                   from tariff_redu_attr_def
                    where event_id=c1.charging_event_id ;
       --获得资费切换单位，舍入单位
       select ratecode,switch_unit,roundtype into  nRateCode,nSwitchUnit,nRoundtype
                   from tariff_redu_charging_def
                    where  event_id=c1.charging_event_id ;

       --如果参数设置类型是空 参数设置方法是空  参数ID无值  则作最后处理
       if(c1.param_set_type is NULL or c1.param_set_Method is NULL or  c1.param_set_id=0)

then
dbms_output.put_line('20.........');
           GOTO PROG_NEXT;
       end if;

       --如果 状态是 审核驳回 和资费更新则 如果 参数设置类型不为空  则删除资费模式表，资费模式项表数据
       --参数表数据不做处理 ，避免错误删除数据
       if(nStatus=3 or nStatus=4) then
           if(c1.param_set_type is not NULL ) THEN
		   dbms_output.put_line('2.........');
               delete from tariff_schema_redu where tariff_schema_id=c1.tariff_schema_id;
               delete from tariff_item_redu where tariff_schema_id=c1.tariff_schema_id;
           end if ;
       end if;

       --判断优惠量的合法性
       if(c1.attr_value=0) then
           a_szInfo:='新增参数优惠量已经存在不能为空，请重新设置';
           return;
       end if;

       --赠送
       if(c1.param_set_type='1') then
          --属性已经存在，新增数据插入临时资费模式表，临时资费模式项表
          if(c1.param_set_method='1') then
		  dbms_output.put_line('3.........');
              --新增资费模式名称
              szTariffSchemaName:='赠送资费模式'||nMaxTariffSchemaId;
              --判断参数ID
              if(c1.param_set_id=0) then
                a_szInfo:='赠送属性已经存在参数属性ID不能为0';
                return;
              end if;
              --组合参数定义
              szFieldDef:='0003-.'||lpad(to_char(c1.param_set_id),4,0);
              --属性ID >0 表明已经插入到正式表，小于0 表明还没有 插入正式表 可以修改优惠属性值
              if(c1.param_set_id>0) then
			  dbms_output.put_line('4.........');
                select attr_name into szAttrName from entity_attr_def where entity_id=1 and

attr_id=c1.param_set_id;
              else
			  dbms_output.put_line('5.........');
                select attr_name into szAttrName from entity_attr_def_redu where entity_id=1

and attr_id=c1.param_set_id;
                update  free_usage_attr_def_redu set

fixed_free_usage=c1.attr_value,free_limit=c1.attr_value
                         where entity_id=1 and attr_id=c1.param_set_id;
              end if;
              --组合参数定义说明文字
              szGFieldDef:='用户.'||trim(szAttrName);
              --插入中间临时表
             insert into tariff_schema_redu(TARIFF_SCHEMA_ID ,TARIFF_NAME ,TARIFF_TYPE
,FIELDCOUNT ,FIELD_DEF ,MATCH_ORDER ,MATCH_TYPE,APPLY_METHOD,REFID,DISCOUNT_FEE_ID , G_FIELD_DEF ,ROUND_METHOD ,ROUND_SCALE ,REF_OFFSET,EVENT_ID )
values (nMaxTariffSchemaId,szTariffSchemaName,6,1,szFieldDef,2,1,2,0,0,szGFieldDef,0,0,0,c1.CHARGING_EVENT_ID);
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME

,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE
,EXPR_ID ,G_PARAM,IS_DYNAMIC,G_CRITERIA ) values(nMaxTariffSchemaId,c1.applytime,NULL,' ',3,1,1.00,'',NULL,0,NULL,NULL,0,' ');
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values(nMaxTariffSchemaId,c1.applytime,NULL,' ',1,nRateCode,1.00,'',NULL,9,NULL,NULL,0,' ');
              --更新资费政策项表
             update tariff_plan_item_redu set tariff_schema_id=nMaxTariffSchemaId where

rowid=c1.nrowid;
             --目前不处理审核驳回或审核不通过过程中优惠量发生变化的情况。
			 dbms_output.put_line('6.........');
         else  --赠送 ，新增属性
             if(c1.param_set_id=0) then
                a_szInfo:='赠送新增参数属性ID不能为0';
                return;
             end if;
dbms_output.put_line('7.........');
             --新增资费模式名称
             szTariffSchemaName:='赠送资费模式'||nMaxTariffSchemaId||'赠送'||c1.attr_value;
             --判断新增参数ID的合法性
             nCount:=0;
             select count(*) into nCount from entity_attr_def_redu where entity_id=1 and

attr_id=c1.param_set_id;
             if(nCount>0) then
               a_szInfo:='赠送新增参数属性ID已经存在，请重新设置';
               return;
             end if;
dbms_output.put_line('8.........');
             --组合实体属性名称
             szAttrName:='赠送'||c1.attr_value;
             --插入实体属性表，赠送属性表，基本属性表
             --用到变量 nAttrType  nBaseAttrId nSwitchUnit nRoundtype
insert into entity_attr_def_redu(ENTITY_ID,ATTR_ID,ATTR_NAME ,ATTR_TYPE,MULTIVALUE ,ISUNIQUE ,ISRESIDENT,ATTR_CLASS,DATATYPE,LENGTH,SCALE,EXPIRE_DAYS)
values(1,c1.param_set_id,szAttrName,4,0,0,1,0,3,10,0,0);
insert into FREE_USAGE_ATTR_DEF_REDU(ENTITY_ID,ATTR_ID,FIXED_FREE_USAGE,FREE_LIMIT ,FREE_PERIOD,FREE_TYPE)
                          values(1,c1.param_set_id,c1.attr_value,c1.attr_value,1,2);
             insert into base_attr_def_redu

(ENTITY_ID,ATTR_ID,BASE_ATTR_TYPE,BASE_ATTR_ID,CHARGING_EVENT_ID,


CALL_USAGE_COND,ROUND_UNIT,ROUND_TYPE,G_CALL_USAGE_COND)
                          values

(1,c1.param_set_Id,nAttrType,nBaseAttrId,c1.charging_event_id,1,nSwitchUnit,nRoundtype,1);
             --组合字段
             szFieldDef:='0003-.'||lpad(to_char(c1.param_set_id),4,0);
             --组合字段名称
             szGFieldDef:='用户.'||trim(szAttrName);
insert into tariff_schema_redu(TARIFF_SCHEMA_ID ,TARIFF_NAME ,TARIFF_TYPE
,FIELDCOUNT ,FIELD_DEF ,MATCH_ORDER ,MATCH_TYPE,APPLY_METHOD,REFID
,DISCOUNT_FEE_ID , G_FIELD_DEF ,ROUND_METHOD ,ROUND_SCALE ,REF_OFFSET,EVENT_ID )
values
(nMaxTariffSchemaId,szTariffSchemaName,6,1,szFieldDef,2,1,2,0,0,szGFieldDef,
0,0,0,c1.CHARGING_EVENT_ID);
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME,TARIFF_CRITERIA,SUBTARIFF_TYPE ,
TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values(nMaxTariffSchemaId,c1.applytime,NULL ,' ',3,1,1.00,'',NULL,0,NULL,NULL,0,' ');
             --用到变量nRateCode
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values(nMaxTariffSchemaId,c1.applytime,NULL,' ',1,nRateCode,1.00,'',NULL,9,NULL,NULL,0,' ');
update tariff_plan_item_redu set
tariff_schema_id=nMaxTariffSchemaId,param_set_method=1 where rowid=c1.nrowid;
         end if; --end param_set_method
      end if; --end param_set_type
       --累计
      if(c1.param_set_type='2') then
           --新增资费模式名称
           szTariffSchemaName:='累计资费模式'||nMaxTariffSchemaId||'累计'||c1.attr_value;
           --属性已经存在，新增数据插入资费模式表，资费模式项表
           if(c1.param_set_method='1') then
		   	  dbms_output.put_line('9.........');
              if(c1.param_set_id=0) then
                a_szInfo:='累计属性已经存在,参数属性ID不能为0';
                return;
              end if;
              --组合参数定义
              szFieldDef:='0003-.'||lpad(to_char(c1.param_set_id),4,0);
              --属性ID 大于0 是插入到实体属性表的，小于0是没有插入到正式表的
              if(c1.param_set_id>0) then
                select attr_name into szAttrName from entity_attr_def where entity_id=1 and

attr_id=c1.param_set_id;
              else
                select attr_name into szAttrName from entity_attr_def_redu where entity_id=1

and attr_id=c1.param_set_id;
              end if;
              --组合参数定义中文显示
              szGFieldDef:='用户.'||trim(szAttrName);
             insert into tariff_schema_redu(TARIFF_SCHEMA_ID ,TARIFF_NAME ,TARIFF_TYPE

,FIELDCOUNT ,FIELD_DEF ,
                                      MATCH_ORDER ,MATCH_TYPE,APPLY_METHOD,REFID

,DISCOUNT_FEE_ID , G_FIELD_DEF ,
                                      ROUND_METHOD ,ROUND_SCALE ,REF_OFFSET,EVENT_ID )
                       values

(nMaxTariffSchemaId,szTariffSchemaName,5,1,szFieldDef,2,1,2,0,0,szGFieldDef,
                               0,0,0,c1.CHARGING_EVENT_ID);
             --用到变量nRateCode
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME
,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values(nMaxTariffSchemaId,c1.applytime,NULL,c1.attr_value,1,nRateCode,1.00,'',NULL,0,NULL,NULL,0,NULL);
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values(nMaxTariffSchemaId,c1.applytime,NULL,'99999999',3,1,1.00,'',NULL,9,NULL,NULL,0,NULL);
--更改资费模式编号
update tariff_plan_item_redu set tariff_schema_id=nMaxTariffSchemaId where

rowid=c1.nrowid;

         else  --累计,新增属性
             if(c1.param_set_id=0) then
                a_szInfo:='累计新增参数属性ID不能为0';
                return;
             end if;
              --判断新增参数ID的合法性
             nCount:=0;
             select count(*) into nCount from entity_attr_def_redu where entity_id=1 and

attr_id=c1.param_set_id;
             if(nCount>0) then
               a_szInfo:='累计新增参数属性ID已经存在，请重新设置';
               return;
             end if;
			 dbms_output.put_line('10.........');
             --组合实体属性名称
             szAttrName:='累计'||c1.attr_value;
             --插入实体相关表
             insert into entity_attr_def_redu(ENTITY_ID,ATTR_ID,ATTR_NAME ,ATTR_TYPE

,MULTIVALUE ,ISUNIQUE ,
                                         ISRESIDENT,ATTR_CLASS,DATATYPE,LENGTH

,SCALE,EXPIRE_DAYS)
                          values(1,c1.param_set_id,szAttrName,3,0,0,1,0,3,10,0,0);
             insert into TOTAL_USAGE_ATTR_DEF_REDU

(ENTITY_ID,ATTR_ID,TOTAL_PERIOD,USAGE_TYPE)
                          values(1,c1.param_set_id,1,2);
             insert into base_attr_def_redu

(ENTITY_ID,ATTR_ID,BASE_ATTR_TYPE,BASE_ATTR_ID,CHARGING_EVENT_ID,


CALL_USAGE_COND,ROUND_UNIT,ROUND_TYPE,G_CALL_USAGE_COND)
                          values

(1,c1.param_set_Id,nAttrType,nBaseAttrId,c1.charging_event_id,1,nSwitchUnit,nRoundtype,1);
             --组合参数定义，参数定义中文显示
             szFieldDef:='0003-.'||lpad(to_char(c1.param_set_id),4,0);
             szGFieldDef:='用户.'||trim(szAttrName);
             insert into tariff_schema_redu(TARIFF_SCHEMA_ID ,TARIFF_NAME ,TARIFF_TYPE

,FIELDCOUNT ,FIELD_DEF ,
                                      MATCH_ORDER ,MATCH_TYPE,APPLY_METHOD,REFID

,DISCOUNT_FEE_ID , G_FIELD_DEF ,
                                      ROUND_METHOD ,ROUND_SCALE ,REF_OFFSET,EVENT_ID )
                       values

(nMaxTariffSchemaId,szTariffSchemaName,5,1,szFieldDef,2,1,2,0,0,szGFieldDef,0,0,0,c1.CHARGING_EVENT_ID);
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values
(nMaxTariffSchemaId,c1.applytime,NULL,c1.attr_value,1,nRateCode,1.00,'',NULL,0,NULL,NULL,0,NULL);
insert into tariff_item_redu(TARIFF_SCHEMA_ID,APPLYTIME ,EXPIRETIME,TARIFF_CRITERIA,SUBTARIFF_TYPE ,TARIFF_ID ,RATIO,RATETYPE,PARAM_STRING,PRECEDENCE,EXPR_ID ,G_PARAM,IS_DYNAMIC ,G_CRITERIA )
values(nMaxTariffSchemaId,c1.applytime,NULL,'99999999',3,1,1.00,'',NULL,9,NULL,NULL,0,NULL);
            --修改资费模式编号
update tariff_plan_item_redu set
tariff_schema_id=nMaxTariffSchemaId,param_set_method=1 where rowid=c1.nrowid;
         end if; --end param_set_method
      end if; --end param_set_type
      --修正资费模式编号
      nMaxTariffSchemaId:=nMaxTariffSchemaId-1;

   <<PROG_NEXT>>
   dbms_output.put_line('21.........');
     --设置资费切换单位
       update   tariff_plan_item_redu set switch_Unit=nSwitchUnit where ROWID=C1.nrowid;

   end loop; --end for;
   --设置初始化标记    如是 资费更新状态  更改为 更新提交  如是 待审核和审核驳回状态  更改为提交审核
      if(nStatus=4) then
         update  tariff_plan_redu set status=5 where tariff_plan_id=a_tariff_plan_id and

status =4;
      else
         update  tariff_plan_redu set status=1 where tariff_plan_id=a_tariff_plan_id and

status in(0,3);
      end if;
   commit;
   exception when others then
            rollback;
            raise_application_error(-20101,sqlerrm);
            a_szInfo:='资费政策生成失败';
            return;
   end;
   a_szInfo:='资费政策生成成功';
   return;
end ap_tariff_plan_attr_init;
//




CREATE OR REPLACE PROCEDURE "AP_TIETONG_JF" is
v_billingcycle varchar2(10);
v_tt_billingcycle varchar2(10);
v_spcode varchar2(20);
v_service varchar2(30);
v_ACCTITEMID  varchar2(32);
v_fee number;
type service_code is varray(14) of varchar2(30);
v_SERVICECODE service_code := service_code('gh_monthfee'  ,'gh_newfee'  ,'gh_localfee',
                                           'gh_outskirt'  ,'gh_long'    ,'gh_international',
                                           'gh_hmt'       ,'gh_infofee' ,'gh_contributions',
                                           'gh_foundation','gh_penalty' ,'gh_discount',
                                           'gh_other'     ,'gh_package'
                                           );
begin
  /*根据实际情况调整赋值*/
  v_billingcycle := to_char(sysdate,'yyyymm');
  v_tt_billingcycle := to_char(sysdate,'yyyymm');
  delete Tietonginfocdr where billingcycle = v_billingcycle ;
  commit;
update Tietongcdr a
   set res1 = '8'
 where billingcycle = v_tt_billingcycle
   and rowid > (select min(rowid)
                  from Tietongcdr t
                 where billingcycle = v_tt_billingcycle
                   and t.tt_accountid = a.tt_accountid);
  commit;
  dbms_output.put_line('1.........');
  for tt in ( select a.*,rowid crow from Tietongcdr a where billingcycle = v_tt_billingcycle  and ERRORCODE is  null and  subscriberid is not null and res1 ='3' ) loop
  dbms_output.put_line('2.........');
      /*
      费用对应关系
      固话-月租  gh_monthfee  　rentfee
      固话-新业务  gh_newfee  　newfee
      固话-市话  gh_localfee  　cityfee
      固话-郊话  gh_outskirt  　countryfee
      固话-长话  gh_long  　longfee
      固话-国际长途  gh_international  　ilongfee
      固话-港澳台  gh_hmt  　hongkongfee
      固话-信息费  gh_infofee	　inforfee
      固话-捐款	gh_contributions	　Juankuan
      固话-慈善基金	gh_foundation	　jijin
      固话-违约金	gh_penalty	　weiyuejin
      固话-优惠	gh_discount	　discount
      固话-其他	gh_other	　otherfee
      固话-套餐包月	gh_package	　monthfee
      宽带-包月费	kd_monthfee	　rentfee
      */

      case tt.devicetype
           when '310' then v_spcode := '0000GH'; v_ACCTITEMID:='2322';
           when '311' then v_spcode := '0000KD'; v_ACCTITEMID:='2324';
           else v_spcode := NULL; v_ACCTITEMID:=null ;
      end case ;


      for i in v_SERVICECODE.first..v_SERVICECODE.last  loop
		dbms_output.put_line('3.........');
          case v_SERVICECODE(i)
                when 'gh_monthfee'		then v_fee :=tt.rentfee/100     ;
                when 'gh_newfee'		then v_fee :=tt.newfee/100     ;
                when 'gh_localfee'		then v_fee :=tt.cityfee/100    ;
                when 'gh_outskirt'		then v_fee :=tt.countryfee/100 ;
                when 'gh_long'			then v_fee :=tt.longfee/100    ;
                when 'gh_international'	then v_fee :=tt.ilongfee/100   ;
                when 'gh_hmt'			then v_fee :=tt.hongkongfee/100 ;
                when 'gh_infofee'		then v_fee :=tt.inforfee/100   ;
                when 'gh_contributions'	then v_fee :=tt.Juankuan/100   ;
                when 'gh_foundation'		then v_fee :=tt.jijin/100     ;
                when 'gh_penalty'		then v_fee :=tt.weiyuejin/100  ;
                when 'gh_discount'		then v_fee :=tt.discount/100   ;
                when 'gh_other'		then v_fee :=tt.otherfee/100   ;
                when 'gh_package'		then v_fee :=tt.monthfee/100   ;
                else v_fee := 0 ;
          end case ;

          if v_SERVICECODE(i) = 'gh_monthfee' and v_spcode = '0000KD' then
             v_service := 'kd_monthfee';
          else
             v_service := v_SERVICECODE(i) ;
          end if ;


          insert into Tietonginfocdr
            (CDRTYPE,
             HREGION,
             TELNUM,
             SPCODE,
             SERVICECODE,
             ACCTITEMID,
             STARTTIME,
             BILLINGCYCLE,
             FEE,
             FEE_S,
             DEVICETYPE,
             SOURFILENAME,
             SUBSCRIBERID,
             ACCOUNTID,
             REGION,
             USERTYPE,
             DISCOUNT_INFO,
             ROLLBACKFLAG,
             ERRORCODE,
             PROCESSTIME)
          values
            ('42',
             tt.region,
             tt.subscriberid,
             v_spcode,
             v_servicecode(i),
             v_ACCTITEMID,
             to_date(v_billingcycle, 'yyyymm'),
             tt.billingcycle,
             v_fee,
             v_fee,
             tt.devicetype,
             tt.sourfilename,
             tt.subscriberid,
             tt.accountid,
             null,
             null,
             null,
             null,
             null,
             sysdate);

      end loop ;
      commit;
      
      update Tietongcdr
      set res1 ='9'
      where rowid = tt.crow ;
      commit;
  end loop ;

      /*在Tietonginfocdr表中copy一份,event_mv用 用 SOURFILENAME表示, event_mv的加以TT前缀标示*/
      insert into Tietonginfocdr
      select CDRTYPE,
             HREGION,
             TELNUM,
             SPCODE,
             SERVICECODE,
             ACCTITEMID,
             STARTTIME,
             BILLINGCYCLE,
             FEE,
             FEE_S,
             DEVICETYPE,
             'TT' || SOURFILENAME,
             SUBSCRIBERID,
             ACCOUNTID,
             REGION,
             USERTYPE,
             DISCOUNT_INFO,
             ROLLBACKFLAG,
             ERRORCODE,
             PROCESSTIME,
             DISCOUNTLIST
        from Tietonginfocdr
       where billingcycle = v_billingcycle
       and SOURFILENAME not like 'TT%';
       commit;
end ;
//




  CREATE OR REPLACE PROCEDURE "AP_TRANS_DICT" (isdel# in number) is
v_order number(3);
begin
  if (isdel#>0) then
    delete from WRTOFF_ORDER_PLAN_DEF;
    delete from wrtoff_order_def;
    delete from subject_def;
    delete from amerce_plan_def;
    delete from amerce_rate_def;
    delete from CUSTBILL_DEF;
    delete from CUSTBILL_CELL_DEF;
    delete from BILLITEM_CELL_DEF;
    delete from collect_priv;
    delete from collect_priv_def;
    delete from collect_priv_item_def;
  end if;
  --trans_feetype:tbcs.feetype
  insert into WRTOFF_ORDER_PLAN_DEF
  select 1,'6',1
  from dual;
  v_order := 0;
  for c in (select * from trans_feetype  where usagetype = 'feutCll' order by payorder asc) loop
  dbms_output.put_line('1.........');
    v_order := v_order + 1;
    insert into wrtoff_order_def
    values(1,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),v_order);
  end loop;
  --
  v_order := 0;
  for c in (select * from trans_acc_subject  where isuse = 1 order by draworder asc) loop
  dbms_output.put_line('2.........');
    v_order := v_order + 1;
    insert into subject_def(SUBJECTID        ,
SUBJECTNAME      ,
PERCENT_OF_REFUND,
CYCLES_OF_REFUND ,
DRAW_ORDER       ,
OFFSET_SCOPE     ,
AVAILABLE_CYCLE  ,
START_CYCLE      ,
IN_USE           ,
DRAW_AMT         ,
FLOWIN_TYPE      ,
SUBJECT_SIGN     )
    values(c.subjectid,c.subjectname,decode(c.returnable,1,100,0),0,v_order,'',0,0,1,0,0,decode(c.returnable,0,'0',1,'1','2'));
  end loop;
  --
  insert into amerce_plan_def(AMERCE_PLANID,AMERCE_PLANNAME,EXCLUDED_SUBJECT,IN_USE)
  values(1,'6','',1);
  insert into amerce_rate_def(amerce_planid,itemtype,cycle,startdate,enddate,rate_of_amerce)
  select 1,0,billcycle,punishstartdate,to_date('20370101','yyyymmdd'),dayrate
  from trans_late_punishrate
  where region = 951;
  insert into amerce_plan_def(AMERCE_PLANID,AMERCE_PLANNAME,EXCLUDED_SUBJECT,IN_USE)
  values(2,'6','',1);
  insert into amerce_rate_def(amerce_planid,itemtype,cycle,startdate,enddate,rate_of_amerce)
  select 2,0,billcycle,punishstartdate,to_date('20370101','yyyymmdd'),dayrate
  from trans_late_punishrate
  where region = 952;
  insert into amerce_plan_def(AMERCE_PLANID,AMERCE_PLANNAME,EXCLUDED_SUBJECT,IN_USE)
  values(3,'6','',1);
  insert into amerce_rate_def(amerce_planid,itemtype,cycle,startdate,enddate,rate_of_amerce)
  select 3,0,billcycle,punishstartdate,to_date('20370101','yyyymmdd'),dayrate
  from trans_late_punishrate
  where region = 953;
  insert into amerce_plan_def(AMERCE_PLANID,AMERCE_PLANNAME,EXCLUDED_SUBJECT,IN_USE)
  values(4,'6','',1);
  insert into amerce_rate_def(amerce_planid,itemtype,cycle,startdate,enddate,rate_of_amerce)
  select 4,0,billcycle,punishstartdate,to_date('20370101','yyyymmdd'),dayrate
  from trans_late_punishrate
  where region = 954;
  --
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select oid,typename,1
  from trans_clientbill_define_type;
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select 6,'6',1
  from dual;
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select 7,'6',1
  from dual;
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select 0,'6',1
  from dual;
  --
  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 3,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShowForMail';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 5,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShowForTrust';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 4,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShowForOldInvoice';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 1,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 952 and a.typeid = 'ctpdBillShowForInvoice';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 2,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 952 and a.typeid = 'ctpdBillShow';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 6,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 951 and a.typeid = 'ctpdBillShowForInvoice';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 7,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 951 and a.typeid = 'ctpdBillShow';
  dbms_output.put_line('3.........');

  v_order := 0;
  for c in (select * from trans_feetype  where usagetype = 'feutCll' order by payorder asc) loop
  dbms_output.put_line('4.........');
    v_order := v_order + 1;
    insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
    values(0,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),c.itemname,v_order,0);
  end loop;

  --
  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select b.oid,a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_define_type b
  where b.region = 300 and b.typeid in ('ctpdBillShowForMail','ctpdBillShowForTrust','ctpdBillShowForOldInvoice')
  and a.typeid = b.typeid
  and a.region = b.region;

  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select decode(b.typeid,'ctpdBillShowForInvoice',1,'ctpdBillShow',2,5),a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_def_typeitem b
  where b.region = 952 and b.typeid in ('ctpdBillShowForInvoice','ctpdBillShow')
  and a.typeid = b.typeid
  and a.region = b.region
  and a.itemid = b.itemid;

  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select 6,a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_def_typeitem b
  where b.region = 951 and b.typeid in ('ctpdBillShowForInvoice')
  and a.typeid = b.typeid
  and a.region = b.region
  and a.itemid = b.itemid;
  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select 7,a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_def_typeitem b
  where b.region = 951 and b.typeid in ('ctpdBillShow')
  and a.typeid = b.typeid
  and a.region = b.region
  and a.itemid = b.itemid;
  dbms_output.put_line('5.........');

  for c in (select * from trans_feetype  where usagetype = 'feutCll' order by payorder asc) loop
  dbms_output.put_line('6.........');
    insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
    values(0,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),
           1,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),
           '',2,'','');
  end loop;
  --
  --collect_priv
  --collect_priv_def
  --collect_priv_item_def
end ap_trans_dict;
//




CREATE OR REPLACE PROCEDURE "AP_TRANS_DICT_ACCTITEM_DEF" (isdel# in number) is
v_order number(3);
begin
  if (isdel#>0) then
    delete from acctitem_def;
	dbms_output.put_line('0.........');
  end if;
  --trans_feetype:tbcs.feetype
  for c in (select * from trans_feetype  where usagetype = 'feutCll' order by payorder asc) loop
  dbms_output.put_line('1.........');
    insert into acctitem_def(itemcode,itemname)
    values(decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),c.itemname);
  end loop;
end ap_trans_dict_acctitem_def;
//




CREATE OR REPLACE PROCEDURE "AP_TRANS_DICT_CUSTBILL" (isdel# in number) is
v_order number(3);
begin
  if (isdel#>0) then
    delete from CUSTBILL_DEF;
    delete from CUSTBILL_CELL_DEF;
    delete from BILLITEM_CELL_DEF;
	dbms_output.put_line('0.........');
  end if;
  --
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select oid,typename,1
  from trans_clientbill_define_type;
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select 6,'6',1
  from dual;
  insert into CUSTBILL_DEF(TEMPLATE_ID,TEMPLATE_NAME,INUSE)
  select 0,'6',1
  from dual;
  --
  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 5,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShowForMail';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 3,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShowForTrust';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 4,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShowForOldInvoice';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 1,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 952 and a.typeid = 'ctpdBillShowForInvoice';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 2,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 300 and a.typeid = 'ctpdBillShow';

  insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
  select 6,a.itemid,a.itemname,a.sortorder,0
  from trans_clientbill_def_typeitem a
  where a.region = 951 and a.typeid = 'ctpdBillShowForInvoice';
  dbms_output.put_line('1.........');

  v_order := 0;
  for c in (select * from trans_feetype  where usagetype = 'feutCll' order by payorder asc) loop
  dbms_output.put_line('2.........');
    v_order := v_order + 1;
    insert into  CUSTBILL_CELL_DEF(TEMPLATE_ID,CELL_ID,CELL_NAME,SORT_ORDER,CELL_TYPE)
    values(0,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),c.itemname,v_order,0);
  end loop;

  --
  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select b.oid,a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_define_type b
  where b.region = 300 and b.typeid in ('ctpdBillShowForMail','ctpdBillShowForTrust','ctpdBillShowForOldInvoice','ctpdBillShow')
  and a.typeid = b.typeid
  and a.region = b.region;

  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select decode(b.typeid,'ctpdBillShowForInvoice',1,5),a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_def_typeitem b
  where b.region = 952 and b.typeid in ('ctpdBillShowForInvoice','ctpdBillShow')
  and a.typeid = b.typeid
  and a.region = b.region
  and a.itemid = b.itemid;

  insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
  select 6,a.itemid,1,decode(a.feetypeid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',a.feetypeid),'',2,'',''
  from trans_clientbilldef_itemfee a,trans_clientbill_def_typeitem b
  where b.region = 951 and b.typeid in ('ctpdBillShowForInvoice')
  and a.typeid = b.typeid
  and a.region = b.region
  and a.itemid = b.itemid;
  dbms_output.put_line('3.........');

  for c in (select * from trans_feetype  where usagetype = 'feutCll' order by payorder asc) loop
  dbms_output.put_line('4.........');
    insert into  BILLITEM_CELL_DEF(TEMPLATE_ID,CELL_ID,ITEMTYPE,ITEMCODE,ITEMADDCODE,ISWRTOFFED,INCL_SUBJECTID,EXCL_SUBJECTID)
    values(0,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),
           1,decode(c.itemid,'SMS','SMSFEE','WLAN','WLANFEE','INFOFEE_FEE3','FEE3INFO',c.itemid),
           '',2,'','');
  end loop;
end ap_trans_dict_custbill;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_AUDIT" (
file_name varchar2, --文件名,带%模糊查询
sect_str varchar2,  --阶段串,"-1"汇总稽核,分阶段稽核串型如"'sect1','sect2','sect3'"
begindate number,   --开始日期
enddate number      --结束日期
)
as
v_file_name varchar2(128);
v_tmp_sect  varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';
begin
  v_file_name := file_name;
  if(file_name is null) then
    v_file_name := '%';
  end if;
  if(trim(sect_str)='-1') then  --汇总稽核
    --遍历汇总表
    for c1 in(select * from CDR_BALANCECHECK where filename like v_file_name
                and  procdate>=begindate and procdate<=enddate)
    loop
	dbms_output.put_line('1.........');
        --针对汇总表记录,求其分阶段统计数据插入临时表
        insert into WORK_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
            HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
            NOCHARGECOUNT,REJECTCOUNT)
        select c1.procdate,c1.filename,'-1',c1.stdcount,
          nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
          nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from CDR_BALANCECHECK_DETAIL
          where procdate=c1.procdate and 
          filename=c1.filename and flow_type=1
            and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
    end loop;
  else  --分阶段稽核
  dbms_output.put_line('2.........');
    v_tmp_sect  := sect_str;
    --如果v_tmp_sect不是以','结尾,则主动添加一个,方便下面的算法
    if(substr(v_tmp_sect,length(v_tmp_sect)-1,1)<>',') then
        v_tmp_sect := v_tmp_sect||',';
    end if;
    v_tmp_pos   := instr(v_tmp_sect,',');
    while(v_tmp_pos > 0)
    loop
	dbms_output.put_line('3.........');
        v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);
        --求到
        for c2 in(select procdate,filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
            nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
            nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
            from CDR_BALANCECHECK_DETAIL where procdate>=begindate and procdate<=enddate and filename like v_file_name
            and sect_src=v_sect_param and flow_type=1 group by procdate,filename,sect_src)
        loop
		dbms_output.put_line('4.........');
            insert into WORK_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
              HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
              NOCHARGECOUNT,REJECTCOUNT)
            values(c2.procdate,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
            c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT);
        end loop;
        if(v_sect_param='01_SECTION_SEP') then
            for c3 in(select * from CDR_BALANCECHECK
                    where procdate>=begindate and procdate<=enddate and filename like v_file_name)
            loop
			dbms_output.put_line('5.........');
                update WORK_CDR_BALANCECHECK set all_count=c3.stdcount
                    where procdate=c3.procdate and filename=c3.filename and sect_code='01_SECTION_SEP';
            end loop;
        end if;
        if(v_sect_param='09_SECTION_ROAMOUT') then
            for c3 in(select * from CDR_BALANCECHECK
                    where procdate>=begindate and procdate<=enddate and filename like v_file_name)
            loop
			dbms_output.put_line('6.........');
                update WORK_CDR_BALANCECHECK set all_count=c3.stdcount
                    where procdate=c3.procdate and filename=c3.filename and sect_code='09_SECTION_ROAMOUT';
            end loop;
        end if;
        for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
        nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
        all_count from CDR_BALANCECHECK_DETAIL where filename like v_file_name and procdate>=begindate
        and procdate<=enddate and sect_dst=v_sect_param and flow_type=1
        group by procdate,filename,sect_dst)
        loop
		dbms_output.put_line('7.........');
            update WORK_CDR_BALANCECHECK set all_count=c1.all_count
                where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
        end loop;
        v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
        v_tmp_pos    := instr(v_tmp_sect,',');
    end loop;
  end if;
end;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_MON" (
proc_date varchar2 --查询日期:'20060420'
)
as
begin

     --清除旧数据  
     --truncate table MON_CDR_BALANCECHECK;
     --保留7天内数据
     --delete MON_CDR_BALANCECHECK where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
     --遍历汇总表
     for c1 in(select * from CDR_BALANCECHECK where  procdate=proc_date)
     loop
        dbms_output.put_line('1.........');
        --针对汇总表记录,求其分阶段统计数据插入临时表
        insert into MON_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
            HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
            NOCHARGECOUNT,REJECTCOUNT)
        select c1.procdate,c1.filename,null,c1.stdcount,
          nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
          nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from CDR_BALANCECHECK_DETAIL
          where procdate=c1.procdate and 
          filename=c1.filename and flow_type=1
            and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
       
     end loop;
     --把平衡的记录置上标志
     update MON_CDR_BALANCECHECK set sect_code='ok' where all_count=HOMECOUNT+EMPTYCOUNT+ERRORCOUNT+RMHOMECOUNT+RMEMPTYCOUNT+RMERRORCOUNT+
            NOCHARGECOUNT+REJECTCOUNT;
	dbms_output.put_line('2.........');
     commit;
end;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_MON_BY_NAME" (
file_name varchar2, --filename
check_id  number  --1=汇总稽核;0=分阶段稽核
)
as
v_tmp_sect  varchar2(1024) := '';
v_sect   varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';
v_sepin        number(14);
v_sepout      number(14);
v_chargein        number(14);
v_chargeout      number(14);
v_billin        number(14);
v_billout      number(14);
v_transin        number(14);
v_transout      number(14);
v_flag          char(1);
v_procdate number(12);
begin
    for sec in (select code from code_dict where catalog='T_FLOW_SECTION')
    loop
        v_sect:=v_sect||sec.code||',';
    end loop;


    if(check_id=1) then  --汇总稽核
        --清除旧数据
        delete MON_CDR_BALANCECHECK where procdate='20080229' and filename=file_name;
        --遍历汇总表
        for c1 in(select * from CDR_BALANCECHECK where   filename=file_name)
        loop         
            --针对汇总表记录,求其分阶段统计数据插入临时表
			dbms_output.put_line('1..........');
            insert into MON_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
                HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                NOCHARGECOUNT,REJECTCOUNT)
            select c1.procdate,c1.filename,null,c1.stdcount,
                nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
                nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from CDR_BALANCECHECK_DETAIL
                where procdate=c1.procdate and 
                filename=c1.filename and flow_type=1
                and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
        end loop;
        --把平衡的记录置上标志
        update MON_CDR_BALANCECHECK set sect_code='ok' where procdate='20080229' 
            and filename=file_name
            /*            and 
            all_count=HOMECOUNT+EMPTYCOUNT+ERRORCOUNT+RMHOMECOUNT+RMEMPTYCOUNT+RMERRORCOUNT+
            NOCHARGECOUNT+REJECTCOUNT*/ ;
        commit;
        
    else  --分阶段稽核
	dbms_output.put_line('2..........');
        delete from MON_CDR_BALANCECHECK_STEP_SUM where filename=file_name ;
        delete from MON_CDR_BALANCECHECK_STEP where filename=file_name;
          
            v_tmp_sect  := v_sect;
            v_tmp_pos   := instr(v_tmp_sect,',');
            --求到
            select max(procdate) into v_procdate from CDR_BALANCECHECK_DETAIL where filename=file_name ;
            for c2 in(select filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
                    nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
                    nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
                    from CDR_BALANCECHECK_DETAIL where --and sect_dst<>'00_SECTION_IGNORE'
                    filename=file_name 
                    and flow_type=1 group by filename,sect_src)
            loop
			dbms_output.put_line('3..........');
                    insert into MON_CDR_BALANCECHECK_STEP(procdate,filename,sect_code,all_count,
                      HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                      NOCHARGECOUNT,REJECTCOUNT)
                    values(v_procdate,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
                    c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT);
            end loop;
   --dbms_output.put_line(v_sect_param);         
    --         commit;
                   
            while(v_tmp_pos > 0)
            loop
			dbms_output.put_line('4..........');
                v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);
                
               if(v_sect_param in ('01_SECTION_SEP','09_SECTION_ROAMOUT')) then
                    for c3 in(select * from CDR_BALANCECHECK
                            where filename=file_name)
                    loop
                        update MON_CDR_BALANCECHECK_STEP  set all_count=c3.stdcount
                            where procdate=c3.procdate and filename=c3.filename and 
                            sect_code=v_sect_param;
                    end loop;
               end if;
             
        --dbms_output.put_line('ok');            
        --     commit;          
             
                for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
                nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
                all_count from CDR_BALANCECHECK_DETAIL where --filename like v_file_name and 
                filename=file_name and sect_dst=v_sect_param and flow_type=1
                group by procdate,filename,sect_dst)
                loop
				dbms_output.put_line('5..........');
                    update MON_CDR_BALANCECHECK_STEP set all_count=c1.all_count
                       where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
                end loop;
                  
    --dbms_output.put_line(v_sect_param); 

                v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
                v_tmp_pos    := instr(v_tmp_sect,',');
               
            end loop;
        commit;
        
        for c4 in(select  * from CDR_BALANCECHECK where filename=file_name)
        loop
		dbms_output.put_line('6..........');
            insert into MON_CDR_BALANCECHECK_STEP_SUM (procdate,filename) values(c4.procdate,c4.filename);
            begin
                select all_count , homecount into v_sepin , v_sepout from MON_CDR_BALANCECHECK_STEP where 
                sect_code='01_SECTION_SEP' and filename=c4.filename;
                exception  when no_data_found then         
			              v_sepin :=0;
                    v_sepout :=0;
            end  ;
            begin
            select all_count , homecount into v_chargein , v_chargeout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='02_SECTION_CHARGE' and filename=c4.filename;
            exception when no_data_found then
			          v_chargein :=0;
                v_chargeout :=0;
            end  ;
            begin
            select all_count , homecount into v_billin , v_billout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='03_SECTION_BILLING' and filename=c4.filename ;
            exception when no_data_found then
			          v_billin :=0;
                v_billout :=0;
            end  ;
            begin
            select all_count , homecount into v_transin , v_transout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='04_SECTION_TRANS' and filename=c4.filename ;
            exception when no_data_found then
			          v_transin :=0;
                v_transout :=0;
            end  ;

            if ( v_sepout <> v_chargeout or v_chargeout<>v_billout or v_billout<>v_transout)  then      
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;
      
            update MON_CDR_BALANCECHECK_STEP_SUM set sepin=v_sepin, sepout=v_sepout ,
            chargein=v_chargein, chargeout=v_chargeout, billin=v_billin, billout=v_billout,
            transin=v_transin, transout=v_transout, flag=v_flag
            where filename=c4.filename ;
           
        end loop;
               
        commit;
    end if;
end;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_PROC" (proc_date varchar2 --处理日期,型如'20050601',不带时间
                                                    ) as
    v_sect_src     varchar2(64) := '-'; --源阶段
    v_sect_dst     varchar2(64) := '-'; --目标阶段
    v_sect_tmp     varchar2(64) := '-'; --临时阶段,用来翻转源目标阶段用
    v_log_field    varchar2(64) := '-'; --event_flag对应的表中的字段
    v_update_sql   varchar2(1024) := '-'; --动态更新统计表的SQL
    v_reverse_flag number(1) := 0; --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
    v_calc_factor  number(1) := 1; --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
    v_flow_type    number(4) := 0;
    v_event_flag   varchar2(20) := '-';
begin

    --遍历分拣/漫游来访文件
    for c1 in (select filename, recordnum, stdcount, errcount
                 from sepfilelog
                where beginprocessdate >= to_date(proc_date, 'yyyymmdd')
                  and beginprocessdate < to_date(proc_date, 'yyyymmdd') + 1
               union
               select filename,
                      billcount recordnum,
                      billcount stdcount,
                      errcount
                 from visittranslog
                where trunc(processtime) = to_date(proc_date, 'yyyymmdd')) loop
		dbms_output.put_line('1.........');
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_balancecheck
                (procdate, filename, recordnum, stdcount, errcount)
            values
                (proc_date,
                 c1.filename,
                 c1.recordnum,
                 c1.stdcount,
                 c1.errcount);
        
            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            for flow_row in (select entry_module_id,
                                    module_id,
                                    flow_id,
                                    file_event_count sumcount
                               from v_event_flow_log_sourfile
                              where source_name = c1.filename) loop
                --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
				dbms_output.put_line('2.........');
                select nvl(max(flow_section), flow_row.module_id)
                  into v_sect_src
                  from event_flow_module
                 where module_id = flow_row.entry_module_id;
                --为防止某个分流被删除导致无法统计，先设置缺省值，默认为丢弃。
                v_event_flag := 'LOSER';
                v_flow_type  := 1;
                v_sect_dst   := '08_SECTION_TERM';
                select event_flag,
                       flow_type,
                       nvl(next_section,
                           flow_row.module_id || ',' || flow_row.flow_id)
                  into v_event_flag, v_flow_type, v_sect_dst
                  from event_flow_def
                 where module_id = flow_row.module_id
                   and flow_id = flow_row.flow_id;
                --判断源阶段是否是翻转阶段
                select count(*)
                  into v_reverse_flag
                  from code_dict
                 where catalog = 'T_SECTION_REVERSE'
                   and code = v_sect_src;
                if (v_reverse_flag > 0) then
                    if (v_event_flag = 'CHOME') then
					dbms_output.put_line('3.........');
                        select case v_sect_src
                                   when '90_REPROC_EMPTY' then
                                    'EMPTY'
                                   when '91_REPROC_ERROR' then
                                    'ERROR'
                                   when '93_REPROC_NOCHARGE' then
                                    'WAIFS'
                                   else
                                    'CHOME'
                               end
                          into v_event_flag
                          from dual;
                    end if;
                    --实现阶段翻转
                    v_sect_tmp    := v_sect_dst;
                    v_sect_dst    := v_sect_src;
                    v_sect_src    := v_sect_tmp;
                    v_calc_factor := -1; --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor := 1;
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
                    select logfield
                      into v_log_field
                      from EVENT_FLAG_DEF
                     where code = v_event_flag;
                    v_update_sql := 'update CDR_BALANCECHECK_DETAIL_GLOBAL set ' ||
                                    v_log_field || '=' || v_log_field ||
                                    '+ :sumcount*(:v_calc_factor) ' ||
                                    'where procdate= :proc_date and filename= :filename ' ||
                                    'and sect_src= :v_sect_src and sect_dst= :v_sect_dst ' ||
                                    'and flow_type= :v_flow_type';
                    execute immediate (v_update_sql)
                        using flow_row.sumcount, v_calc_factor, proc_date, c1.filename, v_sect_src, v_sect_dst, v_flow_type;
                
				dbms_output.put_line('4.........');
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount = 0 then
                        insert into CDR_BALANCECHECK_DETAIL_GLOBAL
                            (procdate,
                             filename,
                             sect_src,
                             sect_dst,
                             flow_type)
                        values
                            (proc_date,
                             c1.filename,
                             v_sect_src,
                             v_sect_dst,
                             v_flow_type);
                        --execute immediate(v_update_sql);
						dbms_output.put_line('5.........');
                        execute immediate (v_update_sql)
                            using flow_row.sumcount, v_calc_factor, proc_date, c1.filename, v_sect_src, v_sect_dst, v_flow_type;
                    
                    end if;
                
                exception
                    when others then
                        null;
                end;
            end loop;
            INSERT INTO CDR_BALANCECHECK_DETAIL
                   SELECT * FROM CDR_BALANCECHECK_DETAIL_GLOBAL
                    WHERE FILENAME=c1.filename;

            commit;
        
        exception
            when others then
            null;
        end;
    end loop;
    --
    commit;
end CDR_BALANCECHECK_PROC;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_PROC_BY_NAME" (
file_name varchar2  --处理文件名
) as
v_sect_src      varchar2(64)    := '-';      --源阶段
v_sect_dst      varchar2(64)    := '-';      --目标阶段
v_sect_tmp      varchar2(64)    := '-';      --临时阶段,用来翻转源目标阶段用
v_log_field    varchar2(64)    := '-';       --event_flag对应的表中的字段
v_update_sql    varchar2(1024)  := '-';      --动态更新统计表的SQL
v_reverse_flag  number(1)       := 0;       --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
v_calc_factor   number(1)       := 1;       --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
v_flow_type     number(4)       := 0;
v_event_flag    varchar2(20)    := '-';
v_num number(10) :=0;
v_ins_sql    varchar2(1024)  := '-';
begin
    begin
        --清理历史数据
       delete cdr_balancecheck where filename=file_name;
       delete cdr_balancecheck_detail where filename=file_name;
       commit;
   exception when others then
        rollback;
        raise_application_error(-20101,sqlerrm);
        return;
   end;
dbms_output.put_line('++++++++++++++++++++++++++++++'); 
dbms_output.put_line(file_name);   
   --遍历分拣/漫游来访文件
   for c1 in(
            select filename,recordnum,stdcount,errcount,to_number(to_char(beginprocessdate,'yyyymmdd')) proc_date from sepfilelog
                where filename=file_name
                
            union
            select filename,billcount recordnum,billcount stdcount,errcount,to_number(to_char(processtime,'yyyymmdd')) proc_date from visittranslog
                where filename=file_name    
            )
    loop
	dbms_output.put_line('1..........');
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_balancecheck(procdate,filename,recordnum,stdcount,errcount)
                values(c1.proc_date, c1.filename, c1.recordnum, c1.stdcount, c1.errcount);

            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            --select count(*) into v_num from v_event_flow_log_sourfile where source_name=c1.filename;
            --dbms_output.put_line('sql:'||v_num);
            
            for flow_row in (select entry_module_id,module_id,flow_id,file_event_count sumcount
                                from v_event_flow_log_sourfile where source_name=c1.filename)
                                --and module_id=1002 and flow_id=155)--order by module_id)
                              --and entry_module_id<>10)
            loop
            dbms_output.put_line('2..........');
                 --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
                select nvl(max(flow_section),flow_row.module_id) into v_sect_src
                    from event_flow_module where module_id=flow_row.entry_module_id;
                                                   
                begin
                    select event_flag,flow_type,nvl(next_section,flow_row.module_id||','||flow_row.flow_id)
                    into v_event_flag,v_flow_type,v_sect_dst
                    from event_flow_def where module_id=flow_row.module_id and flow_id=flow_row.flow_id;
                exception when others then
                    v_event_flag := 'LOSER';
                    v_flow_type  := 1;   
                    v_sect_dst   := '08_SECTION_TERM';     
                
            --dbms_output.put_line(v_ins_sql||'...'||v_event_flag||','||v_flow_type||','|| v_sect_dst);
            --v_num:=-100; 
                     null;
                end;
                
              --if v_num=-100 then
              --dbms_output.put_line('sql:'||v_num);
              --v_num:=0;
              --end if;
                --判断源阶段是否是翻转阶段
                select count(*) into v_reverse_flag from code_dict
                    where catalog='T_SECTION_REVERSE' and code=v_sect_src;
                if(v_reverse_flag > 0) then
                    if(v_event_flag='CHOME') then
					dbms_output.put_line('3..........');
                        select case v_sect_src 
                             when '90_REPROC_EMPTY' then 'EMPTY'
                             when '91_REPROC_ERROR' then 'ERROR'
                             when '93_REPROC_NOCHARGE' then 'WAIFS'
                             else 'CHOME' end 
                        into v_event_flag 
                        from dual; 
                    end if;
                    --实现阶段翻转
                    v_sect_tmp      := v_sect_dst;
                    v_sect_dst      := v_sect_src;
                    v_sect_src      := v_sect_tmp;
                    --v_sect_dst := '08_REPROC_TERM'
                    v_calc_factor   := -1;  --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor  := 1;
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
				dbms_output.put_line('4..........');
                    select logfield into v_log_field from EVENT_FLAG_DEF where code=v_event_flag;                  
                    v_update_sql := 'update CDR_BALANCECHECK_DETAIL set '
                        ||v_log_field||'='||v_log_field||'+'||flow_row.sumcount*(v_calc_factor)||' '
                        ||'where procdate='''||c1.proc_date||''' and filename='''||c1.filename||''' '
                        ||'and sect_src='''||v_sect_src||''' and sect_dst='''||v_sect_dst||''' '
                        ||'and flow_type='||v_flow_type;
                        
                    --if  (flow_row.module_id=1002 and flow_row.flow_id=155) then
                    --end if;
                    execute immediate(v_update_sql);
                     
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
					dbms_output.put_line('5..........');
                        --v_ins_sql := 'insert into CDR_BALANCECHECK_DETAIL (procdate,filename,sect_src,sect_dst,flow_type) '
                        --||'values('||c1.proc_date||','''||c1.filename||''','''||v_sect_src||''','''
                        --||v_sect_dst||''','||v_flow_type||')';     
                        insert into CDR_BALANCECHECK_DETAIL(procdate,filename,sect_src,sect_dst,flow_type)
                                values(c1.proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);    
                        --dbms_output.put_line('sql:'||v_update_sql);        
                        execute immediate(v_update_sql);  
                    end if;
                exception when others then               
                    null;
                end;
            end loop;
            commit;
        exception when others then
            --rollback;
            null;
            --dbms_output.put_line(v_ins_sql); 
            --raise_application_error(-20101,sqlerrm);
            
            return;
        end;
    end loop;
end CDR_BALANCECHECK_PROC_BY_NAME;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_PROC_J" (proc_date varchar2 --处理日期,型如'20050601',不带时间
                                                    ) as
    v_sect_src     varchar2(64) := '-'; --源阶段
    v_sect_dst     varchar2(64) := '-'; --目标阶段
    v_sect_tmp     varchar2(64) := '-'; --临时阶段,用来翻转源目标阶段用
    v_log_field    varchar2(64) := '-'; --event_flag对应的表中的字段
    v_update_sql   varchar2(1024) := '-'; --动态更新统计表的SQL
    v_reverse_flag number(1) := 0; --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
    v_calc_factor  number(1) := 1; --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
    v_flow_type    number(4) := 0;
    v_event_flag   varchar2(20) := '-';
begin

    --遍历分拣/漫游来访文件
    for c1 in (select filename, recordnum, stdcount, errcount
                 from sepfilelog
                where beginprocessdate >= to_date(proc_date, 'yyyymmdd')
                  and beginprocessdate < to_date(proc_date, 'yyyymmdd') + 1
               union
               select filename,
                      billcount recordnum,
                      billcount stdcount,
                      errcount
                 from visittranslog
                where trunc(processtime) = to_date(proc_date, 'yyyymmdd')) loop
				dbms_output.put_line('1..........');
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_balancecheck
                (procdate, filename, recordnum, stdcount, errcount)
            values
                (proc_date,
                 c1.filename,
                 c1.recordnum,
                 c1.stdcount,
                 c1.errcount);
        
            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            for flow_row in (select entry_module_id,
                                    module_id,
                                    flow_id,
                                    file_event_count sumcount
                               from v_event_flow_log_sourfile
                              where source_name = c1.filename) loop
                --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
				dbms_output.put_line('2..........');
                select nvl(max(flow_section), flow_row.module_id)
                  into v_sect_src
                  from event_flow_module
                 where module_id = flow_row.entry_module_id;
                --为防止某个分流被删除导致无法统计，先设置缺省值，默认为丢弃。
                v_event_flag := 'LOSER';
                v_flow_type  := 1;
                v_sect_dst   := '08_SECTION_TERM';
                select event_flag,
                       flow_type,
                       nvl(next_section,
                           flow_row.module_id || ',' || flow_row.flow_id)
                  into v_event_flag, v_flow_type, v_sect_dst
                  from event_flow_def
                 where module_id = flow_row.module_id
                   and flow_id = flow_row.flow_id;
                --判断源阶段是否是翻转阶段
                select count(*)
                  into v_reverse_flag
                  from code_dict
                 where catalog = 'T_SECTION_REVERSE'
                   and code = v_sect_src;
                if (v_reverse_flag > 0) then
                    if (v_event_flag = 'CHOME') then
					dbms_output.put_line('3..........');
                        select case v_sect_src
                                   when '90_REPROC_EMPTY' then
                                    'EMPTY'
                                   when '91_REPROC_ERROR' then
                                    'ERROR'
                                   when '93_REPROC_NOCHARGE' then
                                    'WAIFS'
                                   else
                                    'CHOME'
                               end
                          into v_event_flag
                          from dual;
                    end if;
                    --实现阶段翻转
                    v_sect_tmp    := v_sect_dst;
                    v_sect_dst    := v_sect_src;
                    v_sect_src    := v_sect_tmp;
                    v_calc_factor := -1; --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor := 1;
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
                    select logfield
                      into v_log_field
                      from EVENT_FLAG_DEF
                     where code = v_event_flag;
                    v_update_sql := 'update CDR_BALANCECHECK_DETAIL_GLOBAL set ' ||
                                    v_log_field || '=' || v_log_field ||
                                    '+ :sumcount*(:v_calc_factor) ' ||
                                    'where procdate= :proc_date and filename= :filename ' ||
                                    'and sect_src= :v_sect_src and sect_dst= :v_sect_dst ' ||
                                    'and flow_type= :v_flow_type';
                    execute immediate (v_update_sql)
                        using flow_row.sumcount, v_calc_factor, proc_date, c1.filename, v_sect_src, v_sect_dst, v_flow_type;
						dbms_output.put_line('4..........');
                
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount = 0 then
					dbms_output.put_line('5..........');
                        insert into CDR_BALANCECHECK_DETAIL_GLOBAL
                            (procdate,
                             filename,
                             sect_src,
                             sect_dst,
                             flow_type)
                        values
                            (proc_date,
                             c1.filename,
                             v_sect_src,
                             v_sect_dst,
                             v_flow_type);
                        --execute immediate(v_update_sql);
                        execute immediate (v_update_sql)
                            using flow_row.sumcount, v_calc_factor, proc_date, c1.filename, v_sect_src, v_sect_dst, v_flow_type;
                    
                    end if;
                
                exception
                    when others then
                        null;
                end;
            end loop;
            INSERT INTO CDR_BALANCECHECK_DETAIL
                   SELECT * FROM CDR_BALANCECHECK_DETAIL_GLOBAL
                    WHERE FILENAME=c1.filename;

            commit;
        
        exception
            when others then
                --rollback;
                --dbms_output.put_line('sql:'||v_update_sql);
                --raise_application_error(-20101,sqlerrm);
                --return;
                null;
        end;
    end loop;
    --
    commit;
end CDR_BALANCECHECK_PROC_J;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_PROC_SINGLE" (
    proc_date varchar2,  --处理日期,型如'20050601',不带时间
    av_filename varchar2
) as
    v_sect_src      varchar2(64)    := '-';      --源阶段
    v_sect_dst      varchar2(64)    := '-';      --目标阶段
    v_sect_tmp      varchar2(64)    := '-';      --临时阶段,用来翻转源目标阶段用
    v_log_field    varchar2(64)    := '-';       --event_flag对应的表中的字段
    v_update_sql    varchar2(1024)  := '-';      --动态更新统计表的SQL
    v_reverse_flag  number(1)       := 0;       --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
    v_calc_factor   number(1)       := 1;       --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
    v_flow_type     number(4)       := 0;
    v_event_flag    varchar2(20)    := '-';
begin
    --
    EXECUTE IMMEDIATE 'truncate table cdr_balancecheck_single';
    EXECUTE IMMEDIATE 'truncate table CDR_CHECK_DETAIL_SINGLE';
    EXECUTE IMMEDIATE 'truncate table MON_CDR_BALANCECHECK_SINGLE';
    EXECUTE IMMEDIATE 'truncate table MON_CDR_CHECK_STEP_SUM_SINGLE';
    
    --遍历分拣/漫游来访文件
    for c1 in(
            select /*+index (t PK_SEPFILELOG)*/ filename,recordnum,stdcount,errcount from sepfilelog t
                where beginprocessdate>=to_date(proc_date,'yyyymmdd')
                     and beginprocessdate<to_date(proc_date,'yyyymmdd')+1
                     and filename=av_filename
             union all
             select filename,billcount recordnum,billcount stdcount,errcount from visittranslog
                where trunc(processtime)=to_date(proc_date,'yyyymmdd'))
             
    loop
	dbms_output.put_line('1..........');
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_balancecheck_single(procdate,filename,recordnum,stdcount,errcount)
                values(proc_date, c1.filename, c1.recordnum, c1.stdcount, c1.errcount);
            --
            commit;
            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            for flow_row in (select entry_module_id,module_id,flow_id,file_event_count sumcount
                                from v_event_flow_log_sourfile_0412 /****************************/
                                where source_name=c1.filename
                            )
            loop
			dbms_output.put_line('2..........');
                --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
                select nvl(max(flow_section),flow_row.module_id) into v_sect_src
                    from event_flow_module where module_id=flow_row.entry_module_id;
                --为防止某个分流被删除导致无法统计，先设置缺省值，默认为丢弃。
                v_event_flag := 'LOSER';
                v_flow_type  := 1;
                v_sect_dst   := '08_SECTION_TERM';
                select event_flag,flow_type,nvl(next_section,flow_row.module_id||','||flow_row.flow_id)
                    into v_event_flag,v_flow_type,v_sect_dst
                        from event_flow_def where module_id=flow_row.module_id and flow_id=flow_row.flow_id;
                --判断源阶段是否是翻转阶段
                select count(*) into v_reverse_flag from code_dict
                    where catalog='T_SECTION_REVERSE' and code=v_sect_src;
                if(v_reverse_flag > 0) then
                    if(v_event_flag='CHOME') then
					dbms_output.put_line('3..........');
                        select case v_sect_src
                             when '90_REPROC_EMPTY' then 'EMPTY'
                             when '91_REPROC_ERROR' then 'ERROR'
                             when '93_REPROC_NOCHARGE' then 'WAIFS'
                             else 'CHOME' end
                        into v_event_flag
                        from dual;
                    end if;
                    --实现阶段翻转
                    v_sect_tmp      := v_sect_dst;
                    v_sect_dst      := v_sect_src;
                    v_sect_src      := v_sect_tmp;
                    v_calc_factor   := -1;  --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor  := 1;
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
                    select logfield into v_log_field from EVENT_FLAG_DEF where code=v_event_flag;
                    v_update_sql := 'update CDR_CHECK_DETAIL_SINGLE set '
                        ||v_log_field||'='||v_log_field||'+ :sumcount*(:v_calc_factor) '
                        ||'where procdate= :proc_date and filename= :filename '
                        ||'and sect_src= :v_sect_src and sect_dst= :v_sect_dst '
                        ||'and flow_type= :v_flow_type';
                    execute immediate(v_update_sql) using flow_row.sumcount,v_calc_factor,proc_date,
                    c1.filename,v_sect_src,v_sect_dst,v_flow_type;
					dbms_output.put_line('4..........');

                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
					dbms_output.put_line('5..........');
                         insert into CDR_CHECK_DETAIL_SINGLE(procdate,filename,sect_src,sect_dst,flow_type)
                                values(proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);
                         --execute immediate(v_update_sql);
                         execute immediate(v_update_sql) using flow_row.sumcount,v_calc_factor,proc_date,
                         c1.filename,v_sect_src,v_sect_dst,v_flow_type;
                    end if;
                    --
                    commit;


                exception when others then
                    null;
                end;
            end loop;
            commit;
        exception when others then
            --rollback;
            --raise_application_error(-20101,sqlerrm);
            --return;
            null;
        end;
    end loop;
end CDR_BALANCECHECK_PROC_SINGLE;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_STEP_MON" (
proc_date number ,--查询日期:'20060420'
check_id  number  --1=汇总稽核;0=分阶段稽核
)
as
v_tmp_sect  varchar2(1024) := '';
v_sect   varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';

v_sepin        number(14);
v_sepout      number(14);
v_seperr      number(14);
v_chargein        number(14);
v_chargeout      number(14);
v_chargeerr      number(14);
v_billin        number(14);
v_billout      number(14);
v_billerr      number(14);
v_transin        number(14);
v_transout      number(14);
v_transerr     number(14);
v_roamin        number(14);
v_roamout      number(14);
v_roamerr     number(14);
v_flag          char(1);

begin
    
    for sec in (select code from code_dict where catalog='T_FLOW_SECTION')
    loop
        v_sect:=v_sect||sec.code||',';
    end loop;


    if(check_id=1) then  --汇总稽核
        --清除旧数据,保留7天内数据
        --delete MON_CDR_BALANCECHECK where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        --遍历汇总表
        for c1 in(select * from CDR_BALANCECHECK where  procdate=proc_date)
        loop         
            dbms_output.put_line('1..........');
			--针对汇总表记录,求其分阶段统计数据插入临时表
            insert into MON_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
                HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                NOCHARGECOUNT,REJECTCOUNT)
            select c1.procdate,c1.filename,null,c1.stdcount,
                nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
                nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from CDR_BALANCECHECK_DETAIL
                where procdate=c1.procdate and 
                filename=c1.filename and flow_type=1
                and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
        end loop;
        --把平衡的记录置上标志
        update MON_CDR_BALANCECHECK set sect_code='ok' where procdate=proc_date and 
            all_count=HOMECOUNT+EMPTYCOUNT+ERRORCOUNT+RMHOMECOUNT+RMEMPTYCOUNT+RMERRORCOUNT+
            NOCHARGECOUNT+REJECTCOUNT;
        commit;
    
    else  --分阶段稽核
        --delete from MON_CDR_BALANCECHECK_STEP_SUM where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        --delete from MON_CDR_BALANCECHECK_STEP where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
		dbms_output.put_line('2..........');
          
            v_tmp_sect  := v_sect;
            v_tmp_pos   := instr(v_tmp_sect,',');
            --求到
            
            for c2 in(select filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
                    nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
                    nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
                    from CDR_BALANCECHECK_DETAIL where procdate=proc_date --and sect_dst<>'00_SECTION_IGNORE'
                    --and filename=c1.filename 
                    and flow_type=1 group by filename,sect_src)
            loop
			dbms_output.put_line('3..........');
                    insert into MON_CDR_BALANCECHECK_STEP(procdate,filename,sect_code,all_count,
                      HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                      NOCHARGECOUNT,REJECTCOUNT)
                    values(proc_date,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
                    c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT);
            end loop;
   --dbms_output.put_line(v_sect_param);         
    --         commit;
                   
            while(v_tmp_pos > 0)
            loop
			dbms_output.put_line('4..........');
                v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);
                
               if(v_sect_param in ('01_SECTION_SEP','09_SECTION_ROAMOUT')) then
                    for c3 in(select * from CDR_BALANCECHECK
                            where procdate=proc_date)
                    loop
					dbms_output.put_line('5..........');
                        update MON_CDR_BALANCECHECK_STEP  set all_count=c3.stdcount
                            where procdate=c3.procdate and filename=c3.filename and 
                            sect_code=v_sect_param;
                    end loop;
               end if;
             
        --dbms_output.put_line('ok');            
        --     commit;          
             
                for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
                nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
                all_count from CDR_BALANCECHECK_DETAIL where --filename like v_file_name and 
                procdate=proc_date and sect_dst=v_sect_param and flow_type=1
                group by procdate,filename,sect_dst)
                loop
				dbms_output.put_line('6..........');
                    update MON_CDR_BALANCECHECK_STEP set all_count=c1.all_count
                       where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
                end loop;
                  
    --dbms_output.put_line(v_sect_param); 

                v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
                v_tmp_pos    := instr(v_tmp_sect,',');
               
            end loop;
        commit;
      /*  
        for c4 in(select  * from CDR_BALANCECHECK where procdate=proc_date)
        loop
            insert into MON_CDR_BALANCECHECK_STEP_SUM (procdate,filename) values(proc_date,c4.filename);
            begin
                select all_count , homecount into v_sepin , v_sepout from MON_CDR_BALANCECHECK_STEP where 
                sect_code='01_SECTION_SEP' and filename=c4.filename and procdate=proc_date;
                exception  when no_data_found then         
			              v_sepin :=0;
                    v_sepout :=0;
            end  ;
            begin
            select all_count , homecount into v_chargein , v_chargeout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='02_SECTION_CHARGE' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_chargein :=0;
                v_chargeout :=0;
            end  ;
            begin
            select all_count , homecount into v_billin , v_billout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='03_SECTION_BILLING' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_billin :=0;
                v_billout :=0;
            end  ;
            begin
            select all_count , homecount into v_transin , v_transout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='04_SECTION_TRANS' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_transin :=0;
                v_transout :=0;
            end  ;

            if ( v_sepout <> v_chargeout or v_chargeout<>v_billout or v_billout<>v_transout)  then      
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;
      
            update MON_CDR_BALANCECHECK_STEP_SUM set sepin=v_sepin, sepout=v_sepout ,
            chargein=v_chargein, chargeout=v_chargeout, billin=v_billin, billout=v_billout,
            transin=v_transin, transout=v_transout, flag=v_flag
            where filename=c4.filename and procdate=proc_date;
           
        end loop;
*/
        for c4 in(select  * from CDR_BALANCECHECK where procdate=proc_date )
        loop
		dbms_output.put_line('7..........');
            insert into MON_CDR_BALANCECHECK_STEP_SUM (procdate,filename) values(proc_date,c4.filename);
            begin
                select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
                 into v_sepin , v_sepout, v_seperr from MON_CDR_BALANCECHECK_STEP where 
                sect_code='01_SECTION_SEP' and filename=c4.filename and procdate=proc_date;
                exception  when no_data_found then         
			              v_sepin :=0;
                    v_sepout :=0;
                    v_seperr :=0;
            end  ;
            
            begin
            select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT 
             into v_chargein, v_chargeout, v_chargeerr from MON_CDR_BALANCECHECK_STEP where 
            sect_code='02_SECTION_CHARGE' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_chargein :=0;
                v_chargeout :=0;
                v_chargeerr :=0;
            end  ;
            
            begin
            select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
             into v_billin, v_billout,v_billerr from MON_CDR_BALANCECHECK_STEP where 
            sect_code='03_SECTION_BILLING' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_billin :=0;
                v_billout :=0;
                v_billerr :=0;
            end  ;
            
            begin
            select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT 
             into v_transin, v_transout,v_transerr from MON_CDR_BALANCECHECK_STEP where 
            sect_code='04_SECTION_TRANS' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_transin :=0;
                v_transout :=0;
                v_transerr :=0;
            end  ;

            begin
            select all_count, rmhomecount, RMEMPTYCOUNT+ RMERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT 
             into v_roamin, v_roamout,v_roamerr from MON_CDR_BALANCECHECK_STEP where 
            sect_code='05_SECTION_ROAM' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_roamin :=0;
                v_roamout :=0;
                v_roamerr :=0;
            end  ;

            if ( v_sepin <> v_sepout+v_seperr+v_roamout+v_roamerr or v_chargein <> v_chargeout+v_chargeerr
                or v_billin <> v_billout+v_billerr --or v_roamin <> v_roamout+v_roamerr 
                or v_transin <> v_transout+v_transerr
                or v_sepout <> v_chargein or v_chargeout <> v_billin or v_billout <> v_transin                
                )  then      
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;
      
            update MON_CDR_BALANCECHECK_STEP_SUM set sepin=v_sepin, sepout=v_sepout , seperr=v_seperr,
            chargein=v_chargein, chargeout=v_chargeout, chargeerr=v_chargeerr, 
            billin=v_billin, billout=v_billout, billerr=v_billerr,
            transin=v_transin, transout=v_transout, transerr=v_transerr, 
            roamin=v_roamin, roamout=v_roamin, /*roamout=v_roamout,*/roamerr=v_roamerr, flag=v_flag
            where filename=c4.filename and procdate=proc_date;
           
        end loop;
        dbms_output.put_line('8..........');
        commit;
    end if;
end;
//




  CREATE OR REPLACE PROCEDURE "CDR_BALANCECHECK_STEP_MON0" (
proc_date number ,--查询日期:'20060420'
check_id  number  --1=汇总稽核;0=分阶段稽核
)
as
v_tmp_sect  varchar2(1024) := '';
v_sect   varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';
v_sepin        number(14);
v_sepout      number(14);
v_chargein        number(14);
v_chargeout      number(14);
v_billin        number(14);
v_billout      number(14);
v_transin        number(14);
v_transout      number(14);
v_flag          char(1);
begin
    
    for sec in (select code from code_dict where catalog='T_FLOW_SECTION')
    loop
        v_sect:=v_sect||sec.code||',';
    end loop;


    if(check_id=1) then  --汇总稽核
        --清除旧数据,保留7天内数据
        delete MON_CDR_BALANCECHECK where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        --遍历汇总表
        for c1 in(select * from CDR_BALANCECHECK where  procdate=proc_date)
        loop         
            dbms_output.put_line('1..........');
			--针对汇总表记录,求其分阶段统计数据插入临时表
            insert into MON_CDR_BALANCECHECK(procdate,filename,sect_code,all_count,
                HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                NOCHARGECOUNT,REJECTCOUNT)
            select c1.procdate,c1.filename,null,c1.stdcount,
                nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
                nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from CDR_BALANCECHECK_DETAIL
                where procdate=c1.procdate and 
                filename=c1.filename and flow_type=1
                and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
        end loop;
        --把平衡的记录置上标志
        update MON_CDR_BALANCECHECK set sect_code='ok' where all_count=HOMECOUNT+EMPTYCOUNT+ERRORCOUNT+RMHOMECOUNT+RMEMPTYCOUNT+RMERRORCOUNT+
            NOCHARGECOUNT+REJECTCOUNT;
        commit;
    
    else  --分阶段稽核
	dbms_output.put_line('2..........');
        delete from MON_CDR_BALANCECHECK_STEP_SUM where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        delete from MON_CDR_BALANCECHECK_STEP where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
          
            v_tmp_sect  := v_sect;
            v_tmp_pos   := instr(v_tmp_sect,',');
            --求到
            
            for c2 in(select filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
                    nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
                    nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
                    from CDR_BALANCECHECK_DETAIL where procdate=proc_date --and sect_dst<>'00_SECTION_IGNORE'
                    --and filename=c1.filename 
                    and flow_type=1 group by filename,sect_src)
            loop
			dbms_output.put_line('3..........');
                    insert into MON_CDR_BALANCECHECK_STEP(procdate,filename,sect_code,all_count,
                      HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                      NOCHARGECOUNT,REJECTCOUNT)
                    values(proc_date,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
                    c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT);
            end loop;
   --dbms_output.put_line(v_sect_param);         
    --         commit;
                   
            while(v_tmp_pos > 0)
            loop
			dbms_output.put_line('4..........');
                v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);
                
               if(v_sect_param in ('01_SECTION_SEP','09_SECTION_ROAMOUT')) then
                    for c3 in(select * from CDR_BALANCECHECK
                            where procdate=proc_date)
                    loop
                        update MON_CDR_BALANCECHECK_STEP  set all_count=c3.stdcount
                            where procdate=c3.procdate and filename=c3.filename and 
                            sect_code=v_sect_param;
                    end loop;
               end if;
             
        --dbms_output.put_line('ok');            
        --     commit;          
             
                for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
                nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
                all_count from CDR_BALANCECHECK_DETAIL where --filename like v_file_name and 
                procdate=proc_date and sect_dst=v_sect_param and flow_type=1
                group by procdate,filename,sect_dst)
                loop
				dbms_output.put_line('5..........');
                    update MON_CDR_BALANCECHECK_STEP set all_count=c1.all_count
                       where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
                end loop;
                  
    --dbms_output.put_line(v_sect_param); 

                v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
                v_tmp_pos    := instr(v_tmp_sect,',');
               
            end loop;
        commit;
        
        for c4 in(select  * from CDR_BALANCECHECK where procdate=proc_date)
        loop
		dbms_output.put_line('6..........');
            insert into MON_CDR_BALANCECHECK_STEP_SUM (procdate,filename) values(proc_date,c4.filename);
            begin
                select all_count , homecount into v_sepin , v_sepout from MON_CDR_BALANCECHECK_STEP where 
                sect_code='01_SECTION_SEP' and filename=c4.filename and procdate=proc_date;
                exception  when no_data_found then         
			              v_sepin :=0;
                    v_sepout :=0;
            end  ;
            begin
            select all_count , homecount into v_chargein , v_chargeout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='02_SECTION_CHARGE' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_chargein :=0;
                v_chargeout :=0;
            end  ;
            begin
            select all_count , homecount into v_billin , v_billout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='03_SECTION_BILLING' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_billin :=0;
                v_billout :=0;
            end  ;
            begin
            select all_count , homecount into v_transin , v_transout from MON_CDR_BALANCECHECK_STEP where 
            sect_code='04_SECTION_TRANS' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_transin :=0;
                v_transout :=0;
            end  ;

            if ( v_sepout <> v_chargeout or v_chargeout<>v_billout or v_billout<>v_transout)  then      
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;
      
            update MON_CDR_BALANCECHECK_STEP_SUM set sepin=v_sepin, sepout=v_sepout ,
            chargein=v_chargein, chargeout=v_chargeout, billin=v_billin, billout=v_billout,
            transin=v_transin, transout=v_transout, flag=v_flag
            where filename=c4.filename and procdate=proc_date;
           
        end loop;
        dbms_output.put_line('7..........');
        commit;
    end if;
end;
//




  CREATE OR REPLACE PROCEDURE "CDR_BCHECK_PROC_TEST" (
proc_date varchar2  --处理日期,型如'20050601',不带时间
) as
v_sect_src      varchar2(64)    := '-';      --源阶段
v_sect_dst      varchar2(64)    := '-';      --目标阶段
v_sect_tmp      varchar2(64)    := '-';      --临时阶段,用来翻转源目标阶段用
v_log_field    varchar2(64)    := '-';       --event_flag对应的表中的字段
v_update_sql    varchar2(1024)  := '-';      --动态更新统计表的SQL
v_reverse_flag  number(1)       := 0;       --翻转标志,对某阶段为源的分流数据要翻转处理(重处理,话单回收)
v_calc_factor   number(1)       := 1;       --计算因子,对于翻转运算的分流,要将其数据翻转,即*(-1)
v_flow_type     number(4)       := 0;
v_event_flag    varchar2(20)    := '-';
begin
   --begin
        --清理历史数据
       delete cdr_bcheck where procdate=proc_date;--or to_date(procdate,'yyyymmdd')<trunc(sysdate-7);
       delete cdr_bcheck_detail where procdate=proc_date;--or to_date(procdate,'yyyymmdd')<trunc(sysdate-7);
       --commit;
   --exception when others then
    -- rollback;
    -- raise_application_error(-20101,sqlerrm);
    -- return;
   --end;
   --遍历分拣/漫游来访文件
   for c1 in(
            select filename,recordnum,stdcount,errcount from sepfilelog
                where trunc(beginprocessdate)=to_date(proc_date,'yyyymmdd')
            union
            select filename,billcount recordnum,billcount stdcount,errcount from visittranslog
                where trunc(processtime)=to_date(proc_date,'yyyymmdd'))
    loop
	dbms_output.put_line('1..........');
        begin
            --从分拣/漫游来访日志文件得到总量数据，插入相应表中
            insert into cdr_bcheck(procdate,filename,recordnum,stdcount,errcount)
                values(proc_date, c1.filename, c1.recordnum, c1.stdcount, c1.errcount);

            --从分流日志表统计分流数据，按模块，分流ID，分流类型，事件分类
            for flow_row in (select entry_module_id,module_id,flow_id,file_event_count sumcount
                                from v_event_flow_log_sourfile_test where source_name=c1.filename)
            loop
			dbms_output.put_line('2..........');
                 --取源阶段/目标阶段数据,如果源为空,则取模块号;如果目标为空,则取"模块号,规则号"
                select nvl(max(flow_section),flow_row.module_id) into v_sect_src
                    from event_flow_module where module_id=flow_row.entry_module_id;
                --为防止某个分流被删除导致无法统计，先设置缺省值，默认为丢弃。
                v_event_flag := 'LOSER';
                v_flow_type  := 1;   
                v_sect_dst   := '08_SECTION_TERM';               
                select event_flag,flow_type,nvl(next_section,flow_row.module_id||','||flow_row.flow_id)
                    into v_event_flag,v_flow_type,v_sect_dst
                        from event_flow_def where module_id=flow_row.module_id and flow_id=flow_row.flow_id;
                --判断源阶段是否是翻转阶段
                select count(*) into v_reverse_flag from code_dict
                    where catalog='T_SECTION_REVERSE' and code=v_sect_src;
                if(v_reverse_flag > 0) then
                    if(v_event_flag='CHOME') then
					dbms_output.put_line('3..........');
                        select case v_sect_src 
                             when '90_REPROC_EMPTY' then 'EMPTY'
                             when '91_REPROC_ERROR' then 'ERROR'
                             when '93_REPROC_NOCHARGE' then 'WAIFS'
                             else 'CHOME' end 
                        into v_event_flag 
                        from dual; 
                    end if;
                    --实现阶段翻转
                    v_sect_tmp      := v_sect_dst;
                    v_sect_dst      := v_sect_src;
                    v_sect_src      := v_sect_tmp;
                    v_calc_factor   := -1;  --计算因子取-1,实现后面统计数据取反值
                else
                    v_calc_factor  := 1;
                end if;
                --取日志字段,动态组织SQL,更新数据到统计表
                begin
                    select logfield into v_log_field from EVENT_FLAG_DEF where code=v_event_flag;
                    v_update_sql := 'update CDR_BCHECK_DETAIL set '
                        ||v_log_field||'='||v_log_field||'+'||flow_row.sumcount*(v_calc_factor)||' '
                        ||'where procdate='''||proc_date||''' and filename='''||c1.filename||''' '
                        ||'and sect_src='''||v_sect_src||''' and sect_dst='''||v_sect_dst||''' '
                        ||'and flow_type='||v_flow_type;
                    execute immediate(v_update_sql);
                    --如果目标记录不存在，则先增加记录再更新
                    if sql%rowcount=0 then
					dbms_output.put_line('4..........');
                         insert into CDR_BCHECK_DETAIL(procdate,filename,sect_src,sect_dst,flow_type)
                                values(proc_date,c1.filename, v_sect_src, v_sect_dst, v_flow_type);
                         execute immediate(v_update_sql);
                    end if;
                exception when others then
                    null;
                end;
            end loop;
            commit;
        exception when others then
            --rollback;
            --raise_application_error(-20101,sqlerrm);
            --return;
            null;
        end;
    end loop;
end CDR_BCHECK_PROC_TEST;
//




  CREATE OR REPLACE PROCEDURE "CDR_BCHECK_STEP_MON" (
proc_date number ,--查询日期:'20060420'
check_id  number  --1=汇总稽核;0=分阶段稽核
)
as
v_tmp_sect  varchar2(1024) := '';
v_sect   varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';
v_sepin        number(14);
v_sepout      number(14);
v_chargein        number(14);
v_chargeout      number(14);
v_billin        number(14);
v_billout      number(14);
v_transin        number(14);
v_transout      number(14);
v_flag          char(1);
begin
    
    for sec in (select code from code_dict where catalog='T_FLOW_SECTION')
    loop
        v_sect:=v_sect||sec.code||',';
    end loop;


    if(check_id=1) then  --汇总稽核
        --清除旧数据,保留7天内数据
        delete MON_CDR_BCHECK where procdate=proc_date ;--or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        --遍历汇总表
        for c1 in(select * from CDR_BCHECK where  procdate=proc_date)
        loop    
		dbms_output.put_line('1..........');
            --针对汇总表记录,求其分阶段统计数据插入临时表
            insert into MON_CDR_BCHECK(procdate,filename,sect_code,all_count,
                HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                NOCHARGECOUNT,REJECTCOUNT)
            select c1.procdate,c1.filename,null,c1.stdcount,
                nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
                nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from CDR_BCHECK_DETAIL
                where procdate=c1.procdate and 
                filename=c1.filename and flow_type=1
                and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
        end loop;
        --把平衡的记录置上标志
        update MON_CDR_BCHECK set sect_code='ok' where all_count=HOMECOUNT+EMPTYCOUNT+ERRORCOUNT+RMHOMECOUNT+RMEMPTYCOUNT+RMERRORCOUNT+
            NOCHARGECOUNT+REJECTCOUNT;
        commit;
    
    else  --分阶段稽核
	dbms_output.put_line('2..........');
        delete from MON_CDR_BCHECK_STEP_SUM where procdate=proc_date;--or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        delete from MON_CDR_BCHECK_STEP where procdate=proc_date;--or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
          
            v_tmp_sect  := v_sect;
            v_tmp_pos   := instr(v_tmp_sect,',');
            --求到
            
            for c2 in(select filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
                    nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
                    nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
                    from CDR_BCHECK_DETAIL where procdate=proc_date --and sect_dst<>'00_SECTION_IGNORE'
                    --and filename=c1.filename 
                    and flow_type=1 group by filename,sect_src)
            loop
			dbms_output.put_line('3..........');
                    insert into MON_CDR_BCHECK_STEP(procdate,filename,sect_code,all_count,
                      HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                      NOCHARGECOUNT,REJECTCOUNT)
                    values(proc_date,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
                    c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT);
            end loop;
   --dbms_output.put_line(v_sect_param);         
    --         commit;
                   
            while(v_tmp_pos > 0)
            loop
			dbms_output.put_line('4..........');
                v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);
                
               if(v_sect_param in ('01_SECTION_SEP','09_SECTION_ROAMOUT')) then
                    for c3 in(select * from CDR_BCHECK
                            where procdate=proc_date)
                    loop
                        update MON_CDR_BCHECK_STEP  set all_count=c3.stdcount
                            where procdate=c3.procdate and filename=c3.filename and 
                            sect_code=v_sect_param;
                    end loop;
               end if;
             
        --dbms_output.put_line('ok');            
        --     commit;          
             
                for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
                nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
                all_count from CDR_BCHECK_DETAIL where --filename like v_file_name and 
                procdate=proc_date and sect_dst=v_sect_param and flow_type=1
                group by procdate,filename,sect_dst)
                loop
				dbms_output.put_line('5..........');
                    update MON_CDR_BCHECK_STEP set all_count=c1.all_count
                       where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
                end loop;
                  
    --dbms_output.put_line(v_sect_param); 

                v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
                v_tmp_pos    := instr(v_tmp_sect,',');
               
            end loop;
        commit;
        
        for c4 in(select  * from CDR_BCHECK where procdate=proc_date)
        loop
		dbms_output.put_line('6..........');
            insert into MON_CDR_BCHECK_STEP_SUM (procdate,filename) values(proc_date,c4.filename);
            begin
                select all_count , homecount into v_sepin , v_sepout from MON_CDR_BCHECK_STEP where 
                sect_code='01_SECTION_SEP' and filename=c4.filename and procdate=proc_date;
                exception  when no_data_found then         
			              v_sepin :=0;
                    v_sepout :=0;
            end  ;
            begin
            select all_count , homecount into v_chargein , v_chargeout from MON_CDR_BCHECK_STEP where 
            sect_code='02_SECTION_CHARGE' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_chargein :=0;
                v_chargeout :=0;
            end  ;
            begin
            select all_count , homecount into v_billin , v_billout from MON_CDR_BCHECK_STEP where 
            sect_code='03_SECTION_BILLING' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_billin :=0;
                v_billout :=0;
            end  ;
            begin
            select all_count , homecount into v_transin , v_transout from MON_CDR_BCHECK_STEP where 
            sect_code='04_SECTION_TRANS' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
			          v_transin :=0;
                v_transout :=0;
            end  ;

            if ( v_sepout <> v_chargeout or v_chargeout<>v_billout or v_billout<>v_transout)  then      
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;
      
            update MON_CDR_BCHECK_STEP_SUM set sepin=v_sepin, sepout=v_sepout ,
            chargein=v_chargein, chargeout=v_chargeout, billin=v_billin, billout=v_billout,
            transin=v_transin, transout=v_transout, flag=v_flag
            where filename=c4.filename and procdate=proc_date;
           
        end loop;
               
        commit;
    end if;
end;
//




  CREATE OR REPLACE PROCEDURE "CDR_CHECK_STEP_MON_SINGLE" (
proc_date number ,--查询日期:'20060420'
check_id  number  --1=汇总稽核;0=分阶段稽核
)
as
v_tmp_sect  varchar2(1024) := '';
v_sect   varchar2(1024) := '';
v_tmp_pos   number(4) := -1;
v_sect_param varchar2(1024) := '';

v_sepin        number(14);
v_sepout      number(14);
v_seperr      number(14);
v_chargein        number(14);
v_chargeout      number(14);
v_chargeerr      number(14);
v_billin        number(14);
v_billout      number(14);
v_billerr      number(14);
v_transin        number(14);
v_transout      number(14);
v_transerr     number(14);
v_roamin        number(14);
v_roamout      number(14);
v_roamerr     number(14);
v_flag          char(1);

begin

    for sec in (select code from code_dict where catalog='T_FLOW_SECTION')
    loop
        v_sect:=v_sect||sec.code||',';
    end loop;


    if(check_id=1) then  --汇总稽核
        --清除旧数据,保留7天内数据
        --delete MON_CDR_BALANCECHECK where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        --遍历汇总表
        for c1 in(select * from cdr_balancecheck_single where  procdate=proc_date)
        loop
		
            if substr(c1.filename,1,4)='chg2' then
			dbms_output.put_line('1..........');
              --针对汇总表记录,求其分阶段统计数据插入临时表,调整了chg话单的漫游丢弃话单环节
              insert into MON_CDR_BALANCECHECK_SINGLE(procdate,filename,sect_code,all_count,
                  HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                  NOCHARGECOUNT,REJECTCOUNT)
              select c1.procdate,c1.filename,null,c1.stdcount,
                  nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
                  nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),0/*nvl(sum(REJECTCOUNT),0)*/ from cdr_check_detail_single
                  where procdate=c1.procdate and
                  filename=c1.filename and flow_type=1
                  and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
            else
			dbms_output.put_line('2..........');
              --针对汇总表记录,求其分阶段统计数据插入临时表
              insert into MON_CDR_BALANCECHECK_SINGLE(procdate,filename,sect_code,all_count,
                  HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                  NOCHARGECOUNT,REJECTCOUNT)
              select c1.procdate,c1.filename,null,c1.stdcount,
                  nvl(sum(HOMECOUNT),0),nvl(sum(EMPTYCOUNT),0),nvl(sum(ERRORCOUNT),0),nvl(sum(RMHOMECOUNT),0),nvl(sum(RMEMPTYCOUNT),0),
                  nvl(sum(RMERRORCOUNT),0),nvl(sum(NOCHARGECOUNT),0),nvl(sum(REJECTCOUNT),0) from cdr_check_detail_single
                  where procdate=c1.procdate and
                  filename=c1.filename and flow_type=1
                  and sect_dst in(select code from code_dict where catalog='T_SECTION_TERMS');
            end if;
        end loop;
        --把平衡的记录置上标志
        update MON_CDR_BALANCECHECK_SINGLE set sect_code='ok' where procdate=proc_date and
            all_count=HOMECOUNT+EMPTYCOUNT+ERRORCOUNT+RMHOMECOUNT+RMEMPTYCOUNT+RMERRORCOUNT+
            NOCHARGECOUNT+REJECTCOUNT;
        commit;

    else  --分阶段稽核
	dbms_output.put_line('3..........');
        --delete from MON_CDR_BALANCECHECK_STEP_SUM where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;
        --delete from MON_CDR_BALANCECHECK_STEP where procdate=proc_date or to_date(procdate,'yyyymmdd')<trunc(sysdate-7) ;

            v_tmp_sect  := v_sect;
            v_tmp_pos   := instr(v_tmp_sect,',');
            --求到

            for c2 in(select filename,sect_src,nvl(sum(HOMECOUNT),0) HOMECOUNT,nvl(sum(EMPTYCOUNT),0) EMPTYCOUNT,
                    nvl(sum(ERRORCOUNT),0) ERRORCOUNT,nvl(sum(RMHOMECOUNT),0) RMHOMECOUNT,nvl(sum(RMEMPTYCOUNT),0) RMEMPTYCOUNT,
                    nvl(sum(RMERRORCOUNT),0) RMERRORCOUNT,nvl(sum(NOCHARGECOUNT),0) NOCHARGECOUNT,nvl(sum(REJECTCOUNT),0) REJECTCOUNT
                    from cdr_check_detail_single where procdate=proc_date --and sect_dst<>'00_SECTION_IGNORE'
                    --and filename=c1.filename
                    and flow_type=1 group by filename,sect_src)
            loop
			dbms_output.put_line('4..........');
                    insert into MON_CDR_CHECK_STEP_SINGLE(procdate,filename,sect_code,all_count,
                      HOMECOUNT,EMPTYCOUNT,ERRORCOUNT,RMHOMECOUNT,RMEMPTYCOUNT,RMERRORCOUNT,
                      NOCHARGECOUNT,REJECTCOUNT)
                    values(proc_date,c2.filename,c2.sect_src,0,c2.HOMECOUNT,c2.EMPTYCOUNT,c2.ERRORCOUNT,c2.RMHOMECOUNT,c2.RMEMPTYCOUNT,
                    c2.RMERRORCOUNT,c2.NOCHARGECOUNT,c2.REJECTCOUNT);
            end loop;
   --dbms_output.put_line(v_sect_param);
    --         commit;

            while(v_tmp_pos > 0)
            loop
			dbms_output.put_line('5..........');
                v_sect_param := substr(v_tmp_sect,1,v_tmp_pos-1);

               if(v_sect_param in ('01_SECTION_SEP','09_SECTION_ROAMOUT')) then
                    for c3 in(select * from cdr_balancecheck_single
                            where procdate=proc_date)
                    loop
                        update MON_CDR_CHECK_STEP_SINGLE  set all_count=c3.stdcount
                            where procdate=c3.procdate and filename=c3.filename and
                            sect_code=v_sect_param;
                    end loop;
               end if;

        --dbms_output.put_line('ok');
        --     commit;

                for c1 in(select procdate,filename,sect_dst,nvl(sum(HOMECOUNT),0)+nvl(sum(EMPTYCOUNT),0)+nvl(sum(ERRORCOUNT),0)+
                nvl(sum(RMHOMECOUNT),0)+nvl(sum(RMEMPTYCOUNT),0)+nvl(sum(RMERRORCOUNT),0)+nvl(sum(NOCHARGECOUNT),0)+nvl(sum(REJECTCOUNT),0)
                all_count from cdr_check_detail_single where --filename like v_file_name and
                procdate=proc_date and sect_dst=v_sect_param and flow_type=1
                group by procdate,filename,sect_dst)
                loop
				dbms_output.put_line('6..........');
                    update MON_CDR_CHECK_STEP_SINGLE set all_count=c1.all_count
                       where procdate=c1.procdate and filename=c1.filename and sect_code=c1.sect_dst;
                end loop;

    --dbms_output.put_line(v_sect_param);

                v_tmp_sect   := substr(v_tmp_sect,v_tmp_pos+1);
                v_tmp_pos    := instr(v_tmp_sect,',');

            end loop;
        commit;
      /*
        for c4 in(select  * from CDR_BALANCECHECK where procdate=proc_date)
        loop
            insert into MON_CDR_BALANCECHECK_STEP_SUM (procdate,filename) values(proc_date,c4.filename);
            begin
                select all_count , homecount into v_sepin , v_sepout from MON_CDR_BALANCECHECK_STEP where
                sect_code='01_SECTION_SEP' and filename=c4.filename and procdate=proc_date;
                exception  when no_data_found then
                          v_sepin :=0;
                    v_sepout :=0;
            end  ;
            begin
            select all_count , homecount into v_chargein , v_chargeout from MON_CDR_BALANCECHECK_STEP where
            sect_code='02_SECTION_CHARGE' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_chargein :=0;
                v_chargeout :=0;
            end  ;
            begin
            select all_count , homecount into v_billin , v_billout from MON_CDR_BALANCECHECK_STEP where
            sect_code='03_SECTION_BILLING' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_billin :=0;
                v_billout :=0;
            end  ;
            begin
            select all_count , homecount into v_transin , v_transout from MON_CDR_BALANCECHECK_STEP where
            sect_code='04_SECTION_TRANS' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_transin :=0;
                v_transout :=0;
            end  ;

            if ( v_sepout <> v_chargeout or v_chargeout<>v_billout or v_billout<>v_transout)  then
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;

            update MON_CDR_BALANCECHECK_STEP_SUM set sepin=v_sepin, sepout=v_sepout ,
            chargein=v_chargein, chargeout=v_chargeout, billin=v_billin, billout=v_billout,
            transin=v_transin, transout=v_transout, flag=v_flag
            where filename=c4.filename and procdate=proc_date;

        end loop;
*/
        for c4 in(select  * from cdr_balancecheck_single where procdate=proc_date )
        loop
		dbms_output.put_line('7..........');
            insert into MON_CDR_CHECK_STEP_SUM_SINGLE (procdate,filename) values(proc_date,c4.filename);
            begin
                select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
                 into v_sepin , v_sepout, v_seperr from MON_CDR_CHECK_STEP_SINGLE where
                sect_code='01_SECTION_SEP' and filename=c4.filename and procdate=proc_date;
                exception  when no_data_found then
                          v_sepin :=0;
                    v_sepout :=0;
                    v_seperr :=0;
            end  ;

            begin
            select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
             into v_chargein, v_chargeout, v_chargeerr from MON_CDR_CHECK_STEP_SINGLE where
            sect_code='02_SECTION_CHARGE' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_chargein :=0;
                v_chargeout :=0;
                v_chargeerr :=0;
            end  ;

            begin
            select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
             into v_billin, v_billout,v_billerr from MON_CDR_CHECK_STEP_SINGLE where
            sect_code='03_SECTION_BILLING' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_billin :=0;
                v_billout :=0;
                v_billerr :=0;
            end  ;

            begin
            select all_count, homecount, EMPTYCOUNT+ ERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
             into v_transin, v_transout,v_transerr from MON_CDR_CHECK_STEP_SINGLE where
            sect_code='04_SECTION_TRANS' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_transin :=0;
                v_transout :=0;
                v_transerr :=0;
            end  ;

            begin
            select all_count, rmhomecount, RMEMPTYCOUNT+ RMERRORCOUNT+ NOCHARGECOUNT+ REJECTCOUNT
             into v_roamin, v_roamout,v_roamerr from MON_CDR_CHECK_STEP_SINGLE where
            sect_code='05_SECTION_ROAM' and filename=c4.filename and procdate=proc_date;
            exception when no_data_found then
                      v_roamin :=0;
                v_roamout :=0;
                v_roamerr :=0;
            end  ;

            if ( v_sepin <> v_sepout+v_seperr+v_roamout+v_roamerr or v_chargein <> v_chargeout+v_chargeerr
                or v_billin <> v_billout+v_billerr --or v_roamin <> v_roamout+v_roamerr
                or v_transin <> v_transout+v_transerr
                or v_sepout <> v_chargein or v_chargeout <> v_billin or v_billout <> v_transin
                )  then
                v_flag :=0;
            ELSE
                v_flag :=1;
            END IF;

            update MON_CDR_CHECK_STEP_SUM_SINGLE set sepin=v_sepin, sepout=v_sepout , seperr=v_seperr,
            chargein=v_chargein, chargeout=v_chargeout, chargeerr=v_chargeerr,
            billin=v_billin, billout=v_billout, billerr=v_billerr,
            transin=v_transin, transout=v_transout, transerr=v_transerr,
            roamin=v_roamin, roamout=v_roamin, /*roamout=v_roamout,*/roamerr=v_roamerr, flag=v_flag
            where filename=c4.filename and procdate=proc_date;

        end loop;

        commit;
    end if;
end CDR_CHECK_STEP_MON_SINGLE;
//




  CREATE OR REPLACE PROCEDURE "EXCDR_PROCESS" (V_REGION varchar2,v_nodeid number,v_tablename VARCHAR2,v_cond varchar2)
is
    --Type CDRCur is ref cursor;
    --cdr_cv CDRCur;

   v_fields varchar2(256);
   debugflag number(1);
   v_sql  varchar2(512);
   v_groupcond varchar2(256);
   v_insertsql varchar2(512);
   v_fieldsql varchar2(256);
   v_condsql varchar2(256);
   v_defcond varchar2(256);
   v_tname varchar (128);

begin

    debugflag := 0;
    
    --增加支持分区查询的功能，下面从v_tablename得到表名到v_tname中
    if substr(upper(v_tablename),1,12) = 'ISMGCHECKCDR' then
	dbms_output.put_line('1..........');
       select substr(v_tablename,1,instr(upper(v_tablename),'CDR')+8) into v_tname from dual;
       update EX_CDR_TAB_MAP set table_name=v_tname where 
              substr(upper(table_name),1,12)='ISMGCHECKCDR'
              and substr(table_name,17,2)=substr(v_tname,17,2);
       commit;
    else
        select substr(v_tablename,1,instr(upper(v_tablename),'CDR')+2) into v_tname from dual;
    end if;
    
    begin 
    SELECT FIELDS,groupcond,cond INTO V_FIELDS,v_groupcond, v_defcond FROM  EX_CDR_TAB_MAP WHERE upper(TABLE_NAME)=upper(v_tname);
    exception when others  then
        dbms_output.put_line('table:'||v_tname||',err:'||sqlerrm);            
        return;    
    end ;

    v_fieldsql := 'select '|| to_char(nvl(v_region,'NULL'))||','||to_char(v_nodeid)||','||v_fields;
    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||v_fieldsql);
    end if;

    if v_region is null
    then
        v_condsql := ' where hregion is null'||' ';
    elsif v_region = 888
         then
             v_condsql := ' where 1=1 ';
         else
             v_condsql := ' where hregion ='||v_region||' ';
    end if;
   if length(v_cond) > 0
   then
       v_condsql := v_condsql||'and '||v_cond||v_defcond||' ';
   end if;
   
   --select to_char(add_months(sysdate,-2),'yyyymm') from dual;   
   
   v_sql := v_fieldsql||' from '||v_tablename||v_condsql||v_groupcond ;
/*
   if length(v_cond) > 0
   then
       v_sql := v_fieldsql||' from '||v_tablename||' where '||v_cond||' '||v_groupcond ;
   else
       v_sql := v_fieldsql||' from '||v_tablename||' '||v_groupcond ;
   end if;
*/
   v_insertsql := 'insert into    ex_cdr_rec_base '
   ||'(region,nodeid,errclass,errtype,errtotal,statdate,cdrdate,cdrclass,fee,amountclass,amount) '
   || v_sql;



    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,1,230));
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,231));
    end if;
       
    execute  immediate  v_insertsql;
	dbms_output.put_line('2..........');
    return;


end ;
//




  CREATE OR REPLACE PROCEDURE "EXCDR_PROCESS_NEW" (V_REGION varchar2,v_nodeid number,v_tablename VARCHAR2,v_cond varchar2)
is
    --Type CDRCur is ref cursor;
    --cdr_cv CDRCur;

   v_fields varchar2(256);
   debugflag number(1);
   v_sql  varchar2(512);
   v_groupcond varchar2(256);
   v_insertsql varchar2(512);
   v_fieldsql varchar2(256);
   v_condsql varchar2(256);
   v_defcond varchar2(256);
   v_tname varchar (128);
   v_statdate date;
   v_pnum     number(8);
   v_endpnum  number(8);
   v_rowid    ROWID;
   v_pos      number(8);
begin

    debugflag := 0;

    --增加支持分区查询的功能，下面从v_tablename得到表名到v_tname中
    if substr(upper(v_tablename),1,12) = 'ISMGCHECKCDR' then
	dbms_output.put_line('1..........');
        --select substr(v_tablename,1,instr(upper(v_tablename),'CDR')+8) into v_tname from dual;
        v_tname := substr(v_tablename,1,18);
        update ex_cdr_tab_map_new set table_name=v_tname where
            substr(upper(table_name),1,12)='ISMGCHECKCDR';
        -- and substr(table_name,17,2)=substr(v_tname,17,2);
        commit;
    else
	dbms_output.put_line('2..........');
        v_pos := instr(upper(v_tablename),' ');
        if v_pos >0 then
           v_tname := substr(v_tablename,1,v_pos-1);
        else
           v_tname := v_tablename;
        end if;
    end if;

    begin
    SELECT FIELDS,groupcond,cond INTO v_fields,v_groupcond, v_defcond FROM  ex_cdr_tab_map_new WHERE upper(TABLE_NAME)=upper(v_tname);
    exception when others  then
        dbms_output.put_line('table:'||v_tname||',err:'||sqlerrm);
        return;
    end ;

    v_fieldsql := 'select '|| to_char(nvl(v_region,'NULL'))||','||to_char(v_nodeid)||','||v_fields;
    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||v_fieldsql);
    end if;

    if v_region is null
    then
        v_condsql := ' where hregion is null'||' ';
    elsif v_region = 888
         then
             v_condsql := ' where 1=1 ';
         else
             v_condsql := ' where hregion ='||v_region||' ';
    end if;
   if length(v_cond) > 0
   then
       v_condsql := v_condsql||'and '||v_cond||v_defcond||' ';
   end if;

   --select to_char(add_months(sysdate,-2),'yyyymm') from dual;

   v_sql := v_fieldsql||' from '||v_tablename||v_condsql||v_groupcond ;
/*
   if length(v_cond) > 0
   then
       v_sql := v_fieldsql||' from '||v_tablename||' where '||v_cond||' '||v_groupcond ;
   else
       v_sql := v_fieldsql||' from '||v_tablename||' '||v_groupcond ;
   end if;
*/
   v_insertsql := 'insert into    ex_cdr_rec_base_new '
   ||'(region,nodeid,errclass,errtype,errtotal,statdate,cdrdate,processdate,cdrclass,fee,amountclass,amount) '
   || v_sql;



    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,1,230));
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,231));
    end if;
       --V_REGION varchar2,v_nodeid number,v_tablename VARCHAR2,v_cond varchar2

    v_statdate:=trunc(sysdate);

    insert into ex_cdr_task_log_new (STATDATE, REGION, NODEID, TABLENAME, COND, PARTNUM, ENDPARTNUM, STARTTIME, ENDTIME, END_FLAG)
        values (v_statdate, v_region, v_nodeid, v_tablename, v_cond, 1, 0, sysdate, null, '0');

    execute  immediate  v_insertsql;

    update ex_cdr_task_log_new set endtime=sysdate, end_flag='1', endpartnum=1 where tablename= v_tablename and cond=v_cond
        and statdate=v_statdate;

    --if has partitions
    if instr(v_tablename,'(') <> 0 then
	dbms_output.put_line('3..........');
        select partnum,endpartnum,rowid into v_pnum,v_endpnum,v_rowid from ex_cdr_task_log_new where tablename = v_tname
            and statdate=v_statdate;
        if v_pnum = v_endpnum +1 then
		dbms_output.put_line('4..........');
            update ex_cdr_task_log_new set endtime=sysdate, end_flag='1', endpartnum=v_pnum where rowid=v_rowid;
        else
		dbms_output.put_line('5..........');
            update ex_cdr_task_log_new set endpartnum=endpartnum+1 where rowid=v_rowid;
        end if;
    end if;

    commit;
    return;


    exception when others  then
        dbms_output.put_line('procedure() error:');
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,1,230));
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,231));
        rollback;

end ;
//




  CREATE OR REPLACE PROCEDURE "EXECUTE_IMMEDIATE" ( p_sql_text VARCHAR2 ) IS

   COMPILATION_ERROR EXCEPTION;
   PRAGMA EXCEPTION_INIT(COMPILATION_ERROR,-24344);

   l_cursor INTEGER DEFAULT 0;
   rc       INTEGER DEFAULT 0;
   stmt     VARCHAR2(1000);

BEGIN

   l_cursor := DBMS_SQL.OPEN_CURSOR;
   DBMS_SQL.PARSE(l_cursor, p_sql_text, DBMS_SQL.NATIVE);
   rc := DBMS_SQL.EXECUTE(l_cursor);
   DBMS_SQL.CLOSE_CURSOR(l_cursor);
   dbms_output.put_line('1..........');
   
   EXCEPTION WHEN COMPILATION_ERROR THEN DBMS_SQL.CLOSE_CURSOR(l_cursor);
       WHEN OTHERS THEN
          BEGIN
              DBMS_SQL.CLOSE_CURSOR(l_cursor);
              raise_application_error(-20101,sqlerrm || '  when executing ''' || p_sql_text || '''   ');
          END;
END;
//




  CREATE OR REPLACE PROCEDURE "FORMAT_CHECK" (

proc_date number --查询日期:'20060420'
)
as

begin
       insert into mon_cdr_bc_raw select * from mon_cdr_balancecheck where sect_code is  null and procdate=proc_date;
       insert into MON_CDR_BCS_RAW select * from MON_CDR_BALANCECHECK_STEP_SUM 
       where flag=0 and procdate=proc_date;

       update mon_cdr_balancecheck set sect_code='ok',homecount=all_count-emptycount-errorcount-
       rmhomecount-rmemptycount-rmerrorcount-nochargecount-rejectcount 
       where sect_code is  null and procdate=proc_date;

        for c1 in(select  * from MON_CDR_BALANCECHECK_STEP_SUM where flag=0 and procdate=proc_date )
        loop          
            dbms_output.put_line('1..........');
			update MON_CDR_BALANCECHECK_STEP_SUM set 
            chargein=c1.sepout, 
            chargeout=c1.sepout,
            billin=c1.sepout, 
            billout=c1.sepout,
            transin=c1.sepout, 
            transout=c1.sepout,
            flag=1
            where filename=c1.filename and procdate=proc_date;
           
        end loop;
               
        commit;
 
end;
//




  CREATE OR REPLACE PROCEDURE "GENERATESEQUENCE"   (p_tableName in  varchar2, preStr  out varchar2,endStr out varchar2,curVal in out int, seqLength out int,mx out int,errCode out int) is

  step int;
  mn int;
  cursor seq_cursor(t_name in varchar2)
  is
   select t.prestr,t.endstr,t.curval,t.step,t.mn,t.mx,t.length from T_SEQUENCE t where upper(t.tablename) = t_name for update of t.curval;
begin
  errCode := -100;

  open seq_cursor(p_tableName);

  loop
  fetch seq_cursor into preStr,endStr,curVal,step,mn, mx,seqLength;


  if seq_cursor%found
  then
  dbms_output.put_line('1.........');
      errCode := 1;
      if mn <= mx and curVal >= mn
      then
		dbms_output.put_line('2.........');
          if (curVal + step <= mx and curVal + step >= mn )
          then
		  dbms_output.put_line('3.........');
              update T_sequence t set t.curval = curVal + step where upper(t.tableName) = p_tableName;

          else
             errCode := -200;
          end if;


          if step > mx
          then
             errCode := -300;
          end if;
       else
          errCode := -400;
       end if;

  else
      exit;
  end if;

  end loop;
  close seq_cursor;
end generateSequence;
//
 



  CREATE OR REPLACE PROCEDURE "GPRSMERGE" 
/*
merge gprscdr 
from gprspartcdr
to   create merge cdr to gprsmergecdr
	 move raw merged cdr  to gprsrawmergecdr for query
	 delete rawmerged cdr from gprspartcdr
*/
as        
timelimit number(10) := 0; 
 
mergeok   number(1):=0;
needmerge number(1):=0;  
currow    number(10):=0;     

mergedrow gprspartcdr%rowtype;   

cursor c_partcdr(v_cdrtype varchar2,v_chargingid varchar2,v_ggsn varchar2,v_maxtime date) is  
	select gprspartcdr.* from gprspartcdr 
 	where cdrtype=v_cdrtype and chargingid=v_chargingid and ggsn=v_ggsn  
 		and starttime<=v_maxtime  order by starttime for update;
begin 
for c_key in (select cdrtype,chargingid,ggsn,min(starttime) mintime,max(starttime) maxtime, min(cause_close) close ,count(*) cnt 
				from gprspartcdr  group by cdrtype,chargingid,ggsn) loop
	dbms_output.put_line('1..........');
	mergeok 	:=0; 
	needmerge 	:=0;    
	--step 2 check if need merge  gprspartcdr
/*
	if  c_key.close = 0 then 
		needmerge :=1;
	end if;  
	if  c_key.close !=0 then
		if (sysdate - c_key.mintime )* 24 > timelimit then
			needmerge := 1;
		end if;
	end if ;
*/
	needmerge := 1;--modify by 20040602 		
	--step 3 try to merge one cdr
	if needmerge = 1 then   
		currow :=0;   
    	for c_merge in c_partcdr(c_key.cdrtype,c_key.chargingid,c_key.ggsn,c_key.maxtime) loop
		dbms_output.put_line('2..........');
			currow := currow + 1; 
			if currow = 1 then
				--init mergedrow  
				mergedrow :=c_merge;
			else   
			dbms_output.put_line('3..........');
				--check continuity
				if c_merge.starttime != mergedrow.starttime+mergedrow.duration/86400 then
					--no continuity
					update gprspartcdr set ROLLBACK_FLAG='nc' where current of c_partcdr ;
				end if;
				--merge volums
				mergedrow.duration := mergedrow.duration + c_merge.duration;
				mergedrow.flowup1  := mergedrow.flowup1  + c_merge.flowup1;
				mergedrow.flowdown1:= mergedrow.flowdown1+ c_merge.flowdown1;
				mergedrow.time1	   := mergedrow.time1    + c_merge.time1;
				mergedrow.flowup2  := mergedrow.flowup2  + c_merge.flowup2;
				mergedrow.flowdown2:= mergedrow.flowdown2+ c_merge.flowdown2;
				mergedrow.time2	   := mergedrow.time2    + c_merge.time2;	
     		end if;	  
			--set cause for close and new result of merged cdr
			if currow = c_key.cnt then
			dbms_output.put_line('4..........');
				if c_merge.cause_close != '0' then 
					mergedrow.cause_close := c_key.close;
					mergedrow.result      := '0';
                 else
					mergedrow.cause_close :='0';
					mergedrow.result      :='1'; 
				end if;   
				c_merge.processtime :=sysdate;
			end if;
		mergeok :=1;
		end loop;       
		--step 4 merge ok then move cdrs
		if mergeok = 1 and not mergedrow.starttime is null  then 
		dbms_output.put_line('5..........');
			mergedrow.billingcycle := null;
/*
			if sysdate - mergedrow.starttime >1 and  mergedrow.roamtype='21' then    
				insert into gprserrorcdr select 
				mergedrow.TELNUM   		,
				mergedrow.IMSI          ,         
				mergedrow.SGSN          ,         
				mergedrow.LAC           ,         
				mergedrow.ROUTEAREA     ,         
				mergedrow.CELL          ,         
				mergedrow.CHARGINGID    ,         
				mergedrow.GGSN          ,         
				mergedrow.APNN          ,         
				mergedrow.APNO          ,         
				mergedrow.PAPTYPE       ,         
				mergedrow.PAPADDRESS    ,         
				mergedrow.SGSN_CHANGE   ,         
				mergedrow.CAUSE_CLOSE   ,         
				mergedrow.RESULT        ,         
				mergedrow.HREGION       ,         
				mergedrow.VREGION       ,         
				mergedrow.ROAMTYPE      ,         
				mergedrow.SERVICETYPE   ,         
				mergedrow.STARTTIME     ,         
				mergedrow.DURATION      ,         
				mergedrow.FLOWUP1       ,         
				mergedrow.FLOWDOWN1     ,         
				mergedrow.TIME1         ,         
				mergedrow.FLOWUP2       ,         
				mergedrow.FLOWDOWN2     ,         
				mergedrow.TIME2         ,         
				mergedrow.FLOWUP3       ,         
				mergedrow.FLOWDOWN3     ,         
				mergedrow.TIME3         ,         
				mergedrow.FLOWUP4       ,         
				mergedrow.FLOWDOWN4     ,         
				mergedrow.TIME4         ,         
				mergedrow.FLOWUP5       ,         
				mergedrow.FLOWDOWN5     ,         
				mergedrow.TIME5         ,         
				mergedrow.FLOWUP6       ,         
				mergedrow.FLOWDOWN6     ,         
				mergedrow.TIME6         ,         
				mergedrow.ERRORCODE     ,         
				mergedrow.DEVICETYPE    ,         
				mergedrow.SOURFILENAME  ,         
				mergedrow.PROCESSTIME   ,         
				mergedrow.DEVICEID      ,         
				mergedrow.DESTFILENAME  ,         
				mergedrow.HMANAGE       ,         
				mergedrow.BASICFEE      ,         
				mergedrow.BASICFEE_S    ,         
				mergedrow.BILLINGCYCLE  ,         
				mergedrow.SUBSCRIBERID  ,         
				mergedrow.ACCOUNTID     ,         
				mergedrow.TARIFFFLAG    ,         
				mergedrow.TOTAL_FREE    ,         
				mergedrow.TOTALBYTE     ,         
				mergedrow.TOTALBYTE_UP  ,         
				mergedrow.TOTALBYTE_DOWN,         
				mergedrow.CDRTYPE       ,         
				mergedrow.ROLLBACK_FLAG          
				from dual;
			else 
*/			
				insert into gprsmergecdr(telnum, 
imsi, 
sgsn, 
lac, 
routearea, 
cell, 
chargingid, 
ggsn, 
apnn, 
apno, 
paptype, 
papaddress, 
sgsn_change, 
cause_close, 
result, 
hregion, 
vregion, 
roamtype, 
servicetype, 
starttime, 
duration, 
flowup1, 
flowdown1, 
time1, 
flowup2, 
flowdown2, 
time2, 
flowup3, 
flowdown3, 
time3, 
flowup4, 
flowdown4, 
time4, 
flowup5, 
flowdown5, 
time5, 
flowup6, 
flowdown6, 
time6, 
errorcode, 
devicetype, 
sourfilename, 
processtime, 
deviceid, 
destfilename, 
hmanage, 
basicfee, 
basicfee_s, 
billingcycle, 
subscriberid, 
accountid, 
tariffflag, 
total_free, 
totalbyte, 
totalbyte_up, 
totalbyte_down, 
cdrtype, 
rollback_flag) select 
				mergedrow.TELNUM   		,
				mergedrow.IMSI          ,         
				mergedrow.SGSN          ,         
				mergedrow.LAC           ,         
				mergedrow.ROUTEAREA     ,         
				mergedrow.CELL          ,         
				mergedrow.CHARGINGID    ,         
				mergedrow.GGSN          ,         
				mergedrow.APNN          ,         
				mergedrow.APNO          ,         
				mergedrow.PAPTYPE       ,         
				mergedrow.PAPADDRESS    ,         
				mergedrow.SGSN_CHANGE   ,         
				mergedrow.CAUSE_CLOSE   ,         
				mergedrow.RESULT        ,         
				mergedrow.HREGION       ,         
				mergedrow.VREGION       ,         
				mergedrow.ROAMTYPE      ,         
				mergedrow.SERVICETYPE   ,         
				mergedrow.STARTTIME     ,         
				mergedrow.DURATION      ,         
				mergedrow.FLOWUP1       ,         
				mergedrow.FLOWDOWN1     ,         
				mergedrow.TIME1         ,         
				mergedrow.FLOWUP2       ,         
				mergedrow.FLOWDOWN2     ,         
				mergedrow.TIME2         ,         
				mergedrow.FLOWUP3       ,         
				mergedrow.FLOWDOWN3     ,         
				mergedrow.TIME3         ,         
				mergedrow.FLOWUP4       ,         
				mergedrow.FLOWDOWN4     ,         
				mergedrow.TIME4         ,         
				mergedrow.FLOWUP5       ,         
				mergedrow.FLOWDOWN5     ,         
				mergedrow.TIME5         ,         
				mergedrow.FLOWUP6       ,         
				mergedrow.FLOWDOWN6     ,         
				mergedrow.TIME6         ,         
				mergedrow.ERRORCODE     ,         
				mergedrow.DEVICETYPE    ,         
				mergedrow.SOURFILENAME  ,         
				mergedrow.PROCESSTIME   ,         
				mergedrow.DEVICEID      ,         
				mergedrow.DESTFILENAME  ,         
				mergedrow.HMANAGE       ,         
				mergedrow.BASICFEE      ,         
				mergedrow.BASICFEE_S    ,         
				mergedrow.BILLINGCYCLE  ,         
				mergedrow.SUBSCRIBERID  ,         
				mergedrow.ACCOUNTID     ,         
				mergedrow.TARIFFFLAG    ,         
				mergedrow.TOTAL_FREE    ,         
				mergedrow.TOTALBYTE     ,         
				mergedrow.TOTALBYTE_UP  ,         
				mergedrow.TOTALBYTE_DOWN,         
				mergedrow.CDRTYPE       ,         
				mergedrow.ROLLBACK_FLAG          
				from dual;
			insert into gprsrawmergecdr   select * from gprspartcdr 
				where chargingid=c_key.chargingid and ggsn=c_key.ggsn and starttime<=c_key.maxtime;
			delete from gprspartcdr where chargingid=c_key.chargingid and ggsn=c_key.ggsn and starttime<=c_key.maxtime;
		end if;
	end if;	 
<<nextone>>
	null;
end loop;           

end;
//




  CREATE OR REPLACE PROCEDURE "KJB_AP_SPDATALOAD" (v_date1 date)  is
/*---------------------------------------------------------
用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。

涉及的表：
SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。

业务类别编码	描述              类别      spid operid
001102	梦网短信               "SMS"       60  61
001103	一点结算梦网短信       "CM"        77  78
001104	农信通                 "CI"        ----
001122	捐款短信               "CHILD"     79  80
001123	流媒体                 "STREAM"    73  74
001322	手机邮箱               "EMAIL"     81  82
000103	梦网彩信               "MMS"       66  67
000104	WAP                    "WAP"       62  63
000105	手机动画               "FLASH"     75  76
000005	通用下载               "KJAVA"     68  69
008002	手机地图               "MAP"       ----
008001	无线音乐               "MUSIC"     90  91
000204	彩铃                   "CRING"     110 --

----------------------------------------------------------*/

v_mapping_id   number (10);

v_date         varchar2(14);
v_yestoday     varchar2(14);
v_filename     varchar2(40);
v_sourfilename varchar2(40);
v_lastfilename_0     varchar2(40);
v_lastfilename_1     varchar2(40);
v_info         varchar2(200);
v_count        number(10);

v_mapping_sour varchar2(200);
v_mapping_dest varchar2(200);

EXCEPTION_UNKNOWN_SERVTYPE EXCEPTION; --发现未知的serv_type
EXCEPTION_NO_DATA_LOAD     EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

begin

    dbms_output.put_line(': '||'ap_spdataload()开始运行...');
    select to_char(v_date1,'yyyymmdd') into v_date from dual;
    select to_char(v_date1-1,'yyyymmdd') into v_yestoday from dual;


    -------------------以下处理企业局数据--------------------------
    dbms_output.put_line(': '||'ap_spdataload()装入企业局数据...');
    v_mapping_id := 0;
    v_filename := 'MCBBJ____SP_INFO_'||v_date||'.txt';

    --查找上次处理的SP企业数据文件名
    select max(filename) into v_lastfilename_0 from spdataload_log where filename like 'MCBBJ_00_SP_INFO%';
    select max(filename) into v_lastfilename_1 from spdataload_log where filename like 'MCBBJ_01_SP_INFO%';
    if (length(v_lastfilename_0)<=0 or length(v_lastfilename_1)<=0) then
        dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
        raise EXCEPTION_NO_DATA_LOAD;
    end if;
    --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
    select count(*) into v_count from sepfilelog where filename like v_filename;
    if v_count <2 then
        dbms_output.put_line('表sepfilelog中今天（'||v_date||'）的数据业务SP信息文件未完全分拣（filename like '''||v_filename||'''），无法装载业务局数据。清先查看分拣日志。');
        raise EXCEPTION_NO_DATA_LOAD;
    end if;

    for c1 in --今天数据与上次处理数据对比
    (select serv_type,sp_code,serv_code,sp_name,prov_code,dev_code,valid_date,expire_date from spcodecdr where sourfilename like v_filename
     minus select serv_type,sp_code,serv_code,sp_name,prov_code,dev_code,valid_date,expire_date from spcodecdr
     where sourfilename =v_lastfilename_0 or sourfilename =v_lastfilename_1
    ) loop
	dbms_output.put_line('1..........');

    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
        case
        when c1.serv_type='001102' then v_mapping_id:=60;  --梦网短信
            v_mapping_sour:=c1.sp_code||','||c1.serv_code;
            v_mapping_dest:='1,'||c1.sp_name||','||c1.prov_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='001103' then v_mapping_id:=77;  --一点结算梦网短信
            v_mapping_sour:=c1.sp_code||','||c1.serv_code;
            v_mapping_dest:='1,'||c1.sp_name||','||c1.prov_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
        when c1.serv_type='001104' then v_mapping_id:=60;  --农信通
            v_mapping_sour:=c1.sp_code||','||c1.serv_code;
            v_mapping_dest:='1,'||c1.sp_name||','||c1.prov_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='001122' then v_mapping_id:=79;  --捐款短信
            v_mapping_sour:=c1.sp_code||','||c1.serv_code;
            v_mapping_dest:='1,'||c1.sp_name||','||c1.prov_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='001123' then v_mapping_id:=73;  --流媒体
            v_mapping_sour:=c1.sp_code||','||c1.dev_code;
            v_mapping_dest:=c1.sp_name||','||c1.prov_code||','||c1.serv_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';


        when c1.serv_type='001322' then v_mapping_id:=81;  --手机邮箱
            v_mapping_sour:=c1.sp_code||','||c1.serv_code;
            v_mapping_dest:='1,'||c1.sp_name||','||c1.prov_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='000103' then v_mapping_id:=66;  --梦网彩信
            v_mapping_sour:=c1.sp_code||','||c1.serv_code;
            v_mapping_dest:='1,'||c1.sp_name||','||c1.prov_code;
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='000104' then v_mapping_id:=62;  --WAP
            v_mapping_sour:=c1.sp_code;
            v_mapping_dest:='1,'||c1.sp_name||',';
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='000105' then v_mapping_id:=75;  --手机动画
            v_mapping_sour:=c1.sp_code;
            v_mapping_dest:='1,'||c1.sp_name||',';
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='000005' then v_mapping_id:=68;  --通用下载
            v_mapping_sour:=c1.sp_code;
            v_mapping_dest:='1,'||c1.sp_name||',';
            v_sourfilename:='MCBBJ_00_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='008002' then v_mapping_id:=-1;  --手机地图
        when c1.serv_type='008001' then v_mapping_id:=90;  --无线音乐
            v_mapping_sour:=c1.sp_code;
            v_mapping_dest:=c1.sp_name||','||c1.prov_code||','||c1.dev_code;
            v_sourfilename:='MCBBJ_01_SP_INFO_'||v_date||'.txt';

        when c1.serv_type='000204' then v_mapping_id:=110;  --彩铃
            v_mapping_sour:=c1.sp_code;
            v_mapping_dest:=c1.sp_name||',1,1|85';
            v_sourfilename:='MCBBJ_01_SP_INFO_'||v_date||'.txt';

        else
             v_info := '发现未定义的serv_type:' || c1.serv_type ||'，请确认';
             dbms_output.put_line('sql:['||v_info||']');
             raise EXCEPTION_UNKNOWN_SERVTYPE ;
        end case;

        if v_mapping_id<>-1 then
            --删除旧数据
			dbms_output.put_line('2..........');
            select count(*) into v_count from mapping_list where mapping_sour=v_mapping_sour and
            mapping_dest=v_mapping_dest and applytime=c1.valid_date and mapping_id=v_mapping_id;
            if (v_count >0) then
			dbms_output.put_line('3..........');
                delete from mapping_list where mapping_sour=v_mapping_sour and
                mapping_dest=v_mapping_dest and applytime=c1.valid_date and mapping_id=v_mapping_id and (expiretime>sysdate or expiretime is null);
                --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);
            end if;
            dbms_output.put_line(c1.serv_type||'....'||c1.sp_code||','||c1.serv_code);
            --插入新数据
            insert into mapping_list (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note)
            values (v_mapping_sour, v_mapping_dest, c1.valid_date, c1.expire_date, v_mapping_id, v_sourfilename);
        end if;

    end loop;
        --文件名插入处理日志
        insert into spdataload_log (filename,processtime) values ('MCBBJ_00_SP_INFO_'||v_date||'.txt',v_date1);
        insert into spdataload_log (filename,processtime) values ('MCBBJ_01_SP_INFO_'||v_date||'.txt',v_date1);


    -------------------以下处理业务局数据--------------------------
    dbms_output.put_line(': '||'ap_spdataload()装入业务局数据...');
    v_mapping_id :=0;
    v_filename := 'MCBBJ____SP_OPER_'||v_date||'.txt';
    --v_oldfilename := 'MCBBJ____SP_OPER_'||v_yestoday||'.txt';

    --查找上次处理的SP业务数据文件名
    select max(filename) into v_lastfilename_0 from spdataload_log where filename like 'MCBBJ_00_SP_OPER%';
    select max(filename) into v_lastfilename_1 from spdataload_log where filename like 'MCBBJ_01_SP_OPER%';
    if (length(v_lastfilename_0)<=0 or length(v_lastfilename_1)<=0) then
        dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
        raise EXCEPTION_NO_DATA_LOAD;
    end if;

    --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
    select count(*) into v_count from sepfilelog where filename like v_filename;
    if v_count <2 then
        dbms_output.put_line('表sepfilelog中今天（'||v_yestoday||'）的数据业务SP业务代码文件未完全分拣（filename like '''||v_filename||'''），无法装载业务局数据。清先查看分拣日志。');
        raise EXCEPTION_NO_DATA_LOAD;
    end if;
    --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
    for c1 in ----今天数据与上次处理数据对比
    (select serv_type,sp_code,operator_code,operator_name,content_code,bill_flag,fee,valid_date,expire_date,out_prop from spopercdr where sourfilename like v_filename --and serv_type=c1.serv_type
     minus select serv_type,sp_code,operator_code,operator_name,content_code,bill_flag,fee,valid_date,expire_date,out_prop from spopercdr
     where sourfilename =v_lastfilename_0 or sourfilename =v_lastfilename_1 --and serv_type=c1.serv_type
    ) loop
	dbms_output.put_line('4..........');
        case
        when c1.serv_type='001102' then v_mapping_id:=61;  --梦网短信
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='001103' then v_mapping_id:=78;  --一点结算梦网短信
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
        when c1.serv_type='001104' then v_mapping_id:=61;  --农信通
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='001122' then v_mapping_id:=80;  --捐款短信
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='001123' then v_mapping_id:=74;  --流媒体
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='001322' then v_mapping_id:=82;  --手机邮箱
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='000103' then v_mapping_id:=67;  --梦网彩信
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='000104' then v_mapping_id:=63;  --WAP
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='000105' then v_mapping_id:=76;  --手机动画
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='000005' then v_mapping_id:=69;  --通用下载
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||lpad(c1.bill_flag,2,'0')||'.'||c1.fee/10||',1|'||c1.out_prop||'|1';
            v_sourfilename:='MCBBJ_00_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='008002' then v_mapping_id:=-1;  --手机地图

        when c1.serv_type='008001' then v_mapping_id:=91;  --无线音乐
            v_mapping_sour:=c1.sp_code||','||c1.operator_code;
            v_mapping_dest:=c1.operator_name||','||c1.content_code||','||lpad(c1.bill_flag,2,'0')||','||c1.fee/10||','||c1.out_prop;
            v_sourfilename:='MCBBJ_01_SP_OPER_'||v_date||'.txt';

        when c1.serv_type='000204' then v_mapping_id:=-1;  --彩铃

        else
             v_info := '发现未定义的serv_type:' || c1.serv_type ||'，请确认';
             dbms_output.put_line('sql:['||v_info||']');
             raise EXCEPTION_UNKNOWN_SERVTYPE ;
        end case;

        if v_mapping_id<>-1 then
            --删除旧数据
			dbms_output.put_line('5..........');
            select count(*) into v_count from mapping_list where mapping_sour=v_mapping_sour and
            mapping_dest=v_mapping_dest and applytime=c1.valid_date and mapping_id=v_mapping_id;
            if (v_count >0) then
			dbms_output.put_line('6..........');
                delete from mapping_list where mapping_sour=v_mapping_sour and
                mapping_dest=v_mapping_dest and applytime=c1.valid_date and mapping_id=v_mapping_id and (expiretime>sysdate or expiretime is null);
                --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);
            end if;
            dbms_output.put_line(c1.serv_type||'....'||c1.sp_code||','||c1.operator_code);
            --插入新数据
            insert into mapping_list (mapping_sour, mapping_dest, applytime, expiretime, mapping_id, note)
            values (v_mapping_sour, v_mapping_dest, c1.valid_date, c1.expire_date, v_mapping_id, v_sourfilename);
        end if;

    end loop;
        --文件名插入处理日志
        insert into spdataload_log (filename,processtime) values ('MCBBJ_00_SP_OPER_'||v_date||'.txt',v_date1);
        insert into spdataload_log (filename,processtime) values ('MCBBJ_01_SP_OPER_'||v_date||'.txt',v_date1);

    dbms_output.put_line(': '||'ap_spdataload()运行结束.');

    --异常处理
    exception
    when EXCEPTION_UNKNOWN_SERVTYPE then
        rollback;
    when EXCEPTION_NO_DATA_LOAD then
        rollback;
    when others then
        dbms_output.put_line(v_date1||': '||substr(sqlerrm,1,200));
        if v_filename like 'MCBBJ____SP_INFO%' then
            dbms_output.put_line(v_date1||': '||' SP企业代码装载数据失败！');
        else
            dbms_output.put_line(v_date1||': '||' SP业务代码装载数据失败！');
        end if;

        rollback;

end kjb_ap_spdataload;
//





  CREATE OR REPLACE PROCEDURE "KJB_SPDATALOAD" IS
  /*---------------------------------------------------------
  用途：集团下发数据业务SP信息和业务编码文件分拣入库后，从详单表导入mapping_list，
  采用增量导入方式，对比当前处理的文件和上次处理的文件的差异，将差异导入mapping_list。

  涉及的表：
  SP信息数据表 SPCODECDR：存放导入的正常SP信息局数据。
  SP业务编码数据表 SPOPERCDR：存放导入的正常SP业务编码局数据。
  SP装载日志表 SPDATALOAD_LOG: 记录处理过的文件。

  业务类别编码  描述              类别      spid operid
  001102  梦网短信               "SMS"       60  61
  001103  一点结算梦网短信       "CM"        77  78
  001104  农信通                 "CI"        ----
  001122  捐款短信               "CHILD"     79  80
  001123  流媒体                 "STREAM"    73  74
  001322  手机邮箱               "EMAIL"     81  82
  000103  梦网彩信               "MMS"       66  67
  000104  WAP                    "WAP"       62  63
  000105  手机动画               "FLASH"     75  76
  000005  通用下载               "KJAVA"     68  69
  008002  手机地图               "MAP"       ----
  008001  无线音乐               "MUSIC"     90  91
  000204  彩铃                   "CRING"     110 --
  ----------------------------------------------------------*/
  --变量定义
  
  v_mapping_id NUMBER(10);

  v_date         VARCHAR2(14);
  v_filename     VARCHAR2(40);
  v_filename1     VARCHAR2(40);
  v_sourfilename VARCHAR2(40);
  --v_oldfilename     varchar2(40);
  v_lastfilename_0 VARCHAR2(40);
  v_lastfilename_1 VARCHAR2(40);
  v_info           VARCHAR2(200);
  --v_sql          varchar2(400);
  v_count NUMBER(10);

  v_mapping_sour VARCHAR2(200);
  v_mapping_dest VARCHAR2(200);

  exception_unknown_servtype EXCEPTION; --发现未知的serv_type
  exception_no_data_load EXCEPTION; --未找到上次处理的文件或今天的数据文件尚未分拣

BEGIN

  dbms_output.put_line(': ' || 'ap_spdataload()开始运行...');
  SELECT to_char(SYSDATE -1, 'yyyymmdd') INTO v_date FROM dual;
  --v_filename := 'MCBBJ____SP_INFO_20070404'||'.txt';
  --dbms_output.put_line('sql:['||v_filename||']');

  -------------------以下处理企业局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入企业局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ____SP_INFO_' || v_date || '.txt';

  --查找上次处理的SP企业数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_INFO%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_INFO%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP信息文件名的记录，无法装载今天的企业局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;
  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
   WHERE filename LIKE v_filename;
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP信息文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;

  FOR c1 IN --今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename LIKE v_filename
    MINUS
    SELECT serv_type,
           sp_code,
           serv_code,
           sp_name,
           prov_code,
           dev_code,
           valid_date,
           expire_date
      FROM spcodecdr
     WHERE sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1)
  LOOP
  dbms_output.put_line('1..........');

    --for c1 in (select distinct(serv_type) from SPCODECDR where sourfilename like v_filename ) loop
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 60; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 77; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 60; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 79; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 73; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.dev_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.serv_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 81; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 66; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.serv_code;
        v_mapping_dest := '1,' || c1.sp_name || ',' || c1.prov_code;
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 62; --WAP
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 75; --手机动画
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 68; --通用下载
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := '1,' || c1.sp_name || ',';
        v_sourfilename := 'MCBBJ_00_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图
      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 90; --无线音乐
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',' || c1.prov_code || ',' ||
                          c1.dev_code;
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';

      WHEN c1.serv_type = '000204' THEN
        v_mapping_id   := 110; --彩铃
        v_mapping_sour := c1.sp_code;
        v_mapping_dest := c1.sp_name || ',1,1|85';
        v_sourfilename := 'MCBBJ_01_SP_INFO_' || v_date || '.txt';

      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;
  --dbms_output.put_line('['||c1.serv_type||','||v_mapping_id||']');

    IF v_mapping_id <> -1
    THEN
	dbms_output.put_line('2..........');
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('3..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.serv_code);
      END IF;
      dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
                           c1.serv_code);
      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
          NULL;
      END;
    END IF;

  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_INFO_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_INFO_' || v_date || '.txt', SYSDATE);
  COMMIT;

  -------------------以下处理业务局数据--------------------------
  dbms_output.put_line(': ' || 'ap_spdataload()装入业务局数据...');
  v_mapping_id := 0;
  v_filename   := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';
  v_filename1  := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';

  --查找上次处理的SP业务数据文件名
  SELECT MAX(filename)
    INTO v_lastfilename_0
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_00_SP_OPER%';
  SELECT MAX(filename)
    INTO v_lastfilename_1
    FROM spdataload_log
   WHERE filename LIKE 'MCBBJ_01_SP_OPER%';
  IF (length(v_lastfilename_0) <= 0 OR length(v_lastfilename_1) <= 0)
  THEN
    dbms_output.put_line('表spdataload_log中没有上次处理的SP业务数据文件名的记录，无法装载今天的业务局数据。清先查看处理日志。');
    RAISE exception_no_data_load;
  END IF;

  --先查找今天数据文件是否已分拣，如果未分拣，不做处理。
  SELECT COUNT(*)
    INTO v_count
    FROM sepfilelog
    WHERE filename = v_filename or filename= v_filename1; --20080118 modify
  IF v_count < 2
  THEN
    dbms_output.put_line('表sepfilelog中今天（' || v_date ||
                         '）的数据业务SP业务代码文件未完全分拣（filename like ''' ||
                         v_filename || '''），无法装载业务局数据。清先查看分拣日志。');
    RAISE exception_no_data_load;
  END IF;
  --for c1 in (select distinct(serv_type) from SPOPERCDR where sourfilename like v_filename) loop
  FOR c1 IN ----今天数据与上次处理数据对比
   (SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     WHERE (sourfilename = v_filename or sourfilename = v_filename1) --20080118 modify
     --WHERE sourfilename LIKE v_filename --and serv_type=c1.serv_type
    MINUS
    SELECT serv_type,
           sp_code,
           operator_code,
           operator_name,
           content_code,
           bill_flag,
           fee,
           valid_date,
           expire_date,
           out_prop
      FROM spopercdr
     WHERE (sourfilename = v_lastfilename_0
        OR sourfilename = v_lastfilename_1) --and serv_type=c1.serv_type
    )
  LOOP
  dbms_output.put_line('4..........');
    CASE
      WHEN c1.serv_type = '001102' THEN
        v_mapping_id   := 61; --梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '001103' THEN
        v_mapping_id   := 78; --一点结算梦网短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

    ---when c1.serv_type='001104' then v_mapping_id:=-1;  --农信通
      WHEN c1.serv_type = '001104' THEN
        v_mapping_id   := 61; --农信通
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '001122' THEN
        v_mapping_id   := 80; --捐款短信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '001123' THEN
        v_mapping_id   := 74; --流媒体
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '001322' THEN
        v_mapping_id   := 82; --手机邮箱
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000103' THEN
        v_mapping_id   := 67; --梦网彩信
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000104' THEN
        v_mapping_id   := 63; --WAP
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000105' THEN
        v_mapping_id   := 76; --手机动画
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000005' THEN
        v_mapping_id   := 69; --通用下载
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' ||
                          lpad(c1.bill_flag, 2, '0') || '.' || c1.fee / 10 ||
                          ',1|' || c1.out_prop || '|1';
        v_sourfilename := 'MCBBJ_00_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '008002' THEN
        v_mapping_id := -1; --手机地图

      WHEN c1.serv_type = '008001' THEN
        v_mapping_id   := 91; --无线音乐
        v_mapping_sour := c1.sp_code || ',' || c1.operator_code;
        v_mapping_dest := c1.operator_name || ',' || c1.content_code || ',' ||
                          lpad(c1.bill_flag, 2, '0') || ',' || c1.fee / 10 || ',' ||
                          c1.out_prop;
        v_sourfilename := 'MCBBJ_01_SP_OPER_' || v_date || '.txt';

      WHEN c1.serv_type = '000204' THEN
        v_mapping_id := -1; --彩铃

      ELSE
        v_info := '发现未定义的serv_type:' || c1.serv_type || '，请确认';
        dbms_output.put_line('sql:[' || v_info || ']');
        RAISE exception_unknown_servtype;
    END CASE;

    IF v_mapping_id <> -1
    THEN
      --删除旧数据
      SELECT COUNT(*)
        INTO v_count
        FROM mapping_list
       WHERE mapping_sour = v_mapping_sour
         AND mapping_dest = v_mapping_dest
         AND applytime = c1.valid_date
         AND mapping_id = v_mapping_id;
      IF (v_count > 0)
      THEN
	  dbms_output.put_line('5..........');
        DELETE FROM mapping_list
         WHERE mapping_sour = v_mapping_sour
           AND mapping_dest = v_mapping_dest
           AND applytime = c1.valid_date
           AND mapping_id = v_mapping_id
           AND (expiretime > SYSDATE OR expiretime IS NULL);
        --dbms_output.put_line(c1.serv_type||'-----'||c1.sp_code||','||c1.operator_code);
      END IF;
      dbms_output.put_line(c1.serv_type || '....' || c1.sp_code || ',' ||
                           c1.operator_code);
      --插入新数据
      BEGIN
        INSERT INTO mapping_list
          (mapping_sour,
           mapping_dest,
           applytime,
           expiretime,
           mapping_id,
           note)
        VALUES
          (v_mapping_sour,
           v_mapping_dest,
           c1.valid_date,
           c1.expire_date,
           v_mapping_id,
           v_sourfilename);
      EXCEPTION
        --WHEN OTHERS THEN
        WHEN DUP_VAL_ON_INDEX THEN
          NULL;
      END;

    END IF;

  END LOOP;
  --文件名插入处理日志
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_00_SP_OPER_' || v_date || '.txt', SYSDATE);
  INSERT INTO spdataload_log
    (filename, processtime)
  VALUES
    ('MCBBJ_01_SP_OPER_' || v_date || '.txt', SYSDATE);

  COMMIT;
  dbms_output.put_line(': ' || 'ap_spdataload()运行结束.');

  --异常处理
EXCEPTION
  WHEN exception_unknown_servtype THEN
    ROLLBACK;
  WHEN exception_no_data_load THEN
    ROLLBACK;
  WHEN OTHERS THEN
    --     dbms_output.put_line('error: '||substr(v_sql,1,200));
    --     dbms_output.put_line(substr(v_sql,201));
    dbms_output.put_line(SYSDATE || ': ' || substr(SQLERRM, 1, 200));
    IF v_filename LIKE 'MCBBJ____SP_INFO%'
    THEN
      dbms_output.put_line(SYSDATE || ': ' || ' SP企业代码装载数据失败！');
    ELSE
      dbms_output.put_line(SYSDATE || ': ' || ' SP业务代码装载数据失败！');
    END IF;
    ROLLBACK;
END kjb_spdataload;
//




  CREATE OR REPLACE PROCEDURE "LUJG_PROCESS_EXCDR_200510" (V_REGION varchar2,v_nodeid number,v_tablename VARCHAR2,v_cond varchar2)
is
    Type CDRCur is ref cursor;
    
    cdr_cv CDRCur;

   
   v_fields varchar2(256);
   debugflag number(1);        
   v_sql  varchar2(256);
   v_groupcond varchar2(256);
   v_insertsql varchar2(512); 
   v_fieldsql varchar2(256); 
   v_condsql varchar2(256);
begin

    debugflag := 1;    
    
   
   SELECT FIELDS,groupcond INTO V_FIELDS,v_groupcond FROM  EX_CDR_TAB_MAP WHERE upper(TABLE_NAME)=upper(V_TABLENAME);
   
   v_fieldsql := 'select '||to_char(nvl(v_region,'NULL'))||','||to_char(v_nodeid)||','||v_fields;
    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||v_fieldsql);
  
    end if;

    if v_region is null
    then
        v_condsql := ' where hregion is null'||' ';
    else
        v_condsql := ' where hregion ='||v_region||' ';
    end if;
   if length(v_cond) > 0
   then
       v_condsql := v_condsql||'and '||v_cond||' ';   
   end if; 
   
   v_sql := v_fieldsql||' from '||v_tablename||v_condsql||v_groupcond ;
   
/*   if length(v_cond) > 0
   then
       v_sql := v_fieldsql||' from '||v_tablename||' where '||v_cond||' '||v_groupcond ;
   else
       v_sql := v_fieldsql||' from '||v_tablename||' '||v_groupcond ;
   end if;
*/
   v_insertsql := 'insert into    ex_cdr_rec_base '
   ||'(region,nodeid,errclass,errtype,errtotal,statdate,cdrdate,cdrclass,fee,amountclass,amount) '
   || v_sql;


    dbms_output.put_line('1..........');
    if debugflag =1 then

        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,1,230));        
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,231));                
    end if;
    execute  immediate  v_insertsql;
   
    return;   
/*create table ex_cdr_rec_base(
    region          number(6),    --
    nodeid          number(6),     --统计的主机ID ,使用环境变量NODEID
    errclass        varchar2(8),   --错误表类型：nocharge.error.empty
    errtype         varchar2(8),   --错误类型
    errtotal        number(6),     --异常话单总数         
    statdate        date,          --统计日
    cdrdate         date,          --话单日期
    CDRCLASS        varchar2(8) ,   --话单种类
    fee             number(14,2), -- 回收金额
    amountclass     number(2),     --业务量类别：1 :minute ;2 :kbyte;3 : time
    amount          number(12)      --业务量
);
*/   

    return;

        open cdr_cv for v_sql;        
        
        loop
            --fetch cdr_cv into c_value;
            null;
            
            exit when cdr_cv%NOTFOUND ;
    
            if debugflag =1 then    
             dbms_output.put_line('sqlbuf:3');
            end if;

        end loop;

                    
        close cdr_Cv;
        

     dbms_output.put_line('now:'||sysdate); 
    
    commit;   
    
end ;
//




  CREATE OR REPLACE PROCEDURE "P_BL_COLLECT_SEPCHK" (i_Cycle NUMBER) IS
  /*******************************************************************************
  作    者:裘学欣39824
  创建时间:2006-05-12
  修改时间:
  --------------------------------------------------------------------------------
  主要功能:生成采集与预处理文件对比检查结果表COLLECT_SEPCHK
  *******************************************************************************/
  v_Startcycle DATE; /*检查开始时间*/
  v_Endcycle   DATE; /*检查结束时间*/
  v_Lastcycle  DATE; /*检查开始时间前两天*/
  v_Cycle      NUMBER; /*周期变量，当传入周期为0时，置缺省值*/
  v_Count      NUMBER; /*文件个数*/

BEGIN

  IF i_Cycle = 0 THEN

    IF To_Char(SYSDATE, 'hh24') > '05' THEN

      /*当传入周期为0时，并且当前时间超过5点时，缺省为当天*/
      v_Cycle := To_Char(SYSDATE, 'yyyymmdd');

    ELSE

      /*当传入周期为0时，并且当前时间未超过5点时，缺省为昨天*/
      v_Cycle := To_Char(SYSDATE - 1, 'yyyymmdd');

    END IF;

  ELSE

    /*置周期变量为传入周期*/
    v_Cycle := i_Cycle;

  END IF;

  v_Startcycle := To_Date(v_Cycle, 'yyyymmdd'); /*检查开始时间*/
  v_Endcycle   := To_Date(v_Cycle, 'yyyymmdd') + 1; /*检查结束时间*/
  v_Lastcycle  := To_Date(v_Cycle, 'yyyymmdd') - 2; /*检查开始时间前两天*/

  DELETE Collect_Sepchk
   WHERE Collectfiletime >= v_Startcycle
     AND Collectfiletime < v_Endcycle
     AND Collectfilename IS NULL; /*删除检查周期中只有预处理文件的数据*/

  COMMIT;

  FOR c_Collect IN (SELECT Ceid,
                           Collectfilepath,
                           Collectfilename,
                           Collectfilesize,
                           Collectfiletime
                      FROM Collectfilelog
                     WHERE 
                       Collectfiletime >= v_Startcycle - 5/24/60
                       AND Collectfiletime < v_Endcycle + 5/24/60) 
                     --instr(collectfilepath,v_Cycle)<>0 ) 
                     LOOP
    BEGIN
	dbms_output.put_line('1.........');
      INSERT INTO Collect_Sepchk
        (Ceid,
         Collectfilepath,
         Collectfilename,
         Collectfilesize,
         Collectfiletime)
      VALUES
        (c_Collect.Ceid,
         c_Collect.Collectfilepath,
         c_Collect.Collectfilename,
         c_Collect.Collectfilesize,
         c_Collect.Collectfiletime); /*插入新增采集日志数据*/

      COMMIT;

    EXCEPTION
      WHEN OTHERS THEN

        ROLLBACK;

    END;

  END LOOP;

  COMMIT;

  /*取检查周期数据，包含上个周期不一致数据*/
  FOR c_Collect IN (SELECT ROWID, /*rowid*/
                           Collectfilename /*文件名*/
                      FROM Collect_Sepchk
                     WHERE Collectfiletime >= v_Lastcycle
                       AND Collectfiletime < v_Endcycle + 5/24/60
                       AND Isok = '0') LOOP
	dbms_output.put_line('2.........');
    /*查找预处理日志Sepfilelog同名文件*/
    FOR c_Sep IN (SELECT Filename         Sepfilename, /*预处理文件名*/
                         Filesize         Sepfilesize, /*预处理文件字节数*/
                         Beginprocessdate Sepfiletime /*预处理时间*/
                    FROM Sepfilelog
                   WHERE Filename = c_Collect.Collectfilename) LOOP
	dbms_output.put_line('3.........');
      UPDATE Collect_Sepchk
         SET Sepfilename = c_Sep.Sepfilename, /*置预处理文件名*/
             Sepfilesize = c_Sep.Sepfilesize, /*置预处理文件字节数*/
             Sepfiletime = c_Sep.Sepfiletime, /*置预处理时间*/

             /*置文件一致性标识，当采集文件大小等于预处理文件大小时置为'1'*/
             Isok = Decode(Collectfilesize, c_Sep.Sepfilesize, '1', '0')
       WHERE ROWID = c_Collect.ROWID;

    END LOOP;

    COMMIT;

    /*查找漫游文件处理日志Visittranslog同名文件*/
    FOR c_Sep IN (SELECT Filename    Sepfilename, /*预处理文件名*/
                         0           Sepfilesize, /*预处理文件字节数*/
                         Processtime Sepfiletime /*预处理时间*/
                    FROM Visittranslog
                   WHERE Filename = c_Collect.Collectfilename) LOOP
		dbms_output.put_line('4.........');
      UPDATE Collect_Sepchk
         SET Sepfilename = c_Sep.Sepfilename, /*置预处理文件名*/
             Sepfilesize = c_Sep.Sepfilesize, /*置预处理文件字节数*/
             Sepfiletime = c_Sep.Sepfiletime, /*置预处理时间*/
             Isok        = '1' /*置文件一致性标识为'1'*/
       WHERE ROWID = c_Collect.ROWID;

    END LOOP;

    COMMIT;

  END LOOP;

  /*处理预处理日志Sepfilelog或漫游文件处理日志Visittranslog中有文件，
  但采集日志中没有的文件，插入采集与预处理文件对比检查表中*/
  FOR c_Sep IN (SELECT Filename         Sepfilename, /*预处理文件名*/
                       Filesize         Sepfilesize, /*预处理文件字节数*/
                       Beginprocessdate Sepfiletime /*预处理时间*/
                  FROM Sepfilelog
                 WHERE Beginprocessdate >= v_Startcycle
                   AND Beginprocessdate < v_Endcycle
               UNION 
               SELECT  Filename         Sepfilename, /*预处理文件名*/
                       0                Sepfilesize, /*预处理文件字节数*/
                       Processtime      Sepfiletime /*预处理时间*/
                  FROM Visittranslog
                 WHERE Processtime >= v_Startcycle
                   AND Processtime < v_Endcycle
               ) LOOP
	dbms_output.put_line('5.........');
    SELECT nvl(sum(collectfilesize),-1) --count(*)很慢
      INTO v_Count
      FROM Collect_Sepchk
     WHERE Collectfilename = c_Sep.Sepfilename;

    IF v_Count < 0 THEN

      INSERT INTO Collect_Sepchk
        (Sepfilename,
         Sepfilesize,
         Sepfiletime,
         Collectfiletime,
         Isok)
      VALUES
        (c_Sep.Sepfilename, /*置预处理文件名*/
         c_Sep.Sepfilesize, /*置预处理文件字节数*/
         c_Sep.Sepfiletime, /*置预处理时间*/
         c_Sep.Sepfiletime, /*置采集时间，为查询使用*/
         '0' /*置文件一致性标识为'0'*/);

      COMMIT;

    END IF;

  END LOOP;

END p_Bl_Collect_Sepchk;
//




  CREATE OR REPLACE PROCEDURE "P_ERROR_STAT" (V_REGION varchar2,v_nodeid number,v_tablename VARCHAR2,v_cond varchar2)
is

   v_fields varchar2(256);
   debugflag number(1);
   v_sql  varchar2(512);
   v_groupcond varchar2(256);
   v_insertsql varchar2(512);
   v_fieldsql varchar2(256);
   v_condsql varchar2(256);
   v_defcond varchar2(256);
   v_statdate date;

begin
    debugflag := 0;

    begin
    SELECT FIELDS,groupcond,cond INTO v_fields,v_groupcond, v_defcond FROM  stat_error_cond1 WHERE upper(TABLE_NAME)=upper(v_tablename);
    exception when others  then
        dbms_output.put_line('table:'||v_tablename||',err:'||sqlerrm);
        return;
    end ;

    v_fieldsql := 'select /*+parallel(a,8)*/ '|| to_char(nvl(v_region,'NULL'))||','||to_char(v_nodeid)||','||v_fields;
    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||v_fieldsql);
    end if;

    if v_region is null
    then
        v_condsql := ' a where hregion is null'||' ';
    elsif v_region = 999
         then
             v_condsql := ' a  where 1=1 ';
         else
             v_condsql := ' a  where hregion ='||v_region||' ';
    end if;
   if length(v_cond) > 0
   then
       v_condsql := v_condsql||'and '||v_cond||v_defcond||' ';
   end if;

   v_sql := v_fieldsql||' from '||v_tablename||v_condsql||v_groupcond ;

   v_insertsql := 'insert into stat_error_base '
   ||'(region,nodeid,cdrclass,errclass,errtype,errtotal) '
   || v_sql;

    if debugflag =1 then
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,1,230));
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,231));
    end if;
   execute  immediate  v_insertsql;
   commit;

    v_statdate:=trunc(sysdate);
    insert into stat_error_log (STATDATE, REGION, NODEID, TABLENAME, COND, PARTNUM, ENDPARTNUM, STARTTIME, ENDTIME, END_FLAG)
        values (v_statdate, v_region, v_nodeid, v_tablename, v_cond, 1, 0, sysdate, null, '0');

    update stat_error_log set endtime=sysdate, end_flag='1', endpartnum=1 where tablename= v_tablename and cond=v_cond
        and statdate=v_statdate;
	dbms_output.put_line('1..........');
    commit;
    return;

/*    exception when others  then
        dbms_output.put_line('procedure() error:');
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,1,230));
        dbms_output.put_line('sqlbuf:'||substr(v_insertsql,231));
        rollback;
        return;*/
end p_error_stat;
//




  CREATE OR REPLACE PROCEDURE "P_PARAMWIZARD" (p_Item  VARCHAR2,
                                          p_Id    NUMBER,
                                          p_Id2   NUMBER,
                                          p_Value OUT BLOB) IS

  /*******************************************************************************
  作    者:裘学欣39824
  创建时间:2006-06-8
  修改时间:
  --------------------------------------------------------------------------------
  主要功能:BOSS V200R002C05B17计费参数维护向导，更新BLOB字段数据
  *******************************************************************************/
BEGIN

  /*计费参数配置向导表PARAMWIZARD*/
  IF p_Item = 'WIZARDGUIDE' THEN
  dbms_output.put_line('1..........');
    UPDATE Paramwizard
       SET Wizardguide = Empty_Blob()
     WHERE Wizardid = p_Id RETURNING Wizardguide INTO p_Value;
  END IF;

  /*计费参数配置向导操作表PARAMWIZARDOP*/
  IF p_Item = 'OPGUIDE' THEN
  dbms_output.put_line('2..........');
    UPDATE Paramwizardop
       SET Opguide = Empty_Blob()
     WHERE Wizardid = p_Id
       AND Opid = p_Id2 RETURNING Opguide INTO p_Value;
  END IF;

  /*计费参数配置向导操作表PARAMWIZARDOP*/
  IF p_Item = 'OPLOGMODUL' THEN
  dbms_output.put_line('3..........');
    UPDATE Paramwizardop
       SET Oplogmodul = Empty_Blob()
     WHERE Wizardid = p_Id
       AND Opid = p_Id2 RETURNING Oplogmodul INTO p_Value;
  END IF;

  /*计费参数配置向导任务列表PARAMTASKLIST*/
  IF p_Item = 'TASKINFO' THEN
  dbms_output.put_line('4..........');
    UPDATE Paramtasklist
       SET Taskinfo = Empty_Blob()
     WHERE Taskid = p_Id RETURNING Taskinfo INTO p_Value;
  END IF;

  /*计费参数配置向导任务操作列表PARAMTASKOPLIST*/
  IF p_Item = 'OPLOGINFO' THEN
  dbms_output.put_line('5..........');
    UPDATE Paramtaskoplist
       SET Oploginfo = Empty_Blob()
     WHERE Taskid = p_Id
       AND Opid = p_Id2 RETURNING Oploginfo INTO p_Value;
  END IF;

END;
//




  CREATE OR REPLACE PROCEDURE "QUERY_CHILD" (
subsoid# in Varchar2,
accoid# in Varchar2,
usagetype# in Varchar2,
yearmon# in number,
feelist# OUT Varchar2
) is
v_sql varchar2(2000);
v_fee number(12,2);
v_cid INTEGER;
v_updaterows INTEGER;

v_accoid varchar2(16);
v_subsoid varchar2(16);
v_yearmon varchar2(16);
v_flag integer;
begin
	v_accoid := trim(accoid#);
	v_subsoid := trim(subsoid#);
	v_yearmon := to_char(yearmon#);
	for c in (select * from int_feetype
                   where usagetype = usagetype#
                   and not sourcetable is null
                   and not sourcecolumn is null
                   order by sourcetable,feetypeid) loop
		dbms_output.put_line('1..........');
		v_flag := 0;
      	v_sql := 'select ';
      	if (c.iscode = '1') then
		dbms_output.put_line('2..........');
          	v_sql := v_sql||'sum('||c.feecolumnname||')';
          	v_sql := v_sql||' from '||c.sourcetable||' where '||c.cyclecolumnname||'=to_number(:yearmon)';
          	v_sql := v_sql||' and '||c.accoidcolumnname||'=to_number(:accoid)';
          	if (length(v_subsoid) > 0) then
          		v_sql := v_sql||' and '||c.subsoidcolumnname||'=to_number(:subsoid) ';
          		v_flag := 1;
          	end if;
          	v_sql := v_sql||' and '||c.codecolumnname||' in ('||c.sourcecolumn||')';
      	else
		dbms_output.put_line('3..........');
          	v_sql := v_sql||'sum('||c.sourcecolumn||')';
          	v_sql := v_sql||' from '||c.sourcetable||' where '||c.cyclecolumnname||'=to_number(:yearmon) ';
          	v_sql := v_sql||' and '||c.accoidcolumnname||'=to_number(:accoid)';
          	if (length(v_subsoid) > 0) then
	             v_sql := v_sql||' and '||c.subsoidcolumnname||'=to_number(:subsoid)';
	             v_flag := 1;
	    	    end if;
      	end if;

      	v_cid := DBMS_SQL.OPEN_CURSOR;
	    DBMS_SQL.PARSE(v_cid, v_sql, dbms_sql.v7);
	    DBMS_SQL.BIND_VARIABLE(v_cid, ':yearmon', v_yearmon);
	    DBMS_SQL.BIND_VARIABLE(v_cid, ':accoid', v_accoid);
	    IF v_flag = 1 then
		    DBMS_SQL.BIND_VARIABLE(v_cid, ':subsoid', v_subsoid);
		END IF;
      	DBMS_SQL.DEFINE_COLUMN(v_cid, 1, v_fee);
	    v_updaterows := DBMS_SQL.EXECUTE(v_cid);
      	IF DBMS_SQL.FETCH_ROWS(v_cid)>0 THEN
          	DBMS_SQL.COLUMN_VALUE(v_cid, 1, v_fee);
      	ELSE
          	v_fee := 0;
      	END IF;
      	DBMS_SQL.CLOSE_CURSOR(v_cid);
      	if (v_fee!= 0) then
         	feelist# := feelist#||c.feetypeid||';'||to_char(v_fee*100)||';';
      	end if;
  	end loop;
end;
//




CREATE OR REPLACE PROCEDURE "QUERYBILL" 
(region# in Varchar2, 	
subsoid# in Varchar2, 	
accoid# in Varchar2, 	  
accYearmon1# in Varchar2,
accYearmon2# in Varchar2,
needDisc# in Varchar2,
feelist1# OUT Varchar2,
disclist1# OUT Varchar2,
feelist2# OUT Varchar2,
disclist2# OUT Varchar2,
retcode# out Varchar2
) is
v_accYearmon1 number(6);
v_accYearmon2 number(6);
v_signTwoMon number(6);
v_date date;
begin

  v_accYearmon1 := to_number(accYearmon1#);

  v_accYearmon2 := to_number(accYearmon2#);

  if (v_accYearmon2<>0) then
  dbms_output.put_line('1..........');
      v_signTwoMon := 1;
  else
      v_signTwoMon := 0;
  end if;


  feelist1# := '';
  feelist2# := '';
  query_child(subsoid#,accoid#,'feutCll',v_accYearmon1,feelist1#);
  if (v_signTwoMon = 1) then
  dbms_output.put_line('1..........');
     query_child(subsoid#,accoid#,'feutCll',v_accYearmon2,feelist2#);
  end if;

  disclist1# := '';
  disclist2# := '';
  retcode# := '1';
end;
//




  CREATE OR REPLACE PROCEDURE "SDCJ_DATA_ANALYSE" ( v_analyse_type in varchar2,   
                                               v_analyse_date in varchar2
                                             ) as
         v_sql           varchar2(1024);
         v_week_begin    varchar2(10);
         v_week_end      varchar2(10);
         v_month_begin   varchar2(10);
         v_month_end     varchar2(10);
         v_tableA_name   varchar2(20);
         v_tableB_name   varchar2(20);
         v_loop_day      varchar2(5);
         v_while_day     varchar2(5);
         v_partition     varchar2(10);
         v_date_next     varchar2(20);
         v_time_begin    varchar2(20);
         v_time_end      varchar2(20);
         v_hour_begin    varchar2(10);
         v_hour_end      varchar2(10);
         v_loop_time     varchar2(10);
         v_minute_begin  varchar2(10);
         v_minute_end    varchar2(10);
  begin
    --w 按周分析
    if(v_analyse_type = 'w') then
	dbms_output.put_line('1..........');
      --取周一
      select to_char(trunc(to_date(v_analyse_date,'yyyymmdd'), 'd') + 1,'yyyymmdd') into v_week_begin from dual;
      --取周日
      select to_char(trunc(to_date(v_analyse_date,'yyyymmdd'), 'd') + 7,'yyyymmdd') into v_week_end from dual;
      --判断跨月
      if  substr(v_week_begin,1,6) != substr(v_week_end,1,6) then
	  dbms_output.put_line('2..........');
        --取周日所在的月初
        select to_char(trunc(to_date(v_week_end,'yyyymmdd'),'month'),'yyyymmdd') into v_month_begin from dual;
        --取周一所在的月底
        select to_char(last_day(trunc(to_date(v_week_begin,'yyyymmdd'),'month')),'yyyymmdd') into v_month_end from dual;
        v_tableA_name:='cprun_'||substr(v_week_begin,1,6);
        v_tableB_name:='cprun_'||substr(v_week_end,1,6);
        v_loop_day:=substr(v_week_begin,7,2);
        v_while_day:=to_number(substr(v_month_end,7,2))+to_number(substr(v_week_end,7,2));
        v_date_next:=v_week_begin;
        while to_number(v_loop_day) <= to_number(v_while_day)  loop
		dbms_output.put_line('3..........');
          v_loop_day:=lpad(v_loop_day,2,'0');
          v_partition:='P_00'||substr(v_date_next,7,2);
          if to_number(v_loop_day) <= to_number(substr(v_month_end,7,2)) then
		  dbms_output.put_line('4..........');
            v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
            select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
            sum(t.filesize)/86400 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
            where t.id=c.id
            group by t.id,c.description ,c.node';
            --execute immediate v_sql;
          else
		  dbms_output.put_line('5..........');
            v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
            select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
            sum(t.filesize)/86400 from '||v_tableB_name||' partition('||v_partition||') t ,cpconfig c
            where t.id=c.id
            group by t.id,c.description ,c.node';
            --execute immediate v_sql; 
          end if;
          execute immediate v_sql;
          v_loop_day:=v_loop_day+1;
          v_date_next:=to_char(to_date(v_date_next,'yyyymmdd')+1,'yyyymmdd');
        end loop;
        commit;
      else
	  dbms_output.put_line('6..........');
        --取表
        v_tableA_name:='cprun_'||substr(v_week_begin,1,6);           
        --按分区获取数据
        v_loop_day:=substr(v_week_begin,7,2);
        v_while_day:=substr(v_week_end,7,2);
        v_date_next:=v_week_begin;
        while to_number(v_loop_day) <= to_number(v_while_day)  loop
		dbms_output.put_line('7..........');
          /*select * from v_tableA_name t where t.day=substr(v_week_begin,7,2)*/
          v_loop_day:=lpad(v_loop_day,2,'0');
          --取分区
          v_partition:='P_00'||v_loop_day;
          --v_loop_day:=4;
          v_loop_day:=v_loop_day+1;
          v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/86400 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id
          group by t.id,c.description ,c.node';
          execute immediate v_sql;
          --select  to_char(to_date('20150409','yyyymmdd')+1,'yyyymmdd') from dual;
          v_date_next:=to_char(to_date(v_date_next,'yyyymmdd')+1,'yyyymmdd');
        end loop;       
        commit;	
      end if;  
    end if;
    --m 按月分析
    if(v_analyse_type = 'm') then
	dbms_output.put_line('8..........');
      --select trunc(sysdate, 'mm') from dual;
      --select last_day(trunc(sysdate)) from dual;
      select to_char(trunc(to_date(v_analyse_date,'yyyymmdd'), 'mm'),'yyyymmdd') into v_month_begin from dual;
      select to_char(last_day(trunc(to_date(v_analyse_date,'yyyymmdd'))),'yyyymmdd') into v_month_end from dual;
      --取表
      v_tableA_name:='cprun_'||substr(v_analyse_date,1,6);
      v_loop_day:=substr(v_month_begin,7,2);
      v_while_day:=substr(v_month_end,7,2);
      v_date_next:=v_month_begin;
      while to_number(v_loop_day) <= to_number(v_while_day) loop
	  dbms_output.put_line('9..........');
	      v_partition:='P_00'||substr(v_date_next,7,2);
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/86400 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id 
          group by t.id,c.description ,c.node';
        execute immediate v_sql;
        v_loop_day:=v_loop_day+1;
        v_date_next:=to_char(to_date(v_date_next,'yyyymmdd')+1,'yyyymmdd');
      end loop;
      commit;      
    end if;
    --d 按天分析
    if(v_analyse_type = 'd') then
	dbms_output.put_line('10..........');
      v_tableA_name:='cprun_'||substr(v_analyse_date,1,6);
      v_partition:='P_00'||substr(v_analyse_date,7,2);
      v_hour_begin:='00';
      v_hour_end:='01';
      v_loop_time:=0;
      while v_loop_time < 12 loop
	  dbms_output.put_line('11..........');
        v_time_begin:=v_analyse_date||v_hour_begin||'0000';
        v_time_end:=v_analyse_date||v_hour_end||'5959';
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_analyse_date||v_hour_begin||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/86400 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id and t.begintime between to_date('||v_time_begin||',''yyyymmddhh24miss'') and to_date('||v_time_end||',''yyyymmddhh24miss'')
          group by t.id,c.description ,c.node';
        execute immediate v_sql;  
        v_hour_begin:=lpad(v_hour_begin+2,2,'0');
        v_hour_end:=lpad(v_hour_end+2,2,'0');
        v_loop_time:=v_loop_time+1;        
      end loop;
      commit;
    end if;
    --h 按小时分析
    if(v_analyse_type = 'h') then
	dbms_output.put_line('12..........');
      v_tableA_name:='cprun_'||substr(v_analyse_date,1,6);
      v_partition:='P_00'||substr(v_analyse_date,7,2);
      v_minute_begin:='00';
      v_minute_end:='09';
      v_loop_time:=0;
      while v_loop_time < 6 loop
	  dbms_output.put_line('13..........');
        v_time_begin:=v_analyse_date||v_minute_begin||'00';
        v_time_end:=v_analyse_date||v_minute_end||'59';
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_analyse_date||v_minute_begin||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/86400 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id and t.begintime between to_date('||v_time_begin||',''yyyymmddhh24miss'') and to_date('||v_time_end||',''yyyymmddhh24miss'')
          group by t.id,c.description ,c.node';
        execute immediate v_sql; 
        v_minute_begin:=lpad(v_minute_begin+10,2,'0');
        v_minute_end:=lpad(v_minute_end+10,2,'0');
        v_loop_time:=v_loop_time+1;
      end loop;
      commit;
    end if;
  end ;
  //



CREATE OR REPLACE PROCEDURE "SDCJ_DATA_ANALYSE_INID" ( v_analyse_type in varchar2,
                                                  v_analyse_date in varchar2,
                                                  v_analyse_id in varchar2
                                                ) as
       v_sql           varchar2(1024);
       v_week_begin    varchar2(10);
       v_week_end      varchar2(10);
       v_month_begin   varchar2(10);
       v_month_end     varchar2(10);
       v_tableA_name   varchar2(20);
       v_tableB_name   varchar2(20);
       v_loop_day      varchar2(5);
       v_while_day     varchar2(5);
       v_partition     varchar2(10);
       v_date_next     varchar2(20);
       v_time_begin    varchar2(20);
       v_time_end      varchar2(20);
       v_hour_begin    varchar2(10);
       v_hour_end      varchar2(10);
       v_loop_time     varchar2(10);
       v_minute_begin  varchar2(10);
       v_minute_end    varchar2(10);
begin
  v_sql:='truncate table tbl_sdcj_data_analyse';
  execute immediate v_sql;
  --w 按周分析
  if(v_analyse_type = 'w') then
  dbms_output.put_line('1..........');
    --取周一
    select to_char(trunc(to_date(v_analyse_date,'yyyymmdd'), 'd') + 1,'yyyymmdd') into v_week_begin from dual;
    --取周日
    select to_char(trunc(to_date(v_analyse_date,'yyyymmdd'), 'd') + 7,'yyyymmdd') into v_week_end from dual;
    --判断跨月
    if  substr(v_week_begin,1,6) != substr(v_week_end,1,6) then
	dbms_output.put_line('2..........');
      --取周日所在的月初
      select to_char(trunc(to_date(v_week_end,'yyyymmdd'),'month'),'yyyymmdd') into v_month_begin from dual;
      --取周一所在的月底
      select to_char(last_day(trunc(to_date(v_week_begin,'yyyymmdd'),'month')),'yyyymmdd') into v_month_end from dual;
      v_tableA_name:='cprun_'||substr(v_week_begin,1,6);
      v_tableB_name:='cprun_'||substr(v_week_end,1,6);
      v_loop_day:=substr(v_week_begin,7,2);
      v_while_day:=to_number(substr(v_month_end,7,2))+to_number(substr(v_week_end,7,2));
      v_date_next:=v_week_begin;
      while to_number(v_loop_day) <= to_number(v_while_day)  loop
	  dbms_output.put_line('3..........');
        v_loop_day:=lpad(v_loop_day,2,'0');
        v_partition:='P_00'||substr(v_date_next,7,2);
        if to_number(v_loop_day) <= to_number(substr(v_month_end,7,2)) then
          if v_analyse_id is not null then
		  dbms_output.put_line('4..........');
            v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
            select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
            sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
            where t.id=c.id and t.id='''||v_analyse_id||'''
            group by t.id,c.description ,c.node';
          else
		  dbms_output.put_line('5..........');
            v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
            select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
            sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
            where t.id=c.id
            group by t.id,c.description ,c.node';
          end if;
          --execute immediate v_sql;
        else
          if v_analyse_id is not null then
		  dbms_output.put_line('6..........');
            v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
            select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
            sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableB_name||' partition('||v_partition||') t ,cpconfig c
            where t.id=c.id  and t.id='''||v_analyse_id||'''
            group by t.id,c.description ,c.node';
          else
		  dbms_output.put_line('7..........');
            v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
            select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
            sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableB_name||' partition('||v_partition||') t ,cpconfig c
            where t.id=c.id 
            group by t.id,c.description ,c.node';  
          end if;             
          --execute immediate v_sql;
        end if;
        execute immediate v_sql;
        v_loop_day:=v_loop_day+1;
        v_date_next:=to_char(to_date(v_date_next,'yyyymmdd')+1,'yyyymmdd');
      end loop;
      commit;
    else
      --取表
	  dbms_output.put_line('8..........');
      v_tableA_name:='cprun_'||substr(v_week_begin,1,6);
      --按分区获取数据
      v_loop_day:=substr(v_week_begin,7,2);
      v_while_day:=substr(v_week_end,7,2);
      v_date_next:=v_week_begin;
      while to_number(v_loop_day) <= to_number(v_while_day)  loop
	  dbms_output.put_line('9..........');
        /*select * from v_tableA_name t where t.day=substr(v_week_begin,7,2)*/
        v_loop_day:=lpad(v_loop_day,2,'0');
        --取分区
        v_partition:='P_00'||v_loop_day;
        --v_loop_day:=4;
        v_loop_day:=v_loop_day+1;
        if v_analyse_id is not null then
		dbms_output.put_line('10..........');
          v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id  and t.id='''||v_analyse_id||'''
          group by t.id,c.description ,c.node';
        else
		dbms_output.put_line('11..........');
          v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id 
          group by t.id,c.description ,c.node';
        end if;
        execute immediate v_sql;
        --select  to_char(to_date('20150409','yyyymmdd')+1,'yyyymmdd') from dual;
        v_date_next:=to_char(to_date(v_date_next,'yyyymmdd')+1,'yyyymmdd');
      end loop;
      commit;
    end if;
  end if;
  --m 按月分析
  if(v_analyse_type = 'm') then
  dbms_output.put_line('12..........');
    --select trunc(sysdate, 'mm') from dual;
    --select last_day(trunc(sysdate)) from dual;
    select to_char(trunc(to_date(v_analyse_date,'yyyymmdd'), 'mm'),'yyyymmdd') into v_month_begin from dual;
    select to_char(last_day(trunc(to_date(v_analyse_date,'yyyymmdd'))),'yyyymmdd') into v_month_end from dual;
    --取表
    v_tableA_name:='cprun_'||substr(v_analyse_date,1,6);
    v_loop_day:=substr(v_month_begin,7,2);
    v_while_day:=substr(v_month_end,7,2);
    v_date_next:=v_month_begin;
    while to_number(v_loop_day) <= to_number(v_while_day) loop
      v_partition:='P_00'||substr(v_date_next,7,2);
      if v_analyse_id is not null then
	  dbms_output.put_line('13..........');
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id  and t.id='''||v_analyse_id||'''
          group by t.id,c.description ,c.node';
      else
	  dbms_output.put_line('14..........');
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_date_next||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id 
          group by t.id,c.description ,c.node';
      end if;
      execute immediate v_sql;
      v_loop_day:=v_loop_day+1;
      v_date_next:=to_char(to_date(v_date_next,'yyyymmdd')+1,'yyyymmdd');
    end loop;
    commit;
  end if;
  --d 按天分析
  if(v_analyse_type = 'd') then
    v_tableA_name:='cprun_'||substr(v_analyse_date,1,6);
    v_partition:='P_00'||substr(v_analyse_date,7,2);
    v_hour_begin:='00';
    v_hour_end:='01';
    v_loop_time:=0;
    while v_loop_time < 12 loop
      v_time_begin:=v_analyse_date||v_hour_begin||'0000';
      v_time_end:=v_analyse_date||v_hour_end||'5959';
      if v_analyse_id is not null then
	  dbms_output.put_line('15..........');
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_analyse_date||v_hour_begin||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id and t.begintime between to_date('||v_time_begin||',''yyyymmddhh24miss'') and to_date('||v_time_end||',''yyyymmddhh24miss'')
           and t.id='''||v_analyse_id||'''
          group by t.id,c.description ,c.node';
      else
	  dbms_output.put_line('16..........');
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_analyse_date||v_hour_begin||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id and t.begintime between to_date('||v_time_begin||',''yyyymmddhh24miss'') and to_date('||v_time_end||',''yyyymmddhh24miss'')
          group by t.id,c.description ,c.node';
      end if;
      execute immediate v_sql;
      v_hour_begin:=lpad(v_hour_begin+2,2,'0');
      v_hour_end:=lpad(v_hour_end+2,2,'0');
      v_loop_time:=v_loop_time+1;
    end loop;
    commit;
  end if;
  --h 按小时分析
  if(v_analyse_type = 'h') then
  dbms_output.put_line('17..........');
    v_tableA_name:='cprun_'||substr(v_analyse_date,1,6);
    v_partition:='P_00'||substr(v_analyse_date,7,2);
    v_minute_begin:='00';
    v_minute_end:='09';
    v_loop_time:=0;
    while v_loop_time < 6 loop
      v_time_begin:=v_analyse_date||v_minute_begin||'00';
      v_time_end:=v_analyse_date||v_minute_end||'59';
      if v_analyse_id is not null then
	  dbms_output.put_line('18..........');
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_analyse_date||v_minute_begin||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id and t.begintime between to_date('||v_time_begin||',''yyyymmddhh24miss'') and to_date('||v_time_end||',''yyyymmddhh24miss'')
           and t.id='''||v_analyse_id||'''
          group by t.id,c.description ,c.node';
      else
	  dbms_output.put_line('19..........');
        v_sql:='insert into tbl_sdcj_data_analyse a (a.datetime,a.node,a.id,a.description,a.file_count,a.file_size,a.file_width)
          select '||v_analyse_date||v_minute_begin||',c.node,t.id,c.description,count(*),sum(t.filesize)/1024/1024,
          sum(t.filesize)/sum((t.endtime-t.begintime)*86400)/1024/1024 from '||v_tableA_name||' partition('||v_partition||') t ,cpconfig c
          where t.id=c.id and t.begintime between to_date('||v_time_begin||',''yyyymmddhh24miss'') and to_date('||v_time_end||',''yyyymmddhh24miss'')
          group by t.id,c.description ,c.node';
      end if;
      execute immediate v_sql;
      v_minute_begin:=lpad(v_minute_begin+10,2,'0');
      v_minute_end:=lpad(v_minute_end+10,2,'0');
      v_loop_time:=v_loop_time+1;
    end loop;
    commit;
  end if;
end ;
//




  CREATE OR REPLACE PROCEDURE "STAT_PERCENT_TOOL" (a_Region varchar2) as
v_sql varchar2(2048);
v_percent number(5,2);
v_sum number;
v_lengsum number;
v_longcount number;
v_length number(5,2);
begin
  for c1 in(select *  from cust_entity_def) loop
  dbms_output.put_line('1..........');
    v_sql:='select count(*),nvl(sum(decode(sign(length(value)-8),1,1,0)),0),nvl(sum(decode(sign(length(value)-8),1,ceil(length(value)/4)*4,0)),0) from '||c1.ATTR_TABLENAME||
           ' where key_id like '''||a_Region||'%'' and attr_id in(select attr_id From entity_attr_def where ATTR_TYPE=2 and ENTITY_ID='||c1.ENTITY_ID||')';
    execute immediate v_sql into v_sum,v_longcount,v_lengsum;
    v_percent := 0;
    if(v_sum>0)then
      v_percent := v_longcount/v_sum;
    end if;
    v_length := 0;
    if(v_longcount > 0) then
      v_length := v_lengsum/v_longcount;
    end if;
    dbms_output.put_line('地区['||a_Region||'] '||c1.ENTITY_NAME||'装入内存数量为:'||v_sum||' 长度超过8的数量为:'||v_longcount||' 比例为:'||to_char(v_percent)||' 平均长度为:'||v_length);
  end loop;
  dbms_output.put_line('建议设置的参数比实际值大一点');
end;
//




CREATE OR REPLACE PROCEDURE "GATHER_TAB_CNT_NEW" 
is
cursor all_tab is select table_name from ALL_PARAM_TABLE;
v_tabname varchar2(50);
v_sql01 varchar2(2000);
v_sql02 varchar2(2000);
v_sql03 varchar2(2000);
own varchar2(40);
tb_name varchar2(40);
cnt number;
begin
for i in all_tab loop
dbms_output.put_line('1..........');
v_tabname:=i.table_name;
dbms_output.put_line('v_tabname:'||v_tabname);
v_sql01:='select owner,table_name from dba_tables where owner=''TEST'' and table_name='''||v_tabname||'''';
execute immediate v_sql01 into own,tb_name;
dbms_output.put_line('own,tb_name:'||own||','||tb_name);
v_sql02:='select  count(*) from '||own||'.'||tb_name;
execute immediate v_sql02 into cnt;
v_sql03:='insert gather_tab_cnt values ('''||own||''','''||tb_name||''','''||cnt||''')';
execute immediate v_sql03;
commit;
end loop;
end;
//

delimiter ;//


