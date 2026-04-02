



delimiter $;

  CREATE OR REPLACE FUNCTION "AF_CHANGE_REGION" (unit varchar2,rectype varchar2)
return number is
  V_unit varchar2(36);
  R_region number(3) ;

  begin
   if rectype = 1 then  --·？3？？D？àμ？？？
   V_unit :=  replace(
              replace(
              REPLACE(
              unit,'NX.03.02','NX.05.03'),
                       'NX.04.02','NX.05.02'),
                       'NX.03.07','NX.05.01') ;
   R_region := to_number(95||nvl(substr(V_unit,5,1),0)) ;
   dbms_output.put_line('1');
   elsif rectype = 0  then --？？í？μ？？-2？？？
   V_unit :=  replace(
              replace(
              REPLACE(
              unit,'NX.05.03','NX.03.02'),
                     'NX.05.02','NX.04.02'),
                     'NX.05.01','NX.03.07') ;
   R_region := to_number(95||nvl(substr(V_unit,5,1),0)) ;
   dbms_output.put_line('2');
   else
   R_region := 888   ;
   dbms_output.put_line('3');
   end if ;
  return(R_region);
end af_change_region;
  

$


  CREATE OR REPLACE FUNCTION "AF_CHANGE_UNIT" (unit varchar2,rectype varchar2)
return varchar2 is
  R_unit varchar2(36);
begin
   if rectype = 1 then  --·？3？？D？àμ？？？
   R_unit :=  replace(
              replace(
              REPLACE(
              unit,'NX.03.02','NX.05.03'),
                       'NX.04.02','NX.05.02'),
                       'NX.03.07','NX.05.01') ;
					   dbms_output.put_line('1');
   elsif rectype = 0  then --？？í？μ？？-2？？？
   R_unit :=  replace(
              replace(
              REPLACE(
              unit,'NX.05.03','NX.03.02'),
                     'NX.05.02','NX.04.02'),
                     'NX.05.01','NX.03.07') ;
					 dbms_output.put_line('2');
   else
   R_unit := unit||'′|àíààDí′í？ó'   ;
   dbms_output.put_line('3');
   end if ;
  return(R_unit);
end af_change_unit;
  

$

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
	  dbms_output.put_line('1');
    elsif v_str_10 ='10' then
     v_last_str_t:=to_number(v_last_str);
	 dbms_output.put_line('2');
    else
     v_last_str_t:=to_number(v_last_str);
	 dbms_output.put_line('3');
    end if ;

    if v_last_str_t>to_number(v_pos)-1 then
       v_resturn := 1 ;
	   dbms_output.put_line('4');
    elsif v_last_str_t<to_number(v_pos)-1 then
       v_resturn := 2 ;
	   dbms_output.put_line('5');
    else
       v_resturn := 3 ;
	   dbms_output.put_line('6');
    end if ;

    RETURN v_resturn;
  END;

$


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
			 dbms_output.put_line('1');
        end if ;


        if i  > 1 and i <= 2 then
           if mod(v_tmp_duration,60) <> 0 then
              v_zs_flag :=v_zs_flag||',t_head('||(i-1)||')';
			  dbms_output.put_line('2');
           end if ;

           if v_tmp_duration <= 60 then
              v_zs_flag :=v_zs_flag||',s_head('||(i-1)||')';
			  dbms_output.put_line('3');
           end if ;

        elsif i  > 2 and i < v_loop_count then
           if mod(v_tmp_duration,60) <> 0 then
              v_zs_flag:=v_zs_flag||',t_mid('||(i-1)||')';
			  dbms_output.put_line('4');
           end if ;

           if v_tmp_duration <= 60 then
              v_zs_flag :=v_zs_flag||',s_mid('||(i-1)||')';
			  dbms_output.put_line('5');
           end if ;

           if (to_date(v_mid_time,'yyyymmddhh24miss')+v_mid_duration*1/24/60/60 ) <> to_date(v_tmp_time,'yyyymmddhh24miss') then
              v_ls_flag := v_ls_flag ||',l_mid('||(i-1)||')';
			  dbms_output.put_line('6');
           end if ;

        else

           if (to_date(v_mid_time,'yyyymmddhh24miss')+v_mid_duration*1/24/60/60 ) <> to_date(v_tmp_time,'yyyymmddhh24miss') then
              v_ls_flag := v_ls_flag ||',l_end('||(i-1)||')';
			  dbms_output.put_line('7');
           end if ;

        end if ;


        v_mid_time := af_getstr(v_tmp_str,'|',1);
        v_mid_duration := to_number(af_getstr(v_tmp_str,'|',2));
dbms_output.put_line('8');
    end loop ;

    if (v_zs_flag='0' and v_ls_flag ='0') then
       v_resturn:='0#0';
	   dbms_output.put_line('9');
    else
       v_resturn := v_zs_flag||'#'||v_ls_flag ;
	   dbms_output.put_line('10');
    end if ;
    RETURN v_resturn;

   EXCEPTION
   WHEN OTHERS THEN
    RETURN 0;
  END;


$

  CREATE OR REPLACE FUNCTION "AF_GETRIGHT" (v_string in varchar2)
 return varchar2
 as
 v_len number;
 v_result varchar2(20);
 i int;
 p_rec varchar2(10);
 as_rec number;
 begin
   v_len:=length(v_string);
   v_result:='';
   for i in 1..v_len
   loop
   p_rec:=substr(v_string,i,1);
   as_rec:=ascii(p_rec);
   if((as_rec>=48 and as_rec<=57) or (as_rec>=97 and as_rec<=122)
   or (as_rec>=65 and as_rec<=90) or (as_rec=45 or as_rec=43))
   then
    v_result:=v_result||p_rec;
	dbms_output.put_line('1');
   elsif(as_rec=41901) then 
      v_result:=v_result||'-';
	  dbms_output.put_line('2');
    elsif(as_rec=41899) then 
      v_result:=v_result||'+';
	  dbms_output.put_line('3');
    end if;
    end loop;
    return v_result;
 end;

$


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
	  dbms_output.put_line('1');
    ELSE
      v_arstr := str;
	  dbms_output.put_line('2');
    END IF;
    IF pos != 1
    THEN
      v_last_pos := instr(v_arstr, v_split, 1, pos - 1);
      v_pos      := instr(v_arstr, v_split, 1, pos);
      v_str      := substr(v_arstr, v_last_pos + 1, v_pos - v_last_pos - 1);
	  dbms_output.put_line('3');
    ELSE
      v_pos := instr(v_arstr, v_split, 1, pos);
      v_str := substr(v_arstr, 1, v_pos - 1);
	  dbms_output.put_line('4');
    END IF;

    RETURN v_str;
  END;

$


  CREATE OR REPLACE FUNCTION "AF_GETSTR_NEW" (str VARCHAR2, v_split VARCHAR2, pos NUMBER)
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
	  dbms_output.put_line('1');
    ELSE
      v_arstr := str;
	  dbms_output.put_line('2');
    END IF;
    IF pos != 1
    THEN
      v_last_pos := instr(v_arstr, v_split, 1, pos - 1);
      v_pos      := instr(v_arstr, v_split, 1, pos);
      v_str      := substr(v_arstr, v_last_pos + 1, v_pos - v_last_pos - 1);
	  dbms_output.put_line('3');
    ELSE
      v_pos := instr(v_arstr, v_split, 1, pos);
      v_str := substr(v_arstr, 1, v_pos - 1);
	  dbms_output.put_line('4');
    END IF;

    RETURN v_str;

    EXCEPTION
   WHEN OTHERS THEN
    RETURN 0;
  END;

$


  CREATE OR REPLACE FUNCTION "AF_GET_BILLINGCHKRESULT" (a_genEventFlowId in number,
                                                      a_billingEventFlowId in number, 
                                                      a_sourceFile in varchar2)
                                                      return number
as
v_gen_count number(8);
v_billing_count number(8);
v_resoult number(1);
begin
     begin
          select nvl(sum(file_event_count),0) into v_gen_count
          from event_flow_log_sourfile 
          where  module_id =a_genEventFlowId and source_name = a_sourceFile;
          
          select nvl(sum(file_event_count),0) into v_billing_count
          from event_flow_log_sourfile
          where  module_id =a_billingEventFlowId and source_name = a_sourceFile;
          
          if((v_gen_count = v_billing_count) and (v_billing_count <> 0)) then
              v_resoult := 1;
              Dbms_Output.put_line('Billing was completed!');
			  dbms_output.put_line('1');
          else
              if(v_gen_count = 0) then
               v_resoult := -1;
               Dbms_Output.put_line('No find data of having sourcefilename!');   
			   dbms_output.put_line('2');
              else
                  v_resoult := 0;
                  Dbms_Output.put_line('Billing is not still completed!');
				  dbms_output.put_line('3');
              end if;
          end if;
          
      exception when others then
           Dbms_Output.put_line('Biiling checking is failed!');
            v_resoult := -2;
      end;
      
      return  v_resoult;
end AF_GET_BILLINGCHKRESULT;

$

	
  CREATE OR REPLACE FUNCTION "AF_GET_BILLINGCYCLE" (ad_date date)
return number
as
/*
	return the proper billing cycle of ad_date
	default change date is 21
*/
vn_billingcycle number(8);
vn_billingcycle_before number(8);
vn_tmp number(8);
begin
	select to_number(to_char(add_months(trunc(ad_date) - 20,1),'yyyymm')) into vn_billingcycle from dual;

	select count(*) into vn_tmp  from user_bill_sign where billingmonth=to_number(to_char(ad_date,'yyyymm'))  and isbilled =1 ;

if vn_tmp = 0 then
	dbms_output.put_line('1');
		return to_number(to_char(ad_date,'yyyymm'));
	else
	dbms_output.put_line('2');
		return vn_billingcycle;	
	end if;
end af_get_billingcycle;
  

$


  CREATE OR REPLACE FUNCTION "AF_GET_CUSTNOTE" (
        a_entity_id number,
        a_attr_id number ,
        a_value varchar2
) return varchar2 is
  v_attr_class number(10);
  v_note    varchar2(128);
begin
   v_note := null;
   select nvl(max(attr_class),0) into v_attr_class
      from entity_attr_def
        where entity_id=a_entity_id and attr_id=a_attr_id;

   if v_attr_class = 0 then
   dbms_output.put_line('1');
      return '';
   end if;

   if v_attr_class=12 then
       select tariff_plan_name into v_note
         from tariff_plan where tariff_plan_id = to_number(a_value) ;
		 dbms_output.put_line('2');
   end if;

/*
   if v_attr_class=9 then
      select value into v_note
         from subscriber_attr
            where key_id=to_number(a_value)
               and attr_id=14 and nvl(expiretime,to_date('20201104','yyyymmdd')+1)>=to_date('20201104','yyyymmdd') and rownum=1;
   end if;

   if v_attr_class=10 then
      select value into v_note
         from account_attr
            where key_id=to_number(a_value)
               and attr_id=12 and nvl(expiretime,to_date('20201104','yyyymmdd')+1)>=to_date('20201104','yyyymmdd') and rownum=1;
   end if;

   if v_attr_class>=30 and v_attr_class<=32  then
      select value into v_note
         from customer_attr
            where key_id=to_number(a_value)
               and attr_id=11 and nvl(expiretime,to_date('20201104','yyyymmdd')+1)>=to_date('20201104','yyyymmdd') and rownum=1;
   end if;
*/

   return v_note;

exception
   when others then
      return '';

end ;



$

  CREATE OR REPLACE FUNCTION "AF_GET_DELAYTIME" (acctid varchar2) return number is
    v_bankname  varchar2(32);
    v_delay     number(10);
begin
     begin
      if acctid='9510800540446' then
	  dbms_output.put_line('1');
      	return 24*3600;
      end if;
      v_delay := 24*3600;
	  dbms_output.put_line('2');
      return  v_delay;
    exception
      when no_data_found then
	  dbms_output.put_line('3');
          return 24*3600;
    end;
end;
  

$

  CREATE OR REPLACE FUNCTION "AF_GET_FIELD" (av_str in varchar2,av_split in varchar2,av_field in number)
       return varchar2 
is
    v_pos_last    number(5);
    v_pos_first   number(5);
    v_str         varchar2(1024);
    v_len         number(5);
begin
    v_len:=length(av_split);
    select instr(av_str,av_split,1,av_field) into v_pos_last from dual;
    if av_field=1 then
       v_pos_first:=0;
       v_len:=1;
	   dbms_output.put_line('1');
    else
       select instr(av_str,av_split,1,av_field-1) into v_pos_first from dual;
	   dbms_output.put_line('2');
    end if;
    v_str:=substr(av_str,v_pos_first+v_len,v_pos_last-v_pos_first-v_len);
    return v_str;
end af_get_field;

$

  CREATE OR REPLACE FUNCTION "AF_GET_FILEAREACODE" ( newfilename varchar2)
return varchar2 as
v_areacode varchar2(3):=null;
begin   
	v_areacode:=substr(newfilename,3,3);
	if v_areacode = '530' then
	dbms_output.put_line('1');
		return 'HZ';
	elsif v_areacode = '531' then
	dbms_output.put_line('2');
		return 'JN';
	elsif v_areacode = '532' then
	dbms_output.put_line('3');
		return 'QD';
	elsif v_areacode = '533' then
	dbms_output.put_line('4');
		return 'ZB';
	elsif v_areacode = '534' then
	dbms_output.put_line('5');
		return 'DZ';
	elsif v_areacode = '535' then
	dbms_output.put_line('6');
		return 'YT';
	elsif v_areacode = '536' then
	dbms_output.put_line('7');
		return 'WF';
	elsif v_areacode = '537' then
	dbms_output.put_line('8');
		return 'JG';
	elsif v_areacode = '538' then
	dbms_output.put_line('9');
		return 'TA';
	elsif v_areacode = '539' then
	dbms_output.put_line('10');
		return 'LY';
	elsif v_areacode = '543' then
	dbms_output.put_line('11');
		return 'BZ';
	elsif v_areacode = '546' then
	dbms_output.put_line('12');
		return 'DY';
	elsif v_areacode = '631' then
	dbms_output.put_line('13');
		return 'WH';
	elsif v_areacode = '632' then
	dbms_output.put_line('14');
		return 'ZZ';
	elsif v_areacode = '633' then
	dbms_output.put_line('15');
		return 'RZ';
	elsif v_areacode = '634' then
	dbms_output.put_line('16');
		return 'LW';
	elsif v_areacode = '635' then
	dbms_output.put_line('17');
		return 'LC';
	end if;
	return v_areacode;
end ;
  

$

  CREATE OR REPLACE FUNCTION "AF_GET_LATEFEE" (a_planid number, a_itemtype number,a_cycle number ,a_debtfee number,a_latefeedate date) return number Is
   v_latefee   number(14);
   v_rate  number(5);
   v_startdate date;
   v_enddate   date;
begin
   SELECT greatest(round(a_debtfee*rate_of_amerce*trunc(a_latefeedate - startdate)/10000),0) INTO v_latefee FROM amerce_rate_def
    WHERE amerce_planid = a_planid AND cycle = a_cycle AND itemtype = a_itemtype;
dbms_output.put_line('1');
   return v_latefee;

   exception when others then
      return -1;
end;
  

$


  CREATE OR REPLACE FUNCTION "AF_GET_NEED_CHECK" 
(
  v_intype     in varchar2,
  v_store_name in varchar2
) return varchar2 as
  a_out_need_check varchar2(10);

begin
  select b.need_check
  into a_out_need_check
  from store_class_def b, store_class_map a
  where a.store_type = Nvl(v_intype, '#')
        and Nvl(decode(v_intype,'=','total',v_store_name), '#') like a.store_name_prefix || '%'
        and b.class_code = a.store_class;
		dbms_output.put_line('1');
  return a_out_need_check;
exception
  when NO_DATA_FOUND then
  dbms_output.put_line('2');
    return 'Unknow';
end;


$

  CREATE OR REPLACE FUNCTION "AF_GET_RAWFILENAME" ( newfilename varchar2)
return varchar2 as
v_areacode varchar2(3):=null;
v_filetype varchar2(2):=null;
begin   
	v_filetype:=substr(newfilename,1,2);
	if v_filetype ='SD' then
		v_areacode:=af_get_fileareacode(newfilename);
		dbms_output.put_line('1');
    	return  v_areacode||substr(newfilename,6,2)||'.TTFILE'||substr(newfilename,-4,4);
	elsif v_filetype='HG' then
		v_areacode:=af_get_fileareacode(newfilename);
		dbms_output.put_line('2');
		return  v_areacode||substr(newfilename,6,2)||'.'||substr(newfilename,-9,9)||'.dat';
	end if;
	return newfilename;
end ;
  

$


  CREATE OR REPLACE FUNCTION "AF_GET_STORE_CLASS_CODE" 
(
  v_intype     in varchar2,
  v_store_name in varchar2,
  v_rbflg in varchar2
) return varchar2 as
  a_out_type varchar2(10);
  ----Add by gKF12550 2009-4-30
  ----功能：根据传入的存储类型和存储文件名来获取存储分类编码
begin
  if v_rbflg = 'R' then
  dbms_output.put_line('1');
    return 'RB_CDR';
  end if;

  select b.class_code
  into a_out_type
  from store_class_def b, store_class_map a
  where a.store_type = Nvl(v_intype, '#')
        and Nvl(decode(v_intype,'=','total',v_store_name), '#') like a.store_name_prefix || '%'
        and b.class_code = a.store_class;
		dbms_output.put_line('2');
  return a_out_type;
exception
  when NO_DATA_FOUND then
  dbms_output.put_line('3');
    return 'Unknow_CDR'; ----没有找到,将其归为未知话单
end;


$


  CREATE OR REPLACE FUNCTION "AF_GET_TARIFF_SN" return varchar2
is
   v_sn   varchar2(20);
begin
    select  to_char(to_date('20201104','yyyymmdd'),'yymmddhh24miss') || lpad(SEQ_TARIFF_SN.NEXTVAL,8,'0') into v_sn from dual;
	dbms_output.put_line('1');
    return v_sn;
end;


$

  CREATE OR REPLACE FUNCTION "AF_GET_TOKEN" 
 (v_source IN OUT varchar2, v_split IN varchar2)
RETURN varchar2
IS
    v_ret   varchar2(1000);
    v_b     number;
    v_pos   number;
BEGIN
    v_b := length(v_split);
    v_ret := '';
    v_pos := instr(v_source,v_split,1,1);
    if v_pos > 0 then
        v_ret := substr(v_source,1,v_pos-1);
        v_source := substr(v_source,v_pos+v_b);
		dbms_output.put_line('1');
    else
        v_ret := substr(v_source,1);
        v_source := '';
		dbms_output.put_line('2');
    end if;
    return v_ret;
END ;


$

  CREATE OR REPLACE FUNCTION "AF_LPADB" (a_str varchar2, a_len binary_integer, a_pad varchar2 := ' ') return varchar2
is
    v_origin_lenb binary_integer;
begin
    v_origin_lenb := lengthb(a_str);
	if v_origin_lenb > a_len then
	dbms_output.put_line('1');
	    return substrb(a_str, 1, a_len);
    elsif v_origin_lenb < a_len then
	dbms_output.put_line('2');
	    return substrb(rpad(a_pad, a_len - v_origin_lenb, a_pad), 1, a_len - v_origin_lenb) || a_str;
	else
	dbms_output.put_line('3');
	    return a_str;
	end if;
end af_lpadb;


$


  CREATE OR REPLACE FUNCTION "AF_RPADB" (a_str varchar2, a_len binary_integer, a_pad varchar2 := ' ') return varchar2
is
begin
dbms_output.put_line('1');
    return substrb(rpad(a_str, a_len, a_pad), 1, a_len);
end af_rpadb;


$


  CREATE OR REPLACE FUNCTION "AF_SPTOOL" (v_fee in NUMBER)
 return varchar2
 as
 
 v_result varchar2(20);
 
 begin
   if v_fee < 1 and length(to_char(v_fee))=2 then
      v_result := trim(to_char(v_fee,'0.9'));
	  dbms_output.put_line('1');
   elsif v_fee < 1 and length(to_char(v_fee))=3 then
      v_result := trim(to_char(v_fee,'0.99'));
	  dbms_output.put_line('2');
   else
       v_result := v_fee;
	   dbms_output.put_line('3');
   end if;
   return v_result;
 end;

$


  CREATE OR REPLACE FUNCTION "AF_TEL_TO_IMSI" (as_telnum VARCHAR2)
  RETURN VARCHAR2 IS
  vs_imsi VARCHAR2(15);
  vs_head VARCHAR2(15);
BEGIN
  IF substr(as_telnum, 1, 3) NOT IN
     ('134', '135', '136', '137', '138', '139','147', '152', '158', '159', '150', '157', '151', '187', '188','182','183','184','178','170','172','198','148','165')
  THEN
  dbms_output.put_line('1');
    RETURN as_telnum;
  END IF;

  IF (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348') OR
     (substr(as_telnum, 1, 3) IN ('158', '159', '150', '151', '152','187','182','183','184'))
  THEN
    vs_head := '2';
	dbms_output.put_line('2');
  ELSIF (substr(as_telnum, 1, 3) in ('157', '147', '188','178','172','198','165') or substr(as_telnum, 1, 4) in ('1705','1703',1706))
  THEN
    vs_head := '7';
	dbms_output.put_line('3');
  ELSE
    vs_head := '0';
	dbms_output.put_line('4');
  END IF;

  IF vs_head = '2' AND (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('5');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '152')
  THEN
    vs_imsi := '4600' || vs_head || '2' || substr(as_telnum, 4, 4);
	dbms_output.put_line('6');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '187')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
	dbms_output.put_line('7');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '158')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('8');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '159')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
	dbms_output.put_line(9);
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '150')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
	dbms_output.put_line('10');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '151')
  THEN
    vs_imsi := '4600' || vs_head || '1' || substr(as_telnum, 4, 4);
	dbms_output.put_line('11');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '182')
  THEN
    vs_imsi := '4600' || vs_head || '6' || substr(as_telnum, 4, 4);
	dbms_output.put_line('12');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
	dbms_output.put_line('13');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 4) = '1705')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('14');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 4) = '1703')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('15');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 4) = '1706')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('16');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '188')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('17');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '147')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
	dbms_output.put_line('18');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '178')
  THEN
    vs_imsi := '4600' || vs_head || '5' || substr(as_telnum, 4, 4);
	dbms_output.put_line('19');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '172')
  THEN
    vs_imsi := '4600' || vs_head || '2' || substr(as_telnum, 4, 4);
	dbms_output.put_line('20');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '198')
  THEN
    vs_imsi := '4600' || vs_head || '1' || substr(as_telnum, 4, 4);
	dbms_output.put_line('21');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '165')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
	dbms_output.put_line('22');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '183')
  THEN
    vs_imsi := '4600' || vs_head || '5' || substr(as_telnum, 4, 4);
	dbms_output.put_line('23');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '184')
  THEN
    vs_imsi := '4600' || vs_head || '4' || substr(as_telnum, 4, 4);
	dbms_output.put_line('24');
  ELSIF substr(as_telnum, 4, 1) = '0'
  THEN
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               substr(as_telnum, 3, 1);
			   dbms_output.put_line('25');
  ELSE
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               (substr(as_telnum, 3, 1) - 5) || substr(as_telnum, 4, 1);
			   dbms_output.put_line('26');
  END IF;

IF (substr(as_telnum, 1, 3) = '148')
  THEN
    vs_head := '3';
    vs_imsi := '4601' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('27');
END IF;

  RETURN vs_imsi;
END af_tel_to_imsi;


$


  CREATE OR REPLACE FUNCTION "AF_TEL_TO_IMSI2" (as_telnum VARCHAR2)
  RETURN VARCHAR2 IS
  vs_imsi VARCHAR2(15);
  vs_head VARCHAR2(15);
BEGIN
  IF substr(as_telnum, 1, 3) NOT IN
     ('134', '135', '136', '137', '138', '139', '158', '159', '150', '157')
  THEN
  dbms_output.put_line('1');
    RETURN as_telnum;
  END IF;

  IF (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348') OR
     (substr(as_telnum, 1, 3) IN ('158', '159', '150'))
  THEN
    vs_head := '2';
	dbms_output.put_line('2');
  ELSIF (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_head := '7';
	dbms_output.put_line('3');
  ELSE
    vs_head := '0';
	dbms_output.put_line('4');
  END IF;

  IF vs_head = '2' AND (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('5');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '158')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('6');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '159')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
	dbms_output.put_line('7');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '150')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
	dbms_output.put_line('8');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
	dbms_output.put_line('9');
  ELSIF substr(as_telnum, 4, 1) = '0'
  THEN
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               substr(as_telnum, 3, 1);
			   dbms_output.put_line('10');
  ELSE
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               (substr(as_telnum, 3, 1) - 5) || substr(as_telnum, 4, 1);
			   dbms_output.put_line('11');
  END IF;

  RETURN vs_imsi;
END af_tel_to_imsi2;

$

  CREATE OR REPLACE FUNCTION "AF_TEL_TO_IMSI_B" (as_telnum VARCHAR2)
  RETURN VARCHAR2 IS
  vs_imsi VARCHAR2(15);
  vs_head VARCHAR2(15);
BEGIN
  IF substr(as_telnum, 1, 3) NOT IN
     ('134', '135', '136', '137', '138', '139', '158', '159', '150', '157', '151')
  THEN
  dbms_output.put_line('1');
    RETURN as_telnum;
  END IF;

  IF (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348') OR
     (substr(as_telnum, 1, 3) IN ('158', '159', '150', '151'))
  THEN
    vs_head := '2';
	dbms_output.put_line('2');
  ELSIF (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_head := '7';
	dbms_output.put_line('3');
  ELSE
    vs_head := '0';
	dbms_output.put_line('4');
  END IF;

  IF vs_head = '2' AND (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('5');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '158')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('6');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '159')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
	dbms_output.put_line('7');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '150')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
	dbms_output.put_line('8');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '151')
  THEN
    vs_imsi := '4600' || vs_head || '1' || substr(as_telnum, 4, 4);
	dbms_output.put_line('9');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
	dbms_output.put_line('10');
  ELSIF substr(as_telnum, 4, 1) = '0'
  THEN
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               substr(as_telnum, 3, 1);
			   dbms_output.put_line('11');
  ELSE
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               (substr(as_telnum, 3, 1) - 5) || substr(as_telnum, 4, 1);
			   dbms_output.put_line('12');
  END IF;

  RETURN vs_imsi;
END af_tel_to_imsi_b;



$

  CREATE OR REPLACE FUNCTION "AF_TEL_TO_IMSI_BAK" (as_telnum VARCHAR2)
  RETURN VARCHAR2 IS
  vs_imsi VARCHAR2(15);
  vs_head VARCHAR2(15);
BEGIN
  IF substr(as_telnum, 1, 3) NOT IN
     ('134', '135', '136', '137', '138', '139','147', '152', '158', '159', '150', '157', '151', '187', '188','182','183','184','178')
  THEN
  dbms_output.put_line('1');
    RETURN as_telnum;
  END IF;

  IF (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348') OR
     (substr(as_telnum, 1, 3) IN ('158', '159', '150', '151', '152','187','182','183','184'))
  THEN
    vs_head := '2';
	dbms_output.put_line('2');
  ELSIF (substr(as_telnum, 1, 3) in ('157', '147', '188','178'))
  THEN
    vs_head := '7';
	dbms_output.put_line('3');
  ELSE
    vs_head := '0';
	dbms_output.put_line('4');
  END IF;

  IF vs_head = '2' AND (substr(as_telnum, 1, 4) BETWEEN '1340' AND '1348')
  THEN
    vs_imsi := '4600' || vs_head || '0' || substr(as_telnum, 4, 4);
	dbms_output.put_line('5');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '152')
  THEN
    vs_imsi := '4600' || vs_head || '2' || substr(as_telnum, 4, 4);
	dbms_output.put_line('6');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '187')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
	dbms_output.put_line('7');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '158')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('8');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '159')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
	dbms_output.put_line('9');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '150')
  THEN
    vs_imsi := '4600' || vs_head || '3' || substr(as_telnum, 4, 4);
	dbms_output.put_line('10');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '151')
  THEN
    vs_imsi := '4600' || vs_head || '1' || substr(as_telnum, 4, 4);
	dbms_output.put_line('11');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '182')
  THEN
    vs_imsi := '4600' || vs_head || '6' || substr(as_telnum, 4, 4);
	dbms_output.put_line('12');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '157')
  THEN
    vs_imsi := '4600' || vs_head || '7' || substr(as_telnum, 4, 4);
	dbms_output.put_line('13');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '188')
  THEN
    vs_imsi := '4600' || vs_head || '8' || substr(as_telnum, 4, 4);
	dbms_output.put_line('14');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '147')
  THEN
    vs_imsi := '4600' || vs_head || '9' || substr(as_telnum, 4, 4);
	dbms_output.put_line('15');
  ELSIF vs_head = '7' AND (substr(as_telnum, 1, 3) = '178')
  THEN
    vs_imsi := '4600' || vs_head || '5' || substr(as_telnum, 4, 4); 
	dbms_output.put_line('16');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '183')
  THEN
    vs_imsi := '4600' || vs_head || '5' || substr(as_telnum, 4, 4);
	dbms_output.put_line('17');
  ELSIF vs_head = '2' AND (substr(as_telnum, 1, 3) = '184')
  THEN
    vs_imsi := '4600' || vs_head || '4' || substr(as_telnum, 4, 4);
	dbms_output.put_line('18');
  ELSIF substr(as_telnum, 4, 1) = '0'
  THEN
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               substr(as_telnum, 3, 1);
			   dbms_output.put_line('19');
  ELSE
    vs_imsi := '4600' || vs_head || substr(as_telnum, 5, 3) ||
               (substr(as_telnum, 3, 1) - 5) || substr(as_telnum, 4, 1);
			   dbms_output.put_line('20');
  END IF;

  RETURN vs_imsi;
END af_tel_to_imsi_bak;

$

  CREATE OR REPLACE FUNCTION "AF_TEL_TO_IMSI_OLD" (as_telnum varchar2)
return varchar2
is
vs_imsi varchar2(15);
vs_head varchar2(15);
begin
	if substr(as_telnum,1,3) not in ('134','135','136','137','138','139') then
	dbms_output.put_line('1');
		return as_telnum;
	end if;
	if substr(as_telnum,1,4) between '1340' and '1348' then
		vs_head:='2';
		dbms_output.put_line('2');
	else
		vs_head:='0';
		dbms_output.put_line('3');
	end if;
	if vs_head = '2' then
	   	vs_imsi := '4600'||vs_head||'0'||substr(as_telnum,4,4);
		dbms_output.put_line('4');
	elsif substr(as_telnum,4,1)='0' then
		vs_imsi :='4600'||vs_head||substr(as_telnum,5,3)||substr(as_telnum,3,1);
		dbms_output.put_line('5');
	else
		vs_imsi :='4600'||vs_head||substr(as_telnum,5,3)||(substr(as_telnum,3,1)-5)||substr(as_telnum,4,1);
		dbms_output.put_line('6');
	end if;
	return vs_imsi;
end af_tel_to_imsi_old;

$

  CREATE OR REPLACE FUNCTION "AF_TIME_GROUP" (a_date date, a_unit_time number) return date
is

begin
dbms_output.put_line('1');
    return ceil((a_date-trunc(a_date))*86400/a_unit_time)*a_unit_time/86400+trunc(a_date);
end ;


$

  CREATE OR REPLACE FUNCTION "AF_ZW_TRANSFER" (id in varchar2) return varchar2 is
  nret varchar2(13):='0000000000000';
begin
  if id is null then
  dbms_output.put_line('1');
     return id;
  end if;
  if id like '953%' then
     nret := '9550'||substr(id,5);
	 dbms_output.put_line('2');
  elsif id like '954%' then
     nret := '9551'||substr(id,5);
	 dbms_output.put_line('3');
  else
  	 nret := id;
	 dbms_output.put_line('4');
  end if;
  return nret;
end af_zw_transfer;
  

$


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
		 dbms_output.put_line('1');
      else
         hexdigit := to_char(digit);
		 dbms_output.put_line('2');
      end if;
      hexval := hexdigit || hexval;
      N2 := trunc( N2 / 16 );
   end loop;
   return hexval;
  end;

$

  CREATE OR REPLACE FUNCTION "F_CURLOGONID" RETURN NUMBER IS
BEGIN
dbms_output.put_line('1');
  RETURN Pkg_Validuser.Logonid;
END;

$

  CREATE OR REPLACE FUNCTION "F_CURUSERTYPE" RETURN NUMBER IS
BEGIN
dbms_output.put_line('1');
  RETURN Pkg_Validuser.Usertype; --V3R1C01B01 39824 2007-3-19
END;


$

  CREATE OR REPLACE FUNCTION "F_GETFIELDLEN" (a_Event_Id  IN NUMBER,
                                         a_Attr_Id   IN NUMBER,
                                         a_Field_Len IN NUMBER) RETURN NUMBER IS
  v_Version NUMBER;
  v_Length  NUMBER;
BEGIN
  SELECT MAX(Version)
    INTO v_Version
    FROM Charging_Event_Def
   WHERE Event_Id = a_Event_Id;

  SELECT Nvl(MAX(Length), a_Field_Len)
    INTO v_Length
    FROM Event_Attr_Len_Def
   WHERE Event_Id = a_Event_Id
     AND Attr_Id = a_Attr_Id
     AND v_Version >= Nvl(Minversion, 1)
     AND v_Version <= Decode(Nvl(Maxversion, 0), 0, 9999, Maxversion);
dbms_output.put_line('1');
  RETURN v_Length;
END f_Getfieldlen;


$

  CREATE OR REPLACE FUNCTION "F_GET_FEE_RATE" (v_tariff_schema_id varchar2,v_TARIFF_ITEM_SN varchar2) return number
is

  n_schema   varchar2(14);
  n_SUBTARIFF_TYPE  number(1);
  n_TARIFF_ID  number(8);
  n_RATIO number(14,6);
begin

  select SUBTARIFF_TYPE,TARIFF_ID into n_SUBTARIFF_TYPE,n_TARIFF_ID
  from tariff_item t
  where (EXPIRETIME > to_date('20201104','yyyymmdd') or EXPIRETIME is null)
  and tariff_schema_id = v_tariff_schema_id
  and TARIFF_ITEM_SN = v_TARIFF_ITEM_SN;

  if n_SUBTARIFF_TYPE = 1 then
  if (n_TARIFF_ID in (270)) then
  n_schema:=1;
  dbms_output.put_line('1');
  else
    n_schema:=0;
	dbms_output.put_line('2');
    end if;
  elsif n_SUBTARIFF_TYPE = 2 then
  n_schema:=n_TARIFF_ID;
  dbms_output.put_line('3');
  elsif n_SUBTARIFF_TYPE = 3 then
  n_schema:=0;
  dbms_output.put_line('4');
  end if;

  return (n_schema);
end ;

$

  CREATE OR REPLACE FUNCTION "F_GET_LATEFEE" (a_planid number, a_itemtype number,a_cycle number ,a_debtfee number,a_latefeedate date) return number Is
   v_latefee   number(14);
   v_rate  number(5);
   v_startdate date;
   v_enddate   date;
begin
   SELECT greatest(round(a_debtfee*rate_of_amerce*trunc(a_latefeedate - startdate)/10000),0) INTO v_latefee FROM amerce_rate_def
    WHERE amerce_planid = a_planid AND cycle = a_cycle AND itemtype = a_itemtype;
	dbms_output.put_line('1');
   return v_latefee;
   exception when others then
      return -1;
end;
  

$

  CREATE OR REPLACE FUNCTION "F_GET_SCHEMA" (v_schema varchar2) return varchar2
is
    v_SqlStr       varchar2(4096);
    v_num          number(20):= 0;
    v_num1          number(20):= 0;
    v_num2          number(20):= 0;
    n_SUBTARIFF_TYPE  number(1);

begin
  FOR c1 IN (select t.* from tariff_item t
  where t.tariff_schema_id = v_schema and (expiretime > to_date('20201104','yyyymmdd') or expiretime is null)
  order by TARIFF_ITEM_SN) LOOP
  v_num := f_get_fee_rate(c1.TARIFF_SCHEMA_ID,c1.TARIFF_ITEM_SN);
  if(v_num not in (0,1)) then
  v_SqlStr := v_SqlStr||f_get_schema(v_num);
  dbms_output.put_line('1');
    elsif (v_num in (0,1)) then
  v_SqlStr := v_SqlStr||v_num;
  dbms_output.put_line('2');
  end if;

  END LOOP;
    commit;

  return (v_SqlStr);
end ;

$

  CREATE OR REPLACE FUNCTION "F_GET_SUBJECT_NAME" (v_subjectId varchar2,a_include_name number)
return varchar2
is
 /*

 max_DRAW_AMT,				   //最大限额
    DRAW_AMT,                 //最大限额  如果 支出限额参考量类型 0--金额 填金额  1--比例(%) 填比例
MIN_DRAW_AMT,                 //最小限额      －－－最小限额为零，代表充最小扣最小，最小限额等于最大限额表示充最小扣应充。
REF_TYPE number(1)           	//支出限额参考量类型　　0--金额   1--比例(%)
COMP_METHOD  number(1)      	//比较方式 0--仅按科目余额  1--仅考虑帐单金额 2--两者比较取最小 3--两者比较取最大
BILLFEE_DRAW_AMT number(14) 	//参考帐单金额的支出限额 0---无支出限额   >0支出限额(为金额或占帐单的比例)
BILLFEE_REF_TYPE number(1)      //帐单金额参数量类型  0--金额   1--比例(%)
EXCL_OFFSET_SCOPE varchar2(256) //费用组，如果不填则表示无限制
 */

v_subjectname       varchar2(64)   ;
v_percent_of_refund number(5,2)    ;
v_cycles_of_refund  number(4)      ;
v_draw_order        number(2)      ;
v_offset_scope      varchar2(50) ;
v_available_cycle   number(10)     ;
v_start_cycle       number(10)     ;
v_in_use            number(1)      ;
v_draw_amt          number(14)     ;
v_flowin_type       number(8)      ;
v_subject_sign      varchar2(8)    ;
v_subject_type      number(2)      ;
v_subjectidn        number(8)      ;
v_min_draw_amt      number(15)     ;
v_ref_type          number(1)      ;
v_comp_method       number(1)      ;
v_billfee_draw_amt  number(14)     ;
v_billfee_ref_type  number(1)      ;
v_max_draw_amt      number(15)     ;

  v_Temp varchar(50);
  v_Desc varchar(1000);
begin
	v_Desc :=' ';
	if v_subjectId is null then
	dbms_output.put_line('1');
		return (v_Desc);
	end if;

	select  nvl(trim(subjectname),' '), nvl(trim(percent_of_refund),' '), nvl(cycles_of_refund,0), nvl(draw_order,0),
			nvl(trim(offset_scope),' '), nvl(available_cycle,0), nvl(start_cycle,0), nvl(in_use,0), nvl(draw_amt,0),
			nvl(flowin_type,0), nvl(subject_sign,0), nvl(subject_type,0), nvl(subjectidn,0), nvl(min_draw_amt,0),
			nvl(ref_type,0), nvl(comp_method,0), nvl(billfee_draw_amt,0), nvl(billfee_ref_type,0), nvl(max_draw_amt,0)
	into 	v_subjectname,v_percent_of_refund,v_cycles_of_refund,v_draw_order,
			v_offset_scope,v_available_cycle,v_start_cycle,v_in_use,v_draw_amt,
			v_flowin_type,v_subject_sign,v_subject_type,v_subjectidn,v_min_draw_amt,
			v_ref_type,v_comp_method,v_billfee_draw_amt,v_billfee_ref_type,v_max_draw_amt
	from subject_def t
	where subjectId = trim(v_subjectId) ;

    if a_include_name =1 then
    	v_Desc := v_subjectname;
		dbms_output.put_line('2');
    else
        v_desc := '';
		dbms_output.put_line('3');
    end if;

	if v_subject_type =4 then
		v_Desc := v_Desc||'/赠送话费';
		dbms_output.put_line('4');
	end if;
	if v_subject_type =5 then
		v_Desc := v_Desc||'/协议款';
		dbms_output.put_line('5');
	end if;

	if v_subject_type in(4,5) then
		if v_ref_type=0 then
			v_Desc := v_Desc||'/限额方式:金额';
			dbms_output.put_line('6');
		end if;
		if v_ref_type=1 then
			v_Desc := v_Desc||'/限额方式:比例(%)';
			dbms_output.put_line('7');
		end if;

		if v_max_draw_amt > 0 then
			v_Desc := v_Desc||'/支出限额:'||to_char(v_max_draw_amt/100)||'元';
			dbms_output.put_line('8');
		end if ;
		if v_max_draw_amt=0 then
			v_Desc := v_Desc||'/支出限额:无限制';
			dbms_output.put_line('9');
		end if ;

		if (v_max_draw_amt=v_min_draw_amt) and v_min_draw_amt >0 then
			v_Desc := v_Desc||'/使用方法:充最小,扣应充';
			dbms_output.put_line('10');
		end if ;
		if  v_min_draw_amt =0 then
			v_Desc := v_Desc||'/使用方法:充最小,扣最小';
			dbms_output.put_line('11');
		end if ;

	    v_Temp := ' /抵扣范围:全部费用 ';

		if  length(trim(v_offset_scope)) > 1 then
			begin
				 select '/抵扣范围:'||nvl(itemName,' ')
				 into v_Temp
				from AcctItem_def
				where itemCode = v_offset_scope;
				dbms_output.put_line('12');
			exception when others then
				null;
			end;
			begin
				select '/抵扣范围:'||nvl(feegroupName,' ')
				 into v_Temp
				from feegroup_def
				where feegroupId = v_offset_scope;
				dbms_output.put_line('13');
			exception when others then
				null;
			end;
		end if ;

		v_Desc := v_Desc||v_Temp;
	end if;

  return (v_Desc);
end ;

$

  CREATE OR REPLACE FUNCTION "GETDISTINCTKEYID" (tableName in varchar2, colName in varchar2, seqName varchar2)
  return varchar2 is Result varchar2(64);
   getSeqSql varchar2(64);
   tempSeqVal varchar2(64);
   valiDataExistSql varchar2(128);
   existFlag number(1);
begin
    getSeqSql := 'select '||seqName||'.nextval from dual';
    loop
      execute immediate getSeqSql into tempSeqVal;
      valiDataExistSql :='select count(*) from(select 1 from '||tableName||' where '||colName||'='||tempSeqVal ||' union select 0 from dual)';
      execute immediate valiDataExistSql into existFlag;
      if existFlag=1 then
	  dbms_output.put_line('1');
          exit;
      end if;
    end loop;
      Result := tempSeqVal;
  return(Result);
end GetDistinctKeyId;

$

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
		   dbms_output.put_line('1');
        else
           current_digit_dec := to_number(current_digit);
		   dbms_output.put_line('2');
        end if;
        result := (result * 16) + current_digit_dec;
    end loop;
    return result;
end;

$

  CREATE OR REPLACE FUNCTION "NUMTOCHAR" 
 (inNum IN  number)
RETURN varchar2
IS
    v_ret   varchar2(5);
    v_b     number;
BEGIN
    v_b := length(inNum);
    v_ret := '';
    if v_b = 1 then
      v_ret := '000'||inNum;
	  dbms_output.put_line('1');
    end if;
    if v_b = 2 then
      v_ret := '00'||inNum;
	  dbms_output.put_line('2');
    end if;
    if v_b = 3 then
      v_ret := '0'||inNum;
	  dbms_output.put_line('3');
    end if;
    if v_b = 4 then
      v_ret := ''||inNum;
	  dbms_output.put_line('4');
    end if;
    return v_ret;
END ;

$

  CREATE OR REPLACE FUNCTION "NUMTOCHAR_8" (inNum IN number) RETURN varchar2 IS
  v_ret varchar2(9);
  v_b   number;
BEGIN
  v_b   := length(inNum);
  v_ret := '';
  if v_b < 5 then
    if v_b = 1 then
      v_ret := '000' || inNum;
	  dbms_output.put_line('1');
    end if;
    if v_b = 2 then
      v_ret := '00' || inNum;
	  dbms_output.put_line('2');
    end if;
    if v_b = 3 then
      v_ret := '0' || inNum;
	  dbms_output.put_line('3');
    end if;
    if v_b = 4 then
      v_ret := '' || inNum;
	  dbms_output.put_line('4');
    end if;
    return v_ret;
  end if;

  if v_b > 4 then
    if v_b = 5 then
      v_ret := '000' || inNum;
	  dbms_output.put_line('5');
    end if;
    if v_b = 6 then
      v_ret := '00' || inNum;
	  dbms_output.put_line('6');
    end if;
    if v_b = 7 then
      v_ret := '0' || inNum;
	  dbms_output.put_line('7');
    end if;
    if v_b = 8 then
      v_ret := '' || inNum;
	  dbms_output.put_line('8');
    end if;
    return v_ret;
  end if;
END;

$

  CREATE OR REPLACE FUNCTION "NUMTOCHAR_N" (inNum IN number) RETURN varchar2 IS
  v_ret varchar2(9);
  v_b   number;
BEGIN
  v_b   := length(inNum);
  v_ret := '';
  if v_b = 1 then
    v_ret := '000' || inNum;
	dbms_output.put_line('1');
  end if;
  if v_b = 2 then
    v_ret := '00' || inNum;
	dbms_output.put_line('2');
  end if;
  if v_b = 3 then
    v_ret := '0' || inNum;
	dbms_output.put_line('3');
  end if;
  if v_b = 4 then
    v_ret := '' || inNum;
	dbms_output.put_line('4');
  end if;
  if v_b > 4 then
    v_ret := '' || inNum;
	dbms_output.put_line('5');
  end if;
  return v_ret;
END;
$

  CREATE OR REPLACE FUNCTION "P_SYNC_IMSI_TRANS_NET_P" 
(
    a_region       Varchar2,
    a_entity_id    Varchar2,--实体ID
    a_opertype     Varchar2,--操作同步方法
    a_changetime   Varchar2,--变更时间
    a_priority     Varchar2,   -- 优先级
    a_realflag     Number,       -- 0 非实时处理 1 实时处理
    a_flow_no      Number,
    a_operate_seq  Number,
    a_szSocketInfo In Out varchar2,--实时处理时返回的 socket包体信息  多条时以“|^^|”间隔


    v_SUBSID    Varchar2,
    v_RESID     Varchar2,
    v_STARTDATE    Varchar2,
    v_ENDDATE     Varchar2,

    v_old_SUBSID    Varchar2,
    v_old_RESID     Varchar2,
    v_old_STARTDATE    Varchar2,
    v_old_ENDDATE     Varchar2,

    v_has_SUBSID Number,
    v_has_RESID  Number,
    v_has_STARTDATE   Number,
    v_has_ENDDATE    Number
) Return Number
Is
   V_REGION  Varchar2(240);
   V_MAXDATE CONSTANT DATE :=to_date('20370101','yyyymmdd'); --系统支持最大资料时间
   g_IMSI_MAPPING_ID CONSTANT NUMBER(4) := 250; --携号转移IMSI/地区,值映射编号,请根据现场配置情况修改。
begin
	--禁用触发器 20120705 197892
     pkg_validuser.istriggerneed :=0;

    if a_opertype like 'I%'  then--插入新IMSI时，需要使用原IMSI映射的地区
      insert into mapping_list  (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE)
       (select v_RESID,
         MAPPING_DEST,
         to_date(v_STARTDATE ,'YYYYMMDDHH24MISS'),
         to_date(v_ENDDATE ,'YYYYMMDDHH24MISS') ,
         g_IMSI_MAPPING_ID,
         v_SUBSID
         from mapping_list
         where  MAPPING_ID = g_IMSI_MAPPING_ID and NOTE= v_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>=to_date(v_STARTDATE,'YYYYMMDDHH24MISS') and nvl(EXPIRETIME,V_MAXDATE)>APPLYTIME and rownum = 1
         );
		 dbms_output.put_line('1');
end if;
    if a_opertype like 'U%' and v_old_RESID<>v_RESID then --修改未来生效的记录，直接修改IMSI
      update mapping_list set
      MAPPING_SOUR = v_RESID
      where  MAPPING_ID = g_IMSI_MAPPING_ID and MAPPING_SOUR = v_old_RESID and NOTE= v_old_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>APPLYTIME and nvl(EXPIRETIME,V_MAXDATE)>to_date('20201104','yyyymmdd') ;
	  dbms_output.put_line('2');
    end if;
    if a_opertype like 'U%' and v_old_RESID=v_RESID  then  --修改正在生效的记录，更新失效时间
     update mapping_list set
         EXPIRETIME = to_date(v_ENDDATE,'YYYYMMDDHH24MISS')
      where  MAPPING_ID = g_IMSI_MAPPING_ID and MAPPING_SOUR = v_old_RESID and NOTE= v_old_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>to_date(v_ENDDATE,'YYYYMMDDHH24MISS') and APPLYTIME<=to_date(v_ENDDATE,'YYYYMMDDHH24MISS') ;
   dbms_output.put_line('3');
   end if;
    return 1;
Exception
    When Others Then
        Raise_Application_Error(-20100,'Call P_SYNC_IMSI_TRANS_NET_P() exception:'||Sqlerrm);
    Return 0;
End P_SYNC_IMSI_TRANS_NET_P;

$

  CREATE OR REPLACE FUNCTION "P_SYNC_TRANS_NET_P" 
(
    a_region       Varchar2,
    a_entity_id    Varchar2,--实体ID
    a_opertype     Varchar2,--操作同步方法
    a_changetime   Varchar2,--变更时间
    a_priority     Varchar2,   -- 优先级
    a_realflag     Number,       -- 0 非实时处理 1 实时处理
    a_flow_no      Number,
    a_operate_seq  Number,
    a_szSocketInfo In Out varchar2,--实时处理时返回的 socket包体信息  多条时以“|^^|”间隔

    v_OID Varchar2,
    v_SERVNUMBER     Varchar2,
    v_TRANSIN_IMSI    Varchar2,
    v_TRANSTYPE    Varchar2,
    v_ORIGIN_REGION    Varchar2,
    v_ORIGIN_SUBSID     Varchar2,
    v_TRANSOUT_REGION     Varchar2,
    v_TRANSIN_REGION     Varchar2,
    v_TRANSOUT_SUBSID     Varchar2,
    v_TRANSIN_SUBSID     Varchar2,
    v_TRANSOUT_ACCTID     Varchar2,
    v_TRANSIN_ACCTID     Varchar2,
    v_APPLYDATE     Varchar2,
    v_APPLYOID     Varchar2,
    v_STARTDATE    Varchar2,
    v_ENDDATE     Varchar2,

    v_old_OID Varchar2,
    v_old_SERVNUMBER   Varchar2,
    v_old_TRANSIN_IMSI    Varchar2,
    v_old_TRANSTYPE    Varchar2,
    v_old_ORIGIN_REGION    Varchar2,
    v_old_ORIGIN_SUBSID     Varchar2,
    v_old_TRANSOUT_REGION    Varchar2,
    v_old_TRANSIN_REGION    Varchar2,
    v_old_TRANSOUT_SUBSID    Varchar2,
    v_old_TRANSIN_SUBSID    Varchar2,
    v_old_TRANSOUT_ACCTID    Varchar2,
    v_old_TRANSIN_ACCTID    Varchar2,
    v_old_APPLYDATE     Varchar2,
    v_old_APPLYOID     Varchar2,
    v_old_STARTDATE     Varchar2,
    v_old_ENDDATE    Varchar2,

    v_has_OID Number,
    v_has_SERVNUMBER  Number,
    v_has_TRANSIN_IMSI   Number,
    v_has_TRANSTYPE    Number,
    v_has_ORIGIN_REGION   Number,
    v_has_ORIGIN_SUBSID   Number,
    v_has_TRANSOUT_REGION   Number,
    v_has_TRANSIN_REGION   Number,
    v_has_TRANSOUT_SUBSID   Number,
    v_has_TRANSIN_SUBSID   Number,
    v_has_TRANSOUT_ACCTID   Number,
    v_has_TRANSIN_ACCTID   Number,
    v_has_APPLYDATE   Number,
    v_has_APPLYOID  Number,
    v_has_STARTDATE     Number,
    v_has_ENDDATE    Number


) Return Number
Is
   V_MAXDATE CONSTANT DATE :=to_date('20370101','yyyymmdd'); --系统支持最大资料时间
   g_IMSI_MAPPING_ID CONSTANT NUMBER(4) := 250; --携号转移IMSI/地区，值映射编号，请根据现场配置情况修改。
   g_SERVNUMBER_MAPPING_ID CONSTANT NUMBER(4) :=251;--携号转移手机号码/地区，值映射编号，请根据现场配置情况修改。
begin
	--禁用触发器 20120705 197892
     pkg_validuser.istriggerneed :=0;

     if a_opertype like 'I%' then
       insert into mapping_list  (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE) values (v_TRANSIN_IMSI,v_TRANSIN_REGION,to_date(v_STARTDATE ,'YYYYMMDDHH24MISS'),to_date(v_ENDDATE ,'YYYYMMDDHH24MISS') ,g_IMSI_MAPPING_ID,v_TRANSIN_SUBSID);
       insert into mapping_list  (MAPPING_SOUR,MAPPING_DEST,APPLYTIME,EXPIRETIME,MAPPING_ID,NOTE) values (v_SERVNUMBER,v_TRANSIN_REGION,to_date(v_STARTDATE ,'YYYYMMDDHH24MISS'),to_date(v_ENDDATE ,'YYYYMMDDHH24MISS'),g_SERVNUMBER_MAPPING_ID,v_TRANSIN_SUBSID);
       dbms_output.put_line('1');
	   return 1;
    end if;
    if a_opertype like 'U%' and to_date(v_STARTDATE ,'YYYYMMDDHH24MISS')>=to_date(v_ENDDATE ,'YYYYMMDDHH24MISS') then
      delete from mapping_list where MAPPING_ID = g_IMSI_MAPPING_ID and MAPPING_SOUR = v_old_TRANSIN_IMSI and NOTE = v_old_TRANSIN_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>APPLYTIME;
      delete from mapping_list where  MAPPING_ID = g_SERVNUMBER_MAPPING_ID and MAPPING_SOUR = v_old_SERVNUMBER and NOTE = v_old_TRANSIN_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>APPLYTIME;
      dbms_output.put_line('2');
	  return 1;
    end if;
    if a_opertype like 'U%' then
       update mapping_list set EXPIRETIME = to_date(v_ENDDATE ,'YYYYMMDDHH24MISS') where  MAPPING_ID = g_IMSI_MAPPING_ID and MAPPING_SOUR = v_old_TRANSIN_IMSI and NOTE = v_old_TRANSIN_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>APPLYTIME;
       update mapping_list set EXPIRETIME = to_date(v_ENDDATE ,'YYYYMMDDHH24MISS') where  MAPPING_ID = g_SERVNUMBER_MAPPING_ID and MAPPING_SOUR = v_old_SERVNUMBER and NOTE = v_old_TRANSIN_SUBSID and nvl(EXPIRETIME,V_MAXDATE)>APPLYTIME;
       dbms_output.put_line('3');
	   return 1;
    end if;

Exception
    When Others Then
        Raise_Application_Error(-20100,'Call P_SYNC_TRANS_NET_P() exception:'||Sqlerrm);
    Return 0;
End P_SYNC_TRANS_NET_P;
$

CREATE OR REPLACE FUNCTION "SNC_RMCONST_6YHN" ( p_query in varchar2 ) return varchar2 as 
l_query long;
l_char  varchar2(10);
l_in_quotes boolean default FALSE;
begin
 for i in 1 .. length( p_query ) loop
	l_char := substr(p_query,i,1);
	if ( l_char = '''' and l_in_quotes ) then
		l_in_quotes := FALSE;
		dbms_output.put_line('1');
	elsif ( l_char = '''' and NOT l_in_quotes ) then
		l_in_quotes := TRUE;
		l_query := l_query || '''#';
		dbms_output.put_line('2');
	end if;
	if ( NOT l_in_quotes ) then
		l_query := l_query || l_char;
		dbms_output.put_line('3');
	end if;
 end loop;
 l_query := translate( l_query, '0123456789', '@@@@@@@@@@' );
 for i in 0 .. 8 loop
	l_query := replace( l_query, lpad('@',10-i,'@'), '@' );
	l_query := replace( l_query, lpad(' ',10-i,' '), ' ' );
	dbms_output.put_line('4');
 end loop;
 return upper(l_query);
 end;

$

#--  CREATE OR REPLACE FUNCTION "TARIFF_PLAN_SHOW1" (plan_id number, fee_flag number)
#--return sys_refcursor
#--as
#--kk sys_refcursor;
#--item_num number(10) := 0;
#--level_num int:=0;
#--level_lab varchar2(2048):='';
#--corner_lab varchar2(10):='└';
#--tee_lab varchar2(10):='├';
#--vline_lab varchar2(10):='│';
#--hline_lab varchar2(10):='─';
#--blank_lab varchar2(10):='  ';
#--plan_count int  :=0;
#--plan_ind int    :=0;
#--item_count int  :=0;
#--item_ind int    :=0;
#--proc_before_name varchar2(64);
#--proc_after_name varchar2(64);
#--procedure schema_deal(schema_id number, cur_ind int, max_count int)
#--as
#--schema_count int:=0;
#--schema_ind int  :=0;
#--schema_item_count int := 0;
#--schema_item_ind int   := 0;
#--rate_name varchar2(64);
#--feename varchar2(64);
#--begin
#--    --求资费模式记录数
#--    select count(*) into schema_count from tariff_schema where tariff_schema_id=schema_id;
#--    if(schema_count>0) then
#--                if(cur_ind <> max_count-1) then
#--                    level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
#--					dbms_output.put_line('1');
#--                else
#--                    level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
#--					dbms_output.put_line('2');
#--                end if;
#--        level_num   := level_num + 1;
#--        level_lab   := level_lab||tee_lab;
#--        schema_ind  := 0;
#--        --遍历资费模式记录
#--        for schema_row in(
#--        select a.tariff_schema_id,a.tariff_name,a.tariff_type,b.type_name,a.fieldcount,a.g_field_def,c.name match_order,
#--        d.name match_type,e.name apply_method,a.refid,f.fee_name discount_fee,g.name round_method,
#--        a.round_scale,a.ref_offset
#--        from tariff_schema a,tariff_type b,code_dict c,code_dict d,code_dict e,
#--        (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) f,code_dict g
#--        where a.tariff_type=b.tariff_type and (c.catalog='T_MATCH_ORDER' and a.match_order=to_number(c.code))
#--        and (d.catalog='T_MATCH_TYPE' and a.match_type=to_number(d.code))
#--        and (e.catalog='T_APPLY_METHOD' and a.apply_method=to_number(e.code)) and a.discount_fee_id=f.fee_id
#--        and (g.catalog='T_ROUND_METHOD' and a.round_method=to_number(g.code)) and tariff_schema_id=schema_id)
#--        loop
#--            if(schema_ind = schema_count-1) then
#--                level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
#--				dbms_output.put_line('3');
#--            else
#--                level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
#--				dbms_output.put_line('4');
#--            end if;
#--            insert into work_tariff_show(
#--            txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,tariff_plan_id,item_sort)
#--            values(
#--            level_lab||'资费模式【'||schema_id||'】:'||schema_row.tariff_name||'===<<'||schema_row.type_name||'>>',
#--            schema_row.tariff_schema_id,
#--            schema_row.tariff_name,
#--            schema_row.type_name,
#--            schema_row.fieldcount,
#--            schema_row.g_field_def,
#--            schema_row.match_order,
#--            schema_row.match_type,
#--            schema_row.apply_method,
#--            schema_row.refid,
#--            schema_row.discount_fee,
#--            schema_row.round_method,
#--            schema_row.round_scale,
#--            schema_row.ref_offset,
#--            plan_id,
#--            item_num);
#--            item_num := item_num + 1;
#--            --求资费模式项记录数
#--            select count(*) into schema_item_count from tariff_item where tariff_schema_id=schema_id;
#--            if(schema_item_count>0) then
#--                if(schema_ind <> schema_count-1) then
#--                    level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
#--					dbms_output.put_line('5');
#--                else
#--                    level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
#--					dbms_output.put_line('6');
#--                end if;
#--                level_num := level_num + 1;
#--                level_lab := level_lab||tee_lab;
#--                schema_item_ind := 0;
#--                --遍历资费模式项
#--                for tariff_item_row in(
#--                select a.*,b.name subtariff_type_name,c.name tariff_name
#--                from tariff_item a,code_dict b,
#--                (select name,code,type from(
#--                    SELECT '【'||LPAD(Code,6)||'】'||Name NAME, TO_NUMBER(Code) CODE ,type  FROM (
#--                        select 1 type,ratecode code,ratename name from charge_rate
#--                        union all select 2 type,tariff_schema_id code,tariff_name name from tariff_schema
#--                        union all select 3 type,fee_id code,fee_name name from fee)
#--                    )
#--                ORDER BY 2,1) c
#--                where (b.catalog='T_SUBTARIFF_TYPE' and a.subtariff_type=to_number(b.code))
#--                      and a.tariff_id=c.code and a.subtariff_type=c.type and tariff_schema_id=schema_id
#--                      order by a.tariff_criteria,a.applytime)
#--                loop
#--                    if(schema_item_ind = schema_item_count - 1) then
#--                        level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
#--						dbms_output.put_line('7');
#--                    else
#--                        level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
#--						dbms_output.put_line('8');
#--                    end if;
#--                    insert into work_tariff_show(
#--                    txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,tariff_plan_id,item_sort)
#--                    values(
#--                    level_lab||'资费模式项',
#--                    to_char(tariff_item_row.applytime,'yyyy-mm-dd'),
#--                    to_char(tariff_item_row.expiretime,'yyyy-mm-dd'),
#--                    decode(schema_row.tariff_type,1,tariff_item_row.g_criteria,tariff_item_row.tariff_criteria),
#--                    tariff_item_row.subtariff_type_name,
#--                    tariff_item_row.tariff_name,
#--                    tariff_item_row.ratio,
#--                    tariff_item_row.ratetype,
#--                    tariff_item_row.g_param,
#--                    tariff_item_row.precedence,
#--                    tariff_item_row.is_dynamic,
#--                    plan_id,
#--                    item_num);
#--                    item_num := item_num + 1;
#--                    if(fee_flag=1 and tariff_item_row.subtariff_type=1) then --费率
#--                        if(schema_item_ind <> schema_item_count-1) then
#--                            level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
#--							dbms_output.put_line('9');
#--                        else
#--                            level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
#--							dbms_output.put_line('10');
#--                        end if;
#--                        insert into work_tariff_show(
#--                        txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,tariff_plan_id,item_sort)
#--                        select
#--                        level_lab||corner_lab||'费率【'||tariff_item_row.tariff_id||'】:'||ratename,
#--                        a.ratecode,
#--                        a.ratename,
#--                        a.currency,
#--                        b.name currency_type,
#--                        a.units,
#--                        c.name unit_type,
#--                        a.round_unit,
#--                        d.name round_type,
#--                        e.name attr_class,
#--                        plan_id,
#--                        item_num
#--                        from charge_rate a,code_dict b,code_dict c,code_dict d,code_dict e
#--                        where ratecode=tariff_item_row.tariff_id
#--                        and (b.catalog='T_CURRENCY_TYPE' and a.currencytype=to_number(b.code))
#--                        and (c.catalog='T_UNITTYPE' and a.unittype=to_number(c.code))
#--                        and (d.catalog='T_ROUND_TYPE' and a.round_type=to_number(d.code))
#--                        and (e.catalog='T_EVENT_ATTR_CLASS' and a.attr_class=to_number(e.code));
#--                        item_num := item_num + 1;
#--                    else
#--                        if(fee_flag=1 and tariff_item_row.subtariff_type=3) then --费用项
#--                            if(schema_item_ind <> schema_item_count-1) then
#--                                level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
#--                            else
#--                                level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
#--                            end if;
#--                            insert into work_tariff_show(
#--                            txt1,txt2,txt3,tariff_plan_id,item_sort)
#--                            select
#--                            level_lab||corner_lab||'费用项【'||tariff_item_row.tariff_id||'】:'||fee_name,
#--                            fee_id,
#--                            fee_name,
#--                            plan_id,
#--                            item_num
#--                            from fee
#--                            where fee_id=tariff_item_row.tariff_id;
#--                            item_num := item_num + 1;
#--							dbms_output.put_line('11');
#--                        else
#--                            if(tariff_item_row.subtariff_type = 2) then  --资费模式
#--                                schema_deal(tariff_item_row.tariff_id, schema_item_ind, schema_item_count);
#--								dbms_output.put_line('12');
#--                            end if;
#--                        end if;
#--                    end if;
#--                    schema_item_ind := schema_item_ind + 1;
#--                end loop;
#--                level_num := level_num - 1;
#--                level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
#--            end if;
#--            schema_ind := schema_ind + 1;
#--        end loop;
#--        level_num := level_num - 1;
#--        level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
#--    end if;
#--end; --内部函数schema_deal结束
#--begin
#--    --求符合条件的资费政策记录数
#--    select count(*) into plan_count from tariff_plan where tariff_plan_id in(plan_id);
#--    --如果存在符合条件记录
#--    if(plan_count>0) then
#--        --初始化资费索引，资费树级数更新（后面会更新回来）
#--        level_num   := level_num + 1;
#--        level_lab   := level_lab||tee_lab;
#--        plan_ind    := 0;
#--        --遍历资费政策记录
#--        for plan_row in(select * from tariff_plan where tariff_plan_id in (plan_id))
#--        loop
#--            --如果是最后一个资费政策,用corner_lab,否则用tee_lab
#--            if(plan_ind = plan_count-1) then
#--                level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
#--				dbms_output.put_line('13');
#--            else
#--                level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
#--				dbms_output.put_line('14');
#--            end if;
#--            begin
#--                select proc_schema_name into proc_before_name from process_schema where proc_schema_id = plan_row.process_before;
#--				dbms_output.put_line('15');
#--            exception when no_data_found then
#--                proc_before_name := '【空值】';
#--            end;
#--            begin
#--                select proc_schema_name into proc_after_name from process_schema where proc_schema_id = plan_row.process_after;
#--				dbms_output.put_line('16');
#--            exception when no_data_found then
#--                proc_before_name := '【空值】';
#--            end;
#--            --往临时表插入一条数据
#--            insert into work_tariff_show(
#--            txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,tariff_plan_id,item_sort)
#--            values(
#--            '资费政策【'||plan_row.tariff_plan_id||'】:'||plan_row.tariff_plan_name,
#--            plan_row.tariff_plan_id,
#--            plan_row.tariff_plan_name,
#--            plan_row.is_recursive,
#--            plan_row.precedence,
#--            proc_before_name,
#--            proc_after_name,
#--            plan_row.note,
#--            plan_id,
#--            item_num);
#--            item_num := item_num + 1;
#--            --求符合条件的下级资费政策项记录数
#--            select count(*) into item_count from tariff_plan_item where tariff_plan_id=plan_row.tariff_plan_id;
#--            --如果存在资费政策项
#--            if(item_count > 0) then
#--                --如果是最后一条资费政策,用竖线处理,否则用空格
#--                if(plan_ind <> plan_count-1) then
#--                    level_lab := substrb(level_lab,1,(level_num-1)*2)||vline_lab;
#--					dbms_output.put_line('17');
#--                else
#--                    level_lab := substrb(level_lab,1,(level_num-1)*2)||blank_lab;
#--					dbms_output.put_line('18');
#--                end if;
#--                level_num := level_num + 1;
#--                level_lab := level_lab||tee_lab;
#--                item_ind := 0;
#--                --遍历资费政策项
#--                for item_row in(
#--                select a.tariff_plan_id,b.name calctype,a.fee_id,c.fee_name fee,a.tariff_schema_id,d.tariff_name,
#--                e.event_name charging_event_name,a.precedence,a.applytime,a.expiretime,f.name apply_method,
#--                g.name discount_type,h.fee_name base_fee,i.fee_name discount_fee,a.g_charging_cond,
#--                j.name switch_method,a.switch_unit
#--                from tariff_plan_item a, code_dict b,
#--                (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) c,
#--                tariff_schema d, charging_event_def e,code_dict f,code_dict g,
#--                (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) h,
#--                (select 0 fee_id,'【空值】' fee_name from dual union select fee_id,fee_name from fee) i,code_dict j
#--                where (b.catalog='T_CALCTYPE' and a.calctype=to_number(b.code)) and a.fee_id=c.fee_id
#--                and a.tariff_schema_id=d.tariff_schema_id and a.charging_event_id=e.event_id
#--                and (f.catalog='T_APPLY_METHOD' and a.apply_method=to_number(f.code))
#--                and (g.catalog='T_DISCOUNT_TYPE' and a.discount_type=to_number(g.code))
#--                and a.base_fee_id=h.fee_id and a.discount_fee_id=i.fee_id
#--                and (j.catalog='T_SWITCH_TYPE' and a.switch_method=to_number(j.code))
#--                and a.tariff_plan_id=plan_row.tariff_plan_id order by precedence)
#--                loop
#--                    if(item_ind = item_count-1) then
#--                         level_lab := substrb(level_lab,1,(level_num-1)*2)||corner_lab;
#--						 dbms_output.put_line('19');
#--                    else
#--                         level_lab := substrb(level_lab,1,(level_num-1)*2)||tee_lab;
#--						 dbms_output.put_line('20');
#--                    end if;
#--                    insert into work_tariff_show(
#--                    txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,txt15,txt16,tariff_plan_id,item_sort)
#--                    values(
#--                    level_lab||'资费政策项【'||item_row.fee||'】:'||plan_row.tariff_plan_name,
#--                    item_row.tariff_plan_id,
#--                    item_row.calctype,
#--                    item_row.fee,
#--                    item_row.tariff_name,
#--                    item_row.charging_event_name,
#--                    item_row.precedence,
#--                    to_char(item_row.applytime,'yyyy-mm-dd'),
#--                    to_char(item_row.expiretime,'yyyy-mm-dd'),
#--                    item_row.apply_method,
#--                    item_row.discount_type,
#--                    item_row.base_fee,
#--                    item_row.discount_fee,
#--                    item_row.g_charging_cond,
#--                    item_row.switch_method,
#--                    item_row.switch_unit,
#--                    plan_id,
#--                    item_num);
#--                    item_num := item_num + 1;
#--                    schema_deal(item_row.tariff_schema_id, item_ind, item_count);
#--                    item_ind := item_ind + 1;
#--                end loop;
#--                level_num := level_num - 1;
#--                level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
#--            end if;
#--            plan_ind := plan_ind + 1;
#--        end loop;
#--        level_num := level_num - 1;
#--        level_lab := substrb(level_lab,1,lengthb(level_lab)-2);
#--    end if;
#--    open kk for select txt1,txt2,txt3,txt4,txt5,txt6,txt7,txt8,txt9,txt10,txt11,txt12,txt13,txt14,txt15,txt16 from work_tariff_show;
#--    return kk;
#--end;
#--
#--$

  CREATE OR REPLACE FUNCTION "TMP_GET_PROV" (code number)
return number is
  
  region number(4) ;

  begin
   select  substr(mapping_dest,instr(mapping_dest,'.')+1) into region  from mapping_list where mapping_id=4 
and mapping_sour=code;
dbms_output.put_line('1');
  return(region);
end tmp_get_prov;
  
$

  CREATE OR REPLACE FUNCTION "TRIM9" (
s_str  char
)
return char
as
begin
dbms_output.put_line('1');
   return rtrim(rtrim(ltrim(ltrim(s_str, chr(0))), chr(0)));
end  trim9;

$

  CREATE OR REPLACE FUNCTION "WRITEPLRUNLOG" (as_create number,as_plname varchar2,as_plruntime date,as_runstatus varchar2,as_runinfo varchar2,
   				as_data1 varchar2 default 'NONE',as_data2 varchar2  default 'NONE',as_data3 varchar2 default 'NONE',
			   		as_data4 varchar2 default 'NONE',as_data5 varchar2 default 'NONE')
return number is
type Ctype is ref cursor  ;
c_tmp Ctype;
n_plname plrunlog.plname%type;
begin
 /**************************************/
 /*The Function is write plrun status */
 /* as_creeate=0 to craterunlog 	*/
 /* as_creeate=1 to updaterunlog 	*/
 /**************************************/
if as_create=0 then
begin
  	insert into plrunlog (plname,plruntime,runstatus,runinfo,rundata1,rundata2,rundata3,rundata4,rundata5)
  		values(upper(as_plname),as_plruntime,as_runstatus,as_runinfo,as_data1,as_data2,as_data3,as_data4,as_data5);
		dbms_output.put_line('1');
end;
end if;
if as_create=1 then
	update plrunlog set runstatus=as_runstatus,plendtime=to_date('20201104','yyyymmdd'),runinfo=as_runinfo
		where plname=upper(as_plname) and plruntime=as_plruntime and runstatus='RUNNING' and rundata1=as_data1
			and rundata2=as_data2 and rundata3=as_data3 and rundata4=as_data4 and rundata5=as_data5;
			dbms_output.put_line('2');
end if;
return 0;
end;
  

$
delimiter ;$


