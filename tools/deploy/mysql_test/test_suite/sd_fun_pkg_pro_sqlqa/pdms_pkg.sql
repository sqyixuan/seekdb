delimiter //;
create or replace package pdms.DM_PKG_CONST is

  CARD_TYPE_ALL constant number(1) := 0;

  
  CARD_TYPE_PORTION constant number(1) := 1;

 
  CARD_TYPE_STATE_UNACTIVE constant number(1) := 0;

 
  CARD_TYPE_STATE_ACTIVE constant number(1) := 1;

  
  CARD_TYPE_ATTR_XCXK constant number(1) := 0;

 
  CARD_TYPE_ATTR_LBYK constant number(1) := 1;

  
  ATTR_LEVEL_0 constant number(2) := 0;

 
  ATTR_LEVEL_1 constant number(2) := 1;

  
  ATTR_LEVEL_2 constant number(2) := 2;

 
  ATTR_END_TAG_0 constant varchar2(10)  := '0';

 
  ATTR_END_TAG_1 constant varchar2(10)  := '1';

  
  ATTR_MAX_NOTE constant number(2)  := 30;

 
  DATA_SEG_STATE_UNUSED constant number(1) := 0;

 
  DATA_SEG_STATE_USED constant number(1) := 1;

 
  DATA_SEG_STATE_ALL_USED constant number(1) := 2;

 
  DATA_SEG_STATE_NG constant number(1) := 3;

  
  MSISDN_UNUSED_COUNT constant number(6) := 100000;

  
  MSISDN_195_UNUSED_COUNT constant number(6) := 20000;


  IMSI_UNUSED_COUNT constant number(5) := 10000;

 
  IMSI_TYPE_NEW constant number(1) := 0;


  IMSI_TYPE_REPLACE constant number(1) := 1;

 
  IMSI_TYPE_NEW_STR constant varchar2(20) := '新卡';

  
  IMSI_TYPE_REPLACE_STR constant varchar2(20) := '补卡';

 
  IMSI_STATE_UNUSED constant number(1) := 0;

 
  IMSI_STATE_USED constant number(1) := 1;

 
  IMSI_STATE_ALL_USED constant number(1) := 2;

  
  IMSI_STATE_NG constant number(1) := 3;

  
  BACTH_STATE_INIT constant number(1) := 0;

 
  BACTH_STATE_GENERATE constant number(1) := 1;

  
  BACTH_STATE_SUBMIT constant number(1) := 2;


  BACTH_STATE_DATA_GEN constant number(1) := 3;

  
  DATA_STATE_INIT constant number(1) := 0;

 
  DATA_STATE_GENERATE constant number(1) := 1;

  
  BC_BACTH_STATE_INIT constant VARCHAR2(1) := 0;

  
  BC_BACTH_STATE_GENERATING constant VARCHAR2(1) := 1;

  
  BC_BACTH_STATE_GENERATE constant VARCHAR2(1) := 2;

  
  BC_BACTH_STATE_EXPORT constant VARCHAR2(1) := 3;

  
  BC_BACTH_STATE_SUBMIT constant VARCHAR2(1) := 4;

  
  CARN_SN_PRESET_FLAG_YES constant varchar2(1) := '0';

  
  CARN_SN_PRESET_FLAG_NO constant varchar2(1) := '1';

 
  CARN_SN_SINGLE_FLAG_YES constant varchar2(1) := '0';

  
  CARN_SN_SINGLE_FLAG_NO constant varchar2(1) := '1';

 
  CARN_SN_SIM_FLAG_YES constant varchar2(2) := '00';

  
  CARN_SN_SIM_FLAG_NO constant varchar2(2) := '01';

  
  CARN_SN_SWP_FLAG_YES constant varchar2(1) := '1';

 
  CARN_SN_SWP_FLAG_NO constant varchar2(1) := '0';

  
  CARN_SN_M2M_FLAG_YES constant varchar2(1) := '1';

  
  CARN_SN_M2M_FLAG_NO constant varchar2(1) := '0';

  
  FACTORY_NAME_RPS constant varchar2(20) := '远程写卡';

  PIN1 constant varchar2(4) := '1234';

 
  KIND_ID constant NUMBER(3)   := 105;

  ORDER_TYPE_LBYK constant varchar2(100)   := '2';

  ORDER_TYPE_CYC constant varchar2(100)  := '3';

  ORDER_TYPE_CYC_LBYK constant varchar2(100)   := '4';

  BK_CARD_TYPE_LBYK constant varchar2(10)  := 'LBYK@';

  BATCH_TYPE_CYC constant varchar2(10)   := 'CYC@';

  IMSI_CYC_STATUS_UNUSED constant NUMBER(1)  := 0;

  IMSI_CYC_STATUS_USED constant NUMBER(1)  := 1;

  IMSI_CYC_STATUS_GENERATE constant NUMBER(1)  := 2;

  IMSI_CYC_TEMP_STATUS_UNUSED constant NUMBER(1)   := 0;

  IMSI_CYC_TEMP_STATUS_USED constant NUMBER(1)   := 1;

  IMSI_CYC_BACKUP_DAYS constant NUMBER(1)  := 7;

  BATCH_TYPE_IMSI_CYC_NO constant NUMBER(1) := 1;

  BATCH_TYPE_IMSI_CYC_YES constant NUMBER(1)  := 2;

  IMSI_AUTO_CYC_LIMIT_MAX constant NUMBER(10) := 100;

  IMSI_AUTO_CYC_STATUS_0 constant NUMBER(10)  := 0;
  
  IMSI_AUTO_CYC_STATUS_1 constant NUMBER(10)  := 1;
  
  IMSI_AUTO_CYC_STATUS_2 constant NUMBER(10)  := 2;
 
  IMSI_AUTO_CYC_STATUS_3 constant NUMBER(10)  := 3;

  IMSI_CYC_IMPORT_TYPE_0  constant NUMBER(1)  := 0;
  IMSI_CYC_IMPORT_TYPE_1  constant NUMBER(1)  := 1;

  BATCH_CYC_IMSI_STATE_0 constant number(1) := 0;
  BATCH_CYC_IMSI_STATE_1 constant number(1) := 1;
  BATCH_CYC_IMSI_STATE_2 constant number(1) := 2;
  BATCH_CYC_IMSI_STATE_3 constant number(1) := 3;
  BATCH_CYC_IMSI_STATE_4 constant number(1) := 4;

  OPERATORS_CHINA_UNICOM constant number(1) := 1;

  OPERATORS_CHINA_TELECOM constant number(1) := 2;

  OPERATORS_CHINA_MOBILE constant number(1) := 3;

  XH_NET_SEG_UNUSED_COUNT constant number(6) := 300000;

  XH_NET_SEG_UNUSED_N0_COUNT constant number(6) := 100000;

  XH_NET_SEG_UNUSED_TY_COUNT constant number(8) := 1700000;

  XH_NET_SEG_STATE_UNUSED constant number(1) := 0;

  XH_NET_SEG_STATE_USED constant number(1) := 1;

  XH_NET_SEG_STATE_ALL_USED constant number(1) := 2;

  XH_IMSI_UNUSED_COUNT constant number(6) := 100000;

  XH_IMSI_TYPE_NEW constant number(1) := 0;

  XH_IMSI_TYPE_REPLACE constant number(1) := 1;


  XH_IMSI_STATE_UNUSED constant number(1) := 0;


  XH_IMSI_STATE_USED constant number(1) := 1;

  XH_IMSI_STATE_ALL_USED constant number(1) := 2;

  XH_DATA_STATE_INIT constant number(1) := 0;

  XH_DATA_STATE_GENERATE constant number(1) := 1;

  YHDZD_FACTORY constant varchar2(1) := 'E';

  AUTO_MAKE_DATATYPE0 constant varchar2(100)   := '0';
  AUTO_MAKE_DATATYPE1 constant varchar2(100)   := '1';
  AUTO_RESOURCE_STATE0 constant varchar2(2)  := '0';
  AUTO_RESOURCE_STATE1 constant varchar2(2)  := '1';
  AUTO_RESOURCE_STATE2 constant varchar2(2)  := '2';
  AUTO_RESOURCE_STATE3 constant varchar2(2)  := '3';

  AUTO_ORDER_TYPE6     constant varchar2(2)  := '6';
  
  AUTO_RESOURCE_MAX constant number(3) := 900;
  
  AUTO_ASSIGN_COUNT constant varchar2(30)  := 'PDMS.AUTO.MAKECOUNT';
 
  AUTO_ASSIGN_FACTORY constant varchar2(30)  := 'PDMS.AUTOBATCH.FACTORY';

  AUTO_ASSIGN_CARD_TYPE constant varchar2(30)  := 'PDMS.AUTOBATCH.CARDTYPE';

  AUTO_IMSI_TYPE constant varchar2(30)   := 'PDMS.AUTOBATCH.IMSITYPE';

  AUTO_BATCH_TYPE constant varchar2(30)  := 'PDMS.AUTOBATCH.BATCHTYPE';

  AUTO_LOG_TYPE constant number(2)   := 7;

end DM_PKG_CONST;
//

create or replace package pdms.DM_PKG_UTILS is

  type cursorType is ref cursor;

  procedure tokenizer(iStart in number, sPattern in varchar2, sBuffer in varchar2, sResult out varchar2, sNextBuffer out varchar2);

  procedure foundIn(src in varchar2, str in varchar2, founded out boolean);

  procedure append(src in varchar2, str in varchar2, results out varchar2);

  procedure remove(src in varchar2, str in varchar2, results out varchar2);

  procedure stringToArray1(src in varchar2, results out NT, length out number);

  procedure stringToArray2(src  in varchar2, results out VT, length out number);

  procedure stringToArray3(src in varchar2, pattern in varchar2, results out NT, length out number);

  procedure stringToArray4(src  in varchar2, pattern in varchar2, results out VT, length out number);

  function padding(src in varchar2, char in varchar2, len in number, type in varchar2) return varchar2;

  function getRegionIdByDataSeg(strDataSeg in varchar2) return varchar2;

  function getRegionIdByAttrId(strAttrId in number) return varchar2;

end DM_PKG_UTILS;
//

create or replace package body pdms.DM_PKG_UTILS is
  procedure tokenizer(iStart in number, sPattern in varchar2, sBuffer in varchar2, sResult out varchar2, sNextBuffer out varchar2)
  is
    nPos1 number;
    begin
    nPos1 := instr(sBuffer, sPattern, iStart);
    if (nPos1 = 0 and sBuffer is not null) then
      sResult := sBuffer;
      sNextBuffer := null;
    else
      sResult := substr(sBuffer, 1, nPos1-1);
      sNextBuffer := substr(sBuffer, nPos1+1);
    end if;
    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure tokenizer:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end tokenizer;

  procedure foundIn(src in varchar2, str in varchar2, founded out boolean)
  is
    temp_src varchar2(32767) := '';
    temp_str varchar2(32767) := '';
    temp_nextstr varchar2(32767) := ' ';
  begin
    if src is null or str is null then
      founded := false;
    else
      founded := false;
      temp_src := src;
      while(temp_nextstr is not null) loop
        tokenizer(1, ',', temp_src, temp_str, temp_nextstr);
        if temp_str = str then
          founded := true;
          return;
        end if;
        temp_src := temp_nextstr;
      end loop;
    end if;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure foundin:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end foundin;

  procedure append(src in varchar2, str in varchar2, results out varchar2)
  is
    temp_founded boolean := false;
  begin
    if src is null then
      if str is null then
        results := '';
      else
        results := str;
      end if;
    else
      if str is null then
        results := src;
      else
        foundin(src, str, temp_founded);
        if temp_founded then
          results := src;
        else
          results := src || ',' || str;
        end if;
      end if;
    end if;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure append:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end append;

  procedure remove(src in varchar2, str in varchar2, results out varchar2)
  is
    temp_src varchar2(32767) := '';
    temp_str varchar2(32767) := '';
    temp_nextstr varchar2(32767) := ' ';
  begin
    if src is null or str is null then
      results := '';
    else
      temp_src := src;
      while(temp_nextstr is not null) loop
        tokenizer(1, ',', temp_src, temp_str, temp_nextstr);
        if temp_str != str then
          results := results || temp_str || ',';
        end if;
        temp_src := temp_nextstr;
      end loop;
      results := substr(results, 0, length(results)-1);
    end if;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure remove:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end remove;

  procedure stringToArray1(src in varchar2, results out NT, length out number)
  is
  begin
    stringToArray3(src, ',', results, length);
  end stringtoarray1;

  procedure stringToArray2(src in varchar2, results out VT, length out number)
  is
  begin
    stringToArray4(src, ',', results, length);
  end stringtoarray2;

  procedure stringToArray3(src in varchar2, pattern in varchar2, results out NT, length out number)
  is
    temp_src varchar2(32767) := src;
    temp_str varchar2(32767) := null;
    temp_next varchar2(32767) := ' ';
    temp_nt NT := NT();
    i number := 0;
  begin
    while (temp_next is not null) loop
      tokenizer(1, pattern, temp_src, temp_str, temp_next);
      i := i+1;
      length := i;
      temp_nt.extend();
      temp_nt(temp_nt.count) := temp_str;
      temp_src := temp_next;
    end loop;
    results := temp_nt;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure stringtoarray:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end stringToArray3;

  procedure stringToArray4(src in varchar2, pattern in varchar2, results out VT, length out number)
  is
    temp_src varchar2(32767) := src;
    temp_str varchar2(32767) := null;
    temp_next varchar2(32767) := ' ';
    temp_vt VT := VT();
    i number := 0;
  begin
    while (temp_next is not null) loop
      tokenizer(1, pattern, temp_src, temp_str, temp_next);
      i := i+1;
      length := i;
      temp_vt.extend();
      temp_vt(temp_vt.count) := temp_str;
      temp_src := temp_next;
    end loop;
    results := temp_vt;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure stringtoarray:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end stringtoarray4;

  function padding(src in varchar2, char in varchar2, len in number, type in varchar2) return varchar2
  is
    temp_length number := 0;
    temp_str varchar2(50) := null;
    temp_type varchar2(1) := 'L';
    temp_i number := 0;
    temp_results varchar2(255) := null;
  begin
    if length(src) >= len then
      temp_results := src;
    else
      temp_type := Upper(type);
      temp_length := len - length(src);
      while temp_i < temp_length loop
        temp_str := temp_str || char;
        temp_i := temp_i + 1;
      end loop;

      if temp_type = 'L' then
        temp_results := temp_str || src;
      end if;
      if temp_type = 'R' then
        temp_results := src || temp_str;
      end if;
    end if;

    return temp_results;
  end padding;

function getRegionIdByDataSeg(strDataSeg in varchar2) return varchar2
  is
    temp_flag number := 0;
    temp_attr_name varchar2(24) := null;
    temp_parent_id number := 0;
    temp_attr_level number := 0;
    temp_region_Id number := -1;
  begin
    select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=strDataSeg;
    if temp_flag < 1 then
      return temp_region_Id;
    end if;

    select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=(select ATTR_ID from DM_DATA_SEG where DATA_SEG=strDataSeg);
    if temp_flag < 1 then
      return temp_region_Id;
    end if;

    select count(*), ATTR_NAME, PARENT_ID, ATTR_LEVEL into temp_flag, temp_attr_name, temp_parent_id,temp_attr_level
      from DM_HLR_LINK where ATTR_ID=(select ATTR_ID from DM_DATA_SEG where DATA_SEG=strDataSeg);

    if temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_1 and temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_2 then
      return temp_region_Id;
    end if;

    if temp_attr_level=DM_PKG_CONST.ATTR_LEVEL_2 then

      select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=temp_parent_id;
      if temp_flag < 1 then
        return temp_region_Id;
      end if;

      select ATTR_NAME into temp_attr_name from DM_HLR_LINK where ATTR_ID=temp_parent_id;
    end if;

    select count(*) into temp_flag from DM_REGION where REGION_NAME=temp_attr_name;
    if temp_flag < 1 then
        return temp_region_Id;
    end if;

    select REGION_ID into temp_region_Id from DM_REGION where REGION_NAME=temp_attr_name;

    return temp_region_Id;
  end getRegionIdByDataSeg;

  function getRegionIdByAttrId(strAttrId in number) return varchar2
  is
    temp_flag number := -1;
    temp_attr_name varchar2(24) := null;
    temp_parent_id number := 0;
    temp_attr_level number := 0;
    temp_region_Id number := -1;
  begin
    select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=strAttrId;
    if temp_flag < 1 then
      return temp_region_Id;
    end if;

    select ATTR_NAME, PARENT_ID, ATTR_LEVEL into temp_attr_name, temp_parent_id,temp_attr_level
      from DM_HLR_LINK where ATTR_ID=strAttrId;

    if temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_1 and temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_2 then
      return temp_region_Id;
    end if;

    if temp_attr_level=DM_PKG_CONST.ATTR_LEVEL_2 then

      select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=temp_parent_id;
      if temp_flag < 1 then
        return temp_region_Id;
      end if;

      select ATTR_NAME into temp_attr_name from DM_HLR_LINK where ATTR_ID=temp_parent_id;
    end if;

    select count(*) into temp_flag from DM_REGION where REGION_NAME=temp_attr_name;
    if temp_flag < 1 then
        return temp_region_Id;
    end if;

    select REGION_ID into temp_region_Id from DM_REGION where REGION_NAME=temp_attr_name;

    return temp_region_Id;
  end getRegionIdByAttrId;

end DM_PKG_UTILS;
//

delimiter ;//
