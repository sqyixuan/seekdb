delimiter //;
create or replace package pdms.DM_PKG_XH_DATA_MNG is
  
  type cursorType is ref cursor;

  procedure XHBatchInfoGenerate(strRegionShort in varchar2, strBatchNum out varchar2, strSMSP out varchar2, retid out number, retmsg out varchar2);

  procedure XHBatchInfoList(strBatchNum in varchar2, strDataSeg in varchar2, strFactoryName in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHCycBatchInfoList(strBatchNum in varchar2, strDataSeg in varchar2, strFactoryName in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHBatchInfo(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHUpdateBatchStatus(iStatus in number, strBatchNum in varchar2, retid out number, retmsg out varchar2);

  procedure XHBatchInfoDelete(strBatchNum in varchar2, retid out number, retmsg out varchar2);

  procedure XHNetSegList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHNetSegInfo(strXHNetSeg in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHIMSISegAdd(strXHNetSeg in varchar2, iOperator in number, strNotes in varchar2,retid out number, retmsg out varchar2);

  procedure XHIMSISegList(iPage in number, iPageSize in number,strNetSeg in varchar2,strRegionShort in varchar2,strFlag in varchar2, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHIMSISegInfo(strXHIMSISeg in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure XHDataAssign(strRegionShort in varchar2, strXHDataSeg in varchar2, strBatchNum in varchar2,strFactoryName in varchar2,strCardName in varchar2,strDataType in varchar2,strIMSIStart in varchar2,strIMSIEnd in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure XHIMSIStartInfo(strIMSISeg in varchar2, strIMSIStart out varchar2, retid out number, retmsg out varchar2);

  procedure XHIMSISegGenerate(strXHNetSeg in varchar2, strIMSIseg out varchar2, retid out number, retmsg out varchar2);

  procedure XHICCIDSegGenerate(strXHDataSeg in varchar2, strFactoryName in varchar2, iCount in number, strICCIDStart out varchar2, strICCIDEnd out varchar2, retid out number, retmsg out varchar2);

end DM_PKG_XH_DATA_MNG;
//

create or replace package body pdms.DM_PKG_XH_DATA_MNG is
  
  procedure XHBatchInfoGenerate(strRegionShort in varchar2, strBatchNum out varchar2,strSMSP out varchar2, retid out number, retmsg out varchar2)
  is
    temp_date varchar2(6) :=  to_char(sysdate, 'YYMMDD');
    batchNum_1_10 varchar2(10) := null;
    batchNum_11_14 varchar2(4) := null;
    temp_num number := -1;
  begin
    select count(*) into temp_num from DM_REGION where REGION_SHORT=strRegionShort;
    if temp_num<1 then
      retid := 8041;
      retmsg  := '地市缩写['|| strRegionShort ||']不存在！';
      return;
    end if;
    select SMSP into strSMSP from DM_REGION where REGION_SHORT=strRegionShort;

    batchNum_1_10 := strRegionShort || 'XH' || temp_date;
    if batchNum_1_10 is null or length(batchNum_1_10)<>10 then
      retid := 8008;
      retmsg  := '携号批次号生成错误！';
      return;
    end if;
    select COUNT(*), MAX(SUBSTR(BATCH_NUM,11,4)) into temp_num, batchNum_11_14 from DM_XH_BATCH where BATCH_NUM like batchNum_1_10 || '%';
    if batchNum_11_14 is null then
      batchNum_11_14 := '0001';
      strBatchNum := batchNum_1_10 || batchNum_11_14;
    elsif batchNum_11_14='9999' then
      retid := 8009;
      retmsg  := '今天分配携号的次数已用完，请明天继续分配数据！';
      return;
    else
      batchNum_11_14 := substr(to_char(10000 + TO_NUMBER(batchNum_11_14) + 1),2,4);
      strBatchNum := batchNum_1_10 || batchNum_11_14;
    end if;

    retid := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid := sqlcode;
        retmsg := sqlerrm;
  end XHBatchInfoGenerate;

 
  procedure XHBatchInfoList(strBatchNum in varchar2, strDataSeg in varchar2, strFactoryName in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
    temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
    temp_sql_recodes varchar2(1024) := '';
    temp_sql_list varchar2(2048) := '';
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    temp_sql_recodes := 'select count(*) as records from DM_XH_BATCH where BATCH_NUM is not null ';
    temp_sql_list := 'select * from ( ';
    temp_sql_list := temp_sql_list || 'select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,';
    temp_sql_list := temp_sql_list || 'ICCID_START,ICCID_END,to_char(BATCH_TIME, ''yyyy-mm-dd hh24:mi:ss'') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM ';
    temp_sql_list := temp_sql_list || 'from (select * from DM_XH_BATCH where BATCH_NUM is not null and (NOTES not like ''' || DM_PKG_CONST.BATCH_TYPE_CYC || '%'' or NOTES is null) ';

    if strBatchNum is not null then
      temp_sql_recodes := temp_sql_recodes || ' and BATCH_NUM=''' || strBatchNum || '''' ;
      temp_sql_list := temp_sql_list || ' and BATCH_NUM=''' || strBatchNum || '''';
    end if;
    if strFactoryName is not null then
      temp_sql_recodes := temp_sql_recodes || ' and FACTORY_NAME=''' || strFactoryName || '''' ;
      temp_sql_list := temp_sql_list || ' and FACTORY_NAME=''' || strFactoryName || '''';
    end if;
    if strDataSeg is not null then
      temp_sql_recodes := temp_sql_recodes || ' and substr(MSISDN_START, 1, 4)=''' || strDataSeg || '''' ;
      temp_sql_list := temp_sql_list || ' and substr(MSISDN_START, 1, 4)=''' || strDataSeg || '''' ;
    end if;
    if strStartTime is not null and strEndTime is not null then
      temp_sql_recodes := temp_sql_recodes || ' and BATCH_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
      temp_sql_list := temp_sql_list || ' and BATCH_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
    end if;

    temp_sql_list := temp_sql_list || ' order by BATCH_TIME desc)';
    temp_sql_list := temp_sql_list || ') where NUM between ' || temp_minPage || ' and ' || temp_maxPage;

    execute immediate temp_sql_recodes into records;
    open results for temp_sql_list;

    retid := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end XHBatchInfoList;

  
  procedure XHCycBatchInfoList(strBatchNum in varchar2, strDataSeg in varchar2, strFactoryName in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
    temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
    temp_sql_recodes varchar2(1024) := '';
    temp_sql_list varchar2(2048) := '';
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    temp_sql_recodes := 'select count(*) as records from DM_XH_BATCH where BATCH_NUM is not null ';
    temp_sql_list := 'select * from ( ';
    temp_sql_list := temp_sql_list || 'select BATCH_NUM,BATCH_REGION,SMSP,HLR_NAME,BRAND_NAME,FACTORY_NAME,CARD_TYPE,MSISDN_START,MSISDN_END,DATA_TYPE,BATCH_COUNT,IMSI_START,IMSI_END,';
    temp_sql_list := temp_sql_list || 'ICCID_START,ICCID_END,to_char(BATCH_TIME, ''yyyy-mm-dd hh24:mi:ss'') as BATCH_TIME,BATCH_STATUS,PRODUCE_STATUS,ORDER_CODE,NOTES, ROWNUM NUM ';
    temp_sql_list := temp_sql_list || 'from (select * from DM_XH_BATCH where BATCH_NUM is not null and (NOTES like ''' || DM_PKG_CONST.BATCH_TYPE_CYC || '%'' ) ';

    if strBatchNum is not null then
      temp_sql_recodes := temp_sql_recodes || ' and BATCH_NUM=''' || strBatchNum || '''' ;
      temp_sql_list := temp_sql_list || ' and BATCH_NUM=''' || strBatchNum || '''';
    end if;
    if strFactoryName is not null then
      temp_sql_recodes := temp_sql_recodes || ' and FACTORY_NAME=''' || strFactoryName || '''' ;
      temp_sql_list := temp_sql_list || ' and FACTORY_NAME=''' || strFactoryName || '''';
    end if;
    if strDataSeg is not null then
      temp_sql_recodes := temp_sql_recodes || ' and substr(MSISDN_START, 1, 4)=''' || strDataSeg || '''' ;
      temp_sql_list := temp_sql_list || ' and substr(MSISDN_START, 1, 4)=''' || strDataSeg || '''' ;
    end if;
    if strStartTime is not null and strEndTime is not null then
      temp_sql_recodes := temp_sql_recodes || ' and BATCH_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
      temp_sql_list := temp_sql_list || ' and BATCH_TIME between (to_date(''' || temp_startTime || ''', ''yyyy-mm-dd hh24:mi:ss'')) and (to_date(''' || temp_endTime || ''', ''yyyy-mm-dd hh24:mi:ss''))';
    end if;

    temp_sql_list := temp_sql_list || ' order by BATCH_TIME desc)';
    temp_sql_list := temp_sql_list || ') where NUM between ' || temp_minPage || ' and ' || temp_maxPage;

    execute immediate temp_sql_recodes into records;
    open results for temp_sql_list;

    retid := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end XHCycBatchInfoList;

 
  procedure XHBatchInfo(strBatchNum in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_XH_BATCH where BATCH_NUM=strBatchNum;
    if temp_flag < 1 then
      retid := 8035;
      retmsg  := '指定携号批次[' || strBatchNum || ']信息不存在。';
      return;
    end if;

    select COUNT(*) into temp_flag from DM_XH_BATCH a, DM_XH_NET_SEG b where a.BATCH_NUM=strBatchNum and a.HLR_NAME=b.NET_SEG;
    if temp_flag < 1 then
      retid := 8036;
      retmsg  := '指定携号批次的携号网段信息不存在。';
      return;
    end if;

    open results for
      select a.BATCH_NUM,a.BATCH_REGION,a.SMSP,a.HLR_NAME,a.BRAND_NAME,a.FACTORY_NAME,a.CARD_TYPE,a.MSISDN_START,a.MSISDN_END,a.DATA_TYPE,a.BATCH_COUNT,a.IMSI_START,a.IMSI_END,
        a.ICCID_START,a.ICCID_END,to_char(a.BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,a.BATCH_STATUS,a.PRODUCE_STATUS,a.ORDER_CODE,a.NOTES,ROWNUM NUM
      from DM_XH_BATCH a where a.BATCH_NUM=strBatchNum;

    update DM_XH_BATCH set PRODUCE_STATUS=DM_PKG_CONST.DATA_STATE_GENERATE where BATCH_NUM=strBatchNum;
    commit;

    retid := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
              rollback;
  end XHBatchInfo;

  
  procedure XHUpdateBatchStatus(iStatus in number, strBatchNum in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_XH_BATCH where BATCH_NUM = strBatchNum;
    if temp_flag < 1 then
      retid  := 5003;
      retmsg  := '指定的批次号[' || strBatchNum || ']信息不存在！';
      return;
    end if;

    update DM_XH_BATCH set PRODUCE_STATUS=iStatus where BATCH_NUM = strBatchNum;
    commit;

    retid  := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end XHUpdateBatchStatus;

  
  procedure XHBatchInfoDelete(strBatchNum in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_imsiSeg varchar2(11) := null;
    temp_imsiEnd varchar2(5) := null;
    temp_max_imsiEnd varchar2(15) := null;
    temp_batchStatus number := -1;
    temp_produceStatus number := -1;
    temp_netSeg varchar2(3) := null;
    temp_count number := -1;
    temp_batchCount number := -1;
    temp_states number := -1;
    temp_procuce_states number := -1;
    temp_imsi_count number := -1;
    temp_iflag number := -1;
  begin
    select count(*) into temp_flag from DM_XH_BATCH where BATCH_NUM = strBatchNum;
    if temp_flag < 1 then
      retid := 8037;
      retmsg  := '指定的携号批次号[' || strBatchNum || ']信息不存在！';
      return;
    end if;
    select PRODUCE_STATUS into temp_procuce_states from DM_XH_BATCH where BATCH_NUM = strBatchNum;
    if temp_procuce_states = DM_PKG_CONST.XH_DATA_STATE_GENERATE then
      retid := 8042;
      retmsg  := '指定的携号批次号[' || strBatchNum || ']信息已生成数据，不能删除批次！';
      return;
    end if;

    select substr(MSISDN_START, 1, 3), BATCH_COUNT into temp_netSeg, temp_batchCount from DM_XH_BATCH where BATCH_NUM = strBatchNum;

  select substr(IMSI_END,1,10), substr(IMSI_END,11,5), BATCH_STATUS, PRODUCE_STATUS into temp_imsiSeg, temp_imsiEnd, temp_batchStatus, temp_produceStatus
  from DM_XH_BATCH where BATCH_NUM = strBatchNum;
  select max(substr(IMSI_END,11,5)) into temp_max_imsiEnd from DM_XH_BATCH where substr(IMSI_END,1,10)=temp_imsiSeg;
  if temp_imsiEnd <> temp_max_imsiEnd then
  retid := 8038;
  retmsg  := '删除的携号批次必须是所属IMSI段最后一个批次！';
  return;
  end if;

  delete from DM_XH_BATCH where BATCH_NUM = strBatchNum;

  select count(*), max(IMSI_END) into temp_flag, temp_max_imsiEnd from DM_XH_BATCH where substr(IMSI_START, 1, 10) = temp_imsiSeg;
  select SEG_COUNT,XH_RULE_FLAG into temp_count,temp_iflag from DM_XH_NET_SEG where NET_SEG=temp_netSeg;
  if temp_flag = 0 then

   update DM_XH_IMSI set XH_STATUS = DM_PKG_CONST.XH_IMSI_STATE_UNUSED, XH_IMSI_COUNT = DM_PKG_CONST.XH_IMSI_UNUSED_COUNT where XH_IMSI_SEG = temp_imsiSeg;

   if temp_iflag = 1 then

    select count(*) into temp_flag from DM_XH_BATCH where substr(MSISDN_START, 1, 3) = temp_netSeg;
    if temp_flag = 0 then
      if temp_netSeg in(130,131,132,133,153,155,156,186,189,145) then
      temp_states := DM_PKG_CONST.XH_NET_SEG_UNUSED_COUNT;
      elsif temp_netSeg in(1349,176,177,180,181,185,173,149,175,199,166) then -- 20170220 add 173规则,20181010 add 149,175,199,166,20190417 add 191
      temp_states := DM_PKG_CONST.XH_NET_SEG_UNUSED_N0_COUNT;
      end if;
      update DM_XH_NET_SEG set SEG_STATE = DM_PKG_CONST.XH_NET_SEG_STATE_UNUSED, SEG_COUNT = temp_states where NET_SEG = temp_netSeg;
    elsif temp_flag > 0 then
      select SEG_COUNT into temp_count from DM_XH_NET_SEG where NET_SEG=temp_netSeg;
      temp_count := temp_count + temp_batchCount;
      update DM_XH_NET_SEG set SEG_STATE = DM_PKG_CONST.XH_NET_SEG_STATE_USED, SEG_COUNT = temp_count where NET_SEG = temp_netSeg;
     end if;

    elsif temp_iflag = 2 then

    select count(*) into temp_flag from DM_XH_BATCH where substr(MSISDN_START, 1, 3) in(select NET_SEG from DM_XH_NET_SEG where XH_RULE_FLAG=2);
    if temp_flag = 0 then
      temp_states := DM_PKG_CONST.XH_NET_SEG_UNUSED_TY_COUNT;
      update DM_XH_NET_SEG set SEG_STATE = DM_PKG_CONST.XH_NET_SEG_STATE_UNUSED, SEG_COUNT = temp_states where XH_RULE_FLAG = 2;
    elsif temp_flag > 0 then
      select SEG_COUNT into temp_count from DM_XH_NET_SEG where NET_SEG=temp_netSeg;
      temp_count := temp_count + temp_batchCount;
      update DM_XH_NET_SEG set SEG_STATE = DM_PKG_CONST.XH_NET_SEG_STATE_USED, SEG_COUNT = temp_count where XH_RULE_FLAG = 2;
    end if;
    end if;
  elsif temp_flag > 0 then

    update DM_XH_IMSI set XH_STATUS = DM_PKG_CONST.XH_IMSI_STATE_USED,XH_IMSI_COUNT = (100000-to_number(substr(temp_max_imsiEnd, 11, 5))-1) where XH_IMSI_SEG = temp_imsiSeg;

    select SEG_COUNT into temp_count from DM_XH_NET_SEG where NET_SEG=temp_netSeg;
    temp_count := temp_count + temp_batchCount;
    temp_states := DM_PKG_CONST.XH_NET_SEG_STATE_USED;
    if temp_iflag = 1 then
      update DM_XH_NET_SEG set SEG_COUNT=temp_count, SEG_STATE=temp_states where NET_SEG=temp_netSeg;
    elsif temp_iflag = 2 then
      update DM_XH_NET_SEG set SEG_COUNT=temp_count, SEG_STATE=temp_states where XH_RULE_FLAG = temp_iflag;
    end if;

  end if;

  select SEG_COUNT into temp_count from DM_XH_NET_SEG where NET_SEG=temp_netSeg;
  if temp_iflag=2 then
    select sum(XH_IMSI_COUNT) into temp_imsi_count from DM_XH_IMSI where XH_NET_SEG='TY';
  elsif temp_iflag=1 then
    select sum(XH_IMSI_COUNT) into temp_imsi_count from DM_XH_IMSI where XH_NET_SEG=temp_netSeg;
  else
    retid := 8016;
    retmsg  := '携号网段表中网段[' || temp_netSeg || ']的携号规则类型不存在。';
    return;
  end if;

  if temp_count<>temp_imsi_count then
    retid := 8039;
    retmsg  := '携号IMSI表与携号网段表的数量不匹配，请确认！';
    rollback;
    return;
  end if;

    commit;

    retid := 0;
    retmsg  := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end XHBatchInfoDelete;

  
  procedure XHNetSegList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    select COUNT(*) into records from DM_XH_NET_SEG;
    open results for
      select * from (
        select ATTR_ID,NET_SEG,SEG_COUNT,SEG_STATE,OPERATORS,NOTES,XH_RULE_FLAG,ROWNUM as NUM
        from (select * from DM_XH_NET_SEG order by NET_SEG)
      ) where NUM between temp_minPage and temp_maxPage;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg := sqlerrm;
  end XHNetSegList;

 
  procedure XHNetSegInfo(strXHNetSeg in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_XH_NET_SEG where NET_SEG=strXHNetSeg;
    if temp_flag < 1 then
      retid := 8007;
      retmsg  := '携号网段[' || strXHNetSeg || ']信息不存在。';
      return;
    end if;

    open results for select * from DM_XH_NET_SEG where NET_SEG=strXHNetSeg;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end XHNetSegInfo;

  
  procedure XHIMSISegAdd(strXHNetSeg in varchar2, iOperator in number, strNotes in varchar2,retid out number, retmsg out varchar2)
  is
    temp_num number := 0;
    temp_flag number := 0;
    temp_imsi varchar2(10);
    imsi_seg varchar2(11);
    strIMSIType varchar2(1);
    data_seg varchar2(4);
    i number := 0;
  begin
   
    if length(strXHNetSeg)<>3 then
      retid := 8019;
      retmsg  := '输入的携号网段位数不正确！';
      return;
    end if;

    
    if (substr(strXHNetSeg,1,2)= '13' and substr(strXHNetSeg,3,1) between '0' and '3') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '145') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '153') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,2)= '15' and substr(strXHNetSeg,3,1) between '5' and '6') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '186') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '189') then
      temp_flag := 1;
    
    elsif (substr(strXHNetSeg,1,3)= '176') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '177') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '180') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '181') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '185') then
      temp_flag := 1;
    
    elsif (substr(strXHNetSeg,1,3)= '173') then
      temp_flag := 1;
    
    elsif (substr(strXHNetSeg,1,3)= '149') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '175') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '199') then
      temp_flag := 1;
    elsif (substr(strXHNetSeg,1,3)= '166') then
      temp_flag := 1;
    
    elsif (substr(strXHNetSeg,1,3)= '191') then
      temp_flag := 1;
    else
      temp_flag := 0;
    end if;
    if temp_flag = 0 then
      retid := 8020;
      retmsg  := '插入的携号网段数据不合法！';
      return;
    end if;

   
    select count(*) into temp_num from DM_XH_NET_SEG where NET_SEG = strXHNetSeg;
    if temp_num > 0 then
      retid := 8021;
      retmsg  := '插入的携号网段数据已经存在！';
      return;
    end if;

    
    if iOperator<>DM_PKG_CONST.OPERATORS_CHINA_UNICOM and iOperator<>DM_PKG_CONST.OPERATORS_CHINA_TELECOM and iOperator<>DM_PKG_CONST.OPERATORS_CHINA_MOBILE then
      retid := 8022;
      retmsg  := '运营商代码不存在！';
      return;
    end if;

    
    XHIMSISegGenerate(strXHNetSeg, temp_imsi, retid, retmsg);
      if retid <> 0 then
      
      return;
    end if;

    
    if substr(temp_imsi, 9, 1) = 'X' then
    
      insert into DM_XH_NET_SEG(ATTR_ID,NET_SEG,SEG_COUNT,SEG_STATE,OPERATORS,NOTES,XH_RULE_FLAG)
      values(0, strXHNetSeg, DM_PKG_CONST.XH_NET_SEG_UNUSED_COUNT, DM_PKG_CONST.XH_NET_SEG_STATE_UNUSED, iOperator, strNotes,0);
      for j in 7..9 loop
        imsi_seg := substr(temp_imsi, 1, 8) || TO_CHAR(j) || substr(temp_imsi, 10, 1);
        strIMSIType := DM_PKG_CONST.XH_IMSI_TYPE_REPLACE;
        insert into DM_XH_IMSI(XH_IMSI_SEG, XH_IMSI_COUNT, XH_NET_SEG, XH_IMSI_TYPE, XH_STATUS)
        values(imsi_seg, DM_PKG_CONST.XH_IMSI_UNUSED_COUNT, strXHNetSeg, strIMSIType, DM_PKG_CONST.XH_IMSI_STATE_UNUSED);
      end loop;
    elsif (substr(temp_imsi, 9, 1) = '0') or (substr(temp_imsi, 9, 1) = '1') then
      insert into DM_XH_NET_SEG(ATTR_ID,NET_SEG,SEG_COUNT,SEG_STATE,OPERATORS,NOTES,XH_RULE_FLAG)
      values(0, strXHNetSeg, DM_PKG_CONST.XH_NET_SEG_UNUSED_N0_COUNT, DM_PKG_CONST.XH_NET_SEG_STATE_UNUSED, iOperator, strNotes,0);

      strIMSIType := DM_PKG_CONST.XH_IMSI_TYPE_REPLACE;
      imsi_seg := temp_imsi;
      insert into DM_XH_IMSI(XH_IMSI_SEG, XH_IMSI_COUNT, XH_NET_SEG, XH_IMSI_TYPE, XH_STATUS)
      values(imsi_seg, DM_PKG_CONST.XH_IMSI_UNUSED_COUNT, strXHNetSeg, strIMSIType, DM_PKG_CONST.XH_IMSI_STATE_UNUSED);
    end if;
    commit;

    retid := 0;
    retmsg  := '成功。';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end XHIMSISegAdd;

  
  procedure XHIMSISegList(iPage in number, iPageSize in number,strNetSeg in varchar2,strRegionShort in varchar2,strFlag in varchar2, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := 0;
    temp_regionCode varchar2(5) :=null;
    temp_regionName varchar2(20) :=null;
    temp_num number := 0;
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;
    if strFlag=1 then
        select XH_RULE_FLAG into temp_flag from DM_XH_NET_SEG where NET_SEG=strNetSeg;
        if temp_flag=2 then
           select COUNT(*) into records from DM_XH_IMSI where XH_NET_SEG='TY';
           open results for
             select * from (
                select XH_IMSI_SEG,XH_IMSI_COUNT,XH_NET_SEG,XH_IMSI_TYPE,XH_STATUS,ROWNUM as NUM
                from DM_XH_IMSI
                where XH_NET_SEG='TY' order by XH_IMSI_SEG asc
                )where NUM between temp_minPage and temp_maxPage;
        elsif temp_flag=1 then
          select COUNT(*) into records from DM_XH_IMSI where XH_NET_SEG=strNetSeg;
          open results for
             select * from (
                select XH_IMSI_SEG,XH_IMSI_COUNT,XH_NET_SEG,XH_IMSI_TYPE,XH_STATUS,ROWNUM as NUM
                from DM_XH_IMSI
                where XH_NET_SEG=strNetSeg order by XH_IMSI_SEG asc
                ) where NUM between temp_minPage and temp_maxPage;
        else
          retid := 8001;
          retmsg  := '携号网段表中网段[' || strNetSeg || ']的携号规则类型不存在。';
          return;
        end if;
     elsif strFlag=2 then
        if strRegionShort is NULL then
          select COUNT(*) into records from DM_XH_IMSI where XH_NET_SEG='TY';
          open results for
             select * from (
                select XH_IMSI_SEG,XH_IMSI_COUNT,XH_NET_SEG,XH_IMSI_TYPE,XH_STATUS,ROWNUM as NUM
                from DM_XH_IMSI
                where XH_NET_SEG='TY' order by XH_IMSI_SEG asc
                )where NUM between temp_minPage and temp_maxPage;
        else
          select count(*) into temp_num from DM_REGION where REGION_SHORT=strRegionShort;
          if temp_num<1 then
            retid := 8041;
            retmsg  := '地市缩写['|| strRegionShort ||']不存在！';
            return;
          end if;
          select REGION_CODE,REGION_NAME into temp_regionCode,temp_regionName from DM_REGION where REGION_SHORT=strRegionShort;
          select COUNT(*) into records from DM_XH_IMSI where XH_REGION_CODE=temp_regionCode and XH_REGION_NAME=temp_regionName;
          open results for
             select * from (
                select XH_IMSI_SEG,XH_IMSI_COUNT,XH_NET_SEG,XH_IMSI_TYPE,XH_STATUS,ROWNUM as NUM
                from DM_XH_IMSI
                where XH_REGION_CODE=temp_regionCode and XH_REGION_NAME=temp_regionName order by XH_IMSI_SEG asc
                )where NUM between temp_minPage and temp_maxPage;
        end if;
     end if;
    retid := 0;
    retmsg  := '成功！';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end XHIMSISegList;
  
  
  procedure XHIMSISegInfo(strXHIMSISeg in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_XH_IMSI where XH_IMSI_SEG=strXHIMSISeg;
    if temp_flag < 1 then
      retid := 8007;
      retmsg  := '携号IMSI[' || strXHIMSISeg || ']信息不存在。';
      return;
    end if;

    open results for select * from DM_XH_IMSI where XH_IMSI_SEG=strXHIMSISeg;

    retid := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end XHIMSISegInfo;
  procedure XHDataAssign(strRegionShort in varchar2, strXHDataSeg in varchar2, strBatchNum in varchar2,strFactoryName in varchar2,strCardName in varchar2,strDataType in varchar2,strIMSIStart in varchar2,strIMSIEnd in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_batchNum varchar2(14) := null;
    temp_smsp varchar2(14) := null;
    temp_region_name varchar2(20) := null;
    temp_attr_name varchar2(24) := null;
    temp_imsi_type varchar2(20) := null;
    temp_imsi_status number := -1;
    temp_imsi_count number := -1;
    temp_imsiSeg varchar2(11) := null;
    temp_iccid_start varchar2(20) := null;
    temp_iccid_end varchar2(20) := null;
    temp_count number := -1;
    temp_imsi_start varchar2(15) := null;
    temp_msisdn_start varchar2(11) := null;
    temp_flag number := -1;
    temp_states number := -1;
    temp_iflag number := -1;
  begin
    XHBatchInfoGenerate(strRegionShort, temp_batchNum,temp_smsp,retid,retmsg);
    if retid <> 0 then
      return;
    end if;
    if strBatchNum <> temp_batchNum then
      retid := 8003;
      retmsg  := '输入的携号批次号不正确！';
      return;
    end if;

    select REGION_NAME,SMSP into temp_region_name,temp_smsp from DM_REGION where REGION_SHORT=strRegionShort;

    select count(*) into temp_flag from DM_FACTORY where FACTORY_NAME = strFactoryName;
    if temp_flag < 1 then
      retid := 8004;
      retmsg  := '名称为[' || strFactoryName || ']的卡商信息不存在！';
      return;
    end if;

    select count(*) into temp_flag from DM_CARD_TYPE where CARD_NAME=strCardName;
    if temp_flag < 1 then
      retid := 8013;
      retmsg  := '指定卡类型[' || strCardName || ']不存在！';
      return;
    end if;

    select count(*) into temp_flag from DM_XH_NET_SEG where NET_SEG=substr(strXHDataSeg, 1, 3);
    if temp_flag < 1 then
      retid := 8014;
      retmsg  := '携号网段表中不存在网段[' || substr(strXHDataSeg, 1, 3) || ']信息！';
      return;
    end if;

    if length(strIMSIStart)<> 15 or length(strIMSIEnd)<> 15 or
      not regexp_like(substr(strIMSIStart,11,5),'[0-9][0-9][0-9][0-9][0-9]') or
      not regexp_like(substr(strIMSIEnd,11,5),'[0-9][0-9][0-9][0-9][0-9]')
    then
      retid := 8017;
      retmsg  := '输入的起始或终止携号IMSI不合法！';
      return;
    end if;

    select XH_RULE_FLAG into temp_iflag from DM_XH_NET_SEG where NET_SEG=substr(strXHDataSeg, 1, 3);

    if substr(strIMSIStart,1,10) <> substr(strIMSIEnd,1,10) then
      retid := 8018;
      retmsg  := '输入的起始和终止IMSI前10位不对应！';
      return;
    end if;
    select count(*) into temp_flag from DM_XH_IMSI where XH_IMSI_SEG=substr(strIMSIStart,1,10);
    if  temp_flag < 1 then
      retid := 8016;
      retmsg  := '要分配的携号IMSI段不存在。';
      return;
    end if;
    XHIMSISegGenerate(strXHDataSeg, temp_imsiSeg, retid, retmsg);
    if  temp_iflag=1 then
      if (substr(temp_imsiSeg, 1,8)<>substr(strIMSIStart, 1,8)) or (substr(temp_imsiSeg, 10,1)<>substr(strIMSIStart, 10,1)) then
        retid := 8020;
        retmsg  := '输入的携号网段与IMSI段不匹配！';
        return;
      end if;
    elsif temp_iflag=2 then
      if (substr(temp_imsiSeg, 1,8)<>substr(strIMSIStart, 1,8)) then
        retid := 8020;
        retmsg  := '输入的携号网段与IMSI段不匹配！';
        return;
      end if;
    end if;

    select XH_STATUS, XH_IMSI_COUNT into temp_imsi_status, temp_imsi_count from DM_XH_IMSI where XH_IMSI_SEG=substr(strIMSIStart,1,10);
    if  temp_imsi_status<>0 and temp_imsi_status<>1 then
      retid := 8022;
      retmsg  := '输入的携号IMSI段不可用！';
      return;
    end if;

    if  temp_imsi_count <> (100000 - to_number(substr(strIMSIStart,11,5))) then
      retid := 8023;
      retmsg  := '输入的起始携号IMSI段不正确！';
      return;
    end if;

    if substr(strICCIDStart, 1, 13) <> substr(strICCIDEnd, 1, 13) then
      retid := 8024;
      retmsg  := '输入的起始和终止携号ICCID的前13位不一致！';
      return;
    end if;

    XHICCIDSegGenerate(strXHDataSeg, strFactoryName, iBatchCount, temp_iccid_start, temp_iccid_end, retid, retmsg);
    if retid <> 0 then
      return;
    end if;
    if temp_iccid_start <> strICCIDStart then
      retid := 8029;
      retmsg  := '输入的起始携号ICCID不正确！';
      return;
    end if;
    if temp_iccid_end <> strICCIDEnd then
      retid := 8030;
      retmsg  := '输入的终止携号ICCID不正确！';
      return;
    end if;
    temp_count := to_number(substr(strIMSIEnd, -5, 5)) - to_number(substr(strIMSIStart, -5, 5)) + 1;
    if temp_count <> iBatchCount then
      retid := 8031;
      retmsg  := '携号IMSI起止范围与批次数量不匹配！';
      return;
    end if;
    temp_count := to_number(substr(strICCIDEnd, -7, 7)) - to_number(substr(strICCIDStart, -7, 7)) + 1;
    if temp_count <> iBatchCount then
      retid  := 8032;
      retmsg := '携号ICCID起止范围与批次数量不匹配！';
      return;
    end if;
    temp_count := to_number(substr(strIMSIEnd, 11,5)) - to_number(substr(strIMSIStart, 11,5)) + 1;
    if (to_number(substr(strIMSIStart, 11,5)) + temp_count) > 100000 then
      retid := 8033;
      retmsg  := '输入的携号批次数量不正确！';
      return;
    end if;
    XHIMSIStartInfo(substr(strIMSIStart, 1,10), temp_imsi_start, retid , retmsg);
    if retid <> 0 then
      return;
    end if;
    if temp_imsi_start <> strIMSIStart then
      retid := 8035;
      retmsg  := '输入的携号IMSI起始值不正确！';
      return;
    end if;
    select count(*) into temp_flag from DM_XH_BATCH
      where substr(IMSI_START, 1, 10)=substr(strIMSIStart, 1, 10) and
      to_number(substr(IMSI_END, 11, 5))>=to_number(substr(strIMSIStart, 11, 5));
    if temp_flag > 0 then
      retid := 8036;
      retmsg  := '携号IMSI段重叠，请检查携号IMSI起始和终止值！';
      return;
    end if;
    select count(*) into temp_flag from DM_XH_BATCH
      where substr(ICCID_START, 1, 13)=substr(strICCIDStart, 1, 13)
      and to_number(substr(ICCID_END, -7, 7))>=to_number(substr(strICCIDStart, -7, 7));
    if temp_flag > 0 then
      retid := 8037;
      retmsg  := '携号ICCID段重叠，请检查携号ICCID起始和终止值！';
      return;
    end if;

    temp_msisdn_start := strXHDataSeg || '0000000';
    insert into DM_XH_BATCH values(
          strBatchNum,
          temp_region_name,
          temp_smsp,
          substr(strXHDataSeg,1,3),
          '未知',
          strFactoryName,
          strCardName,
          temp_msisdn_start,
          temp_msisdn_start,
          strDataType,
          iBatchCount,
          strIMSIStart,
          strIMSIEnd,
          strICCIDStart,
          strICCIDEnd,
          sysdate,
          '0',
          '0',
          '0', 
          strNotes); 


    if  temp_iflag=1 then
      select SEG_COUNT into temp_count from DM_XH_NET_SEG where NET_SEG=substr(strXHDataSeg,1,3);
      temp_count := temp_count - iBatchCount;
      if 0 < temp_count and temp_count <300000 then
      temp_states := DM_PKG_CONST.XH_NET_SEG_STATE_USED;
      elsif temp_count = 0 then
      temp_states := DM_PKG_CONST.XH_NET_SEG_STATE_ALL_USED;
      end if;
      update DM_XH_NET_SEG set SEG_COUNT=temp_count, SEG_STATE=temp_states where NET_SEG=substr(strXHDataSeg,1,3);

    elsif  temp_iflag=2 then
      select SEG_COUNT into temp_count from DM_XH_NET_SEG where NET_SEG=substr(strXHDataSeg,1,3);
      temp_count := temp_count - iBatchCount;
      if 0 < temp_count and temp_count < DM_PKG_CONST.XH_NET_SEG_UNUSED_TY_COUNT then
      temp_states := DM_PKG_CONST.XH_NET_SEG_STATE_USED;
      elsif temp_count = 0 then
      temp_states := DM_PKG_CONST.XH_NET_SEG_STATE_ALL_USED;
      end if;
      update DM_XH_NET_SEG set SEG_COUNT=temp_count, SEG_STATE=temp_states where XH_RULE_FLAG=2;
    else
      retid := 8016;
      retmsg  := '携号网段表中网段[' || substr(strXHDataSeg,1,3) || ']的携号规则类型不存在。';
      return;
    end if;

    temp_imsi_count := temp_imsi_count-iBatchCount;
    if 0 < temp_imsi_count and temp_imsi_count <100000 then
    temp_states := DM_PKG_CONST.XH_IMSI_STATE_USED;
    elsif temp_imsi_count = 0 then
    temp_states := DM_PKG_CONST.XH_IMSI_STATE_ALL_USED;
    end if;
    update DM_XH_IMSI set XH_IMSI_COUNT=temp_imsi_count, XH_STATUS=temp_states where XH_IMSI_SEG=substr(strIMSIStart,1,10);--集团

    if temp_iflag=1 then
      select sum(XH_IMSI_COUNT) into temp_imsi_count from DM_XH_IMSI where XH_NET_SEG=substr(strXHDataSeg,1,3);
    elsif temp_iflag=2 then
      select sum(XH_IMSI_COUNT) into temp_imsi_count from DM_XH_IMSI where XH_NET_SEG='TY';
    else
      retid := 8016;
      retmsg  := '携号网段表中网段[' || substr(strXHDataSeg,1,3) || ']的携号规则类型不存在。';
      return;
    end if;

    if temp_count<>temp_imsi_count then
    retid := 8040;
    retmsg  := '携号IMSI表与携号网段表的数量不匹配，请确认！';
    rollback;
    return;
    end if;

    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
      retid := sqlcode;
      retmsg  := sqlerrm;
      rollback;
  end XHDataAssign;

  procedure XHIMSIStartInfo(strIMSISeg in varchar2, strIMSIStart out varchar2, retid out number, retmsg out varchar2)
  is
    temp_netSeg varchar2(4) := null;
    temp_imsi_status number := -1;
    temp_imsi_count number := -1;
    temp_str varchar2(5) := null;
    temp_flag number := -1;
  begin

    select count(*) into temp_flag from DM_XH_IMSI where XH_IMSI_SEG=strIMSISeg;
    if temp_flag < 1 then
      retid := 8034;
      retmsg  := '携号IMSI段[' || strIMSISeg || ']信息不存存在！';
      return;
    end if;

    select XH_NET_SEG, XH_STATUS, XH_IMSI_COUNT into temp_netSeg, temp_imsi_status, temp_imsi_count from DM_XH_IMSI where XH_IMSI_SEG=strIMSISeg;

    if temp_imsi_status = 0 then
      strIMSIStart := strIMSISeg || '00000';
    elsif temp_imsi_status = 1 then
      temp_str := to_char(100000 - to_number(temp_imsi_count));
      temp_str := DM_PKG_UTILS.padding(temp_str,'0',5,'L');
    end if;


    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end XHIMSIStartInfo;


  procedure XHIMSISegGenerate(strXHNetSeg in varchar2, strIMSIseg out varchar2, retid out number, retmsg out varchar2)
  is
  begin
    if (substr(strXHNetSeg,1,2)= '13' and substr(strXHNetSeg,3,1) between '0' and '3') or
      (substr(strXHNetSeg,1,3)= '145') or
      (substr(strXHNetSeg,1,3)= '153') or
      (substr(strXHNetSeg,1,2)= '15' and substr(strXHNetSeg,3,1) between '5' and '6') or
      (substr(strXHNetSeg,1,3)= '186') or
      (substr(strXHNetSeg,1,3)= '189') then
      strIMSIseg:='46007' || '6' || '21' || 'X';
    elsif (substr(strXHNetSeg,1,4)= '1349') or
      (substr(strXHNetSeg,1,2)= '17' and substr(strXHNetSeg,3,1) between '6' and '7') or
      (substr(strXHNetSeg,1,2)= '18' and substr(strXHNetSeg,3,1) between '0' and '1') or
      (substr(strXHNetSeg,1,3)= '185') or
      (substr(strXHNetSeg,1,3)= '173') or
      (substr(strXHNetSeg,1,3)= '149') or
      (substr(strXHNetSeg,1,3)= '175') or
      (substr(strXHNetSeg,1,3)= '199') or
      (substr(strXHNetSeg,1,3)= '166') then
      strIMSIseg:='46007' || '6' || '21' || '0';
    elsif (substr(strXHNetSeg,1,3)= '191') then
      strIMSIseg:='46007' || '6' || '21' || '1';
    else
      retid := 8005;
      retmsg  := '输入的携号号段不正确，无法生成IMSI！';
      return;
    end if;

    if substr(strXHNetSeg,1,3) = '130' then strIMSIseg:=strIMSIseg || '0' ;
    elsif substr(strXHNetSeg,1,3) = '131' then strIMSIseg:=strIMSIseg || '1' ;
    elsif substr(strXHNetSeg,1,3) = '132' then strIMSIseg:=strIMSIseg || '2' ;
    elsif substr(strXHNetSeg,1,3) = '133' then strIMSIseg:=strIMSIseg || '3' ;
    elsif substr(strXHNetSeg,1,3) = '153' then strIMSIseg:=strIMSIseg || '4' ;
    elsif substr(strXHNetSeg,1,3) = '155' then strIMSIseg:=strIMSIseg || '5' ;
    elsif substr(strXHNetSeg,1,3) = '156' then strIMSIseg:=strIMSIseg || '6' ;
    elsif substr(strXHNetSeg,1,3) = '186' then strIMSIseg:=strIMSIseg || '7' ;
    elsif substr(strXHNetSeg,1,3) = '189' then strIMSIseg:=strIMSIseg || '8' ;
    elsif substr(strXHNetSeg,1,3) = '145' then strIMSIseg:=strIMSIseg || '9' ;
    elsif substr(strXHNetSeg,1,4) = '1349' then strIMSIseg:=strIMSIseg || '0' ;
    elsif substr(strXHNetSeg,1,3) = '176' then strIMSIseg:=strIMSIseg || '1' ;
    elsif substr(strXHNetSeg,1,3) = '177' then strIMSIseg:=strIMSIseg || '2' ;
    elsif substr(strXHNetSeg,1,3) = '180' then strIMSIseg:=strIMSIseg || '3' ;
    elsif substr(strXHNetSeg,1,3) = '181' then strIMSIseg:=strIMSIseg || '4' ;
    elsif substr(strXHNetSeg,1,3) = '185' then strIMSIseg:=strIMSIseg || '5' ;
    elsif substr(strXHNetSeg,1,3) = '173' then strIMSIseg:=strIMSIseg || '7' ;
    elsif substr(strXHNetSeg,1,3) = '149' then strIMSIseg:=strIMSIseg || '6' ;
    elsif substr(strXHNetSeg,1,3) = '175' then strIMSIseg:=strIMSIseg || '8' ;
    elsif substr(strXHNetSeg,1,3) = '199' then strIMSIseg:=strIMSIseg || '9' ;
    elsif substr(strXHNetSeg,1,3) = '166' then strIMSIseg:=strIMSIseg || '0' ;
    elsif substr(strXHNetSeg,1,3) = '191' then strIMSIseg:=strIMSIseg || '0' ;
    else
      retid  := 8006;
      retmsg  := '输入的携号号段不正确，无法生成IMSI！';
      return;
    end if;

    retid := 0;
    retmsg  := '成功!';
    exception
      when others then
        retid := sqlcode;
        retmsg := sqlerrm;
  end XHIMSISegGenerate;

  
  procedure XHICCIDSegGenerate(strXHDataSeg in varchar2, strFactoryName in varchar2, iCount in number, strICCIDStart out varchar2, strICCIDEnd out varchar2, retid out number, retmsg out varchar2)
  is
    temp_num number := -1;
    temp_factory_code varchar2(2) := null;
    temp_iccid_1_6 varchar2(6) := null;
    temp_iccid_7 varchar2(1) := null;
    temp_iccid_8 varchar2(1) := null;
    temp_iccid_9_10 varchar2(2) := null;
    temp_iccid_11_12 varchar2(2) := null;
    temp_iccid_13 varchar2(1) := null;
    temp_iccid_1_13 varchar2(13) := null;
    temp_iccid_14_20 varchar2(7) := null;
    temp_count number := 0;
  begin
    select count(*) into temp_num from DM_XH_NET_SEG where NET_SEG=substr(strXHDataSeg, 1, 3);
    if temp_num < 1 then
      retid := 8025;
      retmsg  := '携号网段表中不存在网段[' || substr(strXHDataSeg, 1, 3) || ']信息！';
      return;
    end if;

    select count(*) into temp_num from DM_FACTORY where FACTORY_NAME = strFactoryName;
    if temp_num < 1 then
      retid := 8010;
      retmsg  := '输入的卡商不存在！';
      return;
    end if;

      select FACTORY_CODE into temp_factory_code from DM_FACTORY where FACTORY_NAME = strFactoryName;
    if (substr(strXHDataSeg,1,2)= '13' and substr(strXHDataSeg,3,1) between '0' and '3') or
      (substr(strXHDataSeg,1,3)= '145') or
      (substr(strXHDataSeg,1,3)= '153') or
      (substr(strXHDataSeg,1,2)= '15' and substr(strXHDataSeg,3,1) between '5' and '6') or
      (substr(strXHDataSeg,1,3)= '186') or
      (substr(strXHDataSeg,1,3)= '189') then
      temp_iccid_1_6:= '898602';
    elsif (substr(strXHDataSeg,1,3)= '149') or
      (substr(strXHDataSeg,1,3)= '173') or
      (substr(strXHDataSeg,1,3)= '175') or
      (substr(strXHDataSeg,1,3)= '176') or
      (substr(strXHDataSeg,1,3)= '177') or
      (substr(strXHDataSeg,1,3)= '180') or
      (substr(strXHDataSeg,1,3)= '181') or
      (substr(strXHDataSeg,1,3)= '185') or
      (substr(strXHDataSeg,1,3)= '199') or
      (substr(strXHDataSeg,1,3)= '166') or
      (substr(strXHDataSeg,1,3)= '191') then
      temp_iccid_1_6:= '898607';
    else
      retid := 8011;
      retmsg  := '输入的号段不正确，无法生成ICCID！';
      return;
    end if;

    if substr(strXHDataSeg,1,3) = '130' then temp_iccid_7:='0' ;
    elsif substr(strXHDataSeg,1,3) = '131' then temp_iccid_7:='1' ;
    elsif substr(strXHDataSeg,1,3) = '132' then temp_iccid_7:='2' ;
    elsif substr(strXHDataSeg,1,3) = '133' then temp_iccid_7:='3' ;
    elsif substr(strXHDataSeg,1,3) = '153' then temp_iccid_7:='4' ;
    elsif substr(strXHDataSeg,1,3) = '155' then temp_iccid_7:='5' ;
    elsif substr(strXHDataSeg,1,3) = '156' then temp_iccid_7:='6' ;
    elsif substr(strXHDataSeg,1,3) = '186' then temp_iccid_7:='7' ;
    elsif substr(strXHDataSeg,1,3) = '189' then temp_iccid_7:='8' ;
    elsif substr(strXHDataSeg,1,3) = '145' then temp_iccid_7:='9' ;
    elsif substr(strXHDataSeg,1,3) = '149' then temp_iccid_7:='0' ;
    elsif substr(strXHDataSeg,1,3) = '173' then temp_iccid_7:='1' ;
    elsif substr(strXHDataSeg,1,3) = '175' then temp_iccid_7:='2' ;
    elsif substr(strXHDataSeg,1,3) = '176' then temp_iccid_7:='3' ;
    elsif substr(strXHDataSeg,1,3) = '177' then temp_iccid_7:='4' ;
    elsif substr(strXHDataSeg,1,3) = '180' then temp_iccid_7:='5' ;
    elsif substr(strXHDataSeg,1,3) = '181' then temp_iccid_7:='6' ;
    elsif substr(strXHDataSeg,1,3) = '185' then temp_iccid_7:='7' ;
    elsif substr(strXHDataSeg,1,3) = '199' then temp_iccid_7:='8' ;
    elsif substr(strXHDataSeg,1,3) = '166' then temp_iccid_7:='9' ;
    elsif substr(strXHDataSeg,1,3) = '191' then temp_iccid_7:='C' ;
    else
      retid := 8012;
      retmsg  := '输入的号段不正确，无法生成ICCID!';
      return;
    end if;

    temp_iccid_8 := substr(strXHDataSeg,4,1);

    temp_iccid_9_10 := '15';

    temp_iccid_11_12 := TO_CHAR(SYSDATE,'YY');

    temp_iccid_13 := temp_factory_code;

    temp_iccid_1_13 := temp_iccid_1_6 || temp_iccid_7 || temp_iccid_8 || temp_iccid_9_10 || temp_iccid_11_12 || temp_iccid_13;

    select max(SUBSTR(ICCID_END,14,7)) into temp_iccid_14_20 from DM_XH_BATCH where SUBSTR(ICCID_END,1,13) = temp_iccid_1_13;

    if temp_iccid_14_20 = '9999999' then
      retid := 8027;
      retmsg := 'ICCID已经达到最大值!';
      return;
    end if;

    if temp_iccid_14_20 is null then temp_iccid_14_20 := '0000000';
    else temp_iccid_14_20 := substr((to_number(temp_iccid_14_20) + 10000001),2,7);
    end if;

    temp_count := 9999999 - to_number(temp_iccid_14_20) + 1;
    if temp_count < iCount then
      retid := 8028;
      retmsg := '分配数量过界,当前可用数量为:' || temp_count;
      return;
    end if;

    strICCIDStart := temp_iccid_1_13 || temp_iccid_14_20;

    strICCIDEnd := temp_iccid_1_13 || substr(to_char(10000000 + to_number(substr(strICCIDStart,14,7)) + iCount - 1), 2, 7);

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg := sqlerrm;
  end XHICCIDSegGenerate;

end DM_PKG_XH_DATA_MNG;
//

delimiter ;//