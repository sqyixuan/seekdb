delimiter //;
create or replace package pdms.DM_PKG_DATA_ASSIGN is
  
  type cursorType is ref cursor;

  procedure GenerateBatchInfoByAttrId(strAttrId in number, strBatchNum out varchar2, strSmsp out varchar2, strRegionName out varchar2, retid out number, retmsg out varchar2);

  procedure DataAssign(strAttrId in number, strBatchNum in varchar2,strFactoryCode in varchar2,strFactoryName in varchar2,strCardName in varchar2,strDataType in varchar2,strMSISDNStart in varchar2,strMSISDNEnd in varchar2,strIMSIStart in varchar2,strIMSIEnd in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure GenerateICCID(strDataSeg in varchar2, strIMSI in varchar2, strFactoryCode in varchar2, iCount in number, strICCIDStart out varchar2, strICCIDEnd out varchar2,retid out number, retmsg out varchar2);

  procedure GetDataGroupCountList(results out cursorType,retid out number, retmsg out varchar2);

  procedure AutoGenerateCRMData(strAutoBatchNum in varchar2, strSMSP out varchar2,results out cursorType, retid out number, retmsg out varchar2);

  procedure AutoMendSelctBatchList(strOrderCode in varchar2,temp_batchNum in varchar2,results out cursorType,retid out number,retmsg out varchar2);

  procedure AutoMendDeleteBatchList(strAutoBatchNum in varchar2,temp_batchList in varchar2,strid in number,strmeg in varchar2,retid out number,retmsg out varchar2);

  procedure AutoMendDataBackUp(strAutoBatchNum in varchar2,tempBatchNum in varchar2,retid out number,retmsg out varchar2);

  procedure AutoMendBatchErrorLog(temp_logid in varchar2,retmsg in varchar2);

end DM_PKG_DATA_ASSIGN;
//
create or replace package body pdms.DM_PKG_DATA_ASSIGN is
  
  procedure GenerateBatchInfoByAttrId(strAttrId in number, strBatchNum out varchar2, strSmsp out varchar2, strRegionName out varchar2, retid out number, retmsg out varchar2)
  is
    temp_region_id number := -1;
    temp_region_short varchar2(5) := null;
    temp_date varchar2(6) :=  to_char(sysdate, 'YYMMDD');
    batchNum_1_8 varchar2(8) := null;
    batchNum_9_12 varchar2(4) := null;
    temp_num number := -1;
  begin
    -- 根据属性ID获取地市ID
    temp_region_id := DM_PKG_UTILS.getRegionIdByAttrId(strAttrId);
    if temp_region_id < 0 then
          retid  := 4005;
          retmsg  := '根据属性ID获取地市ID失败!';
          return;
    end if;

    -- 判断地市表中是否存在该地市信息
    select count(*) into temp_num from DM_REGION where REGION_ID=temp_region_id;
    if temp_num < 1 then
          retid  := 4006;
          retmsg  := '地市表中不存在ID为[' || temp_region_id || ']的地市信息！';
          return;
    end if;

    -- 查询地市缩写、短信中心、地市名称
    select REGION_SHORT, SMSP, REGION_NAME into temp_region_short, strSmsp, strRegionName from DM_REGION where REGION_ID=temp_region_id;

    -- 判断批次号子字符串是否正确
    batchNum_1_8 := temp_region_short || temp_date;
    if batchNum_1_8 is null or length(batchNum_1_8)<>8 then
      retid  := 4007;
          retmsg  := '批次号生成错误！';
          return;
    end if;

    -- 从批次表中查询匹配的批次号数量和最大值
    select COUNT(*), MAX(SUBSTR(BATCH_NUM,9,4)) into temp_num, batchNum_9_12 from DM_BATCH where BATCH_NUM like batchNum_1_8 || '%';

    -- 根据批次表中的数据生成新的批次号
    if batchNum_9_12 is null then
      batchNum_9_12 := '0001';
      strBatchNum := batchNum_1_8 || batchNum_9_12;
    elsif batchNum_9_12='9999' then
      retid  := 4008;
      retmsg  := '今天分配的次数已用完，请明天继续分配数据！';
      return;
    else
      batchNum_9_12 := substr(to_char(10000 + TO_NUMBER(batchNum_9_12) + 1),2,4);
      strBatchNum := batchNum_1_8 || batchNum_9_12;
    end if;

    retid  := 0;
    retmsg  := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid := sqlcode;
        retmsg := sqlerrm;
  end GenerateBatchInfoByAttrId;

  
  procedure DataAssign(strAttrId in number, strBatchNum in varchar2,strFactoryCode in varchar2,strFactoryName in varchar2,strCardName in varchar2,strDataType in varchar2,strMSISDNStart in varchar2,strMSISDNEnd in varchar2,strIMSIStart in varchar2,strIMSIEnd in varchar2,strICCIDStart in varchar2,strICCIDEnd in varchar2,iBatchCount in number,strNotes in varchar2, retid out number, retmsg out varchar2 )
  is
    temp_batchNum varchar2(12) := null;
    temp_smsp varchar2(14) := null;
    temp_region_name varchar2(20) := null;
    temp_attr_name varchar2(24) := null;
    temp_imsi_type varchar2(20) := null;
    temp_imsi_status number := -1;
    temp_imsi_count number := -1;
    temp_imsiSeg varchar2(100) := null;
    temp_iccid_start varchar2(20) := null;
    temp_iccid_end varchar2(20) := null;
    temp_count number := -1;
    temp_imsi_start varchar2(15) := null;
    temp_msisdn_start varchar2(11) := null;
    temp_flag number := -1;
    temp_states number := -1;
    founded boolean;
  begin
    -- 判断批次号是否正确,生成批次号和短信中心
    DM_PKG_DATA_ASSIGN.GenerateBatchInfoByAttrId(strAttrId,temp_batchNum,temp_smsp,temp_region_name,retid,retmsg);
    if retid <> 0 then
      -- 获取批次号失败，直接返回错误信息
      return;
    end if;
    if strBatchNum <> temp_batchNum then
      retid  := 4009;
      retmsg  := '输入的批次号不正确！';
      return;
    end if;

    -- 判断输入的卡商是否存在
    select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE = strFactoryCode;
    if temp_flag < 1 then
      retid  := 4010;
      retmsg  := '名称为[' || strFactoryName || ']的卡商信息不存在！';
      return;
    end if;

    -- 判断输入的卡类型是否存在
    select count(*) into temp_flag from DM_CARD_TYPE where CARD_NAME=strCardName;
    if temp_flag < 1 then
      retid  := 4011;
      retmsg  := '指定卡类型[' || strCardName || ']不存在！';
      return;
    end if;

    select ATTR_NAME into temp_attr_name from DM_HLR_LINK where ATTR_ID=strAttrId;

    -- 判断输入的起始和终止号段是否合法
    if length(strMSISDNStart)<> 11 or length(strMSISDNEnd)<> 11 or
      not regexp_like(substr(strMSISDNStart,8,4),'[0-9][0-9][0-9][0-9]') or
      not regexp_like(substr(strMSISDNEnd,8,4),'[0-9][0-9][0-9][0-9]')
    then
      retid  := 4032;
      retmsg  := '输入的起始或终止号段不合法！';
      return;
    end if;

    -- 判断起始和终止号段的前7位是否对应
    if substr(strMSISDNStart,1,7) <> substr(strMSISDNEnd,1,7) then
      retid  := 4030;
      retmsg  := '输入的起始和终止号段前7位不对应！';
      return;
    end if;

    -- 判断号段是否存在
    select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=substr(strMSISDNStart,1,7);
    if  temp_flag < 1 then
      retid  := 4031;
      retmsg  := '要分配的号段不存在。';
      return;
    end if;

    -- 判断输入的起始和终止IMSI段是否合法
    if length(strIMSIStart)<> 15 or length(strIMSIEnd)<> 15 or
      not regexp_like(substr(strIMSIStart,12,4),'[0-9][0-9][0-9][0-9]') or
      not regexp_like(substr(strIMSIEnd,12,4),'[0-9][0-9][0-9][0-9]')
    then
      retid  := 4013;
      retmsg  := '输入的起始或终止IMSI不合法！';
      return;
    end if;

    -- 判断起始和终止IMSI的前11位是否对应
    if substr(strIMSIStart,1,11) <> substr(strIMSIEnd,1,11) then
      retid  := 4014;
      retmsg  := '输入的起始和终止IMSI前11位不对应！';
      return;
    end if;

    -- [新PDMS]判断IMSI是否存在
    select count(*) into temp_flag from DM_IMSI where IMSI_SEG=substr(strIMSIStart,1,11);
    if  temp_flag < 1 then
      retid  := 4001;
      retmsg  := '要分配的IMSI段不存在。';
      return;
    end if;

    -- 判断输入的IMSI类型与分配类型是否一致
    select IMSI_TYPE into temp_imsi_type from DM_IMSI where IMSI_SEG=substr(strIMSIStart,1,11);
    if temp_imsi_type <> strDataType then
      retid  := 4015;
      retmsg  := '输入的分配类型与IMSI类型不一致！';
      return;
    end if;

    -- 判断号段与IMSI是否匹配
    DM_PKG_DATA_SEG.IMSISegGenerate(substr(strMSISDNStart, 1, 7), temp_imsiSeg, retid, retmsg);
    if substr(strMSISDNStart, 1, 3)='195' then
      DM_PKG_UTILS.foundIn(temp_imsiSeg,substr(strIMSIStart, 1, 11),founded);
      if founded=false then
        retid  := 4012;
        retmsg  := '输入的号段与IMSI不匹配！';
        return;
      end if;
    else
      if  temp_imsiSeg<>substr(strIMSIStart, 1,10) then
        retid  := 4012;
        retmsg  := '输入的号段与IMSI段不匹配！';
        return;
      end if;
    end if;

    -- 判断输入的IMSI段是否可用(0:未用，1：部分使用)
    select STATUS, IMSI_COUNT into temp_imsi_status, temp_imsi_count from DM_IMSI where IMSI_SEG=substr(strIMSIStart,1,11);
    if  temp_imsi_status<>0 and temp_imsi_status<>1 then
      retid  := 4016;
      retmsg  := '输入的IMSI段不可用！';
      return;
    end if;

    --ICCID的地市代码已经用完[新PDMS去掉]

    -- 判断输入的起始IMSI是否是该IMSI段最小值
    if  temp_imsi_count <> (10000 - to_number(substr(strIMSIStart,12,4))) then
      retid  := 4017;
      retmsg  := '输入的起始IMSI段不正确！';
      return;
    end if;

    -- 判断起始和终止ICCID的前13位是否一致
    if substr(strICCIDStart, 1, 13) <> substr(strICCIDEnd, 1, 13) then
      retid  := 4020;
      retmsg  := '输入的起始和终止ICCID的前13位不一致！';
      return;
    end if;

    -- 判断起始ICCID是否合法
    GenerateICCID(substr(strMSISDNStart, 1, 7), strIMSIStart, strFactoryCode, iBatchCount, temp_iccid_start, temp_iccid_end, retid, retmsg);
    if retid <> 0 then
      -- 直接返回错误信息
      return;
    end if;
    if temp_iccid_start <> strICCIDStart then
      retid  := 4018;
      retmsg  := '输入的起始ICCID不正确！';
      return;
    end if;
    -- 判断终止ICCID是否合法
    if temp_iccid_end <> strICCIDEnd then
      retid  := 4019;
      retmsg  := '输入的终止ICCID不正确！';
      return;
    end if;

    -- 判断输入的ICCID区间是否与批次表中号段区间重叠
    if substr(strICCIDStart, 13, 1) = 'F' then   --卡商为远程写卡
      select count(*) into temp_flag from DM_BATCH
        where substr(ICCID_START, 1, 14)=substr(strICCIDStart, 1, 14) and
        substr(ICCID_END, 15, 6)>=substr(strICCIDStart, 15, 6);
    else
      select count(*) into temp_flag from DM_BATCH
        where substr(ICCID_START, 1, 13)=substr(strICCIDStart, 1, 13) and
        substr(ICCID_END, 14, 7)>=substr(strICCIDStart, 14, 7);
    end if;
    if temp_flag > 0 then
      retid  := 4033;
      retmsg  := 'ICCID段重叠，请检查ICCID起始和终止值！';
      return;
    end if;

    temp_count := to_number(substr(strIMSIEnd, -4, 4)) - to_number(substr(strIMSIStart, -4, 4)) + 1;
    if temp_count <> iBatchCount then
      retid  := 4002;
      retmsg  := 'IMSI起止范围与批次数量不匹配！';
      return;
    end if;

    -- 判断ICCID起止范围与批次数量是否相等
    if substr(strICCIDEnd, 13, 1) = 'F' then
      temp_count := to_number(substr(strICCIDEnd, -6, 6)) - to_number(substr(strICCIDStart, -6, 6)) + 1;
    else
      temp_count := to_number(substr(strICCIDEnd, -7, 7)) - to_number(substr(strICCIDStart, -7, 7)) + 1;
    end if;

    if temp_count <> iBatchCount then
      retid  := 4003;
      retmsg := 'ICCID起止范围与批次数量不匹配！';
      return;
    end if;

    temp_count := to_number(substr(strIMSIEnd, 12,4)) - to_number(substr(strIMSIStart, 12,4)) + 1;
    if (to_number(substr(strIMSIStart, 12,4)) + temp_count) > 10000 then
      retid  := 4022;
      retmsg  := '输入的批次数量不正确！';
      return;
    end if;

    -- 判断输入的IMSI区间是否与IMSI表中IMSI区间重叠
    DM_PKG_DATA_SEG.IMSIStartInfo(substr(strIMSIStart, 1,11), temp_imsi_start, temp_msisdn_start, retid , retmsg);
    if temp_imsi_start <> strIMSIStart then
      retid  := 4023;
      retmsg  := '输入的IMSI区间不正确！';
      return;
    end if;

    -- 判断输入的IMSI区间是否与批次表中IMSI区间重叠
    select count(*) into temp_flag from DM_BATCH
      where substr(IMSI_START, 1, 11)=substr(strIMSIStart, 1, 11) and
          substr(IMSI_END, 12, 4)>=substr(strIMSIStart, 12, 4);
    if temp_flag > 0 then
      retid  := 4034;
      retmsg  := 'IMSI段重叠，请检查IMSI起始和终止值！';
      return;
    end if;

    -- 插入批次信息
    insert into DM_BATCH values(
          strBatchNum,   -- BATCH_NUM
          temp_region_name,   -- BATCH_REGION
          temp_smsp,    -- SMSP
          temp_attr_name,  -- HLR_NAME
          '未知',      -- BRAND_NAME,山东?
          strFactoryName,  -- FACTORY_NAME
          strCardName,  -- CARD_TYPE
          strMSISDNStart,  -- MSISDN_START
          strMSISDNEnd,  -- MSISDN_END
          strDataType,  -- DATA_TYPE
          iBatchCount,  -- BATCH_COUNT
          strIMSIStart,  -- IMSI_START
          strIMSIEnd,    -- IMSI_END
          strICCIDStart,  -- ICCID_START
          strICCIDEnd,  -- ICCID_END
          sysdate,    -- BATCH_TIME
          '0',      -- BATCH_STATUS
          '0',      -- PRODUCE_STATUS
          '0',      -- ORDER_CODE
          strNotes, -- NOTES
          strFactoryCode); -- FACTORY_CODE

    -- 更新号段表的剩余量和状态
    select MSISDN_COUNT into temp_count from DM_DATA_SEG where DATA_SEG=substr(strMSISDNStart,1,7);
    temp_count := temp_count - iBatchCount; --该批次执行后的剩余量
    if 0 < temp_count and temp_count <100000 then
      temp_states := DM_PKG_CONST.DATA_SEG_STATE_USED;
    elsif temp_count = 0 then
      temp_states := DM_PKG_CONST.DATA_SEG_STATE_ALL_USED;
    end if;
    update DM_DATA_SEG set MSISDN_COUNT=temp_count, DATA_SEG_STATE=temp_states where DATA_SEG=substr(strMSISDNStart,1,7);

    -- 更新IMSI表的剩余量和状态
    temp_count := temp_imsi_count-iBatchCount; --该批次执行后的剩余量
    if 0 < temp_count and temp_count <10000 then
      temp_states := DM_PKG_CONST.IMSI_STATE_USED;
    elsif temp_count = 0 then
      temp_states := DM_PKG_CONST.IMSI_STATE_ALL_USED;
    end if;
    update DM_IMSI set IMSI_COUNT=temp_count, STATUS=temp_states where IMSI_SEG=substr(strIMSIStart,1,11);

    commit;

    retid  := 0;
    retmsg  := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end DataAssign;

  
  procedure GenerateICCID(strDataSeg in varchar2, strIMSI in varchar2, strFactoryCode in varchar2, iCount in number, strICCIDStart out varchar2, strICCIDEnd out varchar2,retid out number, retmsg out varchar2)
  is
    temp_num number := -1;
    temp_imsi_10 varchar2(100) := null;
    temp_iccid_1_6 varchar2(6) := null;
    temp_iccid_7 varchar2(1) := null;
    temp_iccid_8 varchar2(1) := null;
    temp_iccid_9_10 varchar2(2) := null;
    temp_iccid_11_12 varchar2(2) := null;
    temp_iccid_13 varchar2(1) := null;
    temp_iccid_14_20 varchar2(7) := null;
   -- temp_factory_code varchar2(2) := null;
    temp_count number := 0;
    founded boolean;
  begin
    -- 判断号段数据是否存在
    select count(*) into temp_num from DM_DATA_SEG where DATA_SEG = strDataSeg;
    if temp_num < 1 then
      retid  := 4024;
      retmsg  := '输入的号段数据不存在！';
      return;
    end if;

    -- 判断IMSI段数据是否正确
    DM_PKG_DATA_SEG.IMSISegGenerate(strDataSeg, temp_imsi_10, retid, retmsg);
    if substr(strDataSeg, 1, 3)='195' then
      DM_PKG_UTILS.foundIn(temp_imsi_10,substr(strIMSI, 1, 11),founded);
      if founded=false then
        retid  := 4025;
        retmsg  := '输入的号段与IMSI不对应！';
        return;
      end if;
    else
      if temp_imsi_10 <> substr(strIMSI, 1, 10) then
        retid  := 4025;
        retmsg  := '输入的号段与IMSI不对应！';
        return;
      end if;
    end if;

    -- 判断卡商是否正确
    select count(*) into temp_num from DM_FACTORY where FACTORY_CODE = strFactoryCode;
    if temp_num < 1 then
      retid  := 4026;
      retmsg  := '输入的卡商不存在！';
      return;
    end if;
    
    if (substr(strDataSeg,1,2)= '13' and substr(strDataSeg,3,1) between '4' and '9') or
      (substr(strDataSeg,1,2)= '15' and substr(strDataSeg,3,1) between '0' and '2') or
      (substr(strDataSeg,1,2)= '15' and substr(strDataSeg,3,1) between '7' and '9') or
      (substr(strDataSeg,1,2)= '18' and substr(strDataSeg,3,1) between '7' and '8') or
      (substr(strDataSeg,1,3)= '182') or (substr(strDataSeg,1,3)= '147') then
        temp_iccid_1_6:= '898600';
    elsif (substr(strDataSeg,1,3)= '183') or
      (substr(strDataSeg,1,3)= '184') or
      (substr(strDataSeg,1,3)= '178') or
      -- (substr(strDataSeg,1,4)= '1705') then
      (substr(strDataSeg,1,3)= '170') or -- 20151204 modify
      (substr(strDataSeg,1,3)= '172') then -- 20170608 Add
        temp_iccid_1_6:= '898602';
    elsif (substr(strDataSeg,1,3)= '198') or -- 20171204 added by mhl
      (substr(strDataSeg,1,3)= '165') or
    (substr(strDataSeg,1,3)= '195') then -- 20180813 added by mhl
        temp_iccid_1_6:= '898607';
    else
      retid  := 4027;
      retmsg  := '输入的号段不正确！';
      return;
    end if;

    -- 生成ICCID第7位
    if substr(strDataSeg,1,2)= '13' then temp_iccid_7 := substr(strDataSeg,3,1);
    elsif substr(strDataSeg,1,3)= '159' then temp_iccid_7 := '0';
    elsif substr(strDataSeg,1,3)= '158' then temp_iccid_7 := '1';
    elsif substr(strDataSeg,1,3)= '150' then temp_iccid_7 := '2';
    elsif substr(strDataSeg,1,3)= '151' then temp_iccid_7 := '3';
    elsif substr(strDataSeg,1,3)= '157' then temp_iccid_7 := 'A';
    elsif substr(strDataSeg,1,3)= '188' then temp_iccid_7 := 'B';
    elsif substr(strDataSeg,1,3)= '152' then temp_iccid_7 := 'C';
    elsif substr(strDataSeg,1,3)= '147' then temp_iccid_7 := 'D';
    elsif substr(strDataSeg,1,3)= '187' then temp_iccid_7 := 'E';
    elsif substr(strDataSeg,1,3)= '182' then temp_iccid_7 := 'F';
    elsif substr(strDataSeg,1,3)= '183' then temp_iccid_7 := 'A';
    elsif substr(strDataSeg,1,3)= '184' then temp_iccid_7 := 'C';
    elsif substr(strDataSeg,1,3)= '178' then temp_iccid_7 := 'D';
    -- elsif substr(strDataSeg,1,4)= '1705' then temp_iccid_7 := 'E';
    elsif substr(strDataSeg,1,3)= '170' then temp_iccid_7 := 'E'; -- 20151204 modify
    elsif substr(strDataSeg,1,3)= '172' then temp_iccid_7 := 'F'; -- 20170608 Add
    elsif substr(strDataSeg,1,3)= '198' then temp_iccid_7 := 'A'; -- 20171204 added by mhl
    elsif substr(strDataSeg,1,3)= '165' then temp_iccid_7 := 'D'; -- 20180813 added by mhl
    elsif substr(strDataSeg,1,3)= '195' then temp_iccid_7 := 'E';
    else
      retid  := 4027;
      retmsg  := '输入的号段不正确!';
      return;
    end if;

    -- 生成ICCID第8位
    temp_iccid_8 := substr(strDataSeg,4,1);

    -- 生成ICCID第9-10位,省代码
    temp_iccid_9_10 := '15';

    -- 生成ICCID第11-12位,年份(有些地区可能需要修改时间，暂不支持此需求)
    temp_iccid_11_12 := TO_CHAR(SYSDATE,'YY');

    -- 生成ICCID第13位,卡商代码
    temp_iccid_13 := strFactoryCode;

    --首先取得当前年份ICCID的后7位的最大值,作为新的iccid的起始值
    select max(SUBSTR(ICCID_END,14,7)) into temp_iccid_14_20 from DM_BATCH
    where SUBSTR(ICCID_END,1,13) = temp_iccid_1_6 || temp_iccid_7 || temp_iccid_8 || temp_iccid_9_10 || temp_iccid_11_12 || temp_iccid_13;
    if temp_iccid_13 = 'F' then   --卡商为远程写卡
      --如果temp_iccid_14_20为F999999时，已经达到最大值，报错
      if temp_iccid_14_20 = 'F999999' then
        retid := 4028;
        retmsg := 'ICCID已经达到最大值!';
        return;
      end if;
      --如果temp_iccid_14_20为空时，设置新的ICCID从0000000开始,否则加1开始
      if temp_iccid_14_20 is null then temp_iccid_14_20 := 'A000000';
      elsif temp_iccid_14_20 = 'A999999' then temp_iccid_14_20 := 'B000000';
      elsif temp_iccid_14_20 = 'B999999' then temp_iccid_14_20 := 'C000000';
      elsif temp_iccid_14_20 = 'C999999' then temp_iccid_14_20 := 'D000000';
      elsif temp_iccid_14_20 = 'D999999' then temp_iccid_14_20 := 'E000000';
      elsif temp_iccid_14_20 = 'E999999' then temp_iccid_14_20 := 'F000000';
      else temp_iccid_14_20 := substr(temp_iccid_14_20,1,1) || substr(to_char(to_number(substr(temp_iccid_14_20,2,6)) + 1000001),2,6);
      end if;
      --判断输入的数量是否过界
      temp_count := 999999 - to_number(substr(temp_iccid_14_20,2,6)) + 1;
      if temp_count < iCount then
        retid := 4029;
        retmsg := '分配数量过界,当前可用数量为:' || temp_count;
        return;
      end if;
      -- 组装起始ICCID
      strICCIDStart := temp_iccid_1_6 || temp_iccid_7 || temp_iccid_8 || temp_iccid_9_10 || temp_iccid_11_12 || temp_iccid_13 || temp_iccid_14_20;
      -- 组装终止ICCID
      strICCIDEnd := substr(strICCIDStart,1,14) || substr(to_char(1000000 + to_number(substr(strICCIDStart,15,6)) + iCount - 1), 2, 6);

    else  --卡商为非远程写卡
      --如果temp_iccid_14_20为9999999时，已经达到最大值，抱错
      if temp_iccid_14_20 = '9999999' then
        retid := 4028;
        retmsg := 'ICCID已经达到最大值!';
        return;
      end if;
     
      if temp_iccid_14_20 is null then temp_iccid_14_20 := '0000000';
      else temp_iccid_14_20 := substr((to_number(temp_iccid_14_20) + 10000001),2,7);
      end if;
      --判断输入的数量是否过界
      temp_count := 9999999 - to_number(temp_iccid_14_20) + 1;
      if temp_count < iCount then
        retid := 4029;
        retmsg := '分配数量过界,当前可用数量为:' || temp_count;
        return;
      end if;
      -- 组装起始ICCID
      strICCIDStart := temp_iccid_1_6 || temp_iccid_7 || temp_iccid_8 || temp_iccid_9_10 || temp_iccid_11_12 || temp_iccid_13 || temp_iccid_14_20;
      -- 组装终止ICCID
      strICCIDEnd := substr(strICCIDStart,1,13) || substr(to_char(10000000 + to_number(substr(strICCIDStart,14,7)) + iCount - 1), 2, 7);

    end if;

    retid  := 0;
    retmsg := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid  := sqlcode;
        retmsg := sqlerrm;
  end GenerateICCID;


 -----------------------------------------------------------------------------------------
  -- 名称: GetDataGroupCountList
  -- 功能: 通过监控CRM系统余量，获取当天需要生成数据的号群列表
  -- 调用: { DM_PKG_DATA_ASSIGN.GetDataGroupCountList(?,?,?,?,?,?,?,?) }
  -- 参数:
  --  results    out  cursorType    需要处理的号群列表
  --  retid      out  number        成功/失败信息ID
  --  retmsg     out  varchar2      成功/失败信息描述
  -- 返回值:
  --  retid      := 0;
  --  retmsg      := '成功。';
  -----------------------------------------------------------------------------------------
  procedure GetDataGroupCountList(results out cursorType,retid out number, retmsg out varchar2)
  is
    temp_date varchar2(6) :=  to_char(sysdate, 'YYMMDD');
    temp_flag         number := 0;
  begin

    -- 数据本地化，更新当天批次ATTR ID
    update dm_resource_warning drw set drw.attr_id=
    (
     select k.attr_id from dm_hlr_link l,dm_hlr_link k,dm_region r
     where r.region_name=l.attr_name
     and l.attr_id=k.parent_id and drw.region_code=r.region_code and drw.net_seg=k.attr_name
    )
    where to_char(drw.data_time, 'yymmdd')=temp_date and drw.STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0
    and drw.make_data_type=DM_PKG_CONST.AUTO_MAKE_DATATYPE0;
    commit;

    -- 检查数据本地化数据
    update dm_resource_warning t set t.status=DM_PKG_CONST.AUTO_RESOURCE_STATE2, t.notes='本地化数据时，无匹配数据。'
      where t.attr_id is null and to_char(t.data_time, 'yymmdd')=temp_date and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0;
    commit;

    -- 检查重复数据（region_code+net_seg），只保留一条，其他的更新状态和备注信息
    update dm_resource_warning t set t.status=DM_PKG_CONST.AUTO_RESOURCE_STATE2, t.notes='补充批次有重复数据'
    WHERE (region_code,net_seg) IN ( SELECT region_code,net_seg FROM dm_resource_warning where to_char(data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0  GROUP BY region_code,net_seg HAVING COUNT(*) > 1)
    AND ROWID NOT IN (SELECT MIN(ROWID) FROM dm_resource_warning where to_char(data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0 GROUP BY hlr_code HAVING COUNT(*) > 1)
    and to_char(t.data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0;
    commit;

    -- Tag1:每天处理批次数量不能超过上限AUTO_ASSIGN_LIMIT_MAX=900 start
    select count(*) into temp_flag from dm_resource_warning t WHERE to_char(t.data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and t.STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0;

    if temp_flag>DM_PKG_CONST.AUTO_RESOURCE_MAX then
      -- 超过900条时，更新多出的数据状态和备注信息
      update dm_resource_warning t set t.status=DM_PKG_CONST.AUTO_RESOURCE_STATE2, t.notes='超过每日处理批次量上限[' || DM_PKG_CONST.AUTO_RESOURCE_MAX || ']条。'
        where (region_code,net_seg) in(
          select region_code,net_seg from (
            select m.*,ROWNUM NUM from (
              select * from dm_resource_warning t
                where to_char(t.data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0 order by t.data_count,t.region_code,t.net_seg
                ) m
          ) where num > 900
        ) and to_char(t.data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0;
      commit;
      -- 剩下数据全部是待补充数据
    end if;

    -- 查询号段余量信息，按地市、3位号段排序
    open results for
         select data_num,region_code,hlr_code,attr_id,net_seg,data_count, to_char(data_time, 'yyyy-mm-dd hh24:mi:ss') data_time,make_data_count,make_data_type,status,notes
         from dm_resource_warning
         where to_char(data_time, 'yymmdd')=to_char(sysdate, 'YYMMDD') and STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE0
         order by region_code,net_seg;

    retid  := 0;
    retmsg := '成功!';

    -- 系统异常处理
    exception
      when others then
        retid  := sqlcode;
        retmsg := sqlerrm;
  end GetDataGroupCountList;

-----------------------------------------------------------------------------------------
  -- 名称: AutoGenerateCRMData
  -- 功能: 根据AttrID和3位号段自动生成资源补充
  -- 规则：按通补规则找同一HLR下的3位号段相同的号段，并且要求自动生成数据标记位为"允许"，从剩余量少的号段开始使用
  -- 调用: { DM_PKG_DATA_ASSIGN.AutoGenerateCRMData(?,?,?,?,?,?,?,?) }
  -- 参数:
  --  strAutoBatchNum  in varchar2  批次号
  --  results    out  cursorType    写卡数据列表
  --  retid      out  number        成功/失败信息ID
  --  retmsg     out  varchar2      成功/失败信息描述
  -- 返回值:
  --  retid      := 0;
  --  retmsg     := '成功。';
  -----------------------------------------------------------------------------------------
  procedure AutoGenerateCRMData(strAutoBatchNum in varchar2, strSMSP out varchar2,results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_cursor       cursorType := null;
    temp_attrId       number := 0;
    temp_net_seg      VARCHAR2(7) :='';
    temp_data_seg     VARCHAR2(7) :='';
    temp_imsi_count   number := 0;
    temp_batchNum     VARCHAR2(12) :='';
    temp_regionName   VARCHAR2(20) :='';
    --temp_hlrName      VARCHAR2(50) :='';
    temp_iCount       number := 0;
    temp_assign_count   number := 0;            --分配数量
    temp_msisdn_count number := 0;              --分配IMSI总数据量
    temp_imsi_seg     VARCHAR2(11) :='';         --分配IMSI 11位段
    --temp_assign_cyc_count number := 0;          --分配的回收写卡数量
    --temp_xk_batchType    VARCHAR2(4) := '';     --分配的回收写卡数量
    temp_assign_1_count number := 0;            --单个批次分配数量
    temp_assign_batch_count number := 0;        --批次分配数量相加
    temp_assign_factorycode VARCHAR2(20) := '';   --分配卡商
    temp_assign_Factoryname VARCHAR2(20) := '';
    temp_assign_cardType  VARCHAR2(20) := '';   --分配卡类型
    temp_batchType    VARCHAR2(20) := '';       --分配批次类型
    temp_ImsiType     VARCHAR2(20) := '';       --分配IMSI类型
    temp_msisdnStart  VARCHAR2(11) := '';
    temp_msisdnEnd    VARCHAR2(11) := '';
    temp_imsiStart    VARCHAR2(15) := '';
    temp_imsiEnd      VARCHAR2(15) := '';
    temp_iccidStart   VARCHAR2(20) := '';
    temp_iccidEnd     VARCHAR2(20) := '';
    temp_last4        VARCHAR2(4) := '';
    temp_iccid_last5  VARCHAR2(5) := '';
    temp_orderType    VARCHAR2(4) := '';
    temp_batchNums    VARCHAR2(32767) := '';
    temp_orderNum     VARCHAR2(12) := '';
    temp_imsi         VARCHAR2(15) :='';
    temp_iccid        VARCHAR2(20) :='';
    temp_msg          varchar2(1024) := ' ';
    temp_flag         number := 0;
    temp_i            number := 0;
    temp_str_sql      VARCHAR2(2048) :='';
    temp_log_id       number := 0;
  begin
    temp_log_id:=seq_dm_log.nextval;
    -- 获取自动补充写卡数据批次信息   --retid 为异常编码，请勿修改
    select count(*) into temp_flag from dm_resource_warning t where t.data_num = strAutoBatchNum;
    if temp_flag < 1 then
      retid := 4040;
      retmsg  := '自动生成资源补充批次[' || strAutoBatchNum || ']信息不存在！';
      AutoMendBatchErrorLog(temp_log_id, '[''||retid||'']'||retmsg);
      commit;
      return;
    end if;

    select t.attr_id,t.net_seg into temp_attrId,temp_net_seg from dm_resource_warning t where t.data_num=strAutoBatchNum;

    -- 更新批次状态--正在生成
    update dm_resource_warning set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE3 where data_num = strAutoBatchNum;
    commit;

    SELECT T.CONFIG_VALUE into temp_assign_count FROM dm_sys_config t where t.config_key=DM_PKG_CONST.AUTO_ASSIGN_COUNT;
    SELECT T.CONFIG_VALUE into temp_assign_factorycode FROM dm_sys_config t where t.config_key=DM_PKG_CONST.AUTO_ASSIGN_FACTORY;
    SELECT T.CONFIG_VALUE into temp_assign_cardType FROM dm_sys_config t where t.config_key=DM_PKG_CONST.AUTO_ASSIGN_CARD_TYPE;
    SELECT T.CONFIG_VALUE into temp_ImsiType FROM dm_sys_config t where t.config_key=DM_PKG_CONST.AUTO_IMSI_TYPE;
    SELECT T.CONFIG_VALUE into temp_batchType FROM dm_sys_config t where t.config_key=DM_PKG_CONST.AUTO_BATCH_TYPE;
    temp_orderType:=DM_PKG_CONST.AUTO_ORDER_TYPE6;
    -- 获取卡商编码信息
    select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE = temp_assign_factorycode;
    if temp_flag < 1 then
      retid := 4041;
      retmsg  := '自动资源补充卡商编码[' || temp_assign_factorycode || ']信息不存在！';
      update dm_resource_warning set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE2,NOTES=retmsg where data_num=strAutoBatchNum;
      commit;
      return;
    end if;

    select FACTORY_NAME into temp_assign_factoryname from DM_FACTORY where FACTORY_CODE = temp_assign_factorycode;

    -- 获取卡类编码信息
    select count(*) into temp_flag from DM_CARD_TYPE where CARD_NAME = temp_assign_cardType;
    if temp_flag < 1 then
      retid := 4042;
      retmsg  := '自动生成资源补充卡类型[' || temp_assign_cardType || ']信息不存在！';
      update dm_resource_warning set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE2,NOTES=retmsg where DATA_NUM=strAutoBatchNum;
      commit;
      return;
    end if;

    -- 查询指定attrid下 netseg的号段数据，进行分配N个批次
    temp_str_sql := ' select x.*, ROWNUM NUM from(';
    temp_str_sql := temp_str_sql || 'select d.data_seg,d.msisdn_count,i.imsi_seg,i.imsi_count, sum(i.imsi_count) over(order by d.msisdn_count, i.imsi_count,i.imsi_seg) as iCount ';
    temp_str_sql := temp_str_sql || 'from dm_data_seg d, dm_imsi i where d.attr_id='||temp_attrId;
    temp_str_sql := temp_str_sql || ' and substr(d.data_seg,1,3)='||temp_net_seg||'and i.imsi_type ='||temp_ImsiType;
    temp_str_sql := temp_str_sql || ' and d.data_seg_state<>2 and d.data_seg_state<>4 and d.auto_make_data_seg_tab=1 ';
    temp_str_sql := temp_str_sql || ' and d.data_seg=i.data_seg and i.status<>2 order by d.msisdn_count, i.imsi_count ASC) x ';
    temp_str_sql := temp_str_sql || ' where x.iCount<=(20000)';

    open temp_cursor for temp_str_sql;

    -- 遍历生成批次
    <<batchCycles>>
    if  temp_cursor %isopen then
      loop      --temp_imsi_seg
        fetch temp_cursor into temp_data_seg,temp_msisdn_count,temp_imsi_seg,temp_imsi_count,temp_iCount,temp_i;
              exit when temp_cursor %notfound;
        -- 计算本批次分配数量
        if (temp_assign_count - temp_assign_batch_count) < temp_imsi_count then
          temp_assign_1_count := (temp_assign_count - temp_assign_batch_count);
        else
          temp_assign_1_count := temp_imsi_count;
        end if;

        if temp_assign_1_count<0 or temp_assign_1_count>10000 then
          retid := 4455;
          retmsg  :=strAutoBatchNum ||':数量不正确[' || temp_assign_1_count ;
          update dm_resource_warning set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE2,NOTES=retmsg where DATA_NUM=strAutoBatchNum;
          commit;
          -- 删除批次信息
          AutoMendDeleteBatchList(strAutoBatchNum,temp_batchNums,4045,temp_msg,retid,retmsg);
          return;
        end if;
        -- 根据属性ID获取批次号和地市名称、短消息中心
        GenerateBatchInfoByAttrId(temp_attrId, temp_batchNum, strSMSP, temp_regionName, retid, retmsg);
          if retid <> 0 then
            retmsg := '序列['||strAutoBatchNum||']生成批次号：'||retmsg;
            AutoMendBatchErrorLog(temp_log_id, '['||retid||']'||retmsg);
            goto batchCycles;
          end if;
          -- IMSI和号段起始段
          temp_last4 := substr(to_char(10000 + 10000 - temp_imsi_count),2,4);
          temp_msisdnStart := temp_data_seg || '0000';
          temp_imsiStart := temp_imsi_seg || temp_last4;

          -- IMSI和号段终止段
          temp_last4 := substr(to_char(10000 + 10000 - temp_imsi_count + temp_assign_1_count - 1),2,4);
          temp_msisdnEnd := temp_data_seg ||  '0000';
          temp_imsiEnd := temp_imsi_seg || temp_last4;

          GenerateICCID(temp_data_seg, temp_imsiStart, temp_assign_factorycode, temp_assign_1_count, temp_iccidStart, temp_iccidEnd, retid, retmsg);
          if retid <> 0 then
            retmsg := '序列['||strAutoBatchNum||']生成ICCID：'||retmsg;
            AutoMendBatchErrorLog(temp_log_id, '['||retid||']'||retmsg);
            goto batchCycles;
          end if;

          DataAssign(temp_attrId,temp_batchNum,temp_assign_factorycode,temp_assign_factoryname,temp_assign_cardType,temp_ImsiType,temp_msisdnStart,temp_msisdnEnd,temp_imsiStart,temp_imsiEnd,temp_iccidStart,temp_iccidEnd,temp_assign_1_count,strAutoBatchNum,retid, retmsg);
          if retid <> 0 then
            retmsg := '序列['||strAutoBatchNum||']数据分配：'||retmsg;
            AutoMendBatchErrorLog(temp_log_id, '['||retid||']'||retmsg);
            commit;
            goto batchCycles;
          end if;

          AutoMendBatchErrorLog(temp_log_id, '[0]:序列 ['||strAutoBatchNum||']成功生成资源补充批次['||temp_batchNum||']');

          temp_assign_batch_count := temp_assign_batch_count + temp_assign_1_count; -- 每个批次分配的数量相加
          temp_batchNums := temp_batchNums ||',' ||temp_batchNum ;
          IF temp_assign_batch_count = temp_assign_count THEN
            EXIT;
          END IF;
      end loop;
    end if;


    -- 检查数量和批次列表  retid 为异常编码，请勿修改
    if temp_assign_batch_count=0 or temp_batchNums is null then
      retid  := 4044;
      retmsg := '[' || strAutoBatchNum ||']下没有可生成的数据';
      update DM_RESOURCE_WARNING set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE2,NOTES=retmsg,MAKE_DATA_COUNT=0 where DATA_NUM=strAutoBatchNum;
      commit;
      return;
    end if;

    /*在数据不足情况下，temp_assign_batch_count比默认分配数量小*/
    -- 生成订单号
    temp_orderNum:='330100206100623201';

    -- 循环生成数据
    open temp_cursor for select b.batch_num,b.imsi_start,b.imsi_end,b.iccid_start,b.imsi_end,b.batch_count from dm_order o, dm_batch b
      where o.order_code=temp_orderNum and o.order_code=b.order_code;
    if temp_cursor %isopen then
      loop
        fetch temp_cursor into temp_batchNum,temp_imsiStart,temp_imsiEnd,temp_iccidStart,temp_iccidEnd,temp_assign_1_count;
        exit when temp_cursor %notfound;
        temp_flag := 0;
        temp_last4 := substr(temp_imsiStart, 12, 4);
        temp_iccid_last5 := substr(temp_iccidStart, 17, 4);
        -- 生成数据
        while temp_assign_1_count > temp_flag loop
          -- IMSI和号段起始段
          --temp_imsiStart := temp_data_seg || temp_last4;
          temp_imsi := substr(temp_imsiStart, 1, 11) || substr(to_char(10000 + to_number(temp_last4) + temp_flag),2,4);
          temp_iccid := substr(temp_iccidStart, 1, 16) || substr(to_char(10000 + to_number(temp_iccid_last5) + temp_flag),2,4);

          insert into dm_resource_data_detail (ORDER_CODE, BATCH_NUM, IMSI, ICCID) values (temp_orderNum, temp_batchNum,temp_imsi, temp_iccid);
          commit;
          temp_flag := temp_flag + 1;
        end loop;
      end loop;
    end if;
    --关闭游标
    close temp_cursor;

    open results for select order_code,BATCH_NUM from DM_BATCH where ORDER_CODE=temp_orderNum;
    update dm_resource_warning
           set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE2,
               MAKE_DATA_COUNT=temp_assign_batch_count,
               notes= '生成资源补充订单'||temp_orderNum||'成功'
               where data_num=strAutoBatchNum;
    commit;

    retid  :=0;
    retmsg := '成功!';
    -- 系统异常处理
    exception
      when others then
        rollback;
        retid  := sqlcode;
        retmsg := sqlerrm;
        -- 更新状态
        if strAutoBatchNum <> '' then
           update DM_RESOURCE_WARNING set STATUS=DM_PKG_CONST.AUTO_RESOURCE_STATE2,NOTES=retmsg where DATA_NUM=strAutoBatchNum;
           commit;
        end if;
        if strAutoBatchNum is not null or strAutoBatchNum <> ''then
          -- 删除批次信息
          AutoMendDeleteBatchList(strAutoBatchNum,temp_batchNums,4046,temp_msg,retid,retmsg);
      return;
        end if;
  end AutoGenerateCRMData;

  -----------------------------------------------------------------------------------------
  -- 名称: AutoMendDeleteBatchList
  -- 功能: 批量删除同步批次
  -- 调用: { DM_PKG_DATA_ASSIGN.AutoMendDeleteBatchList(?,?,?,?,?,?,?,?) }
  -- 参数:
  --  strAutoBatchNum    in  varchar2   余量批次
  --  temp_batchList     in  varchar2   生成的批次
  --  strid              in  number
  --  strmeg             in  varchar2
  -- 返回值:
  --  retid     := 0;
  --  retmsg      := '成功。';
  -----------------------------------------------------------------------------------------
  procedure AutoMendSelctBatchList(strOrderCode in varchar2,temp_batchNum in varchar2,results out cursorType,retid out number,retmsg out varchar2)
  is
    temp_flag      number      := 0;
  begin

    select count(*) into temp_flag from dm_resource_data_detail where ORDER_CODE=strOrderCode and batch_num=temp_batchNum;
    if temp_flag < 0 then
       retid  := 4070;
       retmsg := '未找到订单'||strOrderCode||'批次数据：'||temp_batchNum;
    end if;

    open results for select order_code,BATCH_NUM,IMSI,ICCID from dm_resource_data_detail
                      where ORDER_CODE=strOrderCode and batch_num=temp_batchNum
                      order by IMSI,BATCH_NUM;

    retid := 0;
    retmsg  := '成功。';

    -- 系统异常处理
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end AutoMendSelctBatchList;

  -----------------------------------------------------------------------------------------
  -- 名称: AutoMendDeleteBatchList
  -- 功能: 批量删除同步批次
  -- 调用: { DM_PKG_DATA_ASSIGN.AutoMendDeleteBatchList(?,?,?,?,?,?,?,?) }
  -- 参数:
  --  strAutoBatchNum    in  varchar2   余量批次
  --  temp_batchList     in  varchar2   生成的批次
  --  strid              in  number
  --  strmeg             in  varchar2
  -- 返回值:
  --  retid     := 0;
  --  retmsg      := '成功。';
  -----------------------------------------------------------------------------------------
  procedure AutoMendDeleteBatchList(strAutoBatchNum in varchar2,temp_batchList in varchar2,strid in number,strmeg in varchar2,retid out number,retmsg out varchar2)
  is
    temp_batchNums     VARCHAR2(32767) := '';
    temp_strBatchNum   VARCHAR2(20) := null;
    temp_next          VARCHAR2(32767) := ' ';
    temp_log_id        number:=0;
  begin
      temp_batchNums     := temp_batchList;           --批次
      while (temp_next is not null) loop
            DM_PKG_UTILS.tokenizer(1, ',', temp_batchNums, temp_strBatchNum, temp_next);
            temp_batchNums := temp_next;
            if retid <> 0 then
              temp_log_id:=seq_dm_log.nextval;
              retid  := strid;
              retmsg := strmeg||retmsg;
              AutoMendBatchErrorLog(temp_log_id,'['||strid||']'||retmsg);
              commit;
              return;
            end if;
      end loop;
    retid := 0;
    retmsg  := '成功。';

    -- 系统异常处理
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end AutoMendDeleteBatchList;

  -----------------------------------------------------------------------------------------
  -- 名称: AutoMendDeleteBatchList
  -- 功能: 同步批次数据至备份表
  -- 调用: { DM_PKG_DATA_ASSIGN.AutoMendDeleteBatchList(?,?,?,?,?,?,?,?) }
  -- 参数:
  --  strAutoBatchNum    in  varchar2   余量批次
  --  temp_batchList     in  varchar2   生成的批次
  --  strid              in  number
  --  strmeg             in  varchar2
  -- 返回值:
  --  retid     := 0;
  --  retmsg      := '成功。';
  -----------------------------------------------------------------------------------------
  procedure AutoMendDataBackUp(strAutoBatchNum in varchar2,tempBatchNum in varchar2,retid out number,retmsg out varchar2)
  is
      temp_flag      number      := 0;
  begin
       select count(*) into temp_flag from dm_resource_warning t where t.data_num=strAutoBatchNum ;
      if temp_flag > 0 then
            insert into dm_resource_data_backup(order_code,batch_num,imsi,iccid,ki,opc) select t.order_code,t.batch_num,t.imsi,t.iccid,t.ki,t.opc from dm_resource_data_detail t
            where batch_num=tempBatchNum;

            delete from dm_resource_data_detail where batch_num=tempBatchNum;
            commit;
      else
           retid := 4045;
           retmsg  := '未找到批次【'||strAutoBatchNum||'】数据信息!';
      END IF;
    retid := 0;
    retmsg  := '成功。';

    -- 系统异常处理
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end AutoMendDataBackUp;


  -----------------------------------------------------------------------------------------
  -- 名称: AutoMendDeleteBatchList
  -- 功能: 记录自动写卡资源日志
  -- 调用: { DM_PKG_DATA_ASSIGN.AutoMendDeleteBatchList(?,?,?,?,?,?,?,?) }
  -- 参数:
  --  strAutoBatchNum    in  varchar2   余量批次
  --  temp_batchList     in  varchar2   生成的批次
  --  strid              in  number
  --  strmeg             in  varchar2
  -- 返回值:
  --  retid     := 0;
  --  retmsg      := '成功。';
  -----------------------------------------------------------------------------------------
  procedure AutoMendBatchErrorLog(temp_logid in varchar2,retmsg in varchar2)
  is
    temp_flag         number := 0;
    temp_log_id       number := 0;
  begin
      if temp_logid is null or temp_logid = '' then
         temp_log_id:=seq_dm_log.nextval;
       else
         temp_log_id:=temp_logid;
      end if;

      select count(*) into temp_flag from dm_log t where t.log_id=temp_logid ;
      if temp_flag > 0 then
         update dm_log set log_time = sysdate, log_content=retmsg where log_id =temp_logid ;
         commit;
      else
         insert into dm_log(log_id,user_name,log_type,log_ip,log_content,log_time)
         values(temp_log_id,'admin',DM_PKG_CONST.AUTO_LOG_TYPE,'127.0.0.1',retmsg,sysdate);
         commit;
      end if;

  end AutoMendBatchErrorLog;

end DM_PKG_DATA_ASSIGN;
//

delimiter ;//