delimiter //;
create or replace package pdms.DM_PKG_BLANK_CARD_MNG is
  
  type cursorType is ref cursor;

  procedure GenerateEnhancedKi(strEKi out varchar2, retid out number, retmsg out varchar2);

  procedure CardSNGenerate(strFactoryName in varchar2, strCardName in varchar2, strPresetFlag in varchar2, strSingleFlag in varchar2,strSIMFlag in varchar2, strSWPFlag in varchar2, strM2MFlag in varchar2, strCardSN out varchar2, retid out number, retmsg out varchar2);

  procedure SeIdGenerate(strFactoryName in varchar2, seCode in varchar2, seIdStart out varchar2, retid out number, retmsg out varchar2);

  procedure BlankCardBacthGenerate(strBatchNum out varchar2, retid out number, retmsg out varchar2);

  procedure BlankCardBacthAdd(strBatchNum in varchar2,strFactoryCode in varchar2, strFactoryName in varchar2, strCardName in varchar2, strPresetFlag in varchar2, strSingleFlag in varchar2,strSIMFlag in varchar2, strSWPFlag in varchar2, strM2MFlag in varchar2, iCount in number, strAllot in varchar2, strNotes in varchar2, seId in varchar2, retid out number, retmsg out varchar2);

  procedure BlankCardBacthList(strCardName in varchar2, strFactoryCode in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure BlankCardStateModify(strBatchNum in varchar2, iStatus in number, retid out number, retmsg out varchar2);

  procedure BlankCardBacthDel(strBatchNum in varchar2, retid out number, retmsg out varchar2);

  procedure BlankCardBacthInfo(iType in number, strIDVal in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure CardSNImport(strBatchNum in varchar2, retid out number, retmsg out varchar2);

  procedure BlankCardDetailList(strBatchNum in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure BlankCardAllotList(strBatchNum in varchar2, iNum in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure BlankCardDetailListExport(strBatchNum in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

  procedure StatusCount(iStatus in number, records out number, retid out number, retmsg out varchar2);

  procedure getCheckNumber(strNumber in varchar2,checkNo out varchar2);

end DM_PKG_BLANK_CARD_MNG;
//

create or replace package body pdms.DM_PKG_BLANK_CARD_MNG is

  procedure GenerateEnhancedKi(strEKi out varchar2, retid out number, retmsg out varchar2)
  is
      temp_index  number := 0;
      temp_ekiId  number := -1;
      temp_kiSegNum number := 8;
      temp_kiDataSeg  VARCHAR2(4) :=null;
    temp_enhancedKi1  VARCHAR2(16) := '';
    temp_enhancedKi2  VARCHAR2(16) := '';
  begin
      while( temp_index < temp_kiSegNum ) loop

          temp_ekiId := floor(DBMS_RANDOM.VALUE(1,45397));
          if temp_ekiId < 1 or temp_ekiId > 45396 then
            retid := 6001;
        retmsg  := '产生增强性ki的随机数失败!';
        return;
        end if;

          select ENCRYPTED_DATA into temp_kiDataSeg from DM_ENCRYPTED_KI where DATA_ID=temp_ekiId;


          temp_enhancedKi1 := temp_enhancedKi1 || substr(temp_kiDataSeg,1,2);
          temp_enhancedKi2 := temp_enhancedKi2 || substr(temp_kiDataSeg,3,2);

          temp_index := temp_index + 1;
          temp_ekiId := -1;
      end loop;

      strEKi := temp_enhancedKi1 || temp_enhancedKi2;

    retid := 0;
    retmsg  := '';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end GenerateEnhancedKi;


  procedure CardSNGenerate(strFactoryName in varchar2, strCardName in varchar2, strPresetFlag in varchar2, strSingleFlag in varchar2,strSIMFlag in varchar2, strSWPFlag in varchar2, strM2MFlag in varchar2, strCardSN out varchar2, retid out number, retmsg out varchar2)
  is
    temp_factoryCode varchar2(2) := null;
    temp_cardCode varchar2(3) := null;
    temp_cardTypeFlag varchar2(4) := null;
    temp_cardSN_1_13 varchar2(13) := null;
    temp_cardSN_14_20 varchar2(7) := null;
    temp_cardSN_T1_T2 varchar2(8) := null;
    temp_cardSN_T1_T4 varchar2(4) := null;
    temp_date varchar2(2) :=  to_char(sysdate, 'YY');
    temp_maxCardSN varchar2(7) :=  null;
    temp_num number := -1;
  begin

    select count(*) into temp_num from DM_FACTORY where FACTORY_NAME = strFactoryName;
    if temp_num < 1 then
          retid := 6002;
          retmsg  := '卡商[' || strFactoryName || ']信息不存在！';
          return;
    end if;
    select FACTORY_CODE into temp_factoryCode from DM_FACTORY where FACTORY_NAME = strFactoryName;

    select count(*) into temp_num from DM_CARD_TYPE where CARD_NAME = strCardName;
    if temp_num < 1 then
          retid := 6003;
          retmsg  := '卡类型[' || strCardName || ']信息不存在！';
          return;
    end if;
    select CARD_CODE into temp_cardCode from DM_CARD_TYPE where CARD_NAME = strCardName;

    if strPresetFlag is null or length(strPresetFlag)<>1 or (strPresetFlag <> DM_PKG_CONST.CARN_SN_PRESET_FLAG_YES and strPresetFlag <> DM_PKG_CONST.CARN_SN_PRESET_FLAG_NO) then
      retid := 6004;
          retmsg  := '预置卡标识不正确!';
          return;
    end if;

    if strSingleFlag is null or length(strSingleFlag)<>1 or (strSingleFlag <> DM_PKG_CONST.CARN_SN_SINGLE_FLAG_YES and strSingleFlag <> DM_PKG_CONST.CARN_SN_SINGLE_FLAG_NO) then
      retid := 6005;
          retmsg  := '单号卡标识不正确!';
          return;
    end if;

    if strSIMFlag is null or length(strSIMFlag)<>2 or (strSIMFlag <> DM_PKG_CONST.CARN_SN_SIM_FLAG_YES and strSIMFlag <> DM_PKG_CONST.CARN_SN_SIM_FLAG_NO) then
      retid := 6006;
          retmsg  := 'SIM卡标识不正确!';
          return;
    end if;

    if strSWPFlag is null or length(strSWPFlag)<>1 or (strSWPFlag <> DM_PKG_CONST.CARN_SN_SWP_FLAG_YES and strSWPFlag <> DM_PKG_CONST.CARN_SN_SWP_FLAG_NO) then
      retid := 6007;
          retmsg  := 'SWP卡标识不正确!';
          return;
    end if;

    if strM2MFlag is null or length(strM2MFlag)<>1 or (strM2MFlag <> DM_PKG_CONST.CARN_SN_M2M_FLAG_YES and strM2MFlag <> DM_PKG_CONST.CARN_SN_M2M_FLAG_NO) then
      retid := 6008;
          retmsg  := 'M2M卡标识不正确!';
          return;
    end if;
    temp_cardSN_T1_T2 := '0' || strPresetFlag || strSingleFlag || strSIMFlag || strSWPFlag || strM2MFlag || '0';
	temp_cardSN_T1_T4 := pkg_number_trans.f_bin_to_hex(temp_cardSN_T1_T2) || '00';
    temp_cardSN_1_13 := '15' || temp_date || '00' || temp_cardCode || temp_cardTypeFlag || temp_cardSN_T1_T4 || temp_factoryCode;
    select count(*) into temp_num from DM_BLANK_CARD_BATCH where substr(CARD_SN_END, 1, 13) = temp_cardSN_1_13;
    if temp_num < 1 then
      strCardSN := temp_cardSN_1_13 || '0000000';
    else
      select substr(max(CARD_SN_END), -7, 7) into temp_maxCardSN from DM_BLANK_CARD_BATCH where substr(CARD_SN_END, 1, 13) = temp_cardSN_1_13;
	  dbms_output.put_line(temp_maxCardSN);
      if temp_maxCardSN = '9999999' then
        retid := 6009;
            retmsg  := '此类型卡数据已用完!';
            return;
          end if;
      temp_cardSN_14_20 := substr(to_char(10000000 + to_number(temp_maxCardSN) + 1), -7, 7);
	  dbms_output.put_line(temp_cardSN_14_20);
      strCardSN := temp_cardSN_1_13 || temp_cardSN_14_20;
    end if;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end CardSNGenerate;


  procedure SeIdGenerate(strFactoryName in varchar2, seCode in varchar2, seIdStart out varchar2, retid out number, retmsg out varchar2)
  is
    temp_year varchar2(2) :=  to_char(sysdate, 'YY');
    temp_num number := -1;
    temp_factoryCode varchar2(2) := null;
    temp_seId_1_10 varchar2(10) := null;
    temp_maxSeId varchar2(9) :=  null;
    temp_seId_11_19 varchar2(9) := null;
  begin

    select count(*) into temp_num from DM_FACTORY where FACTORY_NAME = strFactoryName;
    if temp_num < 1 then
          retid := 6101;
          retmsg  := '卡商[' || strFactoryName || ']信息不存在！';
          return;
    end if;
    select FACTORY_CODE into temp_factoryCode from DM_FACTORY where FACTORY_NAME = strFactoryName;


    temp_seId_1_10 := temp_year || '15' ||'000' || temp_factoryCode || seCode;


    select count(*) into temp_num from DM_BLANK_CARD_BATCH where substr(SEID_END, 1, 10) = temp_seId_1_10;
    if temp_num < 1 then
      temp_seId_11_19 := '000000000';
    else
      select substr(max(SEID_END), -10, 9) into temp_maxSeId from DM_BLANK_CARD_BATCH where substr(SEID_END, 1, 10) = temp_seId_1_10;
      if temp_maxSeId = '999999999' then
        retid := 6102;
        retmsg  := '此SE类型卡数据已用完!';
        return;
      end if;
      temp_seId_11_19 := substr(to_char(1000000000 + to_number(temp_maxSeId) + 1), 2, 9);
    end if;


    seIdStart := temp_seId_1_10 || temp_seId_11_19;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end SeIdGenerate;

  procedure BlankCardBacthGenerate(strBatchNum out varchar2, retid out number, retmsg out varchar2)
  is
    temp_maxIndex varchar2(3) := null;
    temp_maxBatchNum varchar2(9) := null;
    temp_date varchar2(6) :=  to_char(sysdate, 'YYMMDD');
    temp_num number := -1;
  begin
    --
    select count(*) into temp_num from DM_BLANK_CARD_BATCH where substr(BATCH_NUM, 1, 6) = temp_date;
    if temp_num < 1 then
      strBatchNum := temp_date || '000';
    else
      select max(BATCH_NUM) into temp_maxBatchNum from DM_BLANK_CARD_BATCH where substr(BATCH_NUM, 1, 6) = temp_date;
      temp_num := to_number(substr(temp_maxBatchNum, -3, 3));
      if temp_num = 999 then
            retid := 6010;
            retmsg  := '当天最大可分配批次数量已用完!';
            return;
        else
          select max(substr(BATCH_NUM, -3, 3)) into temp_maxIndex from DM_BLANK_CARD_BATCH where substr(BATCH_NUM, 1, 6) = temp_date;
          strBatchNum := temp_date || substr(to_char(1000 + to_number(temp_maxIndex) + 1), -3, 3);
      end if;
    end if;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end BlankCardBacthGenerate;


  procedure BlankCardBacthAdd(strBatchNum in varchar2,strFactoryCode in varchar2, strFactoryName in varchar2, strCardName in varchar2, strPresetFlag in varchar2, strSingleFlag in varchar2,strSIMFlag in varchar2, strSWPFlag in varchar2, strM2MFlag in varchar2, iCount in number, strAllot in varchar2, strNotes in varchar2, seId in varchar2, retid out number, retmsg out varchar2)
  is
    temp_bacthNum varchar2(9) := null;
    temp_cardSNStart varchar2(20) := null;
    temp_cardSNEnd varchar2(20) := null;
    temp_remainCount number := -1;
    temp_seIdStart VARCHAR2(20) := null;
    temp_seIdEnd VARCHAR2(20) := null;
    temp_checkNoS number;
    temp_checkNoE number;
  begin

    BlankCardBacthGenerate(temp_bacthNum, retid, retmsg);
    if retid <> 0 then
      retmsg := '生成白卡批次号错误：' || retmsg;
      return;
    end if;


    if temp_bacthNum <> strBatchNum then
      retid := 6011;
          retmsg  := '批次号验证失败，请重新生成！' ;
          return;
    end if;


    CardSNGenerate(strFactoryName, strCardName, strPresetFlag, strSingleFlag, strSIMFlag, strSWPFlag, strM2MFlag, temp_cardSNStart, retid, retmsg);
    if retid <> 0 then
      retmsg := '生成起始卡片序列号错误：' || retmsg;
      return;
    end if;


    temp_remainCount := 10000000 - to_number(substr(temp_cardSNStart, -7, 7));
    if iCount > temp_remainCount then
          retid := 6012;
          retmsg  := '分配数量过界,当前最大可用数量为:' || temp_remainCount;
          return;
      end if;


    temp_cardSNEnd := substr(temp_cardSNStart, 1, 13) || substr(to_char(10000000 + substr(temp_cardSNStart, -7, 7) + iCount - 1), -7,7);

    if seId is not null then

      SeIdGenerate(strFactoryName, seId, temp_seIdStart, retid, retmsg);
      if retid <> 0 then
        retmsg := '生成起始SEID序列号错误：' || retmsg;
        return;
      end if;

      temp_remainCount := 1000000000 - to_number(substr(temp_seIdStart, 11, 9));
      if iCount > temp_remainCount then
        retid := 6111;
        retmsg  := '分配数量过界,SEID当前最大可用数量为:' || temp_remainCount;
        return;
      end if;

      temp_seIdEnd := substr(temp_seIdStart, 1, 10) || substr(to_char(1000000000 + substr(temp_seIdStart, 11, 9) + iCount - 1), 2,9);

      getCheckNumber(temp_seIdStart,temp_checkNoS);
      temp_seIdStart := temp_seIdStart || temp_checkNoS;

      getCheckNumber(temp_seIdEnd,temp_checkNoE);
      temp_seIdEnd := temp_seIdEnd || temp_checkNoE;
    end if;

    INSERT INTO DM_BLANK_CARD_BATCH VALUES(
      temp_bacthNum,
      strFactoryName,
      strCardName,
      strPresetFlag,
      strSingleFlag,
      strSIMFlag,
      strSWPFlag,
      strM2MFlag,
      temp_cardSNStart,
      temp_cardSNEnd,
      iCount,
      sysdate,
      '0',
      strAllot,
      strNotes,
      strFactoryCode,
      temp_seIdStart,
      temp_seIdEnd
      );
    commit;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end BlankCardBacthAdd;


  procedure BlankCardBacthList(strCardName in varchar2, strFactoryCode in varchar2, strStartTime in varchar2, strEndTime in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_startTime varchar2(20) := strStartTime || ' 00:00:00';
    temp_endTime varchar2(20) := strEndTime || ' 23:59:59';
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    if strCardName is null then
      if strFactoryCode is null then
        if strStartTime is null and strEndTime is null then

          select count(*) into records from DM_BLANK_CARD_BATCH where NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%';
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%' order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        else

          select count(*) into records from DM_BLANK_CARD_BATCH where (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%') and
            BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%') and
                BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        end if;
      else
        if strStartTime is null and strEndTime is null then

          select count(*) into records from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%');
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%') order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        else

          select count(*) into records from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%')
            and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%')
                and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        end if;
      end if;
    else
      if strFactoryCode is null then
        if strStartTime is null and strEndTime is null then

          select count(*) into records from DM_BLANK_CARD_BATCH where CARD_NAME=strCardName and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%');
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where CARD_NAME=strCardName and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%') order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        else

          select count(*) into records from DM_BLANK_CARD_BATCH where CARD_NAME=strCardName and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%')
            and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where CARD_NAME=strCardName and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%')
                and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        end if;
      else
        if strStartTime is null and strEndTime is null then

          select count(*) into records from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and CARD_NAME=strCardName
            and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%');
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and CARD_NAME=strCardName
              and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%')order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        else

          select count(*) into records from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and CARD_NAME=strCardName
            and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%') and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss'));
          open results for
            select * from (
              select BATCH_NUM,FACTORY_NAME,CARD_NAME,PRESET_FLAG,SINGLE_FLAG,SIM_FLAG,SWP_FLAG,M2M_FLAG,CARD_SN_START,CARD_SN_END,SEID_START,SEID_END,BATCH_COUNT,to_char(BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,STATUS,ALLOT,NOTES, ROWNUM NUM
              from (select * from DM_BLANK_CARD_BATCH where FACTORY_CODE=strFactoryCode and CARD_NAME=strCardName
              and (NOTES is null or NOTES not like DM_PKG_CONST.BK_CARD_TYPE_LBYK || '%') and BATCH_TIME between (to_date(temp_startTime, 'yyyy-mm-dd hh24:mi:ss')) and (to_date(temp_endTime, 'yyyy-mm-dd hh24:mi:ss')) order by BATCH_NUM)
            ) where NUM between temp_minPage and temp_maxPage;
        end if;
      end if;
    end if;

    retid := 0;
    retmsg  := '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end BlankCardBacthList;


  procedure BlankCardStateModify(strBatchNum in varchar2, iStatus in number, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    select count(*) into temp_flag from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    if temp_flag < 1 then
      retid := 6013;
      retmsg  := '输入的白卡批次号不存在！';
      return;
    end if;

    update DM_BLANK_CARD_BATCH set STATUS=iStatus where BATCH_NUM = strBatchNum;
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end BlankCardStateModify;

  procedure BlankCardBacthDel(strBatchNum in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_cardSN varchar2(20) := null;
    temp_cardSN_check varchar2(20) := null;
    temp_state varchar2(1) := null;
  begin

    select count(*) into temp_flag from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    if temp_flag < 1 then
      retid := 6014;
      retmsg  := '输入的白卡批次号不存在！';
      return;
    end if;


    select CARD_SN_END, STATUS into temp_cardSN, temp_state from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    select max(CARD_SN_END) into temp_cardSN_check from DM_BLANK_CARD_BATCH where substr(CARD_SN_END,1,13) = substr(temp_cardSN,1,13);
    if temp_cardSN <> temp_cardSN_check then
          retid := 6015;
          retmsg  := '删除的白卡批次必须是对应卡商卡类型的最后一个批次！';
          return;
    end if;

        if temp_state <> DM_PKG_CONST.BC_BACTH_STATE_INIT and temp_state <> DM_PKG_CONST.BC_BACTH_STATE_GENERATING then
      retid := 6022;
      retmsg  := '将要删除的白卡批次已经生成数据，不能删除！';
          return;
    end if;


    delete from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    delete from DM_BLANK_CARD where BATCH_NUM = strBatchNum;
    commit;

    retid := 0;
    retmsg  := '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end BlankCardBacthDel;

  procedure BlankCardBacthInfo(iType in number, strIDVal in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_num number := -1;
  begin
    if iType=1 then
      select COUNT(*) into temp_num from DM_BLANK_CARD_BATCH a, DM_FACTORY b where a.BATCH_NUM=strIDVal and a.FACTORY_NAME=b.FACTORY_NAME;
      if temp_num < 1 then
        retid := 6016;
        retmsg  := '白卡批次[' || strIDVal || ']信息不存在。';
        return;
      end if;

      open results for
        select a.BATCH_NUM,a.FACTORY_NAME,a.CARD_NAME,a.PRESET_FLAG,a.SINGLE_FLAG,a.SIM_FLAG,a.SWP_FLAG,a.M2M_FLAG,a.CARD_SN_START,
        a.CARD_SN_END,a.BATCH_COUNT,to_char(a.BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,a.STATUS,a.ALLOT,a.NOTES,b.FACTORY_CODE,
        a.SEID_START,a.SEID_END
        from DM_BLANK_CARD_BATCH a, DM_FACTORY b where a.BATCH_NUM=strIDVal and a.FACTORY_NAME=b.FACTORY_NAME;
    elsif iType=2 then
      select COUNT(*) into temp_num from DM_BLANK_CARD_BATCH a, DM_FACTORY b where a.NOTES=strIDVal and a.FACTORY_NAME=b.FACTORY_NAME;
      if temp_num < 1 then
        retid := 1;
        retmsg  := '两不一快白卡批次[' || strIDVal || ']信息不存在。';
        return;
      end if;

      open results for
        select a.BATCH_NUM,a.FACTORY_NAME,a.CARD_NAME,a.PRESET_FLAG,a.SINGLE_FLAG,a.SIM_FLAG,a.SWP_FLAG,a.M2M_FLAG,a.CARD_SN_START,
        a.CARD_SN_END,a.BATCH_COUNT,to_char(a.BATCH_TIME, 'yyyy-mm-dd hh24:mi:ss') as BATCH_TIME,a.STATUS,a.ALLOT,a.NOTES,b.FACTORY_CODE
        from DM_BLANK_CARD_BATCH a, DM_FACTORY b where a.NOTES=strIDVal and a.FACTORY_NAME=b.FACTORY_NAME;
    else
      retid := 6016;
      retmsg:= '非法查询白卡批次信息!';
    end if;

    retid := 0;
    retmsg:= '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg := sqlerrm;
  end BlankCardBacthInfo;


  procedure CardSNImport(strBatchNum in varchar2, retid out number, retmsg out varchar2)
  is
    temp_count1 number := -1;
    temp_count2 number := -1;
    temp_simFlag varchar2(2) := null;
    temp_singleFlag varchar2(1) := null;
    temp_cardSNStart varchar2(20) := null;
    temp_batch_num  varchar2(9) := null;
    temp_cardSN varchar2(20) := null;
    temp_k1 varchar2(32) := null;
    temp_ki varchar2(32) := null;
    temp_viceKi varchar2(32) := '';
    temp_cursor cursorType := null;
    temp_seId varchar2(20) := null;
  begin

    select BATCH_COUNT, CARD_SN_START, SIM_FLAG, SINGLE_FLAG into temp_count1, temp_cardSNStart, temp_simFlag, temp_singleFlag from DM_BLANK_CARD_BATCH where BATCH_NUM=strBatchNum;
    select count(*) into temp_count2 from DM_BLANK_CARD_TEMP where BATCH_NUM=strBatchNum;
    if temp_count1 <> temp_count2 then
      delete from DM_BLANK_CARD_TEMP;
      commit;
      retid := 6017;
      retmsg := '该批次生成的卡片序列号数量不正确！';
      update DM_BLANK_CARD_BATCH set STATUS=DM_PKG_CONST.BC_BACTH_STATE_INIT where BATCH_NUM=strBatchNum;
      commit;
      return;
    end if;

    delete from DM_BLANK_CARD;

    commit;

    open temp_cursor for
      select BATCH_NUM,CARD_SN,K1,SEID from DM_BLANK_CARD_TEMP where BATCH_NUM=strBatchNum;
      if temp_cursor %isopen then
        loop
          fetch temp_cursor into temp_batch_num,temp_cardSN,temp_k1,temp_seId;
          exit when temp_cursor %notfound;

          DM_PKG_BLANK_CARD_MNG.GenerateEnhancedKi(temp_ki, retid, retmsg);
          if temp_ki is null or length(temp_ki) <> 32 or retid <> 0 then
              retid := 6019;
            retmsg  := '生成增强性Ki失败，卡片序列号=['|| temp_cardSN ||']Ki=['|| temp_ki || ']。' || retmsg;
            delete from DM_BLANK_CARD_TEMP;
            delete from DM_BLANK_CARD;
            update DM_BLANK_CARD_BATCH set STATUS=DM_PKG_CONST.BC_BACTH_STATE_INIT where BATCH_NUM=strBatchNum;
            commit;
            return;
          end if;

          if temp_singleFlag = DM_PKG_CONST.CARN_SN_SINGLE_FLAG_NO and temp_simFlag = DM_PKG_CONST.CARN_SN_SIM_FLAG_YES then

            DM_PKG_BLANK_CARD_MNG.GenerateEnhancedKi(temp_viceKi, retid, retmsg);
            if temp_viceKi is null or length(temp_viceKi) <> 32 or retid <> 0 then
                retid := 6019;
              retmsg  := '生成增强性Ki失败，卡片序列号=['|| temp_cardSN ||']Ki=['|| temp_viceKi || ']。' || retmsg;
              delete from DM_BLANK_CARD_TEMP;
              delete from DM_BLANK_CARD;
              update DM_BLANK_CARD_BATCH set STATUS=DM_PKG_CONST.BC_BACTH_STATE_INIT where BATCH_NUM=strBatchNum;
              commit;
              return;
            end if;
          end if;

          if temp_simFlag = DM_PKG_CONST.CARN_SN_SIM_FLAG_YES then
            insert into DM_BLANK_CARD values (temp_batch_num,temp_cardSN,temp_k1,temp_ki,'','',temp_viceKi,'','',sysdate,temp_seId);
          elsif temp_simFlag = DM_PKG_CONST.CARN_SN_SIM_FLAG_NO then
            insert into DM_BLANK_CARD values (temp_batch_num,temp_cardSN,temp_k1,'',temp_ki,'','',temp_viceKi,'',sysdate,temp_seId);
          end if;
        end loop;
      end if;
    commit;

    delete from DM_BLANK_CARD_TEMP;
    commit;

    retid := 0;
    retmsg  := 'success';

    exception
      when others then
        update DM_BLANK_CARD_BATCH set STATUS=DM_PKG_CONST.BC_BACTH_STATE_INIT where BATCH_NUM=strBatchNum;
        commit;
        retid := sqlcode;
        retmsg  := sqlerrm;
  end CardSNImport;

  procedure BlankCardDetailList(strBatchNum in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_num number := -1;
    temp_num_check number := -1;
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    select count(*) into temp_num from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    if temp_num < 1 then
      retid := 6020;
      retmsg  := '输入的白卡批次号不存在！';
      return;
    end if;

    select BATCH_COUNT into records from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    select count(*) into temp_num_check from DM_BLANK_CARD where BATCH_NUM = strBatchNum;
    if records <> temp_num_check then
      retid := 6021;
      retmsg  := '白卡批次表中的数量与详细数据数量不符！';
      return;
    end if;

    open results for
      select * from (
          select BATCH_NUM,CARD_SN,K1,EKI,K,OPC,VICE_EKI,VICE_K,VICE_OPC,to_char(SN_TIME, 'yyyy-mm-dd hh24:mi:ss') as SN_TIME, ROWNUM NUM
          from DM_BLANK_CARD where BATCH_NUM = strBatchNum order by CARD_SN
      ) where NUM between temp_minPage and temp_maxPage;
    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end BlankCardDetailList;


  procedure BlankCardAllotList(strBatchNum in varchar2, iNum in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_num number := -1;
    temp_min_cardsn number := 0;
    temp_max_cardsn number := 0;
  begin

    select count(*) into temp_num from DM_BLANK_CARD_BATCH where BATCH_NUM = strBatchNum;
    if temp_num < 1 then
      retid := 6020;
      retmsg  := '输入的白卡批次号不存在！';
      return;
    end if;

    select count(*) into records from DM_BLANK_CARD where BATCH_NUM = strBatchNum;
    if records < iNum then
      retid := 6021;
      retmsg  := '白卡批次表中的数量与详细数据数量不符！';
      return;
    end if;

    select substr(min(CARD_SN), -7, 7) into temp_min_cardsn from DM_BLANK_CARD where BATCH_NUM = strBatchNum ;
    temp_max_cardsn := temp_min_cardsn + iNum -1;

    open results for
      select BATCH_NUM,CARD_SN,SEID,K1,EKI,K,OPC,VICE_EKI,VICE_K,VICE_OPC,to_char(SN_TIME, 'yyyy-mm-dd hh24:mi:ss') as SN_TIME
      from DM_BLANK_CARD where BATCH_NUM = strBatchNum and substr(CARD_SN, -7, 7) between temp_min_cardsn and temp_max_cardsn order by CARD_SN asc;


    delete from DM_BLANK_CARD t where t.BATCH_NUM = strBatchNum and substr(CARD_SN, -7, 7) between temp_min_cardsn and temp_max_cardsn;
    commit;

    retid := 0;
    retmsg  := '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end BlankCardAllotList;

  procedure BlankCardDetailListExport(strBatchNum in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
  begin

    BlankCardDetailList(strBatchNum, iPage, iPageSize, records, results, retid, retmsg);
    if retid <> 0 then
      return;
    end if;


    update DM_BLANK_CARD_BATCH set STATUS=DM_PKG_CONST.BC_BACTH_STATE_EXPORT where BATCH_NUM=strBatchNum;


    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end BlankCardDetailListExport;


  procedure StatusCount(iStatus in number, records out number, retid out number, retmsg out varchar2)
  is
  begin

    select count(*) into records from DM_BLANK_CARD_BATCH where STATUS=iStatus;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end StatusCount;


  procedure getCheckNumber(strNumber in varchar2,checkNo out varchar2)
  is
    tem_i number := LENGTH(strNumber);
    tmpNumber number;
    numToChar VARCHAR2(4);
    sumCore number;
    tem_j number;
    totalNumber number := 0;
    lastNum number;
  begin

    while tem_i >= 0 loop
        tmpNumber := (TO_NUMBER(substr(strNumber, tem_i, 1),'x')*2);
        numToChar := TO_CHAR(tmpNumber);
        sumCore := 0;
        tem_j := 1;
        while tem_j <= LENGTH(numToChar) loop
            sumCore := sumCore+TO_NUMBER(substr(numToChar, tem_j, 1));
            tem_j := tem_j+1;
        end loop;
        if tem_i = 1 then
            totalNumber := totalNumber + sumCore;
        else
            totalNumber := totalNumber + sumCore + TO_NUMBER(substr(strNumber, tem_i-1, 1),'x');
        end if;
        tem_i := (tem_i - 2);
    end loop;
    if (totalNumber >= 0 and totalNumber < 9) then
        checkNo := (10-totalNumber);
    else
        lastNum := TO_NUMBER(substr(totalNumber,LENGTH(TO_CHAR(totalNumber)),1));
        if lastNum = 0 then
            checkNo := 0;
        else
            checkNo := (10-lastNum);
        end if;
    end if;

  end getCheckNumber;

end DM_PKG_BLANK_CARD_MNG;
//

delimiter ;//