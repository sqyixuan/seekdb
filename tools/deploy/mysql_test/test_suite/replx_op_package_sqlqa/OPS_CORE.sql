CREATE OR REPLACE PACKAGE "OPS_CORE" is
	
	type cursorType is ref cursor;


	procedure FactoryList(results out cursorType, retid out number, retmsg out varchar2);


	procedure FactoryInfo(strFactoryCode in varchar2, results out cursorType, retid out number, retmsg out varchar2);


	procedure FactoryAdd(strFactoryCode in varchar2, strFactoryName in varchar2, strFactoryAbb in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

	procedure FactoryModify(strFactoryCode in varchar2, strFactoryName in varchar2, strFactoryAbb in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);


	procedure FactoryDel(strFactoryCode in varchar2, retid out number, retmsg out varchar2);

	procedure CardTypeList(results out cursorType, retid out number, retmsg out varchar2);

	procedure CardTypeInfo(strCardType in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	procedure CardTypeAdd(strCardType in varchar2, strCardFlag in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

	procedure CardTypeModify(strCardType in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

	procedure CardTypeDel(strCardType in varchar2, retid out number, retmsg out varchar2);

	procedure DllComponentList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

	procedure DllComponentAdd(strDllName in varchar2, strDllVersion in varchar2,  strDllState in varchar2, strNotes in varchar2,retid out number, retmsg out varchar2);

	procedure DllComponentDel(iDllID in number, retid out number, retmsg out varchar2);

	procedure DllComponentModify(iDllID in number, retid out number, retmsg out varchar2);

	procedure DllComponentInfo(results out cursorType, retid out number, retmsg out varchar2);

	procedure PersonalDataList(strICCID in varchar2, strCardSN in varchar2, strState in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

	procedure PersonalDataAdd(iRecords in number, strCardSN in varchar2, retid out number, retmsg out varchar2);

	procedure PersonalRandomData(strCardSN in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	procedure PersonalDataUpdate(strSeqNo in varchar2, strCardSN in varchar2, strState in varchar2, retid out number, retmsg out varchar2);

	procedure PresetDataAdd(strSeqNo in varchar2, strInOutFlag in varchar2, strCardType in varchar2, strEncKi in varchar2, strEncOpc in varchar2, strLocalProvCode in varchar2, strSignature in varchar2, retid out number, retmsg out varchar2);

	procedure PersonalDataDel(strCardSN in varchar2, strRandomData in varchar2, retid out number, retmsg out varchar2);

	procedure ProvinceList(results out cursorType, retid out number, retmsg out varchar2);

	procedure ProvinceLists(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);

	procedure ProvinceInfo(strProvinceCode in varchar2, results out cursorType, retid out number, retmsg out varchar2);

	procedure ProvinceAdd(strProvinceCode in varchar2, strProvinceName in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

	procedure ProvinceModify(strProvinceCode in varchar2, strProvinceName in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

	procedure ProvinceDel(strProvinceCode in varchar2, retid out number, retmsg out varchar2);

	procedure ImprotKey(strProvinceCode in varchar2, iPubKeyIndex in varchar2, iPubKeyVersion in varchar2,retid out number, retmsg out varchar2);

end OPS_CORE;
//

CREATE OR REPLACE PACKAGE BODY "OPS_CORE" is

  procedure FactoryList(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    open results for
      select FACTORY_CODE,FACTORY_NAME,FACTORY_ABB,NOTES from OPS_FACTORY;

    retid  := 0;
    retmsg  := '???';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end FactoryList;


  procedure FactoryInfo(strFactoryCode in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_FACTORY where FACTORY_CODE=strFactoryCode;
    if temp_flag < 1 then
      retid  := 2011;
      retmsg  := '????????[' || strFactoryCode || ']??????';
      return;
    end if;

    open results for
      select FACTORY_CODE,FACTORY_NAME,FACTORY_ABB,NOTES from OPS_FACTORY where FACTORY_CODE=strFactoryCode;

    retid  := 0;
    retmsg  := 'success';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end FactoryInfo;


  procedure FactoryAdd(strFactoryCode in varchar2, strFactoryName in varchar2, strFactoryAbb in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_FACTORY where FACTORY_CODE=strFactoryCode;
    if temp_flag > 0 then
      retid  := 2021;
      retmsg  := '???' || strFactoryCode || '??????????';
      return;
    end if;

    select count(*) into temp_flag from OPS_FACTORY where FACTORY_NAME = strFactoryName;
    if temp_flag > 0 then
      retid  := 2022;
      retmsg  := '???' || strFactoryName || '??????????';
      return;
    end if;

    insert into OPS_FACTORY values (strFactoryCode, strFactoryName, strFactoryAbb, strNotes);
    commit;

    retid  := 0;
    retmsg  := '???';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end FactoryAdd;

  procedure FactoryModify(strFactoryCode in varchar2, strFactoryName in varchar2, strFactoryAbb in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_FACTORY where FACTORY_CODE=strFactoryCode;
    if temp_flag < 1 then
      retid  := 2031;
      retmsg  := '??????????[' || strFactoryCode || ']??????';
      return;
    end if;

    update OPS_FACTORY set FACTORY_NAME=strFactoryName, FACTORY_ABB=strFactoryAbb, NOTES=strNotes where FACTORY_CODE=strFactoryCode;
    commit;

    retid  := 0;
    retmsg  := '???';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end FactoryModify;


  procedure FactoryDel(strFactoryCode in varchar2, retid out number, retmsg out varchar2)
  is
  begin
    delete from OPS_FACTORY where FACTORY_CODE=strFactoryCode;
    commit;

    retid  := 0;
    retmsg  := '???';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end FactoryDel;


  procedure CardTypeList(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    open results for
      select CARD_CODE,CARD_FLAG,CARD_NAME,CARD_CAPACITY,NOTES from OPS_CARD_TYPE order by CARD_CODE;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end CardTypeList;


  procedure CardTypeInfo(strCardType in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_CARD_TYPE where CARD_CODE=strCardType;
    if temp_flag < 1 then
      retid  := 2061;
      retmsg  := '??????????[' || strCardType || ']???';
      return;
    end if;

    open results for
      select CARD_CODE,CARD_FLAG,CARD_NAME,CARD_CAPACITY,NOTES from OPS_CARD_TYPE where CARD_CODE=strCardType;

    retid  := 0;
    retmsg  := '??';

    -- ??????
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end CardTypeInfo;


  procedure CardTypeAdd(strCardType in varchar2, strCardFlag in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_CARD_TYPE where CARD_CODE=strCardType;
    if temp_flag > 0 then
      retid  := 2071;
      retmsg  := '?????[' || strCardType || ']?????';
      return;
    end if;

    insert into OPS_CARD_TYPE values (strCardType, strCardFlag, strCardName, strCardCapacity, strNotes);
    commit;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CardTypeAdd;


  procedure CardTypeModify(strCardType in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_CARD_TYPE where CARD_CODE=strCardType;
    if temp_flag < 1 then
      retid  := 2081;
      retmsg  := '??????????[' || strCardType || ']???';
      return;
    end if;

    update OPS_CARD_TYPE set CARD_NAME=strCardName, CARD_CAPACITY=strCardCapacity, NOTES=strNotes where CARD_CODE=strCardType;
    commit;

    retid  := 0;
    retmsg  := '??';

    -- ??????
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CardTypeModify;


  procedure CardTypeDel(strCardType in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_CARD_TYPE where CARD_CODE=strCardType;
    if temp_flag < 1 then
      retid  := 2091;
      retmsg  := '??????????[' || strCardType || ']???';
      return;
    end if;

    delete from OPS_CARD_TYPE where CARD_CODE=strCardType;
    commit;

    retid  := 0;
    retmsg  := '??';

    -- ??????
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CardTypeDel;


  procedure DllComponentList(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
    temp_flag number := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    select COUNT(*) into temp_flag from OPS_DLL a;
    if temp_flag < 1 then
      retid  := 2101;
      retmsg  := '??????????????';
      return;
    end if;

    select COUNT(*) into records from OPS_DLL;
    open results for
      select * from (
        select
          a.DLL_ID,a.DLL_NAME,a.DLL_VERSION,a.DLL_STATE,to_char(a.UPLOAD_DATE, 'yyyy-mm-dd hh24:mi:ss') as UPLOAD_DATE,
          a.NOTES, ROWNUM num
        from OPS_DLL a
        order by UPLOAD_DATE desc
      ) where num between temp_minPage and temp_maxPage;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end DllComponentList;


  procedure DllComponentAdd(strDllName in varchar2, strDllVersion in varchar2, strDllState in varchar2,  strNotes in varchar2,retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from OPS_DLL where DLL_NAME=strDllName and DLL_VERSION=strDllVersion;
    if temp_flag > 0 then
      retid  := 2111;
      retmsg  := '???[' || strDllName || ']????????????';
      return;
    end if;

    insert into OPS_DLL values (OPS_SEQ_DLL.nextval, strDllName, strDllVersion, strDllState, sysdate, strNotes);
    commit;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;

  end DllComponentAdd;


  procedure DllComponentDel(iDllID in number, retid out number, retmsg out varchar2)
  is
  begin
    delete from OPS_DLL where DLL_ID=iDllID;
    commit;

    retid  := 0;
    retmsg  := '??';

    -- ??????
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end DllComponentDel;



  procedure DllComponentModify(iDllID in number, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from OPS_DLL where DLL_ID=iDllID;
    if temp_flag < 1 then
      retid  := 2131;
      retmsg  := '????????????[' || iDllID || ']???';
      return;
    end if;

    update OPS_DLL set DLL_STATE='1' where DLL_ID=iDllID;
    commit;

    retid  := 0;
    retmsg  := '??';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end DllComponentModify;


  procedure DllComponentInfo(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from OPS_DLL;
    if temp_flag < 1 then
      retid  := 2141;
      retmsg  := '???????????????';
      return;
    end if;

    open results for
      select
        a.Dll_ID,a.DLL_NAME,a.DLL_VERSION,a.DLL_STATE,to_char(a.UPLOAD_DATE, 'yyyy-mm-dd hh24:mi:ss') as UPLOAD_DATE,
        a.NOTES,b.CARD_NAME,c.FACTORY_NAME
      from OPS_DLL a, OPS_CARD_TYPE b, OPS_FACTORY c;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end DllComponentInfo;



  procedure PersonalDataList(strICCID in varchar2, strCardSN in varchar2, strState in varchar2, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    if strState <> '4' then
      if strCardSN is null then
        if strICCID is null then
          select count(*) into records from OPS_PERSONAL_DATA where STATE=strState; --and USIM_FLAG=cardType;
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where STATE=strState 
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        else
          select count(*) into records from OPS_PERSONAL_DATA where ICCID like strICCID || '%' and STATE=strState; --and USIM_FLAG=cardType;
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where ICCID like strICCID || '%' and STATE=strState 
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        end if;
      else
        if strICCID is null then
          select count(*) into records from OPS_PERSONAL_DATA where CARD_SN like strCardSN || '%' and STATE=strState; 
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where CARD_SN like strCardSN || '%' and STATE=strState 
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        else
          select count(*) into records from OPS_PERSONAL_DATA where CARD_SN like strCardSN || '%' and ICCID like strICCID || '%' and STATE=strState; 
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where CARD_SN like strCardSN || '%' and ICCID like strICCID || '%' and STATE=strState 
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        end if;
      end if;
    else
      if strCardSN is null then
        if strICCID is null then
          select count(*) into records from OPS_PERSONAL_DATA; 
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA

              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        else
          select count(*) into records from OPS_PERSONAL_DATA where ICCID like strICCID || '%' ;
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where ICCID like strICCID || '%'
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        end if;
      else
        if strICCID is null then
          select count(*) into records from OPS_PERSONAL_DATA where CARD_SN like strCardSN || '%'; 
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where CARD_SN like strCardSN || '%'
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        else
          select count(*) into records from OPS_PERSONAL_DATA where CARD_SN like strCardSN || '%' and ICCID like strICCID || '%'; 
          open results for
            select * from (
              select
                ICCID,IMSI,SMSP,PIN1,PUK1,PIN2,PUK2,KI,COLLISIOM,FAKE_KI,K,OPC,
                MSISDN,USIM_FLAG,CARD_SN,STATE,MAC_RANDOM,to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') as REQUEST_DATE,
                to_char(FINISH_DATE, 'yyyy-mm-dd hh24:mi:ss') as FINISH_DATE,ROWNUM num
              from OPS_PERSONAL_DATA
              where CARD_SN like strCardSN || '%' and ICCID like strICCID || '%' 
              order by IMSI
            ) where num between temp_minPage and temp_maxPage;
        end if;
      end if;
    end if;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end PersonalDataList;


  procedure PersonalDataAdd(iRecords in number, strCardSN in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    select count(*) into temp_flag from OPS_PERSONAL_DATA_TEMP where CARD_SN=strCardSN;
    if temp_flag <> iRecords then
      delete from OPS_PERSONAL_DATA_TEMP where CARD_SN=strCardSN;
      commit;
      retid  := 2161;
      retmsg  := strCardSN;
      return;
    end if;


    select count(*) into temp_flag from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN;
    if temp_flag > 0 then
      insert into OPS_PERSONAL_DATA select * from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN;
      delete from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN;
      commit;
    end if;


    insert into OPS_PERSONAL_DATA_BUSINESS select * from OPS_PERSONAL_DATA_TEMP where CARD_SN=strCardSN;


    delete from OPS_PERSONAL_DATA_TEMP where CARD_SN=strCardSN;
    commit;


    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        delete from OPS_PERSONAL_DATA_TEMP where CARD_SN=strCardSN;
        commit;
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end PersonalDataAdd;


  procedure PersonalRandomData(strCardSN in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    select COUNT(*) into temp_flag from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN;
    if temp_flag < 1 then
      retid  := 2171;
      retmsg  := '???????['|| strCardSN || ']???????';
      return;
    end if;

    open results for
           select * from (
             select CARD_SN,MAC_RANDOM,REQUEST_DATE,ROWNUM NUM from (
              select * from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN
              order by to_char(REQUEST_DATE, 'yyyy-mm-dd hh24:mi:ss') desc)
            ) where num=1;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end PersonalRandomData;


  procedure PersonalDataUpdate(strSeqNo in varchar2, strCardSN in varchar2, strState in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN and SEQ_NO=strSeqNo;
    if temp_flag < 1 then
      retid  := 2181;
      retmsg  := '????????????[' || strCardSN || ']???';
      return;
    end if;

    update OPS_PERSONAL_DATA_BUSINESS set STATE=strState,FINISH_DATE=sysdate where CARD_SN=strCardSN and SEQ_NO=strSeqNo;


    insert into OPS_PERSONAL_DATA select * from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN and SEQ_NO=strSeqNo;


    delete from OPS_PERSONAL_DATA_BUSINESS where CARD_SN=strCardSN and SEQ_NO=strSeqNo;
    commit;

    retid  := 0;
    retmsg  := '??!';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end PersonalDataUpdate;



  procedure PresetDataAdd(strSeqNo in varchar2, strInOutFlag in varchar2, strCardType in varchar2, strEncKi in varchar2, strEncOpc in varchar2, strLocalProvCode in varchar2, strSignature in varchar2, retid out number, retmsg out varchar2)
  is
  begin
    insert into OPS_PRESET_DATA values (strSeqNo, strEncKi, strEncOpc, strCardType, strLocalProvCode, strInOutFlag, strSignature, sysdate);
    commit;

    retid  := 0;
    retmsg  := '???';

   
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end PresetDataAdd;


  procedure PersonalDataDel(strCardSN in varchar2, strRandomData in varchar2, retid out number, retmsg out varchar2)
  is
  begin
    delete from OPS_PERSONAL_DATA where CARD_SN=strCardSN and MAC_RANDOM=strRandomData;
    commit;

    retid  := 0;
    retmsg  := '??';

    
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end PersonalDataDel;


  procedure ProvinceList(results out cursorType, retid out number, retmsg out varchar2)
  is
  begin
    open results for
      select PROVINCE_CODE,PROVINCE_NAME,PUBKEY_STATUS,PUBKEY_INDEX,PUBKEY_VERSION,NOTES from OPS_PROVINCE;
    retid  := 0;
    retmsg  := '??';

    
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end ProvinceList;


  procedure ProvinceLists(iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    select COUNT(*) into records from OPS_PROVINCE;
    open results for
      select * from (
         select PROVINCE_CODE,PROVINCE_NAME,PUBKEY_STATUS,PUBKEY_INDEX,PUBKEY_VERSION,NOTES,ROWNUM as NUM from (select * from OPS_PROVINCE order by PROVINCE_CODE)
      ) where NUM between temp_minPage and temp_maxPage;

    retid  := 0;
    retmsg  := '??';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end;


  procedure ProvinceInfo(strProvinceCode in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    if temp_flag < 1 then
      retid  := 2221;
      retmsg  := '????????[' || strProvinceCode || ']??????';
      return;
    end if;

    open results for
      select PROVINCE_CODE,PROVINCE_NAME,PUBKEY_STATUS,PUBKEY_INDEX,PUBKEY_VERSION,NOTES from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    retid  := 0;
    retmsg  := '??';

    
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end ProvinceInfo;


  procedure ProvinceAdd(strProvinceCode in varchar2, strProvinceName in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    if temp_flag > 0 then
      retid  := 2231;
      retmsg  := '???' || strProvinceCode || '??????????';
      return;
    end if;

    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_NAME = strProvinceName;
    if temp_flag > 0 then
      retid  := 2232;
      retmsg  := '???' || strProvinceName || '??????????';
      return;
    end if;

    insert into OPS_PROVINCE (PROVINCE_CODE, PROVINCE_NAME, NOTES) values (strProvinceCode, strProvinceName, strNotes);
    commit;

    retid  := 0;
    retmsg  := '???';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end ProvinceAdd;

  procedure ProvinceModify(strProvinceCode in varchar2, strProvinceName in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    if temp_flag < 1 then
      retid  := 2241;
      retmsg  := '??????[' || strProvinceCode || ']??????';
      return;
    end if;
    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_CODE <> strProvinceCode and PROVINCE_NAME = strProvinceName;
    if temp_flag > 1 then
      retid  := 2242;
      retmsg  := '???[' || strProvinceCode || ']??????????';
      return;
    end if;
    update OPS_PROVINCE set PROVINCE_NAME = strProvinceName, NOTES = strNotes where PROVINCE_CODE = strProvinceCode;
    commit;

    retid  := 0;
    retmsg  := '???';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end ProvinceModify;


  procedure ProvinceDel(strProvinceCode in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    if temp_flag < 1 then
      retid  := 2251;
      retmsg  := '??????[' || strProvinceCode || ']??????';
      return;
    end if;

    select PUBKEY_STATUS into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode and ROWNUM = 1;
    if temp_flag <> 0 then
      retid  := 2252;
      retmsg  := '???[' || strProvinceCode || ']?????????';
      return;
    end if;

    delete from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    commit;

    retid  := 0;
    retmsg  := '???';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end ProvinceDel;

  procedure ImprotKey(strProvinceCode in varchar2, iPubKeyIndex in varchar2, iPubKeyVersion in varchar2,retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode;
    if temp_flag < 1 then
      retid  := 2261;
      retmsg  := '??????[' || strProvinceCode || ']??????';
      return;
    end if;

    select PUBKEY_STATUS into temp_flag from OPS_PROVINCE where PROVINCE_CODE = strProvinceCode and ROWNUM = 1;
    if temp_flag = 1 then
      retid  := 2262;
      retmsg  := '???[' || strProvinceCode || ']?????????';
      return;
    end if;

    select count(*) into temp_flag from OPS_PROVINCE where PUBKEY_INDEX = iPubKeyIndex;
    if temp_flag > 0 then
      retid  := 2263;
      retmsg  := '???[' || iPubKeyIndex || ']???????';
      return;
    end if;

    update OPS_PROVINCE set PUBKEY_STATUS =1, PUBKEY_INDEX = iPubKeyIndex, PUBKEY_VERSION = iPubKeyVersion where PROVINCE_CODE = strProvinceCode;
    commit;

    retid  := 0;
    retmsg  := '???';


    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end ImprotKey;

end OPS_CORE;
//
