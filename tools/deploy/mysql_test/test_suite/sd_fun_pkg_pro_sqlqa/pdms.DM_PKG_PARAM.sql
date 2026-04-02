delimiter //;
create or replace package pdms.DM_PKG_PARAM is
  
  type cursorType is ref cursor;

  procedure FactoryList(results out cursorType, retid out number, retmsg out varchar2);

  procedure FactoryInfo(strFactoryCode in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure FactoryAdd(strFactoryCode in varchar2, strFactoryName in varchar2, strContactPerson in varchar2, strContactTel in varchar2, strFaxNum in varchar2, strPortNum in varchar2, strPortAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure FactoryModify(iFactoryID in number, strContactPerson in varchar2, strContactTel in varchar2, strFaxNum in varchar2, strPortNum in varchar2, strPortAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure FactoryDel(iFactoryID in number, strFactoryCode in varchar2, retid out number, retmsg out varchar2);

  procedure RegionList(results out cursorType, retid out number, retmsg out varchar2);

  procedure RegionInfo(strRegionID in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure RegionAdd(strRegionCode in varchar2, strRegionName in varchar2, strRegionShort in varchar2, strSMSP in varchar2, strConPerson in varchar2, strConTel in varchar2, strFaxNum in varchar2, strPostNum in varchar2, strPostAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure RegionModify(iRegionID in number, strConPerson in varchar2, strConTel in varchar2, strFaxNum in varchar2, strPostNum in varchar2, strPostAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure RegionDel(iRegionID in number, strRegionCode in varchar2, retid out number, retmsg out varchar2);

  procedure CardTypeList(iFlag in number, iAttr in number, results out cursorType, retid out number, retmsg out varchar2);

  procedure CardTypeInfo(strCardCode in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure CardTypeAdd(strCardCode in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strSatus in varchar2, iAttr in number, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure CardTypeModify(iCardID in number, strCardCode in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strCardState in varchar2, iAttr in number, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure CardTypeDel(iCardID in number, strCardCode in varchar2, retid out number, retmsg out varchar2);

  procedure HLRList(results out cursorType, retid out number, retmsg out varchar2);

  procedure HLRListByRegionName(strRegionName in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure HLRAdd(strHlrName in varchar2, strHlrCode in varchar2, strRegionName in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure HLRInfo(strHlrName in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure HLRModify(iAttrId in number, strHlrName in varchar2, strHlrCode in varchar2, strRegionCode in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2);

  procedure HLRDel(iAttrId in number, strRegionCode in varchar2, retid out number, retmsg out varchar2);

  procedure CompanyList(results out cursorType, retid out number, retmsg out varchar2);

  procedure CompanyModify(strCompanyName in varchar2, strNewName in varchar2, strContactPerson in varchar2, strContactTel in varchar2, strFaxNum in varchar2, strEMail in varchar2, retid out number, retmsg out varchar2);

  procedure SelConfigByKey(strConfigkey in varchar2,results out cursorType, retid out number, retmsg out varchar2);

  procedure SelConfigByKeyList(strConfigkey in varchar2,results out cursorType, retid out number, retmsg out varchar2);

  procedure ModifyConfigValue(strConfigkey in varchar2,strConfigValue in varchar2,retid out number, retmsg out varchar2);

end DM_PKG_PARAM;
//

create or replace package body pdms.DM_PKG_PARAM is

  procedure FactoryList(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_FACTORY;
    if temp_flag < 1 then
      retid := 2001;
      retmsg  := '无任何卡商信息。';
      return;
    end if;

    open results for
      select FACTORY_ID,FACTORY_CODE,FACTORY_NAME,CONTACT_PERSON,CONTACT_TEL,FAX_NUM,POST_NUM,POST_ADDR,NOTES from DM_FACTORY order by FACTORY_CODE asc ;

    retid := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end FactoryList;


  procedure FactoryInfo(strFactoryCode in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    if length(strFactoryCode) = 1 then
      select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE=strFactoryCode;
      if temp_flag < 1 then
        retid := 2002;
        retmsg  := '卡商代码为[' || strFactoryCode || ']的卡商信息不存在。';
        return;
      end if;

      open results for
        select FACTORY_ID,FACTORY_CODE,FACTORY_NAME,CONTACT_PERSON,CONTACT_TEL,FAX_NUM,POST_NUM,POST_ADDR,NOTES
        from DM_FACTORY where FACTORY_CODE=strFactoryCode;
    else
      select count(*) into temp_flag from DM_FACTORY where FACTORY_NAME=strFactoryCode;
      if temp_flag < 1 then
        retid := 2002;
        retmsg  := '卡商名为[' || strFactoryCode || ']的卡商信息不存在。';
        return;
      end if;

      open results for
        select FACTORY_ID,FACTORY_CODE,FACTORY_NAME,CONTACT_PERSON,CONTACT_TEL,FAX_NUM,POST_NUM,POST_ADDR,NOTES
        from DM_FACTORY where FACTORY_NAME=strFactoryCode;
    end if;

    retid := 0;
    retmsg  := '成功！';
    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end FactoryInfo;

  procedure FactoryAdd(strFactoryCode in varchar2, strFactoryName in varchar2, strContactPerson in varchar2, strContactTel in varchar2, strFaxNum in varchar2, strPortNum in varchar2, strPortAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_FACTORY where FACTORY_CODE=strFactoryCode;
    if temp_flag > 0 then
      retid := 2003;
      retmsg  := '代码为' || strFactoryCode || '的卡商信息已经存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_FACTORY where FACTORY_NAME = strFactoryName;
    if temp_flag > 0 then
      retid := 2004;
      retmsg  := '名称为' || strFactoryName || '的卡商信息已经存在。';
      return;
    end if;

    insert into DM_FACTORY values (SEQ_DM_FACTORY.nextval, strFactoryCode, strFactoryName, strContactPerson, strContactTel, strFaxNum, strPortNum, strPortAddr, strNotes);
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end FactoryAdd;


  procedure FactoryModify(iFactoryID in number, strContactPerson in varchar2, strContactTel in varchar2, strFaxNum in varchar2, strPortNum in varchar2, strPortAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_FACTORY where FACTORY_ID=iFactoryID;
    if temp_flag < 1 then
      retid := 2005;
      retmsg  := '不存在ID为[' || iFactoryID || ']的卡商信息。';
      return;
    end if;

    update DM_FACTORY set CONTACT_PERSON=strContactPerson, CONTACT_TEL=strContactTel, FAX_NUM=strFaxNum, POST_NUM=strPortNum, POST_ADDR=strPortAddr, NOTES=strNotes where FACTORY_ID=iFactoryID;
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end FactoryModify;


  procedure FactoryDel(iFactoryID in number, strFactoryCode in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_FACTORY where FACTORY_ID=iFactoryID and FACTORY_CODE=strFactoryCode;
    if temp_flag < 1 then
      retid := 2006;
      retmsg  := '不存在代码为[' || strFactoryCode || ']的卡商信息。';
      return;
    end if;

    delete from DM_FACTORY where FACTORY_ID=iFactoryID and FACTORY_CODE=strFactoryCode;
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end FactoryDel;

  procedure RegionList(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_REGION;
    if temp_flag < 1 then
      retid := 2007;
      retmsg  := '无任何地市信息。';
      return;
    end if;

    open results for
      select REGION_ID,REGION_CODE,REGION_NAME,REGION_SHORT,SMSP,CONTACT_PERSON,CONTACT_TEL,FAX_NUM,POST_NUM,POST_ADDR,NOTES
      from DM_REGION  order by REGION_CODE asc;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end RegionList;

  procedure RegionInfo(strRegionID in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_REGION where REGION_ID=strRegionID;
    if temp_flag < 1 then
      retid := 2008;
      retmsg  := '地市ID为[' || strRegionID || ']的地市信息不存在。';
      return;
    end if;

    open results for
      select REGION_ID,REGION_CODE,REGION_NAME,REGION_SHORT,SMSP,CONTACT_PERSON,CONTACT_TEL,FAX_NUM,POST_NUM,POST_ADDR,NOTES
      from DM_REGION
      where REGION_ID=strRegionID;

    retid := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end RegionInfo;

  procedure RegionAdd(strRegionCode in varchar2, strRegionName in varchar2, strRegionShort in varchar2, strSMSP in varchar2, strConPerson in varchar2, strConTel in varchar2, strFaxNum in varchar2, strPostNum in varchar2, strPostAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_attrId number := -1;
  begin
    if strRegionCode is null or strRegionName is null or strRegionShort is null or strSMSP is null then
      retid := 2009;
      retmsg  := '输入的信息不全！';
      return;
    end if;
    select COUNT(*) into temp_flag from DM_REGION where REGION_CODE=strRegionCode;
    if temp_flag > 0 then
      retid := 2010;
      retmsg  := '代码为[' || strRegionCode || ']的地市信息已存在。';
      return;
    end if;
    select COUNT(*) into temp_flag from DM_REGION where REGION_NAME=strRegionName;
    if temp_flag > 0 then
      retid := 2011;
      retmsg  := '名称为[' || strRegionName || ']的地市信息已存在。';
      return;
    end if;
    select max(ATTR_ID) into temp_attrId from DM_HLR_LINK;
    insert into DM_REGION values (SEQ_DM_REGION.nextval, strRegionCode, strRegionName, strRegionShort, strSMSP, strConPerson, strConTel, strFaxNum, strPostNum, strPostAddr, strNotes);
    insert into DM_HLR_LINK values(temp_attrId + 1,strRegionName,'0','1','0','');
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end RegionAdd;

  procedure RegionModify(iRegionID in number, strConPerson in varchar2, strConTel in varchar2, strFaxNum in varchar2, strPostNum in varchar2, strPostAddr in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_REGION where REGION_ID=iRegionID;
    if temp_flag < 1 then
      retid := 2012;
      retmsg  := '地市ID为[' || iRegionID || ']的地市信息不存在。';
      return;
    end if;


    update DM_REGION set CONTACT_PERSON=strConPerson,CONTACT_TEL=strConTel,FAX_NUM=strFaxNum,POST_NUM=strPostNum,POST_ADDR=strPostAddr,NOTES=strNotes where REGION_ID=iRegionID;
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end RegionModify;


  procedure RegionDel(iRegionID in number, strRegionCode in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_regionName varchar2(20) := null;
  begin
    select COUNT(*) into temp_flag from DM_REGION where REGION_ID=iRegionID and REGION_CODE=strREGIONCODE;
    if temp_flag < 1 then
      retid := 2013;
      retmsg  := '代码为[' || strRegionCode || '],地市ID为[' || iRegionID || ']的地市信息不存在。';
      return;
    end if;

    select REGION_NAME into temp_regionName from DM_REGION where REGION_ID=iRegionID and REGION_CODE=strREGIONCODE;
    select count(*) into temp_flag from DM_HLR_LINK where PARENT_ID in (select ATTR_ID from DM_HLR_LINK where ATTR_NAME = temp_regionName);
    if temp_flag > 0 then
      retid := 2014;
      retmsg  := '在删除地区之前,请先删除该地区的所有交换机。';
      return;
    end if;

    delete from DM_REGION where REGION_ID=iRegionID and REGION_CODE=strREGIONCODE;
    delete from DM_HLR_LINK where ATTR_NAME = temp_regionName;
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end RegionDel;


  procedure CardTypeList(iFlag in number, iAttr in number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_CARD_TYPE;
    if temp_flag < 1 then
      retid := 2015;
      retmsg  := '系统找不到任何卡类型信息。';
      return;
    end if;

    if iFlag<>DM_PKG_CONST.CARD_TYPE_ALL and iFlag<>DM_PKG_CONST.CARD_TYPE_PORTION then
      retid := 2016;
      retmsg  := '传入的标志位不正确。';
      return;
    end if;

    if iFlag=DM_PKG_CONST.CARD_TYPE_ALL then
      open results for
        select CARD_ID,CARD_CODE,CARD_NAME,CARD_CAPACITY,CARD_STATE,ATTR,NOTES from DM_CARD_TYPE order by CARD_CODE;
    else
      if iAttr<>DM_PKG_CONST.CARD_TYPE_ATTR_XCXK and iAttr<>DM_PKG_CONST.CARD_TYPE_ATTR_LBYK then
        open results for
          select CARD_ID,CARD_CODE,CARD_NAME,CARD_CAPACITY,CARD_STATE,ATTR,NOTES from DM_CARD_TYPE where CARD_STATE=DM_PKG_CONST.CARD_TYPE_STATE_ACTIVE order by CARD_CODE;
      else
        open results for
          select CARD_ID,CARD_CODE,CARD_NAME,CARD_CAPACITY,CARD_STATE,ATTR,NOTES from DM_CARD_TYPE where CARD_STATE=DM_PKG_CONST.CARD_TYPE_STATE_ACTIVE and ATTR=iAttr order by CARD_CODE;
      end if;
    end if;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end CardTypeList;

  procedure CardTypeInfo(strCardCode in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_CARD_TYPE where CARD_CODE=strCardCode;
    if temp_flag < 1 then
      retid := 2017;
      retmsg  := '系统找不到指定卡类型[' || strCardCode || ']信息。';
      return;
    end if;

    open results for
      select CARD_CODE,CARD_NAME,CARD_CAPACITY,CARD_STATE,ATTR,NOTES from DM_CARD_TYPE where CARD_CODE=strCardCode;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end CardTypeInfo;

  procedure CardTypeAdd(strCardCode in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strSatus in varchar2, iAttr in number, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    select count(*) into temp_flag from DM_CARD_TYPE where CARD_CODE=strCardCode;
    if temp_flag > 0 then
      retid := 2018;
      retmsg  := '指定卡类型代码[' || strCardCode || ']已经存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_CARD_TYPE where CARD_NAME=strCardName;
    if temp_flag > 0 then
      retid := 2019;
      retmsg  := '指定卡类型名称[' || strCardName || ']已经存在。';
      return;
    end if;

    insert into DM_CARD_TYPE values (SEQ_DM_CARD_TYPE.nextval, strCardCode, strCardName, strCardCapacity, strSatus, iAttr, strNotes);
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CardTypeAdd;


  procedure CardTypeModify(iCardID in number, strCardCode in varchar2, strCardName in varchar2, strCardCapacity in varchar2, strCardState in varchar2, iAttr in number, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_CARD_TYPE where CARD_CODE=strCardCode and CARD_ID=iCardID;
    if temp_flag < 1 then
      retid := 2020;
      retmsg  := '指定卡类型[' || strCardCode || ']不存在。';
      return;
    end if;

    update DM_CARD_TYPE set CARD_NAME=strCardName, CARD_CAPACITY=strCardCapacity, CARD_STATE=strCardState, ATTR=iAttr, NOTES=strNotes where CARD_CODE=strCardCode and CARD_ID=iCardID;
    commit;

    retid := 0;
    retmsg  := '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CardTypeModify;


  procedure CardTypeDel(iCardID in number, strCardCode in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_CARD_TYPE where CARD_CODE=strCardCode and CARD_ID=iCardID;
    if temp_flag < 1 then
      retid := 2020;
      retmsg  := '指定卡类型[' || strCardCode || ']不存在。';
      return;
    end if;

    delete from DM_CARD_TYPE where CARD_CODE=strCardCode and CARD_ID=iCardID;
    commit;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CardTypeDel;


  procedure HLRList(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_HLR_LINK a, DM_HLR b where a.ATTR_NAME=b.HLR_NAME;
    if temp_flag < 1 then
      retid := 2021;
      retmsg  := '无任何HLR信息。';
      return;
    end if;

    open results for
      select a.ATTR_ID, a.ATTR_NAME, a.PARENT_ID, a.ATTR_LEVEL, a.END_TAG, a.NOTES, b.HLR_NAME, b.HLR_CODE, b.REGION_CODE
      from DM_HLR_LINK a, DM_HLR b where a.ATTR_NAME=b.HLR_NAME order by REGION_CODE asc ;

    retid := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end HLRList;


  procedure HLRListByRegionName(strRegionName in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_regionCode varchar2(5) := -1;
    temp_parentID number := -1;
  begin
    select count(*) into temp_flag from DM_REGION where REGION_NAME=strRegionName;
    if temp_flag < 1 then
      retid := 2022;
      retmsg  := '指定地市['|| strRegionName ||']信息在地市表中不存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_HLR_LINK where ATTR_NAME=strRegionName;
    if temp_flag < 1 then
      retid := 2023;
      retmsg  := '指定地市['|| strRegionName ||']信息在菜单关系表中不存在。';
      return;
    end if;

    select REGION_CODE into temp_regionCode from DM_REGION where REGION_NAME = strRegionName;

    select count(*) into temp_flag from DM_HLR where REGION_CODE=temp_regionCode;
    if temp_flag < 1 then
      retid := 2024;
      retmsg  := '指定地市['|| strRegionName ||']下没有HLR信息。';
      return;
    end if;

    select ATTR_ID into temp_parentID from DM_HLR_LINK where ATTR_NAME=strRegionName;

    open results for
      select a.HLR_NAME,a.HLR_CODE, a.REGION_CODE, b.ATTR_ID, b.ATTR_NAME, b.PARENT_ID, b.ATTR_LEVEL, b.END_TAG, b.NOTES
      from DM_HLR a, DM_HLR_LINK b where a.REGION_CODE=temp_regionCode and a.HLR_NAME=b.ATTR_NAME and b.PARENT_ID=temp_parentID
      order by a.HLR_NAME asc;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end HLRListByRegionName;

  procedure HLRAdd(strHlrName in varchar2, strHlrCode in varchar2, strRegionName in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_regionCode varchar2(5) := null;
    temp_parentId number := -1;
    temp_level varchar2(2) := null;
  begin
    select count(*) into temp_flag from DM_REGION where REGION_NAME = strRegionName;
    if temp_flag < 1 then
      retid := 2028;
      retmsg  := '地市名为[' || strRegionName || ']的信息在地市表中不存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_HLR_LINK where ATTR_NAME = strRegionName;
    if temp_flag < 1 then
      retid := 2027;
      retmsg  := '属性名称为[' || strRegionName || ']的信息在菜单关系表中不存在。';
      return;
    end if;

      select REGION_CODE into temp_regionCode from DM_REGION where REGION_NAME = strRegionName;
      select ATTR_ID,ATTR_LEVEL into temp_parentId,temp_level from DM_HLR_LINK where ATTR_NAME = strRegionName;

    select count(*) into temp_flag from DM_HLR_LINK where ATTR_NAME = strHlrName and PARENT_ID = temp_parentId;
    if temp_flag > 0 then
      retid := 2025;
      retmsg  := 'HLR名为[' || strHlrName || ']的信息在同父类菜单关系表中已经存在。';
      return;
    end if;

      select count(*) into temp_flag from DM_HLR where HLR_NAME = strHlrName and REGION_CODE=temp_regionCode;
    if temp_flag > 0 then
      retid := 2026;
      retmsg  := 'HLR名为[' || strHlrName || ']的信息在同地市HLR表中已经存在。';
      return;
    end if;

    if temp_level <> DM_PKG_CONST.ATTR_LEVEL_1 then
      retid := 2037;
      retmsg  := '父节点[' || strRegionName || ']为叶子节点，不能填加HLR信息。';
      return;
    end if;

    select count(*) into temp_flag from DM_HLR_LINK where PARENT_ID = temp_parentId;
    if temp_flag >= DM_PKG_CONST.ATTR_MAX_NOTE then
      retid := 2038;
      retmsg  := '同一地市[' || strRegionName || ']下的HLR数量不能超过' || DM_PKG_CONST.ATTR_MAX_NOTE || '个。';
      return;
    end if;

    select count(*) into temp_flag from DM_REGION where REGION_NAME = strHlrName;
    if temp_flag > 0 then
      retid := 2039;
      retmsg  := '新增的HLR名称[' || strHlrName || ']不合法，不能与地市名称相同。';
      return;
    end if;


    insert into DM_HLR values (strHlrName, strHlrCode, temp_regionCode);
    insert into DM_HLR_LINK values (SEQ_DM_ATTR.nextval, strHlrName, temp_parentId, DM_PKG_CONST.ATTR_LEVEL_2, DM_PKG_CONST.ATTR_END_TAG_1, strNotes);
    commit;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end HLRAdd;


  procedure HLRInfo(strHlrName in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_HLR_LINK a, DM_HLR b where a.ATTR_NAME=b.HLR_NAME and b.HLR_NAME=strHlrName;
    if temp_flag < 1 then
      retid := 2029;
      retmsg  := '系统找不到指定HLR[' || strHlrName || ']信息。';
      return;
    end if;

    open results for
      select a.ATTR_ID, a.ATTR_NAME, a.PARENT_ID, a.ATTR_LEVEL, a.END_TAG, a.NOTES, b.HLR_NAME, b.HLR_CODE, b.REGION_CODE
      from DM_HLR_LINK a, DM_HLR b where a.ATTR_NAME=b.HLR_NAME and b.HLR_NAME=strHlrName;

    retid := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end HLRInfo;

  procedure HLRModify(iAttrId in number, strHlrName in varchar2, strHlrCode in varchar2, strRegionCode in varchar2, strNotes in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_hlrName varchar2(24) := null;
    temp_parentAttrId number := -1;
  begin

    select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID = iAttrId;
    if temp_flag < 1 then
      retid := 2030;
      retmsg  := '属性ID为[' || iAttrId || ']的信息在菜单关系表中不存在。';
      return;
    end if;

    select ATTR_NAME,PARENT_ID into temp_hlrName,temp_parentAttrId from DM_HLR_LINK where ATTR_ID = iAttrId;
    if temp_parentAttrId = 0 then
      retid := 2042;
      retmsg  := '地市项不能修改。';
      return;
    end if;

      select count(*) into temp_flag from DM_HLR where HLR_NAME = temp_hlrName and REGION_CODE=strRegionCode;
    if temp_flag < 1 then
      retid := 2033;
      retmsg  := 'HLR名称为[' || temp_hlrName || ']的信息在HLR表中不存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_HLR_LINK where ATTR_NAME = strHlrName and ATTR_ID<>iAttrId and PARENT_ID=temp_parentAttrId;
    if temp_flag > 0 then
      retid := 2031;
      retmsg  := '属性名称为[' || strHlrName || ']的信息在同父类菜单关系表中已经存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_HLR where HLR_NAME = strHlrName and HLR_NAME<>temp_hlrName and REGION_CODE=strRegionCode;
    if temp_flag > 0 then
      retid := 2032;
      retmsg  := 'HLR名称为[' || strHlrName || ']的信息在同地市HLR表中已经存在。';
      return;
    end if;

    update DM_HLR_LINK set ATTR_NAME=strHlrName, NOTES=strNotes where ATTR_ID = iAttrId;
    update DM_HLR set HLR_NAME=strHlrName, HLR_CODE=strHlrCode where HLR_NAME = temp_hlrName and REGION_CODE=strRegionCode;

    update DM_BATCH set HLR_NAME=strHlrName where HLR_NAME = temp_hlrName and BATCH_REGION=strRegionCode;
    commit;

    retid := 0;
    retmsg  := '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end HLRModify;

  procedure HLRDel(iAttrId in number, strRegionCode in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
    temp_hlrName varchar2(24) := null;
    temp_parentAttrId number := -1;
  begin
    select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID = iAttrId;
    if temp_flag < 1 then
      retid := 2030;
      retmsg  := '属性ID为[' || iAttrId || ']的信息在菜单关系表中不存在。';
      return;
    end if;

    select ATTR_NAME,PARENT_ID into temp_hlrName,temp_parentAttrId from DM_HLR_LINK where ATTR_ID = iAttrId;
    if temp_parentAttrId = 0 then
      retid := 2042;
      retmsg  := '地市项不能删除。';
      return;
    end if;

      select count(*) into temp_flag from DM_HLR where HLR_NAME = temp_hlrName and REGION_CODE = strRegionCode;
    if temp_flag < 1 then
      retid := 2033;
      retmsg  := 'HLR名称为[' || temp_hlrName || ']的信息在HLR表中不存在。';
      return;
    end if;

    select count(*) into temp_flag from DM_DATA_SEG where ATTR_ID = iAttrId;
    if temp_flag > 0 then
      retid := 2036;
      retmsg  := '指定HLR下有号段信息。';
      return;
    end if;

    select count(*) into temp_flag from DM_HLR_LINK where PARENT_ID = iAttrId;
    if temp_flag > 0 then
      retid := 2040;
      retmsg  := '指定节点下有子节点信息。';
      return;
    end if;

    delete from DM_HLR_LINK where ATTR_ID = iAttrId ;
    delete from DM_HLR where HLR_NAME = temp_hlrName and REGION_CODE = strRegionCode;
    commit;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end HLRDel;


  procedure CompanyList(results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_COMPANY;
    if temp_flag < 1 then
      retid := 2042;
      retmsg  := '无任何公司信息。';
      return;
    end if;

    open results for
      select COMPANY_NAME,CONTACT_PERSON,CONTACT_TEL,FAX_NUM,E_MAIL from DM_COMPANY;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end CompanyList;


  procedure CompanyModify(strCompanyName in varchar2, strNewName in varchar2, strContactPerson in varchar2, strContactTel in varchar2, strFaxNum in varchar2, strEMail in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from DM_COMPANY where COMPANY_NAME<>strCompanyName and COMPANY_NAME=strNewName;
    if temp_flag > 0 then
      retid := 2043;
      retmsg  := '名称为[' || strNewName || ']的公司信息已经存在。';
      return;
    end if;

    update DM_COMPANY set COMPANY_NAME=strNewName, CONTACT_PERSON=strContactPerson, CONTACT_TEL=strContactTel, FAX_NUM=strFaxNum, E_MAIL=strEMail where COMPANY_NAME=strCompanyName;
    commit;

    retid := 0;
    retmsg  := '成功!';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end CompanyModify;


  procedure SelConfigByKey(strConfigkey in varchar2,results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from dm_sys_config t where t.config_key=strConfigkey;
    if temp_flag < 1 then
      retid := 2021;
      retmsg  := '未找到键为['|| strConfigkey ||']的配置信息。';
      return;
    end if;

    open results for
         select t.config_key,t.config_value,to_char(t.create_time, 'yyyy-MM-dd HH:mm:ss') create_time from dm_sys_config t where t.config_key=strConfigkey;

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end SelConfigByKey;


  procedure SelConfigByKeyList(strConfigkey in varchar2,results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from dm_sys_config t where t.config_key like '%'||strConfigkey||'%';
    if temp_flag < 1 then
      retid := 2021;
      retmsg  := '未找到键为['|| strConfigkey ||']的配置信息。';
      return;
    end if;

    open results for
         select t.config_key,t.config_value,to_char(t.create_time, 'yyyy-MM-dd HH:mm:ss') create_time from dm_sys_config t
         where t.config_key like '%'||strConfigkey||'%';

    retid := 0;
    retmsg  := '成功！';


    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end SelConfigByKeyList;


  procedure ModifyConfigValue(strConfigkey in varchar2,strConfigValue in varchar2,retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select count(*) into temp_flag from dm_sys_config t where t.config_key = strConfigkey;
    if temp_flag < 1 then
      retid := 2021;
      retmsg  := '未找到键为['|| strConfigkey ||']的配置信息。';
      return;
    end if;

    update dm_sys_config t set t.config_value=strConfigValue,t.create_time=sysdate where t.config_key = strConfigkey;
    commit;
    retid := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid := sqlcode;
        retmsg  := sqlerrm;
  end ModifyConfigValue;

end DM_PKG_PARAM;
//

delimiter ;//