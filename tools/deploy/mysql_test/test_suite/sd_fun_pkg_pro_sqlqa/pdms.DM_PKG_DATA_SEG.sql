delimiter //;
create or replace package pdms.DM_PKG_DATA_SEG is

  type cursorType is ref cursor;


  procedure DataSegList(iAttrId in number, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2);


  procedure DataSegAdd(strDataSeg in varchar2, iAttrId in number, strNotes in varchar2,retid out number, retmsg out varchar2);


  procedure DataSegImport(strDataSeg in varchar2, arrDataSeg in ARRAY, retid out number, retmsg out varchar2);


  procedure DataSegModify(strDataSeg in varchar2, strNotes in varchar2,retid out number, retmsg out varchar2);

  procedure DataSegDel(strDataSeg in varchar2, retid out number, retmsg out varchar2);

  procedure DataSegInfo(strDataSeg in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure IMSISegList(strDataSeg in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure IMSISegInfo(strIMSIseg in varchar2, results out cursorType, retid out number, retmsg out varchar2);

  procedure IMSISegGenerate(strDataSeg in varchar2, strIMSIseg out varchar2, retid out number, retmsg out varchar2);

  procedure IMSIStartInfo(strIMSISeg in varchar2, strIMSIStart out varchar2, strMSISDNStart out varchar2, retid out number, retmsg out varchar2);

  procedure DataSegCount(results OUT cursorType, retid out number, retmsg out varchar2);

  procedure DataSegImport2(strNetSeg in varchar ,strBatchNum in varchar2,retid out number, retmsg out varchar2);

  procedure checkMsisdnBatchFile(strBatchNum in varchar2,strFileName in varchar2, strStatus in number,retid out number, retmsg out varchar2);

  procedure checkMsisdnBatchStatus(strBatchNum in varchar2,batchFileName in varchar2,fileStatus in varchar2,results out cursorType,retid out number, retmsg out varchar2);

  procedure selRegionHlrSegInfo(strRegionName in varchar2,strNetSeg in varchar2,results OUT cursorType,retid out number, retmsg out varchar2);

  procedure saveOrUpdateInfo(strBatchNum in varchar2,batchFileName in varchar2,fileStatus in varchar2,batchCount in varchar2,strNotes in varchar2,retid out number, retmsg out varchar2);
  
  function postgresql(p_tname in varchar2, p_colname in varchar2) return varchar2;

end DM_PKG_DATA_SEG;
//
create or replace package body pdms.DM_PKG_DATA_SEG is
  
  procedure DataSegList(iAttrId in number, iPage in number, iPageSize in number, records out number, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_minPage number(11) := -1;
    temp_maxPage number(11) := -1;
  begin
    temp_minPage := ((iPage - 1) * iPageSize) + 1;
    temp_maxPage := iPage * iPageSize;

    select COUNT(*) into records from DM_DATA_SEG where ATTR_ID=iAttrId;
    open results for
      select * from (
        select DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES,NEW_CARD,REPLACE_CARD,ROWNUM as NUM
        from (
          select a.DATA_SEG,a.ATTR_ID,a.MSISDN_COUNT,a.DATA_SEG_STATE,a.NOTES,
            (select SUM(b.IMSI_COUNT) from DM_IMSI b where b.DATA_SEG=a.DATA_SEG and b.imsi_type=0) as NEW_CARD,
            (select SUM(b.IMSI_COUNT) from DM_IMSI b where b.DATA_SEG=a.DATA_SEG and b.imsi_type=1) as REPLACE_CARD
          from DM_DATA_SEG a where ATTR_ID=iAttrId
          order by DATA_SEG asc)
      ) where NUM between temp_minPage and temp_maxPage;

    retid  := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end DataSegList;

 
  procedure DataSegAdd(strDataSeg in varchar2, iAttrId in number, strNotes in varchar2,retid out number, retmsg out varchar2)
  is
    temp_flag number := 0;
    temp_exists number := 0;
    temp_imsi varchar2(100);
    imsi_seg varchar2(11);
    strIMSIType varchar2(1);
    i number := 0;
    results VT;
    lenght varchar2(11);
  begin

    if length(strDataSeg)<>7 then 
      retid  := 3001;
      retmsg  := '输入的号段不正确！';
      return;
    end if;

    if substr(strDataSeg,1,2)= '13' and substr(strDataSeg,3,1) between '4' and '9' then
      temp_flag := 1;
    elsif substr(strDataSeg,1,2)= '15' and (substr(strDataSeg,3,1) between '0' and '2' or substr(strDataSeg,3,1) between '7' and '9' )then
      temp_flag := 1;
    elsif substr(strDataSeg,1,2)= '18' and (substr(strDataSeg,3,1) between '2' and '4' or substr(strDataSeg,3,1) between '7' and '8' )then
      temp_flag := 1;
    elsif substr(strDataSeg,1,3)= '147' then
      temp_flag := 1;
    elsif substr(strDataSeg,1,3)= '178' then
      temp_flag := 1;
    
    elsif substr(strDataSeg,1,3)= '170' then 
      temp_flag := 1;
    elsif substr(strDataSeg,1,3)= '172' then 
      temp_flag := 1;
    elsif substr(strDataSeg,1,3)= '198' then 
      temp_flag := 1;
    elsif substr(strDataSeg,1,3)= '165' then 
      temp_flag := 1;
    elsif substr(strDataSeg,1,3)= '195' then
      temp_flag := 1;
    else
          temp_flag := 0;
        end if;
        if temp_flag = 0 then
          retid  := 3002;
          retmsg  := '插入的号段数据不合法！';
          return;
        end if;

    select count(*) into temp_exists from DM_DATA_SEG where DATA_SEG = strDataSeg;
    if temp_exists > 0 then
          retid  := 3003;
          retmsg  := '插入的号段数据已经存在！';
          return;
        end if;

    select count(*) into temp_exists from DM_HLR_LINK where ATTR_ID = iAttrId and ATTR_LEVEL = DM_PKG_CONST.ATTR_LEVEL_2;
    if temp_exists < 1 then
          retid  := 3004;
          retmsg  := '插入的属性标识不合法！';
          return;
        end if;

      
        DM_PKG_DATA_SEG.IMSISegGenerate(strDataSeg, temp_imsi, retid, retmsg);
        if retid <> 0 then
    
          return;
        end if;

   
     if substr(strDataSeg,1,3)= '195' then
      insert into DM_DATA_SEG(DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES)
    values(strDataSeg, iAttrId, DM_PKG_CONST.MSISDN_195_UNUSED_COUNT, DM_PKG_CONST.DATA_SEG_STATE_UNUSED, strNotes);
    else
      insert into DM_DATA_SEG(DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES)
    values(strDataSeg, iAttrId, DM_PKG_CONST.MSISDN_UNUSED_COUNT, DM_PKG_CONST.DATA_SEG_STATE_UNUSED, strNotes);
    end if;
   
    if substr(strDataSeg,1,3)= '195' then
      DM_PKG_UTILS.stringToArray2(temp_imsi,results,lenght);
      for i in results.first..results.last loop
        strIMSIType := DM_PKG_CONST.IMSI_TYPE_REPLACE;
        imsi_seg := results(i);
        insert into DM_IMSI(IMSI_SEG, IMSI_COUNT, DATA_SEG, IMSI_TYPE, STATUS)
        values(imsi_seg, DM_PKG_CONST.IMSI_UNUSED_COUNT, strDataSeg, strIMSIType, DM_PKG_CONST.IMSI_STATE_UNUSED);
      end loop;

    else
      for i in 0..9 loop
     
        strIMSIType := DM_PKG_CONST.IMSI_TYPE_REPLACE;
        imsi_seg := temp_imsi || TO_CHAR(i);
        insert into DM_IMSI(IMSI_SEG, IMSI_COUNT, DATA_SEG, IMSI_TYPE, STATUS)
        values(imsi_seg, DM_PKG_CONST.IMSI_UNUSED_COUNT, strDataSeg, strIMSIType, DM_PKG_CONST.IMSI_STATE_UNUSED);
      end loop;
    end if;

    commit;

    retid  := 0;
    retmsg  := '成功。';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end DataSegAdd;

  
  procedure DataSegImport(strDataSeg in varchar2, arrDataSeg in ARRAY, retid out number, retmsg out varchar2)
    is
      temp_flag number := 0;
      temp_attrId number := -1;
      temp_imsi varchar2(100);
      imsi_seg varchar2(11);
      strIMSIType varchar2(1);
      temp_arr VT := VT();
      temp_arr_attrId NT := NT();
      results VT;
      lenght varchar2(11);
    begin

      if strDataSeg between '134' and '139' then
        temp_flag := 1;
      elsif strDataSeg between '150' and '152' or ( strDataSeg  between '157' and '159' )then
        temp_flag := 1;
      elsif strDataSeg between '182' and '184' or ( strDataSeg  between '187' and '188' )then
        temp_flag := 1;
      elsif strDataSeg = '147' then
        temp_flag := 1;
      elsif strDataSeg = '178' then
        temp_flag := 1;
      elsif strDataSeg = '170' then 
        temp_flag := 1;
      elsif strDataSeg = '172' then 
        temp_flag := 1;
      elsif strDataSeg = '198' then 
        temp_flag := 1;
      elsif strDataSeg = '165' then 
        temp_flag := 1;
      elsif strDataSeg = '195' then
        temp_flag := 1;
      else
        temp_flag := 0;
      end if;
      if temp_flag = 0 then
        retid  := 3002;
        retmsg  := '插入的号段数据不合法！';
        return;
      end if;


      for i in arrDataSeg.first..arrDataSeg.last loop
       
        select count(*) into temp_flag from DM_HLR_LINK
        where PARENT_ID = (select ATTR_ID from DM_HLR_LINK where ATTR_NAME = temp_arr(1))
        and ATTR_NAME = strDataSeg;
        if temp_flag < 1 then
          retid  := 3004;
          retmsg := '地区[' || temp_arr(1) || ']菜单号段[' || strDataSeg || ']错误或不存在，请在地市参数管理中核对！';
          return;
        end if;

        if temp_arr.last > 1 then
            for j in 2..temp_arr.last loop

              if length(temp_arr(j))<>7 or substr(temp_arr(j),1,3)<>strDataSeg then
                retid  := 3001;
                retmsg := '输入的号段[' || temp_arr(j) ||']不正确！';
                return;
              end if;
              select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=temp_arr(j);
              if temp_flag > 0 then
                retid  := 3003;
                retmsg := '插入的号段[' || temp_arr(j) ||']已经存在！';
                return;
              end if;
            end loop;
          end if;

          select ATTR_ID into temp_attrId from DM_HLR_LINK
          where PARENT_ID = (select ATTR_ID from DM_HLR_LINK where ATTR_NAME = temp_arr(1))
          and ATTR_NAME = strDataSeg;
          temp_arr_attrId.Extend;
          temp_arr_attrId(i) := temp_attrId;
      end loop;


      for i in arrDataSeg.first..arrDataSeg.last loop
        if temp_arr.last > 1 then
            for j in 2..temp_arr.last loop
              select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=temp_arr(j);
              if temp_flag > 0 then
                retid  := 3003;
                retmsg := '插入的号段[' || temp_arr(j) ||']已经存在！';
                rollback;
                return;
              end if;

              DM_PKG_DATA_SEG.IMSISegGenerate(temp_arr(j), temp_imsi, retid, retmsg);
              if retid <> 0 then

                rollback;
                return;
              end if;

              if strDataSeg = '195' then
                insert into DM_DATA_SEG(DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES)
                values(temp_arr(j), temp_arr_attrId(i), DM_PKG_CONST.MSISDN_195_UNUSED_COUNT, DM_PKG_CONST.DATA_SEG_STATE_UNUSED, '');
              else
                insert into DM_DATA_SEG(DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES)
                values(temp_arr(j), temp_arr_attrId(i), DM_PKG_CONST.MSISDN_UNUSED_COUNT, DM_PKG_CONST.DATA_SEG_STATE_UNUSED, '');
              end if;

              if strDataSeg = '195' then
                DM_PKG_UTILS.stringToArray2(temp_imsi,results,lenght);
                for i in results.first..results.last loop
                  strIMSIType := DM_PKG_CONST.IMSI_TYPE_REPLACE;
                  imsi_seg := results(i);
                  insert into DM_IMSI(IMSI_SEG, IMSI_COUNT, DATA_SEG, IMSI_TYPE, STATUS)
                  values(imsi_seg, DM_PKG_CONST.IMSI_UNUSED_COUNT, temp_arr(j), strIMSIType, DM_PKG_CONST.IMSI_STATE_UNUSED);
                end loop;
              else
                for k in 0..9 loop
               
                  strIMSIType := DM_PKG_CONST.IMSI_TYPE_REPLACE;
                  imsi_seg := temp_imsi || TO_CHAR(k);
                  insert into DM_IMSI(IMSI_SEG, IMSI_COUNT, DATA_SEG, IMSI_TYPE, STATUS)
                  values(imsi_seg, DM_PKG_CONST.IMSI_UNUSED_COUNT, temp_arr(j), strIMSIType, DM_PKG_CONST.IMSI_STATE_UNUSED);
                end loop;
              end if;
            end loop;
          end if;
      end loop;
      commit;

      retid := 0;
      retmsg := '导入成功';

      exception
        when others then
          retid := sqlcode;
          retmsg := sqlerrm;
          rollback;
    end DataSegImport;

 
  procedure DataSegModify(strDataSeg in varchar2, strNotes in varchar2,retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=strDataSeg;
    if temp_flag < 1 then
      retid  := 3005;
      retmsg  := '指定的号段[' || strDataSeg || ']不存在。';
      return;
    end if;

  

    update DM_DATA_SEG set NOTES=strNotes where DATA_SEG=strDataSeg;
    commit;

    retid  := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end DataSegModify;

  
  procedure DataSegDel(strDataSeg in varchar2, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=strDataSeg;
    if temp_flag < 1 then
      retid  := 3006;
      retmsg  := '指定的号段[' || strDataSeg || ']不存在。';
      return;
    end if;

    select DATA_SEG_STATE into temp_flag from DM_DATA_SEG where DATA_SEG=strDataSeg;
    if temp_flag <> DM_PKG_CONST.DATA_SEG_STATE_UNUSED then
      retid  := 3007;
      retmsg  := '指定的号段[' || strDataSeg || ']已经使用，不能删除。';
      return;
    end if;

    select count(*) into temp_flag from DM_BATCH where substr(MSISDN_START, 1, 7) = strDataSeg;
    if temp_flag > 0 then
      retid  := 3008;
      retmsg  := '指定的号段[' || strDataSeg || ']已经生成批次，不能删除。';
      return;
    end if;

    delete from DM_IMSI where DATA_SEG=strDataSeg;
    delete from DM_DATA_SEG where DATA_SEG=strDataSeg;
    commit;

    retid  := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
        rollback;
  end DataSegDel;


  procedure DataSegInfo(strDataSeg in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin

    if length(strDataSeg) = 7 then
      select COUNT(*) into temp_flag from DM_DATA_SEG a, DM_IMSI b where a.DATA_SEG=strDataSeg and a.DATA_SEG=b.DATA_SEG;
      if temp_flag < 1 then
        retid  := 3009;
        retmsg  := '号段[' || strDataSeg || ']信息不存在。';
        return;
      end if;
      open results for
        select a.DATA_SEG,a.ATTR_ID,a.MSISDN_COUNT,a.DATA_SEG_STATE,a.NOTES,
          (select SUM(b.IMSI_COUNT) from DM_IMSI b where b.DATA_SEG=a.DATA_SEG and b.imsi_type=0) as NEW_CARD,
          (select SUM(b.IMSI_COUNT) from DM_IMSI b where b.DATA_SEG=a.DATA_SEG and b.imsi_type=1) as REPLACE_CARD,
          (select d.ATTR_NAME from DM_HLR_LINK d where d.ATTR_ID=c.PARENT_ID) as REGION_NAME,
          c.ATTR_NAME
        from DM_DATA_SEG a, DM_HLR_LINK c where a.ATTR_ID=c.ATTR_ID and a.DATA_SEG=strDataSeg;

    elsif length(strDataSeg) = 11 then
        select COUNT(*) into temp_flag from DM_DATA_SEG a, DM_IMSI b where a.DATA_SEG=b.DATA_SEG and b.IMSI_SEG=strDataSeg;
          if temp_flag < 1 then
          retid  := 3009;
          retmsg  := 'IMSI段[' || strDataSeg || ']信息不存在。';
          return;
        end if;
        open results for
        select a.DATA_SEG,a.ATTR_ID,a.MSISDN_COUNT,a.DATA_SEG_STATE,a.NOTES,
          (select SUM(b.IMSI_COUNT) from DM_IMSI b where b.DATA_SEG=a.DATA_SEG and b.imsi_type=0) as NEW_CARD,
          (select SUM(b.IMSI_COUNT) from DM_IMSI b where b.DATA_SEG=a.DATA_SEG and b.imsi_type=1) as REPLACE_CARD,
          (select d.ATTR_NAME from DM_HLR_LINK d where d.ATTR_ID=c.PARENT_ID) as REGION_NAME,
          c.ATTR_NAME
        from DM_DATA_SEG a, DM_HLR_LINK c, DM_IMSI e where a.ATTR_ID=c.ATTR_ID and a.DATA_SEG=e.DATA_SEG and e.IMSI_SEG=strDataSeg;
     else
         retid  := 3009;
         retmsg  := '输入的号段或IMSI段[' || strDataSeg || ']非法。';
     end if;

    retid  := 0;
    retmsg  := '成功!';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end DataSegInfo;

  
  procedure IMSISegList(strDataSeg in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
  begin
    open results for
      select IMSI_SEG,IMSI_COUNT,DATA_SEG,IMSI_TYPE,STATUS from DM_IMSI where DATA_SEG=strDataSeg order by IMSI_SEG asc ;

    retid  := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end IMSISegList;

  
  procedure IMSISegInfo(strIMSIseg in varchar2, results out cursorType, retid out number, retmsg out varchar2)
  is
    temp_flag number := -1;
  begin
    select COUNT(*) into temp_flag from DM_DATA_SEG a, DM_IMSI b where b.IMSI_SEG=strIMSIseg and a.DATA_SEG=b.DATA_SEG;
    if temp_flag < 1 then
      retid  := 3010;
      retmsg  := 'IMSI段[' || strIMSIseg || ']信息不存在。';
      return;
    end if;

    open results for
      select a.DATA_SEG,a.ATTR_ID,a.MSISDN_COUNT,a.DATA_SEG_STATE,a.NOTES,b.IMSI_SEG,b.IMSI_COUNT,b.IMSI_TYPE,b.STATUS
      from DM_DATA_SEG a, DM_IMSI b where b.IMSI_SEG=strIMSIseg and a.DATA_SEG=b.DATA_SEG
      order by DATA_SEG asc;

    retid  := 0;
    retmsg  := '成功！';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end IMSISegInfo;

  
  procedure IMSISegGenerate(strDataSeg in varchar2, strIMSIseg out varchar2, retid out number, retmsg out varchar2)
  is
  begin

    if substr(strDataSeg,1,3) = '134' then
      strIMSIseg:='46002' || '0' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '159' then
      strIMSIseg:='46002' || '9' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '158' then
      strIMSIseg:='46002' || '8' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '150' then
      strIMSIseg:='46002' || '3' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '151' then
      strIMSIseg:='46002' || '1' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '152' then
      strIMSIseg:='46002' || '2' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '182' then
      strIMSIseg:='46002' || '6' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '187' then
      strIMSIseg:='46002' || '7' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '157' and substr(strDataSeg,4,1) <> '9' then
      strIMSIseg:='46007' || '7' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '157' and substr(strDataSeg,4,1) = '9' then
      strIMSIseg:='46008' || '7' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '188' then
      strIMSIseg:='46007' || '8' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '147' then
      strIMSIseg:='46007' || '9' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '178' then
      strIMSIseg:='46007' || '5' || substr(strDataSeg,4,4);

    elsif substr(strDataSeg,1,3) = '170' then
      strIMSIseg:='46007' || '0' ||  substr(strDataSeg,4,4); 
    elsif substr(strDataSeg,1,3) = '198' then
      strIMSIseg:='46007' || '1' ||  substr(strDataSeg,4,4); 
    elsif substr(strDataSeg,1,3) = '172' then
      strIMSIseg:='46007' || '2' ||  substr(strDataSeg,4,4); 
    elsif substr(strDataSeg,1,3) = '165' then
      strIMSIseg:='46007' || '3' ||  substr(strDataSeg,4,4); 
    elsif substr(strDataSeg,1,3) = '183' then
       strIMSIseg:='46002' || '5' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,3) = '184' then
       strIMSIseg:='46002' || '4' || substr(strDataSeg,4,4);
    elsif substr(strDataSeg,1,2)= '13' and substr(strDataSeg,3,1) between '5' and '9' and substr(strDataSeg,4,1)= '0' then
      strIMSIseg:='46000' || substr(strDataSeg,5,3) || substr(strDataSeg,3,1) || '0';
    elsif substr(strDataSeg,1,2)= '13' and substr(strDataSeg,3,1) between '5' and '9' and substr(strDataSeg,4,1)<>'0' then
      strIMSIseg:='46000' || substr(strDataSeg,5,3) || (substr(strDataSeg,3,1)-5) || substr(strDataSeg,4,1);
    elsif substr(strDataSeg,1,3) = '195' then
      strIMSIseg:='46007' || '44' || substr(strDataSeg,4,4) || ',' || '46007' || '45' || substr(strDataSeg,4,4);
    else
      retid := 3011;
      retmsg := '输入的号段不正确！';
      return;
    end if;

    retid  := 0;
    retmsg := '成功！';


    exception
      when others then
        retid  := sqlcode;
        retmsg := sqlerrm;
  end IMSISegGenerate;


  
  procedure IMSIStartInfo(strIMSISeg in varchar2, strIMSIStart out varchar2, strMSISDNStart out varchar2, retid out number, retmsg out varchar2)
  is
    temp_dataSeg varchar2(7) := null;
    temp_imsi_status number := -1;
    temp_imsi_count number := -1;
    temp_str varchar2(4) := null;
    temp_flag number := -1;
  begin
   
    select count(*) into temp_flag from DM_IMSI where IMSI_SEG=strIMSISeg;
    if temp_flag < 1 then
      retid  := 3014;
      retmsg  := 'IMSI段[' || strIMSISeg || ']信息不存存在！';
      return;
    end if;

  
    select DATA_SEG, STATUS, IMSI_COUNT into temp_dataSeg, temp_imsi_status, temp_imsi_count from DM_IMSI where IMSI_SEG=strIMSISeg;

  

    if temp_imsi_status = 0 then
      strIMSIStart := strIMSISeg || '0000';
    elsif temp_imsi_status = 1 then
      temp_str := to_char(10000 - to_number(temp_imsi_count));
      temp_str := DM_PKG_UTILS.padding(temp_str,'0',4,'L');
      strIMSIStart := strIMSISeg || temp_str;
    end if;

    strMSISDNStart := temp_dataSeg || '0000';

    retid  := 0;
    retmsg  := '成功!';

   
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end IMSIStartInfo;


  procedure DataSegCount(results OUT cursorType, retid out number, retmsg out varchar2)
    is
  begin
    OPEN results FOR
      select m.attr_name,substr(s.DATA_SEG,1,3) as net_seg, postgresql('DM_DATA_SEG',s.DATA_SEG) as lastFourDataSeg
      from  DM_DATA_SEG S,DM_HLR_LINK d,DM_HLR_LINK m
      where s.attr_id=d.attr_id
      and d.parent_id=m.attr_id
      group by m.ATTR_NAME,substr(s.DATA_SEG,1,3),postgresql('DM_DATA_SEG',s.DATA_SEG);
    retid  := 0;
    retmsg  := '导出成功';
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;  
  end DataSegCount;

  
 procedure DataSegImport2(strNetSeg in varchar ,strBatchNum in varchar2,retid out number, retmsg out varchar2)
 is
      temp_flag number := 0;
      temp_attrId number := -1;
      temp_imsi varchar2(100);
      imsi_seg varchar2(11);
      strIMSIType varchar2(1);
     
      temp_cursor       cursorType := null;
      tempRregionName   VARCHAR2(15):= null;
      tempRegionCode    VARCHAR2(15):= null;
      tempHlrName       VARCHAR2(15):= null;
      tempHlrCode       VARCHAR2(15):= null;
      tempDataSeg       VARCHAR2(7):= null;
      tempAttrId        VARCHAR2(5):= null;
      tempDataType      VARCHAR2(3):= null;
 begin
      
      if strNetSeg between '134' and '139' then
        temp_flag := 1;
      elsif strNetSeg between '150' and '152' or ( strNetSeg  between '157' and '159' )then
        temp_flag := 1;
      elsif strNetSeg between '182' and '184' or ( strNetSeg  between '187' and '188' )then
        temp_flag := 1;
      elsif strNetSeg = '147' then
        temp_flag := 1;
      elsif strNetSeg = '178' then
        temp_flag := 1;
      elsif strNetSeg = '170' then 
        temp_flag := 1;
      elsif strNetSeg = '172' then 
        temp_flag := 1;
      elsif strNetSeg = '198' then 
        temp_flag := 1;
      elsif strNetSeg = '165' then 
        temp_flag := 1;
      elsif strNetSeg = '195' then
        temp_flag := 1;
      else
        temp_flag := 0;
      end if;
      if temp_flag = 0 then
        retid  := 3002;
        retmsg  := '插入的号段数据不合法！';
        return;
      end if;

     
      open temp_cursor for select region_name,region_code,hlr_name,hlr_code,data_seg,attr_id,data_type from
           dm_data_seg_temp where data_seg_seq= strBatchNum and hlr_code = strNetSeg;
         if  temp_cursor %isopen then
         loop
         fetch temp_cursor into tempRregionName,tempRegionCode,tempHlrName,tempHlrCode,tempDataSeg,tempAttrId,tempDataType;
              exit when temp_cursor %notfound;

              select count(*) into temp_flag from DM_HLR_LINK
              where PARENT_ID = (select ATTR_ID from DM_HLR_LINK where ATTR_NAME = tempRregionName) and ATTR_NAME = tempHlrName;
              if temp_flag < 1 then
                 retid  := 3004;
                 retmsg := '地区[' || tempRregionName || ']菜单号段[' || strNetSeg || ']错误或不存在，请在地市参数管理中核对！';
              return;
              end if;
             
              select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=tempDataSeg;
              if temp_flag > 0 then
                 retid  := 3003;
                 retmsg := '插入的号段[' || tempDataSeg ||']已经存在！';
                 return;
              end if;

              select ATTR_ID into temp_attrId from DM_HLR_LINK
              where PARENT_ID = (select ATTR_ID from DM_HLR_LINK where ATTR_NAME = tempRregionName) and ATTR_NAME = tempHlrName;
              if temp_attrId<>tempAttrId then
                 retid  := 3003;
                 retmsg := '插入的网段的标识[' || tempHlrName ||']的标识位不正确！';
                 return;
              end if;

             
              DM_PKG_DATA_SEG.IMSISegGenerate(tempDataSeg, temp_imsi, retid, retmsg);
              if retid <> 0 then
               
                rollback;
                return;
              end if;
              
              if strNetSeg = '195' then
                insert into DM_DATA_SEG(DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES)
                values(tempDataSeg, tempAttrId, DM_PKG_CONST.MSISDN_195_UNUSED_COUNT, DM_PKG_CONST.DATA_SEG_STATE_UNUSED, '批量导入');
              else
                insert into DM_DATA_SEG(DATA_SEG,ATTR_ID,MSISDN_COUNT,DATA_SEG_STATE,NOTES)
                values(tempDataSeg,tempAttrId, DM_PKG_CONST.MSISDN_UNUSED_COUNT, DM_PKG_CONST.DATA_SEG_STATE_UNUSED, '批量导入');
              end if;
              
              if strNetSeg = '195' then
                for i in 0..1 loop
                  strIMSIType := DM_PKG_CONST.IMSI_TYPE_REPLACE;
                  imsi_seg := temp_imsi || TO_CHAR(i);
                  insert into DM_IMSI(IMSI_SEG, IMSI_COUNT, DATA_SEG, IMSI_TYPE, STATUS)
                  values(imsi_seg, DM_PKG_CONST.IMSI_UNUSED_COUNT, tempDataSeg, strIMSIType, DM_PKG_CONST.IMSI_STATE_UNUSED);
                end loop;
              else
                for k in 0..9 loop
                
                  strIMSIType := DM_PKG_CONST.IMSI_TYPE_REPLACE;
				  dbms_output.put_line(temp_imsi || TO_CHAR(k));
                  imsi_seg := temp_imsi || TO_CHAR(k);
                  insert into DM_IMSI(IMSI_SEG, IMSI_COUNT, DATA_SEG, IMSI_TYPE, STATUS)
                  values(imsi_seg, DM_PKG_CONST.IMSI_UNUSED_COUNT,tempDataSeg,strIMSIType,DM_PKG_CONST.IMSI_STATE_UNUSED);
                end loop;
              end if;
         end loop;
        end if;
        delete from dm_data_seg_temp;
      commit;

      retid := 0;
      retmsg := '导入成功';

   
    exception
        when others then
          retid := sqlcode;
          retmsg := sqlerrm;
          delete from dm_data_seg_temp;
          commit;
        rollback;
  end DataSegImport2;


 
  procedure checkMsisdnBatchFile(strBatchNum in varchar2,strFileName in varchar2, strStatus in number,retid out number, retmsg out varchar2)
    is
    temp_flag number := -1;
  begin
     
     select count(*) into temp_flag from DM_DATASEG_BATCH t where ROUND(TO_NUMBER(sysdate - t.data_batch_time)* 24) > 24;
     if temp_flag>0 then
      
        delete DM_DATASEG_BATCH t where t.data_batch_file is null and STATUS = 0
        and ROUND(TO_NUMBER(sysdate - t.data_batch_time)* 24) > 24;
       
        update DM_DATASEG_BATCH t set t.data_batch_time=sysdate,t.data_batch_num=strBatchNum,STATUS = 4
        where ROUND(TO_NUMBER(sysdate - t.data_batch_time)* 24) > 24 and STATUS=1;
        commit;
     end if;

   
     select count(1) into temp_flag from DM_DATASEG_BATCH r where status = strStatus ;
     if temp_flag > 0 then
        retid  := -1;
        retmsg  := '有文件正在执行导入操作，请稍后...';
        return;
     end if;

     select count(1) into temp_flag from DM_DATASEG_BATCH r where DATA_BATCH_FILE =strFileName;
     if temp_flag > 0 then
         select count(1) into temp_flag from DM_DATASEG_BATCH r where DATA_BATCH_FILE =strFileName and r.status=3;
            if temp_flag > 0 then
            retid  := -1;
            retmsg  := '文件名已成功导入.请勿重复导入！';
            return;
         else
           update DM_DATASEG_BATCH r set r.status = 0 ,r.data_batch_time=sysdate where DATA_BATCH_FILE =strFileName  and r.status<>3;
        end if;
     end if;
     if strBatchNum is not null then
      
       select count(1) into temp_flag from DM_DATASEG_BATCH r where DATA_BATCH_NUM =strBatchNum;
        if temp_flag < 1 then
          insert into DM_DATASEG_BATCH(DATA_BATCH_NUM,STATUS)values(strBatchNum,0);
        end if;
     end if;
    commit;

    retid  := 0;
    retmsg  := '校验成功';

   
    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end checkMsisdnBatchFile;


  procedure checkMsisdnBatchStatus(strBatchNum in varchar2,batchFileName in varchar2,fileStatus in varchar2,results out cursorType,retid out number, retmsg out varchar2)
    is
    temp_flag number := -1;
    temp_sql_list varchar2(2048) := '';
  begin
      select count(*) into temp_flag from dm_dataseg_batch where data_batch_num=strBatchNum;
      if temp_flag < 0 then
        retid  := -1;
        retmsg := '号段批次[' || strBatchNum || ']不存在,请确认！';
        return;
      end if;

     temp_sql_list := 'select data_batch_num,data_batch_file,status ';
     temp_sql_list :=temp_sql_list||'from DM_DATASEG_BATCH where 1 = 1';

     if strBatchNum is not null then
        temp_sql_list := temp_sql_list || ' and data_batch_num='''||strBatchNum||'''';
     end if;
     if batchFileName is not null then
        temp_sql_list := temp_sql_list || ' and data_batch_file = '''||batchFileName||'''';
     end if;
     if fileStatus is not null then
        temp_sql_list := temp_sql_list || ' and STATUS = '''||fileStatus||'''';
     end if;

    open results for temp_sql_list;

    retid  := 0;
    retmsg  := '成功';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end checkMsisdnBatchStatus;

 
  procedure selRegionHlrSegInfo(strRegionName in varchar2,strNetSeg in varchar2,results OUT cursorType,retid out number, retmsg out varchar2)
    is
    temp_flag number := -1;
  begin

       select count(*) into temp_flag
          from dm_hlr h ,dm_region r,dm_hlr_link l,dm_hlr_link k
          where h.region_code=r.region_code and r.region_name=l.attr_name
          and l.attr_id=k.parent_id and k.attr_name=h.hlr_name
          and r.region_name=strRegionName and h.hlr_code=strNetSeg;

      if temp_flag < 1 then
        retid  := -1;
        retmsg := '地区[' || strRegionName || ']与网段[' || strNetSeg || ']对应关系不存在,请确认！';
        return;
      end if;

    OPEN results FOR
          select r.region_name REGION_NAME,r.region_code REGION_CODE,
          h.hlr_name HLR_NAME,h.hlr_code HLR_CODE,k.attr_id ATTR_ID
          from dm_hlr h ,dm_region r,dm_hlr_link l,dm_hlr_link k
          where h.region_code=r.region_code
          and r.region_name=l.attr_name
          and l.attr_id=k.parent_id
          and k.attr_name=h.hlr_name
          and r.region_name=strRegionName
          and h.hlr_code=strNetSeg;

    retid  := 0;
    retmsg  := '查询成功';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end selRegionHlrSegInfo;


  procedure saveOrUpdateInfo(strBatchNum in varchar2,batchFileName in varchar2,fileStatus in varchar2,batchCount in varchar2,strNotes in varchar2,retid out number, retmsg out varchar2)
    is
    temp_flag number := -1;
    temp_sql_list varchar2(2048) := '';
  begin
      select count(*) into temp_flag from dm_dataseg_batch where data_batch_num=strBatchNum;
      if temp_flag < 0 then
        retid  := -1;
        retmsg := '号段批次[' || strBatchNum || ']不存在,请确认！';
        return;
      end if;

     temp_sql_list := 'update dm_dataseg_batch set status='||fileStatus||'';
     if batchCount is not null then
        temp_sql_list := temp_sql_list || ',data_batch_count = '||batchCount||'';
     end if;
     if batchFileName is not null then
        temp_sql_list := temp_sql_list || ',data_batch_file = '''||batchFileName||'''';
     end if;

     if strNotes is not null then
        temp_sql_list := temp_sql_list || ',notes = '''||strNotes||'''';
     end if;

     temp_sql_list := temp_sql_list || ' where data_batch_num='''||strBatchNum||'''';

    execute immediate temp_sql_list;
    commit;

    retid  := 0;
    retmsg  := '修改成功';

    exception
      when others then
        retid  := sqlcode;
        retmsg  := sqlerrm;
  end saveOrUpdateInfo;
  
function postgresql(p_tname varchar2, p_colname varchar2)
return varchar2 is
v_tmp    varchar2(200);
v_result varchar2(200);
v_cum    sys_refcursor;
begin
open v_cum for 'select ' || p_colname || ' from ' || p_tname;
loop
    fetch v_cum into v_tmp;
    exit when v_cum%notfound;
    v_result := v_result || v_tmp||',';
end loop;
v_result :=SUBSTR(v_result,1,length(v_result)-1);
return v_result;
end postgresql;

end DM_PKG_DATA_SEG;
//

delimiter ;//
